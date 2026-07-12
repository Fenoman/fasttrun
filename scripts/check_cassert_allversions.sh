#!/usr/bin/env bash
#
# check_cassert_allversions.sh — прогон regress-набора fasttrun на
# cassert-сборках PostgreSQL 16/17/18 с детекцией assert-падений (TRAP).
#
# Зачем: обычный installcheck на release-сборке PG НЕ ловит нарушения
# инвариантов, которые PostgreSQL проверяет только под --enable-cassert.
# У fasttrun вся архитектура xact-колбэков опирается на то, что в
# TRANS_COMMIT/TRANS_ABORT запрещён каталожный доступ (Assert(IsTransactionState())
# в RelationIdGetRelation; PG18 добавил AssertCouldGetRelation).  Эти
# ассерты обязаны проверяться живым cassert-бэкендом на КАЖДОЙ поддержанной
# версии, а не только на PG16.
#
# Что делает: для каждой заданной cassert-сборки чисто пересобирает
# расширение, ставит его, гоняет все REGRESS-тесты и считает дельту
# строк "TRAP:" в логе сервера.  Успех = все тесты ok И нулевая дельта TRAP.
#
# --- ПРЕДУСЛОВИЕ: cassert-сборки ядра -----------------------------------
# Скрипт НЕ собирает ядро (это ~5 мин × 3 версии и специфично для машины).
# Собрать cassert-ядро версии N один раз:
#
#   git -C <postgres-src> worktree add --detach <SRC_N> REL_N_STABLE
#   cd <SRC_N>
#   ./configure --prefix=<PFX_N> --enable-cassert --enable-debug \
#               --without-icu --without-readline
#   make -j8 && make install
#   [ -x <PFX_N>/lib/postgresql/pgxs/.../pg_regress ] || \
#       make -C src/test/regress install
#   <PFX_N>/bin/initdb -D <DATA_N> --locale=ru_RU.UTF-8 -E UTF8
#   <PFX_N>/bin/pg_ctl -D <DATA_N> -l <SRV_N>.log \
#       -o "-p <PORT_N> -c listen_addresses=127.0.0.1 \
#           -c unix_socket_directories=<SHORT_SOCKDIR> -c fsync=off" -w start
#
# ВАЖНО (macOS): unix_socket_directories должен быть КОРОТКИМ путём
# (лимит сокета 104 байта) -- длинный scratch-путь не влезает.
#
# --- ЗАПУСК -------------------------------------------------------------
# Передать сборки через FT_CASSERT_TARGETS: "версия:pg_config:port:server_log"
# через запятую.  Пример:
#
#   FT_CASSERT_TARGETS="\
#   16:/scratch/pg16-cassert/bin/pg_config:5546:/scratch/server16.log,\
#   17:/scratch/pg17-cassert/bin/pg_config:5547:/scratch/server17.log,\
#   18:/scratch/pg18-cassert/bin/pg_config:5548:/scratch/server18.log" \
#     bash scripts/check_cassert_allversions.sh
#
# Каждый сервер должен быть УЖЕ ПОДНЯТ на своём порту (см. предусловие), но
# без fasttrun в shared_preload_libraries: расширение устанавливается после
# старта сервера и должно загрузиться из новой сборки в новом процессе.
# Набор должен содержать ровно по одной цели PG16, PG17 и PG18. Скрипт
# сверяет метку цели с pg_config и живым сервером, а также проверяет cassert.
#
# КЛЮЧЕВОЙ УРОК (не удалять): для cross-version пересборки НЕДОСТАТОЧНО
# `rm -f fasttrun.o`.  PGXS-сборка переиспользует оставшиеся fasttrun.dylib
# / .bc / .so от прошлой версии -> расширение линкуется со СТАРЫМИ inline'ами
# (напр. RelationGetSmgr инлайнит smgrsetowner из PG16 вместо smgrpin из
# PG17/18) -> dlopen падает с "Symbol not found: _smgrsetowner" и ВСЕ тесты
# падают ложно.  Поэтому ниже чистятся ВСЕ артефакты.
#
set -uo pipefail

# C-локаль для старта: без неё тестовый postmaster на macOS падает с
# "postmaster became multithreaded during startup".  installcheck ниже
# переопределяет на ru_RU.UTF-8 внутри своего окружения.
export LC_ALL=C LANG=C

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

TARGETS=${FT_CASSERT_TARGETS:-}
if [ -z "$TARGETS" ]; then
	echo "ОШИБКА: задайте FT_CASSERT_TARGETS (см. шапку скрипта)." >&2
	exit 2
fi
BRIN_LEVEL=${FASTTRUN_BRIN_LEVEL:-smoke}
case "$BRIN_LEVEL" in
	smoke|full) ;;
	*)
		echo "ОШИБКА: FASTTRUN_BRIN_LEVEL должен быть smoke или full." >&2
		exit 2
		;;
esac

# Каталог вывода pg_regress: НЕ дефолтный results/ в корне репо -- он может
# принадлежать root (наследие sudo-прогонов) и ломать pg_regress.
OUTDIR=${FT_CASSERT_OUTDIR:-$(mktemp -d "${TMPDIR:-/tmp}/ft-cassert.XXXXXX")}
mkdir -p "$OUTDIR"
EXPECTED_REGRESS=13
EXPECTED_MAJORS="16 17 18 "

IFS=',' read -r -a TARGET_ARR <<< "$TARGETS"
if [ "${#TARGET_ARR[@]}" -ne 3 ]; then
	echo "ОШИБКА: FT_CASSERT_TARGETS должен содержать ровно три цели: PG16, PG17 и PG18." >&2
	exit 2
fi

target_majors=()
for t in "${TARGET_ARR[@]}"; do
	IFS=':' read -r ver pgcfg port srvlog extra <<< "$t"
	if [ -z "$ver" ] || [ -z "$pgcfg" ] || [ -z "$port" ] || \
		[ -z "$srvlog" ] || [ -n "${extra:-}" ]; then
		echo "ОШИБКА: неверная цель '$t'; нужен формат версия:pg_config:port:server_log." >&2
		exit 2
	fi
	case "$ver" in
		16|17|18) ;;
		*)
			echo "ОШИБКА: неподдерживаемая метка версии PG$ver в '$t'." >&2
			exit 2
			;;
	esac
	target_majors+=("$ver")
done

actual_majors=$(printf '%s\n' "${target_majors[@]}" | sort -n | tr '\n' ' ')
if [ "$actual_majors" != "$EXPECTED_MAJORS" ]; then
	echo "ОШИБКА: нужны уникальные цели PG16, PG17 и PG18; получено: $actual_majors" >&2
	exit 2
fi

HARNESS_ENV_ARGS=(
	-u PG_RUN_AS
	-u PGPORT
	-u FASTTRUN_BRIN_PORT
	-u FASTTRUN_BRIN_LEVEL
	-u FASTTRUN_CACHE_INIT_MODE
	-u FASTTRUN_ALLOW_RELEASE_TEST_BUILD
	-u FASTTRUN_PUBLICATION_CASE
	-u FASTTRUN_TRACKING_PERSISTENCE_CASE
	-u WORKDIR
	-u DBNAME
	-u PSQL
	-u INITDB
	-u PG_CTL
	-u CREATEDB
	-u PYTHON
	-u KEEP_WORKDIR
	-u PGUSER
	-u PGHOST
	-u PGDATABASE
	-u PGOPTIONS
	-u PGSERVICE
	-u PGSERVICEFILE
	-u DESTDIR
	-u MAKEFLAGS
	-u GNUMAKEFLAGS
	-u MFLAGS
	-u MAKELEVEL
	-u MAKEFILES
	-u MAKEOVERRIDES
	-u REGRESS
	-u REGRESS_OPTS
	-u PG_REGRESS
	-u BASH_ENV
	-u ENV
)

run_isolated_harness()
{
	local pgcfg=$1
	shift
	if [ -n "${PG_RUN_AS:-}" ]; then
		if ! command -v runuser >/dev/null 2>&1; then
			echo "runuser is required for PG_RUN_AS=$PG_RUN_AS" >&2
			return 127
		fi
		runuser -u "$PG_RUN_AS" -- env "${HARNESS_ENV_ARGS[@]}" \
			PG_CONFIG="$pgcfg" "$@"
	else
		env "${HARNESS_ENV_ARGS[@]}" PG_CONFIG="$pgcfg" "$@"
	fi
}

canonical_dir()
{
	(cd "$1" 2>/dev/null && pwd -P)
}

# Все цели проверяются до первой сборки, чтобы частичный набор не мог выглядеть
# как успешный релизный прогон.
target_roles=()
for t in "${TARGET_ARR[@]}"; do
	IFS=':' read -r ver pgcfg port srvlog <<< "$t"
	if [ ! -x "$pgcfg" ]; then
		echo "ОШИБКА: pg_config не исполняемый для PG$ver: $pgcfg" >&2
		exit 2
	fi
	pgcfg_major=$(
		"$pgcfg" --version 2>/dev/null |
			sed -nE 's/^PostgreSQL[[:space:]]+([0-9]+)(\..*)?$/\1/p'
	)
	if [ "$pgcfg_major" != "$ver" ]; then
		echo "ОШИБКА: цель PG$ver использует $($pgcfg --version 2>&1)." >&2
		exit 2
	fi
	case "$port" in
		''|*[!0-9]*)
			echo "ОШИБКА: неверный порт для PG$ver: $port" >&2
			exit 2
			;;
	esac
	if [ ! -r "$srvlog" ]; then
		echo "ОШИБКА: журнал сервера PG$ver недоступен для чтения: $srvlog" >&2
		exit 2
	fi
	psql="$($pgcfg --bindir)/psql"
	if [ ! -x "$psql" ]; then
		echo "ОШИБКА: psql не исполняемый для PG$ver: $psql" >&2
		exit 2
	fi
	server_info=$(
		run_isolated_harness "$pgcfg" "$psql" \
			-h 127.0.0.1 -p "$port" -d "${FT_CASSERT_DATABASE:-postgres}" \
			-XAtq -F '|' -v ON_ERROR_STOP=1 -c \
			"SELECT current_setting('server_version_num'),
			        current_setting('debug_assertions'),
			        EXISTS (
			          SELECT 1
			          FROM unnest(string_to_array(
			            current_setting('shared_preload_libraries'), ',')) AS item
			          WHERE regexp_replace(btrim(item), '^.*/', '')
			                ~ '^fasttrun(\\.(so|dylib))?$'
			        ),
			        (SELECT setting FROM pg_catalog.pg_config
			          WHERE name = 'PKGLIBDIR'),
			        (SELECT setting FROM pg_catalog.pg_config
			          WHERE name = 'SHAREDIR'),
			        current_user" 2>/dev/null
	) || {
		echo "ОШИБКА: сервер PG$ver не отвечает на 127.0.0.1:$port." >&2
		exit 2
	}
	IFS='|' read -r server_version_num debug_assertions fasttrun_preloaded \
		server_pkglibdir server_sharedir test_role \
		<<< "$server_info"
	case "$server_version_num" in
		''|*[!0-9]*)
			echo "ОШИБКА: сервер PG$ver вернул неверный server_version_num: $server_version_num" >&2
			exit 2
			;;
	esac
	server_major=$((server_version_num / 10000))
	if [ "$server_major" -ne "$ver" ]; then
		echo "ОШИБКА: цель PG$ver указывает на сервер PG$server_major (порт $port)." >&2
		exit 2
	fi
	if [ "$debug_assertions" != on ]; then
		echo "ОШИБКА: сервер PG$ver запущен без debug_assertions." >&2
		exit 2
	fi
	if [ "$fasttrun_preloaded" != f ]; then
		echo "ОШИБКА: сервер PG$ver уже загрузил fasttrun через shared_preload_libraries; перезапустите его без fasttrun." >&2
		exit 2
	fi
	configured_pkglibdir=$(canonical_dir "$($pgcfg --pkglibdir)") || {
		echo "ОШИБКА: PKGLIBDIR из pg_config PG$ver недоступен." >&2
		exit 2
	}
	configured_sharedir=$(canonical_dir "$($pgcfg --sharedir)") || {
		echo "ОШИБКА: SHAREDIR из pg_config PG$ver недоступен." >&2
		exit 2
	}
	live_pkglibdir=$(canonical_dir "$server_pkglibdir") || {
		echo "ОШИБКА: PKGLIBDIR живого сервера PG$ver недоступен." >&2
		exit 2
	}
	live_sharedir=$(canonical_dir "$server_sharedir") || {
		echo "ОШИБКА: SHAREDIR живого сервера PG$ver недоступен." >&2
		exit 2
	}
	if [ "$live_pkglibdir" != "$configured_pkglibdir" ] || \
		[ "$live_sharedir" != "$configured_sharedir" ]; then
		echo "ОШИБКА: pg_config и сервер PG$ver относятся к разным установкам." >&2
		echo "  pg_config: $configured_pkglibdir | $configured_sharedir" >&2
		echo "  сервер:    $live_pkglibdir | $live_sharedir" >&2
		exit 2
	fi
	if [ -z "$test_role" ]; then
		echo "ОШИБКА: не удалось определить роль для тестов PG$ver." >&2
		exit 2
	fi
	target_roles+=("$test_role")

	log_probe="FASTTRUN_CASSERT_LOG_PROBE_$$_${ver}_$(date +%s)"
	if ! run_isolated_harness "$pgcfg" "$psql" \
		-h 127.0.0.1 -p "$port" -d "${FT_CASSERT_DATABASE:-postgres}" \
		-XAtq -v ON_ERROR_STOP=1 \
		-c "DO 'BEGIN RAISE LOG ''$log_probe''; END'" >/dev/null 2>&1; then
		echo "ОШИБКА: не удалось записать контрольную строку в журнал PG$ver." >&2
		exit 2
	fi
	if ! grep -Fq "$log_probe" "$srvlog"; then
		sleep 1
	fi
	if ! grep -Fq "$log_probe" "$srvlog"; then
		echo "ОШИБКА: $srvlog не является журналом указанного сервера PG$ver." >&2
		exit 2
	fi
done

overall_rc=0

target_idx=0
for t in "${TARGET_ARR[@]}"; do
	IFS=':' read -r ver pgcfg port srvlog <<< "$t"
	test_role=${target_roles[$target_idx]}
	target_idx=$((target_idx + 1))
	echo "========================================================"
	echo "PG${ver} cassert  (pg_config=$pgcfg  port=$port)"
	echo "========================================================"

	# 1. Чистая пересборка -- ВСЕ артефакты, не только .o (см. урок в шапке).
	rm -f fasttrun.o fasttrun.dylib fasttrun.so fasttrun.bc
	if ! env "${HARNESS_ENV_ARGS[@]}" make PG_CONFIG="$pgcfg" \
		> "$OUTDIR/build${ver}.log" 2>&1; then
		echo "  СБОРКА расширения FAILED (см. $OUTDIR/build${ver}.log)" >&2
		tail -5 "$OUTDIR/build${ver}.log" >&2
		overall_rc=1
		continue
	fi
	warns=$(grep -ci 'warning:' "$OUTDIR/build${ver}.log")
	echo "  сборка ok, warnings: $warns"
	if ! env "${HARNESS_ENV_ARGS[@]}" make install PG_CONFIG="$pgcfg" \
		>> "$OUTDIR/build${ver}.log" 2>&1; then
		echo "  не удалось установить расширение (см. $OUTDIR/build${ver}.log)" >&2
		overall_rc=1
		continue
	fi

	# 2. Снять базовую отметку TRAP до прогона.
	trap_before=$(grep -c TRAP "$srvlog" 2>/dev/null || true)
	trap_before=${trap_before//[!0-9]/}; trap_before=${trap_before:-0}

	# 3. installcheck с ru_RU.UTF-8 и обособленным outputdir.
	# (Скрипт работает без set -e -- см. `set -uo pipefail` в шапке -- поэтому
	# ненулевой installcheck/grep не должен ронять цикл; коды разбираем сами.)
	rm -rf "$OUTDIR/rc${ver}"
	env "${HARNESS_ENV_ARGS[@]}" \
		LC_ALL=ru_RU.UTF-8 LANG=ru_RU.UTF-8 PGUSER="$test_role" \
		make installcheck PG_CONFIG="$pgcfg" PGPORT="$port" PGHOST=127.0.0.1 \
		REGRESS_OPTS="--outputdir=$OUTDIR/rc${ver}" \
		> "$OUTDIR/check${ver}.log" 2>&1
	check_rc=$?

	passed=$(grep -cE '^ok ' "$OUTDIR/check${ver}.log" 2>/dev/null); passed=${passed//[!0-9]/}; passed=${passed:-0}
	failed=$(grep -cE '^not ok ' "$OUTDIR/check${ver}.log" 2>/dev/null); failed=${failed//[!0-9]/}; failed=${failed:-0}

	# 4. Дельта TRAP.
	if [ ! -r "$srvlog" ]; then
		echo "  журнал сервера стал недоступен: $srvlog" >&2
		trap_after=$trap_before
		overall_rc=1
	else
		trap_after=$(grep -c TRAP "$srvlog" 2>/dev/null || true)
		trap_after=${trap_after//[!0-9]/}; trap_after=${trap_after:-0}
	fi
	trap_delta=$((trap_after - trap_before))

	fault_rc=125
	cache_init_rc=125
	brin_rc=125
	order_rc=125
	memory_rc=125
	planner_rc=125
	publication_rc=125
	persistence_rc=125
	if [ "$check_rc" -eq 0 ] && [ "$passed" -eq "$EXPECTED_REGRESS" ] && \
		[ "$failed" -eq 0 ]; then
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_fault_matrix.sh \
			>"$OUTDIR/fault${ver}.log" 2>&1
		fault_rc=$?
		run_isolated_harness "$pgcfg" env FASTTRUN_CACHE_INIT_MODE=cassert \
			scripts/check_fasttrun_cache_init_faults.sh \
			>"$OUTDIR/cache_init${ver}.log" 2>&1
		cache_init_rc=$?
		run_isolated_harness "$pgcfg" env FASTTRUN_BRIN_LEVEL="$BRIN_LEVEL" \
			scripts/check_fasttrun_brin_stress.sh \
			>"$OUTDIR/brin${ver}.log" 2>&1
		brin_rc=$?
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_tracking_order.sh \
			>"$OUTDIR/order${ver}.log" 2>&1
		order_rc=$?
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_xact_journal_memory.sh \
			>"$OUTDIR/memory${ver}.log" 2>&1
		memory_rc=$?
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_planner_probes.sh all \
			>"$OUTDIR/planner${ver}.log" 2>&1
		planner_rc=$?
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_publication_atomicity.sh \
			>"$OUTDIR/publication${ver}.log" 2>&1
		publication_rc=$?
		run_isolated_harness "$pgcfg" scripts/check_fasttrun_tracking_persistence.sh \
			>"$OUTDIR/persistence${ver}.log" 2>&1
		persistence_rc=$?
	fi

	echo "  тесты: пройдено ${passed}/${EXPECTED_REGRESS}, ошибок ${failed}, новых TRAP ${trap_delta}; дополнительные проверки: ошибки=${fault_rc}, cache-init=${cache_init_rc}, BRIN-${BRIN_LEVEL}=${brin_rc}, порядок=${order_rc}, память=${memory_rc}, планировщик=${planner_rc}, публикация=${publication_rc}, persistence=${persistence_rc}"
	if [ "$check_rc" -ne 0 ] || [ "$passed" -ne "$EXPECTED_REGRESS" ] || \
		[ "$failed" -ne 0 ]; then
		echo "  РЕГРЕСС: см. $OUTDIR/rc${ver}/regression.diffs" >&2
		overall_rc=1
	fi
	if [ "$fault_rc" -ne 0 ] || [ "$cache_init_rc" -ne 0 ] || \
		[ "$brin_rc" -ne 0 ] || [ "$order_rc" -ne 0 ] || \
		[ "$memory_rc" -ne 0 ] || [ "$planner_rc" -ne 0 ] || \
		[ "$publication_rc" -ne 0 ] || [ "$persistence_rc" -ne 0 ]; then
		echo "  не прошли дополнительные проверки; журналы находятся в $OUTDIR" >&2
		overall_rc=1
	fi
	if [ "$trap_delta" -ne 0 ]; then
		echo "  падение на Assert: $(grep TRAP "$srvlog" | tail -"$trap_delta")" >&2
		overall_rc=1
	fi
done

echo "========================================================"
if [ "$overall_rc" -eq 0 ]; then
	echo "ИТОГ: все версии прошли 13/13 тестов и дополнительные проверки; новых TRAP нет."
else
	echo "ИТОГ: есть ошибки тестов или падения на Assert; см. вывод выше и $OUTDIR." >&2
fi
exit "$overall_rc"
