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
# Каждый сервер должен быть УЖЕ ПОДНЯТ на своём порту (см. предусловие).
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

# Каталог вывода pg_regress: НЕ дефолтный results/ в корне репо -- он может
# принадлежать root (наследие sudo-прогонов) и ломать pg_regress.
OUTDIR=${FT_CASSERT_OUTDIR:-$(mktemp -d "${TMPDIR:-/tmp}/ft-cassert.XXXXXX")}
mkdir -p "$OUTDIR"

overall_rc=0

IFS=',' read -r -a TARGET_ARR <<< "$TARGETS"
for t in "${TARGET_ARR[@]}"; do
	IFS=':' read -r ver pgcfg port srvlog <<< "$t"
	echo "========================================================"
	echo "PG${ver} cassert  (pg_config=$pgcfg  port=$port)"
	echo "========================================================"

	if [ ! -x "$pgcfg" ]; then
		echo "  ПРОПУСК: pg_config не исполняемый: $pgcfg" >&2
		overall_rc=1
		continue
	fi

	# 1. Чистая пересборка -- ВСЕ артефакты, не только .o (см. урок в шапке).
	rm -f fasttrun.o fasttrun.dylib fasttrun.so fasttrun.bc
	if ! make PG_CONFIG="$pgcfg" > "$OUTDIR/build${ver}.log" 2>&1; then
		echo "  СБОРКА расширения FAILED (см. $OUTDIR/build${ver}.log)" >&2
		tail -5 "$OUTDIR/build${ver}.log" >&2
		overall_rc=1
		continue
	fi
	warns=$(grep -ci 'warning:' "$OUTDIR/build${ver}.log")
	echo "  сборка ok, warnings: $warns"
	make install PG_CONFIG="$pgcfg" >> "$OUTDIR/build${ver}.log" 2>&1

	# 2. Снять базовую отметку TRAP до прогона.
	trap_before=0
	[ -f "$srvlog" ] && trap_before=$(grep -c TRAP "$srvlog" 2>/dev/null)
	trap_before=${trap_before//[!0-9]/}; trap_before=${trap_before:-0}

	# 3. installcheck с ru_RU.UTF-8 и обособленным outputdir.
	# (Скрипт работает без set -e -- см. `set -uo pipefail` в шапке -- поэтому
	# ненулевой installcheck/grep не должен ронять цикл; коды разбираем сами.)
	rm -rf "$OUTDIR/rc${ver}"
	LC_ALL=ru_RU.UTF-8 LANG=ru_RU.UTF-8 \
		make installcheck PG_CONFIG="$pgcfg" PGPORT="$port" PGHOST=127.0.0.1 \
		REGRESS_OPTS="--outputdir=$OUTDIR/rc${ver}" \
		> "$OUTDIR/check${ver}.log" 2>&1
	check_rc=$?

	passed=$(grep -cE '^ok ' "$OUTDIR/check${ver}.log" 2>/dev/null); passed=${passed//[!0-9]/}; passed=${passed:-0}
	failed=$(grep -cE '^not ok ' "$OUTDIR/check${ver}.log" 2>/dev/null); failed=${failed//[!0-9]/}; failed=${failed:-0}

	# 4. Дельта TRAP.
	trap_after=0
	[ -f "$srvlog" ] && trap_after=$(grep -c TRAP "$srvlog" 2>/dev/null)
	trap_after=${trap_after//[!0-9]/}; trap_after=${trap_after:-0}
	trap_delta=$((trap_after - trap_before))

	echo "  тесты: ${passed} ok, ${failed} not ok · TRAP delta: ${trap_delta}"
	if [ "$check_rc" -ne 0 ] || [ "$failed" -ne 0 ]; then
		echo "  РЕГРЕСС: см. $OUTDIR/rc${ver}/regression.diffs" >&2
		overall_rc=1
	fi
	if [ "$trap_delta" -ne 0 ]; then
		echo "  ASSERT-ПАДЕНИЕ (cassert TRAP): $(grep TRAP "$srvlog" | tail -"$trap_delta")" >&2
		overall_rc=1
	fi
done

echo "========================================================"
if [ "$overall_rc" -eq 0 ]; then
	echo "ИТОГ: все cassert-версии зелёные, ноль TRAP."
else
	echo "ИТОГ: есть регрессы/TRAP -- см. вывод выше и $OUTDIR." >&2
fi
exit "$overall_rc"
