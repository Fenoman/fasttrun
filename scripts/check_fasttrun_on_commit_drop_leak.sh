#!/usr/bin/env bash
#
# Regression-проверка против memory leak в fasttrun analyze cache
# при сценарии CREATE TEMP ... ON COMMIT DROP + fasttrun_analyze в
# долгоживущем backend.
#
# Что было сломано (выявлено внешним ревью 2.3.0-pre):
#   ON COMMIT DROP уничтожает temp таблицу через
#   PreCommit_on_commit_actions() -- до того как стрельнёт наш
#   XACT_EVENT_COMMIT callback.  Этот путь не идёт через утильный
#   ProcessUtility-хук, и fasttrun_evict_temp_relid не отрабатывает.
#   В 2.2.0/2.2.1 fasttrun_cache_commit_xact удалял entry только при
#   !has_relstats, поэтому каждая ON COMMIT DROP таблица оставляла
#   живую запись на dead OID.  Backend с длинной сессией медленно
#   копил мёртвые entries.
#
# Что должно быть после фикса:
#   object_access_hook(OAT_DROP) ставит на relid drop-отметку с subid,
#   fasttrun_cache_commit_xact для каждого touched relid проверяет её
#   (syscache в TRANS_COMMIT запрещён).  Если relation dropped --
#   HASH_REMOVE entry полностью.  Никакого linear growth.
#
# Условия успеха при ITERATIONS=500: сумма used_bytes по всем контекстам
# fasttrun в двух финальных точках не превышает прогретое значение более чем
# на 64 KiB, а число контекстов не меняется. Так учитывается и дочерний кеш.
#
set -euo pipefail

# Пиним C-локаль на весь скрипт: без этого на macOS тестовый postmaster
# падает при старте с "postmaster became multithreaded during startup".
# На Linux C-локаль тоже валидна, поведение не меняется.
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PG_RUN_AS=${PG_RUN_AS:-}
PORT=${PGPORT:-55457}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-leak.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_leak}
ITERATIONS=${ITERATIONS:-500}
MAX_GROWTH_BYTES=${MAX_GROWTH_BYTES:-65536}

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

require_cmd()
{
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "не нашёл команду: $1" >&2
		exit 1
	fi
}

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi

if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR"
fi
run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null

# Reproducer: один backend, $ITERATIONS отдельных xact'ов, каждый
# создаёт temp таблицу с одним и тем же именем через ON COMMIT DROP,
# наполняет, делает fasttrun_analyze, коммитит.  ON COMMIT DROP
# гарантирует что каждая итерация работает с НОВЫМ OID -- старый OID
# становится сиротой, по которому до фикса оставалась запись в кэше.
cat >"$WORKDIR/leak.sql" <<SQL
\\set ON_ERROR_STOP on
\\pset format unaligned
\\pset tuples_only on
\\pset fieldsep '|'

SELECT 'BASELINE|' || coalesce(sum(total_bytes), 0) || '|' ||
       coalesce(sum(used_bytes), 0) || '|' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

-- "Warmup" -- forced lazy init: первая итерация аллоцирует HTAB.
-- Это нужно ДО baseline чтобы наш baseline учитывал служебные слоты
-- пустой HTAB, а не "не существует вовсе".
BEGIN;
CREATE TEMP TABLE ft_leak_warmup (id int) ON COMMIT DROP;
INSERT INTO ft_leak_warmup SELECT 1;
SELECT fasttrun_analyze('ft_leak_warmup');
COMMIT;

SELECT 'WARM|' || coalesce(sum(total_bytes), 0) || '|' ||
       coalesce(sum(used_bytes), 0) || '|' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

-- N циклов worst-case паттерна.
DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$ITERATIONS LOOP
    EXECUTE 'CREATE TEMP TABLE ft_leak_t (id int) ON COMMIT DROP';
    EXECUTE 'INSERT INTO ft_leak_t SELECT generate_series(1, 100)';
    PERFORM fasttrun_analyze('ft_leak_t');
    COMMIT;
  END LOOP;
END\$do\$;

SELECT 'FINAL|' || coalesce(sum(total_bytes), 0) || '|' ||
       coalesce(sum(used_bytes), 0) || '|' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'STABLE|' || coalesce(sum(total_bytes), 0) || '|' ||
       coalesce(sum(used_bytes), 0) || '|' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SQL

psql_rc=0
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/leak.sql" \
	>"$WORKDIR/leak.out" 2>"$WORKDIR/leak.err" || psql_rc=$?

# Реальный сбой измерения: psql упал (ошибка соединения или SQL).
# Это НЕ то же самое, что "запрос отработал, но контекст отсутствует".
if [ "$psql_rc" -ne 0 ]; then
	echo "psql завершился с кодом $psql_rc -- измерение не удалось:" >&2
	cat "$WORKDIR/leak.err" >&2
	exit 1
fi

metric()
{
	awk -F'|' -v key="$1" '$1 == key {print $2 " " $3 " " $4}' \
		"$WORKDIR/leak.out" | tail -1
}

read -r warm_total warm_used warm_contexts <<<"$(metric WARM)"
read -r final_total final_used final_contexts <<<"$(metric FINAL)"
read -r stable_total stable_used stable_contexts <<<"$(metric STABLE)"
for value in "$warm_total" "$warm_used" "$warm_contexts" \
	"$final_total" "$final_used" "$final_contexts" \
	"$stable_total" "$stable_used" "$stable_contexts"; do
	case "$value" in
		''|*[!0-9]*) echo "неверная метрика памяти ON COMMIT DROP: $value" >&2; exit 1 ;;
	esac
done

growth=$((final_used - warm_used))
stable_growth=$((stable_used - warm_used))

echo ""
echo "fasttrun после прогрева, всего/занято/контекстов: $warm_total/$warm_used/$warm_contexts"
echo "fasttrun после циклов, всего/занято/контекстов:  $final_total/$final_used/$final_contexts"
echo "fasttrun после ожидания, всего/занято/контекстов: $stable_total/$stable_used/$stable_contexts"
echo "рост занятой памяти, сразу/после ожидания:       $growth/$stable_growth байт"
echo "лимит:                            $MAX_GROWTH_BYTES байт"

if [ "$growth" -gt "$MAX_GROWTH_BYTES" ] || \
	[ "$stable_growth" -gt "$MAX_GROWTH_BYTES" ] || \
	[ "$final_contexts" -ne "$warm_contexts" ] || \
	[ "$stable_contexts" -ne "$warm_contexts" ]; then
	echo ""
	echo "FAIL: память fasttrun не стабилизировалась за $ITERATIONS циклов" >&2
	echo "записи ON COMMIT DROP не удаляются в fasttrun_cache_commit_xact" >&2
	echo "См. fasttrun.c:fasttrun_cache_commit_xact (SearchSysCacheExists1 проверка)" >&2
	exit 1
fi

echo ""
echo "проверка памяти fasttrun при ON COMMIT DROP прошла"
