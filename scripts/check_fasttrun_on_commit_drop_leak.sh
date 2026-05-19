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
#   fasttrun_cache_commit_xact для каждого touched relid делает
#   SearchSysCacheExists1(RELOID).  Если relation gone -- HASH_REMOVE
#   entry полностью.  Никакого linear growth.
#
# Pass criteria (по умолчанию ITERATIONS=500):
#   - "fasttrun analyze cache" used_bytes после прогона не должен
#     отличаться от baseline (initial allocation) больше чем на
#     MAX_GROWTH_BYTES (по умолчанию 4096 байт -- запас на один-два
#     служебных слота HTAB).  Если кэш растёт линейно по N циклов --
#     leak вернулся.
#
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PORT=${PGPORT:-55457}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-leak.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_leak}
ITERATIONS=${ITERATIONS:-500}
MAX_GROWTH_BYTES=${MAX_GROWTH_BYTES:-4096}

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
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

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
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

-- Baseline: cache ещё не аллоцирован.
SELECT 'BASELINE_BEGIN';
SELECT name, used_bytes
FROM pg_backend_memory_contexts
WHERE name ~ 'fasttrun analyze cache'
ORDER BY name;
SELECT 'BASELINE_END';

-- "Warmup" -- forced lazy init: первая итерация аллоцирует HTAB.
-- Это нужно ДО baseline чтобы наш baseline учитывал служебные слоты
-- пустой HTAB, а не "не существует вовсе".
BEGIN;
CREATE TEMP TABLE ft_leak_warmup (id int) ON COMMIT DROP;
INSERT INTO ft_leak_warmup SELECT 1;
SELECT fasttrun_analyze('ft_leak_warmup');
COMMIT;

SELECT 'WARMED_BEGIN';
SELECT name, used_bytes
FROM pg_backend_memory_contexts
WHERE name ~ 'fasttrun analyze cache'
ORDER BY name;
SELECT 'WARMED_END';

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

SELECT 'AFTER_BEGIN';
SELECT name, used_bytes
FROM pg_backend_memory_contexts
WHERE name ~ 'fasttrun analyze cache'
ORDER BY name;
SELECT 'AFTER_END';
SQL

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/leak.sql" \
	>"$WORKDIR/leak.out" 2>"$WORKDIR/leak.err"

extract_bytes()
{
	# вывод: "fasttrun analyze cache|<used_bytes>"; берём первое число.
	local begin="$1"
	local end="$2"
	awk -v b="$begin" -v e="$end" '
		$0 == b { capture=1; next }
		$0 == e { capture=0; next }
		capture && /fasttrun analyze cache\|/ {
			n = $0
			sub(/^.*\|/, "", n)
			print n
			exit
		}
	' "$WORKDIR/leak.out"
}

warmed=$(extract_bytes WARMED_BEGIN WARMED_END)
after=$(extract_bytes AFTER_BEGIN AFTER_END)

if [ -z "$warmed" ] || [ -z "$after" ]; then
	echo "не смог снять размер 'fasttrun analyze cache' из вывода:" >&2
	cat "$WORKDIR/leak.out" >&2
	exit 1
fi

growth=$((after - warmed))

echo ""
echo "fasttrun analyze cache на старте (после warmup): $warmed байт"
echo "fasttrun analyze cache после $ITERATIONS циклов: $after байт"
echo "delta:                                          $growth байт"
echo "лимит:                                          $MAX_GROWTH_BYTES байт"

if [ "$growth" -gt "$MAX_GROWTH_BYTES" ]; then
	echo ""
	echo "FAIL: fasttrun analyze cache вырос на $growth байт за $ITERATIONS циклов" >&2
	echo "ON COMMIT DROP entries не эвиктятся в fasttrun_cache_commit_xact" >&2
	echo "См. fasttrun.c:fasttrun_cache_commit_xact (SearchSysCacheExists1 проверка)" >&2
	exit 1
fi

echo ""
echo "regression-проверка fasttrun ON COMMIT DROP leak прошла"
