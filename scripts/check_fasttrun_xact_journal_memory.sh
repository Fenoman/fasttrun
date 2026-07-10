#!/usr/bin/env bash
#
# Проверяет журнал изменений по уровням подтранзакций. Обработчик должен
# обходить только завершившийся уровень, а после отката память должна
# возвращаться к прогретому уровню в том же серверном процессе.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-xact-journal.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_xact_journal}

cleanup()
{
	if [ -f "$DATA/postmaster.pid" ]; then
		"$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

"$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on" \
	-w start >/dev/null
"$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

debug_assertions=$("$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')
if [ "$debug_assertions" != on ] && [ "${FASTTRUN_ALLOW_RELEASE_TEST_BUILD:-0}" != 1 ]; then
	echo "SKIP: для проверки журнала нужна сборка PostgreSQL с cassert" >&2
	exit 77
fi

cat >"$WORKDIR/check.sql" <<'SQL'
\set ON_ERROR_STOP 1
CREATE EXTENSION fasttrun;
CREATE FUNCTION pg_temp.fasttrun_test_subxact_visits(boolean)
RETURNS bigint
AS '$libdir/fasttrun', 'fasttrun_test_subxact_visits'
LANGUAGE C STRICT;

DO $warm$
DECLARE i int;
BEGIN
  FOR i IN 1..20 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_warm_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_warm_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_warm_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат прогретого уровня';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$warm$;

SELECT 'WARM_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

BEGIN;
SELECT pg_temp.fasttrun_test_subxact_visits(true);
DO $visits$
DECLARE i int;
BEGIN
  FOR i IN 1..200 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_visit_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_visit_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_visit_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат уровня обхода';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$visits$;
SELECT 'VISITS ' || pg_temp.fasttrun_test_subxact_visits(false);
SELECT 'VISIT_FRAMES ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame';
COMMIT;

BEGIN;
DO $memory$
DECLARE i int;
BEGIN
  FOR i IN 1..500 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_frame_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_frame_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_frame_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат уровня';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$memory$;
SELECT 'FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'FINAL_FRAMES ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame';
SELECT 'FINAL_CONTEXT ' || name || ' ' || used_bytes
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%'
ORDER BY name;
COMMIT;
SQL

"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/check.sql" >"$WORKDIR/check.out" 2>"$WORKDIR/check.err" || {
	cat "$WORKDIR/check.err" >&2
	exit 1
}

value()
{
	awk -v key="$1" '$1 == key {print $2}' "$WORKDIR/check.out" | tail -1
}

warm_used=$(value WARM_USED)
visits=$(value VISITS)
visit_frames=$(value VISIT_FRAMES)
final_used=$(value FINAL_USED)
final_frames=$(value FINAL_FRAMES)

for item in "$warm_used" "$visits" "$visit_frames" "$final_used" "$final_frames"; do
	case "$item" in
		''|*[!0-9]*) echo "неверная метрика журнала: $item" >&2; exit 1 ;;
	esac
done

growth=$((final_used - warm_used))
[ "$growth" -lt 0 ] && growth=0

echo "журнал транзакций: обходы=$visits прогрев=$warm_used итог=$final_used рост=$growth уровни_при_обходе=$visit_frames уровни_в_конце=$final_frames"

if [ "$visits" -gt 400 ]; then
	echo "FAIL: обработчик подтранзакций обошёл $visits таблиц, ожидалось не более 400" >&2
	exit 1
fi
if [ "$visit_frames" -ne 0 ] || [ "$final_frames" -ne 0 ]; then
	echo "FAIL: остались уровни транзакции: при обходе=$visit_frames, в конце=$final_frames" >&2
	exit 1
fi
if [ "$growth" -gt 65536 ]; then
	echo "FAIL: контексты fasttrun выросли на $growth байт, предел 65536" >&2
	exit 1
fi

echo "проверка памяти журнала транзакций прошла: обходы=$visits рост=$growth активных_уровней=0"
