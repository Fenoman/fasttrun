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
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-xact-journal.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_xact_journal}

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
	if [ -f "$DATA/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then
	command -v runuser >/dev/null 2>&1 || { echo "missing runuser" >&2; exit 1; }
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR"
fi
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

debug_assertions=$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
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
SELECT 'WARM_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'WARM_CONTEXTS ' || count(*)
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

/* Вложенные уровни одной таблицы восстанавливают согласованное состояние. */
CREATE TEMP TABLE ft_same_relid(id int PRIMARY KEY, payload text);
INSERT INTO ft_same_relid VALUES (1, 'original');
SELECT fasttrun_analyze('ft_same_relid');
BEGIN;
DO $nested$
BEGIN
  BEGIN
    UPDATE ft_same_relid SET payload = 'outer' WHERE id = 1;
    PERFORM fasttrun_analyze('ft_same_relid');
    BEGIN
      INSERT INTO ft_same_relid VALUES (2, 'inner');
      PERFORM fasttrun_analyze('ft_same_relid');
      RAISE EXCEPTION USING MESSAGE = 'rollback inner same-relid frame';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback inner same-relid frame' THEN
        RAISE;
      END IF;
    END;
    RAISE EXCEPTION USING MESSAGE = 'rollback outer same-relid frame';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'rollback outer same-relid frame' THEN
      RAISE;
    END IF;
  END;
END
$nested$;
COMMIT;
SELECT 'NESTED_ROWS ' || count(*) FROM ft_same_relid;
SELECT 'NESTED_ORIGINAL ' || count(*)
FROM ft_same_relid WHERE id = 1 AND payload = 'original';
SELECT 'NESTED_FRAMES ' || count(*)
FROM pg_backend_memory_contexts WHERE name = 'fasttrun xact frame';
DROP TABLE ft_same_relid;

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
SELECT 'FINAL_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'FINAL_CONTEXTS ' || count(*)
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

/* Successful calls must release the state left by earlier calls. */
CREATE TEMP TABLE ft_truncate_memory(
  id int PRIMARY KEY,
  payload text
);
ALTER TABLE ft_truncate_memory ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE TEMP TABLE ft_payload AS
SELECT string_agg(md5(s::text), '') AS value
FROM generate_series(1, 450) s;
INSERT INTO ft_truncate_memory SELECT 1, value FROM ft_payload;

BEGIN;
DO $truncate_warm$
DECLARE i int;
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT i, value FROM ft_payload;
  END LOOP;
END
$truncate_warm$;
SELECT 'TRUNCATE_WARM_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

DO $truncate_final$
DECLARE i int;
BEGIN
  FOR i IN 101..2000 LOOP
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT i, value FROM ft_payload;
  END LOOP;
END
$truncate_final$;
SELECT 'TRUNCATE_FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';
COMMIT;

/* A successful retry after each caught error must release the block. */
DO $poison_cycles$
DECLARE i int;
BEGIN
  FOR i IN 1..200 LOOP
    PERFORM set_config('fasttrun.test_failpoint', 'after_user_index:1', false);
    BEGIN
      PERFORM fasttruncate('ft_truncate_memory');
    EXCEPTION WHEN SQLSTATE '55000' THEN
      NULL;
    END;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT 2000 + i, value FROM ft_payload;
  END LOOP;
END
$poison_cycles$;
SELECT 'POISON_FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

/* One active block survives a transaction rollback and uses little memory. */
BEGIN;
DO $active_poison$
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', 'after_user_index:1', false);
  BEGIN
    PERFORM fasttruncate('ft_truncate_memory');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
END
$active_poison$;
ROLLBACK;
SELECT 'ACTIVE_POISON_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';
SELECT fasttruncate('ft_truncate_memory');
SELECT 'REPAIRED_POISON_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

DROP TABLE ft_truncate_memory, ft_payload;
DISCARD TEMP;
SELECT 'STABLE_ONE_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_ONE_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_ONE_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'STABLE_TWO_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_TWO_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_TWO_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SQL

run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/check.sql" >"$WORKDIR/check.out" 2>"$WORKDIR/check.err" || {
	cat "$WORKDIR/check.err" >&2
	exit 1
}

value()
{
	awk -v key="$1" '$1 == key {print $2}' "$WORKDIR/check.out" | tail -1
}

warm_used=$(value WARM_USED)
warm_total=$(value WARM_TOTAL)
warm_contexts=$(value WARM_CONTEXTS)
visits=$(value VISITS)
visit_frames=$(value VISIT_FRAMES)
nest_rows=$(value NESTED_ROWS)
nest_original=$(value NESTED_ORIGINAL)
nest_frames=$(value NESTED_FRAMES)
final_used=$(value FINAL_USED)
final_total=$(value FINAL_TOTAL)
final_contexts=$(value FINAL_CONTEXTS)
final_frames=$(value FINAL_FRAMES)
truncate_warm_used=$(value TRUNCATE_WARM_USED)
truncate_final_used=$(value TRUNCATE_FINAL_USED)
poison_final_used=$(value POISON_FINAL_USED)
active_poison_used=$(value ACTIVE_POISON_USED)
repaired_poison_used=$(value REPAIRED_POISON_USED)
stable_one_total=$(value STABLE_ONE_TOTAL)
stable_one_used=$(value STABLE_ONE_USED)
stable_one_contexts=$(value STABLE_ONE_CONTEXTS)
stable_two_total=$(value STABLE_TWO_TOTAL)
stable_two_used=$(value STABLE_TWO_USED)
stable_two_contexts=$(value STABLE_TWO_CONTEXTS)

for item in "$warm_used" "$warm_total" "$warm_contexts" "$visits" \
	"$visit_frames" "$nest_rows" "$nest_original" "$nest_frames" \
	"$final_used" "$final_total" "$final_contexts" "$final_frames" \
	"$truncate_warm_used" "$truncate_final_used" "$poison_final_used" \
	"$active_poison_used" "$repaired_poison_used" \
	"$stable_one_total" "$stable_one_used" "$stable_one_contexts" \
	"$stable_two_total" "$stable_two_used" "$stable_two_contexts"; do
	case "$item" in
		''|*[!0-9]*) echo "неверная метрика журнала: $item" >&2; exit 1 ;;
	esac
done

growth=$((final_used - warm_used))
[ "$growth" -lt 0 ] && growth=0
truncate_growth=$((truncate_final_used - truncate_warm_used))
[ "$truncate_growth" -lt 0 ] && truncate_growth=0

echo "журнал транзакций: обходы=$visits прогрев=$warm_used итог=$final_used рост=$growth уровни_при_обходе=$visit_frames уровни_в_конце=$final_frames"
echo "память очистки: прогрев=$truncate_warm_used итог=$truncate_final_used рост=$truncate_growth после_восстановления=$repaired_poison_used активная_блокировка=$active_poison_used"
echo "стабильность памяти fasttrun: прогрев=$warm_total/$warm_used/$warm_contexts итог=$final_total/$final_used/$final_contexts повтор1=$stable_one_total/$stable_one_used/$stable_one_contexts повтор2=$stable_two_total/$stable_two_used/$stable_two_contexts"

if [ "$visits" -gt 400 ]; then
	echo "FAIL: обработчик подтранзакций обошёл $visits таблиц, ожидалось не более 400" >&2
	exit 1
fi
if [ "$visit_frames" -ne 0 ] || [ "$final_frames" -ne 0 ]; then
	echo "FAIL: остались уровни транзакции: при обходе=$visit_frames, в конце=$final_frames" >&2
	exit 1
fi
if [ "$nest_rows" -ne 1 ] || [ "$nest_original" -ne 1 ] || \
	[ "$nest_frames" -ne 0 ]; then
	echo "FAIL: неверный откат вложенных уровней одной таблицы: строк=$nest_rows исходных=$nest_original уровней=$nest_frames" >&2
	exit 1
fi
if [ "$growth" -gt 65536 ]; then
	echo "FAIL: контексты fasttrun выросли на $growth байт, предел 65536" >&2
	exit 1
fi
if [ "$truncate_growth" -gt 65536 ]; then
	echo "FAIL: контекст операции очистки вырос на $truncate_growth байт" >&2
	exit 1
fi
if [ "$poison_final_used" -ne 0 ] || [ "$repaired_poison_used" -ne 0 ]; then
	echo "FAIL: после успешного восстановления остался контекст операции" >&2
	exit 1
fi
if [ "$active_poison_used" -le 0 ] || [ "$active_poison_used" -gt 65536 ]; then
	echo "FAIL: одна активная блокировка заняла $active_poison_used байт" >&2
	exit 1
fi
if [ "$stable_one_used" -gt $((warm_used + 65536)) ] || \
	[ "$stable_two_used" -gt $((warm_used + 65536)) ] || \
	[ "$stable_one_contexts" -ne "$warm_contexts" ] || \
	[ "$stable_two_contexts" -ne "$warm_contexts" ]; then
	echo "FAIL: общая память fasttrun не вернулась к прогретому уровню" >&2
	exit 1
fi
if grep -Eq 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG"; then
	grep -E 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG" >&2
	echo "FAIL: проверка памяти вызвала Assert или завершение серверного процесса" >&2
	exit 1
fi

echo "проверка памяти журнала транзакций прошла: обходы=$visits рост=$growth рост_очистки=$truncate_growth активных_уровней=0"
