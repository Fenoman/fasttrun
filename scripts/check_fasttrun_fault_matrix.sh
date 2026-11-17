#!/usr/bin/env bash
#
# Проверка перехватываемых ошибок на каждом этапе fasttruncate. Тестовый
# PostgreSQL изолирован: каталог данных, сокет и журнал находятся в WORKDIR.
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
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-fault-matrix.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_fault_matrix}

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

mkdir -p "$SOCKET_DIR" "$WORKDIR/tablespace"
if [ -n "$PG_RUN_AS" ]; then
	command -v runuser >/dev/null 2>&1 || { echo "missing runuser" >&2; exit 1; }
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR" "$WORKDIR/tablespace"
fi
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on -c max_prepared_transactions=10" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

if [ "$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')" != on ]; then
	echo "SKIP: truncate fault matrix requires a cassert PostgreSQL build" >&2
	exit 77
fi

cat >"$WORKDIR/boundary.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
SET fasttrun.zero_sinval_truncate = :mode;

CREATE TEMP TABLE ft_fault (
  id int PRIMARY KEY,
  grp int,
  brin_key int,
  payload text
) WITH (fillfactor = 10);
CREATE INDEX ft_fault_grp_idx ON ft_fault (grp);
CREATE INDEX ft_fault_brin_idx ON ft_fault USING brin (brin_key)
  WITH (pages_per_range = 1);
ALTER TABLE ft_fault ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO ft_fault
SELECT g, g % 31, g,
       string_agg(md5((g * 1000 + s)::text), '')
FROM generate_series(1, 200) g
CROSS JOIN LATERAL generate_series(1, 450) s
GROUP BY g;
SELECT 1 / (brin_summarize_new_values('ft_fault_brin_idx') > 0)::int;
CREATE TEMP TABLE ft_expected AS
SELECT count(*) AS rows,
       md5(string_agg(md5(payload), '' ORDER BY id)) AS checksum
FROM ft_fault;
CREATE TEMP TABLE ft_case_meta(failpoint text, phase text);
INSERT INTO ft_case_meta VALUES (:'failpoint', :'phase');

SELECT fasttrun_analyze('ft_fault');
SELECT fasttrun_collect_stats('ft_fault');
SET plan_cache_mode = force_generic_plan;
PREPARE ft_prepared(int) AS
SELECT count(*) FROM ft_fault WHERE grp = $1;
EXECUTE ft_prepared(11);

CREATE FUNCTION pg_temp.expect_poison(command text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE caught boolean := false;
BEGIN
  BEGIN
    EXECUTE command;
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'command was not blocked: %', command;
  END IF;
END
$fn$;

DO $case$
DECLARE
  point text;
  test_phase text;
  caught boolean := false;
  before_rows bigint;
  before_checksum text;
  after_rows bigint;
  after_checksum text;
  toast_oid oid;
  toast_chunks bigint;
  context_count bigint;
  plan_line text;
  brin_used boolean;
  summarized_ranges int;
BEGIN
  SELECT failpoint, phase INTO point, test_phase FROM ft_case_meta;
  SELECT rows, checksum INTO before_rows, before_checksum FROM ft_expected;

  PERFORM set_config('fasttrun.test_failpoint', point, false);
  BEGIN
    PERFORM fasttruncate('ft_fault');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: ' || point THEN
      RAISE;
    END IF;
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION '% did not fire', point;
  END IF;

  IF test_phase = 'early' THEN
    PERFORM set_config('enable_seqscan', 'off', true);
    SELECT count(*), md5(string_agg(md5(payload), '' ORDER BY id))
      INTO after_rows, after_checksum
      FROM ft_fault;
    IF after_rows <> before_rows OR after_checksum <> before_checksum THEN
      RAISE EXCEPTION '% changed heap/TOAST before mutation', point;
    END IF;
    IF (SELECT count(*) FROM ft_fault WHERE id = 73) <> 1 OR
       (SELECT count(*) FROM ft_fault WHERE grp = 11) = 0 OR
       (SELECT count(*) FROM ft_fault WHERE brin_key BETWEEN 5 AND 10) <> 6 THEN
      RAISE EXCEPTION '% damaged a forced index path', point;
    END IF;
    brin_used := false;
    FOR plan_line IN EXPLAIN (COSTS OFF)
      SELECT * FROM ft_fault WHERE brin_key BETWEEN 5 AND 10
    LOOP
      brin_used := brin_used OR
        position('ft_fault_brin_idx' IN plan_line) > 0;
    END LOOP;
    IF NOT brin_used THEN
      RAISE EXCEPTION '% did not use BRIN before mutation', point;
    END IF;
    IF (SELECT count(*) FROM fasttrun_inspect_stats('ft_fault')) = 0 OR
       (SELECT reltuples FROM fasttrun_relstats('ft_fault')) <= 0 THEN
      RAISE EXCEPTION '% lost pre-call local statistics', point;
    END IF;
  ELSE
    PERFORM pg_temp.expect_poison('SELECT count(*) FROM ft_fault');
    PERFORM pg_temp.expect_poison('EXECUTE ft_prepared(11)');
    PERFORM pg_temp.expect_poison(
      'INSERT INTO ft_fault VALUES (999, 1, 999, ''x'')');
    PERFORM pg_temp.expect_poison('UPDATE ft_fault SET grp = 1 WHERE id = 1');
    PERFORM pg_temp.expect_poison('DELETE FROM ft_fault WHERE id = 1');
    PERFORM pg_temp.expect_poison('COPY ft_fault TO ''/dev/null''');
    PERFORM pg_temp.expect_poison('COPY ft_fault FROM ''/dev/null''');
    PERFORM pg_temp.expect_poison('SELECT fasttrun_analyze(''ft_fault'')');
    PERFORM pg_temp.expect_poison(
      'SELECT fasttrun_analyze_bulk(VARIADIC ARRAY[''ft_fault''])');
    PERFORM pg_temp.expect_poison('SELECT fasttrun_collect_stats(''ft_fault'')');
    PERFORM pg_temp.expect_poison('SELECT * FROM fasttrun_relstats(''ft_fault'')');
    PERFORM pg_temp.expect_poison(
      'SELECT count(*) FROM fasttrun_inspect_stats(''ft_fault'')');
    PERFORM pg_temp.expect_poison('TRUNCATE ft_fault');
    PERFORM pg_temp.expect_poison('VACUUM ft_fault');
    PERFORM pg_temp.expect_poison('ANALYZE ft_fault');
    PERFORM pg_temp.expect_poison('CLUSTER ft_fault');
    PERFORM pg_temp.expect_poison(
      'ALTER TABLE ft_fault SET (autovacuum_enabled = false)');
    PERFORM pg_temp.expect_poison(
      'CREATE INDEX ft_fault_payload_fail_idx ON ft_fault ((md5(payload)))');

    /* PostgreSQL turns an error inside RelationTruncate into PANIC.
     * Test errors are injected only after RelationTruncate returns. */
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    PERFORM fasttruncate('ft_fault');
    IF (SELECT count(*) FROM ft_fault) <> 0 THEN
      RAISE EXCEPTION '% repair did not leave an empty heap', point;
    END IF;
    SELECT reltoastrelid INTO toast_oid
      FROM pg_class WHERE oid = 'ft_fault'::regclass;
    EXECUTE format('SELECT count(*) FROM %s', toast_oid::regclass)
      INTO toast_chunks;
    IF toast_chunks <> 0 THEN
      RAISE EXCEPTION '% repair left % TOAST chunks', point, toast_chunks;
    END IF;

    INSERT INTO ft_fault
    SELECT 1000 + g, g % 31, 1000 + g,
           string_agg(md5((900000 + g * 1000 + s)::text), '')
    FROM generate_series(1, 20) g
    CROSS JOIN LATERAL generate_series(1, 120) s
    GROUP BY g;
    SELECT brin_summarize_new_values('ft_fault_brin_idx')
      INTO summarized_ranges;
    IF summarized_ranges <= 0 THEN
      RAISE EXCEPTION '% repair summarized no BRIN ranges', point;
    END IF;
    PERFORM set_config('enable_seqscan', 'off', true);
    IF (SELECT count(*) FROM ft_fault) <> 20 OR
       (SELECT min(id) FROM ft_fault) <> 1001 OR
       (SELECT count(*) FROM ft_fault WHERE id = 1007) <> 1 OR
       (SELECT count(*) FROM ft_fault WHERE grp = 7) = 0 OR
       (SELECT count(*) FROM ft_fault
         WHERE brin_key BETWEEN 1005 AND 1010) <> 6 THEN
      RAISE EXCEPTION '% repair/refill returned stale or missing rows', point;
    END IF;
    brin_used := false;
    FOR plan_line IN EXPLAIN (COSTS OFF)
      SELECT * FROM ft_fault WHERE brin_key BETWEEN 1005 AND 1010
    LOOP
      brin_used := brin_used OR
        position('ft_fault_brin_idx' IN plan_line) > 0;
    END LOOP;
    IF NOT brin_used THEN
      RAISE EXCEPTION '% did not use BRIN after repair', point;
    END IF;
  END IF;

  PERFORM set_config('fasttrun.test_failpoint', '', false);
  SELECT count(*) INTO context_count
    FROM pg_backend_memory_contexts
   WHERE name = 'fasttrun operation context';
  IF context_count <> 0 THEN
    RAISE EXCEPTION '% left % operation contexts', point, context_count;
  END IF;
END
$case$;

DEALLOCATE ft_prepared;
DROP TABLE ft_fault, ft_expected, ft_case_meta;
SELECT 'BOUNDARY_MEMORY_1 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'BOUNDARY_MEMORY_2 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'BOUNDARY_OK ' || :'mode' || ' ' || :'failpoint';
SQL

boundary_count=0

run_boundary_case()
{
	local mode=$1
	local failpoint=$2
	local phase=$3
	local label=${failpoint//:/_}
	local out="$WORKDIR/${mode}_${label}.out"
	local err="$WORKDIR/${mode}_${label}.err"

	if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v mode="$mode" -v failpoint="$failpoint" -v phase="$phase" \
		-f "$WORKDIR/boundary.sql" >"$out" 2>"$err"; then
		cat "$out" "$err" >&2
		echo "FAIL: mode=$mode failpoint=$failpoint" >&2
		exit 1
	fi
	if ! grep -q "^BOUNDARY_OK $mode $failpoint$" "$out"; then
		cat "$out" "$err" >&2
		echo "FAIL: missing boundary marker for $mode/$failpoint" >&2
		exit 1
	fi
	if ! grep -q '^BOUNDARY_MEMORY_1 0 0 0$' "$out" || \
		! grep -q '^BOUNDARY_MEMORY_2 0 0 0$' "$out"; then
		cat "$out" "$err" >&2
		echo "FAIL: aggregate boundary memory did not return to zero" >&2
		exit 1
	fi
	echo "boundary passed: mode=$mode failpoint=$failpoint"
	boundary_count=$((boundary_count + 1))
}

early_failpoints=(
	after_prepare
	after_phase0
	reserve_after_state:1
	reserve_after_state:2
	reserve_after_state:3
	reserve_after_state:4
	reserve_after_state:5
	reserve_after_state:6
)
post_failpoints=(
	after_user_index:1
	after_user_index:2
	after_user_index:3
	after_toast_index:1
	after_toast_heap
	after_main_heap
	after_ambuild:1
	after_ambuild:2
	after_ambuild:3
	after_ambuild:4
	before_publish
	after_publish:1
	after_publish:2
	after_publish:3
	after_publish:4
	after_publish:5
	after_publish:6
)
modes=(
	on
	off
)
EXPECTED_BOUNDARY_CASES=50
failpoint_count=$((${#early_failpoints[@]} + ${#post_failpoints[@]}))
expected_boundary_count=$((
	${#modes[@]} * (${#early_failpoints[@]} + ${#post_failpoints[@]})
))
if [ "$expected_boundary_count" -ne "$EXPECTED_BOUNDARY_CASES" ]; then
	echo "FAIL: fault matrix defines $expected_boundary_count cases, expected $EXPECTED_BOUNDARY_CASES" >&2
	exit 1
fi

for mode in "${modes[@]}"; do
	for failpoint in "${early_failpoints[@]}"; do
		run_boundary_case "$mode" "$failpoint" early
	done
	for failpoint in "${post_failpoints[@]}"; do
		run_boundary_case "$mode" "$failpoint" post
	done
done

if [ "$boundary_count" -ne "$EXPECTED_BOUNDARY_CASES" ]; then
	echo "FAIL: expected $EXPECTED_BOUNDARY_CASES boundary cases, got $boundary_count" >&2
	exit 1
fi

cat >"$WORKDIR/lifecycle.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
SET fasttrun.zero_sinval_truncate = :mode;

CREATE FUNCTION pg_temp.make_fixture(relname text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
  EXECUTE format(
    'CREATE TEMP TABLE %I (id int PRIMARY KEY, grp int, payload text)',
    relname);
  EXECUTE format('CREATE INDEX %I ON %I (grp)',
                 relname || '_grp_idx', relname);
  EXECUTE format('ALTER TABLE %I ALTER COLUMN payload SET STORAGE EXTERNAL',
                 relname);
  EXECUTE format(
    'INSERT INTO %I '
    'SELECT g, g %% 31, string_agg(md5((g * 1000 + s)::text), '''') '
    'FROM generate_series(1, 80) g '
    'CROSS JOIN LATERAL generate_series(1, 450) s GROUP BY g', relname);
END
$fn$;

CREATE FUNCTION pg_temp.inject(relname text, point text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE caught boolean := false;
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', point, false);
  BEGIN
    PERFORM fasttruncate(relname);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: ' || point THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION '% did not fire for %', point, relname;
  END IF;
END
$fn$;

CREATE FUNCTION pg_temp.expect_poison(command text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE caught boolean := false;
BEGIN
  BEGIN
    EXECUTE command;
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'command was not blocked: %', command;
  END IF;
END
$fn$;

CREATE FUNCTION pg_temp.assert_operation_context_zero(label text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE n bigint;
BEGIN
  SELECT count(*) INTO n
    FROM pg_backend_memory_contexts
   WHERE name = 'fasttrun operation context';
  IF n <> 0 THEN
    RAISE EXCEPTION '% left % operation contexts', label, n;
  END IF;
END
$fn$;

-- Повторная очистка согласованной пустой таблицы не входит в полный путь.
CREATE FUNCTION pg_temp.truncate_probe(boolean)
RETURNS bigint[] AS '$libdir/fasttrun', 'fasttrun_test_planner_probe'
LANGUAGE C STRICT;
CREATE TEMP TABLE ft_empty(id int PRIMARY KEY, payload text);
BEGIN;
INSERT INTO ft_empty SELECT g, repeat('x', 100) FROM generate_series(1, 100) g;
SELECT fasttrun_analyze('ft_empty');
SELECT fasttruncate('ft_empty');
SELECT pg_temp.truncate_probe(true);
SET fasttrun.test_failpoint = 'after_prepare';
SELECT fasttruncate('ft_empty');
SAVEPOINT empty_child;
SELECT fasttruncate('ft_empty');
ROLLBACK TO SAVEPOINT empty_child;
SELECT fasttruncate('ft_empty');
COMMIT;
SELECT fasttruncate('ft_empty');
BEGIN;
SELECT fasttruncate('ft_empty');
ROLLBACK;
RESET fasttrun.test_failpoint;
SELECT 1 / ((pg_temp.truncate_probe(false))[4] = 0)::int;
SELECT 1 / (count(*) = 0)::int FROM ft_empty;
SELECT 1 / (reltuples = 0)::int FROM fasttrun_relstats('ft_empty');
SELECT pg_temp.assert_operation_context_zero('empty repeat');

-- Кеш с нулём строк не разрешает пропустить очистку после вставки.
INSERT INTO ft_empty VALUES (1001, 'refill');
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT 1 / (count(*) = 1)::int FROM ft_empty;
SELECT fasttruncate('ft_empty');
INSERT INTO ft_empty VALUES (2001, 'next refill');
SELECT fasttrun_analyze('ft_empty');
SELECT 1 / (reltuples = 1)::int FROM fasttrun_relstats('ft_empty');
SET enable_seqscan = off;
SELECT 1 / (count(*) = 0)::int FROM ft_empty WHERE id = 1001;
SELECT 1 / (count(*) = 1)::int FROM ft_empty WHERE id = 2001;
RESET enable_seqscan;

-- DELETE оставляет физические страницы даже при нуле видимых строк.
DELETE FROM ft_empty;
SELECT fasttrun_analyze('ft_empty');
SELECT 1 / (pg_relation_size('ft_empty') > 0)::int;
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT fasttruncate('ft_empty');
SELECT 1 / (pg_relation_size('ft_empty') = 0)::int;

-- Откат вставки тоже оставляет страницы, хотя прежняя статистика пуста.
BEGIN;
SAVEPOINT empty_child;
INSERT INTO ft_empty VALUES (3001, 'aborted refill');
ROLLBACK TO SAVEPOINT empty_child;
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT fasttruncate('ft_empty');
COMMIT;
SELECT 1 / (pg_relation_size('ft_empty') = 0)::int;

-- Новый индекс и передача статистики ядру требуют согласования состояния.
CREATE INDEX ft_empty_payload_idx ON ft_empty(payload);
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT fasttruncate('ft_empty');
ANALYZE ft_empty(payload);
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT fasttruncate('ft_empty');

-- Без счётчиков остаётся обязательная проверка физического размера.
SET track_counts = off;
SELECT fasttruncate('ft_empty');
SET fasttrun.test_failpoint = 'after_prepare';
SELECT fasttruncate('ft_empty');
RESET fasttrun.test_failpoint;
INSERT INTO ft_empty VALUES (4001, 'untracked refill');
SELECT pg_temp.inject('ft_empty', 'after_prepare');
SELECT fasttruncate('ft_empty');
RESET track_counts;
SELECT 1 / (count(*) = 0)::int FROM ft_empty;
DROP TABLE ft_empty;

-- После COMMIT быстрый путь должен заново задать базу счётчиков для refill.
CREATE TEMP TABLE ft_empty_delta(id int);
SELECT fasttruncate('ft_empty_delta');
BEGIN;
SET LOCAL fasttrun.auto_collect_stats = off;
SET LOCAL fasttrun.sample_rows = 0;
SELECT fasttruncate('ft_empty_delta');
INSERT INTO ft_empty_delta SELECT generate_series(1, 1000);
DO $delta$
DECLARE before_scan bigint;
BEGIN
  before_scan := pg_stat_get_xact_tuples_returned('ft_empty_delta'::regclass);
  PERFORM fasttrun_analyze('ft_empty_delta');
  IF pg_stat_get_xact_tuples_returned('ft_empty_delta'::regclass) <> before_scan THEN
    RAISE EXCEPTION 'empty truncate lost delta seed: unexpected heap scan';
  END IF;
END
$delta$;
SELECT 1 / (reltuples = 1000)::int FROM fasttrun_relstats('ft_empty_delta');
COMMIT;
DROP TABLE ft_empty_delta;

-- Ленивое обнаружение пустого heap не подтверждает свежесть сохранённых планов.
CREATE TEMP TABLE ft_empty_core(id int) ON COMMIT DELETE ROWS;
BEGIN;
SET LOCAL fasttrun.auto_collect_stats = off;
SET LOCAL fasttrun.sample_rows = 0;
INSERT INTO ft_empty_core SELECT generate_series(1, 100);
SELECT fasttrun_analyze('ft_empty_core');
PREPARE ft_empty_plan AS SELECT * FROM ft_empty_core;
EXPLAIN (COSTS OFF) EXECUTE ft_empty_plan;
COMMIT;
SELECT 1 / (reltuples = 0)::int FROM fasttrun_relstats('ft_empty_core');
SELECT pg_temp.truncate_probe(true);
SELECT fasttruncate('ft_empty_core');
SELECT 1 / ((pg_temp.truncate_probe(false))[4] = 1)::int;
DEALLOCATE ft_empty_plan;
DROP TABLE ft_empty_core;

/* A block created after file changes must survive a transaction rollback. */
SELECT pg_temp.make_fixture('ft_abort');
BEGIN;
SELECT pg_temp.inject('ft_abort', 'after_main_heap');
ROLLBACK;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_abort');
SELECT fasttruncate('ft_abort');
SELECT 1 / (count(*) = 0)::int FROM ft_abort;
SELECT pg_temp.assert_operation_context_zero('top-abort repair');
DROP TABLE ft_abort;

/* Aborted table births must release incomplete operations, including when
 * the first operation ran in an independently aborted descendant. */
DO $births$
BEGIN
  FOR i IN 1..100 LOOP
    BEGIN
      CREATE TEMP TABLE ft_birth_error(id int PRIMARY KEY);
      INSERT INTO ft_birth_error VALUES (1);
      PERFORM set_config('fasttrun.test_failpoint', 'after_main_heap', true);
      PERFORM fasttruncate('ft_birth_error');
      RAISE EXCEPTION 'after_main_heap did not fire';
    EXCEPTION WHEN SQLSTATE '55000' THEN
      IF SQLERRM <> 'fasttrun test failpoint: after_main_heap' THEN
        RAISE;
      END IF;
    END;
    IF to_regclass('pg_temp.ft_birth_error') IS NOT NULL THEN
      RAISE EXCEPTION 'aborted table birth survived';
    END IF;
  END LOOP;
END
$births$;
SELECT pg_temp.assert_operation_context_zero('aborted table births');

BEGIN;
SAVEPOINT birth_parent;
CREATE TEMP TABLE ft_birth_ancestor(id int PRIMARY KEY);
INSERT INTO ft_birth_ancestor VALUES (1);
SAVEPOINT birth_middle;
SELECT pg_temp.inject('ft_birth_ancestor', 'after_main_heap');
ROLLBACK TO SAVEPOINT birth_middle;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_birth_ancestor');
ROLLBACK TO SAVEPOINT birth_parent;
SELECT pg_temp.assert_operation_context_zero('aborted ancestor birth');
COMMIT;

BEGIN;
SAVEPOINT birth_parent;
SAVEPOINT birth_child;
CREATE TEMP TABLE ft_birth_released(id int PRIMARY KEY);
SELECT pg_temp.inject('ft_birth_released', 'after_main_heap');
RELEASE SAVEPOINT birth_child;
ROLLBACK TO SAVEPOINT birth_parent;
SELECT pg_temp.assert_operation_context_zero('released child birth');
COMMIT;

BEGIN;
CREATE TEMP TABLE ft_birth_top(id int PRIMARY KEY);
SELECT pg_temp.inject('ft_birth_top', 'after_main_heap');
ROLLBACK;
SELECT pg_temp.assert_operation_context_zero('aborted top birth');

/* A committed table birth must not be mistaken for a later aborted one. */
BEGIN;
CREATE TEMP TABLE ft_birth_committed(id int PRIMARY KEY);
SELECT pg_temp.inject('ft_birth_committed', 'after_main_heap');
COMMIT;
BEGIN;
SELECT 1;
ROLLBACK;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_birth_committed');
SELECT fasttruncate('ft_birth_committed');
DROP TABLE ft_birth_committed;
SELECT pg_temp.assert_operation_context_zero('committed birth repair');

/* A born index belongs to an older heap; abort must keep the heap blocked. */
CREATE TEMP TABLE ft_birth_index_owner(id int PRIMARY KEY, grp int);
INSERT INTO ft_birth_index_owner VALUES (1, 1);
DO $born_index$
BEGIN
  BEGIN
    CREATE INDEX ft_birth_new_idx ON ft_birth_index_owner(grp);
    PERFORM set_config('fasttrun.test_failpoint', 'after_main_heap', true);
    PERFORM fasttruncate('ft_birth_index_owner');
    RAISE EXCEPTION 'after_main_heap did not fire';
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: after_main_heap' THEN
      RAISE;
    END IF;
  END;
END
$born_index$;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_birth_index_owner');
SELECT fasttruncate('ft_birth_index_owner');
SELECT pg_temp.assert_operation_context_zero('born index owner repair');
DROP TABLE ft_birth_index_owner;

/* Removing a dead birth must not clear another relation's active block. */
CREATE TEMP TABLE ft_birth_old(id int PRIMARY KEY);
SELECT pg_temp.inject('ft_birth_old', 'after_main_heap');
BEGIN;
CREATE TEMP TABLE ft_birth_new(id int PRIMARY KEY);
SELECT pg_temp.inject('ft_birth_new', 'after_main_heap');
ROLLBACK;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_birth_old');
SELECT fasttruncate('ft_birth_old');
SELECT pg_temp.assert_operation_context_zero('mixed registry repair');
DROP TABLE ft_birth_old;

/* An unrelated prepared transaction must not remove a physical block. */
SELECT pg_temp.make_fixture('ft_prepare_poison');
SELECT pg_temp.inject('ft_prepare_poison', 'after_main_heap');
BEGIN;
PREPARE TRANSACTION 'fasttrun_poison_lifecycle';
ROLLBACK PREPARED 'fasttrun_poison_lifecycle';
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_prepare_poison');
SELECT fasttruncate('ft_prepare_poison');
SELECT pg_temp.assert_operation_context_zero('prepared transaction repair');
DROP TABLE ft_prepare_poison;

/* A failed retry before file changes must preserve the existing block. */
SELECT pg_temp.make_fixture('ft_repair');
SELECT pg_temp.inject('ft_repair', 'after_main_heap');
SELECT pg_temp.inject('ft_repair', 'after_prepare');
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_repair');
SELECT pg_temp.inject('ft_repair', 'after_phase0');
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_repair');
SELECT fasttruncate('ft_repair');
SELECT pg_temp.assert_operation_context_zero('prepared repair failures');
DROP TABLE ft_repair;

/* Rolling back DROP preserves the block; committing DROP removes it. */
SELECT pg_temp.make_fixture('ft_drop');
SELECT pg_temp.inject('ft_drop', 'after_user_index:1');
BEGIN;
DROP TABLE ft_drop;
ROLLBACK;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_drop');
BEGIN;
DROP TABLE ft_drop;
COMMIT;
SELECT pg_temp.assert_operation_context_zero('committed drop');
CREATE TEMP TABLE ft_drop(id int);
SELECT count(*) FROM ft_drop;
DROP TABLE ft_drop;

/* A test-only locator change checks that an obsolete block is removed. */
SELECT pg_temp.make_fixture('ft_locator');
SELECT pg_temp.inject('ft_locator', 'after_user_index:1');
CREATE FUNCTION pg_temp.corrupt_poison(oid)
RETURNS void AS '$libdir/fasttrun', 'fasttrun_test_poison_locator_mismatch'
LANGUAGE C STRICT;
SELECT pg_temp.corrupt_poison('ft_locator'::regclass);
SELECT * FROM fasttrun_relstats('ft_locator');
SELECT pg_temp.assert_operation_context_zero('locator mismatch');
DROP TABLE ft_locator;

/* A successful cleanup remains complete after the transaction rolls back. */
SELECT pg_temp.make_fixture('ft_success_abort');
BEGIN;
SELECT fasttruncate('ft_success_abort');
ROLLBACK;
SELECT 1 / (count(*) = 0)::int FROM ft_success_abort;
INSERT INTO ft_success_abort
SELECT 1000 + g, g % 31,
       string_agg(md5((700000 + g * 1000 + s)::text), '')
FROM generate_series(1, 20) g
CROSS JOIN LATERAL generate_series(1, 120) s
GROUP BY g;
SET enable_seqscan = off;
SELECT 1 / (count(*) = 20)::int FROM ft_success_abort;
SELECT 1 / (count(*) = 1)::int FROM ft_success_abort WHERE id = 1007;
RESET enable_seqscan;
SELECT pg_temp.assert_operation_context_zero('successful rollback');
DROP TABLE ft_success_abort;

/*
 * fasttrun_temp_is_ours: a table damaged by an incomplete fasttruncate is not
 * fresh, even one created in the transaction, so the caller goes on to the
 * repairing fasttruncate.  A failure before file changes leaves the old rows
 * and no mark.
 */
BEGIN;
SELECT pg_temp.make_fixture('ft_fresh_born');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_born') IS TRUE)::int;
SELECT pg_temp.inject('ft_fresh_born', 'after_main_heap');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_born') IS FALSE)::int;
SELECT fasttruncate('ft_fresh_born');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_born') IS TRUE)::int;
COMMIT;
DROP TABLE ft_fresh_born;
SELECT pg_temp.make_fixture('ft_fresh_old');
BEGIN;
SELECT pg_temp.inject('ft_fresh_old', 'after_prepare');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_old') IS FALSE)::int;
SELECT 1 / (count(*) = 80)::int FROM ft_fresh_old;
SELECT pg_temp.inject('ft_fresh_old', 'after_user_index:1');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_old') IS FALSE)::int;
SELECT pg_temp.inject('ft_fresh_old', 'before_publish');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_old') IS FALSE)::int;
SELECT pg_temp.expect_poison('SELECT count(*) FROM ft_fresh_old');
SELECT fasttruncate('ft_fresh_old');
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_old') IS TRUE)::int;
COMMIT;
SELECT 1 / (fasttrun_temp_is_ours('ft_fresh_old') IS FALSE)::int;
SELECT pg_temp.assert_operation_context_zero('fresh after repair');
DROP TABLE ft_fresh_old;

/* Unsupported traversal is rejected before any mutation. */
CREATE TEMP TABLE ft_parent(id int);
CREATE TEMP TABLE ft_child() INHERITS (ft_parent);
INSERT INTO ft_parent VALUES (1);
INSERT INTO ft_child VALUES (2);
DO $case$
DECLARE caught boolean := false;
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_parent');
  EXCEPTION WHEN feature_not_supported THEN
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'inheritance parent was accepted';
  END IF;
END
$case$;
SELECT 1 / (count(*) = 2)::int FROM ft_parent;
DROP TABLE ft_parent CASCADE;

SELECT 'LIFECYCLE_MEMORY_1 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'LIFECYCLE_MEMORY_2 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'LIFECYCLE_OK ' || :'mode';
SQL

for mode in on off; do
	if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v mode="$mode" -f "$WORKDIR/lifecycle.sql" \
		>"$WORKDIR/lifecycle_${mode}.out" \
		2>"$WORKDIR/lifecycle_${mode}.err"; then
		cat "$WORKDIR/lifecycle_${mode}.out" \
			"$WORKDIR/lifecycle_${mode}.err" >&2
		echo "FAIL: lifecycle mode=$mode" >&2
		exit 1
	fi
	grep -q "^LIFECYCLE_OK $mode$" "$WORKDIR/lifecycle_${mode}.out" || {
		cat "$WORKDIR/lifecycle_${mode}.out" >&2
		echo "FAIL: missing lifecycle marker for $mode" >&2
		exit 1
	}
	if ! grep -q '^LIFECYCLE_MEMORY_1 0 0 0$' \
		"$WORKDIR/lifecycle_${mode}.out" || \
		! grep -q '^LIFECYCLE_MEMORY_2 0 0 0$' \
		"$WORKDIR/lifecycle_${mode}.out"; then
		cat "$WORKDIR/lifecycle_${mode}.out" >&2
		echo "FAIL: lifecycle aggregate memory did not stabilize" >&2
		exit 1
	fi
	echo "lifecycle passed: mode=$mode"
done

cat >"$WORKDIR/discard.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_discard(id int PRIMARY KEY, payload text);
CREATE INDEX ft_discard_payload_idx ON ft_discard ((md5(payload)));
INSERT INTO ft_discard
SELECT g, string_agg(md5((g * 1000 + s)::text), '')
FROM generate_series(1, 40) g
CROSS JOIN LATERAL generate_series(1, 450) s
GROUP BY g;
SET fasttrun.test_failpoint = 'after_user_index:1';
DO $case$
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_discard');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
END
$case$;
SET fasttrun.test_failpoint = '';
:discard_command
SELECT 'DISCARD_MEMORY_1 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'DISCARD_MEMORY_2 ' || coalesce(sum(total_bytes), 0) || ' ' ||
       coalesce(sum(used_bytes), 0) || ' ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SQL

for target in TEMP ALL; do
	if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v discard_command="DISCARD $target;" -f "$WORKDIR/discard.sql" \
		>"$WORKDIR/discard_${target}.out" \
		2>"$WORKDIR/discard_${target}.err"; then
		cat "$WORKDIR/discard_${target}.out" \
			"$WORKDIR/discard_${target}.err" >&2
		echo "FAIL: DISCARD $target" >&2
		exit 1
	fi
	if ! grep -q '^DISCARD_MEMORY_1 0 0 0$' \
		"$WORKDIR/discard_${target}.out" || \
		! grep -q '^DISCARD_MEMORY_2 0 0 0$' \
		"$WORKDIR/discard_${target}.out"; then
		cat "$WORKDIR/discard_${target}.out" >&2
		echo "FAIL: DISCARD $target left fasttrun contexts" >&2
		exit 1
	fi
	echo "discard passed: target=$target"
done

# DISCARD TEMP откатывается вместе с транзакцией, и заблокированная таблица
# возвращается.  Блокировка обязана вернуться с ней: запрос дает SQLSTATE
# 55000, а повтор fasttruncate чинит таблицу.
cat >"$WORKDIR/discard_rollback.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_discard_rb(id int PRIMARY KEY, payload text);
INSERT INTO ft_discard_rb SELECT g, md5(g::text) FROM generate_series(1, 1000) g;
CREATE FUNCTION pg_temp.ft_discard_rb_probe() RETURNS text
LANGUAGE plpgsql AS $probe$
BEGIN
  PERFORM count(*) FROM ft_discard_rb WHERE id = 5;
  RETURN 'readable';
EXCEPTION WHEN OTHERS THEN
  RETURN SQLSTATE;
END
$probe$;
SET fasttrun.test_failpoint = 'after_user_index:1';
DO $case$
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_discard_rb');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
END
$case$;
SET fasttrun.test_failpoint = '';
BEGIN;
DISCARD TEMP;
ROLLBACK;
SELECT 'DISCARD_ROLLBACK_BLOCKED ' || pg_temp.ft_discard_rb_probe();
SELECT fasttruncate('ft_discard_rb');
SELECT 'DISCARD_ROLLBACK_REPAIRED ' || pg_temp.ft_discard_rb_probe();
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/discard_rollback.sql" >"$WORKDIR/discard_rollback.out" \
	2>"$WORKDIR/discard_rollback.err"; then
	cat "$WORKDIR/discard_rollback.out" "$WORKDIR/discard_rollback.err" >&2
	echo "FAIL: DISCARD TEMP rollback" >&2
	exit 1
fi
if ! grep -q '^DISCARD_ROLLBACK_BLOCKED 55000$' "$WORKDIR/discard_rollback.out" || \
	! grep -q '^DISCARD_ROLLBACK_REPAIRED readable$' "$WORKDIR/discard_rollback.out"; then
	cat "$WORKDIR/discard_rollback.out" >&2
	echo "FAIL: DISCARD TEMP rollback did not keep the block" >&2
	exit 1
fi
echo "discard rollback passed"

# ANALYZE без списка таблиц проверяет каждую заблокированную таблицу. Одна из
# двух удалена в той же транзакции, и ее запись живет до COMMIT, другая обязана
# остановить ANALYZE. Порядок записей в хеше заранее не известен, поэтому
# удаляется то одна, то другая.
cat >"$WORKDIR/poison_any.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_poison_a(id int PRIMARY KEY);
CREATE TEMP TABLE ft_poison_b(id int PRIMARY KEY);
INSERT INTO ft_poison_a SELECT generate_series(1, 1000);
INSERT INTO ft_poison_b SELECT generate_series(1, 1000);
SET fasttrun.test_failpoint = 'after_user_index:1';
DO $case$
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_poison_a');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
  BEGIN
    PERFORM fasttruncate('ft_poison_b');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
END
$case$;
SET fasttrun.test_failpoint = '';
DO $case$
BEGIN
  DROP TABLE ft_poison_a;
  ANALYZE;
  RAISE WARNING 'POISON_ANY not blocked';
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'POISON_ANY %', SQLSTATE;
END
$case$;
DO $case$
BEGIN
  DROP TABLE ft_poison_b;
  ANALYZE;
  RAISE WARNING 'POISON_ANY not blocked';
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'POISON_ANY %', SQLSTATE;
END
$case$;
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/poison_any.sql" >"$WORKDIR/poison_any.out" \
	2>"$WORKDIR/poison_any.err"; then
	cat "$WORKDIR/poison_any.out" "$WORKDIR/poison_any.err" >&2
	echo "FAIL: poison check of every blocked table" >&2
	exit 1
fi
if [ "$(grep -c 'POISON_ANY 55000' "$WORKDIR/poison_any.err")" -ne 2 ]; then
	cat "$WORKDIR/poison_any.out" "$WORKDIR/poison_any.err" >&2
	echo "FAIL: ANALYZE passed a blocked table" >&2
	exit 1
fi
echo "poison any passed"

# Блокировка действует и на отношения, которых нет в тексте запроса: тело
# встроенной SQL-функции и потомка, до которого план доходит через родителя
# наследования. Таблицы без индексов, а отказ идет после очистки кучи, поэтому
# без блокировки оба запроса молча вернули бы ноль строк.
cat >"$WORKDIR/poison_plan.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_poison_inline(id int, payload text);
INSERT INTO ft_poison_inline SELECT g, md5(g::text) FROM generate_series(1, 1000) g;
CREATE FUNCTION pg_temp.ft_poison_inline_rows() RETURNS TABLE(id int)
LANGUAGE sql STABLE AS 'SELECT id FROM ft_poison_inline';
CREATE TEMP TABLE ft_poison_parent(id int);
CREATE TEMP TABLE ft_poison_child() INHERITS (ft_poison_parent);
INSERT INTO ft_poison_child SELECT generate_series(1, 1000);
CREATE FUNCTION pg_temp.ft_poison_plan_probe(q text) RETURNS text
LANGUAGE plpgsql AS $probe$
DECLARE n bigint;
BEGIN
  EXECUTE q INTO n;
  RETURN 'readable ' || n;
EXCEPTION WHEN OTHERS THEN
  RETURN SQLSTATE;
END
$probe$;
SET fasttrun.test_failpoint = 'after_main_heap';
DO $case$
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_poison_inline');
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
  BEGIN
    PERFORM fasttruncate('ft_poison_child');
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
END
$case$;
SET fasttrun.test_failpoint = '';
SELECT 'POISON_PLAN_DIRECT ' || pg_temp.ft_poison_plan_probe('SELECT count(*) FROM ft_poison_inline');
SELECT 'POISON_PLAN_INLINE ' || pg_temp.ft_poison_plan_probe('SELECT count(*) FROM pg_temp.ft_poison_inline_rows()');
SELECT 'POISON_PLAN_CHILD ' || pg_temp.ft_poison_plan_probe('SELECT count(*) FROM ft_poison_parent');
SELECT 'POISON_PLAN_BLOCKED ' || string_agg(relname, ',' ORDER BY relname)
  FROM fasttrun_blocked_tables();
SELECT fasttruncate('ft_poison_inline');
SELECT fasttruncate('ft_poison_child');
SELECT 'POISON_PLAN_BLOCKED_AFTER ' || count(*) FROM fasttrun_blocked_tables();
SELECT 'POISON_PLAN_REPAIRED ' || pg_temp.ft_poison_plan_probe('SELECT count(*) FROM pg_temp.ft_poison_inline_rows()')
  || ' ' || pg_temp.ft_poison_plan_probe('SELECT count(*) FROM ft_poison_parent');
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/poison_plan.sql" >"$WORKDIR/poison_plan.out" \
	2>"$WORKDIR/poison_plan.err"; then
	cat "$WORKDIR/poison_plan.out" "$WORKDIR/poison_plan.err" >&2
	echo "FAIL: poison check of the finished plan" >&2
	exit 1
fi
for probe in DIRECT INLINE CHILD; do
	if ! grep -q "^POISON_PLAN_$probe 55000\$" "$WORKDIR/poison_plan.out"; then
		cat "$WORKDIR/poison_plan.out" >&2
		echo "FAIL: a query reached a blocked table ($probe)" >&2
		exit 1
	fi
done
if ! grep -qx 'POISON_PLAN_BLOCKED ft_poison_child,ft_poison_inline' "$WORKDIR/poison_plan.out" ||
	! grep -qx 'POISON_PLAN_BLOCKED_AFTER 0' "$WORKDIR/poison_plan.out"; then
	cat "$WORKDIR/poison_plan.out" >&2
	echo "FAIL: fasttrun_blocked_tables() does not list the blocked tables" >&2
	exit 1
fi
if ! grep -q '^POISON_PLAN_REPAIRED readable 0 readable 0$' "$WORKDIR/poison_plan.out"; then
	cat "$WORKDIR/poison_plan.out" >&2
	echo "FAIL: fasttruncate did not repair the blocked tables" >&2
	exit 1
fi
echo "poison plan passed"

# Запись через секционированного родителя доходит до секции, которую выбирает
# маршрутизация строк, а не план. Блокировка секции действует и для INSERT, и
# для COPY через родителя.
cat >"$WORKDIR/poison_route.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_poison_route(id int) PARTITION BY RANGE (id);
CREATE TEMP TABLE ft_poison_route_leaf PARTITION OF ft_poison_route FOR VALUES FROM (0) TO (100000);
INSERT INTO ft_poison_route SELECT generate_series(1, 1000);
CREATE FUNCTION pg_temp.ft_poison_route_probe(q text) RETURNS text
LANGUAGE plpgsql AS $probe$
BEGIN
  EXECUTE q;
  RETURN 'written';
EXCEPTION WHEN OTHERS THEN
  RETURN SQLSTATE;
END
$probe$;
SET fasttrun.test_failpoint = 'after_main_heap';
DO $case$
BEGIN
  PERFORM fasttruncate('ft_poison_route_leaf');
EXCEPTION WHEN OTHERS THEN
  NULL;
END
$case$;
SET fasttrun.test_failpoint = '';
SELECT 'POISON_ROUTE_INSERT ' || pg_temp.ft_poison_route_probe('INSERT INTO ft_poison_route VALUES (5)');
SELECT 'POISON_ROUTE_COPY ' || pg_temp.ft_poison_route_probe($q$COPY ft_poison_route FROM PROGRAM 'echo 6'$q$);
SELECT fasttruncate('ft_poison_route_leaf');
SELECT 'POISON_ROUTE_REPAIRED ' || pg_temp.ft_poison_route_probe('INSERT INTO ft_poison_route VALUES (7)');
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/poison_route.sql" >"$WORKDIR/poison_route.out" \
	2>"$WORKDIR/poison_route.err"; then
	cat "$WORKDIR/poison_route.out" "$WORKDIR/poison_route.err" >&2
	echo "FAIL: poison check of routed writes" >&2
	exit 1
fi
for probe in INSERT COPY; do
	if ! grep -q "^POISON_ROUTE_$probe 55000\$" "$WORKDIR/poison_route.out"; then
		cat "$WORKDIR/poison_route.out" >&2
		echo "FAIL: a routed write reached a blocked partition ($probe)" >&2
		exit 1
	fi
done
if ! grep -q '^POISON_ROUTE_REPAIRED written$' "$WORKDIR/poison_route.out"; then
	cat "$WORKDIR/poison_route.out" >&2
	echo "FAIL: fasttruncate did not repair the blocked partition" >&2
	exit 1
fi
echo "poison route passed"

# План, построенный до сбоя, исполняется позже: SQL-функция в PG 16 и 17
# планирует все операторы до первого, а первый вызывает отказавший
# fasttruncate. TRUNCATE через родителя наследования дает потомку новый файл,
# и откат возвращает поврежденный: блокировка обязана вернуться вместе с ним.
cat >"$WORKDIR/poison_late.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_poison_sqlfn(id int);
INSERT INTO ft_poison_sqlfn VALUES (1);
CREATE FUNCTION pg_temp.ft_poison_make() RETURNS void
LANGUAGE plpgsql AS $f$
BEGIN
  PERFORM fasttruncate('ft_poison_sqlfn');
EXCEPTION WHEN OTHERS THEN
  NULL;
END
$f$;
CREATE FUNCTION pg_temp.ft_poison_read_after() RETURNS bigint
LANGUAGE sql VOLATILE AS $f$
  SELECT pg_temp.ft_poison_make();
  SELECT count(*) FROM ft_poison_sqlfn;
$f$;
CREATE FUNCTION pg_temp.ft_poison_late_probe(q text) RETURNS text
LANGUAGE plpgsql AS $probe$
DECLARE r text;
BEGIN
  EXECUTE q INTO r;
  RETURN 'readable ' || r;
EXCEPTION WHEN OTHERS THEN
  RETURN SQLSTATE;
END
$probe$;
SET fasttrun.test_failpoint = 'after_main_heap';
SELECT 'POISON_LATE_SQLFN ' || pg_temp.ft_poison_late_probe('SELECT pg_temp.ft_poison_read_after()');
SET fasttrun.test_failpoint = '';
CREATE TEMP TABLE ft_poison_inh_parent(id int);
CREATE TEMP TABLE ft_poison_inh_child() INHERITS (ft_poison_inh_parent);
INSERT INTO ft_poison_inh_child SELECT generate_series(1, 100);
SET fasttrun.test_failpoint = 'after_main_heap';
DO $case$
BEGIN
  PERFORM fasttruncate('ft_poison_inh_child');
EXCEPTION WHEN OTHERS THEN
  NULL;
END
$case$;
SET fasttrun.test_failpoint = '';
BEGIN;
TRUNCATE ft_poison_inh_parent;
SELECT 'POISON_LATE_TRUNCATED ' || pg_temp.ft_poison_late_probe('SELECT count(*) FROM ft_poison_inh_child');
SELECT 'POISON_LATE_RETRY ' || pg_temp.ft_poison_late_probe('SELECT fasttruncate(''ft_poison_inh_child'')');
ROLLBACK;
SELECT 'POISON_LATE_ROLLBACK ' || pg_temp.ft_poison_late_probe('SELECT count(*) FROM ft_poison_inh_child');
BEGIN;
SAVEPOINT old_file;
TRUNCATE ft_poison_inh_parent;
SAVEPOINT new_file;
SELECT 'POISON_LATE_SUBRETRY ' || pg_temp.ft_poison_late_probe('SELECT fasttruncate(''ft_poison_inh_child'')');
ROLLBACK TO SAVEPOINT new_file;
ROLLBACK TO SAVEPOINT old_file;
SELECT 'POISON_LATE_SUBROLLBACK ' || pg_temp.ft_poison_late_probe('SELECT count(*) FROM ft_poison_inh_child');
COMMIT;
BEGIN;
TRUNCATE ft_poison_inh_parent;
SELECT 'POISON_LATE_COMMIT_RETRY ' || pg_temp.ft_poison_late_probe('SELECT fasttruncate(''ft_poison_inh_child'')');
COMMIT;
SELECT fasttruncate('ft_poison_inh_child');
SELECT 'POISON_LATE_COMMITTED ' || count(*) FROM fasttrun_blocked_tables()
WHERE relname = 'ft_poison_inh_child';
SELECT 'POISON_LATE_REPAIRED ' || pg_temp.ft_poison_late_probe('SELECT count(*) FROM ft_poison_inh_child');

-- An aborted replacement destroys its damaged file and restores a clean one.
BEGIN;
TRUNCATE ft_poison_inh_parent;
SET LOCAL fasttrun.test_failpoint = 'after_main_heap';
DO $case$
BEGIN
  PERFORM fasttruncate('ft_poison_inh_child');
EXCEPTION WHEN OTHERS THEN
  NULL;
END
$case$;
ROLLBACK;
SELECT fasttruncate('ft_poison_inh_child');
SELECT 'POISON_LATE_ABORT_FILE ' || count(*) FROM fasttrun_blocked_tables()
WHERE relname = 'ft_poison_inh_child';
BEGIN;
SAVEPOINT clean_file;
TRUNCATE ft_poison_inh_parent;
SET LOCAL fasttrun.test_failpoint = 'after_main_heap';
DO $case$
BEGIN
  PERFORM fasttruncate('ft_poison_inh_child');
EXCEPTION WHEN OTHERS THEN
  NULL;
END
$case$;
ROLLBACK TO SAVEPOINT clean_file;
SELECT fasttruncate('ft_poison_inh_child');
SELECT 'POISON_LATE_SUBABORT_FILE ' || count(*) FROM fasttrun_blocked_tables()
WHERE relname = 'ft_poison_inh_child';
COMMIT;

-- A restored outer uncommitted file is healthy after nested file changes.
CREATE FUNCTION pg_temp.ft_poison_fail(relname text) RETURNS void
LANGUAGE plpgsql AS $fn$
DECLARE caught boolean := false;
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', 'after_main_heap', true);
  BEGIN
    PERFORM fasttruncate(relname);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: after_main_heap' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', true);
  IF NOT caught THEN
    RAISE EXCEPTION 'truncate failpoint did not fire';
  END IF;
END
$fn$;
BEGIN;
CREATE TEMP TABLE ft_poison_outer_file(id int);
INSERT INTO ft_poison_outer_file VALUES (71);
SAVEPOINT replacement;
TRUNCATE ft_poison_outer_file;
SAVEPOINT forget_birth;
TRUNCATE ft_poison_outer_file;
ROLLBACK TO SAVEPOINT forget_birth;
SELECT pg_temp.ft_poison_fail('ft_poison_outer_file');
ROLLBACK TO SAVEPOINT replacement;
SELECT 'POISON_CREATED_RESTORED ' || id FROM ft_poison_outer_file;
SELECT fasttruncate('ft_poison_outer_file');
SELECT 'POISON_CREATED_RETRY ' || count(*) FROM ft_poison_outer_file;
COMMIT;
BEGIN;
TRUNCATE ft_poison_outer_file;
INSERT INTO ft_poison_outer_file VALUES (72);
SAVEPOINT replacement;
TRUNCATE ft_poison_outer_file;
SAVEPOINT forget_birth;
TRUNCATE ft_poison_outer_file;
ROLLBACK TO SAVEPOINT forget_birth;
SELECT pg_temp.ft_poison_fail('ft_poison_outer_file');
ROLLBACK TO SAVEPOINT replacement;
SELECT 'POISON_OUTER_RESTORED ' || id FROM ft_poison_outer_file;
SELECT fasttruncate('ft_poison_outer_file');
SELECT 'POISON_OUTER_RETRY ' || count(*) FROM ft_poison_outer_file;
COMMIT;

CREATE TABLESPACE ft_n1_space LOCATION :'n1_space_dir';
CREATE FUNCTION pg_temp.must_reject(relname text) RETURNS void LANGUAGE plpgsql AS $f$
BEGIN
  BEGIN
    PERFORM fasttruncate(relname);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun: table files can roll back independently' THEN RAISE; END IF;
    RETURN;
  END;
  RAISE EXCEPTION 'unsafe storage reset was accepted: %', relname;
END $f$;
CREATE FUNCTION pg_temp.must_have_rows(wanted int) RETURNS void LANGUAGE plpgsql AS $f$
DECLARE actual int;
BEGIN
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  SELECT count(*) INTO actual FROM n1_preflight;
  IF actual <> wanted THEN RAISE EXCEPTION 'wrong sequential rows: %', actual; END IF;
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', 'on', true);
  SELECT count(*) INTO actual FROM n1_preflight WHERE id > 0;
  IF actual <> wanted THEN RAISE EXCEPTION 'wrong index rows: %', actual; END IF;
END $f$;
CREATE TEMP TABLE n1_preflight(id int, payload text);
CREATE UNIQUE INDEX n1_preflight_pkey ON n1_preflight(id);
ALTER TABLE n1_preflight ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO n1_preflight SELECT n, repeat(md5(n::text),100) FROM generate_series(1,1000)n;
BEGIN;
SAVEPOINT moved;
ALTER TABLE n1_preflight SET TABLESPACE ft_n1_space;
SELECT pg_temp.must_reject('n1_preflight');
ROLLBACK TO SAVEPOINT moved;
SELECT pg_temp.must_have_rows(1000);
COMMIT;
\echo PREFLIGHT_MOVE_PASS
BEGIN;
SAVEPOINT reindexed;
REINDEX INDEX n1_preflight_pkey;
SELECT pg_temp.must_reject('n1_preflight');
ROLLBACK TO SAVEPOINT reindexed;
SELECT pg_temp.must_have_rows(1000);
COMMIT;
\echo PREFLIGHT_REINDEX_PASS
BEGIN;
SAVEPOINT omitted;
DROP INDEX n1_preflight_pkey CASCADE;
SELECT pg_temp.must_reject('n1_preflight');
ROLLBACK TO SAVEPOINT omitted;
SELECT pg_temp.must_have_rows(1000);
COMMIT;
\echo PREFLIGHT_DROPPED_PASS
BEGIN;
CREATE INDEX n1_preflight_new ON n1_preflight((left(payload,32)));
SELECT fasttruncate('n1_preflight');
ROLLBACK;
SELECT pg_temp.must_have_rows(0);
\echo PREFLIGHT_NEW_COMPONENT_PASS
INSERT INTO n1_preflight SELECT n, repeat(md5(n::text),100) FROM generate_series(1,1000)n;
BEGIN;
CREATE INDEX n1_preflight_gone ON n1_preflight(id);
DROP INDEX n1_preflight_gone;
SELECT fasttruncate('n1_preflight');
ROLLBACK;
SELECT pg_temp.must_have_rows(0);
\echo PREFLIGHT_NEW_DROPPED_PASS
SELECT count(*) = 0 AS no_poison_from_preflight FROM fasttrun_blocked_tables();
DROP TABLE n1_preflight;
DROP TABLESPACE ft_n1_space;

-- Parent utilities must reject a damaged leaf before core reads its storage.
CREATE TEMP TABLE ft_tree_parent(id int, payload text) PARTITION BY RANGE(id);
CREATE TEMP TABLE ft_tree_leaf PARTITION OF ft_tree_parent FOR VALUES FROM (0) TO (1000);
ALTER TABLE ft_tree_leaf ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE INDEX ft_tree_idx ON ft_tree_parent(id);
INSERT INTO ft_tree_leaf SELECT n, repeat(md5(n::text),200) FROM generate_series(1,100)n;
SELECT pg_temp.ft_poison_fail('ft_tree_leaf');
-- Unrelated maintenance remains available while a temporary heap is blocked.
CREATE TABLE public.ft_reindex_control(id int PRIMARY KEY);
REINDEX SCHEMA public;
REINDEX SYSTEM :DBNAME;
\echo TREE_UNRELATED_REINDEX_PASS
DO $$DECLARE detail text; blocked boolean:=false;
BEGIN
 BEGIN EXECUTE 'REINDEX TABLE ft_tree_leaf';
 EXCEPTION WHEN object_not_in_prerequisite_state THEN
  GET STACKED DIAGNOSTICS detail=PG_EXCEPTION_DETAIL;
  blocked:=true;
 END;
 IF NOT blocked OR detail NOT LIKE 'A previous fasttruncate may have changed table files.%' THEN
  RAISE EXCEPTION 'poison detail misnames the failed operation: %',detail;
 END IF;
END$$;
\set ON_ERROR_STOP 0
SELECT quote_ident(nspname) AS tmp_schema FROM pg_namespace WHERE oid=pg_my_temp_schema() \gset
REINDEX SCHEMA :tmp_schema;
\echo TREE_OWN_SCHEMA_REINDEX :SQLSTATE
REINDEX DATABASE :DBNAME;
\echo TREE_DATABASE_REINDEX :SQLSTATE
\set ON_ERROR_STOP 1
DROP TABLE public.ft_reindex_control;
SELECT 'TREE_RELSTATS_HEAP ' || pg_temp.ft_poison_late_probe('SELECT count(*) FROM fasttrun_relstats(''ft_tree_leaf'')');
SELECT 'TREE_RELSTATS_INDEX ' || pg_temp.ft_poison_late_probe(format(
  'SELECT count(*) FROM fasttrun_relstats(%L)',
  (SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid = 'ft_tree_leaf'::regclass LIMIT 1)));
SELECT 'TREE_ANALYZE ' || pg_temp.ft_poison_late_probe('ANALYZE ft_tree_parent');
SELECT 'TREE_INDEX ' || pg_temp.ft_poison_late_probe('CREATE INDEX ON ft_tree_parent(payload)');
SELECT 'TREE_ALTER ' || pg_temp.ft_poison_late_probe('ALTER TABLE ft_tree_parent ALTER COLUMN payload TYPE varchar');
\set ON_ERROR_STOP 0
VACUUM ft_tree_parent;
\echo TREE_VACUUM :SQLSTATE
REINDEX TABLE ft_tree_parent;
\echo TREE_REINDEX :SQLSTATE
REINDEX INDEX ft_tree_idx;
\echo TREE_REINDEX_INDEX :SQLSTATE
CLUSTER ft_tree_parent USING ft_tree_idx;
\echo TREE_CLUSTER :SQLSTATE
\set ON_ERROR_STOP 1
TRUNCATE ft_tree_parent;
SELECT fasttruncate('ft_tree_leaf');
SELECT 'TREE_REPAIRED ' || count(*) FROM ft_tree_leaf;
SELECT 'TREE_RELSTATS_REPAIRED ' || pg_temp.ft_poison_late_probe(format(
  'SELECT count(*) FROM fasttrun_relstats(%L)',
  (SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid = 'ft_tree_leaf'::regclass LIMIT 1)));
DROP TABLE ft_tree_parent;
CREATE TEMP TABLE ft_default_parent(id int) PARTITION BY RANGE(id);
CREATE TEMP TABLE ft_default_leaf PARTITION OF ft_default_parent DEFAULT;
CREATE TEMP TABLE ft_default_new(id int);
INSERT INTO ft_default_leaf VALUES (1);
SELECT pg_temp.ft_poison_fail('ft_default_leaf');
SELECT 'TREE_ATTACH_DEFAULT ' || pg_temp.ft_poison_late_probe(
  'ALTER TABLE ft_default_parent ATTACH PARTITION ft_default_new FOR VALUES FROM (0) TO (10)');
TRUNCATE ft_default_parent;
SELECT fasttruncate('ft_default_leaf');
DROP TABLE ft_default_parent;
DROP TABLE ft_default_new;
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v n1_space_dir="$WORKDIR/tablespace" \
	-f "$WORKDIR/poison_late.sql" >"$WORKDIR/poison_late.out" \
	2>"$WORKDIR/poison_late.err"; then
	cat "$WORKDIR/poison_late.out" "$WORKDIR/poison_late.err" >&2
	echo "FAIL: poison check of late plans and swapped files" >&2
	exit 1
fi
for expect in 'POISON_LATE_SQLFN 55000' 'POISON_LATE_TRUNCATED readable 0' \
	'POISON_LATE_RETRY 55000' 'POISON_LATE_ROLLBACK 55000' \
	'POISON_LATE_SUBRETRY 55000' 'POISON_LATE_SUBROLLBACK 55000' \
	'POISON_LATE_COMMIT_RETRY 55000' 'POISON_LATE_COMMITTED 0' \
	'POISON_LATE_REPAIRED readable 0' 'POISON_LATE_ABORT_FILE 0' \
	'POISON_LATE_SUBABORT_FILE 0' 'POISON_CREATED_RESTORED 71' \
	'POISON_CREATED_RETRY 0' 'POISON_OUTER_RESTORED 72' \
	'POISON_OUTER_RETRY 0' 'PREFLIGHT_MOVE_PASS' 'PREFLIGHT_REINDEX_PASS' \
	'PREFLIGHT_DROPPED_PASS' 'PREFLIGHT_NEW_COMPONENT_PASS' \
	'PREFLIGHT_NEW_DROPPED_PASS' 'TREE_ANALYZE 55000' 'TREE_INDEX 55000' \
	'TREE_UNRELATED_REINDEX_PASS' 'TREE_OWN_SCHEMA_REINDEX 55000' 'TREE_DATABASE_REINDEX 55000' \
	'TREE_ALTER 55000' 'TREE_VACUUM 55000' 'TREE_REINDEX 55000' \
	'TREE_REINDEX_INDEX 55000' 'TREE_CLUSTER 55000' 'TREE_REPAIRED 0' \
	'TREE_RELSTATS_HEAP 55000' 'TREE_RELSTATS_INDEX 55000' \
	'TREE_RELSTATS_REPAIRED readable 1' 'TREE_ATTACH_DEFAULT 55000'; do
	if ! grep -qx "$expect" "$WORKDIR/poison_late.out"; then
		cat "$WORKDIR/poison_late.out" >&2
		echo "FAIL: expected '$expect'" >&2
		exit 1
	fi
done
echo "poison late passed"

# A cold first call rejects unknown history before changing files or poison.
run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 >"$WORKDIR/late_load.out" 2>&1 <<'SQL'
CREATE TEMP TABLE ft_cold_history(id int);
BEGIN;
INSERT INTO ft_cold_history VALUES (1);
DO $test$ DECLARE hint text; caught boolean := false;
BEGIN
  BEGIN
    PERFORM fasttruncate('ft_cold_history');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    GET STACKED DIAGNOSTICS hint = PG_EXCEPTION_HINT;
    IF hint NOT LIKE '%Preload fasttrun%' THEN
      RAISE EXCEPTION 'late load has no preload/retry hint: %', hint;
    END IF;
    caught := true;
  END;
  IF NOT caught OR (SELECT count(*) FROM ft_cold_history) <> 1
     OR EXISTS (SELECT FROM fasttrun_blocked_tables()) THEN
    RAISE EXCEPTION 'late-load preflight changed rows or poison';
  END IF;
END $test$;
COMMIT;
SELECT fasttruncate('ft_cold_history');
DO $test$ BEGIN
  IF EXISTS (SELECT FROM ft_cold_history) THEN
    RAISE EXCEPTION 'late-load retry did not clear rows';
  END IF;
END $test$;
SQL
echo 'late-load preflight and retry passed'

# fasttrun.log_min_duration = 0 пишет в журнал сервера строку на каждый вызов:
# имя таблицы, время и путь, которым шел вызов.
cat >"$WORKDIR/slow_log.sql" <<'SQL'
\set ON_ERROR_STOP 1
SET client_min_messages = warning;
SET fasttrun.log_min_duration = 0;
CREATE TEMP TABLE ft_slow_log(id int);
INSERT INTO ft_slow_log SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('ft_slow_log');
SELECT fasttruncate('ft_slow_log');
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/slow_log.sql" >"$WORKDIR/slow_log.out" 2>&1; then
	cat "$WORKDIR/slow_log.out" >&2
	echo "FAIL: slow call log scenario" >&2
	exit 1
fi
for expect in 'fasttrun: fasttrun_analyze of "ft_slow_log" took' \
	'fasttrun: fasttruncate of "ft_slow_log" took' 'Path: full scan.' \
	'Path: storage reset.'; do
	if ! grep -qF "$expect" "$LOG"; then
		grep -n 'fasttrun:' "$LOG" | tail -5 >&2
		echo "FAIL: server log has no '$expect'" >&2
		exit 1
	fi
done
echo "slow log passed"

# Targeted lifecycle and recovery cases: subtransaction merge failure, missing
# main fork and btree metapage extension failure, TOAST REINDEX block, and
# two-transaction parent TRUNCATE retirement.
cat >"$WORKDIR/targeted_lifecycle.sql" <<'SQL'
\set ON_ERROR_STOP 1
SET client_min_messages = warning;

-- 1. Failpoint during SUBXACT_EVENT_PRE_COMMIT_SUB (RELEASE SAVEPOINT / block exit)
CREATE TEMP TABLE ft_submerge(a int, b text);
INSERT INTO ft_submerge SELECT g, md5(g::text) FROM generate_series(1, 500) g;
SELECT fasttrun_analyze('ft_submerge');
BEGIN;
SAVEPOINT sp_outer;
DO $submerge$
DECLARE caught boolean := false;
BEGIN
  INSERT INTO ft_submerge SELECT g, md5(g::text) FROM generate_series(501, 700) g;
  BEGIN
    INSERT INTO ft_submerge SELECT g, md5(g::text) FROM generate_series(701, 1000) g;
    PERFORM fasttrun_analyze('ft_submerge');
    PERFORM set_config('fasttrun.test_failpoint', 'subcommit_merge', false);
  EXCEPTION WHEN division_by_zero THEN
    NULL;
  END;
EXCEPTION WHEN SQLSTATE '55000' THEN
  caught := true;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'subcommit_merge failpoint did not fire';
  END IF;
END $submerge$;
-- Verify parent subxact inside DO survived cleanly, and test SQL RELEASE SAVEPOINT too:
DO $submerge_mid$
BEGIN
  IF (SELECT reltuples FROM fasttrun_relstats('ft_submerge')) <> 500 THEN
    RAISE EXCEPTION 'failed subcommit polluted parent subxact reltuples';
  END IF;
END $submerge_mid$;
SAVEPOINT sp_inner;
INSERT INTO ft_submerge SELECT g, md5(g::text) FROM generate_series(1001, 1200) g;
SELECT fasttrun_analyze('ft_submerge');
SET fasttrun.test_failpoint = 'subcommit_merge';
\set ON_ERROR_STOP 0
RELEASE SAVEPOINT sp_inner;
\set ON_ERROR_STOP 1
ROLLBACK TO SAVEPOINT sp_outer;
SET fasttrun.test_failpoint = '';
COMMIT;
DO $submerge_verify$
BEGIN
  IF (SELECT reltuples FROM fasttrun_relstats('ft_submerge')) <> 500
     OR (SELECT count(*) FROM fasttrun_inspect_stats('ft_submerge')) <> 2 THEN
    RAISE EXCEPTION 'rollback after subcommit_merge corrupted outer cache state';
  END IF;
END $submerge_verify$;
DROP TABLE ft_submerge;

CREATE TEMP TABLE ft_submerge_old(id int, drop_me int);
INSERT INTO ft_submerge_old VALUES (42, 7);
SELECT fasttrun_collect_stats('ft_submerge_old');
CREATE TEMP TABLE ft_submerge_anchor(id int);
BEGIN;
SELECT fasttrun_analyze('ft_submerge_anchor');
DO $submerge_fresh$
DECLARE
  caught boolean := false;
BEGIN
  BEGIN
    ALTER TABLE ft_submerge_old DROP COLUMN drop_me;
    TRUNCATE ft_submerge_old;
    PERFORM set_config('fasttrun.test_failpoint', 'subcommit_merge', false);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: subcommit_merge' THEN
      RAISE;
    END IF;
    caught := true;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'subcommit_merge failpoint did not fire';
  END IF;
  IF (SELECT array_agg(id) FROM ft_submerge_old) IS DISTINCT FROM ARRAY[42] THEN
    RAISE EXCEPTION 'TRUNCATE rollback after subcommit_merge did not restore old rows';
  END IF;
  IF fasttrun_temp_is_ours('ft_submerge_old'::regclass) THEN
    RAISE EXCEPTION 'failed subcommit left stale fresh mark after subxact rollback';
  END IF;
END $submerge_fresh$;
COMMIT;
DROP TABLE ft_submerge_old, ft_submerge_anchor;

SELECT 'SUBCOMMIT_MERGE_OK';

-- 2. Failure between unlink() and smgrcreate(MAIN_FORKNUM) and before btree smgrextend
DO $missing_fork$
DECLARE
  is_ee boolean := false;
  caught boolean := false;
BEGIN
  IF to_regprocedure('pg_catalog.pgpro_edition()') IS NOT NULL THEN
    EXECUTE 'SELECT pg_catalog.pgpro_edition() = ''enterprise''' INTO is_ee;
  END IF;
  IF NOT is_ee THEN
    EXECUTE 'CREATE TEMP TABLE ft_missing_main(id int PRIMARY KEY, v text)';
    EXECUTE 'INSERT INTO ft_missing_main SELECT g, md5(g::text) FROM generate_series(1, 200) g';
    PERFORM fasttrun_analyze('ft_missing_main');
    PERFORM set_config('fasttrun.zero_sinval_truncate', 'on', false);
    PERFORM set_config('fasttrun.test_failpoint', 'before_smgrcreate_main', false);
    BEGIN
      PERFORM fasttruncate('ft_missing_main');
    EXCEPTION WHEN SQLSTATE '55000' THEN
      caught := true;
    END;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    IF NOT caught OR NOT EXISTS (
      SELECT 1 FROM fasttrun_blocked_tables() WHERE relname = 'ft_missing_main'
    ) THEN
      RAISE EXCEPTION 'before_smgrcreate_main did not poison ft_missing_main';
    END IF;
    -- Diagnostic view must not fail with ENOENT when main fork is unlinked.
    PERFORM count(*) FROM fasttrun_cached_tables();
    -- Repair under zero_sinval_truncate = off must not PANIC inside RelationTruncate.
    PERFORM set_config('fasttrun.zero_sinval_truncate', 'off', false);
    PERFORM fasttruncate('ft_missing_main');
    IF EXISTS (SELECT 1 FROM fasttrun_blocked_tables() WHERE relname = 'ft_missing_main') THEN
      RAISE EXCEPTION 'zero_sinval_truncate=off repair left ft_missing_main blocked';
    END IF;
    -- Re-poison and repair under zero_sinval_truncate = on + DROP TABLE.
    EXECUTE 'INSERT INTO ft_missing_main VALUES (1, ''ok'')';
    PERFORM set_config('fasttrun.zero_sinval_truncate', 'on', false);
    PERFORM set_config('fasttrun.test_failpoint', 'before_smgrcreate_main', false);
    caught := false;
    BEGIN
      PERFORM fasttruncate('ft_missing_main');
    EXCEPTION WHEN SQLSTATE '55000' THEN
      caught := true;
    END;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    IF NOT caught THEN
      RAISE EXCEPTION 'second before_smgrcreate_main did not fire';
    END IF;
    PERFORM fasttruncate('ft_missing_main');
    EXECUTE 'DROP TABLE ft_missing_main';

    -- Failure right before btree metapage smgrextend.
    EXECUTE 'CREATE TEMP TABLE ft_missing_bt(id int PRIMARY KEY)';
    EXECUTE 'INSERT INTO ft_missing_bt SELECT generate_series(1, 50)';
    PERFORM set_config('fasttrun.test_failpoint', 'before_btree_smgrextend', false);
    caught := false;
    BEGIN
      PERFORM fasttruncate('ft_missing_bt');
    EXCEPTION WHEN SQLSTATE '55000' THEN
      caught := true;
    END;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    IF NOT caught OR NOT EXISTS (
      SELECT 1 FROM fasttrun_blocked_tables() WHERE relname = 'ft_missing_bt'
    ) THEN
      RAISE EXCEPTION 'before_btree_smgrextend did not poison ft_missing_bt';
    END IF;
    PERFORM fasttruncate('ft_missing_bt');
    EXECUTE 'INSERT INTO ft_missing_bt VALUES (42)';
    IF (SELECT count(*) FROM ft_missing_bt WHERE id = 42) <> 1 THEN
      RAISE EXCEPTION 'repaired btree index after before_btree_smgrextend is unusable';
    END IF;
    EXECUTE 'DROP TABLE ft_missing_bt';
  END IF;
END $missing_fork$;
SELECT 'MISSING_FORK_REPAIR_OK';

-- 3. REINDEX TABLE / REINDEX SCHEMA on pg_toast_temp_N blocked while poisoned
CREATE TEMP TABLE ft_toast_reindex(id int, big text);
ALTER TABLE ft_toast_reindex ALTER COLUMN big SET STORAGE EXTERNAL;
INSERT INTO ft_toast_reindex SELECT g, repeat('z', 4000) FROM generate_series(1, 30) g;
DO $toast_reindex$
DECLARE
  toast_oid oid;
  toast_nsp name;
  toast_rel name;
  caught_poison boolean := false;
  caught_tbl boolean := false;
  caught_sch boolean := false;
BEGIN
  SELECT c.reltoastrelid, n.nspname, t.relname
    INTO toast_oid, toast_nsp, toast_rel
  FROM pg_class c
  JOIN pg_class t ON t.oid = c.reltoastrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  WHERE c.oid = 'ft_toast_reindex'::regclass;

  PERFORM set_config('fasttrun.test_failpoint', 'after_toast_index:1', false);
  BEGIN
    PERFORM fasttruncate('ft_toast_reindex');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught_poison := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught_poison THEN
    RAISE EXCEPTION 'after_toast_index:1 did not fire';
  END IF;

  BEGIN
    EXECUTE format('REINDEX TABLE %I.%I', toast_nsp, toast_rel);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught_tbl := true;
  END;
  BEGIN
    EXECUTE format('REINDEX SCHEMA %I', toast_nsp);
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught_sch := true;
  END;
  IF NOT caught_tbl OR NOT caught_sch THEN
    RAISE EXCEPTION 'TOAST REINDEX bypassed poison: table=%, schema=%', caught_tbl, caught_sch;
  END IF;
  PERFORM fasttruncate('ft_toast_reindex');
END $toast_reindex$;
DROP TABLE ft_toast_reindex;
SELECT 'TOAST_REINDEX_BLOCK_OK';

-- 4. Committed parent TRUNCATE retires child poison without reading child
CREATE TEMP TABLE ft_retire_p(id int);
CREATE TEMP TABLE ft_retire_c() INHERITS (ft_retire_p);
INSERT INTO ft_retire_c VALUES (1), (2);
DO $parent_retire$
DECLARE caught boolean := false;
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', 'after_main_heap', false);
  BEGIN
    PERFORM fasttruncate('ft_retire_c');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'after_main_heap did not fire on ft_retire_c';
  END IF;
END $parent_retire$;
BEGIN;
TRUNCATE ft_retire_p;
COMMIT;
BEGIN;
TRUNCATE ft_retire_p;
SELECT fasttruncate('ft_retire_c');
COMMIT;
DROP TABLE ft_retire_c, ft_retire_p;
SELECT 'PARENT_TRUNCATE_RETIRE_OK';
SQL

if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/targeted_lifecycle.sql" >"$WORKDIR/targeted_lifecycle.out" 2>&1; then
	cat "$WORKDIR/targeted_lifecycle.out" >&2
	echo "FAIL: targeted lifecycle fault scenarios" >&2
	exit 1
fi
for expect in 'SUBCOMMIT_MERGE_OK' 'MISSING_FORK_REPAIR_OK' 'TOAST_REINDEX_BLOCK_OK' 'PARENT_TRUNCATE_RETIRE_OK'; do
	if ! grep -qx "$expect" "$WORKDIR/targeted_lifecycle.out"; then
		cat "$WORKDIR/targeted_lifecycle.out" >&2
		echo "FAIL: missing '$expect' in targeted_lifecycle.out" >&2
		exit 1
	fi
done
echo "targeted lifecycle fault scenarios passed"

# 5. Real asynchronous pg_cancel_backend() mid-truncate after storage mutation:
#    must abort with SQLSTATE 57014, leave the table blocked (55000), and repair cleanly.
cat >"$WORKDIR/cancel_truncate.sql" <<'SQL'
\set VERBOSITY verbose
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
CREATE TEMP TABLE ft_cancel_mid (
  id int PRIMARY KEY,
  grp int,
  payload text
);
ALTER TABLE ft_cancel_mid ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE INDEX ft_cancel_mid_grp ON ft_cancel_mid (grp);
INSERT INTO ft_cancel_mid
SELECT g, g % 10, repeat('X', 3000) FROM generate_series(1, 120) g;
SELECT fasttrun_analyze('ft_cancel_mid');
SELECT pg_backend_pid();
SELECT 'CANCEL_READY';
SET client_min_messages = notice;
SET fasttrun.test_failpoint = 'pause:after_main_heap';
\set ON_ERROR_STOP 0
SELECT fasttruncate('ft_cancel_mid');
\set ON_ERROR_STOP 1
SET client_min_messages = warning;
SET fasttrun.test_failpoint = '';
DO $verify_cancel$
DECLARE
  blocked_read boolean := false;
  blocked_dml boolean := false;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM fasttrun_blocked_tables() WHERE relname = 'ft_cancel_mid') THEN
    RAISE EXCEPTION 'canceled fasttruncate did not poison ft_cancel_mid';
  END IF;
  BEGIN
    PERFORM count(*) FROM ft_cancel_mid;
  EXCEPTION WHEN SQLSTATE '55000' THEN
    blocked_read := true;
  END;
  BEGIN
    INSERT INTO ft_cancel_mid VALUES (1, 1, 'a');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    blocked_dml := true;
  END;
  IF NOT blocked_read OR NOT blocked_dml THEN
    RAISE EXCEPTION 'poison after cancel did not block read=% or dml=%', blocked_read, blocked_dml;
  END IF;
  PERFORM fasttruncate('ft_cancel_mid');
  IF EXISTS (SELECT 1 FROM fasttrun_blocked_tables() WHERE relname = 'ft_cancel_mid') THEN
    RAISE EXCEPTION 'fasttruncate repair did not clear poison after cancel';
  END IF;
  INSERT INTO ft_cancel_mid VALUES (77, 7, repeat('Y', 3000));
  PERFORM set_config('enable_seqscan', 'off', true);
  IF (SELECT count(*) FROM ft_cancel_mid WHERE id = 77 AND length(payload) = 3000) <> 1 THEN
    RAISE EXCEPTION 'repaired ft_cancel_mid index/TOAST check failed';
  END IF;
END $verify_cancel$;
DROP TABLE ft_cancel_mid;
SELECT 'PG_CANCEL_TRUNCATE_OK';
SQL

run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/cancel_truncate.sql" >"$WORKDIR/cancel_truncate.out" \
	2>"$WORKDIR/cancel_truncate.err" &
cancel_psql_pid=$!

cancel_bpid=""
for _ in $(seq 1 200); do
	if grep -q '^CANCEL_READY$' "$WORKDIR/cancel_truncate.out" 2>/dev/null; then
		cancel_bpid=$(grep -E '^[0-9]+$' "$WORKDIR/cancel_truncate.out" | head -1 || true)
		break
	fi
	if ! kill -0 "$cancel_psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.02
done
terminate_cancel_worker()
{
	local pid=${1:-}
	local i
	[ -n "$pid" ] || return 0
	kill -TERM "$pid" >/dev/null 2>&1 || true
	kill -CONT "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	kill -KILL "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	return 0
}

wait_cancel_worker()
{
	local pid=$1
	local ticks=${2:-}
	local i
	local rc=0
	[ -n "$ticks" ] || ticks=${FASTTRUN_WAIT_TICKS:-200}
	for i in $(seq 1 "$ticks"); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" || rc=$?
			return "$rc"
		fi
		sleep 0.05
	done
	terminate_cancel_worker "$pid"
	return 124
}

if [ -z "$cancel_bpid" ]; then
	terminate_cancel_worker "$cancel_psql_pid"
	cat "$WORKDIR/cancel_truncate.out" "$WORKDIR/cancel_truncate.err" >&2
	echo "FAIL: cancel_truncate worker did not reach CANCEL_READY" >&2
	exit 1
fi
for _ in $(seq 1 400); do
	if grep -Fq 'fasttrun test failpoint paused: after_main_heap' "$WORKDIR/cancel_truncate.err" 2>/dev/null; then
		break
	fi
	if ! kill -0 "$cancel_psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.02
done
if ! grep -Fq 'fasttrun test failpoint paused: after_main_heap' "$WORKDIR/cancel_truncate.err" 2>/dev/null; then
	terminate_cancel_worker "$cancel_psql_pid"
	cat "$WORKDIR/cancel_truncate.out" "$WORKDIR/cancel_truncate.err" >&2
	echo "FAIL: cancel_truncate worker did not report pause:after_main_heap" >&2
	exit 1
fi
cancel_sent=$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c "SELECT pg_cancel_backend($cancel_bpid)")
if [ "$cancel_sent" != "t" ]; then
	terminate_cancel_worker "$cancel_psql_pid"
	cat "$WORKDIR/cancel_truncate.out" "$WORKDIR/cancel_truncate.err" >&2
	echo "FAIL: pg_cancel_backend($cancel_bpid) returned '$cancel_sent'" >&2
	exit 1
fi

if ! wait_cancel_worker "$cancel_psql_pid"; then
	cat "$WORKDIR/cancel_truncate.out" "$WORKDIR/cancel_truncate.err" >&2
	echo "FAIL: cancel_truncate session exited nonzero" >&2
	exit 1
fi
if ! grep -q '57014' "$WORKDIR/cancel_truncate.err" || \
	! grep -qx 'PG_CANCEL_TRUNCATE_OK' "$WORKDIR/cancel_truncate.out"; then
	cat "$WORKDIR/cancel_truncate.out" "$WORKDIR/cancel_truncate.err" >&2
	echo "FAIL: missing 57014 or PG_CANCEL_TRUNCATE_OK in cancel_truncate" >&2
	exit 1
fi
echo "pg_cancel_backend mid-truncate poison and repair passed"

# ----------------------------------------------------------------------
# Simulated ATX relcache invalidation inside an active outer transaction:
# creation birth in fasttrun_temp_is_ours and uncommitted storage replacement
# over a poisoned locator must both survive RelationCacheInvalidate().
# ----------------------------------------------------------------------
if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 >"$WORKDIR/atx_relcache.out" 2>"$WORKDIR/atx_relcache.err" <<'SQL'
CREATE EXTENSION IF NOT EXISTS fasttrun;
CREATE FUNCTION pg_temp.ft_sim_atx_reset() RETURNS void
  AS '$libdir/fasttrun', 'fasttrun_test_simulate_atx_relcache_reset'
  LANGUAGE C VOLATILE;

BEGIN;
CREATE TEMP TABLE ft_atx_born(id int PRIMARY KEY);
INSERT INTO ft_atx_born VALUES (1), (2);
SAVEPOINT sp_child;
CREATE TEMP TABLE ft_atx_child(id int);
SELECT pg_temp.ft_sim_atx_reset();
DO $$
BEGIN
  IF NOT fasttrun_temp_is_ours('ft_atx_born'::regclass) OR
     NOT fasttrun_temp_is_ours('ft_atx_child'::regclass) THEN
    RAISE EXCEPTION 'creation birth lost across relcache reset';
  END IF;
END $$;
ROLLBACK TO SAVEPOINT sp_child;
DO $$
BEGIN
  IF NOT fasttrun_temp_is_ours('ft_atx_born'::regclass) THEN
    RAISE EXCEPTION 'outer creation birth lost after child rollback';
  END IF;
END $$;
COMMIT;
DO $$
BEGIN
  IF fasttrun_temp_is_ours('ft_atx_born'::regclass) THEN
    RAISE EXCEPTION 'creation birth leaked into next transaction';
  END IF;
END $$;

CREATE TEMP TABLE ft_atx_poison_inh(id int);
CREATE TEMP TABLE ft_atx_poison_child() INHERITS (ft_atx_poison_inh);
INSERT INTO ft_atx_poison_child VALUES (10);
SET fasttrun.test_failpoint = 'after_main_heap';
DO $$
BEGIN
  PERFORM fasttruncate('ft_atx_poison_child');
  RAISE EXCEPTION 'expected failpoint';
EXCEPTION WHEN object_not_in_prerequisite_state THEN
  IF SQLERRM <> 'fasttrun test failpoint: after_main_heap' THEN
    RAISE;
  END IF;
END $$;
SET fasttrun.test_failpoint = '';

BEGIN;
SAVEPOINT sp_rep;
TRUNCATE ft_atx_poison_inh;
SELECT pg_temp.ft_sim_atx_reset();
SELECT count(*) FROM ft_atx_poison_child;
ROLLBACK TO SAVEPOINT sp_rep;
DO $$
DECLARE blocked boolean := false;
BEGIN
  BEGIN
    PERFORM count(*) FROM ft_atx_poison_child;
  EXCEPTION WHEN object_not_in_prerequisite_state THEN
    blocked := true;
  END;
  IF NOT blocked THEN
    RAISE EXCEPTION 'poison block was prematurely retired across relcache reset';
  END IF;
END $$;
ROLLBACK;

DROP TABLE ft_atx_poison_child, ft_atx_poison_inh, ft_atx_born;

BEGIN;
SAVEPOINT sp_subp;
CREATE TEMP TABLE ft_atx_subp(id int);
INSERT INTO ft_atx_subp VALUES (1);
SELECT pg_temp.ft_sim_atx_reset();
SAVEPOINT sp_inner_fail;
SET fasttrun.test_failpoint = 'after_main_heap';
DO $$
BEGIN
  PERFORM fasttruncate('ft_atx_subp');
  RAISE EXCEPTION 'expected failpoint';
EXCEPTION WHEN object_not_in_prerequisite_state THEN
  IF SQLERRM <> 'fasttrun test failpoint: after_main_heap' THEN
    RAISE;
  END IF;
END $$;
SET fasttrun.test_failpoint = '';
ROLLBACK TO SAVEPOINT sp_inner_fail;
DO $$
BEGIN
  IF (SELECT count(*) FROM fasttrun_blocked_tables() WHERE relname = 'ft_atx_subp') <> 1 THEN
    RAISE EXCEPTION 'subxact-born poison lost after inner rollback';
  END IF;
END $$;
ROLLBACK TO SAVEPOINT sp_subp;
DO $$
BEGIN
  IF (SELECT count(*) FROM fasttrun_blocked_tables()) <> 0 THEN
    RAISE EXCEPTION 'subxact-born poison leaked after creation savepoint rollback';
  END IF;
END $$;
ROLLBACK;

SELECT 'ATX_RELCACHE_RESET_OK';
SQL
then
	cat "$WORKDIR/atx_relcache.out" "$WORKDIR/atx_relcache.err" >&2
	echo "FAIL: ATX relcache reset survival check failed" >&2
	exit 1
fi
if ! grep -qx 'ATX_RELCACHE_RESET_OK' "$WORKDIR/atx_relcache.out"; then
	cat "$WORKDIR/atx_relcache.out" "$WORKDIR/atx_relcache.err" >&2
	echo "FAIL: ATX relcache reset survival check failed" >&2
	exit 1
fi
echo "ATX relcache reset birth and poison survival passed"

if grep -Eq 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|Segmentation fault|Abort trap|leaked hash_seq_search|AbortSubTransaction while in COMMIT state' \
	"$LOG"; then
	grep -E 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|Segmentation fault|Abort trap|leaked hash_seq_search|AbortSubTransaction while in COMMIT state' \
		"$LOG" >&2
	echo "FAIL: fault matrix caused an assertion or server exit" >&2
	exit 1
fi

echo "truncate fault matrix passed: $boundary_count cases across $failpoint_count failpoints plus lifecycle, zero unexpected exits"
