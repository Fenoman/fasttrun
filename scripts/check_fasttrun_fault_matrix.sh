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
  payload text
);
CREATE INDEX ft_fault_grp_idx ON ft_fault (grp);
ALTER TABLE ft_fault ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO ft_fault
SELECT g, g % 31,
       string_agg(md5((g * 1000 + s)::text), '')
FROM generate_series(1, 200) g
CROSS JOIN LATERAL generate_series(1, 450) s
GROUP BY g;
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
       (SELECT count(*) FROM ft_fault WHERE grp = 11) = 0 THEN
      RAISE EXCEPTION '% damaged a forced index path', point;
    END IF;
    IF (SELECT count(*) FROM fasttrun_inspect_stats('ft_fault')) = 0 OR
       (SELECT reltuples FROM fasttrun_relstats('ft_fault')) <= 0 THEN
      RAISE EXCEPTION '% lost pre-call local statistics', point;
    END IF;
  ELSE
    PERFORM pg_temp.expect_poison('SELECT count(*) FROM ft_fault');
    PERFORM pg_temp.expect_poison('EXECUTE ft_prepared(11)');
    PERFORM pg_temp.expect_poison('INSERT INTO ft_fault VALUES (999, 1, ''x'')');
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
    SELECT 1000 + g, g % 31,
           string_agg(md5((900000 + g * 1000 + s)::text), '')
    FROM generate_series(1, 20) g
    CROSS JOIN LATERAL generate_series(1, 120) s
    GROUP BY g;
    PERFORM set_config('enable_seqscan', 'off', true);
    IF (SELECT count(*) FROM ft_fault) <> 20 OR
       (SELECT min(id) FROM ft_fault) <> 1001 OR
       (SELECT count(*) FROM ft_fault WHERE id = 1007) <> 1 OR
       (SELECT count(*) FROM ft_fault WHERE grp = 7) = 0 THEN
      RAISE EXCEPTION '% repair/refill returned stale or missing rows', point;
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
}

early_failpoints=(
	after_prepare
	after_phase0
)
post_failpoints=(
	after_user_index:1
	after_user_index:2
	after_toast_index:1
	after_toast_heap
	after_main_heap
	after_ambuild:1
	after_ambuild:2
	after_ambuild:3
	before_publish
	after_publish:1
	after_publish:2
	after_publish:3
	after_publish:4
	after_publish:5
)

for mode in on off; do
	for failpoint in "${early_failpoints[@]}"; do
		run_boundary_case "$mode" "$failpoint" early
	done
	for failpoint in "${post_failpoints[@]}"; do
		run_boundary_case "$mode" "$failpoint" post
	done
done

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

if grep -Eq 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG"; then
	grep -E 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG" >&2
	echo "FAIL: fault matrix caused an assertion or server exit" >&2
	exit 1
fi

echo "truncate fault matrix passed: 32 boundaries plus lifecycle, zero unexpected exits"
