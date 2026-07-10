#!/usr/bin/env bash
# Проверка хуков планировщика в cassert-сборке.
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PYTHON=${PYTHON:-python3}
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-planner-probes.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_planner_probes}
MODE=${1:-all}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
FIXTURE_DIR=$REPO_ROOT/tests/fixtures/fasttrun_hook_probe

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
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop >/dev/null 2>&1 || true
	fi
	make -C "$FIXTURE_DIR" clean PG_CONFIG="$PG_CONFIG" >/dev/null 2>&1 || true
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

check_width=0
check_owner=0
check_nested=0
case "$MODE" in
	outside-width) check_width=1 ;;
	owner) check_owner=1 ;;
	nested) check_nested=1 ;;
	all) check_width=1; check_owner=1; check_nested=1 ;;
	*) echo "unknown planner probe mode: $MODE" >&2; exit 2 ;;
esac

mkdir -p "$WORKDIR/socket"
if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR" "$WORKDIR/socket"
fi
PORT=${PGPORT:-$($PYTHON -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

make -C "$FIXTURE_DIR" clean PG_CONFIG="$PG_CONFIG" >/dev/null
make -C "$FIXTURE_DIR" PG_CONFIG="$PG_CONFIG" >/dev/null
PROBE_SO=""
for candidate in \
	"$FIXTURE_DIR/fasttrun_hook_probe.so" \
	"$FIXTURE_DIR/fasttrun_hook_probe.dylib"; do
	if [ -f "$candidate" ]; then
		PROBE_SO=$candidate
		break
	fi
done
if [ -z "$PROBE_SO" ]; then
	echo "test hook module did not produce a shared library" >&2
	exit 1
fi

run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" -l "$WORKDIR/postgres.log" \
	-o "-k $WORKDIR/socket -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$WORKDIR/socket" -p "$PORT" "$DBNAME"

run_pg "$PSQL" -h "$WORKDIR/socket" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -v check_width="$check_width" \
	-v check_owner="$check_owner" -v check_nested="$check_nested" \
	-v probe_so="$PROBE_SO" <<'SQL'
CREATE EXTENSION fasttrun;
CREATE FUNCTION pg_temp.fasttrun_test_planner_probe(boolean)
RETURNS bigint[] AS '$libdir/fasttrun', 'fasttrun_test_planner_probe'
LANGUAGE C STRICT;
CREATE FUNCTION pg_temp.fasttrun_test_call_attavgwidth(oid, smallint)
RETURNS integer AS '$libdir/fasttrun', 'fasttrun_test_call_attavgwidth'
LANGUAGE C STRICT;

CREATE TEMP TABLE ft_probe_warm (id int);
INSERT INTO ft_probe_warm SELECT generate_series(1, 10);
SELECT fasttrun_analyze('ft_probe_warm');
LOAD :'probe_so';
CREATE FUNCTION pg_temp.fasttrun_hook_probe_counts()
RETURNS bigint[] AS :'probe_so', 'fasttrun_hook_probe_counts'
LANGUAGE C;
CREATE FUNCTION pg_temp.fasttrun_hook_probe_reset()
RETURNS void AS :'probe_so', 'fasttrun_hook_probe_reset'
LANGUAGE C;
CREATE FUNCTION pg_temp.fasttrun_hook_probe_arm_nested(boolean)
RETURNS void AS :'probe_so', 'fasttrun_hook_probe_arm_nested'
LANGUAGE C STRICT;

\if :check_width
BEGIN;
CREATE TEMP TABLE ft_width_outside (id int, payload text);
INSERT INTO ft_width_outside
SELECT g, md5(g::text) FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('ft_width_outside');
COMMIT;

SET fasttrun.stats_refresh_threshold = 0.001;
BEGIN;
SAVEPOINT width_probe;
UPDATE ft_width_outside SET payload = repeat('x', 200) WHERE id <= 20;
SELECT pg_temp.fasttrun_test_planner_probe(true);
DO $check$
DECLARE
  width integer;
  counts bigint[];
BEGIN
  width := pg_temp.fasttrun_test_call_attavgwidth(
           'ft_width_outside'::regclass, 2::smallint);
  counts := pg_temp.fasttrun_test_planner_probe(false);
  IF width <= 0 THEN
    RAISE EXCEPTION 'width hook returned %', width;
  END IF;
  IF counts[1] = 0 OR counts[2] = 0 THEN
    RAISE EXCEPTION 'width hook did not reach freshness probes: %', counts;
  END IF;
END
$check$;
ROLLBACK TO SAVEPOINT width_probe;
DO $check$
DECLARE counts bigint[];
BEGIN
  counts := pg_temp.fasttrun_test_planner_probe(false);
  IF counts[4] <> 0 THEN
    RAISE EXCEPTION 'width hook outside planner caused % abort invalidations',
                    counts[4];
  END IF;
END
$check$;
COMMIT;
SELECT 'OUTSIDE_WIDTH_OK';
\endif

\if :check_owner
CREATE TABLE ft_perm_expr (id int, name text);
INSERT INTO ft_perm_expr
SELECT g, CASE WHEN g <= 9000 THEN 'dup' ELSE 'v_' || g END
FROM generate_series(1, 10000) g;
CREATE INDEX ft_perm_expr_idx ON ft_perm_expr (lower(name));
ANALYZE ft_perm_expr;

CREATE TEMP TABLE ft_unrelated_cache (id int, grp int);
INSERT INTO ft_unrelated_cache SELECT g, g FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('ft_unrelated_cache');
SELECT pg_temp.fasttrun_test_planner_probe(true);
DO $check$
DECLARE
  ln text;
  counts bigint[];
BEGIN
  FOR ln IN EXPLAIN SELECT * FROM ft_perm_expr WHERE lower(name) = 'dup' LOOP
    NULL;
  END LOOP;
  counts := pg_temp.fasttrun_test_planner_probe(false);
  IF counts[5] = 0 THEN
    RAISE EXCEPTION 'permanent expression query missed index stats hook';
  END IF;
  IF counts[3] <> 0 THEN
    RAISE EXCEPTION 'unmanaged expression query opened % index owners',
                    counts[3];
  END IF;
END
$check$;
SELECT 'OWNER_LOOKUP_OK';

SELECT pg_temp.fasttrun_test_planner_probe(true);
SELECT pg_temp.fasttrun_hook_probe_reset();
SELECT pg_temp.fasttrun_hook_probe_arm_nested(false);
DO $check$
DECLARE
  ln text;
  fasttrun_counts bigint[];
  hook_counts bigint[];
BEGIN
  FOR ln IN EXPLAIN
      SELECT * FROM ft_unrelated_cache WHERE grp = 42 AND id = 42 LOOP
    NULL;
  END LOOP;
  fasttrun_counts := pg_temp.fasttrun_test_planner_probe(false);
  hook_counts := pg_temp.fasttrun_hook_probe_counts();
  IF hook_counts[7] <> 1 THEN
    RAISE EXCEPTION 'nested unmanaged planner hook count is %', hook_counts;
  END IF;
  IF fasttrun_counts[3] <> 0 THEN
    RAISE EXCEPTION 'nested unmanaged expression query opened % index owners',
                    fasttrun_counts[3];
  END IF;
END
$check$;
SELECT 'NESTED_UNMANAGED_OWNER_OK';
\endif

\if :check_nested
CREATE TEMP TABLE ft_nested_memo (id int, grp int, payload text);
INSERT INTO ft_nested_memo
SELECT g, g % 1000, md5(g::text) FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('ft_nested_memo');
SELECT pg_temp.fasttrun_test_planner_probe(true);
SELECT pg_temp.fasttrun_hook_probe_reset();
SELECT pg_temp.fasttrun_hook_probe_arm_nested(true);
DO $check$
DECLARE
  ln text;
  fasttrun_counts bigint[];
  hook_counts bigint[];
BEGIN
  FOR ln IN EXPLAIN
      SELECT * FROM ft_nested_memo WHERE grp = 42 AND payload = 'x' LOOP
    NULL;
  END LOOP;
  fasttrun_counts := pg_temp.fasttrun_test_planner_probe(false);
  hook_counts := pg_temp.fasttrun_hook_probe_counts();
  IF hook_counts[7] <> 1 THEN
    RAISE EXCEPTION 'nested planner hook count is %', hook_counts;
  END IF;
  IF fasttrun_counts[1] < 3 OR fasttrun_counts[2] < 3 THEN
    RAISE EXCEPTION 'outer freshness memo survived nested planner: %, hook=%',
                    fasttrun_counts, hook_counts;
  END IF;
END
$check$;
SELECT 'NESTED_MEMO_OK';
\endif
SQL

echo "planner probe passed: $MODE"
