#!/usr/bin/env bash
#
# The bundled test module checks both hook load orders. Installed third-party
# modules provide an optional extra check.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PG_PKGLIBDIR=$($PG_CONFIG --pkglibdir)
PG_SHAREDIR=$($PG_CONFIG --sharedir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PYTHON=${PYTHON:-python3}
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-hook-chain.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_hook_chain}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
FIXTURE_DIR=$REPO_ROOT/tests/fixtures/fasttrun_hook_probe

shared_candidates=(
	citus_columnar
	columnar
	timescaledb
	pg_query_state
	pg_cron
	pg_wait_sampling
	pgpro_stats
	pg_xclaim
)
session_candidates=(
	pg_query_stack
	pg_variables
)

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

stop_cluster()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop \
			>/dev/null 2>&1 || true
	fi
}

cleanup()
{
	stop_cluster
	make -C "$FIXTURE_DIR" clean PG_CONFIG="$PG_CONFIG" >/dev/null 2>&1 || true
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

require_cmd()
{
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing command: $1" >&2
		exit 1
	fi
}

for cmd in "$PSQL" "$INITDB" "$PG_CTL" "$CREATEDB" "$PYTHON" make; do
	require_cmd "$cmd"
done
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi

mkdir -p "$WORKDIR"
if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR"
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

join_by_comma()
{
	local IFS=,
	echo "$*"
}

library_exists()
{
	local lib=$1
	[ -f "$PG_PKGLIBDIR/$lib.so" ] || [ -f "$PG_PKGLIBDIR/$lib.dylib" ]
}

extension_control_exists()
{
	local ext=$1
	[ -f "$PG_SHAREDIR/extension/$ext.control" ]
}

start_with_libs()
{
	local shared_csv=$1
	local session_csv=$2
	local opts="-k $WORKDIR -p $PORT -c listen_addresses=''"

	if [ -n "$shared_csv" ]; then
		opts="$opts -c shared_preload_libraries=$shared_csv"
	fi
	if [ -n "$session_csv" ]; then
		opts="$opts -c session_preload_libraries=$session_csv"
	fi

	run_pg "$PG_CTL" -D "$WORKDIR/data" -o "$opts" \
		-l "$WORKDIR/postgres.log" -w start >/dev/null 2>&1
}

run_synthetic_order()
{
	local order=$1
	local probe_first=$2
	local probe_after=$3
	local out=$WORKDIR/synthetic-$order.out
	local err=$WORKDIR/synthetic-$order.err

	if ! run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -v probe_so="$PROBE_SO" -v order="$order" \
		-v probe_first="$probe_first" -v probe_after="$probe_after" \
		>"$out" 2>"$err" <<'SQL'
SET client_min_messages = warning;
\if :probe_first
LOAD :'probe_so';
\endif
\if :probe_after
LOAD 'fasttrun';
\endif

CREATE TEMP TABLE t_hook_probe (id int, grp int, amount numeric(10,2), payload text);
\if :probe_first
CREATE FUNCTION pg_temp.fasttrun_hook_probe_counts()
RETURNS bigint[] AS :'probe_so', 'fasttrun_hook_probe_counts'
LANGUAGE C;
CREATE FUNCTION pg_temp.fasttrun_hook_probe_reset()
RETURNS void AS :'probe_so', 'fasttrun_hook_probe_reset'
LANGUAGE C;
\endif

INSERT INTO t_hook_probe
SELECT g, g % 1000, (g % 10000)::numeric / 100, md5(g::text)
FROM generate_series(1, 50000) g;
CREATE INDEX ON t_hook_probe (grp);
CREATE INDEX ON t_hook_probe (amount);
SELECT fasttrun_analyze('t_hook_probe');

\if :probe_after
LOAD :'probe_so';
CREATE FUNCTION pg_temp.fasttrun_hook_probe_counts()
RETURNS bigint[] AS :'probe_so', 'fasttrun_hook_probe_counts'
LANGUAGE C;
CREATE FUNCTION pg_temp.fasttrun_hook_probe_reset()
RETURNS void AS :'probe_so', 'fasttrun_hook_probe_reset'
LANGUAGE C;
\endif

SELECT pg_temp.fasttrun_hook_probe_reset();
DO $check$
DECLARE
  plan_line text;
  grp_rows int := NULL;
  range_rows int := NULL;
BEGIN
  FOR plan_line IN EXPLAIN SELECT * FROM t_hook_probe WHERE grp = 42 LOOP
    IF plan_line ~ 'rows=' THEN
      grp_rows := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  FOR plan_line IN EXPLAIN
      SELECT * FROM t_hook_probe WHERE amount BETWEEN 10 AND 20 LOOP
    IF plan_line ~ 'rows=' THEN
      range_rows := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF grp_rows IS NULL OR grp_rows < 10 OR grp_rows > 200 THEN
    RAISE EXCEPTION 'test hook grp estimate out of bounds: %', grp_rows;
  END IF;
  IF range_rows IS NULL OR range_rows < 2000 OR range_rows > 8000 THEN
    RAISE EXCEPTION 'test hook range estimate out of bounds: %', range_rows;
  END IF;

  /* A miss must chain through both stats hooks in either load order. */
  FOR plan_line IN EXPLAIN
      SELECT payload FROM hook_probe_perm WHERE grp = 42 ORDER BY payload LOOP
    NULL;
  END LOOP;
END
$check$;

SELECT count(*) > 0 FROM t_hook_probe WHERE grp = 42;
DROP TABLE t_hook_probe;
DO $check$
DECLARE counts bigint[];
BEGIN
  counts := pg_temp.fasttrun_hook_probe_counts();
  IF array_length(counts, 1) <> 5 OR
     counts[1] <= 0 OR counts[2] <= 0 OR counts[3] <= 0 OR
     counts[4] <= 0 OR counts[5] <= 0 THEN
    RAISE EXCEPTION 'test hook counters are incomplete: %', counts;
  END IF;
END
$check$;
SELECT 'SYNTHETIC_ORDER_OK|' || :'order' || '|' ||
       array_to_string(pg_temp.fasttrun_hook_probe_counts(), ',');
SQL
	then
		cat "$out" "$err" >&2 || true
		echo "synthetic hook order failed: $order" >&2
		exit 1
	fi
	grep -q "^SYNTHETIC_ORDER_OK|$order|" "$out" || {
		cat "$out" "$err" >&2
		echo "synthetic hook order emitted no success marker: $order" >&2
		exit 1
	}
	grep "^SYNTHETIC_ORDER_OK|$order|" "$out"
}

run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null

# First run the bundled module without optional third-party modules.
start_with_libs "" ""
run_pg "$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE EXTENSION fasttrun;
CREATE TABLE hook_probe_perm (id int PRIMARY KEY, grp int, payload text);
INSERT INTO hook_probe_perm
SELECT g, g % 1000, md5(g::text) FROM generate_series(1, 50000) g;
ANALYZE hook_probe_perm;
SQL
run_synthetic_order probe_before_fasttrun 1 0
run_synthetic_order probe_after_fasttrun 0 1
stop_cluster
echo "test hook module passed in both load orders"

# Installed third-party modules are checked when available.
accepted_shared=()
for lib in "${shared_candidates[@]}"; do
	if ! library_exists "$lib"; then
		echo "SKIP shared $lib: library not found"
		continue
	fi

	trial=("${accepted_shared[@]}" "$lib" fasttrun)
	trial_csv=$(join_by_comma "${trial[@]}")
	if start_with_libs "$trial_csv" ""; then
		echo "LOAD shared $lib"
		accepted_shared+=("$lib")
		stop_cluster
	else
		echo "SKIP shared $lib: cluster failed to start with this preload"
		stop_cluster
	fi
done

accepted_session=()
for lib in "${session_candidates[@]}"; do
	if library_exists "$lib"; then
		echo "LOAD session $lib"
		accepted_session+=("$lib")
	else
		echo "SKIP session $lib: library not found"
	fi
done

real_count=$(( ${#accepted_shared[@]} + ${#accepted_session[@]} ))
if [ "$real_count" -eq 0 ]; then
	echo "SKIP real companion: no candidate library is installed"
	exit 0
fi

final_shared=("${accepted_shared[@]}" fasttrun)
shared_csv=$(join_by_comma "${final_shared[@]}")
session_csv=$(join_by_comma "${accepted_session[@]}")
start_with_libs "$shared_csv" "$session_csv"

echo "FINAL shared_preload_libraries=$shared_csv"
echo "FINAL session_preload_libraries=${session_csv:-<empty>}"

for ext in "${accepted_shared[@]}" "${accepted_session[@]}"; do
	case "$ext" in
		citus_columnar|pg_query_state|pgpro_stats|pg_query_stack)
			echo "SKIP create extension $ext: preload-only or no public CREATE EXTENSION expectation"
			continue
			;;
	esac

	if ! extension_control_exists "$ext"; then
		echo "SKIP create extension $ext: control file not found"
		continue
	fi

	if run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS \"$ext\"" \
		>/dev/null 2>"$WORKDIR/create-$ext.err"; then
		echo "CREATE EXTENSION $ext"
	else
		echo "SKIP create extension $ext: $(tr '\n' ' ' <"$WORKDIR/create-$ext.err")"
	fi
done

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 <<'SQL'
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun;
SHOW shared_preload_libraries;
SHOW session_preload_libraries;
CREATE TEMP TABLE t_hook_chain (id int, grp int, amount numeric(10,2));
INSERT INTO t_hook_chain
SELECT g, g % 1000, (g % 10000)::numeric / 100
FROM generate_series(1, 50000) g;
CREATE INDEX ON t_hook_chain (grp);
CREATE INDEX ON t_hook_chain (amount);
SELECT fasttrun_analyze('t_hook_chain');
DO $check$
DECLARE
  plan_line text;
  grp_rows int := NULL;
  range_rows int := NULL;
BEGIN
  FOR plan_line IN EXPLAIN SELECT * FROM t_hook_chain WHERE grp = 42 LOOP
    IF plan_line ~ 'rows=' THEN
      grp_rows := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  FOR plan_line IN EXPLAIN
      SELECT * FROM t_hook_chain WHERE amount BETWEEN 10 AND 20 LOOP
    IF plan_line ~ 'rows=' THEN
      range_rows := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF grp_rows IS NULL OR grp_rows < 10 OR grp_rows > 200 THEN
    RAISE EXCEPTION 'real hook-chain grp estimate out of bounds: %', grp_rows;
  END IF;
  IF range_rows IS NULL OR range_rows < 2000 OR range_rows > 8000 THEN
    RAISE EXCEPTION 'real hook-chain range estimate out of bounds: %', range_rows;
  END IF;
END
$check$;
SQL

echo "real companion hook-chain smoke passed"
