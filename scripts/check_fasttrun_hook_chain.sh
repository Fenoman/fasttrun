#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PG_PKGLIBDIR=$("$PG_CONFIG" --pkglibdir)
PG_SHAREDIR=$("$PG_CONFIG" --sharedir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PORT=${PGPORT:-55437}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-hook-chain.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_hook_chain}

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

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

for cmd in "$PSQL" "$INITDB" "$PG_CTL" "$CREATEDB"; do
	if ! command -v "$cmd" >/dev/null 2>&1; then
		echo "missing command: $cmd" >&2
		exit 1
	fi
done

join_by_comma()
{
	local IFS=,
	echo "$*"
}

library_exists()
{
	local lib=$1
	[ -f "$PG_PKGLIBDIR/$lib.so" ]
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
	local opts="-k $WORKDIR -p $PORT"

	if [ -n "$shared_csv" ]; then
		opts="$opts -c shared_preload_libraries=$shared_csv"
	fi
	if [ -n "$session_csv" ]; then
		opts="$opts -c session_preload_libraries=$session_csv"
	fi

	"$PG_CTL" -D "$WORKDIR/data" -o "$opts" \
		-l "$WORKDIR/postgres.log" -w start >/dev/null 2>&1
}

stop_cluster()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
}

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null

accepted_shared=()
for lib in "${shared_candidates[@]}"; do
	if ! library_exists "$lib"; then
		echo "SKIP shared $lib: $PG_PKGLIBDIR/$lib.so not found"
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
		echo "SKIP session $lib: $PG_PKGLIBDIR/$lib.so not found"
	fi
done

final_shared=("${accepted_shared[@]}" fasttrun)
shared_csv=$(join_by_comma "${final_shared[@]}")
session_csv=$(join_by_comma "${accepted_session[@]}")

start_with_libs "$shared_csv" "$session_csv"
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"

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

	if "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS \"$ext\"" \
		>/dev/null 2>"$WORKDIR/create-$ext.err"; then
		echo "CREATE EXTENSION $ext"
	else
		echo "SKIP create extension $ext: $(tr '\n' ' ' <"$WORKDIR/create-$ext.err")"
	fi
done

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
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
DO $$
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
  FOR plan_line IN EXPLAIN SELECT * FROM t_hook_chain WHERE amount BETWEEN 10 AND 20 LOOP
    IF plan_line ~ 'rows=' THEN
      range_rows := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF grp_rows IS NULL OR grp_rows < 10 OR grp_rows > 200 THEN
    RAISE EXCEPTION 'hook-chain grp estimate out of bounds: %', grp_rows;
  END IF;
  IF range_rows IS NULL OR range_rows < 2000 OR range_rows > 8000 THEN
    RAISE EXCEPTION 'hook-chain numeric range estimate out of bounds: %', range_rows;
  END IF;
END$$;
SQL

echo "prod-like hook-chain smoke passed"
