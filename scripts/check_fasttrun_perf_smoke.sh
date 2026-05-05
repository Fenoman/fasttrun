#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PG_PKGLIBDIR=$("$PG_CONFIG" --pkglibdir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
BPFTRACE=${BPFTRACE:-bpftrace}
SUDO=${SUDO:-sudo}
PORT=${PGPORT:-55436}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-perf.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_perf}
TRACE_SECONDS=${TRACE_SECONDS:-6}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}

cleanup()
{
	if [ -n "${TRACE_PID:-}" ]; then
		kill "$TRACE_PID" >/dev/null 2>&1 || true
		wait "$TRACE_PID" >/dev/null 2>&1 || true
	fi
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
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

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
require_cmd "$BPFTRACE"

if [ ! -f "$FASTTRUN_SO" ]; then
	echo "missing fasttrun shared library: $FASTTRUN_SO" >&2
	exit 1
fi

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c shared_preload_libraries=fasttrun" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null

run_trace()
{
	local name=$1
	local program=$2
	local sqlfile=$3
	local outfile="$WORKDIR/$name.bpftrace.out"

	$SUDO timeout "$TRACE_SECONDS" "$BPFTRACE" -e "$program" \
		>"$outfile" 2>&1 &
	TRACE_PID=$!
	sleep 1
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -f "$sqlfile" >"$WORKDIR/$name.psql.out" \
		2>"$WORKDIR/$name.psql.err"
	wait "$TRACE_PID" >/dev/null 2>&1 || true
	TRACE_PID=""
	cat "$outfile"
}

map_count()
{
	local map_name=$1
	local file=$2

	awk -v name="$map_name" '$1 == name":" {gsub(/[^0-9]/, "", $2); print $2}' "$file" | tail -1
}

assert_count()
{
	local label=$1
	local actual=$2
	local op=$3
	local expected=$4

	actual=${actual:-0}
	case "$op" in
		eq)
			[ "$actual" -eq "$expected" ] || {
				echo "$label expected $expected, got $actual" >&2
				exit 1
			}
			;;
		gt)
			[ "$actual" -gt "$expected" ] || {
				echo "$label expected > $expected, got $actual" >&2
				exit 1
			}
			;;
		*)
			echo "unknown assert op: $op" >&2
			exit 1
			;;
	esac
}

cat >"$WORKDIR/no_fasttrun.sql" <<'SQL'
DO $$
BEGIN
  FOR i IN 1..1000 LOOP
    EXECUTE 'SELECT 1';
  END LOOP;
END$$;
SQL

run_trace fresh_no_stats "
uprobe:$FASTTRUN_SO:fasttrun_planner_hook { @planner = count(); }
END { print(@planner); }
" "$WORKDIR/no_fasttrun.sql" >"$WORKDIR/fresh_no_stats.trace"
assert_count "fresh backend planner hook" \
	"$(map_count @planner "$WORKDIR/fresh_no_stats.trace")" eq 0
echo "fresh_no_stats passed"

cat >"$WORKDIR/permanent_after_stats.sql" <<'SQL'
DROP TABLE IF EXISTS ft_perm_smoke;
CREATE TABLE ft_perm_smoke (a int, b text);
INSERT INTO ft_perm_smoke SELECT g, 'v' || g FROM generate_series(1, 10000) g;
ANALYZE ft_perm_smoke;
CREATE TEMP TABLE ft_temp_smoke (a int, b text);
INSERT INTO ft_temp_smoke SELECT g, 'v' || g FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('ft_temp_smoke');
DO $$
BEGIN
  FOR i IN 1..500 LOOP
    EXECUTE 'EXPLAIN SELECT * FROM ft_perm_smoke WHERE a = 42';
  END LOOP;
END$$;
DROP TABLE ft_perm_smoke;
SQL

run_trace permanent_after_stats "
uprobe:$FASTTRUN_SO:fasttrun_planner_hook { @planner = count(); }
uprobe:$FASTTRUN_SO:fasttrun_get_relation_stats_hook { @relstats = count(); }
uprobe:$FASTTRUN_SO:fasttrun_read_pgstat_counters_for_hook* { @pgstat_hook = count(); }
END { print(@planner); print(@relstats); print(@pgstat_hook); }
" "$WORKDIR/permanent_after_stats.sql" >"$WORKDIR/permanent_after_stats.trace"
assert_count "permanent-query planner hook" \
	"$(map_count @planner "$WORKDIR/permanent_after_stats.trace")" gt 0
assert_count "permanent-query relstats hook" \
	"$(map_count @relstats "$WORKDIR/permanent_after_stats.trace")" gt 0
assert_count "permanent-query pgstat freshness lookup" \
	"$(map_count @pgstat_hook "$WORKDIR/permanent_after_stats.trace")" eq 0
echo "permanent_after_stats passed"

cat >"$WORKDIR/temp_stats_hit.sql" <<'SQL'
CREATE TEMP TABLE ft_temp_hit (id int, amount numeric(10,2));
INSERT INTO ft_temp_hit
SELECT g, (g % 10000)::numeric / 100
FROM generate_series(1, 50000) g;
CREATE INDEX ON ft_temp_hit (amount);
SELECT fasttrun_analyze('ft_temp_hit');
DO $$
DECLARE
  plan_line text;
  rows_est int := NULL;
BEGIN
  FOR plan_line IN EXPLAIN SELECT * FROM ft_temp_hit WHERE amount BETWEEN 10 AND 20 LOOP
    IF plan_line ~ 'rows=' THEN
      rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF rows_est IS NULL OR rows_est < 2000 OR rows_est > 8000 THEN
    RAISE EXCEPTION 'temp stats estimate out of bounds: %', rows_est;
  END IF;
END$$;
SQL

run_trace temp_stats_hit "
uprobe:$FASTTRUN_SO:fasttrun_planner_hook { @planner = count(); }
uprobe:$FASTTRUN_SO:fasttrun_get_relation_stats_hook { @relstats = count(); }
uprobe:$FASTTRUN_SO:fasttrun_read_pgstat_counters_for_hook* { @pgstat_hook = count(); }
END { print(@planner); print(@relstats); print(@pgstat_hook); }
" "$WORKDIR/temp_stats_hit.sql" >"$WORKDIR/temp_stats_hit.trace"
assert_count "temp-query planner hook" \
	"$(map_count @planner "$WORKDIR/temp_stats_hit.trace")" gt 0
assert_count "temp-query relstats hook" \
	"$(map_count @relstats "$WORKDIR/temp_stats_hit.trace")" gt 0
assert_count "temp-query pgstat freshness lookup" \
	"$(map_count @pgstat_hook "$WORKDIR/temp_stats_hit.trace")" gt 0
echo "temp_stats_hit passed"

cat >"$WORKDIR/no_dml_analyze.sql" <<'SQL'
CREATE TEMP TABLE ft_noop (a int, b text);
INSERT INTO ft_noop
SELECT g, md5(g::text)
FROM generate_series(1, 10000) g;
BEGIN;
SELECT fasttrun_analyze('ft_noop');
SELECT pg_backend_pid();
SELECT 'READY_FOR_TRACE';
SELECT pg_sleep(2);
DO $$
BEGIN
  FOR i IN 1..200 LOOP
    PERFORM fasttrun_analyze('ft_noop');
  END LOOP;
END$$;
SELECT 'CASE_DONE';
SELECT pg_sleep(2);
COMMIT;
SQL

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/no_dml_analyze.sql" \
	>"$WORKDIR/no_dml_analyze.psql.out" \
	2>"$WORKDIR/no_dml_analyze.psql.err" &
psql_pid=$!
for _ in $(seq 1 100); do
	if grep -q '^READY_FOR_TRACE$' "$WORKDIR/no_dml_analyze.psql.out"; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^READY_FOR_TRACE$' "$WORKDIR/no_dml_analyze.psql.out"; then
	wait "$psql_pid" || true
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	echo "no_dml_analyze did not reach READY_FOR_TRACE" >&2
	exit 1
fi
backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/no_dml_analyze.psql.out" | head -1 || true)
if [ -z "$backend_pid" ]; then
	wait "$psql_pid" || true
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	echo "no_dml_analyze did not report backend pid" >&2
	exit 1
fi
$SUDO timeout "$TRACE_SECONDS" "$BPFTRACE" -e "
uprobe:$FASTTRUN_SO:fasttrun_analyze /pid == $backend_pid/ { @in_fasttrun[tid] = 1; }
uretprobe:$FASTTRUN_SO:fasttrun_analyze /pid == $backend_pid/ { delete(@in_fasttrun[tid]); }
uprobe:$("$PG_CONFIG" --bindir)/postgres:RelationGetNumberOfBlocksInFork /pid == $backend_pid && @in_fasttrun[tid]/ { @nblocks = count(); }
uprobe:$("$PG_CONFIG" --bindir)/postgres:smgrnblocks /pid == $backend_pid && @in_fasttrun[tid]/ { @smgr = count(); }
END { print(@nblocks); print(@smgr); }
" >"$WORKDIR/no_dml_analyze.trace" 2>&1 &
TRACE_PID=$!
for _ in $(seq 1 200); do
	if grep -q '^CASE_DONE$' "$WORKDIR/no_dml_analyze.psql.out"; then
		break
	fi
	if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.05
done
wait "$psql_pid"
kill "$TRACE_PID" >/dev/null 2>&1 || true
wait "$TRACE_PID" >/dev/null 2>&1 || true
TRACE_PID=""
assert_count "no-DML RelationGetNumberOfBlocksInFork" \
	"$(map_count @nblocks "$WORKDIR/no_dml_analyze.trace")" eq 0
assert_count "no-DML smgrnblocks" \
	"$(map_count @smgr "$WORKDIR/no_dml_analyze.trace")" eq 0
echo "no_dml_analyze passed"

echo "fasttrun perf smoke passed"
