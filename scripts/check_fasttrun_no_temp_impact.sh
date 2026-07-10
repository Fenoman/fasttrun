#!/usr/bin/env bash
#
# Check the same permanent-table query in three fasttrun states:
#   1. local statistics caches were never created;
#   2. an unrelated temporary table has neutral local statistics;
#   3. the last managed temporary table was dropped and its caches released.
#
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
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-no-temp.XXXXXX)}
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_no_temp}
EXECUTIONS=${EXECUTIONS:-10000}
PLAN_REPETITIONS=${PLAN_REPETITIONS:-7}
PLAN_QUERIES=${PLAN_QUERIES:-1000}
KEEP_WORKDIR=${KEEP_WORKDIR:-0}
BASELINE=${FASTTRUN_NO_TEMP_BASELINE_JSON:-}

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
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop \
			>/dev/null 2>&1 || true
	fi
	if [ "$KEEP_WORKDIR" -eq 0 ]; then
		rm -rf "$WORKDIR"
	else
		echo "no-temp test files kept in $WORKDIR" >&2
	fi
}
trap cleanup EXIT

require_cmd()
{
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing command: $1" >&2
		exit 1
	fi
}

require_positive_int()
{
	case "$2" in
		''|*[!0-9]*|0) echo "$1 must be a positive integer: $2" >&2; exit 1 ;;
	esac
}

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
require_cmd "$PYTHON"
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi
require_positive_int EXECUTIONS "$EXECUTIONS"
require_positive_int PLAN_REPETITIONS "$PLAN_REPETITIONS"
require_positive_int PLAN_QUERIES "$PLAN_QUERIES"
if [ -n "$BASELINE" ] && [ ! -f "$BASELINE" ]; then
	echo "baseline JSON does not exist: $BASELINE" >&2
	exit 1
fi

mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR"
fi
PORT=${PGPORT:-$($PYTHON -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on -c log_line_prefix='%m [%p] '" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE EXTENSION fasttrun;
CREATE TABLE perm_no_temp (id int PRIMARY KEY, grp int, payload text);
INSERT INTO perm_no_temp
SELECT g, g % 100, md5(g::text) FROM generate_series(1,100000) g;
ANALYZE perm_no_temp;

CREATE FUNCTION public.fasttrun_no_temp_marker_begin(marker text)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE LOG 'FASTTRUN_NO_TEMP_BEGIN_%', marker;
END
$function$;

CREATE FUNCTION public.fasttrun_no_temp_marker_end(marker text)
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE LOG 'FASTTRUN_NO_TEMP_END_%', marker;
END
$function$;
SQL

run_state()
{
	local state=$1
	local make_neutral=$2
	local drop_neutral=$3
	local out=$WORKDIR/$state.out
	local err=$WORKDIR/$state.err
	local plan_file=$WORKDIR/$state.plan.json
	local result_file=$WORKDIR/$state.result.json

	echo "measuring no-temp state: $state" >&2
	if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
		-XAtq -v ON_ERROR_STOP=1 \
		-v state="$state" \
		-v make_neutral="$make_neutral" \
		-v drop_neutral="$drop_neutral" \
		-v executions="$EXECUTIONS" \
		-v plan_repetitions="$PLAN_REPETITIONS" \
		-v plan_queries="$PLAN_QUERIES" \
		-v plan_file="$plan_file" \
		-v result_file="$result_file" >"$out" 2>"$err" <<'SQL'
SELECT 'BACKEND_PID|' || pg_backend_pid();
SET plan_cache_mode = force_generic_plan;

\if :make_neutral
CREATE TEMP TABLE ft_no_temp_state (id int, grp int, payload text);
INSERT INTO ft_no_temp_state
SELECT g, g % 50, md5(g::text) FROM generate_series(1,5000) g;
ANALYZE ft_no_temp_state;
DO $check$
BEGIN
  IF (SELECT count(*) FROM pg_statistic
      WHERE starelid = 'ft_no_temp_state'::regclass) = 0 THEN
    RAISE EXCEPTION 'core ANALYZE did not create pg_statistic rows';
  END IF;
END
$check$;
\endif

PREPARE q_perm(int) AS
SELECT count(*), min(payload), max(payload)
FROM perm_no_temp
WHERE grp = $1;
\o /dev/null
EXECUTE q_perm(42);
\o

\if :make_neutral
SELECT fasttruncate('ft_no_temp_state');
\endif

\if :drop_neutral
DROP TABLE ft_no_temp_state;
DO $check$
BEGIN
  IF to_regclass('pg_temp.ft_no_temp_state') IS NOT NULL THEN
    RAISE EXCEPTION 'committed DROP left the temp relation visible';
  END IF;
  IF EXISTS (SELECT FROM pg_backend_memory_contexts
             WHERE name LIKE 'fasttrun%') THEN
    RAISE EXCEPTION 'committed DROP did not remove the last fasttrun context';
  END IF;
END
$check$;
\else
\if :make_neutral
DO $check$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_backend_memory_contexts
                 WHERE name = 'fasttrun stats cache') THEN
    RAISE EXCEPTION 'REL_LOCAL_NEUTRAL state has no stats cache';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_backend_memory_contexts
                 WHERE name = 'fasttrun analyze cache') THEN
    RAISE EXCEPTION 'neutralized relation has no relstats cache';
  END IF;
  IF (SELECT reltuples FROM fasttrun_relstats('ft_no_temp_state')) <> 0 THEN
    RAISE EXCEPTION 'fasttruncate did not publish empty relstats';
  END IF;
END
$check$;
\else
DO $check$
BEGIN
  IF EXISTS (SELECT FROM pg_backend_memory_contexts
             WHERE name LIKE 'fasttrun%') THEN
    RAISE EXCEPTION 'never-initialized state already has fasttrun contexts';
  END IF;
END
$check$;
\endif
\endif

SET log_planner_stats = on;
SELECT public.fasttrun_no_temp_marker_begin(:'state');
EXECUTE q_perm(42);
SET log_planner_stats = off;
SELECT public.fasttrun_no_temp_marker_end(:'state');

\o :plan_file
EXPLAIN (FORMAT JSON, COSTS OFF, SUMMARY OFF) EXECUTE q_perm(42);
\o
\o :result_file
SELECT json_build_array(count(*), min(payload), max(payload))::text
FROM perm_no_temp
WHERE grp = 42;
\o

SELECT format('MEM_WARM|%s|%s|%s',
              coalesce(sum(total_bytes), 0),
              coalesce(sum(used_bytes), 0),
              count(*))
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

\o /dev/null
SELECT 'EXECUTE q_perm(42);' FROM generate_series(1, :executions) \gexec
\o

SELECT format('MEM_FINAL|%s|%s|%s',
              coalesce(sum(total_bytes), 0),
              coalesce(sum(used_bytes), 0),
              count(*))
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

\o /dev/null
SELECT format($command$
DO $bench$
DECLARE i int; started timestamptz; elapsed_ms numeric;
BEGIN
  started := clock_timestamp();
  FOR i IN 1..%s LOOP
    EXECUTE format('EXPLAIN (COSTS OFF) SELECT * FROM perm_no_temp WHERE id = %%s', i);
  END LOOP;
  elapsed_ms := extract(epoch FROM clock_timestamp() - started) * 1000;
  RAISE NOTICE 'NO_TEMP_PLAN_MS=%%', round(elapsed_ms, 3);
END
$bench$;
$command$, :plan_queries)
FROM generate_series(1, :plan_repetitions) \gexec
\o
SQL
	then
		cat "$out" "$err" >&2 || true
		echo "state $state failed" >&2
		exit 1
	fi
}

run_state never_initialized 0 0
run_state neutral_live 1 0
run_state dropped_reset 1 1

PG_VERSION=$($PG_CONFIG --version)
"$PYTHON" - "$WORKDIR" "$LOG" "$PG_VERSION" "$EXECUTIONS" \
	"$PLAN_REPETITIONS" "$PLAN_QUERIES" "$BASELINE" <<'PY'
import hashlib
import json
import re
import statistics
import sys
from pathlib import Path

workdir = Path(sys.argv[1])
log_path = Path(sys.argv[2])
pg_version = sys.argv[3]
executions = int(sys.argv[4])
plan_repetitions = int(sys.argv[5])
plan_queries = int(sys.argv[6])
baseline_path = sys.argv[7]
states = ("never_initialized", "neutral_live", "dropped_reset")
failures = []
log_lines = log_path.read_text(errors="replace").splitlines()


def canonical_hash(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def read_memory(text, label, state):
    match = re.search(rf"^MEM_{label}\|(\d+)\|(\d+)\|(\d+)$", text, re.M)
    if not match:
        failures.append(f"{state}: missing MEM_{label}")
        return {"total_bytes": 0, "used_bytes": 0, "contexts": -1}
    return {
        "total_bytes": int(match.group(1)),
        "used_bytes": int(match.group(2)),
        "contexts": int(match.group(3)),
    }


def planner_blocks(state):
    begin_token = f"FASTTRUN_NO_TEMP_BEGIN_{state}"
    end_token = f"FASTTRUN_NO_TEMP_END_{state}"
    begins = [i for i, line in enumerate(log_lines) if begin_token in line]
    ends = [i for i, line in enumerate(log_lines) if end_token in line]
    if len(begins) != 1 or len(ends) != 1 or begins[0] >= ends[0]:
        failures.append(
            f"{state}: invalid planner marker counts begin={len(begins)} end={len(ends)}"
        )
        return -1
    return sum("PLANNER STATISTICS" in line
               for line in log_lines[begins[0] + 1:ends[0]])


report = {
    "postgres_version": pg_version,
    "executions": executions,
    "planning_queries_per_sample": plan_queries,
    "planning_repetitions": plan_repetitions,
    "states": {},
}

for state in states:
    stdout = (workdir / f"{state}.out").read_text(errors="replace")
    stderr = (workdir / f"{state}.err").read_text(errors="replace")
    try:
        plan_value = json.loads((workdir / f"{state}.plan.json").read_text())
    except Exception as exc:
        failures.append(f"{state}: invalid plan JSON: {exc}")
        plan_value = None
    try:
        result_value = json.loads((workdir / f"{state}.result.json").read_text())
    except Exception as exc:
        failures.append(f"{state}: invalid result JSON: {exc}")
        result_value = None

    samples = [float(value) for value in
               re.findall(r"NO_TEMP_PLAN_MS=([0-9]+(?:\.[0-9]+)?)", stderr)]
    if len(samples) != plan_repetitions:
        failures.append(
            f"{state}: expected {plan_repetitions} planning samples, got {len(samples)}"
        )
    median_ms = statistics.median(samples) if samples else 0.0
    warm = read_memory(stdout, "WARM", state)
    final = read_memory(stdout, "FINAL", state)
    used_growth = final["used_bytes"] - warm["used_bytes"]
    if used_growth > 65536:
        failures.append(f"{state}: fasttrun used memory grew by {used_growth} bytes")
    if final["contexts"] != warm["contexts"]:
        failures.append(
            f"{state}: fasttrun context count changed "
            f"{warm['contexts']} -> {final['contexts']}"
        )

    blocks = planner_blocks(state)
    if blocks != 0:
        failures.append(f"{state}: warmed permanent plan rebuilt {blocks} time(s)")
    report["states"][state] = {
        "plan_sha256": canonical_hash(plan_value),
        "result_sha256": canonical_hash(result_value),
        "planner_statistics_blocks": blocks,
        "memory_warm": warm,
        "memory_final": final,
        "memory_used_growth": used_growth,
        "planning_ms": samples,
        "planning_median_ms": median_ms,
    }

plan_hashes = {entry["plan_sha256"] for entry in report["states"].values()}
result_hashes = {entry["result_sha256"] for entry in report["states"].values()}
if len(plan_hashes) != 1:
    failures.append(f"plan hashes differ across states: {sorted(plan_hashes)}")
if len(result_hashes) != 1:
    failures.append(f"result hashes differ across states: {sorted(result_hashes)}")

if baseline_path:
    baseline = json.loads(Path(baseline_path).read_text())
    comparison = {}
    expected_config = {
        "executions": executions,
        "planning_queries_per_sample": plan_queries,
        "planning_repetitions": plan_repetitions,
    }
    for key, expected in expected_config.items():
        if baseline.get(key) != expected:
            failures.append(
                f"baseline {key}={baseline.get(key)!r}, expected {expected!r}"
            )
    for state in states:
        if state not in baseline.get("states", {}):
            failures.append(f"baseline is missing state {state}")
            continue
        base_entry = baseline["states"][state]
        base_samples = [float(value) for value in base_entry["planning_ms"]]
        base_median = statistics.median(base_samples)
        mad = statistics.median(abs(value - base_median) for value in base_samples)
        noise_ms = mad * 3.0
        threshold = max(base_median * 1.05, base_median + noise_ms)
        current_median = report["states"][state]["planning_median_ms"]
        comparison[state] = {
            "baseline_median_ms": base_median,
            "baseline_mad_ms": mad,
            "measured_noise_ms": noise_ms,
            "allowed_median_ms": threshold,
            "current_median_ms": current_median,
        }
        if current_median > threshold:
            failures.append(
                f"{state}: planning median {current_median:.3f} ms exceeds "
                f"{threshold:.3f} ms"
            )
    report["baseline_comparison"] = comparison

report["status"] = "PASS" if not failures else "FAIL"
report["failures"] = failures
json.dump(report, sys.stdout, sort_keys=True, indent=2)
sys.stdout.write("\n")
if failures:
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    raise SystemExit(1)
PY
