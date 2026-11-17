#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PG_PKGLIBDIR=$("$PG_CONFIG" --pkglibdir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PYTHON=${PYTHON:-python3}
BPFTRACE=${BPFTRACE:-bpftrace}
SUDO=${SUDO-sudo}
PG_RUN_AS=${PG_RUN_AS:-}
PORT=${PGPORT:-55436}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-perf.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_perf}
TRACE_SECONDS=${TRACE_SECONDS:-60}
KEEP_WORKDIR=${KEEP_WORKDIR:-0}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}
# Пределы времени fasttruncate, мс. Их два, потому что замеров два и стоят
# они разного.
#
# MAX_TRUNC_MS -- для узкой таблицы: 1 млн строк, две колонки, один индекс.
# Под эту нагрузку предел и калибровался. Он тесный, но переносимый: на
# медленном виртуальном сервере узкая таблица очищается за 32 мс, то есть
# запас трехкратный, и настоящее замедление он поймает где угодно.
#
# MAX_TRUNC_WIDE_MS -- для широкой: 1 млн строк, 50 колонок, четыре индекса,
# TOAST, семь файлов отношения, 637 МБ. Этот замер появился позже и унаследовал
# чужой предел в 100 мс, из-за чего на обычном сервере показывал отказ там, где
# отказа нет: та же машина дает по узкой таблице 32 мс, а по широкой 194.
# Значение 500 мс взято по образцу мягкого предела в pg_regress, поднятого
# ровно из-за медленных машин. Оно предварительное: его стоит пересчитать по
# замеру на эталонной машине, и скрипт для этого печатает обе медианы.
#
# В pg_regress оставлен мягкий предел 500 мс, чтобы тест не зависел от скорости
# машины.
MAX_TRUNC_MS=${MAX_TRUNC_MS:-100}
MAX_TRUNC_WIDE_MS=${MAX_TRUNC_WIDE_MS:-500}
# MAX_REFRESH_MS -- предел пересбора статистики колонок fasttrun_analyze после
# DELETE и после UPDATE 500 тыс. строк таблицы 1 млн строк x 50 колонок, шаги
# sql/fasttrun_bench.sql (с выборкой sample_rows=10000 и выборкой блоков при
# известном исходном числе строк выше порога цели выборки). Бенч идет
# REFRESH_RUNS раз: с медианой каждого шага сравнивается предел, с каждым
# отдельным замером - удвоенный предел. В самом бенче для этих шагов стоит
# мягкий порог 2000 мс.
MAX_REFRESH_MS=${MAX_REFRESH_MS:-1000}
REFRESH_RUNS=${REFRESH_RUNS:-5}
BENCH_SQL=${BENCH_SQL:-"$(cd "$(dirname "$0")" && pwd)/../sql/fasttrun_bench.sql"}

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

terminate_child()
{
	local pid=${1:-}
	local sig=${2:--TERM}
	local as_root=${3:-0}
	local i
	local -a kcmd=(kill)
	[ -n "$pid" ] || return 0
	if [ "$as_root" -eq 1 ] && [ -n "$SUDO" ]; then
		# shellcheck disable=SC2206
		kcmd=($SUDO kill)
	fi
	"${kcmd[@]}" "$sig" "$pid" >/dev/null 2>&1 || true
	"${kcmd[@]}" -CONT "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! "${kcmd[@]}" -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	"${kcmd[@]}" -KILL "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! "${kcmd[@]}" -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	return 0
}

wait_bounded()
{
	local pid=$1
	local ticks=${2:-}
	local as_root=${3:-0}
	local i
	local rc=0
	local -a kcmd=(kill)
	[ -n "$ticks" ] || ticks=${FASTTRUN_WAIT_TICKS:-200}
	if [ "$as_root" -eq 1 ] && [ -n "$SUDO" ]; then
		# shellcheck disable=SC2206
		kcmd=($SUDO kill)
	fi
	for i in $(seq 1 "$ticks"); do
		if ! "${kcmd[@]}" -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" || rc=$?
			return "$rc"
		fi
		sleep 0.05
	done
	terminate_child "$pid" -TERM "$as_root"
	return 124
}

cleanup()
{
	if [ -n "${TRACE_PID:-}" ]; then
		terminate_child "$TRACE_PID" -INT 1
	fi
	if [ -n "${psql_pid:-}" ]; then
		terminate_child "$psql_pid"
	fi
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop >/dev/null 2>&1 \
			|| run_pg "$PG_CTL" -D "$WORKDIR/data" -m immediate -w stop >/dev/null 2>&1 \
			|| true
	fi
	if [ "$KEEP_WORKDIR" -eq 0 ]; then
		rm -rf "$WORKDIR"
	else
		echo "performance test files kept in $WORKDIR" >&2
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

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
require_cmd "$PYTHON"
require_cmd "$BPFTRACE"
require_cmd stdbuf
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi

if [ ! -f "$FASTTRUN_SO" ]; then
	echo "missing fasttrun shared library: $FASTTRUN_SO" >&2
	exit 1
fi

if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR"
fi
run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	'DROP TABLE IF EXISTS ft_trace_gate; CREATE TABLE ft_trace_gate (go boolean NOT NULL); INSERT INTO ft_trace_gate VALUES (false)' \
	>/dev/null

run_trace()
{
	local name=$1
	local program_tpl=$2
	local setup_sql=$3
	local workload_sql=$4
	local workload_map=$5
	local min_workload=${6:-1}
	local outfile="$WORKDIR/$name.bpftrace.out"
	local fullsql="$WORKDIR/$name.full.sql"
	local psql_pid=""
	local backend_pid=""
	local program=""
	local trace_rc=0
	local workload_count

	run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -c 'UPDATE ft_trace_gate SET go = false' >/dev/null

	cat >"$fullsql" <<SQL
$setup_sql
SELECT pg_backend_pid();
SELECT 'READY_FOR_TRACE';
DO \$gate\$
BEGIN
  WHILE NOT (SELECT go FROM ft_trace_gate) LOOP
    PERFORM pg_sleep(0.05);
  END LOOP;
END
\$gate\$;
$workload_sql
SELECT pg_backend_pid() \g /dev/null
SELECT 'CASE_DONE';
SQL

	run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -f "$fullsql" >"$WORKDIR/$name.psql.out" \
		2>"$WORKDIR/$name.psql.err" &
	psql_pid=$!

	for _ in $(seq 1 400); do
		if grep -q '^READY_FOR_TRACE$' "$WORKDIR/$name.psql.out" 2>/dev/null; then
			break
		fi
		if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.05
	done
	if ! grep -q '^READY_FOR_TRACE$' "$WORKDIR/$name.psql.out" 2>/dev/null; then
		terminate_child "$psql_pid"
		cat "$WORKDIR/$name.psql.out" "$WORKDIR/$name.psql.err" >&2
		echo "$name did not reach READY_FOR_TRACE" >&2
		exit 1
	fi
	backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/$name.psql.out" | head -1 || true)
	if [ -z "$backend_pid" ]; then
		terminate_child "$psql_pid"
		cat "$WORKDIR/$name.psql.out" "$WORKDIR/$name.psql.err" >&2
		echo "$name did not report backend pid" >&2
		exit 1
	fi

	program=${program_tpl//__PID__/$backend_pid}
	if [[ "$program" == *"END {"* ]]; then
		program=${program/END \{/END \{ print(@end_sentinel);}
	else
		program="$program"$'\n'"END { print(@end_sentinel); printf(\"BPFTRACE_END\\n\"); }"
	fi
	$SUDO stdbuf -oL "$BPFTRACE" -e "
BEGIN { printf(\"ATTACHED_READY\\n\"); }
uprobe:$PG_BINDIR/postgres:pg_backend_pid /pid == $backend_pid/ { @end_sentinel = count(); }
$program" \
		>"$outfile" 2>&1 &
	TRACE_PID=$!
	wait_trace_ready "$outfile" "$TRACE_PID" || {
		cat "$outfile" >&2
		echo "$name bpftrace did not attach" >&2
		exit 1
	}

	run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -c 'UPDATE ft_trace_gate SET go = true' >/dev/null

	for _ in $(seq 1 1200); do
		if grep -q '^CASE_DONE$' "$WORKDIR/$name.psql.out" 2>/dev/null; then
			break
		fi
		if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.05
	done
	if ! grep -q '^CASE_DONE$' "$WORKDIR/$name.psql.out" 2>/dev/null; then
		terminate_child "$psql_pid"
		cat "$WORKDIR/$name.psql.out" "$WORKDIR/$name.psql.err" >&2
		echo "$name did not reach CASE_DONE" >&2
		exit 1
	fi
	if ! wait_bounded "$psql_pid"; then
		cat "$WORKDIR/$name.psql.out" "$WORKDIR/$name.psql.err" >&2
		exit 1
	fi

	$SUDO kill -INT "$TRACE_PID" >/dev/null 2>&1 || true
	wait_bounded "$TRACE_PID" "" 1 || trace_rc=$?
	TRACE_PID=""
	if [ "$trace_rc" -ne 0 ] && [ "$trace_rc" -ne 130 ]; then
		cat "$outfile" >&2
		echo "$name bpftrace exited with status $trace_rc" >&2
		exit 1
	fi
	if ! grep -q '^BPFTRACE_END$' "$outfile"; then
		cat "$outfile" >&2
		echo "$name bpftrace did not emit BPFTRACE_END" >&2
		exit 1
	fi
	workload_count=$(map_count "$workload_map" "$outfile")
	workload_count=${workload_count:-0}
	end_sentinel=$(map_count "@end_sentinel" "$outfile")
	end_sentinel=${end_sentinel:-0}
	if [ "$end_sentinel" -lt 1 ]; then
		cat "$outfile" >&2
		echo "$name bpftrace did not observe post-workload @end_sentinel" >&2
		exit 1
	fi
	if [ "$workload_count" -lt "$min_workload" ]; then
		cat "$outfile" >&2
		echo "$name expected $workload_map >= $min_workload, got $workload_count" >&2
		exit 1
	fi
	cat "$outfile"
}

wait_trace_ready()
{
	local outfile=$1
	local trace_pid=$2
	local i
	local tries

	# Метку ATTACHED_READY ждем TRACE_SECONDS, но не меньше 10 с: на машине,
	# где зонды присоединяются долго, поднимают TRACE_SECONDS, и ожидание
	# растет вместе с ним.
	tries=$(( TRACE_SECONDS * 20 ))
	[ "$tries" -lt 200 ] && tries=200

	for i in $(seq 1 "$tries"); do
		if grep -q '^ATTACHED_READY$' "$outfile" 2>/dev/null; then
			# Метка напечатана самим bpftrace: зонды уже присоединены.
			# Непривилегированный kill -0 по процессу под sudo дал бы EPERM,
			# поэтому достаточным признаком готовности служит сама метка.
			return 0
		fi
		sleep 0.05
	done
	return 1
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

no_fasttrun_workload=$(cat <<'SQL'
DO $$
BEGIN
  FOR i IN 1..1000 LOOP
    EXECUTE 'SELECT 1';
  END LOOP;
END$$;
SQL
)

run_trace fresh_no_stats "
uprobe:$PG_BINDIR/postgres:standard_planner /pid == __PID__/ { @workload = count(); }
uprobe:$FASTTRUN_SO:fasttrun_planner_hook /pid == __PID__/ { @planner = count(); }
END { print(@workload); print(@planner); printf(\"BPFTRACE_END\\n\"); }
" "" "$no_fasttrun_workload" @workload 1000 >"$WORKDIR/fresh_no_stats.trace"
assert_count "fresh backend planner hook" \
	"$(map_count @planner "$WORKDIR/fresh_no_stats.trace")" eq 0
echo "fresh_no_stats passed"

perm_after_setup=$(cat <<'SQL'
DROP TABLE IF EXISTS ft_perm_smoke;
CREATE TABLE ft_perm_smoke (a int, b text);
INSERT INTO ft_perm_smoke SELECT g, 'v' || g FROM generate_series(1, 10000) g;
ANALYZE ft_perm_smoke;
CREATE TEMP TABLE ft_temp_smoke (a int, b text);
INSERT INTO ft_temp_smoke SELECT g, 'v' || g FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('ft_temp_smoke');
SQL
)
perm_after_workload=$(cat <<'SQL'
DO $$
BEGIN
  FOR i IN 1..500 LOOP
    EXECUTE 'EXPLAIN SELECT * FROM ft_perm_smoke WHERE a = 42';
  END LOOP;
END$$;
DROP TABLE ft_perm_smoke;
SQL
)

run_trace permanent_after_stats "
uprobe:$PG_BINDIR/postgres:standard_planner /pid == __PID__/ { @workload = count(); }
uprobe:$FASTTRUN_SO:fasttrun_planner_hook /pid == __PID__/ { @planner = count(); }
uprobe:$FASTTRUN_SO:fasttrun_get_relation_stats_hook /pid == __PID__/ { @relstats = count(); }
uprobe:$FASTTRUN_SO:fasttrun_read_pgstat_counters_for_hook* /pid == __PID__/ { @pgstat_hook = count(); }
END { print(@workload); print(@planner); print(@relstats); print(@pgstat_hook); printf(\"BPFTRACE_END\\n\"); }
" "$perm_after_setup" "$perm_after_workload" @workload 500 >"$WORKDIR/permanent_after_stats.trace"
assert_count "permanent-query planner hook" \
	"$(map_count @planner "$WORKDIR/permanent_after_stats.trace")" gt 499
assert_count "permanent-query relstats hook" \
	"$(map_count @relstats "$WORKDIR/permanent_after_stats.trace")" gt 499
assert_count "permanent-query pgstat freshness lookup" \
	"$(map_count @pgstat_hook "$WORKDIR/permanent_after_stats.trace")" eq 0
echo "permanent_after_stats passed"

temp_hit_setup=$(cat <<'SQL'
CREATE TEMP TABLE ft_temp_hit (id int, amount numeric(10,2));
INSERT INTO ft_temp_hit
SELECT g, (g % 10000)::numeric / 100
FROM generate_series(1, 50000) g;
CREATE INDEX ON ft_temp_hit (amount);
SELECT fasttrun_analyze('ft_temp_hit');
SQL
)
temp_hit_workload=$(cat <<'SQL'
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
)

run_trace temp_stats_hit "
uprobe:$PG_BINDIR/postgres:standard_planner /pid == __PID__/ { @workload = count(); }
uprobe:$FASTTRUN_SO:fasttrun_planner_hook /pid == __PID__/ { @planner = count(); }
uprobe:$FASTTRUN_SO:fasttrun_get_relation_stats_hook /pid == __PID__/ { @relstats = count(); }
uprobe:$FASTTRUN_SO:fasttrun_read_pgstat_counters_for_hook* /pid == __PID__/ { @pgstat_hook = count(); }
END { print(@workload); print(@planner); print(@relstats); print(@pgstat_hook); printf(\"BPFTRACE_END\\n\"); }
" "$temp_hit_setup" "$temp_hit_workload" @workload 1 >"$WORKDIR/temp_stats_hit.trace"
assert_count "temp-query planner hook" \
	"$(map_count @planner "$WORKDIR/temp_stats_hit.trace")" gt 0
assert_count "temp-query relstats hook" \
	"$(map_count @relstats "$WORKDIR/temp_stats_hit.trace")" gt 0
assert_count "temp-query pgstat freshness lookup" \
	"$(map_count @pgstat_hook "$WORKDIR/temp_stats_hit.trace")" gt 0
echo "temp_stats_hit passed"

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	'UPDATE ft_trace_gate SET go = false' \
	>/dev/null

cat >"$WORKDIR/no_dml_analyze.sql" <<'SQL'
CREATE TEMP TABLE ft_noop (a int, b text);
INSERT INTO ft_noop
SELECT g, md5(g::text)
FROM generate_series(1, 10000) g;
BEGIN;
SELECT fasttrun_analyze('ft_noop');
SELECT pg_backend_pid();
SELECT 'READY_FOR_TRACE';
DO $gate$
BEGIN
  WHILE NOT (SELECT go FROM ft_trace_gate) LOOP
    PERFORM pg_sleep(0.05);
  END LOOP;
END
$gate$;
DO $$
BEGIN
  FOR i IN 1..200 LOOP
    PERFORM fasttrun_analyze('ft_noop');
  END LOOP;
END$$;
SELECT pg_backend_pid() \g /dev/null
SELECT 'CASE_DONE';
SELECT pg_sleep(2);
COMMIT;
SQL

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
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
	terminate_child "$psql_pid"
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	echo "no_dml_analyze did not reach READY_FOR_TRACE" >&2
	exit 1
fi
backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/no_dml_analyze.psql.out" | head -1 || true)
if [ -z "$backend_pid" ]; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	echo "no_dml_analyze did not report backend pid" >&2
	exit 1
fi
$SUDO stdbuf -oL "$BPFTRACE" -e "
BEGIN { printf(\"ATTACHED_READY\\n\"); }
uprobe:$FASTTRUN_SO:fasttrun_analyze /pid == $backend_pid/ { @workload = count(); @in_fasttrun[tid] = 1; }
uretprobe:$FASTTRUN_SO:fasttrun_analyze /pid == $backend_pid/ { delete(@in_fasttrun[tid]); }
uprobe:$("$PG_CONFIG" --bindir)/postgres:RelationGetNumberOfBlocksInFork /pid == $backend_pid && @in_fasttrun[tid]/ { @nblocks = count(); }
uprobe:$("$PG_CONFIG" --bindir)/postgres:smgrnblocks /pid == $backend_pid && @in_fasttrun[tid]/ { @smgr = count(); }
uprobe:$("$PG_CONFIG" --bindir)/postgres:pg_backend_pid /pid == $backend_pid/ { @end_sentinel = count(); }
END { print(@workload); print(@nblocks); print(@smgr); print(@end_sentinel); printf(\"BPFTRACE_END\\n\"); }
" >"$WORKDIR/no_dml_analyze.trace" 2>&1 &
TRACE_PID=$!
wait_trace_ready "$WORKDIR/no_dml_analyze.trace" "$TRACE_PID" || {
	cat "$WORKDIR/no_dml_analyze.trace" >&2
	echo "no_dml_analyze bpftrace did not attach" >&2
	exit 1
}
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c 'UPDATE ft_trace_gate SET go = true' >/dev/null
for _ in $(seq 1 600); do
	if grep -q '^CASE_DONE$' "$WORKDIR/no_dml_analyze.psql.out"; then
		break
	fi
	if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^CASE_DONE$' "$WORKDIR/no_dml_analyze.psql.out"; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	echo "no_dml_analyze did not reach CASE_DONE" >&2
	exit 1
fi
if ! wait_bounded "$psql_pid"; then
	cat "$WORKDIR/no_dml_analyze.psql.out" "$WORKDIR/no_dml_analyze.psql.err" >&2
	exit 1
fi
$SUDO kill -INT "$TRACE_PID" >/dev/null 2>&1 || true
no_dml_trace_rc=0
wait_bounded "$TRACE_PID" "" 1 || no_dml_trace_rc=$?
TRACE_PID=""
if [ "$no_dml_trace_rc" -ne 0 ] && [ "$no_dml_trace_rc" -ne 130 ]; then
	cat "$WORKDIR/no_dml_analyze.trace" >&2
	echo "no_dml_analyze bpftrace exited with status $no_dml_trace_rc" >&2
	exit 1
fi
if ! grep -q '^BPFTRACE_END$' "$WORKDIR/no_dml_analyze.trace"; then
	cat "$WORKDIR/no_dml_analyze.trace" >&2
	echo "no_dml_analyze bpftrace did not emit BPFTRACE_END" >&2
	exit 1
fi
assert_count "no-DML traced fasttrun workload" \
	"$(map_count @workload "$WORKDIR/no_dml_analyze.trace")" gt 199
assert_count "no-DML post-workload end sentinel" \
	"$(map_count @end_sentinel "$WORKDIR/no_dml_analyze.trace")" gt 0
assert_count "no-DML RelationGetNumberOfBlocksInFork" \
	"$(map_count @nblocks "$WORKDIR/no_dml_analyze.trace")" eq 0
assert_count "no-DML smgrnblocks" \
	"$(map_count @smgr "$WORKDIR/no_dml_analyze.trace")" eq 0
echo "no_dml_analyze passed"

# Основная проверка: 1 млн строк, 50 колонок, четыре пользовательских индекса
# и внешний TOAST. Результат для небольшой таблицы печатается отдельно и не
# отменяет ошибку основной проверки.
cat >"$WORKDIR/trunc_slo.sql" <<SQL
DO \$fixture\$
DECLARE ddl text := 'CREATE TEMP TABLE ft_trunc_wide (id bigint PRIMARY KEY';
DECLARE i int;
BEGIN
  FOR i IN 1..35 LOOP
    ddl := ddl || format(', i%s int', to_char(i, 'FM00'));
  END LOOP;
  FOR i IN 1..5 LOOP
    ddl := ddl || format(', n%s numeric(18,4)', to_char(i, 'FM00'));
  END LOOP;
  FOR i IN 1..4 LOOP
    ddl := ddl || format(', d%s date', to_char(i, 'FM00'));
  END LOOP;
  FOR i IN 1..4 LOOP
    ddl := ddl || format(', t%s text', to_char(i, 'FM00'));
  END LOOP;
  ddl := ddl || ', payload text)';
  EXECUTE ddl;
  EXECUTE 'ALTER TABLE ft_trunc_wide ALTER COLUMN payload SET STORAGE EXTERNAL';
  IF (SELECT count(*) FROM pg_attribute
      WHERE attrelid = 'ft_trunc_wide'::regclass
        AND attnum > 0 AND NOT attisdropped) <> 50 THEN
    RAISE EXCEPTION 'wide test table must have exactly 50 columns';
  END IF;
END
\$fixture\$;

CREATE PROCEDURE pg_temp.fill_ft_trunc_wide()
LANGUAGE plpgsql AS \$fill\$
DECLARE expr text := 'g::bigint';
DECLARE i int;
BEGIN
  FOR i IN 1..35 LOOP
    expr := expr || format(', (g %% %s)::int', 1000 + i);
  END LOOP;
  FOR i IN 1..5 LOOP
    expr := expr || format(', round(g::numeric / %s, 4)', i + 1);
  END LOOP;
  FOR i IN 1..4 LOOP
    expr := expr || format(', date ''2020-01-01'' + (g %% %s)::int', 365 * i);
  END LOOP;
  FOR i IN 1..4 LOOP
    expr := expr || format(', md5((g * %s)::text)', i);
  END LOOP;
  expr := expr || \$payload\$,
    CASE WHEN g % 1000 = 0 THEN
      (SELECT string_agg(md5((g * 1000 + s)::text), '' ORDER BY s)
         FROM generate_series(1,160) AS toast_s(s))
    ELSE md5(g::text) || md5((g + 1)::text)
      || md5((g + 2)::text) || md5((g + 3)::text)
    END\$payload\$;
  EXECUTE 'INSERT INTO ft_trunc_wide SELECT ' || expr
       || ' FROM generate_series(1,1000000) g';
END
\$fill\$;

CREATE PROCEDURE pg_temp.assert_ft_trunc_wide_toast()
LANGUAGE plpgsql AS \$assert\$
DECLARE toast_oid oid;
DECLARE chunk_count bigint;
BEGIN
  SELECT reltoastrelid INTO toast_oid
  FROM pg_class WHERE oid = 'ft_trunc_wide'::regclass;
  IF toast_oid = 0 OR pg_relation_size(toast_oid) <= 0 THEN
    RAISE EXCEPTION 'wide test table has no physical external TOAST';
  END IF;
  EXECUTE format('SELECT count(*) FROM %s', toast_oid::regclass)
  INTO chunk_count;
  IF chunk_count <= 0 THEN
    RAISE EXCEPTION 'wide test table TOAST relation has no chunks';
  END IF;
END
\$assert\$;

CALL pg_temp.fill_ft_trunc_wide();
CREATE INDEX ft_trunc_wide_i01_idx ON ft_trunc_wide (i01);
CREATE INDEX ft_trunc_wide_i02_i03_idx ON ft_trunc_wide (i02, i03);
CREATE INDEX ft_trunc_wide_payload_idx ON ft_trunc_wide ((left(payload, 16)));
CALL pg_temp.assert_ft_trunc_wide_toast();

DO \$wide_bench\$
DECLARE sample_no int;
DECLARE started timestamptz;
DECLARE elapsed_ms numeric;
BEGIN
  FOR sample_no IN 1..7 LOOP
    started := clock_timestamp();
    PERFORM fasttruncate('ft_trunc_wide');
    elapsed_ms := extract(epoch FROM clock_timestamp() - started) * 1000;
    RAISE NOTICE 'WIDE_SAMPLE|%|%', sample_no, round(elapsed_ms, 3);
    CALL pg_temp.fill_ft_trunc_wide();
    CALL pg_temp.assert_ft_trunc_wide_toast();
  END LOOP;
END
\$wide_bench\$;

DO \$wide_metrics\$
DECLARE toast_oid oid;
DECLARE chunk_count bigint;
DECLARE user_index_bytes bigint;
DECLARE workset_relations int;
BEGIN
  SELECT reltoastrelid INTO toast_oid
  FROM pg_class WHERE oid = 'ft_trunc_wide'::regclass;
  EXECUTE format('SELECT count(*) FROM %s', toast_oid::regclass)
  INTO chunk_count;
  SELECT coalesce(sum(pg_relation_size(indexrelid)), 0), count(*)
  INTO user_index_bytes, workset_relations
  FROM pg_index WHERE indrelid = 'ft_trunc_wide'::regclass;
  workset_relations := 1 + workset_relations + 1 +
    (SELECT count(*) FROM pg_index WHERE indrelid = toast_oid);
  RAISE NOTICE 'WIDE_STORAGE|%|%|%|%|%|%',
    pg_total_relation_size('ft_trunc_wide'::regclass),
    pg_relation_size('ft_trunc_wide'::regclass),
    user_index_bytes,
    pg_total_relation_size(toast_oid),
    chunk_count,
    workset_relations;
END
\$wide_metrics\$;

CREATE TEMP TABLE ft_trunc_skinny (id bigint, payload text);
CREATE INDEX ft_trunc_skinny_id_idx ON ft_trunc_skinny (id);
CREATE PROCEDURE pg_temp.fill_ft_trunc_skinny()
LANGUAGE SQL AS \$fill\$
  INSERT INTO ft_trunc_skinny
  SELECT g, md5(g::text) FROM generate_series(1, 1000000) g;
\$fill\$;
CALL pg_temp.fill_ft_trunc_skinny();

DO \$skinny_bench\$
DECLARE sample_no int;
DECLARE started timestamptz;
DECLARE elapsed_ms numeric;
BEGIN
  FOR sample_no IN 1..7 LOOP
    started := clock_timestamp();
    PERFORM fasttruncate('ft_trunc_skinny');
    elapsed_ms := extract(epoch FROM clock_timestamp() - started) * 1000;
    RAISE NOTICE 'SKINNY_SAMPLE|%|%', sample_no, round(elapsed_ms, 3);
    CALL pg_temp.fill_ft_trunc_skinny();
  END LOOP;
END
\$skinny_bench\$;

SELECT 'SERVER_VERSION|' || version();
SELECT 'BLOCK_SIZE|' || current_setting('block_size');
SELECT 'TEMP_BUFFERS|' || current_setting('temp_buffers');
SQL
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/trunc_slo.sql" \
	>"$WORKDIR/trunc_slo.psql.out" 2>&1 || {
	cat "$WORKDIR/trunc_slo.psql.out" >&2
	echo "trunc_slo failed" >&2
	exit 1
}

cpu_model=$(awk -F: '/model name/{sub(/^[[:space:]]*/, "", $2); print $2; exit}' \
	/proc/cpuinfo 2>/dev/null || true)
cpu_model=${cpu_model:-unknown}
if command -v findmnt >/dev/null 2>&1; then
	fs_type=$(findmnt -no FSTYPE -T "$WORKDIR" 2>/dev/null || true)
else
	fs_type=$(stat -f -c %T "$WORKDIR" 2>/dev/null || true)
fi
fs_type=${fs_type:-unknown}
echo "ENV|PG_CONFIG_VERSION|$($PG_CONFIG --version)"
echo "ENV|PG_CONFIGURE|$($PG_CONFIG --configure)"
echo "ENV|CPU_MODEL|$cpu_model"
echo "ENV|FILESYSTEM|$fs_type"
cat "$WORKDIR/trunc_slo.psql.out"

"$PYTHON" - "$WORKDIR/trunc_slo.psql.out" "$MAX_TRUNC_WIDE_MS" "$MAX_TRUNC_MS" <<'PY'
import math
import re
import statistics
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="replace")
wide_limit = float(sys.argv[2])
skinny_limit = float(sys.argv[3])
for name, value in (("MAX_TRUNC_WIDE_MS", wide_limit), ("MAX_TRUNC_MS", skinny_limit)):
    if not math.isfinite(value) or value <= 0:
        raise SystemExit(f"{name} must be a positive number, got {value}")
failed = False


def samples(prefix):
    rows = re.findall(rf"{prefix}_SAMPLE\|(\d+)\|([0-9]+(?:\.[0-9]+)?)", text)
    if len(rows) != 7 or [int(row[0]) for row in rows] != list(range(1, 8)):
        raise SystemExit(f"{prefix}: expected seven ordered samples, got {rows}")
    return [float(row[1]) for row in rows]


wide = samples("WIDE")
skinny = samples("SKINNY")
for label, values in (("wide_1m_x50", wide), ("skinny_secondary", skinny)):
    ordered = sorted(values)
    median = statistics.median(values)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    rendered = ",".join(f"{value:.3f}" for value in values)
    print(f"{label} samples_ms=[{rendered}] median_ms={median:.3f} p95_ms={p95:.3f}")

for label, values, limit in (
    ("wide", wide, wide_limit),
    ("skinny", skinny, skinny_limit),
):
    median = statistics.median(values)
    if median > limit:
        print(
            f"FAIL: {label} median {median:.3f} ms > {limit:.3f} ms",
            file=sys.stderr,
        )
        failed = True
    if max(values) > 2.0 * limit:
        print(
            f"FAIL: {label} sample {max(values):.3f} ms > 2x limit "
            f"{2.0 * limit:.3f} ms",
            file=sys.stderr,
        )
        failed = True
if failed:
    raise SystemExit(1)
PY
echo "strict truncate SLO passed"

# Строгий предел полного пересбора: те же шаги, что в sql/fasttrun_bench.sql,
# но по медиане REFRESH_RUNS прогонов. Каждый прогон идет в новой базе, потому
# что бенч сам создает и удаляет расширение. Параметры, от которых зависит путь
# пересбора, выставлены в копии бенча явно, а PGOPTIONS снят: иначе, например,
# stats_refresh_threshold=1 из окружения подменил бы пересбор дельтой.
if [ ! -f "$BENCH_SQL" ]; then
	echo "missing bench file: $BENCH_SQL" >&2
	exit 1
fi
case "$REFRESH_RUNS" in
	''|*[!0-9]*|0)
		echo "refresh SLO: REFRESH_RUNS must be a positive integer" >&2
		exit 1
		;;
esac
sed 's/^SET client_min_messages = WARNING;$/SET client_min_messages = NOTICE;\
SET fasttrun.stats_refresh_threshold = 0.2;\
SET fasttrun.auto_collect_stats = on;\
SET fasttrun.sample_rows = 10000;\
SET fasttrun.use_typanalyze = on;\
SET fasttrun.max_analyze_pages = 100000;\
SET fasttrun.max_stats_memory = 0;/' \
	"$BENCH_SQL" >"$WORKDIR/refresh_bench.sql"
if ! grep -q '^SET client_min_messages = NOTICE;$' "$WORKDIR/refresh_bench.sql"; then
	echo "refresh SLO: the bench notices could not be switched on" >&2
	exit 1
fi
: >"$WORKDIR/refresh_slo.out"
run_no=1
while [ "$run_no" -le "$REFRESH_RUNS" ]; do
	run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d postgres -X -q \
		-v ON_ERROR_STOP=1 -c "CREATE DATABASE ft_refresh_slo" >/dev/null
	run_pg env -u PGOPTIONS "$PSQL" -h "$WORKDIR" -p "$PORT" -d ft_refresh_slo -X -q \
		-v ON_ERROR_STOP=1 -f "$WORKDIR/refresh_bench.sql" \
		>"$WORKDIR/refresh_run$run_no.out" 2>&1 || {
		cat "$WORKDIR/refresh_run$run_no.out" >&2
		echo "refresh SLO: bench run $run_no failed" >&2
		exit 1
	}
	run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d postgres -X -q \
		-v ON_ERROR_STOP=1 -c "DROP DATABASE ft_refresh_slo" >/dev/null
	cat "$WORKDIR/refresh_run$run_no.out" >>"$WORKDIR/refresh_slo.out"
	run_no=$((run_no + 1))
done

"$PYTHON" - "$WORKDIR/refresh_slo.out" "$MAX_REFRESH_MS" "$REFRESH_RUNS" <<'PY'
import math
import re
import statistics
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="replace")
limit = float(sys.argv[2])
if not math.isfinite(limit) or limit <= 0:
    raise SystemExit(f"MAX_REFRESH_MS must be a positive number, got {sys.argv[2]}")
runs = int(sys.argv[3])
failed = False
for step in ("DELETE", "UPDATE"):
    pairs = [
        (float(first), float(second))
        for first, second in re.findall(
            rf"after {step} 500k: 1st=([0-9]+(?:\.[0-9]+)?) ms \(full refresh\), "
            r"2nd=([0-9]+(?:\.[0-9]+)?) ms",
            text,
        )
    ]
    if len(pairs) != runs:
        raise SystemExit(
            f"refresh after {step}: expected {runs} samples, got {len(pairs)}"
        )
    # Пересбор после DML выбирает блоки и пересчитывает выборку, а чистая дельта
    # читает только счетчики. Первый замер, сравнимый со вторым, значит, что
    # пересбора не было.
    for first, second in pairs:
        if first < 20.0 * second:
            print(
                f"FAIL: refresh after {step} took {first:.3f} ms against a pure "
                f"delta of {second:.3f} ms, so no full refresh ran",
                file=sys.stderr,
            )
            failed = True
    values = [first for first, _ in pairs]
    median = statistics.median(values)
    rendered = ",".join(f"{value:.3f}" for value in values)
    print(
        f"refresh_after_{step.lower()}_500k samples_ms=[{rendered}] "
        f"median_ms={median:.3f}"
    )
    if median > limit:
        print(
            f"FAIL: refresh after {step} median {median:.3f} ms > {limit:.3f} ms",
            file=sys.stderr,
        )
        failed = True
    if max(values) > 2.0 * limit:
        print(
            f"FAIL: refresh after {step} sample {max(values):.3f} ms > 2x limit "
            f"{2.0 * limit:.3f} ms",
            file=sys.stderr,
        )
        failed = True
if failed:
    raise SystemExit(1)
PY
echo "strict refresh SLO passed"

echo "fasttrun perf smoke passed"
