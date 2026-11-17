#!/usr/bin/env bash
#
# Regression-проверка против O(cache_size) обхода в xact-callback.
#
# check_fasttrun_commit_inval_overhead.sh уже ловит "callback вообще не
# должен дёргать инвалидации".  Этот скрипт ловит соседний регресс:
# walk внутри callback не должен расти линейно по размеру локальных
# кэшей -- иначе при большом долгоживущем backend каждый COMMIT будет
# дорогим, даже без инвалидаций.
#
# Worst-case паттерн: один backend держит MANY temp таблиц с
# несколькими аналитическими колонками (статистика собрана на каждую),
# и в каждой xact трогает все таблицы через fasttrun_analyze_bulk.
# До P1/P2 оптимизаций commit/subxact callbacks делали
# hash_seq_search по всей таблице кэшей.  После -- идут только по
# touched relids через fasttrun_stats_relid_cache.attkeys.
#
# Тест замеряет среднее время в:
#   - fasttrun_cache_commit_xact;
#   - fasttrun_stats_cache_commit_xact.
#
# Pass criteria (по умолчанию):
#   avg duration per call <= MAX_AVG_NS (200000 = 200 микросекунд).
#   200us -- большой запас.  На debug PG16 aarch64 (~2 GHz, 2 CPU
#   VM) реально получается порядка 5-30us для нашего workload.
#
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PG_PKGLIBDIR=$("$PG_CONFIG" --pkglibdir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
BPFTRACE=${BPFTRACE:-bpftrace}
SUDO=${SUDO-sudo}
PG_RUN_AS=${PG_RUN_AS:-}
PORT=${PGPORT:-55459}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-cdur.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_cdur}
TEMP_TABLES=${TEMP_TABLES:-50}
COLUMNS=${COLUMNS:-4}
ITERATIONS=${ITERATIONS:-100}
MAX_AVG_NS=${MAX_AVG_NS:-200000}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}

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
		terminate_child "$TRACE_PID" -TERM 1
	fi
	if [ -n "${psql_pid:-}" ]; then
		terminate_child "$psql_pid"
	fi
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop >/dev/null 2>&1 \
			|| run_pg "$PG_CTL" -D "$WORKDIR/data" -m immediate -w stop >/dev/null 2>&1 \
			|| true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

require_cmd()
{
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "не нашёл команду: $1" >&2
		exit 1
	fi
}

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
require_cmd "$BPFTRACE"
require_cmd stdbuf
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi

if [ ! -f "$FASTTRUN_SO" ]; then
	echo "не нашёл shared library fasttrun: $FASTTRUN_SO" >&2
	exit 1
fi

if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR"
fi
run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	"CREATE TABLE ft_cdur_trace_gate (go boolean NOT NULL); INSERT INTO ft_cdur_trace_gate VALUES (false)" >/dev/null

# Build column list once: c1 int, c2 int, ...
cols=""
for c in $(seq 1 "$COLUMNS"); do
	if [ -z "$cols" ]; then
		cols="c${c} int"
	else
		cols="$cols, c${c} int"
	fi
done
inserts=""
for c in $(seq 1 "$COLUMNS"); do
	if [ -z "$inserts" ]; then
		inserts="g"
	else
		inserts="$inserts, g + ${c}"
	fi
done
table_names=""
for i in $(seq 1 "$TEMP_TABLES"); do
	if [ -z "$table_names" ]; then
		table_names="'ft_cdur_t_${i}'"
	else
		table_names="${table_names}, 'ft_cdur_t_${i}'"
	fi
done

cat >"$WORKDIR/repro.sql" <<SQL
SELECT pg_backend_pid();

-- Setup: create $TEMP_TABLES temp tables, populate, analyze each so stats cache
-- holds ($TEMP_TABLES * $COLUMNS) statsTuple entries.  This is the "large
-- cache" against which we measure commit-callback walking cost.
DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$TEMP_TABLES LOOP
    EXECUTE format(
      'CREATE TEMP TABLE ft_cdur_t_%s ($cols) ON COMMIT DELETE ROWS', i);
  END LOOP;
END\$do\$;
COMMIT;

DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$TEMP_TABLES LOOP
    EXECUTE format(
      'INSERT INTO ft_cdur_t_%s SELECT $inserts FROM generate_series(1, 200) g', i);
    EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_cdur_t_'||i);
  END LOOP;
END\$do\$;
COMMIT;

SELECT 'WORKLOAD_READY';
DO \$gate\$
BEGIN
  WHILE NOT (SELECT go FROM ft_cdur_trace_gate) LOOP
    PERFORM pg_sleep(0.05);
  END LOOP;
END\$gate\$;

DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$ITERATIONS LOOP
    IF i = 1 THEN
      PERFORM pg_backend_pid();
    END IF;
    FOR j IN 1..$TEMP_TABLES LOOP
      EXECUTE format(
        'INSERT INTO ft_cdur_t_%s SELECT $inserts FROM generate_series(1, 20) g', j);
    END LOOP;
    PERFORM fasttrun_analyze_bulk(VARIADIC ARRAY[$table_names]);
    IF i < $ITERATIONS THEN
      COMMIT;
    END IF;
  END LOOP;
END\$do\$;

SELECT pg_backend_pid() \g /dev/null
SELECT 'WORKLOAD_DONE';
SQL

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/repro.sql" \
	>"$WORKDIR/repro.psql.out" 2>"$WORKDIR/repro.psql.err" &
psql_pid=$!

backend_pid=""
for _ in $(seq 1 200); do
	backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/repro.psql.out" 2>/dev/null | head -1 || true)
	if [ -n "$backend_pid" ]; then
		break
	fi
	sleep 0.05
done
if [ -z "$backend_pid" ]; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/repro.psql.out" "$WORKDIR/repro.psql.err" >&2
	echo "не удалось прочитать pid бэкенда" >&2
	exit 1
fi

for _ in $(seq 1 400); do
	if grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.psql.out"; then
		break
	fi
	if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.psql.out"; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/repro.psql.out" "$WORKDIR/repro.psql.err" >&2
	echo "нагрузка не дошла до WORKLOAD_READY" >&2
	exit 1
fi

# bpftrace: измеряем суммарное время в callback'ах через uprobe/uretprobe пары
# строго внутри фазы 1 (между первым рабочим витком и выходом из DO), чтобы
# контрольные транзакции до и после DO не размывали среднее и счетчик.
$SUDO stdbuf -oL "$BPFTRACE" -e "
BEGIN { @phase = 0; printf(\"ATTACHED_READY\\n\"); }
uprobe:$PG_BINDIR/postgres:pg_backend_pid /pid == $backend_pid/ { @phase = @phase + 1; }
uprobe:$FASTTRUN_SO:fasttrun_cache_commit_xact /pid == $backend_pid && @phase == 1/ { @ts_a[tid] = nsecs; }
uretprobe:\
$FASTTRUN_SO:fasttrun_cache_commit_xact /pid == $backend_pid && @ts_a[tid] != 0/ {
  @analyze_total = sum(nsecs - @ts_a[tid]);
  @analyze_count = count();
  delete(@ts_a[tid]);
}
uprobe:$FASTTRUN_SO:fasttrun_stats_cache_commit_xact /pid == $backend_pid && @phase == 1/ { @ts_s[tid] = nsecs; }
uretprobe:\
$FASTTRUN_SO:fasttrun_stats_cache_commit_xact /pid == $backend_pid && @ts_s[tid] != 0/ {
  @stats_total = sum(nsecs - @ts_s[tid]);
  @stats_count = count();
  delete(@ts_s[tid]);
}
END {
  print(@analyze_total);
  print(@analyze_count);
  print(@stats_total);
  print(@stats_count);
  print(@phase);
  printf(\"BPFTRACE_END\\n\");
}
" >"$WORKDIR/cdur.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 400); do
	if grep -q '^ATTACHED_READY$' "$WORKDIR/cdur.bpf.out" 2>/dev/null; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^ATTACHED_READY$' "$WORKDIR/cdur.bpf.out" 2>/dev/null; then
	cat "$WORKDIR/cdur.bpf.out" >&2
	echo "FAIL: bpftrace не подтвердил ATTACHED_READY" >&2
	exit 1
fi

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "UPDATE ft_cdur_trace_gate SET go = true" >/dev/null

for _ in $(seq 1 6000); do
	if grep -q '^WORKLOAD_DONE$' "$WORKDIR/repro.psql.out"; then
		break
	fi
	if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.1
done
if ! grep -q '^WORKLOAD_DONE$' "$WORKDIR/repro.psql.out"; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/repro.psql.out" "$WORKDIR/repro.psql.err" >&2
	echo "FAIL: SQL-сценарий не дошел до WORKLOAD_DONE" >&2
	exit 1
fi
if ! wait_bounded "$psql_pid"; then
	cat "$WORKDIR/repro.psql.out" "$WORKDIR/repro.psql.err" >&2
	echo "FAIL: SQL-сценарий завершился с ошибкой" >&2
	exit 1
fi

$SUDO kill -INT "$TRACE_PID" >/dev/null 2>&1 || true
trace_rc=0
wait_bounded "$TRACE_PID" "" 1 || trace_rc=$?
TRACE_PID=""
if [ "$trace_rc" -ne 0 ] && [ "$trace_rc" -ne 130 ]; then
	cat "$WORKDIR/cdur.bpf.out" >&2
	echo "FAIL: bpftrace завершился с кодом $trace_rc" >&2
	exit 1
fi
if ! grep -q '^BPFTRACE_END$' "$WORKDIR/cdur.bpf.out"; then
	cat "$WORKDIR/cdur.bpf.out" >&2
	echo "FAIL: bpftrace не напечатал финальный маркер BPFTRACE_END" >&2
	exit 1
fi

map_value()
{
	awk -v name="$1:" '$1 == name {gsub(/[^0-9]/, "", $2); print $2}' \
		"$WORKDIR/cdur.bpf.out" | tail -1
}

analyze_total=$(map_value "@analyze_total")
analyze_count=$(map_value "@analyze_count")
stats_total=$(map_value "@stats_total")
stats_count=$(map_value "@stats_count")
end_phase=$(map_value "@phase")

analyze_total=${analyze_total:-0}
analyze_count=${analyze_count:-0}
stats_total=${stats_total:-0}
stats_count=${stats_count:-0}
end_phase=${end_phase:-0}

analyze_avg=0
if [ "$analyze_count" -gt 0 ]; then
	analyze_avg=$((analyze_total / analyze_count))
fi
stats_avg=0
if [ "$stats_count" -gt 0 ]; then
	stats_avg=$((stats_total / stats_count))
fi

echo ""
echo "workload: $ITERATIONS xact x ($TEMP_TABLES temp таблиц * $COLUMNS колонок)"
echo "                              count       avg ns/call"
echo "fasttrun_cache_commit_xact:       $analyze_count       $analyze_avg"
echo "fasttrun_stats_cache_commit_xact: $stats_count       $stats_avg"
echo "лимит avg ns/call: $MAX_AVG_NS"

# Positive-attach guard: внутри фазы 1 оба commit-колбэка ОБЯЗАНЫ сработать
# ровно по числу рабочих транзакций ITERATIONS, а фаза дойти до 2.
if [ "$analyze_count" -ne "$ITERATIONS" ] || [ "$stats_count" -ne "$ITERATIONS" ] || [ "$end_phase" -ne 2 ]; then
	echo "FAIL: uprobe поймал неточное окно (@analyze_count=$analyze_count @stats_count=$stats_count @phase=$end_phase, ожидается ровно $ITERATIONS и phase=2) -- харнес недостоверен" >&2
	echo "  (это НЕ превышение avg-лимита: проверьте bpftrace/uprobe attach к commit-колбэкам)" >&2
	exit 1
fi

failed=0
if [ "$analyze_avg" -gt "$MAX_AVG_NS" ]; then
	echo "FAIL: fasttrun_cache_commit_xact avg duration $analyze_avg ns > limit $MAX_AVG_NS ns" >&2
	failed=1
fi
if [ "$stats_avg" -gt "$MAX_AVG_NS" ]; then
	echo "FAIL: fasttrun_stats_cache_commit_xact avg duration $stats_avg ns > limit $MAX_AVG_NS ns" >&2
	failed=1
fi
if [ "$failed" -ne 0 ]; then
	exit 1
fi

echo ""
echo "regression-проверка fasttrun commit-callback duration прошла"
