#!/usr/bin/env bash
#
# Regression-проверка для оверхеда в xact-callback.
#
# Воспроизводит worst-case паттерн нагрузки.  Именно он дал большую
# CPU-регрессию в fasttrun 2.2.0.  Один бэкенд держит много temp
# таблиц ON COMMIT DELETE ROWS через много транзакций.  В каждой xact
# наполняет, делает fasttrun_analyze для каждой, коммитит.  И так по
# кругу.
#
# В 2.2.0 fasttrun_cache_commit_xact / fasttrun_stats_cache_commit_xact
# обходили бэкенд-локальные кеши на каждом COMMIT целиком.  Дёргали
# RelationGetNumberOfBlocks на каждую запись.  И выдавали град вызовов
# fasttrun_invalidate_local_plan_cache.  Это было O(cache_size) на
# COMMIT.  И на каждую инвалидацию обходило все кешированные планы.
# На бэкенде с горой кешированных temp таблиц и большим SPI/PREPARE
# plan cache это съедало основную долю CPU.
#
# После фикса:
#   - fasttrun_cache_commit_xact НЕ должен звать
#     fasttrun_invalidate_local_plan_cache на ON COMMIT DELETE ROWS
#     truncate.  Принятие пустого storage уехало в ленивый reinject
#     внутрь fasttrun_planner_hook.
#   - fasttrun_cache_make_empty_storage_authoritative НЕ должен
#     срабатывать из COMMIT-колбэка вообще.  Он остаётся доступен для
#     ветки SAVEPOINT ABORT.
#   - fasttrun_stats_cache_evict_relid НЕ должен срабатывать из
#     COMMIT-колбэка вообще.  Его вложенный O(N^2) обход stats cache
#     убран.
#
# Pass criteria для дефолтной нагрузки (200 xact'ов x 19 temp таблиц):
#   @ft_inval_from_commit_callback        <= 5    (запас на мелкие сбои)
#   @empty_auth_from_commit_callback      <= 5
#   @stats_evict_from_commit_callback     <= 5
# Собственные инвалидации fasttrun_analyze (need_plan_inval после
# delta-hit) сюда не относятся.  Они продолжают эмититься -- это
# задуманное поведение.  И учитываются в @ft_inval_other.
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
PORT=${PGPORT:-55438}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-inval.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_inval}
ITERATIONS=${ITERATIONS:-200}
TEMP_TABLES=${TEMP_TABLES:-19}
MAX_COMMIT_INVAL=${MAX_COMMIT_INVAL:-5}
MAX_EMPTY_AUTH=${MAX_EMPTY_AUTH:-5}
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
	"CREATE TABLE ft_inval_trace_gate (go boolean NOT NULL); INSERT INTO ft_inval_trace_gate VALUES (false)" >/dev/null

cat >"$WORKDIR/repro.sql" <<SQL
SELECT pg_backend_pid();

DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$TEMP_TABLES LOOP
    EXECUTE format(
      'CREATE TEMP TABLE ft_inval_t_%s (id int, grp int, payload text) ON COMMIT DELETE ROWS',
      i);
  END LOOP;
END\$do\$;
COMMIT;

SELECT 'WORKLOAD_READY';
DO \$gate\$
BEGIN
  WHILE NOT (SELECT go FROM ft_inval_trace_gate) LOOP
    PERFORM pg_sleep(0.05);
  END LOOP;
END\$gate\$;

DO \$do\$
DECLARE i int; j int;
BEGIN
  FOR i IN 1..$ITERATIONS LOOP
    FOR j IN 1..$TEMP_TABLES LOOP
      EXECUTE format(
        'INSERT INTO ft_inval_t_%s SELECT g, g %% 100, md5(g::text) FROM generate_series(1, 500) g',
        j);
      EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_inval_t_'||j);
    END LOOP;
    COMMIT;
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

# Ждем WORKLOAD_READY - только потом цепляем probe'ы.
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

# Probe: считаем инвалидации и empty-auth изнутри xact_callback цепочки.
# Через uretprobe на fasttrun_xact_callback отделяем "мы внутри
# колбэка".  И затем считаем хиты fasttrun_invalidate_local_plan_cache,
# которые случились именно внутри.  Так оверхед COMMIT-колбэка
# изолируется от собственных (задуманных) инвалидаций fasttrun_analyze.
$SUDO stdbuf -oL "$BPFTRACE" -e "
BEGIN { printf(\"ATTACHED_READY\\n\"); }
uprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { @in_xact_cb[tid] = 1; }
uretprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { delete(@in_xact_cb[tid]); }
uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache /pid == $backend_pid && @in_xact_cb[tid]/ { @ft_inval_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache /pid == $backend_pid && !@in_xact_cb[tid]/ { @ft_inval_other = count(); }
uprobe:$FASTTRUN_SO:fasttrun_cache_make_empty_storage_authoritative /pid == $backend_pid && @in_xact_cb[tid]/ { @empty_auth_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_cache_make_empty_storage_authoritative /pid == $backend_pid && !@in_xact_cb[tid]/ { @empty_auth_other = count(); }
uprobe:$FASTTRUN_SO:fasttrun_stats_cache_evict_relid /pid == $backend_pid && @in_xact_cb[tid]/ { @stats_evict_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { @xact_cb_calls = count(); }
uprobe:$PG_BINDIR/postgres:pg_backend_pid /pid == $backend_pid/ { @end_sentinel = count(); }
END {
  print(@ft_inval_from_commit_callback);
  print(@ft_inval_other);
  print(@empty_auth_from_commit_callback);
  print(@empty_auth_other);
  print(@stats_evict_from_commit_callback);
  print(@xact_cb_calls);
  print(@end_sentinel);
  printf(\"BPFTRACE_END\\n\");
}
" >"$WORKDIR/inval.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 400); do
	if grep -q '^ATTACHED_READY$' "$WORKDIR/inval.bpf.out" 2>/dev/null; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^ATTACHED_READY$' "$WORKDIR/inval.bpf.out" 2>/dev/null; then
	cat "$WORKDIR/inval.bpf.out" >&2
	echo "FAIL: bpftrace не подтвердил ATTACHED_READY" >&2
	exit 1
fi

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "UPDATE ft_inval_trace_gate SET go = true" >/dev/null

for _ in $(seq 1 3000); do
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
	cat "$WORKDIR/inval.bpf.out" >&2
	echo "FAIL: bpftrace завершился с кодом $trace_rc" >&2
	exit 1
fi
if ! grep -q '^BPFTRACE_END$' "$WORKDIR/inval.bpf.out"; then
	cat "$WORKDIR/inval.bpf.out" >&2
	echo "FAIL: bpftrace не напечатал финальный маркер BPFTRACE_END" >&2
	exit 1
fi

map_count()
{
	awk -v name="$1:" '$1 == name {gsub(/[^0-9]/, "", $2); print $2}' \
		"$WORKDIR/inval.bpf.out" | tail -1
}

ft_inval_commit=$(map_count "@ft_inval_from_commit_callback")
ft_inval_other=$(map_count "@ft_inval_other")
empty_commit=$(map_count "@empty_auth_from_commit_callback")
empty_other=$(map_count "@empty_auth_other")
stats_evict_commit=$(map_count "@stats_evict_from_commit_callback")
xact_calls=$(map_count "@xact_cb_calls")
end_sentinel=$(map_count "@end_sentinel")

ft_inval_commit=${ft_inval_commit:-0}
ft_inval_other=${ft_inval_other:-0}
empty_commit=${empty_commit:-0}
empty_other=${empty_other:-0}
stats_evict_commit=${stats_evict_commit:-0}
xact_calls=${xact_calls:-0}
end_sentinel=${end_sentinel:-0}

cat <<METRICS
reproducer оверхеда xact-callback: $ITERATIONS xact'ов x $TEMP_TABLES temp таблиц ON COMMIT DELETE ROWS
вызовов xact_callback:                                        $xact_calls
fasttrun_invalidate_local_plan_cache из COMMIT-колбэка:       $ft_inval_commit
fasttrun_invalidate_local_plan_cache из других мест (норма):  $ft_inval_other
fasttrun_cache_make_empty_storage_authoritative из колбэка:   $empty_commit
fasttrun_cache_make_empty_storage_authoritative из других:    $empty_other
fasttrun_stats_cache_evict_relid из COMMIT-колбэка:           $stats_evict_commit
БЮДЖЕТ commit-callback inval <= $MAX_COMMIT_INVAL, empty-auth <= $MAX_EMPTY_AUTH
METRICS

# Positive-attach guard: под нагрузкой xact_callback ОБЯЗАН сработать.
# Ноль означает, что uprobe не подцепился (stripped-бинарь, инлайн символа,
# нет pg_noinline-цели, права) -- харнес ничего не измерил, а не "прошёл".
# Это отдельная причина от превышения бюджета ниже.
# На каждый COMMIT ядро вызывает xact_callback минимум дважды (PRE_COMMIT и
# COMMIT), плюс финальный вызов pg_backend_pid обязан попасть в @end_sentinel.
expected_xact_calls=$(( ITERATIONS * 2 ))
if [ "$xact_calls" -lt "$expected_xact_calls" ] || [ "$end_sentinel" -lt 1 ]; then
	echo "FAIL: uprobe поймал неполное окно (@xact_cb_calls=$xact_calls при ожидаемых >=$expected_xact_calls, @end_sentinel=$end_sentinel)" >&2
	echo "  (это НЕ превышение бюджета: проверьте bpftrace/uprobe attach к xact-callback)" >&2
	exit 1
fi

failed=0
if [ "$ft_inval_commit" -gt "$MAX_COMMIT_INVAL" ]; then
	echo "FAIL: инвалидаций из commit-колбэка $ft_inval_commit > $MAX_COMMIT_INVAL" >&2
	failed=1
fi
if [ "$empty_commit" -gt "$MAX_EMPTY_AUTH" ]; then
	echo "FAIL: empty-auth обходов из commit-колбэка $empty_commit > $MAX_EMPTY_AUTH" >&2
	failed=1
fi
if [ "$stats_evict_commit" -gt "$MAX_EMPTY_AUTH" ]; then
	echo "FAIL: stats_evict обходов из commit-колбэка $stats_evict_commit > $MAX_EMPTY_AUTH" >&2
	failed=1
fi

if [ "$failed" -ne 0 ]; then
	exit 1
fi

echo "regression-проверка fasttrun commit-callback overhead прошла"
