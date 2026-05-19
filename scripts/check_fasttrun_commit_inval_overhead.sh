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
SUDO=${SUDO:-sudo}
PORT=${PGPORT:-55438}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-inval.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_inval}
ITERATIONS=${ITERATIONS:-200}
TEMP_TABLES=${TEMP_TABLES:-19}
MAX_COMMIT_INVAL=${MAX_COMMIT_INVAL:-5}
MAX_EMPTY_AUTH=${MAX_EMPTY_AUTH:-5}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}

cleanup()
{
	if [ -n "${TRACE_PID:-}" ]; then
		$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
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
		echo "не нашёл команду: $1" >&2
		exit 1
	fi
}

require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
require_cmd "$BPFTRACE"

if [ ! -f "$FASTTRUN_SO" ]; then
	echo "не нашёл shared library fasttrun: $FASTTRUN_SO" >&2
	exit 1
fi

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null

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
SELECT pg_sleep(2);

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

SELECT 'WORKLOAD_DONE';
SELECT pg_sleep(2);
SQL

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
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
	wait "$psql_pid" || true
	cat "$WORKDIR/repro.psql.out" "$WORKDIR/repro.psql.err" >&2
	echo "не удалось прочитать pid бэкенда" >&2
	exit 1
fi

# Ждём WORKLOAD_READY -- только потом цепляем probe'ы.
for _ in $(seq 1 200); do
	if grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.psql.out"; then
		break
	fi
	sleep 0.05
done

# Probe: считаем инвалидации и empty-auth изнутри xact_callback цепочки.
# Через uretprobe на fasttrun_xact_callback отделяем "мы внутри
# колбэка".  И затем считаем хиты fasttrun_invalidate_local_plan_cache,
# которые случились именно внутри.  Так оверхед COMMIT-колбэка
# изолируется от собственных (задуманных) инвалидаций fasttrun_analyze.
$SUDO timeout 60 "$BPFTRACE" -e "
uprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { @in_xact_cb[tid] = 1; }
uretprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { delete(@in_xact_cb[tid]); }
uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache /pid == $backend_pid && @in_xact_cb[tid]/ { @ft_inval_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache /pid == $backend_pid && !@in_xact_cb[tid]/ { @ft_inval_other = count(); }
uprobe:$FASTTRUN_SO:fasttrun_cache_make_empty_storage_authoritative /pid == $backend_pid && @in_xact_cb[tid]/ { @empty_auth_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_cache_make_empty_storage_authoritative /pid == $backend_pid && !@in_xact_cb[tid]/ { @empty_auth_other = count(); }
uprobe:$FASTTRUN_SO:fasttrun_stats_cache_evict_relid /pid == $backend_pid && @in_xact_cb[tid]/ { @stats_evict_from_commit_callback = count(); }
uprobe:$FASTTRUN_SO:fasttrun_xact_callback /pid == $backend_pid/ { @xact_cb_calls = count(); }
" -o "$WORKDIR/inval.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 3000); do
	if grep -q '^WORKLOAD_DONE$' "$WORKDIR/repro.psql.out"; then
		break
	fi
	if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.1
done
wait "$psql_pid"
$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
wait "$TRACE_PID" >/dev/null 2>&1 || true
TRACE_PID=""

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

ft_inval_commit=${ft_inval_commit:-0}
ft_inval_other=${ft_inval_other:-0}
empty_commit=${empty_commit:-0}
empty_other=${empty_other:-0}
stats_evict_commit=${stats_evict_commit:-0}
xact_calls=${xact_calls:-0}

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
