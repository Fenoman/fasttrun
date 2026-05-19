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
SUDO=${SUDO:-sudo}
PORT=${PGPORT:-55459}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-cdur.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_cdur}
TEMP_TABLES=${TEMP_TABLES:-50}
COLUMNS=${COLUMNS:-4}
ITERATIONS=${ITERATIONS:-100}
MAX_AVG_NS=${MAX_AVG_NS:-200000}
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
SELECT pg_sleep(2);

DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$ITERATIONS LOOP
    FOR j IN 1..$TEMP_TABLES LOOP
      EXECUTE format(
        'INSERT INTO ft_cdur_t_%s SELECT $inserts FROM generate_series(1, 20) g', j);
    END LOOP;
    PERFORM fasttrun_analyze_bulk(VARIADIC ARRAY[$table_names]);
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

for _ in $(seq 1 400); do
	if grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.psql.out"; then
		break
	fi
	sleep 0.05
done

# bpftrace: измеряем суммарное время в callback'ах через uprobe/uretprobe пары.
# Используем nsecs builtin -- BPF без HW PMU (Parallels) с ним справляется.
$SUDO timeout 120 "$BPFTRACE" -e "
uprobe:$FASTTRUN_SO:fasttrun_cache_commit_xact /pid == $backend_pid/ { @ts_a[tid] = nsecs; }
uretprobe:\
$FASTTRUN_SO:fasttrun_cache_commit_xact /pid == $backend_pid && @ts_a[tid] != 0/ {
  @analyze_total = sum(nsecs - @ts_a[tid]);
  @analyze_count = count();
  delete(@ts_a[tid]);
}
uprobe:$FASTTRUN_SO:fasttrun_stats_cache_commit_xact /pid == $backend_pid/ { @ts_s[tid] = nsecs; }
uretprobe:\
$FASTTRUN_SO:fasttrun_stats_cache_commit_xact /pid == $backend_pid && @ts_s[tid] != 0/ {
  @stats_total = sum(nsecs - @ts_s[tid]);
  @stats_count = count();
  delete(@ts_s[tid]);
}
" -o "$WORKDIR/cdur.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 6000); do
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

map_value()
{
	awk -v name="$1:" '$1 == name {gsub(/[^0-9]/, "", $2); print $2}' \
		"$WORKDIR/cdur.bpf.out" | tail -1
}

analyze_total=$(map_value "@analyze_total")
analyze_count=$(map_value "@analyze_count")
stats_total=$(map_value "@stats_total")
stats_count=$(map_value "@stats_count")

analyze_total=${analyze_total:-0}
analyze_count=${analyze_count:-0}
stats_total=${stats_total:-0}
stats_count=${stats_count:-0}

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
