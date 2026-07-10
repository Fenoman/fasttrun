#!/usr/bin/env bash
#
# Regression-проверка giant-temp guardrail (fasttrun.max_analyze_pages).
#
# Выше порога холодный fasttrun_analyze обязан переходить на block-sampling
# и НЕ итерировать всю таблицу. Считаем вызовы heap_getnext внутри бэкенда
# через bpftrace для двух прогонов на одной большой temp-таблице:
#
#   A. block-sampling: fasttrun.max_analyze_pages = 100  (порог << размера)
#   B. full-scan:      fasttrun.max_analyze_pages = 0     (точный полный скан)
#
# heap_getnext вызывается по строке в полном скане (fasttrun_scan_with_sample)
# и НЕ вызывается на пути block-sampling (там table_scan_analyze_next_tuple).
# Метрика version-стабильна: в PG17+ ANALYZE-скан ушёл на ReadStream, поэтому
# считать ReadBufferExtended бессмысленно, а heap_getnext стабилен во всех
# поддержанных версиях.
#
# Pass criteria:
#   - A (block-sampling) итерирует заметно меньше строк, чем B (full-scan);
#     по умолчанию A <= B / 3 (на деле A ~ 0).
#   - B прошёл ~все строки таблицы (positive control: полный скан реально
#     итерирует всю таблицу, иначе сравнение бессмысленно).
#
# Требует Linux + bpftrace + sudo (uprobe на символ postgres).
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PG_PKGLIBDIR=$("$PG_CONFIG" --pkglibdir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
BPFTRACE=${BPFTRACE:-bpftrace}
SUDO=${SUDO:-sudo}
PG_RUN_AS=${PG_RUN_AS:-}
PORT=${PGPORT:-55471}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-giant.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_giant}
ROWS=${ROWS:-5000000}          # ~5M строк -> десятки тысяч блоков >> sample_target
RATIO_DIVISOR=${RATIO_DIVISOR:-3}
SAMPLE_MAX_PAGES=${SAMPLE_MAX_PAGES:-100}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}

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
	if [ -n "${TRACE_PID:-}" ]; then
		$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
		wait "$TRACE_PID" >/dev/null 2>&1 || true
	fi
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

require_cmd()
{
	command -v "$1" >/dev/null 2>&1 || { echo "не нашёл команду: $1" >&2; exit 1; }
}
require_cmd "$PSQL"; require_cmd "$INITDB"; require_cmd "$PG_CTL"
require_cmd "$CREATEDB"; require_cmd "$BPFTRACE"
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
# Локальный сокет + без TCP: не задеваем чужие сервисы на боксе.
run_pg "$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on -c temp_buffers=8MB" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	'CREATE TABLE fasttrun_giant_trace_gate (go boolean NOT NULL); INSERT INTO fasttrun_giant_trace_gate VALUES (false)' \
	>/dev/null

cat >"$WORKDIR/repro.sql" <<SQL
SELECT pg_backend_pid();
CREATE TEMP TABLE giant (id int, grp int, pad text);
INSERT INTO giant SELECT g, g % 1000, repeat('x', 40)
FROM generate_series(1, $ROWS) g;
SELECT 'BLOCKS ' ||
       ceil(pg_relation_size('giant')::numeric /
            current_setting('block_size')::numeric)::bigint;
SELECT 'WORKLOAD_READY';
DO \$gate\$
BEGIN
  WHILE NOT (SELECT go FROM fasttrun_giant_trace_gate) LOOP
    PERFORM pg_sleep(0.05);
  END LOOP;
END
\$gate\$;

-- Контрольный полный проход подтверждает, что трассировка видит
-- heap_getnext этого серверного процесса.
CREATE TEMP TABLE trace_control AS SELECT g FROM generate_series(1, 1000) g;
SET fasttrun.max_analyze_pages = 0;
SELECT 'CONTROL_READY';
SELECT pg_sleep(1);
SELECT fasttrun_analyze('trace_control');
SELECT 'CONTROL_DONE';
SELECT pg_sleep(1);

-- Прогон A: block-sampling.
SET fasttrun.max_analyze_pages = $SAMPLE_MAX_PAGES;
SELECT 'A_READY';
SELECT pg_sleep(1);
SELECT fasttrun_analyze('giant');
SELECT 'A_DONE';
SELECT pg_sleep(2);

-- Сброс кэша, прогон B: полный скан (порог 0).
SELECT fasttruncate('giant');
INSERT INTO giant SELECT g, g % 1000, repeat('x', 40)
FROM generate_series(1, $ROWS) g;
SET fasttrun.max_analyze_pages = 0;
SELECT 'B_READY';
SELECT pg_sleep(1);
SELECT fasttrun_analyze('giant');
SELECT 'B_DONE';
SELECT pg_sleep(2);
SQL

run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/repro.sql" \
	>"$WORKDIR/repro.out" 2>"$WORKDIR/repro.err" &
psql_pid=$!

backend_pid=""
for _ in $(seq 1 400); do
	backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/repro.out" 2>/dev/null | head -1 || true)
	[ -n "$backend_pid" ] && break
	sleep 0.05
done
[ -n "$backend_pid" ] || { wait "$psql_pid" || true; cat "$WORKDIR/repro."* >&2; echo "нет pid бэкенда" >&2; exit 1; }

for _ in $(seq 1 6000); do
	grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.out" && break
	kill -0 "$psql_pid" 2>/dev/null || break
	sleep 0.1
done
if ! grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.out"; then
	wait "$psql_pid" || true
	cat "$WORKDIR/repro.out" "$WORKDIR/repro.err" >&2
	echo "тестовая нагрузка не дошла до точки запуска трассировки" >&2
	exit 1
fi

total_blocks=$(awk '/^BLOCKS /{print $2}' "$WORKDIR/repro.out" | head -1)
total_blocks=${total_blocks:-0}
case "$total_blocks" in
	''|*[!0-9]*) echo "неверное число блоков: $total_blocks" >&2; exit 1 ;;
esac

# Считаем heap_getnext в бэкенде с интервальным флашем; снимаем дельты
# на границах A_READY..A_DONE и B_READY..B_DONE через shell-сэмплинг.
$SUDO timeout 300 "$BPFTRACE" -e "
BEGIN { printf(\"ATTACHED_READY\\n\"); }
uprobe:$PG_BINDIR/postgres:heap_getnext /pid == $backend_pid/ { @reads = count(); }
interval:ms:200 { print(@reads); }
" -o "$WORKDIR/reads.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 200); do
	if grep -q '^ATTACHED_READY$' "$WORKDIR/reads.bpf.out" 2>/dev/null; then
		break
	fi
	if ! kill -0 "$TRACE_PID" >/dev/null 2>&1; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^ATTACHED_READY$' "$WORKDIR/reads.bpf.out" 2>/dev/null || \
	! kill -0 "$TRACE_PID" >/dev/null 2>&1; then
	if [ -f "$WORKDIR/reads.bpf.out" ]; then
		cat "$WORKDIR/reads.bpf.out" >&2
	fi
	echo "FAIL: bpftrace не подтвердил подключение" >&2
	exit 1
fi
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	'UPDATE fasttrun_giant_trace_gate SET go = true' >/dev/null

snap() { awk '$1 == "@reads:" {gsub(/[^0-9]/,"",$2); print $2}' "$WORKDIR/reads.bpf.out" | tail -1 | tr -dc 0-9; }
wait_marker() {
	for _ in $(seq 1 6000); do
		grep -q "^$1\$" "$WORKDIR/repro.out" && return 0
		kill -0 "$psql_pid" 2>/dev/null || return 1
		sleep 0.1
	done
	return 1
}

wait_marker CONTROL_READY || { echo "контрольный полный проход не начался" >&2; exit 1; }
sleep 0.5; control_start=$(snap)
wait_marker CONTROL_DONE || { echo "контрольный полный проход не завершился" >&2; exit 1; }
sleep 0.5; control_end=$(snap)
wait_marker A_READY || { echo "выборка по блокам не началась" >&2; exit 1; }
sleep 0.5; a_start=$(snap)
wait_marker A_DONE  || { echo "выборка по блокам не завершилась" >&2; exit 1; }
sleep 0.5; a_end=$(snap)
wait_marker B_READY || { echo "полный проход не начался" >&2; exit 1; }
sleep 0.5; b_start=$(snap)
wait_marker B_DONE  || { echo "полный проход не завершился" >&2; exit 1; }
sleep 0.5; b_end=$(snap)

if ! wait "$psql_pid"; then
	cat "$WORKDIR/repro.out" "$WORKDIR/repro.err" >&2
	echo "SQL-сценарий большой временной таблицы завершился ошибкой" >&2
	exit 1
fi
$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
wait "$TRACE_PID" >/dev/null 2>&1 || true
TRACE_PID=""

control_start=${control_start:-0}; control_end=${control_end:-0}
a_start=${a_start:-0}; a_end=${a_end:-0}; b_start=${b_start:-0}; b_end=${b_end:-0}
control_reads=$(( control_end - control_start ))
a_reads=$(( a_end - a_start ))
b_reads=$(( b_end - b_start ))

cat <<METRICS

таблица: $ROWS строк, $total_blocks блоков
контрольный полный проход                         : heap_getnext = $control_reads
A выборка по блокам (max_analyze_pages=$SAMPLE_MAX_PAGES): heap_getnext = $a_reads
B полный проход    (max_analyze_pages=0)  : heap_getnext = $b_reads
порог: A <= B / $RATIO_DIVISOR
METRICS

failed=0
if [ "$control_reads" -le 0 ]; then
	echo "FAIL: трассировка не увидела контрольный полный проход" >&2
	failed=1
fi
if [ "$total_blocks" -le "$SAMPLE_MAX_PAGES" ]; then
	echo "FAIL: размер таблицы $total_blocks блоков не превышает max_analyze_pages=$SAMPLE_MAX_PAGES" >&2
	failed=1
fi
if [ "$b_reads" -lt $(( ROWS * 90 / 100 )) ]; then
	echo "FAIL: полный проход прочитал $b_reads строк, ожидалось не менее 90% от $ROWS" >&2
	failed=1
fi
if [ "$b_reads" -gt 0 ] && [ "$a_reads" -gt $(( b_reads / RATIO_DIVISOR )) ]; then
	echo "FAIL: выборка по блокам выполнила $a_reads heap_getnext, предел $(( b_reads / RATIO_DIVISOR ))" >&2
	failed=1
fi
[ "$failed" -ne 0 ] && exit 1

echo "проверка ограничения для большой временной таблицы прошла"
