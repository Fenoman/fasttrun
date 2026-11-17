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
SUDO=${SUDO-sudo}
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
	command -v "$1" >/dev/null 2>&1 || { echo "не нашёл команду: $1" >&2; exit 1; }
}
require_cmd "$PSQL"; require_cmd "$INITDB"; require_cmd "$PG_CTL"
require_cmd "$CREATEDB"; require_cmd "$BPFTRACE"; require_cmd stdbuf
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
-- heap_getnext этого серверного процесса. Границы фаз переключает синхронный
-- uprobe на pg_backend_pid() внутри самого трассера без гонок со sleep:
--   phase 1 = control, phase 2 = A (block-sampling), phase 3 = reload,
--   phase 4 = B (full-scan), phase 5 = завершение сценария.
CREATE TEMP TABLE trace_control AS SELECT g FROM generate_series(1, 1000) g;
SET fasttrun.max_analyze_pages = 0;
SELECT pg_backend_pid() \g /dev/null
SELECT fasttrun_analyze('trace_control');

-- Прогон A: block-sampling.
SET fasttrun.max_analyze_pages = $SAMPLE_MAX_PAGES;
SELECT pg_backend_pid() \g /dev/null
SELECT fasttrun_analyze('giant');

-- Сброс кэша (phase 3), затем прогон B: полный скан (phase 4, порог 0).
SELECT pg_backend_pid() \g /dev/null
SELECT fasttruncate('giant');
INSERT INTO giant SELECT g, g % 1000, repeat('x', 40)
FROM generate_series(1, $ROWS) g;
SET fasttrun.max_analyze_pages = 0;
SELECT pg_backend_pid() \g /dev/null
SELECT fasttrun_analyze('giant');
SELECT pg_backend_pid() \g /dev/null
SELECT 'WORKLOAD_DONE';
SELECT pg_sleep(1);
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
[ -n "$backend_pid" ] || { terminate_child "$psql_pid"; cat "$WORKDIR/repro."* >&2; echo "нет pid бэкенда" >&2; exit 1; }

for _ in $(seq 1 6000); do
	grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.out" && break
	kill -0 "$psql_pid" 2>/dev/null || break
	sleep 0.1
done
if ! grep -q '^WORKLOAD_READY$' "$WORKDIR/repro.out"; then
	terminate_child "$psql_pid"
	cat "$WORKDIR/repro.out" "$WORKDIR/repro.err" >&2
	echo "тестовая нагрузка не дошла до точки запуска трассировки" >&2
	exit 1
fi

total_blocks=$(awk '/^BLOCKS /{print $2}' "$WORKDIR/repro.out" | head -1)
total_blocks=${total_blocks:-0}
case "$total_blocks" in
	''|*[!0-9]*) echo "неверное число блоков: $total_blocks" >&2; exit 1 ;;
esac

# Фазы переключает синхронный вызов pg_backend_pid() в том же серверном процессе,
# поэтому каждое чтение heap_getnext попадает строго в корзину своей фазы.
$SUDO stdbuf -oL "$BPFTRACE" -e "
BEGIN { @phase = 0; printf(\"ATTACHED_READY\\n\"); }
uprobe:$PG_BINDIR/postgres:pg_backend_pid /pid == $backend_pid/ { @phase = @phase + 1; }
uprobe:$PG_BINDIR/postgres:heap_getnext /pid == $backend_pid && @phase > 0/ { @reads[@phase] = count(); }
END { print(@phase); print(@reads); printf(\"BPFTRACE_END\\n\"); }
" >"$WORKDIR/reads.bpf.out" 2>&1 &
TRACE_PID=$!

for _ in $(seq 1 400); do
	if grep -q '^ATTACHED_READY$' "$WORKDIR/reads.bpf.out" 2>/dev/null; then
		break
	fi
	sleep 0.05
done
if ! grep -q '^ATTACHED_READY$' "$WORKDIR/reads.bpf.out" 2>/dev/null; then
	if [ -f "$WORKDIR/reads.bpf.out" ]; then
		cat "$WORKDIR/reads.bpf.out" >&2
	fi
	echo "FAIL: bpftrace не подтвердил подключение" >&2
	exit 1
fi
run_pg "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	'UPDATE fasttrun_giant_trace_gate SET go = true' >/dev/null

wait_marker() {
	for _ in $(seq 1 6000); do
		grep -q "^$1\$" "$WORKDIR/repro.out" && return 0
		kill -0 "$psql_pid" 2>/dev/null || return 1
		sleep 0.1
	done
	return 1
}

wait_marker WORKLOAD_DONE || {
	terminate_child "$psql_pid"
	cat "$WORKDIR/repro.out" "$WORKDIR/repro.err" >&2
	echo "SQL-сценарий большой временной таблицы не дошел до WORKLOAD_DONE" >&2
	exit 1
}

if ! wait_bounded "$psql_pid"; then
	cat "$WORKDIR/repro.out" "$WORKDIR/repro.err" >&2
	echo "SQL-сценарий большой временной таблицы завершился ошибкой" >&2
	exit 1
fi
$SUDO kill -INT "$TRACE_PID" >/dev/null 2>&1 || true
trace_rc=0
wait_bounded "$TRACE_PID" "" 1 || trace_rc=$?
TRACE_PID=""
if [ "$trace_rc" -ne 0 ] && [ "$trace_rc" -ne 130 ]; then
	cat "$WORKDIR/reads.bpf.out" >&2
	echo "FAIL: bpftrace завершился с кодом $trace_rc" >&2
	exit 1
fi
if ! grep -q '^BPFTRACE_END$' "$WORKDIR/reads.bpf.out"; then
	cat "$WORKDIR/reads.bpf.out" >&2
	echo "FAIL: bpftrace не напечатал финальный маркер BPFTRACE_END" >&2
	exit 1
fi

phase_reads() {
	val=$(awk -v k="@reads[$1]:" '$1 == k {gsub(/[^0-9]/,"",$2); print $2}' "$WORKDIR/reads.bpf.out" | tail -1)
	printf '%s' "${val:-0}"
}
end_phase=$(awk '$1 == "@phase:" {gsub(/[^0-9]/,"",$2); print $2}' "$WORKDIR/reads.bpf.out" | tail -1)
end_phase=${end_phase:-0}
if [ "$end_phase" -ne 5 ]; then
	cat "$WORKDIR/reads.bpf.out" >&2
	echo "FAIL: трассировка завершилась на фазе $end_phase вместо 5 (неполное окно)" >&2
	exit 1
fi

control_reads=$(phase_reads 1)
a_reads=$(phase_reads 2)
b_reads=$(phase_reads 4)

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
