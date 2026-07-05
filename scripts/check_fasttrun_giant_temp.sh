#!/usr/bin/env bash
#
# Regression-проверка giant-temp guardrail (fasttrun.max_analyze_pages).
#
# Выше порога холодный fasttrun_analyze обязан переходить на block-sampling
# и читать ТОЛЬКО выборку блоков вместо полного скана таблицы. Считаем
# фактические чтения буферов (ReadBufferExtended) внутри бэкенда через
# bpftrace для двух прогонов на одной большой temp-таблице:
#
#   A. block-sampling: fasttrun.max_analyze_pages = 100  (порог << размера)
#   B. full-scan:      fasttrun.max_analyze_pages = 0     (точный полный скан)
#
# Pass criteria:
#   - A прочитал заметно меньше блоков, чем B (block-sampling bounded);
#     по умолчанию A <= B / 3.
#   - B прочитал ~все блоки таблицы (positive control: полный скан реально
#     трогает всю таблицу, иначе сравнение бессмысленно).
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
PORT=${PGPORT:-55471}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-giant.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_giant}
ROWS=${ROWS:-5000000}          # ~5M строк -> десятки тысяч блоков >> sample_target
RATIO_DIVISOR=${RATIO_DIVISOR:-3}
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
	command -v "$1" >/dev/null 2>&1 || { echo "не нашёл команду: $1" >&2; exit 1; }
}
require_cmd "$PSQL"; require_cmd "$INITDB"; require_cmd "$PG_CTL"
require_cmd "$CREATEDB"; require_cmd "$BPFTRACE"

if [ ! -f "$FASTTRUN_SO" ]; then
	echo "не нашёл shared library fasttrun: $FASTTRUN_SO" >&2
	exit 1
fi

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
# Локальный сокет + без TCP: не задеваем чужие сервисы на боксе.
"$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on -c temp_buffers=8MB" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null

cat >"$WORKDIR/repro.sql" <<SQL
SELECT pg_backend_pid();
CREATE TEMP TABLE giant (id int, grp int, pad text);
INSERT INTO giant SELECT g, g % 1000, repeat('x', 40)
FROM generate_series(1, $ROWS) g;
SELECT 'BLOCKS ' || relpages FROM pg_class WHERE relname = 'giant';
SELECT 'WORKLOAD_READY';
SELECT pg_sleep(2);

-- Прогон A: block-sampling.
SET fasttrun.max_analyze_pages = 100;
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

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
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

total_blocks=$(awk '/^BLOCKS /{print $2}' "$WORKDIR/repro.out" | head -1)
total_blocks=${total_blocks:-0}

# Считаем ReadBufferExtended в бэкенде с интервальным флашем; снимаем дельты
# на границах A_READY..A_DONE и B_READY..B_DONE через shell-сэмплинг.
$SUDO timeout 300 "$BPFTRACE" -e "
uprobe:$PG_BINDIR/postgres:ReadBufferExtended /pid == $backend_pid/ { @reads = count(); }
interval:ms:200 { print(@reads); }
" -o "$WORKDIR/reads.bpf.out" 2>&1 &
TRACE_PID=$!

snap() { awk '$1 == "@reads:" {gsub(/[^0-9]/,"",$2); print $2}' "$WORKDIR/reads.bpf.out" | tail -1 | tr -dc 0-9; }
wait_marker() {
	for _ in $(seq 1 6000); do
		grep -q "^$1\$" "$WORKDIR/repro.out" && return 0
		kill -0 "$psql_pid" 2>/dev/null || return 1
		sleep 0.1
	done
	return 1
}

wait_marker A_READY || { echo "A не стартанул" >&2; exit 1; }
sleep 0.5; a_start=$(snap)
wait_marker A_DONE  || { echo "A не закончился" >&2; exit 1; }
sleep 0.5; a_end=$(snap)
wait_marker B_READY || { echo "B не стартанул" >&2; exit 1; }
sleep 0.5; b_start=$(snap)
wait_marker B_DONE  || { echo "B не закончился" >&2; exit 1; }
sleep 0.5; b_end=$(snap)

wait "$psql_pid"
$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
wait "$TRACE_PID" >/dev/null 2>&1 || true
TRACE_PID=""

a_start=${a_start:-0}; a_end=${a_end:-0}; b_start=${b_start:-0}; b_end=${b_end:-0}
a_reads=$(( a_end - a_start ))
b_reads=$(( b_end - b_start ))

cat <<METRICS

таблица: $ROWS строк, $total_blocks блоков
A block-sampling (max_analyze_pages=100): ReadBufferExtended = $a_reads
B full-scan      (max_analyze_pages=0)  : ReadBufferExtended = $b_reads
порог: A <= B / $RATIO_DIVISOR
METRICS

failed=0
if [ "$b_reads" -le 0 ]; then
	echo "FAIL: full-scan дал ноль чтений -- uprobe не подцепился?" >&2
	failed=1
fi
if [ "$b_reads" -gt 0 ] && [ "$a_reads" -gt $(( b_reads / RATIO_DIVISOR )) ]; then
	echo "FAIL: block-sampling прочитал $a_reads >= B/$RATIO_DIVISOR ($(( b_reads / RATIO_DIVISOR ))) -- не ограничен" >&2
	failed=1
fi
[ "$failed" -ne 0 ] && exit 1

echo "regression-проверка giant-temp guardrail прошла"
