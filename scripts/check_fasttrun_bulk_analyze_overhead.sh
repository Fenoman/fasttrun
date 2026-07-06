#!/usr/bin/env bash
#
# Regression-проверка для fasttrun.invalidate_threshold и
# fasttrun_analyze_bulk.  Считает, сколько раз fasttrun_analyze реально
# инвалидирует локальный plan-cache (uprobe на
# fasttrun_invalidate_local_plan_cache -- символ живёт в нашей .so и
# резолвится даже на stripped-бинаре PG).
#
# Надёжность измерения: bpftrace ставится ОДИН раз на .so ДО прогонов и
# ключует счётчик по pid (@ft_inval[pid]).  Каждый вариант -- отдельная
# psql-сессия со своим pid, поэтому счётчик атрибутируется по варианту
# без interval-сэмплинга и без гонок атача.
#
# Варианты:
#   A. single, threshold=0    -- каждый analyze инвалидирует (baseline)
#   B. single, threshold=0.2  -- под-пороговый дрейф не инвалидирует
#   C. bulk,   threshold=0    -- fasttrun_analyze_bulk, каждый инвалидирует
#   D. bulk,   threshold=0.2  -- bulk + порог
#
# Pass: baseline дал видимую работу (A>0), и порог срезал инвалидации в
# single (A->B) и в bulk (C->D) как минимум на MIN_CUT_PCT.  Амортизация
# прохода plan-cache внутри ядра (эффект bulk) -- отдельный core-символ
# PlanCacheRelCallback; если он резолвится (небинарь без strip / dbgsym),
# счётчик печатается справочно, иначе секция пропускается.
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
PORT=${PGPORT:-55439}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-bulk.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_bulk}
ITERATIONS=${ITERATIONS:-100}
TEMP_TABLES=${TEMP_TABLES:-19}
PER_ITER_ROWS=${PER_ITER_ROWS:-10}
BASE_ROWS=${BASE_ROWS:-10000}
MIN_CUT_PCT=${MIN_CUT_PCT:-30}
FASTTRUN_SO=${FASTTRUN_SO:-"$PG_PKGLIBDIR/fasttrun.so"}

BPF_PID=""
cleanup()
{
	if [ -n "$BPF_PID" ]; then
		$SUDO kill "$BPF_PID" >/dev/null 2>&1 || true
		wait "$BPF_PID" >/dev/null 2>&1 || true
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

[ -f "$FASTTRUN_SO" ] || { echo "не нашёл shared library fasttrun: $FASTTRUN_SO" >&2; exit 1; }

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $WORKDIR -p $PORT -c shared_preload_libraries=fasttrun -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -c "CREATE EXTENSION fasttrun" >/dev/null

# Резолвится ли core-символ PlanCacheRelCallback на этом бинаре?
PLAN_CB_OK=0
if $SUDO "$BPFTRACE" -l "uprobe:$PG_BINDIR/postgres:PlanCacheRelCallback" 2>/dev/null \
		| grep -q PlanCacheRelCallback; then
	PLAN_CB_OK=1
fi

# bpftrace-программа: счётчики по pid.  END печатает карты при выходе.
BPF_PROG="$WORKDIR/bulk.bt"
{
	echo "uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache { @ft_inval[pid] = count(); }"
	if [ "$PLAN_CB_OK" -eq 1 ]; then
		echo "uprobe:$PG_BINDIR/postgres:PlanCacheRelCallback { @plan_cb[pid] = count(); }"
	fi
	echo "END { print(@ft_inval);"
	[ "$PLAN_CB_OK" -eq 1 ] && echo "        print(@plan_cb);"
	echo "}"
} >"$BPF_PROG"

BPF_DUMP="$WORKDIR/bulk.out"
BPF_ERR="$WORKDIR/bulk.err"
$SUDO "$BPFTRACE" "$BPF_PROG" -o "$BPF_DUMP" 2>"$BPF_ERR" &
BPF_PID=$!

# Ждём, пока uprobes реально прицепятся (иначе первый вариант не сосчитается).
# bpftrace с -o пишет "Attaching N probes..." в сам dump-файл.
attached=0
for _ in $(seq 1 200); do
	if grep -qi "Attaching" "$BPF_DUMP" 2>/dev/null; then attached=1; break; fi
	if ! kill -0 "$BPF_PID" >/dev/null 2>&1; then break; fi
	sleep 0.1
done
if [ "$attached" -ne 1 ]; then
	echo "bpftrace не прицепился:" >&2; cat "$BPF_DUMP" "$BPF_ERR" >&2; exit 1
fi
sleep 1

# --- SQL одного варианта: своя сессия, свой pid ---
# Всё в ОДНОЙ транзакции (один DO-блок без COMMIT): порог invalidate
# срабатывает только пока column-stats не пересобираются, а через commit
# счётчики temp обнуляются и каждый analyze становится "холодным"
# (stats_recollected -> обход порога).  Внутри txn стата свежая.
# Таблица преднаполнена BASE_ROWS, затем добавляется по PER_ITER_ROWS:
# дрейф relstats на вызов ~PER_ITER_ROWS/BASE_ROWS (доли %), ниже 20%.
# threshold=0.2 не инвалидирует, threshold=0 инвалидирует на каждый вызов.
emit_variant_sql()
{
	local marker=$1 threshold=$2 mode=$3
	local sqlpath="$WORKDIR/variant_${marker}.sql"
	{
		echo "SELECT pg_backend_pid();"
		local t
		for t in $(seq 1 "$TEMP_TABLES"); do
			echo "CREATE TEMP TABLE ft_bt_${t} (id int, grp int, payload text);"
		done
		echo "SET fasttrun.invalidate_threshold = ${threshold};"
		cat <<VSQL
DO \$do\$
DECLARE i int; j int;
BEGIN
  -- prefill + холодный baseline (инвалидирует один раз на таблицу)
  FOR j IN 1..${TEMP_TABLES} LOOP
    EXECUTE format('INSERT INTO ft_bt_%s SELECT g, g %% 100, md5(g::text) FROM generate_series(1, ${BASE_ROWS}) g', j);
    EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_bt_'||j);
  END LOOP;
  -- измеряемый цикл в той же транзакции, без COMMIT
  FOR i IN 1..${ITERATIONS} LOOP
    FOR j IN 1..${TEMP_TABLES} LOOP
      EXECUTE format('INSERT INTO ft_bt_%s SELECT g, g %% 100, md5(g::text) FROM generate_series(1, ${PER_ITER_ROWS}) g', j);
    END LOOP;
VSQL
		if [ "$mode" = single ]; then
			cat <<VSQL
    FOR j IN 1..${TEMP_TABLES} LOOP
      EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_bt_'||j);
    END LOOP;
VSQL
		else
			cat <<VSQL
    DECLARE names text[] := ARRAY[]::text[];
    BEGIN
      FOR j IN 1..${TEMP_TABLES} LOOP names := names || ('ft_bt_'||j)::text; END LOOP;
      PERFORM fasttrun_analyze_bulk(VARIADIC names);
    END;
VSQL
		fi
		cat <<VSQL
  END LOOP;
END\$do\$;
SELECT '${marker}_DONE';
VSQL
	} >"$sqlpath"
}

emit_variant_sql A 0    single
emit_variant_sql B 0.2  single
emit_variant_sql C 0    bulk
emit_variant_sql D 0.2  bulk

declare -A vpid
run_variant()
{
	local v=$1
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 -f "$WORKDIR/variant_${v}.sql" \
		>"$WORKDIR/out_${v}" 2>"$WORKDIR/err_${v}"
	local pid
	pid=$(grep -E '^[0-9]+$' "$WORKDIR/out_${v}" | head -1 || true)
	if [ -z "$pid" ]; then
		echo "вариант $v: не прочитал pid бэкенда" >&2
		cat "$WORKDIR/out_${v}" "$WORKDIR/err_${v}" >&2
		exit 1
	fi
	vpid[$v]=$pid
}

for v in A B C D; do run_variant "$v"; done

# Останавливаем bpftrace -- END печатает карты по pid.
$SUDO kill "$BPF_PID" >/dev/null 2>&1 || true
wait "$BPF_PID" >/dev/null 2>&1 || true
BPF_PID=""
# Дать файлу дописаться.
for _ in $(seq 1 50); do grep -q '@ft_inval\[' "$BPF_DUMP" 2>/dev/null && break; sleep 0.1; done

# @ft_inval[<pid>]: <count>  ->  count для pid варианта
count_for()
{
	local map=$1 pid=$2
	awk -v p="$pid" -v m="$map" '
		$0 ~ m"\\["p"\\]:" { gsub(/[^0-9]/,"",$NF); print $NF; found=1 }
		END { if (!found) print 0 }' "$BPF_DUMP" | tail -1
}

A_inval=$(count_for @ft_inval "${vpid[A]}")
B_inval=$(count_for @ft_inval "${vpid[B]}")
C_inval=$(count_for @ft_inval "${vpid[C]}")
D_inval=$(count_for @ft_inval "${vpid[D]}")

echo "xact-итераций: $ITERATIONS, temp таблиц: $TEMP_TABLES, строк/итерацию: $PER_ITER_ROWS"
echo "PlanCacheRelCallback резолвится: $([ "$PLAN_CB_OK" -eq 1 ] && echo да || echo 'нет (stripped) -- core-метрика пропущена')"
echo
printf 'A (single, thr=0)   pid=%-7s invalidations=%s\n' "${vpid[A]}" "$A_inval"
printf 'B (single, thr=0.2) pid=%-7s invalidations=%s\n' "${vpid[B]}" "$B_inval"
printf 'C (bulk,   thr=0)   pid=%-7s invalidations=%s\n' "${vpid[C]}" "$C_inval"
printf 'D (bulk,   thr=0.2) pid=%-7s invalidations=%s\n' "${vpid[D]}" "$D_inval"

if [ "$PLAN_CB_OK" -eq 1 ]; then
	echo
	echo "plan_cache_walks (справочно):"
	for v in A B C D; do
		printf '  %s: %s\n' "$v" "$(count_for @plan_cb "${vpid[$v]}")"
	done
fi

failed=0
if [ "$A_inval" -le 0 ]; then
	echo "FAIL: baseline дал ноль инвалидаций -- uprobe на fasttrun.so не подцепился?" >&2
	exit 1
fi

cut_single=$(( (A_inval - B_inval) * 100 / A_inval ))
echo
echo "порог срезал инвалидации single (A->B): ${cut_single}%"
if [ "$C_inval" -gt 0 ]; then
	cut_bulk=$(( (C_inval - D_inval) * 100 / C_inval ))
	echo "порог срезал инвалидации bulk   (C->D): ${cut_bulk}%"
else
	cut_bulk=0
	echo "bulk baseline (C) = 0 -- пропуск проверки bulk-оси"
fi

if [ "$cut_single" -lt "$MIN_CUT_PCT" ]; then
	echo "FAIL: порог в single срезал всего ${cut_single}% (ждали >= ${MIN_CUT_PCT}%)" >&2
	failed=1
fi
if [ "$C_inval" -gt 0 ] && [ "$cut_bulk" -lt "$MIN_CUT_PCT" ]; then
	echo "FAIL: порог в bulk срезал всего ${cut_bulk}% (ждали >= ${MIN_CUT_PCT}%)" >&2
	failed=1
fi

[ "$failed" -ne 0 ] && exit 1
echo
echo "regression-проверка fasttrun bulk/threshold overhead прошла"
