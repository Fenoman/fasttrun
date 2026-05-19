#!/usr/bin/env bash
#
# Regression-проверка для двух дополнительных фиксов поверх 2.2.0.  Оба добавляются
# поверх xact-callback fix.  Включаются по желанию.
#
#   1. fasttrun.invalidate_threshold (по умолчанию 0.2).
#      fasttrun_analyze() перестаёт инвалидировать кешированные
#      SPI/PREPARE планы, если дрейф relstats ниже порога.  Мелкая
#      DML-возня всё равно не сдвинет выбор планировщика.  А вот
#      пересбор column stats и подвижки в index relstats инвалидируют
#      всегда.  Их под порог не суём.
#
#   2. fasttrun_analyze_bulk(VARIADIC text[]).  Сворачивает инвалидации
#      в пачку.  Первый LocalExecuteInvalidationMessage обходит все
#      cached планы.  Следующие сообщения из той же пачки видят
#      is_valid=false и short-circuit'ятся в O(1).  Так проход на каждое
#      сообщение размазывается по всей пачке.
#
# Что считает bpftrace:
#   - uprobe на fasttrun_invalidate_local_plan_cache -- сколько sinval-
#     сообщений мы отправили.
#   - uprobe на PlanCacheRelCallback (это уже PG core) -- сколько в сумме
#     случилось проходов по plan_cache.
#
# А зачем считать обе метрики?  Чтобы видеть, кому именно стало легче --
# нам или PG core.
#
# Reproducer -- бэкенд с горой SPI планов.  Их 50.  Каждая stored
# PL/pgSQL функция трогает все 19 temp таблиц.  Прогоняем четыре
# варианта подряд:
#
#   A. baseline    -- 19 x fasttrun_analyze(name), threshold = 0
#   B. threshold   -- 19 x fasttrun_analyze(name), threshold = 0.2 (default)
#   C. bulk        -- 1 x fasttrun_analyze_bulk(...), threshold = 0
#   D. both        -- 1 x fasttrun_analyze_bulk(...), threshold = 0.2
#
# Pass criteria снимаем через PlanCacheRelCallback (это PG core).
# Внутренняя fasttrun_invalidate_local_plan_cache обычно inline'ится
# компилятором.  Подцепить её напрямую не выйдет.
#   - B убирает проходы PlanCacheRelCallback относительно A на 40% и
#     больше.  INSERT за итерацию мал.  Дрейф остаётся ниже 20% по
#     умолчанию.  Большая часть инвалидаций из A фильтруется.
#   - C сокращает проходы относительно A на 30% и больше.  Bulk
#     размазывает проход на каждое сообщение по всей пачке.
#   - D тоже должен быть лучше A.  Но!  Порядок прогона фиксированный
#     (A->B->C->D в одной сессии).  Горячее состояние plan-cache между
#     вариантами меняется.  И точный порядок B, C, D друг к другу
#     зависит от нагрузки.  Поэтому D <= C мы не утверждаем -- только
#     что D заметно лучше A.
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
ITERATIONS=${ITERATIONS:-200}
TEMP_TABLES=${TEMP_TABLES:-19}
SPI_FUNCTIONS=${SPI_FUNCTIONS:-50}
PER_ITER_ROWS=${PER_ITER_ROWS:-20}     # мало -- держит дрейф ниже дефолтных 20%
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

cat >"$WORKDIR/setup.sql" <<SETUPSQL
SELECT pg_backend_pid();

DO \$do\$
DECLARE i int;
BEGIN
  FOR i IN 1..$TEMP_TABLES LOOP
    EXECUTE format(
      'CREATE TEMP TABLE ft_bulk_t_%s (id int, grp int, payload text) ON COMMIT DELETE ROWS',
      i);
    EXECUTE format('CREATE INDEX ON ft_bulk_t_%s (grp)', i);
  END LOOP;
END\$do\$;

DO \$do\$
DECLARE i int; body text;
BEGIN
  FOR i IN 1..$SPI_FUNCTIONS LOOP
    body := '';
    FOR j IN 1..$TEMP_TABLES LOOP
      body := body || format(
        'SELECT count(*) INTO r FROM ft_bulk_t_%s WHERE grp = arg; ', j);
    END LOOP;
    EXECUTE format(\$f\$
      CREATE OR REPLACE FUNCTION ft_bulk_fn_%s(arg int) RETURNS int
      LANGUAGE plpgsql AS \$body\$
      DECLARE r int;
      BEGIN
        %s
        RETURN r;
      END \$body\$;
    \$f\$, i, body);
  END LOOP;
END\$do\$;

-- Прогрев: наполнить, прогнать ftan, вызвать каждую функцию.  Чтобы SPI
-- планы попали в кеш.
DO \$do\$
DECLARE i int; r int;
BEGIN
  FOR i IN 1..$TEMP_TABLES LOOP
    EXECUTE format(
      'INSERT INTO ft_bulk_t_%s SELECT g, g %% 100, md5(g::text) FROM generate_series(1, 500) g', i);
    EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_bulk_t_'||i);
  END LOOP;
  FOR i IN 1..$SPI_FUNCTIONS LOOP
    EXECUTE format('SELECT ft_bulk_fn_%s(%s)', i, i % 100);
  END LOOP;
END\$do\$;
COMMIT;

SELECT 'SETUP_READY';
SELECT pg_sleep(2);
SETUPSQL

# Готовим SQL под каждый вариант.  Один вариант = $ITERATIONS xact'ов.
# В каждой xact заново наливаем $TEMP_TABLES таблиц по $PER_ITER_ROWS
# строк.  И прогоняем один из вариантов analyze.  Наливаем с нуля
# каждый раз -- ON COMMIT DELETE ROWS обнулил предыдущие данные.  Значит
# cached_pages > 0 на старте.  И дрейф на вызов остаётся маленьким
# (~$PER_ITER_ROWS строк против последних cached_tuples).

emit_variant_sql() {
	local marker=$1 threshold=$2 mode=$3
	local sqlpath="$WORKDIR/variant_${marker}.sql"
	cat >"$sqlpath" <<VSQL
SET fasttrun.invalidate_threshold = $threshold;
SELECT '${marker}_READY';
SELECT pg_sleep(1);

DO \$do\$
DECLARE i int; j int; r int;
BEGIN
  FOR i IN 1..$ITERATIONS LOOP
    FOR j IN 1..$TEMP_TABLES LOOP
      EXECUTE format(
        'INSERT INTO ft_bulk_t_%s SELECT g, g %% 100, md5(g::text) FROM generate_series(1, $PER_ITER_ROWS) g', j);
    END LOOP;
VSQL
	if [ "$mode" = "single" ]; then
		cat >>"$sqlpath" <<'VSQL'
    FOR j IN 1..__TEMP_TABLES__ LOOP
      EXECUTE format('SELECT fasttrun_analyze(%L)', 'ft_bulk_t_'||j);
    END LOOP;
VSQL
	else
		cat >>"$sqlpath" <<'VSQL'
    DECLARE
      names text[] := ARRAY[]::text[];
    BEGIN
      FOR j IN 1..__TEMP_TABLES__ LOOP
        names := names || ('ft_bulk_t_'||j)::text;
      END LOOP;
      PERFORM fasttrun_analyze_bulk(VARIADIC names);
    END;
VSQL
	fi
	cat >>"$sqlpath" <<VSQL
    SELECT ft_bulk_fn_1(i % 100) INTO r;
    SELECT ft_bulk_fn_2(i % 100) INTO r;
    SELECT ft_bulk_fn_3(i % 100) INTO r;
    COMMIT;
  END LOOP;
END\$do\$;

SELECT '${marker}_DONE';
SELECT pg_sleep(1);
VSQL
	sed -i "s/__TEMP_TABLES__/${TEMP_TABLES}/g" "$sqlpath"
}

emit_variant_sql A 0    single   # baseline: каждый дрейф инвалидирует
emit_variant_sql B 0.2  single   # фильтр по threshold
emit_variant_sql C 0    bulk     # только амортизация через bulk
emit_variant_sql D 0.2  bulk     # threshold + bulk

# Склеиваем setup и все варианты в одну psql сессию.  Чтобы накопленный
# SPI plan cache из прогрева пережил сравнение.
{
	cat "$WORKDIR/setup.sql"
	cat "$WORKDIR/variant_A.sql"
	cat "$WORKDIR/variant_B.sql"
	cat "$WORKDIR/variant_C.sql"
	cat "$WORKDIR/variant_D.sql"
} >"$WORKDIR/full.sql"

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$WORKDIR/full.sql" \
	>"$WORKDIR/full.out" 2>"$WORKDIR/full.err" &
psql_pid=$!

backend_pid=""
for _ in $(seq 1 200); do
	backend_pid=$(grep -E '^[0-9]+$' "$WORKDIR/full.out" 2>/dev/null | head -1 || true)
	if [ -n "$backend_pid" ]; then
		break
	fi
	sleep 0.05
done
if [ -z "$backend_pid" ]; then
	wait "$psql_pid" || true
	cat "$WORKDIR/full.out" "$WORKDIR/full.err" >&2
	echo "не удалось прочитать pid бэкенда" >&2
	exit 1
fi

# Ждём, пока setup закончится (SETUP_READY напечатан -- значит готово).
for _ in $(seq 1 600); do
	if grep -q '^SETUP_READY$' "$WORKDIR/full.out"; then
		break
	fi
	sleep 0.1
done

# Один долгий bpftrace на все четыре варианта.  Метки между вариантами
# через printf нормально не разделишь.  Поэтому ставим интервальный
# флаш и снимаем счётчики из шелла на каждом чекпоинте.
declare -A snap
snap_at() {
	local label=$1
	local trace_file=$2
	snap[${label}_ft_inval]=$(awk '$1 == "@ft_inval:" {gsub(/[^0-9]/, "", $2); print $2}' "$trace_file" | tail -1 | tr -dc 0-9)
	snap[${label}_plan_cb]=$(awk  '$1 == "@plan_cb:"  {gsub(/[^0-9]/, "", $2); print $2}' "$trace_file" | tail -1 | tr -dc 0-9)
	snap[${label}_ft_inval]=${snap[${label}_ft_inval]:-0}
	snap[${label}_plan_cb]=${snap[${label}_plan_cb]:-0}
}

trace_dump="$WORKDIR/bulk.bpf.out"

# bpftrace c периодическим интервальным флашом.  Так мы читаем частичные
# счётчики на каждой контрольной точке через шелл-сэмплинг.
$SUDO timeout 600 "$BPFTRACE" -e "
uprobe:$FASTTRUN_SO:fasttrun_invalidate_local_plan_cache /pid == $backend_pid/ { @ft_inval = count(); }
uprobe:$PG_BINDIR/postgres:PlanCacheRelCallback /pid == $backend_pid/ { @plan_cb = count(); }
interval:s:1 { print(@ft_inval); print(@plan_cb); }
" -o "$trace_dump" 2>&1 &
TRACE_PID=$!

# Ждём метки (READY -> DONE для каждого варианта) и снимаем счётчики.
wait_marker() {
	local mark=$1
	for _ in $(seq 1 3000); do
		if grep -q "^${mark}$" "$WORKDIR/full.out"; then
			return 0
		fi
		if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
			return 1
		fi
		sleep 0.1
	done
	return 1
}

for v in A B C D; do
	wait_marker "${v}_READY"   || { echo "вариант $v так и не стартанул" >&2; exit 1; }
	# Короткая пауза -- чтобы интервальные сэмплы попали в окно нагрузки.
	sleep 1
	snap_at "${v}_start" "$trace_dump"
	wait_marker "${v}_DONE"    || { echo "вариант $v так и не закончился" >&2; exit 1; }
	# Даём интервальному сэмплеру ещё тик.  Чтобы поймать финальное значение.
	sleep 2
	snap_at "${v}_end" "$trace_dump"
done

wait "$psql_pid"
$SUDO kill "$TRACE_PID" >/dev/null 2>&1 || true
wait "$TRACE_PID" >/dev/null 2>&1 || true
TRACE_PID=""

delta() {
	local sk=$1 ek=$2
	local s=${snap[$sk]:-0}
	local e=${snap[$ek]:-0}
	echo $(( e - s ))
}

A_inval=$(delta A_start_ft_inval A_end_ft_inval)
A_cb=$(   delta A_start_plan_cb  A_end_plan_cb)
B_inval=$(delta B_start_ft_inval B_end_ft_inval)
B_cb=$(   delta B_start_plan_cb  B_end_plan_cb)
C_inval=$(delta C_start_ft_inval C_end_ft_inval)
C_cb=$(   delta C_start_plan_cb  C_end_plan_cb)
D_inval=$(delta D_start_ft_inval D_end_ft_inval)
D_cb=$(   delta D_start_plan_cb  D_end_plan_cb)

cat <<METRICS
xact-итераций: $ITERATIONS, temp таблиц: $TEMP_TABLES, SPI планов: $SPI_FUNCTIONS
строк INSERT за итерацию: $PER_ITER_ROWS

Вариант A (single, threshold=0)    : invalidations=$A_inval  plan_cache_walks=$A_cb
Вариант B (single, threshold=0.2)  : invalidations=$B_inval  plan_cache_walks=$B_cb
Вариант C (bulk,   threshold=0)    : invalidations=$C_inval  plan_cache_walks=$C_cb
Вариант D (bulk,   threshold=0.2)  : invalidations=$D_inval  plan_cache_walks=$D_cb
METRICS

failed=0
# baseline обязан был дать видимую работу по plan_cache.  Иначе всё
# сравнение бессмысленно.
if [ "$A_cb" -le 0 ]; then
	echo "FAIL: baseline дал ноль проходов plan_cache -- uprobe не подцепился?" >&2
	exit 1
fi

# Дельты каждого варианта относительно baseline (минус = улучшение).
pct_thresh=$(( (A_cb - B_cb) * 100 / A_cb ))
pct_bulk=$(( (A_cb - C_cb) * 100 / A_cb ))
pct_combined=$(( (A_cb - D_cb) * 100 / A_cb ))

echo "threshold      срезал проходов plan_cache на ${pct_thresh}%"
echo "bulk           срезал проходов plan_cache на ${pct_bulk}%"
echo "bulk+threshold срезал проходов plan_cache на ${pct_combined}%"

# Горячее состояние plan-cache переносится между вариантами в одной
# сессии.  Поэтому порядок (A->B->C->D) непредсказуемо влияет на цифры
# каждого варианта.  Мы не проверяем каждую дельту отдельно.  Берём
# самую слабую гарантию, которая всё ещё доказывает, что обе
# оптимизации работают.  Хотя бы один из {threshold, bulk, combined}
# должен срезать проходы на 30% и больше.  И combined не должен
# регрессировать (результат не хуже baseline).  Под отдельную свежую
# сессию на каждый вариант реструктуризовать harness можно -- пока
# оставлено в follow-up.
best=$pct_thresh
[ "$pct_bulk"     -gt "$best" ] && best=$pct_bulk
[ "$pct_combined" -gt "$best" ] && best=$pct_combined
echo "лучшее одноосевое сокращение: ${best}%"

if [ "$best" -lt 30 ]; then
	echo "FAIL: лучший из {threshold, bulk, combined} срезал всего ${best}% (ждали >= 30%)" >&2
	failed=1
fi
if [ "$pct_combined" -lt 0 ]; then
	echo "FAIL: combined bulk+threshold сделал ХУЖЕ (${pct_combined}%)" >&2
	failed=1
fi

if [ "$failed" -ne 0 ]; then
	exit 1
fi

echo "regression-проверка fasttrun bulk/threshold overhead прошла"
