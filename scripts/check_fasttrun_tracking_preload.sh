#!/bin/sh
#
# check_fasttrun_tracking_preload.sh - регрессионный тест трекинга с
# предзагрузкой.
#
# Тест fasttrun_tracking проверяет общий реестр учета CREATE TEMP TABLE и
# prewarm только в основной ветке: реестр есть лишь при shared_preload_libraries,
# без предзагрузки тест идет по запасной ветке и сверяется с
# expected/fasttrun_tracking_1.out. Обязательный cassert-прогон поднимает серверы
# без fasttrun в shared_preload_libraries, поэтому основную ветку гоняет только
# этот скрипт. Он поднимает свой кластер с предзагрузкой, запускает один тест
# через make installcheck и требует, чтобы вывод совпал с эталоном и в нем была
# строка основной ветки. Совпадение с запасным эталоном при предзагрузке -
# провал.
#
# Тот же кластер проверяет счетчики fasttrun_counters(): при предзагрузке они
# общие. Один сеанс вызывает fasttrun_analyze, другой должен увидеть этот вызов
# в сумме по кластеру.
# Расширение должно быть установлено в сборку PG_CONFIG. Скрипт печатает PASS
# или FAIL и не требует cassert-сборки.
#
set -e
# На macOS postmaster отказывается стартовать при неустановленной локали.
: "${LC_ALL:=C}"
export LC_ALL
unset PGOPTIONS
PGCONNECT_TIMEOUT=10
export PGCONNECT_TIMEOUT
PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
REPO=$(cd "$(dirname "$0")/.." && pwd)
WORKDIR=$(mktemp -d)
DATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/sock"
LOG="$WORKDIR/log"
OUT="$WORKDIR/regress"
PORT=${PGPORT_CHECK:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
# Строка вывода основной ветки. pg_regress печатает и исходный текст теста, где
# эта фраза тоже есть, поэтому нужна именно строка NOTICE.
MAIN_BRANCH='^NOTICE:  shmem available'

# Каталог удаляется только после подтвержденной остановки сервера. Иначе он
# остается для разбора, а проверка проваливается.
cleanup() {
	rc=$?
	if [ -f "$DATA/postmaster.pid" ] &&
		! "$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null 2>&1; then
		echo "tracking preload: сервер не остановился, каталог сохранен: $WORKDIR"
		exit 1
	fi
	# Сбой удаления не должен подменять исходный код выхода.
	rm -rf "$WORKDIR" || :
	exit "$rc"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR" "$OUT"
"$BINDIR/initdb" -D "$DATA" --no-locale -E UTF8 >/dev/null
cat >>"$DATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$SOCKET_DIR'
port = $PORT
shared_preload_libraries = 'fasttrun'
CONF
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -w start >/dev/null

rc=0
make -C "$REPO" -s installcheck PG_CONFIG="$PG_CONFIG" REGRESS=fasttrun_tracking \
	REGRESS_OPTS="--outputdir=$OUT" PGHOST="$SOCKET_DIR" PGPORT="$PORT" \
	>"$WORKDIR/installcheck.log" 2>&1 || rc=$?
result="$OUT/results/fasttrun_tracking.out"
main=$(grep -c "$MAIN_BRANCH" "$result" 2>/dev/null || true)
echo "installcheck rc=$rc main branch lines=${main:-0}"

PSQL="$BINDIR/psql -X -A -t -q -v ON_ERROR_STOP=1 -h $SOCKET_DIR -p $PORT -d postgres"
counters=$($PSQL -c "CREATE EXTENSION IF NOT EXISTS fasttrun" \
	-c "SELECT fasttrun_counters_reset()" \
	-c "CREATE TEMP TABLE ft_counter_probe (id int)" \
	-c "INSERT INTO ft_counter_probe SELECT generate_series(1, 100)" \
	-c "SELECT fasttrun_analyze('ft_counter_probe')" >/dev/null 2>&1 &&
	$PSQL -c "SELECT scope || ' ' || value FROM fasttrun_counters() WHERE counter = 'analyze_calls'" 2>&1) ||
	counters="error: $counters"
echo "cluster counters: $counters"

capacity=$($PSQL <<'SQL'
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
CREATE SCHEMA dummy_tmp;
CREATE TABLE dummy_tmp.ft_capacity_template (id int);
SELECT fasttrun_reset_temp_stats();
SELECT fasttrun_counters_reset();
\o /dev/null
SELECT format('CREATE TEMP TABLE ft_capacity_%s (LIKE dummy_tmp.ft_capacity_template); DROP TABLE ft_capacity_%s;', n, n)
FROM generate_series(1, 8400) n
\gexec
CREATE TEMP TABLE ft_capacity_1 (LIKE dummy_tmp.ft_capacity_template);
DROP TABLE ft_capacity_1;
\o
SELECT 'capacity ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(0)) || ' ' ||
       (SELECT value FROM fasttrun_counters() WHERE counter = 'tracking_rejected') || ' ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(32)) || ' ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(8192)) || ' ' ||
       (SELECT create_count FROM fasttrun_hot_temp_tables(0) WHERE relname = 'ft_capacity_1');
SQL
) || capacity="error: $capacity"
echo "$capacity"

"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -m fast -w restart >/dev/null
restored=$($PSQL -c "SELECT count(*) || ' ' || max(create_count) FROM fasttrun_hot_temp_tables(0)") ||
	restored="error: $restored"
echo "capacity restored: $restored"

$PSQL -c 'SELECT fasttrun_reset_temp_stats()' >/dev/null
"$BINDIR/createdb" -h "$SOCKET_DIR" -p "$PORT" -T template0 -E LATIN1 --locale=C ft_tracking_latin1
$PSQL -d ft_tracking_latin1 <<'SQL'
CREATE EXTENSION fasttrun;
CREATE SCHEMA dummy_tmp;
CREATE TABLE dummy_tmp.encoding_template(id int);
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
DO $test$ DECLARE name text := 'ft_encoded_' || convert_from(decode('e9','hex'),'LATIN1');
BEGIN
  EXECUTE format('CREATE TEMP TABLE %I(LIKE dummy_tmp.encoding_template)', name);
END $test$;
SQL
encoding=$($PSQL <<'SQL'
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
CREATE TEMP TABLE ft_ascii(LIKE dummy_tmp.ft_capacity_template);
DO $test$ DECLARE name text := 'ft_utf8_' || convert_from(decode('c3a9','hex'),'UTF8');
BEGIN
  EXECUTE format('CREATE TEMP TABLE %I(LIKE dummy_tmp.ft_capacity_template)', name);
END $test$;
SELECT 'encoding ' || (count(*) = 3 AND sum(create_count) = 3
  AND bool_and(octet_length(convert_to(relname, 'LATIN1')) > 0)
  AND bool_or(relname = E'\\x66745f656e636f6465645fe9')
  AND bool_or(relname = 'ft_ascii')
  AND bool_or(relname = 'ft_utf8_' || convert_from(decode('c3a9','hex'),'UTF8')))
FROM fasttrun_hot_temp_tables(0);
SQL
) || encoding="error: $encoding"
echo "$encoding"

if [ "$rc" -eq 0 ] && [ "${main:-0}" -ge 1 ] && [ "$counters" = "cluster 1" ]; then
	if [ "$(printf '%s\n' "$capacity" | tail -1)" = "capacity 8192 208 32 8192 2" ] &&
		[ "$restored" = "8192 2" ] && [ "$encoding" = 'encoding true' ]; then
		echo "tracking preload: PASS"
		exit 0
	fi
fi
echo "tracking preload: FAIL"
tail -5 "$WORKDIR/installcheck.log" || true
head -40 "$OUT/regression.diffs" 2>/dev/null || true
grep -n "shmem" "$result" 2>/dev/null || true
exit 1
