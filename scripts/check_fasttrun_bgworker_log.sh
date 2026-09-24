#!/bin/sh
#
# check_fasttrun_bgworker_log.sh - ленивая загрузка в фоновом воркере.
#
# session_preload_libraries обрабатывают только клиентские сеансы, поэтому
# фоновый воркер (pg_background, воркер логической репликации) загружает
# fasttrun по первому вызову, и учет по колонкам в нем выключен. Клиентскому
# сеансу об этом один раз пишется LOG. У короткоживущих воркеров та же строка
# повторялась бы тысячи раз в сутки, а совет про session_preload_libraries им не
# помогает. Поэтому у фонового воркера сообщение идет уровнем DEBUG1 и советует
# shared_preload_libraries.
#
# Фоновый воркер здесь - воркер применения логической репликации: он исполняет
# триггер ENABLE ALWAYS со сбором статистики временной таблицы и загружает
# библиотеку под активным снимком, как воркер pg_background. Клиентский сеанс
# в той же базе служит положительным контролем: строка LOG у него обязана
# остаться. Сервер пишет журнал с log_min_messages=debug1, иначе отсутствие
# строки LOG у воркера ничего бы не доказывало.
# Скрипт печатает PASS или FAIL и не требует cassert-сборки.
#
set -e
# На macOS postmaster отказывается стартовать при неустановленной локали.
: "${LC_ALL:=C}"
export LC_ALL
# Предзагрузка из окружения сделала бы контрольный клиентский сеанс ранним.
unset PGOPTIONS
# Ограничено только подключение: зависшее соединение не держит проверку
# вечно, а время самих запросов этим не ограничивается.
PGCONNECT_TIMEOUT=10
export PGCONNECT_TIMEOUT
PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
WORKDIR=$(mktemp -d)
DATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/sock"
LOG="$WORKDIR/log"
PORT=${PGPORT_CHECK:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
MSG='fasttrun: per-column UPDATE accounting is off in this session'
HINT='This process does not load session_preload_libraries, so add fasttrun to shared_preload_libraries.'

# Журнал сервера живет во временном каталоге, поэтому при любом ненулевом
# выходе, в том числе досрочном по set -e, его ошибки печатаются до очистки.
show_errors() {
	grep -E '\|(ERROR|FATAL|PANIC):|TRAP' "$LOG" 2>/dev/null | tail -20 || true
}

cleanup() {
	rc=$?
	[ "$rc" -eq 0 ] || show_errors
	"$BINDIR/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1 || true
	# Сбой очистки не должен подменять исходный код выхода.
	rm -rf "$WORKDIR" || :
	exit "$rc"
}
trap cleanup EXIT

run_psql() {
	"$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -XAtq -v ON_ERROR_STOP=1 "$@"
}

mkdir -p "$SOCKET_DIR"
"$BINDIR/initdb" -D "$DATA" --no-locale -E UTF8 >/dev/null
cat >>"$DATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$SOCKET_DIR'
port = $PORT
wal_level = logical
max_wal_senders = 4
max_replication_slots = 4
log_min_messages = debug1
log_line_prefix = '%b|'
CONF
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -w start >/dev/null
"$BINDIR/createdb" -h "$SOCKET_DIR" -p "$PORT" fasttrun_pub
"$BINDIR/createdb" -h "$SOCKET_DIR" -p "$PORT" fasttrun_sub

run_psql -d fasttrun_pub >/dev/null <<'SQL'
CREATE TABLE src (id int PRIMARY KEY);
CREATE PUBLICATION fasttrun_pub FOR TABLE src;
SELECT 1 FROM pg_create_logical_replication_slot('fasttrun_slot', 'pgoutput');
SQL

# Триггер в базе подписчика: сбор идет в воркере применения. У воркера
# пустой search_path, поэтому имена квалифицированы.
run_psql -d fasttrun_sub <<SQL
CREATE EXTENSION fasttrun;
CREATE TABLE src (id int PRIMARY KEY);
CREATE FUNCTION src_collect() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS w (id int);
  INSERT INTO pg_temp.w SELECT generate_series(1, 1000);
  PERFORM public.fasttrun_analyze('pg_temp.w');
  RETURN NEW;
END\$\$;
CREATE TRIGGER src_collect BEFORE INSERT ON src
  FOR EACH ROW EXECUTE FUNCTION src_collect();
ALTER TABLE src ENABLE ALWAYS TRIGGER src_collect;
CREATE SUBSCRIPTION fasttrun_sub
  CONNECTION 'host=$SOCKET_DIR port=$PORT dbname=fasttrun_pub'
  PUBLICATION fasttrun_pub
  WITH (create_slot = false, slot_name = 'fasttrun_slot', copy_data = false);
SQL

run_psql -d fasttrun_pub -c "INSERT INTO src VALUES (1)"
tries=0
until [ "$(run_psql -d fasttrun_sub -c 'SELECT count(*) FROM src')" = 1 ]; do
	tries=$((tries + 1))
	if [ "$tries" -gt 300 ]; then
		echo "bgworker lazy-load log: FAIL (строка не дошла до подписчика)"
		exit 1
	fi
	sleep 0.1
done

# Положительный контроль: клиентский сеанс, библиотека грузится первым вызовом.
run_psql -d fasttrun_sub >/dev/null <<'SQL'
CREATE TEMP TABLE c (id int);
INSERT INTO c SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('c');
SQL

client_log=$(grep -c "^client backend|LOG:  $MSG" "$LOG" || true)
worker_log=$(grep "|LOG:  $MSG" "$LOG" | grep -vc '^client backend|' || true)
worker_debug=$(grep "|DEBUG:  $MSG" "$LOG" | grep -vc '^client backend|' || true)
worker_hint=$(grep -F "|HINT:  $HINT" "$LOG" | grep -vc '^client backend|' || true)

echo "client LOG=$client_log worker LOG=$worker_log worker DEBUG=$worker_debug worker HINT=$worker_hint"
# Воркер применения, перезапущенный ядром, напишет строку еще раз, поэтому у
# него проверяется "хотя бы одна", а у единственного клиентского сеанса - ровно одна.
if [ "$client_log" -eq 1 ] && [ "$worker_log" -eq 0 ] &&
	[ "$worker_debug" -ge 1 ] && [ "$worker_hint" -ge 1 ]; then
	echo "bgworker lazy-load log: PASS"
	exit 0
fi
echo "bgworker lazy-load log: FAIL"
grep -F -e "$MSG" -e 'HINT:' "$LOG" || true
exit 1
