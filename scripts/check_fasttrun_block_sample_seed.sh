#!/bin/sh
#
# check_fasttrun_block_sample_seed.sh - независимая выборка блоков в разных
# процессах.
#
# Выборка блоков в fasttrun_analyze берет зерно из генератора процесса. При
# shared_preload_libraries библиотека грузится в postmaster, и все бэкенды
# наследуют его память. Генератор, засеянный там, дал бы каждому бэкенду одну и
# ту же последовательность зерен и одни и те же блоки. Здесь два бэкенда
# кластера с предзагрузкой анализируют одинаковую таблицу из 300 страниц по
# одной строке на странице, выборка - 30 строк. Строк в выбранных блоках ровно
# столько, сколько просит выборка, поэтому ее состав задает только выбор блоков,
# а гистограмма уникальной колонки перечисляет выбранные строки. Гистограммы
# двух бэкендов обязаны различаться.
# Скрипт печатает PASS или FAIL и не требует cassert-сборки.
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
WORKDIR=$(mktemp -d)
DATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/sock"
LOG="$WORKDIR/log"
PORT=${PGPORT_CHECK:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

# Каталог удаляется только после подтвержденной остановки сервера. Иначе он
# остается для разбора, а проверка проваливается.
cleanup() {
	rc=$?
	if [ -f "$DATA/postmaster.pid" ] &&
		! "$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null 2>&1; then
		echo "block sample seed: сервер не остановился, каталог сохранен: $WORKDIR"
		exit 1
	fi
	# Сбой удаления не должен подменять исходный код выхода.
	rm -rf "$WORKDIR" || :
	exit "$rc"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
"$BINDIR/initdb" -D "$DATA" --no-locale -E UTF8 >/dev/null
cat >>"$DATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$SOCKET_DIR'
port = $PORT
shared_preload_libraries = 'fasttrun'
log_line_prefix = '%p|'
CONF
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -w start >/dev/null
"$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -d postgres -XAtq -v ON_ERROR_STOP=1 \
	-c 'CREATE EXTENSION fasttrun'

# fillfactor 10 при строке около 1,5 КБ оставляет на странице одну строку.
# Строка DEBUG1 подтверждает, что анализ пошел выборкой блоков.
session() {
	"$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -d postgres -XAt -v ON_ERROR_STOP=1 2>&1 <<'SQL'
SET fasttrun.max_analyze_pages = 1;
SET fasttrun.sample_rows = 30;
CREATE TEMP TABLE t_seed (v int, pad text) WITH (fillfactor = 10);
INSERT INTO t_seed SELECT g, repeat('x', 1500) FROM generate_series(1, 300) g;
SELECT 'PAGES=' || pg_relation_size('t_seed') / current_setting('block_size')::int;
SET client_min_messages = debug1;
SELECT fasttrun_analyze('t_seed') IS NOT NULL;
RESET client_min_messages;
SELECT 'SAMPLE=' || stavalues1::text
FROM fasttrun_inspect_stats('t_seed')
WHERE staattnum = 1 AND stakind1 = 2;
SQL
}

out1=$(session) || out1="$out1
PSQL_FAILED"
out2=$(session) || out2="$out2
PSQL_FAILED"
"$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null

sample1=$(echo "$out1" | sed -n 's/^SAMPLE=//p')
sample2=$(echo "$out2" | sed -n 's/^SAMPLE=//p')
count1=$(echo "$sample1" | tr ',' '\n' | grep -c '[0-9]' || true)
count2=$(echo "$sample2" | tr ',' '\n' | grep -c '[0-9]' || true)
blocks=$(echo "$out1$out2" | grep -c 'DEBUG:  fasttrun: block-sampling analyze scan' || true)
pages=$(echo "$out1" | sed -n 's/^PAGES=//p')
errors=$(grep -cE '\|(ERROR|FATAL|PANIC):|TRAP' "$LOG" || true)
echo "pages=${pages:-none}, block sampling scans=$blocks, sample sizes=$count1/$count2, server errors=$errors"
if [ "$pages" = 300 ] && [ "$blocks" -eq 2 ] && [ "$count1" -eq 30 ] &&
	[ "$count2" -eq 30 ] && [ "$sample1" != "$sample2" ] && [ "$errors" -eq 0 ]; then
	echo "block sample seed: PASS"
	exit 0
fi
echo "block sample seed: FAIL"
echo "--- session 1"
echo "$out1"
echo "--- session 2"
echo "$out2"
echo "--- server log errors"
grep -E '\|(ERROR|FATAL|PANIC):|TRAP' "$LOG" | tail -20 || true
exit 1
