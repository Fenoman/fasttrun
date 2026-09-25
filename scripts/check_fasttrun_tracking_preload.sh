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
if [ "$rc" -eq 0 ] && [ "${main:-0}" -ge 1 ]; then
	echo "tracking preload: PASS"
	exit 0
fi
echo "tracking preload: FAIL"
tail -5 "$WORKDIR/installcheck.log" || true
head -40 "$OUT/regression.diffs" 2>/dev/null || true
grep -n "shmem" "$result" 2>/dev/null || true
exit 1
