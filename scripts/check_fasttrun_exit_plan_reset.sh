#!/bin/sh
#
# check_fasttrun_exit_plan_reset.sh - сброс кэша планов перед очисткой
# временных таблиц при выходе бэкенда.
#
# Ядро удаляет временные таблицы сеанса в колбэке выхода
# RemoveTempRelationsCallback. На каждое удаленное отношение приходит
# relcache-сообщение, и каждое сообщение обходит все сохраненные планы
# бэкенда. fasttrun до этого помечает планы невалидными (ResetPlanCache), и
# обход становится дешевле. Колбэки выхода выполняются в обратном порядке
# регистрации, а ядро регистрирует свой при коммите транзакции, создавшей
# временную схему. Поэтому fasttrun регистрирует свой колбэк в первом
# операторе после такого коммита, из хука исполнителя или хука служебных
# команд, и тот выполняется раньше очистки ядра.
#
# Сеансы пишут журнал с log_min_messages=debug2, строка fasttrun обязана стоять
# раньше перечня удаляемых объектов, который очистка ядра пишет до самого
# удаления:
#   main    - таблица, затем два SELECT отдельными транзакциями. Регистрирует
#             хук исполнителя, строка fasttrun ровно одна, повторной
#             регистрации нет
#   utility - таблица, затем SET. Регистрирует хук служебных команд
#   control - таблица и SELECT в одной транзакции. Регистрировать некому,
#             строки fasttrun нет, а перечень удаляемых объектов есть, то есть
#             уровня журнала достаточно
# Любая строка ERROR, FATAL, PANIC или TRAP в журнале, включая остановку
# сервера, - провал: ошибка в колбэке выхода не должна прятаться за правильным
# порядком строк.
# Сам эффект сброса, невалидность планов, журнал не показывает. Его
# подтверждает замер времени выхода в docs/ru/internals.md, раздел
# "Производительность".
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
MSG='fasttrun: marking cached plans invalid before temporary relation cleanup'
DROP_RE='DEBUG:  drop (auto-)?cascades to '
ERROR_RE='\|(ERROR|FATAL|PANIC):|TRAP'

show_errors() {
	grep -E "$ERROR_RE" "$LOG" 2>/dev/null | tail -20 || true
}

# Каталог удаляется только после подтвержденной остановки сервера. Иначе он
# остается для разбора, а проверка проваливается.
cleanup() {
	rc=$?
	[ "$rc" -eq 0 ] || show_errors
	if [ -f "$DATA/postmaster.pid" ] &&
		! "$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null 2>&1; then
		echo "exit plan reset: сервер не остановился, каталог сохранен: $WORKDIR"
		exit 1
	fi
	# Сбой удаления не должен подменять исходный код выхода.
	rm -rf "$WORKDIR" || :
	exit "$rc"
}
trap cleanup EXIT

run_psql() {
	"$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -d postgres -XAtq -v ON_ERROR_STOP=1 "$@"
}

# Ждет, пока бэкенд завершит выход: строки очистки пишутся уже после отключения
# клиента, а из pg_stat_activity бэкенд уходит позже колбэков before_shmem_exit.
wait_gone() {
	tries=0
	while [ "$(run_psql -c "SELECT count(*) FROM pg_stat_activity WHERE pid = $1")" != 0 ]; do
		tries=$((tries + 1))
		if [ "$tries" -gt 100 ]; then
			echo "exit plan reset: FAIL (бэкенд $1 не завершился)"
			exit 1
		fi
		sleep 0.1
	done
}

# Номер первой строки журнала бэкенда, подходящей под выражение, или пусто.
first_line() {
	grep -nE "^$1\|$2" "$LOG" | head -1 | cut -d: -f1
}

count_lines() {
	grep -cE "^$1\|$2" "$LOG" || true
}

# Сеанс: первая строка вывода - PID. Вывод забирается целиком, чтобы канал не
# закрылся раньше, чем psql выполнит все -c.
session() {
	out=$(PGOPTIONS='-c log_min_messages=debug2' run_psql "$@")
	echo "$out" | head -1
}

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

# Каждая -c идет своей транзакцией.
main_pid=$(session -c 'SELECT pg_backend_pid()' \
	-c 'CREATE TEMP TABLE t_main (id int PRIMARY KEY, payload text)' \
	-c 'SELECT count(*) FROM t_main' \
	-c 'SELECT count(*) FROM t_main')
wait_gone "$main_pid"

utility_pid=$(session -c 'SELECT pg_backend_pid()' \
	-c 'CREATE TEMP TABLE t_utility (id int PRIMARY KEY, payload text)' \
	-c 'SET work_mem = 8192')
wait_gone "$utility_pid"

# Несколько операторов в одной -c идут одной неявной транзакцией.
ctl_pid=$(session \
	-c 'CREATE TEMP TABLE t_ctl (id int PRIMARY KEY, payload text) ; SELECT pg_backend_pid()')
wait_gone "$ctl_pid"

# Сервер останавливается до разбора журнала, чтобы ошибки остановки тоже попали
# в проверку. Если он не остановился, cleanup сохранит каталог.
if ! "$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null 2>&1; then
	echo "exit plan reset: FAIL (сервер не остановился)"
	exit 1
fi

fail=0
for s in main utility control; do
	case $s in
		main) pid=$main_pid ;;
		utility) pid=$utility_pid ;;
		control) pid=$ctl_pid ;;
	esac
	msg=$(first_line "$pid" "DEBUG:  $MSG")
	drop=$(first_line "$pid" "$DROP_RE")
	nmsg=$(count_lines "$pid" "DEBUG:  $MSG")
	echo "$s: fasttrun lines=$nmsg first=${msg:-none} first drop line=${drop:-none}"
	if [ -z "$drop" ]; then
		fail=1
	elif [ "$s" = control ]; then
		[ "$nmsg" -eq 0 ] || fail=1
	elif [ "$nmsg" -ne 1 ] || [ "$msg" -ge "$drop" ]; then
		fail=1
	fi
done
errors=$(grep -cE "$ERROR_RE" "$LOG" || true)
echo "errors in server log: $errors"
[ "$errors" -eq 0 ] || fail=1

if [ "$fail" -eq 0 ]; then
	echo "exit plan reset: PASS"
	exit 0
fi
echo "exit plan reset: FAIL"
grep -E "^($main_pid|$utility_pid|$ctl_pid)\|" "$LOG" | grep -E "fasttrun|cascades to" || true
show_errors
exit 1
