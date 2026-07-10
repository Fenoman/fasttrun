#!/usr/bin/env bash
#
# Проверяет общие сообщения инвалидации через Linux/gdb:
#   * отдельная строка подтверждает подключение gdb;
#   * SMGR-сообщения считаются внутри SendSharedInvalidMessages;
#   * SIInsertDataEntries подтверждает, что счётчик общих сообщений работает;
#   * локальный сброс планов должен сработать для каждой операции fasttrun.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
GDB=${GDB:-gdb}
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-sinval.XXXXXX)}
SOCKET_DIR=${PGHOST:-$WORKDIR/socket}
DBNAME=${DBNAME:-fasttrun_sinval}
SLEEP_SECONDS=${SLEEP_SECONDS:-3}

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$WORKDIR/data" -m fast -w stop \
			>/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}
trap cleanup EXIT

require_cmd()
{
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing command: $1" >&2
		exit 1
	fi
}

require_cmd "$GDB"
require_cmd "$PSQL"
require_cmd "$INITDB"
require_cmd "$PG_CTL"
require_cmd "$CREATEDB"
if [ -n "$PG_RUN_AS" ]; then
	require_cmd runuser
fi

mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR"
fi
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

cat >"$WORKDIR/gdb.commands" <<'GDB'
set pagination off
set confirm off
set breakpoint pending off
break SIInsertDataEntries
commands
silent
printf "SHARED_INSERT\n"
continue
end
break SendSharedInvalidMessages
commands
silent
set $i = 0
while $i < n
  if msgs[$i].id == -3
    printf "SMGR_MESSAGE\n"
  end
  set $i = $i + 1
end
continue
end
break LocalExecuteInvalidationMessage
commands
silent
printf "LOCAL_HIT\n"
continue
end
printf "ATTACHED_READY\n"
continue
GDB

run_pg "$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$WORKDIR/data" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

run_case()
{
	local name=$1
	local shared_expect=$2
	local smgr_expect=$3
	local require_local=$4
	local validate_truncate=$5
	local workload=$6
	local sqlfile="$WORKDIR/$name.sql"
	local psqlout="$WORKDIR/$name.psql.out"
	local psqlerr="$WORKDIR/$name.psql.err"
	local gdblog="$WORKDIR/$name.gdb.out"
	local pid=""
	local psql_pid
	local gdb_pid
	local shared_inserts
	local smgr_messages
	local local_hits
	local expected_smgr
	local toast_before
	local toast_after
	local toast_refill
	local before_checksum
	local after_checksum
	local i

	cat >"$sqlfile" <<SQL
\set ON_ERROR_STOP 1
SET client_min_messages = warning;
CREATE TEMP TABLE t_zero_sinval (
  id int PRIMARY KEY,
  grp int,
  payload text
);
CREATE INDEX t_zero_sinval_grp_idx ON t_zero_sinval (grp);
ALTER TABLE t_zero_sinval ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO t_zero_sinval
SELECT g, g % 31,
       string_agg(md5((g * 1000 + s)::text), '')
FROM generate_series(1, 200) g
CROSS JOIN LATERAL generate_series(1, 450) s
GROUP BY g;
SELECT 'EXPECTED_SMGR ' ||
       (1
        + (SELECT count(*) FROM pg_index
            WHERE indrelid = 't_zero_sinval'::regclass)
        + CASE WHEN c.reltoastrelid <> 0 THEN
            1 + (SELECT count(*) FROM pg_index
                 WHERE indrelid = c.reltoastrelid)
          ELSE 0 END)
FROM pg_class c
WHERE c.oid = 't_zero_sinval'::regclass;
SELECT format('SELECT ''TOAST_BEFORE '' || count(*) FROM %s',
              c.reltoastrelid::regclass)
FROM pg_class c
WHERE c.oid = 't_zero_sinval'::regclass \gexec
SELECT 'BEFORE_CHECKSUM ' ||
       md5(string_agg(md5(payload), '' ORDER BY id))
FROM t_zero_sinval;
SELECT pg_backend_pid();
SELECT pg_sleep($SLEEP_SECONDS);
$workload
SELECT 'CASE_DONE';
SELECT pg_sleep($SLEEP_SECONDS);
SQL

	run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -f "$sqlfile" >"$psqlout" 2>"$psqlerr" &
	psql_pid=$!

	for i in $(seq 1 200); do
		pid=$(grep -E '^[0-9]+$' "$psqlout" | head -1 || true)
		if [ -n "$pid" ]; then
			break
		fi
		sleep 0.05
	done
	if [ -z "$pid" ]; then
		wait "$psql_pid" || true
		echo "[$name] could not read backend pid" >&2
		cat "$psqlout" "$psqlerr" >&2 || true
		exit 1
	fi

	"$GDB" -q -nx -batch -x "$WORKDIR/gdb.commands" -p "$pid" \
		>"$gdblog" 2>&1 &
	gdb_pid=$!
	for i in $(seq 1 200); do
		if grep -q '^ATTACHED_READY$' "$gdblog"; then
			break
		fi
		if ! kill -0 "$gdb_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.05
	done
	if ! grep -q '^ATTACHED_READY$' "$gdblog"; then
		kill "$gdb_pid" "$psql_pid" >/dev/null 2>&1 || true
		wait "$gdb_pid" || true
		wait "$psql_pid" || true
		cat "$gdblog" >&2
		echo "[$name] gdb did not emit ATTACHED_READY" >&2
		exit 1
	fi

	for i in $(seq 1 400); do
		if grep -q '^CASE_DONE$' "$psqlout"; then
			break
		fi
		if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.05
	done
	if ! grep -q '^CASE_DONE$' "$psqlout"; then
		kill "$gdb_pid" >/dev/null 2>&1 || true
		wait "$gdb_pid" || true
		wait "$psql_pid" || true
		cat "$psqlout" "$psqlerr" "$gdblog" >&2 || true
		echo "[$name] workload did not reach CASE_DONE" >&2
		exit 1
	fi

	kill "$gdb_pid" >/dev/null 2>&1 || true
	wait "$gdb_pid" || true
	if ! wait "$psql_pid"; then
		cat "$psqlout" "$psqlerr" "$gdblog" >&2 || true
		echo "[$name] psql workload failed after CASE_DONE" >&2
		exit 1
	fi

	shared_inserts=$(grep -c '^SHARED_INSERT$' "$gdblog" || true)
	smgr_messages=$(grep -c '^SMGR_MESSAGE$' "$gdblog" || true)
	local_hits=$(grep -c '^LOCAL_HIT$' "$gdblog" || true)
	expected_smgr=$(awk '$1 == "EXPECTED_SMGR" {print $2}' "$psqlout" | tail -1)
	toast_before=$(awk '$1 == "TOAST_BEFORE" {print $2}' "$psqlout" | tail -1)

	case "$expected_smgr:$toast_before" in
		*[!0-9:]*|:*|*:) cat "$psqlout" >&2; echo "[$name] неверный формат метрик тестовой таблицы" >&2; exit 1 ;;
	esac
	if [ "$toast_before" -le 0 ]; then
		echo "[$name] тестовая таблица не создала внешний TOAST" >&2
		exit 1
	fi

	case "$smgr_expect" in
		fixture) smgr_expect=$expected_smgr ;;
		''|*[!0-9]*) echo "[$name] invalid SMGR expectation: $smgr_expect" >&2; exit 1 ;;
	esac

	printf '%-31s shared_insert=%s smgr=%s/%s local=%s\n' \
		"$name" "$shared_inserts" "$smgr_messages" "$smgr_expect" "$local_hits"

	case "$shared_expect" in
		positive)
			if [ "$shared_inserts" -le 0 ]; then
				cat "$gdblog" >&2
				echo "[$name] контрольная операция не отправила общее сообщение" >&2
				exit 1
			fi
			;;
		zero)
			if [ "$shared_inserts" -ne 0 ]; then
				cat "$gdblog" >&2
				echo "[$name] expected zero shared insertion calls" >&2
				exit 1
			fi
			;;
		any) ;;
		*) echo "[$name] invalid shared expectation: $shared_expect" >&2; exit 1 ;;
	esac
	if [ "$smgr_messages" -ne "$smgr_expect" ]; then
		cat "$gdblog" >&2
		echo "[$name] expected $smgr_expect SMGR messages, got $smgr_messages" >&2
		exit 1
	fi
	if [ "$require_local" -eq 1 ] && [ "$local_hits" -le 0 ]; then
		cat "$gdblog" >&2
		echo "[$name] локальный сброс планов не сработал" >&2
		exit 1
	fi

	if [ "$validate_truncate" -eq 1 ]; then
		toast_after=$(awk '$1 == "TOAST_AFTER" {print $2}' "$psqlout" | tail -1)
		toast_refill=$(awk '$1 == "TOAST_REFILL" {print $2}' "$psqlout" | tail -1)
		before_checksum=$(awk '$1 == "BEFORE_CHECKSUM" {print $2}' "$psqlout" | tail -1)
		after_checksum=$(awk '$1 == "AFTER_CHECKSUM" {print $2}' "$psqlout" | tail -1)
		if [ "$toast_after" != 0 ] || [ -z "$toast_refill" ] ||
			[ "$toast_refill" -le 0 ] || [ -z "$before_checksum" ] ||
			[ -z "$after_checksum" ] || [ "$before_checksum" = "$after_checksum" ]; then
			cat "$psqlout" >&2
			echo "[$name] TOAST/refill outcome is not authoritative" >&2
			exit 1
		fi
	fi
}

truncate_validation=$(cat <<'SQL'
SELECT format('SELECT ''TOAST_AFTER '' || count(*) FROM %s',
              c.reltoastrelid::regclass)
FROM pg_class c
WHERE c.oid = 't_zero_sinval'::regclass \gexec
INSERT INTO t_zero_sinval
SELECT 1000 + g, g % 31,
       string_agg(md5((900000 + g * 1000 + s)::text), '')
FROM generate_series(1, 20) g
CROSS JOIN LATERAL generate_series(1, 120) s
GROUP BY g;
SELECT 'AFTER_CHECKSUM ' ||
       md5(string_agg(md5(payload), '' ORDER BY id))
FROM t_zero_sinval;
SELECT format('SELECT ''TOAST_REFILL '' || count(*) FROM %s',
              c.reltoastrelid::regclass)
FROM pg_class c
WHERE c.oid = 't_zero_sinval'::regclass \gexec
SQL
)

run_case regular_analyze positive 0 0 0 \
	"ANALYZE t_zero_sinval;"
run_case fasttrun_analyze zero 0 1 0 \
	"SELECT fasttrun_analyze('t_zero_sinval');"
run_case fasttrun_collect_stats zero 0 1 0 \
	"SELECT fasttrun_collect_stats('t_zero_sinval');"
run_case fasttruncate_default zero 0 1 1 \
	"SELECT fasttruncate('t_zero_sinval'); $truncate_validation"
run_case fasttruncate_fallback any fixture 1 1 \
	"SET fasttrun.zero_sinval_truncate = off; SELECT fasttruncate('t_zero_sinval'); $truncate_validation"

debug_assertions=$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')
if [ "$debug_assertions" = on ]; then
	for spec in \
		'after_user_index:1=1' \
		'after_user_index:2=2' \
		'after_toast_index:1=3' \
		'after_toast_heap=4' \
		'after_main_heap=5'; do
		point=${spec%%=*}
		expected=${spec##*=}
		label=${point//:/_}
		workload="SET fasttrun.zero_sinval_truncate = off;
SET fasttrun.test_failpoint = '$point';
DO \$case\$
BEGIN
  BEGIN
    PERFORM fasttruncate('t_zero_sinval');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
END
\$case\$;"
		run_case "fallback_$label" any "$expected" 1 0 "$workload"
	done
fi

if grep -Eq 'TRAP|Assertion|PANIC|server process .* was terminated' \
	"$WORKDIR/postgres.log"; then
	grep -E 'TRAP|Assertion|PANIC|server process .* was terminated' \
		"$WORKDIR/postgres.log" >&2
	echo "проверка общих сообщений вызвала Assert или завершение серверного процесса" >&2
	exit 1
fi

echo "zero shared sinval contract verified with exact SMGR counts"
