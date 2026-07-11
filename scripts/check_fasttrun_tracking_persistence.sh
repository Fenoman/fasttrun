#!/usr/bin/env bash
# Проверяет штатное и отказоустойчивое сохранение tracking-файла.
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PYTHON=${PYTHON:-python3}
PG_RUN_AS=${PG_RUN_AS:-}
if [ -n "$PG_RUN_AS" ] && ! command -v runuser >/dev/null 2>&1; then
	echo "FAIL: PG_RUN_AS требует команду runuser" >&2
	exit 2
fi
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-tracking-persistence.XXXXXX)}
CASE_NAME=${FASTTRUN_TRACKING_PERSISTENCE_CASE:-all}
KEEP_WORKDIR=${KEEP_WORKDIR:-0}
DBNAME=${DBNAME:-fasttrun_tracking_persistence}
ASSERTIONS_ENABLED=unknown

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

cleanup()
{
	local exit_status=$?
	local pidfile
	local stop_failed=0

	for pidfile in "$WORKDIR"/*/data/postmaster.pid; do
		[ -f "$pidfile" ] || continue
		if ! run_pg "$PG_CTL" -D "${pidfile%/postmaster.pid}" \
			-m fast -w stop >/dev/null 2>&1; then
			echo "FAIL: test server did not stop: ${pidfile%/postmaster.pid}" >&2
			stop_failed=1
		fi
	done
	if [ "$stop_failed" -ne 0 ]; then
		echo "tracking persistence files kept in $WORKDIR" >&2
		[ "$exit_status" -ne 0 ] || exit_status=1
	elif [ "$KEEP_WORKDIR" -eq 0 ]; then
		rm -rf "$WORKDIR"
	else
		echo "tracking persistence files kept in $WORKDIR" >&2
	fi
	trap - EXIT
	exit "$exit_status"
}
trap cleanup EXIT

mkdir -p "$WORKDIR"
if [ -n "$PG_RUN_AS" ]; then
	chown "$PG_RUN_AS" "$WORKDIR"
	chmod 700 "$WORKDIR"
fi

file_sha256()
{
	"$PYTHON" - "$1" <<'PY'
import hashlib
import sys
from pathlib import Path

print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())
PY
}

start_cluster()
{
	local data=$1
	local socket=$2
	local log=$3
	local port=$4
	local failpoint=$5
	local opts="-k $socket -p $port -c listen_addresses='' -c shared_preload_libraries=fasttrun"

	if [ -n "$failpoint" ]; then
		opts="$opts -c fasttrun.test_failpoint=$failpoint"
	fi
	run_pg "$PG_CTL" -D "$data" -l "$log" -o "$opts" -w start >/dev/null
}

stop_cluster()
{
	run_pg "$PG_CTL" -D "$1" -m fast -w stop >/dev/null
}

snapshot()
{
	local socket=$1
	local port=$2

	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 <<'SQL'
SELECT 'SNAPSHOT ' || coalesce(
  string_agg(relname || ':' || create_count, ',' ORDER BY relname), '')
FROM fasttrun_hot_temp_tables(0);
SQL
}

install_test_function()
{
	local socket=$1
	local port=$2

	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE OR REPLACE FUNCTION public.fasttrun_test_track_set(text, bigint, bigint)
RETURNS void
AS '$libdir/fasttrun', 'fasttrun_test_track_set'
LANGUAGE C STRICT;
SQL
}

set_rows()
{
	local socket=$1
	local port=$2
	local prefix=$3

	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -v prefix="$prefix" >/dev/null <<'SQL'
SELECT public.fasttrun_test_track_set(:'prefix' || '_alpha', 11, 1000);
SELECT public.fasttrun_test_track_set(:'prefix' || '_beta', 22, 2000);
SQL
}

check_log_clean()
{
	local log=$1
	local failure_pattern='TRAP|Assertion|Segmentation fault|Abort trap|server process .* (was )?terminated|(^|[[:space:]])(ERROR|FATAL|PANIC):'

	if grep -Eq "$failure_pattern" "$log"; then
		grep -E "$failure_pattern" "$log" >&2
		return 1
	fi
	if ! grep -q 'database system is shut down' "$log"; then
		echo "FAIL: normal shutdown marker missing in $log" >&2
		return 1
	fi
}

run_clean_case()
{
	local case_dir=$WORKDIR/clean
	local data=$case_dir/data
	local socket=$case_dir/socket
	local track_file=$data/pg_stat/fasttrun_temp_stats
	local tmp_file=$track_file.tmp
	local save_log=$case_dir/save.log
	local load_log=$case_dir/load.log
	local saved_snapshot loaded_snapshot saved_sha
	local port

	mkdir -p "$socket"
	if [ -n "$PG_RUN_AS" ]; then
		chown "$PG_RUN_AS" "$case_dir" "$socket"
	fi
	port=${PGPORT:-$($PYTHON -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
	run_pg "$INITDB" -D "$data" --no-locale -E UTF8 >/dev/null

	start_cluster "$data" "$socket" "$save_log" "$port" ""
	run_pg "$CREATEDB" -h "$socket" -p "$port" "$DBNAME"
	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE EXTENSION fasttrun;
SELECT fasttrun_reset_temp_stats();
CREATE SCHEMA dummy_tmp;
CREATE TABLE dummy_tmp.persist_clean (id integer);
SET fasttrun.track_schedule = '';
CREATE TEMP TABLE persist_clean
  (LIKE dummy_tmp.persist_clean INCLUDING ALL);
SQL
	ASSERTIONS_ENABLED=$(run_pg "$PSQL" -h "$socket" -p "$port" \
		-d "$DBNAME" -XAtq -c 'SHOW debug_assertions')
	saved_snapshot=$(snapshot "$socket" "$port")
	[ "$saved_snapshot" = 'SNAPSHOT persist_clean:1' ] || {
		echo "FAIL: clean save has unexpected rows: $saved_snapshot" >&2
		return 1
	}
	stop_cluster "$data"
	[ -f "$track_file" ] || { echo "FAIL: clean save file missing" >&2; return 1; }
	[ ! -e "$tmp_file" ] || { echo "FAIL: clean save left .tmp" >&2; return 1; }
	saved_sha=$(file_sha256 "$track_file")

	start_cluster "$data" "$socket" "$load_log" "$port" ""
	loaded_snapshot=$(snapshot "$socket" "$port")
	[ "$loaded_snapshot" = 'SNAPSHOT persist_clean:1' ] || {
		echo "FAIL: clean restart loaded unexpected rows: $loaded_snapshot" >&2
		return 1
	}
	stop_cluster "$data"
	[ ! -e "$tmp_file" ] || { echo "FAIL: clean restart left .tmp" >&2; return 1; }

	check_log_clean "$save_log"
	check_log_clean "$load_log"
	echo "tracking persistence passed: clean sha256=$saved_sha"
}

run_case()
{
	local point=$1
	local case_dir=$WORKDIR/$point
	local data=$case_dir/data
	local socket=$case_dir/socket
	local track_file=$data/pg_stat/fasttrun_temp_stats
	local tmp_file=$track_file.tmp
	local baseline_log=$case_dir/baseline.log
	local failure_log=$case_dir/failure.log
	local recovery_log=$case_dir/recovery.log
	local baseline_sha after_sha before_snapshot recovery_snapshot
	local failure_snapshot expected_errno
	local port

	mkdir -p "$socket"
	if [ -n "$PG_RUN_AS" ]; then
		chown "$PG_RUN_AS" "$case_dir" "$socket"
	fi
	port=${PGPORT:-$($PYTHON -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
	run_pg "$INITDB" -D "$data" --no-locale -E UTF8 >/dev/null

	start_cluster "$data" "$socket" "$baseline_log" "$port" ""
	run_pg "$CREATEDB" -h "$socket" -p "$port" "$DBNAME"
	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null
	install_test_function "$socket" "$port"
	run_pg "$PSQL" -h "$socket" -p "$port" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -c 'SELECT fasttrun_reset_temp_stats()' >/dev/null
	set_rows "$socket" "$port" old
	before_snapshot=$(snapshot "$socket" "$port")
	[ "$before_snapshot" = 'SNAPSHOT old_alpha:11,old_beta:22' ] || {
		echo "FAIL: unstable baseline snapshot: $before_snapshot" >&2
		return 1
	}
	stop_cluster "$data"
	[ -f "$track_file" ] || { echo "FAIL: baseline file missing" >&2; return 1; }
	[ ! -e "$tmp_file" ] || { echo "FAIL: baseline left .tmp" >&2; return 1; }
	baseline_sha=$(file_sha256 "$track_file")

	start_cluster "$data" "$socket" "$failure_log" "$port" "$point"
	install_test_function "$socket" "$port"
	before_snapshot=$(snapshot "$socket" "$port")
	[ "$before_snapshot" = 'SNAPSHOT old_alpha:11,old_beta:22' ] || {
		echo "FAIL: persisted baseline changed before $point: $before_snapshot" >&2
		return 1
	}
	set_rows "$socket" "$port" new
	failure_snapshot=$(snapshot "$socket" "$port")
	[ "$failure_snapshot" = \
		'SNAPSHOT new_alpha:11,new_beta:22,old_alpha:11,old_beta:22' ] || {
		echo "FAIL: $point fixture has no new rows: $failure_snapshot" >&2
		return 1
	}
	stop_cluster "$data"
	after_sha=$(file_sha256 "$track_file")
	[ "$after_sha" = "$baseline_sha" ] || {
		echo "FAIL: $point replaced the stable tracking file" >&2
		return 1
	}
	[ ! -e "$tmp_file" ] || { echo "FAIL: $point left .tmp" >&2; return 1; }
	grep -q "fasttrun: could not save tracking statistics at phase $point:" \
		"$failure_log" || {
		echo "FAIL: $point phase log missing" >&2
		return 1
	}
	if [ "$point" = track_save_write ]; then
		expected_errno='No space left on device'
	else
		expected_errno='Input/output error'
	fi
	grep -q "$expected_errno" "$failure_log" || {
		echo "FAIL: $point errno log missing: $expected_errno" >&2
		return 1
	}

	start_cluster "$data" "$socket" "$recovery_log" "$port" ""
	recovery_snapshot=$(snapshot "$socket" "$port")
	[ "$recovery_snapshot" = 'SNAPSHOT old_alpha:11,old_beta:22' ] || {
		echo "FAIL: $point recovery loaded unexpected rows: $recovery_snapshot" >&2
		return 1
	}
	stop_cluster "$data"
	[ ! -e "$tmp_file" ] || { echo "FAIL: recovery left .tmp" >&2; return 1; }

	check_log_clean "$baseline_log"
	check_log_clean "$failure_log"
	check_log_clean "$recovery_log"
	echo "tracking persistence passed: $point sha256=$baseline_sha"
}

case "$CASE_NAME" in
	all|clean|track_save_write|track_save_close|track_save_pre_rename)
		;;
	*)
		echo "unknown FASTTRUN_TRACKING_PERSISTENCE_CASE: $CASE_NAME" >&2
		exit 2
		;;
esac

run_clean_case

if [ "$CASE_NAME" = clean ]; then
	exit 0
fi
if [ "$ASSERTIONS_ENABLED" != on ]; then
	echo "SKIP: fault-case tracking persistence требуют cassert-сборку" >&2
	exit 0
fi

case "$CASE_NAME" in
	all)
		run_case track_save_write
		run_case track_save_close
		run_case track_save_pre_rename
		;;
	track_save_write|track_save_close|track_save_pre_rename)
		run_case "$CASE_NAME"
		;;
esac

echo "tracking persistence passed: $CASE_NAME"
