#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
GDB=${GDB:-gdb}
PORT=${PGPORT:-55433}
SOCKET_DIR=${PGHOST:-/tmp}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-sinval.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_sinval}
SLEEP_SECONDS=${SLEEP_SECONDS:-8}

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
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

cat >"$WORKDIR/gdb.commands" <<'GDB'
set pagination off
set confirm off
set breakpoint pending off
break SIInsertDataEntries
commands
silent
printf "SHARED_HIT SIInsertDataEntries\n"
continue
end
break SendSharedInvalidMessages
commands
silent
printf "SHARED_HIT SendSharedInvalidMessages\n"
continue
end
break LocalExecuteInvalidationMessage
commands
silent
printf "LOCAL_HIT LocalExecuteInvalidationMessage\n"
continue
end
continue
GDB

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" -o "-k $SOCKET_DIR -p $PORT" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -v ON_ERROR_STOP=1 \
	-qAt -c "CREATE EXTENSION fasttrun" >/dev/null

run_case()
{
	local name=$1
	local expected=$2
	local workload=$3
	local sqlfile="$WORKDIR/$name.sql"
	local psqlout="$WORKDIR/$name.psql.out"
	local psqlerr="$WORKDIR/$name.psql.err"
	local gdblog="$WORKDIR/$name.gdb.out"
	local pid=""
	local shared_hits
	local local_hits
	local i

	cat >"$sqlfile" <<SQL
SET client_min_messages = warning;
CREATE TEMP TABLE t_zero_sinval (id int, grp int, payload text);
INSERT INTO t_zero_sinval
SELECT g, g % 10, repeat('x', 32)
FROM generate_series(1, 10000) g;
CREATE INDEX t_zero_sinval_grp_idx ON t_zero_sinval (grp);
SELECT pg_backend_pid();
SELECT pg_sleep($SLEEP_SECONDS);
$workload
SELECT 'CASE_DONE';
SELECT pg_sleep($SLEEP_SECONDS);
SQL

	"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -v ON_ERROR_STOP=1 \
		-qAt -f "$sqlfile" >"$psqlout" 2>"$psqlerr" &
	local psql_pid=$!

	for i in $(seq 1 100); do
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
	local gdb_pid=$!

	for i in $(seq 1 100); do
		if grep -q "Continuing" "$gdblog"; then
			break
		fi
		sleep 0.05
	done

	for i in $(seq 1 200); do
		if grep -q '^CASE_DONE$' "$psqlout"; then
			break
		fi
		if ! kill -0 "$psql_pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.05
	done
	if ! grep -q '^CASE_DONE$' "$psqlout"; then
		wait "$psql_pid" || true
		echo "[$name] workload did not reach CASE_DONE" >&2
		cat "$psqlout" "$psqlerr" >&2 || true
		cat "$gdblog" >&2 || true
		exit 1
	fi

	kill "$gdb_pid" >/dev/null 2>&1 || true
	wait "$gdb_pid" || true
	wait "$psql_pid"

	shared_hits=$(grep -c '^SHARED_HIT ' "$gdblog" || true)
	local_hits=$(grep -c '^LOCAL_HIT ' "$gdblog" || true)

	printf '%-26s shared=%s local=%s\n' "$name" "$shared_hits" "$local_hits"

	case "$expected" in
		positive)
			if [ "$shared_hits" -eq 0 ]; then
				echo "[$name] expected shared sinval hits, got zero" >&2
				cat "$gdblog" >&2
				exit 1
			fi
			;;
		zero)
			if [ "$shared_hits" -ne 0 ]; then
				echo "[$name] expected zero shared sinval hits" >&2
				cat "$gdblog" >&2
				exit 1
			fi
			;;
		*)
			echo "unknown expectation: $expected" >&2
			exit 1
			;;
	esac
}

run_case regular_analyze positive "ANALYZE t_zero_sinval;"
run_case fasttrun_analyze zero "SELECT fasttrun_analyze('t_zero_sinval');"
run_case fasttrun_collect_stats zero "SELECT fasttrun_collect_stats('t_zero_sinval');"
run_case fasttruncate zero "SELECT fasttruncate('t_zero_sinval');"

echo "zero shared sinval contract verified"
