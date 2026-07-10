#!/usr/bin/env bash
#
# Проверяет стабильный порядок статистики часто создаваемых таблиц. Требует
# сборку с cassert и shared_preload_libraries=fasttrun для тестовой загрузки.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-tracking-order.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_tracking_order}

cleanup()
{
	if [ -f "$DATA/postmaster.pid" ]; then
		"$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

"$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun" \
	-w start >/dev/null
"$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

if [ "$("$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')" != on ]; then
	echo "SKIP: для проверки порядка нужна сборка PostgreSQL с cassert" >&2
	exit 77
fi

cat >"$WORKDIR/check.sql" <<'SQL'
\set ON_ERROR_STOP 1
SELECT fasttrun_reset_temp_stats();
CREATE FUNCTION pg_temp.fasttrun_test_track_set(text, bigint, bigint)
RETURNS void
AS '$libdir/fasttrun', 'fasttrun_test_track_set'
LANGUAGE C STRICT;

SELECT pg_temp.fasttrun_test_track_set('alpha', 10, 1000);
SELECT pg_temp.fasttrun_test_track_set('beta',  10, 1000);
SELECT pg_temp.fasttrun_test_track_set('gamma', 10, 2000);
SELECT pg_temp.fasttrun_test_track_set('delta', 11,  500);
SELECT 'FULL_ORDER ' || string_agg(relname, ',' ORDER BY ord)
FROM fasttrun_hot_temp_tables(0) WITH ORDINALITY
  AS t(relname, create_count, last_create, ord);

SELECT pg_temp.fasttrun_test_track_set(
       'n' || lpad(g::text, 2, '0'), 100, 1000 + g)
FROM generate_series(0, 19) g;
SELECT 'TOP_ONE ' || relname
FROM fasttrun_hot_temp_tables(1);

SELECT pg_temp.fasttrun_test_track_set('tie_b', 200, 5000);
SELECT pg_temp.fasttrun_test_track_set('tie_a', 200, 5000);
SELECT 'TIE_ORDER ' || string_agg(relname, ',' ORDER BY ord)
FROM fasttrun_hot_temp_tables(2) WITH ORDINALITY
  AS t(relname, create_count, last_create, ord);
SQL

"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/check.sql" >"$WORKDIR/check.out" 2>"$WORKDIR/check.err" || {
	cat "$WORKDIR/check.out" "$WORKDIR/check.err" >&2
	exit 1
}

grep -q '^FULL_ORDER delta,gamma,alpha,beta$' "$WORKDIR/check.out" || {
	cat "$WORKDIR/check.out" >&2
	echo "FAIL: full tracking order is not deterministic" >&2
	exit 1
}
grep -q '^TOP_ONE n19$' "$WORKDIR/check.out" || {
	cat "$WORKDIR/check.out" >&2
	echo "FAIL: top-1 ignored last_create tie-break" >&2
	exit 1
}
grep -q '^TIE_ORDER tie_a,tie_b$' "$WORKDIR/check.out" || {
	cat "$WORKDIR/check.out" >&2
	echo "FAIL: relname tie-break is not ascending" >&2
	exit 1
}
if grep -Eq 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG"; then
	grep -E 'TRAP|Assertion|PANIC|server process .* was terminated' "$LOG" >&2
	exit 1
fi

echo "tracking order passed: count DESC, timestamp DESC, relname ASC"
