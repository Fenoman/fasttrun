#!/usr/bin/env bash
# Проверяет атомарную публикацию кешей при перехватываемых ошибках.
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-publication.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_publication}
CASE_NAME=${FASTTRUN_PUBLICATION_CASE:-all}

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
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on" \
	-w start >/dev/null
"$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

if [ "$("$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')" != on ]; then
	echo "SKIP: publication atomicity requires a cassert PostgreSQL build" >&2
	exit 77
fi

cat >"$WORKDIR/reserve.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE FUNCTION pg_temp.fasttrun_test_subxact_visits(boolean)
RETURNS bigint
AS '$libdir/fasttrun', 'fasttrun_test_subxact_visits'
LANGUAGE C STRICT;

CREATE TEMP TABLE ft_reserve(id int PRIMARY KEY, grp int, payload text);
INSERT INTO ft_reserve
SELECT g, g % 17, repeat(md5(g::text), 4)
FROM generate_series(1, 5000) g;
SELECT fasttrun_analyze('ft_reserve');

DO $case$
DECLARE
  before_columns bigint;
  after_columns bigint;
  before_tuples float4;
  after_tuples float4;
  visits bigint;
  caught boolean := false;
BEGIN
  SELECT count(*) INTO before_columns
  FROM fasttrun_inspect_stats('ft_reserve');
  SELECT reltuples INTO before_tuples FROM fasttrun_relstats('ft_reserve');
  PERFORM pg_temp.fasttrun_test_subxact_visits(true);
  PERFORM set_config('fasttrun.test_failpoint', 'reserve_after_state:1', false);
  BEGIN
    PERFORM fasttruncate('ft_reserve');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: reserve_after_state:1' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'reserve failpoint did not fire';
  END IF;
  visits := pg_temp.fasttrun_test_subxact_visits(false);
  IF visits = 0 THEN
    RAISE EXCEPTION 'reserve changed cache state without a journal frame';
  END IF;
  SELECT count(*) INTO after_columns
  FROM fasttrun_inspect_stats('ft_reserve');
  SELECT reltuples INTO after_tuples FROM fasttrun_relstats('ft_reserve');
  IF after_columns <> before_columns OR after_tuples <> before_tuples THEN
    RAISE EXCEPTION 'reserve error did not restore cache: columns %/%, tuples %/%',
      before_columns, after_columns, before_tuples, after_tuples;
  END IF;
  PERFORM fasttruncate('ft_reserve');
  IF (SELECT count(*) FROM ft_reserve) <> 0 THEN
    RAISE EXCEPTION 'reserve retry did not empty the table';
  END IF;
  INSERT INTO ft_reserve VALUES (1, 1, 'ok');
  PERFORM set_config('enable_seqscan', 'off', true);
  IF (SELECT count(*) FROM ft_reserve WHERE id = 1) <> 1 THEN
    RAISE EXCEPTION 'reserve retry left the index unusable';
  END IF;
END
$case$;

SELECT 'PUBLICATION_OK reserve';
SQL

cat >"$WORKDIR/index.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE FUNCTION pg_temp.fasttrun_test_raw_relpages(oid)
RETURNS integer
AS '$libdir/fasttrun', 'fasttrun_test_raw_relpages'
LANGUAGE C STRICT;

SET fasttrun.auto_collect_stats = off;
SET fasttrun.sample_rows = 0;
CREATE TEMP TABLE ft_index(id int, grp int, payload text);
CREATE INDEX ft_index_grp_idx ON ft_index(grp);
INSERT INTO ft_index
SELECT g, g % 101, repeat(md5(g::text), 2)
FROM generate_series(1, 2000) g;
SELECT fasttrun_analyze('ft_index');
CREATE TEMP TABLE ft_index_meta AS
SELECT pg_temp.fasttrun_test_raw_relpages('ft_index_grp_idx'::regclass) AS old_pages;
INSERT INTO ft_index
SELECT g, g % 101, repeat(md5(g::text), 2)
FROM generate_series(2001, 102000) g;

DO $case$
DECLARE
  old_pages integer;
  physical_pages bigint;
  before_pages integer;
  after_pages integer;
  caught boolean := false;
BEGIN
  SELECT m.old_pages INTO old_pages FROM ft_index_meta m;
  physical_pages := pg_relation_size('ft_index_grp_idx') /
    current_setting('block_size')::integer;
  before_pages := pg_temp.fasttrun_test_raw_relpages('ft_index_grp_idx'::regclass);
  IF physical_pages <= old_pages OR before_pages <> old_pages THEN
    RAISE EXCEPTION 'index fixture did not create relpages drift: old %, raw %, physical %',
      old_pages, before_pages, physical_pages;
  END IF;
  PERFORM set_config('fasttrun.test_failpoint', 'index_publication_boundary', false);
  BEGIN
    PERFORM fasttrun_analyze('ft_index');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: index_publication_boundary' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'index publication failpoint did not fire';
  END IF;
  after_pages := pg_temp.fasttrun_test_raw_relpages('ft_index_grp_idx'::regclass);
  IF after_pages <> old_pages THEN
    RAISE EXCEPTION 'index rd_rel changed before cache publication: old %, after %',
      old_pages, after_pages;
  END IF;
  PERFORM fasttrun_analyze('ft_index');
  after_pages := pg_temp.fasttrun_test_raw_relpages('ft_index_grp_idx'::regclass);
  IF after_pages <> physical_pages THEN
    RAISE EXCEPTION 'index retry did not publish relpages: expected %, got %',
      physical_pages, after_pages;
  END IF;
END
$case$;

SELECT 'PUBLICATION_OK index';
SQL

cat >"$WORKDIR/column.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE TEMP TABLE ft_column(id int, grp int, payload text);
INSERT INTO ft_column
SELECT g, g % 29, repeat(md5(g::text), 3)
FROM generate_series(1, 3000) g;

DO $case$
DECLARE
  cache_row record;
  before_count bigint;
  after_count bigint;
  before_width bigint;
  after_width bigint;
  before_distinct double precision;
  after_distinct double precision;
  before_nullfrac double precision;
  after_nullfrac double precision;
  caught boolean := false;
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', 'after_stats_relid_enter', false);
  BEGIN
    PERFORM fasttrun_collect_stats('ft_column');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: after_stats_relid_enter' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'column back-reference failpoint did not fire';
  END IF;
  SELECT * INTO cache_row FROM fasttrun_cache_stats();
  IF cache_row.analyze_tables <> 0 OR cache_row.stats_tables <> 0 OR
     cache_row.stats_columns <> 0 THEN
    RAISE EXCEPTION 'back-reference failure left cache rows: analyze %, tables %, columns %',
      cache_row.analyze_tables, cache_row.stats_tables, cache_row.stats_columns;
  END IF;

  caught := false;
  PERFORM set_config('fasttrun.test_failpoint', 'before_stats_tuple_copy', false);
  BEGIN
    PERFORM fasttrun_collect_stats('ft_column');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: before_stats_tuple_copy' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'column tuple failpoint did not fire';
  END IF;
  SELECT * INTO cache_row FROM fasttrun_cache_stats();
  IF cache_row.analyze_tables <> 0 OR cache_row.stats_tables <> 0 OR
     cache_row.stats_columns <> 0 THEN
    RAISE EXCEPTION 'fresh tuple failure left cache rows: analyze %, tables %, columns %',
      cache_row.analyze_tables, cache_row.stats_tables, cache_row.stats_columns;
  END IF;

  PERFORM fasttrun_collect_stats('ft_column');
  SELECT count(*), sum(stawidth), sum(stadistinct), sum(stanullfrac)
    INTO before_count, before_width, before_distinct, before_nullfrac
  FROM fasttrun_inspect_stats('ft_column');
  IF before_count = 0 THEN
    RAISE EXCEPTION 'column retry did not publish statistics';
  END IF;

  caught := false;
  PERFORM set_config('fasttrun.test_failpoint', 'before_stats_tuple_copy', false);
  BEGIN
    PERFORM fasttrun_collect_stats('ft_column');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: before_stats_tuple_copy' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
  IF NOT caught THEN
    RAISE EXCEPTION 'column overwrite failpoint did not fire';
  END IF;
  SELECT count(*), sum(stawidth), sum(stadistinct), sum(stanullfrac)
    INTO after_count, after_width, after_distinct, after_nullfrac
  FROM fasttrun_inspect_stats('ft_column');
  IF after_count <> before_count OR after_width <> before_width OR
     after_distinct IS DISTINCT FROM before_distinct OR
     after_nullfrac IS DISTINCT FROM before_nullfrac THEN
    RAISE EXCEPTION 'column overwrite failure changed old statistics';
  END IF;
END
$case$;

SELECT 'PUBLICATION_OK column';
SQL

cat >"$WORKDIR/policy.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE FUNCTION pg_temp.fasttrun_test_relid_undo_depth(oid)
RETURNS bigint
AS '$libdir/fasttrun', 'fasttrun_test_relid_undo_depth'
LANGUAGE C STRICT;

SET fasttrun.auto_collect_stats = off;
SET fasttrun.sample_rows = 0;
CREATE TEMP TABLE ft_policy(id int);
INSERT INTO ft_policy SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('ft_policy');

DO $case$
BEGIN
  IF pg_temp.fasttrun_test_relid_undo_depth('ft_policy'::regclass) <> 0 THEN
    RAISE EXCEPTION 'policy fixture retained undo state';
  END IF;
END
$case$;

BEGIN;
SELECT fasttrun_analyze('ft_policy');
DO $case$
DECLARE
  depth bigint;
BEGIN
  depth := pg_temp.fasttrun_test_relid_undo_depth('ft_policy'::regclass);
  IF depth <> 0 THEN
    RAISE EXCEPTION 'unchanged relation policy created undo state: depth %', depth;
  END IF;
END
$case$;
ROLLBACK;

DO $case$
BEGIN
  IF pg_temp.fasttrun_test_relid_undo_depth('ft_policy'::regclass) <> 0 THEN
    RAISE EXCEPTION 'policy rollback retained undo state';
  END IF;
END
$case$;

SELECT 'PUBLICATION_OK policy';
SQL

run_case()
{
	local name=$1
	local out="$WORKDIR/$name.out"
	local err="$WORKDIR/$name.err"

	if ! "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -f "$WORKDIR/$name.sql" >"$out" 2>"$err"; then
		cat "$out" "$err" >&2
		echo "FAIL: publication case $name" >&2
		return 1
	fi
	if ! grep -q "^PUBLICATION_OK $name$" "$out"; then
		cat "$out" "$err" >&2
		echo "FAIL: missing publication marker for $name" >&2
		return 1
	fi
	echo "publication case passed: $name"
}

case "$CASE_NAME" in
	all)
		run_case reserve
		run_case index
		run_case column
		run_case policy
		;;
	reserve|index|column|policy)
		run_case "$CASE_NAME"
		;;
	*)
		echo "unknown FASTTRUN_PUBLICATION_CASE: $CASE_NAME" >&2
		exit 2
		;;
esac

echo "publication atomicity passed: $CASE_NAME"
