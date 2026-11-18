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
  IF cache_row.analyze_entries <> 0 OR
     cache_row.column_stats_relid_entries <> 0 OR
     cache_row.column_stats_entries <> 0 THEN
    RAISE EXCEPTION 'back-reference failure left cache rows: analyze %, tables %, columns %',
      cache_row.analyze_entries, cache_row.column_stats_relid_entries,
      cache_row.column_stats_entries;
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
  IF cache_row.analyze_entries <> 0 OR
     cache_row.column_stats_relid_entries <> 0 OR
     cache_row.column_stats_entries <> 0 THEN
    RAISE EXCEPTION 'fresh tuple failure left cache rows: analyze %, tables %, columns %',
      cache_row.analyze_entries, cache_row.column_stats_relid_entries,
      cache_row.column_stats_entries;
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

-- A failed reset of a new file must preserve the outer file's saved samples.
CREATE TEMP TABLE ft_column_outer(a int,b int);
INSERT INTO ft_column_outer SELECT 0,0 FROM generate_series(1,10000);
SELECT fasttrun_analyze('ft_column_outer');
BEGIN;
SAVEPOINT outer_file;
TRUNCATE ft_column_outer;
SAVEPOINT failed_reset;
DO $$DECLARE caught boolean:=false;
BEGIN
 PERFORM set_config('fasttrun.test_failpoint','after_main_heap',true);
 BEGIN
  PERFORM fasttruncate('ft_column_outer');
 EXCEPTION WHEN SQLSTATE '55000' THEN
  IF SQLERRM<>'fasttrun test failpoint: after_main_heap' THEN RAISE; END IF;
  caught:=true;
 END;
 PERFORM set_config('fasttrun.test_failpoint','',true);
 IF NOT caught THEN RAISE EXCEPTION 'outer-file reset failpoint did not fire'; END IF;
END$$;
ROLLBACK TO failed_reset;
ROLLBACK TO outer_file;
DO $$DECLARE p json;
BEGIN
 IF (SELECT count(*) FROM fasttrun_inspect_stats('ft_column_outer'))<>2 THEN
  RAISE EXCEPTION 'failed reset discarded outer-file samples';
 END IF;
 EXECUTE 'EXPLAIN (ANALYZE,FORMAT JSON) SELECT * FROM ft_column_outer WHERE a=0' INTO p;
 IF (p->0->'Plan'->>'Plan Rows')::int<>10000 OR (p->0->'Plan'->>'Actual Rows')::numeric<>10000 THEN
  RAISE EXCEPTION 'failed reset damaged outer-file statistics: %',p;
 END IF;
END$$;
COMMIT;
DROP TABLE ft_column_outer;
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

cat >"$WORKDIR/miss.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE FUNCTION pg_temp.fasttrun_test_evict_missing_relid(oid)
RETURNS void
AS '$libdir/fasttrun', 'fasttrun_test_evict_missing_relid'
LANGUAGE C STRICT;

CREATE TEMP TABLE ft_miss(id int, payload text);
INSERT INTO ft_miss
SELECT g, repeat(md5(g::text), 2)
FROM generate_series(1, 2000) g;
SELECT fasttrun_analyze('ft_miss');

DO $case$
DECLARE
  cache_row record;
BEGIN
  SELECT * INTO cache_row FROM fasttrun_cache_stats();
  IF cache_row.analyze_entries = 0 OR
     cache_row.column_stats_relid_entries = 0 OR
     cache_row.column_stats_entries = 0 THEN
    RAISE EXCEPTION 'missing-path fixture did not seed all caches: analyze %, tables %, columns %',
      cache_row.analyze_entries, cache_row.column_stats_relid_entries,
      cache_row.column_stats_entries;
  END IF;
END
$case$;

BEGIN;
SELECT pg_temp.fasttrun_test_evict_missing_relid('ft_miss'::regclass);
ROLLBACK;

DO $case$
DECLARE
  cache_row record;
BEGIN
  SELECT * INTO cache_row FROM fasttrun_cache_stats();
  IF cache_row.analyze_entries = 0 OR
     cache_row.column_stats_relid_entries = 0 OR
     cache_row.column_stats_entries = 0 THEN
    RAISE EXCEPTION 'missing-path rollback did not restore caches: analyze %, tables %, columns %',
      cache_row.analyze_entries, cache_row.column_stats_relid_entries,
      cache_row.column_stats_entries;
  END IF;
END
$case$;

BEGIN;
SELECT pg_temp.fasttrun_test_evict_missing_relid('ft_miss'::regclass);
COMMIT;

DO $case$
DECLARE
  cache_row record;
BEGIN
  SELECT * INTO cache_row FROM fasttrun_cache_stats();
  IF cache_row.analyze_entries <> 0 OR
     cache_row.column_stats_relid_entries <> 0 OR
     cache_row.column_stats_entries <> 0 THEN
    RAISE EXCEPTION 'missing-path commit retained cache rows: analyze %, tables %, columns %',
      cache_row.analyze_entries, cache_row.column_stats_relid_entries,
      cache_row.column_stats_entries;
  END IF;
END
$case$;

SELECT 'PUBLICATION_OK miss';
SQL

cat >"$WORKDIR/cancel.sql" <<'SQL'
\set ON_ERROR_STOP on
SET fasttrun.use_typanalyze=off;
SET fasttrun.sample_rows=50000;
SELECT format('CREATE TEMP TABLE t_cancel_collect AS SELECT %s FROM generate_series(1,50000) g',string_agg(format('((g::bigint * %s) %% 100003)::int AS c%s',n*7919,n),',')) FROM generate_series(1,128) n \gexec
CREATE TEMP TABLE t_cancel_budget(elapsed double precision);
DO $$DECLARE started timestamptz:=clock_timestamp();
BEGIN
 PERFORM fasttrun_collect_stats('t_cancel_collect');
 INSERT INTO t_cancel_budget VALUES(extract(epoch FROM clock_timestamp()-started));
END$$;
CREATE TEMP TABLE t_cancel_saved AS
SELECT staattnum,to_jsonb(s) AS sample FROM fasttrun_inspect_stats('t_cancel_collect') s;
BEGIN;
-- Keep changed data in the parent transaction while only collection is canceled.
UPDATE t_cancel_collect SET c1=NULL WHERE c1<200;
SET statement_timeout='50ms';
DO $$DECLARE started timestamptz:=clock_timestamp(); canceled boolean:=false; elapsed double precision; budget double precision;
BEGIN
 SELECT greatest(0.3,b.elapsed*0.5) INTO budget FROM t_cancel_budget b;
 BEGIN
  PERFORM fasttrun_collect_stats('t_cancel_collect');
 EXCEPTION WHEN query_canceled THEN canceled:=true;
 END;
 elapsed:=extract(epoch FROM clock_timestamp()-started);
 IF NOT canceled OR elapsed>budget THEN
  RAISE EXCEPTION 'collection cancellation delayed: canceled=%, elapsed=%, budget=%',canceled,elapsed,budget;
 END IF;
END$$;
RESET statement_timeout;
DO $$BEGIN
 IF (SELECT count(*) FROM fasttrun_inspect_stats('t_cancel_collect'))<>128 THEN
  RAISE EXCEPTION 'canceled collection did not restore the previous sample';
 END IF;
 IF EXISTS (
  SELECT 1 FROM fasttrun_inspect_stats('t_cancel_collect') s
  FULL JOIN t_cancel_saved old USING(staattnum)
  WHERE to_jsonb(s) IS DISTINCT FROM old.sample
 ) THEN
  RAISE EXCEPTION 'canceled collection changed stored sample contents';
 END IF;
 PERFORM fasttrun_collect_stats('t_cancel_collect');
 IF (SELECT stanullfrac FROM fasttrun_inspect_stats('t_cancel_collect') WHERE staattnum=1)<=0 THEN
  RAISE EXCEPTION 'cancellation fixture does not distinguish old and new samples';
 END IF;
END$$;
ROLLBACK;
DROP TABLE t_cancel_collect,t_cancel_budget,t_cancel_saved;

-- Verify cancellation under use_typanalyze = on as well.
SET fasttrun.use_typanalyze=on;
SET fasttrun.sample_rows=50000;
SELECT format('CREATE TEMP TABLE t_cancel_typ AS SELECT %s FROM generate_series(1,50000) g',string_agg(format('((g::bigint * %s) %% 100003)::text AS c%s',n*7919,n),',')) FROM generate_series(1,96) n \gexec
CREATE TEMP TABLE t_cancel_typ_budget(elapsed double precision);
DO $$DECLARE started timestamptz:=clock_timestamp();
BEGIN
 PERFORM fasttrun_collect_stats('t_cancel_typ');
 INSERT INTO t_cancel_typ_budget VALUES(extract(epoch FROM clock_timestamp()-started));
END$$;
CREATE TEMP TABLE t_cancel_typ_saved AS
SELECT staattnum,to_jsonb(s) AS sample FROM fasttrun_inspect_stats('t_cancel_typ') s;
BEGIN;
UPDATE t_cancel_typ SET c1=NULL WHERE c1<'1000';
SET statement_timeout='50ms';
DO $$DECLARE started timestamptz:=clock_timestamp(); canceled boolean:=false; elapsed double precision; budget double precision;
BEGIN
 SELECT greatest(0.4,b.elapsed*0.6) INTO budget FROM t_cancel_typ_budget b;
 BEGIN
  PERFORM fasttrun_collect_stats('t_cancel_typ');
 EXCEPTION WHEN query_canceled THEN canceled:=true;
 END;
 elapsed:=extract(epoch FROM clock_timestamp()-started);
 IF NOT canceled OR elapsed>budget THEN
  RAISE EXCEPTION 'typanalyze collection cancellation delayed: canceled=%, elapsed=%, budget=%',canceled,elapsed,budget;
 END IF;
END$$;
RESET statement_timeout;
DO $$BEGIN
 IF (SELECT count(*) FROM fasttrun_inspect_stats('t_cancel_typ'))<>96 THEN
  RAISE EXCEPTION 'canceled typanalyze collection did not restore the previous sample';
 END IF;
 IF EXISTS (
  SELECT 1 FROM fasttrun_inspect_stats('t_cancel_typ') s
  FULL JOIN t_cancel_typ_saved old USING(staattnum)
  WHERE to_jsonb(s) IS DISTINCT FROM old.sample
 ) THEN
  RAISE EXCEPTION 'canceled typanalyze collection changed stored sample contents';
 END IF;
END$$;
ROLLBACK;
DROP TABLE t_cancel_typ,t_cancel_typ_budget,t_cancel_typ_saved;

RESET fasttrun.use_typanalyze;
RESET fasttrun.sample_rows;
SELECT 'PUBLICATION_OK cancel';
SQL

cat >"$WORKDIR/template_memo.sql" <<SQL
SELECT pg_advisory_lock(424242), pg_advisory_lock(424244), pg_advisory_lock(424246);
CREATE SCHEMA ftq_schema_live;
CREATE SCHEMA ftq_schema_next;
CREATE TABLE ftq_schema_live.ftq_dataset(a int);
CREATE TABLE ftq_schema_next.ftq_dataset(a int);
ALTER TABLE ftq_schema_live.ftq_dataset ALTER COLUMN a SET STATISTICS 0;
ALTER TABLE ftq_schema_next.ftq_dataset ALTER COLUMN a SET STATISTICS 100;
CREATE TEMP TABLE ftq_dataset(a int);
INSERT INTO ftq_dataset SELECT generate_series(1, 200);
SET fasttrun.template_stattarget = on;
SET fasttrun.prewarm_schema = 'ftq_schema_live';
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT fasttrun_collect_stats('ftq_dataset');
DO \$\$
BEGIN
  IF (SELECT count(*) FROM fasttrun_inspect_stats('ftq_dataset')) <> 0 THEN
    RAISE EXCEPTION 'expected 0 stats columns before schema swap';
  END IF;
END \$\$;
\\! "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq -v ON_ERROR_STOP=1 -c 'BEGIN; ALTER SCHEMA ftq_schema_live RENAME TO ftq_schema_old; ALTER SCHEMA ftq_schema_next RENAME TO ftq_schema_live; COMMIT;'
SELECT fasttrun_collect_stats('ftq_dataset');
DO \$\$
BEGIN
  IF (SELECT count(*) FROM fasttrun_inspect_stats('ftq_dataset')) <> 1 THEN
    RAISE EXCEPTION 'stale template OID survived concurrent schema swap in READ COMMITTED';
  END IF;
END \$\$;
COMMIT;
-- Verify consecutive simple-expression calls (IF fn(...) IS NULL, evaluated via
-- exec_eval_simple_expr with zero ExecutorStart calls) advance statement_seq
-- in fasttrun_collect_stats, fasttrun_analyze, and fasttrun_analyze_bulk.
CREATE SCHEMA ftq_schema_s0;
CREATE SCHEMA ftq_schema_s100a;
CREATE SCHEMA ftq_schema_s0b;
CREATE TABLE ftq_schema_s0.ftq_dataset(a int);
CREATE TABLE ftq_schema_s100a.ftq_dataset(a int);
CREATE TABLE ftq_schema_s0b.ftq_dataset(a int);
ALTER TABLE ftq_schema_s0.ftq_dataset ALTER COLUMN a SET STATISTICS 0;
ALTER TABLE ftq_schema_s100a.ftq_dataset ALTER COLUMN a SET STATISTICS 100;
ALTER TABLE ftq_schema_s0b.ftq_dataset ALTER COLUMN a SET STATISTICS 0;
SELECT set_config('app.s0_oid', ('ftq_schema_s0.ftq_dataset'::regclass::oid)::text, false);
SELECT set_config('app.s100a_oid', ('ftq_schema_s100a.ftq_dataset'::regclass::oid)::text, false);
SELECT set_config('app.s0b_oid', ('ftq_schema_s0b.ftq_dataset'::regclass::oid)::text, false);

-- 1) fasttrun_collect_stats: 1 -> 0 columns inside one simple-expr DO block
BEGIN ISOLATION LEVEL READ COMMITTED;
DO \$\$
DECLARE
  step int;
  ready boolean := false;
BEGIN
  IF fasttrun_collect_stats('ftq_dataset') IS NULL THEN NULL; END IF;
  FOR step IN 1..250 LOOP
    IF NOT pg_try_advisory_lock(424241) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_advisory_unlock(424241) IS NULL THEN NULL; END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 1';
  END IF;
  IF pg_advisory_unlock(424242) IS NULL THEN NULL; END IF;
  ready := false;
  FOR step IN 1..250 LOOP
    IF pg_try_advisory_lock(424241) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 1 completion';
  END IF;
  IF pg_advisory_unlock(424241) IS NULL THEN NULL; END IF;
  IF pg_relation_size(current_setting('app.s0_oid')::oid) < 0 THEN NULL; END IF;
  IF fasttrun_collect_stats('ftq_dataset') IS NULL THEN NULL; END IF;
END \$\$;
DO \$\$
BEGIN
  IF (SELECT count(*) FROM fasttrun_inspect_stats('ftq_dataset')) <> 0 THEN
    RAISE EXCEPTION 'stale template OID survived simple-expr fasttrun_collect_stats';
  END IF;
END \$\$;
COMMIT;

-- 2) fasttrun_analyze: 0 -> 1 column inside one simple-expr DO block
--    (fresh ftq_dataset has no analyze-cache entry yet, so fasttrun_collect_stats
--    populates template_memo while leaving scan_needed=true for fasttrun_analyze).
DROP TABLE ftq_dataset;
CREATE TEMP TABLE ftq_dataset(a int);
INSERT INTO ftq_dataset SELECT generate_series(1, 200);
BEGIN ISOLATION LEVEL READ COMMITTED;
DO \$\$
DECLARE
  step int;
  ready boolean := false;
BEGIN
  IF fasttrun_collect_stats('ftq_dataset') IS NULL THEN NULL; END IF;
  FOR step IN 1..250 LOOP
    IF NOT pg_try_advisory_lock(424243) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_advisory_unlock(424243) IS NULL THEN NULL; END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 2';
  END IF;
  IF pg_advisory_unlock(424244) IS NULL THEN NULL; END IF;
  ready := false;
  FOR step IN 1..250 LOOP
    IF pg_try_advisory_lock(424243) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 2 completion';
  END IF;
  IF pg_advisory_unlock(424243) IS NULL THEN NULL; END IF;
  IF pg_relation_size(current_setting('app.s100a_oid')::oid) < 0 THEN NULL; END IF;
  IF fasttrun_analyze('ftq_dataset') IS NULL THEN NULL; END IF;
END \$\$;
DO \$\$
BEGIN
  IF (SELECT count(*) FROM fasttrun_inspect_stats('ftq_dataset')) <> 1 THEN
    RAISE EXCEPTION 'stale template OID survived simple-expr fasttrun_analyze';
  END IF;
END \$\$;
COMMIT;

-- 3) fasttrun_analyze_bulk: 1 -> 0 columns inside one simple-expr DO block
DROP TABLE ftq_dataset;
CREATE TEMP TABLE ftq_dataset(a int);
INSERT INTO ftq_dataset SELECT generate_series(1, 200);
BEGIN ISOLATION LEVEL READ COMMITTED;
DO \$\$
DECLARE
  step int;
  ready boolean := false;
BEGIN
  IF fasttrun_collect_stats('ftq_dataset') IS NULL THEN NULL; END IF;
  FOR step IN 1..250 LOOP
    IF NOT pg_try_advisory_lock(424245) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_advisory_unlock(424245) IS NULL THEN NULL; END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 3';
  END IF;
  IF pg_advisory_unlock(424246) IS NULL THEN NULL; END IF;
  ready := false;
  FOR step IN 1..250 LOOP
    IF pg_try_advisory_lock(424245) THEN
      ready := true;
      EXIT;
    END IF;
    IF pg_sleep(0.02) IS NULL THEN NULL; END IF;
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for schema swap worker 3 completion';
  END IF;
  IF pg_advisory_unlock(424245) IS NULL THEN NULL; END IF;
  IF pg_relation_size(current_setting('app.s0b_oid')::oid) < 0 THEN NULL; END IF;
  IF fasttrun_analyze_bulk('ftq_dataset') IS NULL THEN NULL; END IF;
END \$\$;
DO \$\$
BEGIN
  IF (SELECT count(*) FROM fasttrun_inspect_stats('ftq_dataset')) <> 0 THEN
    RAISE EXCEPTION 'stale template OID survived simple-expr fasttrun_analyze_bulk';
  END IF;
END \$\$;
COMMIT;
DROP TABLE ftq_dataset;
DROP SCHEMA ftq_schema_old, ftq_schema_prev1, ftq_schema_prev2, ftq_schema_prev3, ftq_schema_live CASCADE;
RESET fasttrun.template_stattarget;
RESET fasttrun.prewarm_schema;
SELECT 'PUBLICATION_OK template_memo';
SQL

cat >"$WORKDIR/template_memo_worker.sql" <<'SQL'
SET lock_timeout = '5s';
DO $$
DECLARE step int; ready boolean := false;
BEGIN
  FOR step IN 1..250 LOOP
    IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 424242) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.02);
  END LOOP;
  IF NOT ready THEN RAISE EXCEPTION 'worker did not observe main advisory lock 424242'; END IF;
END $$;
SELECT pg_advisory_lock(424241);
SELECT pg_advisory_lock(424242);
BEGIN;
ALTER SCHEMA ftq_schema_live RENAME TO ftq_schema_prev1;
ALTER SCHEMA ftq_schema_s0 RENAME TO ftq_schema_live;
COMMIT;
SELECT pg_advisory_unlock(424242);
SELECT pg_advisory_unlock(424241);

SELECT pg_advisory_lock(424243);
SELECT pg_advisory_lock(424244);
BEGIN;
ALTER SCHEMA ftq_schema_live RENAME TO ftq_schema_prev2;
ALTER SCHEMA ftq_schema_s100a RENAME TO ftq_schema_live;
COMMIT;
SELECT pg_advisory_unlock(424244);
SELECT pg_advisory_unlock(424243);

SELECT pg_advisory_lock(424245);
SELECT pg_advisory_lock(424246);
BEGIN;
ALTER SCHEMA ftq_schema_live RENAME TO ftq_schema_prev3;
ALTER SCHEMA ftq_schema_s0b RENAME TO ftq_schema_live;
COMMIT;
SELECT pg_advisory_unlock(424246);
SELECT pg_advisory_unlock(424245);
SQL

terminate_memo_worker()
{
	local pid=${1:-}
	local i
	[ -n "$pid" ] || return 0
	kill -TERM "$pid" >/dev/null 2>&1 || true
	kill -CONT "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	kill -KILL "$pid" >/dev/null 2>&1 || true
	kill -CONT "$pid" >/dev/null 2>&1 || true
	for i in $(seq 1 20); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" >/dev/null 2>&1 || true
			return 0
		fi
		sleep 0.05
	done
	return 0
}

wait_memo_worker()
{
	local pid=$1
	local ticks=${2:-}
	local state_file=${3:-}
	local i
	local rc=0
	local pstate=""
	[ -n "$ticks" ] || ticks=${FASTTRUN_WAIT_TICKS:-200}
	for i in $(seq 1 "$ticks"); do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			wait "$pid" || rc=$?
			return "$rc"
		fi
		sleep 0.05
	done
	if [ -n "$state_file" ]; then
		pstate=$(ps -o state= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
		printf 'pid=%s state=%s\n' "$pid" "$pstate" >"$state_file"
	fi
	terminate_memo_worker "$pid"
	return 124
}

run_memo_pair()
{
	local label=$1
	local main_sql=$2
	local worker_sql=$3
	local out=$4
	local err=$5
	local wout=$6
	local werr=$7
	local wait_ticks=${8:-}
	local stop_marker=${9:-}
	local state_file=${10:-}
	local worker_pid=""
	local stopper_pid=""
	local worker_rc=0
	local pstate=""

	"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -f "$worker_sql" >"$wout" 2>"$werr" &
	worker_pid=$!

	if [ -n "$stop_marker" ]; then
		(
			for _ in $(seq 1 200); do
				if grep -q "^${stop_marker}\$" "$wout" 2>/dev/null; then
					kill -STOP "$worker_pid" >/dev/null 2>&1 || true
					exit 0
				fi
				if ! kill -0 "$worker_pid" >/dev/null 2>&1; then
					exit 0
				fi
				sleep 0.01
			done
		) &
		stopper_pid=$!
	fi

	if ! "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 -f "$main_sql" >"$out" 2>"$err"; then
		if [ -n "$stopper_pid" ]; then
			wait "$stopper_pid" >/dev/null 2>&1 || true
		fi
		if [ -n "$state_file" ]; then
			pstate=$(ps -o state= -p "$worker_pid" 2>/dev/null | tr -d '[:space:]' || true)
			printf 'pid=%s state=%s\n' "$worker_pid" "$pstate" >"$state_file"
		fi
		terminate_memo_worker "$worker_pid"
		cat "$werr" >&2 || true
		cat "$out" "$err" >&2
		echo "FAIL: publication case $label" >&2
		return 1
	fi

	wait_memo_worker "$worker_pid" "$wait_ticks" "$state_file" || worker_rc=$?
	if [ -n "$stopper_pid" ]; then
		wait "$stopper_pid" >/dev/null 2>&1 || true
	fi
	if [ "$worker_rc" -ne 0 ]; then
		cat "$wout" "$werr" >&2 || true
		echo "FAIL: $label worker exited with rc=$worker_rc" >&2
		return 1
	fi
	return 0
}

verify_stopped_memo_worker()
{
	local hold_pid=""
	local hold_state=""
	local exit_pid=""
	local exit_state=""

	cat >"$WORKDIR/memo_stop_hold_worker.sql" <<'SQL'
SET lock_timeout = '5s';
DO $$
DECLARE step int; ready boolean := false;
BEGIN
  FOR step IN 1..250 LOOP
    IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 424290) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.01);
  END LOOP;
  IF NOT ready THEN RAISE EXCEPTION 'worker did not observe main advisory lock 424290'; END IF;
END $$;
SELECT pg_advisory_lock(424291);
SELECT 'HOLD_READY';
SELECT pg_sleep(0.06);
SELECT pg_sleep(0.06);
SELECT pg_advisory_unlock(424291);
SQL
	cat >"$WORKDIR/memo_stop_hold_main.sql" <<'SQL'
SELECT pg_advisory_lock(424290);
DO $$
DECLARE
  step int;
  ready boolean := false;
BEGIN
  FOR step IN 1..200 LOOP
    IF NOT pg_try_advisory_lock(424291) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_advisory_unlock(424291);
    PERFORM pg_sleep(0.01);
  END LOOP;
  PERFORM pg_advisory_unlock(424290);
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for hold_ready worker';
  END IF;
  ready := false;
  FOR step IN 1..40 LOOP
    IF pg_try_advisory_lock(424291) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.02);
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'expected completion timeout on stopped worker';
  END IF;
  PERFORM pg_advisory_unlock(424291);
END $$;
SQL
	if run_memo_pair "memo_stop_hold" \
		"$WORKDIR/memo_stop_hold_main.sql" \
		"$WORKDIR/memo_stop_hold_worker.sql" \
		"$WORKDIR/memo_stop_hold.out" \
		"$WORKDIR/memo_stop_hold.err" \
		"$WORKDIR/memo_stop_hold_w.out" \
		"$WORKDIR/memo_stop_hold_w.err" \
		16 "HOLD_READY" "$WORKDIR/memo_stop_hold.state" >/dev/null 2>"$WORKDIR/memo_stop_hold.diag"; then
		echo "FAIL: stopped worker during lock hold unexpectedly succeeded" >&2
		return 1
	fi
	if ! grep -q "expected completion timeout on stopped worker" "$WORKDIR/memo_stop_hold.diag"; then
		cat "$WORKDIR/memo_stop_hold.diag" >&2
		echo "FAIL: missing expected completion timeout for stopped worker" >&2
		return 1
	fi
	if ! grep -Eq '^pid=[0-9]+ state=T' "$WORKDIR/memo_stop_hold.state"; then
		cat "$WORKDIR/memo_stop_hold.state" >&2 || true
		echo "FAIL: memo_stop_hold worker was not observed in stopped state T before cleanup" >&2
		return 1
	fi
	hold_pid=$(sed -n 's/^pid=\([0-9][0-9]*\) .*/\1/p' "$WORKDIR/memo_stop_hold.state")
	hold_state=$(sed -n 's/^pid=[0-9][0-9]* state=\(.*\)/\1/p' "$WORKDIR/memo_stop_hold.state")
	if [ -z "$hold_pid" ] || kill -0 "$hold_pid" >/dev/null 2>&1; then
		echo "FAIL: memo_stop_hold worker pid=$hold_pid still alive after cleanup" >&2
		return 1
	fi

	cat >"$WORKDIR/memo_stop_exit_worker.sql" <<'SQL'
SET lock_timeout = '5s';
DO $$
DECLARE step int; ready boolean := false;
BEGIN
  FOR step IN 1..250 LOOP
    IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 424293) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.01);
  END LOOP;
  IF NOT ready THEN RAISE EXCEPTION 'worker did not observe main advisory lock 424293'; END IF;
END $$;
SELECT pg_advisory_lock(424292);
SELECT pg_advisory_lock(424293);
SELECT pg_advisory_unlock(424293);
SELECT CASE WHEN pg_advisory_unlock(424292) THEN 'EXIT_READY' END;
SELECT pg_sleep(0.06);
SELECT pg_sleep(0.06);
SQL
	cat >"$WORKDIR/memo_stop_exit_main.sql" <<'SQL'
SELECT pg_advisory_lock(424293);
DO $$
DECLARE
  step int;
  ready boolean := false;
BEGIN
  FOR step IN 1..200 LOOP
    IF NOT pg_try_advisory_lock(424292) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_advisory_unlock(424292);
    PERFORM pg_sleep(0.01);
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'timeout waiting for exit_ready worker';
  END IF;
  PERFORM pg_advisory_unlock(424293);
  ready := false;
  FOR step IN 1..200 LOOP
    IF pg_try_advisory_lock(424292) THEN
      ready := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.01);
  END LOOP;
  IF NOT ready THEN
    RAISE EXCEPTION 'worker did not unlock 424292 before stop';
  END IF;
  PERFORM pg_advisory_unlock(424292);
END $$;
SQL
	if run_memo_pair "memo_stop_exit" \
		"$WORKDIR/memo_stop_exit_main.sql" \
		"$WORKDIR/memo_stop_exit_worker.sql" \
		"$WORKDIR/memo_stop_exit.out" \
		"$WORKDIR/memo_stop_exit.err" \
		"$WORKDIR/memo_stop_exit_w.out" \
		"$WORKDIR/memo_stop_exit_w.err" \
		16 "EXIT_READY" "$WORKDIR/memo_stop_exit.state" >/dev/null 2>"$WORKDIR/memo_stop_exit.diag"; then
		echo "FAIL: stopped worker after unlock unexpectedly succeeded" >&2
		return 1
	fi
	if ! grep -q "FAIL: memo_stop_exit worker exited with rc=124" "$WORKDIR/memo_stop_exit.diag"; then
		cat "$WORKDIR/memo_stop_exit.diag" >&2
		echo "FAIL: missing rc=124 bounded wait failure for stopped worker" >&2
		return 1
	fi
	if ! grep -Eq '^pid=[0-9]+ state=T' "$WORKDIR/memo_stop_exit.state"; then
		cat "$WORKDIR/memo_stop_exit.state" >&2 || true
		echo "FAIL: memo_stop_exit worker was not observed in stopped state T before cleanup" >&2
		return 1
	fi
	exit_pid=$(sed -n 's/^pid=\([0-9][0-9]*\) .*/\1/p' "$WORKDIR/memo_stop_exit.state")
	exit_state=$(sed -n 's/^pid=[0-9][0-9]* state=\(.*\)/\1/p' "$WORKDIR/memo_stop_exit.state")
	if [ -z "$exit_pid" ] || kill -0 "$exit_pid" >/dev/null 2>&1; then
		echo "FAIL: memo_stop_exit worker pid=$exit_pid still alive after cleanup" >&2
		return 1
	fi
	echo "ok: stopped template_memo worker bounded cleanup verified (hold pid=$hold_pid state=$hold_state, exit pid=$exit_pid state=$exit_state)"
}

run_case()
{
	local name=$1
	local out="$WORKDIR/$name.out"
	local err="$WORKDIR/$name.err"

	if [ "$name" = "template_memo" ]; then
		run_memo_pair "$name" \
			"$WORKDIR/$name.sql" \
			"$WORKDIR/template_memo_worker.sql" \
			"$out" "$err" \
			"$WORKDIR/template_memo_worker.out" \
			"$WORKDIR/template_memo_worker.err" || return 1
		verify_stopped_memo_worker || return 1
	else
		if ! "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
			-v ON_ERROR_STOP=1 -f "$WORKDIR/$name.sql" >"$out" 2>"$err"; then
			cat "$out" "$err" >&2
			echo "FAIL: publication case $name" >&2
			return 1
		fi
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
		run_case miss
		run_case cancel
		run_case template_memo
		;;
	reserve|index|column|policy|miss|cancel|template_memo)
		run_case "$CASE_NAME"
		;;
	*)
		echo "unknown FASTTRUN_PUBLICATION_CASE: $CASE_NAME" >&2
		exit 2
		;;
esac

if grep -Eq 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|leaked hash_seq_search' "$LOG"; then
	grep -E 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|leaked hash_seq_search' "$LOG" >&2
	echo "FAIL: publication atomicity caused an assertion or crash" >&2
	exit 1
fi

echo "publication atomicity passed: $CASE_NAME"
