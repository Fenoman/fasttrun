#!/usr/bin/env bash
# Check the shipped integration helper with real pg_variables and xid epochs.
set -euo pipefail
export LC_ALL=C LANG=C
PG_CONFIG=${PG_CONFIG:-pg_config}
BIN=$($PG_CONFIG --bindir)
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-example.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
PORT=${PGPORT:-55438}
run_pg() {
    if [ -n "$PG_RUN_AS" ]; then runuser -u "$PG_RUN_AS" -- "$@"; else "$@"; fi
}
cleanup() {
    if [ -f "$DATA/postmaster.pid" ]; then
        run_pg "$BIN/pg_ctl" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT
if [ ! -f "$($PG_CONFIG --sharedir)/extension/pg_variables.control" ]; then
    echo 'SKIP: real pg_variables is required' >&2
    exit 77
fi
mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then chown -R "$PG_RUN_AS" "$WORKDIR"; fi
run_pg "$BIN/initdb" -D "$DATA" --no-locale -E UTF8 >"$WORKDIR/initdb.log" 2>&1
sql() { run_pg "$BIN/psql" -XAtq -h "$SOCKET_DIR" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 "$@"; }
for epoch in 0 2; do
    if [ "$epoch" -ne 0 ]; then
        # The server is cleanly stopped. Only this disposable cluster changes.
        if ! run_pg "$BIN/pg_resetwal" -e "$epoch" "$DATA" >"$WORKDIR/epoch.log" 2>&1; then
            if grep -q "invalid option -- 'e'" "$WORKDIR/epoch.log"; then
                echo 'SKIP: forced epoch 2 requires pg_resetwal --epoch (epoch 0 passed)' >&2
                exit 77
            fi
            cat "$WORKDIR/epoch.log" >&2
            exit 1
        fi
    fi
    run_pg "$BIN/pg_ctl" -D "$DATA" -l "$WORKDIR/server.log" -w start \
        -o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun" >/dev/null
    sql -c 'CREATE EXTENSION IF NOT EXISTS fasttrun; CREATE EXTENSION IF NOT EXISTS pg_variables;'
    sql -f "$ROOT/examples/create_temp_table.sql"
    if ! sql >"$WORKDIR/proof.log" 2>&1 <<'SQL'
SELECT extversion FROM pg_extension WHERE extname = 'pg_variables';
BEGIN;
CREATE TEMP TABLE epoch_current(id int);
INSERT INTO epoch_current VALUES (42);
DO $test$ DECLARE result boolean; BEGIN
  result := public.create_temp_table('epoch_current', true);
  IF result IS DISTINCT FROM false
     OR (SELECT count(*) FROM epoch_current WHERE id = 42) <> 1 THEN
    RAISE EXCEPTION 'incoming table from current transaction was cleared';
  END IF;
END $test$;
SAVEPOINT nested;
CREATE TEMP TABLE epoch_nested(id int);
INSERT INTO epoch_nested VALUES (43);
DO $test$ DECLARE result boolean; BEGIN
  result := public.create_temp_table('epoch_nested', true);
  IF result IS DISTINCT FROM false
     OR (SELECT count(*) FROM epoch_nested WHERE id = 43) <> 1 THEN
    RAISE EXCEPTION 'incoming table from subtransaction was cleared';
  END IF;
END $test$;
ROLLBACK TO nested;
COMMIT;
DO $test$ DECLARE result boolean; BEGIN
  result := public.create_temp_table('epoch_current', true);
  IF result IS NOT NULL
     OR (SELECT count(*) FROM epoch_current) <> 0 THEN
    RAISE EXCEPTION 'stale incoming table was not cleared';
  END IF;
  INSERT INTO epoch_current VALUES (44);
  result := public.create_temp_table('epoch_current', true);
  IF result IS DISTINCT FROM false
     OR (SELECT count(*) FROM epoch_current WHERE id = 44) <> 1 THEN
    RAISE EXCEPTION 'successful cleanup ownership was not retained';
  END IF;
END $test$;
CREATE TEMP TABLE epoch_retry(id int);
INSERT INTO epoch_retry VALUES (73);
BEGIN;
DECLARE active_reader CURSOR FOR SELECT * FROM epoch_retry;
DO $test$ BEGIN
  BEGIN
    PERFORM public.create_temp_table('epoch_retry', true);
    RAISE EXCEPTION 'expected active cursor rejection';
  EXCEPTION WHEN object_in_use THEN
    NULL;
  END;
  IF pgv_exists('tx_info', md5('epoch_retry')) THEN
    RAISE EXCEPTION 'failed cleanup claimed incoming data';
  END IF;
END $test$;
CLOSE active_reader;
DO $test$ DECLARE result boolean; BEGIN
  result := public.create_temp_table('epoch_retry', true);
  IF result IS NOT NULL OR (SELECT count(*) FROM epoch_retry) <> 0 THEN
    RAISE EXCEPTION 'cleanup retry kept stale incoming rows';
  END IF;
END $test$;
COMMIT;
DO $$BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='_client') THEN CREATE ROLE _client; END IF;
END$$;
CREATE SCHEMA IF NOT EXISTS dummy_tmp;
CREATE TABLE IF NOT EXISTS dummy_tmp.helper_rewind(id int);
CREATE TABLE IF NOT EXISTS dummy_tmp.helper_recreate(id int, added text);
GRANT USAGE ON SCHEMA dummy_tmp TO _client;
GRANT SELECT ON ALL TABLES IN SCHEMA dummy_tmp TO _client;
CREATE TEMP TABLE helper_rewind(id int);
INSERT INTO helper_rewind VALUES(101);
BEGIN;
SAVEPOINT replace_file;
TRUNCATE helper_rewind;
SELECT create_temp_table('helper_rewind',false);
ROLLBACK TO replace_file;
DO $$DECLARE r boolean;
BEGIN
 IF fasttrun_temp_is_ours('helper_rewind') OR NOT pgv_exists('tx_info',md5('helper_rewind'))
    OR (SELECT count(*) FROM helper_rewind WHERE id=101)<>1 THEN
  RAISE EXCEPTION 'storage rollback fixture is not restored';
 END IF;
 r:=create_temp_table('helper_rewind',true);
 IF r IS NOT NULL OR (SELECT count(*) FROM helper_rewind)<>0 THEN
  RAISE EXCEPTION 'incoming helper trusted a mark from a rolled-back file';
 END IF;
END$$;
COMMIT;
CREATE TEMP TABLE helper_recreate(id int);
INSERT INTO helper_recreate VALUES(201);
BEGIN;
SAVEPOINT replace_table;
SELECT create_temp_table('helper_recreate',false);
ROLLBACK TO replace_table;
DO $$DECLARE r boolean;
BEGIN
 IF fasttrun_temp_is_ours('helper_recreate') OR NOT pgv_exists('tx_info',md5('helper_recreate'))
    OR (SELECT count(*) FROM helper_recreate WHERE id=201)<>1 THEN
  RAISE EXCEPTION 'table recreation rollback fixture is not restored';
 END IF;
 r:=create_temp_table('helper_recreate',true);
 IF r IS NOT NULL OR (SELECT count(*) FROM helper_recreate)<>0 THEN
  RAISE EXCEPTION 'incoming helper trusted a mark from a rolled-back recreation';
 END IF;
END$$;
COMMIT;

DO $$BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='helper_login') THEN CREATE ROLE helper_login LOGIN; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='helper_caller') THEN CREATE ROLE helper_caller; END IF;
END$$;
GRANT helper_caller, _client TO helper_login;
GRANT _client TO helper_caller;
CREATE TABLE IF NOT EXISTS dummy_tmp.helper_role_template(id int);
GRANT SELECT ON dummy_tmp.helper_role_template TO _client;
CREATE TABLE IF NOT EXISTS public.helper_role_rows(owner_role name, payload int PRIMARY KEY);
TRUNCATE public.helper_role_rows;
INSERT INTO public.helper_role_rows VALUES('helper_caller',401),('helper_login',402);
ALTER TABLE public.helper_role_rows ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS helper_identity ON public.helper_role_rows;
CREATE POLICY helper_identity ON public.helper_role_rows USING(owner_role=current_user);
GRANT SELECT ON public.helper_role_rows TO helper_caller,helper_login;
SET SESSION AUTHORIZATION helper_login;
SELECT create_temp_table('helper_role_none',false,'dummy_tmp','helper_role_template');
DO $$BEGIN
 IF current_user<>'helper_login' OR current_setting('role')<>'none' THEN
  RAISE EXCEPTION 'helper did not preserve the default role';
 END IF;
END$$;
SET ROLE helper_caller;
BEGIN;
SELECT create_temp_table('helper_role_session',false,'dummy_tmp','helper_role_template');
DO $$BEGIN
 IF current_user<>'helper_caller' OR current_setting('role')<>'helper_caller'
    OR (SELECT array_agg(payload) FROM public.helper_role_rows) IS DISTINCT FROM ARRAY[401] THEN
  RAISE EXCEPTION 'helper changed the caller role or RLS rows';
 END IF;
 IF (SELECT rolname FROM pg_roles WHERE oid=(SELECT relowner FROM pg_class WHERE oid=to_regclass('pg_temp.helper_role_session'))) IS DISTINCT FROM '_client' THEN
  RAISE EXCEPTION 'helper did not preserve the requested table owner';
 END IF;
END$$;
COMMIT;
DO $$BEGIN
 IF current_user<>'helper_caller' THEN RAISE EXCEPTION 'helper lost the session role at COMMIT'; END IF;
END$$;
RESET ROLE;
BEGIN;
SET LOCAL ROLE helper_caller;
SELECT create_temp_table('helper_role_local',false,'dummy_tmp','helper_role_template');
DO $$BEGIN
 IF current_user<>'helper_caller' THEN RAISE EXCEPTION 'helper lost the local role'; END IF;
 BEGIN
  PERFORM create_temp_table('helper_role_failure',false,'dummy_tmp','absent_role_template');
  RAISE EXCEPTION 'missing-template fixture unexpectedly succeeded';
 EXCEPTION WHEN undefined_table THEN NULL;
 END;
 IF current_user<>'helper_caller' OR current_setting('role')<>'helper_caller' THEN
  RAISE EXCEPTION 'failed helper creation changed the caller role';
 END IF;
END$$;
COMMIT;
DO $$BEGIN
 IF current_user<>'helper_login' OR current_setting('role')<>'none'
    OR (SELECT array_agg(payload) FROM public.helper_role_rows) IS DISTINCT FROM ARRAY[402] THEN
  RAISE EXCEPTION 'helper leaked a transaction-local role beyond COMMIT';
 END IF;
END$$;
RESET SESSION AUTHORIZATION;

DO $$DECLARE r boolean; remaining bigint;
BEGIN
 r:=create_temp_table('helper "quoted"',false,'dummy_tmp','helper_role_template');
 IF r IS DISTINCT FROM true THEN RAISE EXCEPTION 'quoted helper table was not created'; END IF;
 INSERT INTO pg_temp."helper ""quoted""" VALUES(123);
 r:=create_temp_table('helper "quoted"',false,'dummy_tmp','helper_role_template');
 SELECT count(*) INTO remaining FROM pg_temp."helper ""quoted""";
 IF r IS DISTINCT FROM false OR remaining<>0 THEN RAISE EXCEPTION 'quoted helper table was not reused and cleared'; END IF;
END$$;

SELECT 'EXAMPLE_PASS epoch=' || (pg_current_xact_id()::text::bigint >> 32);
SQL
    then
        cat "$WORKDIR/proof.log" >&2
        exit 1
    fi
    cat "$WORKDIR/proof.log"
    grep -qx "EXAMPLE_PASS epoch=$epoch" "$WORKDIR/proof.log"
    run_pg "$BIN/pg_ctl" -D "$DATA" -m fast -w stop >/dev/null
done
