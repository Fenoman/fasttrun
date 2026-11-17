#!/usr/bin/env bash
#
# Проверяет журнал изменений по уровням подтранзакций. Обработчик должен
# обходить только завершившийся уровень, а после отката память должна
# возвращаться к прогретому уровню в том же серверном процессе.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-xact-journal.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_xact_journal}

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
	if [ -f "$DATA/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then
	command -v runuser >/dev/null 2>&1 || { echo "missing runuser" >&2; exit 1; }
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR"
fi
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on -c max_locks_per_transaction=256" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

debug_assertions=$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')
if [ "$debug_assertions" != on ] && [ "${FASTTRUN_ALLOW_RELEASE_TEST_BUILD:-0}" != 1 ]; then
	echo "SKIP: для проверки журнала нужна сборка PostgreSQL с cassert" >&2
	exit 77
fi

cat >"$WORKDIR/check.sql" <<'SQL'
\set ON_ERROR_STOP 1
CREATE EXTENSION fasttrun;
CREATE FUNCTION pg_temp.fasttrun_test_subxact_visits(boolean)
RETURNS bigint
AS '$libdir/fasttrun', 'fasttrun_test_subxact_visits'
LANGUAGE C STRICT;

DO $warm$
DECLARE i int;
BEGIN
  FOR i IN 1..20 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_warm_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_warm_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_warm_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат прогретого уровня';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$warm$;

SELECT 'WARM_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'WARM_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'WARM_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';

BEGIN;
SELECT pg_temp.fasttrun_test_subxact_visits(true);
DO $visits$
DECLARE i int;
BEGIN
  FOR i IN 1..200 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_visit_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_visit_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_visit_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат уровня обхода';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$visits$;
SELECT 'VISITS ' || pg_temp.fasttrun_test_subxact_visits(false);
SELECT 'VISIT_FRAMES ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame' AND ident IS NULL;
SELECT 'VISIT_POOL ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame' AND ident = 'pooled';
COMMIT;

/* Вложенные уровни одной таблицы восстанавливают согласованное состояние. */
CREATE TEMP TABLE ft_same_relid(id int PRIMARY KEY, payload text);
INSERT INTO ft_same_relid VALUES (1, 'original');
SELECT fasttrun_analyze('ft_same_relid');
BEGIN;
DO $nested$
BEGIN
  BEGIN
    UPDATE ft_same_relid SET payload = 'outer' WHERE id = 1;
    PERFORM fasttrun_analyze('ft_same_relid');
    BEGIN
      INSERT INTO ft_same_relid VALUES (2, 'inner');
      PERFORM fasttrun_analyze('ft_same_relid');
      RAISE EXCEPTION USING MESSAGE = 'rollback inner same-relid frame';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback inner same-relid frame' THEN
        RAISE;
      END IF;
    END;
    RAISE EXCEPTION USING MESSAGE = 'rollback outer same-relid frame';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'rollback outer same-relid frame' THEN
      RAISE;
    END IF;
  END;
END
$nested$;
COMMIT;
SELECT 'NESTED_ROWS ' || count(*) FROM ft_same_relid;
SELECT 'NESTED_ORIGINAL ' || count(*)
FROM ft_same_relid WHERE id = 1 AND payload = 'original';
SELECT 'NESTED_FRAMES ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame' AND ident IS NULL;
DROP TABLE ft_same_relid;

BEGIN;
DO $memory$
DECLARE i int;
BEGIN
  FOR i IN 1..500 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE ft_frame_%s (id int)', i);
      EXECUTE format('INSERT INTO ft_frame_%s VALUES (1)', i);
      PERFORM fasttrun_analyze(format('ft_frame_%s', i));
      RAISE EXCEPTION USING MESSAGE = 'откат уровня';
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
END
$memory$;
SELECT 'FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'FINAL_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'FINAL_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SELECT 'FINAL_FRAMES ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame' AND ident IS NULL;
SELECT 'FINAL_POOL ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun xact frame' AND ident = 'pooled';
SELECT 'FINAL_CONTEXT ' || name || ' ' || used_bytes
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%'
ORDER BY name;
COMMIT;

/* Successful calls must release the state left by earlier calls. */
CREATE TEMP TABLE ft_truncate_memory(
  id int PRIMARY KEY,
  payload text
);
ALTER TABLE ft_truncate_memory ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE TEMP TABLE ft_payload AS
SELECT string_agg(md5(s::text), '') AS value
FROM generate_series(1, 450) s;
INSERT INTO ft_truncate_memory SELECT 1, value FROM ft_payload;

BEGIN;
DO $truncate_warm$
DECLARE i int;
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT i, value FROM ft_payload;
  END LOOP;
END
$truncate_warm$;
SELECT 'TRUNCATE_WARM_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

DO $truncate_final$
DECLARE i int;
BEGIN
  FOR i IN 101..2000 LOOP
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT i, value FROM ft_payload;
  END LOOP;
END
$truncate_final$;
SELECT 'TRUNCATE_FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';
COMMIT;

/* A successful retry after each caught error must release the block. */
DO $poison_cycles$
DECLARE i int;
BEGIN
  FOR i IN 1..200 LOOP
    PERFORM set_config('fasttrun.test_failpoint', 'after_user_index:1', false);
    BEGIN
      PERFORM fasttruncate('ft_truncate_memory');
    EXCEPTION WHEN SQLSTATE '55000' THEN
      NULL;
    END;
    PERFORM set_config('fasttrun.test_failpoint', '', false);
    PERFORM fasttruncate('ft_truncate_memory');
    INSERT INTO ft_truncate_memory SELECT 2000 + i, value FROM ft_payload;
  END LOOP;
END
$poison_cycles$;
SELECT 'POISON_FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

/* One active block survives a transaction rollback and uses little memory. */
BEGIN;
DO $active_poison$
BEGIN
  PERFORM set_config('fasttrun.test_failpoint', 'after_user_index:1', false);
  BEGIN
    PERFORM fasttruncate('ft_truncate_memory');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    NULL;
  END;
  PERFORM set_config('fasttrun.test_failpoint', '', false);
END
$active_poison$;
ROLLBACK;
SELECT 'ACTIVE_POISON_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';
SELECT fasttruncate('ft_truncate_memory');
SELECT 'REPAIRED_POISON_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun operation context';

DROP TABLE ft_truncate_memory, ft_payload;
DISCARD TEMP;
SELECT 'STABLE_ONE_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_ONE_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_ONE_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT pg_sleep(0.1);
SELECT 'STABLE_TWO_TOTAL ' || coalesce(sum(total_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_TWO_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SELECT 'STABLE_TWO_CONTEXTS ' || count(*)
FROM pg_backend_memory_contexts WHERE name LIKE 'fasttrun%';
SQL

run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/check.sql" >"$WORKDIR/check.out" 2>"$WORKDIR/check.err" || {
	cat "$WORKDIR/check.err" >&2
	exit 1
}

value()
{
	awk -v key="$1" '$1 == key {print $2}' "${2:-$WORKDIR/check.out}" | tail -1
}

# Compare identical committed subtransactions with and without the observer.
# Core retains its own transaction metadata until COMMIT.
cat >"$WORKDIR/fresh_memory.sql" <<'SQL'
\set ON_ERROR_STOP 1
\if :load_fasttrun
LOAD 'fasttrun';
\else
DO $check$ BEGIN
  IF current_setting('fasttrun.sample_rows', true) IS NOT NULL THEN
    RAISE EXCEPTION 'core memory control has fasttrun loaded';
  END IF;
END $check$;
\endif
CREATE TEMP TABLE ft_fresh_memory(id int);
BEGIN;
DO $warm$ BEGIN
  FOR i IN 1..100 LOOP
    BEGIN TRUNCATE ft_fresh_memory; EXCEPTION WHEN OTHERS THEN RAISE; END;
  END LOOP;
END $warm$;
SELECT 'FRESH_WARM_USED ' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'TopTransactionContext';
DO $repeat$ BEGIN
  FOR i IN 1..2000 LOOP
    BEGIN TRUNCATE ft_fresh_memory; EXCEPTION WHEN OTHERS THEN RAISE; END;
  END LOOP;
END $repeat$;
SELECT 'FRESH_FINAL_USED ' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'TopTransactionContext';
COMMIT;

CREATE TEMP TABLE ft_dc_parent(a int);
INSERT INTO ft_dc_parent VALUES (1);
CREATE TEMP TABLE ft_dc_child(a int);
INSERT INTO ft_dc_child VALUES (1);
\if :load_fasttrun
CREATE FUNCTION pg_temp.ft_dc_touch_parent() RETURNS void LANGUAGE sql AS $$
  SELECT fasttrun_analyze('ft_dc_parent');
$$;
CREATE FUNCTION pg_temp.ft_dc_touch_child() RETURNS void LANGUAGE sql AS $$
  SELECT fasttrun_collect_stats('ft_dc_child');
$$;
\else
CREATE FUNCTION pg_temp.ft_dc_touch_parent() RETURNS void LANGUAGE sql AS $$
  SELECT NULL::void;
$$;
CREATE FUNCTION pg_temp.ft_dc_touch_child() RETURNS void LANGUAGE sql AS $$
  SELECT NULL::void;
$$;
\endif
BEGIN;
DO $dropcol_warm$ BEGIN
  FOR i IN 1..30 LOOP
    BEGIN
      PERFORM pg_temp.ft_dc_touch_parent();
      BEGIN
        ALTER TABLE ft_dc_child ADD COLUMN b_tmp int DEFAULT 2;
        PERFORM pg_temp.ft_dc_touch_child();
        ALTER TABLE ft_dc_child DROP COLUMN b_tmp;
      EXCEPTION WHEN division_by_zero THEN NULL;
      END;
      RAISE EXCEPTION USING MESSAGE = 'rollback dropcol outer';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback dropcol outer' THEN RAISE; END IF;
    END;
  END LOOP;
END $dropcol_warm$;
SELECT 'DROPCOL_WARM_USED ' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'TopTransactionContext';
DO $dropcol_repeat$ BEGIN
  FOR i IN 1..1000 LOOP
    BEGIN
      PERFORM pg_temp.ft_dc_touch_parent();
      BEGIN
        ALTER TABLE ft_dc_child ADD COLUMN b_tmp int DEFAULT 2;
        PERFORM pg_temp.ft_dc_touch_child();
        ALTER TABLE ft_dc_child DROP COLUMN b_tmp;
      EXCEPTION WHEN division_by_zero THEN NULL;
      END;
      RAISE EXCEPTION USING MESSAGE = 'rollback dropcol outer';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback dropcol outer' THEN RAISE; END IF;
    END;
  END LOOP;
END $dropcol_repeat$;
SELECT 'DROPCOL_FINAL_USED ' || used_bytes
FROM pg_backend_memory_contexts WHERE name = 'TopTransactionContext';
COMMIT;
DROP FUNCTION pg_temp.ft_dc_touch_parent(), pg_temp.ft_dc_touch_child();
DROP TABLE ft_dc_parent, ft_dc_child;

\if :load_fasttrun
BEGIN;
DO $create_warm$ BEGIN
  FOR i IN 1..50 LOOP
    BEGIN
      CREATE TEMP TABLE ft_fresh_create_rb(id int PRIMARY KEY, v text);
      RAISE EXCEPTION USING MESSAGE = 'rollback create';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback create' THEN RAISE; END IF;
    END;
    CREATE TEMP TABLE ft_fresh_create_drop(id int PRIMARY KEY, v text);
    DROP TABLE ft_fresh_create_drop;
    CREATE TEMP TABLE ft_fresh_sub_drop(id int);
    BEGIN
      DROP TABLE ft_fresh_sub_drop;
    EXCEPTION WHEN OTHERS THEN RAISE;
    END;
  END LOOP;
END $create_warm$;
SELECT 'FRESH_CREATE_WARM ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name = 'fasttrun fresh marks';
DO $create_repeat$ BEGIN
  FOR i IN 1..2000 LOOP
    BEGIN
      CREATE TEMP TABLE ft_fresh_create_rb(id int PRIMARY KEY, v text);
      RAISE EXCEPTION USING MESSAGE = 'rollback create';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM <> 'rollback create' THEN RAISE; END IF;
    END;
    IF i <= 150 THEN
      CREATE TEMP TABLE ft_fresh_create_drop(id int PRIMARY KEY, v text);
      DROP TABLE ft_fresh_create_drop;
      CREATE TEMP TABLE ft_fresh_sub_drop(id int);
      BEGIN
        DROP TABLE ft_fresh_sub_drop;
      EXCEPTION WHEN OTHERS THEN RAISE;
      END;
    END IF;
  END LOOP;
END $create_repeat$;
SELECT 'FRESH_CREATE_FINAL ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts WHERE name = 'fasttrun fresh marks';
COMMIT;

SELECT current_setting('debug_assertions') = 'on' AS has_cassert \gset
\if :has_cassert
CREATE TEMP TABLE ft_subcommit_anchor(a int);
INSERT INTO ft_subcommit_anchor VALUES (1);
BEGIN;
SELECT fasttrun_analyze('ft_subcommit_anchor');
DO $subcommit_fail_warm$
DECLARE
  caught boolean;
  fp text;
BEGIN
  FOR i IN 1..50 LOOP
    caught := false;
    fp := CASE WHEN i % 2 = 0 THEN 'subcommit_merge' ELSE 'after_pre_commit_sub' END;
    BEGIN
      CREATE TEMP TABLE ft_subcommit_child(a int, b int);
      INSERT INTO ft_subcommit_child VALUES (1, 2);
      PERFORM fasttrun_analyze('ft_subcommit_child');
      ALTER TABLE ft_subcommit_child DROP COLUMN b;
      PERFORM set_config('fasttrun.test_failpoint', fp, false);
    EXCEPTION WHEN SQLSTATE '55000' THEN
      IF SQLERRM <> ('fasttrun test failpoint: ' || fp) THEN
        RAISE;
      END IF;
      PERFORM set_config('fasttrun.test_failpoint', '', false);
      caught := true;
    END;
    IF NOT caught THEN
      RAISE EXCEPTION '% failpoint did not fire in warm pass %', fp, i;
    END IF;
  END LOOP;
END $subcommit_fail_warm$;
SELECT 'SUBCOMMIT_FAIL_WARM_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name IN ('fasttrun xact frame', 'fasttrun xact frame relids');
DO $subcommit_fail_repeat$
DECLARE
  caught boolean;
  fp text;
BEGIN
  FOR i IN 1..1000 LOOP
    caught := false;
    fp := CASE WHEN i % 2 = 0 THEN 'subcommit_merge' ELSE 'after_pre_commit_sub' END;
    BEGIN
      CREATE TEMP TABLE ft_subcommit_child(a int, b int);
      INSERT INTO ft_subcommit_child VALUES (1, 2);
      PERFORM fasttrun_analyze('ft_subcommit_child');
      ALTER TABLE ft_subcommit_child DROP COLUMN b;
      PERFORM set_config('fasttrun.test_failpoint', fp, false);
    EXCEPTION WHEN SQLSTATE '55000' THEN
      IF SQLERRM <> ('fasttrun test failpoint: ' || fp) THEN
        RAISE;
      END IF;
      PERFORM set_config('fasttrun.test_failpoint', '', false);
      caught := true;
    END;
    IF NOT caught THEN
      RAISE EXCEPTION '% failpoint did not fire in repeat pass %', fp, i;
    END IF;
  END LOOP;
END $subcommit_fail_repeat$;
SELECT 'SUBCOMMIT_FAIL_FINAL_USED ' || coalesce(sum(used_bytes), 0)
FROM pg_backend_memory_contexts
WHERE name IN ('fasttrun xact frame', 'fasttrun xact frame relids');
DO $subcommit_anchor_check$
BEGIN
  IF (SELECT reltuples FROM fasttrun_relstats('ft_subcommit_anchor')) <> 1 THEN
    RAISE EXCEPTION 'failed child subcommits corrupted surviving parent anchor state';
  END IF;
END $subcommit_anchor_check$;
COMMIT;
DROP TABLE ft_subcommit_anchor;
\else
SELECT 'SUBCOMMIT_FAIL_WARM_USED 0';
SELECT 'SUBCOMMIT_FAIL_FINAL_USED 0';
\endif
\endif
SQL
for load_fasttrun in 0 1; do
	PGOPTIONS='-c session_preload_libraries= -c local_preload_libraries=' \
		run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v load_fasttrun="$load_fasttrun" -f "$WORKDIR/fresh_memory.sql" \
		>"$WORKDIR/fresh_$load_fasttrun.out" 2>"$WORKDIR/fresh_$load_fasttrun.err" || {
		cat "$WORKDIR/fresh_$load_fasttrun.err" >&2
		exit 1
	}
done
fresh_core_warm=$(value FRESH_WARM_USED "$WORKDIR/fresh_0.out")
fresh_core_final=$(value FRESH_FINAL_USED "$WORKDIR/fresh_0.out")
fresh_local_warm=$(value FRESH_WARM_USED "$WORKDIR/fresh_1.out")
fresh_local_final=$(value FRESH_FINAL_USED "$WORKDIR/fresh_1.out")
fresh_create_warm=$(value FRESH_CREATE_WARM "$WORKDIR/fresh_1.out")
fresh_create_final=$(value FRESH_CREATE_FINAL "$WORKDIR/fresh_1.out")
dropcol_core_warm=$(value DROPCOL_WARM_USED "$WORKDIR/fresh_0.out")
dropcol_core_final=$(value DROPCOL_FINAL_USED "$WORKDIR/fresh_0.out")
dropcol_local_warm=$(value DROPCOL_WARM_USED "$WORKDIR/fresh_1.out")
dropcol_local_final=$(value DROPCOL_FINAL_USED "$WORKDIR/fresh_1.out")
subcommit_fail_warm=$(value SUBCOMMIT_FAIL_WARM_USED "$WORKDIR/fresh_1.out")
subcommit_fail_final=$(value SUBCOMMIT_FAIL_FINAL_USED "$WORKDIR/fresh_1.out")
for metric in "$fresh_core_warm" "$fresh_core_final" "$fresh_local_warm" \
	"$fresh_local_final" "$fresh_create_warm" "$fresh_create_final" \
	"$dropcol_core_warm" "$dropcol_core_final" \
	"$dropcol_local_warm" "$dropcol_local_final" \
	"$subcommit_fail_warm" "$subcommit_fail_final"; do
	case "$metric" in
		''|*[!0-9]*) echo "invalid fresh-memory metric: $metric" >&2; exit 1 ;;
	esac
done
fresh_extra=$((fresh_local_final - fresh_local_warm - fresh_core_final + fresh_core_warm))
fresh_create_growth=$((fresh_create_final - fresh_create_warm))
dropcol_growth=$((dropcol_local_final - dropcol_local_warm - dropcol_core_final + dropcol_core_warm))
subcommit_fail_growth=$((subcommit_fail_final - subcommit_fail_warm))
echo "fresh markers: core_growth=$((fresh_core_final-fresh_core_warm)) local_growth=$((fresh_local_final-fresh_local_warm)) extra=$fresh_extra create_churn_growth=$fresh_create_growth dropcol_growth=$dropcol_growth subcommit_fail_growth=$subcommit_fail_growth"
if [ "$fresh_extra" -gt 16384 ]; then
	echo 'FAIL: repeated core TRUNCATE retains fresh marks by call count' >&2
	exit 1
fi
if [ "$fresh_create_growth" -gt 0 ]; then
	echo "FAIL: rolled-back or dropped CREATE TEMP TABLE retains fresh marks ($fresh_create_growth bytes)" >&2
	exit 1
fi
if [ "$dropcol_growth" -gt 16384 ]; then
	echo "FAIL: child DROP COLUMN merge/abort leaks dropped_attrs in TopTransactionContext ($dropcol_growth bytes)" >&2
	exit 1
fi
if [ "$subcommit_fail_growth" -gt 0 ]; then
	echo "FAIL: failed subcommit preparation leaks parent journal entries ($subcommit_fail_growth bytes)" >&2
	exit 1
fi

warm_used=$(value WARM_USED)
warm_total=$(value WARM_TOTAL)
warm_contexts=$(value WARM_CONTEXTS)
visits=$(value VISITS)
visit_frames=$(value VISIT_FRAMES)
visit_pool=$(value VISIT_POOL)
nest_rows=$(value NESTED_ROWS)
nest_original=$(value NESTED_ORIGINAL)
nest_frames=$(value NESTED_FRAMES)
final_used=$(value FINAL_USED)
final_total=$(value FINAL_TOTAL)
final_contexts=$(value FINAL_CONTEXTS)
final_frames=$(value FINAL_FRAMES)
final_pool=$(value FINAL_POOL)
truncate_warm_used=$(value TRUNCATE_WARM_USED)
truncate_final_used=$(value TRUNCATE_FINAL_USED)
poison_final_used=$(value POISON_FINAL_USED)
active_poison_used=$(value ACTIVE_POISON_USED)
repaired_poison_used=$(value REPAIRED_POISON_USED)
stable_one_total=$(value STABLE_ONE_TOTAL)
stable_one_used=$(value STABLE_ONE_USED)
stable_one_contexts=$(value STABLE_ONE_CONTEXTS)
stable_two_total=$(value STABLE_TWO_TOTAL)
stable_two_used=$(value STABLE_TWO_USED)
stable_two_contexts=$(value STABLE_TWO_CONTEXTS)

for item in "$warm_used" "$warm_total" "$warm_contexts" "$visits" \
	"$visit_frames" "$nest_rows" "$nest_original" "$nest_frames" \
	"$final_used" "$final_total" "$final_contexts" "$final_frames" \
	"$truncate_warm_used" "$truncate_final_used" "$poison_final_used" \
	"$active_poison_used" "$repaired_poison_used" \
	"$stable_one_total" "$stable_one_used" "$stable_one_contexts" \
	"$stable_two_total" "$stable_two_used" "$stable_two_contexts"; do
	case "$item" in
		''|*[!0-9]*) echo "неверная метрика журнала: $item" >&2; exit 1 ;;
	esac
done

growth=$((final_used - warm_used))
[ "$growth" -lt 0 ] && growth=0
truncate_growth=$((truncate_final_used - truncate_warm_used))
[ "$truncate_growth" -lt 0 ] && truncate_growth=0

echo "журнал транзакций: обходы=$visits прогрев=$warm_used итог=$final_used рост=$growth уровни_при_обходе=$visit_frames уровни_в_конце=$final_frames"
echo "память очистки: прогрев=$truncate_warm_used итог=$truncate_final_used рост=$truncate_growth после_восстановления=$repaired_poison_used активная_блокировка=$active_poison_used"
echo "стабильность памяти fasttrun: прогрев=$warm_total/$warm_used/$warm_contexts итог=$final_total/$final_used/$final_contexts повтор1=$stable_one_total/$stable_one_used/$stable_one_contexts повтор2=$stable_two_total/$stable_two_used/$stable_two_contexts"

if [ "$visits" -gt 400 ]; then
	echo "FAIL: обработчик подтранзакций обошёл $visits таблиц, ожидалось не более 400" >&2
	exit 1
fi
if [ "$visit_frames" -ne 0 ] || [ "$final_frames" -ne 0 ]; then
	echo "FAIL: остались уровни транзакции: при обходе=$visit_frames, в конце=$final_frames" >&2
	exit 1
fi
# Отработавший уровень отдаёт кадр в пул для повторного использования. Кадр в
# пуле помечен, поэтому от живого он отличим, и здесь проверяется только то,
# что пул не растёт по числу уровней.
FRAME_POOL_MAX=${FRAME_POOL_MAX:-4}
if [ "${visit_pool:-0}" -gt "$FRAME_POOL_MAX" ] || \
	[ "${final_pool:-0}" -gt "$FRAME_POOL_MAX" ]; then
	echo "FAIL: пул кадров вырос: при обходе=${visit_pool:-0}, в конце=${final_pool:-0}, предел=$FRAME_POOL_MAX" >&2
	exit 1
fi
if [ "$nest_rows" -ne 1 ] || [ "$nest_original" -ne 1 ] || \
	[ "$nest_frames" -ne 0 ]; then
	echo "FAIL: неверный откат вложенных уровней одной таблицы: строк=$nest_rows исходных=$nest_original уровней=$nest_frames" >&2
	exit 1
fi
if [ "$growth" -gt 65536 ]; then
	echo "FAIL: контексты fasttrun выросли на $growth байт, предел 65536" >&2
	exit 1
fi
if [ "$truncate_growth" -gt 65536 ]; then
	echo "FAIL: контекст операции очистки вырос на $truncate_growth байт" >&2
	exit 1
fi
if [ "$poison_final_used" -ne 0 ] || [ "$repaired_poison_used" -ne 0 ]; then
	echo "FAIL: после успешного восстановления остался контекст операции" >&2
	exit 1
fi
if [ "$active_poison_used" -le 0 ] || [ "$active_poison_used" -gt 65536 ]; then
	echo "FAIL: одна активная блокировка заняла $active_poison_used байт" >&2
	exit 1
fi
if [ "$stable_one_used" -gt $((warm_used + 65536)) ] || \
	[ "$stable_two_used" -gt $((warm_used + 65536)) ] || \
	[ "$stable_one_contexts" -ne "$warm_contexts" ] || \
	[ "$stable_two_contexts" -ne "$warm_contexts" ]; then
	echo "FAIL: общая память fasttrun не вернулась к прогретому уровню" >&2
	exit 1
fi
if grep -Eq 'TRAP|Assertion|FailedAssertion|PANIC|server process .* was terminated|leaked hash_seq_search' "$LOG"; then
	grep -E 'TRAP|Assertion|FailedAssertion|PANIC|server process .* was terminated|leaked hash_seq_search' "$LOG" >&2
	echo "FAIL: проверка памяти вызвала Assert или завершение серверного процесса" >&2
	exit 1
fi

echo "проверка памяти журнала транзакций прошла: обходы=$visits рост=$growth рост_очистки=$truncate_growth живых_уровней=0 кадров_в_пуле=${final_pool:-0}"
