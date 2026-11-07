--
-- fasttrun_outer_rollback - кеши fasttrun после отката подтранзакции, когда
-- таблица создана или сменила файл на внешнем уровне той же транзакции.
--
-- Tantor SE 18.6.3 при enable_temp_memory_catalog = on такой откат ломает
-- временную таблицу в ядре: созданная раньше таблица пропадает, а после смены
-- файла таблица остается на удаленном файле. Это ошибки ядра, они
-- воспроизводятся без fasttrun. Поэтому там набор пропускается, ожидаемый
-- вывод в этом режиме - fasttrun_outer_rollback_1.out.
--
SELECT coalesce(current_setting('enable_temp_memory_catalog', true), 'off') = 'on'
       AS memory_catalog \gset
\if :memory_catalog
\echo 'skipped: enable_temp_memory_catalog = on'
\quit
\endif

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun;
RESET client_min_messages;

-- ----------------------------------------------------------------------
-- 1. Таблица создана на внешнем уровне, первое обращение fasttrun - в
--    дочерней подтранзакции, которая откатывается.
-- ----------------------------------------------------------------------
CREATE FUNCTION pg_temp.assert_birth_cache_empty() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE s record;
BEGIN
  SELECT * INTO s FROM fasttrun_cache_stats();
  IF s.analyze_entries <> 0 OR s.column_stats_entries <> 0 OR
     s.column_stats_relid_entries <> 0 THEN
    RAISE EXCEPTION 'aborted CREATE left cache entries: %', row_to_json(s);
  END IF;
END
$$;

-- Первое обращение может быть в дочерней подтранзакции, которая откатывается независимо от CREATE.
BEGIN;
SAVEPOINT birth_parent;
CREATE TEMP TABLE t_birth_ancestor(id int PRIMARY KEY);
INSERT INTO t_birth_ancestor SELECT generate_series(1, 100);
SAVEPOINT birth_child;
SELECT fasttruncate('t_birth_ancestor');
ROLLBACK TO SAVEPOINT birth_child;
SELECT count(*) = 0 AS birth_ancestor_still_empty FROM t_birth_ancestor;
SELECT reltuples = 0 AS birth_ancestor_stats_empty
  FROM fasttrun_relstats('t_birth_ancestor');
ROLLBACK TO SAVEPOINT birth_parent;
SELECT pg_temp.assert_birth_cache_empty();
COMMIT;

-- ----------------------------------------------------------------------
-- 2. Внешний уровень переписал таблицу, вложенный откатился.
-- ----------------------------------------------------------------------
-- Запись без relstats следует за текущим файлом, и откат вложенного уровня не
-- возвращает ей файл, который заменил внешний: VACUUM обнуляет ее счет, и она
-- уходит.
SELECT analyze_entries AS follow_entries_before FROM fasttrun_cache_stats() \gset
CREATE TEMP TABLE t_follow (id int, grp int, pad text);
INSERT INTO t_follow SELECT g, g % 4, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('t_follow');
DO $$ BEGIN BEGIN
  INSERT INTO t_follow SELECT g, 9, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
BEGIN;
ALTER TABLE t_follow ALTER COLUMN id TYPE bigint;
SAVEPOINT s;
SELECT fasttrun_collect_stats('t_follow');
ROLLBACK TO SAVEPOINT s;
COMMIT;
VACUUM t_follow;
SELECT analyze_entries = :follow_entries_before AS follow_entry_reclaimed
  FROM fasttrun_cache_stats();
DROP TABLE t_follow;

-- ----------------------------------------------------------------------
-- 3. Перезапись внутри завершенной вложенной подтранзакции и откат
--    внешней точки сохранения сохраняют статистику колонок.
-- ----------------------------------------------------------------------
CREATE FUNCTION pg_temp.st_rows(q text) RETURNS int LANGUAGE plpgsql AS $f$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::int;
END
$f$;
CREATE FUNCTION pg_temp.st_share(tbl text, cond text) RETURNS numeric LANGUAGE sql AS $f$
  SELECT pg_temp.st_rows(format('SELECT * FROM %s WHERE %s', tbl, cond))::numeric /
         pg_temp.st_rows(format('SELECT * FROM %s', tbl))
$f$;
CREATE TEMP TABLE t_ddl_rewrite (id int, a int, b int);
CREATE INDEX ON t_ddl_rewrite ((b % 2));
INSERT INTO t_ddl_rewrite SELECT g, g % 7, g % 20 FROM generate_series(1, 7000) g;
SELECT fasttrun_analyze('t_ddl_rewrite');
SELECT pg_relation_filenode('t_ddl_rewrite') AS ddl_rewrite_node \gset
ALTER TABLE t_ddl_rewrite ADD COLUMN c int GENERATED ALWAYS AS (a + b) STORED;
SELECT pg_relation_filenode('t_ddl_rewrite') <> :ddl_rewrite_node AS ddl_rewrite_new_file,
       pg_temp.st_share('t_ddl_rewrite', 'a = 3') BETWEEN 0.07 AND 0.29
       AS ddl_rewrite_keeps_column_stats,
       pg_temp.st_share('t_ddl_rewrite', 'b % 2 = 0') BETWEEN 0.25 AND 1
       AS ddl_rewrite_keeps_expression_stats;
BEGIN;
SAVEPOINT ddl_rewrite_sp;
SAVEPOINT ddl_rewrite_inner;
ALTER TABLE t_ddl_rewrite ADD COLUMN d uuid DEFAULT gen_random_uuid();
RELEASE SAVEPOINT ddl_rewrite_inner;
SELECT pg_temp.st_share('t_ddl_rewrite', 'b = 5') BETWEEN 0.025 AND 0.1
       AS ddl_rewrite_volatile_default_keeps;
ROLLBACK TO ddl_rewrite_sp;
SELECT pg_temp.st_share('t_ddl_rewrite', 'b = 5') BETWEEN 0.025 AND 0.1
       AS ddl_rewrite_rollback_keeps;
COMMIT;
SELECT column_stats = 4 AND hidden_columns = 0 AS ddl_rewrite_survives_commit
  FROM fasttrun_cached_tables() WHERE relname = 't_ddl_rewrite';
DROP TABLE t_ddl_rewrite;

DROP EXTENSION fasttrun;
