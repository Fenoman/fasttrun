-- ======================================================================
-- Наблюдаемость: счетчики событий, состав кешей сеанса, блокировки и
-- цель статистики из шаблона.
-- ======================================================================
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun;

-- ----------------------------------------------------------------------
-- 1. Счетчики считают вызовы по путям: холодный проход, вызов без изменений,
--    дельта, очистка и ее быстрый путь для уже пустой таблицы.  Без
--    shared_preload_libraries они относятся к текущему backend.
-- ----------------------------------------------------------------------
SELECT fasttrun_counters_reset();
CREATE TEMP TABLE obs_t (id int, grp int);
INSERT INTO obs_t SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('obs_t');
SELECT fasttrun_analyze('obs_t');
INSERT INTO obs_t SELECT g, g % 10 FROM generate_series(1001, 1010) g;
SELECT fasttrun_analyze('obs_t');
SELECT fasttruncate('obs_t');
SELECT fasttruncate('obs_t');
SELECT counter, value FROM fasttrun_counters()
 WHERE counter IN ('analyze_calls', 'analyze_cold_scan', 'analyze_noop',
                   'analyze_delta', 'truncate_calls',
                   'truncate_empty_fast_path', 'column_collects')
 ORDER BY counter COLLATE "C";
SELECT count(*) = count(DISTINCT counter) AS counters_unique,
       bool_and(scope IN ('backend', 'cluster')) AS scope_known,
       bool_and(value >= 0) AS values_non_negative
  FROM fasttrun_counters();

-- План получает кешированную статистику, а после изменения половины строк
-- без analyze - оценки по умолчанию.
INSERT INTO obs_t SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('obs_t');
SELECT fasttrun_counters_reset();
EXPLAIN (COSTS OFF) SELECT * FROM obs_t WHERE grp = 1;
SELECT value > 0 AS stats_served FROM fasttrun_counters() WHERE counter = 'stats_served';
UPDATE obs_t SET grp = grp + 1 WHERE id % 2 = 0;
EXPLAIN (COSTS OFF) SELECT * FROM obs_t WHERE grp = 1;
SELECT value > 0 AS stats_hidden FROM fasttrun_counters() WHERE counter = 'stats_hidden';
SELECT fasttrun_counters_reset();
SELECT bool_and(value = 0) AS counters_reset FROM fasttrun_counters();

-- Пересбор, отложенный записью, выполняет следующий вызов уже без нового DML.
-- Это работа, а не вызов без изменений.
CREATE TEMP TABLE obs_d (id int, grp int);
INSERT INTO obs_d SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('obs_d');
WITH w AS (UPDATE obs_d SET grp = grp RETURNING id)
SELECT count(*) AS obs_d_updated, fasttrun_analyze('obs_d') IS NOT NULL AS analyzed FROM w;
SELECT fasttrun_analyze('obs_d');
SELECT counter, value FROM fasttrun_counters()
 WHERE counter IN ('analyze_noop', 'analyze_delta', 'refresh_deferred',
                   'column_collects')
 ORDER BY counter COLLATE "C";
DROP TABLE obs_d;

-- storage_found_empty считает таблицы, найденные опустошенными мимо счетчиков,
-- а не уже известную пустоту: индексы после fasttruncate его не двигают.
SELECT fasttrun_counters_reset();
CREATE TEMP TABLE obs_e (id int);
CREATE INDEX obs_e_id ON obs_e (id);
INSERT INTO obs_e SELECT generate_series(1, 100);
SELECT fasttrun_analyze('obs_e');
SELECT fasttruncate('obs_e');
SELECT count(*) AS obs_e_rows FROM obs_e WHERE id = 1;
SELECT value AS empty_after_truncate FROM fasttrun_counters()
 WHERE counter = 'storage_found_empty';
CREATE TEMP TABLE obs_odr (id int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO obs_odr SELECT generate_series(1, 10);
SELECT fasttrun_analyze('obs_odr');
COMMIT;
SELECT fasttrun_analyze('obs_odr');
SELECT value AS empty_after_on_commit_delete FROM fasttrun_counters()
 WHERE counter = 'storage_found_empty';
DROP TABLE obs_e;
DROP TABLE obs_odr;

-- ----------------------------------------------------------------------
-- 2. fasttrun_cached_tables() показывает опубликованные оценки таблицы и
--    индекса, дельту и колонки, которые проверка свежести сейчас скрывает.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE obs_c (id int, grp int);
CREATE INDEX obs_c_id ON obs_c (id);
INSERT INTO obs_c SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('obs_c');
SELECT relname, kind, relpages > 0 AS has_pages, reltuples, count_exact,
       delta_state, column_stats, hidden_columns
  FROM fasttrun_cached_tables()
 WHERE relname IN ('obs_c', 'obs_c_id')
 ORDER BY relname COLLATE "C";
BEGIN;
UPDATE obs_c SET grp = grp + 1;
SELECT relname, column_stats, hidden_columns
  FROM fasttrun_cached_tables()
 WHERE relname = 'obs_c';
COMMIT;
-- COMMIT скрывает устаревшие образцы совсем и сбрасывает дельту: следующий
-- analyze проходит таблицу заново.
SELECT relname, column_stats, delta_state
  FROM fasttrun_cached_tables()
 WHERE relname = 'obs_c';
SELECT 'obs_c'::regclass::oid AS obs_c_oid \gset
DROP TABLE obs_c;
SELECT count(*) AS dropped_table_rows
  FROM fasttrun_cached_tables() WHERE relid = :obs_c_oid;

-- Образец, снятый с прежнего файла отношения, планировщик не берет:
-- ALTER TYPE через родителя наследования переписывает потомка.
CREATE TEMP TABLE obs_parent (id int);
CREATE TEMP TABLE obs_child () INHERITS (obs_parent);
INSERT INTO obs_child VALUES (1);
SELECT fasttrun_analyze('obs_child');
ALTER TABLE obs_parent ALTER COLUMN id TYPE bigint;
SELECT relname, column_stats, hidden_columns
  FROM fasttrun_cached_tables()
 WHERE relname = 'obs_child';
DROP TABLE obs_parent CASCADE;

-- ----------------------------------------------------------------------
-- 3. Без незавершенной очистки заблокированных таблиц нет.  Сам сценарий
--    блокировки проверяет check_fasttrun_fault_matrix.sh на cassert-сборке.
-- ----------------------------------------------------------------------
SELECT count(*) AS blocked_tables FROM fasttrun_blocked_tables();

-- ----------------------------------------------------------------------
-- 4. Колонка, оставленная у временной таблицы со значением по умолчанию,
--    берет цель статистики у одноименной колонки шаблона:
--    LIKE не копирует SET STATISTICS.  Цель 0 у шаблона выключает сбор.
-- ----------------------------------------------------------------------
CREATE SCHEMA obs_tmpl;
CREATE TABLE obs_tmpl.obs_like (id int, note text);
ALTER TABLE obs_tmpl.obs_like ALTER COLUMN note SET STATISTICS 0;
SET fasttrun.prewarm_schema = 'obs_tmpl';
CREATE TEMP TABLE obs_like (LIKE obs_tmpl.obs_like);
INSERT INTO obs_like SELECT g, 'n' || g FROM generate_series(1, 500) g;
SELECT fasttrun_analyze('obs_like');
SELECT array_agg(staattnum ORDER BY staattnum) AS template_on_columns
  FROM fasttrun_inspect_stats('obs_like');
SET fasttrun.template_stattarget = off;
SELECT fasttrun_collect_stats('obs_like');
SELECT array_agg(staattnum ORDER BY staattnum) AS template_off_columns
  FROM fasttrun_inspect_stats('obs_like');
RESET fasttrun.template_stattarget;
DROP TABLE obs_like;

-- Цель шаблона действует только на сбор fasttrun.  Ядерный ANALYZE читает
-- цель самой временной таблицы, и fasttrun отдает планировщику то, что ядро
-- собрало.
CREATE FUNCTION pg_temp.obs_rows(q text) RETURNS int LANGUAGE plpgsql AS $f$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::int;
END
$f$;
CREATE TABLE obs_tmpl.obs_core (id int, note text);
ALTER TABLE obs_tmpl.obs_core ALTER COLUMN note SET STATISTICS 0;
CREATE TEMP TABLE obs_core (LIKE obs_tmpl.obs_core);
INSERT INTO obs_core
SELECT g, CASE WHEN g <= 900 THEN 'a' ELSE 'b' END FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('obs_core');
SELECT pg_temp.obs_rows('SELECT * FROM obs_core WHERE note = ''a''') < 100
       AS fasttrun_left_defaults;
ANALYZE obs_core (note);
SELECT pg_temp.obs_rows('SELECT * FROM obs_core WHERE note = ''a''') > 500
       AS core_stats_served;
DROP TABLE obs_core;
RESET fasttrun.prewarm_schema;
DROP SCHEMA obs_tmpl CASCADE;

DROP TABLE obs_t;
DROP EXTENSION fasttrun;
