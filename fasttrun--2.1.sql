\echo Use "CREATE EXTENSION fasttrun" to load this file. \quit

-- ----------------------------------------------------------------------
-- fasttruncate(text)
-- Очищает временную таблицу через heap_truncate без записи в каталог
-- и без последующего ANALYZE.  Подробности в fasttrun.c.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttruncate(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;

-- ----------------------------------------------------------------------
-- fasttrun_analyze(text)
-- In-memory ANALYZE: считает строки временной таблицы и публикует
-- результат в rel->rd_rel.relpages / reltuples без записи в pg_class
-- и без генерации sinval-сообщений.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_analyze(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze'
LANGUAGE C STRICT VOLATILE;

-- ----------------------------------------------------------------------
-- fasttrun_relstats(text)
-- Инспекционный helper: возвращает текущие relpages / reltuples из
-- in-memory rd_rel — то, что видит планировщик при оценке стоимости.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_relstats(IN  rel_name text,
                                             OUT relpages int,
                                             OUT reltuples real)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_relstats'
LANGUAGE C STRICT VOLATILE;

-- ----------------------------------------------------------------------
-- fasttrun_collect_stats(text)
-- Sample-based сбор column-level статистики (n_distinct, null_frac,
-- width) с публикацией в session-local cache, который видит планировщик
-- через get_relation_stats_hook.  Никакой записи в pg_statistic, никаких
-- sinval-сообщений.  Тот же путь автоматически берёт fasttrun_analyze
-- при cold scan'e (контролируется fasttrun.auto_collect_stats).
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_collect_stats(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_collect_stats'
LANGUAGE C STRICT VOLATILE;

-- ----------------------------------------------------------------------
-- fasttrun_inspect_stats(text)
-- Production debugging helper: возвращает наши кешированные statsTuple
-- в формате pg_catalog.pg_statistic для всех колонок указанной
-- временной таблицы.  Идентичный shape с обычным pg_statistic, поэтому
-- можно делать SELECT staattnum, stadistinct, stakind1, stanumbers1,
-- stavalues1 FROM fasttrun_inspect_stats('temp_xxx') и видеть ровно то,
-- что отдаст планировщику get_relation_stats_hook на следующем запросе.
-- Не hot path, не fast API — это observability tool на случай когда
-- нужно глянуть «а что у нас вообще в кэше прямо сейчас».
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_inspect_stats(rel_name text)
RETURNS SETOF pg_catalog.pg_statistic
AS 'MODULE_PATHNAME', 'fasttrun_inspect_stats'
LANGUAGE C STRICT VOLATILE;

-- Tracking: top-N самых создаваемых temp tables (shared memory)
CREATE OR REPLACE FUNCTION fasttrun_hot_temp_tables(
    n int DEFAULT 100,
    OUT relname text,
    OUT create_count bigint,
    OUT last_create timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_hot_temp_tables'
LANGUAGE C STRICT VOLATILE;

-- Прогрев: создаёт top-N temp tables через create_temp_table
CREATE OR REPLACE FUNCTION fasttrun_prewarm()
RETURNS int
AS 'MODULE_PATHNAME', 'fasttrun_prewarm'
LANGUAGE C VOLATILE;

-- Сброс статистики создания temp tables
CREATE OR REPLACE FUNCTION fasttrun_reset_temp_stats()
RETURNS void
AS 'MODULE_PATHNAME', 'fasttrun_reset_temp_stats'
LANGUAGE C VOLATILE;
