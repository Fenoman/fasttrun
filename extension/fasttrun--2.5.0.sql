\echo Use "CREATE EXTENSION fasttrun" to load this file. \quit

-- ----------------------------------------------------------------------
-- fasttruncate(text)
-- Очищает локальную временную heap-таблицу без записи в каталог
-- и без последующего ANALYZE. Подробности в fasttrun.c.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttruncate(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;

-- ANALYZE в памяти без записи в pg_class и общей рассылки сброса кешей.
CREATE OR REPLACE FUNCTION fasttrun_analyze(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze'
LANGUAGE C STRICT VOLATILE;

-- Пакетный вариант для нескольких временных таблиц.
CREATE OR REPLACE FUNCTION fasttrun_analyze_bulk(VARIADIC text[])
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze_bulk'
LANGUAGE C VOLATILE;

-- Текущие relpages/reltuples в памяти, видимые планировщику.
CREATE OR REPLACE FUNCTION fasttrun_relstats(IN  rel_name text,
                                             OUT relpages int,
                                             OUT reltuples real)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_relstats'
LANGUAGE C STRICT VOLATILE;

-- Явный сбор статистики столбцов для текущего сеанса.
CREATE OR REPLACE FUNCTION fasttrun_collect_stats(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_collect_stats'
LANGUAGE C STRICT VOLATILE;

-- Просмотр statsTuple текущего сеанса в формате pg_statistic.
CREATE OR REPLACE FUNCTION fasttrun_inspect_stats(rel_name text)
RETURNS SETOF pg_catalog.pg_statistic
AS 'MODULE_PATHNAME', 'fasttrun_inspect_stats'
LANGUAGE C STRICT VOLATILE;

-- Количество записей и память, занятая двумя кешами текущего сеанса.
CREATE OR REPLACE FUNCTION fasttrun_cache_stats(
    OUT analyze_entries bigint,
    OUT column_stats_relid_entries bigint,
    OUT column_stats_entries bigint,
    OUT analyze_bytes bigint,
    OUT column_stats_bytes bigint,
    OUT total_bytes bigint)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_cache_stats'
LANGUAGE C VOLATILE;

-- Учёт N самых часто создаваемых временных таблиц.
CREATE OR REPLACE FUNCTION fasttrun_hot_temp_tables(
    n int DEFAULT 100,
    OUT relname text,
    OUT create_count bigint,
    OUT last_create timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_hot_temp_tables'
LANGUAGE C STRICT VOLATILE;

-- Предварительное создание N таблиц через create_temp_table.
CREATE OR REPLACE FUNCTION fasttrun_prewarm()
RETURNS int
AS 'MODULE_PATHNAME', 'fasttrun_prewarm'
LANGUAGE C VOLATILE;

-- Сброс статистики создания временных таблиц.
CREATE OR REPLACE FUNCTION fasttrun_reset_temp_stats()
RETURNS void
AS 'MODULE_PATHNAME', 'fasttrun_reset_temp_stats'
LANGUAGE C VOLATILE;
