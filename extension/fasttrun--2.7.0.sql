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

-- Учет N самых часто создаваемых временных таблиц.
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

-- Счетчики событий расширения: по кластеру при shared_preload_libraries,
-- иначе по текущему backend (столбец scope).
CREATE OR REPLACE FUNCTION fasttrun_counters(
    OUT counter text,
    OUT value bigint,
    OUT scope text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_counters'
LANGUAGE C VOLATILE;

-- Обнуление счетчиков fasttrun_counters().
CREATE OR REPLACE FUNCTION fasttrun_counters_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'fasttrun_counters_reset'
LANGUAGE C VOLATILE;

-- Отношения, для которых текущий сеанс хранит оценки и статистику.
CREATE OR REPLACE FUNCTION fasttrun_cached_tables(
    OUT relid oid,
    OUT relname text,
    OUT kind text,
    OUT relpages bigint,
    OUT reltuples double precision,
    OUT count_exact boolean,
    OUT delta_state boolean,
    OUT column_stats integer,
    OUT hidden_columns integer)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_cached_tables'
LANGUAGE C VOLATILE;

-- Таблицы, заблокированные в текущем сеансе после незавершенной очистки.
CREATE OR REPLACE FUNCTION fasttrun_blocked_tables(
    OUT relid oid,
    OUT relname text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_blocked_tables'
LANGUAGE C VOLATILE;

-- TRUE, если все строки временной таблицы сеанса появились в текущей
-- транзакции: таблица создана в ней или очищена TRUNCATE или fasttruncate.
CREATE OR REPLACE FUNCTION fasttrun_temp_is_ours(rel regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'fasttrun_temp_is_ours'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

-- TRUE, если живые колонки временной таблицы и шаблона совпадают по порядку:
-- имя, тип, typmod и правило сортировки.
CREATE OR REPLACE FUNCTION fasttrun_temp_same_cols(temp regclass, template regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'fasttrun_temp_same_cols'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
