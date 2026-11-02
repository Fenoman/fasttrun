\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.6.0'" to load this file. \quit

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
