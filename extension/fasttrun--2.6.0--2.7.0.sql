\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.7.0'" to load this file. \quit

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
