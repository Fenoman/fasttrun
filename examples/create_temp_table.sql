--
-- Пример функции create_temp_table для использования с fasttrun.
--
-- Creates and reuses temporary tables from templates. Handles:
--   * создание temp table из таблицы-шаблона ("чучела")
--   * быструю очистку через fasttruncate (ноль sinval)
--   * "входящие" таблицы (данные заполнены выше по стеку вызовов)
--   * пересоздание при изменении структуры шаблона (деплой)
--   * смена владельца через SET LOCAL ROLE (без ALTER TABLE OWNER = без sinval,
--     требует вызова из SECURITY INVOKER контекста, а при вызове из цепочки
--     SECURITY DEFINER функций используйте вариант без SET LOCAL ROLE, где
--     таблица создается от эффективного владельца текущей функции)
--
-- Зависимости:
--   * fasttrun >= 2.7.0 (fasttruncate / fasttrun_temp_is_ours / fasttrun_temp_same_cols)
--   * расширение pg_variables (pgv_set / pgv_exists) - сессионные переменные
--     для отслеживания "наших" данных при работе через пулер
--     https://github.com/postgrespro/pg_variables
--
-- Параметры:
--   _c_temp_table   - имя временной таблицы
--   _b_in           - "входящая" таблица: данные уже заполнены выше по стеку
--                      вызовов (в другой функции той же транзакции), не удалять
--   _c_dummy_schema - схема с таблицами-шаблонами (по умолчанию dummy_tmp)
--   _c_dummy_table  - имя шаблона, если отличается от _c_temp_table (в обоих
--                      режимах копируются DEFAULTS, CONSTRAINTS, INDEXES,
--                      GENERATED и IDENTITY шаблона)
--
-- Возвращает:
--   TRUE  - таблица создана с нуля
--   FALSE - таблица существовала и очищена (для _b_in: данные заполнены выше, НЕ очищены)
--   NULL  - только для _b_in: данные НЕ "наши", очищены (или пересозданы пустыми из шаблона)
--

CREATE OR REPLACE FUNCTION public.create_temp_table(
    _c_temp_table   text,
    _b_in           boolean DEFAULT false,
    _c_dummy_schema text DEFAULT 'dummy_tmp',
    _c_dummy_table  text DEFAULT NULL
) RETURNS boolean
LANGUAGE plpgsql
AS $_$
DECLARE
    _b_exists_table       bool;
    _temp_table_hash      text;
    _oid_temp_table       regclass;
    _oid_dummy_table      regclass;
    _c_actual_dummy_table text;
    _saved_role           text;
    _recreated_incoming   bool := false;
BEGIN
    -- Приводим к нижнему регистру
    _c_temp_table = pg_catalog.lower(_c_temp_table);
    _temp_table_hash = pg_catalog.md5(_c_temp_table)::text;

    -- Имя шаблона: если не задано явно, совпадает с именем temp table
    _c_actual_dummy_table = CASE
                                WHEN _c_dummy_table IS NULL OR _c_dummy_table = ''
                                    THEN _c_temp_table
                                ELSE _c_dummy_table
                            END;

    _oid_temp_table := pg_catalog.to_regclass(
        pg_catalog.format('pg_temp.%I', _c_temp_table));
    _b_exists_table := _oid_temp_table IS NOT NULL;
    _oid_dummy_table := pg_catalog.to_regclass(
        pg_catalog.format('%I.%I', _c_dummy_schema, _c_actual_dummy_table));


    /*
     * Incoming rows belong to this transaction only while their current
     * storage does. A nontransactional pgv mark can outlive storage rollback.
     */
    IF _b_in
       AND _b_exists_table
    THEN
        IF public.fasttrun_temp_is_ours(_oid_temp_table) IS TRUE
        THEN
            -- Данные наши - ничего не трогаем
            RETURN FALSE;
        END IF;

        IF _oid_dummy_table IS NOT NULL
           AND public.fasttrun_temp_same_cols(_oid_temp_table, _oid_dummy_table) IS NOT TRUE
        THEN
            _saved_role := pg_catalog.current_setting('role');
            IF pg_catalog.to_regrole('_client') IS NOT NULL
               AND (SELECT relowner = pg_catalog.to_regrole('_client')
                      FROM pg_catalog.pg_class WHERE oid = _oid_temp_table)
            THEN
                SET LOCAL ROLE _client;
            END IF;
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', _c_temp_table);
            PERFORM pg_catalog.set_config('role', _saved_role, true);
            _b_exists_table := FALSE;
            _recreated_incoming := TRUE;
        ELSE
            /*
                Данные не наши (остались от прошлого клиента пулера).
                Помечаем и очищаем.
            */
            PERFORM public.fasttruncate(pg_catalog.format('pg_temp.%I', _c_temp_table));
            PERFORM public.pgv_set('tx_info', _temp_table_hash, TRUE);

            RETURN NULL;
        END IF;
    END IF;


    -- Compare live column names, types, typmods and collations through relcache.
    -- Dropped attributes do not affect descriptor equality.
    IF _b_exists_table AND NOT _b_in
    THEN
        IF public.fasttrun_temp_same_cols(_oid_temp_table, _oid_dummy_table) IS NOT TRUE
        THEN
            _saved_role := pg_catalog.current_setting('role');
            IF pg_catalog.to_regrole('_client') IS NOT NULL
               AND (SELECT relowner = pg_catalog.to_regrole('_client')
                      FROM pg_catalog.pg_class WHERE oid = _oid_temp_table)
            THEN
                SET LOCAL ROLE _client;
            END IF;
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', _c_temp_table);
            PERFORM pg_catalog.set_config('role', _saved_role, true);

            _b_exists_table = FALSE;
        END IF;
    END IF;


    /*------------------------------------------------------------------------------------
        Локальная таблица существует - очищаем и выходим
    -------------------------------------------------------------------------------------*/
    IF NOT _b_in
       AND _b_exists_table
    THEN
        PERFORM public.fasttruncate(pg_catalog.format('pg_temp.%I', _c_temp_table));
        PERFORM public.pgv_set('tx_info', _temp_table_hash, TRUE);

        RETURN FALSE;
    END IF;


    /*------------------------------------------------------------------------------------
        Таблицы нет - создаем из шаблона.

        SET LOCAL ROLE - чтобы таблица сразу принадлежала нужному пользователю,
        без дополнительного ALTER TABLE OWNER (который генерит sinval).
    -------------------------------------------------------------------------------------*/
    _saved_role := pg_catalog.current_setting('role');
    EXECUTE
        pg_catalog.format
        (
            $str$
            SET LOCAL ROLE _client;
            CREATE TEMP TABLE %I (
                LIKE %I.%I
                INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
                INCLUDING GENERATED INCLUDING IDENTITY
            );
            $str$,
            _c_temp_table,
            _c_dummy_schema,
            _c_actual_dummy_table
        );

    PERFORM pg_catalog.set_config('role', _saved_role, true);

    PERFORM public.pgv_set('tx_info', _temp_table_hash, TRUE);

    IF _recreated_incoming THEN
        RETURN NULL;
    END IF;

    RETURN TRUE;
END;
$_$;
