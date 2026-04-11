--
-- Пример функции create_temp_table для использования с fasttrun.
--
-- Это продакшн-версия, проверенная под высокой нагрузкой. Обрабатывает:
--   * создание temp table из таблицы-шаблона ("чучела")
--   * быструю очистку через fasttruncate (ноль sinval)
--   * "входящие" таблицы (данные заполнены выше по стеку вызовов)
--   * пересоздание при изменении структуры шаблона (деплой)
--   * смену владельца через SET LOCAL ROLE (без ALTER TABLE OWNER = без sinval)
--
-- Зависимости:
--   * расширение fasttrun (fasttruncate / fasttruncate_c)
--   * расширение pg_variables (pgv_set / pgv_exists) — сессионные переменные
--     для отслеживания "наших" данных при работе через пулер
--     https://github.com/postgrespro/pg_variables
--
-- Параметры:
--   _c_temp_table   — имя временной таблицы
--   _b_in           — "входящая" таблица: данные уже заполнены выше по стеку
--                      вызовов (в другой функции той же транзакции), не удалять
--   _c_dummy_schema — схема с таблицами-шаблонами (по умолчанию dummy_tmp)
--   _c_dummy_table  — имя шаблона, если отличается от _c_temp_table
--
-- Возвращает:
--   TRUE  — таблица создана с нуля
--   FALSE — таблица существовала и очищена (для _b_in: данные заполнены выше, НЕ очищены)
--   NULL  — только для _b_in: данные НЕ "наши", очищены
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
    _XMIN                 xid8;
    _current_xact_id      xid8;
    _temp_table_hash      text;
    _i_temp_relnatts      int2;
    _i_dummy_relnatts     int2;
    _c_actual_dummy_table text;
BEGIN
    -- Приводим к нижнему регистру
    _c_temp_table = LOWER(_c_temp_table);
    _temp_table_hash = MD5(_c_temp_table)::text;
    _current_xact_id = pg_current_xact_id_if_assigned();

    -- Имя шаблона: если не задано явно, совпадает с именем temp table
    _c_actual_dummy_table = CASE
                                WHEN _c_dummy_table IS NULL OR _c_dummy_table = ''
                                    THEN _c_temp_table
                                ELSE _c_dummy_table
                            END;

    /*
        PL/pgSQL использует short-circuit evaluation для AND:
        если левый операнд FALSE, правый не вычисляется.
        Поэтому порядок условий важен.
    */

    SELECT
        TRUE,
        c.XMIN,
        c.relnatts
    INTO _b_exists_table, _XMIN, _i_temp_relnatts
    FROM pg_catalog.pg_class c
    WHERE c.relnamespace = PG_MY_TEMP_SCHEMA()
      AND c.relname = _c_temp_table;


    /*------------------------------------------------------------------------------------
        Проверяем количество атрибутов в шаблоне через relnatts.

        ВАЖНО: relnatts включает ВСЕ атрибуты, в том числе удалённые (dropped).
        При DROP COLUMN PostgreSQL физически не удаляет колонку, а помечает
        attisdropped = true, и relnatts не уменьшается.

        Поэтому при удалении колонки из шаблона нужно ПЕРЕСОЗДАТЬ его,
        а не использовать ALTER TABLE ... DROP COLUMN.
    -------------------------------------------------------------------------------------*/
    IF _b_exists_table AND NOT _b_in
    THEN
        SELECT
            c.relnatts
        INTO _i_dummy_relnatts
        FROM pg_catalog.pg_class c
             INNER JOIN pg_catalog.pg_namespace n
                        ON n.oid = c.relnamespace
        WHERE n.nspname = _c_dummy_schema
          AND c.relname = _c_actual_dummy_table;

        -- Структура изменилась — пересоздаём
        IF _i_temp_relnatts IS DISTINCT FROM _i_dummy_relnatts
        THEN
            EXECUTE pg_catalog.format('DROP TABLE %I', _c_temp_table);

            _b_exists_table = FALSE;
        END IF;
    END IF;


    /*------------------------------------------------------------------------------------
        Входящая таблица (_b_in = true):
        Данные заполнены выше по стеку вызовов (другая функция в той же
        транзакции). Если данные "наши" (есть запись в pgv или таблица
        создана в текущей транзакции) — выходим без очистки.
    -------------------------------------------------------------------------------------*/
    IF _b_in
       AND _b_exists_table
    THEN
        IF pgv_exists('tx_info', _temp_table_hash)
            OR _XMIN >= _current_xact_id
        THEN
            -- Данные наши — ничего не трогаем
            RETURN FALSE;
        END IF;

        /*
            Данные не наши (остались от прошлого клиента пулера).
            Помечаем и очищаем.

            Все pgv-переменные автоматически очищаются пулером:
            * в транзакционном режиме — при завершении транзакции
            * в сессионном — при отсоединении клиента
        */
        PERFORM pgv_set('tx_info', _temp_table_hash, TRUE);

        PERFORM public.fasttruncate(_c_temp_table);

        RETURN NULL;
    END IF;


    /*------------------------------------------------------------------------------------
        Локальная таблица существует — очищаем и выходим
    -------------------------------------------------------------------------------------*/
    IF NOT _b_in
       AND _b_exists_table
    THEN
        PERFORM pgv_set('tx_info', _temp_table_hash, TRUE);

        PERFORM public.fasttruncate(_c_temp_table);

        RETURN FALSE;
    END IF;


    /*------------------------------------------------------------------------------------
        Таблицы нет — создаём из шаблона.

        SET LOCAL ROLE — чтобы таблица сразу принадлежала нужному пользователю,
        без дополнительного ALTER TABLE OWNER (который генерит sinval).
    -------------------------------------------------------------------------------------*/
    EXECUTE
        pg_catalog.format
        (
            $str$
            SET LOCAL ROLE _client;
            CREATE TEMP TABLE %s (LIKE %s.%s %s);
            RESET ROLE;
            $str$,
            _c_temp_table,
            _c_dummy_schema,
            _c_actual_dummy_table,
            CASE
                WHEN _c_dummy_table IS NULL OR _c_dummy_table = ''
                    THEN 'INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING GENERATED INCLUDING IDENTITY'
                ELSE ''
            END
        );

    PERFORM pgv_set('tx_info', _temp_table_hash, TRUE);

    RETURN TRUE;
END;
$_$;
