--
-- Замена ANALYZE temp_xxx → PERFORM fasttrun_analyze('temp_xxx')
-- прямо в системном каталоге (pg_proc.prosrc).
--
-- ВНИМАНИЕ: выполнять под суперпользователем.
-- Скрипт работает в транзакции — если что-то не так, ROLLBACK.
--
-- Порядок:
--   1. Запустить секцию "ШАГ 1" — покажет список функций и замен (dry run)
--   2. Проверить глазами что всё верно
--   3. Запустить секцию "ШАГ 2" — применить замены
--   4. COMMIT или ROLLBACK
--

-- ======================================================================
-- ШАГ 1: DRY RUN — показать что будет заменено
-- ======================================================================

-- Все функции, содержащие ANALYZE temp_
SELECT
    n.nspname || '.' || p.proname AS function_name,
    -- Сколько замен в этой функции
    (length(p.prosrc) - length(
        regexp_replace(p.prosrc, 'ANALYZE\s+(temp_\w+)', '', 'gi')
    )) > 0 AS has_matches,
    -- Показать строки с ANALYZE
    (SELECT string_agg(line, E'\n' ORDER BY rn)
     FROM (
         SELECT row_number() OVER () AS rn, line
         FROM unnest(string_to_array(p.prosrc, E'\n')) WITH ORDINALITY AS t(line, rn)
         WHERE line ~* 'ANALYZE\s+temp_'
           AND line !~* '^\s*--'  -- пропускаем закомментированные
     ) sub
    ) AS matching_lines
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE l.lanname = 'plpgsql'
  AND p.prosrc ~* 'ANALYZE\s+temp_'
  -- Исключаем закомментированные (грубая проверка — основной фильтр ниже)
ORDER BY n.nspname, p.proname;

-- Превью: как будет выглядеть замена (первые 5 функций)
SELECT
    n.nspname || '.' || p.proname AS function_name,
    regexp_matches(p.prosrc, '(ANALYZE\s+(temp_\w+)\s*;)', 'gi') AS original
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE l.lanname = 'plpgsql'
  AND p.prosrc ~* 'ANALYZE\s+temp_\w+\s*;'
LIMIT 20;


-- ======================================================================
-- ШАГ 2: ПРИМЕНИТЬ ЗАМЕНЫ (раскомментировать и выполнить)
-- ======================================================================

-- BEGIN;
--
-- UPDATE pg_proc p
-- SET prosrc = regexp_replace(
--     prosrc,
--     'ANALYZE\s+(temp_\w+)\s*;',
--     E'PERFORM fasttrun_analyze(''\\1'');',
--     'gi'
-- )
-- FROM pg_namespace n
-- JOIN pg_language l ON l.lanname = 'plpgsql'
-- WHERE n.oid = p.pronamespace
--   AND l.oid = p.prolang
--   AND p.prosrc ~* 'ANALYZE\s+temp_\w+\s*;'
--   -- Не трогаем системные схемы
--   AND n.nspname NOT IN ('pg_catalog', 'information_schema');
--
-- -- Проверяем результат
-- SELECT
--     n.nspname || '.' || p.proname AS function_name,
--     p.prosrc ~* 'ANALYZE\s+temp_' AS still_has_analyze,
--     p.prosrc ~* 'fasttrun_analyze' AS has_fasttrun
-- FROM pg_proc p
-- JOIN pg_namespace n ON n.oid = p.pronamespace
-- JOIN pg_language l ON l.oid = p.prolang
-- WHERE l.lanname = 'plpgsql'
--   AND (p.prosrc ~* 'ANALYZE\s+temp_' OR p.prosrc ~* 'fasttrun_analyze')
--   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
-- ORDER BY 1;
--
-- -- Если всё ок:
-- -- COMMIT;
-- -- Если что-то не так:
-- -- ROLLBACK;
