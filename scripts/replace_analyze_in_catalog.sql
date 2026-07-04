--
-- Замена ANALYZE temp_xxx → PERFORM fasttrun_analyze('temp_xxx')
-- прямо в системном каталоге (pg_proc.prosrc).
--
-- ВНИМАНИЕ: выполнять под суперпользователем.
-- Скрипт работает в транзакции — если что-то не так, ROLLBACK.
--
-- Порядок:
--   1. Запустить секцию "ШАГ 1" — покажет список функций и замен (dry run),
--      отдельно — функции, которые будут ПРОПУЩЕНЫ (ручная правка)
--   2. Проверить глазами что всё верно
--   3. Запустить секцию "ШАГ 2" — применить замены
--   4. COMMIT или ROLLBACK
--
-- Замена срабатывает только на ANALYZE в позиции стейтмента: после ';'
-- или ключевых слов BEGIN/THEN/ELSE/LOOP (плюс начало тела).  Вхождения
-- внутри строковых литералов (EXECUTE 'ANALYZE temp_x;') и комментариев
-- не трогаются: вставка кавычек ломала бы литерал, а PERFORM невалиден
-- в динамическом SQL.  Функции, где паттерн замечен внутри литерала,
-- целиком исключаются из UPDATE и попадают в список ручной правки.
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
    -- Функция будет пропущена: паттерн внутри строкового литерала
    p.prosrc ~* '''[^'']*ANALYZE\s+temp_' AS skipped_manual_review,
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
ORDER BY n.nspname, p.proname;

-- Функции, которые UPDATE не тронет — править руками:
-- паттерн замечен внутри одинарных кавычек (динамический SQL).
SELECT
    n.nspname || '.' || p.proname AS function_name,
    'ANALYZE temp_ внутри строкового литерала — заменить вручную' AS reason
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE l.lanname = 'plpgsql'
  AND p.prosrc ~* '''[^'']*ANALYZE\s+temp_'
ORDER BY 1;

-- Превью: как будет выглядеть замена (statement-позиция)
SELECT
    n.nspname || '.' || p.proname AS function_name,
    regexp_matches(p.prosrc,
                   '(^|;|\m(?:BEGIN|THEN|ELSE|LOOP)\M)\s*(ANALYZE\s+temp_\w+\s*;)',
                   'gi') AS original
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE l.lanname = 'plpgsql'
  AND p.prosrc ~* 'ANALYZE\s+temp_\w+\s*;'
LIMIT 20;


-- ======================================================================
-- ШАГ 2: ПРИМЕНИТЬ ЗАМЕНЫ (раскомментировать и выполнить)
-- ======================================================================
-- APPLY_BEGIN
-- BEGIN;
--
-- UPDATE pg_proc p
-- SET prosrc = regexp_replace(
--     prosrc,
--     '(^|;|\m(?:BEGIN|THEN|ELSE|LOOP)\M)(\s*)ANALYZE\s+(temp_\w+)\s*;',
--     E'\\1\\2PERFORM fasttrun_analyze(''\\3'');',
--     'gi'
-- )
-- FROM pg_namespace n
-- JOIN pg_language l ON l.lanname = 'plpgsql'
-- WHERE n.oid = p.pronamespace
--   AND l.oid = p.prolang
--   AND p.prosrc ~* 'ANALYZE\s+temp_\w+\s*;'
--   -- Паттерн внутри строкового литерала: функцию не трогаем (ручная правка)
--   AND p.prosrc !~* '''[^'']*ANALYZE\s+temp_'
--   -- Не трогаем системные схемы
--   AND n.nspname NOT IN ('pg_catalog', 'information_schema');
--
-- -- Проверяем результат: still_has_analyze = t означает остатки —
-- -- либо пропущенные литералы (ручная правка), либо нестандартная
-- -- позиция стейтмента; смотреть глазами.
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
-- APPLY_END
--
-- -- Если всё ок:
-- -- COMMIT;
-- -- Если что-то не так:
-- -- ROLLBACK;
