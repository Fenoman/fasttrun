--
-- fasttrun_migration — полная проверка пути обновления 2.0 -> 2.1.
--
-- Сценарий из реальной жизни: в продакшене уже стоит fasttrun 2.0
-- из upstream-репозитория PostgresPro, поверх него скрипт
-- docker-entrypoint.sh ранее переименовал C-функцию fasttruncate в
-- fasttruncate_c и создал PL/pgSQL-обёртку. Мы хотим, чтобы при
-- выкладке нового образа с нашим форком ALTER EXTENSION fasttrun
-- UPDATE привёл базу в нужное состояние, ничего не сломав.
--

-- ----------------------------------------------------------------------
-- 1. Ставим версию 2.0 — это эквивалент апстрима: только fasttruncate
-- ----------------------------------------------------------------------
CREATE EXTENSION fasttrun VERSION '2.0';

SELECT extname, extversion FROM pg_extension WHERE extname = 'fasttrun';

-- В свежей 2.0 у нас только одна C-функция: fasttruncate
SELECT n.nspname, p.proname, l.lanname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_language  l ON l.oid = p.prolang
 WHERE p.proname IN ('fasttruncate', 'fasttruncate_c',
                     'fasttrun_analyze', 'fasttrun_relstats',
                     'fasttrun_collect_stats')
   AND n.nspname = 'public'
 ORDER BY p.proname;

-- ----------------------------------------------------------------------
-- 2. Воспроизводим то, что делал старый docker-entrypoint.sh:
--    переименование + создание PL/pgSQL-обёртки.  После этого
--    расширение по-прежнему владеет функцией (под именем
--    fasttruncate_c), а в роли public.fasttruncate(text) выступает
--    PL/pgSQL-обёртка.
-- ----------------------------------------------------------------------
ALTER FUNCTION public.fasttruncate(text) RENAME TO fasttruncate_c;

CREATE OR REPLACE FUNCTION public.fasttruncate(_c_temp_table text)
RETURNS void
LANGUAGE plpgsql AS $wrapper$
BEGIN
    PERFORM public.fasttruncate_c(_c_temp_table);
END;
$wrapper$;

-- Картинка после старого entrypoint:
SELECT n.nspname, p.proname, l.lanname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_language  l ON l.oid = p.prolang
 WHERE p.proname IN ('fasttruncate', 'fasttruncate_c',
                     'fasttrun_analyze', 'fasttrun_relstats',
                     'fasttrun_collect_stats')
   AND n.nspname = 'public'
 ORDER BY p.proname;

-- ----------------------------------------------------------------------
-- 3. Проверяем, что в этом гибридном состоянии и обёртка, и
--    алиас действительно работают на временной таблице
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_mig (a int);
INSERT INTO t_mig SELECT generate_series(1, 100);
SELECT count(*) FROM t_mig;

-- через обёртку
SELECT public.fasttruncate('t_mig');
SELECT count(*) FROM t_mig;

-- наполняем заново и через прямой вызов C-функции
INSERT INTO t_mig SELECT generate_series(1, 50);
SELECT public.fasttruncate_c('t_mig');
SELECT count(*) FROM t_mig;

-- ----------------------------------------------------------------------
-- 4. Применяем миграцию 2.0 -> 2.1
-- ----------------------------------------------------------------------
ALTER EXTENSION fasttrun UPDATE TO '2.1';

SELECT extname, extversion FROM pg_extension WHERE extname = 'fasttrun';

-- После миграции:
--   * public.fasttruncate(text)            — снова C-функция (НЕ обёртка)
--   * public.fasttruncate_c(text)          — остался C-алиас (мы его не трогали)
--   * public.fasttrun_analyze(text)        — новая C-функция
--   * public.fasttrun_relstats(text)       — новая C-функция
--   * public.fasttrun_collect_stats(text)  — новая C-функция
-- Все они должны принадлежать расширению fasttrun.
SELECT n.nspname, p.proname, l.lanname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_language  l ON l.oid = p.prolang
 WHERE p.proname IN ('fasttruncate', 'fasttruncate_c',
                     'fasttrun_analyze', 'fasttrun_relstats',
                     'fasttrun_collect_stats')
   AND n.nspname = 'public'
 ORDER BY p.proname;

-- Все функции должны принадлежать расширению fasttrun
SELECT p.proname, e.extname AS owner_extension
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
  JOIN pg_extension e ON e.oid = d.refobjid
 WHERE p.proname IN ('fasttruncate', 'fasttruncate_c',
                     'fasttrun_analyze', 'fasttrun_relstats',
                     'fasttrun_collect_stats')
   AND n.nspname = 'public'
 ORDER BY p.proname;

-- ----------------------------------------------------------------------
-- 5. Проверяем функциональность всех 4 функций после миграции
-- ----------------------------------------------------------------------
INSERT INTO t_mig SELECT generate_series(1, 200);

-- новая C-функция fasttruncate
SELECT count(*) FROM t_mig;
SELECT public.fasttruncate('t_mig');
SELECT count(*) FROM t_mig;

-- алиас fasttruncate_c для обратной совместимости
INSERT INTO t_mig SELECT generate_series(1, 30);
SELECT count(*) FROM t_mig;
SELECT public.fasttruncate_c('t_mig');
SELECT count(*) FROM t_mig;

-- новая fasttrun_analyze
INSERT INTO t_mig SELECT generate_series(1, 75);
SELECT public.fasttrun_analyze('t_mig');
SELECT reltuples = 75 AS analyze_published_count
  FROM public.fasttrun_relstats('t_mig');

-- новая fasttrun_collect_stats: должна доехать апгрейдом и быть
-- вызываемой на ту же временную таблицу.
SELECT public.fasttrun_collect_stats('t_mig');

-- fasttrun_inspect_stats должна доехать апгрейдом 2.0->2.1
BEGIN;
SELECT public.fasttrun_collect_stats('t_mig');
SELECT count(*) > 0 AS inspect_works
  FROM public.fasttrun_inspect_stats('t_mig');
COMMIT;

-- ----------------------------------------------------------------------
-- 6. После DROP EXTENSION все функции должны исчезнуть
--    (поскольку все они принадлежат расширению fasttrun, включая
--    алиас fasttruncate_c, оставленный для обратной совместимости).
-- ----------------------------------------------------------------------
DROP TABLE t_mig;
DROP EXTENSION fasttrun;

SELECT count(*) AS leftover_funcs
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE p.proname IN ('fasttruncate', 'fasttruncate_c',
                     'fasttrun_analyze', 'fasttrun_relstats',
                     'fasttrun_collect_stats', 'fasttrun_inspect_stats')
   AND n.nspname = 'public';
