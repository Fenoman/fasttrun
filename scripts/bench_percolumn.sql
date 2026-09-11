\set ON_ERROR_STOP on
\pset pager off
SET client_min_messages = warning;
SET jit = off;
SET max_parallel_workers_per_gather = 0;

--
-- bench_percolumn.sql — сравнение HEAD и поколоночного учёта UPDATE.
--
-- Прогоняется скриптом scripts/bench_percolumn.sh, который чередует сборки и
-- складывает результат в CSV. Прогрев есть у большинства метрик, но не у всех:
-- у метрик рабочего цикла и переключения видимости прогрев отдельно не сделан.
-- Печатает две колонки через вертикальную черту: имя метрики и миллисекунды.
--
CREATE EXTENSION IF NOT EXISTS fasttrun;

CREATE TEMP TABLE bench_result (metric text, ms numeric);

CREATE OR REPLACE FUNCTION bench_note(m text, t0 timestamptz) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO bench_result
    VALUES (m, round(extract(epoch FROM clock_timestamp() - t0)::numeric * 1000, 3));
END$$;

CREATE OR REPLACE FUNCTION ft_rows_est(q text, pat text) RETURNS int LANGUAGE plpgsql AS $ft$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN ' || q LOOP
    IF ln ~ pat THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  RETURN est;
END$ft$;

-- ---------------------------------------------------------------------------
-- 0. Библиотека загружена, кэша статистики ещё нет: обычная работа с
--    постоянными таблицами. Хуки исполнителя стоят у всех таких соединений.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bench_perm (id int, c1 int);
TRUNCATE bench_perm;
INSERT INTO bench_perm SELECT g, g FROM generate_series(1, 1000) g;
CREATE INDEX IF NOT EXISTS bench_perm_id ON bench_perm (id);
ANALYZE bench_perm;
DO $$
DECLARE t0 timestamptz; i int; n int;
BEGIN
  FOR i IN 1..2000 LOOP SELECT count(*) INTO n FROM bench_perm WHERE id = -1; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP SELECT count(*) INTO n FROM bench_perm WHERE id = -1; END LOOP;
  PERFORM bench_note('select_permanent_no_stats_cache', t0);

  FOR i IN 1..2000 LOOP UPDATE bench_perm SET c1 = c1 WHERE id = -1; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE bench_perm SET c1 = c1 WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_permanent_no_stats_cache', t0);
END$$;

-- ---------------------------------------------------------------------------
-- 1. Короткие подготовленные DML: цена самого хука, без работы со строками.
-- ---------------------------------------------------------------------------
SELECT 'CREATE TEMP TABLE b_wide (id int, ' || string_agg(format('c%s int', i), ', ') || ')'
  FROM generate_series(1, 200) i \gexec
INSERT INTO b_wide SELECT g, g, g FROM generate_series(1, 1000) g;
CREATE INDEX ON b_wide (id);

CREATE TEMP TABLE b_unmanaged (id int, c1 int);
INSERT INTO b_unmanaged SELECT g, g FROM generate_series(1, 1000) g;

CREATE INDEX ON b_unmanaged (id);

SELECT fasttrun_collect_stats('b_wide') \g /dev/null

-- unmanaged control: механизм статистики уже поднят, но эту таблицу не ведёт
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..3000 LOOP UPDATE b_unmanaged SET c1 = c1 WHERE id = -1; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE b_unmanaged SET c1 = c1 WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_update_zero_rows_unmanaged', t0);
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP DELETE FROM b_unmanaged WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_delete_zero_rows_unmanaged', t0);
END$$;

-- свежая маска: каждый цикл начинается с пересбора? нет -- маска копится,
-- поэтому сначала меряем накопленную, затем отдельно первый UPDATE после сбора
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..3000 LOOP UPDATE b_wide SET c1 = c1 WHERE id = -1; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE b_wide SET c1 = c1 WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_update_zero_rows_low_col_accum_mask', t0);

  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE b_wide SET c200 = c200 WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_update_zero_rows_high_col_accum_mask', t0);

  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP DELETE FROM b_wide WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_delete_zero_rows_managed', t0);

  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP INSERT INTO b_wide (id) SELECT 1 WHERE false; END LOOP;
  PERFORM bench_note('dml_insert_zero_rows_managed', t0);
END$$;

-- дальше идёт настоящий массовый UPDATE. На HEAD он прячет статистику всей
-- таблицы, видимость переключается и локальные планы сбрасываются; на патче
-- колонка id не тронута и планы остаются. Отсюда разница ниже -- это не цена
-- хука, а сохранённая работа планировщика.
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE b_wide SET c1 = c1 WHERE id = 1; END LOOP;
  PERFORM bench_note('dml_update_one_row_low_col_with_churn', t0);

  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP DELETE FROM b_wide WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_delete_zero_rows_after_churn', t0);
END$$;

-- Первая маска после пересбора. Меряется парой: сначала один пересбор, затем
-- пересбор плюс один UPDATE. Разница включает весь UPDATE целиком, а не только
-- битовую карту, поэтому она даёт верхнюю границу её цены, а не саму цену.
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..2000 LOOP
    PERFORM fasttrun_collect_stats('b_wide');
  END LOOP;
  PERFORM bench_note('recollect_only', t0);

  t0 := clock_timestamp();
  FOR i IN 1..2000 LOOP
    PERFORM fasttrun_collect_stats('b_wide');
    UPDATE b_wide SET c200 = c200 WHERE id = -1;
  END LOOP;
  PERFORM bench_note('recollect_plus_first_update_high_col', t0);

  t0 := clock_timestamp();
  FOR i IN 1..2000 LOOP
    PERFORM fasttrun_collect_stats('b_wide');
    UPDATE b_wide SET c1 = c1 WHERE id = -1;
  END LOOP;
  PERFORM bench_note('recollect_plus_first_update_low_col', t0);
END$$;

-- Пишущий UPDATE без переключения видимости: таблица достаточно велика, чтобы
-- оборот остался ниже допуска, поэтому обе сборки держат статистику видимой и
-- меряется только цена хука на операторе, который действительно пишет строку.
SELECT 'CREATE TEMP TABLE b_write (id int, ' || string_agg(format('c%s int', i), ', ') || ')'
  FROM generate_series(1, 200) i \gexec
INSERT INTO b_write (id, c1, c200) SELECT g, g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON b_write (id);
SELECT fasttrun_collect_stats('b_write') \g /dev/null
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..500 LOOP UPDATE b_write SET c200 = c200 WHERE id = i; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..4000 LOOP UPDATE b_write SET c200 = c200 WHERE id = i; END LOOP;
  PERFORM bench_note('dml_update_one_row_high_col_no_flip', t0);
END$$;

-- Несколько целей в одном операторе и вложенный DML: новые обходы реестра.
CREATE TEMP TABLE b_parent (id int, note int);
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..8 LOOP
    EXECUTE format('CREATE TEMP TABLE b_child%s () INHERITS (b_parent)', i);
  END LOOP;
END$$;
SELECT fasttrun_collect_stats('b_child1') \g /dev/null
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..2000 LOOP UPDATE b_parent SET note = 1 WHERE id = -1; END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP UPDATE b_parent SET note = 1 WHERE id = -1; END LOOP;
  PERFORM bench_note('dml_update_nine_targets_zero_rows', t0);

  t0 := clock_timestamp();
  FOR i IN 1..20000 LOOP
    WITH u AS (UPDATE b_wide SET c1 = c1 WHERE id = -1 RETURNING id)
    UPDATE b_write SET c1 = c1 WHERE id = -1 AND EXISTS (SELECT 1 FROM u);
  END LOOP;
  PERFORM bench_note('dml_modifying_cte_two_targets', t0);
END$$;

-- Первая маска на дешёвой таблице: пересбор ста строк стоит мало, поэтому
-- добавка от первого UPDATE после него занимает заметную долю итерации.
SELECT 'CREATE TEMP TABLE b_tiny (id int, ' || string_agg(format('c%s int', i), ', ') || ')'
  FROM generate_series(1, 200) i \gexec
INSERT INTO b_tiny (id, c1, c200) SELECT g, g, g FROM generate_series(1, 100) g;
SELECT fasttrun_collect_stats('b_tiny') \g /dev/null
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..2000 LOOP PERFORM fasttrun_collect_stats('b_tiny'); END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..10000 LOOP PERFORM fasttrun_collect_stats('b_tiny'); END LOOP;
  PERFORM bench_note('tiny_recollect_only', t0);

  t0 := clock_timestamp();
  FOR i IN 1..10000 LOOP
    PERFORM fasttrun_collect_stats('b_tiny');
    UPDATE b_tiny SET c200 = c200 WHERE id = -1;
  END LOOP;
  PERFORM bench_note('tiny_recollect_plus_first_update_high_col', t0);
END$$;

-- Вложенный DML и граница подтранзакции: блок EXCEPTION заводит подтранзакцию
-- на каждой итерации, реестр при этом не пуст. Оба UPDATE успевают завершиться
-- до фиксации подтранзакции, поэтому уборка незавершённого писателя здесь не
-- меряется -- только обычный проход по реестру.
CREATE TEMP TABLE b_nest1 (id int, c1 int);
CREATE TEMP TABLE b_nest2 (id int, c1 int);
INSERT INTO b_nest1 SELECT g, g FROM generate_series(1, 1000) g;
INSERT INTO b_nest2 SELECT g, g FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('b_nest1') \g /dev/null
SELECT fasttrun_collect_stats('b_nest2') \g /dev/null
DO $$
DECLARE t0 timestamptz; i int;
BEGIN
  FOR i IN 1..1000 LOOP
    BEGIN
      UPDATE b_nest1 SET c1 = c1 WHERE id = -1;
      UPDATE b_nest2 SET c1 = c1 WHERE id = -1;
    EXCEPTION WHEN others THEN NULL;
    END;
  END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..10000 LOOP
    BEGIN
      UPDATE b_nest1 SET c1 = c1 WHERE id = -1;
      UPDATE b_nest2 SET c1 = c1 WHERE id = -1;
    EXCEPTION WHEN others THEN NULL;
    END;
  END LOOP;
  PERFORM bench_note('nested_dml_subxact_cleanup', t0);
END$$;

-- ---------------------------------------------------------------------------
-- 2. Планирование и COMMIT по отдельности.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE b_plan (id int, grp int, note text);
INSERT INTO b_plan SELECT g, g % 100, 'x' FROM generate_series(1, 20000) g;
CREATE INDEX ON b_plan (id);
CREATE INDEX ON b_plan (grp);
SELECT fasttrun_analyze('b_plan') \g /dev/null

DO $$
DECLARE t0 timestamptz; i int; r record;
BEGIN
  FOR i IN 1..200 LOOP
    EXECUTE 'EXPLAIN SELECT * FROM b_plan WHERE id = 1 AND grp = 1' INTO r;
  END LOOP;
  t0 := clock_timestamp();
  FOR i IN 1..3000 LOOP
    EXECUTE 'EXPLAIN SELECT * FROM b_plan WHERE id = 1 AND grp = 1' INTO r;
  END LOOP;
  PERFORM bench_note('planning_untouched_columns', t0);
END$$;

UPDATE b_plan SET grp = grp WHERE id <= 100;

DO $$
DECLARE t0 timestamptz; i int; r record;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..3000 LOOP
    EXECUTE 'EXPLAIN SELECT * FROM b_plan WHERE id = 1 AND grp = 1' INTO r;
  END LOOP;
  PERFORM bench_note('planning_one_touched_column', t0);
END$$;

CREATE OR REPLACE PROCEDURE bench_commits(n int, m text)
LANGUAGE plpgsql AS $$
DECLARE t0 timestamptz; i int;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..n LOOP
    UPDATE b_plan SET grp = grp WHERE id = -1;
    COMMIT;
  END LOOP;
  PERFORM bench_note(m, t0);
END$$;

CALL bench_commits(300, 'commit_warmup');
DELETE FROM bench_result WHERE metric = 'commit_warmup';
CALL bench_commits(3000, 'commit_small_plan_population');

-- большая популяция сохранённых планов
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..300 LOOP
    EXECUTE format('PREPARE bp_%s AS SELECT * FROM b_plan WHERE id = $1 AND grp = %s', i, i);
    EXECUTE format('EXECUTE bp_%s(1)', i);
  END LOOP;
END$$;
CALL bench_commits(3000, 'commit_large_plan_population');

-- COMMIT с настоящим переходом видимости: каждая транзакция переписывает
-- больше порога, поэтому статистика гаснет и обход планов действительно идёт.
CREATE OR REPLACE PROCEDURE bench_flip_commits(n int, m text)
LANGUAGE plpgsql AS $$
DECLARE t0 timestamptz; i int;
BEGIN
  t0 := clock_timestamp();
  FOR i IN 1..n LOOP
    PERFORM fasttrun_analyze('b_plan');
    UPDATE b_plan SET grp = grp;
    COMMIT;
  END LOOP;
  PERFORM bench_note(m, t0);
END$$;
CALL bench_flip_commits(6, 'commit_visibility_flip_warmup');
DELETE FROM bench_result WHERE metric = 'commit_visibility_flip_warmup';
CALL bench_flip_commits(60, 'commit_visibility_flip_large_plans');

-- ---------------------------------------------------------------------------
-- 3. Повторный рабочий цикл: очистка, набор, анализ, UPDATE, запрос по ключу.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE b_cycle (id int, f_subscr int, f_units int, d_begin date,
                           d_end date, note text, payload text);
CREATE INDEX ON b_cycle (id);
CREATE INDEX ON b_cycle (f_subscr, d_begin);

CREATE OR REPLACE PROCEDURE bench_cycles(rounds int, typanalyze bool, tag text)
LANGUAGE plpgsql AS $$
DECLARE t0 timestamptz; i int; est int; bad int := 0;
BEGIN
  EXECUTE format('SET LOCAL fasttrun.use_typanalyze = %s', typanalyze);
  t0 := clock_timestamp();
  FOR i IN 1..rounds LOOP
    PERFORM fasttruncate('b_cycle');
    INSERT INTO b_cycle
      SELECT g, g % 5000, 0, date '2026-01-01', date '2026-12-31',
             'note', repeat('p', 60)
        FROM generate_series(1, 175000) g;
    PERFORM fasttrun_analyze('b_cycle');
    UPDATE b_cycle SET f_units = 7;
    est := ft_rows_est('SELECT * FROM b_cycle WHERE id = 1', 'on b_cycle');
    IF est > 10 THEN bad := bad + 1; END IF;
  END LOOP;
  PERFORM bench_note(tag, t0);
  INSERT INTO bench_result VALUES (tag || '_bad_estimates', bad);
END$$;

CALL bench_cycles(3, false, 'work_cycle_typanalyze_off');
CALL bench_cycles(3, true,  'work_cycle_typanalyze_on');

\pset format unaligned
\pset fieldsep '|'
SELECT metric, ms FROM bench_result ORDER BY metric;
