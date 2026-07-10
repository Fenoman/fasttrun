--
-- fasttrun_plan_cache_survive — regression-тест для реального production path
-- со stale SPI / PL/pgSQL plan cache.
--
-- Суть сценария:
--   * static nested UPDATE внутри PL/pgSQL/SPI планируется на первом вызове;
--   * затем этот cached plan переиспользуется в том же backend'е;
--   * после fasttruncate + refill + fasttrun_analyze следующий вызов
--     не должен продолжать жить на stale cached plan.
--
-- Важно: тест специально НЕ проверяет точный join type.  Upstream PG и PGPro
-- могут выбрать разные свежие планы.  Контракт здесь другой:
--   1. второй вызов завершается без statement_timeout;
--   2. fasttruncate реально сбрасывает temp tables между итерациями;
--   3. nested UPDATE обновляет все строки после refill.
--

CREATE EXTENSION fasttrun;

CREATE TEMP TABLE t_balance_out
(
    id int,
    f_charges int,
    f_sale_categories int,
    payload text
);

CREATE TEMP TABLE t_rsd_details
(
    f_charges int,
    f_sale_categories int
);

CREATE OR REPLACE FUNCTION f_inner() RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    UPDATE t_balance_out AS bo
       SET f_sale_categories = t.f_sale_categories
      FROM (
            SELECT DISTINCT
                   t.f_charges,
                   t.f_sale_categories
              FROM t_rsd_details AS t
           ) AS t
     WHERE t.f_charges = bo.f_charges;
END;
$fn$;

CREATE OR REPLACE FUNCTION f_outer(_n_bo int, _n_rd int) RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    PERFORM public.fasttruncate('t_balance_out');
    PERFORM public.fasttruncate('t_rsd_details');

    INSERT INTO t_balance_out
    SELECT g, g, NULL, repeat('x', 50)
      FROM generate_series(1, _n_bo) AS g;

    INSERT INTO t_rsd_details
    SELECT g, g % 7
      FROM generate_series(1, _n_rd) AS g;

    PERFORM public.fasttrun_analyze('t_balance_out');
    PERFORM public.fasttrun_analyze('t_rsd_details');

    PERFORM f_inner();
END;
$fn$;

-- Статический SPI statement без параметров держится в generic cached plan.
-- Явно фиксируем этот контракт на уровне сессии.
SET plan_cache_mode = force_generic_plan;

-- Прогреваем SPI cache на маленькой первой итерации: если local invalidation
-- не сработает, stale plan родится именно здесь.
DO $$
BEGIN
    PERFORM f_outer(1, 1);
END
$$;

SELECT count(*) = 1 AS seed_rows_ok,
       count(*) FILTER (WHERE f_sale_categories = id % 7) = 1 AS seed_values_ok
  FROM t_balance_out;

SELECT reltuples = 1 AS seed_stats_ok
  FROM fasttrun_relstats('t_balance_out');

-- Вторая итерация должна и завершиться, и увидеть свежий план.  На PGPro
-- baseline зависает здесь со stale nested-loop SPI plan; timeout нужен ещё
-- и для того, чтобы installcheck не висел бесконечно.
SET statement_timeout = '10s';

DO $$
BEGIN
    PERFORM f_outer(200000, 200000);
EXCEPTION
    WHEN query_canceled THEN
        RAISE EXCEPTION
            'stale SPI cached plan after fasttruncate: f_outer(200000,200000) hit statement_timeout';
END
$$;

RESET statement_timeout;

SELECT count(*) = 200000 AS second_rows_ok,
       count(*) FILTER (WHERE f_sale_categories = id % 7) = 200000 AS second_values_ok
  FROM t_balance_out;

SELECT reltuples = 200000 AS second_stats_ok
  FROM fasttrun_relstats('t_balance_out');

-- ----------------------------------------------------------------------
-- 2. Путь без fasttruncate: generic cached plan, созданный до
--    fasttrun_analyze, должен инвалидироваться самим вызовом, который
--    публикует статистику.  Это покрывает вызывающих, которые не используют
--    fasttruncate в том же цикле, но всё равно заменяют ANALYZE на
--    fasttrun_analyze.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_analyze_cached_plan (id int, grp int);
INSERT INTO t_analyze_cached_plan
SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_analyze_cached_plan (grp);

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
PREPARE q_analyze_only AS
SELECT * FROM t_analyze_cached_plan WHERE grp = 50000;

-- Generic plan создан до fasttrun_analyze: defaults, cached stats ещё нет.
EXPLAIN (COSTS OFF) EXECUTE q_analyze_only;

SELECT fasttrun_analyze('t_analyze_cached_plan');

-- Тот же prepared statement должен перепланироваться и увидеть свежие stats.
EXPLAIN (COSTS OFF) EXECUTE q_analyze_only;
DEALLOCATE q_analyze_only;
COMMIT;

-- ----------------------------------------------------------------------
-- 3. Below-threshold DML без пересбора: generic plan, построенный после
--    мелкого (ниже порога) UPDATE, видит кешированную стату через soft
--    freshness и строится на ней (Index Scan по редкому grp), а не на
--    defaults.  Повторный fasttrun_analyze не инвалидирует план (дрейф ниже
--    порога), и он остаётся на стате.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_analyze_freshness_plan (id int, grp int);
INSERT INTO t_analyze_freshness_plan
SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_analyze_freshness_plan (grp);

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SELECT fasttrun_analyze('t_analyze_freshness_plan');

UPDATE t_analyze_freshness_plan SET id = id WHERE id < 10;
PREPARE q_freshness_only AS
SELECT * FROM t_analyze_freshness_plan WHERE grp = 50000;

-- План построен в окне soft freshness: churn ниже порога → стата видна.
EXPLAIN (COSTS OFF) EXECUTE q_freshness_only;

SELECT fasttrun_analyze('t_analyze_freshness_plan');

-- Below-threshold DML в пределах толерантности: стата остаётся видимой,
-- повторный EXPLAIN — тот же план на стате (Index Scan).
EXPLAIN (COSTS OFF) EXECUTE q_freshness_only;
DEALLOCATE q_freshness_only;
COMMIT;

-- ----------------------------------------------------------------------
-- 4. ROLLBACK TO SAVEPOINT должен инвалидировать generic plan, который
--    родился внутри откатанной подтранзакции и видел subxact-local stats.
--    SQL PREPARE живёт в session memory, поэтому сам rollback не обязан
--    уничтожить prepared statement; fasttrun должен сам послать локальную
--    invalidation при восстановлении stats cache/baseline.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_savepoint_cached_plan (id int, grp int);
INSERT INTO t_savepoint_cached_plan
SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_savepoint_cached_plan (grp);

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SELECT fasttrun_analyze('t_savepoint_cached_plan');

SAVEPOINT sp_plan;
UPDATE t_savepoint_cached_plan SET grp = 1;
SELECT fasttrun_analyze('t_savepoint_cached_plan');
PREPARE q_savepoint_rollback AS
SELECT * FROM t_savepoint_cached_plan WHERE grp = 1;

-- План создан внутри savepoint на распределении, которое будет откатано.
EXPLAIN (COSTS OFF) EXECUTE q_savepoint_rollback;
ROLLBACK TO SAVEPOINT sp_plan;

-- После rollback тот же prepared statement должен быть перепланирован.
EXPLAIN (COSTS OFF) EXECUTE q_savepoint_rollback;
DEALLOCATE q_savepoint_rollback;
COMMIT;

-- ----------------------------------------------------------------------
-- 5. Standalone fasttrun_collect_stats тоже публикует planner-visible
--    stats и должен инвалидировать generic plans, созданные до ручного
--    сбора статистики.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_collect_cached_plan (id int, grp int);
INSERT INTO t_collect_cached_plan
SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_collect_cached_plan (grp);

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SET LOCAL fasttrun.auto_collect_stats = off;
SELECT fasttrun_analyze('t_collect_cached_plan');
PREPARE q_collect_only AS
SELECT * FROM t_collect_cached_plan WHERE grp = 50000;

-- Generic plan создан до standalone collect_stats: column stats ещё нет.
EXPLAIN (COSTS OFF) EXECUTE q_collect_only;
SELECT fasttrun_collect_stats('t_collect_cached_plan');

-- collect_stats должен инвалидировать plan cache без помощи fasttruncate.
EXPLAIN (COSTS OFF) EXECUTE q_collect_only;
DEALLOCATE q_collect_only;
COMMIT;

-- ----------------------------------------------------------------------
-- 6. Sub-threshold DML НЕ инвалидирует generic plan, построенный на свежих
--    column stats.  Plan, собранный когда stats видимы (Index Scan на
--    high-cardinality grp), переживает мелкий (ниже stats_refresh_threshold)
--    DML + повторный fasttrun_analyze и остаётся Index Scan: мелкое смещение
--    распределения не стоит инвалидации и потери хорошего плана ради
--    fallback к default selectivity.  DML идёт по неиндексированному id (HOT),
--    чтобы index relstats не дрейфнули и порог invalidate сработал.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_subthreshold_keep_plan (id int, grp int);
INSERT INTO t_subthreshold_keep_plan SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_subthreshold_keep_plan (grp);

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SELECT fasttrun_analyze('t_subthreshold_keep_plan');

PREPARE q_keep AS
SELECT * FROM t_subthreshold_keep_plan WHERE grp = 50000;

-- Generic plan на свежих stats: Index Scan.
EXPLAIN (COSTS OFF) EXECUTE q_keep;

UPDATE t_subthreshold_keep_plan SET id = id WHERE id <= 50;  -- мелкий churn ниже порога
SELECT fasttrun_analyze('t_subthreshold_keep_plan');

-- Plan не инвалидирован: остаётся Index Scan, а не fallback Bitmap/Seq.
EXPLAIN (COSTS OFF) EXECUTE q_keep;
DEALLOCATE q_keep;
COMMIT;

DROP TABLE t_subthreshold_keep_plan;

-- ----------------------------------------------------------------------
-- 7. ROLLBACK TO SAVEPOINT инвалидирует локальный plan cache ровно один
--    раз на relid, а не по разу на каждую откатываемую колонку стата.
--    Вызовы считаем по DEBUG-строке из
--    fasttrun_invalidate_local_plan_cache; SET внутри subxact ещё
--    действует в момент колбэка (GUC-откат в AbortSubTransaction идёт
--    после subxact-колбэков) и сам снимается откатом.
-- ----------------------------------------------------------------------
RESET fasttrun.stats_refresh_threshold;
RESET fasttrun.invalidate_threshold;
CREATE TEMP TABLE t_abort_inval_once (c1 int, c2 int, c3 int, c4 int,
                                      c5 int, c6 int, c7 int, c8 int);
INSERT INTO t_abort_inval_once
SELECT g, g, g, g, g, g, g, g FROM generate_series(1, 500) g;

BEGIN;
SELECT fasttrun_analyze('t_abort_inval_once');
SAVEPOINT sp_inval;
INSERT INTO t_abort_inval_once
SELECT g, g, g, g, g, g, g, g FROM generate_series(501, 1000) g;
SELECT fasttrun_analyze('t_abort_inval_once');
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_inval;
SELECT reltuples = 500 AS abort_restored
  FROM fasttrun_relstats('t_abort_inval_once');
COMMIT;

DROP TABLE t_abort_inval_once;

-- ----------------------------------------------------------------------
-- 8. DISCARD TEMP роняет все temp-таблицы внутренним сбросом неймспейса
--    (не per-table DropStmt); fasttrun эвиктит оба кэша. Cached-план на
--    temp-таблице с тем же именем, пересозданной после DISCARD, обязан
--    отражать СТАТИСТИКУ НОВОЙ таблицы, а не устаревший план до DISCARD.
--    (DISCARD ALL спасён ядерным ResetPlanCache; DISCARD TEMP полагается
--    на ядерную инвалидацию плана при дропе старой таблицы.)
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_disc_plan (id int, grp int);
CREATE INDEX ON t_disc_plan (grp);
-- v1: низкая кардинальность grp (5 значений) -> grp=1 ~1000 строк.
INSERT INTO t_disc_plan SELECT g, g % 5 FROM generate_series(1, 5000) g;
SELECT fasttrun_analyze('t_disc_plan');
PREPARE dp AS SELECT * FROM t_disc_plan WHERE grp = 1;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE dp LOOP
    IF ln ~ 'rows=' THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  IF est IS NULL OR est < 500 THEN
    RAISE EXCEPTION 'baseline low-card estimate unexpectedly small: %', est;
  END IF;
END$$;
SELECT 'disc_plan_baseline_ok' AS marker;

DISCARD TEMP;

-- v2 то же имя: высокая кардинальность grp (уникальна) -> grp=1 ~1 строка.
CREATE TEMP TABLE t_disc_plan (id int, grp int);
CREATE INDEX ON t_disc_plan (grp);
INSERT INTO t_disc_plan SELECT g, g FROM generate_series(1, 5000) g;
SELECT fasttrun_analyze('t_disc_plan');
-- Старый prepared dp обязан переплаироваться под НОВУЮ таблицу: оценка
-- grp=1 должна коллапсировать к ~1, а не остаться stale ~1000.
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE dp LOOP
    IF ln ~ 'rows=' THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  IF est IS NULL OR est > 50 THEN
    RAISE EXCEPTION 'stale plan after DISCARD TEMP recreate: grp=1 est=%', est;
  END IF;
END$$;
SELECT 'disc_plan_fresh_ok' AS marker;

DEALLOCATE dp;
DROP TABLE t_disc_plan;

-- ----------------------------------------------------------------------
-- 9. Кумулятивный дрейф sub-threshold шагов инвалидирует план от якоря
--    последней инвалидации, а не от предыдущего вызова.  sample_rows=0
--    отключает пересбор column stats (он форсирует инвалидацию мимо
--    порога), таблица без индексов — index-сигнал не участвует.  Каждый
--    шаг +15% строк: ниже порога 0.2 относительно предыдущего вызова,
--    но два шага дают ~30% от последней инвалидации — второй analyze
--    обязан инвалидировать план.  Считаем по DEBUG-строке из
--    fasttrun_invalidate_local_plan_cache.
-- ----------------------------------------------------------------------
SET fasttrun.sample_rows = 0;
CREATE TEMP TABLE t_cum_drift (id int, pad text);
INSERT INTO t_cum_drift SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SELECT fasttrun_analyze('t_cum_drift');
PREPARE q_cum AS SELECT count(*) FROM t_cum_drift;
EXECUTE q_cum;

-- Шаг 1: +15% — ниже порога и от якоря; инвалидации нет.
INSERT INTO t_cum_drift SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
SET client_min_messages = debug1;
SELECT fasttrun_analyze('t_cum_drift');
RESET client_min_messages;

-- Шаг 2: ещё +3000 — от предыдущего вызова ~13%, от якоря ~30%.
INSERT INTO t_cum_drift SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
SET client_min_messages = debug1;
SELECT fasttrun_analyze('t_cum_drift');
RESET client_min_messages;

DEALLOCATE q_cum;
COMMIT;
DROP TABLE t_cum_drift;
RESET fasttrun.sample_rows;

-- ----------------------------------------------------------------------
-- 10. Якорь порога сбрасывается при откате savepoint: инвалидация из
--     ABORT_SUB рушит планы, но якорь last_inval_* без сброса остался бы
--     от отменённого analyze — и подавлял бы следующую инвалидацию, пока
--     дрейф от stale-якоря не пробьёт порог.  sample_rows=0 и таблица
--     без индексов, как в секции 9.
-- ----------------------------------------------------------------------
SET fasttrun.sample_rows = 0;
CREATE TEMP TABLE t_abort_anchor (id int, pad text);
INSERT INTO t_abort_anchor SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;

BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
SELECT fasttrun_analyze('t_abort_anchor');
PREPARE q_anchor AS SELECT count(*) FROM t_abort_anchor;
EXECUTE q_anchor;
SAVEPOINT sp_anchor;
INSERT INTO t_abort_anchor SELECT g, repeat('x', 100) FROM generate_series(1, 6000) g;
SELECT fasttrun_analyze('t_abort_anchor');
ROLLBACK TO SAVEPOINT sp_anchor;
-- +25% от базы (5000/20000), но лишь ~4% от stale-якоря отменённого analyze:
-- инвалидация обязана произойти.
INSERT INTO t_abort_anchor SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g;
SET client_min_messages = debug1;
SELECT fasttrun_analyze('t_abort_anchor');
RESET client_min_messages;
DEALLOCATE q_anchor;
COMMIT;

DROP TABLE t_abort_anchor;
RESET fasttrun.sample_rows;

DROP FUNCTION f_outer(int, int);
-- ----------------------------------------------------------------------
-- 11. ROLLBACK TO SAVEPOINT после fasttruncate сбрасывает локальный
--     кеш планов один раз для основной таблицы, а не для каждого индекса.
--     Одна корневая запись журнала объединяет таблицу, индексы и TOAST;
--     в DEBUG появляется одна строка.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_inval_dedup (a int, b int, c text);
CREATE INDEX ON t_inval_dedup (a);
CREATE INDEX ON t_inval_dedup USING hash (b);
CREATE INDEX ON t_inval_dedup USING gin (to_tsvector('simple', c));
INSERT INTO t_inval_dedup SELECT g, g % 10, 'x' || g FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_inval_dedup');

BEGIN;
SAVEPOINT sp_dedup;
SELECT fasttruncate('t_inval_dedup');
-- Окно наблюдения: только ROLLBACK эмитит abort-инвалидации (fasttruncate
-- выше уже отработал на уровне WARNING). Ожидаем одну DEBUG-строку.
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_dedup;
RESET client_min_messages;
COMMIT;

DROP TABLE t_inval_dedup;

-- ----------------------------------------------------------------------
-- 12. Смена видимости column stats инвалидирует generic plan.  Near-unique
--     колонка получает cardinality-scaled freshness-порог (пол 5%), а
--     пересбор статы гейтится плоским stats_refresh_threshold (20%):
--     churn в полосе между ними прячет стату от новых планов без
--     пересбора.  Generic план, построенный на видимой стате (grp=$1 ->
--     оценка ~1 строка), обязан инвалидироваться тем же fasttrun_analyze,
--     который наблюдает флип visible->hidden, иначе живёт на stale rows=1
--     при факте 10000.  Всё в одной транзакции: analyze в новой xact идёт
--     cold-recollect'ом и инвалидирует безусловно.  Считаем по
--     DEBUG-строке из fasttrun_invalidate_local_plan_cache.
-- ----------------------------------------------------------------------
BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
CREATE TEMP TABLE t_vis_flip (id int, grp int) WITH (fillfactor = 70);
INSERT INTO t_vis_flip SELECT g, g FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_vis_flip');

PREPARE q_flip(int) AS SELECT count(*) FROM t_vis_flip WHERE grp = $1;
EXECUTE q_flip(1);

-- Generic план на видимой стате: near-unique grp -> оценка ~1 строка.
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_flip(1) LOOP
    IF ln ~ 'on t_vis_flip' THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  IF est IS NULL OR est > 50 THEN
    RAISE EXCEPTION 'baseline generic plan not built on cached stats: est=%', est;
  END IF;
END$$;
SELECT 'vis_flip_baseline_ok' AS marker;

-- 10% churn: выше 5%-пола near-unique колонки (стата прячется от новых
-- планов), ниже 20%-порога пересбора.
UPDATE t_vis_flip SET grp = 1 WHERE id <= 10000;

SET client_min_messages = debug1;
SELECT fasttrun_analyze('t_vis_flip');
RESET client_min_messages;

-- План обязан перепланироваться: оценка коллапсирует к defaults (~500),
-- а не остаётся stale rows=1 от спрятанной статы.
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_flip(1) LOOP
    IF ln ~ 'on t_vis_flip' THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  IF est IS NULL OR est < 100 THEN
    RAISE EXCEPTION 'stale generic plan after visibility flip: est=%', est;
  END IF;
END$$;
SELECT 'vis_flip_replanned_ok' AS marker;

DEALLOCATE q_flip;
COMMIT;
DROP TABLE t_vis_flip;

-- ----------------------------------------------------------------------
-- 13. При track_counts=off явный сбор сбрасывает кеш планов только тогда,
--     когда планировщик перестаёт видеть прежнюю статистику. Из четырёх
--     переходов DEBUG должен показать только переходы от каталожной и от
--     свежей локальной статистики к скрытому состоянию.
-- ----------------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE t_tc_absent (id int);
SET LOCAL track_counts = off;
SET client_min_messages = debug1;
SELECT fasttrun_collect_stats('t_tc_absent');
RESET client_min_messages;

CREATE TEMP TABLE t_tc_unknown (id int);
INSERT INTO t_tc_unknown SELECT generate_series(1,1000);
ANALYZE t_tc_unknown;
SET LOCAL track_counts = off;
SET client_min_messages = debug1;
SELECT fasttrun_collect_stats('t_tc_unknown');
RESET client_min_messages;

SET LOCAL track_counts = on;
CREATE TEMP TABLE t_tc_visible (id int);
INSERT INTO t_tc_visible SELECT generate_series(1,1000);
SELECT fasttrun_collect_stats('t_tc_visible');
SET LOCAL track_counts = off;
SET client_min_messages = debug1;
SELECT fasttrun_collect_stats('t_tc_visible');
RESET client_min_messages;

SET client_min_messages = debug1;
SELECT fasttrun_collect_stats('t_tc_visible');
RESET client_min_messages;
COMMIT;
DROP TABLE t_tc_absent;
DROP TABLE t_tc_unknown;
DROP TABLE t_tc_visible;

-- ----------------------------------------------------------------------
-- 14. DML, изменяющий данные CTE и COPY FROM должны записать таблицу в
--     журнал даже без вызова функций fasttrun. COMMIT скрывает устаревшую
--     статистику и один раз сбрасывает подготовленный план, не просматривая
--     весь кеш.
-- ----------------------------------------------------------------------
SET plan_cache_mode = force_generic_plan;
BEGIN;
CREATE TEMP TABLE t_commit_flip (id int, grp int);
INSERT INTO t_commit_flip SELECT g, g FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_commit_flip');
PREPARE q_commit_flip(int) AS
SELECT count(*) FROM t_commit_flip WHERE grp = $1;
EXECUTE q_commit_flip(1);
COMMIT;

BEGIN;
UPDATE t_commit_flip SET grp = 1 WHERE id <= 10000;
SET client_min_messages = debug1;
COMMIT;
RESET client_min_messages;

DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_commit_flip(1) LOOP
    IF ln ~ 'on t_commit_flip' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 100 THEN
    RAISE EXCEPTION 'COMMIT after DML kept a plan based on stale statistics: est=%', est;
  END IF;
END$$;
SELECT 'dml_commit_flip_replanned' AS marker;
DEALLOCATE q_commit_flip;

SELECT fasttrun_analyze('t_commit_flip');
PREPARE q_commit_cte(int) AS
SELECT count(*) FROM t_commit_flip WHERE id = $1;
EXECUTE q_commit_cte(20000);

BEGIN;
SET client_min_messages = debug1;
WITH changed AS (
  UPDATE t_commit_flip SET grp = 2 WHERE id BETWEEN 10001 AND 20000
  RETURNING id
)
SELECT count(*) FROM changed;
COMMIT;
RESET client_min_messages;

DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_commit_cte(20000) LOOP
    IF ln ~ 'on t_commit_flip' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 100 THEN
    RAISE EXCEPTION 'modifying CTE kept stale generic plan: est=%', est;
  END IF;
END$$;
SELECT 'modifying_cte_replanned' AS marker;
DEALLOCATE q_commit_cte;

SET fasttrun.stats_refresh_threshold = 0.001;
BEGIN;
CREATE TEMP TABLE t_commit_copy (id int, grp int);
INSERT INTO t_commit_copy SELECT g, g FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_commit_copy');
PREPARE q_commit_copy(int) AS
SELECT count(*) FROM t_commit_copy WHERE id = $1;
EXECUTE q_commit_copy(1);
COMMIT;

BEGIN;
COPY t_commit_copy (id, grp) FROM STDIN;
1	1
1	1
1	1
1	1
1	1
\.
SET client_min_messages = debug1;
COMMIT;
RESET client_min_messages;

DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_commit_copy(1) LOOP
    IF ln ~ 'on t_commit_copy' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 2 THEN
    RAISE EXCEPTION 'COPY FROM commit kept stale generic plan: est=%', est;
  END IF;
END$$;
SELECT 'copy_from_replanned' AS marker;
DEALLOCATE q_commit_copy;
RESET fasttrun.stats_refresh_threshold;

BEGIN;
CREATE TEMP TABLE t_commit_on_delete (id int) ON COMMIT DELETE ROWS;
INSERT INTO t_commit_on_delete SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('t_commit_on_delete');
SET client_min_messages = debug1;
COMMIT;
RESET client_min_messages;
SELECT count(*) = 0 AS on_commit_delete_empty FROM t_commit_on_delete;

RESET plan_cache_mode;
DROP TABLE t_commit_on_delete;
DROP TABLE t_commit_copy;
DROP TABLE t_commit_flip;

-- ----------------------------------------------------------------------
-- 15. Откат подтранзакции после no-op fasttrun_analyze (без нового DML)
--     не инвалидирует локальный кеш планов: ничего planner-visible не
--     изменилось.  EXCEPTION-цикл PL/pgSQL — каждая итерация свой subxact.
--     Контроль в той же транзакции: итерация с реальным DML перед analyze
--     инвалидирует на откате (плюс одна инлайн-инвалидация от публикации
--     свежей статы самим analyze).  Считаем по DEBUG-строке из
--     fasttrun_invalidate_local_plan_cache.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_noop_abort (id int, grp int);
INSERT INTO t_noop_abort SELECT g, g FROM generate_series(1, 1000) g;

BEGIN;
SELECT fasttrun_analyze('t_noop_abort');
SET client_min_messages = debug1;
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..3 LOOP
    BEGIN
      PERFORM fasttrun_analyze('t_noop_abort');
      RAISE EXCEPTION 'force noop rollback';
    EXCEPTION WHEN raise_exception THEN
      NULL;
    END;
  END LOOP;
END$$;
RESET client_min_messages;
SELECT 'noop_abort_no_inval_ok' AS marker;

SET client_min_messages = debug1;
DO $$
BEGIN
  BEGIN
    INSERT INTO t_noop_abort SELECT g, g FROM generate_series(1001, 2000) g;
    PERFORM fasttrun_analyze('t_noop_abort');
    RAISE EXCEPTION 'force dml rollback';
  EXCEPTION WHEN raise_exception THEN
    NULL;
  END;
END$$;
RESET client_min_messages;
SELECT 'dml_abort_inval_ok' AS marker;
COMMIT;
DROP TABLE t_noop_abort;

-- ----------------------------------------------------------------------
-- 16. Ядерная статистика выражения тоже влияет на смену режима.  У обычных
--     колонок цель сбора = 0, поэтому ANALYZE оставляет единственную строку
--     pg_statistic под OID индекса выражения.  Подготовленный план сначала
--     видит частое значение (~9000 строк), затем fasttrun_analyze и
--     fasttrun_collect_stats без локального сбора скрывают эту статистику.
--     Сохранённый план обязан перестроиться на стандартную оценку (~50).
-- ----------------------------------------------------------------------
SET plan_cache_mode = force_generic_plan;
SET fasttrun.auto_collect_stats = off;
SET fasttrun.sample_rows = 0;

CREATE TEMP TABLE t_expr_policy_analyze (id int, name text);
ALTER TABLE t_expr_policy_analyze ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE t_expr_policy_analyze ALTER COLUMN name SET STATISTICS 0;
INSERT INTO t_expr_policy_analyze
SELECT g, CASE WHEN g <= 9000 THEN 'dup' ELSE 'v_' || g END
FROM generate_series(1, 10000) g;
CREATE INDEX t_expr_policy_analyze_idx ON t_expr_policy_analyze (lower(name));
ANALYZE t_expr_policy_analyze;
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_statistic
             WHERE starelid = 't_expr_policy_analyze'::regclass) OR
     NOT EXISTS (SELECT FROM pg_statistic
                 WHERE starelid = 't_expr_policy_analyze_idx'::regclass) THEN
    RAISE EXCEPTION 'expression-only ANALYZE fixture has wrong catalog stats';
  END IF;
END$$;
PREPARE q_expr_policy_analyze AS
SELECT count(*) FROM t_expr_policy_analyze WHERE lower(name) = 'dup';
EXECUTE q_expr_policy_analyze;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_expr_policy_analyze LOOP
    IF ln ~ 'on t_expr_policy_analyze' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 8000 THEN
    RAISE EXCEPTION 'expression-only baseline ignored core stats: est=%', est;
  END IF;
END$$;
SELECT fasttrun_analyze('t_expr_policy_analyze');
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_expr_policy_analyze LOOP
    IF ln ~ 'on t_expr_policy_analyze' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 OR est > 100 THEN
    RAISE EXCEPTION 'analyze kept expression-only generic plan: est=%', est;
  END IF;
END$$;
SELECT 'expression_policy_analyze_replanned' AS marker;
DEALLOCATE q_expr_policy_analyze;
DROP TABLE t_expr_policy_analyze;

CREATE TEMP TABLE t_expr_policy_collect (id int, name text);
ALTER TABLE t_expr_policy_collect ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE t_expr_policy_collect ALTER COLUMN name SET STATISTICS 0;
INSERT INTO t_expr_policy_collect
SELECT g, CASE WHEN g <= 9000 THEN 'dup' ELSE 'v_' || g END
FROM generate_series(1, 10000) g;
CREATE INDEX t_expr_policy_collect_idx ON t_expr_policy_collect (lower(name));
ANALYZE t_expr_policy_collect;
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_statistic
             WHERE starelid = 't_expr_policy_collect'::regclass) OR
     NOT EXISTS (SELECT FROM pg_statistic
                 WHERE starelid = 't_expr_policy_collect_idx'::regclass) THEN
    RAISE EXCEPTION 'expression-only ANALYZE fixture has wrong catalog stats';
  END IF;
END$$;
PREPARE q_expr_policy_collect AS
SELECT count(*) FROM t_expr_policy_collect WHERE lower(name) = 'dup';
EXECUTE q_expr_policy_collect;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_expr_policy_collect LOOP
    IF ln ~ 'on t_expr_policy_collect' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 8000 THEN
    RAISE EXCEPTION 'expression-only baseline ignored core stats: est=%', est;
  END IF;
END$$;
SELECT fasttrun_collect_stats('t_expr_policy_collect');
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_expr_policy_collect LOOP
    IF ln ~ 'on t_expr_policy_collect' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 OR est > 100 THEN
    RAISE EXCEPTION 'collect kept expression-only generic plan: est=%', est;
  END IF;
END$$;
SELECT 'expression_policy_collect_replanned' AS marker;
DEALLOCATE q_expr_policy_collect;
DROP TABLE t_expr_policy_collect;

RESET fasttrun.sample_rows;
RESET fasttrun.auto_collect_stats;
RESET plan_cache_mode;

-- ----------------------------------------------------------------------
-- 17. План, построенный на статистике, временно скрытой из-за DML, не
--     должен пережить откат DML. Проверяем откат точки сохранения после
--     UPDATE и откат всей транзакции после COPY FROM. До отката общий план
--     видит стандартную оценку (~50 строк), после — почти уникальную
--     статистику (~1 строка). DML без планирования не сбрасывает планы.
-- ----------------------------------------------------------------------
SET plan_cache_mode = force_generic_plan;
SET fasttrun.stats_refresh_threshold = 0.001;
BEGIN;
CREATE TEMP TABLE t_dml_plan_abort (id int, grp int);
INSERT INTO t_dml_plan_abort SELECT g, g FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('t_dml_plan_abort');
COMMIT;

BEGIN;
SAVEPOINT sp_dml_plan;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id <= 20;
PREPARE q_dml_savepoint(int) AS
SELECT count(*) FROM t_dml_plan_abort WHERE grp = $1;
EXECUTE q_dml_savepoint(1);
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_dml_savepoint(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 THEN
    RAISE EXCEPTION 'savepoint fixture did not hide candidate stats: est=%', est;
  END IF;
END$$;
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_dml_plan;
RESET client_min_messages;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_dml_savepoint(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est > 2 THEN
    RAISE EXCEPTION 'savepoint rollback kept DML-hidden generic plan: est=%', est;
  END IF;
END$$;
SELECT 'dml_savepoint_plan_replanned' AS marker;
DEALLOCATE q_dml_savepoint;
COMMIT;

BEGIN;
COPY t_dml_plan_abort (id, grp) FROM STDIN;
10001	1
10002	1
10003	1
10004	1
10005	1
10006	1
10007	1
10008	1
10009	1
10010	1
10011	1
10012	1
10013	1
10014	1
10015	1
10016	1
10017	1
10018	1
10019	1
10020	1
\.
PREPARE q_dml_top(int) AS
SELECT count(*) FROM t_dml_plan_abort WHERE grp = $1;
EXECUTE q_dml_top(1);
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_dml_top(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 THEN
    RAISE EXCEPTION 'top-level fixture did not hide candidate stats: est=%', est;
  END IF;
END$$;
SET client_min_messages = debug1;
ROLLBACK;
RESET client_min_messages;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_dml_top(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est > 2 THEN
    RAISE EXCEPTION 'top-level rollback kept DML-hidden generic plan: est=%', est;
  END IF;
END$$;
SELECT 'dml_top_plan_replanned' AS marker;
DEALLOCATE q_dml_top;

BEGIN;
SAVEPOINT sp_dml_no_plan;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id <= 20;
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_dml_no_plan;
RESET client_min_messages;
COMMIT;
SELECT 'dml_without_plan_no_inval' AS marker;

-- При выключенных счётчиках статистика скрыта независимо от отката DML.
-- План строится на стандартной оценке, но сбрасывать его при откате нельзя.
SET track_counts = off;
BEGIN;
SAVEPOINT sp_tc_off;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id <= 20;
PREPARE q_tc_off_savepoint(int) AS
SELECT count(*) FROM t_dml_plan_abort WHERE grp = $1;
EXECUTE q_tc_off_savepoint(1);
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_tc_off_savepoint(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 THEN
    RAISE EXCEPTION 'track_counts=off plan did not use defaults: est=%', est;
  END IF;
END$$;
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_tc_off;
RESET client_min_messages;
DEALLOCATE q_tc_off_savepoint;
COMMIT;
SELECT 'track_counts_off_savepoint_no_inval' AS marker;

BEGIN;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id <= 20;
PREPARE q_tc_off_top(int) AS
SELECT count(*) FROM t_dml_plan_abort WHERE grp = $1;
EXECUTE q_tc_off_top(1);
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_tc_off_top(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 THEN
    RAISE EXCEPTION 'track_counts=off top plan did not use defaults: est=%', est;
  END IF;
END$$;
SET client_min_messages = debug1;
ROLLBACK;
RESET client_min_messages;
DEALLOCATE q_tc_off_top;
SELECT 'track_counts_off_top_no_inval' AS marker;
SET track_counts = on;

-- Только ближайший DML-уровень мог вернуть видимость при своём откате.
-- После его отката общий план перестраивается; откат родителя уже ничего
-- не меняет и не должен повторно сбрасывать планы.
SET fasttrun.stats_refresh_threshold = 0.01;
BEGIN;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id <= 50;
SAVEPOINT sp_nearest_dml;
UPDATE t_dml_plan_abort SET grp = 1 WHERE id > 50 AND id <= 150;
PREPARE q_nearest_dml(int) AS
SELECT count(*) FROM t_dml_plan_abort WHERE grp = $1;
EXECUTE q_nearest_dml(1);
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_nearest_dml(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est < 20 THEN
    RAISE EXCEPTION 'child DML did not hide statistics: est=%', est;
  END IF;
END$$;
SET client_min_messages = debug1;
ROLLBACK TO SAVEPOINT sp_nearest_dml;
RESET client_min_messages;
DO $$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXPLAIN EXECUTE q_nearest_dml(1) LOOP
    IF ln ~ 'on t_dml_plan_abort' THEN
      est := substring(ln FROM 'rows=(\d+)')::int;
      EXIT;
    END IF;
  END LOOP;
  IF est IS NULL OR est > 2 THEN
    RAISE EXCEPTION 'child rollback kept hidden plan: est=%', est;
  END IF;
END$$;
SELECT 'nearest_dml_child_replanned' AS marker;
SET client_min_messages = debug1;
ROLLBACK;
RESET client_min_messages;
DEALLOCATE q_nearest_dml;
SELECT 'nearest_dml_parent_no_inval' AS marker;

RESET fasttrun.stats_refresh_threshold;
RESET plan_cache_mode;
DROP TABLE t_dml_plan_abort;

DROP FUNCTION f_inner();
DROP TABLE t_rsd_details;
DROP TABLE t_balance_out;
DROP TABLE t_analyze_cached_plan;
DROP TABLE t_analyze_freshness_plan;
DROP TABLE t_savepoint_cached_plan;
DROP TABLE t_collect_cached_plan;
DROP EXTENSION fasttrun;
