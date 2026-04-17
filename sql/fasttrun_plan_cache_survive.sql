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

DROP FUNCTION f_outer(int, int);
DROP FUNCTION f_inner();
DROP TABLE t_rsd_details;
DROP TABLE t_balance_out;
DROP EXTENSION fasttrun;
