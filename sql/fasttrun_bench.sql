--
-- fasttrun_bench — синтетический бенчмарк горячего пути.
--
-- Покрывает реалистичный продакшн-сценарий:
--   * широкая временная таблица (50 колонок разных типов);
--   * 1 миллион строк;
--   * самый первый fasttrun_analyze (единственный полный проход);
--   * последующие analyze без изменений данных (дельта = 0);
--   * DELETE половины + повторный analyze (дельта удалений = 500k);
--   * UPDATE всех + повторный analyze (дельта обновлений ≠ 0, но
--     в формуле не участвует — число живых строк не меняется);
--   * горячий путь маленького DML: DELETE 1000, INSERT 500, смешанный;
--   * fasttruncate в финале.
--
-- Все замеры ленивого режима выполняются ВНУТРИ одного DO-блока,
-- иначе неявная транзакция между PERFORM-вызовами закрывается,
-- callback границы транзакции стирает хэш-таблицу, и опорный снимок
-- дельты сбрасывается.
--
-- Жёсткие пороги внутри DO-блока (срабатывают только при регрессии):
--   * тёплый analyze (попадание через дельту) не медленнее 5 мс;
--   * пересбор через блочную выборку после массового DML не
--     медленнее 500 мс;
--   * ускорение тёплый/холодный не менее 20x;
--   * fasttruncate миллионной таблицы не медленнее 100 мс.
-- Эти пороги намеренно консервативны: на любом современном железе
-- (включая медленные CI-раннеры) они выполняются с большим запасом
-- — обычные локальные значения: ускорение 1000x+, доли мс на тёплый
-- вызов, ~30-80 мс на пересбор.  При срабатывании в исключение
-- пишется фактическая цифра, чтобы можно было быстро понять причину.
--
-- Вывод NOTICE с фактическими таймингами подавлен через
-- client_min_messages=WARNING, иначе diff с expected ломался бы на
-- любом железе.  Если хочется увидеть фактические числа из RAISE
-- NOTICE — закомментируйте строку SET ниже и запустите файл руками.
--

SET client_min_messages = WARNING;

CREATE EXTENSION fasttrun;

-- ----------------------------------------------------------------------
-- Широкая временная таблица: ровно 50 колонок (id + 49) разных типов,
-- характерных для продакшн-использования: int, bigint, text, varchar,
-- numeric, timestamptz, date, boolean.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_bench (
    id      bigint,

    -- 8 x int
    i01 int, i02 int, i03 int, i04 int,
    i05 int, i06 int, i07 int, i08 int,

    -- 8 x bigint
    b01 bigint, b02 bigint, b03 bigint, b04 bigint,
    b05 bigint, b06 bigint, b07 bigint, b08 bigint,

    -- 10 x text
    t01 text, t02 text, t03 text, t04 text, t05 text,
    t06 text, t07 text, t08 text, t09 text, t10 text,

    -- 4 x varchar(50)
    v01 varchar(50), v02 varchar(50),
    v03 varchar(50), v04 varchar(50),

    -- 5 x numeric(20,5)
    n01 numeric(20,5), n02 numeric(20,5), n03 numeric(20,5),
    n04 numeric(20,5), n05 numeric(20,5),

    -- 5 x timestamptz
    ts01 timestamptz, ts02 timestamptz, ts03 timestamptz,
    ts04 timestamptz, ts05 timestamptz,

    -- 4 x date
    d01 date, d02 date, d03 date, d04 date,

    -- 5 x boolean
    bo01 boolean, bo02 boolean, bo03 boolean, bo04 boolean, bo05 boolean
);

-- ----------------------------------------------------------------------
-- 1 миллион строк.  Один INSERT ... SELECT generate_series — самый
-- быстрый способ заполнить таблицу.  Выражения подобраны так, чтобы
-- каждая строка была реалистично "грязной" (разные значения в каждой
-- колонке), но без дорогих per-row функций вроде to_timestamp().
-- ----------------------------------------------------------------------
INSERT INTO t_bench
SELECT
    g,
    g,         g + 1,     g + 2,     g + 3,
    g + 4,     g + 5,     g + 6,     g + 7,
    g::bigint * 2,  g::bigint * 3,  g::bigint * 5,  g::bigint * 7,
    g::bigint * 11, g::bigint * 13, g::bigint * 17, g::bigint * 19,
    'a' || g, 'b' || g, 'c' || g, 'd' || g, 'e' || g,
    'f' || g, 'g' || g, 'h' || g, 'i' || g, 'j' || g,
    'va' || g, 'vb' || g, 'vc' || g, 'vd' || g,
    g::numeric / 7,  g::numeric / 11, g::numeric / 13,
    g::numeric / 17, g::numeric / 19,
    '2026-01-01 00:00:00+00'::timestamptz,
    '2026-01-02 00:00:00+00'::timestamptz,
    '2026-01-03 00:00:00+00'::timestamptz,
    '2026-01-04 00:00:00+00'::timestamptz,
    '2026-01-05 00:00:00+00'::timestamptz,
    '2026-01-01'::date, '2026-02-01'::date,
    '2026-03-01'::date, '2026-04-01'::date,
    g % 2 = 0, g % 3 = 0, g % 5 = 0,
    g % 7 = 0, g % 11 = 0
FROM generate_series(1, 1000000) g;

-- ----------------------------------------------------------------------
-- Сам бенчмарк.  Всё внутри одного DO-блока, потому что кэш ленивого
-- режима живёт внутри одной транзакции и сбрасывается callback'ом
-- границы транзакции на её закрытии.
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start         timestamptz;
    t_cold          numeric;
    t_warm          numeric;
    t_warm2         numeric;
    t_after_d       numeric;
    t_after_d_warm  numeric;
    t_after_u       numeric;
    t_after_u_warm  numeric;
    t_small_d       numeric;
    t_small_i       numeric;
    t_small_mix     numeric;
    t_trunc         numeric;
    n_pages         int;
    n_tuples        real;
    speedup         numeric;
BEGIN
    -- ----------------------------------------------------------
    -- 1. Холодный analyze: хэш-таблица пустая, должен пройти
    --    полное последовательное сканирование миллиона строк.
    -- ----------------------------------------------------------
    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_cold := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT relpages, reltuples
      INTO n_pages, n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_tuples <> 1000000 THEN
        RAISE EXCEPTION 'cold analyze published wrong row count: % (expected 1000000)',
                        n_tuples;
    END IF;

    IF n_pages <= 0 THEN
        RAISE EXCEPTION 'cold analyze published zero pages on a 1M-row table';
    END IF;

    -- ----------------------------------------------------------
    -- 2-3. Дельта-вызовы без изменений данных:
    --      дельта вставок = дельта удалений = 0, new_tuples =
    --      cached_tuples → результат тот же, без сканирования.
    --      Это горячий путь «много analyze подряд между DML».
    -- ----------------------------------------------------------
    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_warm := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_warm2 := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    speedup := t_cold / GREATEST(t_warm, 0.001);

    RAISE NOTICE 'fasttrun_analyze: cold=% ms, warm=% ms, warm2=% ms, speedup ~%x',
                 round(t_cold, 2), round(t_warm, 4),
                 round(t_warm2, 4), round(speedup, 0);

    IF t_warm > 5 THEN
        RAISE EXCEPTION 'lazy-mode delta hit too slow: warm=% ms (expected <5)',
                        round(t_warm, 4);
    END IF;

    IF t_warm * 20 > t_cold THEN
        RAISE EXCEPTION 'lazy-mode speedup insufficient: cold=% ms, warm=% ms',
                        round(t_cold, 2), round(t_warm, 4);
    END IF;

    -- ----------------------------------------------------------
    -- DELETE половины (500k) — доля изменений = 1.0 > порог 0.2
    -- по умолчанию, поэтому первый analyze запускает пересбор
    -- статистики через блочную выборку (дельта-математика обновляет
    -- число строк, блочная выборка обновляет n_distinct / null_frac
    -- / width).  Второй analyze идёт чистым попаданием в кэш через
    -- дельту, без пересбора (доля изменений = 0 после продвижения
    -- опорного снимка).
    -- ----------------------------------------------------------
    DELETE FROM t_bench WHERE id % 2 = 0;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_after_d := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples
      INTO n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_tuples <> 500000 THEN
        RAISE EXCEPTION 'after-DELETE 500k analyze wrong count: % (expected 500000)',
                        n_tuples;
    END IF;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_after_d_warm := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'after DELETE 500k: 1st=% ms (block-sample refresh), 2nd=% ms (pure delta)',
                 round(t_after_d, 4), round(t_after_d_warm, 4);

    /* Пересбор через блочную выборку на 1M строк × 50 колонок:
     * ~30-80 мс это нормально, 500 мс — тревожный порог регрессии */
    IF t_after_d > 500 THEN
        RAISE EXCEPTION 'block-sample refresh after DELETE 500k too slow: % ms',
                        round(t_after_d, 4);
    END IF;
    IF t_after_d_warm > 5 THEN
        RAISE EXCEPTION 'pure delta after DELETE 500k too slow: % ms',
                        round(t_after_d_warm, 4);
    END IF;

    -- ----------------------------------------------------------
    -- UPDATE всех оставшихся 500k — доля изменений = 500k > 20%
    -- порога: первый analyze → пересбор через блочную выборку,
    -- второй → попадание в кэш через дельту без сканирования.
    -- ----------------------------------------------------------
    UPDATE t_bench SET i01 = i01 + 1;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_after_u := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples
      INTO n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_tuples <> 500000 THEN
        RAISE EXCEPTION 'after-UPDATE 500k analyze wrong count: % (expected 500000)',
                        n_tuples;
    END IF;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_after_u_warm := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'after UPDATE 500k: 1st=% ms (block-sample refresh), 2nd=% ms (pure delta)',
                 round(t_after_u, 4), round(t_after_u_warm, 4);

    IF t_after_u > 500 THEN
        RAISE EXCEPTION 'block-sample refresh after UPDATE 500k too slow: % ms',
                        round(t_after_u, 4);
    END IF;
    IF t_after_u_warm > 5 THEN
        RAISE EXCEPTION 'pure delta after UPDATE 500k too slow: % ms',
                        round(t_after_u_warm, 4);
    END IF;

    -- ----------------------------------------------------------
    -- Горячий путь маленького DML: небольшие DELETE / INSERT /
    -- смешанная пара.  Это сценарий из вопроса пользователя —
    -- DELETE 1000 строк из 500000 (0.2%) НЕ должен стоить полного
    -- сканирования.  Дельта-математика обрабатывает его за те же
    -- ~30 микросекунд, что и пустой проход с дельтой = 0.
    --
    -- На входе: 500000 строк (после UPDATE), все с нечётным id
    --           (1, 3, 5, ..., 999999), потому что предыдущий
    --           DELETE id%2=0 уничтожил все чётные.
    -- ----------------------------------------------------------

    -- Маленький DELETE: убираем 1000 строк (нечётные id 1, 3, ..., 1999).
    DELETE FROM t_bench WHERE id < 2001;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_small_d := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples
      INTO n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_tuples <> 499000 THEN
        RAISE EXCEPTION 'small DELETE 1000: wrong count: % (expected 499000)',
                        n_tuples;
    END IF;

    RAISE NOTICE 'small DELETE 1000 of 500000 (0.2%%): % ms', round(t_small_d, 4);

    IF t_small_d > 5 THEN
        RAISE EXCEPTION 'small DELETE delta hit too slow: % ms (expected <5)',
                        round(t_small_d, 4);
    END IF;

    -- Маленький INSERT: добавляем 500 строк с уникальными id за
    -- пределами существующих (id 2000001..2000500).  Заполнено
    -- только id, остальные колонки NULL — нагрузка на pgstat
    -- одинаковая.
    INSERT INTO t_bench (id)
    SELECT g FROM generate_series(2000001, 2000500) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_small_i := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples
      INTO n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_tuples <> 499500 THEN
        RAISE EXCEPTION 'small INSERT 500: wrong count: % (expected 499500)',
                        n_tuples;
    END IF;

    RAISE NOTICE 'small INSERT 500 into 499000 (0.1%%): % ms', round(t_small_i, 4);

    IF t_small_i > 5 THEN
        RAISE EXCEPTION 'small INSERT delta hit too slow: % ms (expected <5)',
                        round(t_small_i, 4);
    END IF;

    -- Смешанный: одновременно INSERT 300 и DELETE 100.
    -- DELETE убирает нечётные id 2001, 2003, ..., 2199 = 100 строк.
    INSERT INTO t_bench (id)
    SELECT g FROM generate_series(2000501, 2000800) g;
    DELETE FROM t_bench WHERE id BETWEEN 2001 AND 2199;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_bench');
    t_small_mix := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples
      INTO n_tuples
      FROM fasttrun_relstats('t_bench');

    -- 499500 + 300 (insert) - 100 (delete) = 499700
    IF n_tuples <> 499700 THEN
        RAISE EXCEPTION 'mixed +300/-100: wrong count: % (expected 499700)',
                        n_tuples;
    END IF;

    RAISE NOTICE 'mixed +300/-100 small DML: % ms', round(t_small_mix, 4);

    IF t_small_mix > 5 THEN
        RAISE EXCEPTION 'mixed small DML delta hit too slow: % ms',
                        round(t_small_mix, 4);
    END IF;

    -- ----------------------------------------------------------
    -- 8. fasttruncate должен быть практически мгновенным даже на
    --    миллионной широкой таблице — это smgr drop (удаление
    --    файлов хранилища), а не сканирование.
    -- ----------------------------------------------------------
    t_start := clock_timestamp();
    PERFORM fasttruncate('t_bench');
    t_trunc := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'fasttruncate of 1M-row wide table: % ms', round(t_trunc, 2);

    IF t_trunc > 100 THEN
        RAISE EXCEPTION 'fasttruncate too slow on 1M-row table: % ms (expected <100)',
                        round(t_trunc, 2);
    END IF;

    -- После truncate кешированная статистика relstats должна
    -- быть обнулена
    SELECT relpages, reltuples
      INTO n_pages, n_tuples
      FROM fasttrun_relstats('t_bench');

    IF n_pages <> 0 OR n_tuples <> 0 THEN
        RAISE EXCEPTION 'after-truncate relstats not reset: pages=%, tuples=%',
                        n_pages, n_tuples;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------
-- 9. Холодный проход только для счётчика строк (auto_collect_stats=off):
--    регрессионная защита от того, чтобы чистый путь подсчёта строк
--    не пострадал от исправления P1 (проверка свежести).  Должен
--    быть в разы быстрее холодного прохода со сбором статистики.
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start timestamptz;
    t_cold  numeric;
BEGIN
    SET LOCAL fasttrun.auto_collect_stats = off;

    CREATE TEMP TABLE t_perf_rowcount (id int, v int);
    INSERT INTO t_perf_rowcount
      SELECT g, g % 1000 FROM generate_series(1, 1000000) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_perf_rowcount');
    t_cold := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'cold rowcount-only (1M rows, 2 cols): % ms', round(t_cold, 2);

    IF t_cold > 1000 THEN
        RAISE EXCEPTION 'rowcount-only cold path regressed: % ms (expected <1000)',
                        round(t_cold, 2);
    END IF;

    DROP TABLE t_perf_rowcount;
END;
$$;

-- ----------------------------------------------------------------------
-- 10. Накладные расходы на проверку свежести в хуке статистики:
--     исправление P1 добавило вызов RelationIdGetRelation + чтение
--     pgstat + сравнение в каждом обращении планировщика к статистике
--     (один раз на колонку на запрос).  Меряем построение 1000 планов
--     простого запроса: накладные расходы на план должны быть в
--     долях миллисекунды.
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start    timestamptz;
    t_with     numeric;
    t_without  numeric;
    i int;
BEGIN
    CREATE TEMP TABLE t_hook_perf (id int, payload text);
    INSERT INTO t_hook_perf
      SELECT g, 'd_' || g FROM generate_series(1, 100000) g;
    CREATE INDEX ON t_hook_perf (id);

    -- Базовая линия: хук установлен, но кэш пуст → быстрый выход
    -- через цепочку (chain-out) на следующий обработчик
    SET LOCAL fasttrun.auto_collect_stats = off;
    PERFORM fasttrun_analyze('t_hook_perf');

    t_start := clock_timestamp();
    FOR i IN 1..1000 LOOP
        PERFORM 1 FROM (SELECT * FROM t_hook_perf WHERE id = i LIMIT 1) x;
    END LOOP;
    t_without := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    -- С активной статистикой: хук делает проверку свежести в каждом
    -- вызове
    PERFORM fasttrun_collect_stats('t_hook_perf');

    t_start := clock_timestamp();
    FOR i IN 1..1000 LOOP
        PERFORM 1 FROM (SELECT * FROM t_hook_perf WHERE id = i LIMIT 1) x;
    END LOOP;
    t_with := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'planning 1000x WITHOUT stats: % ms (% us/plan)',
                 round(t_without, 2), round(t_without, 4);
    RAISE NOTICE 'planning 1000x WITH    stats: % ms (% us/plan)',
                 round(t_with, 2), round(t_with, 4);
    RAISE NOTICE 'freshness-check overhead: % us/plan',
                 round(t_with - t_without, 4);

    -- Жёсткий порог на накладные расходы: даже на медленных CI
    -- проверка свежести должна оставаться меньше 1 мс на план.
    IF (t_with - t_without) / 1000.0 > 1.0 THEN
        RAISE EXCEPTION 'freshness check overhead too high: % us/plan',
                        round((t_with - t_without), 4);
    END IF;

    DROP TABLE t_hook_perf;
END;
$$;

-- ----------------------------------------------------------------------
-- 11. Реальный продакшн-цикл: имитирует типичный сценарий тяжёлого
--     PL/pgSQL-расчёта, где в одной транзакции на одну временную
--     таблицу делаются десятки analyze между маленькими и большими
--     DML.  Шаги (как в продакшене):
--       1.  CREATE TEMP TABLE + INSERT 100k
--       2.  fasttrun_analyze   (первый полный проход + сбор статистики)
--       3.  «запросы» (PERFORM считаем за нагрузку планировщика)
--       4.  +INSERT 50k     (доля изменений = 50% → пересбор статистики)
--       5.  fasttrun_analyze
--       6.  «запросы»
--       7.  +INSERT 30k     (доля изменений = 17% < 20% → пересбора нет)
--       8.  fasttrun_analyze
--       9.  DELETE 90k      (доля изменений = 50% → пересбор)
--       10. fasttrun_analyze
--       11. «запросы»
--       12. UPDATE 90k      (доля изменений = 100% → пересбор,
--                            смена распределения)
--       13. fasttrun_analyze
--       14. «запросы»
--     На каждом шаге проверяем правильность числа строк, и в конце —
--     общее время сценария.  Это самый близкий к реальности тест.
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start         timestamptz;
    t_total         numeric;
    t_step          timestamptz;
    t_cold          numeric;
    t_step5         numeric;
    t_step8         numeric;
    t_step10        numeric;
    t_step13        numeric;
    n_tuples        real;
BEGIN
    t_start := clock_timestamp();

    -- Шаг 1: CREATE + INSERT 100k.  Реалистичная форма временной
    -- таблицы: id (уникальный), grp (1000 уникальных значений),
    -- status (5 уникальных значений), payload.
    CREATE TEMP TABLE t_prod (
        id      bigint,
        grp     int,
        status  text,
        payload text
    );
    INSERT INTO t_prod
      SELECT g, g % 1000, (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;
    CREATE INDEX ON t_prod (id);
    CREATE INDEX ON t_prod (grp);

    -- Шаг 2: первый полный проход (холодный analyze)
    t_step := clock_timestamp();
    PERFORM fasttrun_analyze('t_prod');
    t_cold := EXTRACT(EPOCH FROM (clock_timestamp() - t_step)) * 1000;

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_prod');
    IF n_tuples <> 100000 THEN
        RAISE EXCEPTION 'step 2: wrong count: %', n_tuples;
    END IF;

    -- Шаг 3: имитируем нагрузку планировщика (несколько запросов)
    PERFORM count(*) FROM t_prod WHERE id = 5000;
    PERFORM count(*) FROM t_prod WHERE grp = 42;
    PERFORM count(*) FROM t_prod WHERE status = 'active';
    PERFORM count(*) FROM t_prod WHERE id BETWEEN 1 AND 1000;

    -- Шаг 4: +INSERT 50k (доля изменений = 50% → запустит пересбор
    -- на следующем analyze)
    INSERT INTO t_prod
      SELECT g, g % 1000, 'new', 'p_' || g
      FROM generate_series(100001, 150000) g;

    -- Шаг 5: analyze (срабатывает пересбор через блочную выборку)
    t_step := clock_timestamp();
    PERFORM fasttrun_analyze('t_prod');
    t_step5 := EXTRACT(EPOCH FROM (clock_timestamp() - t_step)) * 1000;

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_prod');
    IF n_tuples <> 150000 THEN
        RAISE EXCEPTION 'step 5: wrong count: %', n_tuples;
    END IF;

    -- Шаг 6: запросы
    PERFORM count(*) FROM t_prod WHERE id = 75000;
    PERFORM count(*) FROM t_prod WHERE grp = 999;

    -- Шаг 7: +INSERT 30k (доля изменений с момента последнего
    -- пересбора = 30k/180k = 17% < 20% → пересбор не сработает,
    -- статистика остаётся от шага 5)
    INSERT INTO t_prod
      SELECT g, g % 1000, 'pending', 'p_' || g
      FROM generate_series(150001, 180000) g;

    -- Шаг 8: analyze
    t_step := clock_timestamp();
    PERFORM fasttrun_analyze('t_prod');
    t_step8 := EXTRACT(EPOCH FROM (clock_timestamp() - t_step)) * 1000;

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_prod');
    IF n_tuples <> 180000 THEN
        RAISE EXCEPTION 'step 8: wrong count: %', n_tuples;
    END IF;

    -- Шаг 9: DELETE 90k строк со статусом pending+new (~50% изменений)
    DELETE FROM t_prod WHERE status IN ('pending', 'new');

    -- Шаг 10: analyze (пересбор через блочную выборку)
    t_step := clock_timestamp();
    PERFORM fasttrun_analyze('t_prod');
    t_step10 := EXTRACT(EPOCH FROM (clock_timestamp() - t_step)) * 1000;

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_prod');
    IF n_tuples >= 100000 THEN
        RAISE EXCEPTION 'step 10: DELETE not reflected: %', n_tuples;
    END IF;

    -- Шаг 11: запросы
    PERFORM count(*) FROM t_prod WHERE id = 5000;
    PERFORM count(*) FROM t_prod WHERE status = 'active';

    -- Шаг 12: UPDATE всех строк со сменой grp на одно значение
    -- (смена распределения — n_distinct по grp падает с 1000 до 1)
    UPDATE t_prod SET grp = 1;

    -- Шаг 13: analyze (пересбор должен обновить n_distinct)
    t_step := clock_timestamp();
    PERFORM fasttrun_analyze('t_prod');
    t_step13 := EXTRACT(EPOCH FROM (clock_timestamp() - t_step)) * 1000;

    -- Шаг 14: запросы
    PERFORM count(*) FROM t_prod WHERE grp = 1;

    t_total := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    RAISE NOTICE 'production round-trip on 100k temp:';
    RAISE NOTICE '  step 2  (cold scan):                      % ms', round(t_cold, 2);
    RAISE NOTICE '  step 5  (refresh after +50k INSERT):      % ms', round(t_step5, 2);
    RAISE NOTICE '  step 8  (refresh after +30k INSERT):      % ms', round(t_step8, 2);
    RAISE NOTICE '  step 10 (refresh after 90k DELETE):       % ms', round(t_step10, 2);
    RAISE NOTICE '  step 13 (refresh after mass UPDATE):      % ms', round(t_step13, 2);
    RAISE NOTICE '  total scenario time:                      % ms', round(t_total, 2);

    -- Жёсткие пороги защиты от регрессии.  Каждый пересбор на узкой
    -- 100k-row таблице должен укладываться в 100 мс с большим
    -- запасом; весь сценарий — в пару секунд (большая часть времени
    -- — это INSERT и CREATE INDEX).
    IF t_step5 > 200 THEN
        RAISE EXCEPTION 'step 5 refresh too slow: % ms', round(t_step5, 2);
    END IF;
    IF t_step10 > 200 THEN
        RAISE EXCEPTION 'step 10 refresh too slow: % ms', round(t_step10, 2);
    END IF;
    IF t_step13 > 200 THEN
        RAISE EXCEPTION 'step 13 refresh too slow: % ms', round(t_step13, 2);
    END IF;

    DROP TABLE t_prod;
END;
$$;

-- ----------------------------------------------------------------------
-- 12. Переиспользование сессии пулером: клиент A создаёт временную
--     таблицу на 50k строк, прогоняет базовый цикл analyze, делает
--     COMMIT.  Затем «клиент B», получивший ту же самую сессию из
--     пула, делает fasttruncate и заполняет таблицу ДРУГИМ объёмом
--     данных (1M строк вместо 50k).  Мы проверяем, что следующий
--     fasttrun_analyze выполнил свежий полный проход (а не отдал
--     устаревшие данные через попадание в кэш через дельту) и
--     сообщил правильное число строк.  Это путь, который важнее
--     всего для продакшна с пулером — статистика старого клиента
--     НЕ должна просачиваться в планы нового клиента.
--
--     fasttruncate должен явно вытеснить оба кэша; сброс через
--     callback границы транзакции на COMMIT — это запасной механизм.
-- ----------------------------------------------------------------------

-- Клиент A
CREATE TEMP TABLE t_pool (id int, grp int, payload text);
INSERT INTO t_pool
  SELECT g, g % 100, 'a_' || g FROM generate_series(1, 50000) g;
CREATE INDEX ON t_pool (id);
CREATE INDEX ON t_pool (grp);

DO $$
DECLARE
    n_tuples real;
BEGIN
    -- Цикл analyze клиента A внутри одной транзакции: первый полный
    -- проход + попадания через дельту, затем COMMIT приведёт к
    -- срабатыванию callback'a границы транзакции, который вытеснит
    -- обе хэш-таблицы.
    PERFORM fasttrun_analyze('t_pool');

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_pool');
    IF n_tuples <> 50000 THEN
        RAISE EXCEPTION 'client A: wrong cold count: %', n_tuples;
    END IF;

    -- Несколько раундов analyze + DML, всё внутри транзакции клиента A
    INSERT INTO t_pool
      SELECT g, g % 100, 'a_' || g FROM generate_series(50001, 60000) g;
    PERFORM fasttrun_analyze('t_pool');
    PERFORM fasttrun_analyze('t_pool');

    DELETE FROM t_pool WHERE id < 5000;
    PERFORM fasttrun_analyze('t_pool');
END;
$$;
-- Граница DO-блока работает как неявный COMMIT — callback границы
-- транзакции вытесняет кэши.

-- Клиент B: получает сессию из пула.  Таблица всё ещё существует
-- (она локальна для сессии), но новый клиент об этом не знает — он
-- делает свой собственный fasttruncate перед повторным заполнением.
DO $$
DECLARE
    t_start    timestamptz;
    t_trunc    numeric;
    t_cold     numeric;
    n_tuples   real;
    n_pages    int;
BEGIN
    t_start := clock_timestamp();
    PERFORM fasttruncate('t_pool');
    t_trunc := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    -- После fasttruncate статистика relstats должна быть обнулена
    SELECT reltuples, relpages INTO n_tuples, n_pages
      FROM fasttrun_relstats('t_pool');
    IF n_tuples <> 0 OR n_pages <> 0 THEN
        RAISE EXCEPTION 'client B: fasttruncate did not reset relstats: t=%, p=%',
                        n_tuples, n_pages;
    END IF;

    -- Клиент B заполняет таблицу НАМНОГО большим объёмом данных
    -- (1M против 50k) и с другим распределением (10000 уникальных
    -- значений grp вместо 100).
    INSERT INTO t_pool
      SELECT g, g % 10000, 'b_' || g FROM generate_series(1, 1000000) g;

    -- Первый analyze клиента B на свежих данных.  fasttruncate
    -- засеял analyze cache опорным снимком pgstat счётчиков, поэтому
    -- этот вызов идёт по delta-hit + block-sample refresh, а не по
    -- старому холодному пути.  Главное — никакого протекания
    -- статистики клиента A: число строк должно быть ровно тем,
    -- что только что вставлено.
    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_pool');
    t_cold := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    SELECT reltuples INTO n_tuples FROM fasttrun_relstats('t_pool');
    IF n_tuples <> 1000000 THEN
        RAISE EXCEPTION 'pooler reuse: wrong row count for client B: % (expected 1000000)',
                        n_tuples;
    END IF;

    RAISE NOTICE 'pooler reuse: fasttruncate=% ms, client B 1st analyze=% ms, count=%',
                 round(t_trunc, 2), round(t_cold, 2), n_tuples::int;

    -- После seed + refresh: попадание через дельту должно
    -- укладываться в микросекунды
    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_pool');
    t_cold := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;
    IF t_cold > 5 THEN
        RAISE EXCEPTION 'pooler reuse: 2nd analyze too slow: % ms',
                        round(t_cold, 4);
    END IF;
    RAISE NOTICE '  2nd analyze (pure delta hot path): % ms', round(t_cold, 4);
END;
$$;

DROP TABLE t_pool;

-- ----------------------------------------------------------------------
-- 13. Стоимость typanalyze vs Haas-Stokes на одной и той же таблице.
--     Сравниваем cold scan на 100k 4-col temp с use_typanalyze=on
--     (default, MCV/histogram/correlation) и use_typanalyze=off
--     (старая Haas-Stokes — только n_distinct/null_frac/width).
--     Жёсткий порог: typanalyze не должна быть больше чем в 5x
--     медленнее, чем Haas-Stokes — иначе trade-off "качество планов
--     vs latency сбора" перестаёт быть приемлемым.
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start    timestamptz;
    t_typ      numeric;
    t_haas     numeric;
    ratio      numeric;
BEGIN
    CREATE TEMP TABLE t_typ_perf (id int, grp int, status text, payload text);
    INSERT INTO t_typ_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    -- Прогон A: typanalyze=on (default).  Засеиваем дельту через
    -- fasttruncate+refill, следующий analyze пойдёт через refresh
    -- с полным сбором.
    SET LOCAL fasttrun.use_typanalyze = on;
    PERFORM fasttruncate('t_typ_perf');
    INSERT INTO t_typ_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_typ_perf');
    t_typ := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    -- Прогон B: use_typanalyze=off
    SET LOCAL fasttrun.use_typanalyze = off;
    PERFORM fasttruncate('t_typ_perf');
    INSERT INTO t_typ_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_typ_perf');
    t_haas := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    ratio := t_typ / GREATEST(t_haas, 0.001);
    RAISE NOTICE 'analyze cost on 100k 4-col: typanalyze=% ms, Haas-Stokes=% ms (ratio %x)',
                 round(t_typ, 2), round(t_haas, 2), round(ratio, 2);

    /* typanalyze не должен быть дороже 5x Haas-Stokes -- обычно
     * разница ~2x, т.к. reservoir scan общий, отличается только
     * compute_stats per-column. */
    IF ratio > 5.0 THEN
        RAISE EXCEPTION 'typanalyze too expensive: %x of Haas-Stokes (limit 5x)',
                        round(ratio, 2);
    END IF;

    DROP TABLE t_typ_perf;
END;
$$;

-- ----------------------------------------------------------------------
-- 14. Стоимость sample_rows = -1 (auto, full ANALYZE-quality) vs
--     default 3000 на одной и той же таблице.  Auto использует
--     std_typanalyze->minrows = 300 * default_statistics_target,
--     при default DST=100 это ~30000 — в 10x больше.  Сравниваем
--     cost cold scan и проверяем что auto не катастрофически
--     дороже (порог 30x — теоретический worst case на wide table
--     с дорогими compute_stats per-column; на 4-col table обычно
--     ratio в районе 5-10x).
-- ----------------------------------------------------------------------
DO $$
DECLARE
    t_start    timestamptz;
    t_3k       numeric;
    t_auto     numeric;
    ratio      numeric;
BEGIN
    CREATE TEMP TABLE t_smpl_perf (
        id int, grp int, status text, payload text
    );
    INSERT INTO t_smpl_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    -- Прогон A: sample_rows = 3000 (default)
    SET LOCAL fasttrun.sample_rows = 3000;
    PERFORM fasttruncate('t_smpl_perf');
    INSERT INTO t_smpl_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_smpl_perf');
    t_3k := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    -- Прогон B: sample_rows = -1 (авто через std_typanalyze minrows)
    SET LOCAL fasttrun.sample_rows = -1;
    PERFORM fasttruncate('t_smpl_perf');
    INSERT INTO t_smpl_perf
      SELECT g, g % 1000,
             (ARRAY['new','active','pending','done','archived'])[1 + g % 5],
             'p_' || g
      FROM generate_series(1, 100000) g;

    t_start := clock_timestamp();
    PERFORM fasttrun_analyze('t_smpl_perf');
    t_auto := EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000;

    ratio := t_auto / GREATEST(t_3k, 0.001);
    RAISE NOTICE 'sample_rows cost on 100k 4-col: 3000=% ms, -1(auto)=% ms (ratio %x)',
                 round(t_3k, 2), round(t_auto, 2), round(ratio, 2);

    /* auto при default_statistics_target=100 берёт ~30000 строк →
     * reservoir в ~10x больше. На 4-col таблице ratio обычно 5-15x.
     * Жёсткий потолок 30x ловит реальную регрессию без дребезга. */
    IF ratio > 30.0 THEN
        RAISE EXCEPTION 'sample_rows=-1 too expensive: %x of 3000 (limit 30x)',
                        round(ratio, 2);
    END IF;

    DROP TABLE t_smpl_perf;
END;
$$;

-- Подтверждаем прохождение в стабильной форме (видно в expected/.out)
SELECT 'fasttrun_bench: passed' AS result;

DROP TABLE t_bench;
DROP EXTENSION fasttrun;
RESET client_min_messages;
