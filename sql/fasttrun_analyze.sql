--
-- fasttrun_analyze — публикация статистики для временных таблиц прямо
-- в памяти процесса (без записи в pg_class и без генерации сообщений
-- инвалидации общего кэша).
--
-- Что покрывают тесты:
--   * первый вызов считает строки и обновляет rd_rel;
--   * повторный вызов после нового INSERT отражает свежий счёт;
--   * после fasttruncate rd_rel обнулено, следующий analyze пересчитывает;
--   * сценарий цикла очистка / заполнение / analyze / заполнение / analyze;
--   * pg_class на диске не меняется (никаких сообщений инвалидации);
--   * молчаливое игнорирование отсутствующей таблицы и ошибка на
--     обычной (не временной) таблице.
--

CREATE EXTENSION fasttrun;

-- Размер основного слоя по данным smgr.  В PostgreSQL он равен
-- pg_relation_size, а Postgres Pro Enterprise держит временную таблицу в
-- локальных буферах, пока она в них помещается, и размер файла там ноль.
CREATE FUNCTION pg_temp.rel_bytes(regclass) RETURNS bigint
  AS '$libdir/fasttrun', 'fasttrun_test_relation_bytes' LANGUAGE C STRICT;

-- При enable_temp_memory_catalog = on (Tantor SE) строк каталога временных
-- таблиц SQL не видит. Сверки с pg_class и pg_stats тогда дают true без
-- проверки: coalesce(bool_and(условие), memory_catalog).
SELECT coalesce(current_setting('enable_temp_memory_catalog', true), 'off') = 'on'
       AS memory_catalog \gset

-- ----------------------------------------------------------------------
-- 1. Базовый сценарий: вставили данные, вызвали fasttrun_analyze,
--    rd_rel показывает реальный счёт.  pg_class на диске не тронут.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_an (a int);

-- pg_class показывает дефолты, потому что никакого ANALYZE ещё не было
SELECT coalesce(bool_and(relpages = 0 AND reltuples = -1), :'memory_catalog'::boolean) AS pgclass_defaults
  FROM pg_class WHERE relname = 't_an' AND relpersistence = 't';

-- rd_rel тоже на дефолтах
SELECT * FROM fasttrun_relstats('t_an');

INSERT INTO t_an SELECT generate_series(1, 1000);

-- До analyze rd_rel ещё не обновлено
SELECT relpages, reltuples FROM fasttrun_relstats('t_an');

-- После fasttrun_analyze rd_rel обновлено
SELECT fasttrun_analyze('t_an');
SELECT relpages > 0 AS has_pages,
       reltuples = 1000 AS exact_tuples
  FROM fasttrun_relstats('t_an');

-- А pg_class на диске не тронут — никаких записей в каталог
SELECT coalesce(bool_and(relpages = 0 AND reltuples = -1), :'memory_catalog'::boolean) AS pgclass_untouched
  FROM pg_class WHERE relname = 't_an' AND relpersistence = 't';

-- ----------------------------------------------------------------------
-- 2. Повторный analyze без изменений данных — счёт остаётся точным
-- ----------------------------------------------------------------------
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 1000 AS still_exact FROM fasttrun_relstats('t_an');

SELECT fasttrun_analyze('t_an');
SELECT reltuples = 1000 AS still_exact FROM fasttrun_relstats('t_an');

-- ----------------------------------------------------------------------
-- 3. После INSERT нового пакета строк analyze отражает новую сумму,
--    даже если page count не изменился (мелкие int4 строки помещаются
--    в одну страницу).
-- ----------------------------------------------------------------------
INSERT INTO t_an SELECT generate_series(1001, 1050);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 1050 AS after_small_insert FROM fasttrun_relstats('t_an');

INSERT INTO t_an SELECT generate_series(2001, 2010);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 1060 AS after_another_insert FROM fasttrun_relstats('t_an');

-- ----------------------------------------------------------------------
-- 4. После fasttruncate rd_rel обнулено самим fasttruncate;
--    следующий analyze видит свежую картинку
-- ----------------------------------------------------------------------
SELECT fasttruncate('t_an');
SELECT relpages, reltuples FROM fasttrun_relstats('t_an');

-- Заполняем по-новой
INSERT INTO t_an SELECT generate_series(1, 200);

-- До analyze: rd_rel всё ещё обнулено (fasttruncate его сбросил)
SELECT relpages, reltuples FROM fasttrun_relstats('t_an');

-- После analyze видны новые значения, специфичные для нового заполнения
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 200 AS new_count FROM fasttrun_relstats('t_an');

-- ----------------------------------------------------------------------
-- 5. Цикл truncate/fill/analyze работает многократно
-- ----------------------------------------------------------------------
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 10);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 10 AS round1 FROM fasttrun_relstats('t_an');

SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 500);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 500 AS round2 FROM fasttrun_relstats('t_an');

SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 1);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 1 AS round3 FROM fasttrun_relstats('t_an');

-- ----------------------------------------------------------------------
-- 6. Analyze на пустой таблице — relpages=0, reltuples=0
-- ----------------------------------------------------------------------
SELECT fasttruncate('t_an');
SELECT fasttrun_analyze('t_an');
SELECT relpages = 0 AS empty_pages, reltuples = 0 AS empty_tuples
  FROM fasttrun_relstats('t_an');

-- ----------------------------------------------------------------------
-- 7. Молчаливое игнорирование отсутствующей таблицы — никакой ошибки,
--    функция просто ничего не делает.
-- ----------------------------------------------------------------------
SELECT fasttrun_analyze('nonexistent_temp_xyz');

-- ----------------------------------------------------------------------
-- 8. Ошибка на обычной (не временной) таблице — защита от опечаток
--    в имени, чтобы случайно не задеть постоянную таблицу.
-- ----------------------------------------------------------------------
CREATE TABLE t_an_persist (a int);
INSERT INTO t_an_persist VALUES (1), (2), (3);
SELECT fasttrun_analyze('t_an_persist');

-- ----------------------------------------------------------------------
-- 9. После ошибки соединение продолжает работать
-- ----------------------------------------------------------------------
SELECT 1 AS still_alive;

-- ----------------------------------------------------------------------
-- 10. Ленивый режим: DELETE должен быть замечен через pgstat-счётчик
--     tuples_deleted, иначе после удаления строк fasttrun_analyze
--     возвращал бы устаревший счёт.  Все шаги внутри одной транзакции,
--     чтобы кэш действительно работал между вызовами.
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy10_initial FROM fasttrun_relstats('t_an');

DELETE FROM t_an WHERE a > 50;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 50 AS lazy10_after_delete FROM fasttrun_relstats('t_an');

DELETE FROM t_an;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 0 AS lazy10_after_full_delete FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 11. Ленивый режим: UPDATE не меняет количество живых строк, но
--     ленивый режим должен замечать его через tuples_updated и
--     пересчитывать (промах HOT, рост числа страниц, миграция строки).
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy11_initial FROM fasttrun_relstats('t_an');

UPDATE t_an SET a = a + 1000;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy11_after_update FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 12. Ленивый режим, горячий путь: подряд несколько analyze без
--     изменений данных внутри одной транзакции — должны давать
--     одинаково корректный результат через попадание в кэш.
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy12_call1 FROM fasttrun_relstats('t_an');
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy12_call2 FROM fasttrun_relstats('t_an');
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy12_call3 FROM fasttrun_relstats('t_an');
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 100 AS lazy12_call4 FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 13. Ленивый режим: смешанная последовательность INSERT / DELETE /
--     UPDATE в одной транзакции с analyze между шагами.
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an VALUES (1), (2), (3);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 3 AS lazy13_step1 FROM fasttrun_relstats('t_an');

INSERT INTO t_an VALUES (4), (5);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 5 AS lazy13_step2 FROM fasttrun_relstats('t_an');

DELETE FROM t_an WHERE a = 3;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 4 AS lazy13_step3 FROM fasttrun_relstats('t_an');

UPDATE t_an SET a = -a;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 4 AS lazy13_step4 FROM fasttrun_relstats('t_an');

INSERT INTO t_an VALUES (10);
DELETE FROM t_an WHERE a = -1;
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 4 AS lazy13_step5 FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 14. Ленивый режим: fasttruncate явно очищает кэш по OID таблицы,
--     чтобы следующий analyze не отдал старые числа (от до-trуncate
--     состояния) через попадание в кэш.
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 50);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 50 AS lazy14_pre FROM fasttrun_relstats('t_an');

SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 7);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 7 AS lazy14_post FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 15. Ленивый режим: откат к точке сохранения (ROLLBACK TO SAVEPOINT)
--     должен корректно отработать.  После отката pgstat-счётчики
--     откатанной подтранзакции исчезают из стека rel->pgstat_info->trans,
--     и ленивый режим видит состояние, соответствующее уровню родителя
--     (до отката).
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 10);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 10 AS lazy15_pre FROM fasttrun_relstats('t_an');

SAVEPOINT sp1;
INSERT INTO t_an SELECT generate_series(11, 20);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 20 AS lazy15_in_subxact FROM fasttrun_relstats('t_an');
ROLLBACK TO SAVEPOINT sp1;

SELECT fasttrun_analyze('t_an');
SELECT reltuples = 10 AS lazy15_after_rollback FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 16. Ленивый режим: освобождение точки сохранения (RELEASE SAVEPOINT,
--     то есть фиксация подтранзакции) — счётчики поднимаются на
--     уровень родительской транзакции, ленивый режим видит совокупный
--     счёт.
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttruncate('t_an');
INSERT INTO t_an SELECT generate_series(1, 10);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 10 AS lazy16_pre FROM fasttrun_relstats('t_an');

SAVEPOINT sp2;
INSERT INTO t_an SELECT generate_series(11, 20);
SELECT fasttrun_analyze('t_an');
SELECT reltuples = 20 AS lazy16_in_subxact FROM fasttrun_relstats('t_an');
RELEASE SAVEPOINT sp2;

SELECT fasttrun_analyze('t_an');
SELECT reltuples = 20 AS lazy16_after_release FROM fasttrun_relstats('t_an');
COMMIT;

-- ----------------------------------------------------------------------
-- 17. fasttruncate внутри savepoint остаётся non-transactional: после
--     ROLLBACK TO данные уже не вернутся, значит relstats тоже не должны
--     откатиться к дорезетному состоянию.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_an_ft_rollback (id int);
INSERT INTO t_an_ft_rollback SELECT generate_series(1, 1000);

BEGIN;
SELECT fasttrun_analyze('t_an_ft_rollback');
SELECT reltuples = 1000 AS lazy17_pre FROM fasttrun_relstats('t_an_ft_rollback');

SAVEPOINT sp_ft_rollback;
SELECT fasttruncate('t_an_ft_rollback');
SELECT reltuples = 0 AS lazy17_after_fasttruncate
  FROM fasttrun_relstats('t_an_ft_rollback');
ROLLBACK TO SAVEPOINT sp_ft_rollback;

SELECT count(*) = 0 AS lazy17_actual_still_empty FROM t_an_ft_rollback;
SELECT reltuples = 0 AS lazy17_relstats_still_empty
  FROM fasttrun_relstats('t_an_ft_rollback');

UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_an_ft_rollback'::regclass;
SELECT 1 AS lazy17_force_inval;
SELECT reltuples = 0 AS lazy17_relstats_after_inval
  FROM fasttrun_relstats('t_an_ft_rollback');
COMMIT;

DROP TABLE t_an_ft_rollback;

-- ----------------------------------------------------------------------
-- 17b. Та же non-transactional семантика для таблицы С индексом: после
--      ROLLBACK TO хранилище пусто, relstats heap'а обнулены, а у индекса
--      остаётся metapage (1 страница) и ноль строк.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_an_ft_rb_idx (id int, grp int);
CREATE INDEX t_an_ft_rb_idx_grp ON t_an_ft_rb_idx (grp);
INSERT INTO t_an_ft_rb_idx SELECT g, g % 10 FROM generate_series(1, 1000) g;

BEGIN;
SELECT fasttrun_analyze('t_an_ft_rb_idx');
SELECT reltuples = 1000 AS idx17b_pre FROM fasttrun_relstats('t_an_ft_rb_idx');

SAVEPOINT sp_ft_rb_idx;
SELECT fasttruncate('t_an_ft_rb_idx');
ROLLBACK TO SAVEPOINT sp_ft_rb_idx;

SELECT count(*) = 0 AS idx17b_actual_empty FROM t_an_ft_rb_idx;
SELECT relpages = 0 AS idx17b_heap_pages_zero,
       reltuples = 0 AS idx17b_heap_tuples_zero
  FROM fasttrun_relstats('t_an_ft_rb_idx');
SELECT relpages = 1 AS idx17b_index_metapage,
       reltuples = 0 AS idx17b_index_tuples_zero
  FROM fasttrun_relstats('t_an_ft_rb_idx_grp');
COMMIT;

DROP TABLE t_an_ft_rb_idx;

-- ----------------------------------------------------------------------
-- 17c. Abort-проба под churn имени: DROP + CREATE той же temp-таблицы
--      внутри подтранзакции, затем ROLLBACK TO. Новая таблица получает
--      свой relfilenode; её analyze-запись помечена подтранзакцией, так
--      что smgr-проба пустого хранилища в abort-колбэке отрабатывает
--      именно по ней. Откат обязан восстановить ОРИГИНАЛ (старый OID,
--      старые relstats) без краша и без ложной «пустоты» от чужого
--      storage. (Истинный relfilenode-reuse недостижим в тесте —
--      счётчик монотонен; это ближайшая воспроизводимая проверка пути.)
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_an_oidchurn (id int);
INSERT INTO t_an_oidchurn SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('t_an_oidchurn');

BEGIN;
SELECT reltuples = 1000 AS churn_pre FROM fasttrun_relstats('t_an_oidchurn');
SAVEPOINT sp_churn;
DROP TABLE t_an_oidchurn;
CREATE TEMP TABLE t_an_oidchurn (id int);
INSERT INTO t_an_oidchurn SELECT generate_series(1, 7);
SELECT fasttrun_analyze('t_an_oidchurn');
SELECT reltuples = 7 AS churn_sub FROM fasttrun_relstats('t_an_oidchurn');
ROLLBACK TO SAVEPOINT sp_churn;

-- Откат восстановил оригинал: 1000 строк, никакой ложной пустоты.
SELECT count(*) = 1000 AS churn_restored FROM t_an_oidchurn;
SELECT reltuples = 1000 AS churn_relstats_restored
  FROM fasttrun_relstats('t_an_oidchurn');
-- Форсируем relcache rebuild — реинжект обязан отдать 1000, не 0.
UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_an_oidchurn'::regclass;
SELECT 1 AS churn_force_inval;
SELECT reltuples = 1000 AS churn_after_inval
  FROM fasttrun_relstats('t_an_oidchurn');
COMMIT;

SELECT count(*) = 1000 AS churn_after_commit FROM t_an_oidchurn;
DROP TABLE t_an_oidchurn;

-- ----------------------------------------------------------------------
-- 17d. Одна таблица на разных уровнях точек сохранения должна иметь
--      независимые копии состояния. RELEASE переносит состояние дочернего
--      уровня родителю, ROLLBACK TO восстанавливает исходную статистику.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_frame_nested (id int, grp int);
INSERT INTO t_frame_nested SELECT g, g FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('t_frame_nested');

BEGIN;
SAVEPOINT frame_parent;
INSERT INTO t_frame_nested SELECT g, g FROM generate_series(10001, 11000) g;
SELECT fasttrun_analyze('t_frame_nested');

SAVEPOINT frame_child;
INSERT INTO t_frame_nested SELECT g, g FROM generate_series(11001, 13000) g;
SELECT fasttrun_analyze('t_frame_nested');
ROLLBACK TO SAVEPOINT frame_child;
SELECT (fasttrun_relstats('t_frame_nested')).reltuples = 11000
       AS frame_child_restored;

SAVEPOINT frame_child_2;
INSERT INTO t_frame_nested SELECT g, g FROM generate_series(11001, 14000) g;
SELECT fasttrun_analyze('t_frame_nested');
RELEASE SAVEPOINT frame_child_2;
ROLLBACK TO SAVEPOINT frame_parent;
SELECT (fasttrun_relstats('t_frame_nested')).reltuples = 10000
       AS frame_parent_restored;
COMMIT;

DROP TABLE t_frame_nested;

-- ----------------------------------------------------------------------
-- 17e. fasttruncate и заполнение в одной точке сохранения, затем откат.
--      Очистка необратима, а заполнение откатывается, поэтому живых строк
--      нет, хотя страницы откатившегося заполнения остаются в файле.
--      Статистика таблицы до очистки не восстанавливается ни сразу, ни
--      следующим analyze, ни после COMMIT.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_refill_rb (id int, grp int);
CREATE INDEX t_refill_rb_grp ON t_refill_rb (grp);
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;

BEGIN;
SELECT fasttrun_analyze('t_refill_rb');
SELECT reltuples = 1000 AS refill17e_pre FROM fasttrun_relstats('t_refill_rb');
SELECT count(*) > 0 AS refill17e_pre_colstats
  FROM fasttrun_inspect_stats('t_refill_rb');

SAVEPOINT sp_refill;
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
ROLLBACK TO SAVEPOINT sp_refill;

SELECT count(*) = 0 AS refill17e_actual_empty FROM t_refill_rb;
SELECT pg_temp.rel_bytes('t_refill_rb') > 0 AS refill17e_dead_pages_left;
SELECT reltuples = 0 AS refill17e_tuples_zero,
       relpages = pg_temp.rel_bytes('t_refill_rb') / current_setting('block_size')::int
         AS refill17e_pages_physical
  FROM fasttrun_relstats('t_refill_rb');
SELECT reltuples = 0 AS refill17e_index_tuples_zero
  FROM fasttrun_relstats('t_refill_rb_grp');
SELECT count(*) = 0 AS refill17e_colstats_hidden
  FROM fasttrun_inspect_stats('t_refill_rb');

SELECT fasttrun_analyze('t_refill_rb');
SELECT reltuples = 0 AS refill17e_after_analyze FROM fasttrun_relstats('t_refill_rb');
COMMIT;
SELECT reltuples = 0 AS refill17e_after_commit FROM fasttrun_relstats('t_refill_rb');

-- То же через блок EXCEPTION: он и есть точка сохранения.
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
DO $$
BEGIN
  PERFORM fasttruncate('t_refill_rb');
  INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
  PERFORM fasttrun_analyze('t_refill_rb');
  RAISE EXCEPTION USING ERRCODE = 'ZX002', MESSAGE = 'abort refill';
EXCEPTION WHEN SQLSTATE 'ZX002' THEN
  NULL;
END
$$;
SELECT count(*) = 0 AS refill17e_plpgsql_empty FROM t_refill_rb;
SELECT reltuples = 0 AS refill17e_plpgsql_tuples_zero
  FROM fasttrun_relstats('t_refill_rb');
SELECT fasttrun_analyze('t_refill_rb');
SELECT reltuples = 0 AS refill17e_plpgsql_after_analyze
  FROM fasttrun_relstats('t_refill_rb');

-- Откат всей транзакции.
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
BEGIN;
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
ROLLBACK;
SELECT count(*) = 0 AS refill17e_top_empty FROM t_refill_rb;
SELECT reltuples = 0 AS refill17e_top_tuples_zero
  FROM fasttrun_relstats('t_refill_rb');

-- Очистка в дочерней точке, RELEASE, затем откат родителя: очистку
-- родитель получил от дочерней, и его откат ее тоже не отменяет.
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
BEGIN;
SAVEPOINT refill_parent;
SAVEPOINT refill_child;
SELECT fasttruncate('t_refill_rb');
RELEASE SAVEPOINT refill_child;
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_refill_rb');
ROLLBACK TO SAVEPOINT refill_parent;
SELECT count(*) = 0 AS refill17e_released_empty FROM t_refill_rb;
SELECT reltuples = 0 AS refill17e_released_tuples_zero
  FROM fasttrun_relstats('t_refill_rb');
COMMIT;

-- Контроль: откат уровня, который очистку не содержит, восстанавливает
-- состояние, сохраненное после очистки.
BEGIN;
SELECT fasttruncate('t_refill_rb');
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(1, 100) g;
SELECT fasttrun_analyze('t_refill_rb');
SAVEPOINT refill_inner;
INSERT INTO t_refill_rb SELECT g, g % 10 FROM generate_series(101, 150) g;
SELECT fasttrun_analyze('t_refill_rb');
SELECT reltuples = 150 AS refill17e_inner_pre FROM fasttrun_relstats('t_refill_rb');
ROLLBACK TO SAVEPOINT refill_inner;
SELECT count(*) = 100 AS refill17e_inner_actual FROM t_refill_rb;
SELECT reltuples = 100 AS refill17e_inner_restored
  FROM fasttrun_relstats('t_refill_rb');
COMMIT;

DROP TABLE t_refill_rb;

-- ----------------------------------------------------------------------
-- 18. Регрессионный тест: ошибка ленивого режима после SQL TRUNCATE
--     (исправление приоритета P1).
--
--     Функция pgstat_count_truncate() сбрасывает счётчики транзакции
--     в ноль и выставляет флаг truncdropped=true.  В ранней версии
--     кэша с дельта-математикой сравнивались только (страницы, ins,
--     upd, del) — для узких строк, у которых число страниц до и после
--     TRUNCATE совпадает, этого было недостаточно, чтобы признать
--     опорный снимок дельты невалидным.  Ошибка проявляется, когда в
--     таблице остались строки от ПРЕДЫДУЩЕЙ транзакции (то есть
--     cached_tuples > 0, а cached_ins = 0), потом происходит SQL
--     TRUNCATE и маленький INSERT.  Без проверки флага truncdropped
--     fasttrun_analyze опубликовал бы reltuples = 100 + 50 = 150
--     вместо правильного 50.
--
--     Этот регрессионный тест воспроизводит ровно такой сценарий.
-- ----------------------------------------------------------------------

-- Подготовка: строки переживают границу транзакции, чтобы следующая
-- транзакция началась со cached_tuples > 0, но cached_ins = 0.
DROP TABLE IF EXISTS t_an_trunc;
CREATE TEMP TABLE t_an_trunc (a int);
INSERT INTO t_an_trunc SELECT generate_series(1, 100);

-- В свежей транзакции воспроизводим сценарий TRUNCATE сразу после
-- первого полного прохода.  Все шаги внутри одной транзакции, чтобы
-- запись кэша ленивого режима жила между вызовами.
BEGIN;
SELECT fasttrun_analyze('t_an_trunc');
SELECT reltuples = 100 AS lazy17_cold FROM fasttrun_relstats('t_an_trunc');

TRUNCATE t_an_trunc;
INSERT INTO t_an_trunc SELECT generate_series(1, 50);

-- До повторного fasttrun_analyze старый session-local relstats cache уже
-- должен быть вытеснен SQL TRUNCATE.  Иначе fasttrun_relstats/planner_hook
-- реинжектят дорезетное reltuples=100 в новую физическую таблицу.
SELECT reltuples <> 100 AS lazy17_truncate_evicted_before_analyze
  FROM fasttrun_relstats('t_an_trunc');

SELECT fasttrun_analyze('t_an_trunc');
SELECT reltuples = 50 AS lazy17_after_truncate FROM fasttrun_relstats('t_an_trunc');
SELECT count(*) = 50 AS lazy17_actual_count FROM t_an_trunc;
COMMIT;

-- Вариант: TRUNCATE происходит глубже, после нескольких DML в той же
-- транзакции.  Проверяет, что бит truncdropped — основной сигнал
-- даже когда кэш уже был перезаполнен внутри текущей транзакции.
BEGIN;
SELECT fasttrun_analyze('t_an_trunc');  -- попадание в кэш через дельту, по-прежнему 50
SELECT reltuples = 50 AS lazy17v_pre FROM fasttrun_relstats('t_an_trunc');

INSERT INTO t_an_trunc SELECT generate_series(101, 110);  -- +10
SELECT fasttrun_analyze('t_an_trunc');
SELECT reltuples = 60 AS lazy17v_after_insert FROM fasttrun_relstats('t_an_trunc');

TRUNCATE t_an_trunc;
INSERT INTO t_an_trunc SELECT generate_series(1, 5);
SELECT fasttrun_analyze('t_an_trunc');
SELECT reltuples = 5 AS lazy17v_after_truncate FROM fasttrun_relstats('t_an_trunc');
SELECT count(*) = 5 AS lazy17v_actual_count FROM t_an_trunc;
COMMIT;

-- ----------------------------------------------------------------------
-- 19. ON COMMIT DELETE ROWS делает truncate на границе COMMIT в обход
--     ProcessUtility hook.  Старые relpages/reltuples из fasttrun cache
--     не должны переживать этот commit-time truncate.
-- ----------------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE t_an_oncommit (id int) ON COMMIT DELETE ROWS;
CREATE INDEX t_an_oncommit_id_idx ON t_an_oncommit (id);
INSERT INTO t_an_oncommit SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_an_oncommit');
SELECT reltuples = 100 AS lazy18_before_commit
  FROM fasttrun_relstats('t_an_oncommit');
COMMIT;

SELECT count(*) = 0 AS lazy18_actual_empty FROM t_an_oncommit;
SELECT relpages = 0 AS lazy18_pages_zero,
       reltuples = 0 AS lazy18_tuples_zero
  FROM fasttrun_relstats('t_an_oncommit');
SELECT reltuples = 0 AS lazy18_index_tuples_zero
  FROM fasttrun_relstats('t_an_oncommit_id_idx');

DROP TABLE t_an_trunc;
DROP TABLE t_an_oncommit;

-- Очистка
DROP TABLE t_an;
DROP TABLE t_an_persist;

-- 20. Откат CREATE не должен оставлять в кеше записи об исчезнувшей пустой таблице.
CREATE FUNCTION pg_temp.assert_birth_cache_empty() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE s record;
BEGIN
  SELECT * INTO s FROM fasttrun_cache_stats();
  IF s.analyze_entries <> 0 OR s.column_stats_entries <> 0 OR
     s.column_stats_relid_entries <> 0 THEN
    RAISE EXCEPTION 'aborted CREATE left cache entries: %', row_to_json(s);
  END IF;
END
$$;

BEGIN;
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    BEGIN
      CREATE TEMP TABLE t_birth_empty(id int PRIMARY KEY, payload text);
      PERFORM fasttrun_analyze('t_birth_empty');
      RAISE EXCEPTION USING ERRCODE = 'ZX001', MESSAGE = 'abort new table';
    EXCEPTION WHEN SQLSTATE 'ZX001' THEN
      NULL;
    END;
  END LOOP;
END
$$;
SELECT pg_temp.assert_birth_cache_empty();
COMMIT;
SELECT pg_temp.assert_birth_cache_empty();

-- RELEASE передаёт родителю изменения кеша и сведения о создании таблицы.
BEGIN;
SAVEPOINT birth_parent;
SAVEPOINT birth_child;
CREATE TEMP TABLE t_birth_released(id int PRIMARY KEY);
SELECT fasttrun_analyze('t_birth_released');
RELEASE SAVEPOINT birth_child;
ROLLBACK TO SAVEPOINT birth_parent;
SELECT pg_temp.assert_birth_cache_empty();
COMMIT;

-- Таблица создана в основной транзакции, а fasttrun вызывается только в дочерней.
BEGIN;
CREATE TEMP TABLE t_birth_top(id int PRIMARY KEY);
SAVEPOINT birth_child;
SELECT fasttrun_analyze('t_birth_top');
ROLLBACK TO SAVEPOINT birth_child;
ROLLBACK;
SELECT pg_temp.assert_birth_cache_empty();

-- Для существующей таблицы откат должен сохранять результат необратимой очистки.
CREATE TEMP TABLE t_birth_existing(id int PRIMARY KEY);
INSERT INTO t_birth_existing SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_birth_existing');
BEGIN;
SAVEPOINT birth_child;
SELECT fasttruncate('t_birth_existing');
ROLLBACK TO SAVEPOINT birth_child;
SELECT count(*) = 0 AS birth_existing_still_empty FROM t_birth_existing;
SELECT reltuples = 0 AS birth_existing_stats_empty
  FROM fasttrun_relstats('t_birth_existing');
ROLLBACK;
SELECT reltuples = 0 AS birth_existing_top_abort_empty
  FROM fasttrun_relstats('t_birth_existing');
DROP TABLE t_birth_existing;
SELECT pg_temp.assert_birth_cache_empty();

-- ----------------------------------------------------------------------
-- 21. Счетчики изменений переживают COMMIT.  При max_analyze_pages = 1
--     любой проход по таблице пишет DEBUG-строку: первый проход - о выборке
--     блоков, пересбор таблицы не больше цели выборки - о полном проходе, так
--     что видно, где analyze читал таблицу.  Первый analyze новой транзакции без
--     DML таблицу не читает и не сбрасывает планы.  Строки, записанные в
--     транзакциях без вызовов fasttrun, учитываются, откаченные - нет.
--     После изменений analyze пересобирает статистику колонок, а число строк
--     берет из перенесенной дельты.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_carry (grp int);
INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('t_carry');

BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry');
RESET client_min_messages;
SELECT reltuples = 10000 AS carry21_noop_count FROM fasttrun_relstats('t_carry');
COMMIT;

INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 1000) g;
BEGIN;
INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 500) g;
ROLLBACK;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry');
RESET client_min_messages;
SELECT reltuples = 11000 AS carry21_counted_without_scan
  FROM fasttrun_relstats('t_carry');
COMMIT;

INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 1000) g;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry');
RESET client_min_messages;
COMMIT;

-- Еще 1000 строк: снова пересбор и точный счет из дельты.
INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 1000) g;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry');
RESET client_min_messages;
SELECT reltuples = 13000 AS carry21_refresh_count FROM fasttrun_relstats('t_carry');
COMMIT;

-- Ядерный TRUNCATE обрывает перенос: следующий analyze сканирует.
TRUNCATE t_carry;
INSERT INTO t_carry SELECT g % 10 FROM generate_series(1, 1000) g;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry');
RESET client_min_messages;
SELECT reltuples = 1000 AS carry21_after_core_truncate FROM fasttrun_relstats('t_carry');
COMMIT;
DROP TABLE t_carry;

-- Статистика колонок тоже видит изменения, накопленные несколькими
-- транзакциями.  Пол допуска near-unique колонки - 5%, каждая транзакция
-- добавляет 2% строк без analyze.  После третьей сумма выше пола, COMMIT
-- скрывает статистику, и оценка уходит к значению по умолчанию.
CREATE TEMP TABLE t_carry_cols (id int);
INSERT INTO t_carry_cols SELECT g FROM generate_series(1, 10000) g;
SELECT fasttrun_analyze('t_carry_cols');
CREATE FUNCTION pg_temp.carry_est() RETURNS int LANGUAGE plpgsql AS $$
DECLARE ln text;
BEGIN
  FOR ln IN EXPLAIN SELECT * FROM t_carry_cols WHERE id = 5 LOOP
    IF ln ~ 'rows=' THEN RETURN substring(ln FROM 'rows=(\d+)')::int; END IF;
  END LOOP;
  RETURN NULL;
END
$$;
SELECT pg_temp.carry_est() < 5 AS carry21_cols_visible;
INSERT INTO t_carry_cols SELECT g FROM generate_series(10001, 10200) g;
INSERT INTO t_carry_cols SELECT g FROM generate_series(10201, 10400) g;
SELECT pg_temp.carry_est() < 5 AS carry21_cols_visible_at_4pct;
INSERT INTO t_carry_cols SELECT g FROM generate_series(10401, 10600) g;
SELECT pg_temp.carry_est() > 20 AS carry21_cols_hidden_at_6pct;
DROP TABLE t_carry_cols;

-- 21b. Перенос верен, только пока журнал видит каждую запись.  Ниже пути,
--      где запись или сброс таблицы идут мимо обычного учета.

-- ON COMMIT DELETE ROWS очищает таблицу на COMMIT без счетчиков, а COPY не
-- планируется, поэтому перед ним пустоту хранилища проверяет сам fasttrun.
CREATE TEMP TABLE t_carry_odr (id int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO t_carry_odr SELECT generate_series(1, 10);
SELECT fasttrun_analyze('t_carry_odr');
COMMIT;
BEGIN;
COPY t_carry_odr FROM stdin;
1
2
3
4
5
6
7
8
9
10
\.
SELECT fasttrun_analyze('t_carry_odr');
SELECT reltuples = 10 AS carry21b_odr_copy FROM fasttrun_relstats('t_carry_odr');
COMMIT;
DROP TABLE t_carry_odr;

-- Ядерный TRUNCATE в точке сохранения: при RELEASE ее счетчики заменяют
-- счетчики родителя, а не складываются с ними.
CREATE TEMP TABLE t_carry_trunc (grp int);
BEGIN;
INSERT INTO t_carry_trunc SELECT g % 10 FROM generate_series(1, 100) g;
SAVEPOINT s;
TRUNCATE t_carry_trunc;
INSERT INTO t_carry_trunc SELECT g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_carry_trunc');
RELEASE SAVEPOINT s;
SELECT fasttrun_analyze('t_carry_trunc');
SELECT reltuples = 1000 AS carry21b_trunc_release FROM fasttrun_relstats('t_carry_trunc');
COMMIT;
SELECT fasttrun_analyze('t_carry_trunc');
SELECT reltuples = 1000 AS carry21b_trunc_commit FROM fasttrun_relstats('t_carry_trunc');
DROP TABLE t_carry_trunc;

-- Запись при выключенном track_counts счетчики не видят.
CREATE TEMP TABLE t_carry_uncounted (id int);
INSERT INTO t_carry_uncounted SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('t_carry_uncounted');
BEGIN;
SET LOCAL track_counts = off;
INSERT INTO t_carry_uncounted SELECT generate_series(1001, 1100);
SET LOCAL track_counts = on;
COMMIT;
SELECT fasttrun_analyze('t_carry_uncounted');
SELECT reltuples = 1100 AS carry21b_uncounted FROM fasttrun_relstats('t_carry_uncounted');
DROP TABLE t_carry_uncounted;

-- analyze внутри modifying CTE сканирует по снимку, который не видит
-- вставленные строки, а счетчики их уже учитывают.
CREATE TEMP TABLE t_carry_cte (id int);
INSERT INTO t_carry_cte SELECT generate_series(1, 1000);
BEGIN;
WITH w AS (INSERT INTO t_carry_cte SELECT generate_series(1001, 2000) RETURNING id)
SELECT count(*) AS carry21b_cte_inserted, fasttrun_analyze('t_carry_cte') IS NOT NULL AS analyzed FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_cte');
SELECT reltuples = 2000 AS carry21b_cte FROM fasttrun_relstats('t_carry_cte');
DROP TABLE t_carry_cte;

-- Строки, вставленные через секционированного родителя, попадают в секцию.
CREATE TEMP TABLE t_carry_parent (id int) PARTITION BY RANGE (id);
CREATE TEMP TABLE t_carry_leaf PARTITION OF t_carry_parent FOR VALUES FROM (0) TO (100000);
INSERT INTO t_carry_leaf SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('t_carry_leaf');
INSERT INTO t_carry_parent SELECT generate_series(1001, 2000);
COPY t_carry_parent FROM stdin;
5001
5002
5003
5004
5005
5006
5007
5008
5009
5010
\.
SELECT fasttrun_analyze('t_carry_leaf');
SELECT reltuples = 2010 AS carry21b_partition FROM fasttrun_relstats('t_carry_leaf');
DROP TABLE t_carry_parent;

-- Счет разреженной кучи по выборке блоков - оценка.  Пересбор не выдает ее
-- за точный счет: 200 строк на первой странице и пустой хвост.
CREATE TEMP TABLE t_carry_sparse (grp int);
INSERT INTO t_carry_sparse SELECT 1 FROM generate_series(1, 200);
INSERT INTO t_carry_sparse SELECT 2 FROM generate_series(1, 20000);
DELETE FROM t_carry_sparse WHERE grp = 2;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL fasttrun.sample_rows = 30;
SELECT fasttrun_analyze('t_carry_sparse');
SET LOCAL fasttrun.sample_rows = 1000;
UPDATE t_carry_sparse SET grp = grp;
SELECT fasttrun_analyze('t_carry_sparse');
SELECT reltuples = 200 AS carry21b_sparse_count FROM fasttrun_relstats('t_carry_sparse');
SELECT stadistinct > -1e9 AND stadistinct < 1e9 AS carry21b_sparse_finite
  FROM fasttrun_inspect_stats('t_carry_sparse') WHERE staattnum = 1;
COMMIT;
DROP TABLE t_carry_sparse;

-- Сдвиг якоря индекса записывается в журнал: откат точки сохранения и затем
-- транзакции возвращает relstats индекса к значению до транзакции.
CREATE TEMP TABLE t_carry_idx (id int);
CREATE INDEX t_carry_idx_id ON t_carry_idx (id);
INSERT INTO t_carry_idx SELECT generate_series(1, 20000);
SELECT fasttrun_analyze('t_carry_idx');
BEGIN;
INSERT INTO t_carry_idx SELECT generate_series(20001, 20400);
SET LOCAL fasttrun.auto_collect_stats = off;
SELECT fasttrun_analyze('t_carry_idx');
SAVEPOINT s;
SET LOCAL fasttrun.stats_refresh_threshold = 0;
SELECT fasttrun_analyze('t_carry_idx');
ROLLBACK TO SAVEPOINT s;
ROLLBACK;
SELECT reltuples = 20000 AS carry21b_index_rollback FROM fasttrun_relstats('t_carry_idx_id');
DROP TABLE t_carry_idx;

-- analyze внутри оператора, который пишет таблицу, статистику колонок
-- планировщику не отдает.  Следующий analyze без нового DML собирает ее
-- заново, и после COMMIT, и в той же транзакции, а не оставляет колонки
-- пустыми.  Первый случай - пересбор дельтой (UPDATE всех строк превышает
-- порог), второй - тот же пересбор без COMMIT, третий - холодный проход.
CREATE TEMP TABLE t_carry_unsafe (id int, grp int);
INSERT INTO t_carry_unsafe SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_carry_unsafe');
BEGIN;
WITH w AS (UPDATE t_carry_unsafe SET grp = grp RETURNING id)
SELECT count(*) AS carry21b_unsafe_updated, fasttrun_analyze('t_carry_unsafe') IS NOT NULL AS analyzed FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_unsafe');
SELECT count(*) = 2 AS carry21b_unsafe_recollected FROM fasttrun_inspect_stats('t_carry_unsafe');
BEGIN;
WITH w AS (UPDATE t_carry_unsafe SET grp = grp RETURNING id)
SELECT count(*) AS carry21b_unsafe_updated_inxact, fasttrun_analyze('t_carry_unsafe') IS NOT NULL AS analyzed FROM w;
SELECT fasttrun_analyze('t_carry_unsafe');
SELECT count(*) = 2 AS carry21b_unsafe_recollected_inxact FROM fasttrun_inspect_stats('t_carry_unsafe');
COMMIT;
DROP TABLE t_carry_unsafe;
CREATE TEMP TABLE t_carry_unsafe_cold (id int, grp int);
INSERT INTO t_carry_unsafe_cold SELECT g, g % 10 FROM generate_series(1, 1000) g;
BEGIN;
WITH w AS (UPDATE t_carry_unsafe_cold SET grp = grp RETURNING id)
SELECT count(*) AS carry21b_unsafe_cold_updated, fasttrun_analyze('t_carry_unsafe_cold') IS NOT NULL AS analyzed FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_unsafe_cold');
SELECT count(*) = 2 AS carry21b_unsafe_cold_recollected FROM fasttrun_inspect_stats('t_carry_unsafe_cold');
SELECT reltuples = 1000 AS carry21b_unsafe_cold_count FROM fasttrun_relstats('t_carry_unsafe_cold');
DROP TABLE t_carry_unsafe_cold;
-- Явный fasttrun_collect_stats внутри пишущего оператора тоже дает образец,
-- который планировщику не отдается.  Следующий analyze собирает заново и
-- после COMMIT, и в той же транзакции.
CREATE FUNCTION pg_temp.unsafe_rows(q text) RETURNS int LANGUAGE plpgsql AS $f$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::int;
END
$f$;
CREATE TEMP TABLE t_carry_unsafe_explicit (id int, grp int);
INSERT INTO t_carry_unsafe_explicit SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_carry_unsafe_explicit');
BEGIN;
WITH w AS (UPDATE t_carry_unsafe_explicit SET grp = grp RETURNING id)
SELECT count(*) AS carry21b_unsafe_explicit_updated, fasttrun_collect_stats('t_carry_unsafe_explicit') FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_unsafe_explicit');
SELECT count(*) = 2 AS carry21b_unsafe_explicit_commit FROM fasttrun_inspect_stats('t_carry_unsafe_explicit');
SELECT pg_temp.unsafe_rows('SELECT * FROM t_carry_unsafe_explicit WHERE grp = 1') BETWEEN 50 AND 200
       AS carry21b_unsafe_explicit_commit_served;
BEGIN;
WITH w AS (UPDATE t_carry_unsafe_explicit SET grp = grp RETURNING id)
SELECT count(*) AS carry21b_unsafe_explicit_updated_inxact, fasttrun_collect_stats('t_carry_unsafe_explicit') FROM w;
SELECT fasttrun_analyze('t_carry_unsafe_explicit');
SELECT count(*) = 2 AS carry21b_unsafe_explicit_inxact FROM fasttrun_inspect_stats('t_carry_unsafe_explicit');
SELECT pg_temp.unsafe_rows('SELECT * FROM t_carry_unsafe_explicit WHERE grp = 1') BETWEEN 50 AND 200
       AS carry21b_unsafe_explicit_inxact_served;
COMMIT;
DROP TABLE t_carry_unsafe_explicit;
-- Явный сбор во время записи при нулевом износе: UPDATE не меняет ни одной
-- строки, а образец все равно небезопасен.  COMMIT его скрывает и сбрасывает
-- дельту, поэтому следующий analyze собирает заново.
CREATE TEMP TABLE t_carry_unsafe_zero (id int, grp int);
INSERT INTO t_carry_unsafe_zero SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_carry_unsafe_zero');
BEGIN;
WITH w AS (UPDATE t_carry_unsafe_zero SET grp = grp WHERE false RETURNING id)
SELECT count(*) AS carry21b_unsafe_zero_updated, fasttrun_collect_stats('t_carry_unsafe_zero') FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_unsafe_zero');
SELECT count(*) = 2 AS carry21b_unsafe_zero_recollected FROM fasttrun_inspect_stats('t_carry_unsafe_zero');
DROP TABLE t_carry_unsafe_zero;

-- Явный сбор пустой выборкой во время записи тоже ничего не говорит о
-- таблице: база пересбора остается на месте, и после COMMIT analyze собирает.
CREATE TEMP TABLE t_carry_unsafe_empty (id int, grp int);
SELECT fasttrun_analyze('t_carry_unsafe_empty');
BEGIN;
WITH w AS (INSERT INTO t_carry_unsafe_empty SELECT g, g % 10 FROM generate_series(1, 1000) g RETURNING id)
SELECT count(*) AS carry21b_unsafe_empty_inserted, fasttrun_collect_stats('t_carry_unsafe_empty') FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_unsafe_empty');
SELECT count(*) = 2 AS carry21b_unsafe_empty_recollected FROM fasttrun_inspect_stats('t_carry_unsafe_empty');
DROP TABLE t_carry_unsafe_empty;

-- ON COMMIT DELETE ROWS и закешированный общий план: INSERT идет без
-- планирования, и пустоту хранилища проверяет первая запись транзакции.
CREATE TEMP TABLE t_carry_odr_generic (id int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO t_carry_odr_generic SELECT generate_series(1, 10);
SELECT fasttrun_analyze('t_carry_odr_generic');
PREPARE carry_odr_ins AS INSERT INTO t_carry_odr_generic SELECT generate_series(1, 10);
EXPLAIN (COSTS OFF) EXECUTE carry_odr_ins;
COMMIT;
BEGIN;
EXECUTE carry_odr_ins;
SELECT fasttrun_analyze('t_carry_odr_generic');
SELECT reltuples = 10 AS carry21b_odr_generic FROM fasttrun_relstats('t_carry_odr_generic');
COMMIT;
DEALLOCATE carry_odr_ins;
DROP TABLE t_carry_odr_generic;

-- Пересчет частичного индекса внутри пишущего оператора ждет конца записи:
-- полный проход по старому снимку дал бы 1000 строк при 1010 по счетчикам.
CREATE TEMP TABLE t_carry_partial (id int, flag bool);
CREATE INDEX t_carry_partial_flag ON t_carry_partial (id) WHERE flag;
INSERT INTO t_carry_partial SELECT g, g % 2 = 0 FROM generate_series(1, 1000) g;
SET fasttrun.auto_collect_stats = off;
SET fasttrun.max_analyze_pages = 0;
SELECT fasttrun_analyze('t_carry_partial');
BEGIN;
WITH w AS (INSERT INTO t_carry_partial SELECT g, true FROM generate_series(1001, 1010) g RETURNING id)
SELECT count(*) AS carry21b_partial_inserted, fasttrun_analyze('t_carry_partial') IS NOT NULL AS analyzed FROM w;
COMMIT;
SELECT fasttrun_analyze('t_carry_partial');
SELECT reltuples = 1010 AS carry21b_partial_count FROM fasttrun_relstats('t_carry_partial');
RESET fasttrun.auto_collect_stats;
RESET fasttrun.max_analyze_pages;
DROP TABLE t_carry_partial;

-- Пересчет без нового DML публикует новое число строк: оценка по выборке
-- блоков заменяется полным проходом, хотя дельта нулевая.
CREATE TEMP TABLE t_carry_recount (grp int);
INSERT INTO t_carry_recount SELECT 1 FROM generate_series(1, 200);
INSERT INTO t_carry_recount SELECT 2 FROM generate_series(1, 20000);
DELETE FROM t_carry_recount WHERE grp = 2;
BEGIN;
SET LOCAL fasttrun.max_analyze_pages = 1;
SET LOCAL fasttrun.sample_rows = 30;
SELECT fasttrun_analyze('t_carry_recount');
SET LOCAL fasttrun.auto_collect_stats = off;
UPDATE t_carry_recount SET grp = grp;
SELECT fasttrun_analyze('t_carry_recount');
SET LOCAL fasttrun.auto_collect_stats = on;
SET LOCAL fasttrun.max_analyze_pages = 0;
SET LOCAL fasttrun.sample_rows = 1000;
SELECT fasttrun_analyze('t_carry_recount');
SELECT reltuples = 200 AS carry21b_recount_published FROM fasttrun_relstats('t_carry_recount');
COMMIT;
DROP TABLE t_carry_recount;

-- VACUUM сокращает хвост кучи мимо счетчиков: следующий analyze проходит
-- таблицу заново и публикует фактический размер.
CREATE TEMP TABLE t_carry_vacuum (grp int);
INSERT INTO t_carry_vacuum SELECT 1 FROM generate_series(1, 200);
INSERT INTO t_carry_vacuum SELECT 2 FROM generate_series(1, 20000);
DELETE FROM t_carry_vacuum WHERE grp = 2;
SELECT fasttrun_analyze('t_carry_vacuum');
VACUUM (TRUNCATE ON) t_carry_vacuum;
SELECT fasttrun_analyze('t_carry_vacuum');
SELECT relpages = pg_temp.rel_bytes('t_carry_vacuum') / 8192 AND reltuples = 200
  AS carry21b_vacuum_pages FROM fasttrun_relstats('t_carry_vacuum');
DROP TABLE t_carry_vacuum;

-- Ноль, оцененный выборкой блоков, не подтверждает базу пересбора, пока
-- пересбор отложен записью: следующий analyze пересчитывает строки.  Выборка
-- из одного блока изредка попадает на единственную живую страницу и дает не
-- ноль, тогда проверять нечего.
CREATE TEMP TABLE t_carry_zero (grp int);
INSERT INTO t_carry_zero SELECT 1 FROM generate_series(1, 200);
INSERT INTO t_carry_zero SELECT 2 FROM generate_series(1, 20000);
DELETE FROM t_carry_zero WHERE grp = 2;
SET fasttrun.max_analyze_pages = 1;
SET fasttrun.sample_rows = 1;
SELECT fasttrun_analyze('t_carry_zero');
SELECT reltuples AS zero_cold_estimate FROM fasttrun_relstats('t_carry_zero') \gset
SET fasttrun.max_analyze_pages = 0;
SET fasttrun.sample_rows = 1000;
WITH w AS (UPDATE t_carry_zero SET grp = grp RETURNING grp)
SELECT count(*) AS carry21b_zero_updated, fasttrun_analyze('t_carry_zero') IS NOT NULL AS analyzed FROM w;
SELECT fasttrun_analyze('t_carry_zero');
SELECT reltuples = 200 OR :zero_cold_estimate <> 0 AS carry21b_zero_recount
  FROM fasttrun_relstats('t_carry_zero');
RESET fasttrun.max_analyze_pages;
RESET fasttrun.sample_rows;
DROP TABLE t_carry_zero;

-- Откат точки сохранения после пересбора возвращает образец прошлого сбора.
-- Следующий analyze видит, что допуск его скрыл, и собирает заново.
CREATE TEMP TABLE t_carry_rollback_seq (id int);
INSERT INTO t_carry_rollback_seq SELECT generate_series(1, 10000);
SELECT fasttrun_analyze('t_carry_rollback_seq');
BEGIN;
INSERT INTO t_carry_rollback_seq SELECT generate_series(10001, 10600);
SAVEPOINT s;
SELECT fasttrun_analyze('t_carry_rollback_seq');
ROLLBACK TO SAVEPOINT s;
SELECT fasttrun_analyze('t_carry_rollback_seq');
SELECT count(*) = 1 AS carry21b_rollback_seq_inxact FROM fasttrun_inspect_stats('t_carry_rollback_seq');
COMMIT;
SELECT fasttrun_analyze('t_carry_rollback_seq');
SELECT count(*) = 1 AS carry21b_rollback_seq_commit FROM fasttrun_inspect_stats('t_carry_rollback_seq');
DROP TABLE t_carry_rollback_seq;

-- Первый analyze транзакции после очистки ON COMMIT DELETE ROWS сам проверяет
-- хранилище: пустая таблица публикуется пустой, и сохраненные планы
-- сбрасываются (строка DEBUG ниже).
CREATE TEMP TABLE t_carry_odr_analyze (id int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO t_carry_odr_analyze SELECT generate_series(1, 100);
SELECT fasttrun_analyze('t_carry_odr_analyze');
COMMIT;
BEGIN;
SET LOCAL client_min_messages = debug1;
SELECT fasttrun_analyze('t_carry_odr_analyze');
RESET client_min_messages;
COMMIT;
DROP TABLE t_carry_odr_analyze;
-- ----------------------------------------------------------------------
-- 22. Откаченная запись оставляет страницы в файле, а счетчики pgstat ее
--     забывают.  Планировщик делит reltuples на relpages и умножает на
--     фактическое число страниц, поэтому устаревший relpages раздул бы
--     оценку во столько раз, во сколько вырос файл.  Откат сам замеряет
--     размер хранилища, а запасной сигнал по размеру не прячет статистику
--     колонок: данные после отката прежние.
-- ----------------------------------------------------------------------
CREATE FUNCTION pg_temp.an_rows(q text) RETURNS int LANGUAGE plpgsql AS $f$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::int;
END
$f$;
CREATE TEMP TABLE t_bloat (id int, grp int, v text);
INSERT INTO t_bloat SELECT g, g % 4, md5(g::text) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_bloat');
DO $$ BEGIN BEGIN
  INSERT INTO t_bloat SELECT g, 9, md5(g::text) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
SELECT pg_temp.an_rows('SELECT * FROM t_bloat') BETWEEN 500 AND 2000
       AS bloat22_rows_after_exception,
       pg_temp.an_rows('SELECT * FROM t_bloat WHERE grp = 1') BETWEEN 150 AND 400
       AS bloat22_column_stats_after_exception;
SELECT fasttrun_analyze('t_bloat');
SELECT relpages = pg_temp.rel_bytes('t_bloat') / 8192 AS bloat22_pages_published,
       reltuples = 1000 AS bloat22_count_kept
  FROM fasttrun_relstats('t_bloat');
-- Откат всей транзакции и откат к точке сохранения.
BEGIN;
INSERT INTO t_bloat SELECT g, 9, md5(g::text) FROM generate_series(1, 100000) g;
ROLLBACK;
BEGIN;
SAVEPOINT s;
INSERT INTO t_bloat SELECT g, 9, md5(g::text) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_bloat');
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_bloat') BETWEEN 500 AND 2000
       AS bloat22_rows_after_savepoint,
       pg_temp.an_rows('SELECT * FROM t_bloat WHERE grp = 1') BETWEEN 150 AND 400
       AS bloat22_column_stats_after_savepoint;
COMMIT;
SELECT fasttrun_analyze('t_bloat');
SELECT relpages = pg_temp.rel_bytes('t_bloat') / 8192 AS bloat22_pages_after_rollbacks,
       reltuples = 1000 AS bloat22_count_after_rollbacks
  FROM fasttrun_relstats('t_bloat');
SELECT pg_temp.an_rows('SELECT * FROM t_bloat') BETWEEN 500 AND 2000
       AS bloat22_rows_after_rollbacks;
DROP TABLE t_bloat;

-- 22b. Откат, вернувший relstats в пределах invalidate_threshold, планы не
--      сбрасывает, как не сбрасывает их и сам analyze ниже порога.  Строка
--      DEBUG об инвалидации появляется только там, где порог пройден.
CREATE TEMP TABLE t_abort_inval (id int);
INSERT INTO t_abort_inval SELECT generate_series(1, 10000);
SET fasttrun.auto_collect_stats = off;
SELECT fasttrun_analyze('t_abort_inval');
BEGIN;
SET LOCAL client_min_messages = debug1;
SAVEPOINT s;
INSERT INTO t_abort_inval VALUES (1);
SELECT fasttrun_analyze('t_abort_inval');   -- ниже порога: без инвалидации
ROLLBACK TO SAVEPOINT s;                     -- ниже порога: без инвалидации
SAVEPOINT s2;
INSERT INTO t_abort_inval SELECT generate_series(1, 5000);
SELECT fasttrun_analyze('t_abort_inval');   -- выше порога: инвалидация
ROLLBACK TO SAVEPOINT s2;                    -- откат выше порога: инвалидация
-- Откат записи без analyze: файл вырос на половину, но откаченные страницы
-- строк не содержат, и оценка, которую видит планировщик, не меняется, поэтому
-- и планы не сбрасываются.
SAVEPOINT s3;
INSERT INTO t_abort_inval SELECT generate_series(1, 5000);
ROLLBACK TO SAVEPOINT s3;                    -- оценка прежняя: без инвалидации
SELECT pg_temp.an_rows('SELECT * FROM t_abort_inval') BETWEEN 9000 AND 11000
       AS abort22b_rows_after_growth;
RESET client_min_messages;
COMMIT;
SELECT fasttrun_analyze('t_abort_inval');
SELECT relpages = pg_temp.rel_bytes('t_abort_inval') / 8192 AS abort22b_pages_published
  FROM fasttrun_relstats('t_abort_inval');
RESET fasttrun.auto_collect_stats;
DROP TABLE t_abort_inval;
-- 22c. Рост файла от строк, зафиксированных без analyze, - не откат.  Откат
--      учитывает только прирост своего уровня, и планировщик продолжает
--      оценивать зафиксированные строки по плотности.  Вложенные откаты не
--      складывают один прирост несколько раз, а откат analyze не снимает
--      обязанность следующего analyze обновить индексы.
CREATE TEMP TABLE t_i1 (id int, pad text);
INSERT INTO t_i1 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_i1');
INSERT INTO t_i1 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
BEGIN;
SAVEPOINT s;
INSERT INTO t_i1 SELECT 1, 'x' WHERE false;
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_i1') > 50000 AS i1_committed_rows_estimated;
COMMIT;
DROP TABLE t_i1;

CREATE TEMP TABLE t_i2 (id int, grp int);
INSERT INTO t_i2 SELECT g, g % 4 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_i2');
SET fasttrun.auto_collect_stats = off;
BEGIN;
SAVEPOINT a; INSERT INTO t_i2 VALUES (1, 1); SELECT fasttrun_analyze('t_i2');
SAVEPOINT b; INSERT INTO t_i2 VALUES (1, 1); SELECT fasttrun_analyze('t_i2');
SAVEPOINT c; INSERT INTO t_i2 VALUES (1, 1); SELECT fasttrun_analyze('t_i2');
SAVEPOINT d; INSERT INTO t_i2 VALUES (1, 1); SELECT fasttrun_analyze('t_i2');
INSERT INTO t_i2 SELECT g, 9 FROM generate_series(1, 100000) g;
ROLLBACK TO SAVEPOINT a;
SELECT pg_temp.an_rows('SELECT * FROM t_i2 WHERE grp = 1') BETWEEN 150 AND 400
       AS i2_nested_rollback_stats_kept,
       pg_temp.an_rows('SELECT * FROM t_i2') BETWEEN 500 AND 2000
       AS i2_nested_rollback_rows;
COMMIT;
RESET fasttrun.auto_collect_stats;
DROP TABLE t_i2;

CREATE TEMP TABLE t_i6 (id int, flag bool);
CREATE INDEX t_i6_part ON t_i6 (id) WHERE flag;
INSERT INTO t_i6 SELECT g, g % 10 = 0 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_i6');
DO $$ BEGIN BEGIN
  INSERT INTO t_i6 SELECT g, true FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
BEGIN;
SAVEPOINT s;
SELECT fasttrun_analyze('t_i6');
ROLLBACK TO SAVEPOINT s;
SELECT fasttrun_analyze('t_i6');
SELECT relpages = pg_temp.rel_bytes('t_i6_part') / 8192 AS i6_index_pages_refreshed,
       reltuples BETWEEN 50 AND 200 AS i6_partial_count_kept
  FROM fasttrun_relstats('t_i6_part');
COMMIT;
DROP TABLE t_i6;

-- Откат ядерного TRUNCATE возвращает прежний файл.  Отметка уровня, снятая на
-- новом файле, к старому не относится.
CREATE TEMP TABLE t_n1 (id int, pad text);
INSERT INTO t_n1 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_n1');
BEGIN;
SAVEPOINT s;
TRUNCATE t_n1;
INSERT INTO t_n1 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n1');
INSERT INTO t_n1 SELECT 1, 'x' WHERE false;
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_n1') > 50000 AS n1_truncate_rollback_rows;
COMMIT;
DROP TABLE t_n1;

-- То же с вложенным откатом на новом файле: его мертвые страницы к старому
-- файлу не относятся.
CREATE TEMP TABLE t_n5 (id int, pad text);
INSERT INTO t_n5 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_n5');
BEGIN;
SAVEPOINT s;
TRUNCATE t_n5;
INSERT INTO t_n5 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n5');
DO $$ BEGIN BEGIN
  INSERT INTO t_n5 SELECT g, repeat('x', 50) FROM generate_series(1, 50000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
INSERT INTO t_n5 SELECT 1, 'x' WHERE false;
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_n5') BETWEEN 80000 AND 120000
       AS n5_nested_rollback_rows;
COMMIT;
DROP TABLE t_n5;

-- Без записи на новом файле после analyze у уровня нет отметки размера, и
-- откат все равно возвращает счет мертвых страниц старого файла.
CREATE TEMP TABLE t_n11 (id int, pad text);
INSERT INTO t_n11 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_n11');
BEGIN;
SAVEPOINT s;
TRUNCATE t_n11;
INSERT INTO t_n11 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n11');
DO $$ BEGIN BEGIN
  INSERT INTO t_n11 SELECT g, repeat('x', 50) FROM generate_series(1, 50000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_n11') BETWEEN 80000 AND 120000
       AS n11_rollback_without_level_mark_rows;
COMMIT;
DROP TABLE t_n11;

-- Откат вернул файл, который рос на этом же уровне до ядерного TRUNCATE:
-- рост считается от отметки уровня, снятой на этом файле.
CREATE TEMP TABLE t_n14 (id int, pad text);
INSERT INTO t_n14 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n14');
BEGIN;
SAVEPOINT s;
INSERT INTO t_n14 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
TRUNCATE t_n14;
INSERT INTO t_n14 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n14');
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_n14') BETWEEN 500 AND 2000
       AS n14_growth_before_truncate_rows;
COMMIT;
DROP TABLE t_n14;

-- Без выборки (sample_rows = 0) DML все равно делает оценку частичного
-- индекса верхней границей: прежнее число строк индекса уже неверно.
CREATE TEMP TABLE t_n6 (id int, flag bool);
CREATE INDEX t_n6_part ON t_n6 (id) WHERE flag;
BEGIN;
INSERT INTO t_n6 SELECT g, false FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n6');
UPDATE t_n6 SET flag = true WHERE id <= 500;
SET LOCAL fasttrun.sample_rows = 0;
SELECT fasttrun_analyze('t_n6');
SELECT reltuples >= 500 AS n6_partial_upper_bound
  FROM fasttrun_relstats('t_n6_part');
COMMIT;
DROP TABLE t_n6;

-- VACUUM, который кучу не обрабатывал, ничего не посчитал.
CREATE TEMP TABLE t_n8 (id int);
INSERT INTO t_n8 SELECT generate_series(1, 10000);
ANALYZE t_n8;
DELETE FROM t_n8 WHERE id > 100;
SELECT fasttrun_analyze('t_n8');
VACUUM (PROCESS_MAIN false) t_n8;
SELECT reltuples = 100 AS n8_process_main_off_keeps_relstats
  FROM fasttrun_relstats('t_n8');
DROP TABLE t_n8;

-- Повторенная опция VACUUM действует последним значением, как в ядре.
CREATE TEMP TABLE t_n13 (id int);
INSERT INTO t_n13 SELECT generate_series(1, 10000);
ANALYZE t_n13;
DELETE FROM t_n13 WHERE id > 100;
SELECT fasttrun_analyze('t_n13');
VACUUM (PROCESS_MAIN true, PROCESS_MAIN false) t_n13;
SELECT reltuples = 100 AS n13_repeated_process_main_keeps_relstats
  FROM fasttrun_relstats('t_n13');
DROP TABLE t_n13;
CREATE TEMP TABLE t_n13b (id int);
INSERT INTO t_n13b SELECT generate_series(1, 10000);
SELECT fasttrun_analyze('t_n13b');
DELETE FROM t_n13b WHERE id > 100;
VACUUM (ONLY_DATABASE_STATS true, ONLY_DATABASE_STATS false) t_n13b;
SELECT reltuples = 100 AS n13_repeated_database_stats_takes_vacuum
  FROM fasttrun_relstats('t_n13b');
DROP TABLE t_n13b;

-- VACUUM, который ядро пропустило из-за прав, ничего не посчитал: учет
-- мертвых страниц и прежние relstats остаются.
CREATE TEMP TABLE t_n3 (id int, pad text);
INSERT INTO t_n3 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
ANALYZE t_n3;
SELECT fasttrun_analyze('t_n3');
DO $$ BEGIN BEGIN
  INSERT INTO t_n3 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
CREATE ROLE ft_n3_reader;
GRANT SELECT ON t_n3 TO ft_n3_reader;
SET client_min_messages = error;
SET ROLE ft_n3_reader;
VACUUM t_n3;
RESET ROLE;
RESET client_min_messages;
SELECT pg_temp.an_rows('SELECT * FROM t_n3') BETWEEN 500 AND 2000
       AS n3_skipped_vacuum_rows;
DROP TABLE t_n3;
DROP ROLE ft_n3_reader;

-- Первая запись уровня берет размер, прочитанный при планировании, только
-- если после чтения не стартовал другой оператор.  Здесь она идет готовым
-- общим планом после отката вложенной записи, а последнее чтение размера было
-- до этой записи: прирост отката посчитался бы второй раз.
CREATE TEMP TABLE t_n9 (id int, pad text);
INSERT INTO t_n9 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n9');
SET plan_cache_mode = force_generic_plan;
PREPARE n9_ins(int) AS INSERT INTO t_n9 VALUES ($1, 'x');
EXECUTE n9_ins(0);
BEGIN;
SAVEPOINT s;
INSERT INTO t_n9 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
ROLLBACK TO SAVEPOINT s;
EXECUTE n9_ins(1);
ROLLBACK;
SELECT pg_temp.an_rows('SELECT * FROM t_n9') BETWEEN 500 AND 2000
       AS n9_level_mark_after_nested_rollback;
DEALLOCATE n9_ins;
RESET plan_cache_mode;
DROP TABLE t_n9;

-- Размер, прочитанный, пока таблицу пишет начатый оператор, отметкой уровня не
-- становится: оператор продолжает писать без нового старта исполнителя, и
-- откат вложенной пустой записи объявил бы его живые строки мертвыми.
CREATE TEMP TABLE t_n10 (id int, pad text);
INSERT INTO t_n10 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n10');
CREATE FUNCTION pg_temp.n10_f(g int) RETURNS text LANGUAGE plpgsql AS $f$
BEGIN
  IF g = 1 THEN
    BEGIN
      PERFORM fasttrun_relstats('t_n10');
    EXCEPTION WHEN others THEN NULL;
    END;
  ELSIF g = 100000 THEN
    BEGIN
      INSERT INTO t_n10 SELECT 0, 'x' WHERE false;
      RAISE EXCEPTION 'boom';
    EXCEPTION WHEN others THEN NULL;
    END;
  END IF;
  RETURN repeat('x', 50);
END
$f$;
SET plan_cache_mode = force_generic_plan;
SELECT pg_temp.n10_f(100000) IS NOT NULL AS n10_plan_ready;
INSERT INTO t_n10 SELECT g, pg_temp.n10_f(g) FROM generate_series(1, 100000) g;
RESET plan_cache_mode;
SELECT pg_temp.an_rows('SELECT * FROM t_n10') BETWEEN 50000 AND 200000
       AS n10_probe_during_writer_rows;
DROP TABLE t_n10;

-- CREATE TABLE AS пишет новую таблицу мимо реестра писателей: его исполнитель
-- выполняет SELECT, а строки идут через DestReceiver.
CREATE FUNCTION pg_temp.n15_f(g int) RETURNS text LANGUAGE plpgsql AS $f$
BEGIN
  IF g IN (1000, 100000) THEN
    BEGIN
      IF g = 1000 THEN
        PERFORM fasttrun_analyze('t_n15');
      END IF;
      INSERT INTO t_n15 SELECT 0, 'x' WHERE false;
      IF g = 100000 THEN
        RAISE EXCEPTION 'boom';
      END IF;
    EXCEPTION WHEN others THEN NULL;
    END;
  ELSIF g = 1001 THEN
    BEGIN
      PERFORM fasttrun_relstats('t_n15');
    EXCEPTION WHEN others THEN NULL;
    END;
  END IF;
  RETURN repeat('x', 50);
END
$f$;
SET plan_cache_mode = force_generic_plan;
CREATE TEMP TABLE t_n15 AS
  SELECT g AS id, pg_temp.n15_f(g) AS pad FROM generate_series(1, 100000) g;
RESET plan_cache_mode;
SELECT pg_temp.an_rows('SELECT * FROM t_n15') BETWEEN 50000 AND 200000
       AS n15_probe_during_ctas_rows;
DROP TABLE t_n15;

-- Запись после DDL в точке сохранения оставляет отметку, и откат возвращает
-- relstats вместе с мертвыми страницами.
CREATE TEMP TABLE t_n17 (id int, pad text);
INSERT INTO t_n17 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n17');
BEGIN;
SAVEPOINT s;
CREATE INDEX t_n17_id ON t_n17 (id);
INSERT INTO t_n17 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
ROLLBACK TO SAVEPOINT s;
SELECT pg_temp.an_rows('SELECT * FROM t_n17') BETWEEN 500 AND 2000
       AS n17_ddl_savepoint_rows;
COMMIT;
DROP TABLE t_n17;

-- Счет мертвых страниц переживает построение индекса: статистика колонок,
-- сохраненная через CREATE INDEX, по-прежнему видна.
CREATE TEMP TABLE t_n17b (id int, grp int, pad text);
INSERT INTO t_n17b SELECT g, g % 4, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n17b');
DO $$ BEGIN BEGIN
  INSERT INTO t_n17b SELECT g, 9, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
CREATE INDEX t_n17b_id ON t_n17b (id);
SELECT pg_temp.an_rows('SELECT * FROM t_n17b WHERE grp = 1') BETWEEN 150 AND 400
       AS n17_column_stats_after_index;
DROP TABLE t_n17b;

-- Статистике, собранной fasttrun_collect_stats без analyze, мертвые страницы
-- отката тоже известны.
CREATE TEMP TABLE t_n17c (id int, grp int, pad text);
INSERT INTO t_n17c SELECT g, g % 4, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('t_n17c');
DO $$ BEGIN BEGIN
  INSERT INTO t_n17c SELECT g, 9, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
SELECT pg_temp.an_rows('SELECT * FROM t_n17c WHERE grp = 1')::float8 /
       pg_temp.an_rows('SELECT * FROM t_n17c') BETWEEN 0.15 AND 0.4
       AS n17_collected_stats_after_rollback;
DROP TABLE t_n17c;

-- Мертвые страницы принадлежат файлу: откат потомка достается и состоянию
-- того же файла, которое вернет более поздний откат после ядерного TRUNCATE.
CREATE TEMP TABLE t_n18 (id int, pad text);
INSERT INTO t_n18 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n18');
INSERT INTO t_n18 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
BEGIN;
SAVEPOINT p;
SELECT fasttrun_analyze('t_n18');
SAVEPOINT c;
INSERT INTO t_n18 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
ROLLBACK TO SAVEPOINT c;
RELEASE SAVEPOINT c;
TRUNCATE t_n18;
INSERT INTO t_n18 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_n18');
ROLLBACK TO SAVEPOINT p;
SELECT pg_temp.an_rows('SELECT * FROM t_n18') BETWEEN 1000 AND 4000
       AS n18_child_rollback_kept;
COMMIT;
DROP TABLE t_n18;

-- Откаченный UPDATE колонки предиката растит частичный индекс, а куча не
-- растет: новые версии ложатся на прежние страницы.  Следующий analyze все
-- равно обновляет число страниц индекса.
CREATE TEMP TABLE t_n19 (id int, flag bool) WITH (fillfactor = 10);
INSERT INTO t_n19 SELECT g, g % 100 = 0 FROM generate_series(1, 10000) g;
CREATE INDEX t_n19_part ON t_n19 (id) WHERE flag;
SELECT fasttrun_analyze('t_n19');
SELECT pg_temp.rel_bytes('t_n19') AS n19_heap_bytes \gset
BEGIN;
SAVEPOINT s;
UPDATE t_n19 SET flag = true;
ROLLBACK TO SAVEPOINT s;
COMMIT;
SELECT fasttrun_analyze('t_n19');
SELECT pg_temp.rel_bytes('t_n19') = :n19_heap_bytes AS n19_heap_same_size,
       relpages = pg_temp.rel_bytes('t_n19_part') / 8192 AS n19_index_pages_refreshed
  FROM fasttrun_relstats('t_n19_part');
DROP TABLE t_n19;

-- Команды, которые индекс не строят, relstats не трогают: CREATE INDEX IF NOT
-- EXISTS с занятым именем и ограничение поверх готового индекса.
CREATE TEMP TABLE t_n20 (id int);
INSERT INTO t_n20 SELECT generate_series(1, 10000);
CREATE INDEX t_n20_id ON t_n20 (id);
CREATE UNIQUE INDEX t_n20_uidx ON t_n20 (id);
ANALYZE t_n20;
DELETE FROM t_n20 WHERE id > 1000;
SELECT fasttrun_analyze('t_n20');
CREATE INDEX IF NOT EXISTS t_n20_id ON t_n20 (id);
SELECT reltuples = 1000 AS n20_existing_index_keeps_relstats
  FROM fasttrun_relstats('t_n20');
ALTER TABLE t_n20 ADD CONSTRAINT t_n20_uq UNIQUE USING INDEX t_n20_uidx;
SELECT reltuples = 1000 AS n20_using_index_keeps_relstats
  FROM fasttrun_relstats('t_n20');
DROP TABLE t_n20;

-- Запись кеша без relstats, оставленная ради выборок колонок, уходит, когда
-- COMMIT скрыл последние из них.
SELECT analyze_entries AS reclaim_entries_before FROM fasttrun_cache_stats() \gset
CREATE TEMP TABLE t_reclaim (id int, pad text);
INSERT INTO t_reclaim SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('t_reclaim');
BEGIN;
INSERT INTO t_reclaim SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
COMMIT;
SELECT analyze_entries = :reclaim_entries_before AS reclaim_entry_without_samples
  FROM fasttrun_cache_stats();
DROP TABLE t_reclaim;

-- VACUUM убирает строки откаченной записи и у записи без relstats: следующий
-- откат считает мертвые страницы заново, а не поверх уже убранных.
CREATE TEMP TABLE t_n21 (id int, grp int, pad text);
INSERT INTO t_n21 SELECT g, g % 4, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('t_n21');
DO $$ BEGIN BEGIN
  INSERT INTO t_n21 SELECT g, 9, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
VACUUM t_n21;
DO $$ BEGIN BEGIN
  INSERT INTO t_n21 SELECT g, 9, repeat('x', 50) FROM generate_series(1, 2000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
SELECT pg_temp.an_rows('SELECT * FROM t_n21 WHERE grp = 1')::float8 /
       pg_temp.an_rows('SELECT * FROM t_n21') BETWEEN 0.15 AND 0.4
       AS n21_vacuum_resets_dead_pages;
DROP TABLE t_n21;

-- Откат создания таблицы уносит и запись, которую завел fasttrun_collect_stats.
SELECT analyze_entries AS n22_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
CREATE TEMP TABLE t_n22 (id int, pad text);
INSERT INTO t_n22 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_collect_stats('t_n22');
SAVEPOINT s;
INSERT INTO t_n22 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
ROLLBACK TO SAVEPOINT s;
ROLLBACK;
SELECT analyze_entries = :n22_entries_before AS n22_created_table_entry_gone
  FROM fasttrun_cache_stats();

-- Построение индекса обновляет relstats одной кучи: свежие relstats других
-- индексов остаются, хотя в каталоге у них прежние значения.
CREATE TEMP TABLE t_n25 (id int, flag bool);
INSERT INTO t_n25 SELECT g, true FROM generate_series(1, 10000) g;
CREATE INDEX t_n25_part ON t_n25 (id) WHERE flag;
ANALYZE t_n25;
UPDATE t_n25 SET flag = false WHERE id > 100;
SELECT fasttrun_analyze('t_n25');
CREATE INDEX t_n25_other ON t_n25 (id);
ALTER INDEX t_n25_part SET (fillfactor = 80);
SELECT reltuples < 1000 AS n25_other_index_keeps_relstats
  FROM fasttrun_relstats('t_n25_part');
DROP TABLE t_n25;

-- Построение индекса не снимает relstats кучи: ядро записало точную пару, кеш
-- ее берет, и relstats частичного индекса доходят и до тела раскрываемой
-- SQL-функции, которое планировщик видит первым.
CREATE FUNCTION pg_temp.an_cost(q text) RETURNS float8 LANGUAGE plpgsql AS $f$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Total Cost')::float8;
END
$f$;
CREATE TEMP TABLE t_n27 (id int, flag bool);
INSERT INTO t_n27 SELECT g, true FROM generate_series(1, 10000) g;
CREATE INDEX t_n27_part ON t_n27 (id) WHERE flag;
ANALYZE t_n27;
UPDATE t_n27 SET flag = false WHERE id > 100;
SELECT fasttrun_analyze('t_n27');
CREATE FUNCTION pg_temp.n27_rows() RETURNS SETOF t_n27 LANGUAGE sql STABLE
  AS 'SELECT * FROM t_n27 WHERE flag AND id < 50';
CREATE INDEX t_n27_other ON t_n27 (id);
ALTER INDEX t_n27_part SET (fillfactor = 80);
SELECT pg_temp.an_cost('SELECT * FROM pg_temp.n27_rows()') AS n27_inline_cost \gset
SELECT abs(:n27_inline_cost - pg_temp.an_cost('SELECT * FROM t_n27 WHERE flag AND id < 50')) < 0.01
       AS n27_inline_sees_index_relstats;
DROP FUNCTION pg_temp.n27_rows();
DROP TABLE t_n27;

-- То же, когда relstats частичного индекса опубликовал fasttrun_collect_stats, а
-- у кучи своих нет.
CREATE TEMP TABLE t_n28 (id int, flag bool);
INSERT INTO t_n28 SELECT g, true FROM generate_series(1, 10000) g;
CREATE INDEX t_n28_part ON t_n28 (id) WHERE flag;
ANALYZE t_n28;
UPDATE t_n28 SET flag = false WHERE id > 100;
SELECT fasttrun_collect_stats('t_n28');
CREATE FUNCTION pg_temp.n28_rows() RETURNS SETOF t_n28 LANGUAGE sql STABLE
  AS 'SELECT * FROM t_n28 WHERE flag AND id < 50';
CREATE INDEX t_n28_other ON t_n28 (id);
ALTER INDEX t_n28_part SET (fillfactor = 80);
SELECT pg_temp.an_cost('SELECT * FROM pg_temp.n28_rows()') AS n28_inline_cost \gset
SELECT abs(:n28_inline_cost - pg_temp.an_cost('SELECT * FROM t_n28 WHERE flag AND id < 50')) < 0.01
       AS n28_inline_sees_collected_index_relstats;
DROP FUNCTION pg_temp.n28_rows();
DROP TABLE t_n28;

-- Пару, взятую после построения индекса, откат точки сохранения возвращает.
CREATE TEMP TABLE t_adopt (id int);
INSERT INTO t_adopt SELECT generate_series(1, 1000);
SELECT fasttrun_analyze('t_adopt');
BEGIN;
INSERT INTO t_adopt SELECT generate_series(1, 5000);
SAVEPOINT s;
CREATE INDEX t_adopt_id ON t_adopt (id);
SELECT reltuples = 6000 AS adopt_takes_build_count FROM fasttrun_relstats('t_adopt');
ROLLBACK TO SAVEPOINT s;
SELECT reltuples = 1000 AS adopt_rolled_back FROM fasttrun_relstats('t_adopt');
COMMIT;
DROP TABLE t_adopt;

-- После зафиксированного ядерного TRUNCATE и VACUUM записи не остается.
SELECT analyze_entries AS m14_entries_before FROM fasttrun_cache_stats() \gset
SET fasttrun.auto_collect_stats = off;
CREATE TEMP TABLE t_m14 (id int, pad text);
INSERT INTO t_m14 SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_m14');
DO $$ BEGIN BEGIN
  INSERT INTO t_m14 SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
TRUNCATE t_m14;
VACUUM t_m14;
SELECT analyze_entries = :m14_entries_before AS m14_truncated_file_entry_gone
  FROM fasttrun_cache_stats();
RESET fasttrun.auto_collect_stats;
DROP TABLE t_m14;

-- Без VACUUM: мертвые страницы старого файла запись не держат.
SELECT analyze_entries AS m14c_entries_before FROM fasttrun_cache_stats() \gset
SET fasttrun.auto_collect_stats = off;
CREATE TEMP TABLE t_m14c (id int, pad text);
INSERT INTO t_m14c SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_m14c');
DO $$ BEGIN BEGIN
  INSERT INTO t_m14c SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
TRUNCATE t_m14c;
SELECT analyze_entries = :m14c_entries_before AS m14c_truncated_entry_gone_without_vacuum
  FROM fasttrun_cache_stats();
RESET fasttrun.auto_collect_stats;
DROP TABLE t_m14c;

-- То же с индексом: TRUNCATE снимает публикации кучи и индекса, и запись кучи
-- больше не держится ради relstats индекса.
SELECT analyze_entries AS m14b_entries_before FROM fasttrun_cache_stats() \gset
SET fasttrun.auto_collect_stats = off;
CREATE TEMP TABLE t_m14b (id int, pad text);
CREATE INDEX t_m14b_id ON t_m14b (id);
INSERT INTO t_m14b SELECT g, repeat('x', 50) FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('t_m14b');
DO $$ BEGIN BEGIN
  INSERT INTO t_m14b SELECT g, repeat('x', 50) FROM generate_series(1, 100000) g;
  RAISE EXCEPTION 'boom';
EXCEPTION WHEN others THEN NULL; END; END $$;
TRUNCATE t_m14b;
VACUUM t_m14b;
SELECT analyze_entries = :m14b_entries_before AS m14b_indexed_entry_gone
  FROM fasttrun_cache_stats();
RESET fasttrun.auto_collect_stats;
DROP TABLE t_m14b;

-- VACUUM, сжавший кучу, переякоривает размер выборок записи без relstats:
-- данные те же, и проверка размера их не прячет.
CREATE TEMP TABLE t_n21b (id int, grp int, pad text);
INSERT INTO t_n21b SELECT g, g % 4, repeat('x', 50) FROM generate_series(1, 100000) g;
DELETE FROM t_n21b WHERE id > 1000;
SELECT fasttrun_collect_stats('t_n21b');
VACUUM t_n21b;
SELECT pg_temp.an_rows('SELECT * FROM t_n21b WHERE grp = 1')::float8 /
       pg_temp.an_rows('SELECT * FROM t_n21b') BETWEEN 0.15 AND 0.4
       AS n21_vacuum_reanchors_samples;
DROP TABLE t_n21b;

-- Relstats индекса из fasttrun_collect_stats доходят до тела раскрываемой
-- функции и после того, как COMMIT скрыл выборки кучи: у кучи остается
-- запись, через которую идет реинжект.
CREATE TEMP TABLE t_n28b (id int, flag bool);
INSERT INTO t_n28b SELECT g, true FROM generate_series(1, 10000) g;
CREATE INDEX t_n28b_part ON t_n28b (id) WHERE flag;
ANALYZE t_n28b;
UPDATE t_n28b SET flag = false WHERE id > 100;
SELECT fasttrun_collect_stats('t_n28b');
INSERT INTO t_n28b SELECT g, false FROM generate_series(10001, 110000) g;
CREATE FUNCTION pg_temp.n28b_rows() RETURNS SETOF t_n28b LANGUAGE sql STABLE
  AS 'SELECT * FROM t_n28b WHERE flag AND id < 50';
CREATE INDEX t_n28b_other ON t_n28b (id);
ALTER INDEX t_n28b_part SET (fillfactor = 80);
SELECT pg_temp.an_cost('SELECT * FROM pg_temp.n28b_rows()') AS n28b_first_cost \gset
SELECT relpages >= 0 AS n28b_reinjected FROM fasttrun_relstats('t_n28b_part');
SELECT abs(:n28b_first_cost - pg_temp.an_cost('SELECT * FROM pg_temp.n28b_rows()')) < 0.01
       AS n28b_hidden_samples_index_relstats;
DROP FUNCTION pg_temp.n28b_rows();
DROP TABLE t_n28b;

-- Запись кучи без своих relstats держится, пока опубликован хотя бы один ее
-- индекс. DROP INDEX в откаченной точке сохранения ничего не меняет, а
-- зафиксированный DROP последнего индекса освобождает и запись кучи.
SELECT analyze_entries AS m17_entries_before FROM fasttrun_cache_stats() \gset
CREATE TEMP TABLE t_m17 (id int);
CREATE INDEX t_m17_a ON t_m17 (id);
CREATE INDEX t_m17_b ON t_m17 (id);
SELECT fasttrun_collect_stats('t_m17');
BEGIN;
SAVEPOINT m17;
DROP INDEX t_m17_a;
ROLLBACK TO m17;
COMMIT;
SELECT analyze_entries = :m17_entries_before + 3 AS m17_rolled_back_drop_keeps_entries
  FROM fasttrun_cache_stats();
DROP INDEX t_m17_a;
SELECT analyze_entries = :m17_entries_before + 2 AS m17_heap_kept_for_other_index
  FROM fasttrun_cache_stats();
DROP INDEX t_m17_b;
SELECT analyze_entries = :m17_entries_before AS m17_last_index_releases_heap
  FROM fasttrun_cache_stats();
DROP TABLE t_m17;

-- Откат первого сбора на пустой таблице оставляет relstats индекса: пустое
-- хранилище - истина. Вместе с ними остается и запись кучи, через которую
-- реинжект доходит до индекса.
CREATE TEMP TABLE t_m18 (id int);
CREATE INDEX t_m18_id ON t_m18 (id);
SELECT analyze_entries AS m18_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
SELECT fasttrun_collect_stats('t_m18');
ROLLBACK;
SELECT analyze_entries = :m18_entries_before + 2 AS m18_rollback_keeps_heap_with_index
  FROM fasttrun_cache_stats();
DROP TABLE t_m18;
CREATE TEMP TABLE t_m18b (id int);
CREATE INDEX t_m18b_id ON t_m18b (id);
SELECT analyze_entries AS m18b_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
SAVEPOINT m18b;
SELECT fasttrun_collect_stats('t_m18b');
ROLLBACK TO m18b;
COMMIT;
SELECT analyze_entries = :m18b_entries_before + 2 AS m18_savepoint_keeps_heap_with_index
  FROM fasttrun_cache_stats();
DROP TABLE t_m18b;

-- Индекс, созданный в откаченной точке сохранения, уходит вместе со своей
-- записью, и куча не остается его владельцем. Таблиц шесть: порядок обхода
-- записей кучи и индекса зависит от их OID.
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    EXECUTE format('CREATE TEMP TABLE t_m18c_%s (id int)', i);
  END LOOP;
END $$;
SELECT analyze_entries AS m18c_entries_before FROM fasttrun_cache_stats() \gset
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    BEGIN
      EXECUTE format('CREATE INDEX ON t_m18c_%s (id)', i);
      PERFORM fasttrun_collect_stats(format('t_m18c_%s', i));
      RAISE EXCEPTION 'rollback';
    EXCEPTION WHEN raise_exception THEN NULL;
    END;
  END LOOP;
END $$;
SELECT analyze_entries = :m18c_entries_before AS m18_created_index_leaves_no_owner
  FROM fasttrun_cache_stats();
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    EXECUTE format('DROP TABLE t_m18c_%s', i);
  END LOOP;
END $$;

-- Вложенный откат оставляет индексу пустую публикацию, повторный сбор
-- сохраняет кучу уже владельцем, а внешний откат удаляет созданный индекс:
-- куча не остается его владельцем. Таблиц шесть: порядок обхода зависит от
-- их OID.
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    EXECUTE format('CREATE TEMP TABLE t_m19_%s (id int)', i);
  END LOOP;
END $$;
SELECT analyze_entries AS m19_entries_before FROM fasttrun_cache_stats() \gset
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    BEGIN
      EXECUTE format('CREATE INDEX ON t_m19_%s (id)', i);
      BEGIN
        PERFORM fasttrun_collect_stats(format('t_m19_%s', i));
        RAISE EXCEPTION 'inner';
      EXCEPTION WHEN raise_exception THEN NULL;
      END;
      PERFORM fasttrun_collect_stats(format('t_m19_%s', i));
      RAISE EXCEPTION 'outer';
    EXCEPTION WHEN raise_exception THEN NULL;
    END;
  END LOOP;
END $$;
SELECT analyze_entries = :m19_entries_before AS m19_nested_rollback_leaves_no_owner
  FROM fasttrun_cache_stats();
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..6 LOOP
    EXECUTE format('DROP TABLE t_m19_%s', i);
  END LOOP;
END $$;

-- Откат передачи ядерному ANALYZE возвращает публикацию индекса вместе с
-- записью кучи без своих relstats.
CREATE TEMP TABLE t_m19h (id int);
CREATE INDEX t_m19h_id ON t_m19h (id);
SELECT fasttrun_collect_stats('t_m19h');
SELECT analyze_entries AS m19h_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
ANALYZE t_m19h;
ROLLBACK;
SELECT analyze_entries = :m19h_entries_before AS m19_handoff_rollback_keeps_owner,
       (SELECT relpages FROM fasttrun_cached_tables()
         WHERE relid = 't_m19h_id'::regclass) = 1 AS m19_handoff_rollback_index_relstats
  FROM fasttrun_cache_stats();
DROP TABLE t_m19h;

-- Индекс создан во внешнем уровне, а сбор во внутреннем, откаченном. Внешний
-- откат удаляет индекс, и куча, которой нет во внешнем фрейме, не остается
-- его владельцем.
CREATE TEMP TABLE t_m20 (id int);
SELECT analyze_entries AS m20_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
CREATE INDEX t_m20_id ON t_m20 (id);
SAVEPOINT m20;
SELECT fasttrun_collect_stats('t_m20');
ROLLBACK TO m20;
RELEASE SAVEPOINT m20;
ROLLBACK;
SELECT analyze_entries = :m20_entries_before AS m20_outer_rollback_leaves_no_owner
  FROM fasttrun_cache_stats();
DROP TABLE t_m20;

-- Выборку, которую вернет внешний откат, внутренний откат не лишает счета
-- мертвых страниц: рост файла от откаченной вставки ее не прячет.
CREATE TEMP TABLE t_m21 (id int, pad text);
ALTER TABLE t_m21 ALTER COLUMN pad SET STATISTICS 0;
INSERT INTO t_m21 SELECT 42, repeat('x', 64) FROM generate_series(1, 1000);
SELECT fasttrun_collect_stats('t_m21');
BEGIN;
SAVEPOINT m21_outer;
ALTER TABLE t_m21 DROP COLUMN id;
SAVEPOINT m21_inner;
INSERT INTO t_m21 (pad) SELECT repeat('x', 64) FROM generate_series(1, 100000);
ROLLBACK TO m21_inner;
ROLLBACK TO m21_outer;
SELECT column_stats = 1 AND hidden_columns = 0 AS m21_restored_sample_served
  FROM fasttrun_cached_tables() WHERE relid = 't_m21'::regclass;
SELECT pg_temp.an_rows('SELECT * FROM t_m21 WHERE id = 42')::float8 /
       pg_temp.an_rows('SELECT * FROM t_m21') > 0.9
       AS m21_restored_sample_selectivity;
COMMIT;
DROP TABLE t_m21;

-- Внешний DROP INDEX держит публикацию индекса в undo. Откат внутреннего
-- сбора не удаляет запись кучи, через которую реинжект дойдет до индекса
-- после отката DROP.
CREATE TEMP TABLE t_m23 (id int);
CREATE INDEX t_m23_id ON t_m23 (id);
SELECT fasttrun_collect_stats('t_m23');
SELECT analyze_entries AS m23_entries_before FROM fasttrun_cache_stats() \gset
BEGIN;
DROP INDEX t_m23_id;
SAVEPOINT m23;
SELECT fasttrun_collect_stats('t_m23');
ROLLBACK TO m23;
ROLLBACK;
SELECT analyze_entries = :m23_entries_before AS m23_outer_undo_keeps_heap
  FROM fasttrun_cache_stats();
DROP TABLE t_m23;
DROP EXTENSION fasttrun;
