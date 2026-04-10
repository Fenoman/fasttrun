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

-- ----------------------------------------------------------------------
-- 1. Базовый сценарий: вставили данные, вызвали fasttrun_analyze,
--    rd_rel показывает реальный счёт.  pg_class на диске не тронут.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_an (a int);

-- pg_class показывает дефолты, потому что никакого ANALYZE ещё не было
SELECT relpages, reltuples FROM pg_class
  WHERE relname = 't_an' AND relpersistence = 't';

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
SELECT relpages, reltuples FROM pg_class
  WHERE relname = 't_an' AND relpersistence = 't';

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
-- 17. Регрессионный тест: ошибка ленивого режима после SQL TRUNCATE
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

DROP TABLE t_an_trunc;

-- Очистка
DROP TABLE t_an;
DROP TABLE t_an_persist;
DROP EXTENSION fasttrun;
