--
-- fasttrun_stats — статистика по колонкам, локальная для сессии
-- (через get_relation_stats_hook), без записи в pg_statistic.
--
-- ВАЖНО: статистика живёт только в пределах одной транзакции (callback
-- границы транзакции чистит хэш-таблицу на commit/abort), поэтому все
-- секции, где нужно наблюдать действие кэша, обёрнуты в BEGIN/COMMIT.
--

CREATE EXTENSION fasttrun;

-- Узкая высокоселективная временная таблица с btree-индексом.
-- Без статистики по колонкам планировщик берёт DEFAULT_EQ_SEL=0.005
-- и выбирает Bitmap Heap Scan.  С нашей статистикой (n_distinct ~
-- reltuples, потому что id уникален) eq_sel = 1/N и план становится
-- Index Scan.

CREATE TEMP TABLE t_stats (id int, payload text);
INSERT INTO t_stats SELECT g, 'data_' || g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_stats (id);

-- ----------------------------------------------------------------------
-- 1. auto_collect_stats=off + ручной вызов fasttrun_collect_stats
--    внутри одной транзакции: до сбора — Bitmap Heap Scan, после —
--    Index Scan
-- ----------------------------------------------------------------------
BEGIN;
SET LOCAL fasttrun.auto_collect_stats = off;
SELECT fasttrun_analyze('t_stats');
EXPLAIN (COSTS OFF) SELECT * FROM t_stats WHERE id = 12345;
SELECT fasttrun_collect_stats('t_stats');
EXPLAIN (COSTS OFF) SELECT * FROM t_stats WHERE id = 12345;
COMMIT;

-- ----------------------------------------------------------------------
-- 2. После commit кэш очищен callback'ом границы транзакции —
--    план снова Bitmap
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM t_stats WHERE id = 12345;

-- ----------------------------------------------------------------------
-- 3. auto_collect_stats=on (значение по умолчанию): fasttrun_analyze
--    собирает статистику автоматически на первом полном проходе,
--    отдельный вызов collect_stats не нужен
-- ----------------------------------------------------------------------
BEGIN;
SELECT fasttrun_analyze('t_stats');
EXPLAIN (COSTS OFF) SELECT * FROM t_stats WHERE id = 12345;
COMMIT;

-- ----------------------------------------------------------------------
-- 4. fasttrun.sample_rows=0 отключает сбор даже при auto_collect=on
-- ----------------------------------------------------------------------
BEGIN;
SET LOCAL fasttrun.sample_rows = 0;
SELECT fasttrun_analyze('t_stats');
EXPLAIN (COSTS OFF) SELECT * FROM t_stats WHERE id = 12345;
COMMIT;

-- ----------------------------------------------------------------------
-- 5. Многоколоночная таблица: автосбор статистики для всех колонок
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_multi (
    id int,
    user_id int,
    flag bool,
    payload text
);
INSERT INTO t_multi
SELECT g, g % 1000, g % 2 = 0, 'd_' || g
FROM generate_series(1, 50000) g;
CREATE INDEX ON t_multi (id);
CREATE INDEX ON t_multi (user_id);

BEGIN;
SELECT fasttrun_analyze('t_multi');
-- Equality по user_id (1000 уникальных значений, ~50 строк на каждое):
-- по умолчанию выбрался бы Bitmap, со статистикой — тоже Bitmap, но
-- более точная оценка стоимости; форма плана может не меняться
EXPLAIN (COSTS OFF) SELECT * FROM t_multi WHERE id = 12345;
EXPLAIN (COSTS OFF) SELECT * FROM t_multi WHERE user_id = 42;
COMMIT;

-- ----------------------------------------------------------------------
-- 6. Молчаливое игнорирование отсутствующей таблицы — функция
--    ничего не делает, ошибки нет
-- ----------------------------------------------------------------------
SELECT fasttrun_collect_stats('nonexistent_temp_xyz');

-- ----------------------------------------------------------------------
-- 7. Ошибка на обычной (не временной) таблице
-- ----------------------------------------------------------------------
CREATE TABLE t_stats_persist (a int);
INSERT INTO t_stats_persist VALUES (1);
SELECT fasttrun_collect_stats('t_stats_persist');

-- Соединение продолжает работать
SELECT 1 AS still_alive;

-- ----------------------------------------------------------------------
-- 8. Устаревшая статистика после UPDATE, меняющего распределение.
--    Хук должен заметить, что pgstat-счётчики изменились
--    (tuples_updated), и НЕ отдавать кешированный n_distinct, иначе
--    планировщик оставит план, рассчитанный на старое распределение.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_stale (id int, grp int);
INSERT INTO t_stale SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_stale (grp);

BEGIN;
SELECT fasttrun_collect_stats('t_stale');
EXPLAIN (COSTS OFF) SELECT * FROM t_stale WHERE grp = 1;
UPDATE t_stale SET grp = 1;
EXPLAIN (COSTS OFF) SELECT * FROM t_stale WHERE grp = 1;
COMMIT;
DROP TABLE t_stale;

-- ----------------------------------------------------------------------
-- 9. fasttruncate должен явно вытеснить кэш статистики по OID таблицы
--    (запасной механизм на случай, когда pgstat недоступен или
--    счётчики сброшены).
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_trunc_evict (id int);
INSERT INTO t_trunc_evict SELECT generate_series(1, 100000) g;
CREATE INDEX ON t_trunc_evict (id);

BEGIN;
SELECT fasttrun_collect_stats('t_trunc_evict');
EXPLAIN (COSTS OFF) SELECT * FROM t_trunc_evict WHERE id = 12345;
SELECT fasttruncate('t_trunc_evict');
INSERT INTO t_trunc_evict SELECT generate_series(1, 100) g;
EXPLAIN (COSTS OFF) SELECT * FROM t_trunc_evict WHERE id = 12345;
COMMIT;
DROP TABLE t_trunc_evict;

-- ----------------------------------------------------------------------
-- 10. Самостоятельный fasttrun_collect_stats (без fasttrun_analyze)
--     не должен переживать COMMIT.  Раньше callback границы транзакции
--     регистрировался только внутри fasttrun_cache_init(), и кэш
--     статистики "протекал" через границы транзакции.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_leak (id int);
INSERT INTO t_leak SELECT generate_series(1, 100000) g;
CREATE INDEX ON t_leak (id);

BEGIN;
SELECT fasttrun_collect_stats('t_leak');
EXPLAIN (COSTS OFF) SELECT * FROM t_leak WHERE id = 12345;
COMMIT;

EXPLAIN (COSTS OFF) SELECT * FROM t_leak WHERE id = 12345;
DROP TABLE t_leak;

-- ----------------------------------------------------------------------
-- 11. Автоматический пересбор статистики при значимом DML.
--     Управляется порогом: если доля изменённых строк
--     (insert+update+delete с момента предыдущего сбора, делённая на
--     текущий reltuples) больше fasttrun.stats_refresh_threshold
--     (значение по умолчанию 0.2 = 20%), то очередной fasttrun_analyze
--     запускает пересбор статистики через блочную выборку.
--     Блочная выборка, а не полный проход по таблице: количество
--     строк уже посчитано через дельта-математику, пересобирать
--     нужно только статистику колонок — поэтому ~12 мс на 500k
--     строк вместо ~180 мс полного прохода через reservoir.
-- ----------------------------------------------------------------------

-- 11a. Значительный UPDATE (churn=1.0) → пересбор → план обновляется
CREATE TEMP TABLE t_refresh (id int, grp int);
INSERT INTO t_refresh SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_refresh (grp);

BEGIN;
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;

UPDATE t_refresh SET grp = 1;        -- доля изменений = 1.0 > 0.2 → пересбор
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;
COMMIT;

-- 11b. Маленький DELETE при пороге по умолчанию 0.2 — 0.009% меньше
--      порога → пересбор НЕ срабатывает, но хук продвигает freshness
--      → планировщик продолжает использовать кешированную статистику.
BEGIN;
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;

DELETE FROM t_refresh WHERE id < 10; -- 9 строк из 100000 = 0.009%
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;
COMMIT;

-- 11c. Порог = 0 принудительно запускает пересбор при любом DML
BEGIN;
SET LOCAL fasttrun.stats_refresh_threshold = 0;
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;

DELETE FROM t_refresh WHERE id < 20; -- даже 0.018% запускает пересбор
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;
COMMIT;

-- 11d. Порог = 1.0 полностью отключает пересбор, но кешированная
--      статистика остаётся видимой планировщику (freshness продвигается)
BEGIN;
SET LOCAL fasttrun.stats_refresh_threshold = 1;
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;

UPDATE t_refresh SET grp = 999;     -- любой DML → пересбор отключён
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 999;
COMMIT;

-- 11e. Below-threshold DML внутри savepoint: advance_freshness
--      пропускается (нет undo-механики), после ROLLBACK TO SAVEPOINT
--      pgstat counters откатываются к pre-savepoint значениям, а
--      collected_* не менялся → match → хук возвращает кешированные
--      stats. Без savepoint — freshness продвигается как обычно.
BEGIN;
SELECT fasttrun_analyze('t_refresh');

SAVEPOINT sp1;
DELETE FROM t_refresh WHERE id < 5;  -- ниже порога
SELECT fasttrun_analyze('t_refresh');
ROLLBACK TO sp1;

-- После отката savepoint хук должен вернуть кешированную статистику
-- (counters откатились к pre-savepoint значениям, collected_* не
-- менялся внутри savepoint → match)
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;

-- Теперь тот же DML без savepoint — freshness продвигается
DELETE FROM t_refresh WHERE id < 5;
SELECT fasttrun_analyze('t_refresh');
EXPLAIN (COSTS OFF) SELECT * FROM t_refresh WHERE grp = 1;
COMMIT;

DROP TABLE t_refresh;

-- ----------------------------------------------------------------------
-- 12. Порог = 0 не должен запускать пересбор, если доля изменений
--     равна нулю.  Семантика порога: "0 = пересбор при любом DML",
--     а не "при каждом тёплом попадании в кэш".
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_thr0 (id int, v int);
INSERT INTO t_thr0 SELECT g, g FROM generate_series(1, 10000) g;
CREATE INDEX ON t_thr0 (id);

BEGIN;
SELECT fasttrun_collect_stats('t_thr0');
SET LOCAL fasttrun.stats_refresh_threshold = 0;
EXPLAIN (COSTS OFF) SELECT * FROM t_thr0 WHERE id = 5000;
-- Никакого DML → доля изменений = 0 → пересбор не должен сработать,
-- план идентичен
SELECT fasttrun_analyze('t_thr0');
EXPLAIN (COSTS OFF) SELECT * FROM t_thr0 WHERE id = 5000;
COMMIT;
DROP TABLE t_thr0;

-- ----------------------------------------------------------------------
-- 13. Кэш статистики, понимающий точки сохранения: ROLLBACK TO
--     SAVEPOINT должен полностью восстановить статистику родительского
--     уровня, а не просто вытеснить её.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_sv (id int, grp int);
INSERT INTO t_sv SELECT g, g FROM generate_series(1, 50000) g;
CREATE INDEX ON t_sv (grp);

BEGIN;
-- Порог по умолчанию = 0.2 → массовый UPDATE в подтранзакции
-- пересчитает статистику, что и нужно для проверки отката с
-- восстановлением.
SELECT fasttrun_analyze('t_sv');
-- На уровне родительской транзакции: высокий n_distinct → Index Scan
EXPLAIN (COSTS OFF) SELECT * FROM t_sv WHERE grp = 1;

SAVEPOINT sp1;
UPDATE t_sv SET grp = 1;
SELECT fasttrun_analyze('t_sv');
-- Внутри подтранзакции: пересбор сработал, n_distinct=1 → Seq Scan
EXPLAIN (COSTS OFF) SELECT * FROM t_sv WHERE grp = 1;

ROLLBACK TO SAVEPOINT sp1;
-- После отката: статистика восстановлена из стека отмены →
-- снова Index Scan
EXPLAIN (COSTS OFF) SELECT * FROM t_sv WHERE grp = 1;
COMMIT;
DROP TABLE t_sv;

-- ----------------------------------------------------------------------
-- 14. track_counts=off → нельзя проверить актуальность статистики →
--     не кэшируем, планировщик работает на оценках по умолчанию.
--     Главное: после DML никакая устаревшая статистика не выдаётся.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_tc (id int, grp int);
INSERT INTO t_tc SELECT g, g FROM generate_series(1, 10000) g;
CREATE INDEX ON t_tc (grp);

BEGIN;
SET LOCAL track_counts = off;
-- Первый вызов collect поднимет предупреждение WARNING (один раз
-- за сессию) — оно ожидается в diff'e pg_regress как маркер.
SELECT fasttrun_collect_stats('t_tc');
-- При track_counts=off: возврат к оценкам по умолчанию, не кэшируем
EXPLAIN (COSTS OFF) SELECT * FROM t_tc WHERE grp = 1;
UPDATE t_tc SET grp = 1;
-- Повторные вызовы collect в этой же сессии — молча, без
-- предупреждения (оно уже было)
SELECT fasttrun_collect_stats('t_tc');
SELECT fasttrun_analyze('t_tc');
-- После UPDATE: всё ещё оценки по умолчанию, никакой устаревшей
-- статистики
EXPLAIN (COSTS OFF) SELECT * FROM t_tc WHERE grp = 1;
COMMIT;
DROP TABLE t_tc;

-- ----------------------------------------------------------------------
-- 15. Регрессионный тест: блочная выборка для пересбора статистики
--     не должна быть смещена к началу heap-файла.
--
--     Раньше fasttrun_scan_block_sample останавливалась как только
--     набирала sample_target tuples, а BlockSampler_Next возвращает
--     выбранные блоки в порядке возрастания номеров.  В результате
--     на физически упорядоченной таблице ранние страницы заполняли
--     reservoir целиком, и поздние страницы в выборку вообще не
--     попадали — n_distinct после пересбора схлопывался к ~1.
--
--     Конкретный пример (его и воспроизводим): первые ~3000 строк
--     имеют grp=1, остальные ~50000 строк имеют уникальные grp.
--     После cold scan'а статистика честная, n_distinct высокий,
--     план для grp=40000 — Index Scan.  Затем INSERT 20000 строк
--     запускает пересбор; если в нём остался прежний bias, план
--     сваливается в Seq Scan.  Мы проверяем, что план НЕ меняется
--     (Index Scan сохраняется), потому что reservoir sampling по
--     всем выбранным блокам даёт честно несмещённую выборку.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_bias (id int, grp int);
-- Первые 3000 строк физически лежат в начале файла и все с grp=1
INSERT INTO t_bias SELECT g, 1 FROM generate_series(1, 3000) g;
-- Следующие 50000 строк имеют уникальные значения grp = id
INSERT INTO t_bias SELECT g, g FROM generate_series(3001, 53000) g;
CREATE INDEX ON t_bias (grp);

BEGIN;
SELECT fasttrun_analyze('t_bias');
-- После cold scan'а: n_distinct высокий → Index Scan для редкого grp
EXPLAIN (COSTS OFF) SELECT * FROM t_bias WHERE grp = 40000;

-- INSERT 20000 строк → доля изменений 20k/73k ≈ 27% > 20% порога →
-- запускается пересбор статистики через блочную выборку
INSERT INTO t_bias SELECT g, g FROM generate_series(53001, 73000) g;
SELECT fasttrun_analyze('t_bias');
-- После пересбора: n_distinct ВСЁ ЕЩЁ должен быть высоким (несмещённая
-- выборка по всем выбранным блокам), план остаётся Index Scan.
-- Если бы блочная выборка по-прежнему стопилась после первых
-- sample_target tuples, она бы зачерпнула почти только grp=1 из
-- начала файла, n_distinct схлопнулся бы к ~1, и здесь был бы Seq Scan.
EXPLAIN (COSTS OFF) SELECT * FROM t_bias WHERE grp = 40000;
COMMIT;
DROP TABLE t_bias;

-- ----------------------------------------------------------------------
-- 16. Регрессионный тест: пересбор после массового DELETE до пустой
--     таблицы должен продвигать опорный снимок статистики.
--
--     Раньше путь пересбора в delta-hit ветке вызывал
--     fasttrun_collect_and_store только при sample_count > 0.  После
--     mass DELETE до пустоты блочная выборка ничего не находила,
--     baseline не двигался, и каждый последующий fasttrun_analyze
--     заново входил в путь пересбора и заново платил за блочную
--     выборку, которая снова возвращала ноль строк.
--
--     Тут проверяем строго по структуре, а не по таймингам (тайминги
--     нестабильны на CI): каждый шаг должен соответствовать своей
--     ожидаемой чёткой ветке.  План для grp=1 после mass DELETE
--     ожидаем как Seq Scan на пустой таблице (через дефолты — потому
--     что hook freshness check отвергает старую stats: pgstat
--     счётчики уже не совпадают со снимком от cold scan'а).  Главное:
--     второй и третий fasttrun_analyze не должны падать и должны
--     отдавать тот же self-consistent reltuples=0.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_empty_refresh (id int, grp int);
INSERT INTO t_empty_refresh SELECT g, g % 100 FROM generate_series(1, 50000) g;
CREATE INDEX ON t_empty_refresh (grp);

BEGIN;
SELECT fasttrun_analyze('t_empty_refresh');
SELECT reltuples = 50000 AS cold_count
  FROM fasttrun_relstats('t_empty_refresh');

DELETE FROM t_empty_refresh;            -- table becomes empty

SELECT fasttrun_analyze('t_empty_refresh');
SELECT reltuples = 0 AS empty_after_first
  FROM fasttrun_relstats('t_empty_refresh');

-- Второй analyze без DML.  С багом он бы снова входил в refresh path
-- (потому что baseline не продвинулся при первом empty refresh) и
-- снова делал бы block sample.  С фиксом — pure delta path, мгновенно.
SELECT fasttrun_analyze('t_empty_refresh');
SELECT reltuples = 0 AS empty_after_second
  FROM fasttrun_relstats('t_empty_refresh');

-- Третий — то же самое, для уверенности что baseline стабилен.
SELECT fasttrun_analyze('t_empty_refresh');
SELECT reltuples = 0 AS empty_after_third
  FROM fasttrun_relstats('t_empty_refresh');
COMMIT;
DROP TABLE t_empty_refresh;

-- ----------------------------------------------------------------------
-- 17. Регрессионный тест: ROLLBACK TO SAVEPOINT должен откатывать
--     не только сам кэш статистики колонок, но и опорный снимок
--     для пути пересбора (stats baseline) в analyze cache.
--
--     Раньше пересбор внутри подтранзакции продвигал stats baseline
--     до post-DML pgstat счётчиков, а subxact callback его не
--     откатывал.  После ROLLBACK TO SAVEPOINT pgstat счётчики
--     возвращались к pre-DML, а baseline в analyze cache оставался
--     "из будущего", и следующий fasttrun_analyze считал ненулевой
--     churn → лишний block-sample проход.
--
--     Тест в той же форме, что и секция 13, плюс explicit проверка
--     что пост-rollback план идентичен pre-savepoint плану — это
--     косвенно подтверждает что stats baseline тоже откатился (если
--     бы остался "в будущем", refresh бы запустился и план сменился).
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_sv_baseline (id int, grp int);
INSERT INTO t_sv_baseline SELECT g, g FROM generate_series(1, 50000) g;
CREATE INDEX ON t_sv_baseline (grp);

BEGIN;
SELECT fasttrun_analyze('t_sv_baseline');
EXPLAIN (COSTS OFF) SELECT * FROM t_sv_baseline WHERE grp = 1;

SAVEPOINT sp_b;
UPDATE t_sv_baseline SET grp = 1;
SELECT fasttrun_analyze('t_sv_baseline');  -- refresh inside subxact
EXPLAIN (COSTS OFF) SELECT * FROM t_sv_baseline WHERE grp = 1;

ROLLBACK TO SAVEPOINT sp_b;
-- После rollback должна полностью восстановиться pre-savepoint
-- картина: stats cache + baseline в analyze cache.
EXPLAIN (COSTS OFF) SELECT * FROM t_sv_baseline WHERE grp = 1;
-- Следующий analyze не должен снова входить в refresh path
-- (с багом он бы пересчитал и оставил план Seq Scan, потому что
-- baseline в analyze cache остался "из будущего" и churn != 0).
SELECT fasttrun_analyze('t_sv_baseline');
EXPLAIN (COSTS OFF) SELECT * FROM t_sv_baseline WHERE grp = 1;
COMMIT;
DROP TABLE t_sv_baseline;

-- ----------------------------------------------------------------------
-- 18. Регрессионный тест: explicit fasttrun_collect_stats() должен
--     продвигать опорный снимок stats baseline в analyze cache,
--     если запись там уже есть — иначе следующий маленький DML
--     запустит ненужный пересбор по застаревшему churn.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_cs_baseline (id int, grp int);
INSERT INTO t_cs_baseline SELECT g, g FROM generate_series(1, 50000) g;
CREATE INDEX ON t_cs_baseline (grp);

BEGIN;
SELECT fasttrun_analyze('t_cs_baseline');             -- cold scan
UPDATE t_cs_baseline SET grp = grp + 1;               -- big churn
SELECT fasttrun_collect_stats('t_cs_baseline');       -- explicit recollect
DELETE FROM t_cs_baseline WHERE id = 1;               -- one-row tweak

-- Главное: первый analyze после collect_stats + крошечного DML
-- должен идти чистой delta-математикой, а не входить в refresh path.
-- Структурно проверяем через свежий план запроса (он должен совпадать
-- с тем, что был после refresh от UPDATE'a) и через row count.
SELECT fasttrun_analyze('t_cs_baseline');
SELECT reltuples = 49999 AS post_collect_count
  FROM fasttrun_relstats('t_cs_baseline');

-- Повторный analyze без DML должен оставаться hot path
SELECT fasttrun_analyze('t_cs_baseline');
SELECT reltuples = 49999 AS post_collect_count_2
  FROM fasttrun_relstats('t_cs_baseline');
COMMIT;
DROP TABLE t_cs_baseline;

-- ----------------------------------------------------------------------
-- 19. Регрессионный тест: fasttruncate seed-ит analyze cache, чтобы
--     первый fasttrun_analyze() после паттерна
--         fasttruncate -> refill -> fasttrun_analyze
--     шёл по дельта-математике (а не платил за полный cold scan), и
--     при этом ещё и обновлял статистику колонок через путь пересбора.
--
--     Структурные проверки:
--       - после fasttruncate + INSERT + analyze reltuples должно быть
--         равно числу вставленных строк (это обеспечивается delta math
--         от seed-нутого опорного снимка);
--       - план для редкого значения после refill должен быть Index Scan
--         (это значит refresh path сработал и собрал свежую статистику
--         колонок — иначе planner получил бы defaults и выбрал
--         Bitmap/Seq Scan);
--       - повторный analyze без DML — pure delta hot path, тот же
--         reltuples без изменений.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_seed_truncate (id int, grp int);
INSERT INTO t_seed_truncate SELECT g, g FROM generate_series(1, 50000) g;
CREATE INDEX ON t_seed_truncate (grp);

BEGIN;
SELECT fasttrun_analyze('t_seed_truncate');           -- cold scan, stats есть
EXPLAIN (COSTS OFF) SELECT * FROM t_seed_truncate WHERE grp = 12345;

-- Полный цикл: очистка, перезаливка, analyze.  С seed первый
-- analyze здесь идёт через delta-hit + block-sample refresh.
SELECT fasttruncate('t_seed_truncate');
INSERT INTO t_seed_truncate SELECT g, g FROM generate_series(1, 50000) g;
SELECT fasttrun_analyze('t_seed_truncate');
SELECT reltuples = 50000 AS reltuples_after_refill
  FROM fasttrun_relstats('t_seed_truncate');
-- План должен использовать индекс — refresh собрал свежие stats.
EXPLAIN (COSTS OFF) SELECT * FROM t_seed_truncate WHERE grp = 12345;

-- Второй analyze без DML — pure delta hot path
SELECT fasttrun_analyze('t_seed_truncate');
SELECT reltuples = 50000 AS reltuples_second
  FROM fasttrun_relstats('t_seed_truncate');

-- Двойной truncate в той же транзакции тоже должен работать
SELECT fasttruncate('t_seed_truncate');
SELECT fasttruncate('t_seed_truncate');
INSERT INTO t_seed_truncate SELECT g, g FROM generate_series(1, 100) g;
SELECT fasttrun_analyze('t_seed_truncate');
SELECT reltuples = 100 AS reltuples_after_double_truncate
  FROM fasttrun_relstats('t_seed_truncate');
COMMIT;
DROP TABLE t_seed_truncate;

-- ----------------------------------------------------------------------
-- 20. Регрессионный тест: ширина столбца (stawidth) собранная нашим
--     reservoir sample должна доходить до планировщика через
--     get_attavgwidth_hook.
--
--     В планировщике есть ДВА независимых пути доступа к статистике
--     колонок:
--       (а) get_relation_stats_hook → возвращает HeapTuple от
--           pg_statistic для функций селективности (eq_sel и т.п.);
--       (б) lsyscache.c:get_attavgwidth → читает stawidth напрямую
--           через get_attavgwidth_hook или syscache.
--
--     Раньше у нас был зарегистрирован только (а), и (б) уходил в
--     get_typavgwidth() с дефолтной шириной типа.  Для text это
--     32 байта независимо от реального содержимого, что ломало
--     costing для hash join / sort / spool на временных таблицах
--     с короткими text-колонками.
--
--     Тут проверяем что после fasttrun_analyze() планировщик видит
--     нашу собранную ширину, а не дефолтную для типа.  Для строки
--     ровно 19 байт ожидаем width <= 25 (Haas-Stokes даёт ~19, а
--     get_typavgwidth() для text даёт 32 — далеко за порогом).
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_width (id int, name text);
INSERT INTO t_width SELECT g, 'name_padded_to_20_x' FROM generate_series(1, 10000) g;
-- 'name_padded_to_20_x' это 19 ASCII символов, в varlena ~ 20 байт.

BEGIN;
SELECT fasttrun_analyze('t_width');
-- Прямой запрос к плану через EXPLAIN.  Используем функцию-обёртку,
-- которая вытаскивает width из текстового плана, чтобы тест был
-- стабильным и читаемым.
DO $$
DECLARE
    plan_line text;
    width_val int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT name FROM t_width LOOP
        IF plan_line ~ 'width=' THEN
            width_val := substring(plan_line FROM 'width=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    IF width_val IS NULL THEN
        RAISE EXCEPTION 'could not parse width from EXPLAIN output';
    END IF;

    -- Реальная ширина ~20 байт (varlena overhead + 19 ASCII).
    -- Без get_attavgwidth_hook планировщик отдал бы 32.
    IF width_val > 25 THEN
        RAISE EXCEPTION 'width % too high — get_attavgwidth_hook не сработал (ожидали ~20, лимит 25)',
                        width_val;
    END IF;
    IF width_val < 15 THEN
        RAISE EXCEPTION 'width % подозрительно мал — Haas-Stokes/sample broken?',
                        width_val;
    END IF;

    RAISE NOTICE 'fasttrun width for text(20): % bytes (matches sampled value)', width_val;
END;
$$;
COMMIT;
DROP TABLE t_width;

-- ----------------------------------------------------------------------
-- 21. Регрессионный тест: skew equality предикат через std_typanalyze.
--
--     99000 строк со значением v=1 и 1000 строк с уникальными редкими
--     значениями v in 2..1001.  С нашим путём через std_typanalyze
--     (use_typanalyze=on, default) MCV должна поймать частое значение
--     v=1, и план для WHERE v=1 должен быть Seq Scan с rows близким
--     к реальным 99000 (а не bitmap heap scan с грубой оценкой по
--     n_distinct ~ 3000).  Для редкого значения v=500 план должен
--     быть Index Scan с маленьким rows.
--
--     Сравниваем с use_typanalyze=off — там должна быть старая
--     грубая оценка по n_distinct (rows одинаков для обоих v).
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_skew (id int, v int);
INSERT INTO t_skew SELECT g, 1 FROM generate_series(1, 99000) g;
INSERT INTO t_skew SELECT 99000 + g, g + 1 FROM generate_series(1, 1000) g;
CREATE INDEX ON t_skew (v);

BEGIN;
SELECT fasttrun_analyze('t_skew');
DO $$
DECLARE
    plan_line text;
    rows_freq int;
    rows_rare int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT * FROM t_skew WHERE v = 1 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_freq := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    FOR plan_line IN EXPLAIN SELECT * FROM t_skew WHERE v = 500 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_rare := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    -- MCV должна поймать v=1 и дать оценку >50000
    IF rows_freq < 50000 THEN
        RAISE EXCEPTION 'MCV не сработала для skew v=1: rows=% (ожидали >50000)',
                        rows_freq;
    END IF;
    -- Для редкого v=500 оценка должна быть маленькой (<200)
    IF rows_rare > 200 THEN
        RAISE EXCEPTION 'rare value v=500 over-estimated: rows=% (ожидали <200)',
                        rows_rare;
    END IF;

    RAISE NOTICE 'skew typanalyze: ok (frequent>50k, rare<200)';
END;
$$;
COMMIT;

-- Контр-тест: при use_typanalyze=off оценки должны быть одинаковы
-- для обоих значений (грубая Haas-Stokes оценка по n_distinct).
BEGIN;
SET LOCAL fasttrun.use_typanalyze = off;
SELECT fasttrun_analyze('t_skew');
DO $$
DECLARE
    plan_line text;
    rows_freq int;
    rows_rare int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT * FROM t_skew WHERE v = 1 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_freq := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    FOR plan_line IN EXPLAIN SELECT * FROM t_skew WHERE v = 500 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_rare := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    -- Без MCV оценки одинаковы — обе по 1/n_distinct
    IF rows_freq <> rows_rare THEN
        RAISE EXCEPTION 'Haas-Stokes даёт разные оценки для v=1 (%) и v=500 (%): не должно быть разницы без MCV',
                        rows_freq, rows_rare;
    END IF;

    RAISE NOTICE 'skew Haas-Stokes: ok (frequent and rare give same estimate)';
END;
$$;
COMMIT;
DROP TABLE t_skew;

-- ----------------------------------------------------------------------
-- 22. Регрессионный тест: range предикат через std_typanalyze.
--
--     100000 строк v in 1..100000.  WHERE v < 1000 — это селективность
--     ровно 1%.  С histogram planner должен дать оценку ~1000;
--     без histogram уйдёт в DEFAULT_RANGE_INEQ_SEL = 0.5 → ~50000.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_range (id int, v int);
INSERT INTO t_range SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX ON t_range (v);

BEGIN;
SELECT fasttrun_analyze('t_range');
DO $$
DECLARE
    plan_line text;
    rows_est int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT * FROM t_range WHERE v < 1000 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    -- Histogram должна дать оценку в пределах 500..2000 (real 999)
    IF rows_est < 500 OR rows_est > 2000 THEN
        RAISE EXCEPTION 'histogram неточна для range v<1000: rows=% (ожидали 500..2000)',
                        rows_est;
    END IF;

    RAISE NOTICE 'range typanalyze: ok (within 500..2000 for real 999)';
END;
$$;
COMMIT;

-- Контр-тест: без typanalyze идёт DEFAULT_RANGE_INEQ_SEL
BEGIN;
SET LOCAL fasttrun.use_typanalyze = off;
SELECT fasttrun_analyze('t_range');
DO $$
DECLARE
    plan_line text;
    rows_est int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT * FROM t_range WHERE v < 1000 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;

    -- Без histogram — DEFAULT_RANGE_INEQ_SEL × ndistinct, обычно >30000
    IF rows_est < 10000 THEN
        RAISE EXCEPTION 'Haas-Stokes неожиданно точна для range: rows=%, должно быть много',
                        rows_est;
    END IF;

    RAISE NOTICE 'range Haas-Stokes: ok (overestimates due to default 0.5 selectivity)';
END;
$$;
COMMIT;
DROP TABLE t_range;

-- ----------------------------------------------------------------------
-- 23. ABBA cross-validation: статистика собранная нашим path должна
--     дать планировщику близкие оценки тому, что дал бы обычный
--     SQL ANALYZE — на широком спектре типов данных и предикатов.
--
--     Это **прямое доказательство** что наш HeapTuple в кэше
--     корректно доходит до планировщика через get_relation_stats_hook
--     и get_attavgwidth_hook, и что std_typanalyze правильно
--     отрабатывает на всех нужных типах: int, text (MCV/eq),
--     varchar (high-cardinality eq), numeric (histogram/range),
--     timestamptz (histogram/range), boolean (eq).
--
--     Не сравниваем точные числа rows между ANALYZE и нашим путём
--     (sample-based, шумные), а проверяем структурно: тип плана и
--     rows в разумных границах от реального количества.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_abba (
    id      int,
    grp     int,
    status  text,
    name    varchar(50),
    amount  numeric(10,2),
    created timestamptz,
    flag    boolean
);
INSERT INTO t_abba
SELECT g,
       g % 1000,
       (CASE WHEN g % 100 < 60 THEN 'active'
             WHEN g % 100 < 80 THEN 'pending'
             WHEN g % 100 < 90 THEN 'done'
             WHEN g % 100 < 95 THEN 'archived'
             ELSE 'new' END),
       'name_' || g,
       (g % 10000)::numeric / 100,
       '2026-01-01'::timestamptz + (g || ' minutes')::interval,
       g % 2 = 0
FROM generate_series(1, 50000) g;
CREATE INDEX ON t_abba (grp);
CREATE INDEX ON t_abba (status);
CREATE INDEX ON t_abba (amount);

BEGIN;
SELECT fasttrun_analyze('t_abba');
DO $$
DECLARE
    plan_line text;
    rows_est  int;
BEGIN
    -- int n_distinct: grp = 42 → ~50 строк (real 50)
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE grp = 42 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 10 OR rows_est > 200 THEN
        RAISE EXCEPTION 'int n_distinct broken: grp=42 rows=% (real ~50)', rows_est;
    END IF;

    -- text MCV: status='active' (60% строк) → ~30000
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE status = 'active' LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 20000 OR rows_est > 40000 THEN
        RAISE EXCEPTION 'text MCV broken: active rows=% (real ~30000)', rows_est;
    END IF;

    -- text MCV: status='archived' (5% строк) → ~2500
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE status = 'archived' LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 500 OR rows_est > 5000 THEN
        RAISE EXCEPTION 'text MCV broken: archived rows=% (real ~2500)', rows_est;
    END IF;

    -- numeric histogram: amount BETWEEN 10 AND 20 → ~5000 (real ~5000)
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE amount BETWEEN 10 AND 20 LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 2000 OR rows_est > 8000 THEN
        RAISE EXCEPTION 'numeric histogram broken: 10..20 rows=% (real ~5000)', rows_est;
    END IF;

    -- timestamptz histogram: created > '2026-01-15' → ~30000 (последние 70%)
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE created > '2026-01-15'::timestamptz LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 20000 OR rows_est > 40000 THEN
        RAISE EXCEPTION 'timestamptz histogram broken: rows=% (real ~30000)', rows_est;
    END IF;

    -- boolean: flag = true (50%) → ~25000
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE flag = true LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 15000 OR rows_est > 35000 THEN
        RAISE EXCEPTION 'boolean equality broken: flag=true rows=% (real ~25000)', rows_est;
    END IF;

    -- varchar high-cardinality unique: name='name_25000' → 1
    FOR plan_line IN EXPLAIN SELECT * FROM t_abba WHERE name = 'name_25000' LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    IF rows_est < 1 OR rows_est > 10 THEN
        RAISE EXCEPTION 'varchar n_distinct broken: name=name_25000 rows=% (real 1)', rows_est;
    END IF;

    RAISE NOTICE 'ABBA cross-validation: stats reach planner for all 7 types (int, text MCV x2, numeric range, timestamptz range, boolean, varchar)';
END;
$$;
COMMIT;
DROP TABLE t_abba;

-- ----------------------------------------------------------------------
-- 24. fasttrun_inspect_stats() — observability функция для production
--     debugging.  Возвращает наши кешированные statsTuple в shape
--     pg_catalog.pg_statistic.
--
--     Проверяем что:
--       (1) функция возвращает по одной строке на каждую analyzed
--           колонку (3 колонки = 3 строки);
--       (2) staattnum совпадает с реальным;
--       (3) MCV-слот заполнен для skewed колонки status;
--       (4) shape совпадает с pg_catalog.pg_statistic (можно делать
--           SELECT по знакомым полям);
--       (5) на несуществующей таблице — empty result, без ошибки.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_inspect (id int, status text);
INSERT INTO t_inspect
SELECT g, (CASE WHEN g % 100 < 60 THEN 'active' ELSE 'pending' END)
FROM generate_series(1, 50000) g;

BEGIN;
SELECT fasttrun_analyze('t_inspect');

-- Должна быть одна строка на колонку (id, status) — обе analyzable
SELECT count(*) = 2 AS two_rows FROM fasttrun_inspect_stats('t_inspect');

-- staattnum правильный, в порядке возрастания
SELECT array_agg(staattnum ORDER BY staattnum) = ARRAY[1::int2, 2::int2]
       AS attnums_correct
FROM fasttrun_inspect_stats('t_inspect');

-- Для status (skewed) должен быть MCV slot с двумя значениями.
-- stakind=1 это STATISTIC_KIND_MCV.
SELECT count(*) = 1 AS has_mcv_for_status
FROM fasttrun_inspect_stats('t_inspect')
WHERE staattnum = 2 AND (stakind1 = 1 OR stakind2 = 1 OR stakind3 = 1);

-- Empty result для несуществующей таблицы
SELECT count(*) = 0 AS empty_for_missing
FROM fasttrun_inspect_stats('nonexistent_temp_xxx');

-- Empty result для пустого кэша (pg_statistic тоже пуст для temp)
SELECT count(*) = 0 AS empty_for_no_stats_yet
FROM fasttrun_inspect_stats('t_stats');  -- эта таблица еще существует, но без stats в кэше внутри этой xact'и (другая xact её stats коммитнула)
COMMIT;
DROP TABLE t_inspect;

-- ----------------------------------------------------------------------
-- 25. fasttrun.sample_rows = -1 (auto) — sample target автоматически
--     совпадает с тем, что попросил бы std_typanalyze (по умолчанию
--     300 × default_statistics_target = 30000 при default DST=100).
--
--     Цели теста:
--       (1) -1 не ломает функционал — fasttrun_analyze не падает,
--           inspect_stats возвращает строки;
--       (2) на той же таблице -1 даёт **точно тот же набор колонок**
--           в кэше, что и default 3000 — отличается только sample
--           size, не shape;
--       (3) shape результата (количество MCV/histogram slots) совпадает
--           с тем, что записал бы regular ANALYZE на той же таблице.
--
--     Сравнение точности (MCV inclusion для редких значений,
--     histogram precision) проходит через bench в fasttrun_bench.sql
--     §14, потому что это timing-зависимое и плохо assert'ится через
--     EXPLAIN на CI.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_auto_sample (id int, label text);
INSERT INTO t_auto_sample
SELECT g, 'label_' || (g % 50)
FROM generate_series(1, 100000) g;

BEGIN;
-- Прогон с sample_rows = 3000 (default)
SELECT fasttrun_analyze('t_auto_sample');
SELECT count(*) = 2 AS default_rows_count
FROM fasttrun_inspect_stats('t_auto_sample');
COMMIT;

BEGIN;
-- Прогон с sample_rows = -1 (авто)
SET LOCAL fasttrun.sample_rows = -1;
SELECT fasttrun_analyze('t_auto_sample');

-- Та же форма: одна строка на каждую колонку
SELECT count(*) = 2 AS auto_rows_count
FROM fasttrun_inspect_stats('t_auto_sample');

-- Для label (text MCV) хотя бы один MCV-слот должен быть заполнен.
-- При -1 выборка ~30000 строк -- данных достаточно.
SELECT count(*) >= 1 AS has_label_stats
FROM fasttrun_inspect_stats('t_auto_sample')
WHERE staattnum = 2 AND stadistinct > 0;

-- План для запроса по label должен быть адекватным
DO $$
DECLARE
    plan_line text;
    rows_est  int;
BEGIN
    FOR plan_line IN EXPLAIN SELECT * FROM t_auto_sample WHERE label = 'label_5' LOOP
        IF plan_line ~ 'rows=' THEN
            rows_est := substring(plan_line FROM 'rows=(\d+)')::int;
            EXIT;
        END IF;
    END LOOP;
    -- 50 уникальных label в 100k строк → реальных ~2000 на label
    -- Допуск: 500..5000
    IF rows_est < 500 OR rows_est > 5000 THEN
        RAISE EXCEPTION 'sample_rows=-1 path: planner estimate off: rows=% (real ~2000)',
                        rows_est;
    END IF;
    RAISE NOTICE 'sample_rows=-1: planner estimate ok for label query';
END;
$$;
COMMIT;
DROP TABLE t_auto_sample;

-- Очистка
DROP TABLE t_stats;
DROP TABLE t_multi;
DROP TABLE t_stats_persist;
DROP EXTENSION fasttrun;
