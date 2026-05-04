--
-- fasttrun_relstats_survive — regression-тест: relpages/reltuples
-- должны переживать перестроение relcache после инвалидации sinval.
--
-- Суть бага: fasttrun_analyze() пишет relpages/reltuples только в rd_rel
-- (в памяти процесса), в pg_class ничего не идёт.  Когда из общей очереди
-- sinval приходит сообщение, которое триггерит перестроение relcache,
-- rd_rel пересобирается из pg_class (а там 0/-1), и планировщик видит
-- 0 страниц → Seq Scan по "пустой" таблице → катастрофический
-- Nested Loop на реальной нагрузке.
--
-- Статистика по колонкам выживает (отдаётся через get_relation_stats_hook
-- из нашей хэш-таблицы), а вот relpages/reltuples — нет, они живут
-- ТОЛЬКО в rd_rel.
--
-- Фикс: planner_hook реинжектит cached_pages/cached_tuples из
-- fasttrun_analyze_cache в rd_rel перед каждым планированием.
-- fasttrun_relstats() тоже делает re-inject напрямую, чтобы SQL-уровневая
-- инспекция видела правду после инвалидации.
--
-- ВАЖНО: индекс создаётся ДО INSERT — иначе CREATE INDEX после INSERT
-- вызвал бы index_update_stats и записал бы реальные (relpages, reltuples)
-- в pg_class.  После этого перестроение relcache прочитало бы правильные
-- значения и замаскировало бы баг.
--

CREATE EXTENSION fasttrun;

-- ----------------------------------------------------------------------
-- 1. Подготовка: временная таблица, индекс до INSERT, потом данные.
--    Такой порядок гарантирует, что pg_class хранит (relpages=0,
--    reltuples=-1) для heap-отношения — без этого баг не воспроизводится.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_survive (id int, payload text);
CREATE INDEX ON t_survive (id);
INSERT INTO t_survive SELECT g, 'data_' || g FROM generate_series(1, 1000) g;

-- Проверяем предусловие: pg_class до сих пор показывает дефолты пустой таблицы
SELECT relpages = 0 AS pgclass_zero_pages,
       reltuples = -1 AS pgclass_default_tuples
  FROM pg_class WHERE oid = 't_survive'::regclass;

BEGIN;

-- ----------------------------------------------------------------------
-- 2. После fasttrun_analyze rd_rel содержит реальные pages/tuples
-- ----------------------------------------------------------------------
SELECT fasttrun_analyze('t_survive');

SELECT relpages > 0 AS has_pages,
       reltuples = 1000 AS exact_tuples
  FROM fasttrun_relstats('t_survive');

-- ----------------------------------------------------------------------
-- 3. Симулируем перестроение relcache через touch pg_class.
--    UPDATE триггерит CacheInvalidateHeapTuple → инвалидация relcache
--    нашей temp таблицы → rd_rel пересобирается из pg_class →
--    relpages/reltuples сбрасываются в (0, -1).
-- ----------------------------------------------------------------------
UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_survive'::regclass;

-- Любой запрос форсит AcceptInvalidationMessages, завершая перестроение.
-- Без фикса rd_rel теперь (0, -1).
SELECT 1 AS force_inval;

-- ----------------------------------------------------------------------
-- 4. После перестроения relcache fasttrun_relstats всё равно должна
--    показывать корректные значения: fasttrun_relstats fix делает
--    re-inject из кэша.
-- ----------------------------------------------------------------------
SELECT relpages > 0 AS pages_survive,
       reltuples = 1000 AS tuples_survive
  FROM fasttrun_relstats('t_survive');

-- ----------------------------------------------------------------------
-- 5. EXPLAIN должен отражать реальный размер таблицы.  С багом:
--    планировщик видит relpages=0 / reltuples=-1 → оценка "маленькая
--    пустая таблица" → Seq Scan с rows=1.  С фиксом: planner_hook
--    реинжектит pages/tuples до построения плана, а column-stats hook
--    повторно привязывает lazy pgstat_info после relcache rebuild →
--    Index Scan с правильной оценкой кардинальности.
-- ----------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT * FROM t_survive WHERE id = 500;

COMMIT;

-- ----------------------------------------------------------------------
-- 6. После COMMIT relcache всё ещё может перестроиться уже без активной
--    транзакции.  Обычный ANALYZE пишет relstats в pg_class, поэтому они
--    переживают такую инвалидацию; fasttrun_analyze должен держать
--    session-local relstats достаточно долго, чтобы planner не откатился
--    к pg_class defaults сразу после границы транзакции.
-- ----------------------------------------------------------------------
UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_survive'::regclass;
SELECT 1 AS force_post_commit_inval;

SELECT relpages > 0 AS post_commit_pages_survive,
       reltuples = 1000 AS post_commit_tuples_survive
  FROM fasttrun_relstats('t_survive');

-- ----------------------------------------------------------------------
-- 7. Регрессия: pg_class может хранить СТАРЫЕ НЕНУЛЕВЫЕ
--    relpages/reltuples.
--
--    Так бывает, когда индекс построили после первого заполнения:
--    core index_update_stats записал реальные heap relstats в pg_class.
--    Позже fasttruncate + refill + fasttrun_analyze обновляют только
--    rd_rel.  Перестроение relcache должно реинжектить cached fasttrun
--    stats даже если pg_class загрузил stale nonzero значения, а не только
--    когда он загрузил zero pages.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_survive_stale (id int, payload text);
INSERT INTO t_survive_stale
SELECT g, repeat('x', 20) FROM generate_series(1, 1000) g;
CREATE INDEX ON t_survive_stale (id);

-- Предусловие: pg_class уже хранит nonzero relstats, которые скоро
-- станут stale.
SELECT relpages > 0 AS pgclass_nonzero_pages,
       reltuples = 1000 AS pgclass_old_tuples
  FROM pg_class WHERE oid = 't_survive_stale'::regclass;

BEGIN;
SELECT fasttrun_analyze('t_survive_stale');
SELECT fasttruncate('t_survive_stale');
INSERT INTO t_survive_stale
SELECT g, repeat('x', 20) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('t_survive_stale');

SELECT relpages > 100 AS large_pages_before_inval,
       reltuples = 100000 AS large_tuples_before_inval
  FROM fasttrun_relstats('t_survive_stale');

-- Форсируем relcache rebuild из stale pg_class (старые nonzero значения).
UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_survive_stale'::regclass;
SELECT 1 AS force_nonzero_inval;

-- И SQL-инспекция, и planner hook должны видеть cached значения fasttrun,
-- а не stale nonzero значения из pg_class.
SELECT relpages > 100 AS large_pages_survive,
       reltuples = 100000 AS large_tuples_survive
  FROM fasttrun_relstats('t_survive_stale');

EXPLAIN (COSTS OFF) SELECT * FROM t_survive_stale WHERE id = 50000;

-- Соседняя регрессия: после fasttruncate cached zero pages тоже
-- authoritative, даже если relcache rebuild перезагрузит старые nonzero
-- stats из pg_class.
SELECT fasttruncate('t_survive_stale');
UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_survive_stale'::regclass;
SELECT 1 AS force_zero_inval;
SELECT relpages = 0 AS zero_pages_survive,
       reltuples = 0 AS zero_tuples_survive
  FROM fasttrun_relstats('t_survive_stale');
COMMIT;

-- ----------------------------------------------------------------------
-- 8. track_counts=off не даёт безопасно кешировать статистику колонок,
--    но relation-level relpages/reltuples известны из полного прохода.
--    Они тоже должны переживать relcache rebuild, иначе планировщик
--    откатывается к stale pg_class даже после точного fasttrun_analyze.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_survive_track_off (id int, payload text);
CREATE INDEX ON t_survive_track_off (id);
INSERT INTO t_survive_track_off
SELECT g, repeat('x', 10) FROM generate_series(1, 2000) g;

BEGIN;
SET LOCAL track_counts = off;
SELECT fasttrun_analyze('t_survive_track_off');

SELECT relpages > 0 AS track_off_pages_before,
       reltuples = 2000 AS track_off_tuples_before
  FROM fasttrun_relstats('t_survive_track_off');

UPDATE pg_class SET relhasindex = relhasindex
 WHERE oid = 't_survive_track_off'::regclass;
SELECT 1 AS force_track_off_inval;

SELECT relpages > 0 AS track_off_pages_survive,
       reltuples = 2000 AS track_off_tuples_survive
  FROM fasttrun_relstats('t_survive_track_off');
COMMIT;

-- ----------------------------------------------------------------------
-- Очистка
-- ----------------------------------------------------------------------
DROP TABLE t_survive;
DROP TABLE t_survive_stale;
DROP TABLE t_survive_track_off;
DROP EXTENSION fasttrun;
