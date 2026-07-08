--
-- fasttrun_zero_sinval_catalog — машинно-проверяемая канарейка на
-- центральный инвариант проекта: операции fasttrun НЕ пишут системный
-- каталог, поэтому НЕ порождают shared sinval-сообщений (именно их
-- шторм изначально выжигал CPU кластера).
--
-- Почему наблюдение каталога, а не счётчик sinval: SharedInvalidMessageCounter
-- инкрементируется только на ПРИЁМЕ сообщений из shared-очереди (от ДРУГИХ
-- бэкендов); собственные инвалидации бэкенд применяет локально при commit
-- мимо receive-пути, поэтому в одной сессии счётчик слеп даже к реальному
-- ANALYZE (проверено эмпирически). Каталожная запись — первопричина
-- relcache/catcache/smgr sinval; её отсутствие детерминированно проверяемо
-- одной сессией без gdb. Send-side проверку даёт gdb-харнес
-- scripts/check_zero_shared_sinval.sh (Linux/CI с gdb).
--
-- Контракт теста: все булевы маркеры обязаны быть t. Позитивные контроли
-- (ядерные TRUNCATE/ANALYZE на ПОСТОЯННОЙ таблице) доказывают, что проверка
-- не вырожденно-истинна: те же наблюдения ловят каталожную запись, когда
-- она реально происходит.
--

CREATE EXTENSION fasttrun;

CREATE TEMP TABLE t_zsc (id int, v int, big text);
CREATE INDEX t_zsc_btree ON t_zsc (v);
CREATE INDEX t_zsc_hash ON t_zsc USING hash (v);
CREATE INDEX t_zsc_gin ON t_zsc USING gin (to_tsvector('simple', big));
INSERT INTO t_zsc SELECT g, g % 50, repeat('w', 60) || g FROM generate_series(1, 5000) g;
SELECT fasttrun_analyze('t_zsc');

-- ----------------------------------------------------------------------
-- 1. fasttruncate + fasttrun_analyze + bulk + collect_stats НЕ трогают
--    каталог: relfilenode таблицы и всех индексов неизменны (ядерный
--    TRUNCATE сменил бы их через RelationSetNewRelfilenumber -> sinval),
--    relpages/reltuples в pg_class остаются каталожными (fasttrun пишет
--    только rd_rel в памяти), pg_statistic пуст.
-- ----------------------------------------------------------------------
DO $$
DECLARE
  heap_rf0 oid; heap_rf1 oid;
  idx_rf0 oid[]; idx_rf1 oid[];
  heap_rp0 int; heap_rp1 int;
  heap_rt0 real; heap_rt1 real;
  nstat0 int; nstat1 int;
BEGIN
  SELECT relfilenode, relpages, reltuples INTO heap_rf0, heap_rp0, heap_rt0
    FROM pg_class WHERE oid = 't_zsc'::regclass;
  SELECT array_agg(relfilenode ORDER BY relname) INTO idx_rf0
    FROM pg_class WHERE oid IN (SELECT indexrelid FROM pg_index WHERE indrelid = 't_zsc'::regclass);
  SELECT count(*) INTO nstat0 FROM pg_statistic WHERE starelid = 't_zsc'::regclass;

  PERFORM fasttruncate('t_zsc');
  INSERT INTO t_zsc SELECT g, g % 50, repeat('w', 60) || g FROM generate_series(1, 5000) g;
  PERFORM fasttrun_analyze('t_zsc');
  PERFORM fasttrun_analyze_bulk('t_zsc');
  PERFORM fasttrun_collect_stats('t_zsc');

  SELECT relfilenode, relpages, reltuples INTO heap_rf1, heap_rp1, heap_rt1
    FROM pg_class WHERE oid = 't_zsc'::regclass;
  SELECT array_agg(relfilenode ORDER BY relname) INTO idx_rf1
    FROM pg_class WHERE oid IN (SELECT indexrelid FROM pg_index WHERE indrelid = 't_zsc'::regclass);
  SELECT count(*) INTO nstat1 FROM pg_statistic WHERE starelid = 't_zsc'::regclass;

  RAISE NOTICE 'heap_relfilenode_unchanged=%', (heap_rf0 = heap_rf1);
  RAISE NOTICE 'index_relfilenodes_unchanged=%', (idx_rf0 = idx_rf1);
  RAISE NOTICE 'heap_catalog_relpages_untouched=%', (heap_rp0 = heap_rp1);
  RAISE NOTICE 'heap_catalog_reltuples_untouched=%', (heap_rt0 = heap_rt1);
  RAISE NOTICE 'no_pg_statistic_rows=%', (nstat0 = 0 AND nstat1 = 0);
END$$;

-- ----------------------------------------------------------------------
-- 2. Позитивный контроль: ядерные TRUNCATE и ANALYZE на ПОСТОЯННОЙ
--    таблице ДОЛЖНЫ менять каталог. Доказывает, что наблюдения выше не
--    вырожденно-истинны -- они ловят каталожную запись, когда она есть.
-- ----------------------------------------------------------------------
CREATE TABLE perm_zsc (id int, v int);
INSERT INTO perm_zsc SELECT g, g % 50 FROM generate_series(1, 5000) g;

DO $$
DECLARE
  rf0 oid; rf1 oid; rp_before int; rp_after int; nstat int;
BEGIN
  SELECT relfilenode INTO rf0 FROM pg_class WHERE oid = 'perm_zsc'::regclass;
  TRUNCATE perm_zsc;
  SELECT relfilenode INTO rf1 FROM pg_class WHERE oid = 'perm_zsc'::regclass;
  RAISE NOTICE 'control_truncate_changes_relfilenode=%', (rf0 <> rf1);

  INSERT INTO perm_zsc SELECT g, g % 50 FROM generate_series(1, 5000) g;
  SELECT relpages INTO rp_before FROM pg_class WHERE oid = 'perm_zsc'::regclass;
  ANALYZE perm_zsc;
  SELECT relpages INTO rp_after FROM pg_class WHERE oid = 'perm_zsc'::regclass;
  SELECT count(*) INTO nstat FROM pg_statistic WHERE starelid = 'perm_zsc'::regclass;
  RAISE NOTICE 'control_analyze_writes_catalog=%', (rp_after > rp_before AND nstat > 0);
END$$;

DROP TABLE perm_zsc;
DROP TABLE t_zsc;
DROP EXTENSION fasttrun;
