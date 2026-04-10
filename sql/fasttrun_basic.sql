--
-- fasttrun_basic — базовая проверка работоспособности расширения.
--
-- Проверяет:
--   * CREATE EXTENSION fasttrun
--   * fasttruncate() очищает временную таблицу
--   * повторный вызов на уже пустой таблице ничего не делает
--   * цикл «вставить -> очистить» работает многократно
--

CREATE EXTENSION fasttrun;

-- ----------------------------------------------------------------------
-- 1. Базовый сценарий: временная таблица -> вставка -> fasttruncate -> пусто
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_basic (a int);
INSERT INTO t_basic SELECT generate_series(1, 100);
SELECT count(*) AS rows_before FROM t_basic;

SELECT fasttruncate('t_basic');

SELECT count(*) AS rows_after FROM t_basic;

-- ----------------------------------------------------------------------
-- 2. fasttruncate на уже пустой таблице — должен ничего не делать
--    и не выдавать ошибок
-- ----------------------------------------------------------------------
SELECT fasttruncate('t_basic');
SELECT count(*) AS rows_still_empty FROM t_basic;

-- ----------------------------------------------------------------------
-- 3. Цикл вставка -> очистка работает многократно
-- ----------------------------------------------------------------------
INSERT INTO t_basic SELECT generate_series(1, 50);
SELECT count(*) AS rows_refilled FROM t_basic;
SELECT fasttruncate('t_basic');
SELECT count(*) AS rows_emptied FROM t_basic;

INSERT INTO t_basic SELECT generate_series(1, 10);
SELECT count(*) AS rows_third_fill FROM t_basic;
SELECT fasttruncate('t_basic');
SELECT count(*) AS rows_third_empty FROM t_basic;

-- ----------------------------------------------------------------------
-- 4. Несколько разных временных таблиц в одной сессии
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_basic2 (b text);
INSERT INTO t_basic2 SELECT 'row-' || g FROM generate_series(1, 25) g;

SELECT count(*) FROM t_basic;
SELECT count(*) FROM t_basic2;

SELECT fasttruncate('t_basic2');

SELECT count(*) FROM t_basic;
SELECT count(*) FROM t_basic2;

DROP TABLE t_basic;
DROP TABLE t_basic2;

-- ----------------------------------------------------------------------
-- 5. fasttrun.zero_sinval_truncate (default on): тот же самый
--    функциональный результат, что и старый путь.  Проверяем что:
--      (а) с дефолтом (zero_sinval_truncate = on) fasttruncate
--          корректно очищает таблицу через unlink+smgrcreate;
--      (б) с явным off (старый heap_truncate_one_rel) — тоже работает;
--      (в) переключение GUC внутри сессии не ломает state.
--    Сами sinval не считаем (это требует параллельной нагрузки и
--    выходит за рамки installcheck), но проверяем что данные после
--    обоих путей совпадают.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_zsi (a int, b text);

-- Дефолтный путь (zero_sinval_truncate = on)
INSERT INTO t_zsi SELECT g, 'row_' || g FROM generate_series(1, 500) g;
SELECT count(*) = 500 AS pre_default FROM t_zsi;
SELECT fasttruncate('t_zsi');
SELECT count(*) = 0 AS empty_default FROM t_zsi;

-- Перезаполняем, переключаемся на старый путь, truncate повторно
INSERT INTO t_zsi SELECT g, 'row_' || g FROM generate_series(1, 300) g;
SET fasttrun.zero_sinval_truncate = off;
SELECT fasttruncate('t_zsi');
SELECT count(*) = 0 AS empty_legacy FROM t_zsi;
RESET fasttrun.zero_sinval_truncate;

-- Снова заполняем при дефолтном пути, стресс-цикл
INSERT INTO t_zsi SELECT g, 'row_' || g FROM generate_series(1, 1000) g;
SELECT count(*) = 1000 AS pre_stress FROM t_zsi;

-- 5 циклов truncate-INSERT с разными размерами
SELECT fasttruncate('t_zsi');
INSERT INTO t_zsi SELECT g, 'r_' || g FROM generate_series(1, 200) g;
SELECT fasttruncate('t_zsi');
INSERT INTO t_zsi SELECT g, 'r_' || g FROM generate_series(1, 50)  g;
SELECT fasttruncate('t_zsi');
INSERT INTO t_zsi SELECT g, 'r_' || g FROM generate_series(1, 800) g;
SELECT fasttruncate('t_zsi');
INSERT INTO t_zsi SELECT g, 'r_' || g FROM generate_series(1, 5)   g;
SELECT fasttruncate('t_zsi');
INSERT INTO t_zsi SELECT g, 'r_' || g FROM generate_series(1, 100) g;
SELECT count(*) = 100 AS post_stress_count FROM t_zsi;

-- Проверяем что данные реально читаются после unlink+smgrcreate
SELECT max(a) = 100 AND min(a) = 1 AS data_intact FROM t_zsi;

DROP TABLE t_zsi;

-- ----------------------------------------------------------------------
-- 6. Disk usage не утекает после fasttruncate.
--    После unlink + smgrcreate физический размер relation должен
--    падать до пустого, а не расти от цикла к циклу.  Это
--    регрессия-защита на случай если кто-то сломает unlink loop
--    или забудет smgrrelease перед unlink (что приведёт к stale
--    fd в md_seg_fds[] но НЕ к утечке диска — POSIX гарантирует
--    "delete on close").
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_disk (id int, payload text);

-- Заполняем ~5 MB данных (50k строк × 100 байт)
INSERT INTO t_disk
SELECT g, repeat('x', 100) FROM generate_series(1, 50000) g;

-- После fasttruncate физический размер должен упасть до ~1 страницы
-- (8192 байт после smgrcreate пустого fork-файла)
SELECT fasttruncate('t_disk');
SELECT pg_relation_size('t_disk') < 16384 AS truncated_to_empty;

-- Несколько циклов заполнения/очистки -- размер не должен расти
INSERT INTO t_disk SELECT g, repeat('x', 100) FROM generate_series(1, 50000) g;
SELECT fasttruncate('t_disk');
INSERT INTO t_disk SELECT g, repeat('x', 100) FROM generate_series(1, 50000) g;
SELECT fasttruncate('t_disk');
INSERT INTO t_disk SELECT g, repeat('x', 100) FROM generate_series(1, 50000) g;
SELECT fasttruncate('t_disk');

-- После ещё 3 циклов: всё ещё пусто
SELECT pg_relation_size('t_disk') < 16384 AS still_empty_after_cycles;

DROP TABLE t_disk;

-- ----------------------------------------------------------------------
-- 7. Индексированная temp-таблица: fasttruncate должен очищать и
--    индексы.  После fasttruncate + новые INSERT, index scan не
--    должен возвращать строки из старых данных.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_idx (v int);
CREATE INDEX ON t_idx (v);
INSERT INTO t_idx SELECT generate_series(1, 5);

SELECT fasttruncate('t_idx');

-- После truncate таблица и индекс должны быть пусты
SELECT count(*) = 0 AS heap_empty FROM t_idx;

-- Вставляем данные, которые НЕ пересекаются со старыми
INSERT INTO t_idx VALUES (100), (200);

-- Принудительно index scan -- если в индексе остался мусор от 1..5,
-- запрос вернёт фантомную строку
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS no_ghost_v1 FROM t_idx WHERE v = 1;
SELECT count(*) = 0 AS no_ghost_v3 FROM t_idx WHERE v = 3;
SELECT count(*) = 1 AS found_v100 FROM t_idx WHERE v = 100;
SELECT count(*) = 1 AS found_v200 FROM t_idx WHERE v = 200;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Многократный цикл: повторная проверка корректности
INSERT INTO t_idx SELECT generate_series(1, 1000);
SELECT fasttruncate('t_idx');
INSERT INTO t_idx VALUES (42);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS no_ghost_v500 FROM t_idx WHERE v = 500;
SELECT count(*) = 1 AS found_v42 FROM t_idx WHERE v = 42;
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE t_idx;

-- ----------------------------------------------------------------------
-- 8. Toast-таблица: fasttruncate должен очищать и toast + toast-индекс.
--    Вставляем широкие строки, очищаем, проверяем что данные
--    не протекают.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_toast (id int, payload text);
INSERT INTO t_toast SELECT g, repeat('T', 10000) FROM generate_series(1, 50) g;
SELECT count(*) = 50 AS pre_toast FROM t_toast;

SELECT fasttruncate('t_toast');
SELECT count(*) = 0 AS toast_empty FROM t_toast;

-- Перезаполняем другими данными, проверяем что есть только новые строки
INSERT INTO t_toast SELECT g, repeat('N', 10000) FROM generate_series(51, 60) g;
SELECT count(*) = 10 AS new_rows_only FROM t_toast;
SELECT min(id) = 51 AND max(id) = 60 AS correct_range FROM t_toast;

-- Toast-хранилище тоже должно сбрасываться после truncate
SELECT fasttruncate('t_toast');
SELECT pg_relation_size('t_toast') < 16384 AS toast_heap_empty;

DROP TABLE t_toast;

-- ----------------------------------------------------------------------
-- 9. Не-btree индексы: hash, GIN, expression, partial.
--    Проверяем что fasttruncate корректно работает со всеми типами
--    AM и metapage перестраиваются.
-- ----------------------------------------------------------------------

-- 9a. Hash-индекс
CREATE TEMP TABLE t_hash (k int);
CREATE INDEX ON t_hash USING hash (k);
INSERT INTO t_hash SELECT generate_series(1, 100);
SELECT fasttruncate('t_hash');
INSERT INTO t_hash VALUES (42), (99);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS hash_no_ghost_v1 FROM t_hash WHERE k = 1;
SELECT count(*) = 1 AS hash_found_v42 FROM t_hash WHERE k = 42;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE t_hash;

-- 9b. GIN-индекс
CREATE TEMP TABLE t_gin (doc tsvector);
CREATE INDEX ON t_gin USING gin (doc);
INSERT INTO t_gin VALUES (to_tsvector('english', 'the quick brown fox'));
INSERT INTO t_gin VALUES (to_tsvector('english', 'lazy dog sleeps'));
SELECT fasttruncate('t_gin');
INSERT INTO t_gin VALUES (to_tsvector('english', 'new data here'));
SELECT count(*) = 1 AS gin_found_new FROM t_gin WHERE doc @@ to_tsquery('new');
SELECT count(*) = 0 AS gin_no_ghost_fox FROM t_gin WHERE doc @@ to_tsquery('fox');
DROP TABLE t_gin;

-- 9c. Expression-индекс (функциональный на lower(text))
CREATE TEMP TABLE t_expr (name text);
CREATE INDEX ON t_expr (lower(name));
INSERT INTO t_expr VALUES ('Alice'), ('BOB'), ('Charlie');
SELECT fasttruncate('t_expr');
INSERT INTO t_expr VALUES ('Dave'), ('EVE');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS expr_no_ghost_alice FROM t_expr WHERE lower(name) = 'alice';
SELECT count(*) = 1 AS expr_found_dave FROM t_expr WHERE lower(name) = 'dave';
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE t_expr;

-- 9d. Partial-индекс (с WHERE-условием)
CREATE TEMP TABLE t_partial (id int, active bool);
CREATE INDEX ON t_partial (id) WHERE active;
INSERT INTO t_partial SELECT g, true FROM generate_series(1, 50) g;
SELECT fasttruncate('t_partial');
INSERT INTO t_partial VALUES (100, true), (200, false);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS partial_no_ghost_v1 FROM t_partial WHERE id = 1 AND active;
SELECT count(*) = 1 AS partial_found_v100 FROM t_partial WHERE id = 100 AND active;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE t_partial;

-- 9e. Несколько индексов на одной таблице -- все должны очиститься
CREATE TEMP TABLE t_multi (a int, b text, c int);
CREATE INDEX ON t_multi (a);
CREATE INDEX ON t_multi USING hash (b);
CREATE INDEX ON t_multi (c) WHERE c > 0;
INSERT INTO t_multi SELECT g, 'v' || g, g FROM generate_series(1, 100) g;
SELECT fasttruncate('t_multi');
INSERT INTO t_multi VALUES (999, 'test', 999);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) = 0 AS multi_no_ghost_a1 FROM t_multi WHERE a = 1;
SELECT count(*) = 1 AS multi_found_a999 FROM t_multi WHERE a = 999;
SELECT count(*) = 0 AS multi_no_ghost_b FROM t_multi WHERE b = 'v50';
SELECT count(*) = 1 AS multi_found_b FROM t_multi WHERE b = 'test';
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE t_multi;

-- ----------------------------------------------------------------------
-- 10. Активный курсор / активный запрос: fasttruncate должен
--     завершаться аккуратной SQL-ошибкой, как обычный TRUNCATE, а не
--     падать в низкоуровневое "block is still referenced" или PANIC.
-- ----------------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE t_busy (v int);
INSERT INTO t_busy SELECT generate_series(1, 10);
DECLARE c_busy NO SCROLL CURSOR FOR SELECT * FROM t_busy;
FETCH 1 FROM c_busy;
SELECT fasttruncate('t_busy');
ROLLBACK;

BEGIN;
SET LOCAL fasttrun.zero_sinval_truncate = off;
CREATE TEMP TABLE t_busy (v int);
INSERT INTO t_busy SELECT generate_series(1, 10);
DECLARE c_busy NO SCROLL CURSOR FOR SELECT * FROM t_busy;
FETCH 1 FROM c_busy;
SELECT fasttruncate('t_busy');
ROLLBACK;

-- ----------------------------------------------------------------------
-- 11. Контракт "только heap": все публичные API должны чисто
--     отказывать на partitioned temp table через ERROR, а не заходить
--     в heap-специфичный код.
-- ----------------------------------------------------------------------
CREATE TEMP TABLE t_part (a int) PARTITION BY RANGE (a);
SELECT fasttruncate('t_part');
SELECT fasttrun_analyze('t_part');
SELECT * FROM fasttrun_relstats('t_part');
SELECT fasttrun_collect_stats('t_part');
DROP TABLE t_part;

DROP EXTENSION fasttrun;
