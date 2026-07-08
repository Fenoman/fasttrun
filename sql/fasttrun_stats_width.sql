--
-- fasttrun_stats_width — регрессионные тесты вычисления stawidth (ширины колонок).
--
-- Требует shared_preload_libraries = 'fasttrun' (как и остальной suite stats).
--

CREATE EXTENSION fasttrun;

-- ======================================================================
-- Haas-Stokes путь (use_typanalyze=off): stawidth для fixed-length
-- by-reference типов (uuid attlen=16, !attbyval) равен attlen, а не 2*attlen.
-- Регресс-guard для attbyval (int4=4) и varlena (text >= 20).
-- ======================================================================

SET fasttrun.use_typanalyze = off;       -- облегчённый Haas-Stokes путь
SET fasttrun.sample_rows = 3000;

CREATE TEMP TABLE t_w (c int4, u uuid, t text);
INSERT INTO t_w
SELECT g, ('00000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid, repeat('a', 20)
FROM generate_series(1, 2000) g;

SELECT fasttrun_collect_stats('t_w');

-- attbyval int4 -> 4; fixed-len by-ref uuid -> 16 (= attlen); varlena text >= 20.
SELECT
  (SELECT stawidth FROM fasttrun_inspect_stats('t_w') WHERE staattnum = 1) = 4   AS int4_width_ok,
  (SELECT stawidth FROM fasttrun_inspect_stats('t_w') WHERE staattnum = 2) = 16  AS uuid_width_ok,
  (SELECT stawidth FROM fasttrun_inspect_stats('t_w') WHERE staattnum = 3) >= 20 AS text_width_ok;

RESET fasttrun.use_typanalyze;
RESET fasttrun.sample_rows;
DROP TABLE t_w;

-- ======================================================================
-- Паритет с ядерным ANALYZE в Haas-Stokes пути: nullable-unique колонка
-- даёт n_distinct = -(1-nullfrac), varlena stawidth считается с
-- заголовком (VARSIZE_ANY).  Эталон — обычная таблица с теми же данными.
-- ======================================================================

SET fasttrun.use_typanalyze = off;
SET fasttrun.sample_rows = -1;           -- полный сэмпл: точное совпадение с ядром

CREATE TEMP TABLE t_hs_parity (id int, val text);
INSERT INTO t_hs_parity
SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE 'v_' || g END
FROM generate_series(1, 2000) g;

SELECT fasttrun_collect_stats('t_hs_parity');

CREATE TABLE t_hs_parity_ref AS SELECT * FROM t_hs_parity;
ANALYZE t_hs_parity_ref;

SELECT s.stadistinct = p.n_distinct AS hs_ndistinct_core_parity,
       s.stawidth    = p.avg_width  AS hs_width_core_parity
FROM fasttrun_inspect_stats('t_hs_parity') s
JOIN pg_stats p ON p.tablename = 't_hs_parity_ref' AND p.attname = 'val'
WHERE s.staattnum = 2;

DROP TABLE t_hs_parity_ref;
DROP TABLE t_hs_parity;
RESET fasttrun.use_typanalyze;
RESET fasttrun.sample_rows;

-- ======================================================================
-- Паритет с ядром, продолжение: (а) inline-compressed varlena — stawidth
-- по ХРАНИМОМУ (сжатому) размеру, detoast только для сравнений;
-- (б) bounded-set (каждое значение повторяется, distinct > 10% строк) —
-- негативная форма n_distinct, как ядерное 10%-масштабирование.
-- PLAIN-балласт (~1600 Б) раздувает tuple за TOAST_TUPLE_THRESHOLD:
-- тостер сжимает только big (raw <= 1024 — мимо toowide), сжатое
-- значение остаётся inline.
-- ======================================================================

SET fasttrun.use_typanalyze = off;
SET fasttrun.sample_rows = -1;

CREATE TEMP TABLE t_hs_parity2 (id int, pad text, big text, grp int);
ALTER TABLE t_hs_parity2 ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO t_hs_parity2
SELECT g, repeat('x', 1600), repeat('ab', 450) || (g % 4), g % 50
FROM generate_series(1, 100) g;

SELECT fasttrun_collect_stats('t_hs_parity2');

CREATE TABLE t_hs_parity2_ref (id int, pad text, big text, grp int);
ALTER TABLE t_hs_parity2_ref ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO t_hs_parity2_ref SELECT * FROM t_hs_parity2;
ANALYZE t_hs_parity2_ref;

SELECT (SELECT s.stawidth FROM fasttrun_inspect_stats('t_hs_parity2') s
         WHERE s.staattnum = 3)
     = (SELECT p.avg_width FROM pg_stats p
         WHERE p.tablename = 't_hs_parity2_ref' AND p.attname = 'big')
       AS hs_width_compressed_parity,
       (SELECT s.stadistinct FROM fasttrun_inspect_stats('t_hs_parity2') s
         WHERE s.staattnum = 4)
     = (SELECT p.n_distinct FROM pg_stats p
         WHERE p.tablename = 't_hs_parity2_ref' AND p.attname = 'grp')
       AS hs_bounded_ndistinct_parity;

DROP TABLE t_hs_parity2_ref;
DROP TABLE t_hs_parity2;
RESET fasttrun.use_typanalyze;
RESET fasttrun.sample_rows;

DROP EXTENSION fasttrun;
