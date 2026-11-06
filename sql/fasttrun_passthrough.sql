--
-- fasttrun_passthrough - режим fasttrun.passthrough.  Функции расширения
-- передают работу ядру: fasttruncate выполняет TRUNCATE, fasttrun_analyze,
-- fasttrun_analyze_bulk и fasttrun_collect_stats выполняют ANALYZE.  Хуки и
-- кеши расширения не работают.  Параметр задается при подключении, проверки
-- смотрят на оценки планировщика и файл таблицы, а не на pg_class и
-- pg_statistic: каталог временных таблиц может жить в памяти сервера.
--
CREATE EXTENSION IF NOT EXISTS fasttrun;
CREATE FUNCTION pt_rows(q text) RETURNS int LANGUAGE plpgsql AS $f$
DECLARE plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || q INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::int;
END
$f$;
CREATE TABLE pt_perm (id int);
\connect -reuse-previous=on "options='-c fasttrun.passthrough=on'"
SHOW fasttrun.passthrough;

-- fasttruncate выполняет TRUNCATE ядра: таблица получает новый файл.
CREATE TEMP TABLE pt (id int PRIMARY KEY, grp int);
INSERT INTO pt SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT pg_relation_filenode('pt') AS pt_node \gset
SELECT fasttruncate('pt');
SELECT pg_relation_filenode('pt') <> :pt_node AS pt_truncate_new_file,
       count(*) = 0 AS pt_truncate_empty
  FROM pt;

-- fasttrun_analyze выполняет ANALYZE ядра: оценка равна числу строк, а
-- распределение колонки известно планировщику.
INSERT INTO pt SELECT g, g % 10 FROM generate_series(1, 1000) g;
SELECT fasttrun_analyze('pt');
SELECT pt_rows('SELECT * FROM pt') = 1000 AS pt_analyze_rows,
       pt_rows('SELECT * FROM pt WHERE grp = 3') BETWEEN 50 AND 200 AS pt_analyze_column;
INSERT INTO pt SELECT g, 3 FROM generate_series(1001, 3000) g;
SELECT fasttrun_collect_stats('pt');
SELECT pt_rows('SELECT * FROM pt') = 3000 AS pt_collect_rows;
INSERT INTO pt SELECT g, 4 FROM generate_series(3001, 4000) g;
SELECT fasttrun_analyze_bulk('pt', 'pt_missing', NULL);
SELECT pt_rows('SELECT * FROM pt') = 4000 AS pt_bulk_rows;

-- Кеши расширения пусты: работу сделало ядро.
SELECT analyze_entries = 0 AND column_stats_relid_entries = 0 AND
       column_stats_entries = 0 AS pt_caches_empty
  FROM fasttrun_cache_stats();
SELECT count(*) = 0 AS pt_no_cached_tables FROM fasttrun_cached_tables();

-- Контракты функций прежние: отсутствующая таблица - тихий выход,
-- постоянная - ошибка.
SELECT fasttruncate('pt_missing');
SELECT fasttrun_analyze('pt_missing');
SELECT fasttrun_collect_stats('pt_missing');
SELECT fasttruncate('pt_perm');
SELECT fasttrun_analyze('pt_perm');
SELECT fasttrun_collect_stats('pt_perm');
SELECT fasttrun_analyze_bulk('pt_perm');

-- Режим задается на весь сеанс.
SET fasttrun.passthrough = off;
DROP TABLE pt;

\connect -reuse-previous=on "options=''"
LOAD 'fasttrun';
SHOW fasttrun.passthrough;
DROP TABLE pt_perm;
DROP FUNCTION pt_rows(text);
