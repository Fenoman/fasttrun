--
-- fasttrun_stats_cold - библиотека загружена по первому вызову, без предзагрузки.
--
-- Хук исполнителя ставится при загрузке библиотеки. Если она загрузилась на
-- первом обращении к расширению, операторы, начавшиеся раньше, остались
-- невидимыми: ни целей, ни даже того, пишут ли они, восстановить нельзя.
-- Поэтому в таком сеансе поколоночный учет UPDATE не работает вовсе, и износ
-- от UPDATE считается по таблице целиком. Один раз за сеанс об этом пишется в
-- серверный лог.
--
-- Набор обязан оставаться без предзагрузки, поэтому он не трогает
-- session_preload_libraries. Создание расширения само загружает библиотеку в
-- свое соединение, поэтому проверки идут в новом: расширение уже стоит,
-- библиотека еще нет, и ее загрузит первый же вызов.
--
CREATE EXTENSION fasttrun;
\connect -

CREATE FUNCTION ft_rows_est(q text, pat text) RETURNS int LANGUAGE plpgsql AS $ft$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN ' || q LOOP
    IF ln ~ pat THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  RETURN est;
END$ft$;

-- Носитель, ради которого учет и выключается: пересбор изнутри уже идущего
-- UPDATE, чей старт хук не видел. Устаревший образец отдавать нельзя.
CREATE TEMP TABLE cold_writer (id int, note int);
INSERT INTO cold_writer SELECT g, 0 FROM generate_series(1, 100000) g;
BEGIN;
UPDATE cold_writer SET id = (SELECT 1 FROM fasttrun_collect_stats('cold_writer'));
UPDATE cold_writer SET note = 7 WHERE false;
SELECT ft_rows_est('SELECT * FROM cold_writer WHERE id = 1', 'on cold_writer')
       >= 100 AS cold_collect_inside_update_not_served;
ROLLBACK;
DROP TABLE cold_writer;

-- Тот же носитель, но пишущий оператор вложен в блок DO и идет через SPI.
CREATE TEMP TABLE cold_spi (id int, note int);
INSERT INTO cold_spi SELECT g, 0 FROM generate_series(1, 100000) g;
BEGIN;
DO $cs$
BEGIN
  UPDATE cold_spi SET id = (SELECT 1 FROM fasttrun_collect_stats('cold_spi'));
END$cs$;
UPDATE cold_spi SET note = 7 WHERE false;
SELECT ft_rows_est('SELECT * FROM cold_spi WHERE id = 1', 'on cold_spi')
       >= 100 AS cold_collect_inside_spi_update_not_served;
ROLLBACK;
DROP TABLE cold_spi;

-- Учет по колонкам выключен: UPDATE служебной колонки гасит и ключевую.
CREATE TEMP TABLE cold_cycle (id int, f_units int, note text);
INSERT INTO cold_cycle SELECT g, 0, repeat('x', 40) FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('cold_cycle');
SELECT ft_rows_est('SELECT * FROM cold_cycle WHERE id = 1', 'on cold_cycle')
       <= 10 AS lazy_backend_has_statistics_before_update;
UPDATE cold_cycle SET f_units = 7;
SELECT ft_rows_est('SELECT * FROM cold_cycle WHERE id = 1', 'on cold_cycle')
       >= 100 AS lazy_backend_falls_back_to_whole_relation;
DROP TABLE cold_cycle;

-- Реестр начатых операторов работает и без предзагрузки, и постоянные цели в
-- него не попадают и здесь. UPDATE секционированной таблицы без отсечения
-- секций перечисляет все сто, а сбор временной таблицы в той же транзакции
-- обязан остаться доступным планировщику. Сбор до транзакции держит
-- библиотеку загруженной к моменту UPDATE независимо от места секции в наборе:
-- до загрузки UPDATE прошел бы мимо хука и ничего не проверил.
CREATE TABLE cold_perm_parts (k int, v int) PARTITION BY HASH (k);
DO $cp$
DECLARE i int;
BEGIN
  FOR i IN 0..99 LOOP
    EXECUTE format('CREATE UNLOGGED TABLE cold_perm_parts_%s PARTITION OF cold_perm_parts '
                   'FOR VALUES WITH (MODULUS 100, REMAINDER %s)', i, i);
  END LOOP;
END$cp$;
CREATE TEMP TABLE cold_probe (id int, note int);
INSERT INTO cold_probe SELECT g, 0 FROM generate_series(1, 100000) g;
SELECT fasttrun_collect_stats('cold_probe');
BEGIN;
UPDATE cold_perm_parts SET v = 1 WHERE k <> 5;
SELECT fasttrun_collect_stats('cold_probe');
SELECT ft_rows_est('SELECT * FROM cold_probe WHERE id = 1', 'on cold_probe')
       <= 10 AS cold_perm_partitions_keep_stats;
COMMIT;
DROP TABLE cold_probe;
DROP TABLE cold_perm_parts;

DROP FUNCTION ft_rows_est(text, text);
DROP EXTENSION fasttrun;
