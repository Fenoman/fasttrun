\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.5.1'" to load this file. \quit

-- Изменения реализованы в C-библиотеке. SQL-объекты не меняются.
--
-- ВНИМАНИЕ: запись в общий реестр учета CREATE TEMP TABLE по умолчанию
-- выключена. Если нужен fasttrun_prewarm(), задайте
-- fasttrun.track_temp_creates = on.
