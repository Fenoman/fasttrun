--
-- fasttrun_fresh - fasttrun_temp_is_ours и fasttrun_temp_same_cols.
--
-- fasttrun_temp_is_ours отвечает, появились ли все строки временной таблицы
-- сеанса в текущей транзакции: таблица создана в ней или очищена TRUNCATE или
-- fasttruncate, и откат не вернул старые строки.  fasttrun_temp_same_cols сравнивает
-- живые колонки временной таблицы и шаблона.  Проверки не читают pg_class для
-- временных таблиц: каталог временных таблиц может жить в памяти сервера.
--
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun;
CREATE EXTENSION IF NOT EXISTS dblink;
RESET client_min_messages;
CREATE FUNCTION fr(t text) RETURNS boolean LANGUAGE sql AS
$f$ SELECT fasttrun_temp_is_ours(to_regclass('pg_temp.' || t)) $f$;

-- ----------------------------------------------------------------------
-- 1. Создание таблицы
-- ----------------------------------------------------------------------
BEGIN;
CREATE TEMP TABLE f_new (id int);
SELECT fr('f_new') AS created_in_xact;
COMMIT;
SELECT fr('f_new') AS created_next_xact;

BEGIN;
SAVEPOINT s;
CREATE TEMP TABLE f_sub (id int);
RELEASE SAVEPOINT s;
SELECT fr('f_sub') AS created_in_released_savepoint;
SAVEPOINT s2;
CREATE TEMP TABLE f_gone (id int);
ROLLBACK TO SAVEPOINT s2;
SELECT fr('f_gone') IS NULL AS created_then_rolled_back_is_null;
CREATE TEMP TABLE f_ctas AS SELECT 1 AS id;
SELECT fr('f_ctas') AS created_as_select;
COMMIT;

-- Удаленная таблица и NULL дают NULL.
CREATE TEMP TABLE f_drop (id int);
SELECT 'f_drop'::regclass::oid AS f_drop_oid \gset
DROP TABLE f_drop;
SELECT fasttrun_temp_is_ours(:f_drop_oid::oid::regclass) IS NULL AS dropped_is_null,
       fasttrun_temp_is_ours(NULL) IS NULL AS null_is_null;

-- ----------------------------------------------------------------------
-- 2. fasttruncate
-- ----------------------------------------------------------------------
CREATE TEMP TABLE f_ft (id int PRIMARY KEY, v text);
INSERT INTO f_ft SELECT g, 'old' FROM generate_series(1, 100) g;

BEGIN;
SELECT fr('f_ft') AS old_rows;
SELECT fasttruncate('f_ft');
SELECT fr('f_ft') AS after_reset;
INSERT INTO f_ft VALUES (1, 'new');
SELECT fasttruncate('f_ft');
SELECT fr('f_ft') AS after_second_reset;
COMMIT;
SELECT fr('f_ft') AS reset_next_xact;

-- Пустая таблица идет быстрым путем без работы с файлами, метка ставится и там.
SELECT value AS fast_before FROM fasttrun_counters()
 WHERE counter = 'truncate_empty_fast_path' \gset
BEGIN;
SELECT fasttruncate('f_ft');
SELECT fr('f_ft') AS reset_already_empty;
COMMIT;
SELECT value - :fast_before AS empty_fast_path_calls FROM fasttrun_counters()
 WHERE counter = 'truncate_empty_fast_path';

-- Быстрый путь в откатываемой точке сохранения: файл тот же, метка остается.
SELECT value AS fast_before FROM fasttrun_counters()
 WHERE counter = 'truncate_empty_fast_path' \gset
BEGIN;
SAVEPOINT s;
SELECT fasttruncate('f_ft');
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_ft') AS fast_path_rolled_back;
COMMIT;
SELECT value - :fast_before AS empty_fast_path_calls FROM fasttrun_counters()
 WHERE counter = 'truncate_empty_fast_path';

-- Откат уровня не возвращает строки, очищенные fasttruncate, и файл тот же.
INSERT INTO f_ft SELECT g, 'old' FROM generate_series(1, 100) g;
BEGIN;
SAVEPOINT s;
SELECT fasttruncate('f_ft');
INSERT INTO f_ft VALUES (1, 'lost');
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_ft') AS reset_rolled_back, count(*) AS rows_left FROM f_ft;
SAVEPOINT s2;
SELECT fasttruncate('f_ft');
RELEASE SAVEPOINT s2;
SELECT fr('f_ft') AS reset_released;
COMMIT;

-- TRUNCATE дает новый файл, fasttruncate очищает его, ROLLBACK TO возвращает
-- прежний файл со старыми строками: ответ false.
INSERT INTO f_ft SELECT g, 'old' FROM generate_series(1, 100) g;
BEGIN;
SAVEPOINT s;
TRUNCATE f_ft;
SELECT fasttruncate('f_ft');
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_ft') AS truncate_reset_rolled_back, count(*) AS old_rows_back FROM f_ft;
COMMIT;

-- Вложенный fasttruncate без смены файла переживает откат внешнего уровня.
BEGIN;
SAVEPOINT a;
SAVEPOINT b;
SELECT fasttruncate('f_ft');
RELEASE SAVEPOINT b;
ROLLBACK TO SAVEPOINT a;
SELECT fr('f_ft') AS nested_reset_same_file, count(*) AS rows_left FROM f_ft;
COMMIT;

-- Метка на файле, который снова стал текущим: откат TRUNCATE после очистки.
BEGIN;
SELECT fasttruncate('f_ft');
INSERT INTO f_ft VALUES (1, 'new');
SAVEPOINT s;
TRUNCATE f_ft;
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_ft') AS reset_then_truncate_rolled_back, count(*) AS new_rows FROM f_ft;
COMMIT;

-- PL/pgSQL-блок с EXCEPTION открывает подтранзакцию.
INSERT INTO f_ft SELECT g, 'old' FROM generate_series(10, 20) g;
BEGIN;
DO $$
BEGIN
    PERFORM fasttruncate('f_ft');
    RAISE EXCEPTION 'undo';
EXCEPTION WHEN raise_exception THEN NULL;
END
$$;
SELECT fr('f_ft') AS reset_in_failed_block, count(*) AS rows_left FROM f_ft;
COMMIT;

-- ----------------------------------------------------------------------
-- 3. Что не считается очисткой в транзакции
-- ----------------------------------------------------------------------
INSERT INTO f_ft SELECT g, 'old' FROM generate_series(10, 20) g;
BEGIN;
DELETE FROM f_ft;
SELECT fr('f_ft') AS after_delete;
ALTER TABLE f_ft ADD COLUMN extra int;
SELECT fr('f_ft') AS after_add_column;
COMMIT;

CREATE TEMP TABLE f_ocd (id int) ON COMMIT DELETE ROWS;
BEGIN;
INSERT INTO f_ocd VALUES (1);
COMMIT;
BEGIN;
SELECT fr('f_ocd') AS on_commit_delete_rows_prev_xact;
COMMIT;

-- ----------------------------------------------------------------------
-- 4. TRUNCATE ядра в любом виде
-- ----------------------------------------------------------------------
CREATE TEMP TABLE f_a (id int);
CREATE TEMP TABLE f_b (id int);
BEGIN;
TRUNCATE f_a, f_b;
SELECT fr('f_a') AS list_first, fr('f_b') AS list_second;
COMMIT;
SELECT fr('f_a') AS list_next_xact;

CREATE TEMP TABLE f_pk (id int PRIMARY KEY);
CREATE TEMP TABLE f_fk (id int REFERENCES f_pk);
BEGIN;
TRUNCATE f_pk CASCADE;
SELECT fr('f_pk') AS cascade_named, fr('f_fk') AS cascade_referencing;
COMMIT;

CREATE TEMP TABLE f_parent (id int);
CREATE TEMP TABLE f_child () INHERITS (f_parent);
BEGIN;
TRUNCATE f_parent;
SELECT fr('f_parent') AS inherit_parent, fr('f_child') AS inherit_child;
COMMIT;
BEGIN;
TRUNCATE ONLY f_parent;
SELECT fr('f_parent') AS only_parent, fr('f_child') AS only_child;
COMMIT;

CREATE TEMP TABLE f_part (id int) PARTITION BY RANGE (id);
CREATE TEMP TABLE f_part_1 PARTITION OF f_part FOR VALUES FROM (0) TO (100);
CREATE TEMP TABLE f_part_2 PARTITION OF f_part FOR VALUES FROM (100) TO (200);
BEGIN;
TRUNCATE f_part;
SELECT fr('f_part_1') AS part_first, fr('f_part_2') AS part_second;
COMMIT;
BEGIN;
TRUNCATE f_part_1;
SELECT fr('f_part_1') AS leaf_named, fr('f_part_2') AS leaf_other;
COMMIT;

-- Метка TRUNCATE живет на своем уровне: RELEASE отдает ее родителю,
-- ROLLBACK TO снимает.
BEGIN;
SAVEPOINT s;
TRUNCATE f_a;
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_a') AS truncate_rolled_back;
SAVEPOINT s2;
TRUNCATE f_a;
RELEASE SAVEPOINT s2;
SELECT fr('f_a') AS truncate_released;
COMMIT;
BEGIN;
DO $$
BEGIN
    TRUNCATE f_a;
    RAISE EXCEPTION 'undo';
EXCEPTION WHEN raise_exception THEN NULL;
END
$$;
SELECT fr('f_a') AS truncate_in_failed_block;
DO $$
BEGIN
    TRUNCATE f_a;
EXCEPTION WHEN raise_exception THEN NULL;
END
$$;
SELECT fr('f_a') AS truncate_in_finished_block;
COMMIT;

-- Счетчики TRUNCATE читаются и после выключения track_counts.
BEGIN;
TRUNCATE f_a;
SET LOCAL track_counts = off;
SELECT fr('f_a') AS truncate_then_counters_off;
COMMIT;

-- Без счетчиков pgstat TRUNCATE видит метка расширения.
SET track_counts = off;
BEGIN;
TRUNCATE f_a;
SELECT fr('f_a') AS no_counters_truncate;
SAVEPOINT s;
TRUNCATE f_b;
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_b') AS no_counters_rolled_back;
SAVEPOINT s2;
TRUNCATE f_b;
RELEASE SAVEPOINT s2;
SELECT fr('f_b') AS no_counters_released;
SET LOCAL track_counts = on;
SELECT fr('f_a') AS no_counters_then_counters_on;
COMMIT;
RESET track_counts;

-- TRUNCATE до загрузки библиотеки видят счетчики pgstat.
\connect -reuse-previous=on
SELECT current_setting('fasttrun.passthrough', true) IS NULL AS library_not_loaded;
CREATE TEMP TABLE f_lazy (id int);
INSERT INTO f_lazy VALUES (1);
BEGIN;
TRUNCATE f_lazy;
SELECT current_setting('fasttrun.passthrough', true) IS NULL AS library_not_loaded_at_truncate;
SELECT fr('f_lazy') AS truncate_before_load;
COMMIT;

-- Граница: TRUNCATE до загрузки библиотеки и без счетчиков не виден.
\connect -reuse-previous=on
SET track_counts = off;
CREATE TEMP TABLE f_lazy_off (id int);
BEGIN;
TRUNCATE f_lazy_off;
SELECT current_setting('fasttrun.passthrough', true) IS NULL AS library_not_loaded_at_truncate;
SELECT fr('f_lazy_off') AS truncate_before_load_no_counters;
COMMIT;
RESET track_counts;

-- ----------------------------------------------------------------------
-- 5. Режим passthrough: fasttruncate выполняет TRUNCATE ONLY ядра
-- ----------------------------------------------------------------------
\connect -reuse-previous=on "options='-c fasttrun.passthrough=on'"
SHOW fasttrun.passthrough;
CREATE TEMP TABLE f_pt (id int);
INSERT INTO f_pt SELECT generate_series(1, 10);
BEGIN;
SELECT fr('f_pt') AS passthrough_old_rows;
SAVEPOINT s;
SELECT fasttruncate('f_pt');
SELECT fr('f_pt') AS passthrough_reset;
ROLLBACK TO SAVEPOINT s;
SELECT fr('f_pt') AS passthrough_reset_rolled_back, count(*) AS old_rows_back FROM f_pt;
SELECT fasttruncate('f_pt');
SELECT fr('f_pt') AS passthrough_reset_top;
COMMIT;
SELECT fr('f_pt') AS passthrough_next_xact;
SET track_counts = off;
BEGIN;
SELECT fasttruncate('f_pt');
SELECT fr('f_pt') AS passthrough_no_counters;
COMMIT;
RESET track_counts;
\connect -reuse-previous=on "options=''"

-- ----------------------------------------------------------------------
-- 6. Область: ошибки и права
-- ----------------------------------------------------------------------
\set VERBOSITY terse
CREATE TABLE f_perm (id int, v text);
CREATE TEMP TABLE f_t (id int, v text);
CREATE INDEX f_t_idx ON f_t (id);
CREATE TEMP VIEW f_view AS SELECT 1 AS id;
CREATE TEMP SEQUENCE f_seq;
CREATE TEMP TABLE f_tpart (id int) PARTITION BY RANGE (id);
SELECT fasttrun_temp_is_ours('f_perm');
SELECT fasttrun_temp_is_ours('f_t_idx');
SELECT fasttrun_temp_is_ours('f_view');
SELECT fasttrun_temp_is_ours('f_seq');
SELECT fasttrun_temp_is_ours('f_tpart');
DO $$
DECLARE
    toast regclass;
BEGIN
    SELECT reltoastrelid INTO toast FROM pg_class WHERE oid = 'f_perm'::regclass;
    PERFORM fasttrun_temp_is_ours(toast);
EXCEPTION WHEN wrong_object_type THEN
    RAISE NOTICE 'toast: %', regexp_replace(SQLERRM, 'pg_toast_[0-9]+', 'pg_toast_N');
END
$$;

-- Временная таблица другого сеанса - ошибка.  Если каталог временных таблиц
-- живет в памяти, другой сеанс ее не видит, и ответ NULL.
SELECT dblink_connect('f_other', 'host=' || :'HOST' || ' port=' || :'PORT' ||
                      ' dbname=' || current_database() || ' user=' || current_user);
SELECT dblink_exec('f_other', 'CREATE TEMP TABLE f_foreign (id int)');
SELECT set_config('fasttrun_fresh.foreign_oid', oid::text, false) IS NOT NULL AS got_oid
  FROM dblink('f_other', 'SELECT ''f_foreign''::regclass::oid') AS d(oid oid);
CREATE FUNCTION pg_temp.other_session(call text) RETURNS text LANGUAGE plpgsql AS $f$
DECLARE
    memory_catalog boolean :=
        coalesce(current_setting('enable_temp_memory_catalog', true), 'off') = 'on';
    r boolean;
BEGIN
    EXECUTE call INTO r;
    RETURN CASE WHEN r IS NULL AND memory_catalog THEN 'rejected'
                ELSE 'unexpected ' || coalesce(r::text, 'NULL') END;
EXCEPTION WHEN wrong_object_type THEN
    RETURN CASE WHEN memory_catalog THEN 'unexpected error' ELSE 'rejected' END;
END
$f$;
SELECT pg_temp.other_session(format('SELECT fasttrun_temp_is_ours(%s::oid::regclass)',
           current_setting('fasttrun_fresh.foreign_oid'))) AS other_session,
       pg_temp.other_session(format('SELECT fasttrun_temp_same_cols(%s::oid::regclass, ''f_perm'')',
           current_setting('fasttrun_fresh.foreign_oid'))) AS other_session_as_temp;
-- Шаблоном может быть и таблица другого сеанса: ответ по ее колонкам, а если
-- каталог временных таблиц в памяти и таблицы не видно - NULL.
CREATE TEMP TABLE f_t_one (id int);
SELECT CASE WHEN coalesce(current_setting('enable_temp_memory_catalog', true), 'off') = 'on'
            THEN CASE WHEN differs IS NULL AND matches IS NULL THEN 'ok'
                      ELSE format('unexpected %s/%s', differs, matches) END
            ELSE CASE WHEN differs IS FALSE AND matches IS TRUE THEN 'ok'
                      ELSE format('unexpected %s/%s', differs, matches) END
       END AS other_session_as_template
  FROM fasttrun_temp_same_cols('f_t', current_setting('fasttrun_fresh.foreign_oid')::oid::regclass) AS differs,
       fasttrun_temp_same_cols('f_t_one', current_setting('fasttrun_fresh.foreign_oid')::oid::regclass) AS matches;
SELECT dblink_disconnect('f_other');

-- Права на таблицу не проверяются: вызывающий получает ответ и для чужой таблицы.
CREATE ROLE regress_fasttrun_fresh NOLOGIN;
BEGIN;
TRUNCATE f_t;
SET LOCAL ROLE regress_fasttrun_fresh;
SELECT fr('f_t') AS fresh_without_privileges,
       fasttrun_temp_same_cols('f_t', 'f_perm') AS same_cols_without_privileges;
COMMIT;
DROP ROLE regress_fasttrun_fresh;
\set VERBOSITY default

-- ----------------------------------------------------------------------
-- 7. fasttrun_temp_same_cols
-- ----------------------------------------------------------------------
CREATE TABLE f_tpl (a int, gone1 int, b text, c varchar(10), gone2 int, d numeric(10, 2));
ALTER TABLE f_tpl DROP COLUMN gone1, DROP COLUMN gone2;

-- Шаблон с удаленными колонками и таблица по LIKE совпадают.
CREATE TEMP TABLE f_like (LIKE f_tpl);
SELECT fasttrun_temp_same_cols('f_like', 'f_tpl') AS like_template;

-- Умолчания, ограничения, индексы, identity и generated вне сравнения.
CREATE TEMP TABLE f_extra (a int GENERATED ALWAYS AS IDENTITY,
                           b text DEFAULT 'x' NOT NULL CHECK (b <> ''),
                           c varchar(10) UNIQUE,
                           d numeric(10, 2) GENERATED ALWAYS AS (a * 2) STORED);
SELECT fasttrun_temp_same_cols('f_extra', 'f_tpl') AS defaults_constraints_ignored;

-- Удаленные колонки временной таблицы тоже пропускаются.
CREATE TEMP TABLE f_tdrop (z int, a int, b text, y int, c varchar(10), d numeric(10, 2));
ALTER TABLE f_tdrop DROP COLUMN z, DROP COLUMN y;
SELECT fasttrun_temp_same_cols('f_tdrop', 'f_tpl') AS dropped_in_temp;

-- Отличия.
CREATE TEMP TABLE f_name (a int, bb text, c varchar(10), d numeric(10, 2));
CREATE TEMP TABLE f_type (a bigint, b text, c varchar(10), d numeric(10, 2));
CREATE TEMP TABLE f_typmod (a int, b text, c varchar(20), d numeric(10, 2));
CREATE TEMP TABLE f_coll (a int, b text COLLATE "C", c varchar(10), d numeric(10, 2));
CREATE TEMP TABLE f_order (b text, a int, c varchar(10), d numeric(10, 2));
CREATE TEMP TABLE f_more (a int, b text, c varchar(10), d numeric(10, 2), e int);
CREATE TEMP TABLE f_less (a int, b text, c varchar(10));
SELECT fasttrun_temp_same_cols('f_name', 'f_tpl') AS diff_name,
       fasttrun_temp_same_cols('f_type', 'f_tpl') AS diff_type,
       fasttrun_temp_same_cols('f_typmod', 'f_tpl') AS diff_typmod,
       fasttrun_temp_same_cols('f_coll', 'f_tpl') AS diff_collation,
       fasttrun_temp_same_cols('f_order', 'f_tpl') AS diff_order,
       fasttrun_temp_same_cols('f_more', 'f_tpl') AS diff_more,
       fasttrun_temp_same_cols('f_less', 'f_tpl') AS diff_less;

-- Колонка, добавленная к временной таблице, ломает совпадение.
ALTER TABLE f_like ADD COLUMN e int;
SELECT fasttrun_temp_same_cols('f_like', 'f_tpl') AS like_after_add_column;

-- Шаблоном может быть секционированная или временная таблица.
CREATE TABLE f_tpl_part (a int, b text, c varchar(10), d numeric(10, 2)) PARTITION BY RANGE (a);
SELECT fasttrun_temp_same_cols('f_tdrop', 'f_tpl_part') AS partitioned_template,
       fasttrun_temp_same_cols('f_tdrop', 'f_extra') AS temp_template;

-- Отсутствующая таблица - NULL, ошибки области как у fasttrun_temp_is_ours.
CREATE TEMP TABLE f_drop2 (id int);
SELECT 'f_drop2'::regclass::oid AS f_drop2_oid \gset
DROP TABLE f_drop2;
SELECT fasttrun_temp_same_cols(:f_drop2_oid::oid::regclass, 'f_tpl') IS NULL AS temp_missing,
       fasttrun_temp_same_cols('f_tdrop', :f_drop2_oid::oid::regclass) IS NULL AS template_missing,
       fasttrun_temp_same_cols(NULL, 'f_tpl') IS NULL AS temp_null,
       fasttrun_temp_same_cols('f_tdrop', NULL) IS NULL AS template_null;
\set VERBOSITY terse
SELECT fasttrun_temp_same_cols('f_perm', 'f_tpl');
SELECT fasttrun_temp_same_cols('f_tpart', 'f_tpl');
SELECT fasttrun_temp_same_cols('f_t_idx', 'f_tpl');
SELECT fasttrun_temp_same_cols('f_tdrop', 'f_view');
SELECT fasttrun_temp_same_cols('f_tdrop', 'f_t_idx');
\set VERBOSITY default

DROP TABLE f_perm, f_tpl, f_tpl_part;
DROP FUNCTION fr(text);
DROP EXTENSION dblink;
