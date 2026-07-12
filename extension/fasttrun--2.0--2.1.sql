\echo Use "ALTER EXTENSION fasttrun UPDATE TO '2.1'" to load this file. \quit

-- ======================================================================
-- Миграция fasttrun 2.0 -> 2.1
--
-- Что меняется в API:
--   * fasttruncate(text) — снова становится канонической C-функцией
--     расширения, если ранее была подменена внешней PL/pgSQL обёрткой;
--   * добавляется fasttrun_analyze(text) — in-memory ANALYZE без
--     записи в pg_class и без sinval-сообщений;
--   * добавляется fasttrun_relstats(text) — инспекционный helper.
--
-- Что не меняется:
--   * семантика fasttruncate (этот аспект — вопрос самой .so, а не
--     SQL-обёртки);
--   * существующий fasttruncate_c, если он есть в БД, не удаляется
--     и не пересоздаётся.  Подробности и причина см. в комментарии
--     к шагу 3 ниже.
-- ======================================================================


-- ----------------------------------------------------------------------
-- Шаг 1: если ранее в БД была создана внешняя PL/pgSQL обёртка
-- public.fasttruncate(text) поверх fasttruncate_c (этот паттерн
-- встречается в инсталляциях, где extension прошёл через rename
-- C-функции и подмену её PL/pgSQL функцией с тем же именем), —
-- дропаем эту обёртку.  Она не принадлежит расширению, поэтому
-- plain DROP FUNCTION срабатывает без ALTER EXTENSION DROP.
-- Освобождение имени нужно для шага 2: мы хотим видеть в роли
-- public.fasttruncate(text) каноническую C-функцию.
-- ----------------------------------------------------------------------
DO $cleanup_wrapper$
DECLARE
    _is_wrapper boolean;
BEGIN
    SELECT (l.lanname <> 'c')
      INTO _is_wrapper
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      JOIN pg_language  l ON l.oid = p.prolang
     WHERE n.nspname = 'public'
       AND p.proname = 'fasttruncate'
       AND p.pronargs = 1
     LIMIT 1;

    IF _is_wrapper THEN
        DROP FUNCTION public.fasttruncate(text);
        RAISE NOTICE 'fasttrun 2.0->2.1: дропнута внешняя PL/pgSQL обёртка public.fasttruncate(text)';
    END IF;
END
$cleanup_wrapper$;


-- ----------------------------------------------------------------------
-- Шаг 2: восстанавливаем (или подтверждаем) каноническую C-функцию
-- fasttruncate(text).  CREATE OR REPLACE покрывает оба сценария:
--
--   (a) обёртка только что была дропнута на шаге 1, функции с таким
--       именем сейчас нет — CREATE создаёт её;
--
--   (b) расширение upstream 2.0 никогда не проходило через rename,
--       и C-функция fasttruncate уже на месте — REPLACE подтверждает
--       её определение (фактически no-op, потому что мы пишем то же
--       самое: тот же C-символ в том же $libdir/fasttrun).
--
-- В обоих случаях, поскольку CREATE OR REPLACE выполняется внутри
-- ALTER EXTENSION UPDATE, PostgreSQL автоматически поддерживает
-- запись в pg_depend, делающую функцию owned by fasttrun.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttruncate(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;


-- ----------------------------------------------------------------------
-- Шаг 3: backward-compat алиас fasttruncate_c(text).
--
-- В установках, прошедших через rename C-функции (см. шаг 1), уже
-- существует fasttruncate_c — функция, на которую ссылается
-- какой-то клиентский PL/pgSQL код.  CREATE OR REPLACE здесь
-- идемпотентен:
--   * если функция уже существует и её определение совпадает —
--     операция почти бесплатная;
--   * если её нет (вариант, когда rename вообще не запускался) —
--     создаётся как чистый алиас на тот же C-символ "fasttruncate".
--
-- Алиас намеренно НЕ включён в fasttrun--2.1.sql (full install
-- для свежих БД): свежим установкам он не нужен, новый код должен
-- вызывать каноническое имя fasttruncate.  Он есть только здесь,
-- в migration-скрипте, чтобы существующий код, ссылавшийся на
-- legacy-имя, не сломался при обновлении.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttruncate_c(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttruncate'
LANGUAGE C RETURNS NULL ON NULL INPUT VOLATILE;


-- ----------------------------------------------------------------------
-- Шаг 4: новые в 2.1
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fasttrun_analyze(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_analyze'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_relstats(IN  rel_name text,
                                             OUT relpages int,
                                             OUT reltuples real)
RETURNS record AS 'MODULE_PATHNAME', 'fasttrun_relstats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_collect_stats(text)
RETURNS void AS 'MODULE_PATHNAME', 'fasttrun_collect_stats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_inspect_stats(rel_name text)
RETURNS SETOF pg_catalog.pg_statistic
AS 'MODULE_PATHNAME', 'fasttrun_inspect_stats'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_hot_temp_tables(
    n int DEFAULT 100,
    OUT relname text,
    OUT create_count bigint,
    OUT last_create timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fasttrun_hot_temp_tables'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_prewarm()
RETURNS int
AS 'MODULE_PATHNAME', 'fasttrun_prewarm'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION fasttrun_reset_temp_stats()
RETURNS void
AS 'MODULE_PATHNAME', 'fasttrun_reset_temp_stats'
LANGUAGE C VOLATILE;
