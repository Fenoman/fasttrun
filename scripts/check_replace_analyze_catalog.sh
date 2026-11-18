#!/usr/bin/env bash
#
# Проверка admin-скрипта replace_analyze_in_catalog.sql на фикстурах.
#
# Замена ANALYZE temp_x -> PERFORM fasttrun_analyze('temp_x') в pg_proc.prosrc
# обязана:
#   - заменять ANALYZE в позиции стейтмента (после ';', BEGIN/THEN/ELSE/LOOP);
#   - НЕ трогать вхождения внутри строковых литералов (EXECUTE 'ANALYZE ...;')
#     и в комментариях -- вставка кавычек ломает литерал, PERFORM невалиден
#     в динамическом SQL;
#   - оставлять все функции исполнимыми.
#
# UPDATE извлекается из самого admin-скрипта (маркеры APPLY_BEGIN/APPLY_END),
# так что проверяется ровно тот регекс, который уедет на прод.
#
set -euo pipefail

# macOS: postmaster падает с "became multithreaded during startup" под
# ru_RU-локалью окружения; C-локаль безвредна и для Linux.
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PORT=${PGPORT:-55461}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-replcat.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_replcat}
ADMIN_SQL=${ADMIN_SQL:-"$(dirname "$0")/replace_analyze_in_catalog.sql"}

cleanup()
{
	if [ -f "$WORKDIR/data/postmaster.pid" ]; then
		"$PG_CTL" -D "$WORKDIR/data" -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

for cmd in "$PSQL" "$INITDB" "$PG_CTL" "$CREATEDB"; do
	if ! command -v "$cmd" >/dev/null 2>&1; then
		echo "не нашёл команду: $cmd" >&2
		exit 1
	fi
done

if [ ! -r "$ADMIN_SQL" ]; then
	echo "не нашёл admin-скрипт: $ADMIN_SQL" >&2
	exit 1
fi

# Извлекаем применяемый блок (UPDATE + контрольный SELECT) из admin-скрипта.
sed -n '/^-- APPLY_BEGIN$/,/^-- APPLY_END$/p' "$ADMIN_SQL" \
	| sed -e '/^-- APPLY_/d' -e 's/^-- \{0,1\}//' >"$WORKDIR/apply.sql"
if ! grep -q "UPDATE pg_proc" "$WORKDIR/apply.sql"; then
	echo "не смог извлечь UPDATE между APPLY_BEGIN/APPLY_END" >&2
	exit 1
fi
echo "COMMIT;" >>"$WORKDIR/apply.sql"

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" -o "-k $WORKDIR -p $PORT -c listen_addresses=''" \
	-l "$WORKDIR/pg.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"

# Фикстуры: statement-позиции, литерал, комментарий.
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE FUNCTION f_plain() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    PERFORM 1;
    ANALYZE temp_b;
END
$body$;

CREATE FUNCTION f_then() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    IF true THEN ANALYZE temp_f; END IF;
END
$body$;

CREATE FUNCTION f_adjacent() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN ANALYZE temp_a; ANALYZE temp_b; END
$body$;

CREATE FUNCTION f_quoted() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE "temp name";
    ANALYZE "temp ""quoted""";
END
$body$;

CREATE FUNCTION f_exec() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    EXECUTE 'ANALYZE temp_d;';
END
$body$;

CREATE FUNCTION f_comment() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    -- ANALYZE temp_e;
    PERFORM 1;
END
$body$;

CREATE FUNCTION f_dollar() RETURNS text LANGUAGE plpgsql AS $body$
BEGIN
    RETURN $q$BEGIN ANALYZE temp_a; END$q$;
END
$body$;

CREATE FUNCTION f_block() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    /* BEGIN ANALYZE temp_a; END */
    RETURN;
END
$body$;

CREATE FUNCTION f_quoted_carrier() RETURNS integer LANGUAGE plpgsql AS $body$
DECLARE
    "temp BEGIN ANALYZE temp_a; END" integer := 7;
BEGIN
    RETURN "temp BEGIN ANALYZE temp_a; END";
END
$body$;

CREATE FUNCTION f_restricted_path() RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $body$
BEGIN
    ANALYZE temp_a;
END
$body$;

CREATE FUNCTION f_backslash_ident() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE "temp_\n";
END
$body$;

CREATE FUNCTION f_partial_opts() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE (VERBOSE) temp_b;
END
$body$;

CREATE FUNCTION f_legacy_verbose() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE VERBOSE temp_b;
END
$body$;

CREATE FUNCTION f_nospace_opts() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE(VERBOSE) temp_b;
END
$body$;

SET check_function_bodies = off;
CREATE FUNCTION f_only_target() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE ONLY temp_b;
END
$body$;

CREATE FUNCTION f_only_parens() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE ONLY (temp_b);
END
$body$;

CREATE FUNCTION f_dot_only_col() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE public.only(temp_col);
END
$body$;

CREATE FUNCTION f_atx_first() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    BEGIN AUTONOMOUS
        ANALYZE temp_b;
    END;
END
$body$;

CREATE FUNCTION f_atx_second() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    BEGIN AUTONOMOUS
        PERFORM 1;
        ANALYZE temp_b;
    END;
END
$body$;
RESET check_function_bodies;

CREATE FUNCTION f_utf8_temp_schema() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE temp_архив.sales;
END
$body$;

CREATE FUNCTION f_returns_found() RETURNS boolean LANGUAGE plpgsql AS $body$
BEGIN
    ANALYSE temp_a;
    RETURN FOUND;
END
$body$;

CREATE FUNCTION f_reads_row_count() RETURNS integer LANGUAGE plpgsql AS $body$
DECLARE
    rc integer;
BEGIN
    ANALYZE temp_a;
    GET DIAGNOSTICS rc = ROW_COUNT;
    RETURN rc;
END
$body$;

CREATE FUNCTION f_paren_alias() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    PERFORM 1 AS "(";
    ANALYZE (VERBOSE) temp_b;
END
$body$;

CREATE FUNCTION f_quoted_non_temp() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE public."sales temp_history";
END
$body$;

CREATE FUNCTION f_escaped_q_non_temp() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE public."sales ""temp_history""";
END
$body$;

CREATE FUNCTION f_perm_qcol() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE public.perm_tbl("fixed", temp_col);
END
$body$;

CREATE FUNCTION f_temp_prefix_schema() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE temp_archive.sales;
END
$body$;

CREATE FUNCTION f_nospace_qtarget() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE"temp b";
END
$body$;

CREATE FUNCTION f_adj_quote_ident() RETURNS void LANGUAGE plpgsql AS $body$
DECLARE
    x int := 1;
BEGIN
    ANALYZE temp_a;
    PERFORM "x"temp_alias;
    ANALYZE VERBOSE temp_b;
    PERFORM 1 AS "z";
END
$body$;

CREATE FUNCTION f_multi_target() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE VERBOSE public.perm_tbl, temp_b;
END
$body$;

CREATE FUNCTION f_british_plain() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYSE temp_a;
END
$body$;

CREATE FUNCTION f_british_analyse() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYSE VERBOSE temp_b;
END
$body$;

CREATE FUNCTION f_perm_col() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
    ANALYZE public.perm_tbl(temp_col);
END
$body$;

CREATE FUNCTION f_false_cand() RETURNS TABLE("analyze" int, temp_a int) LANGUAGE plpgsql AS $body$
BEGIN
    RETURN QUERY SELECT 1 AS "analyze", 2 AS temp_a;
END
$body$;
SQL

snapshot()
{
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-c "SELECT oid::text || E'\n' || prosrc FROM pg_proc WHERE proname = '$1'"
}

for fixture in f_exec f_comment f_dollar f_block f_quoted_carrier f_backslash_ident f_partial_opts f_legacy_verbose f_nospace_opts f_only_target f_only_parens f_atx_first f_atx_second f_returns_found f_reads_row_count f_paren_alias f_nospace_qtarget f_adj_quote_ident f_multi_target f_british_analyse f_false_cand; do
	snapshot "$fixture" >"$WORKDIR/before.$fixture"
done

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q --csv -v ON_ERROR_STOP=1 \
	-c "SELECT p.oid AS orig_oid, p.proname, p.prosrc AS orig_body FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' ORDER BY p.proname" \
	>"$WORKDIR/before.csv"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
	-f "$ADMIN_SQL" >"$WORKDIR/dry.out" 2>&1
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q --csv -v ON_ERROR_STOP=1 \
	-f "$ADMIN_SQL" >"$WORKDIR/dry.csv"
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q --csv -v ON_ERROR_STOP=1 \
	-f "$WORKDIR/apply.sql" >"$WORKDIR/apply.csv"

failed=0
assert()
{
	local label=$1 cond=$2
	local got
	got=$("$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt -c "SELECT ($cond)::text")
	if [ "$got" != "true" ]; then
		echo "FAIL: $label" >&2
		failed=1
	else
		echo "ok: $label"
	fi
}

assert "f_plain: обе замены выполнены" \
	"(SELECT prosrc !~* 'ANALYZE\s+temp_' AND prosrc LIKE '%fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%fasttrun_analyze(''temp_b'')%' FROM pg_proc WHERE proname='f_plain')"
assert "f_then: замена после THEN" \
	"(SELECT prosrc LIKE '%fasttrun_analyze(''temp_f'')%' FROM pg_proc WHERE proname='f_then')"
assert "f_adjacent: both adjacent statements are replaced" \
	"(SELECT prosrc !~* 'ANALYZE\s+temp_' AND prosrc LIKE '%fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%fasttrun_analyze(''temp_b'')%' FROM pg_proc WHERE proname='f_adjacent')"
assert "f_quoted: identifier quotes reach the relation parser" \
	"(SELECT position(\$want\$public.fasttrun_analyze('\"temp name\"')\$want\$ in prosrc) > 0 AND position(\$want\$public.fasttrun_analyze('\"temp \"\"quoted\"\"\"')\$want\$ in prosrc) > 0 FROM pg_proc WHERE proname='f_quoted')"
assert "f_restricted_path: schema-qualified replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' FROM pg_proc WHERE proname='f_restricted_path')"
assert "f_british_plain: British ANALYSE spelling is replaced" \
	"(SELECT prosrc !~* 'ANALY[SZ]E\s+temp_' AND prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' FROM pg_proc WHERE proname='f_british_plain')"
assert "f_perm_col: permanent column named temp_col does not block temp_a replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%ANALYZE public.perm_tbl(temp_col);%' FROM pg_proc WHERE proname='f_perm_col')"
assert "f_perm_qcol: quoted sibling column in permanent column list does not block temp_a replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%ANALYZE public.perm_tbl(\"fixed\", temp_col);%' FROM pg_proc WHERE proname='f_perm_qcol')"
assert "f_temp_prefix_schema: permanent schema prefixed with temp_ does not block temp_a replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%ANALYZE temp_archive.sales;%' FROM pg_proc WHERE proname='f_temp_prefix_schema')"
assert "f_quoted_non_temp: quoted non-temp table containing temp_ does not block temp_a replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%ANALYZE public.\"sales temp_history\";%' FROM pg_proc WHERE proname='f_quoted_non_temp')"
assert "f_escaped_q_non_temp: escaped quote inside non-temp table name does not block temp_a replacement" \
	"(SELECT prosrc LIKE '%public.fasttrun_analyze(''temp_a'')%' AND prosrc LIKE '%ANALYZE public.\"sales \"\"temp_history\"\"\";%' FROM pg_proc WHERE proname='f_escaped_q_non_temp')"

catalog_match=$("$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt -v ON_ERROR_STOP=1 <<SQL
CREATE TEMP TABLE before_snap (
    orig_oid oid,
    proname text,
    orig_body text
);
CREATE TEMP TABLE dry_preview (
    function_oid oid,
    function_name text,
    status text,
    reason text,
    original_body text,
    proposed_body text
);
CREATE TEMP TABLE apply_result (
    function_oid oid,
    function_name text,
    status text,
    reason text
);
\copy before_snap FROM '$WORKDIR/before.csv' WITH (FORMAT csv, HEADER true)
\copy dry_preview FROM '$WORKDIR/dry.csv' WITH (FORMAT csv, HEADER true)
\copy apply_result FROM '$WORKDIR/apply.csv' WITH (FORMAT csv, HEADER true)
WITH expected(proname, dry_status, apply_status, exp_reason, exp_body) AS (
    VALUES
        ('f_plain',              'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    PERFORM 1;\n    PERFORM public.fasttrun_analyze(''temp_b'');\nEND\n'),
        ('f_then',               'READY', 'APPLIED', '', E'\nBEGIN\n    IF true THEN PERFORM public.fasttrun_analyze(''temp_f''); END IF;\nEND\n'),
        ('f_adjacent',           'READY', 'APPLIED', '', E'\nBEGIN PERFORM public.fasttrun_analyze(''temp_a''); PERFORM public.fasttrun_analyze(''temp_b''); END\n'),
        ('f_quoted',             'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''"temp name"'');\n    PERFORM public.fasttrun_analyze(''"temp ""quoted"""'');\nEND\n'),
        ('f_restricted_path',    'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\nEND\n'),
        ('f_british_plain',      'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\nEND\n'),
        ('f_perm_col',           'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE public.perm_tbl(temp_col);\nEND\n'),
        ('f_perm_qcol',          'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE public.perm_tbl("fixed", temp_col);\nEND\n'),
        ('f_temp_prefix_schema', 'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE temp_archive.sales;\nEND\n'),
        ('f_utf8_temp_schema',   'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE temp_архив.sales;\nEND\n'),
        ('f_dot_only_col',       'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE public.only(temp_col);\nEND\n'),
        ('f_quoted_non_temp',    'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE public."sales temp_history";\nEND\n'),
        ('f_escaped_q_non_temp', 'READY', 'APPLIED', '', E'\nBEGIN\n    PERFORM public.fasttrun_analyze(''temp_a'');\n    ANALYZE public."sales ""temp_history""";\nEND\n'),
        ('f_exec',               'SKIP',  'SKIP',    'single quote', NULL),
        ('f_comment',            'SKIP',  'SKIP',    'line comment', NULL),
        ('f_dollar',             'SKIP',  'SKIP',    'dollar sign', NULL),
        ('f_block',              'SKIP',  'SKIP',    'block comment', NULL),
        ('f_quoted_carrier',     'SKIP',  'SKIP',    'semicolon in quoted identifier', NULL),
        ('f_backslash_ident',    'SKIP',  'SKIP',    'backslash in quoted identifier', NULL),
        ('f_atx_first',          'SKIP',  'SKIP',    'autonomous transaction block', NULL),
        ('f_atx_second',         'SKIP',  'SKIP',    'autonomous transaction block', NULL),
        ('f_returns_found',      'SKIP',  'SKIP',    'uses FOUND or ROW_COUNT', NULL),
        ('f_reads_row_count',    'SKIP',  'SKIP',    'uses FOUND or ROW_COUNT', NULL),
        ('f_partial_opts',       'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_legacy_verbose',     'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_nospace_opts',       'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_only_target',        'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_only_parens',        'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_paren_alias',        'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_nospace_qtarget',    'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_adj_quote_ident',    'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_multi_target',       'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_british_analyse',    'SKIP',  'SKIP',    'unsupported ANALYZE statement position or identifier', NULL),
        ('f_false_cand',         'SKIP',  'SKIP',    'no replaceable ANALYZE temp_* statement', NULL)
)
SELECT (count(*) = 34
    AND (SELECT count(*) FROM before_snap) = 34
    AND (SELECT count(*) FROM dry_preview) = 34
    AND (SELECT count(*) FROM apply_result) = 34
    AND bool_and(coalesce(
            p.oid IS NOT NULL
            AND b.orig_oid = p.oid
            AND d.function_oid = p.oid
            AND a.function_oid = p.oid
            AND d.function_name = p.oid::regprocedure::text
            AND a.function_name = p.oid::regprocedure::text
            AND d.status = e.dry_status
            AND a.status = e.apply_status
            AND coalesce(d.reason, '') = e.exp_reason
            AND coalesce(a.reason, '') = e.exp_reason
            AND b.orig_body IS NOT NULL
            AND d.original_body = b.orig_body
            AND d.proposed_body IS NOT NULL
            AND d.proposed_body = p.prosrc
            AND CASE WHEN e.dry_status = 'READY'
                     THEN e.exp_body IS NOT NULL AND p.prosrc = e.exp_body AND p.prosrc <> b.orig_body
                     ELSE p.prosrc = b.orig_body END,
            false)))::text
FROM expected e
LEFT JOIN pg_proc p ON p.proname = e.proname
LEFT JOIN before_snap b ON b.proname = e.proname
LEFT JOIN dry_preview d ON d.function_oid = p.oid
LEFT JOIN apply_result a ON a.function_oid = p.oid;
SQL
)
if [ "$catalog_match" = "true" ]; then
	echo "ok: exact per-OID original_body, expected prosrc, dry-run, and apply status verified for all 34 fixtures"
else
	echo "FAIL: per-OID dry-run or apply verification failed" >&2
	failed=1
fi

for fixture in f_exec f_comment f_dollar f_block f_quoted_carrier f_backslash_ident f_partial_opts f_legacy_verbose f_nospace_opts f_only_target f_only_parens f_atx_first f_atx_second f_returns_found f_reads_row_count f_paren_alias f_nospace_qtarget f_adj_quote_ident f_multi_target f_british_analyse f_false_cand; do
	snapshot "$fixture" >"$WORKDIR/after.$fixture"
	if cmp -s "$WORKDIR/before.$fixture" "$WORKDIR/after.$fixture"; then
		echo "ok: $fixture: skipped body and OID are unchanged"
	else
		echo "FAIL: $fixture: skipped body or OID changed" >&2
		failed=1
	fi
done

for fixture in f_plain f_then f_adjacent f_quoted f_restricted_path f_british_plain f_perm_col f_perm_qcol f_temp_prefix_schema f_utf8_temp_schema f_dot_only_col f_quoted_non_temp f_escaped_q_non_temp; do
	snapshot "$fixture" >"$WORKDIR/first.$fixture"
done
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
	-f "$WORKDIR/apply.sql" >"$WORKDIR/reapply.out" 2>&1
for fixture in f_plain f_then f_adjacent f_quoted f_restricted_path f_british_plain f_perm_col f_perm_qcol f_temp_prefix_schema f_utf8_temp_schema f_dot_only_col f_quoted_non_temp f_escaped_q_non_temp; do
	snapshot "$fixture" >"$WORKDIR/second.$fixture"
	if cmp -s "$WORKDIR/first.$fixture" "$WORKDIR/second.$fixture"; then
		echo "ok: $fixture: reapplying preserves body and OID"
	else
		echo "FAIL: $fixture: reapplying changes body or OID" >&2
		failed=1
	fi
done

# Все функции остаются исполнимыми, а функции с FOUND и ROW_COUNT сохраняют результат.
if ! "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 >/dev/null 2>"$WORKDIR/call.err" <<'SQL'
CREATE EXTENSION fasttrun;
CREATE SCHEMA temp_archive;
CREATE TABLE temp_archive.sales (x int);
CREATE SCHEMA temp_архив;
CREATE TABLE temp_архив.sales (x int);
CREATE TABLE public.perm_tbl (x int, "fixed" int, temp_col int);
CREATE TABLE public."sales temp_history" (x int);
CREATE TABLE public."sales ""temp_history""" (x int);
CREATE TEMP TABLE temp_a (x int);
CREATE TEMP TABLE temp_b (x int);
CREATE TEMP TABLE temp_d (x int);
CREATE TEMP TABLE temp_f (x int);
CREATE TEMP TABLE "temp b" (x int);
CREATE TEMP TABLE "temp name" (x int);
CREATE TEMP TABLE "temp ""quoted""" (x int);
CREATE TEMP TABLE "temp_\n" (x int);
SET standard_conforming_strings = off;
SELECT f_plain();
SELECT f_then();
SELECT f_adjacent();
SELECT f_quoted();
SELECT f_restricted_path();
SELECT f_backslash_ident();
SELECT f_partial_opts();
SELECT f_legacy_verbose();
SELECT f_nospace_opts();
DO $$
BEGIN
  IF current_setting('server_version_num')::int >= 180000 THEN
    PERFORM f_only_target();
    PERFORM f_only_parens();
  END IF;
  IF f_returns_found() IS DISTINCT FROM false THEN
    RAISE EXCEPTION 'f_returns_found changed return value: %', f_returns_found();
  END IF;
  IF f_reads_row_count() IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'f_reads_row_count changed ROW_COUNT: %', f_reads_row_count();
  END IF;
END $$;
SELECT f_paren_alias();
SELECT f_quoted_non_temp();
SELECT f_escaped_q_non_temp();
SELECT f_perm_qcol();
SELECT f_temp_prefix_schema();
SELECT f_utf8_temp_schema();
SELECT f_nospace_qtarget();
SELECT f_adj_quote_ident();
SELECT f_multi_target();
SELECT f_british_plain();
SELECT f_british_analyse();
SELECT f_perm_col();
SELECT count(*) FROM f_false_cand();
SELECT f_exec();
SELECT f_comment();
SELECT f_dollar();
SELECT f_block();
SELECT f_quoted_carrier();
SQL
then
	echo "FAIL: функции не исполняются после замены:" >&2
	cat "$WORKDIR/call.err" >&2
	failed=1
else
	echo "ok: все функции исполнимы"
fi

# Проверка схемы с символом '&' в имени: regexp_replace не должен подставлять
# совпавший текст ANALYZE вместо '&'.
if ! "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 >/dev/null 2>"$WORKDIR/amp.err" <<'SQL'
CREATE SCHEMA "ft&api";
ALTER EXTENSION fasttrun SET SCHEMA "ft&api";
CREATE FUNCTION f_amp_schema() RETURNS void LANGUAGE plpgsql AS $body$
BEGIN
    ANALYZE temp_a;
END
$body$;
SQL
then
	echo "FAIL: не удалось подготовить схему ft&api:" >&2
	cat "$WORKDIR/amp.err" >&2
	failed=1
else
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
		-f "$WORKDIR/apply.sql" >"$WORKDIR/amp_apply.out" 2>&1
	assert "f_amp_schema: literal ampersand preserved in schema qualification" \
		"(SELECT position(\$want\$\"ft&api\".fasttrun_analyze('temp_a')\$want\$ in prosrc) > 0 FROM pg_proc WHERE proname='f_amp_schema')"
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt -v ON_ERROR_STOP=1 \
		-c 'CREATE TEMP TABLE IF NOT EXISTS temp_a (x int); SELECT f_amp_schema();' >/dev/null

	# Verify both DRY RUN and APPLY succeed identically when standard_conforming_strings = off
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
		-c "CREATE FUNCTION f_scs_off() RETURNS void LANGUAGE plpgsql AS \$b\$ BEGIN ANALYZE temp_a; END \$b\$;" >/dev/null
	PGOPTIONS="-c standard_conforming_strings=off" \
		"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
		-f "$ADMIN_SQL" >"$WORKDIR/scs_off_dry.out" 2>&1
	PGOPTIONS="-c standard_conforming_strings=off" \
		"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
		-f "$WORKDIR/apply.sql" >"$WORKDIR/scs_off_apply.out" 2>&1
	assert "f_scs_off: standard_conforming_strings=off replacement" \
		"(SELECT position(\$want\$\"ft&api\".fasttrun_analyze('temp_a')\$want\$ in prosrc) > 0 FROM pg_proc WHERE proname='f_scs_off')"
fi

if [ "$failed" -ne 0 ]; then
	exit 1
fi

echo "проверка replace_analyze_in_catalog прошла"
