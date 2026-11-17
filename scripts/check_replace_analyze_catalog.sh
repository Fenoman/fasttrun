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
SQL

snapshot()
{
	"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-c "SELECT oid::text || E'\n' || prosrc FROM pg_proc WHERE proname = '$1'"
}

for fixture in f_exec f_comment f_dollar f_block f_quoted_carrier f_backslash_ident f_partial_opts; do
	snapshot "$fixture" >"$WORKDIR/before.$fixture"
done

"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
	-f "$WORKDIR/apply.sql" >"$WORKDIR/apply.out" 2>&1

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

for fixture in f_exec f_comment f_dollar f_block f_quoted_carrier f_backslash_ident f_partial_opts; do
	snapshot "$fixture" >"$WORKDIR/after.$fixture"
	if cmp -s "$WORKDIR/before.$fixture" "$WORKDIR/after.$fixture"; then
		echo "ok: $fixture: skipped body and OID are unchanged"
	else
		echo "FAIL: $fixture: skipped body or OID changed" >&2
		failed=1
	fi
done

for skipped in 'f_exec:single quote' 'f_comment:line comment' \
	'f_dollar:dollar sign' 'f_block:block comment' \
	'f_quoted_carrier:semicolon in quoted identifier' \
	'f_backslash_ident:backslash in quoted identifier' \
	'f_partial_opts:unsupported ANALYZE statement position or identifier'; do
	fixture=${skipped%%:*}
	reason=${skipped#*:}
	if grep -Eq "$fixture.*SKIP.*$reason" "$WORKDIR/apply.out"; then
		echo "ok: $fixture: explicit skip reason"
	else
		echo "FAIL: $fixture: missing skip reason: $reason" >&2
		failed=1
	fi
done

for fixture in f_plain f_then f_adjacent f_quoted; do
	snapshot "$fixture" >"$WORKDIR/first.$fixture"
done
"$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -q -v ON_ERROR_STOP=1 \
	-f "$WORKDIR/apply.sql" >"$WORKDIR/reapply.out" 2>&1
for fixture in f_plain f_then f_adjacent f_quoted; do
	snapshot "$fixture" >"$WORKDIR/second.$fixture"
	if cmp -s "$WORKDIR/first.$fixture" "$WORKDIR/second.$fixture"; then
		echo "ok: $fixture: reapplying preserves body and OID"
	else
		echo "FAIL: $fixture: reapplying changes body or OID" >&2
		failed=1
	fi
done

# Все функции остаются исполнимыми (plpgsql-валидность после правки prosrc).
if ! "$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
		-v ON_ERROR_STOP=1 >/dev/null 2>"$WORKDIR/call.err" <<'SQL'
CREATE EXTENSION fasttrun;
CREATE TEMP TABLE temp_a (x int);
CREATE TEMP TABLE temp_b (x int);
CREATE TEMP TABLE temp_d (x int);
CREATE TEMP TABLE temp_f (x int);
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
