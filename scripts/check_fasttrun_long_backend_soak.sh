#!/usr/bin/env bash
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$("$PG_CONFIG" --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PORT=${PGPORT:-55435}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-soak.XXXXXX)}
DBNAME=${DBNAME:-fasttrun_soak}
ITERATIONS=${ITERATIONS:-1000}
WARMUP=${WARMUP:-100}
MAX_GROWTH_BYTES=${MAX_GROWTH_BYTES:-262144}

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
		echo "missing command: $cmd" >&2
		exit 1
	fi
done

cat >"$WORKDIR/soak.sql" <<SQL
\\set ON_ERROR_STOP on
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun;
SELECT 'before' AS phase,
       coalesce(sum(total_bytes), 0) AS total_bytes,
       coalesce(sum(used_bytes), 0) AS used_bytes,
       count(*) AS contexts
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
\\o /dev/null
SQL

for i in $(seq 1 "$ITERATIONS"); do
	cat >>"$WORKDIR/soak.sql" <<'SQL'
BEGIN;
CREATE TEMP TABLE ft_soak (id int, payload text);
INSERT INTO ft_soak
SELECT g, md5(g::text)
FROM generate_series(1, 100) g;
SELECT fasttrun_analyze('ft_soak');
DROP TABLE ft_soak;
COMMIT;
SQL

	if [ "$i" -eq "$WARMUP" ]; then
		cat >>"$WORKDIR/soak.sql" <<SQL
\\o
SELECT 'after_warmup' AS phase,
       coalesce(sum(total_bytes), 0) AS total_bytes,
       coalesce(sum(used_bytes), 0) AS used_bytes,
       count(*) AS contexts
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
\\o /dev/null
SQL
	fi
done

cat >>"$WORKDIR/soak.sql" <<'SQL'
\o
SELECT 'after_final' AS phase,
       coalesce(sum(total_bytes), 0) AS total_bytes,
       coalesce(sum(used_bytes), 0) AS used_bytes,
       count(*) AS contexts
FROM pg_backend_memory_contexts
WHERE name LIKE 'fasttrun%';
SQL

"$INITDB" -D "$WORKDIR/data" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$WORKDIR/data" -o "-k $WORKDIR -p $PORT" \
	-l "$WORKDIR/postgres.log" -w start >/dev/null
"$CREATEDB" -h "$WORKDIR" -p "$PORT" "$DBNAME"

out=$("$PSQL" -h "$WORKDIR" -p "$PORT" -d "$DBNAME" -X -qAt \
	-v ON_ERROR_STOP=1 -F '|' -f "$WORKDIR/soak.sql")
printf '%s\n' "$out"

warm_total=$(printf '%s\n' "$out" | awk -F'|' '$1 == "after_warmup" {print $2}')
warm_used=$(printf '%s\n' "$out" | awk -F'|' '$1 == "after_warmup" {print $3}')
final_total=$(printf '%s\n' "$out" | awk -F'|' '$1 == "after_final" {print $2}')
final_used=$(printf '%s\n' "$out" | awk -F'|' '$1 == "after_final" {print $3}')

if [ -z "$warm_total" ] || [ -z "$warm_used" ] || \
   [ -z "$final_total" ] || [ -z "$final_used" ]; then
	echo "could not parse soak memory output" >&2
	exit 1
fi

if [ "$final_total" -gt $((warm_total + MAX_GROWTH_BYTES)) ] || \
   [ "$final_used" -gt $((warm_used + MAX_GROWTH_BYTES)) ]; then
	echo "fasttrun memory contexts grew after warmup: warm=${warm_total}/${warm_used}, final=${final_total}/${final_used}" >&2
	exit 1
fi

echo "long-lived backend soak passed: iterations=$ITERATIONS warmup=$WARMUP"
