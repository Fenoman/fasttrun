#!/bin/sh
#
# bench_percolumn.sh — чередующиеся серии HEAD и текущей сборки.
#
# Обе библиотеки собираются заранее и лежат в /tmp/fasttrun_head.dylib и
# /tmp/fasttrun_patch.dylib. Скрипт по очереди подкладывает их в каталог
# библиотек PostgreSQL и прогоняет bench_percolumn.sql, складывая строки вида
# «сборка|серия|метрика|миллисекунды» в raw.csv рядом с собой.
#
# Использование: PGPORT=54399 PGUSER=postgres SERIES=6 ./scripts/bench_percolumn.sh
#
set -e
DIR=$(cd "$(dirname "$0")" && pwd)
PG_CONFIG=${PG_CONFIG:-pg_config}
PSQL=$(dirname "$($PG_CONFIG --bindir)/psql")/psql
LIBDIR=$($PG_CONFIG --pkglibdir)
SERIES=${SERIES:-6}
DB=${BENCH_DB:-fasttrun_bench_db}

"$PSQL" -d postgres -qAtc "DROP DATABASE IF EXISTS $DB" >/dev/null
"$PSQL" -d postgres -qAtc "CREATE DATABASE $DB" >/dev/null
# Учёт по колонкам работает только при предзагрузке, а меряем именно его.
"$PSQL" -d postgres -qAtc \
	"ALTER DATABASE $DB SET session_preload_libraries = 'fasttrun'" >/dev/null
: > "$DIR/raw.csv"
s=1
while [ "$s" -le "$SERIES" ]; do
  for build in head patch; do
    cp "/tmp/fasttrun_${build}.dylib" "$LIBDIR/fasttrun.dylib"
    out=$(mktemp)
    "$PSQL" -d "$DB" -qAt -f "$DIR/bench_percolumn.sql" > "$out" 2>"$out.err" || {
      echo "  прогон не удался, см. $out.err" >&2
      exit 1
    }
    grep '|' "$out" \
      | awk -v b="$build" -v s="$s" -F'|' '{print b"|"s"|"$1"|"$2}' >> "$DIR/raw.csv"
    rm -f "$out" "$out.err"
    echo "  готово: серия $s, сборка $build" >&2
  done
  s=$((s + 1))
done
