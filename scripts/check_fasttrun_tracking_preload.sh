#!/bin/sh
#
# check_fasttrun_tracking_preload.sh - регрессионный прогон при активном
# shared_preload_libraries = 'fasttrun'.
#
# Зачем. Обязательный cassert-прогон поднимает основные серверы без fasttrun в
# shared_preload_libraries, поэтому этот скрипт поднимает отдельный кластер с
# предзагрузкой и прогоняет на нем все совместимые наборы REGRESS из Makefile
# (все, кроме единственного по контракту cold-only набора fasttrun_stats_cold).
#
# Что проверяет:
# - все совместимые с предзагрузкой наборы REGRESS проходят на кластере с
#   shared_preload_libraries = 'fasttrun'
# - fasttrun_tracking идет по основной ветке общего реестра, а не по запасному
#   эталону expected/fasttrun_tracking_1.out
# - счетчики fasttrun_counters() при предзагрузке агрегируются по кластеру,
#   емкость реестра ограничена 8192 записями, сохранение переживает рестарт,
#   имена из разных кодировок БД читаются без ошибок.
#
# Расширение должно быть установлено в сборку PG_CONFIG. Скрипт печатает PASS
# или FAIL и не требует cassert-сборки.
#
set -eu
# На macOS postmaster отказывается стартовать при неустановленной локали.
: "${LC_ALL:=C}"
export LC_ALL
unset PGOPTIONS
PGCONNECT_TIMEOUT=10
export PGCONNECT_TIMEOUT
PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
REPO=$(cd "$(dirname "$0")/.." && pwd)
WORKDIR=$(mktemp -d)
DATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/sock"
LOG="$WORKDIR/log"
OUT="$WORKDIR/regress"
PORT=${PGPORT_CHECK:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
# Строка вывода основной ветки. pg_regress печатает и исходный текст теста, где
# эта фраза тоже есть, поэтому нужна именно строка NOTICE.
MAIN_BRANCH='^NOTICE:  shmem available'

# Каталог удаляется только после подтвержденной остановки сервера. Иначе он
# остается для разбора, а проверка проваливается.
cleanup() {
	rc=$?
	if [ -f "$DATA/postmaster.pid" ] &&
		! "$BINDIR/pg_ctl" -D "$DATA" -w -t 60 stop >/dev/null 2>&1; then
		echo "tracking preload: сервер не остановился, каталог сохранен: $WORKDIR"
		exit 1
	fi
	# Сбой удаления не должен подменять исходный код выхода.
	rm -rf "$WORKDIR" || :
	exit "$rc"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR" "$OUT"
"$BINDIR/initdb" -D "$DATA" --no-locale -E UTF8 >/dev/null
cat >>"$DATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$SOCKET_DIR'
port = $PORT
shared_preload_libraries = 'fasttrun'
CONF
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -w start >/dev/null

# Derive the full preload suite list directly from Makefile REGRESS, excluding
# only fasttrun_stats_cold (whose sole contract is testing a non-preloaded backend).
ALL_REGRESS=$(python3 - "$REPO/Makefile" <<'PY'
import re, sys
lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
parts = []
for i, line in enumerate(lines):
    m = re.match(r"^REGRESS\s*=\s*(.*)$", line)
    if not m:
        continue
    val = m.group(1)
    while True:
        cont = val.rstrip().endswith("\\")
        parts.extend(val.rstrip().removesuffix("\\").split())
        if not cont or i + 1 >= len(lines):
            break
        i += 1
        val = lines[i].strip()
    break
print(" ".join(parts))
PY
)
REGRESS_PRELOAD=""
EXPECTED_PRELOAD=0
COLD_SKIPPED=0
for suite in $ALL_REGRESS; do
	if [ "$suite" = "fasttrun_stats_cold" ]; then
		COLD_SKIPPED=$((COLD_SKIPPED + 1))
		continue
	fi
	REGRESS_PRELOAD="${REGRESS_PRELOAD:+$REGRESS_PRELOAD }$suite"
	EXPECTED_PRELOAD=$((EXPECTED_PRELOAD + 1))
done
if [ "$EXPECTED_PRELOAD" -le 0 ] || [ "$COLD_SKIPPED" -ne 1 ]; then
	echo "FAIL: unexpected Makefile REGRESS partition: preload=$EXPECTED_PRELOAD cold=$COLD_SKIPPED" >&2
	exit 1
fi

rc=0
make -C "$REPO" -s installcheck PG_CONFIG="$PG_CONFIG" REGRESS="$REGRESS_PRELOAD" \
	REGRESS_OPTS="--outputdir=$OUT" PGHOST="$SOCKET_DIR" PGPORT="$PORT" \
	>"$WORKDIR/installcheck.log" 2>&1 || rc=$?
passed=$(grep -cE '^ok ' "$WORKDIR/installcheck.log" 2>/dev/null || true)
passed=$(printf '%s' "$passed" | tr -dc '0-9')
passed=${passed:-0}
failed=$(grep -cE '^not ok ' "$WORKDIR/installcheck.log" 2>/dev/null || true)
failed=$(printf '%s' "$failed" | tr -dc '0-9')
failed=${failed:-0}
result="$OUT/results/fasttrun_tracking.out"
main=$(grep -c "$MAIN_BRANCH" "$result" 2>/dev/null || true)
echo "installcheck rc=$rc passed=$passed/$EXPECTED_PRELOAD (cold-only excluded=$COLD_SKIPPED) main branch lines=${main:-0}"

PSQL="$BINDIR/psql -X -A -t -q -v ON_ERROR_STOP=1 -h $SOCKET_DIR -p $PORT -d postgres"
counters=""
capacity=""
restored=""
encoding=""
counters=$($PSQL -c "CREATE EXTENSION IF NOT EXISTS fasttrun" \
	-c "SELECT fasttrun_counters_reset()" \
	-c "CREATE TEMP TABLE ft_counter_probe (id int)" \
	-c "INSERT INTO ft_counter_probe SELECT generate_series(1, 100)" \
	-c "SELECT fasttrun_analyze('ft_counter_probe')" >/dev/null 2>&1 &&
	$PSQL -c "SELECT scope || ' ' || value FROM fasttrun_counters() WHERE counter = 'analyze_calls'" 2>&1) ||
	counters="error: $counters"
echo "cluster counters: $counters"

capacity=$($PSQL <<'SQL'
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
CREATE SCHEMA dummy_tmp;
CREATE TABLE dummy_tmp.ft_capacity_template (id int);
SELECT fasttrun_reset_temp_stats();
SELECT fasttrun_counters_reset();
\o /dev/null
SELECT format('CREATE TEMP TABLE ft_capacity_%s (LIKE dummy_tmp.ft_capacity_template); DROP TABLE ft_capacity_%s;', n, n)
FROM generate_series(1, 8400) n
\gexec
CREATE TEMP TABLE ft_capacity_1 (LIKE dummy_tmp.ft_capacity_template);
DROP TABLE ft_capacity_1;
\o
SELECT 'capacity ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(0)) || ' ' ||
       (SELECT value FROM fasttrun_counters() WHERE counter = 'tracking_rejected') || ' ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(32)) || ' ' ||
       (SELECT count(*) FROM fasttrun_hot_temp_tables(8192)) || ' ' ||
       (SELECT create_count FROM fasttrun_hot_temp_tables(0) WHERE relname = 'ft_capacity_1');
SQL
) || capacity="error: $capacity"
echo "$capacity"

"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" -m fast -w restart >/dev/null
restored=$($PSQL -c "SELECT count(*) || ' ' || max(create_count) FROM fasttrun_hot_temp_tables(0)") ||
	restored="error: $restored"
echo "capacity restored: $restored"

$PSQL -c 'SELECT fasttrun_reset_temp_stats()' >/dev/null
"$BINDIR/createdb" -h "$SOCKET_DIR" -p "$PORT" -T template0 -E LATIN1 --locale=C ft_tracking_latin1
$PSQL -d ft_tracking_latin1 <<'SQL'
CREATE EXTENSION fasttrun;
CREATE SCHEMA dummy_tmp;
CREATE TABLE dummy_tmp.encoding_template(id int);
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
DO $test$ DECLARE name text := 'ft_encoded_' || convert_from(decode('e9','hex'),'LATIN1');
BEGIN
  EXECUTE format('CREATE TEMP TABLE %I(LIKE dummy_tmp.encoding_template)', name);
END $test$;
SQL
encoding=$($PSQL <<'SQL'
SET fasttrun.track_schedule = '';
SET fasttrun.track_temp_creates = on;
CREATE TEMP TABLE ft_ascii(LIKE dummy_tmp.ft_capacity_template);
DO $test$ DECLARE name text := 'ft_utf8_' || convert_from(decode('c3a9','hex'),'UTF8');
BEGIN
  EXECUTE format('CREATE TEMP TABLE %I(LIKE dummy_tmp.ft_capacity_template)', name);
END $test$;
SELECT 'encoding ' || (count(*) = 3 AND sum(create_count) = 3
  AND bool_and(octet_length(convert_to(relname, 'LATIN1')) > 0)
  AND bool_or(relname = E'\\x66745f656e636f6465645fe9')
  AND bool_or(relname = 'ft_ascii')
  AND bool_or(relname = 'ft_utf8_' || convert_from(decode('c3a9','hex'),'UTF8')))
FROM fasttrun_hot_temp_tables(0);
SQL
) || encoding="error: $encoding"
echo "$encoding"

crashes=$(grep -cE 'TRAP|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|leaked hash_seq_search' "$LOG" || true)
if [ "$rc" -eq 0 ] && [ "$passed" -eq "$EXPECTED_PRELOAD" ] && [ "$failed" -eq 0 ] &&
	[ "${main:-0}" -ge 1 ] && [ "$counters" = "cluster 1" ] && [ "$crashes" -eq 0 ]; then
	if [ "$(printf '%s\n' "$capacity" | tail -1)" = "capacity 8192 208 32 8192 2" ] &&
		[ "$restored" = "8192 2" ] && [ "$encoding" = 'encoding true' ]; then
		echo "tracking preload: PASS"
		exit 0
	fi
fi
echo "tracking preload: FAIL"
tail -5 "$WORKDIR/installcheck.log" || true
head -40 "$OUT/regression.diffs" 2>/dev/null || true
grep -n "shmem" "$result" 2>/dev/null || true
exit 1
