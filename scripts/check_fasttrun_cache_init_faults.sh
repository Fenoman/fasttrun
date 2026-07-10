#!/usr/bin/env bash
#
# Проверяет безопасную инициализацию кешей анализа и статистики. Ошибка между
# созданием контекста и публикацией хеш-таблиц не должна оставлять неверные
# указатели, повторно устанавливать хук или занимать память TopMemoryContext.
# Каждый сценарий запускается в отдельном серверном процессе.
#
# FASTTRUN_CACHE_INIT_MODE=cassert: две тестовые ошибки и повторный вызов.
# FASTTRUN_CACHE_INIT_MODE=release: тестовая настройка не должна существовать
# после MarkGUCPrefixReserved("fasttrun").
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-cache-init.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_cache_init}
MODE=${FASTTRUN_CACHE_INIT_MODE:-cassert}

cleanup()
{
	if [ -f "$DATA/postmaster.pid" ]; then
		"$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
PORT=${PGPORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

"$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
"$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on" \
	-w start >/dev/null
"$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"
"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

if [ "$MODE" = release ]; then
	if "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
		-v ON_ERROR_STOP=1 \
		-c "LOAD 'fasttrun'; SET fasttrun.test_failpoint = 'after_analyze_context'" \
		>"$WORKDIR/release.out" 2>"$WORKDIR/release.err"; then
		echo "FAIL: release build accepted cassert-only fasttrun.test_failpoint" >&2
		exit 1
	fi
	if ! grep -q 'invalid configuration parameter name "fasttrun.test_failpoint"' \
		"$WORKDIR/release.err" ||
		! grep -q '"fasttrun" is a reserved prefix' "$WORKDIR/release.err"; then
		cat "$WORKDIR/release.err" >&2
		echo "FAIL: release rejection did not come from reserved fasttrun prefix" >&2
		exit 1
	fi
	echo "cache init release guard passed: test_failpoint is unavailable"
	exit 0
fi

if [ "$MODE" != cassert ]; then
	echo "unknown FASTTRUN_CACHE_INIT_MODE: $MODE" >&2
	exit 2
fi

debug_assertions=$("$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
	-XAtq -c 'SHOW debug_assertions')
if [ "$debug_assertions" != on ] && [ "${FASTTRUN_ALLOW_RELEASE_TEST_BUILD:-0}" != 1 ]; then
	echo "SKIP: cache-init fault injection requires a cassert PostgreSQL build" >&2
	exit 77
fi

cat >"$WORKDIR/analyze.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE TEMP TABLE ft_init_analyze (id int, grp int);
INSERT INTO ft_init_analyze SELECT g, g % 17 FROM generate_series(1, 1000) g;
SET fasttrun.test_failpoint = 'after_analyze_context';
DO $case$
DECLARE caught boolean := false;
BEGIN
  BEGIN
    PERFORM fasttrun_analyze('ft_init_analyze');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: after_analyze_context' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'after_analyze_context did not fire';
  END IF;
END
$case$;
SET fasttrun.test_failpoint = '';
SELECT fasttrun_analyze('ft_init_analyze');
SELECT count(*) FROM ft_init_analyze WHERE grp = 1;
EXPLAIN SELECT * FROM ft_init_analyze WHERE grp = 1;
SELECT 'ANALYZE_TOP ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun analyze cache'
  AND level = (SELECT min(level) FROM pg_backend_memory_contexts
               WHERE name = 'fasttrun analyze cache');
SELECT 'ANALYZE_HASH ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun analyze cache'
  AND level > (SELECT min(level) FROM pg_backend_memory_contexts
               WHERE name = 'fasttrun analyze cache');
SQL

cat >"$WORKDIR/stats.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
CREATE TEMP TABLE ft_init_stats (id int, grp int);
INSERT INTO ft_init_stats SELECT g, g % 19 FROM generate_series(1, 1000) g;
SET fasttrun.test_failpoint = 'after_stats_cache';
DO $case$
DECLARE caught boolean := false;
BEGIN
  BEGIN
    PERFORM fasttrun_collect_stats('ft_init_stats');
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'fasttrun test failpoint: after_stats_cache' THEN
      RAISE;
    END IF;
    caught := true;
  END;
  IF NOT caught THEN
    RAISE EXCEPTION 'after_stats_cache did not fire';
  END IF;
END
$case$;
SET fasttrun.test_failpoint = '';
SELECT fasttrun_collect_stats('ft_init_stats');
SELECT count(*) FROM fasttrun_inspect_stats('ft_init_stats');
EXPLAIN SELECT * FROM ft_init_stats WHERE grp = 1;
SELECT 'STATS_TOP ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun stats cache'
  AND level = (SELECT min(level) FROM pg_backend_memory_contexts
               WHERE name = 'fasttrun stats cache');
SELECT 'STATS_HASH ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun stats cache'
  AND level > (SELECT min(level) FROM pg_backend_memory_contexts
               WHERE name = 'fasttrun stats cache');
SELECT 'RELID_HASH ' || count(*)
FROM pg_backend_memory_contexts
WHERE name = 'fasttrun stats relid cache';
SQL

"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/analyze.sql" >"$WORKDIR/analyze.out" 2>"$WORKDIR/analyze.err" || {
	cat "$WORKDIR/analyze.err" >&2
	exit 1
}
"$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-f "$WORKDIR/stats.sql" >"$WORKDIR/stats.out" 2>"$WORKDIR/stats.err" || {
	cat "$WORKDIR/stats.err" >&2
	exit 1
}

value()
{
	awk -v key="$1" '$1 == key {print $2}' "$2" | tail -1
}

analyze_top=$(value ANALYZE_TOP "$WORKDIR/analyze.out")
analyze_hash=$(value ANALYZE_HASH "$WORKDIR/analyze.out")
stats_top=$(value STATS_TOP "$WORKDIR/stats.out")
stats_hash=$(value STATS_HASH "$WORKDIR/stats.out")
relid_hash=$(value RELID_HASH "$WORKDIR/stats.out")

for metric in "$analyze_top" "$analyze_hash" "$stats_top" "$stats_hash" "$relid_hash"; do
	case "$metric" in
		''|*[!0-9]*) echo "invalid cache-init metric: $metric" >&2; exit 1 ;;
	esac
done

echo "cache init metrics: analyze_top=$analyze_top analyze_hash=$analyze_hash stats_top=$stats_top stats_hash=$stats_hash relid_hash=$relid_hash"
if [ "$analyze_top" -ne 1 ] || [ "$analyze_hash" -ne 1 ] ||
	[ "$stats_top" -ne 1 ] || [ "$stats_hash" -ne 1 ] ||
	[ "$relid_hash" -ne 1 ]; then
	echo "FAIL: retry left a missing/duplicate cache context" >&2
	exit 1
fi

echo "cache init fault injection passed"
