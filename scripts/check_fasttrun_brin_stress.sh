#!/usr/bin/env bash
#
# Проверяет BRIN на скрытой и свежей локальной статистике. Smoke-режим входит
# в обязательный cassert-набор, full используется перед релизом и в nightly.
#
set -euo pipefail
export LC_ALL=C LANG=C

PG_CONFIG=${PG_CONFIG:-pg_config}
PG_BINDIR=$($PG_CONFIG --bindir)
PSQL=${PSQL:-"$PG_BINDIR/psql"}
INITDB=${INITDB:-"$PG_BINDIR/initdb"}
PG_CTL=${PG_CTL:-"$PG_BINDIR/pg_ctl"}
CREATEDB=${CREATEDB:-"$PG_BINDIR/createdb"}
PG_RUN_AS=${PG_RUN_AS:-}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/fasttrun-brin-stress.XXXXXX)}
DATA=$WORKDIR/data
SOCKET_DIR=$WORKDIR/socket
LOG=$WORKDIR/postgres.log
DBNAME=${DBNAME:-fasttrun_brin_stress}
LEVEL=${FASTTRUN_BRIN_LEVEL:-smoke}

case "$LEVEL" in
	smoke)
		row_count=20000
		iterations=12
		explains=2
		warmup=4
		;;
	full)
		row_count=50000
		iterations=50
		explains=5
		warmup=10
		;;
	*)
		echo "unknown FASTTRUN_BRIN_LEVEL: $LEVEL" >&2
		exit 2
		;;
esac

profiles=(default full)
modes=(on off)

run_pg()
{
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

cleanup()
{
	if [ -f "$DATA/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$DATA" -m fast -w stop >/dev/null 2>&1 || true
	fi
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
if [ -n "$PG_RUN_AS" ]; then
	command -v runuser >/dev/null 2>&1 || {
		echo "missing runuser" >&2
		exit 1
	}
	chown "$PG_RUN_AS" "$WORKDIR" "$SOCKET_DIR"
fi
PORT=${FASTTRUN_BRIN_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$DATA" --no-locale -E UTF8 >/dev/null
run_pg "$PG_CTL" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c shared_preload_libraries=fasttrun -c track_counts=on -c fsync=off -c full_page_writes=off -c synchronous_commit=off -c jit=off" \
	-w start >/dev/null
run_pg "$CREATEDB" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"
run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq \
	-v ON_ERROR_STOP=1 -c 'CREATE EXTENSION fasttrun' >/dev/null

debug_assertions=$(run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" \
	-d "$DBNAME" -XAtq -c 'SHOW debug_assertions')
if [ "$debug_assertions" != on ] && \
	[ "${FASTTRUN_ALLOW_RELEASE_TEST_BUILD:-0}" != 1 ]; then
	echo "SKIP: BRIN stress requires a cassert PostgreSQL build" >&2
	exit 77
fi

cat >"$WORKDIR/case.sql" <<'SQL'
\set ON_ERROR_STOP 1
LOAD 'fasttrun';
SET client_min_messages = warning;
SET statement_timeout = '15min';
SET fasttrun.invalidate_threshold = 0;
SET fasttrun.max_stats_memory = 0;
SELECT :'profile' = 'full' AS is_full \gset
\if :is_full
SET fasttrun.sample_rows = -1;
SET fasttrun.stats_refresh_threshold = 0;
SET fasttrun.use_typanalyze = on;
\else
RESET fasttrun.sample_rows;
RESET fasttrun.stats_refresh_threshold;
RESET fasttrun.use_typanalyze;
\endif
SELECT set_config('fasttrun.zero_sinval_truncate', :'zero_mode', false);

CREATE TEMP TABLE ft_brin_cfg AS
SELECT :row_count::int AS row_count,
       :iterations::int AS iterations,
       :explains::int AS explains,
       :warmup::int AS warmup;
CREATE TEMP TABLE ft_brin_result(
  iterations int,
  neutral_explains bigint,
  hidden_checks bigint,
  max_qerror double precision,
  warm_bytes bigint,
  final_bytes bigint,
  analyze_entries bigint,
  column_relids bigint,
  column_entries bigint
);
CREATE TEMP TABLE ft_brin_stress(id int NOT NULL, grp int NOT NULL);
CREATE INDEX ft_brin_stress_grp_idx
  ON ft_brin_stress USING brin(grp) WITH (pages_per_range = 4);

INSERT INTO ft_brin_stress
SELECT g, mod(g, 100)
FROM generate_series(1, (SELECT row_count FROM ft_brin_cfg)) g;
ANALYZE ft_brin_stress;

DO $case$
DECLARE
  cfg ft_brin_cfg%ROWTYPE;
  i int;
  j int;
  probe int;
  plan_line text;
  used_brin boolean;
  actual_rows bigint;
  estimated_rows bigint;
  plan_doc json;
  neutral_plan_doc json;
  neutral_estimated_rows bigint;
  qerror double precision;
  max_qerror double precision := 1;
  neutral_explains bigint := 0;
  hidden_checks bigint := 0;
  warm_analyze_entries bigint := 0;
  warm_column_relids bigint := 0;
  warm_column_entries bigint := 0;
  warm_analyze_bytes bigint := 0;
  warm_column_bytes bigint := 0;
  warm_bytes bigint := 0;
  final_analyze_entries bigint;
  final_column_relids bigint;
  final_column_entries bigint;
  final_analyze_bytes bigint;
  final_column_bytes bigint;
  final_bytes bigint;
  summarized_ranges int;
BEGIN
  SELECT * INTO cfg FROM ft_brin_cfg;

  FOR i IN 1..cfg.iterations LOOP
    PERFORM fasttruncate('ft_brin_stress');
    INSERT INTO ft_brin_stress
    SELECT g,
           CASE WHEN mod(i, 2) = 0
                THEN mod(g, 100)
                ELSE CASE WHEN g <= cfg.row_count * 8 / 10
                          THEN 1 ELSE mod(g, 100) END
           END
    FROM generate_series(1, cfg.row_count) g;

    SELECT brin_summarize_new_values('ft_brin_stress_grp_idx')
      INTO summarized_ranges;
    IF summarized_ranges <= 0 THEN
      RAISE EXCEPTION 'iteration % summarized no BRIN ranges', i;
    END IF;
    probe := CASE WHEN mod(i, 2) = 0 THEN 7 ELSE 1 END;

    /* Каталожная статистика остаётся, но хук скрывает её от планировщика. */
    IF (SELECT count(*) FROM pg_statistic
         WHERE starelid = 'ft_brin_stress'::regclass) <> 2 OR
       (SELECT count(*)
          FROM fasttrun_inspect_stats('ft_brin_stress')) <> 0 THEN
      RAISE EXCEPTION 'iteration % did not enter hidden-stats state', i;
    END IF;

    /* Планировщик должен получить стандартную оценку, а не старые данные. */
    PERFORM set_config('enable_seqscan', 'on', true);
    FOR j IN 1..cfg.explains LOOP
      EXECUTE format(
        'EXPLAIN (FORMAT JSON) SELECT * FROM ft_brin_stress WHERE grp = %s',
        probe)
        INTO neutral_plan_doc;
      neutral_estimated_rows :=
        (neutral_plan_doc->0->'Plan'->>'Plan Rows')::bigint;
      IF neutral_estimated_rows < cfg.row_count * 4 / 1000 OR
         neutral_estimated_rows > cfg.row_count * 6 / 1000 THEN
        RAISE EXCEPTION
          'iteration % exposed stale stats: neutral estimate % for % rows',
          i, neutral_estimated_rows, cfg.row_count;
      END IF;
      neutral_explains := neutral_explains + 1;
    END LOOP;
    hidden_checks := hidden_checks + 1;

    /* Отдельная канарейка доказывает, что сам BRIN-путь пригоден. */
    PERFORM set_config('enable_seqscan', 'off', true);
    used_brin := false;
    FOR plan_line IN EXECUTE format(
      'EXPLAIN (COSTS OFF) SELECT * FROM ft_brin_stress WHERE grp = %s',
      probe)
    LOOP
      used_brin := used_brin OR
        position('ft_brin_stress_grp_idx' IN plan_line) > 0;
    END LOOP;
    IF NOT used_brin THEN
      RAISE EXCEPTION 'forced iteration % did not use BRIN', i;
    END IF;

    PERFORM fasttrun_analyze('ft_brin_stress');
    PERFORM set_config('enable_seqscan', 'on', true);
    EXECUTE format(
      'SELECT count(*) FROM ft_brin_stress WHERE grp = %s', probe)
      INTO actual_rows;
    EXECUTE format(
      'EXPLAIN (FORMAT JSON) SELECT * FROM ft_brin_stress WHERE grp = %s',
      probe)
      INTO plan_doc;
    estimated_rows := (plan_doc->0->'Plan'->>'Plan Rows')::bigint;
    IF estimated_rows <= 0 OR actual_rows <= 0 THEN
      RAISE EXCEPTION 'invalid rows iteration %, estimate %, actual %',
        i, estimated_rows, actual_rows;
    END IF;
    qerror := greatest(estimated_rows::double precision / actual_rows,
                       actual_rows::double precision / estimated_rows);
    max_qerror := greatest(max_qerror, qerror);
    IF qerror > 3 THEN
      RAISE EXCEPTION
        'BRIN q-error iteration %, estimate %, actual %, qerror %',
        i, estimated_rows, actual_rows, qerror;
    END IF;

    IF i = cfg.warmup THEN
      SELECT analyze_entries, column_stats_relid_entries,
             column_stats_entries, analyze_bytes, column_stats_bytes,
             total_bytes
        INTO warm_analyze_entries, warm_column_relids,
             warm_column_entries, warm_analyze_bytes, warm_column_bytes,
             warm_bytes
      FROM fasttrun_cache_stats();
    END IF;
  END LOOP;

  SELECT analyze_entries, column_stats_relid_entries,
         column_stats_entries, analyze_bytes, column_stats_bytes,
         total_bytes
    INTO final_analyze_entries, final_column_relids,
         final_column_entries, final_analyze_bytes, final_column_bytes,
         final_bytes
  FROM fasttrun_cache_stats();
  IF warm_analyze_entries <= 0 OR warm_column_relids <> 1 OR
     warm_column_entries <> 2 OR
     final_analyze_entries <> warm_analyze_entries OR
     final_column_relids <> warm_column_relids OR
     final_column_entries <> warm_column_entries OR
     warm_bytes <> warm_analyze_bytes + warm_column_bytes OR
     final_bytes <> final_analyze_bytes + final_column_bytes OR
     final_bytes > warm_bytes + 8192 THEN
    RAISE EXCEPTION 'BRIN cache did not stabilize: warm %, final %',
      warm_bytes, final_bytes;
  END IF;

  INSERT INTO ft_brin_result
  VALUES (cfg.iterations, neutral_explains, hidden_checks, max_qerror,
          warm_bytes, final_bytes, final_analyze_entries,
          final_column_relids, final_column_entries);
END
$case$;

SELECT format('BRIN_CASE_OK|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
              :'profile', :'zero_mode', iterations, neutral_explains,
              hidden_checks, max_qerror, warm_bytes, final_bytes, analyze_entries,
              column_relids, column_entries)
FROM ft_brin_result;
SQL

case_count=0
neutral_total=0
for profile in "${profiles[@]}"; do
	for mode in "${modes[@]}"; do
		out="$WORKDIR/${profile}_${mode}.out"
		err="$WORKDIR/${profile}_${mode}.err"
		if ! run_pg "$PSQL" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" \
			-XAtq -v ON_ERROR_STOP=1 -v profile="$profile" \
			-v zero_mode="$mode" -v row_count="$row_count" \
			-v iterations="$iterations" -v explains="$explains" \
			-v warmup="$warmup" -f "$WORKDIR/case.sql" \
			>"$out" 2>"$err"; then
			cat "$out" "$err" >&2
			echo "FAIL: BRIN profile=$profile zero_sinval=$mode" >&2
			exit 1
		fi
		marker=$(grep '^BRIN_CASE_OK|' "$out" || true)
		if [ "$(printf '%s\n' "$marker" | grep -c '^BRIN_CASE_OK|')" -ne 1 ]; then
			cat "$out" "$err" >&2
			echo "FAIL: missing BRIN marker for $profile/$mode" >&2
			exit 1
		fi
		IFS='|' read -r marker_tag marker_profile marker_mode \
			marker_iterations marker_neutral marker_hidden _marker_qerror \
			_marker_warm _marker_final _marker_analyze_entries \
			_marker_column_relids _marker_column_entries <<<"$marker"
		expected_neutral=$((iterations * explains))
		for value in "$marker_iterations" "$marker_neutral" "$marker_hidden" \
			"$_marker_warm" "$_marker_final" \
			"$_marker_analyze_entries" "$_marker_column_relids" \
			"$_marker_column_entries"; do
			case "$value" in
				''|*[!0-9]*)
					echo "FAIL: non-numeric BRIN marker: $marker" >&2
					exit 1
					;;
			esac
		done
		if [ "$marker_tag" != BRIN_CASE_OK ] || \
			[ "$marker_profile" != "$profile" ] || \
			[ "$marker_mode" != "$mode" ] || \
			[ "$marker_iterations" -ne "$iterations" ] || \
			[ "$marker_neutral" -ne "$expected_neutral" ] || \
			[ "$marker_hidden" -ne "$iterations" ] || \
			[ "$_marker_analyze_entries" -le 0 ] || \
			[ "$_marker_column_relids" -ne 1 ] || \
			[ "$_marker_column_entries" -ne 2 ] || \
			! awk -v q="$_marker_qerror" 'BEGIN { exit !(q > 0 && q <= 3) }'; then
			cat "$out" "$err" >&2
			echo "FAIL: invalid BRIN marker for $profile/$mode: $marker" >&2
			exit 1
		fi
		case_count=$((case_count + 1))
		neutral_total=$((neutral_total + marker_neutral))
		echo "$marker"
	done
done

if [ "$case_count" -ne 4 ]; then
	echo "FAIL: expected 4 BRIN cases, got $case_count" >&2
	exit 1
fi
if grep -Eq 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|Segmentation fault|Abort trap' \
	"$LOG"; then
	grep -E 'TRAP|Assertion|FailedAssertion|PANIC|terminated by signal|server process .* was terminated|Segmentation fault|Abort trap' \
		"$LOG" >&2
	echo "FAIL: BRIN stress caused an assertion or server exit" >&2
	exit 1
fi

echo "BRIN_STRESS_OK level=$LEVEL cases=$case_count iterations=$iterations neutral_explains=$neutral_total"
