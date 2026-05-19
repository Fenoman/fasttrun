#!/usr/bin/env python3
"""Compare planner estimates from ANALYZE vs fasttrun_analyze.

The harness intentionally checks bounded parity, not byte-for-byte plan
identity.  Sampling can differ between core ANALYZE and fasttrun_analyze, but
fasttrun should not fall back to catastrophic/default estimates for supported
temp-table statistics.
"""


import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Scenario:
    name: str
    setup_sql: str
    query_sql: str
    expected_rows: Optional[int] = None
    template_sql: Optional[str] = None
    full_ratio: float = 3.0
    default_ratio: float = 5.0


ANALYZE_FAST = "DO $$ BEGIN PERFORM public.fasttrun_analyze('t'); END $$;"
ANALYZE_CORE = "ANALYZE t;"


def command_output(cmd):
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, check=True)
    return result.stdout.strip()


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(cmd, **kwargs):
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True, **kwargs)


def plan_rows(plan):
    return int(plan[0]["Plan"]["Plan Rows"])


def ratio_ok(core_rows, fast_rows, scenario, profile):
    if core_rows == fast_rows:
        return True, "exact"

    if core_rows <= 10:
        max_fast = max(20, core_rows * 20)
        if 0 <= fast_rows <= max_fast:
            return True, f"small-row tolerance <= {max_fast}"
        return False, f"small-row estimate {fast_rows} > {max_fast}"

    ratio = fast_rows / core_rows
    max_ratio = scenario.full_ratio if profile == "full" else scenario.default_ratio
    min_ratio = 1.0 / max_ratio
    if min_ratio <= ratio <= max_ratio:
        return True, f"ratio={ratio:.3f}"
    return False, f"ratio={ratio:.3f}, expected {min_ratio:.3f}..{max_ratio:.3f}"


def expected_ok(rows, scenario):
    if scenario.expected_rows is None:
        return True, "no expected row bound"

    expected = scenario.expected_rows
    if expected <= 10:
        upper = max(20, expected * 20)
        lower = 0
    else:
        lower = max(1, int(expected * 0.05))
        upper = max(20, int(expected * 20))

    if lower <= rows <= upper:
        return True, f"expected-bound {lower}..{upper}"
    return False, f"estimate {rows} outside expected-bound {lower}..{upper}"


def psql_json(psql, socket_dir, port, dbname, sql):
    cmd = [
        str(psql), "-h", str(socket_dir), "-p", str(port), "-d", dbname,
        "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
    ]
    result = subprocess.run(cmd, input=sql, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(
            "psql failed\n"
            f"command: {' '.join(cmd)}\n"
            f"stderr:\n{result.stderr}\n"
            f"sql:\n{sql}"
        )
    out = result.stdout.strip()
    if not out:
        raise RuntimeError("psql returned empty output")
    return json.loads(out)


def profile_sql(profile):
    if profile == "full":
        return "\n".join([
            "SET fasttrun.sample_rows = -1;",
            "SET fasttrun.stats_refresh_threshold = 0;",
            "SET fasttrun.use_typanalyze = on;",
        ])
    return "\n".join([
        "RESET fasttrun.sample_rows;",
        "RESET fasttrun.stats_refresh_threshold;",
        "RESET fasttrun.use_typanalyze;",
    ])


def build_sql(scenario, variant, profile):
    analyze = ANALYZE_CORE if variant == "core" else ANALYZE_FAST
    prelude = f"""
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS fasttrun WITH SCHEMA public;
SET search_path = public, pg_temp;
{profile_sql(profile)}
"""
    explain = f"EXPLAIN (FORMAT JSON) {scenario.query_sql};"

    if scenario.template_sql is not None:
        body = scenario.template_sql.format(analyze=analyze)
        return f"{prelude}\n{body}\n{explain}\n"

    return f"{prelude}\n{scenario.setup_sql}\n{analyze}\n{explain}\n"


def scenarios():
    base_50k = """
CREATE TEMP TABLE t (id int, grp int, status text, name varchar(50),
                     amount numeric(10,2), created timestamptz,
                     flag boolean, nullable int);
INSERT INTO t
SELECT g,
       g % 1000,
       CASE WHEN g % 100 < 60 THEN 'active'
            WHEN g % 100 < 80 THEN 'pending'
            WHEN g % 100 < 90 THEN 'done'
            WHEN g % 100 < 95 THEN 'archived'
            ELSE 'new' END,
       'name_' || g,
       (g % 10000)::numeric / 100,
       '2026-01-01'::timestamptz + (g || ' minutes')::interval,
       g % 2 = 0,
       CASE WHEN g % 10 = 0 THEN NULL ELSE g % 100 END
FROM generate_series(1, 50000) g;
CREATE INDEX ON t (id);
CREATE INDEX ON t (grp);
CREATE INDEX ON t (status);
CREATE INDEX ON t (name);
CREATE INDEX ON t (amount);
CREATE INDEX ON t (flag);
CREATE INDEX ON t (nullable);
"""

    return [
        Scenario("uniform_int_equality", base_50k,
                 "SELECT * FROM t WHERE id = 12345", 1),
        Scenario("skew_text_mcv_common", base_50k,
                 "SELECT * FROM t WHERE status = 'active'", 30000),
        Scenario("skew_text_mcv_less_common", base_50k,
                 "SELECT * FROM t WHERE status = 'archived'", 2500),
        Scenario("numeric_histogram_range", base_50k,
                 "SELECT * FROM t WHERE amount BETWEEN 10 AND 20", 5000),
        Scenario("timestamptz_histogram_range", base_50k,
                 "SELECT * FROM t WHERE created > '2026-01-15'::timestamptz",
                 29840),
        Scenario("nullable_is_null", base_50k,
                 "SELECT * FROM t WHERE nullable IS NULL", 5000),
        Scenario("varchar_high_cardinality", base_50k,
                 "SELECT * FROM t WHERE name = 'name_25000'", 1),
        Scenario("boolean_equality", base_50k,
                 "SELECT * FROM t WHERE flag = true", 25000),
        Scenario("partial_index_range", """
CREATE TEMP TABLE t (id int, flag boolean, payload text);
INSERT INTO t
SELECT g, g % 2 = 0, repeat('x', 16)
FROM generate_series(1, 50000) g;
CREATE INDEX t_partial_idx ON t (id) WHERE flag;
""", "SELECT * FROM t WHERE flag AND id BETWEEN 1000 AND 2000", 501),
        Scenario("multicolumn_index_correlated", """
CREATE TEMP TABLE t (a int, b int, payload text);
INSERT INTO t
SELECT g % 100, g % 1000, repeat('x', 16)
FROM generate_series(1, 50000) g;
CREATE INDEX ON t (a, b);
""", "SELECT * FROM t WHERE a = 42 AND b = 142"),
        Scenario("subquery_rte_gating", """
CREATE TEMP TABLE t (id int, grp int);
INSERT INTO t SELECT g, g % 1000 FROM generate_series(1, 50000) g;
CREATE INDEX ON t (grp);
""", "SELECT * FROM (SELECT * FROM t) s WHERE grp = 42", 50),
        Scenario("cte_rte_gating", """
CREATE TEMP TABLE t (id int, grp int);
INSERT INTO t SELECT g, g % 1000 FROM generate_series(1, 50000) g;
CREATE INDEX ON t (grp);
""", "WITH s AS (SELECT * FROM t) SELECT * FROM s WHERE grp = 42", 50),
        Scenario("savepoint_release_visibility", "", """
SELECT * FROM t WHERE grp = 777
""", 150, template_sql="""
CREATE TEMP TABLE t (id int, grp int);
INSERT INTO t SELECT g, g % 1000 FROM generate_series(1, 50000) g;
CREATE INDEX ON t (grp);
BEGIN;
SAVEPOINT s;
INSERT INTO t SELECT 50000 + g, 777 FROM generate_series(1, 100) g;
{analyze}
RELEASE SAVEPOINT s;
"""),
        Scenario("savepoint_rollback_restore", "", """
SELECT * FROM t WHERE grp = 42
""", 50, template_sql="""
CREATE TEMP TABLE t (id int, grp int);
INSERT INTO t SELECT g, g % 1000 FROM generate_series(1, 50000) g;
CREATE INDEX ON t (grp);
BEGIN;
{analyze}
SAVEPOINT s;
INSERT INTO t SELECT 50000 + g, 42 FROM generate_series(1, 10000) g;
{analyze}
ROLLBACK TO SAVEPOINT s;
"""),
        Scenario("commit_stats_survive", "", """
SELECT * FROM t WHERE status = 'active'
""", 30000, template_sql=f"""
{base_50k}
BEGIN;
{{analyze}}
COMMIT;
"""),
        Scenario("drop_recreate_relid_churn", "", """
SELECT * FROM t WHERE status = 'active'
""", 5000, template_sql="""
CREATE TEMP TABLE t (id int, status text);
INSERT INTO t
SELECT g, CASE WHEN g % 10 < 9 THEN 'active' ELSE 'cold' END
FROM generate_series(1, 50000) g;
CREATE INDEX ON t (status);
{analyze}
DROP TABLE t;
CREATE TEMP TABLE t (id int, status text);
INSERT INTO t
SELECT g, CASE WHEN g % 10 = 0 THEN 'active' ELSE 'cold' END
FROM generate_series(1, 50000) g;
CREATE INDEX ON t (status);
{analyze}
"""),
    ]


def start_cluster(args):
    pg_config = Path(args.pg_config)
    bindir = Path(command_output([str(pg_config), "--bindir"]))
    workdir = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix="fasttrun-parity."))
    workdir.mkdir(parents=True, exist_ok=True)
    data = workdir / "data"
    port = args.port or free_port()
    socket_dir = workdir

    initdb = bindir / "initdb"
    pg_ctl = bindir / "pg_ctl"
    createdb = bindir / "createdb"
    psql = bindir / "psql"

    run([str(initdb), "-D", str(data), "--no-locale", "-E", "UTF8"])
    run([str(pg_ctl), "-D", str(data), "-o", f"-k {socket_dir} -p {port}",
         "-l", str(workdir / "postgres.log"), "-w", "start"])
    run([str(createdb), "-h", str(socket_dir), "-p", str(port),
         args.dbname])
    return workdir, data, pg_ctl, psql, socket_dir, port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("full", "default"), default="full")
    parser.add_argument("--pg-config", default=os.environ.get("PG_CONFIG", "pg_config"))
    parser.add_argument("--port", type=int, default=int(os.environ["PGPORT"]) if os.environ.get("PGPORT") else None)
    parser.add_argument("--workdir")
    parser.add_argument("--dbname", default="fasttrun_parity")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    workdir = None
    data = None
    pg_ctl = None
    failures = []

    try:
        workdir, data, pg_ctl, psql, socket_dir, port = start_cluster(args)
        print(f"PROFILE {args.profile}")
        print(f"WORKDIR {workdir}")

        for scenario in scenarios():
            core_plan = psql_json(psql, socket_dir, port, args.dbname,
                                  build_sql(scenario, "core", args.profile))
            fast_plan = psql_json(psql, socket_dir, port, args.dbname,
                                  build_sql(scenario, "fast", args.profile))
            core_rows = plan_rows(core_plan)
            fast_rows = plan_rows(fast_plan)

            ok, detail = ratio_ok(core_rows, fast_rows, scenario, args.profile)
            exp_core_ok, exp_core = expected_ok(core_rows, scenario)
            exp_fast_ok, exp_fast = expected_ok(fast_rows, scenario)
            if not (ok and exp_core_ok and exp_fast_ok):
                failures.append((scenario.name, core_rows, fast_rows, detail,
                                 exp_core, exp_fast))
                status = "FAIL"
            else:
                status = "PASS"

            ratio = "inf" if core_rows == 0 else f"{fast_rows / core_rows:.3f}"
            print(f"{status} {scenario.name}: core_rows={core_rows} "
                  f"fast_rows={fast_rows} ratio={ratio} {detail}")

        if failures:
            print("SUMMARY failed")
            for name, core_rows, fast_rows, detail, exp_core, exp_fast in failures:
                print(f"FAIL_DETAIL {name}: core={core_rows} fast={fast_rows}; "
                      f"{detail}; core {exp_core}; fast {exp_fast}")
            return 1

        print(f"SUMMARY passed scenarios={len(scenarios())} profile={args.profile}")
        return 0
    finally:
        if pg_ctl is not None and data is not None:
            subprocess.run([str(pg_ctl), "-D", str(data), "-w", "stop"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if workdir is not None and not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
