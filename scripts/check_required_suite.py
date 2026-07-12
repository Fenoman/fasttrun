#!/usr/bin/env python3
"""Проверяет состав обязательного cassert и pre-release набора."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def read(errors: list[str], relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        errors.append(f"{relative}: файл не найден")
        return ""
    return path.read_text(encoding="utf-8")


def require(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def require_tokens(
    errors: list[str], relative: str, content: str, tokens: tuple[str, ...]
) -> None:
    for token in tokens:
        require(errors, token in content, f"{relative}: не найдено {token!r}")


def require_regex(
    errors: list[str], relative: str, content: str, pattern: str, message: str
) -> None:
    require(
        errors,
        re.search(pattern, content, re.MULTILINE | re.DOTALL) is not None,
        f"{relative}: {message}",
    )


def shell_array(content: str, name: str) -> list[str]:
    match = re.search(
        rf"^{re.escape(name)}=\(\n(?P<body>.*?)^\)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        return []
    return [
        line.strip()
        for line in match.group("body").splitlines()
        if line.strip()
    ]


def main() -> int:
    errors: list[str] = []
    brin_path = ROOT / "scripts/check_fasttrun_brin_stress.sh"
    prerelease_path = ROOT / "scripts/check_fasttrun_prerelease.sh"
    brin = read(errors, "scripts/check_fasttrun_brin_stress.sh")
    prerelease = read(errors, "scripts/check_fasttrun_prerelease.sh")
    runner = read(errors, "scripts/check_cassert_allversions.sh")
    fault = read(errors, "scripts/check_fasttrun_fault_matrix.sh")
    makefile = read(errors, "Makefile")
    readme = read(errors, "README.md")
    readme_en = read(errors, "README_EN.md")

    require_tokens(
        errors,
        "scripts/check_fasttrun_brin_stress.sh",
        brin,
        (
            "FASTTRUN_BRIN_LEVEL",
            "smoke)",
            "full)",
            "profiles=(default full)",
            "modes=(on off)",
            "BRIN_CASE_OK",
            "BRIN_STRESS_OK",
            "pg_statistic",
            "fasttrun_inspect_stats",
            "neutral_estimated_rows",
            "cfg.row_count * 4 / 1000",
            "cfg.row_count * 6 / 1000",
            "IF qerror > 3 THEN",
            "forced iteration % did not use BRIN",
            '[ "$case_count" -ne 4 ]',
            "warm_bytes + 8192",
        ),
    )
    require_regex(
        errors,
        "scripts/check_fasttrun_prerelease.sh",
        prerelease,
        r'^export FASTTRUN_BRIN_LEVEL=full\s+'
        r'exec "\$SCRIPT_DIR/check_cassert_allversions\.sh" "\$@"$',
        "full-режим не передаётся общему runner-у через exec",
    )
    require_regex(
        errors,
        "scripts/check_fasttrun_brin_stress.sh",
        brin,
        r"smoke\)\s+row_count=20000\s+iterations=12\s+"
        r"explains=2\s+warmup=4",
        "параметры smoke должны быть 20000/12/2/4",
    )
    require_regex(
        errors,
        "scripts/check_fasttrun_brin_stress.sh",
        brin,
        r"full\)\s+row_count=50000\s+iterations=50\s+"
        r"explains=5\s+warmup=10",
        "параметры full должны быть 50000/50/5/10",
    )
    require_tokens(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        (
            "check_fasttrun_cache_init_faults.sh",
            "check_fasttrun_brin_stress.sh",
            "cache_init_rc",
            "brin_rc",
            "FASTTRUN_BRIN_LEVEL",
            'EXPECTED_MAJORS="16 17 18 "',
            "server_version_num",
            "debug_assertions",
            "shared_preload_libraries",
            "PKGLIBDIR",
            "SHAREDIR",
            "--pkglibdir",
            "--sharedir",
            '[ ! -r "$srvlog" ]',
            "FASTTRUN_CASSERT_LOG_PROBE",
            '"${HARNESS_ENV_ARGS[@]}"',
        ),
    )
    harness_env = shell_array(runner, "HARNESS_ENV_ARGS")
    expected_harness_env = [
        f"-u {name}"
        for name in (
            "PG_RUN_AS",
            "PGPORT",
            "FASTTRUN_BRIN_PORT",
            "FASTTRUN_BRIN_LEVEL",
            "FASTTRUN_CACHE_INIT_MODE",
            "FASTTRUN_ALLOW_RELEASE_TEST_BUILD",
            "FASTTRUN_PUBLICATION_CASE",
            "FASTTRUN_TRACKING_PERSISTENCE_CASE",
            "WORKDIR",
            "DBNAME",
            "PSQL",
            "INITDB",
            "PG_CTL",
            "CREATEDB",
            "PYTHON",
            "KEEP_WORKDIR",
            "PGUSER",
            "PGHOST",
            "PGDATABASE",
            "PGOPTIONS",
            "PGSERVICE",
            "PGSERVICEFILE",
            "DESTDIR",
            "MAKEFLAGS",
            "GNUMAKEFLAGS",
            "MFLAGS",
            "MAKELEVEL",
            "MAKEFILES",
            "MAKEOVERRIDES",
            "REGRESS",
            "REGRESS_OPTS",
            "PG_REGRESS",
            "BASH_ENV",
            "ENV",
        )
    ]
    require(
        errors,
        harness_env == expected_harness_env,
        "scripts/check_cassert_allversions.sh: неверная изоляция окружения "
        f"harness: {harness_env}",
    )
    require(
        errors,
        runner.count('"${HARNESS_ENV_ARGS[@]}"') == 5,
        "scripts/check_cassert_allversions.sh: очищенное окружение должно "
        "использоваться в двух ветках harness, build, install и installcheck",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'run_isolated_harness "\$pgcfg" '
        r'scripts/check_fasttrun_fault_matrix\.sh\s*\\\n\s*'
        r'>"\$OUTDIR/fault\$\{ver\}\.log" 2>&1\s*fault_rc=\$\?',
        "fault matrix не запускается с сохранением кода возврата",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'run_isolated_harness "\$pgcfg" env '
        r'FASTTRUN_CACHE_INIT_MODE=cassert\s*\\\n\s*'
        r'scripts/check_fasttrun_cache_init_faults\.sh\s*\\\n\s*'
        r'>"\$OUTDIR/cache_init\$\{ver\}\.log" 2>&1\s*'
        r'cache_init_rc=\$\?',
        "cache-init не запускается с сохранением кода возврата",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'run_isolated_harness "\$pgcfg" env '
        r'FASTTRUN_BRIN_LEVEL="\$BRIN_LEVEL"\s*\\\n\s*'
        r'scripts/check_fasttrun_brin_stress\.sh\s*\\\n\s*'
        r'>"\$OUTDIR/brin\$\{ver\}\.log" 2>&1\s*brin_rc=\$\?',
        "BRIN harness не запускается с сохранением кода возврата",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'if \[ "\$fault_rc" -ne 0 \].*?'
        r'\[ "\$cache_init_rc" -ne 0 \].*?'
        r'\[ "\$brin_rc" -ne 0 \].*?overall_rc=1\s+fi',
        "ошибки cache-init/BRIN не входят в общий код возврата",
    )
    require_tokens(
        errors,
        "scripts/check_fasttrun_fault_matrix.sh",
        fault,
        (
            "USING brin (brin_key)",
            "reserve_after_state:1",
            "reserve_after_state:2",
            "reserve_after_state:3",
            "reserve_after_state:4",
            "reserve_after_state:5",
            "reserve_after_state:6",
            "after_user_index:3",
            "after_ambuild:4",
            "after_publish:6",
            "boundary_count",
        ),
    )
    early = shell_array(fault, "early_failpoints")
    post = shell_array(fault, "post_failpoints")
    modes = shell_array(fault, "modes")
    expected_early = [
        "after_prepare",
        "after_phase0",
        *(f"reserve_after_state:{number}" for number in range(1, 7)),
    ]
    expected_post = [
        *(f"after_user_index:{number}" for number in range(1, 4)),
        "after_toast_index:1",
        "after_toast_heap",
        "after_main_heap",
        *(f"after_ambuild:{number}" for number in range(1, 5)),
        "before_publish",
        *(f"after_publish:{number}" for number in range(1, 7)),
    ]
    expected_modes = ["on", "off"]
    require(
        errors,
        early == expected_early,
        f"scripts/check_fasttrun_fault_matrix.sh: неверный early: {early}",
    )
    require(
        errors,
        post == expected_post,
        f"scripts/check_fasttrun_fault_matrix.sh: неверный post: {post}",
    )
    require(
        errors,
        modes == expected_modes,
        f"scripts/check_fasttrun_fault_matrix.sh: неверные modes: {modes}",
    )
    require(
        errors,
        len(modes) * (len(early) + len(post)) == 50,
        "scripts/check_fasttrun_fault_matrix.sh: должно быть 50 сценариев",
    )
    require_regex(
        errors,
        "scripts/check_fasttrun_fault_matrix.sh",
        fault,
        r'EXPECTED_BOUNDARY_CASES=50.*?'
        r'if \[ "\$expected_boundary_count" -ne '
        r'"\$EXPECTED_BOUNDARY_CASES" \]; then.*?'
        r'if \[ "\$boundary_count" -ne '
        r'"\$EXPECTED_BOUNDARY_CASES" \]; then',
        "нет исполняемого guard-а ровно на 50 сценариев",
    )
    for target, recipe in (
        (
            "check-brin-stress",
            'PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_brin_stress.sh',
        ),
        ("check-required-suite", "python3 scripts/check_required_suite.py"),
        ("check-prerelease", "scripts/check_fasttrun_prerelease.sh"),
    ):
        require_regex(
            errors,
            "Makefile",
            makefile,
            rf"^{re.escape(target)}:\n\t{re.escape(recipe)}$",
            f"цель {target} отсутствует или пуста",
        )
    require_tokens(
        errors,
        "Makefile",
        makefile,
        ("NO_PGXS_GOALS = check-docs check-required-suite check-prerelease",),
    )

    require_tokens(
        errors,
        "README.md",
        readme,
        (
            "50 сценариев на 25 точках отказа",
            "96 обычных",
            "48 принудительных",
            "1000 обычных",
            "200 принудительных",
        ),
    )
    require_tokens(
        errors,
        "README_EN.md",
        readme_en,
        (
            "50 cases across 25",
            "96 regular",
            "48 forced",
            "1,000 regular",
            "200 forced",
        ),
    )
    for relative, content in (
        ("README.md", readme),
        ("README_EN.md", readme_en),
    ):
        require_tokens(
            errors,
            relative,
            content,
            ("check_fasttrun_prerelease.sh", "check_fasttrun_brin_stress.sh"),
        )

    if brin_path.exists():
        require(errors, os.access(brin_path, os.X_OK),
                "scripts/check_fasttrun_brin_stress.sh: файл не исполняемый")
    if prerelease_path.exists():
        require(errors, os.access(prerelease_path, os.X_OK),
                "scripts/check_fasttrun_prerelease.sh: файл не исполняемый")

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(f"required suite contract: FAIL ({len(errors)} errors)",
              file=sys.stderr)
        return 1

    print("required suite contract: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
