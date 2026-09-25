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
    testing_ru = read(errors, "docs/ru/testing.md")
    testing_en = read(errors, "docs/en/testing.md")

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
        "full-режим не передается общему runner-у через exec",
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
    # Каждая строка вызова привязана к началу строки и разделена только
    # пробелами и табуляцией: закомментированная строка вызова оставила бы
    # отдельное перенаправление, то есть пустой журнал и нулевой код.
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'^[ \t]*run_isolated_harness "\$pgcfg" '
        r'scripts/check_fasttrun_fault_matrix\.sh[ \t]*\\\n'
        r'[ \t]*>"\$OUTDIR/fault\$\{ver\}\.log" 2>&1[ \t]*\n'
        r'[ \t]*fault_rc=\$\?',
        "fault matrix не запускается с сохранением кода возврата",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'^[ \t]*run_isolated_harness "\$pgcfg" env '
        r'FASTTRUN_CACHE_INIT_MODE=cassert[ \t]*\\\n'
        r'[ \t]*scripts/check_fasttrun_cache_init_faults\.sh[ \t]*\\\n'
        r'[ \t]*>"\$OUTDIR/cache_init\$\{ver\}\.log" 2>&1[ \t]*\n'
        r'[ \t]*cache_init_rc=\$\?',
        "cache-init не запускается с сохранением кода возврата",
    )
    require_regex(
        errors,
        "scripts/check_cassert_allversions.sh",
        runner,
        r'^[ \t]*run_isolated_harness "\$pgcfg" env '
        r'FASTTRUN_BRIN_LEVEL="\$BRIN_LEVEL"[ \t]*\\\n'
        r'[ \t]*scripts/check_fasttrun_brin_stress\.sh[ \t]*\\\n'
        r'[ \t]*>"\$OUTDIR/brin\$\{ver\}\.log" 2>&1[ \t]*\n'
        r'[ \t]*brin_rc=\$\?',
        "BRIN harness не запускается с сохранением кода возврата",
    )
    # Блок дополнительных проверок разбирается целиком. В нем разрешены только
    # пары "вызов через run_isolated_harness с перенаправлением в журнал" и
    # "сохранение его кода". Комментарий, другая команда или вложенный if
    # отвергаются: иначе проверку можно пропустить, а текст вокруг останется.
    mandatory = (
        "fault_matrix", "cache_init_faults", "brin_stress", "tracking_order",
        "xact_journal_memory", "planner_probes", "publication_atomicity",
        "tracking_persistence", "exit_plan_reset", "tracking_preload",
        "prepare_registry", "bgworker_log", "block_sample_seed",
    )
    ident = r'[A-Za-z_][A-Za-z0-9_]*'
    block = re.search(
        r'^[ \t]*if[ \t]+\[ "\$check_rc" -eq 0 \](?:[^\n]*\\\n)*[^\n]*?'
        r';[ \t]*then[ \t]*\n(.*?)^[ \t]*fi[ \t]*$',
        runner,
        re.MULTILINE | re.DOTALL,
    )
    require(
        errors,
        block is not None,
        "scripts/check_cassert_allversions.sh: не найден блок дополнительных "
        "проверок",
    )
    saved_rc: list[str] = []
    if block is not None:
        commands: list[str] = []
        current: list[str] = []
        for line in block.group(1).splitlines():
            stripped = line.strip()
            if not stripped and not current:
                continue
            continued = stripped.endswith("\\")
            current.append(stripped[:-1] if continued else stripped)
            if not continued:
                commands.append(" ".join(" ".join(current).split()))
                current = []
        if current:
            commands.append(" ".join(" ".join(current).split()))
        called: list[str] = []
        bad: list[str] = []
        if len(commands) % 2:
            bad.append(commands[-1])
        for call, save in zip(commands[0::2], commands[1::2]):
            m_call = re.fullmatch(
                r'run_isolated_harness "\$pgcfg" (?:[^#;&|`$]|\$\{?[A-Za-z_]+\}?|"\$[A-Za-z_]+")*?'
                r'scripts/check_fasttrun_([a-z_]+)\.sh(?: [a-z]+)? '
                r'>"\$OUTDIR/[A-Za-z0-9_]+\$\{ver\}\.log" 2>&1',
                call,
            )
            m_save = re.fullmatch(rf'({ident}_rc)=\$\?', save)
            if m_call is None or m_save is None:
                bad.extend(c for c, m in ((call, m_call), (save, m_save)) if m is None)
                continue
            called.append(m_call.group(1))
            saved_rc.append(m_save.group(1))
        require(
            errors,
            not bad,
            "scripts/check_cassert_allversions.sh: блок дополнительных проверок "
            f"содержит не только вызовы и сохранение кода: {bad[:3]}",
        )
        require(
            errors,
            sorted(called) == sorted(mandatory),
            "scripts/check_cassert_allversions.sh: обязательные проверки "
            f"вызываются не по одному разу: {sorted(called)}",
        )
    # Условие провала дополнительных проверок разбирается как один блок if:
    # строки продолжения до "; then", тело до первого fi. Каждый операнд - это
    # проверка кода возврата, и соединены они только через ||, иначе провал
    # одной проверки мог бы не попасть в общий код возврата. В теле разрешены
    # только сообщение в stderr и overall_rc=1.
    condition = re.search(
        r'^[ \t]*if[ \t]+(\[ "\$fault_rc" -ne 0 \](?:[^\n]*\\\n)*[^\n]*?)'
        r';[ \t]*then[ \t]*\n(.*?)^[ \t]*fi[ \t]*$',
        runner,
        re.MULTILINE | re.DOTALL,
    )
    require(
        errors,
        condition is not None,
        "scripts/check_cassert_allversions.sh: не найдено условие провала "
        "дополнительных проверок",
    )
    if condition is not None:
        normalized = " ".join(condition.group(1).replace("\\\n", " ").split())
        operands = [o.strip() for o in normalized.split("||")]
        require(
            errors,
            all(re.fullmatch(rf'\[ "\${ident}_rc" -ne 0 \]', o) for o in operands),
            "scripts/check_cassert_allversions.sh: операнды условия провала "
            f"должны соединяться только через ||: {normalized}",
        )
        # В условие обязана попасть каждая проверка, код которой заранее
        # выставлен в 125 или сохранен в блоке дополнительных проверок.
        initialized = re.findall(rf'^[ \t]*({ident}_rc)=125[ \t]*$', runner,
                                 re.MULTILINE)
        for rc in sorted(set(initialized) | set(saved_rc)):
            require(
                errors,
                f'[ "${rc}" -ne 0 ]' in operands,
                f"scripts/check_cassert_allversions.sh: {rc} не входит в общий "
                "код возврата",
            )
        body = [line.strip() for line in condition.group(2).splitlines()
                if line.strip()]
        require(
            errors,
            "overall_rc=1" in body
            and all(line == "overall_rc=1" or re.fullmatch(r'echo "[^"`$]*(?:\$OUTDIR[^"`$]*)?" >&2', line)
                    for line in body),
            "scripts/check_cassert_allversions.sh: тело условия провала "
            f"дополнительных проверок должно только сообщать и ставить "
            f"overall_rc=1: {body}",
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
    # README ссылается на docs/ относительными путями, поэтому установка кладет
    # все дерево документации в свой подкаталог, а плоско - только README.fasttrun.
    require_tokens(
        errors,
        "Makefile",
        makefile,
        (
            "DOCS = README.fasttrun",
            "install: install-docs-tree",
            "uninstall: uninstall-docs-tree",
            "$(srcdir)/README.md $(srcdir)/README_EN.md",
            "$(srcdir)/docs/ru/*.md",
            "$(srcdir)/docs/en/*.md",
            "$(srcdir)/docs/images/*.png",
        ),
    )

    require_tokens(
        errors,
        "docs/ru/testing.md",
        testing_ru,
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
        "docs/en/testing.md",
        testing_en,
        (
            "50 cases across 25",
            "96 regular",
            "48 forced",
            "1,000 regular",
            "200 forced",
        ),
    )
    for relative, content in (
        ("docs/ru/testing.md", testing_ru),
        ("docs/en/testing.md", testing_en),
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

    # Обязательный прогон сверяет число пройденных наборов с числом в
    # Makefile. Хранить его отдельным числом нельзя: добавленный набор тогда
    # превращает зеленый прогон в "регрессию".
    cassert = ROOT / "scripts" / "check_cassert_allversions.sh"
    if cassert.exists():
        body = cassert.read_text(encoding="utf-8")
        require(errors,
                re.search(r"^EXPECTED_REGRESS=\d+\s*$", body, re.MULTILINE) is None,
                "check_cassert_allversions.sh: EXPECTED_REGRESS задан числом, "
                "а должен выводиться из Makefile")
        require(errors,
                re.search(r"\b\d+/\d+ тестов", body) is None,
                "check_cassert_allversions.sh: число тестов вшито в текст итога, "
                "а должно подставляться из EXPECTED_REGRESS")

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
