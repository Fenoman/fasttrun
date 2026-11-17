#!/usr/bin/env python3
"""Проверяет состав обязательного cassert и pre-release набора."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
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
        "prepare_registry", "bgworker_log", "block_sample_seed", "example",
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
        example_operand = '{ [ "$example_rc" -ne 0 ] && [ "$example_rc" -ne 77 ]; }'
        require(
            errors,
            all(
                re.fullmatch(rf'\[ "\${ident}_rc" -ne 0 \]', o)
                or o == example_operand
                for o in operands
            ),
            "scripts/check_cassert_allversions.sh: операнды условия провала "
            f"должны соединяться только через ||: {normalized}",
        )
        # В условие обязана попасть каждая проверка, код которой заранее
        # выставлен в 125 или сохранен в блоке дополнительных проверок.
        initialized = re.findall(rf'^[ \t]*({ident}_rc)=125[ \t]*$', runner,
                                 re.MULTILINE)
        for rc in sorted(set(initialized) | set(saved_rc)):
            exp_op = (
                example_operand if rc == "example_rc" else f'[ "${rc}" -ne 0 ]'
            )
            require(
                errors,
                exp_op in operands,
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
        (
            "check-example",
            '@rc=0; PG_CONFIG="$(PG_CONFIG)" scripts/check_fasttrun_example.sh || rc=$$?; \\\n'
            '\t\t[ "$$rc" -eq 0 ] || [ "$$rc" -eq 77 ] || exit "$$rc"',
        ),
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

    # Скрипты трассировки обязаны завершать дочерние процессы и ожидать их в
    # нормальной и аварийной ветке через ограниченные terminate_child() и
    # wait_bounded(), сохраняя реальный код возврата и не блокируясь в голом
    # wait на остановленном (SIGSTOP) или игнорирующем SIGTERM процессе.
    fn_re = re.compile(
        r"(^terminate_child\(\)\n\{.*?\n\}\n\nwait_bounded\(\)\n\{.*?\n\})",
        re.MULTILINE | re.DOTALL,
    )
    for rel_script in (
        "scripts/check_fasttrun_commit_duration.sh",
        "scripts/check_fasttrun_commit_inval_overhead.sh",
        "scripts/check_fasttrun_perf_smoke.sh",
        "scripts/check_fasttrun_giant_temp.sh",
        "scripts/check_fasttrun_bulk_analyze_overhead.sh",
        "scripts/check_zero_shared_sinval.sh",
    ):
        sbody = read(errors, rel_script)
        require(
            errors,
            re.search(
                r'^\s*(?:if\s+!\s+)?wait\s+"\$(?:psql_pid|TRACE_PID|BPF_PID|gdb_pid)"',
                sbody,
                re.MULTILINE,
            )
            is None,
            f"{rel_script}: найден неограниченный прямой wait вне wait_bounded/terminate_child",
        )
        m = fn_re.search(sbody)
        if m is None:
            errors.append(
                f"{rel_script}: не найдена пара terminate_child() + wait_bounded()"
            )
            continue
        priv_probe = ""
        if rel_script != "scripts/check_zero_shared_sinval.sh":
            priv_probe = (
                # 5) Привилегированный процесс (as_root=1): обычный kill -0 дает EPERM,
                #    поэтому проверка живости и завершение обязаны идти через $SUDO kill.
                "mock_sudo() {\n"
                "  if [ \"${1:-}\" = \"kill\" ]; then shift; builtin kill \"$@\"; else \"$@\"; fi\n"
                "}\n"
                "kill() {\n"
                "  if [ \"${1:-}\" = \"-0\" ]; then return 1; fi\n"
                "  builtin kill \"$@\"\n"
                "}\n"
                "SUDO=mock_sudo;\n"
                "sleep 60 >/dev/null 2>&1 </dev/null &\n"
                "p5=$!; builtin kill -STOP \"$p5\"; rc5=0; wait_bounded \"$p5\" \"\" 1 || rc5=$?;\n"
                "unset -f kill; SUDO='';\n"
                "if [ \"$rc5\" -ne 124 ] || builtin kill -0 \"$p5\" 2>/dev/null; then exit 15; fi\n"
            )
        probe = (
            "set -euo pipefail; SUDO=''; FASTTRUN_WAIT_TICKS=4;\n"
            + m.group(1)
            + "\n"
            # 1) Игнорирование SIGTERM в terminate_child
            "bash -c 'trap \"\" TERM; exec sleep 60 >/dev/null 2>&1 </dev/null' >/dev/null 2>&1 </dev/null &\n"
            "p1=$!; sleep 0.05; terminate_child \"$p1\";\n"
            "if kill -0 \"$p1\" 2>/dev/null; then exit 11; fi\n"
            # 2) SIGSTOP в terminate_child
            "sleep 60 >/dev/null 2>&1 </dev/null &\n"
            "p2=$!; kill -STOP \"$p2\"; terminate_child \"$p2\";\n"
            "if kill -0 \"$p2\" 2>/dev/null; then exit 12; fi\n"
            # 3) Сохранение реального ненулевого кода в wait_bounded
            "bash -c 'exit 7' >/dev/null 2>&1 </dev/null &\n"
            "p3=$!; rc3=0; wait_bounded \"$p3\" || rc3=$?;\n"
            "if [ \"$rc3\" -ne 7 ]; then exit 13; fi\n"
            # 4) SIGSTOP в нормальной ветке wait_bounded -> код 124 и уборка
            "sleep 60 >/dev/null 2>&1 </dev/null &\n"
            "p4=$!; kill -STOP \"$p4\"; rc4=0; wait_bounded \"$p4\" || rc4=$?;\n"
            "if [ \"$rc4\" -ne 124 ] || kill -0 \"$p4\" 2>/dev/null; then exit 14; fi\n"
            + priv_probe
        )
        try:
            res = subprocess.run(
                ["bash", "-c", probe],
                capture_output=True,
                text=True,
                timeout=8,
            )
            require(
                errors,
                res.returncode == 0,
                f"{rel_script}: проверка terminate_child/wait_bounded не прошла "
                f"(rc={res.returncode}, stderr={res.stderr.strip()})",
            )
        except subprocess.TimeoutExpired:
            errors.append(
                f"{rel_script}: terminate_child/wait_bounded зависли на "
                "остановленном или игнорирующем SIGTERM процессе"
            )

    perf_smoke = read(errors, "scripts/check_fasttrun_perf_smoke.sh")
    require(
        errors,
        '$program\nEND {' not in perf_smoke
        and 'END \\{/END \\{ print(@end_sentinel);' in perf_smoke,
        "scripts/check_fasttrun_perf_smoke.sh: run_trace не должен дублировать блок END в bpftrace",
    )

    require(
        errors,
        re.search(
            r'^\s*(?:if\s+!\s+)?wait\s+"\$cancel_psql_pid"',
            fault,
            re.MULTILINE,
        )
        is None,
        "scripts/check_fasttrun_fault_matrix.sh: найден неограниченный wait для cancel_psql_pid",
    )

    # Сквозная негативная регрессия самого вызывающего скрипта после получения
    # конечного SQL-маркера WORKLOAD_DONE: проверяем SIGSTOP клиента в окне
    # после маркера и SIGSTOP трассера перед SIGINT.
    cdur_script = ROOT / "scripts" / "check_fasttrun_commit_duration.sh"
    if cdur_script.is_file():
        with tempfile.TemporaryDirectory() as mock_dir:
            mdir = Path(mock_dir)
            (mdir / "fasttrun.so").write_bytes(b"mock")
            for name, script_text in (
                (
                    "pg_config",
                    f"#!/bin/sh\n"
                    f"if [ \"${{1:-}}\" = \"--bindir\" ] || [ \"${{1:-}}\" = \"--pkglibdir\" ]; then\n"
                    f"  echo '{mdir}'\n"
                    f"fi\n",
                ),
                ("initdb", "#!/bin/sh\nmkdir -p \"$2\"\n"),
                ("pg_ctl", "#!/bin/sh\nexit 0\n"),
                ("createdb", "#!/bin/sh\nexit 0\n"),
                ("stdbuf", "#!/bin/sh\nshift\nexec \"$@\"\n"),
                (
                    "psql",
                    "#!/usr/bin/env bash\n"
                    "for arg in \"$@\"; do\n"
                    "  case \"$arg\" in\n"
                    "    *repro.sql)\n"
                    "      echo 42424\n"
                    "      echo WORKLOAD_READY\n"
                    "      while [ ! -f \"$MOCK_GATE_FILE\" ]; do sleep 0.02; done\n"
                    "      echo WORKLOAD_DONE\n"
                    "      if [ \"${MOCK_MODE:-}\" = \"stop_client_after_done\" ]; then\n"
                    "        kill -STOP $$\n"
                    "        sleep 60\n"
                    "      fi\n"
                    "      exit 0\n"
                    "      ;;\n"
                    "    *\"SET go = true\"*)\n"
                    "      : > \"$MOCK_GATE_FILE\"\n"
                    "      exit 0\n"
                    "      ;;\n"
                    "  esac\n"
                    "done\n"
                    "exit 0\n",
                ),
                (
                    "bpftrace",
                    "#!/usr/bin/env bash\n"
                    "echo ATTACHED_READY\n"
                    "if [ \"${MOCK_MODE:-}\" = \"stop_tracer_after_attach\" ]; then\n"
                    "  kill -STOP $$\n"
                    "fi\n"
                    "trap 'exit 130' INT TERM\n"
                    "while :; do sleep 0.05; done\n",
                ),
            ):
                p = mdir / name
                p.write_text(script_text, encoding="utf-8")
                p.chmod(0o755)

            caller_wdir = mdir / "caller-workdir"
            caller_wdir.mkdir()
            caller_keep = caller_wdir / "keep.txt"
            caller_keep.write_text("keep\n", encoding="utf-8")
            for mode, expected_msg in (
                ("stop_client_after_done", "SQL-сценарий завершился с ошибкой"),
                ("stop_tracer_after_attach", "bpftrace завершился с кодом 124"),
            ):
                gate_file = mdir / f"gate_{mode}"
                env = {**os.environ, "WORKDIR": str(caller_wdir)}
                env.update(
                    {
                        "PATH": f"{mdir}:{env.get('PATH', '')}",
                        "PG_CONFIG": str(mdir / "pg_config"),
                        "PSQL": str(mdir / "psql"),
                        "INITDB": str(mdir / "initdb"),
                        "PG_CTL": str(mdir / "pg_ctl"),
                        "CREATEDB": str(mdir / "createdb"),
                        "BPFTRACE": str(mdir / "bpftrace"),
                        "FASTTRUN_SO": str(mdir / "fasttrun.so"),
                        "WORKDIR": str(mdir / f"work_{mode}"),
                        "SUDO": "",
                        "PG_RUN_AS": "",
                        "TEMP_TABLES": "1",
                        "COLUMNS": "1",
                        "ITERATIONS": "1",
                        "FASTTRUN_WAIT_TICKS": "4",
                        "MOCK_GATE_FILE": str(gate_file),
                        "MOCK_MODE": mode,
                    }
                )
                try:
                    run_res = subprocess.run(
                        ["bash", str(cdur_script)],
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=8,
                    )
                    combined = run_res.stdout + "\n" + run_res.stderr
                    require(
                        errors,
                        run_res.returncode != 0 and expected_msg in combined,
                        f"check_fasttrun_commit_duration.sh ({mode}): ожидался "
                        f"ненулевой выход с '{expected_msg}', получено "
                        f"rc={run_res.returncode}, out={combined.strip()}",
                    )
                except subprocess.TimeoutExpired:
                    errors.append(
                        f"check_fasttrun_commit_duration.sh ({mode}): "
                        "зависание при SIGSTOP после конечного SQL-маркера"
                    )
            require(
                errors,
                caller_keep.is_file()
                and caller_keep.read_text(encoding="utf-8") == "keep\n",
                "mock-прогон check_fasttrun_commit_duration.sh затронул внешний WORKDIR",
            )

    example_script = ROOT / "scripts" / "check_fasttrun_example.sh"
    if example_script.is_file():
        with tempfile.TemporaryDirectory() as mock_dir:
            mdir = Path(mock_dir)
            ext_dir = mdir / "share" / "extension"
            ext_dir.mkdir(parents=True)
            (ext_dir / "pg_variables.control").write_text("default_version = '1.4'\n", encoding="utf-8")
            for name, script_text in (
                (
                    "pg_config",
                    f"#!/bin/sh\n"
                    f"case \"${{1:-}}\" in\n"
                    f"  --bindir) echo '{mdir}' ;;\n"
                    f"  --sharedir) echo '{mdir / 'share'}' ;;\n"
                    f"esac\n",
                ),
                (
                    "pg_resetwal",
                    "#!/bin/sh\n"
                    "echo \"pg_resetwal: invalid option -- 'e'\" >&2\n"
                    "exit 1\n",
                ),
                (
                    "initdb",
                    "#!/usr/bin/env bash\n"
                    "if [ \"${1:-}\" = \"--help\" ]; then\n"
                    "  if [ \"${MOCK_EXAMPLE_MODE:-}\" = \"help_fail\" ]; then\n"
                    "    echo 'initdb: error: permission denied' >&2\n"
                    "    exit 1\n"
                    "  fi\n"
                    "  if [ \"${MOCK_EXAMPLE_MODE:-}\" = \"no_x\" ]; then\n"
                    "    echo '  -D, --pgdata=DATADIR'\n"
                    "  else\n"
                    "    echo '  --no-tune'\n"
                    "    echo '  -x, --next-transaction-id=XID'\n"
                    "  fi\n"
                    "  exit 0\n"
                    "fi\n"
                    "for arg in \"$@\"; do\n"
                    "  if [ \"$arg\" = \"8589934595\" ]; then\n"
                    "    if [ \"${MOCK_EXAMPLE_MODE:-}\" = \"range_32\" ]; then\n"
                    "      echo 'initdb: error: transaction ID 8589934595 is out of range' >&2\n"
                    "      exit 1\n"
                    "    fi\n"
                    "    if [ \"${MOCK_EXAMPLE_MODE:-}\" = \"file_too_large\" ]; then\n"
                    "      echo 'initdb: error: could not write file \"postgresql.conf\": File too large' >&2\n"
                    "      exit 1\n"
                    "    fi\n"
                    "    echo 'initdb: error: could not write control file: No space left on device' >&2\n"
                    "    exit 1\n"
                    "  fi\n"
                    "done\n"
                    "exit 0\n",
                ),
                ("pg_ctl", "#!/bin/sh\nexit 0\n"),
                (
                    "psql",
                    "#!/bin/sh\n"
                    "for arg in \"$@\"; do\n"
                    "  if [ \"$arg\" = \"-c\" ] || [ \"$arg\" = \"-f\" ]; then exit 0; fi\n"
                    "done\n"
                    "cat >/dev/null\n"
                    "echo 'EXAMPLE_PASS epoch=0'\n",
                ),
            ):
                p = mdir / name
                p.write_text(script_text, encoding="utf-8")
                p.chmod(0o755)

            caller_wdir = mdir / "caller-workdir"
            caller_wdir.mkdir()
            caller_keep = caller_wdir / "keep.txt"
            caller_keep.write_text("keep\n", encoding="utf-8")
            for mode, exp_rc, exp_sub in (
                ("no_x", 77, "SKIP: forced epoch 2 requires"),
                ("range_32", 77, "SKIP: forced epoch 2 requires"),
                ("io_fail", 1, "No space left on device"),
                ("file_too_large", 1, "File too large"),
                ("help_fail", 1, "permission denied"),
            ):
                env = {**os.environ, "WORKDIR": str(caller_wdir)}
                env.update(
                    {
                        "PATH": f"{mdir}:{env.get('PATH', '')}",
                        "PG_CONFIG": str(mdir / "pg_config"),
                        "WORKDIR": str(mdir / f"work_{mode}"),
                        "PG_RUN_AS": "",
                        "MOCK_EXAMPLE_MODE": mode,
                    }
                )
                run_res = subprocess.run(
                    ["bash", str(example_script)],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=8,
                )
                require(
                    errors,
                    run_res.returncode == exp_rc and exp_sub in run_res.stderr,
                    f"check_fasttrun_example.sh ({mode}): ожидался rc={exp_rc} "
                    f"и '{exp_sub}' в stderr, получено rc={run_res.returncode}, "
                    f"stderr={run_res.stderr.strip()}",
                )
            require(
                errors,
                caller_keep.is_file()
                and caller_keep.read_text(encoding="utf-8") == "keep\n",
                "mock-прогон check_fasttrun_example.sh затронул внешний WORKDIR",
            )

    bench_script = ROOT / "scripts" / "bench_percolumn.sh"
    if bench_script.is_file():
        with tempfile.TemporaryDirectory() as mock_dir:
            mdir = Path(mock_dir)
            (mdir / "head.so").write_bytes(b"h")
            (mdir / "patch.so").write_bytes(b"p")
            for name, script_text in (
                ("pg_config", f"#!/bin/sh\necho '{mdir}'\n"),
                (
                    "initdb",
                    "#!/bin/sh\n"
                    "while [ $# -gt 0 ]; do\n"
                    "  if [ \"$1\" = \"-D\" ]; then\n"
                    "    mkdir -p \"$2\" && : > \"$2/postgresql.conf\"\n"
                    "    exit 0\n"
                    "  fi\n"
                    "  shift\n"
                    "done\n"
                    "exit 1\n",
                ),
                ("pg_ctl", "#!/bin/sh\nexit 0\n"),
                ("createdb", "#!/bin/sh\nexit 0\n"),
                (
                    "psql",
                    "#!/bin/sh\n"
                    "for arg in \"$@\"; do\n"
                    "  case \"$arg\" in\n"
                    "    *bench_percolumn.sql)\n"
                    "      echo 'bench stderr' >&2\n"
                    "      echo 'analyze_col|1.25'\n"
                    "      exit 0\n"
                    "      ;;\n"
                    "  esac\n"
                    "done\n"
                    "exit 0\n",
                ),
            ):
                p = mdir / name
                p.write_text(script_text, encoding="utf-8")
                p.chmod(0o755)

            wdir = mdir / "bench-work"
            ext_dir = mdir / "ext-results"
            ext_dir.mkdir()
            ext_leaf_link = ext_dir / "raw.csv"
            ext_leaf_link.symlink_to(wdir / "lib" / "raw.csv")

            wdir_pre = mdir / "bench-pre"
            (wdir_pre / "lib").mkdir(parents=True)
            (wdir_pre / "lib" / "link.csv").symlink_to(ext_dir / "real.csv")
            wdir_alias = mdir / "bench-alias"
            wdir_alias.symlink_to(wdir_pre)

            hard_head_csv = ext_dir / "hard_head.csv"
            hard_patch_csv = ext_dir / "hard_patch.csv"
            os.link(mdir / "head.so", hard_head_csv)
            os.link(mdir / "patch.so", hard_patch_csv)

            # Намеренно засеваем невалидный внешний PG_RUN_AS, чтобы явный сброс
            # в base_env проверялся на любой ОС и без прав root.
            base_env = {**os.environ, "PG_RUN_AS": "nonexistent_pg_run_as_probe"}
            base_env.update(
                {
                    "PG_CONFIG": str(mdir / "pg_config"),
                    "HEAD_LIB": str(mdir / "head.so"),
                    "PATCH_LIB": str(mdir / "patch.so"),
                    "DLSUFFIX": ".so",
                    "BENCH_DB": "fasttrun_bench_db",
                    "PG_RUN_AS": "",
                    "PGPORT_BENCH": "59999",
                    "SERIES": "1",
                }
            )

            for case_label, case_wdir, case_csv, exp_err in (
                (
                    "direct-lib",
                    wdir,
                    wdir / "lib" / "raw.csv",
                    "удаляемых служебных каталогов",
                ),
                (
                    "direct-runs",
                    wdir,
                    wdir / "runs" / "run_1_4_head.err",
                    "удаляемых служебных каталогов",
                ),
                (
                    "leaf-symlink-into-lib",
                    wdir,
                    ext_leaf_link,
                    "удаляемых служебных каталогов",
                ),
                (
                    "alias-symlink-inside-lib",
                    wdir_pre,
                    wdir_alias / "lib" / "link.csv",
                    "удаляемых служебных каталогов",
                ),
                (
                    "hardlink-head-lib",
                    wdir,
                    hard_head_csv,
                    "HEAD_LIB или PATCH_LIB",
                ),
                (
                    "hardlink-patch-lib",
                    wdir,
                    hard_patch_csv,
                    "HEAD_LIB или PATCH_LIB",
                ),
            ):
                env = base_env.copy()
                env["BENCH_WORKDIR"] = str(case_wdir)
                env["BENCH_OUT_CSV"] = str(case_csv)
                run_res = subprocess.run(
                    ["sh", str(bench_script)],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                require(
                    errors,
                    run_res.returncode != 0 and exp_err in run_res.stderr,
                    f"bench_percolumn.sh ({case_label}) должен отклонять "
                    f"конфликтующий BENCH_OUT_CSV до запуска: "
                    f"rc={run_res.returncode}, stderr={run_res.stderr.strip()}",
                )

            require(
                errors,
                (mdir / "head.so").read_bytes() == b"h"
                and (mdir / "patch.so").read_bytes() == b"p",
                "bench_percolumn.sh изменил входные библиотеки при отказе по hardlink",
            )

            wdir_ok = mdir / "bench-ok"
            n41_csv = wdir_ok / "run_1_4_head.err"
            env_ok = base_env.copy()
            env_ok["BENCH_WORKDIR"] = str(wdir_ok)
            env_ok["BENCH_OUT_CSV"] = str(n41_csv)
            run_ok = subprocess.run(
                ["sh", str(bench_script)],
                env=env_ok,
                capture_output=True,
                text=True,
                timeout=8,
            )
            csv_lines = (
                n41_csv.read_text(encoding="utf-8").splitlines()
                if n41_csv.is_file()
                else []
            )
            expected_lines = [
                "head|1|1|analyze_col|1.25",
                "patch|1|2|analyze_col|1.25",
                "patch|1|3|analyze_col|1.25",
                "head|1|4|analyze_col|1.25",
            ]
            require(
                errors,
                run_ok.returncode == 0 and csv_lines == expected_lines,
                "bench_percolumn.sh не должен затирать или удалять "
                f"BENCH_OUT_CSV={n41_csv.name}: rc={run_ok.returncode}, "
                f"lines={csv_lines}, stderr={run_ok.stderr.strip()}",
            )

            wdir_shared = mdir / "bench-shared"
            (wdir_shared / "lib").mkdir(parents=True)
            sentinel_root = wdir_shared / "keep.txt"
            sentinel_lib = wdir_shared / "lib" / "keep_lib.txt"
            sentinel_root.write_text("keep-root\n", encoding="utf-8")
            sentinel_lib.write_text("keep-lib\n", encoding="utf-8")
            ext_out_csv = ext_dir / "n45_external.csv"
            env_n45 = base_env.copy()
            env_n45["BENCH_WORKDIR"] = str(wdir_shared)
            env_n45["BENCH_OUT_CSV"] = str(ext_out_csv)
            run_n45 = subprocess.run(
                ["sh", str(bench_script)],
                env=env_n45,
                capture_output=True,
                text=True,
                timeout=8,
            )
            n45_lines = (
                ext_out_csv.read_text(encoding="utf-8").splitlines()
                if ext_out_csv.is_file()
                else []
            )
            leftover_clusters = list(wdir_shared.glob("cluster.*"))
            require(
                errors,
                run_n45.returncode == 0
                and n45_lines == expected_lines
                and sentinel_root.is_file()
                and sentinel_root.read_text(encoding="utf-8") == "keep-root\n"
                and sentinel_lib.is_file()
                and sentinel_lib.read_text(encoding="utf-8") == "keep-lib\n"
                and not leftover_clusters,
                "bench_percolumn.sh должен сохранять посторонние файлы "
                f"существующего BENCH_WORKDIR и убирать свой cluster.*: "
                f"rc={run_n45.returncode}, lines={n45_lines}, "
                f"root={sentinel_root.is_file()}, lib={sentinel_lib.is_file()}, "
                f"clusters={leftover_clusters}, stderr={run_n45.stderr.strip()}",
            )

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
