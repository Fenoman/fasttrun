#!/usr/bin/env python3
"""Проверяет, что публичная документация не расходится с кодом."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
README_PATHS = (ROOT / "README.md", ROOT / "README_EN.md")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def make_variable(makefile: str, name: str) -> list[str]:
    """Читает простой список PGXS с переносом строк через обратную косую черту."""
    lines = makefile.splitlines()
    for index, line in enumerate(lines):
        match = re.match(rf"^{re.escape(name)}\s*=\s*(.*)$", line)
        if match is None:
            continue

        parts: list[str] = []
        value = match.group(1)
        while True:
            continued = value.rstrip().endswith("\\")
            parts.extend(value.rstrip().removesuffix("\\").split())
            if not continued:
                return parts
            index += 1
            if index >= len(lines):
                return parts
            value = lines[index].strip()

    return []


def require(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def main() -> int:
    errors: list[str] = []
    makefile = read(ROOT / "Makefile")
    control = read(ROOT / "fasttrun.control")
    code = read(ROOT / "fasttrun.c")

    version_match = re.search(
        r"^default_version\s*=\s*'([^']+)'", control, re.MULTILINE
    )
    require(errors, version_match is not None,
            "fasttrun.control: default_version не найден")
    if version_match is None:
        version = "<unknown>"
    else:
        version = version_match.group(1)

    regress = make_variable(makefile, "REGRESS")
    require(errors, bool(regress), "Makefile: REGRESS не найден")
    require(errors, len(regress) == len(set(regress)),
            "Makefile: REGRESS содержит дубликаты")

    install_sql = ROOT / f"fasttrun--{version}.sql"
    require(errors, install_sql.exists(),
            f"нет install SQL для default_version {version}")
    if install_sql.exists():
        function_count = len(re.findall(
            r"^CREATE\s+OR\s+REPLACE\s+FUNCTION\b", read(install_sql),
            re.IGNORECASE | re.MULTILINE,
        ))
    else:
        function_count = 0

    schedule_match = re.search(
        r'DefineCustomStringVariable\(\s*"fasttrun\.track_schedule".*?'
        r'&fasttrun_track_schedule,\s*"([^"]*)"', code, re.DOTALL,
    )
    require(errors, schedule_match is not None,
            "fasttrun.c: default fasttrun.track_schedule не найден")
    schedule = schedule_match.group(1) if schedule_match else "<unknown>"

    for path in README_PATHS:
        content = read(path)
        label = path.name
        for pg_version in ("PostgreSQL 16", "PostgreSQL 17", "PostgreSQL 18"):
            require(errors, pg_version in content,
                    f"{label}: отсутствует {pg_version}")
        for term in ("prewarm_count", "max_analyze_pages", "PlanCacheRelCallback"):
            require(errors, term in content,
                    f"{label}: отсутствует {term}")
        require(errors, f"`{version}`" in content,
                f"{label}: default_version {version} не документирован")
        require(
            errors,
            re.search(
                rf"\b{len(regress)}\b[^\n]*`?pg_regress`?", content
            ) is not None,
            f"{label}: число pg_regress suites должно быть {len(regress)}",
        )
        require(
            errors,
            re.search(
                rf"\b{function_count}\b[^\n]*(?:SQL[- ]функц|SQL functions)",
                content,
                re.IGNORECASE,
            ) is not None,
            f"{label}: число SQL-функций должно быть {function_count}",
        )
        require(errors, schedule in content,
                f"{label}: default track_schedule {schedule!r} не документирован")

    legacy = read(ROOT / "README.fasttrun")
    require(errors, "README.md" in legacy and "README_EN.md" in legacy,
            "README.fasttrun: нет ссылок на актуальные README")
    require(errors, "блокир" in legacy.lower(),
            "README.fasttrun: блокировка после ошибки не описана")

    forbidden = {
        r"12/12": "устаревший итог 12/12",
        r"12 test cases": "устаревшее число test cases",
        r"tests \(12 files\)": "устаревшее число test files",
        r"~5100 lines": "устаревшее число строк fasttrun.c",
        r"~5100 строк": "устаревшее число строк fasttrun.c",
        r"PlanCacheRelCallback.*O\(1\)": "ложный O(1) contract PlanCacheRelCallback",
        r"heap_truncate_one_rel": "устаревший truncate primitive",
        r"(?:single|one) SMGR": "устаревший single-SMGR contract",
        r"cold-only": "устаревший cold-only contract",
        r"PostgreSQL (?:16\.13|17\.9|18\.3)": (
            "устаревшая patch-версия PostgreSQL"
        ),
    }
    scan_paths = [
        *README_PATHS,
        ROOT / "README.fasttrun",
        *sorted(ROOT.glob("fasttrun--2.3.[0-9].sql")),
    ]
    for path in scan_paths:
        content = read(path)
        for pattern, description in forbidden.items():
            if re.search(pattern, content, re.IGNORECASE):
                errors.append(f"{path.name}: {description}")

    if errors:
        for error in errors:
            print(f"docs consistency: {error}", file=sys.stderr)
        return 1

    print("docs consistency: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
