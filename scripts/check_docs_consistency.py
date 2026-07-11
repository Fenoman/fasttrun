#!/usr/bin/env python3
"""Проверяет, что публичная документация не расходится с кодом."""

from __future__ import annotations

from collections import deque
import hashlib
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
README_PATHS = (ROOT / "README.md", ROOT / "README_EN.md")
CACHE_STATS_FIELDS = (
    "analyze_entries",
    "column_stats_relid_entries",
    "column_stats_entries",
    "analyze_bytes",
    "column_stats_bytes",
    "total_bytes",
)
FROZEN_SQL_SHA256 = {
    "fasttrun--2.3.4.sql": (
        "4134aee5d5a67fa100fe14dd504ffef909454ad6511dd284b93f1a9ac8544a36"
    ),
}
VERSION_PATTERN = r"\d+(?:\.\d+)+"
INSTALL_SQL_RE = re.compile(
    rf"^fasttrun--(?P<version>{VERSION_PATTERN})\.sql$"
)
UPGRADE_SQL_RE = re.compile(
    rf"^fasttrun--(?P<source>{VERSION_PATTERN})"
    rf"--(?P<target>{VERSION_PATTERN})\.sql$"
)
UNPACKAGED_SQL_RE = re.compile(
    rf"^fasttrun--unpackaged--(?P<version>{VERSION_PATTERN})\.sql$"
)


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


def path_exists(graph: dict[str, set[str]], source: str, target: str) -> bool:
    """Проверяет достижимость версии по направленным upgrade-рёбрам."""
    queue = deque([source])
    visited: set[str] = set()

    while queue:
        version = queue.popleft()
        if version == target:
            return True
        if version in visited:
            continue
        visited.add(version)
        queue.extend(graph.get(version, set()) - visited)
    return False


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

    data = make_variable(makefile, "DATA")
    require(errors, bool(data), "Makefile: DATA не найден")
    require(errors, len(data) == len(set(data)),
            "Makefile: DATA содержит дубликаты")
    data_set = set(data)
    repository_sql = {
        path.name for path in ROOT.glob("fasttrun--*.sql") if path.is_file()
    }
    missing_from_data = sorted(repository_sql - data_set)
    extra_in_data = sorted(data_set - repository_sql)
    require(
        errors,
        not missing_from_data,
        "Makefile: DATA не содержит " + ", ".join(missing_from_data),
    )
    require(
        errors,
        not extra_in_data,
        "Makefile: DATA ссылается на отсутствующие файлы "
        + ", ".join(extra_in_data),
    )

    install_versions: dict[str, str] = {}
    upgrade_edges: list[tuple[str, str, str]] = []
    unknown_sql: list[str] = []
    for filename in sorted(data_set):
        install_match = INSTALL_SQL_RE.fullmatch(filename)
        if install_match:
            install_versions[install_match.group("version")] = filename
            continue
        upgrade_match = UPGRADE_SQL_RE.fullmatch(filename)
        if upgrade_match:
            upgrade_edges.append((
                upgrade_match.group("source"),
                upgrade_match.group("target"),
                filename,
            ))
            continue
        if UNPACKAGED_SQL_RE.fullmatch(filename):
            continue
        unknown_sql.append(filename)
    require(
        errors,
        not unknown_sql,
        "Makefile: DATA содержит SQL с неизвестной схемой имени: "
        + ", ".join(unknown_sql),
    )
    require(errors, version in install_versions,
            f"Makefile: DATA не содержит install SQL версии {version}")

    upgrade_graph: dict[str, set[str]] = {}
    for source, target, filename in upgrade_edges:
        require(errors, source in install_versions,
                f"{filename}: нет install SQL исходной версии {source}")
        require(errors, target in install_versions,
                f"{filename}: нет install SQL целевой версии {target}")
        upgrade_graph.setdefault(source, set()).add(target)
    if version in install_versions:
        for legacy in sorted(install_versions):
            if legacy == version:
                continue
            require(
                errors,
                path_exists(upgrade_graph, legacy, version),
                f"нет пути обновления {legacy} -> {version}",
            )

    for filename, expected_sha in FROZEN_SQL_SHA256.items():
        path = ROOT / filename
        require(errors, path.exists(), f"нет замороженного SQL {filename}")
        if path.exists():
            actual_sha = hashlib.sha256(path.read_bytes()).hexdigest()
            require(
                errors,
                actual_sha == expected_sha,
                f"{filename}: изменён замороженный SQL, SHA256 {actual_sha}",
            )

    install_sql = ROOT / f"fasttrun--{version}.sql"
    require(errors, install_sql.exists(),
            f"нет install SQL для default_version {version}")
    install_content = ""
    if install_sql.exists():
        install_content = read(install_sql)
        function_count = len(re.findall(
            r"^CREATE\s+OR\s+REPLACE\s+FUNCTION\b", install_content,
            re.IGNORECASE | re.MULTILINE,
        ))
    else:
        function_count = 0

    cache_stats_match = re.search(
        r"CREATE\s+OR\s+REPLACE\s+FUNCTION\s+fasttrun_cache_stats\s*"
        r"\((.*?)\)\s*RETURNS\s+record",
        install_content,
        re.IGNORECASE | re.DOTALL,
    )
    require(errors, cache_stats_match is not None,
            f"fasttrun--{version}.sql: fasttrun_cache_stats() не найден")
    if cache_stats_match is not None:
        output_columns = re.findall(
            r"\bOUT\s+([a-z_][a-z0-9_]*)\s+([a-z0-9_.]+)",
            cache_stats_match.group(1),
            re.IGNORECASE,
        )
        output_names = tuple(name.lower() for name, _ in output_columns)
        output_types = tuple(sql_type.lower() for _, sql_type in output_columns)
        require(
            errors,
            output_names == CACHE_STATS_FIELDS,
            f"fasttrun--{version}.sql: неверные поля fasttrun_cache_stats()",
        )
        require(
            errors,
            output_types == ("bigint",) * len(CACHE_STATS_FIELDS),
            f"fasttrun--{version}.sql: поля fasttrun_cache_stats() должны быть bigint",
        )

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
        for field in CACHE_STATS_FIELDS:
            require(errors, field in content,
                    f"{label}: поле fasttrun_cache_stats.{field} не документировано")
        require(errors, "LRU" in content,
                f"{label}: отсутствие LRU не документировано")
        require(errors, "check_fasttrun_tracking_persistence.sh" in content,
                f"{label}: persistence-harness не документирован")
        require(
            errors,
            re.search(r"(?:13/13|13\s+(?:из|of)\s+13)", content) is not None,
            f"{label}: cassert-итог 13/13 не документирован",
        )

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
    ]
    if install_sql.exists():
        scan_paths.append(install_sql)
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
