#!/usr/bin/env python3
"""Проверяет, что публичная документация не расходится с кодом."""

from __future__ import annotations

from collections import deque
import argparse
import hashlib
import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent
EXTENSION_SQL_DIR = ROOT / "extension"
README_PATHS = (ROOT / "README.md", ROOT / "README_EN.md")
# Документация языка - это README плюс его каталог docs/<язык>.
DOC_SETS = (
    (ROOT / "README.md", ROOT / "docs" / "ru"),
    (ROOT / "README_EN.md", ROOT / "docs" / "en"),
)
# Цель встроенной ссылки: [текст](цель), [текст](<цель>), с заголовком или без.
MD_INLINE_LINK_RE = re.compile(
    r"\[[^\]]*\]\(\s*(?:<([^>]*)>|([^)\s]+))"
    r"(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?\s*\)"
)
# Сноска: использование [текст][метка] или [метка][] и определение [метка]: цель.
MD_REF_USE_RE = re.compile(r"\[([^\]]+)\]\[([^\]]*)\]")
MD_REF_DEF_RE = re.compile(r"^ {0,3}\[([^\]]+)\]:\s*(?:<([^>]*)>|(\S+))")
HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
INLINE_CODE_RE = re.compile(r"(`+)(.+?)(?<!`)\1(?!`)")
CACHE_STATS_FIELDS = (
    "analyze_entries",
    "column_stats_relid_entries",
    "column_stats_entries",
    "analyze_bytes",
    "column_stats_bytes",
    "total_bytes",
)
FROZEN_SQL_SHA256 = {
    "fasttrun--2.0--2.1.sql": (
        "3134078de967ce61e4d66f99560aa6638c5eafc3af07a8f60f75d2c5cca4816f"
    ),
    "fasttrun--2.0.sql": (
        "69d8756f8e9459f92dc37ba6240ceac2134072ef3fbbe9f11373d7aca74ed08d"
    ),
    "fasttrun--2.1--2.1.1.sql": (
        "c119d118a5900e84d7c6dbe72bbe42bc57617899aed12520ca18bc9d3ea7ecfa"
    ),
    "fasttrun--2.1.1--2.1.2.sql": (
        "d8e26cce14e5c21bd3df05c5f93e4e1715aec45d077de4bf02e8da58eb5008ea"
    ),
    "fasttrun--2.1.1.sql": (
        "617f537dbdc3ce64985a7ef3146a0067a298380dc42844bd13cce84f831c3fb5"
    ),
    "fasttrun--2.1.2--2.2.0.sql": (
        "475213b0f7ac9a4d0f2d17e99f9610062cf0de42d106510ef071e8bf628b3f7c"
    ),
    "fasttrun--2.1.2.sql": (
        "617f537dbdc3ce64985a7ef3146a0067a298380dc42844bd13cce84f831c3fb5"
    ),
    "fasttrun--2.1.sql": (
        "617f537dbdc3ce64985a7ef3146a0067a298380dc42844bd13cce84f831c3fb5"
    ),
    "fasttrun--2.2.0--2.3.0.sql": (
        "94b5fe3d6992755293e74394bdd821e45a6f23f526feb714ee7e0ec8231663f2"
    ),
    "fasttrun--2.2.0.sql": (
        "617f537dbdc3ce64985a7ef3146a0067a298380dc42844bd13cce84f831c3fb5"
    ),
    "fasttrun--2.3.0--2.3.1.sql": (
        "c3d70f646ed4afe3fb8af21aed32cd406adb43267278372182b20dd443059776"
    ),
    "fasttrun--2.3.0.sql": (
        "2e7d5cd64ba1dc2c2e3bcdaae14fec26c4c91ce0651239e74df364e4504cf1bf"
    ),
    "fasttrun--2.3.1--2.3.2.sql": (
        "010b2ed915fde3735416adbf23dc79d5320e3adba0d71c2dbb6446e3dda39aef"
    ),
    "fasttrun--2.3.1.sql": (
        "2e7d5cd64ba1dc2c2e3bcdaae14fec26c4c91ce0651239e74df364e4504cf1bf"
    ),
    "fasttrun--2.3.2--2.3.3.sql": (
        "ef3bde707baf5e27eb893fab677e7397203de05b63d2d20abab07ebec9456343"
    ),
    "fasttrun--2.3.2.sql": (
        "2e7d5cd64ba1dc2c2e3bcdaae14fec26c4c91ce0651239e74df364e4504cf1bf"
    ),
    "fasttrun--2.3.3--2.3.4.sql": (
        "0bddaa218f8271affd4f98779665795df9c4ab0df9e5f9a363727aa39f6ff9b9"
    ),
    "fasttrun--2.3.3.sql": (
        "2e7d5cd64ba1dc2c2e3bcdaae14fec26c4c91ce0651239e74df364e4504cf1bf"
    ),
    "fasttrun--2.3.4--2.4.0.sql": (
        "0379f9676f34beecab59b6f7008f2d903b9ec08579c99fc43d952d4d799c1825"
    ),
    "fasttrun--2.3.4.sql": (
        "2e7d5cd64ba1dc2c2e3bcdaae14fec26c4c91ce0651239e74df364e4504cf1bf"
    ),
    "fasttrun--2.4.0--2.4.1.sql": (
        "ca1cce445e75585e09601fe852d5e883855cb5342ca5464e4dc0936d7b5f4d00"
    ),
    "fasttrun--2.4.0.sql": (
        "99b821bdadab98b20291a2800c4c914913f6307b2bfcc3640d9aa5293818d917"
    ),
    "fasttrun--2.4.1--2.5.0.sql": (
        "1eb401074645c6e25af38499cd317e41d0a1b1c99785b06ef29c6d5abafb43bf"
    ),
    "fasttrun--2.4.1.sql": (
        "99b821bdadab98b20291a2800c4c914913f6307b2bfcc3640d9aa5293818d917"
    ),
    "fasttrun--2.5.0--2.5.1.sql": (
        "4d2f5e1a63191ddd1763bc9fc6c9e5ec76e4b3ba2e289f9bdeba7d43b3926e9f"
    ),
    "fasttrun--2.5.0.sql": (
        "99b821bdadab98b20291a2800c4c914913f6307b2bfcc3640d9aa5293818d917"
    ),
    "fasttrun--2.5.1--2.5.2.sql": (
        "61a19b8dde493ace19f46ec449a70e4f6f357424a1d06a39ba6c905b385e7f2b"
    ),
    "fasttrun--2.5.1.sql": (
        "d1cde6e9324c148b07ce222e6091af00f700e626f873588f664d376d591ccddb"
    ),
    "fasttrun--2.5.2--2.6.0.sql": (
        "87afe7844bdfba7bad1077d6637ca66413ae8dbb2a8ca9a1c223b3f2b2a3285c"
    ),
    "fasttrun--2.5.2.sql": (
        "d1cde6e9324c148b07ce222e6091af00f700e626f873588f664d376d591ccddb"
    ),
    "fasttrun--2.6.0--2.7.0.sql": (
        "fa61a7dfc420a0d9fec1a9fd92e32d67221956524204cdc9f5c86ee7923a75df"
    ),
    "fasttrun--2.6.0.sql": (
        "88debf6cd3c09c7c1819508306f97c8965a536a45358851c70a26129288da696"
    ),
    "fasttrun--2.7.0.sql": (
        "08de4006dca78e7dd03d1ddfd08a86527244370f101ebc17087e7bbe8a480cf6"
    ),
    "fasttrun--2.7.0--2.7.1.sql": (
        "ae29ddc8af2a6a422feac456a88478de1ecf6645477bbbedf05e5590a9cd5d82"
    ),
    "fasttrun--2.7.1.sql": (
        "08de4006dca78e7dd03d1ddfd08a86527244370f101ebc17087e7bbe8a480cf6"
    ),
    "fasttrun--unpackaged--2.0.sql": (
        "570c6f9be968d858d3b09b7a36833dd19b9e654fc7c2b73bfc76ade0fe24e8f5"
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


def github_slug(heading: str) -> str:
    """Основа якоря по правилам GitHub: строчные буквы, без пунктуации,
    пробелы заменены дефисами."""
    text = re.sub(r"[^\w\- ]", "", heading.strip().lower())
    return text.replace(" ", "-")


def strip_html_comments(text: str) -> str:
    """Текст без HTML-комментариев: читатель их не видит."""
    return HTML_COMMENT_RE.sub("", text)


def markdown_outline(text: str) -> tuple[set[str], list[str]]:
    """Якоря заголовков и цели ссылок вне кода и HTML-комментариев.

    Повторы заголовков нумеруются как в github-slugger: следующий свободный
    номер, даже если заголовок с таким суффиксом уже есть."""
    anchors: set[str] = set()
    targets: list[str] = []
    occurrences: dict[str, int] = {}
    labels: dict[str, str] = {}
    used_labels: list[str] = []
    fence = None
    for line in strip_html_comments(text).splitlines():
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if fence is None:
            if marker:
                fence = marker.group(1)
                continue
        else:
            if (marker and marker.group(1)[0] == fence[0]
                    and len(marker.group(1)) >= len(fence)
                    and not line.strip()[len(marker.group(1)):].strip()):
                fence = None
            continue
        line = INLINE_CODE_RE.sub("", line)
        heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if heading:
            base = github_slug(heading.group(1))
            slug = base
            while slug in occurrences:
                occurrences[base] += 1
                slug = f"{base}-{occurrences[base]}"
            occurrences[slug] = 0
            anchors.add(slug)
        definition = MD_REF_DEF_RE.match(line)
        if definition:
            label = " ".join(definition.group(1).lower().split())
            labels[label] = definition.group(2) or definition.group(3)
            continue
        for bracketed, plain in MD_INLINE_LINK_RE.findall(line):
            targets.append(bracketed or plain)
        for shown, label in MD_REF_USE_RE.findall(line):
            used_labels.append(" ".join((label or shown).lower().split()))
    targets.extend(labels.values())
    for label in used_labels:
        if label not in labels:
            targets.append(f"\0undefined-reference:{label}")
    return anchors, targets


def check_doc_structure(errors: list[str], root: Path = ROOT) -> None:
    """Пары документов на двух языках, ссылки README на них и целые ссылки."""
    readmes = (root / "README.md", root / "README_EN.md")
    doc_sets = ((readmes[0], root / "docs" / "ru"),
                (readmes[1], root / "docs" / "en"))
    for readme in readmes:
        require(errors, readme.is_file(), f"{readme.name}: file is missing")
    if not all(readme.is_file() for readme in readmes):
        return
    names = [
        sorted(path.name for path in docs_dir.glob("*.md"))
        for _, docs_dir in doc_sets
    ]
    require(errors, all(names), "docs/ru и docs/en не должны быть пустыми")
    require(errors, names[0] == names[1],
            f"docs/ru и docs/en расходятся по составу: {names[0]} != {names[1]}")
    for readme, docs_dir in doc_sets:
        _, readme_targets = markdown_outline(read(readme))
        linked = {unquote(target.partition("#")[0]) for target in readme_targets}
        for path in sorted(docs_dir.glob("*.md")):
            require(errors, f"docs/{docs_dir.name}/{path.name}" in linked,
                    f"{readme.name}: нет ссылки на docs/{docs_dir.name}/{path.name}")

    pages = [
        *readmes,
        *(path for _, docs_dir in doc_sets for path in sorted(docs_dir.glob("*.md"))),
    ]
    outlines = {path.resolve(): markdown_outline(read(path)) for path in pages}
    for path in pages:
        _, targets = outlines[path.resolve()]
        for target in targets:
            where = f"{path.relative_to(root)}: ссылка {target}"
            if target.startswith("\0undefined-reference:"):
                errors.append(f"{path.relative_to(root)}: сноска "
                              f"[{target.split(':', 1)[1]}] не определена")
                continue
            if re.match(r"^[a-z][a-z0-9+.-]*:", target):
                continue
            file_part, _, anchor = target.partition("#")
            file_part, anchor = unquote(file_part), unquote(anchor)
            dest = (path.parent / file_part).resolve() if file_part else path.resolve()
            if not dest.is_relative_to(root.resolve()):
                errors.append(f"{where}: target is outside the documentation tree")
                continue
            if not dest.exists():
                errors.append(f"{where} ведет на несуществующий файл")
                continue
            if anchor:
                if dest not in outlines:
                    errors.append(f"{where}: якорь в файле вне документации")
                elif anchor not in outlines[dest][0]:
                    errors.append(f"{where}: нет такого заголовка")


def require(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def path_exists(graph: dict[str, set[str]], source: str, target: str) -> bool:
    """Проверяет достижимость версии по направленным upgrade-ребрам."""
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


# Срезы кода: fasttrun.c подключает src/NN_имя.c одной единицей трансляции.
SLICE_INCLUDE_RE = re.compile(
    r'^#include "src/([0-9][0-9])_[a-z_]*\.c"[ \t]*(?:/\*.*\*/)?[ \t]*$'
)
ANY_SLICE_INCLUDE_RE = re.compile(r'^\s*#\s*include\s*"src/')
SLICE_FORBIDDEN_RE = re.compile(
    r"^\s*#\s*(?:include\b|line\b|pragma\b|define\s+FASTTRUN_UNITY\b)"
)
MAX_SOURCE_LINES = 2000


def check_unity_layout(errors: list[str]) -> None:
    """Проверяет раскладку срезов src/ и строки их подключения в fasttrun.c.

    Что проверяет:
    - каждая строка подключения "src/" записана по формату, который читает
      правило Makefile, номера идут подряд с 01 в порядке подключения
    - каждый src/*.c подключен ровно один раз, лишних и пропавших нет
    - #define FASTTRUN_UNITY стоит в корне до первого среза
    - в срезах нет #include, #line, #pragma и #define FASTTRUN_UNITY, у
      каждого есть проверка FASTTRUN_UNITY
    - каждый файл, включая корень, не длиннее MAX_SOURCE_LINES строк
    - OBJS = fasttrun.o, а дерево файлов документации перечисляет src/.
    """
    root_lines = read(ROOT / "fasttrun.c").splitlines()
    included: list[str] = []
    define_at = None
    for number, line in enumerate(root_lines, start=1):
        if line == "#define FASTTRUN_UNITY":
            define_at = number
        if not ANY_SLICE_INCLUDE_RE.match(line):
            continue
        match = SLICE_INCLUDE_RE.match(line)
        if match is None:
            errors.append(
                f"fasttrun.c:{number}: строка подключения среза не по формату"
                f' #include "src/NN_имя.c": {line!r}'
            )
            continue
        if define_at is None:
            errors.append(
                f"fasttrun.c:{number}: срез подключен до #define FASTTRUN_UNITY"
            )
        included.append(line.split('"')[1])

    expected = [f"{index:02d}" for index in range(1, len(included) + 1)]
    numbers = [Path(name).name[:2] for name in included]
    require(errors, numbers == expected,
            f"fasttrun.c: номера срезов идут не подряд с 01: {numbers}")
    duplicates = sorted({name for name in included if included.count(name) > 1})
    require(errors, not duplicates,
            f"fasttrun.c: срезы подключены больше одного раза: {duplicates}")
    on_disk = sorted(f"src/{path.name}" for path in (ROOT / "src").glob("*.c"))
    require(errors, sorted(set(included)) == on_disk,
            "src/ и подключения в fasttrun.c расходятся: лишние "
            f"{sorted(set(on_disk) - set(included))}, "
            f"пропавшие {sorted(set(included) - set(on_disk))}")

    require(errors, len(root_lines) <= MAX_SOURCE_LINES,
            f"fasttrun.c: {len(root_lines)} строк, предел {MAX_SOURCE_LINES}")
    for name in on_disk:
        lines = read(ROOT / name).splitlines()
        require(errors, len(lines) <= MAX_SOURCE_LINES,
                f"{name}: {len(lines)} строк, предел {MAX_SOURCE_LINES}")
        require(errors,
                "#ifndef FASTTRUN_UNITY" in lines
                and "#endif /* FASTTRUN_UNITY */" in lines,
                f"{name}: нет проверки FASTTRUN_UNITY")
        for number, line in enumerate(lines, start=1):
            if SLICE_FORBIDDEN_RE.match(line):
                errors.append(f"{name}:{number}: директива не для среза: {line!r}")

    require(errors, make_variable(read(ROOT / "Makefile"), "OBJS") == ["fasttrun.o"],
            "Makefile: OBJS должен быть ровно fasttrun.o")
    for _, docs_dir in DOC_SETS:
        tree = read(docs_dir / "internals.md")
        require(errors, re.search(r"^src/\s+#", tree, re.MULTILINE) is not None,
                f"docs/{docs_dir.name}/internals.md: каталог src/ не описан в "
                "файловой структуре")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installed-root", type=Path,
                        help="check links in the installed documentation tree only")
    args = parser.parse_args()
    errors: list[str] = []
    if args.installed_root is not None:
        check_doc_structure(errors, args.installed_root.resolve())
        for error in errors:
            print(f"installed docs: {error}", file=sys.stderr)
        if errors:
            return 1
        print("installed docs: PASS")
        return 0
    makefile = read(ROOT / "Makefile")
    control = read(ROOT / "fasttrun.control")
    code = read(ROOT / "fasttrun.c")

    root_extension_sql = sorted(ROOT.glob("fasttrun--*.sql"))
    require(errors, EXTENSION_SQL_DIR.is_dir(),
            "нет каталога extension с SQL расширения")
    require(
        errors,
        not root_extension_sql,
        "versioned SQL должны находиться в extension, а не в корне: "
        + ", ".join(path.name for path in root_extension_sql),
    )

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
    data_set = {Path(item).as_posix() for item in data}
    repository_sql = {
        path.relative_to(ROOT).as_posix()
        for path in EXTENSION_SQL_DIR.glob("fasttrun--*.sql")
        if path.is_file()
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
    for data_path in sorted(data_set):
        filename = Path(data_path).name
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
        path = EXTENSION_SQL_DIR / filename
        require(errors, path.exists(), f"нет замороженного SQL {filename}")
        if path.exists():
            actual_sha = hashlib.sha256(path.read_bytes()).hexdigest()
            require(
                errors,
                actual_sha == expected_sha,
                f"{filename}: изменен замороженный SQL, SHA256 {actual_sha}",
            )

    install_sql = EXTENSION_SQL_DIR / f"fasttrun--{version}.sql"
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

    for readme, docs_dir in DOC_SETS:
        files = [readme, *sorted(docs_dir.glob("*.md"))]
        content = "\n".join(strip_html_comments(read(path)) for path in files)
        label = f"{readme.name} + docs/{docs_dir.name}"
        require(
            errors,
            re.search(r"^extension/\s+#", content, re.MULTILINE) is not None,
            f"{label}: каталог extension не описан в файловой структуре",
        )
        require(
            errors,
            re.search(r"^fasttrun--[^\s]+\.sql\s+#", content, re.MULTILINE)
            is None,
            f"{label}: versioned SQL ошибочно показаны в корне",
        )
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
            re.search(
                rf"(?:{len(regress)}/{len(regress)}"
                rf"|{len(regress)}\s+(?:из|of)\s+{len(regress)})",
                content,
            ) is not None,
            f"{label}: cassert-итог {len(regress)}/{len(regress)} не документирован",
        )

    check_doc_structure(errors)

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
        *(path for _, docs_dir in DOC_SETS for path in sorted(docs_dir.glob("*.md"))),
        ROOT / "README.fasttrun",
        *sorted(EXTENSION_SQL_DIR.glob("fasttrun--2.3.[0-9].sql")),
    ]
    if install_sql.exists():
        scan_paths.append(install_sql)
    for path in scan_paths:
        content = read(path)
        for pattern, description in forbidden.items():
            if re.search(pattern, content, re.IGNORECASE):
                errors.append(f"{path.name}: {description}")

    check_unity_layout(errors)

    if errors:
        for error in errors:
            print(f"docs consistency: {error}", file=sys.stderr)
        return 1

    print("docs consistency: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
