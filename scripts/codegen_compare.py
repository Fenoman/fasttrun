#!/usr/bin/env python3
"""Helpers of check_codegen_identity.sh.

Usage:
  codegen_compare.py normalize-cpp IN OUT
      Copy preprocessor output IN to OUT with the file name and line number
      arguments of errfinish() and ExceptionalCondition() replaced by "F" and
      0.  Prints the number of replaced calls.
  codegen_compare.py compare-bin BASE CAND (obj|lib)
      Compare two builds of the same program whose input differs only in those
      arguments.  Prints every difference and exits 1 on any.

What is compared:
  functions     the set, sizes, and per instruction the mnemonic, registers,
                call and branch targets, relocation targets
  sections      names and sizes
  object data   contents with relocated slots masked, relocation targets
  library data  named data objects of an ELF library: size, contents with
                dynamic relocation slots masked, relocation targets
  strings       the multiset of string literals of the object, file names aside

What may differ:
  numbers         immediates, displacements and addresses
  strings         which string literal a reference points to, file name strings
  debug sections  .debug_*, __debug_*, LTO bytecode and LTO debug sections

Limits.  In a Mach-O library the data of named objects is not compared: dyld
chained fixups keep rebased pointers inside the data and objdump does not list
them, the object file covers that data.  On arm64 a data reference is an adrp
and add pair whose target is not resolved, the object file relocations name it.
"""
import bisect
import collections
import os
import re
import subprocess
import sys

STR_STRING = r'"(?:[^"\\]|\\.)*"'
ERRFINISH_RE = re.compile(r'\berrfinish\(\s*' + STR_STRING + r'\s*,\s*\d+\s*,')
ASSERT_RE = re.compile(r'\bExceptionalCondition\(\s*(' + STR_STRING + r'(?:\s*' + STR_STRING
                       + r')*)\s*,\s*' + STR_STRING + r'\s*,\s*\d+\s*\)')
# References to string literals: Mach-O labels, ELF labels and merged string sections.
STRSYM_RE = re.compile(r'l_\.str(?:\.\d+)?|\.L\.str(?:\.\d+)?|\.LC\d+|\.rodata\.str[\w.]*')
# A number that is not part of a name or a register (x10, %r12d, foo.cold.1).
NUM_RE = re.compile(r'(?<![A-Za-z0-9_.%$])(?:0x[0-9a-fA-F]+|\d+)|(?<=\$)(?:0x[0-9a-fA-F]+|\d+)')
ADDEND_RE = re.compile(r'[+-]0x[0-9a-fA-F]+$')
FILE_NAME_RE = re.compile(r'(?:^|/)(?:fasttrun|[0-9][0-9]_[a-z_]+)\.c$')
SKIP_SECTION_RE = re.compile(r'^(?:\.z?debug|__debug|\.gnu\.lto|\.gnu\.debuglto|\.comment$|\.note|__DWARF)')
# GCC LTO names local symbols after the unit and a hash of its contents.
TU_HASH_RE = re.compile(r'\b[\w-]+\.c\.[0-9a-f]{5,}\b')
STRING_SECTION_RE = re.compile(r'(?:^|,)(?:__cstring|\.rodata\.str[\w.]*)$')
BRANCH_RE = re.compile(r'^(?:b|bl|br|blr|b\.\w+|cbn?z|tbn?z|call\w*|jmp\w*|j\w+|ret\w*)$')
VISIBILITY = {".hidden", ".protected", ".internal"}
POINTER_RELOC_RE = re.compile(r'UNSIGNED|_64$|ABS64|RELATIVE|GLOB_DAT|JUMP_SLOT|IRELATIVE')

problems = []


def problem(msg):
    problems.append(msg)


def normalize_cpp(src, dst):
    with open(src, encoding="utf-8", errors="surrogateescape") as f:
        text = f.read()
    text, n1 = ERRFINISH_RE.subn('errfinish("F", 0,', text)
    text, n2 = ASSERT_RE.subn(lambda m: 'ExceptionalCondition(%s, "F", 0)' % m.group(1), text)
    with open(dst, "w", encoding="utf-8", errors="surrogateescape") as f:
        f.write(text)
    print("normalized: errfinish %d, ExceptionalCondition %d" % (n1, n2))


def run(args):
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          errors="surrogateescape").stdout


def objdump():
    return os.environ.get("OBJDUMP", "objdump")


def file_format(path):
    """(mach-o or elf, x86 flag) from objdump -h, None when objdump says nothing known."""
    m = re.search(r"file format (\S+(?: \S+)?)", run([objdump(), "-h", path]))
    if not m:
        return None
    fmt = m.group(1)
    kind = "mach-o" if fmt.startswith("mach-o") else "elf" if fmt.startswith("elf") else None
    return (kind, "x86-64" in fmt or "x86_64" in fmt) if kind else None


def sections(path):
    """[(name, size, vma)] in file order."""
    res = []
    for line in run([objdump(), "-h", path]).splitlines():
        m = re.match(r"\s*\d+\s+(\S+)\s+([0-9a-fA-F]+)\s+([0-9a-fA-F]+)", line)
        if m:
            res.append((m.group(1), int(m.group(2), 16), int(m.group(3), 16)))
    return res


def norm_target(name):
    name = STRSYM_RE.sub("STR", name)
    name = TU_HASH_RE.sub("TUSYM", name)
    return re.sub(r"_OUTLINED_FUNCTION_\d+", "OUTLINED", name)


class Symbols:
    """Address to name+offset for a linked library, with the extent of each symbol."""

    def __init__(self, path, fmt, headers):
        spans = []
        if fmt[0] == "elf":
            for line in run([objdump(), "-t", path]).splitlines():
                fields = [f for f in line.split() if f not in VISIBILITY]
                if len(fields) < 5 or not re.fullmatch(r"[0-9a-fA-F]{8,16}", fields[0]):
                    continue
                flags = line[len(fields[0]):len(fields[0]) + 9]
                if " F" not in flags and " O" not in flags:
                    continue
                size = int(fields[-2], 16)
                if size:
                    spans.append((int(fields[0], 16), size, fields[-1]))
        else:
            secs = sections(path)
            syms = []
            for line in run(["nm", "-n", path]).splitlines():
                fields = line.split()
                if len(fields) == 3 and fields[1] in "tTdDsSbB":
                    syms.append((int(fields[0], 16), fields[2]))
            for i, (addr, name) in enumerate(syms):
                end = next((v + s for _, s, v in secs if v <= addr < v + s), addr)
                if i + 1 < len(syms):
                    end = min(end, syms[i + 1][0])
                if end > addr:
                    spans.append((addr, end - addr, name))
        for addr, name in headers:
            spans.append((addr, 0, name))
        spans.sort()
        # A slot of the GOT has no symbol: its dynamic relocation names it.
        self.slots = {}
        if fmt[0] == "elf":
            for off, rtype, value, _ in dynamic_relocations(path):
                self.slots[off] = (rtype, value)
        self.code = [(v, v + n) for name, n, v in sections(path)
                     if re.search(r"(?:^|,)(?:\.text|\.plt[\w.]*|\.init|\.fini|__text|__stubs|__stub_helper)$", name)]
        self.spans = spans
        self.starts = [a for a, _, _ in spans]

    def resolve(self, addr, through_slot=True):
        if through_slot and addr in self.slots:
            rtype, value = self.slots[addr]
            m = re.match(r"\*ABS\*\+0x([0-9a-fA-F]+)$", value)
            return "slot:" + (self.resolve(int(m.group(1), 16), False) if m else norm_target(value))
        i = bisect.bisect_right(self.starts, addr) - 1
        for j in range(i, max(i - 64, -1), -1):
            start, size, name = self.spans[j]
            if size and start <= addr < start + size:
                off = addr - start
                return norm_target(name) + ("+%#x" % off if off else "")
            if not size and start == addr:
                return norm_target(name)
        # Code without a symbol per entry (the stub section of a Mach-O
        # library): the nearest disassembly header and the offset from it.
        if any(lo <= addr < hi for lo, hi in self.code):
            for start, size, name in reversed(self.spans[:i + 1]):
                if size == 0:
                    return norm_target(name) + "+%#x" % (addr - start)
        return "ANON"


def norm_insn(text, x86, symbols=None):
    text = text.strip()
    data_target = ""
    # Comments of objdump repeat operands in another radix and are padded by
    # their width: "#" in GNU x86 syntax, ";" and "//" in llvm arm64.  In a
    # linked x86 library the comment holds the only address of a RIP-relative
    # data reference, so its target is resolved before the comment goes.
    if x86:
        m = re.search(r"\s+#\s*([0-9a-fA-F]+)(?:\s*<[^>]*>)?\s*$", text)
        if m and symbols is not None:
            data_target = " @" + symbols.resolve(int(m.group(1), 16))
        text = re.sub(r"\s+#.*$", "", text)
    else:
        text = re.sub(r"\s*(?:;|//).*$", "", text)
    text = re.sub(r"\s+", " ", text)
    parts = text.split(None, 1)
    mnem = parts[0] if parts else ""
    ops = parts[1] if len(parts) > 1 else ""
    if not BRANCH_RE.match(mnem):
        ops = re.sub(r"\s*<[^>]*>", "", ops)
    else:
        m = re.search(r"(?:0x)?([0-9a-fA-F]+)\s*<([^>]*)>", ops)
        if m:
            target = symbols.resolve(int(m.group(1), 16)) if symbols is not None else norm_target(m.group(2))
            # The offset inside a function is masked with the other numbers in
            # an object file. In a library it is part of the resolved name.
            ops = ops[:m.start()] + "<" + target + ">" + ops[m.end():]
    ops = norm_target(ops)
    if symbols is None:
        ops = NUM_RE.sub("N", ops)
    else:
        head, sep, tail = ops.partition("<")
        ops = NUM_RE.sub("N", head) + sep + tail
    return (mnem + " " + ops).strip() + data_target


def norm_reloc(rtype, target):
    target = target.strip()
    if STRSYM_RE.match(target):
        return "RELOC %s STR" % rtype
    return "RELOC %s %s" % (rtype, norm_target(target))


def functions(path, fmt, kind):
    """{name: (size, [normalized lines])} from objdump -d -r."""
    out = run([objdump(), "-d", "-r", "--no-show-raw-insn", path])
    symbols = None
    if kind == "lib":
        headers = [(int(m.group(1), 16), m.group(2)) for m in
                   re.finditer(r"^([0-9a-fA-F]+) <(.+)>:$", out, re.M)]
        symbols = Symbols(path, fmt, headers)
    funcs = collections.OrderedDict()
    order = []
    cur = None
    sec = None
    for line in out.splitlines():
        m = re.match(r"Disassembly of section (.+):$", line)
        if m:
            sec = m.group(1)
            cur = None
            continue
        m = re.match(r"([0-9a-fA-F]+) <(.+)>:$", line)
        if m:
            name = m.group(2)
            if name.startswith("_OUTLINED_FUNCTION_"):
                cur = None
                continue
            key = name if name not in funcs else "%s@%s" % (name, sec)
            cur = funcs.setdefault(key, [int(m.group(1), 16), []])
            order.append((key, sec))
            continue
        if cur is None or not line.strip():
            continue
        m = re.match(r"\s*[0-9a-fA-F]+:\s+(ARM64_RELOC_\w+|R_[A-Z0-9_]+)\s+(\S+)", line)
        if m:
            cur[1].append(norm_reloc(m.group(1), m.group(2)))
            continue
        m = re.match(r"\s*([0-9a-fA-F]+):\s+(.*)$", line)
        if m:
            cur[1].append(norm_insn(m.group(2), fmt[1], symbols))
            cur.append(int(m.group(1), 16))
    res = {}
    by_sec = collections.defaultdict(list)
    for key, s in order:
        by_sec[s].append(key)
    for keys in by_sec.values():
        for i, key in enumerate(keys):
            start = funcs[key][0]
            if i + 1 < len(keys):
                size = funcs[keys[i + 1]][0] - start
            else:
                addrs = funcs[key][2:]
                size = (max(addrs) - start) if addrs else 0
            res[key] = (size, funcs[key][1])
    return res


def contents(path):
    """{section name: (start address, bytes)} from objdump -s."""
    res = collections.OrderedDict()
    cur = None
    for line in run([objdump(), "-s", path]).splitlines():
        m = re.match(r"Contents of section (.+):$", line)
        if m:
            cur = m.group(1)
            res[cur] = [None, bytearray()]
            continue
        m = re.match(r"\s*([0-9a-fA-F]+)((?: [0-9a-fA-F]{2,8}){1,4})\s", line)
        if cur is not None and m:
            if res[cur][0] is None:
                res[cur][0] = int(m.group(1), 16)
            for word in m.group(2).split():
                res[cur][1] += bytes.fromhex(word)
    return res


def relocations(path, fmt):
    """{section name as objdump -s names it: [(offset, type, target, width)]}."""
    res = collections.defaultdict(list)
    if fmt[0] == "mach-o":
        cur = None
        for line in run([objdump(), "--macho", "-r", path]).splitlines():
            m = re.match(r"Relocation information \((.+)\) \d+ entries$", line)
            if m:
                cur = m.group(1)
                continue
            m = re.match(r"([0-9a-fA-F]+)\s+(True|False)\s+\S*\(\s*(\d+)\)\s+(True|False)\s+(\S+)"
                         r"\s+(True|False)\s+(.+)$", line)
            if cur is not None and m:
                res[cur].append((int(m.group(1), 16), m.group(5), m.group(7).strip(),
                                 2 ** int(m.group(3))))
    else:
        cur = None
        for line in run([objdump(), "-r", path]).splitlines():
            m = re.match(r"RELOCATION RECORDS FOR \[(.+)\]:$", line)
            if m:
                cur = m.group(1)
                continue
            m = re.match(r"([0-9a-fA-F]+)\s+(\S+)\s+(\S+)", line)
            if cur is not None and m and not line.startswith("OFFSET"):
                width = 8 if POINTER_RELOC_RE.search(m.group(2)) else 4
                res[cur].append((int(m.group(1), 16), m.group(2), m.group(3), width))
    return res


def masked(data, relocs, base=0):
    data = bytearray(data)
    for off, _, _, width in relocs:
        data[off - base:off - base + width] = b"\0" * width
    return data


def split_strings(data):
    return [s.decode("utf-8", "surrogateescape") for s in bytes(data).split(b"\0")]


def compare_functions(base, cand, fmt, kind):
    fb, fc = functions(base, fmt, kind), functions(cand, fmt, kind)
    if not fb:
        problem("FUNC none found in %s: objdump output not understood" % base)
    for name in sorted(set(fb) - set(fc)):
        problem("FUNC %s: only in base" % name)
    for name in sorted(set(fc) - set(fb)):
        problem("FUNC %s: only in candidate" % name)
    for name in fb:
        if name not in fc:
            continue
        (sb, lb), (sc, lc) = fb[name], fc[name]
        if sb != sc:
            problem("FUNC %s: size %d -> %d" % (name, sb, sc))
        if lb != lc:
            for i, (a, b) in enumerate(zip(lb, lc)):
                if a != b:
                    problem("FUNC %s: line %d: %r -> %r" % (name, i, a, b))
                    break
            else:
                problem("FUNC %s: %d -> %d lines" % (name, len(lb), len(lc)))
    return len(fb)


def compare_sections(base, cand, kind):
    sb = [(n, s) for n, s, _ in sections(base) if not SKIP_SECTION_RE.search(n)]
    sc = [(n, s) for n, s, _ in sections(cand) if not SKIP_SECTION_RE.search(n)]
    if not sb:
        problem("SECTION none found in %s" % base)
    if [n for n, _ in sb] != [n for n, _ in sc]:
        problem("SECTION list differs: %s -> %s" % ([n for n, _ in sb], [n for n, _ in sc]))
        return
    for (name, a), (_, b) in zip(sb, sc):
        if STRING_SECTION_RE.search(name):
            continue
        # A linked ELF library merges .rodata.str* into .rodata, so its size
        # follows the file name strings.  Its named objects are compared by
        # compare_library_data, the object file compares the strings.
        if kind == "lib" and name == ".rodata":
            continue
        if a != b:
            problem("SECTION %s: size %d -> %d" % (name, a, b))


def norm_relocs(relocs):
    return sorted((o, t, "STR" if STRSYM_RE.match(v) else norm_target(v)) for o, t, v, _ in relocs)


def compare_object_data(base, cand, fmt):
    cb, cc = contents(base), contents(cand)
    rb, rc = relocations(base, fmt), relocations(cand, fmt)
    for name, (_, data) in cb.items():
        short = name.split(",")[-1]
        if name not in cc or SKIP_SECTION_RE.search(short):
            continue
        if STRING_SECTION_RE.search(name):
            sb = collections.Counter(s for s in split_strings(data) if not FILE_NAME_RE.search(s))
            sc = collections.Counter(s for s in split_strings(cc[name][1]) if not FILE_NAME_RE.search(s))
            if sb != sc:
                problem("STRINGS %s: only in base %s, only in candidate %s"
                        % (name, sorted((sb - sc).elements())[:5], sorted((sc - sb).elements())[:5]))
            continue
        if re.search(r"__text$|^\.text", short):
            continue
        xb, xc = rb.get(name, []), rc.get(name, [])
        if norm_relocs(xb) != norm_relocs(xc):
            problem("RELOCS %s: %s" % (name, sorted(set(norm_relocs(xb)) ^ set(norm_relocs(xc)))[:5]))
        if masked(data, xb) != masked(cc[name][1], xc):
            problem("DATA %s: contents differ outside relocated slots" % name)


def data_objects(path):
    """{(name, section, n): (address, size)} of the named data objects of an ELF file."""
    res = {}
    seen = collections.Counter()
    for line in run([objdump(), "-t", path]).splitlines():
        fields = [f for f in line.split() if f not in VISIBILITY]
        if len(fields) < 5 or not re.fullmatch(r"[0-9a-fA-F]{8,16}", fields[0]):
            continue
        if " O" not in line[len(fields[0]):len(fields[0]) + 9]:
            continue
        key = (fields[-1], fields[-3])
        res[key + (seen[key],)] = (int(fields[0], 16), int(fields[-2], 16))
        seen[key] += 1
    return res


def dynamic_relocations(path):
    res = []
    for line in run([objdump(), "-R", path]).splitlines():
        m = re.match(r"([0-9a-fA-F]+)\s+(R_\S+)\s+(\S+)", line)
        if m:
            res.append((int(m.group(1), 16), m.group(2), m.group(3), 8))
    return res


def compare_library_data(base, cand, fmt):
    if fmt[0] != "elf":
        return
    ob, oc = data_objects(base), data_objects(cand)
    if set(ob) != set(oc):
        problem("OBJECTS only in base %s, only in candidate %s"
                % (sorted(set(ob) - set(oc))[:5], sorted(set(oc) - set(ob))[:5]))
    cb, cc = contents(base), contents(cand)
    rb, rc = dynamic_relocations(base), dynamic_relocations(cand)
    hb = Symbols(base, fmt, [])
    hc = Symbols(cand, fmt, [])

    def view(objects, cont, relocs, syms, key):
        addr, size = objects[key]
        section = key[1]
        if section not in cont:
            return size, None, []
        start, data = cont[section]
        inside = [r for r in relocs if addr <= r[0] < addr + size]
        body = masked(data[addr - start:addr - start + size], inside, base=addr)
        targets = []
        for off, rtype, value, _ in inside:
            m = re.match(r"\*ABS\*\+0x([0-9a-fA-F]+)$", value)
            target = syms.resolve(int(m.group(1), 16)) if m else norm_target(value)
            targets.append((off - addr, rtype, target))
        return size, bytes(body), targets

    for key in sorted(set(ob) & set(oc)):
        vb, vc = view(ob, cb, rb, hb, key), view(oc, cc, rc, hc, key)
        if vb[0] != vc[0]:
            problem("OBJECT %s: size %d -> %d" % (key[0], vb[0], vc[0]))
        elif vb[1] != vc[1]:
            problem("OBJECT %s: contents differ outside relocated slots" % key[0])
        elif vb[2] != vc[2]:
            problem("OBJECT %s: relocation targets %s -> %s" % (key[0], vb[2][:3], vc[2][:3]))


def compare_bin(base, cand, kind):
    fmt = file_format(base)
    if fmt is None or file_format(cand) != fmt:
        problem("FORMAT not understood or different: %s, %s" % (base, cand))
        n = 0
    else:
        n = compare_functions(base, cand, fmt, kind)
        compare_sections(base, cand, kind)
        if kind == "obj":
            compare_object_data(base, cand, fmt)
        else:
            compare_library_data(base, cand, fmt)
    for p in problems[:200]:
        print(p)
    if len(problems) > 200:
        print("... %d more" % (len(problems) - 200))
    print("compared %d functions of %s: %s" % (n, os.path.basename(cand),
                                             "PASS" if not problems else "FAIL (%d)" % len(problems)))
    sys.exit(1 if problems else 0)


def main(argv):
    if len(argv) == 3 and argv[0] == "normalize-cpp":
        normalize_cpp(argv[1], argv[2])
    elif len(argv) == 4 and argv[0] == "compare-bin" and argv[3] in ("obj", "lib"):
        compare_bin(argv[1], argv[2], argv[3])
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
