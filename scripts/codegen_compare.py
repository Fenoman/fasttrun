#!/usr/bin/env python3
"""Helpers of check_codegen_identity.sh.

Usage:
  codegen_compare.py normalize-cpp IN OUT
      Copy preprocessor output IN to OUT with the file name and line number
      arguments of errfinish() and ExceptionalCondition() replaced by "F" and
      0.  Prints the number of replaced calls.
  codegen_compare.py compare-bin BASE CAND (obj|lib)
      Compare two builds of the same program whose input differs only in those
      arguments.  An object file is compared by functions, sections, data
      with relocated slots masked and string literals.  A linked library is
      compared by functions and sections.  Prints every difference and exits 1 on any.

What may differ between BASE and CAND:
  numbers         immediates, displacements and addresses in instructions
  strings         references to string literals and the file name strings
  debug sections  .debug_*, __debug_*, LTO bytecode sections
Everything else must match: the set of functions, their sizes, mnemonics,
registers and the targets of calls and of relocations to named symbols.
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
STRSYM_RE = re.compile(r'l_\.str(?:\.\d+)?|\.L\.str(?:\.\d+)?|\.LC\d+|\.rodata\.str[\w.]*'
                       r'|\.rodata\.cst\d+')
NUM_RE = re.compile(r'(?<![A-Za-z_.%$])(?:0x[0-9a-fA-F]+|\d+)|(?<=\$)(?:0x[0-9a-fA-F]+|\d+)')
FILE_NAME_RE = re.compile(r'(?:^|/)(?:fasttrun|[0-9][0-9]_[a-z_]+)\.c$')
SKIP_SECTION_RE = re.compile(r'^(?:\.z?debug|__debug|\.gnu\.lto|\.gnu\.debuglto|\.comment$|\.note|__DWARF)')
# GCC LTO names local symbols after the unit and a hash of its contents.
TU_HASH_RE = re.compile(r'\b[\w-]+\.c\.[0-9a-f]{5,}\b')
STRING_SECTION_RE = re.compile(r'(?:^|,)(?:__cstring|\.rodata\.str[\w.]*)$')
BRANCH_RE = re.compile(r'^(?:b|bl|br|blr|b\.\w+|cbn?z|tbn?z|call\w*|jmp\w*|j\w+|ret\w*)$')

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


def arch_of(path):
    out = run([objdump(), "-h", path])
    m = re.search(r"file format (\S+(?: \S+)?)", out)
    return m.group(1) if m else "unknown"


def sections(path):
    """[(name, size)] in file order, names qualified where objdump -s shows them so."""
    out = run([objdump(), "-h", path])
    res = []
    for line in out.splitlines():
        m = re.match(r"\s*\d+\s+(\S+)\s+([0-9a-fA-F]+)\s", line)
        if m:
            res.append((m.group(1), int(m.group(2), 16)))
    return res


def norm_insn(text, x86, resolve=None):
    text = text.strip()
    # Comments of objdump repeat the operands in another radix and are padded
    # by their width: "#" in GNU x86 syntax, ";" and "//" in llvm arm64.
    if x86:
        text = re.sub(r"\s+#.*$", "", text)
    else:
        text = re.sub(r"\s*(?:;|//).*$", "", text)
    text = re.sub(r"\s+", " ", text)
    parts = text.split(None, 1)
    mnem = parts[0] if parts else ""
    ops = parts[1] if len(parts) > 1 else ""
    if not BRANCH_RE.match(mnem):
        ops = re.sub(r"\s*<[^>]*>", "", ops)
    elif resolve is not None:
        # In a linked library several symbols can share an address, and which
        # one objdump prints depends on their names.  Name the target by the
        # function that contains it instead.
        m = re.search(r"(?:0x)?([0-9a-fA-F]+)\s*<[^>]*>", ops)
        if m:
            ops = ops[:m.start()] + "<" + resolve(int(m.group(1), 16)) + ">" + ops[m.end():]
    ops = STRSYM_RE.sub("STR", ops)
    ops = TU_HASH_RE.sub("TUSYM", ops)
    ops = re.sub(r"_OUTLINED_FUNCTION_\d+", "OUTLINED", ops)
    ops = NUM_RE.sub("N", ops)
    return (mnem + " " + ops).strip()


def norm_reloc(text):
    fields = text.split()
    if len(fields) < 2:
        return text.strip()
    rtype, target = fields[-2], fields[-1]
    target = STRSYM_RE.sub("STR", target)
    target = TU_HASH_RE.sub("TUSYM", target)
    target = re.sub(r"_OUTLINED_FUNCTION_\d+", "OUTLINED", target)
    target = re.sub(r"[+-]0x[0-9a-fA-F]+$", "", target)
    return "RELOC " + rtype + " " + target


def functions(path, kind="obj"):
    """{name: (size, [normalized lines])} from objdump -d -r."""
    x86 = "x86-64" in arch_of(path)
    out = run([objdump(), "-d", "-r", "--no-show-raw-insn", path])
    resolve = None
    if kind == "lib":
        heads = sorted((int(m.group(1), 16), m.group(2)) for m in
                       re.finditer(r"^([0-9a-fA-F]+) <(.+)>:$", out, re.M))
        starts = [a for a, _ in heads]

        def resolve(addr):
            i = bisect.bisect_right(starts, addr) - 1
            return heads[i][1] if i >= 0 else "?"
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
            cur = funcs.setdefault(key, [int(m.group(1), 16), None, []])
            order.append((key, sec))
            continue
        if cur is None or not line.strip():
            continue
        if re.match(r"\s*[0-9a-fA-F]+:\s+(?:ARM64_RELOC_\w+|R_[A-Z0-9_]+)\s", line):
            cur[2].append(norm_reloc(line))
            continue
        m = re.match(r"\s*([0-9a-fA-F]+):\s+(.*)$", line)
        if m:
            cur[2].append(norm_insn(m.group(2), x86, resolve))
            cur.append(int(m.group(1), 16))
    # sizes from the next header in the same section, or the last instruction
    res = {}
    by_sec = collections.defaultdict(list)
    for key, sec in order:
        by_sec[sec].append(key)
    for sec, keys in by_sec.items():
        for i, key in enumerate(keys):
            start = funcs[key][0]
            if i + 1 < len(keys):
                size = funcs[keys[i + 1]][0] - start
            else:
                addrs = funcs[key][3:]
                size = (max(addrs) - start) if addrs else 0
            res[key] = (size, funcs[key][2])
    return res


def contents(path):
    """{qualified section name: bytes} from objdump -s."""
    out = run([objdump(), "-s", path])
    res = collections.OrderedDict()
    cur = None
    for line in out.splitlines():
        m = re.match(r"Contents of section (.+):$", line)
        if m:
            cur = m.group(1)
            res[cur] = bytearray()
            continue
        m = re.match(r"\s*[0-9a-fA-F]+((?: [0-9a-fA-F]{2,8}){1,4})\s", line)
        if cur is not None and m:
            for word in m.group(1).split():
                res[cur] += bytes.fromhex(word)
    return res


def relocations(path):
    """[(short section name, [(offset, type, target)])] in file order."""
    out = run([objdump(), "-r", path])
    res = []
    cur = None
    for line in out.splitlines():
        m = re.match(r"RELOCATION RECORDS FOR \[(.+)\]:$", line)
        if m:
            cur = []
            res.append((m.group(1), cur))
            continue
        m = re.match(r"([0-9a-fA-F]+)\s+(\S+)\s+(\S+)", line)
        if cur is not None and m and not line.startswith("OFFSET"):
            cur.append((int(m.group(1), 16), m.group(2), m.group(3)))
    return res


def split_strings(data):
    return [s.decode("utf-8", "surrogateescape") for s in bytes(data).split(b"\0")]


def compare_functions(base, cand, kind):
    fb, fc = functions(base, kind), functions(cand, kind)
    if set(fb) != set(fc):
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
    sb = [x for x in sections(base) if not SKIP_SECTION_RE.search(x[0])]
    sc = [x for x in sections(cand) if not SKIP_SECTION_RE.search(x[0])]
    if [n for n, _ in sb] != [n for n, _ in sc]:
        problem("SECTION list differs: %s -> %s" % ([n for n, _ in sb], [n for n, _ in sc]))
        return
    for (name, a), (_, b) in zip(sb, sc):
        if SKIP_SECTION_RE.search(name) or STRING_SECTION_RE.search(name):
            continue
        # A linked ELF library merges .rodata.str* into .rodata, so its size
        # follows the file name strings.  The object file compares strings.
        if kind == "lib" and name == ".rodata":
            continue
        if a != b:
            problem("SECTION %s: size %d -> %d" % (name, a, b))


def masked(data, relocs, ptr):
    data = bytearray(data)
    for off, rtype, _ in relocs:
        width = ptr if re.search(r"UNSIGNED|_64$|ABS64|R_X86_64_64|R_AARCH64_ABS64", rtype) else 4
        data[off:off + width] = b"\0" * width
    return data


def compare_data(base, cand):
    cb, cc = contents(base), contents(cand)
    rb, rc = relocations(base), relocations(cand)
    ptr = 8
    seen = collections.Counter()

    def relocs_for(rlist, qualified, index):
        short = qualified.split(",")[-1]
        hits = [r for s, r in rlist if s == short or s == qualified]
        return hits[index] if index < len(hits) else []

    for name in cb:
        short = name.split(",")[-1]
        idx = seen[short]
        seen[short] += 1
        if name not in cc or SKIP_SECTION_RE.search(short):
            continue
        if STRING_SECTION_RE.search(name):
            sb = collections.Counter(s for s in split_strings(cb[name]) if not FILE_NAME_RE.search(s))
            sc = collections.Counter(s for s in split_strings(cc[name]) if not FILE_NAME_RE.search(s))
            if sb != sc:
                problem("STRINGS %s: only in base %s, only in candidate %s"
                        % (name, sorted((sb - sc).elements())[:5], sorted((sc - sb).elements())[:5]))
            continue
        if re.search(r"__text$|^\.text", short):
            continue
        xb, xc = relocs_for(rb, name, idx), relocs_for(rc, name, idx)
        nb = [(o, t, STRSYM_RE.sub("STR", v)) for o, t, v in xb]
        nc = [(o, t, STRSYM_RE.sub("STR", v)) for o, t, v in xc]
        nb = [(o, t, re.sub(r"[+-]0x[0-9a-fA-F]+$", "", v)) for o, t, v in nb]
        nc = [(o, t, re.sub(r"[+-]0x[0-9a-fA-F]+$", "", v)) for o, t, v in nc]
        if sorted(nb) != sorted(nc):
            problem("RELOCS %s: %s" % (name, sorted(set(nb) ^ set(nc))[:5]))
        if masked(cb[name], xb, ptr) != masked(cc[name], xc, ptr):
            problem("DATA %s: contents differ outside relocated slots" % name)


def compare_bin(base, cand, kind):
    n = compare_functions(base, cand, kind)
    compare_sections(base, cand, kind)
    if kind == "obj":
        compare_data(base, cand)
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
