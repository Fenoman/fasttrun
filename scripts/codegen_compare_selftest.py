#!/usr/bin/env python3
"""Self-test of codegen_compare.py on the current platform.

Builds tiny C programs with the system compiler in a temporary directory and
checks that compare-bin passes on identical builds and fails on each change
the comparison must see.

Cases:
  registers     normalized instructions keep register numbers (x10 vs x11)
  empty         objdump output without functions is a failure, not a pass
  const-section data of the first of two sections with the same short name
  object-data   a changed initializer of a variable in a linked library (ELF)
  data-target   which global a function reads, in the object and the library
  static-target which of two static variables a function reads (ELF addend)
  identical     two builds of the same source pass

Usage: codegen_compare_selftest.py   (exit 0 when every case behaves)
"""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
sys.path.insert(0, HERE)
import codegen_compare  # noqa: E402

CC = os.environ.get("CC", "cc")
COMPARE = [sys.executable, "-I", os.path.join(HERE, "codegen_compare.py")]
failures = []


def check(name, ok, detail=""):
    print("%-14s %s%s" % (name, "ok" if ok else "FAILED", (": " + detail) if detail and not ok else ""))
    if not ok:
        failures.append(name)


def build(tmp, tag, source):
    src = os.path.join(tmp, tag + ".c")
    with open(src, "w") as f:
        f.write(source)
    obj = os.path.join(tmp, tag + ".o")
    lib = os.path.join(tmp, tag + (".dylib" if sys.platform == "darwin" else ".so"))
    subprocess.run([CC, "-O2", "-fPIC", "-c", src, "-o", obj], check=True)
    flags = ["-dynamiclib", "-undefined", "dynamic_lookup"] if sys.platform == "darwin" else ["-shared"]
    subprocess.run([CC] + flags + [obj, "-o", lib], check=True)
    return obj, lib


def compare(a, b, kind, env=None, strict=False):
    args = ["compare-bin", "--strict", a, b, kind] if strict else ["compare-bin", a, b, kind]
    res = subprocess.run(COMPARE + args, capture_output=True, text=True,
                         env=env)
    return res.returncode, (res.stdout.strip().splitlines() or [""])[-1]


TABLE = """
static const int tab[8] = {%s};
int pick(int i) { return tab[i & 7]; }
static const char *const names[] = {"alpha", "beta", "gamma"};
const char *name_of(int i) { return names[i %% 3]; }
"""

OBJECT = """
int value = %d;
int get_value(void) { return value; }
"""

TARGET = """
int first_global = 1;
int second_global = 2;
int read_global(void) { return %s; }
"""

STATIC = """
static int s_first, s_second;
void bump(int v) { s_first += v; s_second -= v; }
int read_static(void) { return %s; }
"""


def main():
    x86 = os.uname().machine in ("x86_64", "amd64")
    a, b = ("mov %r10d,%r12d", "mov %r11d,%r13d") if x86 else ("mov x10, x20", "mov x11, x21")
    check("registers", codegen_compare.norm_insn(a, x86) != codegen_compare.norm_insn(b, x86),
          "%r and %r normalize the same" % (a, b))

    with tempfile.TemporaryDirectory() as tmp:
        base_obj, base_lib = build(tmp, "t_base", TABLE % "1, 2, 3, 4, 5, 6, 7, 8")
        same_obj, same_lib = build(tmp, "t_same", TABLE % "1, 2, 3, 4, 5, 6, 7, 8")
        mut_obj, mut_lib = build(tmp, "t_mut", TABLE % "9, 2, 3, 4, 5, 6, 7, 8")
        rc, last = compare(base_obj, same_obj, "obj")
        check("identical", rc == 0, last)
        rc, last = compare(base_lib, same_lib, "lib")
        check("identical", rc == 0, last)
        rc, last = compare(base_obj, same_obj, "obj", strict=True)
        check("strict-ident", rc == 0, last)
        rc, last = compare(base_lib, same_lib, "lib", strict=True)
        check("strict-id-lib", rc == 0, last)
        rc, last = compare(base_obj, mut_obj, "obj")
        check("const-section", rc == 1, last)

        env = dict(os.environ, OBJDUMP="true")
        rc, last = compare(base_obj, same_obj, "obj", env)
        check("empty", rc == 1, last)

        o5, l5 = build(tmp, "v5", OBJECT % 5)
        o6, l6 = build(tmp, "v6", OBJECT % 6)
        if sys.platform == "darwin":
            print("%-14s skipped: data of a Mach-O library is compared through its object file"
                  % "object-data")
        else:
            rc, last = compare(l5, l6, "lib")
            check("object-data", rc == 1, last)

        g1o, g1l = build(tmp, "g1", TARGET % "first_global")
        g2o, g2l = build(tmp, "g2", TARGET % "second_global")
        rc, last = compare(g1o, g2o, "obj")
        check("data-target", rc == 1, "object: " + last)
        if sys.platform != "darwin":
            rc, last = compare(g1l, g2l, "lib")
            check("data-target", rc == 1, "library: " + last)

        s1o, _ = build(tmp, "s1", STATIC % "s_first")
        s2o, _ = build(tmp, "s2", STATIC % "s_second")
        rc, last = compare(s1o, s2o, "obj")
        check("static-target", rc == 1, last)

        im1o, im1l = build(tmp, "im1", "int add_imm(int x) { return x + 1; }\n")
        im2o, im2l = build(tmp, "im2", "int add_imm(int x) { return x + 2; }\n")
        rc, last = compare(im1o, im2o, "obj", strict=True)
        check("strict-imm", rc == 1, last)
        rc, last = compare(im1l, im2l, "lib", strict=True)
        check("strict-imm-lib", rc == 1, last)

        fo1o, fo1l = build(tmp, "fo1", "struct S { int a; int b; int c; }; int rd(struct S *p) { return p->b; }\n")
        fo2o, fo2l = build(tmp, "fo2", "struct S { int a; int b; int c; }; int rd(struct S *p) { return p->c; }\n")
        rc, last = compare(fo1o, fo2o, "obj", strict=True)
        check("strict-offset", rc == 1, last)
        rc, last = compare(fo1l, fo2l, "lib", strict=True)
        check("strict-off-lib", rc == 1, last)

        sw1o, sw1l = build(tmp, "sw1", 'const char *pick_s(int c) { return c ? "alpha" : "beta"; }\n')
        sw2o, sw2l = build(tmp, "sw2", 'const char *pick_s(int c) { return c ? "beta" : "alpha"; }\n')
        rc, last = compare(sw1o, sw2o, "obj", strict=True)
        check("strict-str-ord", rc == 1, last)
        rc, last = compare(sw1l, sw2l, "lib", strict=True)
        check("strict-str-lib", rc == 1, last)
        an1o, an1l = build(tmp, "an1", 'const char *anon_s(void) { return "ALPHA"; }\n')
        an2o, an2l = build(tmp, "an2", 'const char *anon_s(void) { return "BRAVO"; }\n')
        rc, last = compare(an1l, an2l, "lib", strict=True)
        check("strict-anon-lib", rc == 1, last)
        rf1o, rf1l = build(tmp, "rf1", 'const char *keep_both(int k) { return k ? "ALPHA" : "BRAVO"; } const char *pick_one(void) { return "ALPHA"; }\n')
        rf2o, rf2l = build(tmp, "rf2", 'const char *keep_both(int k) { return k ? "ALPHA" : "BRAVO"; } const char *pick_one(void) { return "BRAVO"; }\n')
        rc, last = compare(rf1l, rf2l, "lib", strict=True)
        check("strict-ref-swap", rc == 1, last)
        db1o, db1l = build(tmp, "db1", "double keep_d(int k) { return k ? 1.0 : 2.0; } double pick_d(void) { return 1.0; }\n")
        db2o, db2l = build(tmp, "db2", "double keep_d(int k) { return k ? 1.0 : 2.0; } double pick_d(void) { return 2.0; }\n")
        rc, last = compare(db1l, db2l, "lib", strict=True)
        check("strict-dbl-swap", rc == 1, last)
        nz1o, nz1l = build(tmp, "nz1", "double keep_nz(int k) { return k ? 1.0000000000000002 : 2.0000000000000004; } double pick_nz(void) { return 1.0000000000000002; }\n")
        nz2o, nz2l = build(tmp, "nz2", "double keep_nz(int k) { return k ? 1.0000000000000002 : 2.0000000000000004; } double pick_nz(void) { return 2.0000000000000004; }\n")
        rc, last = compare(nz1l, nz2l, "lib", strict=True)
        check("strict-bin-nz", rc == 1, last)
        pa1o, pa1l = build(tmp, "pa1", 'const char *keep_p(int k) { return k ? "ALPHA" : "BRAVO"; } const char *const ptrs[1] = { "ALPHA" };\n')
        pa2o, pa2l = build(tmp, "pa2", 'const char *keep_p(int k) { return k ? "ALPHA" : "BRAVO"; } const char *const ptrs[1] = { "BRAVO" };\n')
        if sys.platform != "darwin":
            rc, last = compare(pa1l, pa2l, "lib", strict=True)
            check("strict-ptr-arr", rc == 1, last)

        # Synthetic outlined-function body mutation check (independent of compiler outliner heuristic)
        fake_dump_1 = """t.o: file format elf64-x86-64
Disassembly of section .text:
0000000000000000 <fn>:
   0: call 0 <_OUTLINED_FUNCTION_0>
   5: ret
0000000000000010 <_OUTLINED_FUNCTION_0>:
  10: mov %edi,%eax
  12: ret
"""
        fake_dump_2 = fake_dump_1.replace("mov %edi,%eax", "mov %esi,%eax")
        sh1 = os.path.join(tmp, "od1.sh")
        sh2 = os.path.join(tmp, "od2.sh")
        fake_bind_1 = """t.o: file format elf64-x86-64
Disassembly of section .text:
0000000000000000 <fn_a>:
   0: call 0 <_OUTLINED_FUNCTION_0>
   5: ret
0000000000000010 <fn_b>:
  10: call 0 <_OUTLINED_FUNCTION_1>
  15: ret
0000000000000020 <_OUTLINED_FUNCTION_0>:
  20: mov %edi,%eax
  22: ret
0000000000000030 <_OUTLINED_FUNCTION_1>:
  30: mov %esi,%eax
  32: ret
"""
        fake_bind_2 = (fake_bind_1
                       .replace("   0: call 0 <_OUTLINED_FUNCTION_0>", "   0: call 0 <__SWAP__>")
                       .replace("  10: call 0 <_OUTLINED_FUNCTION_1>", "  10: call 0 <_OUTLINED_FUNCTION_0>")
                       .replace("   0: call 0 <__SWAP__>", "   0: call 0 <_OUTLINED_FUNCTION_1>"))
        shb1 = os.path.join(tmp, "odb1.sh")
        shb2 = os.path.join(tmp, "odb2.sh")
        for p, body in ((sh1, fake_dump_1), (sh2, fake_dump_2), (shb1, fake_bind_1), (shb2, fake_bind_2)):
            with open(p, "w") as f:
                f.write("#!/bin/sh\ncat <<'EOF'\n" + body + "EOF\n")
            os.chmod(p, 0o755)
        old_od = os.environ.get("OBJDUMP")
        try:
            os.environ["OBJDUMP"] = sh1
            _, ob1 = codegen_compare.functions("dummy", ("elf", True), "obj")
            os.environ["OBJDUMP"] = sh2
            _, ob2 = codegen_compare.functions("dummy", ("elf", True), "obj")
            check("outlined-body", len(ob1) == 1 and len(ob2) == 1 and ob1 != ob2,
                  "outlined body change not detected")
            os.environ["OBJDUMP"] = shb1
            fb1, obb1 = codegen_compare.functions("dummy", ("elf", True), "obj", strict=True)
            os.environ["OBJDUMP"] = shb2
            fb2, obb2 = codegen_compare.functions("dummy", ("elf", True), "obj", strict=True)
            check("outlined-bind", obb1 == obb2 and fb1 != fb2,
                  "swapped outlined call targets not detected")
        finally:
            if old_od is None:
                os.environ.pop("OBJDUMP", None)
            else:
                os.environ["OBJDUMP"] = old_od

    print("selftest: %s" % ("PASS" if not failures else "FAIL (%s)" % ", ".join(failures)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
