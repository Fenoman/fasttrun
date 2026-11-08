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


def compare(a, b, kind, env=None):
    res = subprocess.run(COMPARE + ["compare-bin", a, b, kind], capture_output=True, text=True,
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

    print("selftest: %s" % ("PASS" if not failures else "FAIL (%s)" % ", ".join(failures)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
