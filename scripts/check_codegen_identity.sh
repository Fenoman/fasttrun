#!/usr/bin/env bash
#
# Проверка, что код расширения собирается в ту же программу, что и BASE_REF.
#
# Зачем. fasttrun.c подключает срезы src/NN_*.c одной единицей трансляции.
# Перенос границы между срезами без перестановки кода не должен менять ничего,
# кроме имени файла и номера строки в вызовах errfinish и ExceptionalCondition.
# Скрипт это доказывает для одного профиля сборки (PG_CONFIG).
#
# Что делает.
#   STEP1A   вывод препроцессора базы и кандидата после замены имени файла и
#            номера строки в errfinish/ExceptionalCondition совпадает побайтно
#   STEP1B   то же с маской __FILE__/__LINE__ через -include, без замены
#   STEP1BC  STEP1A для команды биткода, если у профиля with_llvm = yes
#   STEP2    обычные сборки: те же функции, размеры, мнемоники, регистры,
#            секции, данные с точностью до релокаций (codegen_compare.py)
# Команды компилятора берутся из make -n профиля с тем же порядком флагов.
#
# Аргументы: BASE_REF - коммит, с которым сравнивается кандидат.
# Окружение:
#   PG_CONFIG              pg_config профиля, обязательно
#   FT_CODEGEN_OUT         каталог артефактов, по умолчанию новый временный
#   FT_CODEGEN_CAND        каталог кандидата вместо рабочего дерева
#   FT_CODEGEN_NO_OUTLINE  1 - собирать STEP2 с -mno-outline (по умолчанию 1
#                          на Darwin arm64: аутлайнер clang выносит общие
#                          последовательности по-разному при других строках)
#
# Как завершается. Печатает строку профиля и строку на каждый шаг. Код выхода
# 0, если ни один шаг не FAIL, 1 при любом FAIL, 2 при ошибке аргументов.

set -euo pipefail

BASE_REF=${1:-}
if [ -z "$BASE_REF" ] || [ -z "${PG_CONFIG:-}" ]; then
	echo "usage: PG_CONFIG=... $0 BASE_REF" >&2
	exit 2
fi

ROOT=$(git rev-parse --show-toplevel)
OUT=${FT_CODEGEN_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/ft-codegen.XXXXXX")}
mkdir -p "$OUT"
for d in base cand; do
	if [ -e "$OUT/$d" ]; then
		echo "$OUT/$d already exists, pass an empty FT_CODEGEN_OUT" >&2
		exit 2
	fi
	mkdir "$OUT/$d"
done

case "$(uname -sm)" in
	"Darwin arm64") NO_OUTLINE=${FT_CODEGEN_NO_OUTLINE:-1} ;;
	*) NO_OUTLINE=${FT_CODEGEN_NO_OUTLINE:-0} ;;
esac
COMPARE=(python3 -I "$ROOT/scripts/codegen_compare.py")
FAILED=0

git -C "$ROOT" archive "$BASE_REF" | tar -x -C "$OUT/base"
if [ -n "${FT_CODEGEN_CAND:-}" ]; then
	tar -C "$FT_CODEGEN_CAND" --exclude='*.o' --exclude='*.so' --exclude='*.dylib' \
		--exclude='*.bc' --exclude=.git --exclude=.superpowers -cf - . | tar -x -C "$OUT/cand"
	CAND_DESC="dir $FT_CODEGEN_CAND"
else
	(cd "$ROOT" && git ls-files -co --exclude-standard -z -- . ':!:.superpowers' |
		xargs -0 tar -cf -) | tar -x -C "$OUT/cand"
	CAND_DESC="worktree $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet HEAD -- . ':!:.superpowers' || echo +dirty)"
fi

PGXS_GLOBAL="$(dirname "$("$PG_CONFIG" --pgxs)")/../Makefile.global"
WITH_LLVM=no
grep -Eq '^with_llvm[[:space:]]*=[[:space:]]*yes' "$PGXS_GLOBAL" && WITH_LLVM=yes
CASSERT=no
"$PG_CONFIG" --configure | grep -q 'enable-cassert' && CASSERT=yes
CC_WORD=$("$PG_CONFIG" --cc | awk '{print $1}')

echo "PROFILE $(uname -sm) | $("$CC_WORD" --version 2>/dev/null | head -1) | $("$PG_CONFIG" --version)" \
	"| cassert=$CASSERT | llvm=$WITH_LLVM | no_outline=$NO_OUTLINE" \
	"| base $(git -C "$ROOT" rev-parse --short "$BASE_REF") | cand $CAND_DESC | out $OUT"
echo "CFLAGS $("$PG_CONFIG" --cflags)"

cat > "$OUT/mask.h" <<'EOF'
#undef __FILE__
#define __FILE__ "F"
#undef __LINE__
#define __LINE__ 0
EOF

# Команда make -n для цели TARGET в каталоге DIR, без -c и -o TARGET.
preprocess_cmd() {
	local dir=$1 target=$2 line
	line=$(make -n -B -C "$dir" PG_CONFIG="$PG_CONFIG" "$target" 2>/dev/null |
		grep -E -- " -o $target( |$)" | tail -1)
	if [ -z "$line" ]; then
		return 1
	fi
	printf '%s\n' "$line" | sed -e "s/ -o $target / /" -e "s/ -o $target\$//" \
		-e 's/ -c / /' -e 's/ -emit-llvm / /' -e 's/ -flto=thin / /'
}

# Препроцессор: DIR TARGET OUTFILE [extra args]. 0 только при rc 0 и непустом выводе.
run_cpp() {
	local dir=$1 target=$2 out=$3 cmd
	shift 3
	cmd=$(preprocess_cmd "$dir" "$target") || { echo "no make -n command for $target in $dir" > "$out.err"; return 1; }
	echo "$cmd -E -P $*" > "$out.cmd"
	(cd "$dir" && bash -c "$cmd -E -P $*" > "$out" 2> "$out.err") || return 1
	[ -s "$out" ]
}

step1a() {
	local target=$1 tag=$2
	if ! run_cpp "$OUT/base" "$target" "$OUT/base.$tag.i" || ! run_cpp "$OUT/cand" "$target" "$OUT/cand.$tag.i"; then
		echo "$tag FAIL: preprocessor failed, see $OUT/*.$tag.i.err"
		FAILED=1
		return
	fi
	local nb nc
	nb=$("${COMPARE[@]}" normalize-cpp "$OUT/base.$tag.i" "$OUT/base.$tag.norm.i")
	nc=$("${COMPARE[@]}" normalize-cpp "$OUT/cand.$tag.i" "$OUT/cand.$tag.norm.i")
	# -P keeps some blank lines by the layout of the source, they are not tokens.
	sed -i.bak '/^[[:space:]]*$/d' "$OUT/base.$tag.norm.i" "$OUT/cand.$tag.norm.i"
	if [ "$nb" != "$nc" ]; then
		echo "$tag FAIL: base $nb, candidate $nc"
		FAILED=1
	elif cmp -s "$OUT/base.$tag.norm.i" "$OUT/cand.$tag.norm.i"; then
		echo "$tag PASS: $nc, $(wc -l < "$OUT/cand.$tag.norm.i" | tr -d ' ') lines"
	else
		echo "$tag FAIL: normalized outputs differ ($nc)"
		diff "$OUT/base.$tag.norm.i" "$OUT/cand.$tag.norm.i" | head -20 || true
		FAILED=1
	fi
}

step1a fasttrun.o STEP1A

MASK_ARGS="-include $OUT/mask.h"
"$CC_WORD" --version 2>/dev/null | grep -qi clang && MASK_ARGS="$MASK_ARGS -Wno-builtin-macro-redefined"
if run_cpp "$OUT/base" fasttrun.o "$OUT/base.mask.i" $MASK_ARGS &&
	run_cpp "$OUT/cand" fasttrun.o "$OUT/cand.mask.i" $MASK_ARGS; then
	sed '/^[[:space:]]*$/d' "$OUT/base.mask.i" > "$OUT/base.mask.nb.i"
	sed '/^[[:space:]]*$/d' "$OUT/cand.mask.i" > "$OUT/cand.mask.nb.i"
	if cmp -s "$OUT/base.mask.nb.i" "$OUT/cand.mask.nb.i"; then
		echo "STEP1B PASS: $(wc -l < "$OUT/cand.mask.nb.i" | tr -d ' ') lines"
	else
		echo "STEP1B FAIL: masked outputs differ"
		diff "$OUT/base.mask.nb.i" "$OUT/cand.mask.nb.i" | head -20 || true
		FAILED=1
	fi
else
	echo "STEP1B SKIP: preprocessor refused the mask, see $OUT/*.mask.i.err"
fi

if [ "$WITH_LLVM" = yes ]; then
	step1a fasttrun.bc STEP1BC
else
	echo "STEP1BC SKIP: with_llvm = no"
fi

build() {
	local dir=$1
	if [ "$NO_OUTLINE" = 1 ]; then
		make -s -C "$dir" PG_CONFIG="$PG_CONFIG" COPT=-mno-outline > "$dir/build.log" 2>&1
	else
		make -s -C "$dir" PG_CONFIG="$PG_CONFIG" > "$dir/build.log" 2>&1
	fi
}

if build "$OUT/base" && build "$OUT/cand"; then
	lib=fasttrun.so
	[ -f "$OUT/cand/fasttrun.dylib" ] && lib=fasttrun.dylib
	step2_rc=0
	"${COMPARE[@]}" compare-bin "$OUT/base/fasttrun.o" "$OUT/cand/fasttrun.o" obj > "$OUT/step2.obj.txt" || step2_rc=1
	"${COMPARE[@]}" compare-bin "$OUT/base/$lib" "$OUT/cand/$lib" lib > "$OUT/step2.lib.txt" || step2_rc=1
	if [ "$step2_rc" = 0 ]; then
		echo "STEP2 PASS: $(tail -1 "$OUT/step2.obj.txt"); $(tail -1 "$OUT/step2.lib.txt")"
	else
		echo "STEP2 FAIL: see $OUT/step2.obj.txt and $OUT/step2.lib.txt"
		head -20 "$OUT/step2.obj.txt" "$OUT/step2.lib.txt" || true
		FAILED=1
	fi
else
	echo "STEP2 FAIL: build failed, see $OUT/base/build.log and $OUT/cand/build.log"
	FAILED=1
fi

exit $FAILED
