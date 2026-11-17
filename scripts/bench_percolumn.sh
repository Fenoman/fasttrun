#!/bin/sh
#
# bench_percolumn.sh - чередующиеся серии ABBA для двух сборок fasttrun.
#
# Зачем. Сравнивает две заранее собранные библиотеки (HEAD_LIB и PATCH_LIB)
# на наборе микробенчмарков bench_percolumn.sql без перезаписи рабочего
# каталога библиотек PostgreSQL и без засорения дерева исходников.
#
# Что делает.
# - поднимает собственный временный кластер с autovacuum = off и единым
#   абсолютным путем модуля в $CLUSTER_DIR/lib для предзагрузки и SQL-функций
# - в каждой серии выполняет порядок ABBA (head, patch, patch, head) с
#   CHECKPOINT между прогонами и переустановкой файла библиотеки (rm + cp) до
#   старта нового клиентского процесса
# - складывает строки "сборка|серия|прогон|метрика|мс" в BENCH_OUT_CSV
#
# Окружение:
#   PG_CONFIG      путь к pg_config (по умолчанию pg_config)
#   HEAD_LIB       библиотека базовой сборки (/tmp/fasttrun_head.{so,dylib})
#   PATCH_LIB      библиотека проверяемой сборки (/tmp/fasttrun_patch.{so,dylib})
#   SERIES         число четверок ABBA (по умолчанию 3, итого по 6 прогонов)
#   BENCH_OUT_CSV  путь к итоговому CSV (по умолчанию во временном каталоге)
#
set -eu
: "${LC_ALL:=C}"
export LC_ALL
unset PGOPTIONS

DIR=$(cd "$(dirname "$0")" && pwd)
PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
PSQL="$BINDIR/psql"
INITDB="$BINDIR/initdb"
PG_CTL="$BINDIR/pg_ctl"
CREATEDB="$BINDIR/createdb"
SERIES=${SERIES:-3}
DB=${BENCH_DB:-fasttrun_bench_db}
PG_RUN_AS=${PG_RUN_AS:-}

case "$(uname -s)" in
	Darwin) default_dlsuffix=.dylib ;;
	*) default_dlsuffix=.so ;;
esac
DLSUFFIX=${DLSUFFIX:-$default_dlsuffix}
HEAD_LIB=${HEAD_LIB:-"/tmp/fasttrun_head${DLSUFFIX}"}
PATCH_LIB=${PATCH_LIB:-"/tmp/fasttrun_patch${DLSUFFIX}"}

if [ ! -f "$HEAD_LIB" ]; then
	echo "не найдена библиотека HEAD_LIB: $HEAD_LIB" >&2
	exit 1
fi
if [ ! -f "$PATCH_LIB" ]; then
	echo "не найдена библиотека PATCH_LIB: $PATCH_LIB" >&2
	exit 1
fi

DEFAULT_VER=$(sed -n "s/^default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'.*/\\1/p" "$DIR/../fasttrun.control" 2>/dev/null)
DEFAULT_VER=${DEFAULT_VER:-2.7.2}
EXT_SQL="$DIR/../extension/fasttrun--${DEFAULT_VER}.sql"
if [ ! -f "$EXT_SQL" ]; then
	EXT_SQL="$("$PG_CONFIG" --sharedir)/extension/fasttrun--${DEFAULT_VER}.sql"
fi
if [ ! -f "$EXT_SQL" ]; then
	echo "не найден SQL-скрипт установки fasttrun--${DEFAULT_VER}.sql" >&2
	exit 1
fi

CREATED_WORKDIR=0
if [ -n "${BENCH_WORKDIR:-}" ]; then
	WORKDIR=$BENCH_WORKDIR
else
	WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/fasttrun-bench-percolumn.XXXXXX")
	CREATED_WORKDIR=1
fi
OUT_CSV=${BENCH_OUT_CSV:-"$WORKDIR/raw.csv"}
if ! KEEP_WORKDIR=$(python3 - "$WORKDIR" "$OUT_CSV" "$HEAD_LIB" "$PATCH_LIB" "$DIR/bench_percolumn.sql" "$EXT_SQL" "$0" <<'PY'
import errno
import os
import sys

workdir, out_csv = sys.argv[1], sys.argv[2]
head_lib, patch_lib = sys.argv[3], sys.argv[4]
protected_files = sys.argv[3:8]
if (
    not workdir
    or not out_csv
    or out_csv.endswith(os.sep)
    or os.path.basename(out_csv) in ("", ".", "..")
):
    sys.stderr.write(
        f"BENCH_OUT_CSV должен указывать на файл CSV, а не на каталог: {out_csv}\n"
    )
    raise SystemExit(1)

for path in (workdir, out_csv):
    try:
        os.stat(path)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            sys.stderr.write(f"циклическая символьная ссылка в пути: {path}\n")
            raise SystemExit(1)
        if exc.errno == errno.ENOTDIR:
            sys.stderr.write(f"компонент пути не является каталогом: {path}\n")
            raise SystemExit(1)

w_abs = os.path.abspath(workdir)
w_real = os.path.realpath(workdir)
if os.path.exists(w_real) and not os.path.isdir(w_real):
    sys.stderr.write(f"BENCH_WORKDIR не является каталогом: {workdir}\n")
    raise SystemExit(1)

o_abs = os.path.abspath(out_csv)
o_parent_real = os.path.join(
    os.path.realpath(os.path.dirname(o_abs)),
    os.path.basename(o_abs),
)
o_real = os.path.realpath(out_csv)
h_real = os.path.realpath(head_lib)
p_real = os.path.realpath(patch_lib)
protected_reals = {os.path.realpath(p) for p in protected_files}
o_probes = (o_abs, o_parent_real, o_real)

for sub in ("data", "lib", "sock", "runs"):
    for cand in {
        os.path.join(w_abs, sub),
        os.path.join(w_real, sub),
        os.path.realpath(os.path.join(workdir, sub)),
    }:
        for probe in o_probes:
            if probe == cand or probe.startswith(cand + os.sep):
                sys.stderr.write(
                    "BENCH_OUT_CSV не может указывать внутрь удаляемых "
                    "служебных каталогов WORKDIR (data/lib/sock/runs): "
                    f"{out_csv}\n"
                )
                raise SystemExit(1)

if any(p in (w_abs, w_real) for p in o_probes) or os.path.isdir(o_real):
    sys.stderr.write(
        f"BENCH_OUT_CSV должен указывать на файл CSV, а не на каталог: {out_csv}\n"
    )
    raise SystemExit(1)

same_lib = o_real in (h_real, p_real)
if not same_lib and os.path.exists(o_real):
    same_lib = os.path.samefile(o_real, head_lib) or os.path.samefile(
        o_real, patch_lib
    )
if same_lib:
    sys.stderr.write(
        "BENCH_OUT_CSV не может совпадать с HEAD_LIB или PATCH_LIB: "
        f"{out_csv}\n"
    )
    raise SystemExit(1)

same_input = o_real in protected_reals
if not same_input and os.path.exists(o_real):
    same_input = any(
        os.path.exists(p) and os.path.samefile(o_real, p)
        for p in protected_files
    )
if same_input:
    sys.stderr.write(
        f"BENCH_OUT_CSV не может совпадать с входным файлом скрипта: {out_csv}\n"
    )
    raise SystemExit(1)

try:
    os.makedirs(w_real, exist_ok=True)
    os.makedirs(os.path.dirname(o_abs), exist_ok=True)
    os.makedirs(os.path.dirname(o_real), exist_ok=True)
except OSError as exc:
    sys.stderr.write(f"не удалось подготовить каталоги: {exc}\n")
    raise SystemExit(1)

if any(
    p.startswith(b + os.sep)
    for p in o_probes
    for b in (w_abs, w_real)
):
    print("1")
else:
    print("0")
PY
); then
	if [ "$CREATED_WORKDIR" -eq 1 ]; then
		rm -rf "$WORKDIR" || :
	fi
	exit 1
fi
WORKDIR=$(cd "$WORKDIR" && pwd -P)
if [ "$CREATED_WORKDIR" -eq 1 ] && [ -n "$PG_RUN_AS" ]; then
	chmod 755 "$WORKDIR"
fi
if ! CLUSTER_DIR=$(mktemp -d "$WORKDIR/cluster.XXXXXX"); then
	if [ "$CREATED_WORKDIR" -eq 1 ]; then
		rm -rf "$WORKDIR" || :
	fi
	exit 1
fi

run_pg() {
	if [ -n "$PG_RUN_AS" ]; then
		runuser -u "$PG_RUN_AS" -- "$@"
	else
		"$@"
	fi
}

cleanup() {
	rc=$?
	if [ -f "$CLUSTER_DIR/data/postmaster.pid" ]; then
		run_pg "$PG_CTL" -D "$CLUSTER_DIR/data" -m fast -w stop >/dev/null 2>&1 || true
	fi
	if [ "$rc" -eq 0 ]; then
		rm -rf "$CLUSTER_DIR" || :
		if [ "$CREATED_WORKDIR" -eq 1 ] && [ "$KEEP_WORKDIR" -eq 0 ]; then
			rm -rf "$WORKDIR" || :
		fi
	fi
	exit "$rc"
}
trap cleanup EXIT

if ! python3 - "$CLUSTER_DIR" "$OUT_CSV" <<'PY'
import os
import sys

c_real = os.path.realpath(sys.argv[1])
o_real = os.path.realpath(sys.argv[2])
if o_real == c_real or o_real.startswith(c_real + os.sep):
    sys.stderr.write(
        "BENCH_OUT_CSV не может указывать внутрь временного каталога кластера: "
        f"{sys.argv[2]}\n"
    )
    raise SystemExit(1)
PY
then
	rm -rf "$CLUSTER_DIR" || :
	if [ "$CREATED_WORKDIR" -eq 1 ]; then
		rm -rf "$WORKDIR" || :
	fi
	exit 1
fi

mkdir -p "$CLUSTER_DIR/lib" "$CLUSTER_DIR/sock" "$CLUSTER_DIR/runs"
cp "$HEAD_LIB" "$CLUSTER_DIR/lib/fasttrun${DLSUFFIX}"
if [ -n "$PG_RUN_AS" ]; then
	chown -R "$PG_RUN_AS" "$CLUSTER_DIR"
fi
PORT=${PGPORT_BENCH:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

run_pg "$INITDB" -D "$CLUSTER_DIR/data" --no-locale -E UTF8 >/dev/null
cat >>"$CLUSTER_DIR/data/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$CLUSTER_DIR/sock'
port = $PORT
autovacuum = off
checkpoint_timeout = 1h
max_wal_size = 4GB
track_counts = on
CONF
run_pg "$PG_CTL" -D "$CLUSTER_DIR/data" -l "$CLUSTER_DIR/runs/postgres.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$CLUSTER_DIR/sock" -p "$PORT" "$DB"
# Направляем и предзагрузку, и все SQL-точки входа на один и тот же файл
# в $CLUSTER_DIR/lib/fasttrun, чтобы $libdir/fasttrun из fasttrun.control не
# загружал второй образ библиотеки с другим inode.
grep -v '^\\echo' "$EXT_SQL" \
	| sed "s|'MODULE_PATHNAME'|'$CLUSTER_DIR/lib/fasttrun'|g" \
	> "$CLUSTER_DIR/runs/install_fasttrun.sql"
run_pg "$PSQL" -h "$CLUSTER_DIR/sock" -p "$PORT" -d "$DB" -X -qAt \
	-v ON_ERROR_STOP=1 -f "$CLUSTER_DIR/runs/install_fasttrun.sql" >/dev/null
run_pg "$PSQL" -h "$CLUSTER_DIR/sock" -p "$PORT" -d postgres -X -qAt \
	-v ON_ERROR_STOP=1 -c \
	"ALTER DATABASE \"$DB\" SET session_preload_libraries = '$CLUSTER_DIR/lib/fasttrun'" >/dev/null
# Проверяем в свежем сеансе, что и предзагрузка, и SQL-вызов используют один и
# тот же модуль без ошибки повторной регистрации GUC.
run_pg "$PSQL" -h "$CLUSTER_DIR/sock" -p "$PORT" -d "$DB" -X -qAt \
	-v ON_ERROR_STOP=1 -c "SELECT fasttrun_counters_reset()" >/dev/null

: > "$OUT_CSV"
s=1
while [ "$s" -le "$SERIES" ]; do
	leg=0
	for build in head patch patch head; do
		leg=$((leg + 1))
		if [ "$build" = head ]; then
			src_lib=$HEAD_LIB
		else
			src_lib=$PATCH_LIB
		fi
		rm -f "$CLUSTER_DIR/lib/fasttrun${DLSUFFIX}"
		cp "$src_lib" "$CLUSTER_DIR/lib/fasttrun${DLSUFFIX}"
		if [ -n "$PG_RUN_AS" ]; then
			chown "$PG_RUN_AS" "$CLUSTER_DIR/lib/fasttrun${DLSUFFIX}"
		fi
		run_pg "$PSQL" -h "$CLUSTER_DIR/sock" -p "$PORT" -d postgres -X -qAt \
			-v ON_ERROR_STOP=1 -c "CHECKPOINT" >/dev/null
		out="$CLUSTER_DIR/runs/run_${s}_${leg}_${build}.out"
		err="$CLUSTER_DIR/runs/run_${s}_${leg}_${build}.err"
		run_pg "$PSQL" -h "$CLUSTER_DIR/sock" -p "$PORT" -d "$DB" -X -qAt \
			-v ON_ERROR_STOP=1 -f "$DIR/bench_percolumn.sql" >"$out" 2>"$err" || {
			cat "$err" >&2
			echo "  прогон не удался, см. $err" >&2
			exit 1
		}
		grep '|' "$out" \
			| awk -v b="$build" -v s="$s" -v l="$leg" -F'|' '{print b"|"s"|"l"|"$1"|"$2}' >> "$OUT_CSV"
		rm -f "$out" "$err"
		echo "  готово: серия $s (шаг $leg/4), сборка $build" >&2
	done
	s=$((s + 1))
done

run_pg "$PG_CTL" -D "$CLUSTER_DIR/data" -m fast -w stop >/dev/null
rm -rf "$CLUSTER_DIR"
echo "результаты сохранены в $OUT_CSV" >&2
