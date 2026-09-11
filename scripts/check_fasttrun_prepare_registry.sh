#!/bin/sh
#
# check_fasttrun_prepare_registry.sh — реестр начатых операторов и PREPARE.
#
# Реестр переполняется оператором, у которого целей больше, чем слотов, и до
# конца транзакции отвечает осторожно. Граница транзакции обязана его очищать,
# и PREPARE TRANSACTION — тоже: своего COMMIT-callback у этого пути нет.
#
# Основной regression-кластер идёт с max_prepared_transactions=0, поэтому здесь
# поднимается свой временный кластер. Он же идёт с предзагруженной библиотекой:
# без неё поколоночный учёт UPDATE выключен и проверять было бы нечего.
# Скрипт печатает PASS или FAIL и не требует cassert-сборки.
#
set -e
# На macOS postmaster отказывается стартовать при неустановленной локали.
: "${LC_ALL:=C}"
export LC_ALL
PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
WORKDIR=$(mktemp -d)
DATA="$WORKDIR/data"
SOCKET_DIR="$WORKDIR/sock"
LOG="$WORKDIR/log"
DBNAME=fasttrun_prepare_check
PORT=${PGPORT_CHECK:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}

cleanup() {
	"$BINDIR/pg_ctl" -D "$DATA" -w stop >/dev/null 2>&1 || true
	rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$SOCKET_DIR"
"$BINDIR/initdb" -D "$DATA" --no-locale -E UTF8 >/dev/null
"$BINDIR/pg_ctl" -D "$DATA" -l "$LOG" \
	-o "-k $SOCKET_DIR -p $PORT -c listen_addresses='' -c track_counts=on -c max_prepared_transactions=10 -c session_preload_libraries=fasttrun" \
	-w start >/dev/null
"$BINDIR/createdb" -h "$SOCKET_DIR" -p "$PORT" "$DBNAME"

result=$("$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -XAtq -v ON_ERROR_STOP=1 2>&1 <<'SQL'
CREATE EXTENSION fasttrun;

CREATE FUNCTION ft_rows_est(q text, pat text) RETURNS int LANGUAGE plpgsql AS $ft$
DECLARE ln text; est int := NULL;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN ' || q LOOP
    IF ln ~ pat THEN est := substring(ln FROM 'rows=(\d+)')::int; EXIT; END IF;
  END LOOP;
  RETURN est;
END$ft$;

CREATE TEMP TABLE p_probe (id int, note int);
INSERT INTO p_probe SELECT g, 0 FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('p_probe');

/* Постоянные наследники: один UPDATE родителя даёт больше целей, чем слотов. */
CREATE SCHEMA p_many;
CREATE TABLE p_many.parent (id int);
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..20 LOOP
    EXECUTE format('CREATE TABLE p_many.c%s () INHERITS (p_many.parent)', i);
  END LOOP;
END$$;

BEGIN;
UPDATE p_many.parent SET id = 1;
PREPARE TRANSACTION 'fasttrun_registry_overflow';

/* После успешного PREPARE реестр обязан быть чистым. */
SELECT fasttrun_analyze('p_probe');
UPDATE p_probe SET note = 1;
SELECT CASE WHEN ft_rows_est('SELECT * FROM p_probe WHERE id = 1', 'on p_probe') <= 10
            THEN 'PASS' ELSE 'FAIL' END;

ROLLBACK PREPARED 'fasttrun_registry_overflow';
SQL
)

echo "$result" | grep -q '^PASS$' && {
	echo "prepare registry reset: PASS"
	exit 0
}
echo "prepare registry reset: FAIL"
echo "$result"
exit 1
