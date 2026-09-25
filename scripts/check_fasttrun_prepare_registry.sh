#!/bin/sh
#
# check_fasttrun_prepare_registry.sh - реестр начатых операторов и PREPARE.
#
# В реестр попадают только временные отношения, а PREPARE отказывает
# транзакции, которая трогала временные объекты. Проверяются два пути.
#
# Постоянные цели: UPDATE родителя наследования с 20 постоянными потомками, 21
# цель - больше, чем слотов. Он не должен ни переполнить реестр, ни оставить в
# нем след после PREPARE TRANSACTION. Своего COMMIT-callback у этого пути нет, и
# сброс реестра на PREPARE служит второй линией защиты: проверка падает, только
# если сломаны обе.
#
# Временные цели: кэшированный generic plan UPDATE временного родителя с 20
# временными потомками переполняет реестр в хуке исполнителя, а затем падает в
# проверке прав, до открытия временных отношений. Флаг обращения к временной
# схеме при этом не ставится, и после ROLLBACK TO транзакцию можно подготовить.
# Здесь переполнение не уходит с отменой подтранзакции, и его снимает только
# сброс на PREPARE. Без него переполнение перешло бы в следующую транзакцию и
# спрятало бы статистику, собранную в ней. Проверку прав суперпользователь
# обходит, поэтому этот путь идет от имени отдельной роли, которая сама забирает
# у себя UPDATE.
#
# Основной regression-кластер идет с max_prepared_transactions=0, поэтому здесь
# поднимается свой временный кластер. Он же идет с предзагруженной библиотекой:
# без нее поколоночный учет UPDATE выключен и проверять было бы нечего.
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

/* Постоянные наследники: один UPDATE родителя дает больше целей, чем слотов,
   но постоянные цели реестр не занимают. */
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

CREATE ROLE fasttrun_prepare_user LOGIN;
SQL
)

# Ошибки прав здесь ожидаемы, поэтому psql продолжает после них.
temp_result=$("$BINDIR/psql" -h "$SOCKET_DIR" -p "$PORT" -d "$DBNAME" -U fasttrun_prepare_user \
	-XAtq -v ON_ERROR_STOP=0 2>&1 <<'SQL'
CREATE TEMP TABLE p_probe (id int, note int);
INSERT INTO p_probe SELECT g, 0 FROM generate_series(1, 100000) g;
SELECT fasttrun_analyze('p_probe');

CREATE TEMP TABLE t_parent (id int);
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..20 LOOP
    EXECUTE format('CREATE TEMP TABLE t_c%s () INHERITS (t_parent)', i);
  END LOOP;
END$$;

SET plan_cache_mode = force_generic_plan;
PREPARE t_upd(int) AS UPDATE t_parent SET id = id WHERE id = $1;
EXECUTE t_upd(0);
REVOKE UPDATE ON t_parent FROM CURRENT_USER;
/* REVOKE инвалидировал план: этот EXECUTE строит его заново и падает на
   правах, а новый generic plan остается в кэше. */
EXECUTE t_upd(0);

BEGIN;
SAVEPOINT s;
/* Кэшированный план: переполнение реестра, затем отказ в правах. */
EXECUTE t_upd(0);
ROLLBACK TO s;
PREPARE TRANSACTION 'fasttrun_registry_temp';

/* Следующая транзакция: переполнение не должно в нее перейти. */
SELECT fasttrun_analyze('p_probe');
UPDATE p_probe SET note = 1;
SELECT CASE WHEN ft_rows_est('SELECT * FROM p_probe WHERE id = 1', 'on p_probe') <= 10
            THEN 'TEMP_PASS' ELSE 'TEMP_FAIL' END;

/* Подготовленная транзакция обязана существовать: PREPARE в прерванной
   транзакции становится обычным ROLLBACK, и тогда реестр очистил бы abort. */
SELECT 'PREPARED_OK' FROM pg_prepared_xacts WHERE gid = 'fasttrun_registry_temp';
ROLLBACK PREPARED 'fasttrun_registry_temp';
SQL
)

# Допустимы ровно две ошибки, обе - отказ в правах. Любая другая ошибка, в том
# числе ROLLBACK PREPARED без подготовленной транзакции, - провал.
errors=$(echo "$temp_result" | grep -c 'ERROR:' || true)
denied=$(echo "$temp_result" | grep -c 'ERROR:  permission denied for table t_parent' || true)
echo "permanent targets: $(echo "$result" | grep -c '^PASS$' || true), temp targets: $(echo "$temp_result" | grep -c '^TEMP_PASS$' || true), prepared: $(echo "$temp_result" | grep -c '^PREPARED_OK$' || true), errors: $errors, permission errors: $denied"
if echo "$result" | grep -q '^PASS$' &&
	echo "$temp_result" | grep -q '^TEMP_PASS$' &&
	echo "$temp_result" | grep -q '^PREPARED_OK$' &&
	[ "$errors" -eq 2 ] && [ "$denied" -eq 2 ]; then
	echo "prepare registry reset: PASS"
	exit 0
fi
echo "prepare registry reset: FAIL"
echo "$result"
echo "$temp_result"
exit 1
