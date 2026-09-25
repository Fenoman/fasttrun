# Тестирование

> English version: [testing.md](../en/testing.md). Обзор расширения - в [README](../../README.md).

Базовый набор:

```bash
make installcheck PG_CONFIG=/path/to/pg_config
```

Он содержит 14 наборов `pg_regress`. На поддерживаемых PostgreSQL 16,
PostgreSQL 17 и PostgreSQL 18 ожидается результат 14/14.

Расширенный локальный набор:

```bash
make check-deep-local PG_CONFIG=/path/to/pg_config
```

Цель `check-perf-smoke` из этого набора проверяет время по секундомеру и
потому зависит от скорости машины. Для `fasttruncate` замеров два, и пределы у
них разные: узкая таблица (1 млн строк, две колонки, один индекс) - 100 мс,
широкая (1 млн строк, 50 колонок, четыре индекса, TOAST, 637 МБ) - 500 мс.
Оба задаются переменными окружения `MAX_TRUNC_MS` и `MAX_TRUNC_WIDE_MS`.
Предел по широкой таблице предварительный: он поставлен с запасом под медленное
хранилище, и его стоит пересчитать по замеру на своей машине - скрипт печатает
обе медианы. Полный пересбор статистики после `DELETE` и после `UPDATE` 500 тыс.
строк проверяется прогоном `sql/fasttrun_bench.sql` пять раз: медиана каждого
шага должна уложиться в 1000 мс, а каждый замер - в 2000 мс. Предел и число
прогонов задают `MAX_REFRESH_MS` и `REFRESH_RUNS`. На сервере с 2 vCPU медианы
по PostgreSQL 16/17/18 лежат в 260-540 мс. Сам `pg_regress` держит для этих
шагов мягкий порог 2000 мс, а для `fasttruncate` - 500 мс, чтобы не зависеть от
скорости машины. Цель требует Linux, `bpftrace` и `sudo`, поэтому на macOS не
запускается.

Он не заменяет полный обязательный pre-release прогон. Перед релизом и в
ночном задании используется:

```bash
FT_CASSERT_TARGETS="16:/path/pg16/bin/pg_config:port:log,..." \
  scripts/check_fasttrun_prerelease.sh
```

Обязательный набор включает cassert-сборки PostgreSQL 16/17/18, 14/14
`pg_regress`, BRIN stress, cache-init faults, fault matrix, проверки памяти,
планировщика, локальных инвалидаций, сброса планов при завершении backend,
основной ветки теста трекинга с предзагрузкой, сброса реестра идущих пишущих
операторов на `PREPARE TRANSACTION`, журнала фонового воркера и зерна выборки
блоков.

Fault matrix выполняет 50 сценариев на 25 точках отказа. Короткий
`scripts/check_fasttrun_brin_stress.sh` делает 96 обычных и 48 принудительных
проверок планирования. Полный pre-release режим выполняет 1000 обычных и
200 принудительных проверок BRIN-пути на каждой версии PostgreSQL.

Полезные отдельные проверки:

```bash
make check-parity PG_CONFIG=/path/to/pg_config
make check-no-temp-impact PG_CONFIG=/path/to/pg_config
make check-zero-sinval PG_CONFIG=/path/to/pg_config
make check-fault-matrix PG_CONFIG=/path/to/pg_config
make check-brin-stress PG_CONFIG=/path/to/cassert/pg_config
make check-tracking-persistence PG_CONFIG=/path/to/pg_config
make check-exit-plan-reset PG_CONFIG=/path/to/pg_config
make check-tracking-preload PG_CONFIG=/path/to/pg_config
make check-block-sample-seed PG_CONFIG=/path/to/pg_config
make check-required-suite
make check-docs
```

Сохранение файла учета проверяет
`scripts/check_fasttrun_tracking_persistence.sh`.

`make check-exit-plan-reset` проверяет по журналу сервера, что сброс планов при
завершении backend выполняется раньше удаления временных таблиц ядром, что
колбэк регистрируется ровно один раз и из хука исполнителя, и из хука служебных
команд, и что в журнале нет ошибок. Сам эффект сброса журнал не показывает, его
подтверждает замер из раздела ["Производительность"](internals.md#производительность). Проверка входит в
обязательный cassert-прогон.

`make check-tracking-preload` поднимает свой кластер с fasttrun в
`shared_preload_libraries` и гоняет тест `fasttrun_tracking` по основной ветке с
общим реестром. Обязательный cassert-прогон держит серверы без предзагрузки,
поэтому там тест идет по запасной ветке, а основную проверяет только этот скрипт.
Он тоже входит в обязательный прогон.

`make check-block-sample-seed` поднимает кластер с fasttrun в
`shared_preload_libraries`, и два backend анализируют одинаковую таблицу больше
`fasttrun.max_analyze_pages`, по одной строке на странице. Состав такой выборки
задает только выбор блоков, и гистограммы двух backend обязаны различаться.
Проверка входит в обязательный прогон.

`make check-zero-sinval` - отдельная Linux-проверка через `gdb`. Она считает
вызовы отправки общих сообщений и подтверждает, что основной путь fasttrun их
не создает. Каталожный тест из `pg_regress` не заменяет эту send-side проверку.
Она не входит в `scripts/check_fasttrun_prerelease.sh`, поэтому перед релизом ее
нужно запускать отдельно на подходящем Linux-сервере.
