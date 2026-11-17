# Тестирование

> English version: [testing.md](../en/testing.md). Обзор расширения - в [README](../../README.md).

Базовый набор:

```bash
make installcheck PG_CONFIG=/path/to/pg_config
```

Он содержит 20 наборов `pg_regress`. На поддерживаемых PostgreSQL 16,
PostgreSQL 17 и PostgreSQL 18 ожидается результат 20/20. `fasttrun_fresh`
открывает второй сеанс через contrib-расширение `dblink`, на сервере оно должно
быть установлено.

На Tantor SE 18.6.3 при `enable_temp_memory_catalog = on` строк каталога
временных таблиц SQL не видит, а ядро в этом режиме ломает временные таблицы
при откате подтранзакции и при удалении таблицы с объектом `CREATE STATISTICS`
(раздел ["Ограничения"](limitations.md#ограничения)). Наборы учитывают это так:

- сверки с `pg_class`, `pg_stats` и `pg_statistic` временных таблиц в этом
  режиме дают `true` без проверки, остальное в тех же наборах проверяется как
  обычно
- четыре набора пропускаются целиком, их ожидаемый вывод в этом режиме - файлы
  `_1.out`: `fasttrun_zero_sinval_catalog` (наблюдает строки каталога),
  `fasttrun_stats_ext` (`CREATE STATISTICS`), `fasttrun_fresh_rollback` и
  `fasttrun_outer_rollback` (откат подтранзакции после смены файла или создания
  таблицы на внешнем уровне)

Результат `pg_regress` в этом режиме: 20/20 (16 выполненных наборов и 4
пропущенных по `_1.out`). Отдельно `expected/fasttrun_fresh_1.out` и
`expected/fasttrun_observe_1.out` задают эталонный вывод этих двух наборов при
предзагрузке библиотеки через `shared_preload_libraries` или
`session_preload_libraries`.

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

Обязательный набор включает cassert-сборки PostgreSQL 16/17/18, проверку
документации и контракта набора, 20/20 `pg_regress`, BRIN stress, cache-init
faults, fault matrix, порядок и сохранение файла учета (`tracking_order`,
`tracking_persistence`), атомарность публикации кешей (`publication_atomicity`),
проверки памяти журнала транзакций, планировщика, сброса планов при завершении
backend, основной ветки теста трекинга с предзагрузкой, сброса реестра идущих
пишущих операторов на `PREPARE TRANSACTION`, журнала фонового воркера, зерна
выборки блоков и примера интеграции с `pg_variables` (или его штатного пропуска
с кодом 77 при отсутствии `pg_variables` либо 64-битной смены эпохи XID).

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
make check-cache-init-release PG_CONFIG=/path/to/pg_config
make check-example PG_CONFIG=/path/to/pg_config
make check-codegen-selftest
make check-codegen-identity PG_CONFIG=/path/to/pg_config
make check-required-suite
make check-docs
make check-docs-installed PG_CONFIG=/path/to/pg_config
```

`check-docs-installed` устанавливает документацию штатной целью Makefile во
временный каталог и проверяет локальные ссылки и якоря. Обязательный cassert
набор запускает его для каждой установки PostgreSQL. Цель
`check-codegen-identity` сравнивает машинный код сборки с `${BASE_REF:-HEAD}` и
используется при чистых переносах кода между срезами без функциональных правок.

Сохранение файла учета проверяет
`scripts/check_fasttrun_tracking_persistence.sh`.

Если для того же сервера установлен настоящий `pg_variables`, запустите
`PG_CONFIG=/path/to/pg_config scripts/check_fasttrun_example.sh`. Скрипт проверяет
пример с текущими, вложенными, старыми и помеченными входящими таблицами при
эпохах XID 0 и 2. Эпоха меняется только в собственном остановленном временном
кластере. Также проверяется повтор после неудачной очистки. Без pg_variables
скрипт возвращает код пропуска 77. Если вендорский `pg_resetwal` не поддерживает
`--epoch`, для проверки эпохи 2 скрипт пересоздает свой временный кластер через
64-битный `initdb -x 8589934595` и возвращает 77 только там, где недоступен и он.

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
