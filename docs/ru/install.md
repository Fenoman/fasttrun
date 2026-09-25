# Установка и обновление

> English version: [install.md](../en/install.md). Обзор расширения - в [README](../../README.md).

## Сборка и установка

Нужны исходные заголовки PostgreSQL и PGXS для той версии сервера, на которую
устанавливается расширение. Используйте `pg_config` именно этого сервера:

```bash
PG_CONFIG=/path/to/postgresql/bin/pg_config

"$PG_CONFIG" --version
make PG_CONFIG="$PG_CONFIG"
make install PG_CONFIG="$PG_CONFIG"
```

`make install` должен выполняться пользователем, которому разрешена запись в
каталоги библиотек и расширений PostgreSQL.

Для PostgreSQL 16, 17 и 18 нужны отдельные сборки. При последовательной сборке
из одного каталога выполняйте `make clean` перед сменой `PG_CONFIG`, иначе
`make` может повторно использовать объектный файл от другой основной версии.
Установите собранные файлы на все узлы кластера, которые могут обслуживать базу
или стать основным сервером.

Создайте расширение в каждой базе, где оно будет использоваться:

```sql
SHOW track_counts;

CREATE EXTENSION fasttrun;

SELECT extversion
FROM pg_extension
WHERE extname = 'fasttrun';
```

Версия по умолчанию - `2.5.2`.

Вместо обычного `CREATE EXTENSION` расширение можно установить в отдельную
схему. Схема с API не должна разрешать недоверенным ролям создавать объекты:

```sql
CREATE SCHEMA fasttrun_api;
REVOKE CREATE ON SCHEMA fasttrun_api FROM PUBLIC;
CREATE EXTENSION fasttrun WITH SCHEMA fasttrun_api;
```

При таком варианте квалифицируйте вызовы, например
`fasttrun_api.fasttruncate(...)`. Пример из `examples/` жестко вызывает
`public.fasttruncate()` и требует отдельной адаптации.

Все последующие SQL-примеры предполагают обычную установку в `public`. Если вы
выбрали отдельную схему, добавляйте к функциям префикс `fasttrun_api.` либо
настройте для роли безопасный `search_path`.

Для `fasttruncate()`, `fasttrun_analyze()` и остальных основных функций не
нужны ни рестарт PostgreSQL, ни `shared_preload_libraries`, ни
`session_preload_libraries`. Библиотека загрузится при первом вызове функции.

`shared_preload_libraries` требуется для общего реестра учета и
`fasttrun_prewarm()`, а в фоновых воркерах - и для учета `UPDATE` по колонкам
(см. ["Статистика столбцов"](statistics.md#статистика-столбцов)). Специального положения fasttrun в списке библиотек
нет.

## Права

Все 10 SQL-функций после установки имеют стандартное для PostgreSQL право
`EXECUTE` для `PUBLIC` и работают как `SECURITY INVOKER`. Внутренние проверки
владения и ACL таблицы не выполняются. Это особенно важно в transaction
pooling, если один backend работает под разными ролями через `SET ROLE`.

Настройку прав должен выполнять суперпользователь или владелец функций. При
установке trusted extension (`trusted=true`) непривилегированная роль владеет
объектом расширения, но созданные C-функции принадлежат начальному
суперпользователю кластера. Для рекомендованной отдельной схемы закройте весь
API, а затем выдайте минимальные права нужным ролям. Повторяйте проверку после
`ALTER EXTENSION ... UPDATE`, поскольку новая версия может добавить функцию:

```sql
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA fasttrun_api FROM PUBLIC;

GRANT USAGE ON SCHEMA fasttrun_api TO app_role;
GRANT EXECUTE ON FUNCTION fasttrun_api.fasttruncate(text) TO app_role;
GRANT EXECUTE ON FUNCTION fasttrun_api.fasttrun_analyze(text) TO app_role;
```

Ограничьте `fasttrun_inspect_stats()` отдельно: MCV и гистограммы могут
содержать значения из временных данных.

Для обычной установки в `public` используйте полный блок:

```sql
REVOKE EXECUTE ON FUNCTION public.fasttruncate(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_analyze(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_analyze_bulk(text[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_relstats(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_collect_stats(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_inspect_stats(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_cache_stats() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_hot_temp_tables(integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_prewarm() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.fasttrun_reset_temp_stats() FROM PUBLIC;

GRANT EXECUTE ON FUNCTION public.fasttruncate(text) TO app_role;
GRANT EXECUTE ON FUNCTION public.fasttrun_analyze(text) TO app_role;
```

Остальные права выдайте отдельно ролям приложения, пулера, мониторинга и DBA.
После обновления старой установки проверьте также права у старой функции
`fasttruncate_c(text)`, если она осталась.

`trusted=true` также позволяет роли с правом `CREATE` на базу самостоятельно
установить расширение. Реестр учета общий для всего экземпляра PostgreSQL,
поэтому ACL в одной базе не защищает его от функций, установленных в другой
базе. Если общий учет включен, контролируйте возможность установки fasttrun во
всех базах экземпляра. Не используйте эту подсистему между недоверенными
арендаторами.

## Проверка установки

Следующий пример можно выполнить непосредственно в `psql`:

```sql
CREATE TEMP TABLE fasttrun_demo (
    id bigint,
    category integer
);

INSERT INTO fasttrun_demo
SELECT n, n % 10
FROM generate_series(1, 10000) AS g(n);

SELECT fasttrun_analyze('pg_temp.fasttrun_demo');
SELECT * FROM fasttrun_relstats('pg_temp.fasttrun_demo');

SELECT fasttruncate('pg_temp.fasttrun_demo');

SELECT count(*) = 0 AS table_is_empty
FROM fasttrun_demo;

DROP TABLE fasttrun_demo;
```

Последняя проверка должна вернуть `true`.

## Обновление и удаление

Поддерживается обновление с версий `2.0`, `2.1`, `2.1.1`, `2.1.2`, `2.2.0`,
`2.3.0`, `2.3.1`, `2.3.2`, `2.3.3`, `2.3.4`, `2.4.0`, `2.4.1`, `2.5.0` и `2.5.1`.

Обновление C-библиотеки планируйте как обслуживание серверных процессов.
Новая сборка и SQL-файлы должны быть установлены на всех узлах кластера.

- Если fasttrun загружен через `shared_preload_libraries`, остановите
  PostgreSQL, установите новую сборку и снова запустите сервер.
- При ленивой загрузке или `session_preload_libraries` остановите прием нового
  трафика и закройте все backend, которые могли загрузить старую библиотеку.
  Для пулера это означает полный ресайкл серверных соединений. После этого
  установите новую сборку.

Команды установки:

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

После запуска или открытия нового соединения выполните в каждой базе от имени
владельца расширения или суперпользователя:

```sql
ALTER EXTENSION fasttrun UPDATE TO '2.5.2';
```

При физической репликации установите файлы и на резервные узлы, но выполняйте
`ALTER EXTENSION` только на доступном для записи основном сервере. Изменения
системного каталога попадут на реплики через обычную репликацию.

Это важно для обновления 2.3.4 -> 2.4.0: SQL-миграция добавляет функцию, которой
нет в старой C-библиотеке.

Между версиями 2.4.0, 2.4.1, 2.5.0, 2.5.1 и 2.5.2 SQL-объекты расширения не
меняются, поэтому обновление сводится к замене C-библиотеки и
`ALTER EXTENSION ... UPDATE`.
Для учета износа статистики по колонкам версиям 2.5 **нужна предзагрузка
библиотеки**, основные функции работают и без нее. Если fasttrun уже есть
в `shared_preload_libraries`, менять ничего не нужно. Иначе для клиентских
сеансов добавьте:

```
session_preload_libraries = 'fasttrun'
```

Фоновые процессы этот параметр не загружают, им нужен
`shared_preload_libraries`. Без предзагрузки износ статистики от `UPDATE`
считается по таблице целиком, а не по колонкам, и клиентский сеанс один раз
пишет об этом в серверный лог. Причина в том, что список колонок оператора виден
только при его старте, а хуки ставятся при загрузке библиотеки: если она
грузится по первому вызову, операторы, начавшиеся раньше, восстановить уже
нельзя.

Общий реестр учета `CREATE TEMP TABLE` существует только при
`shared_preload_libraries`, и запись в него по умолчанию выключена. Если нужен
`fasttrun_prewarm()`, задайте `fasttrun.track_temp_creates = on` (раздел ["Учет и предварительное создание таблиц"](tracking.md)).

При переходе на новую основную версию через `pg_upgrade` заранее соберите и
установите fasttrun против нового `pg_config`. Записи расширения в системном
каталоге переносятся вместе с базой. Повторный `CREATE EXTENSION` не требуется.
Файл общего реестра учета в системный каталог не входит и должен считаться
восстанавливаемым служебным состоянием, а не данными, которые обязан переносить
`pg_upgrade`.

Удаление SQL-объектов:

```sql
-- Выполните перед DROP, если нужно удалить и общий реестр учета.
SELECT fasttrun_reset_temp_stats();

DROP EXTENSION fasttrun;
```

`DROP EXTENSION` не выгружает уже загруженную библиотеку из backend. Для
полного отключения удалите fasttrun из `shared_preload_libraries`, если он там
есть, и перезапустите PostgreSQL. При ленивой или сессионной загрузке
переработайте старые соединения. Сам `DROP EXTENSION` и удаление из preload не удаляют
`$PGDATA/pg_stat/fasttrun_temp_stats`. Без предварительного сброса старые
счетчики могут загрузиться при следующем включении расширения.

Вызывайте `fasttrun_reset_temp_stats()` до удаления fasttrun из
`shared_preload_libraries`, пока общий реестр доступен. Если preload уже
отключен, остановите PostgreSQL и удалите файл вручную.

## Совместимость

| PostgreSQL | Сборка | Базовые тесты |
|---|---|---|
| PostgreSQL 16 | да | 14/14 |
| PostgreSQL 17 | да | 14/14 |
| PostgreSQL 18 | да | 14/14 |
