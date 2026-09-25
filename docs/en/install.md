# Installing and upgrading

> Russian version: [install.md](../ru/install.md). The overview is in [README_EN.md](../../README_EN.md).

## Building and installing

You need the PostgreSQL server headers and PGXS for the server version on which
the extension will be installed. Use `pg_config` from that exact server:

```bash
PG_CONFIG=/path/to/postgresql/bin/pg_config

"$PG_CONFIG" --version
make PG_CONFIG="$PG_CONFIG"
make install PG_CONFIG="$PG_CONFIG"
```

Run `make install` as an operating-system user that can write to PostgreSQL's
library and extension directories.

PostgreSQL 16, 17, and 18 require separate builds. When building sequentially
from the same source tree, run `make clean` before switching `PG_CONFIG`.
Otherwise, `make` may reuse an object file built for another major version.
Install the built files on every cluster node that can serve the database or be
promoted to primary.

Create the extension in every database where it will be used:

```sql
SHOW track_counts;

CREATE EXTENSION fasttrun;

SELECT extversion
FROM pg_extension
WHERE extname = 'fasttrun';
```

The default version is `2.5.2`.

Instead of a plain `CREATE EXTENSION`, you can install the extension into a
dedicated schema. Untrusted roles must not be allowed to create objects in the
schema that contains the API:

```sql
CREATE SCHEMA fasttrun_api;
REVOKE CREATE ON SCHEMA fasttrun_api FROM PUBLIC;
CREATE EXTENSION fasttrun WITH SCHEMA fasttrun_api;
```

With this layout, qualify calls - for example,
`fasttrun_api.fasttruncate(...)`. The example under `examples/` calls
`public.fasttruncate()` explicitly and must be adapted separately.

All SQL examples below assume a regular installation into `public`. If you use
a dedicated schema, prefix function names with `fasttrun_api.` or configure a
safe `search_path` for the role.

The main functions, including `fasttruncate()` and `fasttrun_analyze()`, require
neither a PostgreSQL restart nor `shared_preload_libraries` or
`session_preload_libraries`. The library is loaded on the first function call.

`shared_preload_libraries` is required for the shared tracking registry and
`fasttrun_prewarm()`, and in background workers also for per-column `UPDATE`
accounting (see ["Column statistics"](statistics.md#column-statistics)). fasttrun does not need a special position
in the library list.

## Privileges

After installation, PostgreSQL grants `EXECUTE` on all 10 SQL functions to
`PUBLIC` by default. They run as `SECURITY INVOKER`. The functions do not perform
internal ownership or table ACL checks. This matters especially in transaction
pooling when one backend operates under different roles through `SET ROLE`.

Privileges must be configured by a superuser or by the function owner. When an
unprivileged role installs this trusted extension (`trusted=true`), the role
owns the extension object, but the C functions are owned by the bootstrap
superuser. For the recommended dedicated-schema layout, revoke public access to
the entire API first, then grant the minimum required privileges to application
roles. Repeat this check after
`ALTER EXTENSION ... UPDATE`, because a new version may add a function:

```sql
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA fasttrun_api FROM PUBLIC;

GRANT USAGE ON SCHEMA fasttrun_api TO app_role;
GRANT EXECUTE ON FUNCTION fasttrun_api.fasttruncate(text) TO app_role;
GRANT EXECUTE ON FUNCTION fasttrun_api.fasttrun_analyze(text) TO app_role;
```

Restrict `fasttrun_inspect_stats()` separately: MCVs and histograms can contain
values from temporary data.

For a regular installation into `public`, use the complete block below:

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

Grant the remaining privileges separately to application, pooler, monitoring,
and DBA roles. After upgrading an older installation, also check privileges on
the old `fasttruncate_c(text)` function if it is still present.

`trusted=true` also allows a role with `CREATE` privilege on a database to
install the extension itself. The tracking registry is shared across the entire
PostgreSQL instance, so ACLs in one database do not protect it from functions
installed in another database. If shared tracking is enabled, control who can
install fasttrun in every database in the instance. Do not use this subsystem
across mutually untrusted tenants.

## Verifying the installation

The following example can be run directly in `psql`:

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

The final check must return `true`.

## Upgrading and removing

Upgrades are supported from versions `2.0`, `2.1`, `2.1.1`, `2.1.2`, `2.2.0`,
`2.3.0`, `2.3.1`, `2.3.2`, `2.3.3`, `2.3.4`, `2.4.0`, `2.4.1`, `2.5.0`, and `2.5.1`.

Treat a C-library upgrade as maintenance on the server processes. Install the
new build and SQL files on every cluster node.

- If fasttrun is loaded through `shared_preload_libraries`, stop PostgreSQL,
  install the new build, and start the server again.
- With lazy loading or `session_preload_libraries`, stop accepting new traffic
  and close every backend that may have loaded the old library. For a pooler,
  this means recycling every server connection. Install the new build afterward.

Installation commands:

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

After startup or after opening a new connection, run the following in every
database as the extension owner or a superuser:

```sql
ALTER EXTENSION fasttrun UPDATE TO '2.5.2';
```

With physical replication, install the files on standby nodes too, but run
`ALTER EXTENSION` only on the writable primary. The catalog changes reach the
standbys through normal replication.

This is important for an upgrade from 2.3.4 to 2.4.0: the SQL migration adds a
function that does not exist in the old C library.

The extension's SQL objects do not change between versions 2.4.0, 2.4.1, 2.5.0,
2.5.1, and 2.5.2, so an upgrade between them comes down to replacing the C
library and
running `ALTER EXTENSION ... UPDATE`. For per-column accounting of statistics
wear, versions 2.5 **need the library to be preloaded**, and the main functions
work without it. If fasttrun is already in `shared_preload_libraries`, nothing needs
to change. Otherwise, for client sessions add:

```
session_preload_libraries = 'fasttrun'
```

Background processes do not load that parameter and need
`shared_preload_libraries`. Without a preload, statistics wear from an `UPDATE`
is counted for the whole relation rather than per column, and a client session
writes one line about it to the server log. A statement's column list is visible
only at its start, and the hooks are installed when the library loads: if it
loads on first use, the statements already running cannot be recovered.

The shared `CREATE TEMP TABLE` tracking registry exists only with
`shared_preload_libraries`, and recording into it is off by default. If you
need `fasttrun_prewarm()`, set `fasttrun.track_temp_creates = on` (see ["Tracking and table prewarming"](tracking.md)).

When moving to a new PostgreSQL major version with `pg_upgrade`, build and
install fasttrun against the new `pg_config` in advance. The extension's catalog
entries move with the database. Do not run `CREATE EXTENSION` again. The shared
tracking-registry file is outside the extension catalog and should be treated
as disposable, reconstructible state rather than data that `pg_upgrade` must
transfer.

To remove the SQL objects:

```sql
-- Run this before DROP if the shared tracking registry must be removed too.
SELECT fasttrun_reset_temp_stats();

DROP EXTENSION fasttrun;
```

`DROP EXTENSION` does not unload a library already loaded in a backend. To
disable fasttrun completely, remove it from `shared_preload_libraries` if it is
listed there and restart PostgreSQL. With lazy or session loading, recycle old
connections.
Neither `DROP EXTENSION` nor removing fasttrun from preload deletes
`$PGDATA/pg_stat/fasttrun_temp_stats`. Without an explicit reset, old counters
may be loaded the next time the extension is enabled.

Call `fasttrun_reset_temp_stats()` before removing fasttrun from
`shared_preload_libraries`, while the shared registry is still available. If
preload is already disabled, stop PostgreSQL and remove the file manually.

## Compatibility

| PostgreSQL | Builds | Basic tests |
|---|---|---|
| PostgreSQL 16 | yes | 14/14 |
| PostgreSQL 17 | yes | 14/14 |
| PostgreSQL 18 | yes | 14/14 |
