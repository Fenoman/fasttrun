# fasttrun

> Russian version: [README.md](README.md)

fasttrun is an extension for PostgreSQL 16, PostgreSQL 17, and PostgreSQL 18.
It speeds up a recurring workflow built around temporary tables:

1. clear the table
2. refill it
3. update planner statistics
4. run the calculation

The extension's main functions are:

- `fasttruncate()`, which clears a local temporary table.
- `fasttrun_analyze()`, which updates planner estimates after the table is filled.

Regular `TRUNCATE` and `ANALYZE` notify other PostgreSQL server processes about
changes to the system catalogs and relation files. Under high concurrency,
processing those notifications can consume a noticeable amount of CPU.
fasttrun performs its main work only in the current server process and, in its
standard mode, sends no shared invalidation messages.

Installing the extension does not make anything faster by itself. The
application must explicitly call fasttrun functions instead of the built-in
commands in a supported use case.

Quick links: [pre-deployment checklist](#pre-deployment-checklist),
[installation](#quick-start),
[using fasttrun with a connection pooler](#using-fasttrun-with-a-connection-pooler),
[recovering from errors](#recovering-from-errors),
[upgrading](#upgrading-and-removing).

## Who it is for

Consider fasttrun when:

- the application repeatedly reuses local temporary tables
- dozens of long-lived server connections operate concurrently
- regular `TRUNCATE` and `ANALYZE` produce measurable overhead
- the application can be changed to call fasttrun functions explicitly
- temporary-table contents do not have to be restored after `ROLLBACK`

A typical use case is a large PL/pgSQL calculation behind a connection pooler.
A PostgreSQL backend - the server process behind one physical connection - lives
for a long time, serves many clients, and gradually accumulates temporary tables
for reuse.

You do not need fasttrun if regular `TRUNCATE` and `ANALYZE` do not cause
noticeable overhead. Measure the problem on your own system first.

## Pre-deployment checklist

**Important:** `fasttruncate()` is not a transactional replacement for
`TRUNCATE`. Once physical cleanup begins, `ROLLBACK` will not restore the old
data.

Before using fasttrun, verify all of the following:

1. The target tables are standalone local temporary heap tables, meaning they
   use PostgreSQL's standard table access method. This is the primary supported
   use case.
2. Partitioning and inheritance do not require child relations to be traversed.
3. Cleanup does not require foreign-key handling, `CASCADE`, TRUNCATE triggers,
   or `RESTART IDENTITY`.
4. `track_counts` is enabled in PostgreSQL:

   ```sql
   SHOW track_counts;
   ```

   The expected value is `on`.

5. The application calls `fasttrun_analyze()` after every refill.
6. The application catches misspelled table names: the main functions treat a
   missing table as a valid no-op and do not raise an error.
7. Memory is budgeted per server connection. By default, the fasttrun statistics
   cache is unlimited.
8. For pooled connections, responsibility for clearing temporary tables before
   the next client uses them is explicitly assigned. The extension itself does
   not know where one pooler client ends and another begins.
9. The application can fall back to regular `TRUNCATE` and `ANALYZE`.
10. `EXECUTE` is granted only to trusted roles. The functions run with the
    caller's privileges, but they do not reproduce all ownership and ACL checks
    performed by built-in `TRUNCATE` and `ANALYZE`.

Do not begin your rollout with tracking and table prewarming. The main functions
work without `shared_preload_libraries`.

## Quick start

### Building and installing

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

The default version is `2.5.1`.

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
accounting (see "Column statistics"). fasttrun does not need a special position
in the library list.

### Privileges

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

### Verifying the installation

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

## Main operating cycle

Clear the table before starting a new calculation. Do not rely only on cleanup
at the end: the client may fail before reaching the final step.

### Plain SQL template

This shows the required operation order, not a ready-to-run query. Replace
`source_data` and the final `SELECT` with objects from your application.

```sql
BEGIN;

CREATE TEMP TABLE IF NOT EXISTS temp_work (
    id bigint,
    value numeric
);

-- Remove data left by the previous use of this backend.
SELECT fasttruncate('pg_temp.temp_work');

INSERT INTO temp_work
SELECT id, amount
FROM source_data
WHERE processing_date = CURRENT_DATE;

-- Required after every refill.
SELECT fasttrun_analyze('pg_temp.temp_work');

SELECT ...
FROM temp_work
JOIN ...;

COMMIT;
```

`ROLLBACK` reverses ordinary transactional changes, but it will not restore data
already removed by a successful `fasttruncate()`.

### PL/pgSQL template

Inside PL/pgSQL, call functions that return `void` with `PERFORM`:

```sql
DO $plpgsql$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS temp_work (
        id bigint,
        value numeric
    );

    PERFORM fasttruncate('pg_temp.temp_work');

    INSERT INTO temp_work
    SELECT id, amount
    FROM source_data
    WHERE processing_date = CURRENT_DATE;

    PERFORM fasttrun_analyze('pg_temp.temp_work');

    -- Continue working with temp_work.
END;
$plpgsql$;
```

`PERFORM` cannot be run as a standalone command in `psql`. In plain SQL, use
`SELECT fasttruncate(...)` and `SELECT fasttrun_analyze(...)`.

A more elaborate `create_temp_table()` example is available in
[examples/create_temp_table.sql](examples/create_temp_table.sql). It is an
integration example, not part of the extension API. It depends on
`pg_variables`, a template schema, and the `_client` role. It interpolates text
names into dynamic SQL without `%I`, so do not pass untrusted values to it, and
adapt the function to your project before using it in production.

`CREATE TEMP TABLE IF NOT EXISTS` does not update the layout of an existing
table. When a template changes after a deployment, check the layout version and
drop and recreate the table when necessary.

## Using fasttrun with a connection pooler

A temporary table belongs to a physical PostgreSQL server process, not to a
logical pooler client. The next client can receive a backend that still contains
tables and data from the previous client.

Follow these rules:

1. Clear each work table at the beginning of every calculation.
2. Always call `fasttrun_analyze()` after filling the table.
3. With transaction pooling, run the entire cycle in one explicit transaction.
4. Statement pooling is not suitable for this workflow.
5. Do not treat fasttrun as a security boundary between clients. The application
   or pooler configuration must enforce cleanup when a backend is handed off.
6. After `SQLSTATE 55000`, do not return the backend to the pool until the table
   has been repaired. If the recovery outcome is uncertain, close the physical
   connection and let the pooler create a new one.
7. `DISCARD TEMP` and `DISCARD ALL` remove temporary tables. This is safe, but it
   eliminates the benefit of reusing them.

After `SQLSTATE 55000`, go directly to
[Recovering from errors](#recovering-from-errors).

Tracking and prewarming are separate, optional features.
Call `fasttrun_prewarm()` when a new physical connection is created, not every
time a connection is handed to a client.

## Functions

The public API contains 10 SQL functions.

### Main operations

| Function | Purpose | Shared invalidation messages |
|---|---|---|
| `fasttruncate(text)` | Clears one local temporary heap table, its indexes, TOAST table, and TOAST indexes | None with `zero_sinval_truncate=on`, while with `off`, one shared storage-manager message is sent for every cleared relation |
| `fasttrun_analyze(text)` | Updates local size estimates and, when needed, column statistics | None |
| `fasttrun_analyze_bulk(VARIADIC text[])` | Runs `fasttrun_analyze` sequentially for an array of tables | None |
| `fasttrun_collect_stats(text)` | Explicitly rebuilds local column statistics | None |

`fasttruncate()` takes an `AccessExclusiveLock`. `fasttrun_analyze()` and
`fasttrun_collect_stats()` take an `AccessShareLock` while they run. An open
cursor or active query against the table being cleared causes a regular SQL
error before any files are changed.

If the table and its TOAST table are physically empty and the local statistics
of the table and its indexes already match that state, a repeated
`fasttruncate()` skips the file cleanup, the index rebuild, and plan
invalidation. The checks for active use of the table still run. A mismatch in
locators, sizes, statistics, or DML counters, or an unfinished cleanup, sends
the call back to the full path. `reltuples = 0` alone is not enough to skip.
After `COMMIT`, the fast path restores the counter baseline for the next refill
and `fasttrun_analyze()`. If the empty state has not yet been reconciled with
the cached plans, a full cleanup with invalidation runs.

For these functions, `NULL` and a missing table are no-ops.
`fasttrun_analyze_bulk()` also skips `NULL` elements in its array. The function
raises an error if it finds a regular table, an unsupported table access method,
a partitioned parent, or an inheritance parent.

The bulk call does not combine local plan invalidations. They run only for
relations whose statistics actually changed. In the worst case, N tables cause
N complete walks over the local plan list.

### Diagnostics

| Function | Purpose |
|---|---|
| `fasttrun_relstats(text)` | Returns the current local `relpages` and `reltuples` values |
| `fasttrun_inspect_stats(text)` | Shows stored statistics candidates in `pg_statistic` format |
| `fasttrun_cache_stats()` | Shows entry counts and memory used by the local caches |

For a missing table, `fasttrun_relstats()` returns `NULL`, while
`fasttrun_inspect_stats()` returns an empty set. An empty set also means that
local column statistics have not been collected yet.

`fasttrun_inspect_stats()` is a diagnostic function. A row in its result does
not guarantee that the planner is using those statistics at that moment: the
freshness check may temporarily hide them.

`fasttrun_cache_stats()` always returns one row with these fields:

- `analyze_entries`
- `column_stats_relid_entries`
- `column_stats_entries`
- `analyze_bytes`
- `column_stats_bytes`
- `total_bytes`

`total_bytes` is the sum of `analyze_bytes` and `column_stats_bytes`. All values
apply only to the current physical backend.

### Tracking and table prewarming

| Function | Purpose |
|---|---|
| `fasttrun_hot_temp_tables(n)` | Returns the most frequently seen temporary-table names |
| `fasttrun_prewarm()` | Calls an external `create_temp_table(text)` function for the selected names |
| `fasttrun_reset_temp_stats()` | Clears the shared registry and its persisted file |

Without `shared_preload_libraries`, the first function returns an empty set, the
second returns `0`, and the reset does nothing.

## How statistics are updated

### Table size

`fasttrun_analyze()` publishes values in the current backend's memory for the
planner to use when estimating the size of the table and its indexes.

Within one transaction, a repeated call usually derives the row count from
change counters instead of scanning the table. This delta state is cleared at
`COMMIT`. The local size estimates and column statistics remain, but the first
call in the next transaction may scan the table again.

If the table does not exceed `fasttrun.max_analyze_pages`, the scan produces an
exact row count. For a larger table, fasttrun takes a bounded block sample and
produces an estimate, as regular `ANALYZE` does.

### Column statistics

By default, fasttrun uses the same mechanism as PostgreSQL to analyze ordinary
columns. It calculates most-common values (MCVs), a histogram, correlation,
null fraction, average width, and number of distinct values.

The default sample size is 3,000 rows. This is a deliberate compromise between
speed and quality. Regular `ANALYZE` often uses a substantially larger sample.
For the closest match to regular `ANALYZE`, use:

```sql
BEGIN;
SET LOCAL fasttrun.sample_rows = -1;
SET LOCAL fasttrun.stats_refresh_threshold = 0;
-- Fill the table, run fasttrun_analyze(), and execute the workload queries.
COMMIT;
```

Even in this mode, identical results are not guaranteed because random samples
can differ. Collection is more expensive than with the default settings.
`SET LOCAL` prevents the modified parameters from leaking to the next pooler
client.

`UPDATE` is accounted for per column. pgstat counts changed rows per relation,
and counted that way, updating a single helper column would age the statistics
of every other column. Wear from an `UPDATE` therefore applies only to the
columns named in `SET`, while `INSERT` and `DELETE` apply to all of them.

The account requires a preload: a statement's target list is visible only at its
start, and the hooks are installed when the library loads.

```
session_preload_libraries = 'fasttrun'
```

Without the preload the per-column account is off entirely, wear from an
`UPDATE` applies to every column, and the server log gets one line about it per
session. A sample taken while the relation was being written is not served to
the planner in either mode until a fresh one replaces it. There is nothing to
compare it with - the scan runs on its own command's snapshot, while the
counters stored beside it already include those writes.

Only client sessions load `session_preload_libraries`. Background workers -
pg_background, pg_cron in background-worker mode, TimescaleDB jobs - never
process it, so in them the per-column account is enabled only by
`shared_preload_libraries`. That requires a PostgreSQL restart, and upgrading
the library then needs a restart too (see "Upgrading and removing"). The shared
`CREATE TEMP TABLE` tracking registry is created as well, but recording into it
is off by default and is enabled by `fasttrun.track_temp_creates` (see "Tracking
and table prewarming"). Such processes write the line about the disabled
account at DEBUG1 instead of LOG: otherwise short-lived workers would write
thousands of identical lines a day.

For a given relation the account is off until the next collect whenever the
column list cannot be trusted: the relation has triggers or a stored generated
column, an `UPDATE` reached an inheritance child through its parent, or the
column's statistics are older than the last collect.

The list comes from the statement's targets, not from the values it actually
changed: an `UPDATE` that matched no row still marks its columns, and from then
on the relation's whole turnover for that period is charged to them.

The price: a bulk `UPDATE` spoils the physical order of rows, so for the
untouched columns the correlation can end up overstated - index access looks
cheaper for them than it is. The number of distinct values, the NULL fraction
and the MCV list stay correct.

After a small amount of DML, cached statistics can remain visible, just as they
do between regular runs of `ANALYZE`. A column's tolerance is
`fasttrun.stats_refresh_threshold` multiplied by `1 - d`, where `d` is the
fraction of distinct values among the rows, but no less than the smaller of the
threshold and 5%. A column with few values gets almost the whole threshold, a
near-unique one at most 5%. With a threshold of `0.2`, the statistics of a
near-unique column are hidden after 5% of changes. When the
threshold itself is reached, the statistics are rebuilt if automatic collection
is enabled, `sample_rows` is not zero, and the memory limit permits collection. Otherwise, fasttrun hides the statistics so that the
planner does not use a distribution known to be outdated. This is not a full
guarantee: the change ratio is measured against the transaction's counters, so
several sub-threshold transactions in a row can rewrite the relation between
them without hiding anything. Across a commit only a change in physical size is
noticed.

`track_counts=on` is required for column-statistics freshness checks and the
fast delta path. With `off`, fasttrun still updates the table-size estimate by
scanning or sampling blocks, but it neither stores nor publishes local column
statistics. On an attempt to collect statistics for a nonempty table, the
extension emits one `WARNING` during the backend's lifetime. After enabling
`track_counts`, call `fasttrun_analyze()` or `fasttrun_collect_stats()` again.

Unless collection is disabled with `sample_rows=0` or stopped by the memory
limit, an explicit `fasttrun_collect_stats()` performs a full table scan. The
`fasttrun.max_analyze_pages` limit applies to scans inside
`fasttrun_analyze()` and does not limit this explicit call. This way the
explicit call gives an exact row count and a row sample drawn from the whole
heap where a block sample cannot be trusted: values are grouped by page, or a
mass `DELETE` left a sparse heap in which a block sample can miss every live
block. The price is a full table scan on every such call.

A regular full `ANALYZE` hands statistics management for the whole table back
to PostgreSQL core. `ANALYZE table (col1, ...)` hands back only the listed
columns, while local statistics for the remaining columns stay in place.
`VACUUM` without `ANALYZE` changes nothing. Commands that rewrite the table hide
old local statistics until the next collection.

## Settings

The "user" context means that the parameter can be changed with `SET`. The
"administrator" context means superuser context or a separately granted
`SET ON PARAMETER` privilege. None of these GUC changes requires a restart.

### Main parameters

| Parameter | Default | Range | Context | Purpose |
|---|---:|---:|---|---|
| `fasttrun.auto_collect_stats` | `on` | boolean | user | Collect column statistics inside `fasttrun_analyze()` |
| `fasttrun.sample_rows` | `3000` | `-1..1000000` | user | `0` disables column collection. With `use_typanalyze=on`, `-1` uses the sample size requested by PostgreSQL, while with `off` it uses 3,000 rows |
| `fasttrun.stats_refresh_threshold` | `0.2` | `0..1` | user | Fraction of DML that triggers an automatic rebuild, and the base of the statistics visibility tolerance. `0` means a rebuild on any DML. `1` disables the DML-triggered rebuild, but statistics past the tolerance are still hidden |
| `fasttrun.invalidate_threshold` | `0.2` | `0..1` | user | Allowed cumulative size change before local plans are invalidated |
| `fasttrun.use_typanalyze` | `on` | boolean | user | Use PostgreSQL's built-in statistics handlers. `off` keeps only basic metrics |
| `fasttrun.zero_sinval_truncate` | `on` | boolean | user | `on` clears files without shared messages, while `off` uses the PostgreSQL core cleanup path |
| `fasttrun.max_analyze_pages` | `100000` | `0..2147483647` pages | user | Above this threshold, `fasttrun_analyze()` switches to block sampling. `0` requires a full scan whenever a scan is needed |
| `fasttrun.max_stats_memory` | `0` | KB | user | Soft memory limit for column statistics. `0` means unlimited |

With an 8 KB page size, `max_analyze_pages=100000` corresponds to approximately
800 MB of heap data.

Keep `fasttrun.zero_sinval_truncate=on` in production. The `off` value is meant
for diagnostics and compatibility testing. It does not make cleanup safer and
re-enables shared invalidation messages. An error inside a PostgreSQL core
critical section in this mode can trigger a `PANIC` that terminates every
server session in the PostgreSQL instance. Automatic process reinitialization
then depends on `restart_after_crash`.

`max_stats_memory` is checked only before the first statistics collection for a
new table. Existing statistics continue to be updated. A current size exactly
equal to the limit is allowed, so the first new table may exceed the limit by
the full size of statistics for all of its columns. The amount of this overshoot
is not bounded in advance. Only `column_stats_bytes` counts toward the limit,
while `analyze_bytes` does not. There is no automatic eviction or LRU. If the
limit stops automatic collection, the backend emits one `WARNING`. An explicit
`fasttrun_collect_stats()` emits a `NOTICE` for every such attempt.

### Tracking and prewarming parameters

| Parameter | Default | Range | Context | Purpose |
|---|---:|---:|---|---|
| `fasttrun.track_temp_creates` | `off` | boolean | administrator | Track matching `CREATE TEMP TABLE` attempts |
| `fasttrun.prewarm_count` | `1000` | `0..8192` | user | Maximum number of helper-function calls. `0` disables prewarming |
| `fasttrun.prewarm_schema` | `dummy_tmp` | schema name | administrator | Schema containing template tables |
| `fasttrun.track_schedule` | `mon-fri 08:00-18:00` | string | administrator | Time windows during which table-creation attempts are tracked |

These parameters are useful only when fasttrun is loaded through
`shared_preload_libraries`.

## Memory

Statistics caches belong to a physical backend and are not shared across
connections. With 300 tables of 300 columns each, they can consume tens or
hundreds of megabytes per backend. Wide values and a larger
`default_statistics_target` increase memory use.

Measure the actual use on your workload:

```sql
SELECT pg_backend_pid(), *
FROM fasttrun_cache_stats();
```

This query shows only the backend on which it runs. With transaction pooling,
different calls may reach different server connections.

Periodically recycling connections releases the local caches. For predictable
degradation, set `fasttrun.max_stats_memory`. Once the limit is reached, new
tables continue to receive size estimates but no local column statistics.
Statistics already in the cache are not evicted.

## Tracking and table prewarming

<details>
<summary>Expand the optional section</summary>


This section is optional. It is intended only for long-lived connection pools
where pre-creating a small set of frequently used temporary tables is more
efficient than creating every possible table.

### Enabling the feature

Add fasttrun to `shared_preload_libraries`, enable recording, and restart
PostgreSQL:

```ini
shared_preload_libraries = 'fasttrun'
fasttrun.track_temp_creates = on
```

Without `fasttrun.track_temp_creates = on`, the shared registry is created but
new attempts are not recorded in it.

If the list already contains other extensions, keep them. fasttrun does not
have to be placed last.

`session_preload_libraries` is not a substitute for
`shared_preload_libraries`: it does not create the shared tracking registry.

### What is tracked

Only attempts to execute commands of this form are tracked:

```sql
CREATE TEMP TABLE temp_work
(LIKE dummy_tmp.temp_work INCLUDING ALL);
```

The schema name in `LIKE` must explicitly match
`fasttrun.prewarm_schema`. Creating a temporary table without such a `LIKE`
clause is not tracked. `INCLUDING ALL` is shown only as an example and is not
required for tracking.

`CREATE TEMP TABLE AS`, `SELECT INTO TEMP`, column definitions without `LIKE`,
an unqualified `LIKE template`, and templates from another schema are not
tracked either. For later prewarming, the temporary table name must match the
template name. The registry itself does not enforce this equality: an attempt
to run `CREATE TEMP TABLE foo (LIKE dummy_tmp.bar)` records the name `foo`, but
prewarming later looks for `dummy_tmp.foo`.

The counter is updated before `CREATE` executes. It therefore counts attempts,
not just successfully created tables. Failed commands,
`CREATE ... IF NOT EXISTS` commands that create nothing, and commands later
rolled back by the transaction also increment it.

The registry:

- is shared across the entire PostgreSQL instance
- holds no more than 8,192 names
- stores only the table name, without a database OID, schema, or user
- combines identical names from different databases
- silently rejects new names after it becomes full

The registry is not an audit or security source. A session that can issue a
matching `CREATE TEMP TABLE` command can increment counters even with failed
attempts and can fill the registry with new names. Use this feature only for
trusted workloads and monitor registry occupancy.

`fasttrun_reset_temp_stats()` clears the entire shared registry, not just data
for the current database.

`fasttrun_hot_temp_tables(0)` returns every entry. Results are sorted by attempt
count, then by the time of the latest attempt, and then by name.

### Schedule

When recording is enabled, attempts are tracked by default on weekdays from
08:00 to 18:00:

```ini
fasttrun.track_schedule = 'mon-fri 08:00-18:00'
```

An empty string enables tracking at all times. Up to eight windows are
supported:

```ini
fasttrun.track_schedule = 'mon-fri 08:00-18:00; sat 10:00-14:00'
```

Days are written as `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, or `sun`. The end
time is exclusive. `24:00` is allowed only as an end time. Split a window that
crosses midnight:

```ini
fasttrun.track_schedule = 'fri 22:00-24:00; sat 00:00-02:00'
```

The schedule uses the server's `log_timezone`. A format error is logged as a
`WARNING`, after which tracking remains enabled at all times.

### Using prewarm

`fasttrun_prewarm()` runs in the current database and connection with the
calling role's privileges. It selects up to `fasttrun.prewarm_count` names.
fasttrun checks only that some relation with the same name exists in
`fasttrun.prewarm_schema`. If no relation exists, the name is skipped. The
user-supplied `create_temp_table()` must verify that the relation is a suitable
template. The function does not replace skipped entries with less popular names.

The extension does not install `create_temp_table(text)`. It executes an
unqualified call:

```sql
SELECT create_temp_table($1);
```

A function with a compatible signature must be visible through the current
`search_path`. Put it in a trusted schema, and do not allow any schema in which
ordinary users can create functions to appear earlier in `search_path`.

The return value is the number of completed `create_temp_table()` calls, not
necessarily the number of newly created tables. Creating a missing table uses
regular DDL and generates standard catalog invalidations. Behavior for an
existing table is defined entirely by the user-supplied helper function.

One `create_temp_table()` error aborts the entire `fasttrun_prewarm()` call.
Previously processed tables may already have been physically cleared by the
user-supplied function, and `ROLLBACK` is not guaranteed to restore their data.

Before enabling prewarm for the first time, set a small
`fasttrun.prewarm_count`, measure the time and number of tables created, and
then increase it gradually. The default value of 1,000 is not a recommendation
for every system.

Example:

```sql
SELECT * FROM fasttrun_hot_temp_tables(20);
SELECT fasttrun_prewarm();
```

### Registry persistence

During a clean PostgreSQL shutdown, the registry is saved atomically to
`$PGDATA/pg_stat/fasttrun_temp_stats` and loaded on the next startup.

After a crash, the current state is not written. An older successfully saved
copy may be loaded instead. Write errors are recorded in the PostgreSQL log,
and an incomplete temporary file is removed.

The file is not written to WAL, is not replicated, and is not included in
`pg_dump`. `fasttrun_reset_temp_stats()` clears the registry and removes the
file immediately. `ROLLBACK` does not undo this reset.

</details>

## Recovering from errors

### Error before files are changed

If an error occurs before physical cleanup begins, PostgreSQL rolls the
operation back normally and the old data remains intact.

### SQLSTATE 55000

If an error occurs after cleanup begins, the old files can no longer be safely
restored. fasttrun blocks the table in the current backend. `SELECT`, DML,
`COPY`, planning, and extension functions for that table return
`SQLSTATE 55000`. A regular `ROLLBACK` or rollback to a savepoint does not remove
the block while the table itself exists. If the creation of the table is rolled
back, the extension removes the table's unfinished-operation record together
with its local caches. This also holds when the first fasttrun call is made
inside a nested block.

First end the failed transaction:

```sql
ROLLBACK;
```

Then choose one recovery method:

1. Run `fasttruncate()` again. After a successful call, the table is available
   and empty and must be refilled.
2. Drop and recreate the table.
3. Run `DISCARD TEMP` or `DISCARD ALL` outside a transaction. This removes every
   temporary table in the current backend.
4. Close the physical connection. For a backend managed by a pooler, this is
   the simplest safe option.

Example of retrying cleanup:

```sql
BEGIN;
SELECT fasttruncate('pg_temp.temp_work');
COMMIT;
```

`DISCARD ALL` also resets prepared plans, session parameters, and other
connection state. If you need to remove only temporary tables, use
`DISCARD TEMP`.

For new operations, the application can temporarily fall back to the built-in
commands:

```sql
TRUNCATE pg_temp.temp_work;
ANALYZE pg_temp.temp_work;
```

This does not repair a table already blocked by fasttrun. Use one of the four
methods above for that table.

### Error in fallback mode

With `fasttrun.zero_sinval_truncate=off`, the extension calls PostgreSQL core's
`RelationTruncate` for every relation. An error in the critical physical-cleanup
section causes a `PANIC`. This terminates every server session in the instance,
not just the current backend. With `restart_after_crash=on`, the postmaster
starts the processes again and performs crash recovery. The pooler sees a mass
disconnect.

With `restart_after_crash=off`, PostgreSQL does not reinitialize the processes
automatically. Restart and recovery are left to the operator or cluster
manager.

Check the PostgreSQL log and the state of the entire instance. The old
connections cannot continue after this event.

## Upgrading and removing

Upgrades are supported from versions `2.0`, `2.1`, `2.1.1`, `2.1.2`, `2.2.0`,
`2.3.0`, `2.3.1`, `2.3.2`, `2.3.3`, `2.3.4`, `2.4.0`, `2.4.1`, and `2.5.0`.

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
ALTER EXTENSION fasttrun UPDATE TO '2.5.1';
```

With physical replication, install the files on standby nodes too, but run
`ALTER EXTENSION` only on the writable primary. The catalog changes reach the
standbys through normal replication.

This is important for an upgrade from 2.3.4 to 2.4.0: the SQL migration adds a
function that does not exist in the old C library.

The extension's SQL objects do not change between versions 2.4.0, 2.4.1, 2.5.0,
and 2.5.1, so an upgrade between them comes down to replacing the C library and
running `ALTER EXTENSION ... UPDATE`. Versions 2.5 also **need the library to be
preloaded**. If fasttrun is already in `shared_preload_libraries`, nothing needs
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
need `fasttrun_prewarm()`, set `fasttrun.track_temp_creates = on` (see
"Tracking and table prewarming").

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

## Limitations

- The primary supported contract is a standalone local temporary heap table in
  the current backend.
- A partitioned parent or inheritance parent is rejected. A leaf partition that
  is itself a heap table, or an inheritance child with no descendants of its
  own, can be accepted, but fasttrun processes only that relation and does not
  traverse the hierarchy.
- Foreign keys are not checked, and `TRUNCATE ... CASCADE` is not performed.
- `BEFORE TRUNCATE` and `AFTER TRUNCATE` triggers are not fired.
- `DELETE` triggers are not fired either.
- SERIAL/IDENTITY sequences are not reset.
- Physical cleanup is not rolled back by the transaction.
- `fasttruncate()` publishes `reltuples=0`. Core `TRUNCATE` uses the special
  value `-1`, so calling `fasttrun_analyze()` after a refill is especially
  important.
- Extended statistics created with `CREATE STATISTICS`, expression-index
  statistics, and inheritance statistics are not collected.
- Stale catalog statistics for index expressions are hidden. Until a regular
  `ANALYZE`, the planner uses default estimates.
- ACL, RLS, and security-barrier behavior of regular `ANALYZE` is not
  reproduced.
- The functions do not check table ownership or the privileges enforced by
  built-in `TRUNCATE` and `ANALYZE`. Restrict access through `EXECUTE` grants.
- Local statistics survive transactions in one backend but disappear when the
  connection ends.
- After DML is committed without `fasttrun_analyze()`, a same-sized change in
  the data distribution can go unnoticed. Call `fasttrun_analyze()` after every
  fill.
- Local plans are invalidated only in the current backend. A global
  `ResetPlanCache` is not called.
- The main functions treat a missing table as an allowed case and do nothing.
- Once per-column `UPDATE` accounting has been dropped for a relation, only the
  next collect brings it back: if a trigger was created inside a savepoint and
  the savepoint was rolled back, the account for that relation stays suspended.
- A write that bypasses the executor - a direct heap/table AM API call from
  another extension - never reaches the column list. No core path does that to
  a temporary table.
- If a statement writes more than 16 temporary tables at once (an `UPDATE` or
  `DELETE` of a partitioned temporary table without partition pruning, an
  inheritance parent with children), every table counts as being written until
  the transaction ends: statistics collected in that transaction after such a
  statement are not served to the planner until a later collect replaces them.
  Permanent tables do not count.
- `DISCARD TEMP` lifts the unfinished-cleanup block at once, not at commit. If
  the transaction that ran it rolls back, the table comes back without the
  block. Queries on it either fail reading an index or TOAST page, or see
  correct data or an already empty table: there are no silent wrong results.
  Running `fasttruncate()` again repairs the table. For recovery, run
  `DISCARD TEMP` outside a transaction.
- fasttrun is not a security boundary between connection-pooler clients.

## Performance

Results depend on table size, number of columns and indexes, storage speed,
PostgreSQL settings, and plan-cache size.

The main expected effects are:

- `fasttruncate()` avoids shared file-change messages in standard mode
- `fasttrun_analyze()` does not write `pg_class` or `pg_statistic`
- repeated analyze calls within one transaction usually do not scan the table
- `invalidate_threshold` reduces unnecessary walks over the local plan cache
- queries without temporary tables keep the same plans and results

The `check-no-temp-impact` test compares plans, results, replanning, and memory
for queries that do not use temporary tables. Once local caches are active,
entering the planner hook has a small but nonzero cost. In one measured scenario
it was about 0.26 microseconds per planning operation. This is a result from one
specific test, not a universal guarantee.

The synthetic test is in
[sql/fasttrun_bench.sql](sql/fasttrun_bench.sql). Use real plans and load tests
for your own system.

To compare two builds against each other there is
[scripts/bench_percolumn.sh](scripts/bench_percolumn.sh): it swaps in two
pre-built libraries in turn and runs
[scripts/bench_percolumn.sql](scripts/bench_percolumn.sql) in alternating
series, collecting the measurements into `scripts/raw.csv`. The alternation
matters: on single runs machine noise easily passes for a difference between
builds that is not there.

Example of the observed effect on one production cluster:

![CPU before fasttrun](docs/images/prod-cpu-before.png)

![CPU after fasttrun](docs/images/prod-cpu-after.png)

These graphs show one specific case and do not guarantee the same result on
another system.

## Testing

<details>
<summary>Expand test commands and implementation details</summary>


Basic suite:

```bash
make installcheck PG_CONFIG=/path/to/pg_config
```

It contains 14 `pg_regress` suites. The expected result is 14/14 on supported
PostgreSQL 16, PostgreSQL 17, and PostgreSQL 18 installations.

Extended local suite:

```bash
make check-deep-local PG_CONFIG=/path/to/pg_config
```

The `check-perf-smoke` target in that suite times `fasttruncate` with a
wall clock, so it depends on machine speed. It takes two measurements with
separate limits: a narrow table (1M rows, two columns, one index) at 100 ms and
a wide one (1M rows, 50 columns, four indexes, TOAST, 637 MB) at 500 ms. Both
come from the `MAX_TRUNC_MS` and `MAX_TRUNC_WIDE_MS` environment variables. The
wide limit is provisional - it is set with room for slow storage and should be
re-derived from a measurement on your own machine. The script prints both
medians. The target needs Linux, `bpftrace` and `sudo`, so it does not run on
macOS.

It does not replace the full mandatory pre-release run. Releases and nightly
jobs use:

```bash
FT_CASSERT_TARGETS="16:/path/pg16/bin/pg_config:port:log,..." \
  scripts/check_fasttrun_prerelease.sh
```

The mandatory suite includes cassert builds of PostgreSQL 16/17/18, 14/14
`pg_regress`, BRIN stress, cache-initialization faults, the fault matrix, and
checks of memory, planning, and local invalidations.

The fault matrix runs 50 cases across 25 failure points. The short
`scripts/check_fasttrun_brin_stress.sh` run performs 96 regular and 48 forced
planning checks. Full pre-release mode performs 1,000 regular and 200 forced
BRIN-path checks on every PostgreSQL version.

Useful individual checks:

```bash
make check-parity PG_CONFIG=/path/to/pg_config
make check-no-temp-impact PG_CONFIG=/path/to/pg_config
make check-zero-sinval PG_CONFIG=/path/to/pg_config
make check-fault-matrix PG_CONFIG=/path/to/pg_config
make check-brin-stress PG_CONFIG=/path/to/cassert/pg_config
make check-tracking-persistence PG_CONFIG=/path/to/pg_config
make check-required-suite
make check-docs
```

Registry-file persistence is checked by
`scripts/check_fasttrun_tracking_persistence.sh`.

`make check-zero-sinval` is a separate Linux check that uses `gdb`. It counts
calls that send shared messages and confirms that the main fasttrun path sends
none. The catalog test in `pg_regress` is not a substitute for this send-side
check. It is not part of `scripts/check_fasttrun_prerelease.sh`, so it must be
run separately on a suitable Linux server before a release.

## Internals

In standard mode, `fasttruncate()` removes the files of a local temporary table
and creates empty replacements. Local buffers belong only to the current
backend, so other processes do not need to be notified about changes to those
files. Indexes, TOAST, and their metadata are rebuilt in the same backend.

`fasttrun_analyze()` keeps table-size estimates and column statistics in backend
memory. Planner hooks substitute those values for stale catalog values. A global
`ResetPlanCache` is not used. When necessary, plans are marked stale only in the
current backend through `PlanCacheRelCallback`. Statistics hooks are installed
lazily when the local cache is first created, so the position of fasttrun in
`shared_preload_libraries` does not determine their order.

fasttrun keeps its own journal of touched relations in backend memory,
organized by subtransaction level. The journal also records the level at which
a temporary relation was created. When that level is rolled back, the entries
of the vanishing table and its indexes are freed. Empty files of a table that
PostgreSQL core has not removed yet do not keep its cache in memory. When a
level ends, only the relations marked in that level are walked.

In standard mode, the main functions do not modify `pg_class`, `pg_statistic`,
or the target table's relfilenode. Prewarming uses regular DDL and is outside
this contract.

</details>

## Compatibility

| PostgreSQL | Builds | Basic tests |
|---|---|---|
| PostgreSQL 16 | yes | 14/14 |
| PostgreSQL 17 | yes | 14/14 |
| PostgreSQL 18 | yes | 14/14 |

## File layout

```
fasttrun.c                    # main C source
fasttrun.control              # extension metadata
extension/                    # installation and upgrade SQL files
Makefile                      # PGXS build
examples/                     # integration examples
scripts/                      # checks and pre-release runner
sql/                          # 14 pg_regress suites
expected/                     # expected pg_regress output
```
