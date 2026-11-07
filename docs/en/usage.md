# Usage

> Russian version: [usage.md](../ru/usage.md). The overview is in [README_EN.md](../../README_EN.md).

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
[examples/create_temp_table.sql](../../examples/create_temp_table.sql). It is an
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
   has been repaired. A query of `fasttrun_blocked_tables()` after the
   transaction ends checks it: an empty result means there are no blocks. If the
   recovery outcome is uncertain, close the physical connection and let the
   pooler create a new one.
7. `DISCARD TEMP` and `DISCARD ALL` remove temporary tables. This is safe, but it
   eliminates the benefit of reusing them.

After `SQLSTATE 55000`, go directly to
[Recovering from errors](#recovering-from-errors).

Tracking and prewarming are separate, optional features.
Call `fasttrun_prewarm()` when a new physical connection is created, not every
time a connection is handed to a client.

## Core passthrough mode

`fasttrun.passthrough = on` turns the extension's own work off without changing
the calling code: `fasttruncate()` runs `TRUNCATE ONLY`, while
`fasttrun_analyze()`, `fasttrun_analyze_bulk()` and `fasttrun_collect_stats()`
run `ANALYZE`. The hooks and session caches stay idle, and the diagnostic
functions show empty caches. Only the clear marks of
`fasttrun_temp_is_ours()` keep working: the extension's hooks set them on
`TRUNCATE`, and the transaction callbacks keep them. Input checks stay the same: a missing table returns
silently, a permanent or partitioned one raises an error.

The mode is an emergency switch: if the extension gets in the way on a
particular server build, it is turned off by configuration rather than by
changing application code. The value is taken at connection start and does not
change for the rest of the session. A superuser sets it in `postgresql.conf`
(it applies to sessions opened after the configuration is reloaded), with
`ALTER ROLE ... SET`, `ALTER DATABASE ... SET`, or in connection options.

The price is the core's work. In PostgreSQL and PostgresPro a core `TRUNCATE`
or `ANALYZE` of a temp table changes catalog rows and sends shared invalidation
messages to every process of the cluster. Tantor SE 18.6.3 keeps the messages
of `TRUNCATE` and `ANALYZE` of a temp table inside its own process and does not
write them to the shared queue, with `enable_temp_memory_catalog` both `on` and
`off` (measured on 18.6.3 by the shared queue counter: 0 messages per 10 calls
of each command). There the price is catalog writes with
`enable_temp_memory_catalog = off` and a full `ANALYZE` instead of the delta
update. `fasttruncate()` and `fasttrun_analyze()` in the normal mode send none.

## Functions

The public API contains 16 SQL functions.

### Main operations

| Function | Purpose | Shared invalidation messages |
|---|---|---|
| `fasttruncate(text)` | Clears one local temporary heap table, its indexes, TOAST table, and TOAST indexes | None with `zero_sinval_truncate=on`, while with `off`, one shared storage-manager message is sent for every cleared relation |
| `fasttrun_analyze(text)` | Updates local size estimates and, when needed, column statistics | None, except for tables with `CREATE STATISTICS` objects: regular `ANALYZE` handles them |
| `fasttrun_analyze_bulk(VARIADIC text[])` | Runs `fasttrun_analyze` sequentially for an array of tables | As for `fasttrun_analyze` |
| `fasttrun_collect_stats(text)` | Explicitly rebuilds local column statistics | None |

`fasttruncate()` takes an `ExclusiveLock` on the table, its indexes and TOAST.
No other session can read a temporary table, while an `AccessExclusiveLock` on
a relation is WAL-logged by the core for standbys and assigns a transaction ID
for that record. So `fasttruncate()` assigns no transaction ID and writes no
WAL, neither on the fast path nor when it resets the files: the storage of
temporary relations never reaches the WAL. `fasttrun_analyze()` and
`fasttrun_collect_stats()` take an `AccessShareLock` while they run. An open
cursor or active query against the table being cleared causes a regular SQL
error before any files are changed.

The cost of a cleanup grows with the number of relations in the set and with
`temp_buffers`: for every relation the core scans the whole local buffer array,
just as with core `TRUNCATE`.

If the table and its TOAST table are physically empty and the local statistics
of the table and its indexes already match that state, a repeated
`fasttruncate()` skips the file cleanup, the index rebuild, and plan
invalidation. The checks for active use of the table still run. A mismatch in
locators, sizes, statistics, or DML counters, or an unfinished cleanup, sends
the call back to the full path. `reltuples = 0` alone is not enough to skip.
If `COMMIT` could not carry the counter baseline over (counters unavailable),
the fast path restores it for the next refill and `fasttrun_analyze()`. If the
empty state has not yet been reconciled with the cached plans, a full cleanup
with invalidation runs.

For these functions, `NULL` and a missing table are no-ops.
`fasttrun_analyze_bulk()` also skips `NULL` elements in its array. The function
raises an error if it finds a regular table, an unsupported table access method,
a partitioned parent, or an inheritance parent.

The bulk call does not combine local plan invalidations. They run only for
relations whose statistics actually changed. In the worst case, N tables cause
N complete walks over the local plan list.

### Checks for reusing a table

| Function | Purpose |
|---|---|
| `fasttrun_temp_is_ours(rel regclass)` | `true` when every row of a temporary table of this session appeared in the current transaction |
| `fasttrun_temp_same_cols(temp regclass, template regclass)` | `true` when the live columns of the temporary table and the template match |

These functions serve a function that creates a temporary table from a template
and can be called again in the same transaction. It keeps a table holding its
own rows as is, clears any other one, and recreates a table of a different
structure.

The functions run no SQL queries against the catalog: they take the facts
about a table from the session's relation cache, so they also work where the
catalog rows of temporary tables live in process memory (Tantor SE 18 with
`enable_temp_memory_catalog = on`).

`fasttrun_temp_is_ours()` returns `true` when the table:

- was created in the current transaction, including a subtransaction handed to
  its parent by `RELEASE`
- was cleared by core `TRUNCATE` in the current transaction or a live
  subtransaction, in any form: a list, `CASCADE`, through an inheritance parent
  or a partitioned table, `TRUNCATE ONLY`. `ROLLBACK TO` that subtransaction
  removes the mark
- was cleared by `fasttruncate()` in the current transaction, including the
  fast path for an already empty table. The mark is set only after a
  successful cleanup. A subtransaction rollback does not bring the cleared rows
  back, so the mark survives `ROLLBACK TO`, but only while the cleared file
  stays current. If the rolled back subtransaction had a `TRUNCATE` or a
  rewrite of the table before the cleanup, the rollback restores the previous
  file with the old rows, and the answer is `false`.

A rewrite of the table after the cleanup (`ALTER COLUMN TYPE`, `CLUSTER`) keeps
the answer as long as the level of the cleanup has not rolled back since
(section ["Limitations"](limitations.md#limitations)). The answer stays `false` after `DELETE`, a rewrite of
old rows, `ALTER TABLE ADD COLUMN` and the `ON COMMIT DELETE ROWS` cleanup at
the end of the previous transaction: the function does not count them as a
clear in this transaction.

A table blocked after an incomplete cleanup (section
["Recovering from errors"](#recovering-from-errors)) also gives `false`: the
caller goes on to `fasttruncate()`, which repairs it. `NULL` means that no table
with this OID exists any more. The call fails for a permanent table, a
temporary table of another session, an index, a TOAST table, a view, a
sequence, a table that is not heap-AM, and a partitioned parent (check its
partitions instead). An inheritance parent is accepted, and the answer covers
its own rows.

`fasttrun_temp_same_cols()` compares the live columns in `attnum` order: name, type,
`typmod` and collation. Dropped columns are skipped on both sides, so a
template that had columns dropped matches a table created from it with
`LIKE`. Defaults, constraints, indexes, and the identity and generation
properties of a column are not compared. The first argument is checked as for `fasttrun_temp_is_ours()`, the
second may be any regular or partitioned table. The result is `NULL` when
either table is missing.

Neither function checks privileges on the tables, so they can be called from a
`SECURITY DEFINER` function when the tables belong to other roles. They take
an `AccessShareLock` for the duration of the call and are declared
`PARALLEL UNSAFE`. They work the same in `fasttrun.passthrough` mode: there
`fasttruncate()` runs core `TRUNCATE ONLY`, which sets the mark.

```sql
IF fasttrun_temp_same_cols(to_regclass('pg_temp.t_work'), 'dummy_tmp.t_work') IS NOT TRUE THEN
    DROP TABLE IF EXISTS pg_temp.t_work;
    CREATE TEMP TABLE t_work (LIKE dummy_tmp.t_work INCLUDING ALL);
ELSIF NOT fasttrun_temp_is_ours('pg_temp.t_work') THEN
    PERFORM fasttruncate('pg_temp.t_work');
END IF;
```

### Diagnostics

| Function | Purpose |
|---|---|
| `fasttrun_relstats(text)` | Returns the current local `relpages` and `reltuples` values |
| `fasttrun_inspect_stats(text)` | Shows stored statistics candidates in `pg_statistic` format. A row of an index expression has the index OID in `starelid` and the index column number in `staattnum` |
| `fasttrun_cache_stats()` | Shows entry counts and memory used by the local caches |
| `fasttrun_cached_tables()` | Lists the relations the session keeps estimates and statistics for |
| `fasttrun_blocked_tables()` | Lists the tables blocked in the session after an incomplete cleanup |
| `fasttrun_counters()` | Returns the extension's event counters |
| `fasttrun_counters_reset()` | Zeroes the event counters |

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

`fasttrun_cached_tables()` returns a row for every relation the current backend
knows. It opens every relation and reads its size, 70-100 ms for 700 tables:
fine for periodic monitoring, too expensive for a check on every connection
checkout by a pooler. The result fields:

| Field | Meaning |
|---|---|
| `relid`, `relname` | The relation. `relname` is empty if the relation is already dropped |
| `kind` | `table` or `index` |
| `relpages`, `reltuples` | The published size estimate |
| `count_exact` | The row count comes from a full scan or an exact delta, not from a block sample. Empty for an index |
| `delta_state` | The next `fasttrun_analyze()` counts rows from the change counters without a pass over the table. Empty for an index |
| `column_stats` | The number of columns and index expressions with a stored sample |
| `hidden_columns` | How many of them the freshness check hides from the planner right now |

`fasttrun_blocked_tables()` returns `relid` and `relname` of the tables blocked
in the current backend after a `fasttruncate()` error. Check it after the
transaction ends: inside one, a `DROP` hides the table, and a rollback brings it
back with its block. An empty result outside a transaction means the
connection can go back to the pool.

`fasttrun_counters()` returns rows of `counter`, `value`, `scope`. With
`shared_preload_libraries` the values are shared by the instance
(`scope = cluster`): each backend adds its events to its own part of shared
memory at the end of every transaction, and the function sums the parts. Without preloading the values belong to the current
backend (`scope = backend`). Through a pooler in transaction mode only the
first variant is useful. The counters start from zero after a server start and
after `fasttrun_counters_reset()`. A reset zeroes the shared values and the
events of the calling backend. Events of another backend from a transaction
open during the reset enter the sum when that transaction ends.

| Counter | What it counts |
|---|---|
| `truncate_calls` | `fasttruncate()` calls for an existing temporary table |
| `truncate_empty_fast_path` | Calls among them where the table was already empty and no file work was needed |
| `truncate_failed_after_change` | Errors after file changes began, that is new blocks |
| `analyze_calls` | `fasttrun_analyze()` calls, including those from `fasttrun_analyze_bulk()` |
| `analyze_noop` | Calls with no changes since the previous call |
| `analyze_delta` | Calls that counted rows from the change counters |
| `analyze_cold_scan` | Full passes over the table |
| `analyze_cold_block_sample` | Block-sampled passes over a table larger than `fasttrun.max_analyze_pages` |
| `analyze_core_handoff` | Calls handed to regular `ANALYZE` because the table has `CREATE STATISTICS` objects |
| `storage_found_empty` | Tables found empty without the counters knowing, for example after `ON COMMIT DELETE ROWS` |
| `column_collects` | Column statistics collections |
| `column_collects_during_write` | Collections made while the table was being written: their sample is not served to the planner |
| `refresh_deferred` | Rebuilds postponed because of a running write |
| `partial_index_rescans` | `fasttrun_analyze()` calls that recounted the table's partial indexes |
| `plan_invalidations` | Local invalidations of cached plans |
| `stats_served` | Planner requests that got a stored column statistic |
| `stats_hidden` | Planner requests that got default estimates instead of a column statistic fasttrun manages: the sample was stale, taken during a write, or dropped by a cleanup or a `COMMIT` |
| `writer_registry_overflows` | Overflows of the registry of writing statements |
| `commit_carried` | Tables whose counters `COMMIT` carried into the next transaction |
| `tracking_rejected` | Names that did not fit into the `CREATE TEMP TABLE` tracking registry |

`fasttrun.log_min_duration` writes individual slow `fasttruncate()` and
`fasttrun_analyze()` calls to the server log with the path each call took.

The tracking and prewarming functions are described in
[tracking.md](tracking.md#functions).

## Recovering from errors

### Error before files are changed

If an error occurs before physical cleanup begins, PostgreSQL rolls the
operation back normally and the old data remains intact.

### SQLSTATE 55000

If an error occurs after cleanup begins, the old files can no longer be safely
restored. fasttrun blocks the table in the current backend. `SELECT`, DML,
`COPY`, planning, and extension functions for that table return
`SQLSTATE 55000`, including a query that reads it through an inlined SQL
function or through an inheritance parent, a write through a partitioned parent,
and a plan made before the failure, such as the next statement of a SQL
function. A regular `ROLLBACK`, a rollback to a savepoint, and a `TRUNCATE`
through an inheritance parent that is later rolled back do not remove the block
while the table itself exists. If the creation of the table is rolled back, the
extension removes the table's unfinished-operation record together with its
local caches. This also holds when the first fasttrun call is made inside a
nested block.

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
