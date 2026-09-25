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
