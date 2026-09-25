# Tracking and table prewarming

> Russian version: [tracking.md](../ru/tracking.md). The overview is in [README_EN.md](../../README_EN.md).

This feature is optional. It is intended only for long-lived connection pools
where pre-creating a small set of frequently used temporary tables is more
efficient than creating every possible table.

## Enabling the feature

Add fasttrun to `shared_preload_libraries`, enable recording, and restart
PostgreSQL:

```ini
shared_preload_libraries = 'fasttrun'
fasttrun.track_temp_creates = on
```

Without `fasttrun.track_temp_creates = on`, the shared registry is created but
new attempts are not recorded in it. `fasttrun_prewarm()` and
`fasttrun_hot_temp_tables()` still read the registry: it keeps the entries
loaded at startup from the file saved at the previous shutdown and the entries
collected while tracking was on. To keep prewarm from creating anything, set
`fasttrun.prewarm_count = 0` or clear the registry with
`fasttrun_reset_temp_stats()`.

If the list already contains other extensions, keep them. fasttrun does not
have to be placed last.

`session_preload_libraries` is not a substitute for
`shared_preload_libraries`: it does not create the shared tracking registry.

## Functions

| Function | Purpose |
|---|---|
| `fasttrun_hot_temp_tables(n)` | Returns the most frequently seen temporary-table names |
| `fasttrun_prewarm()` | Calls an external `create_temp_table(text)` function for the selected names |
| `fasttrun_reset_temp_stats()` | Clears the shared registry and its persisted file |

Without `shared_preload_libraries`, the first function returns an empty set, the
second returns `0`, and the reset does nothing.

## Parameters

| Parameter | Default | Range | Context | Purpose |
|---|---:|---:|---|---|
| `fasttrun.track_temp_creates` | `off` | boolean | administrator | Track matching `CREATE TEMP TABLE` attempts |
| `fasttrun.prewarm_count` | `1000` | `0..8192` | user | Maximum number of helper-function calls. `0` disables prewarming |
| `fasttrun.prewarm_schema` | `dummy_tmp` | schema name | administrator | Schema containing template tables |
| `fasttrun.track_schedule` | `mon-fri 08:00-18:00` | string | administrator | Time windows during which table-creation attempts are tracked |

These parameters are useful only when fasttrun is loaded through
`shared_preload_libraries`.

The "user" and "administrator" contexts are explained in
[statistics.md](statistics.md#settings).

## What is tracked

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

## Schedule

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

## Using prewarm

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

## Registry persistence

During a clean PostgreSQL shutdown, the registry is saved atomically to
`$PGDATA/pg_stat/fasttrun_temp_stats` and loaded on the next startup.

After a crash, the current state is not written. An older successfully saved
copy may be loaded instead. Write errors are recorded in the PostgreSQL log,
and an incomplete temporary file is removed.

The file is not written to WAL, is not replicated, and is not included in
`pg_dump`. `fasttrun_reset_temp_stats()` clears the registry and removes the
file immediately. `ROLLBACK` does not undo this reset.
