# Statistics, settings, and memory

> Russian version: [statistics.md](../ru/statistics.md). The overview is in [README_EN.md](../../README_EN.md).

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
produces an estimate, as regular `ANALYZE` does. The sample seed also comes from
where `ANALYZE` takes it, a generator the core seeds in every process
separately, so backends choose blocks independently of each other even with
`shared_preload_libraries`.

### Column statistics

By default, fasttrun uses the same mechanism as PostgreSQL to analyze ordinary
columns. It calculates most-common values (MCVs), a histogram, correlation,
null fraction, average width, and number of distinct values.

The default sample size is 3,000 rows. This is a compromise between speed and
quality. Regular `ANALYZE` often uses a substantially larger sample.
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
the library then needs a restart too (see ["Upgrading and removing"](install.md#upgrading-and-removing)). The shared
`CREATE TEMP TABLE` tracking registry is created as well, but recording into it
is off by default and is enabled by `fasttrun.track_temp_creates` (see ["Tracking and table prewarming"](tracking.md)). Such processes write the line about the disabled
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

The tracking and prewarming parameters are described in
[tracking.md](tracking.md#parameters).

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
