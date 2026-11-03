# Statistics, settings, and memory

> Russian version: [statistics.md](../ru/statistics.md). The overview is in [README_EN.md](../../README_EN.md).

## How statistics are updated

### Table size

`fasttrun_analyze()` publishes values in the current backend's memory for the
planner to use when estimating the size of the table and its indexes.

A repeated call derives the row count from change counters instead of scanning
the table, in later transactions too: at `COMMIT` fasttrun carries over the
counters of the finished transaction, and writes made without any fasttrun call
are counted the same way. The table is scanned again by the first call in
these cases:

- after a core `TRUNCATE` or a change of the table's file
- after a write with `track_counts` off and with counters unavailable
- after a pass over the table inside a statement that writes it: such a pass
  does not see rows the counters already count
- after a `COMMIT` that hid a column's statistics
- after `VACUUM`, which changes the page count and visibility without counters
- after an `ON COMMIT DELETE ROWS` cleanup: the first call of a transaction
  checks whether the storage is empty

A delta to a row count taken by block sampling stays an estimate as well.

The planner divides the published row count by the published page count and
multiplies by the actual page count. A rolled-back write leaves its pages in the
file while the counters forget it. The rollback of a subtransaction or a
transaction counts the pages its own rolled-back write added: growth from rows
committed without an analyze is not among them, and nested rollbacks of one
growth do not add it twice. The planner leaves these pages out of the estimate,
the size backstop does not take them for a refill, and the next analyze measures
the size anew and refreshes the index estimates. After `VACUUM` the row count,
the page count and the visibility come from `VACUUM`, which has just counted them
in the file. A `VACUUM` the core skipped for lack of privileges or one that did
not process the heap (`PROCESS_MAIN false`, `ONLY_DATABASE_STATS`) changes
nothing. A repeated option takes its last value, as in the core.

If the table does not exceed `fasttrun.max_analyze_pages`, the scan produces an
exact row count. For a larger table, fasttrun takes a bounded block sample and
produces an estimate, as regular `ANALYZE` does. A statistics rebuild after
changes and a partial-index recount know the exact row count from the counters
when a full scan produced the original count, and then take a block sample
instead of reading the whole table. A table no larger than the sample target
they read with a full scan at any `max_analyze_pages` but zero: a block sample
would read all of it anyway and also take `ProcArrayLock`, walking every server
process. When the original count was
an estimate, they behave like the first pass and replace it with what they
measured. `max_analyze_pages=0` brings back the full scan for them too. The
sample seed also comes from where `ANALYZE` takes it, a generator the core
seeds in every process separately, so backends choose blocks independently of
each other even with `shared_preload_libraries`.

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
-- Fill the table, run fasttrun_analyze(), and execute the workload queries.
COMMIT;
```

Even in this mode, identical results are not guaranteed because random samples
can differ. Collection is more expensive than with the default settings.
`SET LOCAL` prevents the modified parameters from leaking to the next pooler
client.

fasttrun honors a column's statistics target (`ALTER TABLE ... ALTER COLUMN ...
SET STATISTICS`) the way `ANALYZE` does: `0` turns collection of the column
off, and a value above zero sets the number of MCVs and histogram buckets.
`CREATE TABLE ... (LIKE template)` does not copy this setting, and an
`ALTER TABLE` on every temporary table writes the catalog itself and sends
shared invalidation messages. So a column of a temporary table with no target
of its own takes the target of the same-named column of the same-named table
in the `fasttrun.prewarm_schema` schema. The setting is made once on the
template:

```sql
ALTER TABLE dummy_tmp.my_table ALTER COLUMN payload SET STATISTICS 0;
```

`fasttrun.template_stattarget = off` turns this behavior off.

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

After any DML since the last collect, `fasttrun_analyze()` rebuilds the column
statistics, as regular `ANALYZE` does, if automatic collection is enabled,
`sample_rows` is not zero, and the memory limit permits collection. The old
sample does not know the values the DML brought in: a new date, a flag set on
part of the rows, or a column filled where it held only NULLs would be
estimated at one row. A call with no changes leaves the sample alone. A rebuild
costs more than a delta and resets the table's cached plans, see
["Cost of a rebuild"](#cost-of-a-rebuild).

A call inside a statement that writes the table postpones the rebuild:
fasttrun never gives the planner a sample taken during a write. The next call
after the write ends, in the same transaction or after `COMMIT`, collects
again. Without new changes a column stays on default estimates in a few cases,
listed in [limitations](limitations.md).

Between DML and the next analyze call the planner uses the collected statistics
within a tolerance, as the core does between two runs of `ANALYZE`, and beyond
the tolerance fasttrun hides them, so that the planner does not use a
distribution known to be outdated. For inserted and updated rows the tolerance
is `fasttrun.stats_refresh_threshold` multiplied by `1 - d`, where `d` is the
fraction of distinct values among the rows, but no less than the smaller of the
threshold and 5%. A column with few values gets almost the whole threshold, a
near-unique one at most 5%: an insert or an update can pile rows onto one value
and turn a "rare" value into a frequent one. A delete cannot do that, the share
of any remaining value grows by no more than `1 / (1 - deleted share)`, so
deleted rows are compared with the threshold itself. Changes add up across
`COMMIT`, so several small transactions in a row hide the statistics just as
one would. A write that goes past both the executor and `COPY` does not reach
the transaction journal, see [limitations](limitations.md).

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
explicit call builds column statistics from an exact row count and a row sample
drawn from the whole heap where a block sample cannot be trusted: values are
grouped by page, or a mass `DELETE` left a sparse heap in which a block sample
can miss every live block. It does not publish the table size for the planner,
`fasttrun_analyze()` does that. The price is a full table scan on every such
call.

A regular full `ANALYZE` hands statistics management for the whole table back
to PostgreSQL core. `ANALYZE table (col1, ...)` hands back only the listed
columns, while local statistics for the remaining columns stay in place.
`VACUUM` without `ANALYZE` does not change column statistics. `VACUUM FULL`,
`CLUSTER` and a column type change rewrite the table and hide old local
statistics until the next collection.

DDL that changes neither the rows nor the meaning of the remaining columns keeps
the column statistics, as the core keeps `pg_statistic`: `CREATE INDEX` and
`ALTER TABLE` with storage parameters, `ADD COLUMN`, `DROP COLUMN`, constraints,
defaults, `NOT NULL`, statistics targets and triggers. `DROP COLUMN` removes
only its own column's statistics. `ADD COLUMN` with a `GENERATED ... STORED`
expression or a volatile default rewrites the table with the same rows, and
the statistics move to the new file. After an index build, including one for a
`UNIQUE` or `PRIMARY KEY` inside `ADD COLUMN`, the row and page counts come from
the core, which has just counted them exactly, and the other indexes keep their
own. `REINDEX` builds no new index: the table keeps its own counts until the
next analyze, as it would without `REINDEX`. A column type change
and any other `ALTER TABLE` hide the table's statistics until the next collect.

### Index expressions

fasttrun collects the statistics of index expressions from the same row sample,
as core `ANALYZE` does in `compute_index_stats`: each expression is evaluated
over the sampled rows and analyzed as a column of its type. The planner gets
them by index OID. Freshness is checked against the table's counters, as for
its columns, and an `UPDATE` always counts for an expression, which may read any
column. The expressions run as the table owner in a restricted security
context, as in `ANALYZE`. Expressions of partial indexes stay on default
estimates: the rows of such an index are only part of the table, and the
planner takes no whole-table statistics from it. With
`fasttrun.sample_rows = -1` the sample size accounts for the expression targets
too. With `fasttrun.use_typanalyze = off` expressions are not collected.
`DROP INDEX` removes the expression statistics of its index.

### Extended statistics

`CREATE STATISTICS` objects are built only by core `ANALYZE`, into
`pg_statistic_ext_data`, and the planner has no hook for them. When a table has
such an object, `fasttrun_analyze()` and `fasttrun_analyze_bulk()` run regular
`ANALYZE` for it, and the planner gets the core statistics, multicolumn
estimates included. The price is catalog writes and shared invalidation
messages, as with any `ANALYZE`, and the core sample, usually larger than the
fasttrun one. The `analyze_core_handoff` counter in `fasttrun_counters()` counts
such calls. The core skips a table the caller may not analyze, so the handoff
happens only for those the core lets in: on PG16 the table or database owner,
on PG17 and later the database owner and holders of the `MAINTAIN` privilege
(the table owner has it unless it revoked it from itself). For other callers
fasttrun collects the
column statistics itself, and the extended statistics objects stay as the last
regular `ANALYZE` left them, describing an earlier fill. The planner then
leaves them out and makes multicolumn estimates from the fresh column
statistics until a regular `ANALYZE` builds every object again: the rows of
their data are compared before and after the command.
An explicit `fasttrun_collect_stats()` collects local column statistics for such
a table too, and the objects likewise stop applying until a regular `ANALYZE`.

## Settings

The "user" context means that the parameter can be changed with `SET`. The
"administrator" context means superuser context or a separately granted
`SET ON PARAMETER` privilege. None of these GUC changes requires a restart.

### Main parameters

| Parameter | Default | Range | Context | Purpose |
|---|---:|---:|---|---|
| `fasttrun.auto_collect_stats` | `on` | boolean | user | Collect column statistics inside `fasttrun_analyze()` |
| `fasttrun.sample_rows` | `3000` | `-1..1000000` | user | `0` disables column collection. With `use_typanalyze=on`, `-1` uses the sample size requested by PostgreSQL, while with `off` it uses 3,000 rows |
| `fasttrun.stats_refresh_threshold` | `0.2` | `0..1` | user | Tolerance up to which the planner uses column statistics between DML and the next analyze: for inserted and updated rows scaled by the fraction of distinct values, for deleted rows the threshold itself. It does not govern the rebuild: analyze rebuilds after any DML. `0` hides statistics after any DML |
| `fasttrun.invalidate_threshold` | `0.2` | `0..1` | user | Allowed cumulative size change of the table and its indexes before local plans are invalidated |
| `fasttrun.use_typanalyze` | `on` | boolean | user | Use PostgreSQL's built-in statistics handlers. `off` keeps only basic metrics |
| `fasttrun.zero_sinval_truncate` | `on` | boolean | user | `on` clears files without shared messages, while `off` uses the PostgreSQL core cleanup path |
| `fasttrun.max_analyze_pages` | `100000` | `0..2147483647` pages | user | Above this threshold, the first `fasttrun_analyze()` pass switches to block sampling. A rebuild after changes with an exact count does not depend on this threshold and samples blocks when the table is larger than the sample target. `0` requires a full scan everywhere |
| `fasttrun.max_stats_memory` | `0` | KB | user | Soft memory limit for column statistics. `0` means unlimited |
| `fasttrun.template_stattarget` | `on` | boolean | user | Take a column's statistics target from the same-named template in `fasttrun.prewarm_schema` when the temporary table sets none |
| `fasttrun.log_min_duration` | `-1` | ms | superuser | Write `fasttruncate()` and `fasttrun_analyze()` calls that take at least this long to the server log: the table, the time, the path of the call, and the pages and rows it published. `-1` turns the log off, `0` logs every call |

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

## Cost of a rebuild

Measured on PG16 with a 3,000-row sample, time of one `fasttrun_analyze()`:

| Table | No changes | Delta without rebuild | After DML, with rebuild |
|---|---:|---:|---:|
| 7 columns, 1k rows | ~0.001 ms | 0.008 ms | 0.72 ms |
| 23 columns, 10k rows | ~0.001 ms | 0.007 ms | 7.8 ms |
| 100 columns, 100k rows | ~0.001 ms | 0.3 ms | 43 ms |

A call with no changes does not depend on the cache size. The delta without a
rebuild is the price with `fasttrun.auto_collect_stats = off`. A rebuild grows
with the number of columns and the sample size, not with the table size: rows
are drawn from random blocks. Every rebuild resets the table's cached plans in
this backend and leaves other backends alone.

## Memory

Statistics caches belong to a physical backend and are not shared across
connections. A table of 7 columns, two of them text, takes about 29 KB at
`default_statistics_target = 100`, and 700 such tables about 20 MB. With 300
tables of 300 columns each, the caches can consume tens or hundreds of megabytes
per backend. Wide values and a larger `default_statistics_target` increase
memory use.

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
