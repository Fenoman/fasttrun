# Pre-deployment checklist and limitations

> Russian version: [limitations.md](../ru/limitations.md). The overview is in [README_EN.md](../../README_EN.md).

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
- Inheritance statistics are not collected. fasttrun does not build
  `CREATE STATISTICS` objects itself: `fasttrun_analyze()` hands a table with
  such objects to regular `ANALYZE`, with all its catalog writes and shared
  invalidation messages. When a cascade from another object, such as
  `DROP SCHEMA ... CASCADE` of its schema or `DROP TYPE ... CASCADE`, drops
  such an object, the backend keeps the table's cache entry until the table is
  dropped or the next `ANALYZE`: no estimate goes wrong.
- Statistics of partial-index expressions are not collected, and with
  `fasttrun.use_typanalyze = off` neither are the expressions of ordinary
  indexes. Stale catalog statistics for such expressions are hidden, and until a
  regular `ANALYZE` the planner uses default estimates.
- ACL, RLS, and security-barrier behavior of regular `ANALYZE` is not
  reproduced.
- The functions do not check table ownership or the privileges enforced by
  built-in `TRUNCATE` and `ANALYZE`. Restrict access through `EXECUTE` grants.
- Local statistics survive transactions in one backend but disappear when the
  connection ends.
- A write that goes past both the executor and `COPY`, such as a direct table
  AM write by another extension, does not reach the transaction journal. If
  the same transaction made no `fasttrun_analyze()` call for that table,
  `COMMIT` does not carry its counters over, and the next call counts rows from
  the old base. The row count stays old, and only a threefold or larger change
  in table size hides the column statistics. A `fasttrun_analyze()` call after
  the fill in the same transaction carries such a write over too, so call it
  after every fill. fasttrun recognizes an `ON COMMIT DELETE ROWS` cleanup by
  the empty storage on the next access to the table.
- Local plans are invalidated only in the current backend. `ResetPlanCache` is
  not called during normal work: it is called at most once, at backend exit,
  before PostgreSQL core removes the temporary tables.
- The main functions treat a missing table as an allowed case and do nothing.
- Once per-column `UPDATE` accounting has been dropped for a relation, only the
  next collect brings it back: if a trigger was created inside a savepoint and
  the savepoint was rolled back, the account for that relation stays suspended.
- A write that bypasses the executor - a direct heap/table AM API call from
  another extension - never reaches the column list. No core path does that to
  a temporary table.
- The registry of running write statements holds 16 "temporary table,
  subtransaction" pairs at a time, counting nested statements as well
  (triggers, functions, modifying CTEs). The root of a partitioned temporary
  table takes a slot too: an `UPDATE` or `DELETE` without pruning over 16
  partitions gives 17 targets, and so does an inheritance parent with 16
  children. On overflow, every table counts as being written until the
  transaction ends: statistics collected in that transaction after such a
  statement are not served to the planner until a later collect replaces them.
  Permanent tables do not count.
- For an `INSERT` or `COPY` into a partitioned temporary table, the registry
  records the parent named in the statement, while PostgreSQL core picks the
  partition later, when it routes rows. Collecting statistics of that partition
  from inside such a statement, for example from a trigger, does not see the
  running write and may serve a sample taken during it. The first pass over the
  partition from inside such a statement counts rows on the old snapshot, and
  that count carries into later transactions until `fasttruncate()` or
  `TRUNCATE` empties the partition. The `UPDATE` of an
  `INSERT ... ON CONFLICT` through the parent goes straight to the partition,
  and the partition's account of changed columns does not see it.
- In a session without a preloaded library, the first fasttrun call inside a
  modifying CTE does not see the outer statement, and the first pass over the
  table may keep a row count from the old snapshot. Preloading through
  `shared_preload_libraries` or `session_preload_libraries` rules this out.
- A function that turns `track_counts` off inside a writing statement (a
  superuser parameter) stops counting for an already open table, and such a
  write is not carried over `COMMIT`.
- Only changes start a rebuild of column statistics. Without new DML or an
  explicit `fasttrun_collect_stats()`, a column stays on default estimates in
  these cases: after an explicit collect during a write when analyze runs
  before `COMMIT`, and until the transaction ends after the registry of writing
  statements overflows.
- Rolling back a savepoint inside which a column type change or another
  `ALTER TABLE` other than adding a column rewrote the table does not bring the
  previous column statistics back: the columns stay on default estimates until
  the next DML and analyze or an explicit `fasttrun_collect_stats()`. In the
  OmniX corpus only `ADD COLUMN ... GENERATED ... STORED` in
  `imp_readings_integral` rewrites temporary tables, and its statistics are kept
  and come back on rollback.
- fasttrun is not a security boundary between connection-pooler clients.
