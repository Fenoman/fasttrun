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
  running write and may serve a sample taken during it.
- `DISCARD TEMP` lifts the unfinished-cleanup block at once, not at commit. If
  the transaction that ran it rolls back, the table comes back without the
  block. Queries on it either fail reading an index or TOAST page, or see
  correct data or an already empty table: there are no silent wrong results.
  Running `fasttruncate()` again repairs the table. For recovery, run
  `DISCARD TEMP` outside a transaction.
- fasttrun is not a security boundary between connection-pooler clients.
