# Internals and performance

> Russian version: [internals.md](../ru/internals.md). The overview is in [README_EN.md](../../README_EN.md).

## Internals

In standard mode, `fasttruncate()` removes the files of a local temporary table
and creates empty replacements. Local buffers belong only to the current
backend, so other processes do not need to be notified about changes to those
files. Indexes, TOAST, and their metadata are rebuilt in the same backend.

`fasttrun_analyze()` keeps table-size estimates and column statistics in backend
memory. Planner hooks substitute those values for stale catalog values. A global
`ResetPlanCache` is not used during normal work. When necessary, plans are
marked stale only in the current backend through `PlanCacheRelCallback`.
Statistics hooks are installed lazily when the local cache is first created, so
the position of fasttrun in `shared_preload_libraries` does not determine their
order.

At backend exit, PostgreSQL core removes the backend's temporary tables. Every
removed relation produces a relcache message, and every such message walks all
saved plans of the backend. Before that, fasttrun marks the saved plans invalid
with `ResetPlanCache`, so the walk checks only that flag on those plans. Core
does not reset plans of transaction-control statements. Exit callbacks run in
reverse order of registration, and core registers its cleanup when the
transaction that created the temporary namespace commits. So at commit fasttrun
only notes that the temporary namespace exists, and registers its callback in
the first statement after that which goes through the executor hook or the
utility hook. That callback runs before the cleanup. There is no reset if the
temporary namespace is first created in the last transaction of the session,
if no such statement runs between that commit and the exit (for example, only
fastpath calls), or if the library is loaded later and has not seen a commit
yet. The exit then proceeds as without fasttrun.

fasttrun keeps its own journal of touched relations in backend memory,
organized by subtransaction level. The journal also records the level at which
a temporary relation was created. When that level is rolled back, the entries
of the vanishing table and its indexes are freed. Empty files of a table that
PostgreSQL core has not removed yet do not keep its cache in memory. When a
level ends, only the relations marked in that level are walked.

In standard mode, the main functions do not modify `pg_class`, `pg_statistic`,
or the target table's relfilenode. Prewarming uses regular DDL and is outside
this contract.

## Performance

Results depend on table size, number of columns and indexes, storage speed,
PostgreSQL settings, and plan-cache size.

The main expected effects are:

- `fasttruncate()` avoids shared file-change messages in standard mode
- `fasttrun_analyze()` does not write `pg_class` or `pg_statistic`
- repeated analyze calls within one transaction usually do not scan the table
- `invalidate_threshold` reduces unnecessary walks over the local plan cache
- queries without temporary tables keep the same plans and results
- at backend exit, the removal of temporary tables by PostgreSQL core walks the
  plan cache faster

The `check-no-temp-impact` test compares plans, results, replanning, and memory
for queries that do not use temporary tables. Once local caches are active,
entering the planner hook has a small but nonzero cost. In one measured scenario
it was about 0.26 microseconds per planning operation. This is a result from one
specific test, not a universal guarantee.

The plan reset at backend exit was measured on a release build of PostgreSQL 16
(Apple M2 Max). The exit of a backend with 2000 temporary tables and 20,000 saved
plans took 11.4 s instead of 20.1 s when the plans have a generic plan, and
15.5 s instead of 17.4 s without one. The absolute saving grows with the number
of temporary tables and saved plans.

The synthetic test is in
[sql/fasttrun_bench.sql](../../sql/fasttrun_bench.sql). Use real plans and load tests
for your own system.

To compare two builds against each other there is
[scripts/bench_percolumn.sh](../../scripts/bench_percolumn.sh): it swaps in two
pre-built libraries in turn and runs
[scripts/bench_percolumn.sql](../../scripts/bench_percolumn.sql) in alternating
series, collecting the measurements into `scripts/raw.csv`. The alternation
matters: on single runs machine noise easily passes for a difference between
builds that is not there.

Example of the observed effect on one production cluster:

![CPU before fasttrun](../images/prod-cpu-before.png)

![CPU after fasttrun](../images/prod-cpu-after.png)

These graphs show one specific case and do not guarantee the same result on
another system.

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
