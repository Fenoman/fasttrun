# Testing

> Russian version: [testing.md](../ru/testing.md). The overview is in [README_EN.md](../../README_EN.md).

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

The `check-perf-smoke` target in that suite times operations with a wall
clock, so it depends on machine speed. For `fasttruncate` it takes two
measurements with separate limits: a narrow table (1M rows, two columns, one
index) at 100 ms and a wide one (1M rows, 50 columns, four indexes, TOAST,
637 MB) at 500 ms. Both come from the `MAX_TRUNC_MS` and `MAX_TRUNC_WIDE_MS`
environment variables. The wide limit is provisional - it is set with room for
slow storage and should be re-derived from a measurement on your own machine.
The script prints both medians. The full statistics refresh after a `DELETE`
and after an `UPDATE` of 500,000 rows is checked by running
`sql/fasttrun_bench.sql` five times: the median of each step must stay within
1000 ms and every single sample within 2000 ms. `MAX_REFRESH_MS` and
`REFRESH_RUNS` set the limit and the number of runs. On a 2-vCPU server the
medians for PostgreSQL 16/17/18 are 260-540 ms. `pg_regress` itself keeps a
soft limit of 2000 ms for these steps and 500 ms for `fasttruncate`, so it does
not depend on machine speed. The target needs Linux, `bpftrace` and `sudo`, so
it does not run on macOS.

It does not replace the full mandatory pre-release run. Releases and nightly
jobs use:

```bash
FT_CASSERT_TARGETS="16:/path/pg16/bin/pg_config:port:log,..." \
  scripts/check_fasttrun_prerelease.sh
```

The mandatory suite includes cassert builds of PostgreSQL 16/17/18, 14/14
`pg_regress`, BRIN stress, cache-initialization faults, the fault matrix, and
checks of memory, planning, local invalidations, the plan reset at backend exit,
the main branch of the tracking test with preloading, the reset of the registry
of running write statements at `PREPARE TRANSACTION`, the background worker log,
and the block sample seed.

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
make check-exit-plan-reset PG_CONFIG=/path/to/pg_config
make check-tracking-preload PG_CONFIG=/path/to/pg_config
make check-block-sample-seed PG_CONFIG=/path/to/pg_config
make check-required-suite
make check-docs
```

Registry-file persistence is checked by
`scripts/check_fasttrun_tracking_persistence.sh`.

`make check-exit-plan-reset` checks in the server log that the plan reset at
backend exit runs before PostgreSQL core removes the temporary tables, that the
callback is registered exactly once from both the executor hook and the utility
hook, and that the log has no errors. The log does not show the effect of the
reset itself: the measurement in the [Performance](internals.md#performance) section confirms it. The check
is part of the mandatory cassert run.

`make check-tracking-preload` starts its own cluster with fasttrun in
`shared_preload_libraries` and runs the `fasttrun_tracking` test through its main
branch with the shared registry. The mandatory cassert run keeps its servers
without preloading, so there the test takes the fallback branch, and only this
script checks the main one. It is part of the mandatory run as well.

`make check-block-sample-seed` starts a cluster with fasttrun in
`shared_preload_libraries`, and two backends analyze the same table larger than
`fasttrun.max_analyze_pages`, one row per page. Only the block choice decides
what such a sample holds, and the histograms of the two backends must differ.
The check is part of the mandatory run.

`make check-zero-sinval` is a separate Linux check that uses `gdb`. It counts
calls that send shared messages and confirms that the main fasttrun path sends
none. The catalog test in `pg_regress` is not a substitute for this send-side
check. It is not part of `scripts/check_fasttrun_prerelease.sh`, so it must be
run separately on a suitable Linux server before a release.
