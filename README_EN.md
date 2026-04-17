# fasttrun

PostgreSQL extension for 16 / 17 / 18.

Fast `TRUNCATE` and `ANALYZE` for temporary tables — **without a single invalidation message** in the shared queue (sinval).

## Why

In large PL/pgSQL calculations, temporary tables are used as intermediate buffers: create → fill → compute → clear → fill again. Every `TRUNCATE` in PostgreSQL goes through `smgrtruncate` → `CacheInvalidateSmgr` and puts a message into the shared invalidation queue (sinval). And every `ANALYZE` is even worse: it writes to `pg_class` and `pg_statistic`, generating dozens of sinval messages per table.

When 50-100 backends run this cycle in parallel — thousands of `TRUNCATE` and `ANALYZE` per second — the sinval queue (4096 slots) overflows. Each backend, on any catalog access, is forced to process the entire accumulated queue via `ReceiveSharedInvalidMessages`, with 99% of messages referring to other backends' temp tables and being useless to us. On a server with 60+ cores, this turns into an O(N²) spiral and eats all the CPU.

This fasttrun fork tries to solve both problems:
* **`fasttruncate`** — physical cleanup via direct `unlink` + `smgrcreate`, bypassing `smgrtruncate`. Zero sinval.
* **`fasttrun_analyze`** — row counting and column statistics collection only in the current process memory. Zero catalog writes, zero sinval. Maximum "similarity" to regular `ANALYZE`.

## Functions

| Function | What it does | sinval? |
|---|---|---|
| `fasttruncate(text)` | Clears a temporary table (heap + indexes + toast) | **no** |
| `fasttrun_analyze(text)` | Publishes `relpages/reltuples` + collects column statistics | **no** |
| `fasttrun_collect_stats(text)` | Explicit column statistics collection. In 99% of cases `fasttrun_analyze` is enough — it does the same automatically on the first pass. This function is needed only if auto-collection is disabled (`auto_collect_stats=off`) or you want to force a rebuild | **no** |
| `fasttrun_relstats(text)` | Returns current `relpages/reltuples` from process memory | **no** |
| `fasttrun_inspect_stats(text)` | Returns cached statsTuple in `pg_statistic` format (for debugging) | **no** |
| `fasttrun_hot_temp_tables(n)` | Top-N most frequently created temp tables (requires `shared_preload_libraries`) | **no** |
| `fasttrun_prewarm()` | Creates top-N hot temp tables via `create_temp_table` | **no** |
| `fasttrun_reset_temp_stats()` | Resets temp table creation counters | **no** |

Functions that accept a temporary table name:
* accept a temp table name (schema-qualified is allowed);
* silently return an empty result if the table does not exist;
* raise an error if the table is not temporary.

## How fasttruncate works

By default (`fasttrun.zero_sinval_truncate = on`) the physical cleanup goes through `unlink` of all fork files + a fresh `smgrcreate`. This does **not** call `CacheInvalidateSmgr` — unlike the standard `smgrtruncate`.

Safe because temporary tables live in the backend's local buffer pool. Other processes don't see our relfilenode, and invalidation is useless to them.

At the same time, `fasttruncate` locally invalidates the current backend plan cache. This is needed so PL/pgSQL / SPI does not reuse an old plan after truncate and refill. This step does not send anything to the shared sinval queue.

Besides the table itself, `fasttruncate` handles:
* **all indexes** — rebuilds via `ambuild` (btree metapage, hash, etc.);
* **toast table** and its index — same path;
* **`rd_amcache`** — clears the index AM metadata cache;
* **`smgr_cached_nblocks`** — invalidated after ambuild;
* **analyze cache** — seeds the baseline for delta math.

Before cleanup, `CheckTableNotInUse` is called — the same check that regular SQL `TRUNCATE` does. If there is an open cursor or an active query on the table, you get a clear SQL error, not a PANIC.

Fallback: `SET fasttrun.zero_sinval_truncate = off` reverts to `heap_truncate_one_rel` (one SMGR sinval message per call).

## How fasttrun_analyze works

### Hot path (delta math)

On a repeat call, the function doesn't scan the table but computes the row count from pgstat counters:

```
new_tuples = cached_tuples + (ins_now - cached_ins) - (del_now - cached_del)
```

Cost: **~1 microsecond**. A hash table lookup + three subtractions + a write to `rd_rel`.

### Cold path (full scan)

Triggers on the first call or when the delta is invalid. Fully walks the heap, counts rows and (if `auto_collect_stats = on`) simultaneously samples for column statistics.

### Column statistics refresh

If after the previous collection the DML change ratio exceeded `stats_refresh_threshold` (20% by default), a block sample is launched. Not a full scan, just random blocks — the row count is already known from the delta.

### Statistics quality

By default (`use_typanalyze = on`) the extension calls **the same** `std_typanalyze` / type-specific `typanalyze` from the PostgreSQL core. So everything that regular `ANALYZE` collects is collected: MCV, histogram, correlation, type-specific stats.

The only difference is sample size. Default `sample_rows = 3000` is ~10x smaller than regular `ANALYZE`. For most tables it's enough. For a full match — `SET fasttrun.sample_rows = -1`.

The cache lives until the end of the transaction (cleaned via `RegisterXactCallback`).

## Settings (GUC)

| Parameter | Default | Description |
|---|---|---|
| `fasttrun.auto_collect_stats` | `on` | Collect column statistics during cold `fasttrun_analyze` pass |
| `fasttrun.sample_rows` | `3000` | Sample size. `0` — disable collection. `-1` — auto (same as regular `ANALYZE`) |
| `fasttrun.use_typanalyze` | `on` | Use `std_typanalyze` from core (MCV/histogram/correlation). `off` — only n_distinct/null_frac/width |
| `fasttrun.stats_refresh_threshold` | `0.2` | DML change ratio threshold for stats refresh. `0` — on any DML. `1` — refresh disabled (stats remain visible to planner) |
| `fasttrun.zero_sinval_truncate` | `on` | Direct `unlink`+`smgrcreate` instead of `smgrtruncate`. `off` — old path with 1 SMGR sinval |

## Performance

PostgreSQL 16.13, macOS arm64, single backend:

```
Scenario                                         Time

fasttruncate (no indexes)                         ~450 us
fasttruncate (1 btree index)                      ~640 us
fasttruncate (toast)                              ~400 us
fasttrun_analyze, hot path (100k rows)              ~1 us
fasttrun_analyze + INSERT (50k rows)              ~1.8 us
fasttrun_analyze vs ANALYZE (4 columns)           ~230x faster
```

Under load the gap is even wider — regular `ANALYZE` forces all other backends to drain the sinval queue, while `fasttrun_analyze` puts nothing into it.

## Production impact

One of our production clusters (64 CPU, 75+ backends, thousands of `CREATE TEMP TABLE` per day) before the fasttrun rework:

![CPU before fasttrun](docs/images/prod-cpu-before.png)

As you can see, CPU is under heavy pressure — the cluster was burning cycles draining the sinval queue in `ReceiveSharedInvalidMessages`. After the fasttrun rewrite (`fasttruncate` + `fasttrun_analyze`):

![CPU after fasttrun](docs/images/prod-cpu-after.png)

Same workload, same hardware: CPU idle stays around ~88%, per-node peaks don't exceed 40%. The numbers match the expectation from the description above — kill the sinval storm and you're left with the useful CPU budget.

## Installation

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

```sql
CREATE EXTENSION fasttrun;
```

By default, this installs version `2.1.2`.

Upgrade from older versions `2.0` / `2.1` / `2.1.1` is supported:
```sql
ALTER EXTENSION fasttrun UPDATE;
```

## Tests

```bash
make installcheck PG_CONFIG=/path/to/pg_config PGPORT=5433
```

10 test cases via `pg_regress`:

| Test | What it checks |
|---|---|
| `fasttrun_basic` | Basic operation, indexes (btree/hash/GIN/expression/partial), toast, active cursor |
| `fasttrun_silent` | Silent behavior on non-existent / non-temp tables |
| `fasttrun_stats_reset` | `relpages/reltuples` reset after fasttruncate |
| `fasttrun_analyze` | Delta math, savepoint rollback, TRUNCATE inside a transaction |
| `fasttrun_migration` | Upgrade path 2.0 → 2.1, including backward compatibility with `fasttruncate_c` |
| `fasttrun_bench` | Synthetic benchmark on 1M rows × 50 columns |
| `fasttrun_stats` | Statistics hook: EXPLAIN before/after, auto-collection, sample_rows=0/-1, refresh threshold |
| `fasttrun_tracking` | Tracking frequently created temp tables and prewarm |
| `fasttrun_relstats_survive` | relstats survive relcache rebuilds inside one backend |
| `fasttrun_plan_cache_survive` | Backend-local SPI/PL/pgSQL plan cache invalidation after fasttruncate |

All tests pass on PG 16.13, 17.9 and 18.3.

## Usage pattern

An example `create_temp_table` function that creates a temp table from a template or clears it via `fasttruncate` is in `examples/create_temp_table.sql`. Adapt it to your project.

Temporary table lifecycle in production with a connection pooler:

```sql
-- 1. Create the table (or clear if it already exists from the previous client)
--    create_temp_table internally calls fasttruncate if the table exists
PERFORM create_temp_table('temp_xxx');

-- 2. Fill with data
INSERT INTO temp_xxx SELECT ... FROM big_table WHERE ...;

-- 3. Update statistics for the planner (instead of ANALYZE temp_xxx)
--    On the first call — full scan + column stats collection.
--    On repeat calls — delta math for ~1 microsecond.
PERFORM fasttrun_analyze('temp_xxx');

-- 4. Work — the planner sees correct relpages/reltuples/n_distinct
SELECT ... FROM temp_xxx JOIN another_table ON ...;

-- 5. Clear before the next cycle (or before the next pooler client)
--    Also seeds the baseline for delta math.
PERFORM fasttruncate('temp_xxx');

-- Then the cycle repeats from step 2
```

In a typical PL/pgSQL calculation, one backend works with 10-30 temporary tables, each going through this cycle many times. With a pooler (pg_doorman, odyssey) the backend lives long and serves hundreds of clients in a row — temporary tables accumulate and get reused. `fasttruncate` resets data and statistics so the next client doesn't inherit anything from the previous one.

## Hot table prewarming

When working with a pooler, a backend serves hundreds of clients. Each client can use dozens of temporary tables. If you have thousands of templates in the database, creating all of them on backend startup is slow and generates sinval. Instead, fasttrun can track which temp tables are created most often and prewarm only the hottest ones.

### How to enable

Add fasttrun to `shared_preload_libraries` **as the last entry**:

```ini
# postgresql.conf
shared_preload_libraries = 'ptrack,citus_columnar,timescaledb,...,fasttrun'
```

Restart PostgreSQL. You need this once — fasttrun will allocate a chunk of shared memory for the counters.

> **Why last.** fasttrun registers a `planner_hook` that re-injects `relpages`/`reltuples` for temp tables into `rd_rel` before planning. In the PostgreSQL hook chain the extension loaded last runs first — so the stats are refreshed before Citus, TimescaleDB, pgpro_stats or other planners read them. Loading fasttrun earlier still works, but those extensions may read stale `rd_rel` values before our hook fires.

### What if you don't enable it

All core extension functions (`fasttruncate`, `fasttrun_analyze`, etc.) work fine — they don't need `shared_preload_libraries`. Only the tracking functions (`fasttrun_hot_temp_tables`, `fasttrun_prewarm`, `fasttrun_reset_temp_stats`) will return an empty result / zero. No errors.

### How to use

```sql
-- See the top 20 most frequently created temp tables:
SELECT * FROM fasttrun_hot_temp_tables(20);
 relname          | create_count | last_create
------------------+--------------+----------------------------
 temp_calc_main   |         1523 | 2026-04-11 12:34:56.789+03
 temp_payment_buf |          892 | 2026-04-11 12:34:55.123+03
 ...

-- Prewarm the hot tables on session start:
SELECT fasttrun_prewarm();
 fasttrun_prewarm
------------------
              127

-- Reset statistics (e.g. after a deploy with new tables):
SELECT fasttrun_reset_temp_stats();
```

`fasttrun_prewarm()` takes top-N from the collected statistics (N is set via `fasttrun.prewarm_count`, 1000 by default) and calls `create_temp_table()` for each. If the table already exists — just clears it via `fasttruncate`. Before calling prewarm checks that a template for the table exists in the `fasttrun.prewarm_schema` schema — if there is no template, the table is skipped without error.

**What goes into statistics**: only tables created via `CREATE TEMP TABLE ... (LIKE dummy_tmp.xxx ...)`. If a developer creates a temp table directly (`CREATE TEMP TABLE foo (id int, ...)`), without LIKE from the template schema — it does not go into statistics and does not interfere with prewarming.

Typical pooler integration — call `fasttrun_prewarm()` when the pooler creates a new physical connection. Especially useful when the pooler supports `min_pool_size` and keeps a certain number of connections ahead of time — then all pool backends start up already prewarmed.

### Settings

| Parameter | Default | Description |
|---|---|---|
| `fasttrun.track_temp_creates` | `on` | Count CREATE TEMP TABLE. Can be disabled via SET for debugging |
| `fasttrun.prewarm_count` | `1000` | How many hot tables to create in `fasttrun_prewarm()` |
| `fasttrun.prewarm_schema` | `dummy_tmp` | Template schema. Only CREATE with LIKE from this schema are tracked |
| `fasttrun.track_schedule` | `'mon-fri 08:00-18:00'` | Tracking schedule. Empty means always. Format described below |

### Tracking schedule

By default fasttrun collects statistics round the clock. But on a typical production server, at night there are jobs that create their own temporary tables — they get into statistics and pollute it. As a result, `fasttrun_prewarm()` ends up creating the wrong tables, not the ones needed for daytime user work.

The `fasttrun.track_schedule` GUC lets you configure a "window" when tracking is active:

```ini
# Weekdays only, 8 to 18
fasttrun.track_schedule = 'mon-fri 08:00-18:00'

# Weekdays plus half Saturday
fasttrun.track_schedule = 'mon-fri 08:00-18:00; sat 10:00-14:00'

# Specific days only
fasttrun.track_schedule = 'mon,wed,fri 09:00-17:00'

# Empty — always on (default)
fasttrun.track_schedule = ''
```

**Format:**
- Day names: `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, `sun` (case-insensitive)
- Day range via `-`: `mon-fri`
- Day list via `,`: `mon,wed,fri`
- Time `HH:MM-HH:MM` in 24-hour format
- Multiple windows separated by `;`
- Up to 8 windows
- Windows crossing midnight are not supported — split into two: `fri 22:00-23:59; sat 00:00-02:00`

**On parse error**: a WARNING is logged, tracking behaves as if no schedule is set (always active). This is safe by design — never silently disables tracking.

**Time is checked against the server timezone** (`log_timezone`). The hook check is one pass over the windows array, ~100 nanoseconds, unnoticeable.

Can be changed on the fly via `SET fasttrun.track_schedule = '...'` (superuser only).

### Persistence

Statistics are saved to disk (`pg_stat/fasttrun_temp_stats`) on server shutdown and loaded on startup. So counters are not lost after a PostgreSQL restart.

## Limitations

* **Heap AM only** — checked on entry of all functions. For columnar and other exotica — an error.
* **Does not check foreign keys** — `fasttruncate` doesn't scan `pg_constraint`. By our convention, FKs are not created on temporary tables.
* **Not transactional** — on ROLLBACK the data is not restored.
* **Extended statistics** (`CREATE STATISTICS`) — not supported, there is no suitable hook in the core.
* **Sequences** — `fasttruncate` does not reset SERIAL/IDENTITY sequences (same as regular `TRUNCATE` without `RESTART IDENTITY`).
* **Cache lives until end of transaction** — not reused across transactions.
* **`relpages/reltuples` do not roll back on ROLLBACK** — `fasttrun_analyze` writes to `rd_rel` directly, without undo. On transaction rollback the values remain from the rolled-back data until the next `fasttrun_analyze` or `fasttruncate` call.
* **Cached plans without `fasttruncate`** — `fasttrun_analyze` itself does not invalidate plans already cached in PL/pgSQL / SPI / PREPARE. The normal `fasttruncate` → refill → `fasttrun_analyze` cycle is safe in `2.1.2+`: `fasttruncate` performs backend-local plan-cache invalidation via `LocalExecuteInvalidationMessage`.
* **Cost of cold-path stats collection** — ~50-150 ms for a 1M rows × 50 columns table. Can be disabled via GUC.

## Compatibility

| PostgreSQL | Build | Tests |
|---|---|---|
| 16.x | ✅ | ✅ |
| 17.x | ✅ | ✅ |
| 18.x | ✅ | ✅ |

Single source file, version differences handled via `#if PG_VERSION_NUM`.

## File structure

```
fasttrun.c                    # main C code (~3300 lines)
fasttrun.control              # extension metadata
fasttrun--2.1.2.sql           # current version (8 functions)
fasttrun--2.0.sql             # old base version
fasttrun--2.0--2.1.sql        # migration 2.0 -> 2.1
fasttrun--2.1--2.1.1.sql      # migration 2.1 -> 2.1.1
fasttrun--2.1.1--2.1.2.sql    # migration 2.1.1 -> 2.1.2
Makefile                      # PGXS
examples/                     # examples (create_temp_table)
sql/                          # tests (10 files)
expected/                     # expected output
```
