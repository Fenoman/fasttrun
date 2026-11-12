/*-------------------------------------------------------------------------
 *
 * fasttrun.c
 *      Fast TRUNCATE for temporary tables that does not generate any
 *      shared invalidation messages beyond the SMGR ones RelationTruncate()
 *      sends when fasttrun.zero_sinval_truncate is off.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <fmgr.h>
#include <funcapi.h>
#include <math.h>
#include <time.h>
#include <unistd.h>

#include "access/amapi.h"
#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_database.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "common/pg_prng.h"
#include "common/relpath.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/pgstat_internal.h"	/* pgstat_fetch_pending_entry */
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "catalog/pg_statistic_ext.h"
#include "utils/typcache.h"
#include "pgtime.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#if PG_VERSION_NUM >= 170000
#include "storage/read_stream.h"		/* PG17+ rewrote analyze scan on ReadStream */
#endif
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "executor/spi.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(fasttruncate);
PG_FUNCTION_INFO_V1(fasttrun_analyze);
PG_FUNCTION_INFO_V1(fasttrun_analyze_bulk);
PG_FUNCTION_INFO_V1(fasttrun_relstats);
PG_FUNCTION_INFO_V1(fasttrun_collect_stats);
PG_FUNCTION_INFO_V1(fasttrun_inspect_stats);
PG_FUNCTION_INFO_V1(fasttrun_cache_stats);
PG_FUNCTION_INFO_V1(fasttrun_hot_temp_tables);
PG_FUNCTION_INFO_V1(fasttrun_prewarm);
PG_FUNCTION_INFO_V1(fasttrun_reset_temp_stats);
PG_FUNCTION_INFO_V1(fasttrun_counters);
PG_FUNCTION_INFO_V1(fasttrun_counters_reset);
PG_FUNCTION_INFO_V1(fasttrun_cached_tables);
PG_FUNCTION_INFO_V1(fasttrun_blocked_tables);
PG_FUNCTION_INFO_V1(fasttrun_temp_is_ours);
PG_FUNCTION_INFO_V1(fasttrun_temp_same_cols);
PG_FUNCTION_INFO_V1(fasttrun_test_relation_bytes);
#ifdef USE_ASSERT_CHECKING
PG_FUNCTION_INFO_V1(fasttrun_test_subxact_visits);
PG_FUNCTION_INFO_V1(fasttrun_test_poison_locator_mismatch);
PG_FUNCTION_INFO_V1(fasttrun_test_track_set);
PG_FUNCTION_INFO_V1(fasttrun_test_track_hold_lock);
PG_FUNCTION_INFO_V1(fasttrun_test_raw_relpages);
PG_FUNCTION_INFO_V1(fasttrun_test_relid_undo_depth);
PG_FUNCTION_INFO_V1(fasttrun_test_evict_missing_relid);
#endif
#ifdef USE_ASSERT_CHECKING
PG_FUNCTION_INFO_V1(fasttrun_test_planner_probe);
PG_FUNCTION_INFO_V1(fasttrun_test_call_attavgwidth);
#endif
Datum	fasttruncate(PG_FUNCTION_ARGS);
Datum	fasttrun_analyze(PG_FUNCTION_ARGS);
Datum	fasttrun_analyze_bulk(PG_FUNCTION_ARGS);
Datum	fasttrun_relstats(PG_FUNCTION_ARGS);
Datum	fasttrun_collect_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_inspect_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_cache_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_hot_temp_tables(PG_FUNCTION_ARGS);
Datum	fasttrun_prewarm(PG_FUNCTION_ARGS);
Datum	fasttrun_reset_temp_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_counters(PG_FUNCTION_ARGS);
Datum	fasttrun_counters_reset(PG_FUNCTION_ARGS);
Datum	fasttrun_cached_tables(PG_FUNCTION_ARGS);
Datum	fasttrun_blocked_tables(PG_FUNCTION_ARGS);
Datum	fasttrun_temp_is_ours(PG_FUNCTION_ARGS);
Datum	fasttrun_temp_same_cols(PG_FUNCTION_ARGS);

void _PG_init(void);

static int fasttrun_track_cmp_desc(const void *a, const void *b);
static void fasttrun_track_schedule_assign_hook(const char *newval, void *extra);

/* GUCs */
static bool		fasttrun_auto_collect_stats = true;
static int		fasttrun_sample_rows = 3000;
static double	fasttrun_stats_refresh_threshold = 0.2;
static double	fasttrun_invalidate_threshold = 0.2;
static bool		fasttrun_use_typanalyze = true;
static bool		fasttrun_zero_sinval_truncate = true;
static int		fasttrun_max_analyze_pages = 100000;
static int		fasttrun_max_stats_memory = 0;	/* KB, 0 = no cap */
static int		fasttrun_log_min_duration = -1;	/* ms, -1 = off */
static bool		fasttrun_template_stattarget = true;
/* Hand the functions' work to core commands and keep the hooks out of the way. */
static bool		fasttrun_passthrough = false;
static char	   *fasttrun_prewarm_schema;	/* defined with the tracking GUCs */

/*
 * Slices of fasttrun.c, in build order.
 *
 * The code of the extension lives in src/: each src/NN_name.c holds a
 * contiguous part of it, and all of them compile here as one translation
 * unit.  The order of the #include lines below is the order of the code:
 * definitions, types and state come before their users, so the order is
 * part of the build.  A slice does not compile on its own.
 */
#define FASTTRUN_UNITY

/* event counters, slow-call log, failpoints, planner memo state */
#include "src/01_counters.c"
/* subtransaction journal frames and relation marks */
#include "src/02_xact_journal.c"
/* shared types, state and forward declarations, make_rangevar */
#include "src/03_shared_decls.c"
/* analyze cache, its commit, xact callback, exit plan reset */
#include "src/04_relstats_cache.c"
/* truncate operation and poison registry */
#include "src/05_poison.c"
/* analyze cache entries, undo, relstats publication, eviction */
#include "src/06_relstats_publish.c"
/* relstats reinjection into relcache */
#include "src/07_reinject.c"
/* column stats cache keys, types, state, hook installation */
#include "src/08_stats_types.c"
/* storage checks, write marks, executor hooks */
#include "src/09_writers.c"
/* column stats cache init, relid entries, memory budget */
#include "src/10_stats_relid.c"
/* registry of started writing statements */
#include "src/11_writers_registry.c"
/* collect period, relid undo, refcounts, keys, policy */
#include "src/12_stats_relid_life.c"
/* column stats freshness check, memo cache */
#include "src/13_stats_freshness.c"
/* planner hooks, plan poison checks, get_relation_info */
#include "src/14_planner.c"
/* pgstat counter reads, commit-time size, wide writers */
#include "src/15_pgstat_read.c"
/* Haas-Stokes n_distinct, stats tuple build */
#include "src/16_stats_build.c"
/* column stats entry state, store, commit, eviction */
#include "src/17_stats_store.c"
/* subtransaction callback */
#include "src/18_subxact_callback.c"
/* column options and targets, sampling columns, pg_statistic */
#include "src/19_collect_columns.c"
/* index relstats, index expressions, collect_and_store */
#include "src/20_collect.c"
/* full scan, block sampling, refresh */
#include "src/21_sample.c"
/* fasttruncate and local plan invalidation */
#include "src/22_truncate.c"
/* analyze, core ANALYZE handoff, analyze SQL functions */
#include "src/23_analyze.c"
/* observability SQL functions */
#include "src/24_observe.c"
/* temp_is_ours, temp_same_cols, truncate marks */
#include "src/25_fresh.c"
/* collect_stats and inspect_stats SQL functions */
#include "src/26_stats_sql.c"
/* tracking schedule, shared memory, registry save and load */
#include "src/27_tracking.c"
/* DDL effects, utility poison checks, core handoffs */
#include "src/28_utility_ddl.c"
/* object access hook, utility hook */
#include "src/29_utility_hooks.c"
/* hot_temp_tables, prewarm, reset_temp_stats */
#include "src/30_tracking_sql.c"
/* cassert-only test SQL functions */
#include "src/31_test_funcs.c"

/* Define GUCs and install the always-on utility hook. */
void
_PG_init(void)
{
	DefineCustomBoolVariable("fasttrun.auto_collect_stats",
							 "Collect column statistics during cold fasttrun_analyze",
							 NULL,
							 &fasttrun_auto_collect_stats,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.sample_rows",
							"Reservoir sample size for column statistics",
							"-1 = auto: pick the largest minrows that "
							"std_typanalyze / type-specific typanalyze ask "
							"for across all columns of the table (this is "
							"normally 300 * default_statistics_target - i.e. "
							"the same sample size that core ANALYZE would "
							"use, with full MCV / histogram parity).  "
							"0 = disable column-stats collection entirely.  "
							"N > 0 = explicit fixed sample size.  Default 3000 "
							"is a deliberate trade-off - lighter than core "
							"ANALYZE in exchange for ~10x cheaper cold scan "
							"on wide tables, sufficient for typical "
							"distributions but may miss MCV entries with "
							"frequency below ~3%.  Set to -1 for full "
							"ANALYZE-quality plans on critical workloads.",
							&fasttrun_sample_rows,
							3000,
							-1,
							1000000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomRealVariable("fasttrun.stats_refresh_threshold",
							 "DML churn ratio (vs reltuples) up to which the planner "
							 "keeps using column stats collected before the DML",
							 "fasttrun_analyze recollects column stats after any DML "
							 "since the last collect.  Until an analyze follows the "
							 "DML, the planner uses a column's stats only while churn "
							 "stays under its tolerance: ratio * (1 - distinct fraction "
							 "of rows), but at least min(ratio, 5%).  0 hides stats on "
							 "any DML.",
							 &fasttrun_stats_refresh_threshold,
							 0.2,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomRealVariable("fasttrun.invalidate_threshold",
							 "Relstats drift ratio (vs reltuples or relpages) below "
							 "which fasttrun_analyze skips invalidating backend-"
							 "local cached SPI/PREPARE plans",
							 "Default 0.2, the same value as "
							 "fasttrun.stats_refresh_threshold, though the two measure "
							 "different things: here the larger relative change of "
							 "reltuples or relpages since the last invalidation, there "
							 "DML churn since the last collect.  Set to 0 to invalidate "
							 "plans on any relstats change.  A column-stats refresh, an "
							 "index relstats change (e.g. partial index) or a stats "
							 "visibility flip always invalidates: those signal "
							 "distribution changes that this size ratio cannot measure.",
							 &fasttrun_invalidate_threshold,
							 0.2,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("fasttrun.use_typanalyze",
							 "Use core std_typanalyze for column statistics "
							 "(MCV, histogram, correlation) instead of the "
							 "lightweight Haas-Stokes path",
							 "When on (default), fasttrun reuses PostgreSQL's "
							 "own std_typanalyze / type-specific typanalyze "
							 "callbacks to compute n_distinct, MCV, histogram, "
							 "correlation and type-specific stats - same path "
							 "as a regular ANALYZE, only without sinval / "
							 "catalog updates.  When off, falls back to the "
							 "lightweight Haas-Stokes path that only computes "
							 "n_distinct/null_frac/width.  It is "
							 "useful for comparing plan quality before / after "
							 "and as a safety fallback if a custom typanalyze "
							 "misbehaves on a particular type.",
							 &fasttrun_use_typanalyze,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("fasttrun.zero_sinval_truncate",
							 "Clear local temporary table files without shared "
							 "SMGR invalidations",
							 "When on (default), fasttruncate physically "
							 "clears the temporary table, its indexes, and "
							 "TOAST relations "
							 "with unlink and smgrcreate, without calling "
							 "CacheInvalidateSmgr.  When off, it calls "
							 "RelationTruncate for each relation, and every "
							 "successful call sends one shared SMGR "
							 "invalidation.  Both modes rebuild indexes and "
							 "block access if cleanup is interrupted.",
							 &fasttrun_zero_sinval_truncate,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.max_analyze_pages",
							"Heap page count above which fasttrun_analyze scans "
							"switch to bounded block sampling",
							"Default 100000 (~800 MB).  Every heap scan of "
							"fasttrun_analyze (cold pass, stats refresh, "
							"partial-index rescan) normally reads the whole temp "
							"table for an exact row count.  Above this many heap "
							"pages it instead reads a bounded random block sample "
							"and ESTIMATES the row count from tuple density (like "
							"a regular ANALYZE), keeping cost O(sample) instead "
							"of O(table) on anomalously giant temp tables.  "
							"Column statistics are collected from the same "
							"sample.  Set to 0 to always do the exact full scan.  "
							"The explicit fasttrun_collect_stats ignores this "
							"limit and always reads the whole heap.",
							&fasttrun_max_analyze_pages,
							100000,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.max_stats_memory",
							"Soft memory cap for the session-local "
							"column-statistics cache",
							"Default 0 = no cap.  When the cache already "
							"holds more than this much memory, column "
							"statistics are not collected for tables that "
							"have none cached yet -- such a table behaves "
							"as with fasttrun.auto_collect_stats = off: "
							"relation-level statistics keep working.  "
							"Tables that already hold cached column "
							"statistics keep refreshing regardless of the "
							"cap, and nothing is evicted.  "
							"fasttrun_cache_stats() reports the current "
							"cache size.",
							&fasttrun_max_stats_memory,
							0,
							0,
							MAX_KILOBYTES,
							PGC_USERSET,
							GUC_UNIT_KB,
							NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.log_min_duration",
							"Log fasttruncate and fasttrun_analyze calls that "
							"take at least this many milliseconds",
							"-1 (the default) turns the log off, 0 logs every "
							"call.  The line names the relation, the time, the "
							"path the call took and the pages and rows it "
							"published.",
							&fasttrun_log_min_duration,
							-1,
							-1,
							INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("fasttrun.template_stattarget",
							 "Take a column's statistics target from the "
							 "same-named template table",
							 "A column of a temporary table left at the default "
							 "target takes the target of the same-named column "
							 "of the same-named table in fasttrun.prewarm_schema. "
							 "CREATE TABLE ... (LIKE ...) does not copy SET "
							 "STATISTICS, so the setting is made once on the "
							 "template.",
							 &fasttrun_template_stattarget,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("fasttrun.passthrough",
							 "Run core TRUNCATE and ANALYZE instead of the "
							 "extension's own work",
							 "When on, fasttruncate runs TRUNCATE, and "
							 "fasttrun_analyze, fasttrun_analyze_bulk and "
							 "fasttrun_collect_stats run ANALYZE.  The hooks and "
							 "session caches stay idle, as if the extension were "
							 "not installed, while callers keep their code.  Only "
							 "the clear marks of fasttrun_temp_is_ours keep "
							 "working.  The "
							 "value is fixed for the whole session: it is taken "
							 "at connection start.",
							 &fasttrun_passthrough,
							 false,
							 PGC_SU_BACKEND,
							 0,
							 NULL, NULL, NULL);

	/*
	 * Register the xact + subxact callbacks once per backend, here in
	 * _PG_init, so they fire regardless of which cache is lazily
	 * allocated first -- or whether only standalone
	 * fasttrun_collect_stats() is ever called.
	 */
	RegisterXactCallback(fasttrun_xact_callback, NULL);
	RegisterSubXactCallback(fasttrun_subxact_callback, NULL);
	/* Not stopped by passthrough: fasttrun_temp_is_ours works in both modes. */
	RegisterXactCallback(fasttrun_fresh_xact_callback, NULL);
	RegisterSubXactCallback(fasttrun_fresh_subxact_callback, NULL);

	/*
	 * The executor hooks go in here, not with the lazily installed stats
	 * hooks.  A statement's start is the only moment its target list can be
	 * read, and the first collect of a backend can happen from inside a
	 * running UPDATE -- by then a lazily installed hook has already missed
	 * that start.  The hook itself leaves a plain SELECT after one branch.
	 *
	 * A library loaded on first use is already too late for the statements
	 * running above it, and nothing recovers them afterwards, so that backend
	 * does without the per-column account entirely.
	 */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = fasttrun_executor_start;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = fasttrun_executor_end;
	fasttrun_executor_hook_installed = true;

	/*
	 * Preloading runs before the session executes anything, so neither a portal
	 * nor a snapshot is set yet -- for shared_preload_libraries and for
	 * session_preload_libraries alike, even though the latter does run
	 * inside the startup transaction.  What matters is the writer that could
	 * be running above us: to write it needs a snapshot, and the paths that
	 * reach the executor without a portal set one before they get there -- a
	 * login event trigger, a logical replication apply worker, a fastpath
	 * function call.  So either mark here means something may already be
	 * running and its start was missed.  Only the absence of both is taken as
	 * proof of an early load.
	 */
	fasttrun_lazy_loaded = (ActiveSnapshotSet() || ActivePortal != NULL);
	fasttrun_unobserved_writers = fasttrun_lazy_loaded;

	/*
	 * Planner/stats hooks are installed lazily on first use of the local
	 * stats caches.  A backend that only loads fasttrun for tracking, or
	 * never calls fasttrun_analyze/fasttrun_collect_stats, should not pay
	 * a planner-hook call on unrelated queries.
	 */
	prev_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = fasttrun_utility_hook;

	prev_object_access_hook = object_access_hook;
	object_access_hook = fasttrun_object_access_hook;

	/* Tracking GUCs */
	DefineCustomBoolVariable("fasttrun.track_temp_creates",
							 "Track CREATE TEMP TABLE frequency in shared memory",
							 NULL,
							 &fasttrun_track_enabled,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.prewarm_count",
							"Number of hottest temp tables to pre-create in fasttrun_prewarm()",
							NULL,
							&fasttrun_prewarm_count,
							1000,
							0,
							FASTTRUN_TRACK_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable("fasttrun.prewarm_schema",
							   "Schema with dummy (template) tables for prewarm tracking",
							   "Only CREATE TEMP TABLE ... (LIKE <schema>.xxx) "
							   "will be tracked. Tables created without LIKE from "
							   "this schema are ignored.",
							   &fasttrun_prewarm_schema,
							   "dummy_tmp",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("fasttrun.track_schedule",
							   "Time windows when CREATE TEMP TABLE tracking is active",
							   "Format: 'mon-fri 08:00-18:00; sat 10:00-14:00'. "
							   "Empty string means tracking is always active. "
							   "Invalid format is logged as WARNING and falls back "
							   "to always-active.",
							   &fasttrun_track_schedule,
							   "mon-fri 08:00-18:00",
								   PGC_SUSET,
								   0,
								   NULL,
								   fasttrun_track_schedule_assign_hook,
								   NULL);

#ifdef USE_ASSERT_CHECKING
	DefineCustomStringVariable("fasttrun.test_failpoint",
							   "Inject deterministic errors in cassert test builds",
							   NULL,
							   &fasttrun_test_failpoint,
							   "",
							   PGC_SUSET,
							   GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);
#endif
	MarkGUCPrefixReserved("fasttrun");

	/* Shared memory tracking -- only when loaded via shared_preload_libraries. */
	if (process_shared_preload_libraries_in_progress)
	{
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = fasttrun_shmem_request;

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = fasttrun_shmem_startup;
	}
}
