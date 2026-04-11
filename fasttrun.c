/*-------------------------------------------------------------------------
 *
 * fasttrun.c
 *      Fast TRUNCATE for temporary tables that does not generate any
 *      shared invalidation messages beyond the unavoidable ones produced
 *      by heap_truncate() itself.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fmgr.h>
#include <funcapi.h>
#include <unistd.h>

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "common/pg_prng.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "executor/spi.h"
#include "tcop/utility.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(fasttruncate);
PG_FUNCTION_INFO_V1(fasttrun_analyze);
PG_FUNCTION_INFO_V1(fasttrun_relstats);
PG_FUNCTION_INFO_V1(fasttrun_collect_stats);
PG_FUNCTION_INFO_V1(fasttrun_inspect_stats);
PG_FUNCTION_INFO_V1(fasttrun_hot_temp_tables);
PG_FUNCTION_INFO_V1(fasttrun_prewarm);
PG_FUNCTION_INFO_V1(fasttrun_reset_temp_stats);
Datum	fasttruncate(PG_FUNCTION_ARGS);
Datum	fasttrun_analyze(PG_FUNCTION_ARGS);
Datum	fasttrun_relstats(PG_FUNCTION_ARGS);
Datum	fasttrun_collect_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_inspect_stats(PG_FUNCTION_ARGS);
Datum	fasttrun_hot_temp_tables(PG_FUNCTION_ARGS);
Datum	fasttrun_prewarm(PG_FUNCTION_ARGS);
Datum	fasttrun_reset_temp_stats(PG_FUNCTION_ARGS);

void _PG_init(void);

static int fasttrun_track_cmp_desc(const void *a, const void *b);

/* GUCs */
static bool		fasttrun_auto_collect_stats = true;
static int		fasttrun_sample_rows = 3000;
static double	fasttrun_stats_refresh_threshold = 0.2;
static bool		fasttrun_use_typanalyze = true;
static bool		fasttrun_zero_sinval_truncate = true;

/* "Silent killer" warning — one shot per backend. */
static bool		fasttrun_warned_track_counts_off = false;

/* RNG state for reservoir sampling */
static pg_prng_state fasttrun_prng_state;

/* Forward decls for stats infrastructure (defined below) */
static void fasttrun_stats_cache_reset(void);
static void fasttrun_subxact_callback(SubXactEvent event,
									  SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);
static void fasttrun_collect_and_store(Relation rel, HeapTuple *sample,
									   int sample_count, int64 totalrows,
									   bool sample_needs_tid_sort);
static bool fasttrun_read_pgstat_counters(Relation rel,
										  int64 *ins, int64 *upd, int64 *del,
										  bool *truncdropped);

/*
 * Build a RangeVar from a text relation name.  Fast path for bare
 * lowercase identifiers (no '.', no '"', no uppercase) skips the parser
 * entirely; everything else falls back to stringToQualifiedNameList.
 */
static RangeVar *
fasttrun_make_rangevar(text *name)
{
	char	   *relname;
	Size		namelen;
	Size		i;
	bool		fast_path = true;
	List	   *relname_list;

	namelen = VARSIZE_ANY_EXHDR(name);
	relname = palloc(namelen + 1);
	memcpy(relname, VARDATA_ANY(name), namelen);
	relname[namelen] = '\0';

	for (i = 0; i < namelen; i++)
	{
		unsigned char c = (unsigned char) relname[i];

		if (!((c >= 'a' && c <= 'z') ||
			  (c >= '0' && c <= '9') ||
			  c == '_'))
		{
			fast_path = false;
			break;
		}
	}

	if (fast_path && namelen > 0)
		return makeRangeVar(NULL, relname, -1);

#if PG_VERSION_NUM >= 160000
	relname_list = stringToQualifiedNameList(relname, NULL);
#else
	relname_list = stringToQualifiedNameList(relname);
#endif
	return makeRangeVarFromNameList(relname_list);
}

/*
 * Lazy-mode cache for fasttrun_analyze.
 *
 * Cached snapshot per relid: (pages, tuples, ins, del, truncdropped).
 * On next call live row count is reconstructed by exact delta math:
 *     new_tuples = cached_tuples + (ins_now - cached_ins)
 *                                - (del_now - cached_del)
 * UPDATE doesn't change live count and is not tracked.  HTAB is reset
 * on every xact boundary by fasttrun_xact_callback.  Cache invalidated
 * if: pages dropped (TRUNCATE-like), truncdropped bit changed, or
 * computed new_tuples < 0.  pgstat unavailable → fall back to scan.
 */

/*
 * Stack frame saved on every cross-subxact stats_baseline_* overwrite.
 * fasttrun_subxact_callback pops this on ROLLBACK TO SAVEPOINT to fully
 * restore the pre-subxact baseline (and not just the per-column stats
 * cache, which has its own undo).  Allocated in fasttrun_analyze_mcxt.
 */
typedef struct FasttrunAnalyzeBaselineUndo
{
	bool		has_stats_baseline;
	int64		stats_baseline_inserted;
	int64		stats_baseline_updated;
	int64		stats_baseline_deleted;
	bool		stats_baseline_truncdropped;
	SubTransactionId	stats_baseline_subid;
	struct FasttrunAnalyzeBaselineUndo *older;
} FasttrunAnalyzeBaselineUndo;

typedef struct FasttrunAnalyzeCacheEntry
{
	Oid			relid;			/* hash key — must be first */
	BlockNumber	cached_pages;
	int64		cached_tuples;
	int64		cached_inserted;
	int64		cached_deleted;
	bool		cached_truncdropped;	/* OR of truncdropped over subxact stack */

	/*
	 * Stats-collection baseline: pgstat counters captured the last time
	 * we successfully collected (or explicitly reset) the column-stats
	 * cache for this relid.  Used by the delta-hit refresh path to
	 * decide whether DML churn since that point has crossed
	 * fasttrun.stats_refresh_threshold.
	 *
	 * Lives here (not in the per-(relid,attnum) stats cache) so that
	 * (a) lookup is O(1), not O(N) over the stats hash, and
	 * (b) when a refresh on an emptied table finds zero visible
	 *     tuples, we can still advance the baseline without leaving a
	 *     stats-cache entry behind — otherwise the next analyze would
	 *     keep re-entering the refresh path forever.
	 *
	 * has_stats_baseline=false means "we never collected column stats
	 * for this relid in the current xact"; refresh path skips and
	 * leaves the planner to use defaults / whatever the hook returns.
	 *
	 * stats_baseline_subid + stats_baseline_undo carry the same
	 * savepoint-aware semantics as the per-column stats cache: an
	 * overwrite that crosses a subxact boundary pushes the old baseline
	 * onto the undo stack, and ROLLBACK TO SAVEPOINT pops it back.
	 * This keeps refresh-path churn computation consistent with the
	 * stats cache after a partial rollback (otherwise the first post-
	 * rollback analyze would re-enter the refresh path with a
	 * "from-the-future" baseline).
	 */
	bool		has_stats_baseline;
	int64		stats_baseline_inserted;
	int64		stats_baseline_updated;
	int64		stats_baseline_deleted;
	bool		stats_baseline_truncdropped;
	SubTransactionId	stats_baseline_subid;
	FasttrunAnalyzeBaselineUndo *stats_baseline_undo;
} FasttrunAnalyzeCacheEntry;

static HTAB			   *fasttrun_analyze_cache = NULL;
static MemoryContext	fasttrun_analyze_mcxt = NULL;

static void fasttrun_xact_callback(XactEvent event, void *arg);

/*
 * Free the entire baseline undo chain hanging off an analyze-cache entry.
 * Used both on full xact reset and when an entry is evicted (fasttruncate).
 */
static void
fasttrun_baseline_free_undo(FasttrunAnalyzeCacheEntry *entry)
{
	while (entry->stats_baseline_undo != NULL)
	{
		FasttrunAnalyzeBaselineUndo *popped = entry->stats_baseline_undo;

		entry->stats_baseline_undo = popped->older;
		pfree(popped);
	}
}

/* Drop the analyze HTAB + its mcxt (frees all baseline undo chains). */
static void
fasttrun_cache_reset(void)
{
	if (fasttrun_analyze_cache == NULL)
		return;

	hash_destroy(fasttrun_analyze_cache);
	fasttrun_analyze_cache = NULL;

	if (fasttrun_analyze_mcxt != NULL)
	{
		MemoryContextDelete(fasttrun_analyze_mcxt);
		fasttrun_analyze_mcxt = NULL;
	}
}

/* Clear analyze + stats caches on xact end.  PRE_COMMIT skipped — txn may still abort. */
static void
fasttrun_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
			fasttrun_cache_reset();
			fasttrun_stats_cache_reset();
			break;
		default:
			break;
	}
}

/* Lazily allocate the analyze HTAB and its dedicated mcxt. */
static void
fasttrun_cache_init(void)
{
	HASHCTL ctl;

	if (fasttrun_analyze_cache != NULL)
		return;

	fasttrun_analyze_mcxt = AllocSetContextCreate(TopMemoryContext,
												  "fasttrun analyze cache",
												  ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FasttrunAnalyzeCacheEntry);
	ctl.hcxt = fasttrun_analyze_mcxt;

	fasttrun_analyze_cache = hash_create("fasttrun analyze cache",
										 64,
										 &ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Returns NULL if no entry or cache not yet allocated. */
static FasttrunAnalyzeCacheEntry *
fasttrun_cache_lookup(Oid relid)
{
	if (fasttrun_analyze_cache == NULL)
		return NULL;

	return (FasttrunAnalyzeCacheEntry *) hash_search(fasttrun_analyze_cache,
													 &relid,
													 HASH_FIND,
													 NULL);
}

/*
 * HASH_ENTER an analyze-cache entry, zero-initializing the
 * stats-baseline fields on first creation.  Callers fill in cached_*
 * (delta-math snapshot) themselves; the stats baseline starts out as
 * "no column stats collected yet for this relid".
 */
static FasttrunAnalyzeCacheEntry *
fasttrun_cache_enter(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry;
	bool	found;

	fasttrun_cache_init();
	entry = (FasttrunAnalyzeCacheEntry *) hash_search(fasttrun_analyze_cache,
													  &relid,
													  HASH_ENTER, &found);
	if (!found)
	{
		entry->has_stats_baseline = false;
		entry->stats_baseline_inserted = 0;
		entry->stats_baseline_updated = 0;
		entry->stats_baseline_deleted = 0;
		entry->stats_baseline_truncdropped = false;
		entry->stats_baseline_subid = InvalidSubTransactionId;
		entry->stats_baseline_undo = NULL;
	}
	return entry;
}

/*
 * Record a fresh stats-collection baseline for relid: called both
 * after a successful collect_and_store (cold scan or refresh with a
 * non-empty sample) and after an empty refresh on a now-empty table.
 *
 * Lazily creates the entry if it doesn't exist yet — fasttrun_analyze
 * normally populates it earlier on the same call, but
 * fasttrun_cache_update_stats_baseline_if_present() is the right entry
 * point for callers (e.g. fasttrun_collect_stats) that must NOT seed a
 * delta-math state.
 *
 * Cross-subxact safety: if the existing baseline was set in an outer
 * subxact, push it onto the undo stack so a later ROLLBACK TO SAVEPOINT
 * restores it; same-subxact overwrites just clobber in place.
 */
static void
fasttrun_cache_set_stats_baseline(Oid relid, int64 ins, int64 upd, int64 del,
								  bool truncdropped)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_enter(relid);
	SubTransactionId cur_subid = GetCurrentSubTransactionId();

	if (entry->stats_baseline_subid != cur_subid)
	{
		MemoryContext oldcxt;
		FasttrunAnalyzeBaselineUndo *saved;

		oldcxt = MemoryContextSwitchTo(fasttrun_analyze_mcxt);
		saved = (FasttrunAnalyzeBaselineUndo *) palloc(sizeof(*saved));
		MemoryContextSwitchTo(oldcxt);

		saved->has_stats_baseline = entry->has_stats_baseline;
		saved->stats_baseline_inserted = entry->stats_baseline_inserted;
		saved->stats_baseline_updated = entry->stats_baseline_updated;
		saved->stats_baseline_deleted = entry->stats_baseline_deleted;
		saved->stats_baseline_truncdropped = entry->stats_baseline_truncdropped;
		saved->stats_baseline_subid = entry->stats_baseline_subid;
		saved->older = entry->stats_baseline_undo;
		entry->stats_baseline_undo = saved;
	}

	entry->has_stats_baseline = true;
	entry->stats_baseline_inserted = ins;
	entry->stats_baseline_updated = upd;
	entry->stats_baseline_deleted = del;
	entry->stats_baseline_truncdropped = truncdropped;
	entry->stats_baseline_subid = cur_subid;
}

/*
 * Variant of set_stats_baseline that ONLY updates an already-existing
 * entry: used by fasttrun_collect_stats() to publish the new baseline
 * without seeding a fresh analyze-cache entry behind the user's back
 * (which would carry a bogus zero delta-math state).
 *
 * No-op if the analyze cache hasn't been touched for this relid yet.
 * In that case the user got fresh column stats but auto-refresh remains
 * disengaged for this relid until the next fasttrun_analyze() — that's
 * the documented contract.
 */
static void
fasttrun_cache_update_stats_baseline_if_present(Oid relid,
												int64 ins, int64 upd, int64 del,
												bool truncdropped)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

	if (entry == NULL)
		return;

	fasttrun_cache_set_stats_baseline(relid, ins, upd, del, truncdropped);
}

/* Drop entry by relid.  Called from fasttruncate. */
static void
fasttrun_cache_remove(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry;

	if (fasttrun_analyze_cache == NULL)
		return;

	entry = fasttrun_cache_lookup(relid);
	if (entry != NULL)
		fasttrun_baseline_free_undo(entry);

	(void) hash_search(fasttrun_analyze_cache, &relid, HASH_REMOVE, NULL);
}

/*
 * Seed an analyze-cache entry from scratch right after fasttruncate(),
 * so the very next fasttrun_analyze() on the refilled table can take the
 * delta hot path instead of paying for a full cold scan.
 *
 * Logically the table now has zero rows and zero pages, and the post-
 * truncate pgstat snapshot becomes the new delta-math baseline.  After
 * a subsequent INSERT N, the next fasttrun_analyze() sees:
 *   pages_now > 0 ≥ cached_pages (=0)               → delta hit OK
 *   truncdropped_now == cached_truncdropped         → delta hit OK
 *   delta_ins = ins_now - cached_ins = N            → exactly the new rows
 *   new_tuples = 0 + N - 0 = N                      → correct
 *
 * The stats baseline is also seeded with the same snapshot, so that the
 * first post-refill analyze passes the refresh-check threshold (any
 * non-trivial INSERT against a 0-row baseline gives churn ratio ≥ 1)
 * and triggers a block-sample stats refresh — which means we still get
 * fresh column stats on the refilled data, just via the cheap refresh
 * path instead of a full cold scan.
 *
 * If pgstat is unavailable (track_counts=off) we can't take a snapshot,
 * so we fall back to the old behaviour: drop the cache entry and let
 * the next analyze pay for a real cold scan.
 */
static void
fasttrun_cache_seed_after_truncate(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	int64		ins_now = 0;
	int64		upd_now = 0;
	int64		del_now = 0;
	bool		truncdropped_now = false;
	FasttrunAnalyzeCacheEntry *entry;
	SubTransactionId cur_subid;

	/* Drop any prior entry (and its baseline undo chain) first. */
	fasttrun_cache_remove(relid);

	if (!fasttrun_read_pgstat_counters(rel, &ins_now, &upd_now, &del_now,
									   &truncdropped_now))
		return;	/* no pgstat → no seed; next analyze will cold-scan */

	cur_subid = GetCurrentSubTransactionId();

	entry = fasttrun_cache_enter(relid);

	/* Delta-math state: post-truncate empty table. */
	entry->cached_pages = 0;
	entry->cached_tuples = 0;
	entry->cached_inserted = ins_now;
	entry->cached_deleted = del_now;
	entry->cached_truncdropped = truncdropped_now;

	/*
	 * Stats baseline: same snapshot.  has_stats_baseline=true so the
	 * first post-refill analyze enters the refresh-check arm; the
	 * resulting churn ratio (any INSERT N / max(N,1) = 1) clears the
	 * default 0.2 threshold and triggers a block-sample collect.
	 */
	entry->has_stats_baseline = true;
	entry->stats_baseline_inserted = ins_now;
	entry->stats_baseline_updated = upd_now;
	entry->stats_baseline_deleted = del_now;
	entry->stats_baseline_truncdropped = truncdropped_now;
	entry->stats_baseline_subid = cur_subid;
	/* stats_baseline_undo stays NULL — no prior version to restore. */
}

/*
 * Session-local pg_statistic cache + planner hooks.
 *
 * Per-(relid, attnum) HeapTuples in pg_statistic format, returned to the
 * planner via two complementary hooks:
 *
 *   * get_relation_stats_hook — feeds n_distinct / null_frac and the
 *     full statsTuple into selectivity estimation paths that read
 *     stats through VariableStatData (e.g. eq_sel, scalararraysel).
 *
 *   * get_attavgwidth_hook    — feeds stawidth into the SEPARATE path
 *     that the planner uses to size hash tables, sort tuplestores and
 *     join tuple widths (lsyscache.c:get_attavgwidth).  Without this
 *     second hook the planner falls back to get_typavgwidth() and
 *     happily uses the type's "default" width — for `text` it's 32
 *     bytes regardless of the actual sample, which throws off
 *     hash/sort/spool costing on temp tables with short text columns.
 *     The two hooks have to be wired up independently because they
 *     read from different places (statsTuple vs. direct syscache
 *     lookup of the stawidth column).
 *
 * Both hooks share the same per-(relid, attnum) cache and the same
 * pgstat-counter freshness check.  The cache lives in a child of
 * TopMemoryContext and is destroyed at every xact end.
 */

typedef struct FasttrunStatsKey
{
	Oid			relid;
	AttrNumber	attnum;
	bool		inh;
} FasttrunStatsKey;

/*
 * Saved previous version of a stats entry, pushed on every overwrite
 * that crosses a subxact boundary.  fasttrun_subxact_callback pops this
 * on ROLLBACK TO SAVEPOINT to fully restore the pre-subxact state — not
 * just delete the subxact-local refresh.  Lives in fasttrun_stats_mcxt.
 */
typedef struct FasttrunStatsSavedState
{
	HeapTuple			statsTuple;
	int64				collected_ins;
	int64				collected_upd;
	int64				collected_del;
	bool				collected_truncdropped;
	SubTransactionId	collected_subid;
	struct FasttrunStatsSavedState *older;
} FasttrunStatsSavedState;

typedef struct FasttrunStatsEntry
{
	FasttrunStatsKey	key;
	HeapTuple			statsTuple;		/* lives in fasttrun_stats_mcxt */
	int64				collected_ins;	/* pgstat snapshot at collect time */
	int64				collected_upd;
	int64				collected_del;
	bool				collected_truncdropped;
	SubTransactionId	collected_subid;	/* subxact in which this entry
											 * was (re)published */
	FasttrunStatsSavedState *undo;		/* stack of older versions saved
										 * at each subxact-boundary store */
} FasttrunStatsEntry;

static HTAB			   *fasttrun_stats_cache = NULL;
static MemoryContext	fasttrun_stats_mcxt = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_attavgwidth_hook_type prev_get_attavgwidth_hook = NULL;

static void
fasttrun_stats_cache_init(void)
{
	HASHCTL ctl;

	if (fasttrun_stats_cache != NULL)
		return;

	fasttrun_stats_mcxt = AllocSetContextCreate(TopMemoryContext,
												"fasttrun stats cache",
												ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FasttrunStatsKey);
	ctl.entrysize = sizeof(FasttrunStatsEntry);
	ctl.hcxt = fasttrun_stats_mcxt;

	fasttrun_stats_cache = hash_create("fasttrun stats cache",
									   64,
									   &ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Drop the stats HTAB + its mcxt (frees all cached tuples at once). */
static void
fasttrun_stats_cache_reset(void)
{
	if (fasttrun_stats_cache == NULL)
		return;

	hash_destroy(fasttrun_stats_cache);
	fasttrun_stats_cache = NULL;

	if (fasttrun_stats_mcxt != NULL)
	{
		MemoryContextDelete(fasttrun_stats_mcxt);
		fasttrun_stats_mcxt = NULL;
	}
}

/* freefunc for VariableStatData — we own the tuple, no-op. */
static void
fasttrun_stats_noop_free(HeapTuple tuple)
{
}

/*
 * Substitute our cached tuple only when its pgstat snapshot still
 * matches the relation's current counters; any DML since collect time
 * makes the stats stale and we fall through so the planner uses
 * defaults instead of bad estimates.
 */
static bool
fasttrun_get_relation_stats_hook(PlannerInfo *root, RangeTblEntry *rte,
								 AttrNumber attnum, VariableStatData *vardata)
{
	FasttrunStatsKey key;
	FasttrunStatsEntry *entry;
	Relation		rel;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;

	if (fasttrun_stats_cache == NULL)
		goto chain;

	key.relid = rte->relid;
	key.attnum = attnum;
	key.inh = rte->inh;

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry == NULL)
		goto chain;

	/* Freshness check: pgstat counters must match the collect-time snapshot. */
	rel = RelationIdGetRelation(rte->relid);
	if (rel == NULL)
		goto chain;
	{
		bool	have = fasttrun_read_pgstat_counters(rel, &ins_now, &upd_now,
													 &del_now, &truncdropped_now);
		RelationClose(rel);
		/*
		 * track_counts=off → a zeroed snapshot cannot prove freshness
		 * (it would match any zeroed collect).  Refuse to return the
		 * cached tuple so the planner falls back to defaults.
		 */
		if (!have)
			goto chain;
	}

	if (ins_now != entry->collected_ins ||
		upd_now != entry->collected_upd ||
		del_now != entry->collected_del ||
		truncdropped_now != entry->collected_truncdropped)
		goto chain;		/* stale — fall back to defaults */

	vardata->statsTuple = entry->statsTuple;
	vardata->freefunc = fasttrun_stats_noop_free;
	vardata->acl_ok = true;
	return true;

chain:
	if (prev_get_relation_stats_hook)
		return prev_get_relation_stats_hook(root, rte, attnum, vardata);
	return false;
}

/*
 * Companion to fasttrun_get_relation_stats_hook for the planner's
 * separate average-width path (lsyscache.c:get_attavgwidth).
 *
 * Without this hook the planner reads stawidth straight from the
 * pg_statistic syscache, which we never write to — so for temp tables
 * it falls back to get_typavgwidth() and uses the type-default width.
 * For varlena types like text that default is 32 bytes regardless of
 * the real sample, which inflates hash-table / sort / spool costing
 * on temp tables with short text columns.
 *
 * Same freshness contract as the relation-stats hook: only return the
 * cached width if the pgstat snapshot still matches.  Returning 0
 * means "no opinion" and lets the planner fall back to its default
 * path (typavgwidth or whatever an outer hook returns).
 */
static int32
fasttrun_get_attavgwidth_hook(Oid relid, AttrNumber attnum)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	Relation			rel;
	int64				ins_now = 0;
	int64				upd_now = 0;
	int64				del_now = 0;
	bool				truncdropped_now = false;
	int32				stawidth;

	if (fasttrun_stats_cache == NULL)
		goto chain;

	key.relid = relid;
	key.attnum = attnum;
	key.inh = false;	/* matches what fasttrun_stats_cache_store writes */

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry == NULL || entry->statsTuple == NULL)
		goto chain;

	rel = RelationIdGetRelation(relid);
	if (rel == NULL)
		goto chain;
	{
		bool	have = fasttrun_read_pgstat_counters(rel, &ins_now, &upd_now,
													 &del_now, &truncdropped_now);
		RelationClose(rel);
		if (!have)
			goto chain;
	}

	if (ins_now != entry->collected_ins ||
		upd_now != entry->collected_upd ||
		del_now != entry->collected_del ||
		truncdropped_now != entry->collected_truncdropped)
		goto chain;		/* stale */

	stawidth = ((Form_pg_statistic) GETSTRUCT(entry->statsTuple))->stawidth;
	if (stawidth > 0)
		return stawidth;
	/* fall through to chain on zero / negative width */

chain:
	if (prev_get_attavgwidth_hook)
		return (*prev_get_attavgwidth_hook) (relid, attnum);
	return 0;
}

/*
 * Sum tuples_inserted/_updated/_deleted and OR truncdropped over the
 * full active subxact stack.  Pass upd=NULL on hot paths that don't
 * care about UPDATE (rolling-delta math); the column-stats freshness
 * check passes a real pointer because UPDATE changes distribution
 * even though it doesn't change live row count.  Returns false if
 * pgstat is off → caller falls back to always-scan.
 */
static bool
fasttrun_read_pgstat_counters(Relation rel,
							  int64 *ins, int64 *upd, int64 *del,
							  bool *truncdropped)
{
	PgStat_TableStatus *pgstat_info;
	PgStat_TableXactStatus *trans;

	*ins = 0;
	if (upd != NULL)
		*upd = 0;
	*del = 0;
	*truncdropped = false;

	pgstat_info = rel->pgstat_info;
	if (pgstat_info == NULL)
		return false;

	for (trans = pgstat_info->trans; trans != NULL; trans = trans->upper)
	{
		*ins += trans->tuples_inserted;
		if (upd != NULL)
			*upd += trans->tuples_updated;
		*del += trans->tuples_deleted;
		if (trans->truncdropped)
			*truncdropped = true;
	}

	return true;
}

/* ---- Stats sample collection: Haas-Stokes n_distinct + null_frac + width ---- */

/* qsort_arg comparator wrapping a SortSupport. */
static int
fasttrun_datum_cmp(const void *a, const void *b, void *arg)
{
	SortSupport	ssup = (SortSupport) arg;
	Datum		da = *(const Datum *) a;
	Datum		db = *(const Datum *) b;

	return ApplySortComparator(da, false, db, false, ssup);
}

/*
 * Haas-Stokes Duj1 estimator, ported from analyze.c.  Returns
 * stadistinct in pg_statistic semantics (positive = absolute count,
 * negative = -(distinct/N) ratio when close to total).
 */
static float4
fasttrun_estimate_ndistinct(int n_nonnull, int ndistinct, int nmultiple,
							int64 totalrows)
{
	int		f1;
	double	numer;
	double	denom;
	double	estimate;

	if (n_nonnull <= 0)
		return 0.0f;
	if (ndistinct == 1)
		return 1.0f;
	if (nmultiple == 0)
		return -1.0f;					/* no repeats — assume unique */
	if (ndistinct == nmultiple)
		return (float4) ndistinct;		/* bounded set */

	f1 = ndistinct - nmultiple;
	numer = (double) n_nonnull * (double) ndistinct;
	denom = (double) (n_nonnull - f1)
		+ (double) f1 * (double) n_nonnull / (double) totalrows;
	estimate = numer / denom;

	if (estimate > (double) totalrows)
		estimate = (double) totalrows;

	if (estimate > 0.1 * (double) totalrows)
		return (float4) -(estimate / (double) totalrows);

	return (float4) estimate;
}

/*
 * Build a pg_statistic-shaped HeapTuple with only stanullfrac /
 * stawidth / stadistinct filled in.  All slots are empty.
 */
static HeapTuple
fasttrun_build_stats_tuple(TupleDesc pg_stats_desc, Oid relid, AttrNumber attnum,
						   float4 stanullfrac, int32 stawidth, float4 stadistinct)
{
	Datum	values[Natts_pg_statistic];
	bool	nulls[Natts_pg_statistic];
	int		i;

	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stanullfrac);
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stawidth);
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stadistinct);

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		values[Anum_pg_statistic_stakind1 - 1 + i] = Int16GetDatum(0);
		values[Anum_pg_statistic_staop1  - 1 + i] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stacoll1 - 1 + i] = ObjectIdGetDatum(InvalidOid);
		nulls [Anum_pg_statistic_stanumbers1 - 1 + i] = true;
		nulls [Anum_pg_statistic_stavalues1  - 1 + i] = true;
	}

	return heap_form_tuple(pg_stats_desc, values, nulls);
}

/*
 * Store stats tuple + pgstat snapshot, replacing any prior entry.
 * Tuple is copied into the cache mcxt.  Also records the current
 * subxact id so fasttrun_subxact_callback can roll the entry back
 * on ROLLBACK TO SAVEPOINT.
 *
 * Savepoint semantics: if we are overwriting an entry that was
 * published in an older subxact, the old values are pushed onto an
 * undo stack so a subsequent ROLLBACK TO SAVEPOINT can fully restore
 * them (not just evict).  Overwrites within the SAME subxact just
 * free the old tuple in place.
 */
static void
fasttrun_stats_cache_store(Oid relid, AttrNumber attnum, HeapTuple statsTuple,
						   int64 ins, int64 upd, int64 del, bool truncdropped)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	bool				found;
	MemoryContext		oldcxt;
	SubTransactionId	cur_subid = GetCurrentSubTransactionId();

	fasttrun_stats_cache_init();

	key.relid = relid;
	key.attnum = attnum;
	key.inh = false;

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_ENTER, &found);

	if (!found)
	{
		entry->undo = NULL;
	}
	else if (entry->collected_subid != cur_subid)
	{
		/* Cross-subxact overwrite: push old values onto undo stack. */
		FasttrunStatsSavedState *saved;

		oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
		saved = (FasttrunStatsSavedState *) palloc(sizeof(*saved));
		MemoryContextSwitchTo(oldcxt);

		saved->statsTuple = entry->statsTuple;	/* transfer ownership */
		saved->collected_ins = entry->collected_ins;
		saved->collected_upd = entry->collected_upd;
		saved->collected_del = entry->collected_del;
		saved->collected_truncdropped = entry->collected_truncdropped;
		saved->collected_subid = entry->collected_subid;
		saved->older = entry->undo;
		entry->undo = saved;
	}
	else
	{
		/* Same subxact — just free the old tuple. */
		if (entry->statsTuple != NULL)
			heap_freetuple(entry->statsTuple);
	}

	oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
	entry->statsTuple = heap_copytuple(statsTuple);
	MemoryContextSwitchTo(oldcxt);

	entry->collected_ins = ins;
	entry->collected_upd = upd;
	entry->collected_del = del;
	entry->collected_truncdropped = truncdropped;
	entry->collected_subid = cur_subid;
}

/* Free the complete undo chain of an entry. */
static void
fasttrun_stats_entry_free_undo(FasttrunStatsEntry *entry)
{
	while (entry->undo != NULL)
	{
		FasttrunStatsSavedState *popped = entry->undo;

		entry->undo = popped->older;
		if (popped->statsTuple != NULL)
			heap_freetuple(popped->statsTuple);
		pfree(popped);
	}
}

/*
 * Drop all stats entries for a given relid.  Called from fasttruncate
 * as a backstop against the freshness check (e.g. if pgstat is off
 * and the hook can't compare counters).
 */
static void
fasttrun_stats_cache_evict_relid(Oid relid, int natts)
{
	AttrNumber		attnum;
	FasttrunStatsKey key;

	if (fasttrun_stats_cache == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.inh = false;

	for (attnum = 1; attnum <= natts; attnum++)
	{
		FasttrunStatsEntry *entry;

		key.attnum = attnum;
		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
		if (entry == NULL)
			continue;
		fasttrun_stats_entry_free_undo(entry);
		if (entry->statsTuple != NULL)
			heap_freetuple(entry->statsTuple);
		(void) hash_search(fasttrun_stats_cache, &key, HASH_REMOVE, NULL);
	}
}

/*
 * Advance collected_* counters on all stats cache entries for relid.
 * This keeps the planner hooks' freshness check in sync after a delta-hit
 * where no re-collection happened (DML below refresh threshold).
 */
static void
fasttrun_stats_cache_advance_freshness(Oid relid, int natts,
									   int64 ins, int64 upd, int64 del,
									   bool truncdropped)
{
	AttrNumber		attnum;
	FasttrunStatsKey key;

	if (fasttrun_stats_cache == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.inh = false;

	for (attnum = 1; attnum <= natts; attnum++)
	{
		FasttrunStatsEntry *entry;

		key.attnum = attnum;
		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
		if (entry == NULL)
			continue;
		entry->collected_ins = ins;
		entry->collected_upd = upd;
		entry->collected_del = del;
		entry->collected_truncdropped = truncdropped;
	}
}

/*
 * SubXact callback: savepoint-aware undo/restore for both the per-column
 * stats cache and the analyze-cache stats baseline.
 *
 * Per-column stats cache (statsTuple + freshness counters):
 *   ABORT_SUB:
 *     - entry has undo → pop top saved state back into entry (full
 *       restore of pre-subxact version);
 *     - no undo → entry was created inside aborting subxact → remove.
 *   COMMIT_SUB: re-label sub-local entries with the parent subid; if
 *     the undo top is already at parent level, collapse it.
 *
 * Analyze-cache stats baseline (used by the refresh-path churn check):
 *   ABORT_SUB:
 *     - baseline was set in this subxact and has undo → pop;
 *     - was set in this subxact and no undo → mark has_stats_baseline
 *       false (it was created from scratch inside the rolled-back
 *       subxact, there is nothing to restore to).
 *   COMMIT_SUB: re-label + collapse undo, same as the stats cache.
 *
 * The delta-math fields of the analyze cache (cached_pages /tuples /
 * inserted /deleted) are NOT touched here: pgstat counters are already
 * subxact-rolled-back by PG core, and delta math / negative new_tuples
 * guard catches any inconsistency on the next call.
 */
static void
fasttrun_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS		status;
	FasttrunStatsEntry *sentry;
	FasttrunAnalyzeCacheEntry *aentry;

	if (event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB)
		return;

	if (fasttrun_stats_cache != NULL)
	{
		hash_seq_init(&status, fasttrun_stats_cache);
		while ((sentry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
		{
			if (sentry->collected_subid != mySubid)
				continue;

			if (event == SUBXACT_EVENT_ABORT_SUB)
			{
				if (sentry->undo != NULL)
				{
					/* Restore pre-subxact version from the undo stack. */
					FasttrunStatsSavedState *popped = sentry->undo;

					if (sentry->statsTuple != NULL)
						heap_freetuple(sentry->statsTuple);
					sentry->statsTuple = popped->statsTuple;
					sentry->collected_ins = popped->collected_ins;
					sentry->collected_upd = popped->collected_upd;
					sentry->collected_del = popped->collected_del;
					sentry->collected_truncdropped = popped->collected_truncdropped;
					sentry->collected_subid = popped->collected_subid;
					sentry->undo = popped->older;
					pfree(popped);	/* popped->statsTuple now owned by entry */
				}
				else
				{
					/* Created in this subxact → drop entirely. */
					if (sentry->statsTuple != NULL)
						heap_freetuple(sentry->statsTuple);
					(void) hash_search(fasttrun_stats_cache, &sentry->key,
									   HASH_REMOVE, NULL);
				}
			}
			else	/* SUBXACT_EVENT_COMMIT_SUB */
			{
				sentry->collected_subid = parentSubid;

				/*
				 * If the undo top was also at parent level, the pre-
				 * and post-store versions now sit at the same subxact
				 * → the undo entry is obsolete (outer rollback would
				 * restore to the same level anyway), collapse it.
				 */
				if (sentry->undo != NULL &&
					sentry->undo->collected_subid == parentSubid)
				{
					FasttrunStatsSavedState *obsolete = sentry->undo;

					if (obsolete->statsTuple != NULL)
						heap_freetuple(obsolete->statsTuple);
					sentry->undo = obsolete->older;
					pfree(obsolete);
				}
			}
		}
	}

	if (fasttrun_analyze_cache != NULL)
	{
		hash_seq_init(&status, fasttrun_analyze_cache);
		while ((aentry = (FasttrunAnalyzeCacheEntry *) hash_seq_search(&status)) != NULL)
		{
			if (aentry->stats_baseline_subid != mySubid)
				continue;

			if (event == SUBXACT_EVENT_ABORT_SUB)
			{
				if (aentry->stats_baseline_undo != NULL)
				{
					FasttrunAnalyzeBaselineUndo *popped =
						aentry->stats_baseline_undo;

					aentry->has_stats_baseline = popped->has_stats_baseline;
					aentry->stats_baseline_inserted = popped->stats_baseline_inserted;
					aentry->stats_baseline_updated = popped->stats_baseline_updated;
					aentry->stats_baseline_deleted = popped->stats_baseline_deleted;
					aentry->stats_baseline_truncdropped = popped->stats_baseline_truncdropped;
					aentry->stats_baseline_subid = popped->stats_baseline_subid;
					aentry->stats_baseline_undo = popped->older;
					pfree(popped);
				}
				else
				{
					/*
					 * Baseline was published from scratch inside this
					 * subxact and is now rolled back.  Mark "no
					 * baseline" so the next refresh check stays away
					 * from the refresh path until a real cold scan
					 * (or a fresh refresh) re-establishes one.
					 */
					aentry->has_stats_baseline = false;
					aentry->stats_baseline_subid = InvalidSubTransactionId;
				}
			}
			else	/* SUBXACT_EVENT_COMMIT_SUB */
			{
				aentry->stats_baseline_subid = parentSubid;

				if (aentry->stats_baseline_undo != NULL &&
					aentry->stats_baseline_undo->stats_baseline_subid == parentSubid)
				{
					FasttrunAnalyzeBaselineUndo *obsolete =
						aentry->stats_baseline_undo;

					aentry->stats_baseline_undo = obsolete->older;
					pfree(obsolete);
				}
			}
		}
	}
}

/* ----------------------------------------------------------------------
 * Core std_typanalyze integration.
 *
 * When fasttrun.use_typanalyze=on (default), fasttrun reuses PostgreSQL's
 * own per-type typanalyze callbacks to compute n_distinct, MCV,
 * histogram, correlation and any type-specific statistics — exactly the
 * same code path that a regular ANALYZE would take, only without ever
 * touching pg_statistic on disk.  Output goes into the same per-(relid,
 * attnum) HeapTuple cache that the lightweight Haas-Stokes path uses,
 * and is read by the same get_relation_stats_hook / get_attavgwidth_hook.
 *
 * The three helpers below replicate the parts of commands/analyze.c we
 * need: examine_attribute (set up VacAttrStats + run typanalyze), the
 * standard fetch func, and the formatting logic from update_attstats
 * (build a pg_statistic-shaped HeapTuple, sans catalog write).
 * ---------------------------------------------------------------------- */

/*
 * Standard fetch function for compute_stats — pulls a column value out
 * of one of our sample tuples.  Mirrors std_fetch_func() in core
 * commands/analyze.c, which is static and not exported.
 */
static Datum
fasttrun_std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			attnum = stats->tupattnum;
	HeapTuple	tuple = stats->rows[rownum];
	TupleDesc	tupDesc = stats->tupDesc;

	return heap_getattr(tuple, attnum, tupDesc, isNull);
}

/*
 * Build a VacAttrStats for one column and run its typanalyze callback.
 * Returns NULL when the column should be skipped (dropped, no usable
 * typanalyze).  All allocations live in anl_context, so the caller can
 * release them in one MemoryContextReset / Delete.
 *
 * Mirrors examine_attribute() in commands/analyze.c (static there).
 *
 * PG 17 reshaped VacAttrStats: it dropped the embedded
 * `Form_pg_attribute attr` and lifted `attstattarget` into a top-level
 * field on the struct.  We don't honour custom per-column
 * attstattarget anyway (temp tables almost never set ALTER COLUMN ...
 * SET STATISTICS), so we just request the default everywhere.
 */
static VacAttrStats *
fasttrun_examine_attribute(Relation rel, int attnum, MemoryContext anl_context)
{
	Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attnum - 1);
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;
	MemoryContext oldcxt;

	if (attr->attisdropped)
		return NULL;

	oldcxt = MemoryContextSwitchTo(anl_context);

	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

#if PG_VERSION_NUM < 170000
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);
#else
	/* PG 17+: attstattarget is a top-level field; use the default. */
	stats->attstattarget = -1;
#endif

	stats->attrtypid = attr->atttypid;
	stats->attrtypmod = attr->atttypmod;
	stats->attrcollid = attr->attcollation;

	typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
	{
		MemoryContextSwitchTo(oldcxt);
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	}
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	MemoryContextSwitchTo(oldcxt);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
		return NULL;

	return stats;
}

/*
 * Build a pg_statistic-shaped HeapTuple from a fully populated
 * VacAttrStats, mirroring the formatting half of update_attstats() in
 * commands/analyze.c — but without ever opening pg_statistic for write.
 *
 * Returned tuple is allocated in CurrentMemoryContext.  Caller is
 * expected to feed it to fasttrun_stats_cache_store(), which deep-copies
 * it into fasttrun_stats_mcxt.
 */
static HeapTuple
fasttrun_build_pg_statistic_tuple(TupleDesc pg_stats_desc, Oid relid,
								  AttrNumber attnum, VacAttrStats *stats)
{
	Datum	values[Natts_pg_statistic];
	bool	nulls[Natts_pg_statistic];
	int		i,
			k,
			n;

	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);

	i = Anum_pg_statistic_stakind1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = Int16GetDatum(stats->stakind[k]);

	i = Anum_pg_statistic_staop1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = ObjectIdGetDatum(stats->staop[k]);

	i = Anum_pg_statistic_stacoll1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = ObjectIdGetDatum(stats->stacoll[k]);

	i = Anum_pg_statistic_stanumbers1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		int			nnum = stats->numnumbers[k];

		if (nnum > 0)
		{
			Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
			ArrayType  *arry;

			for (n = 0; n < nnum; n++)
				numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
			arry = construct_array_builtin(numdatums, nnum, FLOAT4OID);
			values[i++] = PointerGetDatum(arry);
		}
		else
		{
			nulls[i] = true;
			values[i++] = (Datum) 0;
		}
	}

	i = Anum_pg_statistic_stavalues1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		if (stats->numvalues[k] > 0)
		{
			ArrayType  *arry;

			arry = construct_array(stats->stavalues[k],
								   stats->numvalues[k],
								   stats->statypid[k],
								   stats->statyplen[k],
								   stats->statypbyval[k],
								   stats->statypalign[k]);
			values[i++] = PointerGetDatum(arry);
		}
		else
		{
			nulls[i] = true;
			values[i++] = (Datum) 0;
		}
	}

	return heap_form_tuple(pg_stats_desc, values, nulls);
}

/*
 * Compute the maximum sample size that std_typanalyze (or any custom
 * type-specific typanalyze) would request across all columns of rel.
 *
 * Used when fasttrun.sample_rows = -1 ("auto") to size our reservoir
 * sample exactly like core ANALYZE would: each typanalyze callback sets
 * stats->minrows during examine_attribute, and core picks the largest
 * value across all attributes (analyze.c:do_analyze_rel).  We do the
 * same so that the chosen sample is large enough for every column's
 * MCV / histogram / correlation slots.
 *
 * The temporary VacAttrStats objects live in a private mcxt that we
 * delete on the way out — this is just a sizing query, the real
 * collect_and_store path will examine_attribute again.  Each call is
 * O(natts) syscache lookups + a typanalyze function call per column,
 * usually a few hundred microseconds total even on wide tables.  Cheap
 * compared to the actual heap scan.
 *
 * Returns the maximum minrows seen, or 0 if no column has a usable
 * typanalyze (in which case the caller will skip stats collection).
 * A floor of 100 is enforced for sanity — even degenerate cases get
 * a non-trivial sample.
 */
static int
fasttrun_compute_max_minrows(Relation rel)
{
	TupleDesc		tupdesc = RelationGetDescr(rel);
	int				natts = tupdesc->natts;
	int				max_minrows = 0;
	int				a;
	MemoryContext	tmp_ctx;
	MemoryContext	oldcxt;

	tmp_ctx = AllocSetContextCreate(CurrentMemoryContext,
									"fasttrun minrows probe",
									ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(tmp_ctx);

	for (a = 0; a < natts; a++)
	{
		VacAttrStats *stats;

		stats = fasttrun_examine_attribute(rel, a + 1, tmp_ctx);
		if (stats == NULL)
			continue;
		if (stats->minrows > max_minrows)
			max_minrows = stats->minrows;
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmp_ctx);

	if (max_minrows > 0 && max_minrows < 100)
		max_minrows = 100;

	return max_minrows;
}

/*
 * Resolve fasttrun.sample_rows GUC into the actual reservoir size.
 *   < 0 → autosize via fasttrun_compute_max_minrows() (full ANALYZE
 *         parity, follows default_statistics_target);
 *   = 0 → caller treats as "stats collection disabled";
 *   > 0 → explicit override.
 */
static int
fasttrun_effective_sample_target(Relation rel)
{
	if (fasttrun_sample_rows < 0)
	{
		/* Auto sizing via typanalyze minrows. If use_typanalyze=off,
		 * don't call typanalyze callbacks -- fall back to default. */
		if (!fasttrun_use_typanalyze)
			return 3000;
		return fasttrun_compute_max_minrows(rel);
	}
	return fasttrun_sample_rows;
}

/* qsort comparator: order HeapTuples by physical TID (block, offset). */
static int
fasttrun_cmp_heap_tuples_by_tid(const void *a, const void *b)
{
	HeapTuple	ta = *(HeapTuple *) a;
	HeapTuple	tb = *(HeapTuple *) b;

	return ItemPointerCompare(&ta->t_self, &tb->t_self);
}

/*
 * For every column of rel, compute stats from the sample and store in
 * the session-local cache.  Columns without a btree-orderable type are
 * skipped (planner falls back to defaults).  Also captures the current
 * pgstat counters as a freshness baseline — the planner hook compares
 * against them and refuses to return stale stats after DML.
 *
 * Two implementations of "compute the stats" coexist, switched by
 * fasttrun.use_typanalyze (default on):
 *   * on  → fasttrun_collect_via_typanalyze: full core std_typanalyze
 *           with MCV / histogram / correlation;
 *   * off → fasttrun_collect_via_haas_stokes: lightweight n_distinct /
 *           null_frac / width only.
 * Both branches end at the same fasttrun_stats_cache_store() and
 * publish through the same hooks.
 */
static void
fasttrun_collect_and_store(Relation rel, HeapTuple *sample, int sample_count,
						   int64 totalrows, bool sample_needs_tid_sort)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Relation	pg_stats_rel;
	TupleDesc	pg_stats_desc;
	int			natts = tupdesc->natts;
	int			a;
	MemoryContext per_col_mcxt;
	MemoryContext oldcxt;
	int64		snap_ins = 0;
	int64		snap_upd = 0;
	int64		snap_del = 0;
	bool		snap_truncdropped = false;

	if (sample_count <= 0)
		return;

	/*
	 * track_counts=off (or pgstat otherwise unavailable) → no baseline
	 * for the freshness check → do not cache at all, the planner will
	 * fall back to defaults.  This is a "silent killer" — plans
	 * silently degrade without any explicit signal, so we raise a
	 * WARNING once per backend to make DBAs see it in the server log
	 * (and the psql client sees it too).
	 */
	if (!fasttrun_read_pgstat_counters(rel, &snap_ins, &snap_upd, &snap_del,
									   &snap_truncdropped))
	{
		if (!fasttrun_warned_track_counts_off)
		{
			ereport(WARNING,
					(errmsg("fasttrun: pgstat tracking is unavailable, column statistics will not be cached for this session"),
					 errdetail("fasttrun_analyze and fasttrun_collect_stats rely on per-backend pgstat counters to validate cached column statistics after DML. "
							   "With pgstat unavailable (usually because track_counts = off), the planner hook cannot return cached stats and will fall back to default selectivity. "
							   "Plans for temp tables may silently degrade."),
					 errhint("Set track_counts = on in postgresql.conf or per session (SET track_counts = on) to re-enable fasttrun column statistics caching.")));
			fasttrun_warned_track_counts_off = true;
		}
		return;
	}

	pg_stats_rel = table_open(StatisticRelationId, AccessShareLock);
	pg_stats_desc = RelationGetDescr(pg_stats_rel);

	per_col_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "fasttrun collect tmp",
										 ALLOCSET_DEFAULT_SIZES);

	if (fasttrun_use_typanalyze)
	{
		/* ----------------------------------------------------------
		 * Branch A: full core std_typanalyze.
		 *
		 * Sort the sample by physical TID first.  compute_scalar_stats
		 * (and other compute_stats variants) use the array position as
		 * the physical-order rank when computing correlation.  Core
		 * acquire_sample_rows() returns rows in block/offset order;
		 * reservoir sampling places them in random slots.  Without
		 * this sort, correlation degrades to ~0 even on perfectly
		 * ordered data.
		 *
		 * For each non-dropped column we set up a fresh VacAttrStats
		 * inside per_col_mcxt, point its standard fetch fields at our
		 * sample, run compute_stats, serialize the result into a
		 * pg_statistic-shaped HeapTuple and hand it to the cache.
		 * Then reset per_col_mcxt to free everything compute_stats
		 * allocated for this column in one shot.
		 * ---------------------------------------------------------- */
		if (sample_needs_tid_sort && sample_count > 1)
			qsort(sample, sample_count, sizeof(HeapTuple),
				  fasttrun_cmp_heap_tuples_by_tid);

		for (a = 0; a < natts; a++)
		{
			Form_pg_attribute	attr = TupleDescAttr(tupdesc, a);
			AttrNumber			attnum = a + 1;
			VacAttrStats	   *stats;
			HeapTuple			stats_tuple;

			if (attr->attisdropped)
				continue;

			CHECK_FOR_INTERRUPTS();
			MemoryContextReset(per_col_mcxt);
			oldcxt = MemoryContextSwitchTo(per_col_mcxt);

			stats = fasttrun_examine_attribute(rel, attnum, per_col_mcxt);
			if (stats == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			/*
			 * Wire up the std_fetch_func plumbing so compute_stats can
			 * pull column values out of our sample[] array.
			 */
			stats->rows = sample;
			stats->tupDesc = tupdesc;
			/* tupattnum already set by fasttrun_examine_attribute */

			stats->compute_stats(stats, fasttrun_std_fetch_func,
								 sample_count, (double) totalrows);

			if (!stats->stats_valid)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			/* Build tuple inside per_col_mcxt so intermediates are
			 * reclaimed on the next MemoryContextReset. */
			stats_tuple = fasttrun_build_pg_statistic_tuple(pg_stats_desc,
															RelationGetRelid(rel),
															attnum, stats);
			MemoryContextSwitchTo(oldcxt);

			/* cache_store copies tuple into fasttrun_stats_mcxt */
			fasttrun_stats_cache_store(RelationGetRelid(rel), attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped);
			heap_freetuple(stats_tuple);
		}
	}
	else
	{
		Datum	   *values;

		/* ----------------------------------------------------------
		 * Branch B: lightweight Haas-Stokes path.
		 *
		 * Computes only n_distinct / null_frac / width via a per-
		 * column qsort_arg.  No MCV / histogram / correlation, no
		 * type-specific stats — but ~5x cheaper on wide tables and
		 * available as a fallback if a particular type-specific
		 * typanalyze misbehaves.
		 * ---------------------------------------------------------- */
		values = (Datum *) palloc(sizeof(Datum) * sample_count);

		for (a = 0; a < natts; a++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, a);
			AttrNumber		   attnum = a + 1;
			TypeCacheEntry	  *typentry;
			SortSupportData	ssup;
			int				   n_nonnull = 0;
			int				   n_null = 0;
			int64			   total_width = 0;
			int				   ndistinct = 0;
			int				   nmultiple = 0;
			int				   dups_cnt = 0;
			int				   i;
			float4			   stanullfrac;
			int32			   stawidth;
			float4			   stadistinct;
			HeapTuple		   stats_tuple;

			if (attr->attisdropped)
				continue;

			typentry = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);
			if (!OidIsValid(typentry->lt_opr))
				continue;	/* no btree comparator → skip */

			MemoryContextReset(per_col_mcxt);
			oldcxt = MemoryContextSwitchTo(per_col_mcxt);

			for (i = 0; i < sample_count; i++)
			{
				bool	isnull;
				Datum	d = heap_getattr(sample[i], attnum, tupdesc, &isnull);

				if (isnull)
				{
					n_null++;
					continue;
				}

				if (attr->attbyval)
				{
					values[n_nonnull] = d;
				}
				else
				{
					/* Detoast varlena values for sort stability. */
					if (attr->attlen == -1)
					{
						struct varlena *detoasted = pg_detoast_datum_packed(
							(struct varlena *) DatumGetPointer(d));
						values[n_nonnull] = PointerGetDatum(detoasted);
						total_width += VARSIZE_ANY_EXHDR(detoasted);
					}
					else
					{
						values[n_nonnull] = d;
						total_width += attr->attlen;
					}
				}

				if (attr->attbyval || attr->attlen >= 0)
					total_width += (attr->attlen > 0 ? attr->attlen : 0);

				n_nonnull++;
			}

			if (n_nonnull == 0)
			{
				stanullfrac = 1.0f;
				stawidth = 0;
				stadistinct = 0.0f;
			}
			else
			{
				memset(&ssup, 0, sizeof(ssup));
				ssup.ssup_cxt = per_col_mcxt;
				ssup.ssup_collation = attr->attcollation;
				ssup.ssup_nulls_first = false;
				PrepareSortSupportFromOrderingOp(typentry->lt_opr, &ssup);

				qsort_arg(values, n_nonnull, sizeof(Datum),
						  fasttrun_datum_cmp, &ssup);

				/* Count distinct values and multiples in the sorted run. */
				dups_cnt = 1;
				for (i = 1; i <= n_nonnull; i++)
				{
					bool	end_of_run;

					if (i == n_nonnull)
						end_of_run = true;
					else
						end_of_run = ApplySortComparator(values[i - 1], false,
														 values[i], false,
														 &ssup) != 0;
					if (end_of_run)
					{
						ndistinct++;
						if (dups_cnt > 1)
							nmultiple++;
						dups_cnt = 1;
					}
					else
						dups_cnt++;
				}

				stanullfrac = (float4) n_null / (float4) sample_count;
				stawidth = (int32) (total_width / n_nonnull);
				stadistinct = fasttrun_estimate_ndistinct(n_nonnull, ndistinct,
														  nmultiple, totalrows);
			}

			MemoryContextSwitchTo(oldcxt);

			stats_tuple = fasttrun_build_stats_tuple(pg_stats_desc,
													 RelationGetRelid(rel),
													 attnum,
													 stanullfrac, stawidth,
													 stadistinct);

			fasttrun_stats_cache_store(RelationGetRelid(rel), attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped);
			heap_freetuple(stats_tuple);
		}

		pfree(values);
	}

	MemoryContextDelete(per_col_mcxt);
	table_close(pg_stats_rel, AccessShareLock);

	/*
	 * Note: the stats-collection baseline (used by the delta-hit refresh
	 * path to decide whether DML churn warrants another sample) lives in
	 * the analyze cache entry, not in the per-(relid,attnum) stats cache.
	 * It is set by the callers of this function — fasttrun_analyze()
	 * cold path and refresh path — both of which already own (or just
	 * created) an analyze cache entry to attach it to.  fasttrun_collect_
	 * stats() does NOT touch the baseline, by design: it is a "give me
	 * column stats now" entry point that doesn't participate in delta-
	 * math state tracking and shouldn't interfere with it.
	 */
}

/*
 * Sequential scan + reservoir sampling.  Returns exact row count via
 * *tuples_out and up to sample_target tuples via the pre-allocated
 * sample[] array (heap_copytuple'd in the current memory context).
 *
 * Used by the cold-scan path where we NEED the exact row count to
 * populate the analyze cache (pgstat counters can't give it to us).
 * For the delta-hit refresh path, fasttrun_scan_block_sample is far
 * cheaper.
 */
static void
fasttrun_scan_with_sample(Relation rel, int64 *tuples_out,
						  HeapTuple *sample, int sample_target,
						  int *sample_count_out)
{
	TableScanDesc	scan;
	Snapshot		snap;
	HeapTuple		tuple;
	int64			tuples_count = 0;
	int				sample_count = 0;

	snap = GetActiveSnapshot();
	if (snap == NULL)
		snap = GetTransactionSnapshot();

	scan = table_beginscan(rel, snap, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		tuples_count++;

		if (sample == NULL || sample_target <= 0)
			continue;

		if (sample_count < sample_target)
			sample[sample_count++] = heap_copytuple(tuple);
		else
		{
			uint64	r = pg_prng_uint64_range(&fasttrun_prng_state,
											 0, (uint64) tuples_count - 1);
			if (r < (uint64) sample_target)
			{
				heap_freetuple(sample[r]);
				sample[r] = heap_copytuple(tuple);
			}
		}
	}
	table_endscan(scan);

	*tuples_out = tuples_count;
	*sample_count_out = sample_count;
}

/*
 * Block-based sampling via PG's BlockSampler (Knuth Algorithm S),
 * combined with Vitter's reservoir sampling (Algorithm R) over the
 * tuples that live inside the chosen blocks.
 *
 * Why both layers:
 *   * BlockSampler picks a small random subset of heap blocks, so
 *     we read O(sample_target) pages instead of the full heap;
 *   * Reservoir sampling over the tuples inside those blocks gives
 *     each visible tuple in the sampled-block universe an equal
 *     probability of landing in the output, removing the bias the
 *     earlier "stop after sample_target tuples" version had: since
 *     BlockSampler_Next yields chosen blocks in ascending block
 *     order, the early-stop version filled the reservoir from the
 *     physically lowest pages and never reached later pages — fatal
 *     for tables with order-correlated values (e.g. first 3k rows
 *     all share the same grp, the rest are unique → refresh would
 *     collapse n_distinct to ~1).
 *
 * Caller knows the row count (delta math), so we don't return one.
 * Caller is responsible for pfree'ing the sample array; this function
 * heap_copytuple's into sample[] and heap_freetuple's evicted entries
 * itself.
 *
 * Assumes heap AM (caller must have checked rd_tableam earlier).
 */
static void
fasttrun_scan_block_sample(Relation rel,
						   HeapTuple *sample, int sample_target,
						   int *sample_count_out)
{
	BlockSamplerData	bs;
	BlockNumber			nblocks;
	BlockNumber			targblocks;
	Snapshot			snap;
	int					sample_count = 0;
	int64				seen = 0;		/* visible tuples processed so far */
	uint32				seed;
	double				density;

	*sample_count_out = 0;

	nblocks = RelationGetNumberOfBlocks(rel);
	if (nblocks == 0 || sample_target <= 0)
		return;

	/*
	 * Convert tuple-based sample_target into a block count.
	 * Estimate density from reltuples/relpages (already published
	 * by the delta-math path that called us).  Floor at 10 tuples
	 * per page to avoid reading the entire heap on sparse tables.
	 */
	density = rel->rd_rel->reltuples / Max(rel->rd_rel->relpages, 1);
	if (density < 10.0)
		density = 10.0;
	targblocks = (BlockNumber) Min((double) sample_target / density + 1.0,
								   (double) nblocks);

	snap = GetActiveSnapshot();
	if (snap == NULL)
		snap = GetTransactionSnapshot();

	seed = (uint32) pg_prng_uint32(&fasttrun_prng_state);
	BlockSampler_Init(&bs, nblocks, targblocks, seed);

	while (BlockSampler_HasMore(&bs))
	{
		BlockNumber		blkno;

		CHECK_FOR_INTERRUPTS();
		blkno = BlockSampler_Next(&bs);
		Buffer			buf;
		Page			page;
		OffsetNumber	off, maxoff;

		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId			itemid = PageGetItemId(page, off);
			HeapTupleData	htup;

			if (!ItemIdIsNormal(itemid))
				continue;

			htup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			htup.t_len = ItemIdGetLength(itemid);
			htup.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&htup.t_self, blkno, off);

			if (!HeapTupleSatisfiesVisibility(&htup, snap, buf))
				continue;

			/*
			 * Vitter Algorithm R.  `seen` is the 0-based index of the
			 * current tuple within the visible-tuple stream produced
			 * by all sampled blocks combined.
			 *
			 *   - while sample_count < sample_target: just append;
			 *   - afterwards: pick j uniformly from [0, seen], and if
			 *     j < sample_target replace sample[j] with the new
			 *     tuple.  This gives every tuple in the stream the
			 *     same sample_target/(seen+1) survival probability at
			 *     each step, which yields a uniform sample of size
			 *     sample_target across the whole sampled-block universe.
			 */
			if (sample_count < sample_target)
			{
				sample[sample_count++] = heap_copytuple(&htup);
			}
			else
			{
				uint64	j = pg_prng_uint64_range(&fasttrun_prng_state,
												 0, (uint64) seen);

				if (j < (uint64) sample_target)
				{
					heap_freetuple(sample[j]);
					sample[j] = heap_copytuple(&htup);
				}
			}
			seen++;
		}

		UnlockReleaseBuffer(buf);
	}

	*sample_count_out = sample_count;
}

/*
 * Unlink every segment of a relation fork file.
 *
 * heap files are split into 1 GB segments with suffixes .1, .2, ...
 * starting from segment 1; segment 0 has no suffix.  We loop until the
 * next segment doesn't exist (ENOENT) and stop.
 *
 * On unlink failure for any other reason we ereport(WARNING) and bail
 * out — the caller (fasttrun_smgr_bypass_truncate) treats partial
 * unlink as a hard error because the relation will be in a half-state
 * otherwise.
 */
static void
fasttrun_unlink_fork_segments(const char *base_path)
{
	int			seg = 0;
	char		path[MAXPGPATH];

	for (;;)
	{
		if (seg == 0)
			strlcpy(path, base_path, sizeof(path));
		else
			snprintf(path, sizeof(path), "%s.%d", base_path, seg);

		if (unlink(path) < 0)
		{
			if (errno == ENOENT)
				return;	/* no more segments — normal end */
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("fasttrun: could not unlink relation segment \"%s\": %m",
							path)));
		}
		seg++;
	}
}

/*
 * Zero-sinval physical truncation of a temp relation.
 *
 * Functionally equivalent to heap_truncate_one_rel(rel) — empties the
 * heap to zero blocks and discards local buffers — but does NOT call
 * smgrtruncate() and therefore never reaches CacheInvalidateSmgr().
 * That removes the last shared-invalidation message that fasttruncate
 * was sending; the path is now literally zero sinval.
 *
 * How:
 *   1. Discover all forks that currently exist (main / fsm / vm).
 *   2. Drop their local buffers via DropRelationLocalBuffers (temp
 *      tables live in the local buffer pool, not shared).
 *   3. Physically unlink every segment of every fork via raw unlink()
 *      on the path returned by relpathbackend().
 *   4. Reset smgr_cached_nblocks (and smgr_targblock) to invalid so
 *      the next access reopens correctly.
 *   5. Recreate empty fork files via smgrcreate(), which has no
 *      built-in invalidation.
 *
 * The whole operation goes through public smgr.h / buf_internals.h /
 * relpath.h API — no internal smgrsw[] poking.  GetRelationPath returns
 * a palloc'd char* in PG <18 and a stack-allocated RelPathStr in PG 18+,
 * so the wrapping is version-conditional.
 *
 * Caller has already taken AccessExclusiveLock and verified
 * relpersistence='t' + heap AM.
 */
static void
fasttrun_smgr_bypass_truncate(Relation rel)
{
	SMgrRelation			reln;
	RelFileLocatorBackend	rlocator;
	ForkNumber				forks[MAX_FORKNUM + 1];
	int						nforks = 0;
	int						i;

	/*
	 * Self-defense: this helper unlinks files via raw POSIX unlink() and
	 * uses DropRelationLocalBuffers, both of which only make sense on a
	 * temp relation owned by this backend.  fasttruncate() already
	 * checks isTempNamespace + heap-AM in the caller, but a stray direct
	 * call here on a non-temp rel would silently corrupt that rel's
	 * storage.  Re-check explicitly.
	 */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
		elog(ERROR, "fasttrun_smgr_bypass_truncate: relation \"%s\" is not temporary",
			 RelationGetRelationName(rel));

	reln = RelationGetSmgr(rel);

	/*
	 * Snapshot smgr_rlocator now: relpathbackend() needs both the
	 * locator and the backend id, and we'll keep using them after
	 * smgrrelease(). The struct is plain POD, copy by value is safe.
	 */
	rlocator = reln->smgr_rlocator;

	/* 1. Detect existing forks. */
	forks[nforks++] = MAIN_FORKNUM;	/* always exists for a heap */
	if (smgrexists(reln, FSM_FORKNUM))
		forks[nforks++] = FSM_FORKNUM;
	if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
		forks[nforks++] = VISIBILITYMAP_FORKNUM;

	/* 2. Drop local buffers for every fork (temp tables → local pool). */
	for (i = 0; i < nforks; i++)
		DropRelationLocalBuffers(rlocator.locator, forks[i], 0);

	/*
	 * 3. Release all cached file descriptors and per-fork state inside the
	 * SMgrRelation BEFORE we touch the files on disk.  Without this, the
	 * MdfdVec arrays inside md.c keep pointing at the inodes we are about
	 * to unlink, and the next mdcreate() would resize those arrays via
	 * repalloc() rather than rebuild them from scratch — leaving stale fd
	 * entries for any segment beyond segno 0.  Calling smgrrelease() runs
	 * mdclose() for every fork, which properly closes every cached fd and
	 * trims md_seg_fds[] back to zero, so the post-unlink smgrcreate()
	 * starts from a clean slate.  smgrrelease() does NOT call
	 * CacheInvalidateSmgr(), so the path stays sinval-free.
	 *
	 * smgrrelease() does NOT destroy the SMgrRelation in any supported PG
	 * version: in PG 16 the rel is owned via smgrsetowner(); in PG 17/18
	 * RelationGetSmgr() pinned it via smgrpin().  Either way `reln` stays
	 * a valid pointer through this whole function.
	 */
	smgrrelease(reln);

	/* 4. Physically unlink every segment of every fork. */
	for (i = 0; i < nforks; i++)
	{
#if PG_VERSION_NUM >= 180000
		RelPathStr	rpath = relpathbackend(rlocator.locator,
										   rlocator.backend, forks[i]);
		const char *base = rpath.str;
#else
		char	   *base = relpathbackend(rlocator.locator,
										  rlocator.backend, forks[i]);
#endif

		fasttrun_unlink_fork_segments(base);

#if PG_VERSION_NUM < 180000
		pfree(base);
#endif
	}

	/*
	 * 5. Re-fetch the SMgrRelation pointer.  smgrrelease() left it valid,
	 * but PG style guidance is to re-execute RelationGetSmgr() after
	 * any operation that may have closed underlying file handles, rather
	 * than holding the pointer across the call.  Cheap (likely path is
	 * a single field load).
	 */
	reln = RelationGetSmgr(rel);

	/*
	 * 6. Recreate empty fork files via smgrcreate.  smgrcreate() does NOT
	 * call CacheInvalidateSmgr(), so this entire path stays sinval-free.
	 * smgrrelease above already cleared smgr_cached_nblocks and
	 * smgr_targblock, but we re-set the freshly-created forks to 0 blocks
	 * so the next size query is a hit and not a re-probe via lseek.
	 */
	for (i = 0; i < nforks; i++)
	{
		smgrcreate(reln, forks[i], false);
		reln->smgr_cached_nblocks[forks[i]] = 0;
	}
}

/*
 * Zero-sinval truncate of a relation AND all its indexes and toast.
 *
 * Mirrors heap_truncate_one_rel():
 *   1. Truncate the heap via fasttrun_smgr_bypass_truncate.
 *   2. For each index: truncate storage, then rebuild the empty index
 *      structure (metapage) via the AM's ambuild callback directly —
 *      NOT through index_build(), because index_build calls
 *      index_update_stats → systable_inplace_update_finish on pg_class,
 *      which generates shared cache invalidation messages.
 *   3. If a toast table exists, truncate it and its indexes the same way.
 */
/*
 * Truncate and rebuild one index via the zero-sinval path.
 * Factored out to keep the PG_TRY wrapper readable.
 */
static void
fasttrun_rebuild_one_index(Relation heaprel, Relation indexrel)
{
	IndexInfo  *indexInfo = BuildDummyIndexInfo(indexrel);

	fasttrun_smgr_bypass_truncate(indexrel);

	if (indexrel->rd_amcache)
	{
		pfree(indexrel->rd_amcache);
		indexrel->rd_amcache = NULL;
	}

	pfree(indexrel->rd_indam->ambuild(heaprel, indexrel, indexInfo));

	RelationGetSmgr(indexrel)->smgr_cached_nblocks[MAIN_FORKNUM] =
		InvalidBlockNumber;

	indexrel->rd_rel->relpages =
		RelationGetNumberOfBlocks(indexrel);
	indexrel->rd_rel->reltuples = 0;
}

static void
fasttrun_full_bypass_truncate(Relation rel)
{
	ListCell   *indlist;
	Oid			toastrelid;

	/* 1. Truncate the heap itself. */
	fasttrun_smgr_bypass_truncate(rel);

	/*
	 * 2-3. Rebuild indexes and toast.  If any step fails (OOM in ambuild,
	 * disk error in smgrcreate), the table is left in a half-truncated
	 * state: heap is empty but some indexes may lack a metapage.  Since
	 * this is a temp table owned exclusively by this backend, the damage
	 * is contained.  We emit a WARNING so the user knows to DROP and
	 * recreate the table.
	 */
	PG_TRY();
	{
		foreach(indlist, RelationGetIndexList(rel))
		{
			Relation	currentIndex;

			currentIndex = index_open(lfirst_oid(indlist), AccessExclusiveLock);
			fasttrun_rebuild_one_index(rel, currentIndex);
			index_close(currentIndex, NoLock);
		}

		toastrelid = rel->rd_rel->reltoastrelid;
		if (OidIsValid(toastrelid))
		{
			Relation	toastrel = table_open(toastrelid, AccessExclusiveLock);

			fasttrun_smgr_bypass_truncate(toastrel);

			foreach(indlist, RelationGetIndexList(toastrel))
			{
				Relation	currentIndex;

				currentIndex = index_open(lfirst_oid(indlist),
										  AccessExclusiveLock);
				fasttrun_rebuild_one_index(toastrel, currentIndex);
				index_close(currentIndex, NoLock);
			}

			table_close(toastrel, NoLock);
		}
	}
	PG_CATCH();
	{
		ereport(WARNING,
				(errmsg("fasttrun: index rebuild failed for relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("The table may have inconsistent indexes. "
						   "DROP and recreate the temporary table to recover.")));
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * fasttruncate(text)
 *
 * Quickly empty a temporary table.
 *
 * Behaviour:
 *   * Resolves the supplied (possibly schema-qualified) name to an OID,
 *     taking AccessExclusiveLock on the relation.  If the relation does
 *     not exist, the function silently does nothing — this lets callers
 *     drop the usual `IF EXISTS` / pre-check pattern and simplifies the
 *     recovery logic of bulk PL/pgSQL routines.
 *   * Verifies the relation lives in a temp namespace; otherwise raises
 *     an error (this prevents accidentally truncating a regular table).
 *   * Cheap empty-check via RelationGetNumberOfBlocks (single smgr
 *     call, no catalog touch); skips the truncate on empty heap.
 *   * If non-empty, calls heap_truncate_one_rel() directly — bypasses
 *     the heap_truncate(List*) wrapper (avoids a redundant table_open)
 *     AND heap_truncate_check_FKs() (see FK-unsafe warning below).
 *   * Drops any cached fasttrun_analyze entry for this OID.
 *
 * IMPORTANT: this function is FOREIGN-KEY-UNSAFE.  fasttruncate does
 * NOT scan pg_constraint.  If you have FKs involving temp tables, use
 * SQL TRUNCATE instead.  Workloads this extension is built for never
 * declare FKs on temp tables — we skip the check by design.
 *
 * Importantly, fasttruncate() does NOT call vacuum() or analyze_rel()
 * after the truncate.  Earlier versions of this extension used to do so,
 * but on a busy server with many concurrent backends that pattern is
 * catastrophic: each ANALYZE generates dozens of pg_class / pg_statistic
 * invalidation messages, the shared invalidation queue overflows, and
 * every backend ends up spending all of its CPU draining that queue
 * inside ReceiveSharedInvalidMessages() rather than doing useful work.
 *
 * Skipping ANALYZE is functionally safe.  The planner reads the current
 * page count via RelationGetNumberOfBlocks() (which always reflects the
 * real on-disk state, not stale catalogue stats), and falls back to a
 * default density estimate when reltuples / relpages are stale.  For
 * temp tables that are immediately re-filled in the same transaction
 * the catalogue stats would be wrong either way until the next ANALYZE.
 *
 * Warning: this function is NOT transaction-safe.  heap_truncate()
 * physically discards storage and the operation cannot be rolled back
 * cleanly if the surrounding transaction aborts.  Use only on temp
 * tables that the calling code is willing to lose on abort.
 */
Datum
fasttruncate(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	RangeVar	   *relvar;
	Oid				relOid;
	Relation		rel;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessExclusiveLock, true);

	/*
	 * Missing table — silently bail out (see function header).
	 */
	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttruncate: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	/* Heap-AM only. */
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttruncate: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}

	/* Same activity check as core TRUNCATE -- rejects open cursors etc. */
	CheckTableNotInUse(rel, "fasttruncate");

	/* Cheap empty-check via smgr; skip heap_truncate_check_FKs (FK-unsafe). */
	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		if (fasttrun_zero_sinval_truncate)
			fasttrun_full_bypass_truncate(rel);
		else
			heap_truncate_one_rel(rel);
	}

	/*
	 * Re-seed (not just evict) the analyze cache so the first post-
	 * refill fasttrun_analyze() takes the delta hot path.  See header
	 * comment on fasttrun_cache_seed_after_truncate() for the math.
	 */
	fasttrun_cache_seed_after_truncate(rel);
	fasttrun_stats_cache_evict_relid(relOid, RelationGetDescr(rel)->natts);

	/*
	 * Zero out in-memory relpages/reltuples so the planner sees an empty
	 * table.  pg_class on disk is untouched (no sinval).
	 * fasttrun_analyze() will publish real stats after the next refill.
	 */
	rel->rd_rel->relpages = 0;
	rel->rd_rel->reltuples = 0;
	rel->rd_rel->relallvisible = 0;

	/* Release the AccessExclusiveLock taken by RangeVarGetRelid. */
	table_close(rel, AccessExclusiveLock);

	PG_RETURN_VOID();
}

/*
 * fasttrun_analyze(text)
 *
 * In-memory ANALYZE for a temporary table that does NOT generate any
 * shared invalidation messages.  This is the workhorse function for
 * replacing thousands of explicit "ANALYZE temp_xxx" statements scattered
 * across PL/pgSQL code without paying the catalog-update / sinval-storm
 * cost that an ordinary ANALYZE would incur.
 *
 * Behaviour:
 *
 *   * Resolves the relation name; silently returns on a missing
 *     relation, raises ERROR on a non-temp relation (same contract as
 *     fasttruncate).
 *
 *   * Reads the actual on-disk page count via RelationGetNumberOfBlocks
 *     (this never touches the catalog).
 *
 *   * Consults the lazy-mode cache.  Cache hit reuses cached_tuples +
 *     delta_ins - delta_del without touching the heap; cache hit
 *     refreshes the snapshot in place so the next call sees only
 *     the next delta.
 *
 *   * On cache miss (first call, page count dropped, truncdropped
 *     bit changed, or pgstat off) does a full sequential scan with
 *     reservoir sampling, optionally collecting per-column stats
 *     (see fasttrun.auto_collect_stats GUC), and populates the cache.
 *
 * The published reltuples/relpages are visible only inside the current
 * backend's relcache.  pg_class on disk is NOT modified.
 *
 * Column-level coverage: if auto_collect_stats is on (default), the
 * cold scan collects full per-column stats via the planner hook.
 * With use_typanalyze=on (default) this includes MCV, histogram,
 * correlation — same quality as regular ANALYZE.
 */
Datum
fasttrun_analyze(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	RangeVar	   *relvar;
	Oid				relOid;
	Relation		rel;
	BlockNumber		pages_now;
	int64			tuples_count = 0;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;
	bool			have_counters;
	bool			scan_needed = true;
	FasttrunAnalyzeCacheEntry *entry;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);

	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_analyze: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	/* heap_getnext + delta math assume heap-AM. */
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_analyze: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}

	pages_now = RelationGetNumberOfBlocks(rel);
	have_counters = fasttrun_read_pgstat_counters(rel,
												  &ins_now, &upd_now, &del_now,
												  &truncdropped_now);

	/* Lazy-mode delta hit: see header comment block above. */
	if (have_counters)
	{
		entry = fasttrun_cache_lookup(relOid);
		if (entry != NULL
			&& pages_now >= entry->cached_pages
			&& entry->cached_truncdropped == truncdropped_now)
		{
			int64	delta_ins = ins_now - entry->cached_inserted;
			int64	delta_del = del_now - entry->cached_deleted;
			int64	new_tuples = entry->cached_tuples + delta_ins - delta_del;

			if (new_tuples >= 0)
			{
				tuples_count = new_tuples;
				scan_needed = false;

				/* Roll the cache forward — next call sees a fresh baseline. */
				entry->cached_pages = pages_now;
				entry->cached_tuples = new_tuples;
				entry->cached_inserted = ins_now;
				entry->cached_deleted = del_now;
				/* truncdropped unchanged (it's a cache-hit precondition) */
			}
		}
	}

	/*
	 * Delta-hit stats refresh: when DML churn since the last collect
	 * exceeds the threshold, do a block-sample scan to refresh column
	 * stats.  Row count stays from the delta computation.
	 */
	if (!scan_needed && have_counters && fasttrun_auto_collect_stats &&
		fasttrun_sample_rows != 0 && fasttrun_stats_refresh_threshold < 1.0 &&
		entry != NULL && entry->has_stats_baseline)
	{
		int64	churn;
		double	baseline;

		/* Compute churn BEFORE advancing baseline. */
		churn = (ins_now - entry->stats_baseline_inserted)
			+ (upd_now - entry->stats_baseline_updated)
			+ (del_now - entry->stats_baseline_deleted);
		if (churn < 0)
			churn = -churn;
		baseline = (double) Max(tuples_count, 1);

		if (churn > 0 &&
			(double) churn / baseline >= fasttrun_stats_refresh_threshold)
		{
			int		sample_target = fasttrun_effective_sample_target(rel);

			if (sample_target > 0)
			{
				HeapTuple  *sample;
				int			sample_count = 0;
				int			i;

				sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
				fasttrun_scan_block_sample(rel, sample, sample_target,
										   &sample_count);
				if (sample_count > 0)
					fasttrun_collect_and_store(rel, sample, sample_count,
											   tuples_count, true);

				for (i = 0; i < sample_count; i++)
					heap_freetuple(sample[i]);
				pfree(sample);
			}
		}

		/* Advance baseline so next delta-hit sees fresh churn. */
		fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
										  del_now, truncdropped_now);
	}

	/*
	 * After a delta-hit, advance collected_* on cached stats entries so
	 * the planner hooks' freshness check doesn't fall back to defaults.
	 *
	 * Skip inside savepoints: advance_freshness writes collected_* without
	 * pushing subxact undo, so ROLLBACK TO SAVEPOINT would leave stale
	 * counters.  At top level there is no rollback risk.
	 */
	if (!scan_needed && have_counters &&
		GetCurrentSubTransactionId() == TopSubTransactionId)
		fasttrun_stats_cache_advance_freshness(relOid,
											   RelationGetDescr(rel)->natts,
											   ins_now, upd_now, del_now,
											   truncdropped_now);

	if (scan_needed)
	{
		HeapTuple  *sample = NULL;
		int			sample_target = 0;
		int			sample_count = 0;

		/* Reservoir-sample inside the same scan if auto-collect is on. */
		if (pages_now > 0 && fasttrun_auto_collect_stats &&
			fasttrun_sample_rows != 0)
		{
			sample_target = fasttrun_effective_sample_target(rel);
			if (sample_target > 0)
				sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
		}

		if (pages_now > 0)
		{
			fasttrun_scan_with_sample(rel, &tuples_count,
									  sample, sample_target, &sample_count);
		}

		if (sample != NULL && sample_count > 0)
		{
			/* Sort needed only when reservoir sampling displaced entries. */
			fasttrun_collect_and_store(rel, sample, sample_count, tuples_count,
									   tuples_count > sample_target);
		}

		if (sample != NULL)
		{
			int i;
			for (i = 0; i < sample_count; i++)
				heap_freetuple(sample[i]);
			pfree(sample);
		}

		/*
		 * Re-read counters: heap_beginscan calls pgstat_count_heap_scan
		 * which lazily inits pgstat_info, so a pre-scan have_counters=false
		 * may flip to true after the scan.  heap_getnext touches only
		 * non-xact counters, so the values we read here are authoritative.
		 */
		if (!have_counters)
			have_counters = fasttrun_read_pgstat_counters(rel,
														  &ins_now, NULL,
														  &del_now,
														  &truncdropped_now);

		/* Populate cache (only when pgstat is usable). */
		if (have_counters)
		{
			entry = fasttrun_cache_enter(relOid);
			entry->cached_pages = pages_now;
			entry->cached_tuples = tuples_count;
			entry->cached_inserted = ins_now;
			entry->cached_deleted = del_now;
			entry->cached_truncdropped = truncdropped_now;

			/* upd_now already captured in the first read_pgstat_counters call */
			fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
											  del_now, truncdropped_now);
		}
	}

	rel->rd_rel->relpages = pages_now;
	rel->rd_rel->reltuples = (float4) tuples_count;

	table_close(rel, AccessShareLock);
	PG_RETURN_VOID();
}

/*
 * fasttrun_relstats(text)
 *
 * Inspection helper.  Returns the in-memory rd_rel statistics for the
 * given relation: the same (relpages, reltuples) tuple the planner uses
 * when costing queries.  Mostly useful for verifying that fasttruncate /
 * fasttrun_analyze actually updated the cached values, and as a SQL-level
 * substitute for poking around with gdb.
 *
 * Like fasttruncate, this function silently returns NULL when the
 * relation does not exist; it raises ERROR when the relation exists but
 * is not a temporary table, because asking about a regular table via this
 * helper is almost certainly a bug.
 */
Datum
fasttrun_relstats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tuple;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
	if (!OidIsValid(relOid))
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	/* Heap-AM guard. */
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}

	values[0] = Int32GetDatum(rel->rd_rel->relpages);
	values[1] = Float4GetDatum(rel->rd_rel->reltuples);

	table_close(rel, AccessShareLock);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * fasttrun_collect_stats(text)
 *
 * Explicit-call sample → per-column n_distinct/null_frac/width →
 * session-local stats cache.  No catalog write.  Same path is taken
 * automatically by fasttrun_analyze on cold scan when
 * fasttrun.auto_collect_stats is on (default).
 */
Datum
fasttrun_collect_stats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;
	int64		tuples_count = 0;
	HeapTuple  *sample;
	int			sample_count = 0;
	int			sample_target;
	int			i;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);

	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_collect_stats: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	if (rel->rd_tableam != GetHeapamTableAmRoutine())
		elog(ERROR, "fasttrun_collect_stats: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));

	{
		sample_target = fasttrun_effective_sample_target(rel);

		if (sample_target <= 0)
		{
			table_close(rel, AccessShareLock);
			PG_RETURN_VOID();
		}

		sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);

		fasttrun_scan_with_sample(rel, &tuples_count,
								  sample, sample_target, &sample_count);
	}

	if (sample_count > 0)
		fasttrun_collect_and_store(rel, sample, sample_count, tuples_count,
								   tuples_count > sample_target);

	for (i = 0; i < sample_count; i++)
		heap_freetuple(sample[i]);
	pfree(sample);

	/*
	 * Advance the analyze-cache stats baseline (if an entry exists)
	 * so the next fasttrun_analyze() does not re-enter the refresh
	 * path on stale pre-collect churn.  We deliberately do NOT seed a
	 * fresh entry here: fasttrun_collect_stats() must not invent
	 * delta-math state behind the user's back.  If the user never
	 * called fasttrun_analyze() for this relid, the auto-refresh
	 * machinery stays disengaged for it — that's the documented
	 * contract.
	 */
	{
		int64	snap_ins = 0;
		int64	snap_upd = 0;
		int64	snap_del = 0;
		bool	snap_truncdropped = false;

		if (fasttrun_read_pgstat_counters(rel, &snap_ins, &snap_upd, &snap_del,
										  &snap_truncdropped))
		{
			fasttrun_cache_update_stats_baseline_if_present(relOid,
															snap_ins, snap_upd,
															snap_del,
															snap_truncdropped);
		}
	}

	table_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * fasttrun_inspect_stats(text) RETURNS SETOF pg_catalog.pg_statistic
 *
 * Production-debugging entry point.  Returns the per-(relid, attnum)
 * statsTuples that the column-stats cache currently holds for the
 * named table — in the **same row shape** as core pg_statistic, so
 * the caller can do
 *
 *   SELECT staattnum, stadistinct, stakind1, stanumbers1, stavalues1
 *   FROM   fasttrun_inspect_stats('temp_xxx');
 *
 * and see exactly what fasttrun_get_relation_stats_hook would feed
 * the planner on the next query against that table.
 *
 * Implementation notes:
 *   * Materialize-mode SRF (we expect a small handful of rows per
 *     call — one per analysed column — so building a tuplestore is
 *     simpler than per-call state and almost free).
 *   * The cached statsTuples are already in pg_statistic shape (built
 *     by fasttrun_build_pg_statistic_tuple from VacAttrStats), so we
 *     just stuff each one straight into the tuplestore.
 *   * If the table doesn't exist, isn't temp, or has no cached stats
 *     yet, the function returns an empty set — same defensive
 *     behaviour as fasttrun_relstats() / fasttrun_analyze() on
 *     missing tables.
 */
Datum
fasttrun_inspect_stats(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	RangeVar	   *relvar;
	Oid				relOid;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_cxt;
	MemoryContext	oldcxt;
	HASH_SEQ_STATUS	status;
	FasttrunStatsEntry *entry;

	/* Check caller is set-aware. */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/* The function is declared as RETURNS SETOF pg_catalog.pg_statistic,
	 * so get_call_result_type will give us the pg_statistic tupdesc. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	per_query_cxt = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_cxt);

	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcxt);

	/* Resolve relation; missing → empty result. */
	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, NoLock, true);
	if (!OidIsValid(relOid))
		return (Datum) 0;

	if (!isTempNamespace(get_rel_namespace(relOid)))
		elog(ERROR, "fasttrun_inspect_stats: relation is not a temporary table");

	/* Empty cache → empty result. */
	if (fasttrun_stats_cache == NULL)
		return (Datum) 0;

	hash_seq_init(&status, fasttrun_stats_cache);
	while ((entry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.relid != relOid)
			continue;
		if (entry->statsTuple == NULL)
			continue;
		tuplestore_puttuple(tupstore, entry->statsTuple);
	}

	return (Datum) 0;
}

/* ================================================================
 * Temp table creation tracking — shared memory + ProcessUtility hook.
 *
 * Counts how often each temp table name is created across all
 * backends.  Used by fasttrun_prewarm() to pre-create only the
 * hottest tables at session init, instead of all 8000.
 *
 * Requires shared_preload_libraries for shmem allocation.
 * ================================================================ */

#define FASTTRUN_TRACK_MAX		8192
#define FASTTRUN_TRACK_FILE		"pg_stat/fasttrun_temp_stats"
#define FASTTRUN_TRACK_MAGIC	0x46545354	/* "FTST" */

typedef struct FasttrunTrackEntry
{
	char			relname[NAMEDATALEN];	/* hash key */
	int64			create_count;
	TimestampTz		last_create;
} FasttrunTrackEntry;

static HTAB			   *fasttrun_track_htab = NULL;
static LWLockId			fasttrun_track_lock;
static bool				fasttrun_track_enabled = true;
static int				fasttrun_prewarm_count = 500;
static char			   *fasttrun_prewarm_schema = "dummy_tmp";
static ProcessUtility_hook_type prev_utility_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/* Shmem request hook — called during postmaster startup. */
static void
fasttrun_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(hash_estimate_size(FASTTRUN_TRACK_MAX,
											  sizeof(FasttrunTrackEntry)));
	RequestNamedLWLockTranche("fasttrun", 1);
}

/* Save tracking stats to disk. Called at server shutdown. */
static void
fasttrun_track_save(int code, Datum arg)
{
	FILE			   *f;
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;
	int32				magic = FASTTRUN_TRACK_MAGIC;
	int32				count = 0;

	if (fasttrun_track_htab == NULL)
		return;

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);

	f = AllocateFile(FASTTRUN_TRACK_FILE ".tmp", PG_BINARY_W);
	if (f == NULL)
	{
		LWLockRelease(fasttrun_track_lock);
		ereport(LOG, (errmsg("fasttrun: could not save temp stats")));
		return;
	}

	fwrite(&magic, sizeof(magic), 1, f);
	/* placeholder for count — will rewrite */
	fwrite(&count, sizeof(count), 1, f);

	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count > 0)
		{
			fwrite(entry, sizeof(FasttrunTrackEntry), 1, f);
			count++;
		}
	}

	LWLockRelease(fasttrun_track_lock);

	/* rewrite count */
	fseek(f, sizeof(magic), SEEK_SET);
	fwrite(&count, sizeof(count), 1, f);

	FreeFile(f);
	(void) durable_rename(FASTTRUN_TRACK_FILE ".tmp",
						  FASTTRUN_TRACK_FILE, LOG);
}

/* Load tracking stats from disk. Called at shmem startup. */
static void
fasttrun_track_load(void)
{
	FILE			   *f;
	int32				magic, count;
	int					i;

	f = AllocateFile(FASTTRUN_TRACK_FILE, PG_BINARY_R);
	if (f == NULL)
		return;		/* no saved stats — fresh start */

	if (fread(&magic, sizeof(magic), 1, f) != 1 || magic != FASTTRUN_TRACK_MAGIC)
	{
		FreeFile(f);
		return;
	}
	if (fread(&count, sizeof(count), 1, f) != 1 || count < 0)
	{
		FreeFile(f);
		return;
	}

	for (i = 0; i < count && i < FASTTRUN_TRACK_MAX; i++)
	{
		FasttrunTrackEntry	buf;
		FasttrunTrackEntry *entry;
		bool				found;

		if (fread(&buf, sizeof(buf), 1, f) != 1)
			break;

		entry = hash_search(fasttrun_track_htab, buf.relname,
							HASH_ENTER, &found);
		if (!found)
		{
			entry->create_count = buf.create_count;
			entry->last_create = buf.last_create;
		}
		else
		{
			entry->create_count += buf.create_count;
			if (buf.last_create > entry->last_create)
				entry->last_create = buf.last_create;
		}
	}

	FreeFile(f);
}

/* Shared memory startup: create hash table + load from disk. */
static void
fasttrun_shmem_startup(void)
{
	HASHCTL		ctl;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(FasttrunTrackEntry);

	fasttrun_track_htab = ShmemInitHash("fasttrun temp track",
										FASTTRUN_TRACK_MAX,
										FASTTRUN_TRACK_MAX,
										&ctl,
										HASH_ELEM | HASH_STRINGS);

	/* Allocate LWLock */
	fasttrun_track_lock = (LWLockId) GetNamedLWLockTranche("fasttrun");

	LWLockRelease(AddinShmemInitLock);

	/* Load saved stats */
	if (!IsUnderPostmaster)
	{
		fasttrun_track_load();
		on_shmem_exit(fasttrun_track_save, (Datum) 0);
	}
}

/* ProcessUtility hook: track CREATE TEMP TABLE. */
static void
fasttrun_utility_hook(PlannedStmt *pstmt,
					  const char *queryString,
					  bool readOnlyTree,
					  ProcessUtilityContext context,
					  ParamListInfo params,
					  QueryEnvironment *queryEnv,
					  DestReceiver *dest,
					  QueryCompletion *qc)
{
	Node   *parsetree = pstmt->utilityStmt;

	/* Track CREATE TEMP TABLE before execution. */
	if (fasttrun_track_enabled && fasttrun_track_htab != NULL &&
		nodeTag(parsetree) == T_CreateStmt)
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;

		if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
		{
			/*
			 * Only track tables created via LIKE from the dummy schema.
			 * User-created tables (CREATE TEMP TABLE foo (id int, ...))
			 * don't have a matching dummy and would break prewarm.
			 */
			ListCell   *lc;
			bool		from_dummy = false;

			foreach(lc, stmt->tableElts)
			{
				if (IsA(lfirst(lc), TableLikeClause))
				{
					TableLikeClause *like = (TableLikeClause *) lfirst(lc);

					if (like->relation->schemaname != NULL &&
						strcmp(like->relation->schemaname,
							   fasttrun_prewarm_schema) == 0)
					{
						from_dummy = true;
						break;
					}
				}
			}

			if (from_dummy)
			{
				FasttrunTrackEntry *entry;
				bool	found;

				LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
				entry = hash_search(fasttrun_track_htab,
									stmt->relation->relname,
									HASH_ENTER_NULL, &found);
				if (entry != NULL)
				{
					if (!found)
					{
						entry->create_count = 0;
						entry->last_create = 0;
					}
					entry->create_count++;
					entry->last_create = GetCurrentTimestamp();
				}
				LWLockRelease(fasttrun_track_lock);
			}
		}
	}

	/* Chain to next hook or standard ProcessUtility. */
	if (prev_utility_hook)
		prev_utility_hook(pstmt, queryString, readOnlyTree, context,
						  params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

/* SQL: fasttrun_hot_temp_tables(n) — returns top-N most created temp tables. */
Datum
fasttrun_hot_temp_tables(PG_FUNCTION_ARGS)
{
	int32				limit = PG_GETARG_INT32(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate	   *tupstore;
	MemoryContext		per_query_cxt, oldcxt;
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;
	FasttrunTrackEntry *sorted;
	int					count = 0;
	int					alloc = 256;
	int					i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	per_query_cxt = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_cxt);
	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcxt);

	if (fasttrun_track_htab == NULL)
		return (Datum) 0;

	/* Collect all entries into a sortable array. */
	sorted = palloc(sizeof(FasttrunTrackEntry) * alloc);

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;
		if (count >= alloc)
		{
			alloc *= 2;
			sorted = repalloc(sorted, sizeof(FasttrunTrackEntry) * alloc);
		}
		memcpy(&sorted[count++], entry, sizeof(FasttrunTrackEntry));
	}
	LWLockRelease(fasttrun_track_lock);

	/* Sort by create_count DESC. */
	qsort(sorted, count, sizeof(FasttrunTrackEntry),
		  fasttrun_track_cmp_desc);

	/* Emit top-N rows. */
	if (limit <= 0 || limit > count)
		limit = count;

	for (i = 0; i < limit; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		values[0] = CStringGetTextDatum(sorted[i].relname);
		values[1] = Int64GetDatum(sorted[i].create_count);
		values[2] = TimestampTzGetDatum(sorted[i].last_create);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	pfree(sorted);
	return (Datum) 0;
}

/* qsort comparator: descending by create_count. */
static int
fasttrun_track_cmp_desc(const void *a, const void *b)
{
	const FasttrunTrackEntry *ea = (const FasttrunTrackEntry *) a;
	const FasttrunTrackEntry *eb = (const FasttrunTrackEntry *) b;

	if (eb->create_count > ea->create_count) return 1;
	if (eb->create_count < ea->create_count) return -1;
	return 0;
}

/* SQL: fasttrun_prewarm() — creates top-N temp tables via create_temp_table. */
Datum
fasttrun_prewarm(PG_FUNCTION_ARGS)
{
	int		limit = fasttrun_prewarm_count;
	int		created = 0;
	int		count = 0;
	int		alloc = 256;
	int		i;
	FasttrunTrackEntry *sorted;
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;

	if (fasttrun_track_htab == NULL)
		PG_RETURN_INT32(0);

	sorted = palloc(sizeof(FasttrunTrackEntry) * alloc);

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;
		if (count >= alloc)
		{
			alloc *= 2;
			sorted = repalloc(sorted, sizeof(FasttrunTrackEntry) * alloc);
		}
		memcpy(&sorted[count++], entry, sizeof(FasttrunTrackEntry));
	}
	LWLockRelease(fasttrun_track_lock);

	qsort(sorted, count, sizeof(FasttrunTrackEntry),
		  fasttrun_track_cmp_desc);

	if (limit <= 0 || limit > count)
		limit = count;

	{
		/* Verify dummy schema exists; if not, skip all prewarm. */
		Oid		nspOid = get_namespace_oid(fasttrun_prewarm_schema, true);

		for (i = 0; i < limit; i++)
		{
			int		ret;

			/* Skip if no matching dummy table in the schema. */
			if (nspOid == InvalidOid ||
				get_relname_relid(sorted[i].relname, nspOid) == InvalidOid)
				continue;

			ret = SPI_connect();
			if (ret != SPI_OK_CONNECT)
				elog(ERROR, "fasttrun_prewarm: SPI_connect failed");

			SPI_execute_with_args(
				"SELECT create_temp_table($1)",
				1,
				(Oid[]) { TEXTOID },
				(Datum[]) { CStringGetTextDatum(sorted[i].relname) },
				NULL, false, 0);

			SPI_finish();
			created++;
		}
	}

	pfree(sorted);
	PG_RETURN_INT32(created);
}

/* SQL: fasttrun_reset_temp_stats() — clears all tracking counters. */
Datum
fasttrun_reset_temp_stats(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;

	if (fasttrun_track_htab == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
		hash_search(fasttrun_track_htab, entry->relname, HASH_REMOVE, NULL);
	LWLockRelease(fasttrun_track_lock);

	/* Remove the on-disk file too. */
	(void) unlink(FASTTRUN_TRACK_FILE);

	PG_RETURN_VOID();
}

/* Define GUCs and install the planner hook. */
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
							"normally 300 * default_statistics_target — i.e. "
							"the same sample size that core ANALYZE would "
							"use, with full MCV / histogram parity).  "
							"0 = disable column-stats collection entirely.  "
							"N > 0 = explicit fixed sample size; default 3000 "
							"is a deliberate trade-off — lighter than core "
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
							 "DML churn ratio (vs reltuples) at which a delta-hit "
							 "fasttrun_analyze triggers a block-sample stats refresh",
							 "Default 0.2 — significant DML (>=20% churn) auto-"
							 "triggers a cheap block-sampled refresh of column "
							 "stats so plans never go stale.  Set to 0 to refresh "
							 "on any DML, set to 1 to disable auto-refresh entirely "
							 "(hot path stays sub-millisecond, but post-DML stats "
							 "stay frozen at the last cold-scan snapshot).",
							 &fasttrun_stats_refresh_threshold,
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
							 "correlation and type-specific stats — same path "
							 "as a regular ANALYZE, only without sinval / "
							 "catalog updates.  When off, falls back to the "
							 "older lightweight path that only computes "
							 "n_distinct/null_frac/width via Haas-Stokes; "
							 "useful for comparing plan quality before / after "
							 "and as a safety fallback if a custom typanalyze "
							 "misbehaves on a particular type.",
							 &fasttrun_use_typanalyze,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("fasttrun.zero_sinval_truncate",
							 "Bypass smgrtruncate to avoid even the single "
							 "SMGR sinval message that fasttruncate normally "
							 "sends",
							 "When on (default), fasttruncate physically "
							 "unlinks heap fork files and recreates them via "
							 "smgrcreate (no built-in invalidation), instead "
							 "of going through heap_truncate_one_rel → "
							 "smgrtruncate → CacheInvalidateSmgr.  This is "
							 "the only path that gives literal zero "
							 "shared-invalidation messages from fasttruncate.  "
							 "Set to off to fall back on the older "
							 "heap_truncate_one_rel path (1 SMGR sinval per "
							 "call) — this is mostly useful as a safety "
							 "switch if the unlink/smgrcreate dance behaves "
							 "weirdly on some custom storage backend.",
							 &fasttrun_zero_sinval_truncate,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	pg_prng_seed(&fasttrun_prng_state, (uint64) MyProcPid);

	/*
	 * Register the xact + subxact callbacks once per backend, here in
	 * _PG_init, so they fire regardless of which cache is lazily
	 * allocated first — or whether only standalone
	 * fasttrun_collect_stats() is ever called.
	 */
	RegisterXactCallback(fasttrun_xact_callback, NULL);
	RegisterSubXactCallback(fasttrun_subxact_callback, NULL);

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = fasttrun_get_relation_stats_hook;
	prev_get_attavgwidth_hook = get_attavgwidth_hook;
	get_attavgwidth_hook = fasttrun_get_attavgwidth_hook;

	/* Tracking GUCs */
	DefineCustomBoolVariable("fasttrun.track_temp_creates",
							 "Track CREATE TEMP TABLE frequency in shared memory",
							 NULL,
							 &fasttrun_track_enabled,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.prewarm_count",
							"Number of hottest temp tables to pre-create in fasttrun_prewarm()",
							NULL,
							&fasttrun_prewarm_count,
							500,
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

	/* Shared memory + utility hook — only when loaded via shared_preload_libraries. */
	if (process_shared_preload_libraries_in_progress)
	{
		prev_shmem_request_hook = shmem_request_hook;
		shmem_request_hook = fasttrun_shmem_request;

		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = fasttrun_shmem_startup;

		prev_utility_hook = ProcessUtility_hook;
		ProcessUtility_hook = fasttrun_utility_hook;
	}
}
