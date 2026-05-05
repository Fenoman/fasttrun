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

#include <ctype.h>
#include <errno.h>
#include <fmgr.h>
#include <funcapi.h>
#include <math.h>
#include <time.h>
#include <unistd.h>

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "common/pg_prng.h"
#include "common/relpath.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "pgtime.h"
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
static void fasttrun_track_schedule_assign_hook(const char *newval, void *extra);

/* GUCs */
static bool		fasttrun_auto_collect_stats = true;
static int		fasttrun_sample_rows = 3000;
static double	fasttrun_stats_refresh_threshold = 0.2;
static bool		fasttrun_use_typanalyze = true;
static bool		fasttrun_zero_sinval_truncate = true;

/*
 * Per-planning freshness cache for column-stats hooks.
 *
 * A single plan can ask get_relation_stats_hook dozens of times for the same
 * temp relation (one call per predicate/column).  The expensive part is not
 * the stats hash lookup, but reopening the relation and walking pgstat xact
 * counters on every call.  While standard_planner runs, cache that pgstat
 * snapshot per relid; outside planner_hook, fall back to direct reads.
 */
#define FASTTRUN_FRESHNESS_CACHE_SLOTS 16

typedef struct FasttrunFreshnessCacheEntry
{
	Oid			relid;
	bool		have;
	int64		ins;
	int64		upd;
	int64		del;
	bool		truncdropped;
} FasttrunFreshnessCacheEntry;

static bool		fasttrun_in_planner = false;
static int		fasttrun_freshness_cache_used = 0;
static FasttrunFreshnessCacheEntry
				fasttrun_freshness_cache[FASTTRUN_FRESHNESS_CACHE_SLOTS];

/* "Silent killer" warning — one shot per backend. */
static bool		fasttrun_warned_track_counts_off = false;

/* RNG state for reservoir sampling */
static pg_prng_state fasttrun_prng_state;

/* Forward decls for stats infrastructure (defined below) */
static void fasttrun_stats_cache_reset(void);
static void fasttrun_stats_cache_commit_xact(void);
static bool fasttrun_stats_cache_evict_relid(Oid relid);
static void fasttrun_subxact_callback(SubXactEvent event,
									  SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);
static void fasttrun_collect_and_store(Relation rel, HeapTuple *sample,
									   int sample_count, int64 totalrows,
									   bool sample_needs_tid_sort);
static bool fasttrun_read_pgstat_counters(Relation rel,
										  int64 *ins, int64 *upd, int64 *del,
										  bool *truncdropped);
static bool fasttrun_read_pgstat_counters_for_hook(Oid relid,
												   int64 *ins, int64 *upd,
												   int64 *del,
												   bool *truncdropped);
static void fasttrun_invalidate_local_plan_cache(Oid relid);
static bool fasttrun_get_relation_stats_hook(PlannerInfo *root,
											 RangeTblEntry *rte,
											 AttrNumber attnum,
											 VariableStatData *vardata);
static int32 fasttrun_get_attavgwidth_hook(Oid relid, AttrNumber attnum);
static PlannedStmt *fasttrun_planner_hook(Query *parse,
										  const char *query_string,
										  int cursorOptions,
										  ParamListInfo boundParams);
static void fasttrun_ensure_planner_hook(void);
static void fasttrun_ensure_stats_hooks(void);
static bool fasttrun_stats_relid_exists(Oid relid);
static void fasttrun_stats_relid_ref(Oid relid);
static void fasttrun_stats_relid_unref(Oid relid);
static bool fasttrun_query_contains_stats_relid(Query *query);

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
 * Cached snapshot per relid: (pages, tuples, ins, upd, del, truncdropped).
 * On next call live row count is reconstructed by exact delta math:
 *     new_tuples = cached_tuples + (ins_now - cached_ins)
 *                                - (del_now - cached_del)
 * UPDATE doesn't change live count, but it is tracked so a pure no-DML hit can
 * skip visibility-map and index-stat maintenance.  The planner-visible
 * relstats part can survive COMMIT, but the pgstat-delta part is valid only
 * inside the transaction where the xact counters were captured.  Cache
 * invalidated if: pages dropped (TRUNCATE-like), truncdropped bit changed,
 * or computed new_tuples < 0.  pgstat unavailable → fall back to scan.
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

typedef struct FasttrunAnalyzeRelstatsUndo
{
	bool		has_relstats;
	BlockNumber	cached_pages;
	int64		cached_tuples;
	BlockNumber	cached_allvisible;
	RelFileLocator cached_locator;
	bool		has_delta_state;
	int64		cached_inserted;
	int64		cached_updated;
	int64		cached_deleted;
	bool		cached_truncdropped;
	SubTransactionId relstats_subid;
	struct FasttrunAnalyzeRelstatsUndo *older;
} FasttrunAnalyzeRelstatsUndo;

typedef struct FasttrunAnalyzeCacheEntry
{
	Oid			relid;			/* hash key — must be first */

	/*
	 * Planner-visible relstats.  These are session-local and can survive a
	 * transaction boundary, unlike the delta-math state below.  cached_locator
	 * guards against same-OID/new-relfilenode reuse after SQL TRUNCATE/rewrite.
	 */
	bool		has_relstats;
	RelFileLocator cached_locator;
	BlockNumber	cached_pages;
	int64		cached_tuples;
	BlockNumber	cached_allvisible;

	/*
	 * Delta-math state is valid only inside the transaction where pgstat
	 * xact counters were captured.  XACT COMMIT keeps has_relstats but clears
	 * this part so the next transaction cold-scans before doing delta math.
	 */
	bool		has_delta_state;
	int64		cached_inserted;
	int64		cached_updated;
	int64		cached_deleted;
	bool		cached_truncdropped;	/* OR of truncdropped over subxact stack */
	SubTransactionId relstats_subid;
	FasttrunAnalyzeRelstatsUndo *relstats_undo;

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

static bool
fasttrun_relation_has_same_locator(Relation rel, FasttrunAnalyzeCacheEntry *entry)
{
	return entry->has_relstats &&
		RelFileLocatorEquals(rel->rd_locator, entry->cached_locator);
}

/*
 * Some temp-table truncates happen outside ordinary transactional rollback:
 * core ON COMMIT DELETE ROWS truncates storage during COMMIT, and
 * fasttruncate() deliberately performs a non-transactional local storage reset.
 * If a session-local relstats cache survives such a boundary unchanged, the
 * next plan can see rows in an empty table.  Whenever we can observe that the
 * underlying temp storage is empty, make that fact authoritative for heap and
 * index relstats.
 */
static bool
fasttrun_cache_make_empty_storage_authoritative(FasttrunAnalyzeCacheEntry *entry,
												Oid *plan_relid,
												bool *changed)
{
	Relation	rel;
	Relation	heaprel = NULL;
	BlockNumber	pages_now;
	bool		is_empty = false;

	*plan_relid = entry->relid;
	*changed = false;

	if (!entry->has_relstats)
		return false;

	rel = try_relation_open(entry->relid, NoLock);
	if (rel == NULL)
		return false;

	if (!fasttrun_relation_has_same_locator(rel, entry) ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)) ||
		!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
	{
		relation_close(rel, NoLock);
		return false;
	}

	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		Oid		heapid = IndexGetRelation(entry->relid, true);

		if (OidIsValid(heapid))
		{
			heaprel = try_relation_open(heapid, NoLock);
			if (heaprel != NULL)
			{
				if (heaprel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
					isTempNamespace(RelationGetNamespace(heaprel)) &&
					RelationGetNumberOfBlocks(heaprel) == 0)
				{
					is_empty = true;
					*plan_relid = heapid;
				}
			}
		}
	}
	else if (rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		is_empty = (RelationGetNumberOfBlocks(rel) == 0);
	}

	if (is_empty)
	{
		pages_now = RelationGetNumberOfBlocks(rel);
		if (entry->cached_pages != pages_now ||
			entry->cached_tuples != 0 ||
			entry->cached_allvisible != 0 ||
			rel->rd_rel->relpages != pages_now ||
			rel->rd_rel->reltuples != 0 ||
			rel->rd_rel->relallvisible != 0)
			*changed = true;

		entry->has_relstats = true;
		entry->cached_pages = pages_now;
		entry->cached_tuples = 0;
		entry->cached_allvisible = 0;
		entry->relstats_subid = InvalidSubTransactionId;
		entry->has_delta_state = false;

		rel->rd_rel->relpages = pages_now;
		rel->rd_rel->reltuples = 0;
		rel->rd_rel->relallvisible = 0;
	}

	if (heaprel != NULL)
		relation_close(heaprel, NoLock);
	relation_close(rel, NoLock);
	return is_empty;
}

static BlockNumber
fasttrun_count_allvisible(Relation rel)
{
	BlockNumber	relallvisible = 0;

	if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		visibilitymap_count(rel, &relallvisible, NULL);
	return relallvisible;
}

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

static void
fasttrun_relstats_free_undo(FasttrunAnalyzeCacheEntry *entry)
{
	while (entry->relstats_undo != NULL)
	{
		FasttrunAnalyzeRelstatsUndo *popped = entry->relstats_undo;

		entry->relstats_undo = popped->older;
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

static void
fasttrun_cache_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunAnalyzeCacheEntry *entry;

	if (fasttrun_analyze_cache == NULL)
		return;

	hash_seq_init(&status, fasttrun_analyze_cache);
	while ((entry = (FasttrunAnalyzeCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		Oid		relid = entry->relid;
		Oid		plan_relid;
		bool	empty_storage_changed;
		bool	empty_storage_authoritative;

		empty_storage_authoritative =
			fasttrun_cache_make_empty_storage_authoritative(entry, &plan_relid,
														   &empty_storage_changed);

		fasttrun_baseline_free_undo(entry);
		fasttrun_relstats_free_undo(entry);

		entry->has_delta_state = false;
		entry->cached_inserted = 0;
		entry->cached_updated = 0;
		entry->cached_deleted = 0;
		entry->cached_truncdropped = false;
		entry->relstats_subid = InvalidSubTransactionId;

		entry->has_stats_baseline = false;
		entry->stats_baseline_inserted = 0;
		entry->stats_baseline_updated = 0;
		entry->stats_baseline_deleted = 0;
		entry->stats_baseline_truncdropped = false;
		entry->stats_baseline_subid = InvalidSubTransactionId;

		if (!entry->has_relstats)
		{
			(void) hash_search(fasttrun_analyze_cache, &relid,
							   HASH_REMOVE, NULL);
			continue;
		}

		if (empty_storage_authoritative && empty_storage_changed)
		{
			if (OidIsValid(plan_relid))
				fasttrun_stats_cache_evict_relid(plan_relid);
			fasttrun_invalidate_local_plan_cache(entry->relid);
			if (OidIsValid(plan_relid) && plan_relid != entry->relid)
				fasttrun_invalidate_local_plan_cache(plan_relid);
		}
	}
}

/* Finish analyze + stats caches on xact end.  PRE_COMMIT skipped — txn may still abort. */
static void
fasttrun_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			fasttrun_cache_commit_xact();
			fasttrun_stats_cache_commit_xact();
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PREPARE:
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

	fasttrun_ensure_planner_hook();

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
		entry->has_relstats = false;
		memset(&entry->cached_locator, 0, sizeof(entry->cached_locator));
		entry->cached_pages = 0;
		entry->cached_tuples = 0;
		entry->cached_allvisible = 0;
		entry->has_delta_state = false;
		entry->cached_inserted = 0;
		entry->cached_updated = 0;
		entry->cached_deleted = 0;
		entry->cached_truncdropped = false;
		entry->relstats_subid = InvalidSubTransactionId;
		entry->relstats_undo = NULL;
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

static void
fasttrun_cache_save_relstats_undo(FasttrunAnalyzeCacheEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	MemoryContext oldcxt;
	FasttrunAnalyzeRelstatsUndo *saved;

	if (entry->relstats_subid == cur_subid)
		return;

	oldcxt = MemoryContextSwitchTo(fasttrun_analyze_mcxt);
	saved = (FasttrunAnalyzeRelstatsUndo *) palloc(sizeof(*saved));
	MemoryContextSwitchTo(oldcxt);

	saved->has_relstats = entry->has_relstats;
	saved->cached_pages = entry->cached_pages;
	saved->cached_tuples = entry->cached_tuples;
	saved->cached_allvisible = entry->cached_allvisible;
	saved->cached_locator = entry->cached_locator;
	saved->has_delta_state = entry->has_delta_state;
	saved->cached_inserted = entry->cached_inserted;
	saved->cached_updated = entry->cached_updated;
	saved->cached_deleted = entry->cached_deleted;
	saved->cached_truncdropped = entry->cached_truncdropped;
	saved->relstats_subid = entry->relstats_subid;
	saved->older = entry->relstats_undo;
	entry->relstats_undo = saved;
}

static FasttrunAnalyzeCacheEntry *
fasttrun_cache_store_relstats(Relation rel, BlockNumber pages, int64 tuples,
							  BlockNumber allvisible)
{
	FasttrunAnalyzeCacheEntry *entry;

	entry = fasttrun_cache_enter(RelationGetRelid(rel));
	fasttrun_cache_save_relstats_undo(entry);

	entry->has_relstats = true;
	entry->cached_locator = rel->rd_locator;
	entry->cached_pages = pages;
	entry->cached_tuples = tuples;
	entry->cached_allvisible = allvisible;
	entry->relstats_subid = GetCurrentSubTransactionId();

	return entry;
}

static void
fasttrun_cache_store_delta_state(FasttrunAnalyzeCacheEntry *entry,
								 int64 ins, int64 upd, int64 del,
								 bool truncdropped)
{
	entry->has_delta_state = true;
	entry->cached_inserted = ins;
	entry->cached_updated = upd;
	entry->cached_deleted = del;
	entry->cached_truncdropped = truncdropped;
}

static void
fasttrun_cache_save_baseline_undo(FasttrunAnalyzeCacheEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	MemoryContext oldcxt;
	FasttrunAnalyzeBaselineUndo *saved;

	if (entry->stats_baseline_subid == cur_subid)
		return;

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

	fasttrun_cache_save_baseline_undo(entry);

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

static void
fasttrun_cache_mark_evicted(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry;
	SubTransactionId cur_subid = GetCurrentSubTransactionId();

	if (fasttrun_analyze_cache == NULL)
		return;

	entry = fasttrun_cache_lookup(relid);
	if (entry == NULL)
		return;

	fasttrun_cache_save_relstats_undo(entry);
	fasttrun_cache_save_baseline_undo(entry);

	entry->has_relstats = false;
	entry->has_delta_state = false;
	entry->relstats_subid = cur_subid;

	entry->has_stats_baseline = false;
	entry->stats_baseline_subid = cur_subid;
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
	{
		fasttrun_baseline_free_undo(entry);
		fasttrun_relstats_free_undo(entry);
	}

	(void) hash_search(fasttrun_analyze_cache, &relid, HASH_REMOVE, NULL);
}

static void
fasttrun_cache_remove_rel_and_indexes(Relation rel)
{
	List	   *index_oids;
	ListCell   *lc;

	if (fasttrun_analyze_cache == NULL)
		return;

	fasttrun_cache_remove(RelationGetRelid(rel));

	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
		fasttrun_cache_remove(lfirst_oid(lc));
	list_free(index_oids);
}

static void
fasttrun_cache_mark_rel_and_indexes_evicted(Relation rel)
{
	List	   *index_oids;
	ListCell   *lc;

	if (fasttrun_analyze_cache == NULL)
		return;

	fasttrun_cache_mark_evicted(RelationGetRelid(rel));

	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
		fasttrun_cache_mark_evicted(lfirst_oid(lc));
	list_free(index_oids);
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
 * non-trivial INSERT against a 0-row baseline gives churn ratio >= 1)
 * and refreshes column stats on the refilled data.  The refresh uses the
 * same full reservoir scan as the cold path to keep plan quality aligned
 * with regular ANALYZE.
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

	entry = fasttrun_cache_store_relstats(rel, 0, 0, 0);

	if (!fasttrun_read_pgstat_counters(rel, &ins_now, &upd_now, &del_now,
									   &truncdropped_now))
		return;	/* no pgstat → no delta seed; next analyze will cold-scan */

	cur_subid = GetCurrentSubTransactionId();

	/* Delta-math state: post-truncate empty table. */
	fasttrun_cache_store_delta_state(entry, ins_now, upd_now, del_now,
									 truncdropped_now);

	/*
	 * Stats baseline: same snapshot.  has_stats_baseline=true so the
	 * first post-refill analyze enters the refresh-check arm; the
	 * resulting churn ratio (any INSERT N / max(N,1) = 1) clears the
	 * default 0.2 threshold and triggers a full reservoir collect.
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
 * Re-inject cached relpages/reltuples into rd_rel after relcache rebuilds.
 *
 * pg_class can contain either bootstrap defaults (0/-1) or older nonzero
 * relstats, e.g. after CREATE INDEX wrote heap stats before a later
 * fasttruncate/refill/analyze cycle.  Therefore the trigger condition is
 * "current rd_rel differs from our cache", not just "relpages == 0".
 * cached_pages=0 is authoritative too: after fasttruncate an unrelated
 * relcache rebuild must not resurrect stale nonzero pg_class stats.
 */
static void
fasttrun_reinject_relstats(Relation rel, FasttrunAnalyzeCacheEntry *entry)
{
	float4	cached_tuples = (float4) entry->cached_tuples;

	if (!fasttrun_relation_has_same_locator(rel, entry))
		return;

	if (rel->rd_rel->relpages == entry->cached_pages &&
		rel->rd_rel->reltuples == cached_tuples &&
		rel->rd_rel->relallvisible == (int32) entry->cached_allvisible)
		return;

	rel->rd_rel->relpages = entry->cached_pages;
	rel->rd_rel->reltuples = cached_tuples;
	rel->rd_rel->relallvisible = (int32) entry->cached_allvisible;
}

static void
fasttrun_reinject_cached_relation(Relation rel)
{
	FasttrunAnalyzeCacheEntry *entry;

	entry = fasttrun_cache_lookup(RelationGetRelid(rel));
	if (entry == NULL || !entry->has_relstats)
		return;

	fasttrun_reinject_relstats(rel, entry);
}

static void
fasttrun_reinject_rte_relstats(RangeTblEntry *rte)
{
	Relation	rel;
	List	   *index_oids;
	ListCell   *lc;

	if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
		return;

	/*
	 * Most queries either do not touch temp tables or touch a temp table that
	 * fasttrun never analyzed.  Avoid relation_open on that common miss.
	 */
	if (fasttrun_cache_lookup(rte->relid) == NULL)
		return;

	rel = RelationIdGetRelation(rte->relid);
	if (!RelationIsValid(rel))
		return;

	fasttrun_reinject_cached_relation(rel);

	if (rel->rd_rel->relkind == RELKIND_RELATION ||
		rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		index_oids = RelationGetIndexList(rel);
		foreach(lc, index_oids)
		{
			Oid			index_oid = lfirst_oid(lc);
			Relation	indexrel;

			if (fasttrun_cache_lookup(index_oid) == NULL)
				continue;

			indexrel = RelationIdGetRelation(index_oid);
			if (!RelationIsValid(indexrel))
				continue;

			fasttrun_reinject_cached_relation(indexrel);
			RelationClose(indexrel);
		}
		list_free(index_oids);
	}

	RelationClose(rel);
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
 * TopMemoryContext, survives COMMIT for session-local ANALYZE-like planner
 * behavior, and is reset on abort or explicit relation invalidation.
 */

typedef struct FasttrunStatsKey
{
	Oid			relid;
	AttrNumber	attnum;
	bool		inh;
} FasttrunStatsKey;

static inline void
fasttrun_stats_key_init(FasttrunStatsKey *key, Oid relid, AttrNumber attnum,
						bool inh)
{
	memset(key, 0, sizeof(*key));
	key->relid = relid;
	key->attnum = attnum;
	key->inh = inh;
}

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

typedef struct FasttrunStatsRelidEntry
{
	Oid		relid;			/* hash key — must be first */
	int		refcount;		/* visible statsTuple entries for this relid */
} FasttrunStatsRelidEntry;

static HTAB			   *fasttrun_stats_cache = NULL;
static HTAB			   *fasttrun_stats_relid_cache = NULL;
static MemoryContext	fasttrun_stats_mcxt = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_attavgwidth_hook_type prev_get_attavgwidth_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static bool				fasttrun_stats_hooks_installed = false;
static bool				fasttrun_planner_hook_installed = false;

static void
fasttrun_ensure_planner_hook(void)
{
	if (fasttrun_planner_hook_installed)
		return;

	prev_planner_hook = planner_hook;
	planner_hook = fasttrun_planner_hook;
	fasttrun_planner_hook_installed = true;
}

static void
fasttrun_ensure_stats_hooks(void)
{
	if (fasttrun_stats_hooks_installed)
		return;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = fasttrun_get_relation_stats_hook;
	prev_get_attavgwidth_hook = get_attavgwidth_hook;
	get_attavgwidth_hook = fasttrun_get_attavgwidth_hook;
	fasttrun_stats_hooks_installed = true;
}

static void
fasttrun_stats_cache_init(void)
{
	HASHCTL ctl;

	if (fasttrun_stats_cache != NULL)
		return;

	fasttrun_ensure_planner_hook();
	fasttrun_ensure_stats_hooks();

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

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FasttrunStatsRelidEntry);
	ctl.hcxt = fasttrun_stats_mcxt;

	fasttrun_stats_relid_cache = hash_create("fasttrun stats relid cache",
											 16,
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
	fasttrun_stats_relid_cache = NULL;

	if (fasttrun_stats_mcxt != NULL)
	{
		MemoryContextDelete(fasttrun_stats_mcxt);
		fasttrun_stats_mcxt = NULL;
	}
}

static bool
fasttrun_stats_relid_exists(Oid relid)
{
	if (fasttrun_stats_relid_cache == NULL)
		return false;

	return hash_search(fasttrun_stats_relid_cache, &relid,
					   HASH_FIND, NULL) != NULL;
}

static void
fasttrun_stats_relid_ref(Oid relid)
{
	FasttrunStatsRelidEntry *entry;
	bool	found;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													&relid,
													HASH_ENTER, &found);
	if (!found)
		entry->refcount = 0;
	entry->refcount++;
}

static void
fasttrun_stats_relid_unref(Oid relid)
{
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													&relid,
													HASH_FIND, NULL);
	if (entry == NULL)
		return;

	entry->refcount--;
	if (entry->refcount <= 0)
		(void) hash_search(fasttrun_stats_relid_cache, &relid,
						   HASH_REMOVE, NULL);
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
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;

	if (fasttrun_stats_cache == NULL ||
		!fasttrun_stats_relid_exists(rte->relid))
		goto chain;

	fasttrun_stats_key_init(&key, rte->relid, attnum, rte->inh);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry == NULL || entry->statsTuple == NULL)
		goto chain;

	/* Freshness check: pgstat counters must match the collect-time snapshot. */
	if (!fasttrun_read_pgstat_counters_for_hook(rte->relid, &ins_now, &upd_now,
												&del_now, &truncdropped_now))
		goto chain;

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
	int64				ins_now = 0;
	int64				upd_now = 0;
	int64				del_now = 0;
	bool				truncdropped_now = false;
	int32				stawidth;

	if (fasttrun_stats_cache == NULL ||
		!fasttrun_stats_relid_exists(relid))
		goto chain;

	/* matches what fasttrun_stats_cache_store writes */
	fasttrun_stats_key_init(&key, relid, attnum, false);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry == NULL || entry->statsTuple == NULL)
		goto chain;

	if (!fasttrun_read_pgstat_counters_for_hook(relid, &ins_now, &upd_now,
												&del_now, &truncdropped_now))
		goto chain;

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
 * planner hook — re-inject cached relpages/reltuples into rd_rel for any
 * temp relation whose rd_rel was reset to the on-disk pg_class values
 * by a relcache rebuild (typically triggered by a sinval message from a
 * concurrent backend).
 *
 * fasttrun_analyze() writes (relpages, reltuples) only into rd_rel —
 * never into pg_class — to keep the operation sinval-free.  But rd_rel
 * is owned by the relcache, and any RelationCacheInvalidateEntry resets
 * it from pg_class on the next RelationIdGetRelation.  Without this
 * hook, that wipes our analyze results: planner sees relpages=0 and
 * picks catastrophic plans (Seq Scan over what looks like a tiny table,
 * nested loop joins on big working sets).
 *
 * We walk only the current query's rangetable and re-inject cached stats for
 * those temp heaps plus their indexes.  This keeps unrelated cached temp
 * tables at zero cost for plans that don't reference them.
 */
static bool
fasttrun_query_contains_stats_relid(Query *query)
{
	ListCell   *lc;

	if (query == NULL || fasttrun_stats_relid_cache == NULL)
		return false;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			if (fasttrun_stats_relid_exists(rte->relid))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			if (fasttrun_query_contains_stats_relid(rte->subquery))
				return true;
		}
	}

	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query) &&
			fasttrun_query_contains_stats_relid((Query *) cte->ctequery))
			return true;
	}

	return false;
}

static PlannedStmt *
fasttrun_planner_hook(Query *parse, const char *query_string,
					  int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool		saved_in_planner = fasttrun_in_planner;
	int			saved_freshness_cache_used = fasttrun_freshness_cache_used;
	bool		stats_frame_needed = false;

	if (fasttrun_analyze_cache != NULL)
	{
		ListCell   *lc;

		foreach(lc, parse->rtable)
			fasttrun_reinject_rte_relstats((RangeTblEntry *) lfirst(lc));
	}

	stats_frame_needed =
		(fasttrun_stats_cache != NULL &&
		 fasttrun_query_contains_stats_relid(parse));

	/*
	 * The freshness frame is only useful for cached column statistics
	 * referenced by this query.  Keep unrelated plans to a plain hook call
	 * plus relstats reinjection above; avoid PG_TRY/sigsetjmp on ordinary
	 * queries after a backend has used fasttrun stats at least once.
	 */
	if (!stats_frame_needed)
	{
		if (prev_planner_hook)
			return prev_planner_hook(parse, query_string, cursorOptions,
									 boundParams);
		return standard_planner(parse, query_string, cursorOptions,
								boundParams);
	}

	fasttrun_in_planner = true;
	fasttrun_freshness_cache_used = 0;

	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(parse, query_string, cursorOptions,
									   boundParams);
		else
			result = standard_planner(parse, query_string, cursorOptions,
									  boundParams);
	}
	PG_CATCH();
	{
		fasttrun_in_planner = saved_in_planner;
		fasttrun_freshness_cache_used = saved_freshness_cache_used;
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_in_planner = saved_in_planner;
	fasttrun_freshness_cache_used = saved_freshness_cache_used;

	return result;
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

	/*
	 * relcache rebuilds leave rel->pgstat_info NULL even when tracking is
	 * enabled and this backend already has pending xact counters for the
	 * relation.  Associate lazily before deciding pgstat is unavailable;
	 * otherwise a harmless relcache invalidation would make cached column
	 * stats look stale until some later heap scan/DML reinitializes pgstat.
	 */
	if (!pgstat_should_count_relation(rel))
		return false;

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

static bool
fasttrun_read_pgstat_counters_for_hook(Oid relid,
									   int64 *ins, int64 *upd, int64 *del,
									   bool *truncdropped)
{
	int		i;
	Relation rel;
	bool	have;

	if (fasttrun_in_planner)
	{
		for (i = 0; i < fasttrun_freshness_cache_used; i++)
		{
			FasttrunFreshnessCacheEntry *slot = &fasttrun_freshness_cache[i];

			if (slot->relid != relid)
				continue;

			*ins = slot->ins;
			if (upd != NULL)
				*upd = slot->upd;
			*del = slot->del;
			*truncdropped = slot->truncdropped;
			return slot->have;
		}
	}

	rel = RelationIdGetRelation(relid);
	if (rel == NULL)
	{
		have = false;
		*ins = 0;
		if (upd != NULL)
			*upd = 0;
		*del = 0;
		*truncdropped = false;
	}
	else
	{
		have = fasttrun_read_pgstat_counters(rel, ins, upd, del, truncdropped);
		RelationClose(rel);
	}

	if (fasttrun_in_planner &&
		fasttrun_freshness_cache_used < FASTTRUN_FRESHNESS_CACHE_SLOTS)
	{
		FasttrunFreshnessCacheEntry *slot =
			&fasttrun_freshness_cache[fasttrun_freshness_cache_used++];

		slot->relid = relid;
		slot->have = have;
		slot->ins = *ins;
		slot->upd = (upd != NULL) ? *upd : 0;
		slot->del = *del;
		slot->truncdropped = *truncdropped;
	}

	return have;
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
	bool				had_visible;
	MemoryContext		oldcxt;
	SubTransactionId	cur_subid = GetCurrentSubTransactionId();

	fasttrun_stats_cache_init();

	fasttrun_stats_key_init(&key, relid, attnum, false);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_ENTER, &found);
	had_visible = (found && entry->statsTuple != NULL);

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

	if (!had_visible)
		fasttrun_stats_relid_ref(relid);
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

static void
fasttrun_stats_cache_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunStatsEntry *entry;

	if (fasttrun_stats_cache == NULL)
		return;

	hash_seq_init(&status, fasttrun_stats_cache);
	while ((entry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		FasttrunStatsKey key;
		Relation	rel;
		int64		ins_now = 0;
		int64		upd_now = 0;
		int64		del_now = 0;
		bool		truncdropped_now = false;
		bool		have_counters = false;

		key = entry->key;
		if (entry->statsTuple == NULL)
		{
			fasttrun_stats_entry_free_undo(entry);
			(void) hash_search(fasttrun_stats_cache, &key, HASH_REMOVE, NULL);
			continue;
		}

		rel = RelationIdGetRelation(key.relid);
		if (rel != NULL)
		{
			have_counters = fasttrun_read_pgstat_counters(rel, &ins_now,
														  &upd_now, &del_now,
														  &truncdropped_now);
			RelationClose(rel);
		}

		/*
		 * Preserve stats across COMMIT only if they were still fresh at the
		 * boundary.  Below-threshold DML deliberately leaves collected_* at the
		 * old snapshot so the planner hook hides the stale tuple; resetting that
		 * old snapshot to zero would make it look fresh again in the next xact.
		 */
		if (!have_counters ||
			ins_now != entry->collected_ins ||
			upd_now != entry->collected_upd ||
			del_now != entry->collected_del ||
			truncdropped_now != entry->collected_truncdropped)
		{
			fasttrun_stats_entry_free_undo(entry);
			if (entry->statsTuple != NULL)
			{
				fasttrun_stats_relid_unref(key.relid);
				heap_freetuple(entry->statsTuple);
			}
			(void) hash_search(fasttrun_stats_cache, &key, HASH_REMOVE, NULL);
			fasttrun_invalidate_local_plan_cache(key.relid);
			continue;
		}

		fasttrun_stats_entry_free_undo(entry);
		entry->collected_ins = 0;
		entry->collected_upd = 0;
		entry->collected_del = 0;
		entry->collected_truncdropped = false;
		entry->collected_subid = InvalidSubTransactionId;
	}
}

/*
 * Drop all stats entries for a given relid.  Called from fasttruncate
 * as a backstop against the freshness check (e.g. if pgstat is off
 * and the hook can't compare counters).
 */
static bool
fasttrun_stats_cache_evict_relid(Oid relid)
{
	HASH_SEQ_STATUS status;
	FasttrunStatsEntry *entry;
	bool			removed = false;

	if (fasttrun_stats_cache == NULL)
		return false;

	hash_seq_init(&status, fasttrun_stats_cache);
	while ((entry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		FasttrunStatsKey key;

		if (entry->key.relid != relid)
			continue;

		key = entry->key;
		fasttrun_stats_entry_free_undo(entry);
		if (entry->statsTuple != NULL)
		{
			fasttrun_stats_relid_unref(key.relid);
			heap_freetuple(entry->statsTuple);
		}
		(void) hash_search(fasttrun_stats_cache, &key, HASH_REMOVE, NULL);
		removed = true;
	}

	return removed;
}

static bool
fasttrun_stats_cache_mark_evicted_relid(Oid relid)
{
	HASH_SEQ_STATUS status;
	FasttrunStatsEntry *entry;
	bool			changed = false;
	SubTransactionId cur_subid = GetCurrentSubTransactionId();

	if (fasttrun_stats_cache == NULL)
		return false;

	hash_seq_init(&status, fasttrun_stats_cache);
	while ((entry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.relid != relid || entry->statsTuple == NULL)
			continue;

		fasttrun_stats_relid_unref(relid);

		if (entry->collected_subid != cur_subid)
		{
			MemoryContext oldcxt;
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
			heap_freetuple(entry->statsTuple);

		entry->statsTuple = NULL;
		entry->collected_ins = 0;
		entry->collected_upd = 0;
		entry->collected_del = 0;
		entry->collected_truncdropped = false;
		entry->collected_subid = cur_subid;
		changed = true;
	}

	return changed;
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
 * Analyze-cache relstats + delta state:
 *   ABORT_SUB restores the previous planner-visible relstats snapshot
 *   and invalidates local generic plans that may have been born against
 *   the aborted subxact's stats.  COMMIT_SUB promotes the snapshot to
 *   the parent subxact.
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
					{
						fasttrun_stats_relid_unref(sentry->key.relid);
						heap_freetuple(sentry->statsTuple);
					}
					sentry->statsTuple = popped->statsTuple;
					if (sentry->statsTuple != NULL)
						fasttrun_stats_relid_ref(sentry->key.relid);
					sentry->collected_ins = popped->collected_ins;
					sentry->collected_upd = popped->collected_upd;
					sentry->collected_del = popped->collected_del;
					sentry->collected_truncdropped = popped->collected_truncdropped;
					sentry->collected_subid = popped->collected_subid;
					sentry->undo = popped->older;
					pfree(popped);	/* popped->statsTuple now owned by entry */
					fasttrun_invalidate_local_plan_cache(sentry->key.relid);
				}
				else
				{
					/* Created in this subxact -> drop entirely. */
					Oid		relid = sentry->key.relid;

					if (sentry->statsTuple != NULL)
					{
						fasttrun_stats_relid_unref(relid);
						heap_freetuple(sentry->statsTuple);
					}
					(void) hash_search(fasttrun_stats_cache, &sentry->key,
									   HASH_REMOVE, NULL);
					fasttrun_invalidate_local_plan_cache(relid);
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
			if (aentry->relstats_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					Oid		plan_relid;
					bool	empty_storage_changed;

					if (fasttrun_cache_make_empty_storage_authoritative(aentry,
																		&plan_relid,
																		&empty_storage_changed))
					{
						/*
						 * fasttruncate() is non-transactional for local
						 * storage: after ROLLBACK TO SAVEPOINT the rows are
						 * still gone.  Do not restore the old relstats undo
						 * chain; make the observed empty storage the new
						 * outer-subxact truth instead.
						 */
						fasttrun_relstats_free_undo(aentry);
						fasttrun_baseline_free_undo(aentry);

						aentry->has_stats_baseline = false;
						aentry->stats_baseline_inserted = 0;
						aentry->stats_baseline_updated = 0;
						aentry->stats_baseline_deleted = 0;
						aentry->stats_baseline_truncdropped = false;
						aentry->stats_baseline_subid = InvalidSubTransactionId;

						if (OidIsValid(plan_relid))
							fasttrun_stats_cache_evict_relid(plan_relid);
						fasttrun_invalidate_local_plan_cache(aentry->relid);
						if (OidIsValid(plan_relid) && plan_relid != aentry->relid)
							fasttrun_invalidate_local_plan_cache(plan_relid);
						continue;
					}

					if (aentry->relstats_undo != NULL)
					{
						FasttrunAnalyzeRelstatsUndo *popped =
							aentry->relstats_undo;

						aentry->has_relstats = popped->has_relstats;
						aentry->cached_pages = popped->cached_pages;
						aentry->cached_tuples = popped->cached_tuples;
						aentry->cached_allvisible = popped->cached_allvisible;
						aentry->cached_locator = popped->cached_locator;
						aentry->has_delta_state = popped->has_delta_state;
						aentry->cached_inserted = popped->cached_inserted;
						aentry->cached_updated = popped->cached_updated;
						aentry->cached_deleted = popped->cached_deleted;
						aentry->cached_truncdropped = popped->cached_truncdropped;
						aentry->relstats_subid = popped->relstats_subid;
						aentry->relstats_undo = popped->older;
						pfree(popped);
					}
					else
					{
						aentry->has_relstats = false;
						aentry->has_delta_state = false;
						aentry->relstats_subid = InvalidSubTransactionId;
					}
					fasttrun_invalidate_local_plan_cache(aentry->relid);
				}
				else
				{
					aentry->relstats_subid = parentSubid;
					if (aentry->relstats_undo != NULL &&
						aentry->relstats_undo->relstats_subid == parentSubid)
					{
						FasttrunAnalyzeRelstatsUndo *obsolete =
							aentry->relstats_undo;

						aentry->relstats_undo = obsolete->older;
						pfree(obsolete);
					}
				}
			}

			if (aentry->stats_baseline_subid == mySubid)
			{
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

static void
fasttrun_apply_attribute_options(Relation rel, AttrNumber attnum,
								 bool inh, float4 *stadistinct)
{
	AttributeOpts *aopt;

	aopt = get_attribute_options(RelationGetRelid(rel), attnum);
	if (aopt != NULL)
	{
		float8		n_distinct;

		n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
		if (n_distinct != 0.0)
			*stadistinct = (float4) n_distinct;
	}
}

static int
fasttrun_get_attstattarget(Relation rel, AttrNumber attnum)
{
	HeapTuple	atttuple;
	Datum		datum;
	bool		isnull;
	int			attstattarget = -1;

	atttuple = SearchSysCache2(ATTNUM,
							   ObjectIdGetDatum(RelationGetRelid(rel)),
							   Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		return -1;

	datum = SysCacheGetAttr(ATTNUM, atttuple,
							Anum_pg_attribute_attstattarget,
							&isnull);
	if (!isnull)
		attstattarget = DatumGetInt16(datum);
	ReleaseSysCache(atttuple);

	return attstattarget;
}

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
 * field on the struct.  Keep the core semantics: attstattarget=0 means
 * "do not collect stats for this column", negative means default target.
 */
static VacAttrStats *
fasttrun_examine_attribute(Relation rel, int attnum, MemoryContext anl_context)
{
	Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attnum - 1);
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			attstattarget;
	int			i;
	bool		ok;
	MemoryContext oldcxt;

	if (attr->attisdropped)
		return NULL;
	attstattarget = fasttrun_get_attstattarget(rel, attnum);
	if (attstattarget == 0)
		return NULL;

	oldcxt = MemoryContextSwitchTo(anl_context);

	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

#if PG_VERSION_NUM < 170000
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);
	stats->attr->attstattarget = attstattarget;
#else
	stats->attstattarget = attstattarget;
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

static double
fasttrun_estimate_index_fraction(Relation heaprel, Relation indexrel,
								 IndexInfo *indexInfo,
								 HeapTuple *sample, int sample_count)
{
	ExprState  *predicate;
	EState	   *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	int			i;
	int			matched = 0;

	if (indexInfo->ii_Predicate == NIL)
		return 1.0;
	if (sample_count <= 0)
		return 0.0;

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heaprel), &TTSOpsHeapTuple);
	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	for (i = 0; i < sample_count; i++)
	{
		ResetExprContext(econtext);
		ExecStoreHeapTuple(sample[i], slot, false);
		if (ExecQual(predicate, econtext))
			matched++;
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	return (double) matched / (double) sample_count;
}

static bool
fasttrun_relation_has_partial_index(Relation rel)
{
	List	   *index_oids;
	ListCell   *lc;
	bool		found_partial = false;

	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
	{
		Relation	indexrel;

		indexrel = index_open(lfirst_oid(lc), AccessShareLock);
		if (RelationGetIndexPredicate(indexrel) != NIL)
			found_partial = true;
		index_close(indexrel, AccessShareLock);
		if (found_partial)
			break;
	}
	list_free(index_oids);

	return found_partial;
}

static bool
fasttrun_update_index_relstats(Relation rel, HeapTuple *sample, int sample_count,
							   int64 totalrows)
{
	List	   *index_oids;
	ListCell   *lc;
	bool		changed = false;

	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
	{
		Oid			indexOid = lfirst_oid(lc);
		Relation	indexrel;
		BlockNumber	index_pages;
		double		tuple_fract;
		int64		index_tuples;
		bool		is_partial;

		indexrel = index_open(indexOid, AccessShareLock);
		is_partial = (RelationGetIndexPredicate(indexrel) != NIL);

		if (is_partial && sample_count <= 0 && totalrows > 0)
		{
			index_close(indexrel, AccessShareLock);
			continue;
		}

		index_pages = RelationGetNumberOfBlocks(indexrel);
		if (is_partial)
		{
			IndexInfo  *indexInfo = BuildIndexInfo(indexrel);

			tuple_fract = fasttrun_estimate_index_fraction(rel, indexrel,
														   indexInfo, sample,
														   sample_count);
		}
		else
			tuple_fract = 1.0;
		index_tuples = (int64) ceil(tuple_fract * (double) totalrows);

		if (indexrel->rd_rel->relpages != index_pages ||
			indexrel->rd_rel->reltuples != (float4) index_tuples ||
			indexrel->rd_rel->relallvisible != 0)
			changed = true;

		indexrel->rd_rel->relpages = index_pages;
		indexrel->rd_rel->reltuples = (float4) index_tuples;
		indexrel->rd_rel->relallvisible = 0;
		fasttrun_cache_store_relstats(indexrel, index_pages, index_tuples, 0);

		index_close(indexrel, AccessShareLock);
	}
	list_free(index_oids);

	return changed;
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
		if (fasttrun_stats_cache_evict_relid(RelationGetRelid(rel)))
			fasttrun_invalidate_local_plan_cache(RelationGetRelid(rel));

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

			fasttrun_apply_attribute_options(rel, attnum, false,
											 &stats->stadistinct);

			/*
			 * Build tuple inside per_col_mcxt so intermediates are
			 * reclaimed on the next MemoryContextReset.
			 */
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
			int				   attstattarget;
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
			attstattarget = fasttrun_get_attstattarget(rel, attnum);
			if (attstattarget == 0)
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

			fasttrun_apply_attribute_options(rel, attnum, false,
											 &stadistinct);
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
	(void) fasttrun_update_index_relstats(rel, sample, sample_count, totalrows);

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
 * Used by the cold-scan path and by delta-hit stats refresh.  The refresh
 * path used to sample only a subset of heap blocks, but that was not
 * ANALYZE-equivalent for physically clustered or sparse temp tables.
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
	indexrel->rd_rel->relallvisible = 0;
	fasttrun_cache_store_relstats(indexrel, indexrel->rd_rel->relpages, 0, 0);
}

static void
fasttrun_full_bypass_truncate(Relation rel)
{
	List	   *index_oids;
	ListCell   *lc;
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
		index_oids = RelationGetIndexList(rel);
		foreach(lc, index_oids)
		{
			Relation	currentIndex;

			currentIndex = index_open(lfirst_oid(lc), AccessExclusiveLock);
			fasttrun_rebuild_one_index(rel, currentIndex);
			index_close(currentIndex, NoLock);
		}
		list_free(index_oids);

		toastrelid = rel->rd_rel->reltoastrelid;
		if (OidIsValid(toastrelid))
		{
			Relation	toastrel = table_open(toastrelid, AccessExclusiveLock);
			List	   *toast_index_oids;

			fasttrun_smgr_bypass_truncate(toastrel);

			toast_index_oids = RelationGetIndexList(toastrel);
			foreach(lc, toast_index_oids)
			{
				Relation	currentIndex;

				currentIndex = index_open(lfirst_oid(lc),
										  AccessExclusiveLock);
				fasttrun_rebuild_one_index(toastrel, currentIndex);
				index_close(currentIndex, NoLock);
			}
			list_free(toast_index_oids);

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
 * Invalidate backend-local cached plans for heap relation `relid`.
 * This reaches `PlanCacheRelCallback` through
 * `LocalExecuteInvalidationMessage(SHAREDINVALRELCACHE_ID)` and does
 * not publish anything into the shared sinval queue.  That preserves
 * the zero-sinval contract: temp tables are private to the current
 * backend, so other backends do not need to hear about them.
 *
 * Why this exists: fasttruncate physically resets the table via
 * unlink+smgrcreate and intentionally emits no shared invalidation,
 * and fasttrun_analyze / fasttrun_collect_stats publish planner stats
 * without touching pg_class or pg_statistic.  Without a local callback,
 * cached SPI / PREPARE plans can keep is_valid=true and stale rowcount
 * or column-stat assumptions.  Marking those plans invalid forces the
 * next EXECUTE to replan and pick up fresh stats through fasttrun hooks.
 *
 * Side effect: before dispatching relcache callbacks,
 * `LocalExecuteInvalidationMessage` calls
 * `RelationCacheInvalidateEntry(relid)`.  For temp relations that
 * becomes `RelationFlushRelation`, i.e. a full relcache rebuild from
 * pg_class.  Therefore fasttruncate must call us BEFORE zeroing
 * `rd_rel->relpages/reltuples`, otherwise the rebuild would overwrite
 * those zeros from pg_class.  After truncate, the authoritative source
 * of refreshed stats is fasttrun_analyze_cache, reinjected by
 * fasttrun_planner_hook.
 *
 * Contract invariant: after fasttruncate, callers must run
 * fasttrun_analyze() before relying on replanning to see real nonzero
 * stats.  The analyze path calls this helper before publishing rd_rel,
 * so any relcache rebuild caused by the callback cannot wipe the final
 * in-memory values.
 */
static void
fasttrun_invalidate_local_plan_cache(Oid relid)
{
	SharedInvalidationMessage msg;

	/* InvalidOid would target the whole relcache; defend against that. */
	Assert(OidIsValid(relid));

	msg.rc.id = SHAREDINVALRELCACHE_ID;
	msg.rc.dbId = MyDatabaseId;
	msg.rc.relId = relid;
	LocalExecuteInvalidationMessage(&msg);
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

	/*
	 * Evict/invalidate before touching storage.  If index rebuild later
	 * errors and the caller catches it in a savepoint, old planner stats and
	 * generic plans must already be gone.
	 */
	fasttrun_cache_remove_rel_and_indexes(rel);
	fasttrun_stats_cache_evict_relid(relOid);
	fasttrun_invalidate_local_plan_cache(RelationGetRelid(rel));

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
 *     when the cache/delta state cannot prove that storage size is
 *     unchanged (this never touches the catalog).
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
	BlockNumber		pages_now = 0;
	int64			tuples_count = 0;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;
	bool			have_counters;
	bool			scan_needed = true;
	bool			stats_recollected = false;
	bool			stats_visibility_changed = false;
	bool			index_relstats_changed = false;
	bool			need_plan_inval = false;
	bool			pure_delta_noop = false;
	bool			pages_now_known = false;
	BlockNumber		allvisible_now = 0;
	BlockNumber		old_pages;
	int32			old_allvisible;
	float4			old_tuples;
	int64			delta_ins = 0;
	int64			delta_upd = 0;
	int64			delta_del = 0;
	FasttrunAnalyzeCacheEntry *entry = NULL;

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

	old_pages = rel->rd_rel->relpages;
	old_allvisible = rel->rd_rel->relallvisible;
	old_tuples = rel->rd_rel->reltuples;
	have_counters = fasttrun_read_pgstat_counters(rel,
												  &ins_now, &upd_now, &del_now,
												  &truncdropped_now);

	/* Lazy-mode delta hit: see header comment block above. */
	if (have_counters)
	{
		entry = fasttrun_cache_lookup(relOid);
		if (entry != NULL
			&& entry->has_delta_state
			&& fasttrun_relation_has_same_locator(rel, entry)
			&& entry->cached_truncdropped == truncdropped_now)
		{
			int64	new_tuples;

			delta_ins = ins_now - entry->cached_inserted;
			delta_upd = upd_now - entry->cached_updated;
			delta_del = del_now - entry->cached_deleted;
			new_tuples = entry->cached_tuples + delta_ins - delta_del;

			if (new_tuples >= 0)
			{
				pure_delta_noop =
					(delta_ins == 0 && delta_upd == 0 && delta_del == 0);
				if (pure_delta_noop)
				{
					/*
					 * No DML means the heap size cannot have grown through
					 * the delta path.  Use our relstats snapshot as the
					 * authoritative value and avoid one smgrnblocks/lseek
					 * per hot no-op fasttrun_analyze().
					 */
					pages_now = entry->cached_pages;
					pages_now_known = true;
					tuples_count = new_tuples;
					scan_needed = false;
				}
				else
				{
					pages_now = RelationGetNumberOfBlocks(rel);
					pages_now_known = true;
					if (pages_now >= entry->cached_pages)
					{
						tuples_count = new_tuples;
						scan_needed = false;
					}
				}
			}
		}
	}

	if (!pages_now_known)
	{
		pages_now = RelationGetNumberOfBlocks(rel);
		pages_now_known = true;
	}

	/*
	 * Delta-hit stats refresh: when DML churn since the last collect
	 * reaches the threshold, do the same full-table reservoir sample as
	 * the cold path.  The old block-level refresh was cheaper, but it
	 * was not ANALYZE-equivalent on clustered or sparse heaps.
	 */
	if (!scan_needed && have_counters && fasttrun_auto_collect_stats &&
		fasttrun_sample_rows != 0 &&
		entry != NULL && entry->has_stats_baseline)
	{
		int64		churn;
		double		baseline;

		/* Compute churn BEFORE advancing baseline. */
		churn = (ins_now - entry->stats_baseline_inserted)
			+ (upd_now - entry->stats_baseline_updated)
			+ (del_now - entry->stats_baseline_deleted);
		if (churn < 0)
			churn = -churn;
		baseline = (double) Max(tuples_count, 1);

		if (churn > 0)
		{
			double		churn_ratio = (double) churn / baseline;

			if (fasttrun_stats_refresh_threshold < 1.0 &&
				churn_ratio >= fasttrun_stats_refresh_threshold)
			{
				int			sample_target = fasttrun_effective_sample_target(rel);

				if (sample_target > 0)
				{
					HeapTuple  *sample;
					int64		scanned_tuples = 0;
					int			sample_count = 0;
					int			i;

					sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
					fasttrun_scan_with_sample(rel, &scanned_tuples,
											  sample, sample_target,
											  &sample_count);
					tuples_count = scanned_tuples;

					if (sample_count > 0)
					{
						fasttrun_collect_and_store(rel, sample, sample_count,
												   tuples_count,
												   tuples_count > sample_target);
						stats_recollected = true;
					}
					else
					{
						if (fasttrun_stats_cache_evict_relid(relOid))
							stats_visibility_changed = true;
						index_relstats_changed |=
							fasttrun_update_index_relstats(rel, NULL, 0,
														   tuples_count);
					}

					for (i = 0; i < sample_count; i++)
						heap_freetuple(sample[i]);
					pfree(sample);
				}
			}

			if (stats_recollected || tuples_count == 0)
				fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
												  del_now, truncdropped_now);
			else
				stats_visibility_changed = true;
		}
	}

	if (!scan_needed && have_counters && !stats_recollected &&
		fasttrun_sample_rows != 0 &&
		entry != NULL && entry->has_stats_baseline)
	{
		int64		churn;

		churn = (ins_now - entry->stats_baseline_inserted)
			+ (upd_now - entry->stats_baseline_updated)
			+ (del_now - entry->stats_baseline_deleted);
		if (churn < 0)
			churn = -churn;

		if (churn > 0 && fasttrun_relation_has_partial_index(rel))
		{
			int			sample_target = fasttrun_effective_sample_target(rel);

			if (sample_target > 0)
			{
				HeapTuple  *sample;
				int64		scanned_tuples = 0;
				int			sample_count = 0;
				int			i;

				sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
				fasttrun_scan_with_sample(rel, &scanned_tuples,
										  sample, sample_target,
										  &sample_count);
				tuples_count = scanned_tuples;
				index_relstats_changed |=
					fasttrun_update_index_relstats(rel, sample, sample_count,
												   tuples_count);

				for (i = 0; i < sample_count; i++)
					heap_freetuple(sample[i]);
				pfree(sample);
			}
		}
	}

	if (!scan_needed && !stats_recollected && !pure_delta_noop)
		index_relstats_changed |=
			fasttrun_update_index_relstats(rel, NULL, 0, tuples_count);

	/*
	 * Below-threshold DML deliberately does NOT advance collected_*.
	 * Returning stale distributions as "fresh" is worse for plan stability
	 * than falling back to defaults until a real refresh happens.
	 */

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
			stats_recollected = true;
		}

		if (sample != NULL)
		{
			int i;
			for (i = 0; i < sample_count; i++)
				heap_freetuple(sample[i]);
			pfree(sample);
		}

		/*
		 * Re-read counters after the scan.  heap_getnext touches only
		 * non-xact counters, so the values we read here are authoritative
		 * for the tuple DML baseline.
		 */
		if (!have_counters)
			have_counters = fasttrun_read_pgstat_counters(rel,
														  &ins_now, &upd_now,
														  &del_now,
														  &truncdropped_now);

		if (!stats_recollected)
			index_relstats_changed |=
				fasttrun_update_index_relstats(rel, NULL, 0, tuples_count);

		/* Populate stats baseline only when pgstat is usable. */
		if (have_counters)
			fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
											  del_now, truncdropped_now);
	}

	if (pure_delta_noop && entry != NULL && entry->has_relstats)
		allvisible_now = entry->cached_allvisible;
	else
		allvisible_now = fasttrun_count_allvisible(rel);

	if (!(pure_delta_noop && entry != NULL && entry->has_relstats))
	{
		entry = fasttrun_cache_store_relstats(rel, pages_now, tuples_count,
											 allvisible_now);
		if (have_counters)
			fasttrun_cache_store_delta_state(entry, ins_now, upd_now, del_now,
											 truncdropped_now);
	}

	need_plan_inval = (old_pages != pages_now ||
					   old_tuples != (float4) tuples_count ||
					   old_allvisible != (int32) allvisible_now ||
					   stats_recollected ||
					   stats_visibility_changed ||
					   index_relstats_changed);
	if (need_plan_inval)
		fasttrun_invalidate_local_plan_cache(relOid);

	rel->rd_rel->relpages = pages_now;
	rel->rd_rel->reltuples = (float4) tuples_count;
	rel->rd_rel->relallvisible = (int32) allvisible_now;

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

	rel = relation_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		relation_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		relation_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}

	/*
	 * Re-inject cached relstats after relcache rebuilds so direct SQL
	 * inspection sees the same values the planner hook will publish.
	 */
	if (fasttrun_analyze_cache != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry;

		entry = (FasttrunAnalyzeCacheEntry *)
			hash_search(fasttrun_analyze_cache, &relOid, HASH_FIND, NULL);
		if (entry != NULL)
			fasttrun_reinject_relstats(rel, entry);
	}

	values[0] = Int32GetDatum(rel->rd_rel->relpages);
	values[1] = Float4GetDatum(rel->rd_rel->reltuples);

	relation_close(rel, AccessShareLock);

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
	{
		fasttrun_collect_and_store(rel, sample, sample_count, tuples_count,
								   tuples_count > sample_target);
		fasttrun_invalidate_local_plan_cache(relOid);
	}
	else if (fasttrun_stats_cache_evict_relid(relOid))
	{
		(void) fasttrun_update_index_relstats(rel, NULL, 0, tuples_count);
		fasttrun_invalidate_local_plan_cache(relOid);
	}

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
	if (!fasttrun_stats_relid_exists(relOid))
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
#define FASTTRUN_TRACK_TOPN_LIMIT 1024
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
static int				fasttrun_prewarm_count = 1000;
static char			   *fasttrun_prewarm_schema = "dummy_tmp";
static char			   *fasttrun_track_schedule = "mon-fri 08:00-18:00";
static ProcessUtility_hook_type prev_utility_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/*
 * Track schedule: tracking is only active inside the configured time
 * windows.  Empty schedule = always active (default).
 */
#define FASTTRUN_SCHED_MAX_WINDOWS 8

typedef struct
{
	uint8		day_mask;		/* bit 0=Sun, 1=Mon, ..., 6=Sat */
	uint16		start_min;		/* minutes since midnight */
	uint16		end_min;
} FasttrunSchedWindow;

static FasttrunSchedWindow fasttrun_sched_windows[FASTTRUN_SCHED_MAX_WINDOWS];
static int		fasttrun_sched_window_count = 0;
static pg_time_t fasttrun_sched_cache_minute = (pg_time_t) -1;
static bool		fasttrun_sched_cache_active = true;

/* Parse day token: "mon", "tue", ..., "sun". Returns 0-6 or -1 on error. */
static int
fasttrun_sched_parse_day(const char *tok, int len)
{
	static const char *days[7] = { "sun", "mon", "tue", "wed", "thu", "fri", "sat" };
	int			i;

	if (len != 3)
		return -1;
	for (i = 0; i < 7; i++)
		if (pg_strncasecmp(tok, days[i], 3) == 0)
			return i;
	return -1;
}

/* Parse "HH:MM" into minutes since midnight (0-1440). Returns -1 on error. */
static int
fasttrun_sched_parse_time(const char *s, const char *end)
{
	int			h = 0, m = 0;

	if (end - s != 5 || s[2] != ':')
		return -1;
	if (!isdigit((unsigned char) s[0]) || !isdigit((unsigned char) s[1]) ||
		!isdigit((unsigned char) s[3]) || !isdigit((unsigned char) s[4]))
		return -1;
	h = (s[0] - '0') * 10 + (s[1] - '0');
	m = (s[3] - '0') * 10 + (s[4] - '0');
	if (h > 24 || m > 59 || (h == 24 && m > 0))
		return -1;
	return h * 60 + m;
}

/*
 * Parse schedule string into out_windows[]. Returns number of windows,
 * or -1 on parse error. Empty string is valid (returns 0).
 *
 * Format: "mon-fri 08:00-18:00; sat 10:00-14:00"
 */
static int
fasttrun_sched_parse(const char *input,
					 FasttrunSchedWindow *out_windows,
					 int max_windows)
{
	const char *p = input;
	int			count = 0;

	if (input == NULL)
		return 0;

	/* Skip leading whitespace */
	while (*p && isspace((unsigned char) *p))
		p++;

	if (*p == '\0')
		return 0;

	while (*p)
	{
		FasttrunSchedWindow w = { 0, 0, 0 };
		const char *day_start = p;
		const char *day_end;
		const char *time_start;
		const char *time_end;
		const char *dash;
		int			start_min, end_min;

		if (count >= max_windows)
			return -1;				/* too many windows */

		/* Find end of day spec (first whitespace) */
		while (*p && !isspace((unsigned char) *p))
			p++;
		day_end = p;

		/* Skip whitespace between day and time */
		while (*p && isspace((unsigned char) *p) && *p != ';')
			p++;
		if (*p == '\0' || *p == ';')
			return -1;				/* missing time spec */

		time_start = p;
		while (*p && *p != ';')
			p++;
		time_end = p;
		/* Trim trailing whitespace from time */
		while (time_end > time_start && isspace((unsigned char) time_end[-1]))
			time_end--;

		/* Parse day spec: comma-separated list of days or day ranges */
		{
			const char *dp = day_start;

			while (dp < day_end)
			{
				const char *tok_start = dp;
				const char *tok_end;
				const char *day_dash;
				int			d1, d2, d;

				while (dp < day_end && *dp != ',')
					dp++;
				tok_end = dp;
				if (*dp == ',')
					dp++;

				day_dash = memchr(tok_start, '-', tok_end - tok_start);
				if (day_dash)
				{
					d1 = fasttrun_sched_parse_day(tok_start, day_dash - tok_start);
					d2 = fasttrun_sched_parse_day(day_dash + 1,
												  tok_end - day_dash - 1);
					if (d1 < 0 || d2 < 0)
						return -1;
					if (d1 <= d2)
					{
						for (d = d1; d <= d2; d++)
							w.day_mask |= (1 << d);
					}
					else
					{
						/* Wrap around: fri-mon means fri, sat, sun, mon */
						for (d = d1; d <= 6; d++)
							w.day_mask |= (1 << d);
						for (d = 0; d <= d2; d++)
							w.day_mask |= (1 << d);
					}
				}
				else
				{
					d1 = fasttrun_sched_parse_day(tok_start, tok_end - tok_start);
					if (d1 < 0)
						return -1;
					w.day_mask |= (1 << d1);
				}
			}
		}

		/* Parse time spec: HH:MM-HH:MM */
		dash = memchr(time_start, '-', time_end - time_start);
		if (!dash)
			return -1;
		start_min = fasttrun_sched_parse_time(time_start, dash);
		end_min = fasttrun_sched_parse_time(dash + 1, time_end);
		if (start_min < 0 || end_min < 0)
			return -1;
		if (end_min <= start_min)
			return -1;				/* no midnight crossing */

		w.start_min = (uint16) start_min;
		w.end_min = (uint16) end_min;
		out_windows[count++] = w;

		/* Skip window separator */
		if (*p == ';')
			p++;
		while (*p && isspace((unsigned char) *p))
			p++;
	}

	return count;
}

/* Return true if current local time is within any schedule window. */
static bool
fasttrun_schedule_is_active_now(void)
{
	pg_time_t	now;
	pg_time_t	cache_minute;
	struct pg_tm *tm;
	int			weekday, minute_of_day;
	int			i;

	if (fasttrun_sched_window_count == 0)
		return true;				/* no schedule = always active */

	now = (pg_time_t) time(NULL);
	cache_minute = now / 60;
	if (fasttrun_sched_cache_minute == cache_minute)
		return fasttrun_sched_cache_active;

	tm = pg_localtime(&now, log_timezone);
	if (tm == NULL)
	{
		fasttrun_sched_cache_minute = cache_minute;
		fasttrun_sched_cache_active = true;
		return true;				/* tz error — fall back to active */
	}

	weekday = tm->tm_wday;			/* 0=Sunday */
	minute_of_day = tm->tm_hour * 60 + tm->tm_min;

	for (i = 0; i < fasttrun_sched_window_count; i++)
	{
		FasttrunSchedWindow *w = &fasttrun_sched_windows[i];

		if ((w->day_mask & (1 << weekday)) &&
			minute_of_day >= w->start_min &&
			minute_of_day < w->end_min)
		{
			fasttrun_sched_cache_minute = cache_minute;
			fasttrun_sched_cache_active = true;
			return true;
		}
	}

	fasttrun_sched_cache_minute = cache_minute;
	fasttrun_sched_cache_active = false;
	return false;
}

/* GUC assign hook: parse schedule string, apply to static state. */
static void
fasttrun_track_schedule_assign_hook(const char *newval, void *extra)
{
	FasttrunSchedWindow	parsed[FASTTRUN_SCHED_MAX_WINDOWS];
	int		n;

	n = fasttrun_sched_parse(newval, parsed, FASTTRUN_SCHED_MAX_WINDOWS);
	if (n < 0)
	{
		ereport(WARNING,
				(errmsg("fasttrun: invalid track_schedule \"%s\", tracking will stay always-on",
						newval)));
		fasttrun_sched_window_count = 0;
		fasttrun_sched_cache_minute = (pg_time_t) -1;
		fasttrun_sched_cache_active = true;
		return;
	}

	memcpy(fasttrun_sched_windows, parsed, sizeof(parsed));
	fasttrun_sched_window_count = n;
	fasttrun_sched_cache_minute = (pg_time_t) -1;
	fasttrun_sched_cache_active = true;
}

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

static void
fasttrun_evict_temp_relid(Oid relid)
{
	Relation	rel;

	if (!OidIsValid(relid))
		return;
	if (get_rel_persistence(relid) != RELPERSISTENCE_TEMP ||
		!isTempNamespace(get_rel_namespace(relid)))
		return;

	rel = try_relation_open(relid, NoLock);
	if (rel != NULL)
	{
		if (rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELKIND_TOASTVALUE)
			fasttrun_cache_mark_rel_and_indexes_evicted(rel);
		else
			fasttrun_cache_mark_evicted(relid);
		relation_close(rel, NoLock);
	}
	else
		fasttrun_cache_mark_evicted(relid);

	fasttrun_stats_cache_mark_evicted_relid(relid);
	fasttrun_invalidate_local_plan_cache(relid);
}

static void
fasttrun_evict_rangevar(RangeVar *rv)
{
	Oid		relid;

	if (rv == NULL)
		return;
	relid = RangeVarGetRelid(rv, NoLock, true);
	fasttrun_evict_temp_relid(relid);
}

static void
fasttrun_evict_all_session_caches(void)
{
	fasttrun_cache_reset();
	fasttrun_stats_cache_reset();
}

static void
fasttrun_evict_utility_caches(Node *parsetree)
{
	if (parsetree == NULL)
		return;

	if (fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL)
		return;

	switch (nodeTag(parsetree))
	{
		case T_AlterTableStmt:
			{
				AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

				fasttrun_evict_rangevar(stmt->relation);
				break;
			}
		case T_TruncateStmt:
			{
				TruncateStmt *stmt = (TruncateStmt *) parsetree;
				ListCell   *lc;

				foreach(lc, stmt->relations)
					fasttrun_evict_rangevar((RangeVar *) lfirst(lc));
				break;
			}
		case T_IndexStmt:
			{
				IndexStmt  *stmt = (IndexStmt *) parsetree;

				fasttrun_evict_rangevar(stmt->relation);
				break;
			}
		case T_VacuumStmt:
			{
				VacuumStmt *stmt = (VacuumStmt *) parsetree;
				ListCell   *lc;

				if (stmt->rels == NIL)
				{
					fasttrun_evict_all_session_caches();
					break;
				}

				foreach(lc, stmt->rels)
				{
					VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);

					if (OidIsValid(vrel->oid))
						fasttrun_evict_temp_relid(vrel->oid);
					else
						fasttrun_evict_rangevar(vrel->relation);
				}
				break;
			}
		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;
				ListCell   *lc;

				if (stmt->removeType != OBJECT_TABLE &&
					stmt->removeType != OBJECT_INDEX)
					break;
				foreach(lc, stmt->objects)
				{
					List	   *name = (List *) lfirst(lc);
					RangeVar   *rv = makeRangeVarFromNameList(name);

					fasttrun_evict_rangevar(rv);
				}
				break;
			}
		default:
			break;
	}
}

/* ProcessUtility hook: track CREATE TEMP TABLE and evict stats on temp-table DDL. */
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

			if (from_dummy && fasttrun_schedule_is_active_now())
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

	fasttrun_evict_utility_caches(parsetree);

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
	int					output_count;
	int					i;
	int					min_idx = 0;
	bool				min_idx_valid = false;
	bool				bounded_topn;

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

	bounded_topn = (limit > 0 && limit <= FASTTRUN_TRACK_TOPN_LIMIT);
	if (bounded_topn)
		alloc = limit;

	/* Collect entries into a sortable array, or keep only a bounded top-N. */
	sorted = palloc(sizeof(FasttrunTrackEntry) * alloc);

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;

		if (!bounded_topn)
		{
			if (count >= alloc)
			{
				alloc *= 2;
				sorted = repalloc(sorted, sizeof(FasttrunTrackEntry) * alloc);
			}
			memcpy(&sorted[count++], entry, sizeof(FasttrunTrackEntry));
		}
		else if (count < limit)
		{
			memcpy(&sorted[count], entry, sizeof(FasttrunTrackEntry));
			if (!min_idx_valid ||
				sorted[count].create_count < sorted[min_idx].create_count)
			{
				min_idx = count;
				min_idx_valid = true;
			}
			count++;
		}
		else
		{
			if (entry->create_count > sorted[min_idx].create_count)
			{
				int		j;

				memcpy(&sorted[min_idx], entry, sizeof(FasttrunTrackEntry));
				for (j = 1, min_idx = 0; j < count; j++)
				{
					if (sorted[j].create_count < sorted[min_idx].create_count)
						min_idx = j;
				}
			}
		}
	}
	LWLockRelease(fasttrun_track_lock);

	/* Sort by create_count DESC. */
	qsort(sorted, count, sizeof(FasttrunTrackEntry),
		  fasttrun_track_cmp_desc);

	/* Emit top-N rows. */
	if (bounded_topn)
		output_count = count;
	else if (limit <= 0 || limit > count)
		output_count = count;
	else
		output_count = limit;

	for (i = 0; i < output_count; i++)
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
	int		min_idx = 0;
	bool	min_idx_valid = false;
	bool	bounded_topn;
	FasttrunTrackEntry *sorted;
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;

	if (fasttrun_track_htab == NULL)
		PG_RETURN_INT32(0);

	bounded_topn = (limit > 0 && limit <= FASTTRUN_TRACK_TOPN_LIMIT);
	if (bounded_topn)
		alloc = limit;

	sorted = palloc(sizeof(FasttrunTrackEntry) * alloc);

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;

		if (!bounded_topn)
		{
			if (count >= alloc)
			{
				alloc *= 2;
				sorted = repalloc(sorted, sizeof(FasttrunTrackEntry) * alloc);
			}
			memcpy(&sorted[count++], entry, sizeof(FasttrunTrackEntry));
		}
		else if (count < limit)
		{
			memcpy(&sorted[count], entry, sizeof(FasttrunTrackEntry));
			if (!min_idx_valid ||
				sorted[count].create_count < sorted[min_idx].create_count)
			{
				min_idx = count;
				min_idx_valid = true;
			}
			count++;
		}
		else
		{
			if (entry->create_count > sorted[min_idx].create_count)
			{
				int		j;

				memcpy(&sorted[min_idx], entry, sizeof(FasttrunTrackEntry));
				for (j = 1, min_idx = 0; j < count; j++)
				{
					if (sorted[j].create_count < sorted[min_idx].create_count)
						min_idx = j;
				}
			}
		}
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
							 "fasttrun_analyze refreshes column stats",
							 "Default 0.2 — significant DML (>=20% churn) auto-"
							 "triggers a full reservoir-sample refresh of column "
							 "stats.  Set to 0 to refresh on any DML, set to 1 "
							 "to disable auto-refresh entirely; post-DML cached "
							 "stats are then hidden from the planner until an "
							 "explicit refresh.",
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

	/*
	 * Planner/stats hooks are installed lazily on first use of the local
	 * stats caches.  A backend that only loads fasttrun for tracking, or
	 * never calls fasttrun_analyze/fasttrun_collect_stats, should not pay
	 * a planner-hook call on unrelated queries.
	 */
	prev_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = fasttrun_utility_hook;

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

		/* Shared memory tracking — only when loaded via shared_preload_libraries. */
		if (process_shared_preload_libraries_in_progress)
		{
			prev_shmem_request_hook = shmem_request_hook;
			shmem_request_hook = fasttrun_shmem_request;

			prev_shmem_startup_hook = shmem_startup_hook;
			shmem_startup_hook = fasttrun_shmem_startup;
		}
	}
