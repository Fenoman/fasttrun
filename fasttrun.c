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
#include "access/detoast.h"
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
#include "catalog/objectaccess.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
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
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/pgstat_internal.h"	/* pgstat_fetch_pending_entry */
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
#include "storage/procarray.h"
#if PG_VERSION_NUM >= 170000
#include "storage/read_stream.h"		/* PG17+ rewrote analyze scan on ReadStream */
#endif
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "executor/spi.h"
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
PG_FUNCTION_INFO_V1(fasttrun_hot_temp_tables);
PG_FUNCTION_INFO_V1(fasttrun_prewarm);
PG_FUNCTION_INFO_V1(fasttrun_reset_temp_stats);
#ifdef USE_ASSERT_CHECKING
PG_FUNCTION_INFO_V1(fasttrun_test_subxact_visits);
PG_FUNCTION_INFO_V1(fasttrun_test_poison_locator_mismatch);
PG_FUNCTION_INFO_V1(fasttrun_test_track_set);
#endif
Datum	fasttruncate(PG_FUNCTION_ARGS);
Datum	fasttrun_analyze(PG_FUNCTION_ARGS);
Datum	fasttrun_analyze_bulk(PG_FUNCTION_ARGS);
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
static double	fasttrun_invalidate_threshold = 0.2;
static bool		fasttrun_use_typanalyze = true;
static bool		fasttrun_zero_sinval_truncate = true;
static int		fasttrun_max_analyze_pages = 100000;

#ifdef USE_ASSERT_CHECKING
static char *fasttrun_test_failpoint = "";

static void
fasttrun_test_fail(const char *name, int ordinal)
{
	char		key[96];

	if (ordinal > 0)
		snprintf(key, sizeof(key), "%s:%d", name, ordinal);
	else
		strlcpy(key, name, sizeof(key));
	if (fasttrun_test_failpoint[0] != '\0' &&
		strcmp(fasttrun_test_failpoint, key) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun test failpoint: %s", key)));
}
#define FASTTRUN_TEST_FAILPOINT(name, ordinal) \
	fasttrun_test_fail((name), (ordinal))
#else
#define FASTTRUN_TEST_FAILPOINT(name, ordinal) \
	do { (void) sizeof(name); (void) sizeof(ordinal); } while (0)
#endif

/*
 * Per-planning freshness cache for column-stats hooks.
 *
 * A single plan can ask get_relation_stats_hook dozens of times for the same
 * temp relation (one call per predicate/column).  The expensive part is not
 * the stats hash lookup, but reopening the relation and walking pgstat xact
 * counters on every call.  While standard_planner runs, cache that pgstat
 * snapshot per relid; outside planner_hook, fall back to direct reads.
 *
 * One slot per distinct temp relid that supplied column stats in the current
 * plan.  256 slots cover join/CTE/partition-by-temp patterns; once a plan
 * touches more distinct temp relids than that, round-robin eviction
 * (next_evict) makes the evicted relid pay one RelationIdGetRelation + pgstat
 * read on its next probe.  Correctness is unaffected -- every read matches
 * slot->relid, so an evicted slot is a miss, not a wrong answer.  ~10 KB of
 * bss per backend; linear scan stays cache-friendly at this size and beats
 * hashing.  The cache resets after each plan, so a simple FIFO is enough.
 */
#define FASTTRUN_FRESHNESS_CACHE_SLOTS 256

typedef struct FasttrunFreshnessCacheEntry
{
	Oid			relid;
	bool		have;
	int64		ins;
	int64		upd;
	int64		del;
	bool		truncdropped;
	BlockNumber	pages;
} FasttrunFreshnessCacheEntry;

static bool		fasttrun_in_planner = false;
static int		fasttrun_freshness_cache_used = 0;
static int		fasttrun_freshness_cache_next_evict = 0;
static FasttrunFreshnessCacheEntry
				fasttrun_freshness_cache[FASTTRUN_FRESHNESS_CACHE_SLOTS];

/* "Silent killer" warning -- one shot per backend. */
static bool		fasttrun_warned_track_counts_off = false;

/* RNG state for reservoir sampling */
static pg_prng_state fasttrun_prng_state;

#ifdef USE_ASSERT_CHECKING
/* Test probe: counts relids examined by subxact cleanup. */
static uint64 fasttrun_test_subxact_visited = 0;
#endif

/* One bounded journal frame per subtransaction that mutates local state. */
typedef enum FasttrunTouchFlags
{
	FASTTRUN_TOUCH_ANALYZE = 1 << 0,
	FASTTRUN_TOUCH_STATS = 1 << 1,
	FASTTRUN_TOUCH_DML = 1 << 2,
	FASTTRUN_TOUCH_DROPPED = 1 << 3,
	FASTTRUN_TOUCH_TRUNCATE_PREPARED = 1 << 4,
	FASTTRUN_TOUCH_TRUNCATE_MUTATED = 1 << 5,
	FASTTRUN_TOUCH_TRUNCATE_COMPLETE = 1 << 6,
	FASTTRUN_TOUCH_PLAN_INVALIDATE = 1 << 7
} FasttrunTouchFlags;

#define FASTTRUN_TOUCH_TRUNCATE_MASK \
	(FASTTRUN_TOUCH_TRUNCATE_PREPARED | \
	 FASTTRUN_TOUCH_TRUNCATE_MUTATED | \
	 FASTTRUN_TOUCH_TRUNCATE_COMPLETE)

typedef struct FasttrunXactRelEntry
{
	Oid			relid;			/* hash key */
	Oid			root_relid;		/* owning heap */
	uint32		flags;
	uint64		truncate_generation;
} FasttrunXactRelEntry;

typedef struct FasttrunXactFrame
{
	SubTransactionId subid;
	MemoryContext mcxt;
	HTAB	   *entries;
	struct FasttrunXactFrame *parent;
} FasttrunXactFrame;

static FasttrunXactFrame *fasttrun_xact_frame = NULL;
static uint64 fasttrun_truncate_generation = 0;

static FasttrunXactFrame *
fasttrun_xact_frame_for_current(bool create)
{
	SubTransactionId subid = GetCurrentSubTransactionId();
	FasttrunXactFrame *volatile new_frame = NULL;
	MemoryContext volatile new_mcxt = NULL;
	MemoryContext oldcxt;
	HASHCTL		ctl;

	if (fasttrun_xact_frame != NULL && fasttrun_xact_frame->subid == subid)
		return fasttrun_xact_frame;
	if (!create)
		return NULL;

	new_mcxt = AllocSetContextCreate(TopTransactionContext,
									 "fasttrun xact frame",
									 ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo((MemoryContext) new_mcxt);
	PG_TRY();
	{
		new_frame = (FasttrunXactFrame *) palloc0(sizeof(*new_frame));
		new_frame->subid = subid;
		new_frame->mcxt = (MemoryContext) new_mcxt;
		new_frame->parent = fasttrun_xact_frame;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunXactRelEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_frame->entries = hash_create("fasttrun xact frame relids", 16,
										 &ctl,
										 HASH_ELEM | HASH_BLOBS |
										 HASH_CONTEXT);
		MemoryContextSwitchTo(oldcxt);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_xact_frame = (FasttrunXactFrame *) new_frame;
	return fasttrun_xact_frame;
}

static FasttrunXactRelEntry *
fasttrun_xact_mark_relid(Oid relid, Oid root_relid, uint32 flags)
{
	FasttrunXactFrame *frame;
	FasttrunXactRelEntry *entry;
	bool		found;

	if (!OidIsValid(relid))
		return NULL;
	Assert((flags & FASTTRUN_TOUCH_TRUNCATE_MASK) == 0);
	if (!OidIsValid(root_relid))
		root_relid = relid;

	frame = fasttrun_xact_frame_for_current(true);
	entry = (FasttrunXactRelEntry *)
		hash_search(frame->entries, &relid, HASH_ENTER, &found);
	if (!found)
	{
		entry->root_relid = root_relid;
		entry->flags = 0;
		entry->truncate_generation = 0;
	}
	else if (OidIsValid(root_relid))
		entry->root_relid = root_relid;
	entry->flags |= flags;

	/* A root entry makes abort-time plan invalidation allocation-free. */
	if ((flags & FASTTRUN_TOUCH_PLAN_INVALIDATE) != 0 &&
		root_relid != relid)
	{
		FasttrunXactRelEntry *root_entry;

		root_entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &root_relid, HASH_ENTER, &found);
		if (!found)
		{
			root_entry->root_relid = root_relid;
			root_entry->flags = 0;
			root_entry->truncate_generation = 0;
		}
		root_entry->flags |= FASTTRUN_TOUCH_PLAN_INVALIDATE;
	}

	return entry;
}

static int
fasttrun_truncate_phase_rank(uint32 phase)
{
	if (phase == FASTTRUN_TOUCH_TRUNCATE_COMPLETE)
		return 3;
	if (phase == FASTTRUN_TOUCH_TRUNCATE_MUTATED)
		return 2;
	Assert(phase == FASTTRUN_TOUCH_TRUNCATE_PREPARED);
	return 1;
}

static inline FasttrunXactRelEntry *
fasttrun_xact_mark_truncate(Oid relid, Oid root_relid, uint64 generation,
							uint32 phase)
{
	FasttrunXactRelEntry *entry;
	uint32		old_phase;

	Assert((phase & FASTTRUN_TOUCH_TRUNCATE_MASK) == phase);
	Assert(phase != 0 && (phase & (phase - 1)) == 0);
	entry = fasttrun_xact_mark_relid(relid, root_relid, 0);
	old_phase = entry->flags & FASTTRUN_TOUCH_TRUNCATE_MASK;
	if (generation > entry->truncate_generation ||
		(generation == entry->truncate_generation &&
		 (old_phase == 0 ||
		  fasttrun_truncate_phase_rank(phase) >
		  fasttrun_truncate_phase_rank(old_phase))))
	{
		entry->flags = (entry->flags & ~FASTTRUN_TOUCH_TRUNCATE_MASK) | phase;
		entry->truncate_generation = generation;
	}
	return entry;
}

static void
fasttrun_xact_merge_entry(FasttrunXactFrame *parent,
						  FasttrunXactRelEntry *child)
{
	FasttrunXactRelEntry *dst;
	uint32		child_phase = child->flags & FASTTRUN_TOUCH_TRUNCATE_MASK;
	uint32		dst_phase;
	bool		found;

	dst = (FasttrunXactRelEntry *)
		hash_search(parent->entries, &child->relid, HASH_ENTER, &found);
	if (!found)
	{
		*dst = *child;
		return;
	}

	dst->root_relid = child->root_relid;
	dst->flags |= child->flags & ~FASTTRUN_TOUCH_TRUNCATE_MASK;
	dst_phase = dst->flags & FASTTRUN_TOUCH_TRUNCATE_MASK;
	if (child_phase != 0 &&
		(child->truncate_generation > dst->truncate_generation ||
		 (child->truncate_generation == dst->truncate_generation &&
		  (dst_phase == 0 ||
		   fasttrun_truncate_phase_rank(child_phase) >
		   fasttrun_truncate_phase_rank(dst_phase)))))
	{
		dst->flags = (dst->flags & ~FASTTRUN_TOUCH_TRUNCATE_MASK) | child_phase;
		dst->truncate_generation = child->truncate_generation;
	}
}

static bool
fasttrun_xact_entry_dropped(const FasttrunXactRelEntry *entry)
{
	return (entry->flags & FASTTRUN_TOUCH_DROPPED) != 0;
}

typedef struct FasttrunCollectResult
{
	bool		published_any;
	bool		became_neutral;
} FasttrunCollectResult;

/* Forward decls for stats infrastructure (defined below) */
static void fasttrun_stats_cache_reset(void);
static void fasttrun_stats_cache_commit_xact(void);
static bool fasttrun_stats_cache_evict_relid(Oid relid);
static void fasttrun_stats_forget_relid(Oid relid);
static void fasttrun_subxact_callback(SubXactEvent event,
									  SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);
static FasttrunCollectResult fasttrun_collect_and_store(Relation rel,
												HeapTuple *sample,
												int sample_count,
												int64 totalrows,
												bool sample_needs_tid_sort);
static bool fasttrun_read_pgstat_counters(Relation rel,
										  int64 *ins, int64 *upd, int64 *del,
										  bool *truncdropped);
static bool fasttrun_read_pgstat_counters_for_hook(Oid relid,
												   int64 *ins, int64 *upd,
												   int64 *del,
												   bool *truncdropped,
												   BlockNumber *pages);
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
static void fasttrun_executor_start(QueryDesc *queryDesc, int eflags);
static bool fasttrun_stats_relid_exists(Oid relid);
static void fasttrun_stats_relid_ref(Oid relid);
static void fasttrun_stats_relid_unref(Oid relid);
static bool fasttrun_query_contains_stats_relid(Query *query);
static bool fasttrun_contains_stats_sublink_walker(Node *node, void *context);
static bool fasttrun_reinject_query_relstats(Query *query, bool detect_stats);
static bool fasttrun_reinject_sublink_walker(Node *node, void *context);

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
 * or computed new_tuples < 0.  pgstat unavailable -> fall back to scan.
 */

/*
 * Every field that can change planner decisions travels in one COW snapshot.
 * Restoring only relstats or only the pgstat anchors can combine states from
 * different subtransactions.  The allocation-free lazy probe stays outside.
 */
typedef struct FasttrunAnalyzeState
{
	bool		has_relstats;
	RelFileLocator cached_locator;
	BlockNumber	cached_pages;
	int64		cached_tuples;
	BlockNumber	cached_allvisible;
	BlockNumber	last_inval_pages;
	float4		last_inval_tuples;
	bool		last_inval_valid;
	RelFileLocatorBackend probe_rlb;
	Oid			heap_relid;
	RelFileLocatorBackend heap_rlb;
	bool		has_delta_state;
	int64		cached_inserted;
	int64		cached_updated;
	int64		cached_deleted;
	bool		cached_truncdropped;
	bool		has_stats_baseline;
	int64		stats_baseline_inserted;
	int64		stats_baseline_updated;
	int64		stats_baseline_deleted;
	bool		stats_baseline_truncdropped;
	bool		partial_scan_valid;
	int64		partial_scan_inserted;
	int64		partial_scan_updated;
	int64		partial_scan_deleted;
} FasttrunAnalyzeState;

typedef struct FasttrunAnalyzeUndo
{
	FasttrunAnalyzeState state;
	SubTransactionId state_subid;
	struct FasttrunAnalyzeUndo *older;
} FasttrunAnalyzeUndo;

typedef struct FasttrunAnalyzeCacheEntry
{
	Oid			relid;			/* hash key -- must be first */
	FasttrunAnalyzeState state;
	SubTransactionId state_subid;
	FasttrunAnalyzeUndo *undo;

	/*
	 * Memo for the lazy empty-storage probe in fasttrun_reinject_relstats().
	 *
	 * If lazy_check_subid matches GetCurrentSubTransactionId() and
	 * lazy_check_pages matches cached_pages, storage was already verified
	 * in this subxact.  Every later plan can skip RelationGetNumberOfBlocks.
	 *
	 * Reset on three events.  When cached_pages drifts (see
	 * fasttrun_cache_store_relstats).  On xact commit -- the next xact may
	 * see an external truncate from ON COMMIT DELETE ROWS.  And inside
	 * fasttrun_subxact_callback for an aborted subxact -- the observation
	 * from a rolled-back subxact is no longer trustworthy.
	 */
	BlockNumber		lazy_check_pages;
	SubTransactionId lazy_check_subid;
} FasttrunAnalyzeCacheEntry;

static HTAB			   *fasttrun_analyze_cache = NULL;
static MemoryContext	fasttrun_analyze_mcxt = NULL;

/*
 * An ERROR after the first storage reset cannot restore old files.  Keep one
 * operation record until every relation is empty or access to the table has
 * been blocked.
 */
typedef enum FasttrunTruncatePhase
{
	FASTTRUN_TRUNCATE_PREPARED,
	FASTTRUN_TRUNCATE_MUTATED,
	FASTTRUN_TRUNCATE_COMPLETE,
	FASTTRUN_TRUNCATE_POISONED
} FasttrunTruncatePhase;

typedef enum FasttrunTruncateRelKind
{
	FASTTRUN_TRUNCATE_USER_INDEX,
	FASTTRUN_TRUNCATE_TOAST_INDEX,
	FASTTRUN_TRUNCATE_TOAST_HEAP,
	FASTTRUN_TRUNCATE_MAIN_HEAP
} FasttrunTruncateRelKind;

typedef struct FasttrunPoisonEntry FasttrunPoisonEntry;

typedef struct FasttrunTruncateResultSlot
{
	Oid			relid;
	Oid			root_relid;
	RelFileLocatorBackend rlb;
	Oid			heap_relid;
	RelFileLocatorBackend heap_rlb;
	FasttrunTruncateRelKind kind;
	FasttrunAnalyzeCacheEntry *analyze_entry;
	BlockNumber rebuilt_pages;
	bool		published;
} FasttrunTruncateResultSlot;

typedef struct FasttrunTruncateOperation
{
	Oid			root_relid;
	RelFileLocatorBackend root_rlb;
	uint64		generation;
	FasttrunTruncatePhase phase;
	int			nuser_indexes;
	int			ntoast_indexes;
	int			nslots;
	int			published_slots;
	bool		have_pgstat_seed;
	int64		seed_inserted;
	int64		seed_updated;
	int64		seed_deleted;
	bool		seed_truncdropped;
	FasttrunPoisonEntry *registry_entry;
	FasttrunTruncateResultSlot slots[FLEXIBLE_ARRAY_MEMBER];
} FasttrunTruncateOperation;

struct FasttrunPoisonEntry
{
	Oid			root_relid;		/* hash key -- must be first */
	RelFileLocatorBackend root_rlb;
	uint64		generation;
	bool		active;
	FasttrunTruncateOperation *active_operation;
	FasttrunTruncateOperation *pending_operation;
};

typedef struct FasttrunOpenedWorkset
{
	List	   *user_indexes;
	List	   *toast_indexes;
	Relation	toastrel;
	Relation   *slot_relations;
	int			nslots;
} FasttrunOpenedWorkset;

static MemoryContext fasttrun_operation_mcxt = NULL;
static HTAB *fasttrun_poison_cache = NULL;

static void fasttrun_xact_callback(XactEvent event, void *arg);
static void fasttrun_analyze_save_undo(FasttrunAnalyzeCacheEntry *entry);
static void fasttrun_poison_commit_xact(void);
static void fasttrun_poison_clear_all(void);
static void fasttrun_poison_check_relation(Relation rel,
										   const char *operation,
										   bool allow_repair);

static bool
fasttrun_relation_has_same_locator(Relation rel, FasttrunAnalyzeCacheEntry *entry)
{
	return entry->state.has_relstats &&
		RelFileLocatorEquals(rel->rd_locator, entry->state.cached_locator);
}

/*
 * Some temp-table truncates happen outside ordinary transactional rollback:
 * core ON COMMIT DELETE ROWS truncates storage during COMMIT, and
 * fasttruncate() deliberately performs a non-transactional local storage reset.
 * If a session-local relstats cache survives such a boundary unchanged, the
 * next plan can see rows in an empty table.  Whenever we can observe that the
 * underlying temp storage is empty, make that fact authoritative for heap and
 * index relstats.
 *
 * Runs from the subxact-abort callback, where relcache and syscache access
 * is off-limits (TRANS_ABORT; PG18 asserts inside RelationIdGetRelation).
 * The probe therefore stays at smgr level, addressing storage through the
 * RelFileLocatorBackend captured at store time.  A vanished or replaced
 * storage file (smgrexists false) means the cached identity no longer holds
 * -- bail out to the ordinary undo path.  An index keeps its metapage even
 * after truncate, so index emptiness is judged by the owning heap's blocks.
 * rd_rel is left alone: the planner-hook reinject repairs it on the next
 * plan that touches the relation.
 */
static bool
fasttrun_cache_make_empty_storage_authoritative(FasttrunAnalyzeCacheEntry *entry,
												Oid *plan_relid)
{
	SMgrRelation own;

	*plan_relid = entry->state.heap_relid;

	if (!entry->state.has_relstats || !OidIsValid(entry->state.heap_relid))
		return false;

	own = smgropen(entry->state.probe_rlb.locator, entry->state.probe_rlb.backend);
	if (!smgrexists(own, MAIN_FORKNUM))
		return false;

	if (entry->state.heap_relid != entry->relid)
	{
		/* Index entry: emptiness is the owning heap's emptiness. */
		SMgrRelation heap = smgropen(entry->state.heap_rlb.locator,
									 entry->state.heap_rlb.backend);

		if (!smgrexists(heap, MAIN_FORKNUM) ||
			smgrnblocks(heap, MAIN_FORKNUM) != 0)
			return false;
	}
	else if (smgrnblocks(own, MAIN_FORKNUM) != 0)
		return false;

	/* Heap gives 0 pages here; a rebuilt index keeps its metapage. */
	entry->state.has_relstats = true;
	entry->state.cached_pages = smgrnblocks(own, MAIN_FORKNUM);
	entry->state.cached_tuples = 0;
	entry->state.cached_allvisible = 0;
	entry->state_subid = InvalidSubTransactionId;
	entry->state.has_delta_state = false;
	/* cached_pages drifted -- drop the lazy memo. */
	entry->lazy_check_subid = InvalidSubTransactionId;
	entry->lazy_check_pages = 0;

	return true;
}

static BlockNumber
fasttrun_count_allvisible(Relation rel)
{
	BlockNumber	relallvisible = 0;

	if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		visibilitymap_count(rel, &relallvisible, NULL);
	return relallvisible;
}

static void
fasttrun_analyze_free_undo(FasttrunAnalyzeCacheEntry *entry)
{
	while (entry->undo != NULL)
	{
		FasttrunAnalyzeUndo *popped = entry->undo;

		entry->undo = popped->older;
		pfree(popped);
	}
}

static void
fasttrun_cache_reset_partial_scan_anchor(FasttrunAnalyzeCacheEntry *entry)
{
	entry->state.partial_scan_valid = false;
	entry->state.partial_scan_inserted = 0;
	entry->state.partial_scan_updated = 0;
	entry->state.partial_scan_deleted = 0;
}

/*
 * Record the counters at the moment the partial-index relstats were
 * brought in sync with the heap.  See the anchor comment in
 * FasttrunAnalyzeCacheEntry.
 */
static void
fasttrun_cache_set_partial_scan_anchor(FasttrunAnalyzeCacheEntry *entry,
									   int64 ins, int64 upd, int64 del)
{
	fasttrun_analyze_save_undo(entry);
	entry->state.partial_scan_valid = true;
	entry->state.partial_scan_inserted = ins;
	entry->state.partial_scan_updated = upd;
	entry->state.partial_scan_deleted = del;
}

/* Drop the analyze HTAB + its mcxt (frees all baseline undo chains). */
static void
fasttrun_cache_reset(void)
{
	HTAB	   *old_cache = fasttrun_analyze_cache;
	MemoryContext old_mcxt = fasttrun_analyze_mcxt;

	if (old_cache == NULL && old_mcxt == NULL)
		return;

	fasttrun_analyze_cache = NULL;
	fasttrun_analyze_mcxt = NULL;

	if (old_mcxt != NULL)
		MemoryContextDelete(old_mcxt);
	else if (old_cache != NULL)
		hash_destroy(old_cache);
}

/*
 * COMMIT-time bookkeeping for the analyze cache.
 *
 * The 2.2.0 version of this routine was a CPU hog.  It walked the entire
 * cache on every COMMIT.  For each entry it called
 * fasttrun_cache_make_empty_storage_authoritative() -- which does
 * try_relation_open plus RelationGetNumberOfBlocks per entry.  Then a
 * nested fasttrun_stats_cache_evict_relid scanned the stats hash, and a
 * burst of fasttrun_invalidate_local_plan_cache calls fired.  On a
 * backend with many ON COMMIT DELETE ROWS temp tables that get
 * re-analyzed every xact, the cost was N x (smgr call + plan-cache walk)
 * per COMMIT.  It dominated CPU.
 *
 * The empty-storage adoption survived, just in a different place.  It
 * now happens in fasttrun_reinject_relstats() on the planner_hook path.
 * Runs at most once per relation per query, and only when the planner
 * actually touches the table.  Stale shared-invalidation traffic from
 * the COMMIT callback is gone.
 *
 * What this function does now:
 *   - walks the per-xact touched-relid list, not the whole cache;
 *   - drops entries whose relation has vanished (the long-lived
 *     backend case -- ON COMMIT DROP temp tables die in
 *     PreCommit_on_commit_actions() before this callback fires; without
 *     explicit cleanup the entry would stay forever and the backend
 *     would slowly grow);
 *   - frees per-subxact undo chains attached to surviving entries;
 *   - clears the xact-scoped delta-math snapshot and baseline state;
 *   - drops entries that lost their planner-visible relstats
 *     (has_relstats == false -- happens after DDL/DROP eviction).
 */
/*
 * Marked noinline so the duration regression test
 * (check_fasttrun_commit_duration.sh) can keep a uprobe target.
 * The function is hot enough that O2 inlines it into the xact callback
 * otherwise.  The call cost is tiny relative to the work inside.
 */
static pg_noinline void
fasttrun_cache_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_analyze_cache == NULL || fasttrun_xact_frame == NULL)
		return;

	hash_seq_init(&status, fasttrun_xact_frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		Oid			relid = xentry->relid;
		FasttrunAnalyzeCacheEntry *entry;

		entry = (FasttrunAnalyzeCacheEntry *) hash_search(fasttrun_analyze_cache,
														  &relid,
														  HASH_FIND, NULL);
		if (entry == NULL)
			continue;

		/*
		 * Relation dropped this xact?  Drop the entry whole, move on.
		 * Catches ON COMMIT DROP, plain DROP done elsewhere in the xact,
		 * and dependency drops: every drop path fires OAT_DROP, which
		 * records the relid.  No syscache probe here -- this callback
		 * runs at TRANS_COMMIT, where catalog access is forbidden.
		 */
		if (fasttrun_xact_entry_dropped(xentry))
		{
			fasttrun_analyze_free_undo(entry);
			(void) hash_search(fasttrun_analyze_cache, &relid,
							   HASH_REMOVE, NULL);
			continue;
		}

		fasttrun_analyze_free_undo(entry);

		entry->state.has_delta_state = false;
		entry->state.cached_inserted = 0;
		entry->state.cached_updated = 0;
		entry->state.cached_deleted = 0;
		entry->state.cached_truncdropped = false;

		entry->state.has_stats_baseline = false;
		entry->state.stats_baseline_inserted = 0;
		entry->state.stats_baseline_updated = 0;
		entry->state.stats_baseline_deleted = 0;
		entry->state.stats_baseline_truncdropped = false;
		entry->state_subid = InvalidSubTransactionId;

		/* Anchored to this xact's pgstat counters -- meaningless outside. */
		fasttrun_cache_reset_partial_scan_anchor(entry);

		/*
		 * Drop the lazy-probe memo.  ON COMMIT DELETE ROWS truncate runs
		 * in PreCommit_on_commit_actions(), before this callback.  The
		 * cached observation no longer holds outside this xact's lifetime.
		 */
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_pages = 0;

		if (!entry->state.has_relstats)
			(void) hash_search(fasttrun_analyze_cache, &relid,
							   HASH_REMOVE, NULL);
	}

	/*
	 * When the cache is empty, drop the HTAB and its mcxt so the planner-hook
	 * reinject walk returns to the zero-cost `cache == NULL` path.  Re-armed
	 * lazily on the next analyze.
	 */
	if (hash_get_num_entries(fasttrun_analyze_cache) == 0)
		fasttrun_cache_reset();
}

/* Finish analyze + stats caches on xact end.  PRE_COMMIT skipped -- txn may still abort. */
static void
fasttrun_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			fasttrun_poison_commit_xact();
			fasttrun_cache_commit_xact();
			fasttrun_stats_cache_commit_xact();
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * Reuse the frame-local undo path.  An unrelated abort has no frame
			 * and therefore leaves committed session-local statistics untouched.
			 */
			while (fasttrun_xact_frame != NULL)
			{
				FasttrunXactFrame *frame = fasttrun_xact_frame;

				fasttrun_subxact_callback(SUBXACT_EVENT_ABORT_SUB,
									  frame->subid,
									  InvalidSubTransactionId, NULL);
			}
			break;
		case XACT_EVENT_PREPARE:
			/* Prepared xacts cannot retain backend-private temp state. */
			fasttrun_poison_clear_all();
			fasttrun_cache_reset();
			fasttrun_stats_cache_reset();
			break;
		default:
			return;
	}

	/* Frame contexts are children of TopTransactionContext. */
	fasttrun_xact_frame = NULL;
}

/* Lazily allocate the analyze HTAB and its dedicated mcxt. */
static void
fasttrun_cache_init(void)
{
	HASHCTL		ctl;
	MemoryContext volatile new_mcxt = NULL;
	HTAB	   *new_cache = NULL;

	if (fasttrun_analyze_cache != NULL)
		return;

	fasttrun_ensure_planner_hook();

	PG_TRY();
	{
		new_mcxt = AllocSetContextCreate(TopMemoryContext,
										 "fasttrun analyze cache",
										 ALLOCSET_DEFAULT_SIZES);
		FASTTRUN_TEST_FAILPOINT("after_analyze_context", 0);

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunAnalyzeCacheEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_cache = hash_create("fasttrun analyze cache", 64, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	PG_CATCH();
	{
		if (new_mcxt != NULL)
			MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_analyze_mcxt = (MemoryContext) new_mcxt;
	fasttrun_analyze_cache = new_cache;
}

/* Publish the operation registry only after its context and HTAB exist. */
static void
fasttrun_operation_init(void)
{
	HASHCTL		ctl;
	MemoryContext volatile new_mcxt = NULL;
	HTAB	   *new_cache = NULL;

	if (fasttrun_poison_cache != NULL)
		return;

	/* The planner hook enforces active blocks. */
	fasttrun_ensure_planner_hook();

	PG_TRY();
	{
		new_mcxt = AllocSetContextCreate(TopMemoryContext,
										 "fasttrun operation context",
										 ALLOCSET_SMALL_SIZES);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunPoisonEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_cache = hash_create("fasttrun poison cache", 8, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	PG_CATCH();
	{
		if (new_mcxt != NULL)
			MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_operation_mcxt = (MemoryContext) new_mcxt;
	fasttrun_poison_cache = new_cache;
}

static void
fasttrun_operation_reset_if_empty(void)
{
	MemoryContext old_mcxt;

	if (fasttrun_poison_cache == NULL ||
		hash_get_num_entries(fasttrun_poison_cache) != 0)
		return;

	old_mcxt = fasttrun_operation_mcxt;
	fasttrun_poison_cache = NULL;
	fasttrun_operation_mcxt = NULL;
	MemoryContextDelete(old_mcxt);
}

static FasttrunPoisonEntry *
fasttrun_poison_find(Oid root_relid)
{
	if (fasttrun_poison_cache == NULL || !OidIsValid(root_relid))
		return NULL;
	return (FasttrunPoisonEntry *) hash_search(fasttrun_poison_cache,
												  &root_relid, HASH_FIND, NULL);
}

/*
 * Reserve the registry slot before mutation.  The final pointer assignment
 * is the publication boundary: errors before it leave no pending operation.
 */
static FasttrunPoisonEntry *
fasttrun_poison_reserve(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry;
	bool		found;

	fasttrun_operation_init();
	entry = (FasttrunPoisonEntry *) hash_search(fasttrun_poison_cache,
												   &operation->root_relid,
												   HASH_ENTER, &found);
	if (!found)
	{
		entry->root_rlb = operation->root_rlb;
		entry->generation = 0;
		entry->active = false;
		entry->active_operation = NULL;
		entry->pending_operation = NULL;
	}
	if (entry->pending_operation != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun: truncate is already pending for relation %u",
						operation->root_relid)));

	operation->registry_entry = entry;
	entry->root_rlb = operation->root_rlb;
	entry->generation = operation->generation;
	entry->pending_operation = operation;
	return entry;
}

/* A preparation error detaches this attempt and preserves an older block. */
static void
fasttrun_poison_cancel_pending(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	Oid			root_relid = operation->root_relid;

	if (entry != NULL && entry->pending_operation == operation &&
		entry->generation == operation->generation)
		entry->pending_operation = NULL;
	pfree(operation);

	if (entry != NULL && entry->pending_operation == NULL)
	{
		if (entry->active && entry->active_operation != NULL)
		{
			entry->root_rlb = entry->active_operation->root_rlb;
			entry->generation = entry->active_operation->generation;
		}
		else
			(void) hash_search(fasttrun_poison_cache, &root_relid,
								 HASH_REMOVE, NULL);
	}
	fasttrun_operation_reset_if_empty();
}

/* MUTATED failed: no allocation or catalog access is allowed here. */
static void
fasttrun_poison_activate(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	FasttrunTruncateOperation *old_active;

	Assert(entry != NULL);
	Assert(entry->pending_operation == operation);
	old_active = entry->active_operation;
	entry->pending_operation = NULL;
	entry->active_operation = operation;
	entry->active = true;
	entry->root_rlb = operation->root_rlb;
	entry->generation = operation->generation;
	operation->phase = FASTTRUN_TRUNCATE_POISONED;
	if (old_active != NULL && old_active != operation)
		pfree(old_active);
}

/* COMPLETE is the only successful repair boundary. */
static void
fasttrun_poison_complete(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	FasttrunTruncateOperation *old_active;
	Oid			root_relid = operation->root_relid;

	Assert(entry != NULL);
	Assert(entry->pending_operation == operation);
	old_active = entry->active_operation;
	entry->pending_operation = NULL;
	entry->active_operation = NULL;
	entry->active = false;
	if (old_active != NULL && old_active != operation)
		pfree(old_active);
	pfree(operation);
	(void) hash_search(fasttrun_poison_cache, &root_relid,
						 HASH_REMOVE, NULL);
	fasttrun_operation_reset_if_empty();
}

static void
fasttrun_poison_forget_relid(Oid root_relid)
{
	FasttrunPoisonEntry *entry = fasttrun_poison_find(root_relid);
	FasttrunTruncateOperation *active;
	FasttrunTruncateOperation *pending;

	if (entry == NULL)
		return;
	active = entry->active_operation;
	pending = entry->pending_operation;
	entry->active_operation = NULL;
	entry->pending_operation = NULL;
	entry->active = false;
	if (active != NULL)
		pfree(active);
	if (pending != NULL && pending != active)
		pfree(pending);
	(void) hash_search(fasttrun_poison_cache, &root_relid,
						 HASH_REMOVE, NULL);
	fasttrun_operation_reset_if_empty();
}

static void
fasttrun_poison_clear_all(void)
{
	MemoryContext old_mcxt = fasttrun_operation_mcxt;

	if (old_mcxt == NULL)
		return;
	fasttrun_poison_cache = NULL;
	fasttrun_operation_mcxt = NULL;
	MemoryContextDelete(old_mcxt);
}

static void
fasttrun_poison_check_relation(Relation rel, const char *operation,
								bool allow_repair)
{
	Oid			root_relid = RelationGetRelid(rel);
	FasttrunPoisonEntry *entry = fasttrun_poison_find(root_relid);

	if (entry == NULL || !entry->active)
		return;

	/* DROP/recreate or a relfilenode replacement retires an old marker. */
	if (!RelFileLocatorEquals(entry->root_rlb.locator, rel->rd_locator) ||
		entry->root_rlb.backend != rel->rd_backend)
	{
		fasttrun_poison_forget_relid(root_relid);
		return;
	}
	if (allow_repair)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("fasttrun: relation \"%s\" is unavailable after an incomplete truncate",
					RelationGetRelationName(rel)),
			 errdetail("The failed %s had already changed table files.",
					   operation),
			 errhint("Retry fasttruncate, or DROP and recreate the temporary table.")));
}

/* DROP is transactional; only COMMIT removes an active block. */
static void
fasttrun_poison_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_poison_cache == NULL || fasttrun_xact_frame == NULL)
		return;
	hash_seq_init(&status, fasttrun_xact_frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		if (fasttrun_xact_entry_dropped(xentry) &&
			fasttrun_poison_find(xentry->relid) != NULL)
			fasttrun_poison_forget_relid(xentry->relid);
	}
}

#ifdef USE_ASSERT_CHECKING
Datum
fasttrun_test_poison_locator_mismatch(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	FasttrunPoisonEntry *entry = fasttrun_poison_find(relid);

	if (entry == NULL || !entry->active)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun test poison is not active")));
	entry->root_rlb.locator.relNumber++;
	PG_RETURN_VOID();
}
#endif

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
		memset(&entry->state, 0, sizeof(entry->state));
		entry->state.heap_relid = InvalidOid;
		entry->state_subid = InvalidSubTransactionId;
		entry->undo = NULL;
		entry->lazy_check_pages = 0;
		entry->lazy_check_subid = InvalidSubTransactionId;
	}
	return entry;
}

static void
fasttrun_analyze_save_undo(FasttrunAnalyzeCacheEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	MemoryContext oldcxt;
	FasttrunAnalyzeUndo *saved;

	if (entry->state_subid == cur_subid)
		return;

	oldcxt = MemoryContextSwitchTo(fasttrun_analyze_mcxt);
	saved = (FasttrunAnalyzeUndo *) palloc(sizeof(*saved));
	MemoryContextSwitchTo(oldcxt);

	saved->state = entry->state;
	saved->state_subid = entry->state_subid;
	saved->older = entry->undo;
	entry->undo = saved;
	entry->state_subid = cur_subid;
}

static FasttrunAnalyzeCacheEntry *
fasttrun_cache_store_relstats(Relation rel, BlockNumber pages, int64 tuples,
							  BlockNumber allvisible)
{
	FasttrunAnalyzeCacheEntry *entry;

	entry = fasttrun_cache_enter(RelationGetRelid(rel));
	fasttrun_analyze_save_undo(entry);

	entry->state.has_relstats = true;
	entry->state.cached_locator = rel->rd_locator;
	entry->state.cached_pages = pages;
	entry->state.cached_tuples = tuples;
	entry->state.cached_allvisible = allvisible;
	entry->state.probe_rlb.locator = rel->rd_locator;
	entry->state.probe_rlb.backend = rel->rd_backend;
	/* Default: the entry is its own heap; index call sites override. */
	entry->state.heap_relid = RelationGetRelid(rel);
	entry->state.heap_rlb = entry->state.probe_rlb;
	/* cached_pages changed -- the lazy-probe memo is now stale. */
	entry->lazy_check_subid = InvalidSubTransactionId;
	entry->lazy_check_pages = 0;

	/*
	 * Register this relid so the xact-end callback walks it.  Index entries
	 * land here too (via fasttrun_update_index_relstats and
	 * fasttrun_rebuild_one_index) without going through the top-level touch
	 * in fasttrun_analyze or fasttruncate.
	 */
	fasttrun_xact_mark_relid(RelationGetRelid(rel), RelationGetRelid(rel),
							FASTTRUN_TOUCH_ANALYZE);

	return entry;
}

/* Point an index entry's emptiness probe at its owning heap's storage. */
static void
fasttrun_cache_set_owning_heap(FasttrunAnalyzeCacheEntry *entry,
							   Relation heaprel)
{
	uint32		flags = FASTTRUN_TOUCH_ANALYZE;

	entry->state.heap_relid = RelationGetRelid(heaprel);
	entry->state.heap_rlb.locator = heaprel->rd_locator;
	entry->state.heap_rlb.backend = heaprel->rd_backend;
	/* TOAST has no user plan of its own; its main heap already owns the plan. */
	if (heaprel->rd_rel->relkind != RELKIND_TOASTVALUE)
		flags |= FASTTRUN_TOUCH_PLAN_INVALIDATE;
	fasttrun_xact_mark_relid(entry->relid, entry->state.heap_relid,
							flags);
}

static void
fasttrun_cache_store_delta_state(FasttrunAnalyzeCacheEntry *entry,
								 int64 ins, int64 upd, int64 del,
								 bool truncdropped)
{
	entry->state.has_delta_state = true;
	entry->state.cached_inserted = ins;
	entry->state.cached_updated = upd;
	entry->state.cached_deleted = del;
	entry->state.cached_truncdropped = truncdropped;
}

/*
 * Record a fresh stats-collection baseline for relid: called both
 * after a successful collect_and_store (cold scan or refresh with a
 * non-empty sample) and after an empty refresh on a now-empty table.
 *
 * Lazily creates the entry if it doesn't exist yet -- fasttrun_analyze
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

	fasttrun_analyze_save_undo(entry);

	entry->state.has_stats_baseline = true;
	entry->state.stats_baseline_inserted = ins;
	entry->state.stats_baseline_updated = upd;
	entry->state.stats_baseline_deleted = del;
	entry->state.stats_baseline_truncdropped = truncdropped;

	/*
	 * Every baseline publish follows a refresh that also brought the index
	 * relstats (partial included) in sync -- advance the rescan anchor with
	 * the same counters so the next analyze does not re-probe old churn.
	 */
	fasttrun_cache_set_partial_scan_anchor(entry, ins, upd, del);
}

/*
 * Variant of set_stats_baseline that ONLY updates an already-existing
 * entry: used by fasttrun_collect_stats() to publish the new baseline
 * without seeding a fresh analyze-cache entry behind the user's back
 * (which would carry a bogus zero delta-math state).
 *
 * No-op if the analyze cache hasn't been touched for this relid yet.
 * In that case the user got fresh column stats but auto-refresh remains
 * disengaged for this relid until the next fasttrun_analyze() -- that's
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

	if (fasttrun_analyze_cache == NULL)
		return;

	entry = fasttrun_cache_lookup(relid);
	if (entry == NULL)
		return;

	fasttrun_analyze_save_undo(entry);

	entry->state.has_relstats = false;
	entry->state.has_delta_state = false;
	/* Eviction retires the published state -- the drift anchor with it. */
	entry->state.last_inval_valid = false;

	entry->state.has_stats_baseline = false;

	/* Eviction retires the rescan bookkeeping too. */
	fasttrun_cache_reset_partial_scan_anchor(entry);

	/* The next store resets cached_pages -- drop the lazy memo now. */
	entry->lazy_check_subid = InvalidSubTransactionId;
	entry->lazy_check_pages = 0;
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
	float4	cached_tuples;

	if (!fasttrun_relation_has_same_locator(rel, entry))
		return;

	/*
	 * Lazy empty-storage adoption.
	 *
	 * Core ON COMMIT DELETE ROWS truncates temp relations inside
	 * PreCommit_on_commit_actions().  That fires outside our xact callback.
	 * fasttruncate() also resets storage non-transactionally.  Either way,
	 * the cache may carry stale page counts.
	 *
	 * Version 2.2.0 reconciled this eagerly.  It walked the entire cache
	 * in fasttrun_cache_commit_xact() and read nblocks for each entry.  On
	 * a backend holding many ON COMMIT DELETE ROWS temp tables, that scaled
	 * as O(cache_size) per COMMIT and dominated CPU.
	 *
	 * The new approach observes the truth at planning time.  If storage
	 * went to zero since we cached, adopt that fact in place.  One
	 * smgrnblocks call per cached relid per query -- and only for relations
	 * the planner actually touches.  Strictly less work than the old scan.
	 *
	 * Empty heap means zero blocks.  An index keeps its metapage even after
	 * truncate (a "fresh" btree is one page).  So index emptiness is judged
	 * by the underlying heap's block count instead.
	 */
	{
		SubTransactionId cur_subid = GetCurrentSubTransactionId();
		bool		probe_needed = (entry->state.cached_pages > 0 &&
									rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
									RELKIND_HAS_STORAGE(rel->rd_rel->relkind));

		/*
		 * Memoize the probe.  Earlier queries in this subxact already
		 * verified that storage size matches cached_pages -- no need to
		 * repeat smgrnblocks on every plan touch.  Reset happens on
		 * cache_store_relstats (cached_pages drift) and on xact end.
		 */
		if (probe_needed &&
			entry->lazy_check_subid == cur_subid &&
			entry->lazy_check_pages == entry->state.cached_pages)
			probe_needed = false;

		if (probe_needed)
		{
			bool		looks_empty = false;

			if (rel->rd_rel->relkind == RELKIND_INDEX)
			{
				Oid			heapid = IndexGetRelation(RelationGetRelid(rel), true);

				if (OidIsValid(heapid))
				{
					/*
					 * The planner path always holds a lock on the owning
					 * heap, but fasttrun_relstats(index_name) reaches here
					 * with only the index locked -- relation_open with
					 * NoLock asserts then.  A transient AccessShareLock
					 * covers both callers.
					 */
					Relation	heaprel = try_relation_open(heapid,
															AccessShareLock);

					if (heaprel != NULL)
					{
						if (heaprel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
							isTempNamespace(RelationGetNamespace(heaprel)) &&
							RelationGetNumberOfBlocks(heaprel) == 0)
							looks_empty = true;
						relation_close(heaprel, AccessShareLock);
					}
				}
			}
			else if (rel->rd_rel->relkind == RELKIND_RELATION ||
					 rel->rd_rel->relkind == RELKIND_TOASTVALUE)
			{
				if (RelationGetNumberOfBlocks(rel) == 0)
					looks_empty = true;
			}

			if (looks_empty)
			{
				/*
				 * Heap gives 0.  Index gives 1 -- the freshly rebuilt
				 * btree/hash metapage stays put.  One nblocks call returns
				 * the right number for either case.
				 */
				entry->state.cached_pages = RelationGetNumberOfBlocks(rel);
				entry->state.cached_tuples = 0;
				entry->state.cached_allvisible = 0;
				entry->state.has_delta_state = false;
				entry->state.cached_inserted = 0;
				entry->state.cached_updated = 0;
				entry->state.cached_deleted = 0;
				entry->state.cached_truncdropped = false;
				/*
				 * Cached per-column stats for the now-empty relation get
				 * hidden by the freshness check in
				 * fasttrun_get_relation_stats_hook on the next access --
				 * pgstat ins/upd/del will no longer match the collect-time
				 * snapshot.  No need to evict them here.
				 */
			}

			/*
			 * Record the observation so later plans in this subxact skip
			 * the smgr probe.  After adoption the new cached_pages matches
			 * actual.  On a non-empty probe, both already matched the
			 * snapshot we just verified.
			 */
			entry->lazy_check_subid = cur_subid;
			entry->lazy_check_pages = entry->state.cached_pages;
		}
	}

	cached_tuples = (float4) entry->state.cached_tuples;

	if (rel->rd_rel->relpages == entry->state.cached_pages &&
		rel->rd_rel->reltuples == cached_tuples &&
		rel->rd_rel->relallvisible == (int32) entry->state.cached_allvisible)
		return;

	rel->rd_rel->relpages = entry->state.cached_pages;
	rel->rd_rel->reltuples = cached_tuples;
	rel->rd_rel->relallvisible = (int32) entry->state.cached_allvisible;
}

static void
fasttrun_reinject_cached_relation(Relation rel)
{
	FasttrunAnalyzeCacheEntry *entry;

	entry = fasttrun_cache_lookup(RelationGetRelid(rel));
	if (entry == NULL || !entry->state.has_relstats)
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
 *   * get_relation_stats_hook -- feeds n_distinct / null_frac and the
 *     full statsTuple into selectivity estimation paths that read
 *     stats through VariableStatData (e.g. eq_sel, scalararraysel).
 *
 *   * get_attavgwidth_hook    -- feeds stawidth into the SEPARATE path
 *     that the planner uses to size hash tables, sort tuplestores and
 *     join tuple widths (lsyscache.c:get_attavgwidth).  Without this
 *     second hook the planner falls back to get_typavgwidth() and
 *     happily uses the type's "default" width -- for `text` it's 32
 *     bytes regardless of the actual sample, which throws off
 *     hash/sort/spool costing on temp tables with short text columns.
 *     The two hooks have to be wired up independently because they
 *     read from different places (statsTuple vs. direct syscache
 *     lookup of the stawidth column).
 *
 * Both hooks share the same per-(relid, attnum) cache and the same
 * pgstat-counter freshness check.  The cache lives in a child of
 * TopMemoryContext, survives COMMIT for session-local ANALYZE-like planner
 * behavior, and restores only touched entries on transaction abort.
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

typedef enum FasttrunRelStatsPolicy
{
	FASTTRUN_REL_CORE_ALLOWED,
	FASTTRUN_REL_LOCAL_NEUTRAL
} FasttrunRelStatsPolicy;

typedef enum FasttrunColumnStatsState
{
	FASTTRUN_COLUMN_CORE_ALLOWED,
	FASTTRUN_COLUMN_LOCAL_CANDIDATE,
	FASTTRUN_COLUMN_LOCAL_NEUTRAL
} FasttrunColumnStatsState;

typedef struct FasttrunPgstatSnapshot
{
	int64		inserted;
	int64		updated;
	int64		deleted;
	bool		truncdropped;
	BlockNumber pages;
} FasttrunPgstatSnapshot;

/*
 * Saved previous version of a stats entry, pushed on every overwrite
 * that crosses a subxact boundary.  fasttrun_subxact_callback pops this
 * on ROLLBACK TO SAVEPOINT to fully restore the pre-subxact state -- not
 * just delete the subxact-local refresh.  Lives in fasttrun_stats_mcxt.
 */
typedef struct FasttrunStatsSavedState
{
	HeapTuple			statsTuple;
	int64				collected_ins;
	int64				collected_upd;
	int64				collected_del;
	bool				collected_truncdropped;
	BlockNumber			collected_pages;
	bool				was_usable;
	FasttrunColumnStatsState state;
	RelFileLocatorBackend heap_rlb;
	bool				heap_rlb_valid;
	SubTransactionId	state_subid;
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
	BlockNumber			collected_pages;	/* physical block count at collect;
											 * the only change signal that
											 * survives a temp table's commit */
	bool				was_usable;		/* entry passed the freshness check at
										 * the last analyze/publish; drives the
										 * visible->hidden flip detection */
	FasttrunColumnStatsState state;
	RelFileLocatorBackend heap_rlb;
	bool				heap_rlb_valid;
	SubTransactionId	state_subid;
	FasttrunStatsSavedState *undo;		/* stack of older versions saved
										 * at each subxact-boundary store */
} FasttrunStatsEntry;

typedef struct FasttrunStatsRelidSavedState
{
	FasttrunRelStatsPolicy policy;
	RelFileLocatorBackend heap_rlb;
	bool			heap_rlb_valid;
	SubTransactionId state_subid;
	struct FasttrunStatsRelidSavedState *older;
} FasttrunStatsRelidSavedState;

/*
 * Per-relid backref into the (relid, attnum, inh) stats cache.
 *
 * `attkeys` is a List of palloc'd FasttrunStatsKey.  It lives in
 * fasttrun_stats_mcxt and records every key ever inserted into
 * fasttrun_stats_cache for this relid.  An entry may carry statsTuple =
 * NULL after a mark-evicted -- the key still stays live until the row
 * itself is HASH_REMOVE'd from fasttrun_stats_cache.
 *
 * Used by fasttrun_stats_cache_evict_relid and
 * fasttrun_stats_cache_mark_evicted_relid.  They walk only the columns
 * belonging to a given relid -- O(#cached-cols), small.  No more full
 * hash_seq_search over the entire stats cache.  That sequential scan was
 * O(N) and grew large on long-lived backends that analyzed many temp
 * tables.
 */
typedef struct FasttrunStatsRelidEntry
{
	Oid			relid;		/* hash key -- must be first */
	int			refcount;	/* visible statsTuple entries for this relid */
	List	   *attkeys;	/* List of FasttrunStatsKey *, owns palloc'd keys */
	RelFileLocatorBackend heap_rlb;	/* heap storage identity for the commit-time size probe */
	bool		heap_rlb_valid;
	FasttrunRelStatsPolicy policy;
	SubTransactionId state_subid;
	FasttrunStatsRelidSavedState *undo;
} FasttrunStatsRelidEntry;

typedef struct FasttrunAnalyzeHandoffTarget
{
	Oid			relid;
	Bitmapset  *attnums;		/* NULL means every analyzable user column */
} FasttrunAnalyzeHandoffTarget;

static HTAB			   *fasttrun_stats_cache = NULL;
static HTAB			   *fasttrun_stats_relid_cache = NULL;
static MemoryContext	fasttrun_stats_mcxt = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_attavgwidth_hook_type prev_get_attavgwidth_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static bool				fasttrun_stats_hooks_installed = false;
static bool				fasttrun_planner_hook_installed = false;
static bool				fasttrun_executor_hook_installed = false;

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
	if (!fasttrun_executor_hook_installed)
	{
		prev_ExecutorStart = ExecutorStart_hook;
		ExecutorStart_hook = fasttrun_executor_start;
		fasttrun_executor_hook_installed = true;
	}
	fasttrun_stats_hooks_installed = true;
}

static void
fasttrun_executor_start(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *stmt = queryDesc->plannedstmt;
	ListCell   *lc;

	if (fasttrun_stats_relid_cache != NULL && stmt != NULL &&
		(stmt->commandType != CMD_SELECT || stmt->hasModifyingCTE))
	{
		foreach(lc, stmt->resultRelations)
		{
			int			rtindex = lfirst_int(lc);
			RangeTblEntry *rte = rt_fetch(rtindex, stmt->rtable);

			if (rte->rtekind == RTE_RELATION &&
				fasttrun_stats_relid_exists(rte->relid))
				fasttrun_xact_mark_relid(rte->relid, rte->relid,
									  FASTTRUN_TOUCH_DML);
		}
	}

	if (prev_ExecutorStart != NULL)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
fasttrun_stats_cache_init(void)
{
	HASHCTL		ctl;
	MemoryContext volatile new_mcxt = NULL;
	HTAB	   *new_cache = NULL;
	HTAB	   *new_relid_cache = NULL;

	if (fasttrun_stats_cache != NULL)
		return;

	fasttrun_ensure_planner_hook();
	fasttrun_ensure_stats_hooks();

	PG_TRY();
	{
		new_mcxt = AllocSetContextCreate(TopMemoryContext,
										 "fasttrun stats cache",
										 ALLOCSET_DEFAULT_SIZES);

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(FasttrunStatsKey);
		ctl.entrysize = sizeof(FasttrunStatsEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_cache = hash_create("fasttrun stats cache", 64, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		FASTTRUN_TEST_FAILPOINT("after_stats_cache", 0);

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunStatsRelidEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_relid_cache = hash_create("fasttrun stats relid cache", 16,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	PG_CATCH();
	{
		if (new_mcxt != NULL)
			MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_stats_mcxt = (MemoryContext) new_mcxt;
	fasttrun_stats_cache = new_cache;
	fasttrun_stats_relid_cache = new_relid_cache;
}

/* Drop the stats HTAB + its mcxt (frees all cached tuples at once). */
static void
fasttrun_stats_cache_reset(void)
{
	HTAB	   *old_cache = fasttrun_stats_cache;
	HTAB	   *old_relid_cache = fasttrun_stats_relid_cache;
	MemoryContext old_mcxt = fasttrun_stats_mcxt;

	if (old_cache == NULL && old_relid_cache == NULL && old_mcxt == NULL)
		return;

	fasttrun_stats_cache = NULL;
	fasttrun_stats_relid_cache = NULL;
	fasttrun_stats_mcxt = NULL;

	if (old_mcxt != NULL)
		MemoryContextDelete(old_mcxt);
	else
	{
		if (old_cache != NULL)
			hash_destroy(old_cache);
		if (old_relid_cache != NULL)
			hash_destroy(old_relid_cache);
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
fasttrun_stats_relid_init(FasttrunStatsRelidEntry *entry)
{
	entry->refcount = 0;
	entry->attkeys = NIL;
	memset(&entry->heap_rlb, 0, sizeof(entry->heap_rlb));
	entry->heap_rlb_valid = false;
	entry->policy = FASTTRUN_REL_CORE_ALLOWED;
	entry->state_subid = InvalidSubTransactionId;
	entry->undo = NULL;
}

static FasttrunStatsRelidEntry *
fasttrun_stats_relid_enter(Oid relid, bool *found)
{
	FasttrunStatsRelidEntry *entry;

	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_ENTER, found);
	if (!*found)
		fasttrun_stats_relid_init(entry);
	return entry;
}

static void
fasttrun_stats_relid_free_undo(FasttrunStatsRelidEntry *entry)
{
	while (entry->undo != NULL)
	{
		FasttrunStatsRelidSavedState *popped = entry->undo;

		entry->undo = popped->older;
		pfree(popped);
	}
}

static void
fasttrun_stats_relid_save_undo(FasttrunStatsRelidEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	FasttrunStatsRelidSavedState *saved;
	MemoryContext oldcxt;

	if (entry->state_subid == cur_subid)
		return;

	oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
	saved = (FasttrunStatsRelidSavedState *) palloc(sizeof(*saved));
	MemoryContextSwitchTo(oldcxt);

	saved->policy = entry->policy;
	saved->heap_rlb = entry->heap_rlb;
	saved->heap_rlb_valid = entry->heap_rlb_valid;
	saved->state_subid = entry->state_subid;
	saved->older = entry->undo;
	entry->undo = saved;
	entry->state_subid = cur_subid;
}

static void
fasttrun_stats_relid_maybe_drop(Oid relid,
							FasttrunStatsRelidEntry *entry)
{
	if (entry->attkeys == NIL && entry->refcount <= 0 &&
		entry->policy == FASTTRUN_REL_CORE_ALLOWED &&
		entry->state_subid == InvalidSubTransactionId &&
		entry->undo == NULL)
		(void) hash_search(fasttrun_stats_relid_cache, &relid,
						   HASH_REMOVE, NULL);
}

/*
 * fasttrun_stats_relid_cache invariants.
 *
 *   refcount -- count of LOCAL_CANDIDATE entries with a statsTuple.
 *   attkeys  -- list of every FasttrunStatsKey present in the (relid,
 *               attnum, inh) stats hash for this relid.  Membership does
 *               not depend on whether the statsTuple is currently visible.
 *               Lets evict and mark-evicted iterate just this relid's
 *               columns in O(K) -- no full hash_seq_search over the
 *               stats hash.
 *
 * The relid row also survives without keys while its relation policy is
 * LOCAL_NEUTRAL, or while rollback still owns an undo node.  That row is
 * what prevents a missing explicit key from falling through to stale core
 * pg_statistic.
 */
static void
fasttrun_stats_relid_ref(Oid relid)
{
	FasttrunStatsRelidEntry *entry;
	bool	found;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
												&relid,
												HASH_FIND, NULL);
	if (entry == NULL)
		entry = fasttrun_stats_relid_enter(relid, &found);
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

	if (entry->refcount > 0)
		entry->refcount--;

	/*
	 * Do NOT HASH_REMOVE the row when refcount hits zero.  The attkeys list
	 * may still carry entries with statsTuple == NULL that
	 * fasttrun_stats_cache_evict_* needs to walk.  Removal happens later --
	 * in fasttrun_stats_relid_drop_key, once the last attkey is gone.
	 */
}

/*
 * Add or drop a (relid, attnum, inh) key in the backref list.
 *
 * `add_key` allocates the key in fasttrun_stats_mcxt and appends it.
 * Called the first time we insert into fasttrun_stats_cache for this
 * triple.
 *
 * `drop_key` finds the matching FasttrunStatsKey, removes it, and pfrees
 * it.  When the list goes empty AND refcount is zero, the relid row
 * itself is also removed.  Called whenever we HASH_REMOVE from
 * fasttrun_stats_cache.
 */
static void
fasttrun_stats_relid_add_key(const FasttrunStatsKey *key)
{
	FasttrunStatsRelidEntry *entry;
	bool	found;
	MemoryContext oldcxt;
	FasttrunStatsKey *kcopy;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = fasttrun_stats_relid_enter(key->relid, &found);

	oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
	kcopy = (FasttrunStatsKey *) palloc(sizeof(*kcopy));
	*kcopy = *key;
	entry->attkeys = lappend(entry->attkeys, kcopy);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Remember the heap's storage identity so the commit-time freshness walk
 * can read the physical size via smgr without opening the relation.
 */
static void
fasttrun_stats_relid_remember_locator(Relation heaprel)
{
	FasttrunStatsRelidEntry *entry;
	Oid			relid = RelationGetRelid(heaprel);

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													&relid, HASH_FIND, NULL);
	if (entry == NULL)
		return;

	entry->heap_rlb.locator = heaprel->rd_locator;
	entry->heap_rlb.backend = heaprel->rd_backend;
	entry->heap_rlb_valid = true;
}

static void
fasttrun_stats_relid_unlink_key(FasttrunStatsRelidEntry *entry,
								 const FasttrunStatsKey *key)
{
	ListCell   *lc;

	foreach(lc, entry->attkeys)
	{
		FasttrunStatsKey *k = (FasttrunStatsKey *) lfirst(lc);

		if (k->relid == key->relid &&
			k->attnum == key->attnum &&
			k->inh == key->inh)
		{
			entry->attkeys = foreach_delete_current(entry->attkeys, lc);
			pfree(k);
			break;
		}
	}
}

static void
fasttrun_stats_relid_drop_key(const FasttrunStatsKey *key)
{
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	entry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													&key->relid,
													HASH_FIND, NULL);
	if (entry == NULL)
		return;

	fasttrun_stats_relid_unlink_key(entry, key);
	fasttrun_stats_relid_maybe_drop(key->relid, entry);
}

static bool
fasttrun_rlb_equals(RelFileLocatorBackend left,
					RelFileLocatorBackend right)
{
	return left.backend == right.backend &&
		RelFileLocatorEquals(left.locator, right.locator);
}

static bool
fasttrun_stats_set_relation_policy(Relation rel,
								FasttrunRelStatsPolicy policy)
{
	Oid			relid = RelationGetRelid(rel);
	FasttrunStatsRelidEntry *entry;
	RelFileLocatorBackend rlb;
	bool		found;
	bool		changed;
	SubTransactionId cur_subid = GetCurrentSubTransactionId();

	fasttrun_stats_cache_init();
	rlb.locator = rel->rd_locator;
	rlb.backend = rel->rd_backend;
	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (entry != NULL && entry->heap_rlb_valid &&
		!fasttrun_rlb_equals(entry->heap_rlb, rlb) &&
		entry->state_subid != cur_subid)
		fasttrun_stats_forget_relid(relid);
	entry = fasttrun_stats_relid_enter(relid, &found);
	changed = !found || entry->policy != policy ||
		!entry->heap_rlb_valid || !fasttrun_rlb_equals(entry->heap_rlb, rlb);

	if (found)
		fasttrun_stats_relid_save_undo(entry);
	else
		entry->state_subid = cur_subid;

	entry->policy = policy;
	entry->heap_rlb = rlb;
	entry->heap_rlb_valid = true;
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	return changed;
}

/* Catalog probe is allowed only from explicit mutation paths, never hooks. */
static bool
fasttrun_relation_has_core_stats(Relation rel)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Oid			relid = RelationGetRelid(rel);
	int			i;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		HeapTuple	tuple;

		if (attr->attisdropped)
			continue;
		tuple = SearchSysCache3(STATRELATTINH,
								ObjectIdGetDatum(relid),
								Int16GetDatum(i + 1),
								BoolGetDatum(false));
		if (HeapTupleIsValid(tuple))
		{
			ReleaseSysCache(tuple);
			return true;
		}
	}
	return false;
}

/* freefunc for VariableStatData -- we own the tuple, no-op. */
static void
fasttrun_stats_noop_free(HeapTuple tuple)
{
}

/*
 * Lower bound for the cardinality-scaled freshness tolerance (see
 * fasttrun_stats_entry_usable).  A near-unique column gets a tolerance this
 * tight, since a DML that concentrates rows onto one value can blow that
 * value's "rare" estimate past a plan boundary (Index Scan instead of Seq
 * Scan).  Low-cardinality columns scale back up toward stats_refresh_threshold
 * -- a stale estimate there still reads as "many rows" and the plan holds.
 */
#define FASTTRUN_SKEW_REFRESH_FLOOR	0.05

/*
 * Cross-commit physical-size backstop factor (see fasttrun_stats_entry_usable).
 * A temp table's pgstat counters reset at every transaction boundary and never
 * reach shared stats, so a committed plain-SQL refill done without a fasttrun_*
 * call is invisible to the counter-based freshness check.  Physical page count
 * survives the commit but is bloat-contaminated, so only an order-of-size
 * change is trusted as a real refill.
 */
#define FASTTRUN_STALE_SIZE_FACTOR	3.0

/*
 * Soft freshness: is this column-stats entry close enough to keep serving?
 * True while the DML churn since collect time stays below the effective
 * tolerance -- stats_refresh_threshold scaled down by the column's cardinality
 * (see FASTTRUN_SKEW_REFRESH_FLOOR).  Within tolerance the cached distribution
 * still beats planner defaults (core PG keeps using pg_statistic between
 * ANALYZE runs the same way).  A truncdropped flip means the storage was
 * emptied or rewritten; a missing analyze-cache row-count baseline means we
 * cannot bound the drift.  Either makes the entry unusable.  Caller guarantees
 * entry->statsTuple != NULL.
 */
static bool
fasttrun_stats_entry_usable(Oid relid, const FasttrunStatsEntry *entry,
							int64 ins_now, int64 upd_now, int64 del_now,
							bool truncdropped_now, BlockNumber pages_now)
{
	FasttrunAnalyzeCacheEntry *aentry;
	int64		churn;
	double		baseline;
	double		threshold;
	double		floor_threshold;
	double		stadistinct;
	double		dratio;

	if (truncdropped_now != entry->collected_truncdropped)
		return false;

	if (ins_now == entry->collected_ins &&
		upd_now == entry->collected_upd &&
		del_now == entry->collected_del)
	{
		/*
		 * Counters say nothing changed since collect.  For a temp table that
		 * is not enough: pgstat counters reset at every transaction boundary
		 * and temp stats never reach shared pgstat, so a plain-SQL
		 * refill/TRUNCATE done by a prior committed transaction (without a
		 * fasttrun_* call to re-evaluate the entry) leaves both sides at zero
		 * and this branch would serve a distribution the table no longer has.
		 * The physical page count DOES survive commit, so anchor freshness to
		 * it: if storage drifted past the refresh threshold since collect, the
		 * cached MCV/histogram/n_distinct are untrustworthy -- hide them and
		 * let the planner fall back to defaults (as core does with no
		 * pg_statistic row).  Within a transaction the counter path below
		 * governs soft freshness; this check only guards the "counters see
		 * nothing" blind spot.
		 */
		if (pages_now != entry->collected_pages)
		{
			BlockNumber	base_pages = Max(entry->collected_pages, 1);
			double		ratio = (double) pages_now / (double) base_pages;

			/*
			 * Coarse cross-commit backstop.  Physical size is the only change
			 * signal that survives a temp table's commit, but it is
			 * bloat-contaminated (a bulk UPDATE or a rolled-back subxact
			 * roughly doubles pages without changing the data), so it cannot
			 * carry the fine stats_refresh_threshold semantics without
			 * defeating soft freshness and savepoint restore.  Only an
			 * order-of-size change -- the footprint tripled or dropped below a
			 * third since collect -- is taken as "the table was refilled while
			 * the per-xact counters were blind", hiding the now-meaningless
			 * sample so the planner falls back to defaults.
			 */
			if (ratio >= FASTTRUN_STALE_SIZE_FACTOR ||
				ratio <= 1.0 / FASTTRUN_STALE_SIZE_FACTOR)
				return false;
		}
		return true;			/* no DML and size stable -- exactly fresh */
	}

	aentry = fasttrun_cache_lookup(relid);
	if (aentry == NULL || !aentry->state.has_relstats)
		return false;

	churn = (ins_now - entry->collected_ins)
		+ (upd_now - entry->collected_upd)
		+ (del_now - entry->collected_del);
	if (churn < 0)
		churn = -churn;
	baseline = (double) Max(aentry->state.cached_tuples, 1);

	/* stadistinct < 0 is a negative fraction of rows; > 0 is an absolute count. */
	stadistinct = ((Form_pg_statistic) GETSTRUCT(entry->statsTuple))->stadistinct;
	dratio = (stadistinct < 0.0) ? -stadistinct : stadistinct / baseline;

	/*
	 * Scale tolerance down with cardinality: a near-unique column (dratio -> 1)
	 * gets FASTTRUN_SKEW_REFRESH_FLOOR, a low-cardinality column keeps the
	 * configured threshold, and everything between scales linearly -- no cliff,
	 * moderate-cardinality columns are protected too.  The floor never loosens
	 * a stricter user setting.
	 */
	floor_threshold = Min(fasttrun_stats_refresh_threshold,
						  FASTTRUN_SKEW_REFRESH_FLOOR);
	threshold = Max(fasttrun_stats_refresh_threshold * (1.0 - dratio),
					floor_threshold);

	return ((double) churn / baseline) < threshold;
}

static bool
fasttrun_stats_relid_locator_valid(Oid relid,
								   FasttrunStatsRelidEntry *relentry)
{
	Relation	rel;
	RelFileLocatorBackend rlb;
	bool		valid;

	if (!relentry->heap_rlb_valid)
		return false;
	rel = RelationIdGetRelation(relid);
	if (!RelationIsValid(rel))
	{
		fasttrun_stats_forget_relid(relid);
		return false;
	}
	rlb.locator = rel->rd_locator;
	rlb.backend = rel->rd_backend;
	valid = fasttrun_rlb_equals(relentry->heap_rlb, rlb);
	RelationClose(rel);
	if (!valid)
		fasttrun_stats_forget_relid(relid);
	return valid;
}

/*
 * Substitute our cached tuple while it stays within churn tolerance (see
 * fasttrun_stats_entry_usable); past the threshold we fall through so the
 * planner uses defaults instead of a distribution that has shifted too far.
 */
static bool
fasttrun_get_relation_stats_hook(PlannerInfo *root, RangeTblEntry *rte,
								 AttrNumber attnum, VariableStatData *vardata)
{
	FasttrunStatsKey key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;
	BlockNumber		pages_now = 0;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		goto chain;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &rte->relid, HASH_FIND, NULL);
	if (relentry == NULL ||
		!fasttrun_stats_relid_locator_valid(rte->relid, relentry))
		goto chain;

	fasttrun_stats_key_init(&key, rte->relid, attnum, rte->inh);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry != NULL && entry->state == FASTTRUN_COLUMN_CORE_ALLOWED)
		goto chain;
	if (entry == NULL)
	{
		if (relentry->policy == FASTTRUN_REL_LOCAL_NEUTRAL)
			return true;
		goto chain;
	}
	if (entry->state == FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
		entry->statsTuple == NULL)
		return true;

	/* Freshness check: tolerate DML churn up to stats_refresh_threshold. */
	if (!fasttrun_read_pgstat_counters_for_hook(rte->relid, &ins_now, &upd_now,
												&del_now, &truncdropped_now,
												&pages_now))
		return true;

	if (!fasttrun_stats_entry_usable(rte->relid, entry, ins_now, upd_now,
									 del_now, truncdropped_now, pages_now))
		return true;		/* handled: deliberately hide core stats too */

	vardata->statsTuple = entry->statsTuple;
	vardata->freefunc = fasttrun_stats_noop_free;
	vardata->acl_ok = true;
	return true;

chain:
	if (prev_get_relation_stats_hook)
		return prev_get_relation_stats_hook(root, rte, attnum, vardata);
	return false;
}

static int32
fasttrun_type_default_width(Oid relid, AttrNumber attnum)
{
	HeapTuple	atttuple;
	Form_pg_attribute attr;
	int32		width;

	atttuple = SearchSysCache2(ATTNUM,
							 ObjectIdGetDatum(relid),
							 Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		return 1;
	attr = (Form_pg_attribute) GETSTRUCT(atttuple);
	width = get_typavgwidth(attr->atttypid, attr->atttypmod);
	ReleaseSysCache(atttuple);
	return Max(width, 1);
}

/*
 * Companion to fasttrun_get_relation_stats_hook for the planner's
 * separate average-width path (lsyscache.c:get_attavgwidth).
 *
 * Without this hook the planner reads stawidth straight from the
 * pg_statistic syscache, which we never write to -- so for temp tables
 * it falls back to get_typavgwidth() and uses the type-default width.
 * For varlena types like text that default is 32 bytes regardless of
 * the real sample, which inflates hash-table / sort / spool costing
 * on temp tables with short text columns.
 *
 * Same ownership contract as the relation-stats hook.  CORE_ALLOWED chains;
 * neutral, stale, and zero-width local states return the type default here,
 * because returning 0 would make lsyscache read stale pg_statistic.stawidth.
 */
static int32
fasttrun_get_attavgwidth_hook(Oid relid, AttrNumber attnum)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry;
	int64				ins_now = 0;
	int64				upd_now = 0;
	int64				del_now = 0;
	bool				truncdropped_now = false;
	BlockNumber			pages_now = 0;
	int32				stawidth;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		goto chain;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL ||
		!fasttrun_stats_relid_locator_valid(relid, relentry))
		goto chain;

	/* matches what fasttrun_stats_cache_store writes */
	fasttrun_stats_key_init(&key, relid, attnum, false);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry != NULL && entry->state == FASTTRUN_COLUMN_CORE_ALLOWED)
		goto chain;
	if (entry == NULL)
	{
		if (relentry->policy == FASTTRUN_REL_LOCAL_NEUTRAL)
			return fasttrun_type_default_width(relid, attnum);
		goto chain;
	}
	if (entry->state == FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
		entry->statsTuple == NULL)
		return fasttrun_type_default_width(relid, attnum);

	if (!fasttrun_read_pgstat_counters_for_hook(relid, &ins_now, &upd_now,
												&del_now, &truncdropped_now,
												&pages_now))
		return fasttrun_type_default_width(relid, attnum);

	if (!fasttrun_stats_entry_usable(relid, entry, ins_now, upd_now,
									 del_now, truncdropped_now, pages_now))
		return fasttrun_type_default_width(relid, attnum);

	stawidth = ((Form_pg_statistic) GETSTRUCT(entry->statsTuple))->stawidth;
	if (stawidth > 0)
		return stawidth;
	return fasttrun_type_default_width(relid, attnum);

chain:
	if (prev_get_attavgwidth_hook)
		return (*prev_get_attavgwidth_hook) (relid, attnum);
	return 0;
}

/*
 * planner hook -- re-inject cached relpages/reltuples into rd_rel for any
 * temp relation whose rd_rel was reset to the on-disk pg_class values
 * by a relcache rebuild (typically triggered by a sinval message from a
 * concurrent backend).
 *
 * fasttrun_analyze() writes (relpages, reltuples) only into rd_rel --
 * never into pg_class -- to keep the operation sinval-free.  But rd_rel
 * is owned by the relcache, and any RelationCacheInvalidateEntry resets
 * it from pg_class on the next RelationIdGetRelation.  Without this
 * hook, that wipes our analyze results: planner sees relpages=0 and
 * picks catastrophic plans (Seq Scan over what looks like a tiny table,
 * nested loop joins on big working sets).
 *
 * We walk the current query's rangetable (recursing into subquery RTEs,
 * CTEs and, when hasSubLinks is set, SubLink subselects) and re-inject
 * cached stats for those temp heaps plus their indexes.  This keeps
 * unrelated cached temp tables at zero cost for plans that don't
 * reference them.
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

	/*
	 * Subqueries inside expressions (IN/EXISTS/scalar SubLinks) carry their
	 * Query outside rtable and cteList.  hasSubLinks gates the extra
	 * expression walk, so sublink-free queries pay nothing here.
	 */
	if (query->hasSubLinks &&
		query_tree_walker(query, fasttrun_contains_stats_sublink_walker, NULL,
						  QTW_IGNORE_RT_SUBQUERIES | QTW_IGNORE_CTE_SUBQUERIES))
		return true;

	return false;
}

/* Descend into SubLink subselects looking for cached-stats relids. */
static bool
fasttrun_contains_stats_sublink_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
		return fasttrun_query_contains_stats_relid((Query *) node);
	return expression_tree_walker(node, fasttrun_contains_stats_sublink_walker,
								  context);
}

/*
 * Re-inject cached relstats for every temp relation a query references,
 * descending into subqueries and CTEs.  Mirrors the recursion of
 * fasttrun_query_contains_stats_relid, so a temp table nested below the top
 * level gets its rd_rel repaired just like a top-level one -- otherwise the
 * planner would read relpages/reltuples 0 from pg_class for the nested temp.
 *
 * With detect_stats the same single walk also reports whether any visited
 * relation has cached column stats (the answer fasttrun_query_contains_
 * stats_relid gives), so the planner hook pays one tree walk instead of
 * two.  Probing stops after the first hit; the reinject part still visits
 * everything.
 */
typedef struct FasttrunReinjectWalkContext
{
	bool		detect_stats;
	bool		found;
} FasttrunReinjectWalkContext;

static bool
fasttrun_reinject_query_relstats(Query *query, bool detect_stats)
{
	ListCell   *lc;
	bool		found = false;

	if (query == NULL)
		return false;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			fasttrun_reinject_rte_relstats(rte);
			if (detect_stats && !found &&
				fasttrun_stats_relid_exists(rte->relid))
				found = true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			if (fasttrun_reinject_query_relstats(rte->subquery, detect_stats))
				found = true;
		}
	}

	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query) &&
			fasttrun_reinject_query_relstats((Query *) cte->ctequery,
											 detect_stats))
			found = true;
	}

	/* Same SubLink descent as fasttrun_query_contains_stats_relid. */
	if (query->hasSubLinks)
	{
		FasttrunReinjectWalkContext ctx;

		ctx.detect_stats = detect_stats;
		ctx.found = false;
		(void) query_tree_walker(query, fasttrun_reinject_sublink_walker, &ctx,
								 QTW_IGNORE_RT_SUBQUERIES |
								 QTW_IGNORE_CTE_SUBQUERIES);
		if (ctx.found)
			found = true;
	}

	return found;
}

/* Descend into SubLink subselects repairing rd_rel for cached temp rels. */
static bool
fasttrun_reinject_sublink_walker(Node *node, void *context)
{
	FasttrunReinjectWalkContext *ctx = (FasttrunReinjectWalkContext *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		if (fasttrun_reinject_query_relstats((Query *) node,
											 ctx->detect_stats))
			ctx->found = true;
		return false;
	}
	return expression_tree_walker(node, fasttrun_reinject_sublink_walker,
								  context);
}

static void fasttrun_poison_check_query(Query *query);

static bool
fasttrun_poison_sublink_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		fasttrun_poison_check_query((Query *) node);
		return false;
	}
	return expression_tree_walker(node, fasttrun_poison_sublink_walker,
								  context);
}

/* Walk only while poison exists; hash misses do not open relations. */
static void
fasttrun_poison_check_query(Query *query)
{
	ListCell   *lc;

	if (query == NULL || fasttrun_poison_cache == NULL)
		return;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid))
		{
			FasttrunPoisonEntry *entry = fasttrun_poison_find(rte->relid);

			if (entry != NULL && entry->active)
			{
				Relation	rel = RelationIdGetRelation(rte->relid);

				if (RelationIsValid(rel))
				{
					fasttrun_poison_check_relation(rel, "query planning", false);
					RelationClose(rel);
				}
			}
		}
		else if (rte->rtekind == RTE_SUBQUERY)
			fasttrun_poison_check_query(rte->subquery);
	}
	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query))
			fasttrun_poison_check_query((Query *) cte->ctequery);
	}
	if (query->hasSubLinks)
		(void) query_tree_walker(query, fasttrun_poison_sublink_walker, NULL,
								 QTW_IGNORE_RT_SUBQUERIES |
								 QTW_IGNORE_CTE_SUBQUERIES);
}

static PlannedStmt *
fasttrun_planner_hook(Query *parse, const char *query_string,
					  int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool		saved_in_planner = fasttrun_in_planner;
	int			saved_freshness_cache_used = fasttrun_freshness_cache_used;
	int			saved_freshness_cache_next_evict = fasttrun_freshness_cache_next_evict;
	bool		stats_frame_needed = false;

	if (fasttrun_poison_cache != NULL)
		fasttrun_poison_check_query(parse);

	if (fasttrun_analyze_cache != NULL)
	{
		bool		detect = (fasttrun_stats_cache != NULL &&
							  fasttrun_stats_relid_cache != NULL);

		/* One walk: reinject relstats and detect cached column stats. */
		stats_frame_needed = fasttrun_reinject_query_relstats(parse, detect);
	}
	else if (fasttrun_stats_cache != NULL)
	{
		/* Nothing to reinject -- detect-only walk with early exit. */
		stats_frame_needed = fasttrun_query_contains_stats_relid(parse);
	}

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
	fasttrun_freshness_cache_next_evict = 0;

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
		fasttrun_freshness_cache_next_evict = saved_freshness_cache_next_evict;
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_in_planner = saved_in_planner;
	fasttrun_freshness_cache_used = saved_freshness_cache_used;
	fasttrun_freshness_cache_next_evict = saved_freshness_cache_next_evict;

	return result;
}

/*
 * Sum tuples_inserted/_updated/_deleted and OR truncdropped over the
 * full active subxact stack.  Pass upd=NULL on hot paths that don't
 * care about UPDATE (rolling-delta math); the column-stats freshness
 * check passes a real pointer because UPDATE changes distribution
 * even though it doesn't change live row count.  Returns false if
 * pgstat is off -> caller falls back to always-scan.
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

/*
 * Commit-time variant of the counter read.  XACT_EVENT_COMMIT runs at
 * TRANS_COMMIT, where relcache/syscache access is forbidden
 * (RelationIdGetRelation asserts IsTransactionState()), so read the same
 * per-xact sums straight from the pending pgstat entry.  It is still
 * alive here -- AtEOXact_PgStat runs after the xact callbacks.  Temp
 * relations are database-local, so MyDatabaseId is the only dboid to try.
 */
static bool
fasttrun_read_pgstat_counters_at_commit(Oid relid,
										int64 *ins, int64 *upd, int64 *del,
										bool *truncdropped)
{
	PgStat_EntryRef *ref;
	PgStat_TableStatus *tabstat;
	PgStat_TableXactStatus *trans;

	*ins = 0;
	*upd = 0;
	*del = 0;
	*truncdropped = false;

	if (!pgstat_track_counts)
		return false;

	ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId, relid);
	if (ref == NULL || ref->pending == NULL)
		return false;

	tabstat = (PgStat_TableStatus *) ref->pending;
	for (trans = tabstat->trans; trans != NULL; trans = trans->upper)
	{
		*ins += trans->tuples_inserted;
		*upd += trans->tuples_updated;
		*del += trans->tuples_deleted;
		if (trans->truncdropped)
			*truncdropped = true;
	}

	return true;
}

/*
 * Commit-time physical size probe.  Same smgr-only discipline as the
 * subxact-abort probe: no relcache in sight.
 */
static BlockNumber
fasttrun_stats_relid_nblocks_at_commit(const FasttrunStatsRelidEntry *relentry)
{
	SMgrRelation reln;

	if (!relentry->heap_rlb_valid)
		return 0;

	reln = smgropen(relentry->heap_rlb.locator, relentry->heap_rlb.backend);
	if (!smgrexists(reln, MAIN_FORKNUM))
		return 0;

	return smgrnblocks(reln, MAIN_FORKNUM);
}

static bool
fasttrun_read_pgstat_counters_for_hook(Oid relid,
									   int64 *ins, int64 *upd, int64 *del,
									   bool *truncdropped, BlockNumber *pages)
{
	int		i;
	Relation rel;
	bool	have;

	*pages = 0;

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
			*pages = slot->pages;
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
		/*
		 * Physical size is the only change signal that survives a temp
		 * table's commit (pgstat counters reset per xact; temp stats never
		 * reach shared pgstat).  Read it in the same relation open the
		 * freshness path already pays for.
		 */
		if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
			*pages = RelationGetNumberOfBlocks(rel);
		RelationClose(rel);
	}

	if (fasttrun_in_planner)
	{
		FasttrunFreshnessCacheEntry *slot;

		if (fasttrun_freshness_cache_used < FASTTRUN_FRESHNESS_CACHE_SLOTS)
		{
			slot = &fasttrun_freshness_cache[fasttrun_freshness_cache_used++];
		}
		else
		{
			/*
			 * Cache full -- replace round-robin.  FIFO is good enough for
			 * the planner-scoped lifetime; the only way to keep filling here
			 * is plans that genuinely touch more than
			 * FASTTRUN_FRESHNESS_CACHE_SLOTS different temp relids, where the
			 * working set itself does not fit anyway.
			 */
			slot = &fasttrun_freshness_cache[fasttrun_freshness_cache_next_evict];
			fasttrun_freshness_cache_next_evict =
				(fasttrun_freshness_cache_next_evict + 1) % FASTTRUN_FRESHNESS_CACHE_SLOTS;
		}

		slot->relid = relid;
		slot->have = have;
		slot->ins = *ins;
		slot->upd = (upd != NULL) ? *upd : 0;
		slot->del = *del;
		slot->truncdropped = *truncdropped;
		slot->pages = *pages;
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
							int64 totalrows, double stanullfrac)
{
	int		f1;
	double	n;
	double	N;
	double	estimate;

	if (n_nonnull <= 0)
		return 0.0f;
	if (nmultiple == 0)
	{
		/* No repeats -- assume unique, discounted for nulls (as core). */
		return (float4) (-1.0 * (1.0 - stanullfrac));
	}
	if (ndistinct == nmultiple)
	{
		/* Bounded set: the sample says the column has just these values. */
		estimate = (double) ndistinct;
	}
	else
	{
		/*
		 * Haas-Stokes Duj1, exactly as core compute_scalar_stats():
		 * n*d / (n - f1 + f1*n/N), nulls excluded from both n and N.
		 */
		f1 = ndistinct - nmultiple;
		n = (double) n_nonnull;
		N = (double) totalrows * (1.0 - stanullfrac);

		if (N > 0)
			estimate = (n * (double) ndistinct) /
				((n - (double) f1) + (double) f1 * n / N);
		else
			estimate = 0;

		if (estimate < (double) ndistinct)
			estimate = (double) ndistinct;
		if (estimate > N)
			estimate = N;
	}
	estimate = floor(estimate + 0.5);

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
static bool
fasttrun_stats_entry_is_candidate(const FasttrunStatsEntry *entry)
{
	return entry->state == FASTTRUN_COLUMN_LOCAL_CANDIDATE &&
		entry->statsTuple != NULL;
}

static void
fasttrun_stats_entry_save_undo(FasttrunStatsEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	FasttrunStatsSavedState *saved;
	MemoryContext oldcxt;

	if (entry->state_subid == cur_subid)
		return;

	oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
	saved = (FasttrunStatsSavedState *) palloc(sizeof(*saved));
	MemoryContextSwitchTo(oldcxt);

	saved->statsTuple = entry->statsTuple;	/* transfer ownership */
	saved->collected_ins = entry->collected_ins;
	saved->collected_upd = entry->collected_upd;
	saved->collected_del = entry->collected_del;
	saved->collected_truncdropped = entry->collected_truncdropped;
	saved->collected_pages = entry->collected_pages;
	saved->was_usable = entry->was_usable;
	saved->state = entry->state;
	saved->heap_rlb = entry->heap_rlb;
	saved->heap_rlb_valid = entry->heap_rlb_valid;
	saved->state_subid = entry->state_subid;
	saved->older = entry->undo;
	entry->undo = saved;
	entry->statsTuple = NULL;
	entry->state_subid = cur_subid;
}

static bool
fasttrun_stats_set_column_state(Relation rel, AttrNumber attnum, bool inh,
								FasttrunColumnStatsState state,
								HeapTuple tuple,
								const FasttrunPgstatSnapshot *snapshot)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	Oid				relid = RelationGetRelid(rel);
	bool				found;
	bool				old_candidate;
	bool				new_candidate;
	bool				changed;
	MemoryContext		oldcxt;
	SubTransactionId	cur_subid = GetCurrentSubTransactionId();

	fasttrun_stats_cache_init();
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);

	fasttrun_stats_key_init(&key, relid, attnum, inh);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->statsTuple = NULL;
		entry->collected_ins = 0;
		entry->collected_upd = 0;
		entry->collected_del = 0;
		entry->collected_truncdropped = false;
		entry->collected_pages = 0;
		entry->was_usable = false;
		entry->state = FASTTRUN_COLUMN_CORE_ALLOWED;
		memset(&entry->heap_rlb, 0, sizeof(entry->heap_rlb));
		entry->heap_rlb_valid = false;
		entry->state_subid = cur_subid;
		entry->undo = NULL;
		fasttrun_stats_relid_add_key(&key);
	}

	old_candidate = fasttrun_stats_entry_is_candidate(entry);
	changed = !found || entry->state != state ||
		old_candidate != (state == FASTTRUN_COLUMN_LOCAL_CANDIDATE &&
						 tuple != NULL);
	if (found && entry->state_subid != cur_subid)
		fasttrun_stats_entry_save_undo(entry);
	else if (found && entry->statsTuple != NULL)
	{
		heap_freetuple(entry->statsTuple);
		entry->statsTuple = NULL;
	}

	new_candidate = (state == FASTTRUN_COLUMN_LOCAL_CANDIDATE &&
					 tuple != NULL);
	if (old_candidate && !new_candidate)
		fasttrun_stats_relid_unref(relid);
	else if (!old_candidate && new_candidate)
		fasttrun_stats_relid_ref(relid);

	if (new_candidate)
	{
		oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
		entry->statsTuple = heap_copytuple(tuple);
		MemoryContextSwitchTo(oldcxt);
	}
	entry->state = state;
	entry->heap_rlb.locator = rel->rd_locator;
	entry->heap_rlb.backend = rel->rd_backend;
	entry->heap_rlb_valid = true;
	entry->state_subid = cur_subid;
	entry->was_usable = new_candidate;
	if (snapshot != NULL)
	{
		entry->collected_ins = snapshot->inserted;
		entry->collected_upd = snapshot->updated;
		entry->collected_del = snapshot->deleted;
		entry->collected_truncdropped = snapshot->truncdropped;
		entry->collected_pages = snapshot->pages;
	}
	else
	{
		entry->collected_ins = 0;
		entry->collected_upd = 0;
		entry->collected_del = 0;
		entry->collected_truncdropped = false;
		entry->collected_pages = 0;
	}
	return changed;
}

static void
fasttrun_stats_cache_store(Relation rel, AttrNumber attnum,
						   HeapTuple statsTuple, int64 ins, int64 upd,
						   int64 del, bool truncdropped, BlockNumber pages)
{
	FasttrunPgstatSnapshot snapshot;

	snapshot.inserted = ins;
	snapshot.updated = upd;
	snapshot.deleted = del;
	snapshot.truncdropped = truncdropped;
	snapshot.pages = pages;
	(void) fasttrun_stats_set_column_state(rel, attnum, false,
										FASTTRUN_COLUMN_LOCAL_CANDIDATE,
										statsTuple, &snapshot);
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
fasttrun_stats_forget_relid(Oid relid)
{
	FasttrunStatsRelidEntry *relentry;
	ListCell   *lc;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL)
		return;

	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(lc);
		FasttrunStatsEntry *entry;

		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);
		if (entry == NULL)
			continue;
		fasttrun_stats_entry_free_undo(entry);
		if (entry->statsTuple != NULL)
			heap_freetuple(entry->statsTuple);
		(void) hash_search(fasttrun_stats_cache, key, HASH_REMOVE, NULL);
	}
	list_free_deep(relentry->attkeys);
	fasttrun_stats_relid_free_undo(relentry);
	(void) hash_search(fasttrun_stats_relid_cache, &relid, HASH_REMOVE, NULL);
}

static bool
fasttrun_stats_entry_neutralize(Oid relid, FasttrunStatsEntry *entry)
{
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	bool		was_candidate = fasttrun_stats_entry_is_candidate(entry);
	bool		changed = entry->state != FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
		entry->statsTuple != NULL;

	if (!changed)
		return false;
	if (entry->state_subid != cur_subid)
		fasttrun_stats_entry_save_undo(entry);
	else if (entry->statsTuple != NULL)
	{
		heap_freetuple(entry->statsTuple);
		entry->statsTuple = NULL;
	}
	if (was_candidate)
		fasttrun_stats_relid_unref(relid);
	entry->state = FASTTRUN_COLUMN_LOCAL_NEUTRAL;
	entry->state_subid = cur_subid;
	entry->was_usable = false;
	entry->collected_ins = 0;
	entry->collected_upd = 0;
	entry->collected_del = 0;
	entry->collected_truncdropped = false;
	entry->collected_pages = 0;
	return true;
}

/*
 * COMMIT-time bookkeeping for the per-(relid, attnum) column-stats cache.
 *
 * Same pathology as fasttrun_cache_commit_xact() suffered before the
 * 2.2.0 regression.  The old code walked the full stats cache on every
 * COMMIT.  It did RelationIdGetRelation plus fasttrun_read_pgstat_counters
 * per entry.  Then it fired fasttrun_invalidate_local_plan_cache()
 * whenever the pgstat snapshot drifted -- the common case for ON COMMIT
 * DELETE ROWS temp tables re-analyzed every xact.  Result: all cached
 * plans walked once per stale entry, N x M work on every COMMIT.
 *
 * The new code:
 *   - walks the per-xact touched-relid list, then the attkeys backref
 *     for each one -- O(touched * cached-cols-per-rel), not
 *     O(total-stats-entries);
 *   - reads pgstat counters once per relid, not once per attkey
 *     (a 20-column temp table used to cost 20 reads here);
 *   - on staleness, turns the entry LOCAL_NEUTRAL and emits no shared
 *     invalidation.  Core pg_statistic stays hidden until an explicit
 *     handoff.  When DML makes at least one previously visible column
 *     neutral, the owning relation gets one backend-local plan invalidation
 *     after all its columns have been processed.
 *
 * Net effect: column stats stay "fresh-or-hidden" through the planner
 * hook.  COMMIT walks the plan cache at most once per touched owning
 * relation, and only on an actual visible-to-neutral transition while
 * storage remains nonempty.  ON COMMIT DELETE ROWS stays at zero walks.
 */
/* Same reasoning as for fasttrun_cache_commit_xact -- keep the uprobe target. */
static pg_noinline void
fasttrun_stats_cache_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_stats_cache == NULL ||
		fasttrun_stats_relid_cache == NULL ||
		fasttrun_xact_frame == NULL)
		return;

	hash_seq_init(&status, fasttrun_xact_frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		Oid			relid = xentry->relid;
		FasttrunStatsRelidEntry *relentry;
		List	   *keys;
		ListCell   *klc;
		int64		ins_now = 0;
		int64		upd_now = 0;
		int64		del_now = 0;
		bool		truncdropped_now = false;
		BlockNumber	pages_now = 0;
		bool		have_counters = false;
		bool		counters_attempted = false;
		bool		plan_inval_needed = false;

		if ((xentry->flags & (FASTTRUN_TOUCH_DML |
							 FASTTRUN_TOUCH_STATS |
							 FASTTRUN_TOUCH_DROPPED)) == 0)
			continue;

		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
		if (relentry == NULL)
			continue;

		/*
		 * Snapshot the attkey list.  Drop paths below call
		 * fasttrun_stats_relid_drop_key which mutates relentry->attkeys
		 * mid-iteration -- iterating the live list would invalidate our
		 * ListCells.
		 */
		keys = list_copy(relentry->attkeys);

		/*
		 * Relation dropped this xact (ON COMMIT DROP fires before this
		 * callback, plus any plain DROP earlier in the xact)?  Drop the
		 * column-stats entries.  The OAT_DROP note replaces the syscache
		 * probe: TRANS_COMMIT forbids catalog access.  Symmetric to the
		 * analyze-cache commit path.
		 */
		if (fasttrun_xact_entry_dropped(xentry))
		{
			foreach(klc, keys)
			{
				FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(klc);
				FasttrunStatsKey key = *kptr;
				FasttrunStatsEntry *entry;

				entry = (FasttrunStatsEntry *)
					hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
				if (entry == NULL)
					continue;
				fasttrun_stats_entry_free_undo(entry);
				if (entry->statsTuple != NULL)
					heap_freetuple(entry->statsTuple);
				(void) hash_search(fasttrun_stats_cache, &key, HASH_REMOVE, NULL);
			}
			list_free(keys);
			list_free_deep(relentry->attkeys);
			fasttrun_stats_relid_free_undo(relentry);
			(void) hash_search(fasttrun_stats_relid_cache, &relid,
							   HASH_REMOVE, NULL);
			continue;
		}

		foreach(klc, keys)
		{
			FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(klc);
			FasttrunStatsKey key = *kptr;	/* value copy -- drop_key may pfree source */
			FasttrunStatsEntry *entry;

			entry = (FasttrunStatsEntry *)
				hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
			if (entry == NULL)
			{
				/* Stale backref -- unlink it and defer relid pruning. */
				fasttrun_stats_relid_unlink_key(relentry, &key);
				continue;
			}

			if (!fasttrun_stats_entry_is_candidate(entry))
			{
				/*
				 * A full successful handoff leaves CORE_ALLOWED overrides
				 * redundant under a CORE_ALLOWED relation default.  Keep them
				 * through subcommit for rollback, then prune only here when the
				 * top-level outcome is irreversible.  A core override under a
				 * LOCAL_NEUTRAL default remains meaningful after partial ANALYZE.
				 */
				if (entry->state == FASTTRUN_COLUMN_CORE_ALLOWED &&
					relentry->policy == FASTTRUN_REL_CORE_ALLOWED)
				{
					fasttrun_stats_entry_free_undo(entry);
					if (entry->statsTuple != NULL)
						heap_freetuple(entry->statsTuple);
					(void) hash_search(fasttrun_stats_cache, &key,
									   HASH_REMOVE, NULL);
					fasttrun_stats_relid_unlink_key(relentry, &key);
					continue;
				}
				fasttrun_stats_entry_free_undo(entry);
				entry->state_subid = InvalidSubTransactionId;
				continue;
			}

			/*
			 * Read pgstat once per relid, not once per attkey.  All
			 * statsTuples for this relid share the same counters; the
			 * scalar-per-entry loop just compared them against the same
			 * (ins, upd, del, truncdropped) tuple anyway.  Both reads are
			 * relcache-free: this callback runs at TRANS_COMMIT, where
			 * catalog access is forbidden.
			 */
			if (!counters_attempted)
			{
				counters_attempted = true;
				have_counters =
					fasttrun_read_pgstat_counters_at_commit(relid, &ins_now,
															&upd_now, &del_now,
															&truncdropped_now);
				pages_now = fasttrun_stats_relid_nblocks_at_commit(relentry);
			}

			/*
			 * Preserve statistics across COMMIT while changes stay within the
			 * allowed threshold.  If counters are unavailable or the distribution
			 * has changed too much, hide the statistics.  When DML changes visible
			 * statistics to hidden on a nonempty table, invalidate its plans once
			 * after the column loop.
			 */
			if (!have_counters ||
				!fasttrun_stats_entry_usable(relid, entry, ins_now, upd_now,
											 del_now, truncdropped_now, pages_now))
			{
				if (entry->was_usable)
					plan_inval_needed = true;
				fasttrun_stats_entry_free_undo(entry);
				fasttrun_stats_relid_unref(key.relid);
				heap_freetuple(entry->statsTuple);
				entry->statsTuple = NULL;
				entry->state = FASTTRUN_COLUMN_LOCAL_NEUTRAL;
				entry->was_usable = false;
				entry->state_subid = InvalidSubTransactionId;
				continue;
			}

			fasttrun_stats_entry_free_undo(entry);
			entry->collected_ins = 0;
			entry->collected_upd = 0;
			entry->collected_del = 0;
			entry->collected_truncdropped = false;
			entry->state_subid = InvalidSubTransactionId;
		}

		list_free(keys);
		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
		if (relentry == NULL)
			continue;
		fasttrun_stats_relid_free_undo(relentry);
		relentry->state_subid = InvalidSubTransactionId;
		fasttrun_stats_relid_maybe_drop(relid, relentry);
		/*
		 * ON COMMIT DELETE ROWS has already truncated local storage before
		 * this callback.  Keep its old zero-invalidation contract: no rows
		 * can use the cached plan, while the next explicit refresh owns any
		 * needed invalidation after refill.  This is the original hot path
		 * that must not turn N tables into N plan-cache walks per COMMIT.
		 * Ordinary DELETE keeps heap pages and therefore still invalidates.
		 */
		if (plan_inval_needed && pages_now > 0)
			fasttrun_invalidate_local_plan_cache(xentry->root_relid);
	}

	/*
	 * When the cache is empty, drop the stats HTABs and mcxt so the
	 * column-stats hooks and the planner-hook guard return to the zero-cost
	 * `cache == NULL` path.  Re-armed lazily on the next collect.
	 */
	if (hash_get_num_entries(fasttrun_stats_relid_cache) == 0 &&
		hash_get_num_entries(fasttrun_stats_cache) == 0)
		fasttrun_stats_cache_reset();
}

/*
 * Drop all stats entries for a given relid.
 *
 * Called from fasttruncate as a backstop against the freshness check --
 * for example when pgstat is off and the hook cannot compare counters.
 * Also called from fasttrun_collect_stats and fasttrun_analyze when a
 * refresh observes a now-empty sample.
 *
 * Iterates only the keys registered in fasttrun_stats_relid_cache for
 * this relid -- O(#cached-cols).  No more hash_seq_search over the whole
 * stats hash (O(total-stats-entries)).
 */
static bool
fasttrun_stats_cache_evict_relid(Oid relid)
{
	FasttrunStatsRelidEntry *relentry;
	List	   *keys;
	ListCell   *lc;
	bool		removed = false;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return false;

	relentry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													   &relid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->attkeys == NIL)
		return false;

	/*
	 * Snapshot the attkey list.  fasttrun_stats_relid_drop_key mutates
	 * relentry->attkeys during iteration, and that would invalidate the
	 * ListCells we are walking.
	 */
	keys = list_copy(relentry->attkeys);

	foreach(lc, keys)
	{
		FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(lc);
		FasttrunStatsKey key = *kptr;	/* value copy -- drop_key may pfree source */
		FasttrunStatsEntry *entry;

		entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
												   &key, HASH_FIND, NULL);
		if (entry == NULL)
		{
			/* Out of sync -- drop the dangling backref. */
			fasttrun_stats_relid_drop_key(&key);
			continue;
		}

		removed |= fasttrun_stats_entry_neutralize(relid, entry);
	}

	list_free(keys);
	return removed;
}

/*
 * Soft-evict every visible statsTuple for `relid`.
 *
 * statsTuple goes to NULL; the old version is pushed onto the per-entry
 * undo stack so ROLLBACK TO SAVEPOINT can restore it.  Called from DDL
 * eviction via fasttrun_evict_temp_relid.
 *
 * Uses the same O(K) attkeys backref as evict_relid above.
 */
static bool
fasttrun_stats_cache_mark_evicted_relid(Oid relid)
{
	return fasttrun_stats_cache_evict_relid(relid);
}

static bool
fasttrun_stats_neutralize_relation(Relation rel)
{
	bool		changed;

	changed = fasttrun_stats_set_relation_policy(rel,
											 FASTTRUN_REL_LOCAL_NEUTRAL);
	changed |= fasttrun_stats_cache_evict_relid(RelationGetRelid(rel));
	return changed;
}

/* Abort-time adoption after a non-transactional truncate: no allocation. */
static void
fasttrun_stats_force_neutral_relid(Oid relid)
{
	FasttrunStatsRelidEntry *relentry;
	ListCell   *lc;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL)
		return;

	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(lc);
		FasttrunStatsEntry *entry;

		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);
		if (entry == NULL)
			continue;
		if (fasttrun_stats_entry_is_candidate(entry))
			fasttrun_stats_relid_unref(relid);
		fasttrun_stats_entry_free_undo(entry);
		if (entry->statsTuple != NULL)
			heap_freetuple(entry->statsTuple);
		entry->statsTuple = NULL;
		entry->state = FASTTRUN_COLUMN_LOCAL_NEUTRAL;
		entry->state_subid = InvalidSubTransactionId;
		entry->was_usable = false;
	}
	fasttrun_stats_relid_free_undo(relentry);
	relentry->policy = FASTTRUN_REL_LOCAL_NEUTRAL;
	relentry->state_subid = InvalidSubTransactionId;
}

static void
fasttrun_stats_handoff_columns(Relation rel, Bitmapset *attnums,
							   bool full_relation)
{
	Oid			relid = RelationGetRelid(rel);
	FasttrunStatsRelidEntry *relentry;
	List	   *keys;
	ListCell   *lc;
	int			attnum = -1;

	if (!full_relation)
	{
		/*
		 * PREPARE normally created a neutral override for every selected
		 * attribute.  VACUUM can own transaction boundaries, though, and a
		 * rewrite can replace the locator before post-success handoff.  Create
		 * missing CORE_ALLOWED overrides here too; relation default may still
		 * be LOCAL_NEUTRAL after a partial ANALYZE.
		 */
		while ((attnum = bms_next_member(attnums, attnum)) >= 0)
		{
			if (attnum == 0)
				continue;		/* partial target with no analyzable columns */
			(void) fasttrun_stats_set_column_state(rel, attnum, false,
										 FASTTRUN_COLUMN_CORE_ALLOWED,
										 NULL, NULL);
		}
		return;
	}

	(void) fasttrun_stats_set_relation_policy(rel,
									  FASTTRUN_REL_CORE_ALLOWED);
	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->attkeys == NIL)
		return;

	keys = list_copy(relentry->attkeys);
	foreach(lc, keys)
	{
		FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(lc);

		(void) fasttrun_stats_set_column_state(rel, key->attnum, key->inh,
										 FASTTRUN_COLUMN_CORE_ALLOWED,
										 NULL, NULL);
	}
	list_free(keys);
}

/*
 * Detect a visible->hidden flip of this relid's cached column stats.
 *
 * The freshness gate (fasttrun_stats_entry_usable) hides an entry from the
 * planner once churn passes the cardinality-scaled tolerance, while the
 * refresh path recollects only at the flat stats_refresh_threshold.  In the
 * band between them new plans fall back to defaults, but a cached generic
 * plan built on the now-hidden distribution would keep running -- the flip
 * must invalidate it.
 *
 * Walks only this relid's attkeys backref (O(#cached-cols)), recomputes
 * usability with the same check the planner hook applies, and refreshes each
 * entry's was_usable marker.  The marker moves only here and on publish --
 * never from the planner hooks -- so detection does not depend on whether
 * anything was planned between two analyze calls.  Returns true when at
 * least one column that served stats at the last analyze no longer does.
 */
static bool
fasttrun_stats_note_visibility(Oid relid, bool have_counters,
							   int64 ins_now, int64 upd_now, int64 del_now,
							   bool truncdropped_now, BlockNumber pages_now)
{
	FasttrunStatsRelidEntry *relentry;
	ListCell   *lc;
	bool		flipped = false;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return false;

	relentry = (FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
													   &relid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->attkeys == NIL)
		return false;

	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(lc);
		FasttrunStatsEntry *entry;
		bool		usable_now;

		entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   key, HASH_FIND, NULL);
		if (entry == NULL || !fasttrun_stats_entry_is_candidate(entry))
			continue;			/* no published stats -- nothing to flip */

		/* The planner hook hides stats when pgstat is unavailable too. */
		usable_now = have_counters &&
			fasttrun_stats_entry_usable(relid, entry, ins_now, upd_now,
										del_now, truncdropped_now, pages_now);
		if (entry->was_usable && !usable_now)
		{
			flipped = true;
			(void) fasttrun_stats_entry_neutralize(relid, entry);
		}
	}

	return flipped;
}

/*
 * SubXact callback: savepoint-aware undo/restore for both the per-column
 * stats cache and the analyze-cache stats baseline.
 *
 * Per-column stats cache (statsTuple + freshness counters):
 *   ABORT_SUB:
 *     - entry has undo -> pop top saved state back into entry (full
 *       restore of pre-subxact version);
 *     - no undo -> entry was created inside aborting subxact -> remove.
 *   COMMIT_SUB: re-label sub-local entries with the parent subid; if
 *     the undo top is already at parent level, collapse it.
 *
 * Analyze-cache stats baseline (used by the refresh-path churn check):
 *   ABORT_SUB:
 *     - baseline was set in this subxact and has undo -> pop;
 *     - was set in this subxact and no undo -> mark has_stats_baseline
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
	FasttrunXactFrame *frame;
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB)
		return;

	/*
	 * Hot-path shortcut.  When no fasttrun cache mutation happened anywhere
	 * in the current top xact, the cache holds nothing that needs
	 * subxact-aware rollback or promotion.  PL/pgSQL EXCEPTION blocks (each
	 * iteration is its own subxact) and similar savepoint-heavy patterns
	 * skip the whole walk entirely.
	 */
	frame = fasttrun_xact_frame;
	if (frame == NULL || frame->subid != mySubid)
		return;

	/*
	 * Walk only the ending frame, not every relid touched by sibling
	 * subtransactions.  For each relid we hit
	 * the stats cache through fasttrun_stats_relid_cache.attkeys (O(K) per
	 * relid) and the analyze cache via a single hash_search by relid.
	 * Sibling subtransactions release their frame context when they abort.
	 */
	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		Oid			relid = xentry->relid;
		FasttrunAnalyzeCacheEntry *aentry;

#ifdef USE_ASSERT_CHECKING
		fasttrun_test_subxact_visited++;
#endif

		if (fasttrun_stats_cache != NULL && fasttrun_stats_relid_cache != NULL)
		{
			FasttrunStatsRelidEntry *relentry;
			List	   *keys;
			ListCell   *klc;

			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
			if (relentry != NULL && relentry->attkeys != NIL)
			{
				/*
				 * Snapshot attkeys.  Drop paths below pfree list nodes via
				 * fasttrun_stats_relid_drop_key, mutating the live list.
				 */
				keys = list_copy(relentry->attkeys);

				foreach(klc, keys)
				{
					FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(klc);
					FasttrunStatsKey key = *kptr;
					FasttrunStatsEntry *sentry;

					sentry = (FasttrunStatsEntry *)
						hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
					if (sentry == NULL)
					{
						fasttrun_stats_relid_drop_key(&key);
						continue;
					}
					if (sentry->state_subid != mySubid)
						continue;

					if (event == SUBXACT_EVENT_ABORT_SUB)
					{
						if (sentry->undo != NULL)
						{
							/* Restore pre-subxact version from the undo stack. */
							FasttrunStatsSavedState *popped = sentry->undo;

							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_unref(relid);
							if (sentry->statsTuple != NULL)
							{
								heap_freetuple(sentry->statsTuple);
							}
							sentry->statsTuple = popped->statsTuple;
							sentry->collected_ins = popped->collected_ins;
							sentry->collected_upd = popped->collected_upd;
							sentry->collected_del = popped->collected_del;
							sentry->collected_truncdropped = popped->collected_truncdropped;
							sentry->collected_pages = popped->collected_pages;
							sentry->was_usable = popped->was_usable;
							sentry->state = popped->state;
							sentry->heap_rlb = popped->heap_rlb;
							sentry->heap_rlb_valid = popped->heap_rlb_valid;
							sentry->state_subid = popped->state_subid;
							sentry->undo = popped->older;
							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_ref(relid);
							pfree(popped);	/* popped->statsTuple now owned by entry */
						}
						else
						{
							/* Created in this subxact -> drop entirely. */
							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_unref(relid);
							if (sentry->statsTuple != NULL)
								heap_freetuple(sentry->statsTuple);
							(void) hash_search(fasttrun_stats_cache, &key,
											   HASH_REMOVE, NULL);
							fasttrun_stats_relid_drop_key(&key);
						}
					}
					else	/* SUBXACT_EVENT_COMMIT_SUB */
					{
						sentry->state_subid = parentSubid;

						/*
						 * If the undo top was also at parent level, the pre-
						 * and post-store versions now sit at the same subxact
						 * -> the undo entry is obsolete (outer rollback would
						 * restore to the same level anyway), collapse it.
						 */
						if (sentry->undo != NULL &&
							sentry->undo->state_subid == parentSubid)
						{
							FasttrunStatsSavedState *obsolete = sentry->undo;

							if (obsolete->statsTuple != NULL)
								heap_freetuple(obsolete->statsTuple);
							sentry->undo = obsolete->older;
							pfree(obsolete);
						}
					}
				}

				list_free(keys);
			}
		}

		if (fasttrun_stats_relid_cache != NULL)
		{
			FasttrunStatsRelidEntry *relentry;

			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
			if (relentry != NULL && relentry->state_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					if (relentry->undo != NULL)
					{
						FasttrunStatsRelidSavedState *popped = relentry->undo;

						relentry->policy = popped->policy;
						relentry->heap_rlb = popped->heap_rlb;
						relentry->heap_rlb_valid = popped->heap_rlb_valid;
						relentry->state_subid = popped->state_subid;
						relentry->undo = popped->older;
						pfree(popped);
					}
					else
					{
						relentry->policy = FASTTRUN_REL_CORE_ALLOWED;
						memset(&relentry->heap_rlb, 0,
							   sizeof(relentry->heap_rlb));
						relentry->heap_rlb_valid = false;
						relentry->state_subid = InvalidSubTransactionId;
					}
				}
				else
				{
					relentry->state_subid = parentSubid;
					if (relentry->undo != NULL &&
						relentry->undo->state_subid == parentSubid)
					{
						FasttrunStatsRelidSavedState *obsolete = relentry->undo;

						relentry->undo = obsolete->older;
						pfree(obsolete);
					}
				}
				fasttrun_stats_relid_maybe_drop(relid, relentry);
			}
		}

		aentry = NULL;
		if (fasttrun_analyze_cache != NULL)
			aentry = (FasttrunAnalyzeCacheEntry *)
				hash_search(fasttrun_analyze_cache, &relid, HASH_FIND, NULL);

		if (aentry != NULL)
		{
			if (aentry->state_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					Oid		plan_relid;

					if (fasttrun_cache_make_empty_storage_authoritative(aentry,
																		&plan_relid))
					{
						/*
						 * fasttruncate() is non-transactional for local
						 * storage: after ROLLBACK TO SAVEPOINT the rows are
						 * still gone.  Do not restore the old relstats undo
						 * chain; make the observed empty storage the new
						 * outer-subxact truth instead.
						 */
						fasttrun_analyze_free_undo(aentry);

						aentry->state.has_stats_baseline = false;
						aentry->state.stats_baseline_inserted = 0;
						aentry->state.stats_baseline_updated = 0;
						aentry->state.stats_baseline_deleted = 0;
						aentry->state.stats_baseline_truncdropped = false;
						fasttrun_cache_reset_partial_scan_anchor(aentry);

						if (OidIsValid(plan_relid))
							fasttrun_stats_force_neutral_relid(plan_relid);
						aentry->state.last_inval_valid = false;
						goto finish_entry;
					}

					if (aentry->undo != NULL)
					{
						FasttrunAnalyzeUndo *popped = aentry->undo;

						aentry->state = popped->state;
						aentry->state_subid = popped->state_subid;
						aentry->undo = popped->older;
						pfree(popped);
					}
					else
					{
						(void) hash_search(fasttrun_analyze_cache, &relid,
										   HASH_REMOVE, NULL);
						aentry = NULL;
					}
				}
				else
				{
					aentry->state_subid = parentSubid;
					if (aentry->undo != NULL &&
						aentry->undo->state_subid == parentSubid)
					{
						FasttrunAnalyzeUndo *obsolete = aentry->undo;

						aentry->undo = obsolete->older;
						pfree(obsolete);
					}
				}
			}

			/* A row restored to its pre-creation empty state has no owner. */
			if (event == SUBXACT_EVENT_ABORT_SUB && aentry != NULL &&
				!aentry->state.has_relstats &&
				!aentry->state.has_stats_baseline &&
				aentry->undo == NULL)
			{
				(void) hash_search(fasttrun_analyze_cache, &relid,
								   HASH_REMOVE, NULL);
				aentry = NULL;
			}

			/*
			 * Lazy-probe memo.  The observation in
			 * fasttrun_reinject_relstats is valid only inside the subxact
			 * that recorded it.  On ABORT_SUB we drop it -- the storage
			 * state we saw is the pre-abort view, and we cannot tell now
			 * whether later DML in this rolled-back subxact moved
			 * cached_pages.  On COMMIT_SUB we promote it to the parent
			 * subxact so plans there keep skipping the probe.
			 */
			if (aentry != NULL && aentry->lazy_check_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					aentry->lazy_check_subid = InvalidSubTransactionId;
					aentry->lazy_check_pages = 0;
				}
				else		/* SUBXACT_EVENT_COMMIT_SUB */
					aentry->lazy_check_subid = parentSubid;
			}
		}

	finish_entry:
		/* The owning-heap entry deduplicates abort-time plan invalidation. */
		if (event == SUBXACT_EVENT_ABORT_SUB &&
			(xentry->flags & FASTTRUN_TOUCH_PLAN_INVALIDATE) != 0 &&
			xentry->relid == xentry->root_relid)
			fasttrun_invalidate_local_plan_cache(xentry->root_relid);
	}

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		/*
		 * Unique relids from aborted sibling frames must not leave enlarged
		 * empty hash tables in a long-lived backend.  Live session state keeps
		 * either cache nonempty, so deleting an empty cache loses no snapshot.
		 */
		if (fasttrun_analyze_cache != NULL &&
			hash_get_num_entries(fasttrun_analyze_cache) == 0)
			fasttrun_cache_reset();
		if (fasttrun_stats_cache != NULL &&
			fasttrun_stats_relid_cache != NULL &&
			hash_get_num_entries(fasttrun_stats_cache) == 0 &&
			hash_get_num_entries(fasttrun_stats_relid_cache) == 0)
			fasttrun_stats_cache_reset();

		fasttrun_xact_frame = frame->parent;
		MemoryContextDelete(frame->mcxt);
	}
	else if (frame->parent == NULL || frame->parent->subid != parentSubid)
	{
		/*
		 * Frames are lazy, so the immediate parent may have no frame while an
		 * older ancestor does.  Keep this frame and retag it; merging into the
		 * ancestor would make a later ROLLBACK TO the empty parent invisible.
		 */
		frame->subid = parentSubid;
	}
	else
	{
		FasttrunXactFrame *parent = frame->parent;

		hash_seq_init(&status, frame->entries);
		while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
			fasttrun_xact_merge_entry(parent, xentry);
		fasttrun_xact_frame = parent;
		MemoryContextDelete(frame->mcxt);
	}
}

/* ----------------------------------------------------------------------
 * Core std_typanalyze integration.
 *
 * When fasttrun.use_typanalyze=on (default), fasttrun reuses PostgreSQL's
 * own per-type typanalyze callbacks to compute n_distinct, MCV,
 * histogram, correlation and any type-specific statistics -- exactly the
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

/* Core ANALYZE has no stored sample value for a PG18 virtual column. */
static inline bool
fasttrun_attribute_is_analyzable(Form_pg_attribute attr)
{
	if (attr->attisdropped)
		return false;
#if PG_VERSION_NUM >= 180000
	if (attr->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
		return false;
#endif
	return true;
}

/*
 * Standard fetch function for compute_stats -- pulls a column value out
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

	if (!fasttrun_attribute_is_analyzable(attr))
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
 * commands/analyze.c -- but without ever opening pg_statistic for write.
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
 * delete on the way out -- this is just a sizing query, the real
 * collect_and_store path will examine_attribute again.  Each call is
 * O(natts) syscache lookups + a typanalyze function call per column,
 * usually a few hundred microseconds total even on wide tables.  Cheap
 * compared to the actual heap scan.
 *
 * Returns the maximum minrows seen, or 0 if no column has a usable
 * typanalyze (in which case the caller will skip stats collection).
 * A floor of 100 is enforced for sanity -- even degenerate cases get
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
 *   < 0 -> autosize via fasttrun_compute_max_minrows() (full ANALYZE
 *         parity, follows default_statistics_target);
 *   = 0 -> caller treats as "stats collection disabled";
 *   > 0 -> explicit override.
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

static List *
fasttrun_open_index_rels(Relation rel)
{
	List	   *index_oids = RelationGetIndexList(rel);
	List	   *index_rels = NIL;
	ListCell   *lc;

	foreach(lc, index_oids)
		index_rels = lappend(index_rels,
							 index_open(lfirst_oid(lc), AccessShareLock));
	list_free(index_oids);
	return index_rels;
}

static void
fasttrun_close_index_rels(List *index_rels)
{
	ListCell   *lc;

	foreach(lc, index_rels)
		index_close((Relation) lfirst(lc), AccessShareLock);
	list_free(index_rels);
}

static bool
fasttrun_index_rels_have_partial(List *index_rels)
{
	ListCell   *lc;

	foreach(lc, index_rels)
	{
		if (RelationGetIndexPredicate((Relation) lfirst(lc)) != NIL)
			return true;
	}
	return false;
}

/*
 * Refresh relpages/reltuples/relallvisible of every index of `rel`.
 * index_rels is an already-open list from fasttrun_open_index_rels so hot
 * callers pay one open per index per analyze; NIL means open (and close)
 * here.
 */
static bool
fasttrun_update_index_relstats(Relation rel, List *index_rels,
							   HeapTuple *sample, int sample_count,
							   int64 totalrows, bool partial_upper_bound)
{
	ListCell   *lc;
	bool		changed = false;
	List	   *opened = NIL;

	if (index_rels == NIL)
		index_rels = opened = fasttrun_open_index_rels(rel);

	foreach(lc, index_rels)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		BlockNumber	index_pages;
		double		tuple_fract;
		int64		index_tuples;
		bool		is_partial;
		bool		set_reltuples = true;

		is_partial = (RelationGetIndexPredicate(indexrel) != NIL);

		index_pages = RelationGetNumberOfBlocks(indexrel);
		if (is_partial && sample_count > 0)
		{
			IndexInfo  *indexInfo = BuildIndexInfo(indexrel);

			tuple_fract = fasttrun_estimate_index_fraction(rel, indexrel,
															   indexInfo, sample,
															   sample_count);
		}
		else if (is_partial && !partial_upper_bound)
		{
			/*
			 * Sample-less refresh after a sample-based one already set this
			 * partial index's tuple count this analyze: keep that estimate,
			 * refresh only relpages/relallvisible below.
			 */
			tuple_fract = 0.0;
			set_reltuples = false;
		}
		else
			/*
			 * A full index, or a partial index whose tuple count has no better
			 * source: use totalrows as an upper bound -- better than leaving a
			 * stale under-estimate.
			 */
			tuple_fract = 1.0;
		index_tuples = set_reltuples
			? (int64) ceil(tuple_fract * (double) totalrows)
			: (int64) indexrel->rd_rel->reltuples;

		/*
		 * Report a change only for stats that move independently of the heap
		 * row count: index relpages and relallvisible (which vary with
		 * expression and varlena-keyed indexes and btree dedup), and a partial
		 * index's tuple count (which tracks the predicate, not the heap).  A
		 * plain index's reltuples is a linear copy of the heap row count that
		 * the caller already weighs, so it is not a separate signal here.
		 */
		if (indexrel->rd_rel->relpages != index_pages ||
			indexrel->rd_rel->relallvisible != 0 ||
			(is_partial &&
			 indexrel->rd_rel->reltuples != (float4) index_tuples))
			changed = true;

		indexrel->rd_rel->relpages = index_pages;
		indexrel->rd_rel->reltuples = (float4) index_tuples;
		indexrel->rd_rel->relallvisible = 0;
		fasttrun_cache_set_owning_heap(fasttrun_cache_store_relstats(indexrel,
																	 index_pages,
																	 index_tuples,
																	 0),
									   rel);
	}

	if (opened != NIL)
		fasttrun_close_index_rels(opened);

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
 * Above this raw width a varlena sample value is not detoasted for the
 * lightweight Haas-Stokes path: materializing multi-MB TOAST payloads for
 * every sampled row would blow up backend memory.  Matches core
 * analyze.c:WIDTH_THRESHOLD -- such values are counted as distinct instead.
 */
#define FASTTRUN_WIDTH_THRESHOLD	1024

/*
 * For every column of rel, compute stats from the sample and store in
 * the session-local cache.  Columns without a btree-orderable type are
 * skipped (planner falls back to defaults).  Also captures the current
 * pgstat counters as a freshness baseline -- the planner hook compares
 * against them and refuses to return stale stats after DML.
 *
 * Two implementations of "compute the stats" coexist, switched by
 * fasttrun.use_typanalyze (default on):
 *   * on  -> fasttrun_collect_via_typanalyze: full core std_typanalyze
 *           with MCV / histogram / correlation;
 *   * off -> fasttrun_collect_via_haas_stokes: lightweight n_distinct /
 *           null_frac / width only.
 * Both branches end at the same fasttrun_stats_cache_store() and
 * publish through the same hooks.
 */
/*
 * The result separates publishing a candidate from a transition to neutral.
 * Callers invalidate only when either changes planner-visible ownership.
 */
static FasttrunCollectResult
fasttrun_collect_and_store(Relation rel, HeapTuple *sample, int sample_count,
						   int64 totalrows, bool sample_needs_tid_sort)
{
	FasttrunCollectResult result = {false, false};
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
	BlockNumber	snap_pages;

	if (sample_count <= 0)
		return result;

	/*
	 * Physical block count at collect time: the freshness check anchors to it
	 * to detect a cross-commit refill that pgstat counters cannot see (they
	 * reset per xact and temp stats never reach shared pgstat).
	 */
	snap_pages = RELKIND_HAS_STORAGE(rel->rd_rel->relkind)
		? RelationGetNumberOfBlocks(rel) : 0;

	/*
	 * track_counts=off (or pgstat otherwise unavailable) -> no baseline
	 * for the freshness check -> do not cache at all, the planner will
	 * fall back to defaults.  This is a "silent killer" -- plans
	 * silently degrade without any explicit signal, so we raise a
	 * WARNING once per backend to make DBAs see it in the server log
	 * (and the psql client sees it too).
	 */
	if (!fasttrun_read_pgstat_counters(rel, &snap_ins, &snap_upd, &snap_del,
									   &snap_truncdropped))
	{
		result.became_neutral =
			fasttrun_stats_cache_evict_relid(RelationGetRelid(rel));

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
		return result;
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
			AttrNumber			attnum = a + 1;
			VacAttrStats	   *stats;
			HeapTuple			stats_tuple;

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
			fasttrun_stats_cache_store(rel, attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped, snap_pages);
			result.published_any = true;
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
		 * type-specific stats -- but ~5x cheaper on wide tables and
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
			int				   n_array = 0;
			int				   toowide_cnt = 0;
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

			if (!fasttrun_attribute_is_analyzable(attr))
				continue;
			attstattarget = fasttrun_get_attstattarget(rel, attnum);
			if (attstattarget == 0)
				continue;

			typentry = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);
			if (!OidIsValid(typentry->lt_opr))
				continue;	/* no btree comparator -> skip */

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

				/*
				 * Accumulate the average-width sample exactly once per type:
				 * pass-by-value, varlena, and fixed-length by-reference are
				 * mutually exclusive branches, so no width is double-counted.
				 * Only non-toowide values land in values[] for the distinct
				 * sort; n_nonnull counts every non-null, n_array the sortable
				 * subset.
				 */
				if (attr->attbyval)
				{
					values[n_array++] = d;
					total_width += attr->attlen;
				}
				else if (attr->attlen == -1)
				{
					/*
					 * Excessively wide varlena: don't detoast (would
					 * materialize the full TOAST payload and can OOM the
					 * backend on multi-MB values).  Count it as too-wide --
					 * assumed distinct -- mirroring core analyze.c.  The size
					 * probe reads the raw length from the header without
					 * detoasting.
					 */
					/* Stored (possibly compressed) width, as core: before detoast. */
					total_width += VARSIZE_ANY(DatumGetPointer(d));

					if (toast_raw_datum_size(d) > FASTTRUN_WIDTH_THRESHOLD)
					{
						toowide_cnt++;
						n_nonnull++;
						continue;
					}

					/* In-range varlena: detoast once, for the comparisons only. */
					{
						struct varlena *detoasted = pg_detoast_datum_packed(
							(struct varlena *) DatumGetPointer(d));

						values[n_array++] = PointerGetDatum(detoasted);
					}
				}
				else
				{
					/* Fixed-length by-reference (uuid, name, interval, ...). */
					values[n_array++] = d;
					if (attr->attlen > 0)
						total_width += attr->attlen;
				}

				n_nonnull++;
			}

			if (n_nonnull == 0)
			{
				stanullfrac = 1.0f;
				stawidth = (attr->attlen > 0) ? attr->attlen : 0;
				stadistinct = 0.0f;
			}
			else
			{
				if (n_array > 0)
				{
					memset(&ssup, 0, sizeof(ssup));
					ssup.ssup_cxt = per_col_mcxt;
					ssup.ssup_collation = attr->attcollation;
					ssup.ssup_nulls_first = false;
					PrepareSortSupportFromOrderingOp(typentry->lt_opr, &ssup);

					qsort_arg(values, n_array, sizeof(Datum),
							  fasttrun_datum_cmp, &ssup);

					/* Count distinct values and multiples in the sorted run. */
					dups_cnt = 1;
					for (i = 1; i <= n_array; i++)
					{
						bool	end_of_run;

						if (i == n_array)
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
				}

				/*
				 * Too-wide values were never compared, so treat each as its
				 * own distinct singleton (same assumption core makes).
				 */
				stanullfrac = (float4) n_null / (float4) sample_count;
				stawidth = (int32) (total_width / n_nonnull);
				stadistinct = fasttrun_estimate_ndistinct(n_nonnull,
														  ndistinct + toowide_cnt,
														  nmultiple, totalrows,
														  (double) stanullfrac);
			}

			fasttrun_apply_attribute_options(rel, attnum, false,
											 &stadistinct);
			MemoryContextSwitchTo(oldcxt);

			stats_tuple = fasttrun_build_stats_tuple(pg_stats_desc,
													 RelationGetRelid(rel),
													 attnum,
													 stanullfrac, stawidth,
													 stadistinct);

			fasttrun_stats_cache_store(rel, attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped, snap_pages);
			result.published_any = true;
			heap_freetuple(stats_tuple);
		}

		pfree(values);
	}

	MemoryContextDelete(per_col_mcxt);
	table_close(pg_stats_rel, AccessShareLock);
	(void) fasttrun_update_index_relstats(rel, NIL, sample, sample_count,
										  totalrows, true);
	fasttrun_stats_relid_remember_locator(rel);

	/*
	 * Note: the stats-collection baseline (used by the delta-hit refresh
	 * path to decide whether DML churn warrants another sample) lives in
	 * the analyze cache entry, not in the per-(relid,attnum) stats cache.
	 * It is set by the callers of this function -- fasttrun_analyze()
	 * cold path and refresh path -- both of which already own (or just
	 * created) an analyze cache entry to attach it to.  fasttrun_collect_
	 * stats() does NOT touch the baseline, by design: it is a "give me
	 * column stats now" entry point that doesn't participate in delta-
	 * math state tracking and shouldn't interfere with it.
	 */
	return result;
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
	double			samplerows = 0;
	double			rowstoskip = -1;
	ReservoirStateData rstate;

	if (sample != NULL && sample_target > 0)
		reservoir_init_selection_state(&rstate, sample_target);

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
			/* Vitter skip distances: one RNG draw per replacement, not per row. */
			if (rowstoskip < 0)
				rowstoskip = reservoir_get_next_S(&rstate, samplerows,
												  sample_target);

			if (rowstoskip <= 0)
			{
				int		k = (int) (sample_target *
								   sampler_random_fract(&rstate.randstate));

				Assert(k >= 0 && k < sample_target);
				heap_freetuple(sample[k]);
				sample[k] = heap_copytuple(tuple);
			}
			rowstoskip -= 1;
		}
		samplerows += 1;
	}
	table_endscan(scan);

	*tuples_out = tuples_count;
	*sample_count_out = sample_count;
}

/*
 * Block-sampled variant of fasttrun_scan_with_sample for giant temp tables
 * (heap pages > fasttrun.max_analyze_pages).  Reads only a bounded random
 * sample of blocks instead of the whole relation and estimates the row count
 * from tuple density, exactly like core acquire_sample_rows() -- so cold
 * analyze cost stays O(sample) rather than O(table).  *tuples_out is an
 * ESTIMATE (like a regular ANALYZE), not an exact count.
 *
 * Ported from commands/analyze.c on the public block-sampling API
 * (BlockSampler_* + table_scan_analyze_next_block/tuple).  Prefetch and
 * progress reporting are dropped; the reservoir + density math match core.
 */
/*
 * Reservoir bookkeeping for one analyzed block: pull every live tuple the
 * analyze scan yields (which also advances *liverows for the density
 * estimate) and, when the caller wants a column-stats sample, keep a Vitter
 * reservoir of sample_target rows.  Shared by the PG16/17 and PG18 outer
 * loops, which only differ in how blocks are fed to the scan.
 */
static void
fasttrun_reservoir_collect_block(TableScanDesc scan, TransactionId OldestXmin,
								 TupleTableSlot *slot, HeapTuple *sample,
								 int sample_target, int *numrows,
								 double *samplerows, double *rowstoskip,
								 double *liverows, double *deadrows,
								 ReservoirState rstate)
{
	while (table_scan_analyze_next_tuple(scan, OldestXmin,
										 liverows, deadrows, slot))
	{
		if (sample != NULL)
		{
			if (*numrows < sample_target)
				sample[(*numrows)++] = ExecCopySlotHeapTuple(slot);
			else
			{
				if (*rowstoskip < 0)
					*rowstoskip = reservoir_get_next_S(rstate, *samplerows,
													   sample_target);

				if (*rowstoskip <= 0)
				{
					int		k = (int) (sample_target *
									   sampler_random_fract(&rstate->randstate));

					Assert(k >= 0 && k < sample_target);
					heap_freetuple(sample[k]);
					sample[k] = ExecCopySlotHeapTuple(slot);
				}
				*rowstoskip -= 1;
			}
		}
		*samplerows += 1;
	}
}

#if PG_VERSION_NUM >= 170000
/* ReadStream callback: hand out BlockSampler-chosen blocks (PG17+ analyze). */
static BlockNumber
fasttrun_block_sample_stream_next(ReadStream *stream, void *callback_private_data,
								  void *per_buffer_data)
{
	BlockSampler	bs = (BlockSampler) callback_private_data;

	return BlockSampler_HasMore(bs) ? BlockSampler_Next(bs) : InvalidBlockNumber;
}
#endif

static void
fasttrun_block_sample_rows(Relation rel, int64 *tuples_out,
						   HeapTuple *sample, int sample_target,
						   int *sample_count_out)
{
	int				numrows = 0;
	double			samplerows = 0;
	double			liverows = 0;
	double			deadrows = 0;
	double			rowstoskip = -1;
	uint32			randseed;
	BlockNumber		totalblocks;
	TransactionId	OldestXmin;
	BlockSamplerData bs;
	ReservoirStateData rstate;
	TupleTableSlot *slot;
	TableScanDesc	scan;
	BufferAccessStrategy strategy;

	elog(DEBUG1, "fasttrun: block-sampling analyze scan");

	totalblocks = RelationGetNumberOfBlocks(rel);
	OldestXmin = GetOldestNonRemovableTransactionId(rel);
	randseed = pg_prng_uint32(&fasttrun_prng_state);
	(void) BlockSampler_Init(&bs, totalblocks, sample_target, randseed);
	reservoir_init_selection_state(&rstate, sample_target);
	strategy = GetAccessStrategy(BAS_VACUUM);

	scan = table_beginscan_analyze(rel);
	slot = table_slot_create(rel, NULL);

#if PG_VERSION_NUM >= 170000
	{
		/* PG17+ drives the sampled-block scan through a ReadStream. */
		ReadStream *stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE,
														strategy, rel,
														MAIN_FORKNUM,
														fasttrun_block_sample_stream_next,
														&bs, 0);

		while (table_scan_analyze_next_block(scan, stream))
		{
			CHECK_FOR_INTERRUPTS();
			fasttrun_reservoir_collect_block(scan, OldestXmin, slot, sample,
											 sample_target, &numrows,
											 &samplerows, &rowstoskip,
											 &liverows, &deadrows, &rstate);
		}

		read_stream_end(stream);
	}
#else
	while (BlockSampler_HasMore(&bs))
	{
		BlockNumber		targblock = BlockSampler_Next(&bs);

		CHECK_FOR_INTERRUPTS();

		if (!table_scan_analyze_next_block(scan, targblock, strategy))
			continue;

		fasttrun_reservoir_collect_block(scan, OldestXmin, slot, sample,
										 sample_target, &numrows,
										 &samplerows, &rowstoskip,
										 &liverows, &deadrows, &rstate);
	}
#endif

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	FreeAccessStrategy(strategy);

	/* Density extrapolation over the sampled blocks (core's estimator). */
	if (bs.m > 0)
		*tuples_out = (int64) floor((liverows / bs.m) * totalblocks + 0.5);
	else
		*tuples_out = 0;

	*sample_count_out = numrows;
}

/*
 * Sample rows for analyze, honoring fasttrun.max_analyze_pages: above the
 * threshold use bounded block sampling (estimated row count, O(sample) cost);
 * at or below it -- or when the threshold is 0 -- do the exact full scan.
 * Shared by the cold path, the delta-refresh, and the partial-index rescan so
 * the giant-temp guardrail covers all three scans, not just the cold one.
 */
static void
fasttrun_sample_for_analyze(Relation rel, BlockNumber pages_now,
							HeapTuple *sample, int sample_target,
							int64 *tuples_out, int *sample_count_out)
{
	if (fasttrun_max_analyze_pages > 0 &&
		pages_now > (BlockNumber) fasttrun_max_analyze_pages)
	{
		int		block_target = (sample_target > 0) ? sample_target : 3000;

		fasttrun_block_sample_rows(rel, tuples_out, sample,
								   block_target, sample_count_out);
	}
	else
		fasttrun_scan_with_sample(rel, tuples_out, sample,
								  sample_target, sample_count_out);
}

/*
 * Unlink every segment of a relation fork file.
 *
 * heap files are split into 1 GB segments with suffixes .1, .2, ...
 * starting from segment 1; segment 0 has no suffix.  We loop until the
 * next segment doesn't exist (ENOENT) and stop.
 *
 * On unlink failure for any other reason we ereport(WARNING) and bail
 * out -- the caller (fasttrun_smgr_bypass_truncate) treats partial
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
				return;	/* no more segments -- normal end */
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
 * Functionally equivalent to RelationTruncate(rel, 0) -- empties one
 * storage relation to zero blocks and discards local buffers -- but does NOT call
 * smgrtruncate() and therefore never reaches CacheInvalidateSmgr().
 * That removes the last shared-invalidation message that fasttruncate
 * was sending; the path is now literally zero sinval.
 *
 * How:
 *   1. Discover all forks that currently exist (main / fsm / vm).
 *   2. Drop their local buffers via DropRelationAllLocalBuffers (temp
 *      tables live in the local buffer pool, not shared).
 *   3. Physically unlink every segment of every fork via raw unlink()
 *      on the path returned by relpathbackend().
 *   4. Reset smgr_cached_nblocks (and smgr_targblock) to invalid so
 *      the next access reopens correctly.
 *   5. Recreate empty fork files via smgrcreate(), which has no
 *      built-in invalidation.
 *
 * The whole operation goes through public smgr.h / buf_internals.h /
 * relpath.h API -- no internal smgrsw[] poking.  GetRelationPath returns
 * a palloc'd char* in PG <18 and a stack-allocated RelPathStr in PG 18+,
 * so the wrapping is version-conditional.
 *
 * Caller has already taken AccessExclusiveLock and verified
 * relpersistence='t' + heap AM.
 */
static void
fasttrun_smgr_bypass_truncate(Relation rel, bool buffers_already_dropped)
{
	SMgrRelation			reln;
	RelFileLocatorBackend	rlocator;
	ForkNumber				forks[MAX_FORKNUM + 1];
	int						nforks = 0;
	int						i;

	/*
	 * Self-defense: this helper unlinks files via raw POSIX unlink() and
	 * uses DropRelationAllLocalBuffers, both of which only make sense on a
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

	/* 2. Drop local buffers unless already dropped for all opened relations. */
	if (!buffers_already_dropped)
		DropRelationAllLocalBuffers(rlocator.locator);

	/*
	 * 3. Release all cached file descriptors and per-fork state inside the
	 * SMgrRelation BEFORE we touch the files on disk.  Without this, the
	 * MdfdVec arrays inside md.c keep pointing at the inodes we are about
	 * to unlink, and the next mdcreate() would resize those arrays via
	 * repalloc() rather than rebuild them from scratch -- leaving stale fd
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
 * Phase 2 of the zero-sinval index rebuild: reconstruct one empty index
 * structure (metapage) via the AM's ambuild callback directly -- NOT
 * through index_build(), because index_build calls index_update_stats ->
 * systable_inplace_update_finish on pg_class, which generates shared
 * cache invalidation messages.  The index storage must already be
 * dropped and re-created empty: ambuild requires a zero-block relation.
 */
static BlockNumber
fasttrun_ambuild_empty_index(Relation heaprel, Relation indexrel)
{
	IndexInfo  *indexInfo = BuildDummyIndexInfo(indexrel);
	IndexBuildResult *result;
	BlockNumber pages;

	if (indexrel->rd_amcache != NULL)
	{
		pfree(indexrel->rd_amcache);
		indexrel->rd_amcache = NULL;
	}
	result = indexrel->rd_indam->ambuild(heaprel, indexrel, indexInfo);
	pfree(result);
	pfree(indexInfo);
	RelationGetSmgr(indexrel)->smgr_cached_nblocks[MAIN_FORKNUM] =
		InvalidBlockNumber;
	pages = RelationGetNumberOfBlocks(indexrel);
	return pages;
}

static void
fasttrun_truncate_one_storage(Relation rel, bool zero_sinval,
								 bool buffers_already_dropped)
{
	if (zero_sinval)
		fasttrun_smgr_bypass_truncate(rel, buffers_already_dropped);
	else
		RelationTruncate(rel, 0);
}

static FasttrunTruncateResultSlot *
fasttrun_truncate_slot(FasttrunTruncateOperation *operation, Oid relid)
{
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		if (operation->slots[i].relid == relid)
			return &operation->slots[i];
	}
	elog(ERROR, "fasttrun: relation %u was not prepared for truncate", relid);
	return NULL;
}

/* Open the complete relation set before any cache or storage side effect. */
static void
fasttrun_prepare_truncate_workset(Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	List	   *index_oids;
	ListCell   *lc;
	Oid			toastrelid;
	int			i = 0;

	memset(opened, 0, sizeof(*opened));
	if (heaprel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		heaprel->rd_rel->relhassubclass)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fasttruncate does not support partitioned or inheritance-parent temporary tables"),
				 errhint("Use core SQL TRUNCATE for inheritance or partition traversal.")));

	CheckTableNotInUse(heaprel, "fasttruncate");
	index_oids = RelationGetIndexList(heaprel);
	foreach(lc, index_oids)
	{
		Relation	indexrel = index_open(lfirst_oid(lc), AccessExclusiveLock);

		CheckTableNotInUse(indexrel, "fasttruncate");
		opened->user_indexes = lappend(opened->user_indexes, indexrel);
	}
	list_free(index_oids);

	toastrelid = heaprel->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		opened->toastrel = table_open(toastrelid, AccessExclusiveLock);
		CheckTableNotInUse(opened->toastrel, "fasttruncate");
		index_oids = RelationGetIndexList(opened->toastrel);
		foreach(lc, index_oids)
		{
			Relation	indexrel = index_open(lfirst_oid(lc),
										 AccessExclusiveLock);

			CheckTableNotInUse(indexrel, "fasttruncate");
			opened->toast_indexes = lappend(opened->toast_indexes, indexrel);
		}
		list_free(index_oids);
	}

	opened->nslots = list_length(opened->user_indexes) +
		list_length(opened->toast_indexes) +
		(opened->toastrel != NULL ? 1 : 0) + 1;
	opened->slot_relations = (Relation *)
		palloc(sizeof(Relation) * opened->nslots);
	foreach(lc, opened->user_indexes)
		opened->slot_relations[i++] = (Relation) lfirst(lc);
	foreach(lc, opened->toast_indexes)
		opened->slot_relations[i++] = (Relation) lfirst(lc);
	if (opened->toastrel != NULL)
		opened->slot_relations[i++] = opened->toastrel;
	opened->slot_relations[i++] = heaprel;
	Assert(i == opened->nslots);
}

static void
fasttrun_close_truncate_workset(FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;

	foreach(lc, opened->user_indexes)
		index_close((Relation) lfirst(lc), NoLock);
	list_free(opened->user_indexes);
	opened->user_indexes = NIL;
	foreach(lc, opened->toast_indexes)
		index_close((Relation) lfirst(lc), NoLock);
	list_free(opened->toast_indexes);
	opened->toast_indexes = NIL;
	if (opened->toastrel != NULL)
		table_close(opened->toastrel, NoLock);
	opened->toastrel = NULL;
	if (opened->slot_relations != NULL)
		pfree(opened->slot_relations);
	opened->slot_relations = NULL;
	opened->nslots = 0;
}

static FasttrunTruncateOperation *
fasttrun_allocate_truncate_operation(Relation heaprel,
									FasttrunOpenedWorkset *opened,
									uint64 generation)
{
	FasttrunTruncateOperation *operation;
	Size		size;
	int			i;
	int			toast_index_end;
	int			toast_heap_index = -1;
	RelFileLocatorBackend root_rlb;

	fasttrun_operation_init();
	size = offsetof(FasttrunTruncateOperation, slots) +
		(sizeof(FasttrunTruncateResultSlot) * opened->nslots);
	operation = (FasttrunTruncateOperation *)
		MemoryContextAllocZero(fasttrun_operation_mcxt, size);
	root_rlb.locator = heaprel->rd_locator;
	root_rlb.backend = heaprel->rd_backend;
	operation->root_relid = RelationGetRelid(heaprel);
	operation->root_rlb = root_rlb;
	operation->generation = generation;
	operation->phase = FASTTRUN_TRUNCATE_PREPARED;
	operation->nuser_indexes = list_length(opened->user_indexes);
	operation->ntoast_indexes = list_length(opened->toast_indexes);
	operation->nslots = opened->nslots;
	toast_index_end = operation->nuser_indexes + operation->ntoast_indexes;
	if (opened->toastrel != NULL)
		toast_heap_index = toast_index_end;

	for (i = 0; i < opened->nslots; i++)
	{
		Relation	rel = opened->slot_relations[i];
		FasttrunTruncateResultSlot *slot = &operation->slots[i];

		slot->relid = RelationGetRelid(rel);
		slot->root_relid = operation->root_relid;
		slot->rlb.locator = rel->rd_locator;
		slot->rlb.backend = rel->rd_backend;
		if (i < operation->nuser_indexes)
		{
			slot->kind = FASTTRUN_TRUNCATE_USER_INDEX;
			slot->heap_relid = operation->root_relid;
			slot->heap_rlb = operation->root_rlb;
		}
		else if (i < toast_index_end)
		{
			slot->kind = FASTTRUN_TRUNCATE_TOAST_INDEX;
			slot->heap_relid = RelationGetRelid(opened->toastrel);
			slot->heap_rlb.locator = opened->toastrel->rd_locator;
			slot->heap_rlb.backend = opened->toastrel->rd_backend;
		}
		else if (i == toast_heap_index)
		{
			slot->kind = FASTTRUN_TRUNCATE_TOAST_HEAP;
			slot->heap_relid = slot->relid;
			slot->heap_rlb = slot->rlb;
		}
		else
		{
			slot->kind = FASTTRUN_TRUNCATE_MAIN_HEAP;
			slot->heap_relid = slot->relid;
			slot->heap_rlb = slot->rlb;
		}
	}
	return operation;
}

/* Save complete undo and switch every result slot to neutral ownership. */
static void
fasttrun_reserve_empty_publication(FasttrunTruncateOperation *operation,
								   Relation heaprel)
{
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_enter(slot->relid);

		fasttrun_analyze_save_undo(entry);
		entry->state.has_relstats = false;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_pages = 0;
		slot->analyze_entry = entry;
		fasttrun_xact_mark_relid(slot->relid, operation->root_relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
	}
	fasttrun_xact_mark_relid(operation->root_relid, operation->root_relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	(void) fasttrun_stats_neutralize_relation(heaprel);
	operation->have_pgstat_seed = fasttrun_read_pgstat_counters(heaprel,
													&operation->seed_inserted,
													&operation->seed_updated,
													&operation->seed_deleted,
													&operation->seed_truncdropped);
	fasttrun_invalidate_local_plan_cache(operation->root_relid);
}

static void
fasttrun_execute_truncate_storage(FasttrunTruncateOperation *operation,
								  Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;
	int			ordinal = 0;
	int			toast_ordinal = 0;

	fasttrun_xact_mark_truncate(operation->root_relid,
								operation->root_relid,
								operation->generation,
								FASTTRUN_TOUCH_TRUNCATE_MUTATED);
	operation->phase = FASTTRUN_TRUNCATE_MUTATED;

	foreach(lc, opened->user_indexes)
	{
		fasttrun_truncate_one_storage((Relation) lfirst(lc),
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_user_index", ++ordinal);
	}
	foreach(lc, opened->toast_indexes)
	{
		fasttrun_truncate_one_storage((Relation) lfirst(lc),
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_toast_index", ++toast_ordinal);
	}
	if (opened->toastrel != NULL)
	{
		fasttrun_truncate_one_storage(opened->toastrel,
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_toast_heap", 0);
	}
	fasttrun_truncate_one_storage(heaprel, fasttrun_zero_sinval_truncate,
								 false);
	FASTTRUN_TEST_FAILPOINT("after_main_heap", 0);
}

static void
fasttrun_rebuild_truncate_indexes(FasttrunTruncateOperation *operation,
								  Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;
	int			ordinal = 0;

	/* TOAST structures are made usable before user-visible indexes. */
	foreach(lc, opened->toast_indexes)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		FasttrunTruncateResultSlot *slot;

		slot = fasttrun_truncate_slot(operation, RelationGetRelid(indexrel));
		slot->rebuilt_pages =
			fasttrun_ambuild_empty_index(opened->toastrel, indexrel);
		FASTTRUN_TEST_FAILPOINT("after_ambuild", ++ordinal);
	}
	foreach(lc, opened->user_indexes)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		FasttrunTruncateResultSlot *slot;

		slot = fasttrun_truncate_slot(operation, RelationGetRelid(indexrel));
		slot->rebuilt_pages = fasttrun_ambuild_empty_index(heaprel, indexrel);
		FASTTRUN_TEST_FAILPOINT("after_ambuild", ++ordinal);
	}
}

/* No allocation, hash growth, relcache open or pgstat read past this point. */
static void
fasttrun_publish_empty_workset(FasttrunTruncateOperation *operation,
								FasttrunOpenedWorkset *opened)
{
	SubTransactionId subid = GetCurrentSubTransactionId();
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry = slot->analyze_entry;
		Relation	rel = opened->slot_relations[i];

		Assert(entry != NULL);
		entry->state.has_relstats = true;
		entry->state.cached_locator = slot->rlb.locator;
		entry->state.cached_pages = slot->rebuilt_pages;
		entry->state.cached_tuples = 0;
		entry->state.cached_allvisible = 0;
		entry->state.probe_rlb = slot->rlb;
		entry->state.heap_relid = slot->heap_relid;
		entry->state.heap_rlb = slot->heap_rlb;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_pages = 0;
		entry->state.last_inval_tuples = 0;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->state_subid = subid;
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_pages = 0;

		if (slot->kind == FASTTRUN_TRUNCATE_MAIN_HEAP)
		{
			entry->state.last_inval_valid = true;
			if (operation->have_pgstat_seed)
			{
				fasttrun_cache_store_delta_state(entry,
											 operation->seed_inserted,
											 operation->seed_updated,
											 operation->seed_deleted,
											 operation->seed_truncdropped);
				entry->state.has_stats_baseline = true;
				entry->state.stats_baseline_inserted = operation->seed_inserted;
				entry->state.stats_baseline_updated = operation->seed_updated;
				entry->state.stats_baseline_deleted = operation->seed_deleted;
				entry->state.stats_baseline_truncdropped =
					operation->seed_truncdropped;
			}
		}

		rel->rd_rel->relpages = slot->rebuilt_pages;
		rel->rd_rel->reltuples = 0;
		rel->rd_rel->relallvisible = 0;
		slot->published = true;
		operation->published_slots++;
		FASTTRUN_TEST_FAILPOINT("after_publish", operation->published_slots);
	}
}

static void
fasttrun_clear_partial_publication(FasttrunTruncateOperation *operation)
{
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry;

		if (!slot->published)
			continue;
		entry = slot->analyze_entry;
		entry->state.has_relstats = false;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_pages = 0;
		slot->published = false;
	}
	operation->published_slots = 0;
}

/*
 * Zero-sinval truncate of a relation AND all its indexes and toast.
 *
 * The order must not change.  An empty heap must never be paired with an
 * index that still contains old TIDs, because after a refill those TIDs
 * could point to unrelated new rows.
 *
 *   1. Open every index, the toast table and its indexes BEFORE any
 *      destructive call.  A failure here leaves the table untouched.
 *   2. Phase 1: drop the storage of every index and toast (unlink +
 *      smgrcreate); the heap keeps its data.  A failure mid-way leaves
 *      the heap correct: untouched indexes stay valid, already-emptied
 *      ones fail loudly on their missing metapage.
 *   3. Truncate the heap -- the last destructive step; every index is
 *      already empty by now.
 *   4. Phase 2: run every ambuild to recreate the empty index
 *      structures (metapages).
 *
 * Phase 1 must finish before phase 2 starts.  Once all index storage has
 * gone, an ambuild failure leaves at worst an empty index without a
 * metapage, and any scan fails with an error.  Rebuilding each index
 * immediately would leave later indexes carrying old TIDs after a failure.
 */
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

	/* Fixed text (no OID) so regression tests can count calls. */
	elog(DEBUG1, "fasttrun: invalidating backend-local cached plans");

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
 *     not exist, the function silently does nothing -- this lets callers
 *     drop the usual `IF EXISTS` / pre-check pattern and simplifies the
 *     recovery logic of bulk PL/pgSQL routines.
 *   * Verifies the relation lives in a temp namespace; otherwise raises
 *     an error (this prevents accidentally truncating a regular table).
 *   * Opens the heap, its indexes, TOAST heap and TOAST indexes before
 *     changing cache or storage state.  Partition/inheritance traversal
 *     is deliberately unsupported.
 *   * Resets indexes before the heap.  The default path emits no shared
 *     invalidation; the fallback calls RelationTruncate once per relation
 *     and emits one SMGR message after each successful call.
 *   * An error before file changes rolls back normally.  After file changes
 *     start, access stays blocked until fasttruncate is retried successfully.
 *
 * IMPORTANT: this function is FOREIGN-KEY-UNSAFE.  fasttruncate does
 * NOT scan pg_constraint.  If you have FKs involving temp tables, use
 * SQL TRUNCATE instead.  Workloads this extension is built for never
 * declare FKs on temp tables -- we skip the check by design.
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
 * Warning: this function is NOT transaction-safe.  Storage reset cannot be
 * rolled back; after surrounding rollback the table remains physically
 * empty.  The session-local cache follows that physical truth.
 */
Datum
fasttruncate(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	RangeVar	   *relvar;
	Oid				relOid;
	Relation		rel;
	uint64			truncate_generation;
	FasttrunOpenedWorkset opened;
	FasttrunTruncateOperation *operation = NULL;
	FasttrunTruncateOperation *volatile cleanup_operation = NULL;
	volatile uint64 cleanup_generation = 0;
	volatile bool cleanup_pending_published = false;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessExclusiveLock, true);

	/*
	 * Missing table -- silently bail out (see function header).
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

	/* An active marker permits only this repair path or DROP/recreate. */
	fasttrun_poison_check_relation(rel, "fasttruncate", true);
	fasttrun_prepare_truncate_workset(rel, &opened);

	truncate_generation = ++fasttrun_truncate_generation;
	if (truncate_generation == 0)
		truncate_generation = ++fasttrun_truncate_generation;

	PG_TRY();
	{
		operation = fasttrun_allocate_truncate_operation(rel, &opened,
														 truncate_generation);
		cleanup_operation = operation;
		cleanup_generation = truncate_generation;
		fasttrun_xact_mark_truncate(relOid, relOid, truncate_generation,
									FASTTRUN_TOUCH_TRUNCATE_PREPARED);
		(void) fasttrun_poison_reserve(operation);
		cleanup_pending_published = true;
		FASTTRUN_TEST_FAILPOINT("after_prepare", 0);

		/* Reserve all memory and rollback state before changing files. */
		fasttrun_reserve_empty_publication(operation, rel);
		FASTTRUN_TEST_FAILPOINT("after_phase0", 0);

		fasttrun_execute_truncate_storage(operation, rel, &opened);
		fasttrun_rebuild_truncate_indexes(operation, rel, &opened);
		FASTTRUN_TEST_FAILPOINT("before_publish", 0);
		fasttrun_publish_empty_workset(operation, &opened);

		fasttrun_xact_mark_truncate(relOid, relOid, truncate_generation,
									FASTTRUN_TOUCH_TRUNCATE_COMPLETE);
		operation->phase = FASTTRUN_TRUNCATE_COMPLETE;
		fasttrun_poison_complete(operation);
		cleanup_operation = NULL;
		cleanup_generation = 0;
		cleanup_pending_published = false;
	}
	PG_CATCH();
	{
		FasttrunTruncateOperation *failed =
			(FasttrunTruncateOperation *) cleanup_operation;

		if (failed != NULL && failed->generation == cleanup_generation)
		{
			if (failed->phase == FASTTRUN_TRUNCATE_PREPARED)
			{
				if (cleanup_pending_published)
					fasttrun_poison_cancel_pending(failed);
				else
				{
					pfree(failed);
					fasttrun_operation_reset_if_empty();
				}
			}
			else
			{
				fasttrun_clear_partial_publication(failed);
				Assert(cleanup_pending_published);
				fasttrun_poison_activate(failed);
			}
		}
		else
			fasttrun_operation_reset_if_empty();

		fasttrun_close_truncate_workset(&opened);
		table_close(rel, NoLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Release the AccessExclusiveLock taken by RangeVarGetRelid. */
	fasttrun_close_truncate_workset(&opened);
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
 * correlation -- same quality as regular ANALYZE.
 */
/*
 * Workhorse for fasttrun_analyze() and fasttrun_analyze_bulk().
 *
 * Runs the full analyze pipeline on an already-opened Relation:
 * delta-hit cache, refresh-check, cold scan, index relstats update,
 * rd_rel reinjection.  Emits fasttrun_invalidate_local_plan_cache()
 * internally when relstats drift crosses fasttrun.invalidate_threshold.
 *
 * Firing the invalidation BEFORE the final rd_rel mutation is
 * load-bearing.  The invalidation goes through
 * RelationCacheInvalidateEntry().  That rebuilds the relcache entry from
 * pg_class while we still hold AccessShareLock.  We then re-mutate
 * rd_rel afterwards, and the planner observes our cached values -- not
 * the stale 0/0 from pg_class.  Move the invalidate past the mutation
 * (or past table_close) and our values get silently wiped.
 *
 * Caller is responsible for:
 *   - resolving the RangeVar to relOid + table_open(NoLock);
 *   - verifying relpersistence == RELPERSISTENCE_TEMP, isTempNamespace,
 *     heap-AM;
 *   - recording relOid in the current transaction frame;
 *   - table_close(rel, AccessShareLock) afterwards.
 *
 * fasttrun_analyze_bulk() invokes this once per relation in its array.
 * Each invalidation still makes PG's PlanCacheRelCallback walk the whole
 * cached-plan list; the saving is per-plan, not per-walk -- a plan an
 * earlier message already marked is_valid=false is skipped cheaply rather
 * than re-marked.
 */
static void
fasttrun_analyze_relation(Relation rel)
{
	Oid				relOid = RelationGetRelid(rel);
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
	bool			partial_index_sampled = false;
	bool			pages_now_known = false;
	bool			relation_policy_changed;
	BlockNumber		allvisible_now = 0;
	BlockNumber		old_pages;
	int32			old_allvisible;
	float4			old_tuples;
	int64			delta_ins = 0;
	int64			delta_upd = 0;
	int64			delta_del = 0;
	FasttrunAnalyzeCacheEntry *entry = NULL;

	relation_policy_changed = fasttrun_stats_set_relation_policy(rel,
													FASTTRUN_REL_LOCAL_NEUTRAL);
	if (relation_policy_changed && fasttrun_relation_has_core_stats(rel))
		stats_visibility_changed = true;
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
			&& entry->state.has_delta_state
			&& fasttrun_relation_has_same_locator(rel, entry)
			&& entry->state.cached_truncdropped == truncdropped_now)
		{
			int64	new_tuples;

			delta_ins = ins_now - entry->state.cached_inserted;
			delta_upd = upd_now - entry->state.cached_updated;
			delta_del = del_now - entry->state.cached_deleted;
			new_tuples = entry->state.cached_tuples + delta_ins - delta_del;

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
					pages_now = entry->state.cached_pages;
					pages_now_known = true;
					tuples_count = new_tuples;
					scan_needed = false;
				}
				else
				{
					pages_now = RelationGetNumberOfBlocks(rel);
					pages_now_known = true;
					if (pages_now >= entry->state.cached_pages)
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
		entry != NULL && entry->state.has_stats_baseline)
	{
		int64		churn;
		double		baseline;

		/* Compute churn BEFORE advancing baseline. */
		churn = (ins_now - entry->state.stats_baseline_inserted)
			+ (upd_now - entry->state.stats_baseline_updated)
			+ (del_now - entry->state.stats_baseline_deleted);
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
					fasttrun_sample_for_analyze(rel, pages_now, sample,
												sample_target, &scanned_tuples,
												&sample_count);
					tuples_count = scanned_tuples;

					if (sample_count > 0)
					{
						FasttrunCollectResult collect_result;

						collect_result = fasttrun_collect_and_store(rel, sample,
																 sample_count,
																 tuples_count,
																 tuples_count > sample_target);
						stats_recollected |= collect_result.published_any;
						stats_visibility_changed |= collect_result.became_neutral;
					}
					else
					{
						if (fasttrun_stats_cache_evict_relid(relOid))
							stats_visibility_changed = true;
						index_relstats_changed |=
							fasttrun_update_index_relstats(rel, NIL, NULL, 0,
														   tuples_count, true);
					}

					for (i = 0; i < sample_count; i++)
						heap_freetuple(sample[i]);
					pfree(sample);
				}
			}

			/*
			 * Sub-threshold churn leaves the baseline in place so freshness
			 * keeps hiding the now-stale distribution from new plans.  A
			 * cached plan built on the fresh distribution is NOT invalidated
			 * here: a below-threshold shift rarely flips a plan, and forcing
			 * a replan would only drop it to default selectivity.
			 */
			if (stats_recollected || tuples_count == 0)
				fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
												  del_now, truncdropped_now);
		}
	}

	if (!scan_needed && !stats_recollected)
	{
		int64		churn = 0;
		bool		probe_partial;
		bool		trailing_refresh = !pure_delta_noop;

		if (have_counters && fasttrun_sample_rows != 0 &&
			entry != NULL && entry->state.has_stats_baseline)
		{
			/*
			 * Churn is measured from the rescan anchor (the counters at the
			 * last partial-index sync), not from the stats baseline: the
			 * baseline stays put on sub-threshold churn, and measuring
			 * against it would re-sample the partial indexes on every
			 * analyze after a single small DML.
			 */
			if (entry->state.partial_scan_valid)
				churn = (ins_now - entry->state.partial_scan_inserted)
					+ (upd_now - entry->state.partial_scan_updated)
					+ (del_now - entry->state.partial_scan_deleted);
			else
				churn = (ins_now - entry->state.stats_baseline_inserted)
					+ (upd_now - entry->state.stats_baseline_updated)
					+ (del_now - entry->state.stats_baseline_deleted);
			if (churn < 0)
				churn = -churn;
		}
		probe_partial = (churn > 0);

		if (probe_partial || trailing_refresh)
		{
			/* One open per index serves the probe and both refreshes. */
			List	   *index_rels = fasttrun_open_index_rels(rel);

			if (probe_partial)
			{
				/*
				 * Rescan on any new churn when a partial index exists.  A
				 * small DML on the predicate column can change the partial
				 * index's covered tuple count out of proportion to the heap
				 * churn ratio (e.g. flipping a boolean flag on 1% of rows can
				 * double the index), so the heap ratio cannot gate this
				 * rescan.
				 */
				if (fasttrun_index_rels_have_partial(index_rels))
				{
					int			sample_target = fasttrun_effective_sample_target(rel);

					if (sample_target > 0)
					{
						HeapTuple  *sample;
						int64		scanned_tuples = 0;
						int			sample_count = 0;
						int			i;

						sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
						fasttrun_sample_for_analyze(rel, pages_now, sample,
													sample_target, &scanned_tuples,
													&sample_count);
						tuples_count = scanned_tuples;
						index_relstats_changed |=
							fasttrun_update_index_relstats(rel, index_rels,
															   sample, sample_count,
															   tuples_count, true);
						partial_index_sampled = true;
						fasttrun_cache_set_partial_scan_anchor(entry, ins_now,
															   upd_now, del_now);

						for (i = 0; i < sample_count; i++)
							heap_freetuple(sample[i]);
						pfree(sample);
					}
					/*
					 * sample_target <= 0: a rescan is impossible right now.
					 * Leave the anchor alone so a later call (e.g. after the
					 * sampling GUCs change) still sees this churn.
					 */
				}
				else
				{
					/* No partial indexes: nothing to sync, anchor the probe. */
					fasttrun_cache_set_partial_scan_anchor(entry, ins_now,
														   upd_now, del_now);
				}
			}

			/*
			 * A trailing sample-less refresh keeps non-partial index relpages
			 * in step with the heap.  Pass partial_upper_bound=false when a
			 * sample-based rescan above already set the partial index tuple
			 * counts, so this call does not overwrite those estimates with the
			 * totalrows upper bound.
			 */
			if (trailing_refresh)
				index_relstats_changed |=
					fasttrun_update_index_relstats(rel, index_rels, NULL, 0,
												   tuples_count,
												   !partial_index_sampled);

			fasttrun_close_index_rels(index_rels);
		}
	}

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
			/*
			 * Giant temp table: block-sample above max_analyze_pages (estimated
			 * row count, O(sample)); exact full scan at or below.  Same helper
			 * gates the delta-refresh and partial-index rescans.
			 */
			fasttrun_sample_for_analyze(rel, pages_now, sample, sample_target,
										&tuples_count, &sample_count);
		}

		if (sample != NULL && sample_count > 0)
		{
			FasttrunCollectResult collect_result;

			/*
			 * Sort needed only when reservoir sampling displaced entries.
			 * published_any stays false when track_counts is off; then the
			 * !stats_recollected path below still refreshes index relstats.
			 * became_neutral rides the same single plan-invalidation site.
			 */
			collect_result = fasttrun_collect_and_store(rel, sample,
															  sample_count,
															  tuples_count,
															  tuples_count > sample_target);
			stats_recollected = collect_result.published_any;
			stats_visibility_changed |= collect_result.became_neutral;
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
				fasttrun_update_index_relstats(rel, NIL, NULL, 0, tuples_count,
										   true);

		/* Populate stats baseline only when pgstat is usable. */
		if (have_counters)
			fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
											  del_now, truncdropped_now);
	}

	if (pure_delta_noop && entry != NULL && entry->state.has_relstats)
		allvisible_now = entry->state.cached_allvisible;
	else
		allvisible_now = fasttrun_count_allvisible(rel);

	if (!(pure_delta_noop && entry != NULL && entry->state.has_relstats))
	{
		entry = fasttrun_cache_store_relstats(rel, pages_now, tuples_count,
											 allvisible_now);
		if (have_counters)
			fasttrun_cache_store_delta_state(entry, ins_now, upd_now, del_now,
											 truncdropped_now);
	}

	/*
	 * Column stats that served plans at the last analyze but are hidden by
	 * the freshness gate now (e.g. a near-unique column whose scaled
	 * tolerance sits below the recollect threshold) force invalidation:
	 * new plans already see defaults, so a cached plan on the stale
	 * distribution must not outlive the flip.  Runs against the state just
	 * published above -- exactly what the planner hook will see next.
	 */
	if (fasttrun_stats_note_visibility(relOid, have_counters, ins_now,
									   upd_now, del_now, truncdropped_now,
									   pages_now))
		stats_visibility_changed = true;

	need_plan_inval = (old_pages != pages_now ||
					   old_tuples != (float4) tuples_count ||
					   old_allvisible != (int32) allvisible_now ||
					   stats_recollected ||
					   stats_visibility_changed ||
					   index_relstats_changed);

	/*
	 * Below-threshold relstats drift does NOT invalidate cached plans.
	 * A few extra rows rarely flip the optimizer's choice.  And walking
	 * the entire plan_cache once per analyze dominates xact CPU on
	 * backends with large SPI/PREPARE caches.
	 *
	 * Drift is measured against the values published at the LAST actual
	 * invalidation -- the values the surviving plans were built on -- not
	 * against the previous call.  Re-anchoring on every call would let a
	 * series of sub-threshold steps accumulate unbounded drift without a
	 * single invalidation.
	 *
	 * Column-stats refresh, stats-visibility flip, and index relstats
	 * change always invalidate.  They signal distribution changes that
	 * the per-row ratio here cannot measure.
	 */
	if (need_plan_inval && fasttrun_invalidate_threshold > 0.0 &&
		!stats_recollected && !stats_visibility_changed &&
		!index_relstats_changed)
	{
		BlockNumber	anchor_pages = old_pages;
		float4		anchor_tuples = old_tuples;
		double		baseline_tuples;
		double		ratio_tuples;
		double		ratio_pages;
		double		worst;

		if (entry != NULL && entry->state.last_inval_valid)
		{
			anchor_pages = entry->state.last_inval_pages;
			anchor_tuples = entry->state.last_inval_tuples;
		}

		baseline_tuples = (anchor_tuples > 0.0f)
			? (double) anchor_tuples : 1.0;
		ratio_tuples =
			fabs((double) tuples_count - (double) anchor_tuples) / baseline_tuples;
		ratio_pages = (anchor_pages > 0)
			? fabs((double) pages_now - (double) anchor_pages) / (double) anchor_pages
			: (pages_now > 0 ? 1.0 : 0.0);
		worst = Max(ratio_tuples, ratio_pages);

		if (worst < fasttrun_invalidate_threshold)
			need_plan_inval = false;
	}

	if (need_plan_inval)
	{
		fasttrun_invalidate_local_plan_cache(relOid);
		if (entry != NULL)
		{
			entry->state.last_inval_pages = pages_now;
			entry->state.last_inval_tuples = (float4) tuples_count;
			entry->state.last_inval_valid = true;
		}
	}

	rel->rd_rel->relpages = pages_now;
	rel->rd_rel->reltuples = (float4) tuples_count;
	rel->rd_rel->relallvisible = (int32) allvisible_now;
}

/*
 * fasttrun_analyze(text) -- a thin wrapper around
 * fasttrun_analyze_relation().  It resolves the relation name, takes
 * AccessShareLock, runs the helper, and emits any resulting plan-cache
 * invalidation.
 *
 * For the function-level contract -- silent return on missing relation,
 * ERROR on non-temp or non-heap, what gets cached, when sinval fires --
 * see the comment block above fasttrun_analyze_relation() and the
 * section "Lazy-mode cache for fasttrun_analyze" near the top of this
 * file.
 */
Datum
fasttrun_analyze(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;

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
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_analyze: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}
	fasttrun_poison_check_relation(rel, "fasttrun_analyze", false);

	fasttrun_xact_mark_relid(relOid, relOid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	fasttrun_analyze_relation(rel);
	table_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * fasttrun_analyze_bulk(VARIADIC text[])
 *
 * Run fasttrun_analyze logic over a batch of temp relations in one call.
 * Per-relation behaviour matches fasttrun_analyze() exactly.  One
 * performance note: plan-cache invalidations fire per-relation as the
 * batch progresses.  PG's PlanCacheRelCallback walks the whole cached-plan
 * list once per message, so N invalidations are still N walks.  The only
 * saving is per-plan: a plan an earlier message already marked
 * is_valid=false is skipped cheaply instead of re-marked.  That trims the
 * per-walk body on backends with large SPI/PREPARE caches, not the number
 * of walks.
 *
 * NULL array elements are silently skipped.  Missing relations are
 * silently skipped -- same contract as fasttrun_analyze.  Non-temp or
 * non-heap relations raise an error -- same contract.  The batch aborts
 * at the first failure.  Relations analyzed before the failure keep
 * their refreshed cache state -- per-xact rollback semantics kick in
 * via the surrounding xact or savepoint.
 */
Datum
fasttrun_analyze_bulk(PG_FUNCTION_ARGS)
{
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();

	arr = PG_GETARG_ARRAYTYPE_P(0);
	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);
	if (nelems == 0)
		PG_RETURN_VOID();

	for (i = 0; i < nelems; i++)
	{
		text	   *name;
		RangeVar   *relvar;
		Oid			relOid;
		Relation	rel;

		if (nulls[i])
			continue;

		name = DatumGetTextPP(elems[i]);
		relvar = fasttrun_make_rangevar(name);
		relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
		if (!OidIsValid(relOid))
			continue;

		rel = table_open(relOid, NoLock);

		if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
			!isTempNamespace(RelationGetNamespace(rel)))
		{
			table_close(rel, NoLock);
			elog(ERROR, "fasttrun_analyze_bulk: relation \"%s\" "
						"is not a local temporary table",
				 RelationGetRelationName(rel));
		}
		if (rel->rd_tableam != GetHeapamTableAmRoutine())
		{
			table_close(rel, NoLock);
			elog(ERROR, "fasttrun_analyze_bulk: relation \"%s\" is not heap-AM",
				 RelationGetRelationName(rel));
		}
		fasttrun_poison_check_relation(rel, "fasttrun_analyze_bulk", false);

		fasttrun_xact_mark_relid(relOid, relOid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_STATS |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
		fasttrun_analyze_relation(rel);
		table_close(rel, AccessShareLock);
	}

	/*
	 * The helper emits per-relid fasttrun_invalidate_local_plan_cache()
	 * inline.  It has to fire before the rd_rel mutation -- see the
	 * comment above fasttrun_analyze_relation().  Each call still walks the
	 * whole cached-plan list via PG's PlanCacheRelCallback; a plan already
	 * marked is_valid=false by an earlier call is skipped cheaply, so the
	 * batch trims per-plan work, not the walk count.  The rest is just a
	 * convenience wrapper.
	 */
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
	fasttrun_poison_check_relation(rel, "fasttrun_relstats", false);

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
 * Explicit-call sample -> per-column n_distinct/null_frac/width ->
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
	bool		relation_policy_changed;
	bool		planner_fallback_changed;
	FasttrunCollectResult collect_result = {false, false};

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
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_collect_stats: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}
	fasttrun_poison_check_relation(rel, "fasttrun_collect_stats", false);
	relation_policy_changed = fasttrun_stats_set_relation_policy(rel,
													FASTTRUN_REL_LOCAL_NEUTRAL);
	planner_fallback_changed = relation_policy_changed &&
		fasttrun_relation_has_core_stats(rel);

	/* Register for per-xact bookkeeping -- callbacks walk this list only. */
	fasttrun_xact_mark_relid(relOid, relOid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);

	{
		sample_target = fasttrun_effective_sample_target(rel);

		if (sample_target <= 0)
		{
			if (planner_fallback_changed)
				fasttrun_invalidate_local_plan_cache(relOid);
			table_close(rel, AccessShareLock);
			PG_RETURN_VOID();
		}

		sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);

		fasttrun_scan_with_sample(rel, &tuples_count,
								  sample, sample_target, &sample_count);
	}

	if (sample_count > 0)
	{
		collect_result = fasttrun_collect_and_store(rel, sample, sample_count,
														 tuples_count,
														 tuples_count > sample_target);
	}
	else
	{
		collect_result.became_neutral =
			fasttrun_stats_cache_evict_relid(relOid);
		(void) fasttrun_update_index_relstats(rel, NIL, NULL, 0,
										  tuples_count, true);
	}
	if (planner_fallback_changed || collect_result.published_any ||
		collect_result.became_neutral)
		fasttrun_invalidate_local_plan_cache(relOid);

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
	 * machinery stays disengaged for it -- that's the documented
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
 * named table -- in the **same row shape** as core pg_statistic, so
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
 *     call -- one per analysed column -- so building a tuplestore is
 *     simpler than per-call state and almost free).
 *   * The cached statsTuples are already in pg_statistic shape (built
 *     by fasttrun_build_pg_statistic_tuple from VacAttrStats), so we
 *     just stuff each one straight into the tuplestore.
 *   * If the table doesn't exist, isn't temp, or has no cached stats
 *     yet, the function returns an empty set -- same defensive
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
	Relation		rel;
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

	/* Resolve relation; missing -> empty result. */
	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
	if (!OidIsValid(relOid))
		return (Datum) 0;

	rel = relation_open(relOid, NoLock);
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		relation_close(rel, NoLock);
		elog(ERROR, "fasttrun_inspect_stats: relation is not a temporary table");
	}
	fasttrun_poison_check_relation(rel, "fasttrun_inspect_stats", false);
	relation_close(rel, AccessShareLock);

	/* Empty cache -> empty result. */
	if (fasttrun_stats_cache == NULL)
		return (Datum) 0;
	if (!fasttrun_stats_relid_exists(relOid))
		return (Datum) 0;

	hash_seq_init(&status, fasttrun_stats_cache);
	while ((entry = (FasttrunStatsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.relid != relOid)
			continue;
		if (!fasttrun_stats_entry_is_candidate(entry))
			continue;
		tuplestore_puttuple(tupstore, entry->statsTuple);
	}

	return (Datum) 0;
}

/* ================================================================
 * Temp table creation tracking -- shared memory + ProcessUtility hook.
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

typedef struct FasttrunTrackSnapshot
{
	FasttrunTrackEntry *entries;
	int			count;
} FasttrunTrackSnapshot;

static HTAB			   *fasttrun_track_htab = NULL;
static LWLockId			fasttrun_track_lock;
static bool				fasttrun_track_enabled = true;
static int				fasttrun_prewarm_count = 1000;
/*
 * Re-entrancy guard: true while fasttrun_prewarm() runs its SPI create loop.
 * The CREATE TEMP TABLE (LIKE dummy.*) issued by create_temp_table re-enters
 * fasttrun_utility_hook; without this guard prewarm would count its own
 * creates as user creates and inflate its own top-N ranking.
 */
static bool				fasttrun_in_prewarm = false;
static char			   *fasttrun_prewarm_schema = "dummy_tmp";
static char			   *fasttrun_track_schedule = "mon-fri 08:00-18:00";
static ProcessUtility_hook_type prev_utility_hook = NULL;
static object_access_hook_type prev_object_access_hook = NULL;
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
		return true;				/* tz error -- fall back to active */
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

/* Shmem request hook -- called during postmaster startup. */
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
	/* placeholder for count -- will rewrite */
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
		return;		/* no saved stats -- fresh start */

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
	HeapTuple	tp;
	Form_pg_class relform;
	bool		is_our_temp;

	if (!OidIsValid(relid))
		return;

	/*
	 * Filter with one syscache probe BEFORE taking any lock.  The hook runs
	 * for every DDL target, and waiting on somebody else's lock here would
	 * defeat conditional-locking callers such as VACUUM (SKIP_LOCKED).
	 * Only this backend's own temp relations proceed to the open below,
	 * where the AccessShareLock is uncontended.
	 */
	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
	{
		fasttrun_cache_mark_evicted(relid);
		fasttrun_stats_cache_mark_evicted_relid(relid);
		fasttrun_invalidate_local_plan_cache(relid);
		return;
	}
	relform = (Form_pg_class) GETSTRUCT(tp);
	is_our_temp = (relform->relpersistence == RELPERSISTENCE_TEMP &&
				   isTempNamespace(relform->relnamespace));
	ReleaseSysCache(tp);
	if (!is_our_temp)
		return;

	/*
	 * relation_open with NoLock would trip the lock-discipline assert on
	 * assert-enabled builds (the DDL has not taken its own lock yet), so
	 * take a transient AccessShareLock and release it at close.
	 */
	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
	{
		fasttrun_cache_mark_evicted(relid);
		fasttrun_stats_cache_mark_evicted_relid(relid);
		fasttrun_invalidate_local_plan_cache(relid);
		return;
	}

	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	(void) fasttrun_stats_neutralize_relation(rel);

	if (rel->rd_rel->relkind == RELKIND_RELATION ||
		rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *lc;

		foreach(lc, index_oids)
			fasttrun_xact_mark_relid(lfirst_oid(lc), relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
		list_free(index_oids);

		fasttrun_cache_mark_rel_and_indexes_evicted(rel);
	}
	else
		fasttrun_cache_mark_evicted(relid);
	relation_close(rel, AccessShareLock);
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
	fasttrun_poison_clear_all();
	fasttrun_cache_reset();
	fasttrun_stats_cache_reset();
}

static void
fasttrun_poison_check_relid(Oid relid, const char *operation)
{
	Oid			root_relid = relid;
	FasttrunPoisonEntry *entry;
	Relation	rel;

	if (fasttrun_poison_cache == NULL || !OidIsValid(relid))
		return;
	entry = fasttrun_poison_find(root_relid);
	if (entry == NULL && get_rel_relkind(relid) == RELKIND_INDEX)
	{
		root_relid = IndexGetRelation(relid, true);
		entry = fasttrun_poison_find(root_relid);
	}
	if (entry == NULL || !entry->active)
		return;
	rel = try_relation_open(root_relid, AccessShareLock);
	if (rel == NULL)
		return;
	fasttrun_poison_check_relation(rel, operation, false);
	relation_close(rel, AccessShareLock);
}

static void
fasttrun_poison_check_rangevar(RangeVar *rv, const char *operation)
{
	Oid			relid;

	if (fasttrun_poison_cache == NULL || rv == NULL)
		return;
	relid = RangeVarGetRelid(rv, NoLock, true);
	fasttrun_poison_check_relid(relid, operation);
}

static void
fasttrun_poison_check_any(const char *operation)
{
	HASH_SEQ_STATUS status;
	FasttrunPoisonEntry *entry;
	Oid			root_relid = InvalidOid;

	if (fasttrun_poison_cache == NULL)
		return;
	hash_seq_init(&status, fasttrun_poison_cache);
	while ((entry = (FasttrunPoisonEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->active)
		{
			root_relid = entry->root_relid;
			break;
		}
	}
	if (OidIsValid(root_relid))
		fasttrun_poison_check_relid(root_relid, operation);
}

/* Reject direct utility access before it can mutate or inspect storage. */
static void
fasttrun_poison_check_utility(Node *parsetree)
{
	ListCell   *lc;

	if (fasttrun_poison_cache == NULL || parsetree == NULL)
		return;
	switch (nodeTag(parsetree))
	{
		case T_CopyStmt:
			fasttrun_poison_check_rangevar(((CopyStmt *) parsetree)->relation,
										 "COPY");
			break;
		case T_TruncateStmt:
			foreach(lc, ((TruncateStmt *) parsetree)->relations)
				fasttrun_poison_check_rangevar((RangeVar *) lfirst(lc),
											 "SQL TRUNCATE");
			break;
		case T_VacuumStmt:
			if (((VacuumStmt *) parsetree)->rels == NIL)
				fasttrun_poison_check_any("VACUUM/ANALYZE");
			else
			{
				foreach(lc, ((VacuumStmt *) parsetree)->rels)
				{
					VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);

					if (OidIsValid(vrel->oid))
						fasttrun_poison_check_relid(vrel->oid,
												 "VACUUM/ANALYZE");
					else
						fasttrun_poison_check_rangevar(vrel->relation,
													 "VACUUM/ANALYZE");
				}
			}
			break;
		case T_ClusterStmt:
			if (((ClusterStmt *) parsetree)->relation == NULL)
				fasttrun_poison_check_any("CLUSTER");
			else
				fasttrun_poison_check_rangevar(
					((ClusterStmt *) parsetree)->relation, "CLUSTER");
			break;
		case T_AlterTableStmt:
			fasttrun_poison_check_rangevar(
				((AlterTableStmt *) parsetree)->relation, "ALTER TABLE");
			break;
		case T_IndexStmt:
			fasttrun_poison_check_rangevar(
				((IndexStmt *) parsetree)->relation, "CREATE INDEX");
			break;
		case T_ReindexStmt:
			if (((ReindexStmt *) parsetree)->relation == NULL)
				fasttrun_poison_check_any("REINDEX");
			else
				fasttrun_poison_check_rangevar(
					((ReindexStmt *) parsetree)->relation, "REINDEX");
			break;
		default:
			break;
	}
}

typedef struct FasttrunOidSetEntry
{
	Oid			relid;			/* hash key -- must be first */
} FasttrunOidSetEntry;

static bool
fasttrun_manages_relid(Oid relid)
{
	return fasttrun_cache_lookup(relid) != NULL ||
		fasttrun_stats_relid_exists(relid);
}

/*
 * Snapshot the union of analyze-cache owning heaps and stats-cache relids.
 * ANALYZE/CLUSTER without an explicit relation list must not scan unrelated
 * catalogs just to discover which tables this backend manages.
 */
static List *
fasttrun_managed_temp_relids(void)
{
	HASHCTL		ctl;
	HTAB	   *set;
	HASH_SEQ_STATUS status;
	FasttrunOidSetEntry *setentry;
	List	   *relids = NIL;
	long		initial_size = 16;

	if (fasttrun_analyze_cache != NULL)
		initial_size += hash_get_num_entries(fasttrun_analyze_cache);
	if (fasttrun_stats_relid_cache != NULL)
		initial_size += hash_get_num_entries(fasttrun_stats_relid_cache);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FasttrunOidSetEntry);
	ctl.hcxt = CurrentMemoryContext;
	set = hash_create("fasttrun utility managed relids", initial_size,
					  &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	if (fasttrun_stats_relid_cache != NULL)
	{
		FasttrunStatsRelidEntry *entry;

		hash_seq_init(&status, fasttrun_stats_relid_cache);
		while ((entry = (FasttrunStatsRelidEntry *)
				hash_seq_search(&status)) != NULL)
			(void) hash_search(set, &entry->relid, HASH_ENTER, NULL);
	}

	if (fasttrun_analyze_cache != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry;

		hash_seq_init(&status, fasttrun_analyze_cache);
		while ((entry = (FasttrunAnalyzeCacheEntry *)
				hash_seq_search(&status)) != NULL)
		{
			Oid			root_relid = OidIsValid(entry->state.heap_relid) ?
				entry->state.heap_relid : entry->relid;

			(void) hash_search(set, &root_relid, HASH_ENTER, NULL);
		}
	}

	hash_seq_init(&status, set);
	while ((setentry = (FasttrunOidSetEntry *) hash_seq_search(&status)) != NULL)
		relids = lappend_oid(relids, setentry->relid);
	hash_destroy(set);
	return relids;
}

static Relation
fasttrun_try_open_managed_heap(Oid relid)
{
	Relation	rel;

	if (!OidIsValid(relid))
		return NULL;
	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
		return NULL;
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)) ||
		rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		relation_close(rel, AccessShareLock);
		return NULL;
	}
	return rel;
}

static bool
fasttrun_vacuum_option_enabled(VacuumStmt *stmt, const char *name)
{
	ListCell   *lc;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, name) == 0)
			return defGetBoolean(opt);
	}
	return false;
}

static bool
fasttrun_attribute_target_is_analyzable(Relation rel, AttrNumber attnum)
{
	Form_pg_attribute attr;

	if (attnum <= 0 || attnum > RelationGetDescr(rel)->natts)
		return false;
	attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	if (!fasttrun_attribute_is_analyzable(attr))
		return false;
	return fasttrun_get_attstattarget(rel, attnum) != 0;
}

/*
 * A non-NULL bitmap denotes selected columns.  Member zero distinguishes a
 * valid list without analyzable columns from a request for all columns.  An
 * invalid name returns NULL deliberately: core will report the error, and if
 * VACUUM already committed an earlier table, its statistics remain hidden.
 */
static Bitmapset *
fasttrun_analyze_target_attnums(Relation rel, List *va_cols)
{
	Bitmapset  *attnums;
	ListCell   *lc;

	if (va_cols == NIL)
		return NULL;
	attnums = bms_make_singleton(0);
	foreach(lc, va_cols)
	{
		const char *attname = strVal(lfirst(lc));
		AttrNumber	attnum = get_attnum(RelationGetRelid(rel), attname);

		if (attnum <= 0 || attnum > RelationGetDescr(rel)->natts)
		{
			bms_free(attnums);
			return NULL;
		}
		if (fasttrun_attribute_target_is_analyzable(rel, attnum))
			attnums = bms_add_member(attnums, attnum);
	}
	return attnums;
}

static void
fasttrun_stats_neutralize_target(Relation rel, Bitmapset *attnums)
{
	int			attnum = -1;

	if (attnums == NULL)
	{
		(void) fasttrun_stats_neutralize_relation(rel);
		return;
	}
	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
	{
		if (attnum == 0)
			continue;
		(void) fasttrun_stats_set_column_state(rel, attnum, false,
										 FASTTRUN_COLUMN_LOCAL_NEUTRAL,
										 NULL, NULL);
	}
}

/* Relstats written by core ANALYZE/rewrite supersede the session snapshot. */
static void
fasttrun_prepare_relstats_handoff(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	List	   *index_oids;
	ListCell   *lc;

	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
		fasttrun_xact_mark_relid(lfirst_oid(lc), relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
	list_free(index_oids);
	fasttrun_cache_mark_rel_and_indexes_evicted(rel);
}

static void
fasttrun_prepare_relation_handoff(Relation rel, Bitmapset *attnums,
								  bool do_analyze, bool do_rewrite,
								  List **analyze_targets,
								  List **rewrite_relids)
{
	Oid			relid = RelationGetRelid(rel);

	Assert(!do_analyze || analyze_targets != NULL);
	Assert(rewrite_relids != NULL);

	if (do_rewrite)
		(void) fasttrun_stats_neutralize_relation(rel);
	else if (do_analyze)
		fasttrun_stats_neutralize_target(rel, attnums);
	fasttrun_prepare_relstats_handoff(rel);

	if (do_analyze)
	{
		FasttrunAnalyzeHandoffTarget *target;

		target = (FasttrunAnalyzeHandoffTarget *) palloc(sizeof(*target));
		target->relid = relid;
		target->attnums = attnums;
		*analyze_targets = lappend(*analyze_targets, target);
	}
	if (do_rewrite && !list_member_oid(*rewrite_relids, relid))
		*rewrite_relids = lappend_oid(*rewrite_relids, relid);
}

static void
fasttrun_prepare_vacuum_handoff(VacuumStmt *stmt,
								List **analyze_targets,
								List **rewrite_relids)
{
	bool		do_analyze = !stmt->is_vacuumcmd ||
		fasttrun_vacuum_option_enabled(stmt, "analyze");
	bool		do_rewrite = stmt->is_vacuumcmd &&
		fasttrun_vacuum_option_enabled(stmt, "full");
	ListCell   *lc;

	/* Plain VACUUM changes neither distribution ownership nor local state. */
	if (!do_analyze && !do_rewrite)
		return;

	/*
	 * Core VACUUM may commit between tables.  Hide local statistics for every
	 * target before calling core, and allow core statistics only after the
	 * whole command succeeds.  If a later table fails, statistics for earlier
	 * tables stay hidden instead of mixing old and partially refreshed values.
	 */

	if (stmt->rels == NIL)
	{
		List	   *relids = fasttrun_managed_temp_relids();

		foreach(lc, relids)
		{
			Oid			relid = lfirst_oid(lc);
			Relation	rel = fasttrun_try_open_managed_heap(relid);

			if (rel == NULL)
				continue;
			fasttrun_prepare_relation_handoff(rel, NULL, do_analyze,
										  do_rewrite, analyze_targets,
										  rewrite_relids);
			relation_close(rel, AccessShareLock);
		}
		list_free(relids);
		return;
	}

	foreach(lc, stmt->rels)
	{
		VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);
		Oid			relid = vrel->oid;
		Relation	rel;
		Bitmapset  *attnums = NULL;

		if (!OidIsValid(relid) && vrel->relation != NULL)
			relid = RangeVarGetRelid(vrel->relation, NoLock, true);
		if (!OidIsValid(relid) || !fasttrun_manages_relid(relid))
			continue;
		rel = fasttrun_try_open_managed_heap(relid);
		if (rel == NULL)
			continue;
		if (do_analyze)
			attnums = fasttrun_analyze_target_attnums(rel, vrel->va_cols);
		fasttrun_prepare_relation_handoff(rel, attnums, do_analyze,
									  do_rewrite, analyze_targets,
									  rewrite_relids);
		relation_close(rel, AccessShareLock);
	}
}

static bool
fasttrun_relation_has_clustered_index(Relation rel)
{
	List	   *index_oids = RelationGetIndexList(rel);
	ListCell   *lc;
	bool		has_clustered = false;

	foreach(lc, index_oids)
	{
		if (get_index_isclustered(lfirst_oid(lc)))
		{
			has_clustered = true;
			break;
		}
	}
	list_free(index_oids);
	return has_clustered;
}

static void
fasttrun_prepare_cluster_handoff(ClusterStmt *stmt, List **rewrite_relids)
{
	List	   *relids;
	ListCell   *lc;
	bool		all_relations = stmt->relation == NULL;

	if (all_relations)
		relids = fasttrun_managed_temp_relids();
	else
	{
		Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

		if (!OidIsValid(relid) || !fasttrun_manages_relid(relid))
			return;
		relids = list_make1_oid(relid);
	}

	foreach(lc, relids)
	{
		Relation	rel = fasttrun_try_open_managed_heap(lfirst_oid(lc));

		if (rel == NULL)
			continue;
		if (!all_relations || fasttrun_relation_has_clustered_index(rel))
			fasttrun_prepare_relation_handoff(rel, NULL, false, true,
										  NULL, rewrite_relids);
		relation_close(rel, AccessShareLock);
	}
	list_free(relids);
}

static void
fasttrun_remember_managed_rebind(RangeVar *relation, List **rewrite_relids)
{
	Oid			relid;

	if (relation == NULL)
		return;
	relid = RangeVarGetRelid(relation, NoLock, true);
	if (OidIsValid(relid) && fasttrun_manages_relid(relid) &&
		!list_member_oid(*rewrite_relids, relid))
		*rewrite_relids = lappend_oid(*rewrite_relids, relid);
}

static void
fasttrun_prepare_utility_handoff(Node *parsetree,
								 List **analyze_targets,
								 List **rewrite_relids)
{
	if (parsetree == NULL ||
		(fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL))
		return;
	if (IsA(parsetree, VacuumStmt))
		fasttrun_prepare_vacuum_handoff((VacuumStmt *) parsetree,
									  analyze_targets, rewrite_relids);
	else if (IsA(parsetree, ClusterStmt))
		fasttrun_prepare_cluster_handoff((ClusterStmt *) parsetree,
									  rewrite_relids);
	else if (IsA(parsetree, AlterTableStmt))
		fasttrun_remember_managed_rebind(
			((AlterTableStmt *) parsetree)->relation, rewrite_relids);
	else if (IsA(parsetree, IndexStmt))
		fasttrun_remember_managed_rebind(
			((IndexStmt *) parsetree)->relation, rewrite_relids);
	else if (IsA(parsetree, TruncateStmt))
	{
		ListCell   *lc;

		foreach(lc, ((TruncateStmt *) parsetree)->relations)
			fasttrun_remember_managed_rebind((RangeVar *) lfirst(lc),
										 rewrite_relids);
	}
}

/*
 * A heap rewrite changes relfilenode.  Rebind the current neutral state to
 * the new locator without losing the undo nodes that restore the old locator
 * on ROLLBACK TO SAVEPOINT.  Non-rewriting DDL targets share this post-step;
 * for them the locator assignment is an intentional no-op.
 */
static void
fasttrun_finish_rewrite_handoff(List *rewrite_relids)
{
	ListCell   *lc;

	foreach(lc, rewrite_relids)
	{
		Relation	rel = fasttrun_try_open_managed_heap(lfirst_oid(lc));
		FasttrunStatsRelidEntry *relentry;
		ListCell   *klc;
		Oid			relid;

		if (rel == NULL)
			continue;
		relid = RelationGetRelid(rel);
		(void) fasttrun_stats_neutralize_relation(rel);
		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache,
						&relid, HASH_FIND, NULL);
		if (relentry != NULL)
		{
			foreach(klc, relentry->attkeys)
			{
				FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(klc);
				FasttrunStatsEntry *entry;

				entry = (FasttrunStatsEntry *)
					hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);
				if (entry == NULL)
					continue;
				entry->heap_rlb.locator = rel->rd_locator;
				entry->heap_rlb.backend = rel->rd_backend;
				entry->heap_rlb_valid = true;
			}
		}
		relation_close(rel, AccessShareLock);
	}
}

static void
fasttrun_finish_analyze_handoff(List *analyze_targets)
{
	ListCell   *lc;

	foreach(lc, analyze_targets)
	{
		FasttrunAnalyzeHandoffTarget *target =
			(FasttrunAnalyzeHandoffTarget *) lfirst(lc);
		Relation	rel = fasttrun_try_open_managed_heap(target->relid);

		if (rel == NULL)
			continue;
		fasttrun_stats_handoff_columns(rel, target->attnums,
									 target->attnums == NULL);
		relation_close(rel, AccessShareLock);
	}
}

static void
fasttrun_free_analyze_handoff(List *analyze_targets)
{
	ListCell   *lc;

	foreach(lc, analyze_targets)
	{
		FasttrunAnalyzeHandoffTarget *target =
			(FasttrunAnalyzeHandoffTarget *) lfirst(lc);

		if (target->attnums != NULL)
			bms_free(target->attnums);
		pfree(target);
	}
	list_free(analyze_targets);
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

static void
fasttrun_mark_copy_from(Node *parsetree)
{
	CopyStmt   *stmt;
	Oid			relid;

	if (fasttrun_stats_relid_cache == NULL ||
		parsetree == NULL || !IsA(parsetree, CopyStmt))
		return;
	stmt = (CopyStmt *) parsetree;
	if (!stmt->is_from || stmt->relation == NULL)
		return;
	relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (OidIsValid(relid) && fasttrun_stats_relid_exists(relid))
		fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_DML);
}

/*
 * Dependency-machinery drops (DROP ... CASCADE, DROP OWNED BY, DISCARD)
 * delete relations without a per-table DropStmt, so the utility hook never
 * sees them.  Touch the relid and note the drop with its subxact id; the
 * commit callbacks remove noted entries without probing the syscache
 * (TRANS_COMMIT forbids catalog access).  Rollback semantics stay intact:
 * if the drop aborts, the subxact callback prunes the note and the cache
 * entry stays valid.
 */
static void
fasttrun_object_access_hook(ObjectAccessType access, Oid classId,
							Oid objectId, int subId, void *arg)
{
	if (prev_object_access_hook)
		(*prev_object_access_hook) (access, classId, objectId, subId, arg);

	if (access != OAT_DROP || classId != RelationRelationId || subId != 0)
		return;

	if (fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL &&
		fasttrun_poison_cache == NULL)
		return;

	/* Touch only relids we actually track -- keeps the list small on
	 * mass drops of unrelated relations. */
	if (fasttrun_cache_lookup(objectId) == NULL &&
		!fasttrun_stats_relid_exists(objectId) &&
		fasttrun_poison_find(objectId) == NULL)
		return;

	fasttrun_xact_mark_relid(objectId, objectId, FASTTRUN_TOUCH_DROPPED);
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
	List   *analyze_targets = NIL;
	List   *rewrite_relids = NIL;
	bool	discard_session_state = false;

	fasttrun_poison_check_utility(parsetree);
	if (IsA(parsetree, DiscardStmt))
	{
		DiscardStmt *stmt = (DiscardStmt *) parsetree;

		discard_session_state =
			(stmt->target == DISCARD_ALL || stmt->target == DISCARD_TEMP);
	}

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

			if (from_dummy && !fasttrun_in_prewarm &&
				fasttrun_schedule_is_active_now())
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

	fasttrun_mark_copy_from(parsetree);
	fasttrun_prepare_utility_handoff(parsetree, &analyze_targets,
									 &rewrite_relids);
	fasttrun_evict_utility_caches(parsetree);

	/* Chain to next hook or standard ProcessUtility. */
	if (prev_utility_hook)
		prev_utility_hook(pstmt, queryString, readOnlyTree, context,
						  params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);

	/* An ERROR above leaves every registry unchanged. */
	if (discard_session_state)
		fasttrun_evict_all_session_caches();

	fasttrun_finish_rewrite_handoff(rewrite_relids);
	fasttrun_finish_analyze_handoff(analyze_targets);
	fasttrun_free_analyze_handoff(analyze_targets);
	list_free(rewrite_relids);
}

/* Build a bounded immutable snapshot without allocation or sorting under lock. */
static FasttrunTrackSnapshot
fasttrun_track_snapshot(int limit)
{
	FasttrunTrackSnapshot snapshot;
	HASH_SEQ_STATUS status;
	FasttrunTrackEntry *entry;
	int			capacity;
	int			worst = 0;
	bool		bounded;

	/* Tiny top-N stays allocation- and copy-bounded; large readers copy all. */
	bounded = (limit > 0 && limit <= 32);
	capacity = bounded ? limit : FASTTRUN_TRACK_MAX;
	snapshot.entries = (FasttrunTrackEntry *)
		palloc(sizeof(FasttrunTrackEntry) * capacity);
	snapshot.count = 0;

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = (FasttrunTrackEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;
		if (!bounded)
		{
			Assert(snapshot.count < FASTTRUN_TRACK_MAX);
			snapshot.entries[snapshot.count++] = *entry;
		}
		else if (snapshot.count < capacity)
		{
			snapshot.entries[snapshot.count] = *entry;
			if (snapshot.count == 0 ||
				fasttrun_track_cmp_desc(&snapshot.entries[snapshot.count],
										&snapshot.entries[worst]) > 0)
				worst = snapshot.count;
			snapshot.count++;
		}
		else if (fasttrun_track_cmp_desc(entry,
									  &snapshot.entries[worst]) < 0)
		{
			int			i;

			snapshot.entries[worst] = *entry;
			worst = 0;
			for (i = 1; i < snapshot.count; i++)
			{
				if (fasttrun_track_cmp_desc(&snapshot.entries[i],
										&snapshot.entries[worst]) > 0)
					worst = i;
			}
		}
	}
	LWLockRelease(fasttrun_track_lock);
	return snapshot;
}

static void
fasttrun_track_swap(FasttrunTrackEntry *a, FasttrunTrackEntry *b)
{
	FasttrunTrackEntry tmp = *a;

	*a = *b;
	*b = tmp;
}

/* Keep the worst selected entry at heap[0]. */
static void
fasttrun_track_sift_worst(FasttrunTrackEntry *heap, int count, int root)
{
	for (;;)
	{
		int			left = root * 2 + 1;
		int			worst = root;

		if (left < count &&
			fasttrun_track_cmp_desc(&heap[left], &heap[worst]) > 0)
			worst = left;
		if (left + 1 < count &&
			fasttrun_track_cmp_desc(&heap[left + 1], &heap[worst]) > 0)
			worst = left + 1;
		if (worst == root)
			return;
		fasttrun_track_swap(&heap[root], &heap[worst]);
		root = worst;
	}
}

/* Exact top-N outside LWLock, then deterministic order of selected rows. */
static int
fasttrun_track_select_top(FasttrunTrackEntry *entries, int count, int limit)
{
	int			i;

	if (limit <= 0 || limit >= count)
	{
		qsort(entries, count, sizeof(FasttrunTrackEntry),
			  fasttrun_track_cmp_desc);
		return count;
	}

	for (i = limit / 2; i-- > 0;)
		fasttrun_track_sift_worst(entries, limit, i);
	for (i = limit; i < count; i++)
	{
		if (fasttrun_track_cmp_desc(&entries[i], &entries[0]) < 0)
		{
			entries[0] = entries[i];
			fasttrun_track_sift_worst(entries, limit, 0);
		}
	}
	qsort(entries, limit, sizeof(FasttrunTrackEntry),
		  fasttrun_track_cmp_desc);
	return limit;
}

/* SQL: fasttrun_hot_temp_tables(n) -- returns top-N most created temp tables. */
Datum
fasttrun_hot_temp_tables(PG_FUNCTION_ARGS)
{
	int32				limit = PG_GETARG_INT32(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate	   *tupstore;
	MemoryContext		per_query_cxt, oldcxt;
	FasttrunTrackSnapshot snapshot;
	FasttrunTrackEntry *sorted;
	int					output_count;
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

	snapshot = fasttrun_track_snapshot(limit);
	sorted = snapshot.entries;
	output_count = fasttrun_track_select_top(sorted, snapshot.count, limit);

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

/* Complete deterministic order; explicit branches avoid subtraction overflow. */
static int
fasttrun_track_cmp_desc(const void *a, const void *b)
{
	const FasttrunTrackEntry *ea = (const FasttrunTrackEntry *) a;
	const FasttrunTrackEntry *eb = (const FasttrunTrackEntry *) b;

	if (ea->create_count != eb->create_count)
		return (ea->create_count < eb->create_count) ? 1 : -1;
	if (ea->last_create != eb->last_create)
		return (ea->last_create < eb->last_create) ? 1 : -1;
	return strcmp(ea->relname, eb->relname);
}

/* SQL: fasttrun_prewarm() -- creates top-N temp tables via create_temp_table. */
Datum
fasttrun_prewarm(PG_FUNCTION_ARGS)
{
	int		limit = fasttrun_prewarm_count;
	int		created = 0;
	int		i;
	FasttrunTrackSnapshot snapshot;
	FasttrunTrackEntry *sorted;

	if (fasttrun_track_htab == NULL)
		PG_RETURN_INT32(0);

	snapshot = fasttrun_track_snapshot(limit);
	sorted = snapshot.entries;
	limit = fasttrun_track_select_top(sorted, snapshot.count, limit);

	{
		/* Verify dummy schema exists; if not, skip all prewarm. */
		Oid		nspOid = get_namespace_oid(fasttrun_prewarm_schema, true);
		bool	save_in_prewarm = fasttrun_in_prewarm;

		/*
		 * Suppress tracking of our own re-entrant CREATE TEMP TABLE.  Save and
		 * restore the flag so nesting stays correct; PG_FINALLY restores it
		 * even if SPI_connect or the SPI query throws.
		 */
		fasttrun_in_prewarm = true;
		PG_TRY();
		{
			/* One SPI session for the whole batch, not one per table. */
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "fasttrun_prewarm: SPI_connect failed");

			for (i = 0; i < limit; i++)
			{
				/* Skip if no matching dummy table in the schema. */
				if (nspOid == InvalidOid ||
					get_relname_relid(sorted[i].relname, nspOid) == InvalidOid)
					continue;

				SPI_execute_with_args(
					"SELECT create_temp_table($1)",
					1,
					(Oid[]) { TEXTOID },
					(Datum[]) { CStringGetTextDatum(sorted[i].relname) },
					NULL, false, 0);

				/* Free this iteration's result so tuptables don't accumulate. */
				if (SPI_tuptable != NULL)
				{
					SPI_freetuptable(SPI_tuptable);
					SPI_tuptable = NULL;
				}

				created++;
			}

			SPI_finish();
		}
		PG_FINALLY();
		{
			fasttrun_in_prewarm = save_in_prewarm;
		}
		PG_END_TRY();
	}

	pfree(sorted);
	PG_RETURN_INT32(created);
}

/* SQL: fasttrun_reset_temp_stats() -- clears all tracking counters. */
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

#ifdef USE_ASSERT_CHECKING
/* Test helper for deterministic tracking-order checks. */
Datum
fasttrun_test_track_set(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char		key[NAMEDATALEN] = {0};
	int64		count = PG_GETARG_INT64(1);
	TimestampTz last_create = PG_GETARG_INT64(2);
	FasttrunTrackEntry *entry;
	bool		found;

	if (fasttrun_track_htab == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun tracking shared memory is unavailable")));
	strlcpy(key, name, sizeof(key));
	LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
	entry = (FasttrunTrackEntry *)
		hash_search(fasttrun_track_htab, key, HASH_ENTER, &found);
	if (!found)
		memcpy(entry->relname, key, sizeof(entry->relname));
	entry->create_count = count;
	entry->last_create = last_create;
	LWLockRelease(fasttrun_track_lock);
	pfree(name);
	PG_RETURN_VOID();
}

/* Test function available only in cassert builds. */
Datum
fasttrun_test_subxact_visits(PG_FUNCTION_ARGS)
{
	bool		reset = PG_GETARG_BOOL(0);
	uint64		prior = fasttrun_test_subxact_visited;

	if (reset)
		fasttrun_test_subxact_visited = 0;
	PG_RETURN_INT64((int64) prior);
}
#endif

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

	DefineCustomRealVariable("fasttrun.invalidate_threshold",
							 "Relstats drift ratio (vs reltuples or relpages) below "
							 "which fasttrun_analyze skips invalidating backend-"
							 "local cached SPI/PREPARE plans",
							 "Default 0.2 -- matches fasttrun.stats_refresh_threshold. "
							 "Plan-cache invalidation and column-stats refresh fire "
							 "together: below the threshold neither happens, above both do. "
							 "Set to 0 to invalidate plans on any relstats change. "
							 "Other signals always fire an invalidation: a fresh "
							 "column-stats refresh, an index relstats change "
							 "(e.g. partial index), or a stats visibility flip. "
							 "Those signal distribution changes that the per-row "
							 "ratio here cannot measure.",
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
							 "Clear local temporary table files without shared "
							 "SMGR invalidations",
							 "When on (default), fasttruncate physically "
							 "clears the temporary table, its indexes, and "
							 "TOAST relations "
							 "with unlink and smgrcreate, without calling "
							 "CacheInvalidateSmgr.  When off, it calls "
							 "RelationTruncate for each relation; every "
							 "successful call sends one shared SMGR "
							 "invalidation.  Both modes rebuild indexes and "
							 "block access if cleanup is interrupted.",
							 &fasttrun_zero_sinval_truncate,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("fasttrun.max_analyze_pages",
							"Heap page count above which cold fasttrun_analyze "
							"switches to bounded block sampling",
							"Default 100000 (~800 MB).  A cold fasttrun_analyze "
							"normally scans the whole temp table for an exact "
							"row count; above this many heap pages it instead "
							"reads a bounded random block sample and ESTIMATES "
							"the row count from tuple density (like a regular "
							"ANALYZE), keeping cost O(sample) instead of "
							"O(table) on anomalously giant temp tables.  Column "
							"statistics are collected from the same sample.  "
							"Set to 0 to always do the exact full scan.",
							&fasttrun_max_analyze_pages,
							100000,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	pg_prng_seed(&fasttrun_prng_state, (uint64) MyProcPid);

	/*
	 * Register the xact + subxact callbacks once per backend, here in
	 * _PG_init, so they fire regardless of which cache is lazily
	 * allocated first -- or whether only standalone
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

	prev_object_access_hook = object_access_hook;
	object_access_hook = fasttrun_object_access_hook;

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
