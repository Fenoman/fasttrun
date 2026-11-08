/*-------------------------------------------------------------------------
 *
 * src/08_stats_types.c
 *      Column stats cache: keys, types, state and hook installation.
 *
 * What it holds.  The cache keys for columns and index expressions, the cache
 * entry types and their saved states, the cache state itself, the previous
 * hook pointers and the lazy installation of the planner and stats hooks.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/08_stats_types.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
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

/*
 * A column of the heap, or with indexoid set, an expression column of one of
 * its indexes: attnum is then the index column number.  Expression statistics
 * live under the heap relid so that everything done to the heap's statistics
 * (undo, commit carry, eviction, locator checks) applies to them too.
 */
typedef struct FasttrunStatsKey
{
	Oid			relid;
	AttrNumber	attnum;
	bool		inh;
	Oid			indexoid;		/* InvalidOid for a heap column */
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

static inline void
fasttrun_stats_expr_key_init(FasttrunStatsKey *key, Oid relid,
							 AttrNumber indexattnum, Oid indexoid)
{
	fasttrun_stats_key_init(key, relid, indexattnum, false);
	key->indexoid = indexoid;
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
	BlockNumber			collected_dead;
	bool				was_usable;
	uint64				collect_seq;
	bool				unsafe;
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
	BlockNumber			collected_pages;	/* physical block count at collect,
											 * the only change signal that
											 * survives a temp table's commit */
	BlockNumber			collected_dead;	/* the heap's aborted_pages at collect */
	bool				was_usable;		/* entry passed the freshness check at
										 * the last analyze/publish.  Drives the
										 * visible->hidden flip detection */
	uint64				collect_seq;	/* collect period this sample belongs to.
										 * The per-column UPDATE mask describes
										 * the relation's current period only */
	bool				unsafe;		/* taken while the relation was being
									 * written: the scan and the counters
									 * beside it describe different moments */
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
 * belonging to a given relid -- O(#cached-cols), small -- instead of a
 * hash_seq_search over the entire stats cache, which is O(N) and grows
 * large on long-lived backends that analyze many temp tables.
 */
typedef struct FasttrunStatsRelidEntry
{
	Oid			relid;		/* hash key -- must be first */
	int			refcount;	/* visible statsTuple entries for this relid */
	List	   *attkeys;	/* List of FasttrunStatsKey *, owns palloc'd keys */
	Bitmapset  *updated_cols;	/* attnums named by an UPDATE since the last
								 * collect, in RTEPermissionInfo.updatedCols
								 * offset form.  Lives in fasttrun_stats_mcxt */
	uint64		collect_seq;	/* current collect period.  A column sample from
								 * an older period is not covered by the mask */
	RelFileLocatorBackend heap_rlb;	/* heap storage identity for the commit-time size probe */
	bool		heap_rlb_valid;
	bool		updated_cols_exact;	/* the mask can be taken as the full set of
									 * what an UPDATE changed on this relation.
									 * Placed here to reuse existing padding */
	bool		period_unsafe;	/* samples of the current period were taken
								 * while a statement was writing the relation */
	FasttrunRelStatsPolicy policy;
	SubTransactionId state_subid;
	FasttrunStatsRelidSavedState *undo;
} FasttrunStatsRelidEntry;

typedef struct FasttrunAnalyzeHandoffTarget
{
	Oid			relid;
	Bitmapset  *attnums;		/* NULL means every analyzable user column */
	List	   *ext_before;		/* FasttrunExtDataVersion of each CREATE
								 * STATISTICS object while ext_stats_stale is
								 * set, taken before core ANALYZE */
} FasttrunAnalyzeHandoffTarget;

/* The pg_statistic_ext_data row of one object, as a heap tuple version. */
typedef struct FasttrunExtDataVersion
{
	Oid			stxoid;
	bool		present;
	TransactionId xmin;
	ItemPointerData tid;
} FasttrunExtDataVersion;

static HTAB			   *fasttrun_stats_cache = NULL;
static HTAB			   *fasttrun_stats_relid_cache = NULL;
static MemoryContext	fasttrun_stats_mcxt = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_attavgwidth_hook_type prev_get_attavgwidth_hook = NULL;
static get_index_stats_hook_type prev_get_index_stats_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static bool				fasttrun_stats_hooks_installed = false;
static bool				fasttrun_planner_hook_installed = false;
static bool				fasttrun_executor_hook_installed = false;

static void
fasttrun_ensure_planner_hook(void)
{
	if (fasttrun_planner_hook_installed || fasttrun_passthrough)
		return;

	prev_planner_hook = planner_hook;
	planner_hook = fasttrun_planner_hook;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = fasttrun_get_relation_info_hook;
	fasttrun_planner_hook_installed = true;
}

static void
fasttrun_ensure_stats_hooks(void)
{
	if (fasttrun_stats_hooks_installed || fasttrun_passthrough)
		return;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = fasttrun_get_relation_stats_hook;
	prev_get_attavgwidth_hook = get_attavgwidth_hook;
	get_attavgwidth_hook = fasttrun_get_attavgwidth_hook;
	prev_get_index_stats_hook = get_index_stats_hook;
	get_index_stats_hook = fasttrun_get_index_stats_hook;
	fasttrun_stats_hooks_installed = true;
}

