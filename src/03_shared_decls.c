/*-------------------------------------------------------------------------
 *
 * src/03_shared_decls.c
 *      Types, state and forward declarations shared by later slices.
 *
 * What it holds.  The analyze cache entry and its undo state, the transaction
 * epoch and statement sequence, the truncate operation and workset types, the
 * poison registry state, and the forward declarations later slices rely on.
 *
 * Also here.  fasttrun_make_rangevar, the relation name parser of the SQL
 * functions.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/03_shared_decls.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
typedef struct FasttrunCollectResult
{
	bool		published_any;
	bool		became_neutral;
	bool		index_changed;	/* index relstats moved independently of the
								 * heap row count */
	bool		unsafe;			/* samples were taken while the relation was
								 * being written and never serve the planner */
} FasttrunCollectResult;

/* Forward decls for stats infrastructure (defined in later slices) */
static void fasttrun_stats_cache_reset(void);
static void fasttrun_stats_cache_commit_xact(void);
static bool fasttrun_stats_cache_evict_relid(Oid relid);
static void fasttrun_stats_relid_set_size_anchor(Oid relid, BlockNumber pages);
static void fasttrun_stats_neutralize_index_expressions(Oid heap_relid,
														Oid indexoid);
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
static bool fasttrun_read_pgstat_counters_for_hook(Oid relid, bool *wide_writers,
												   int64 *ins, int64 *upd,
												   int64 *del,
												   bool *truncdropped,
												   BlockNumber *pages);
static void fasttrun_fresh_reserve(Relation rel);
static void fasttrun_fresh_mark(Relation rel);
static void fasttrun_fresh_note_core_truncate(Oid relid);
static void fasttrun_invalidate_local_plan_cache(Oid relid);
static bool fasttrun_relation_has_extended_stats(Relation rel);
static bool fasttrun_may_core_analyze(Relation rel);
static bool fasttrun_may_maintain_relid(Oid relid, bool relisshared);
static void fasttrun_run_core_analyze(Relation rel);
static void fasttrun_passthrough_command(const char *command, Oid relid);
static bool fasttrun_get_relation_stats_hook(PlannerInfo *root,
											 RangeTblEntry *rte,
											 AttrNumber attnum,
											 VariableStatData *vardata);
static int32 fasttrun_get_attavgwidth_hook(Oid relid, AttrNumber attnum);
static bool fasttrun_get_index_stats_hook(PlannerInfo *root, Oid indexOid,
										  AttrNumber indexattnum,
										  VariableStatData *vardata);
static void fasttrun_get_relation_info_hook(PlannerInfo *root,
											Oid relationObjectId,
											bool inhparent,
											RelOptInfo *rel);
static PlannedStmt *fasttrun_planner_hook(Query *parse,
										  const char *query_string,
										  int cursorOptions,
										  ParamListInfo boundParams);
static void fasttrun_ensure_planner_hook(void);
static void fasttrun_ensure_stats_hooks(void);
static void fasttrun_executor_start(QueryDesc *queryDesc, int eflags);
static bool fasttrun_stats_relid_exists(Oid relid);
static bool fasttrun_stats_relid_has_samples(Oid relid);
static bool fasttrun_stats_relid_has_saved_samples(Oid relid);
/* Defined with the session-local stats cache in src/08_stats_types.c. */
struct FasttrunStatsRelidEntry;
struct FasttrunStatsEntry;

static void fasttrun_stats_note_update_target(Oid relid,
											  RTEPermissionInfo *perminfo);
static bool fasttrun_relid_is_temp(Oid relid);
static void fasttrun_stats_update_in_flight(Oid relid, int delta);
static bool fasttrun_stats_relid_in_flight(Oid relid);
static bool fasttrun_probe_size_stays(Relation rel);
static bool fasttrun_stats_relation_written_now(Relation rel);
static void fasttrun_stats_forget_in_flight(void);
static void fasttrun_stats_in_flight_subxact_end(SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 bool aborted);
static void fasttrun_stats_note_wide_writer_ddl(RangeVar *rv);
static void fasttrun_executor_end(QueryDesc *queryDesc);
static void fasttrun_executor_walk_targets(PlannedStmt *stmt, bool starting);
static bool fasttrun_stats_column_was_updated(struct FasttrunStatsRelidEntry *relentry,
											  const struct FasttrunStatsEntry *entry);
static bool fasttrun_stats_begin_collect_period(Relation rel);
static bool fasttrun_relation_has_wide_writers(Relation rel);
static void fasttrun_stats_relid_ref(Oid relid);
static void fasttrun_stats_relid_unref(Oid relid);
static bool fasttrun_manages_relid(Oid relid);
static bool fasttrun_query_contains_stats_relid(Query *query);
static bool fasttrun_contains_stats_sublink_walker(Node *node, void *context);
static bool fasttrun_reinject_query_relstats(Query *query, bool detect_stats);
static bool fasttrun_reinject_sublink_walker(Node *node, void *context);

/*
 * Build a RangeVar from a text relation name.  Fast path for bare
 * lowercase identifiers (no '.', no '"', no uppercase) skips the parser
 * entirely.  Everything else falls back to stringToQualifiedNameList.
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
 * skip visibility-map and index-stat maintenance.  Both parts survive
 * COMMIT: the commit callback moves the counter anchors to the next
 * transaction's origin (see fasttrun_cache_commit_xact).  Cache invalidated
 * if: pages dropped (TRUNCATE-like), truncdropped bit changed, or computed
 * new_tuples < 0.  pgstat unavailable -> fall back to scan.
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
	bool		count_exact;	/* cached_tuples is a count, not an estimate
								 * from sampled blocks */
	BlockNumber	cached_allvisible;
	BlockNumber	last_inval_pages;
	float4		last_inval_tuples;
	bool		last_inval_valid;
	BlockNumber	last_inval_dead;	/* aborted_pages at the last invalidation */
	BlockNumber	published_dead;	/* aborted_pages when cached_pages was
								 * published */
	uint64		published_aborted_writes;	/* aborted_writes at that time */
	bool		ext_stats_stale;	/* columns were collected here while the
									 * CREATE STATISTICS objects keep the
									 * core's earlier fill */
	bool		owns_index_relstats;	/* relstats of this heap's indexes are
										 * published: reinjection reaches them
										 * through this entry */
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
	BlockNumber	aborted_pages;	/* the dead-page count of the file this
								 * state describes */
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
	uint64			lazy_check_epoch;	/* fasttrun_xact_epoch of the memo:
										 * subtransaction ids start over in
										 * every transaction */
	uint64			write_check_epoch;	/* transaction whose storage size is
										 * known: an analyze measured it or the
										 * first write probed it */
	BlockNumber		aborted_pages;	/* heap pages added by rolled-back writes
									 * since the storage was last reset.  Not
									 * undone: an abort cannot take pages out
									 * of the file */
	uint64			aborted_writes;	/* rolled-back levels that wrote the
									 * table: their index entries stay even
									 * when the heap did not grow */
	BlockNumber		probe_size;		/* heap size the planner's probe read */
	uint64			probe_size_seq;	/* fasttrun_statement_seq of that read */
	uint64			probe_size_epoch;	/* fasttrun_xact_epoch of that read */
} FasttrunAnalyzeCacheEntry;

/*
 * Counts finished top-level transactions.  Core ON COMMIT DELETE ROWS empties
 * tables at commit without counters, so a storage observation is good for one
 * transaction at most.
 */
static uint64 fasttrun_xact_epoch = 1;

/*
 * Advances at every executor start, utility statement, fasttruncate and
 * subtransaction abort.  A heap size read while the counter had its current
 * value, with no running statement able to grow the file
 * (fasttrun_probe_size_stays), is still the size: nothing that writes the heap
 * or resets its storage has started since.  The first write of a statement uses
 * the size its own planning read and does not read the file again.
 */
static uint64 fasttrun_statement_seq = 1;

/*
 * A statement may be writing a relation the registry of started writers does
 * not know: the library loaded while statements were running, or CREATE TABLE
 * AS started (its executor runs a SELECT, and the rows reach the new table
 * through its DestReceiver).  No statement outlives its transaction, so the
 * registry is complete again from the next one.
 */
static bool fasttrun_unobserved_writers = false;

static HTAB			   *fasttrun_analyze_cache = NULL;
static MemoryContext	fasttrun_analyze_mcxt = NULL;

static FasttrunAnalyzeCacheEntry *fasttrun_cache_lookup(Oid relid);
static BlockNumber fasttrun_cache_aborted_pages(Oid relid);
static void fasttrun_cache_reset(void);

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
static void fasttrun_exit_plan_reset_note_commit(void);
static void fasttrun_exit_plan_reset_register(void);
static void fasttrun_analyze_save_undo(FasttrunAnalyzeCacheEntry *entry);
static bool fasttrun_read_pgstat_counters_at_commit(Oid relid,
													int64 *ins, int64 *upd,
													int64 *del,
													bool *truncdropped);
static void fasttrun_poison_commit_xact(void);
static void fasttrun_poison_check_relid(Oid relid, const char *operation);
static void fasttrun_poison_check_plan(PlannedStmt *plan);
static void fasttrun_poison_check_relation(Relation rel,
										   const char *operation,
										   bool allow_repair);
