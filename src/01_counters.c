/*-------------------------------------------------------------------------
 *
 * src/01_counters.c
 *      Event counters, slow-call log and test failpoints.
 *
 * What it holds.  The activity counters kept per backend and flushed to shared
 * memory at transaction end, the slow-call log of fasttruncate and
 * fasttrun_analyze, and the failpoints of cassert builds.
 *
 * Also here.  The state of the planner freshness memo cache, the one-shot
 * warning flags and the cassert probe counters: later slices read them, and
 * the build order keeps their declarations before the journal.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/01_counters.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Activity counters.
 *
 * What they are.  Monotonic event counts of the extension: how calls went
 * (delta, scan, fast path), how often the planner got cached statistics or
 * defaults, how many local plan invalidations ran.
 *
 * Where they live.  Each backend increments plain local counters on the hot
 * path and adds them at every transaction end to its own slot in shared
 * memory: one atomic add per counter that moved, on cache lines no other
 * backend writes, so busy backends do not bounce them between cores.  The
 * reader sums the slots.  Shared memory exists only with
 * shared_preload_libraries.  Without it the counters stay in the backend and
 * describe that backend alone.
 */
typedef enum FasttrunCounter
{
	FTC_TRUNCATE_CALLS,
	FTC_TRUNCATE_EMPTY_FAST_PATH,
	FTC_TRUNCATE_FAILED_AFTER_CHANGE,
	FTC_ANALYZE_CALLS,
	FTC_ANALYZE_NOOP,
	FTC_ANALYZE_DELTA,
	FTC_ANALYZE_COLD_SCAN,
	FTC_ANALYZE_COLD_BLOCK_SAMPLE,
	FTC_ANALYZE_CORE_HANDOFF,
	FTC_STORAGE_FOUND_EMPTY,
	FTC_COLUMN_COLLECTS,
	FTC_COLUMN_COLLECTS_DURING_WRITE,
	FTC_REFRESH_DEFERRED,
	FTC_PARTIAL_INDEX_RESCANS,
	FTC_PLAN_INVALIDATIONS,
	FTC_STATS_SERVED,
	FTC_STATS_HIDDEN,
	FTC_WRITER_REGISTRY_OVERFLOWS,
	FTC_COMMIT_CARRIED,
	FTC_TRACKING_REJECTED,
	FTC_COUNT
} FasttrunCounter;

static const char *const fasttrun_counter_names[FTC_COUNT] = {
	"truncate_calls",
	"truncate_empty_fast_path",
	"truncate_failed_after_change",
	"analyze_calls",
	"analyze_noop",
	"analyze_delta",
	"analyze_cold_scan",
	"analyze_cold_block_sample",
	"analyze_core_handoff",
	"storage_found_empty",
	"column_collects",
	"column_collects_during_write",
	"refresh_deferred",
	"partial_index_rescans",
	"plan_invalidations",
	"stats_served",
	"stats_hidden",
	"writer_registry_overflows",
	"commit_carried",
	"tracking_rejected"
};

typedef struct FasttrunCounterSlot
{
	pg_atomic_uint64 values[FTC_COUNT];
} FasttrunCounterSlot;

/* Slots start on their own cache lines. */
#define FASTTRUN_COUNTER_SLOT_SIZE \
	TYPEALIGN(PG_CACHE_LINE_SIZE, sizeof(FasttrunCounterSlot))

typedef struct FasttrunSharedCounters
{
	int			nslots;
	char		slots[FLEXIBLE_ARRAY_MEMBER];	/* nslots slots, aligned */
} FasttrunSharedCounters;

static uint64 fasttrun_local_counters[FTC_COUNT];
static bool fasttrun_local_counters_dirty = false;
static FasttrunSharedCounters *fasttrun_shared_counters = NULL;

static Size
fasttrun_shared_counters_size(int nslots)
{
	return add_size(TYPEALIGN(PG_CACHE_LINE_SIZE,
							  offsetof(FasttrunSharedCounters, slots)) +
					PG_CACHE_LINE_SIZE,
					mul_size(nslots, FASTTRUN_COUNTER_SLOT_SIZE));
}

static FasttrunCounterSlot *
fasttrun_counter_slot(int i)
{
	char	   *base = (char *) TYPEALIGN(PG_CACHE_LINE_SIZE,
										  fasttrun_shared_counters->slots);

	return (FasttrunCounterSlot *) (base + (Size) i * FASTTRUN_COUNTER_SLOT_SIZE);
}

/*
 * Regular backends and background workers own the first MaxBackends process
 * numbers.  Anything else wraps around: the adds stay atomic, so a shared slot
 * costs contention, never a lost count.
 */
static FasttrunCounterSlot *
fasttrun_my_counter_slot(void)
{
#if PG_VERSION_NUM >= 170000
	int			procno = MyProcNumber;
#else
	int			procno = MyProc != NULL ? MyProc->pgprocno : 0;
#endif

	if (procno < 0)
		procno = 0;
	return fasttrun_counter_slot(procno % fasttrun_shared_counters->nslots);
}

static inline void
fasttrun_count(FasttrunCounter counter)
{
	fasttrun_local_counters[counter]++;
	fasttrun_local_counters_dirty = true;
}

/* Add the local counts to shared memory.  Called at every transaction end. */
static void
fasttrun_counters_flush(void)
{
	int			i;

	FasttrunCounterSlot *slot;

	if (!fasttrun_local_counters_dirty || fasttrun_shared_counters == NULL)
		return;
	slot = fasttrun_my_counter_slot();
	for (i = 0; i < FTC_COUNT; i++)
	{
		if (fasttrun_local_counters[i] == 0)
			continue;
		pg_atomic_fetch_add_u64(&slot->values[i],
								(int64) fasttrun_local_counters[i]);
		fasttrun_local_counters[i] = 0;
	}
	fasttrun_local_counters_dirty = false;
}

/* The clock runs only while fasttrun.log_min_duration is on. */
static inline void
fasttrun_timer_start(instr_time *start)
{
	if (fasttrun_log_min_duration >= 0)
		INSTR_TIME_SET_CURRENT(*start);
	else
		INSTR_TIME_SET_ZERO(*start);
}

/*
 * One LOG line for a call that took at least fasttrun.log_min_duration
 * milliseconds.  The pages and rows are what the call left published.
 */
static void
fasttrun_log_if_slow(const instr_time *start, const char *operation,
					 Relation rel, const char *path)
{
	instr_time	elapsed;
	double		ms;

	if (fasttrun_log_min_duration < 0 || INSTR_TIME_IS_ZERO(*start))
		return;
	INSTR_TIME_SET_CURRENT(elapsed);
	INSTR_TIME_SUBTRACT(elapsed, *start);
	ms = INSTR_TIME_GET_MILLISEC(elapsed);
	if (ms < (double) fasttrun_log_min_duration)
		return;
	ereport(LOG,
			(errmsg("fasttrun: %s of \"%s\" took %.3f ms",
					operation, RelationGetRelationName(rel), ms),
			 errdetail("Path: %s.  Published %d pages and %.0f rows.",
					   path, (int) rel->rd_rel->relpages,
					   (double) rel->rd_rel->reltuples)));
}

#ifdef USE_ASSERT_CHECKING
static char *fasttrun_test_failpoint = "";
#endif

/* No allocations: a shutdown callback must not throw ERROR. */
static bool
fasttrun_test_failpoint_matches(const char *name, int ordinal)
{
#ifdef USE_ASSERT_CHECKING
	char		key[96];

	if (ordinal > 0)
		snprintf(key, sizeof(key), "%s:%d", name, ordinal);
	else
		strlcpy(key, name, sizeof(key));
	return fasttrun_test_failpoint[0] != '\0' &&
		strcmp(fasttrun_test_failpoint, key) == 0;
#else
	(void) name;
	(void) ordinal;
	return false;
#endif
}

#ifdef USE_ASSERT_CHECKING
static void
fasttrun_test_fail(const char *name, int ordinal)
{
	char		key[96];

	if (ordinal > 0)
		snprintf(key, sizeof(key), "%s:%d", name, ordinal);
	else
		strlcpy(key, name, sizeof(key));
	if (fasttrun_test_failpoint_matches(name, ordinal))
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
 * the stats hash lookup, but reopening the relation on every call -- once for
 * the storage-locator check and once for the pgstat xact counters.  While
 * standard_planner runs, memoize both per relid (a relation cannot be dropped
 * and recreated mid-plan, the same consistency model the pgstat snapshot
 * relies on).  Outside planner_hook, fall back to direct reads.
 *
 * One slot per distinct temp relid that supplied column stats in the current
 * plan.  256 slots cover join/CTE/partition-by-temp patterns.  Once a plan
 * touches more distinct temp relids than that, round-robin eviction
 * (next_evict) makes the evicted relid pay one RelationIdGetRelation + pgstat
 * read on its next probe.  Correctness is unaffected -- every read matches
 * slot->relid, so an evicted slot is a miss, not a wrong answer.  ~10 KB of
 * bss per backend.  Linear scan stays cache-friendly at this size and beats
 * hashing.  The cache resets after each plan, so a simple FIFO is enough.
 */
#define FASTTRUN_FRESHNESS_CACHE_SLOTS 256

typedef struct FasttrunFreshnessCacheEntry
{
	Oid			relid;
	bool		locator_checked;	/* locator_ok is filled in */
	bool		locator_ok;		/* memoized storage-locator verdict */
	bool		counters_cached;	/* pgstat/pages fields are filled in */
	bool		have;
	int64		ins;
	int64		upd;
	int64		del;
	bool		truncdropped;
	bool		wide_writers;	/* relation can change a column no statement
								 * named: row triggers, stored generated cols */
	bool		upd_cached;		/* upd holds a real reading, not the zero a
								 * caller that passed upd = NULL left behind.
								 * Placed here to reuse existing padding */
	BlockNumber	pages;
	bool		owner_checked;	/* owner_heap is filled in (index relids) */
	Oid			owner_heap;		/* memoized owning heap of an index */
} FasttrunFreshnessCacheEntry;

static bool		fasttrun_in_planner = false;
static int		fasttrun_freshness_cache_used = 0;
static int		fasttrun_freshness_cache_next_evict = 0;
static FasttrunFreshnessCacheEntry
				fasttrun_freshness_cache[FASTTRUN_FRESHNESS_CACHE_SLOTS];

/* "Silent killer" warning -- one shot per backend. */
static bool		fasttrun_warned_track_counts_off = false;

/* fasttrun.max_stats_memory warning -- one shot per backend. */
static bool		fasttrun_warned_stats_budget = false;

#ifdef USE_ASSERT_CHECKING
/* Test probe: counts relids examined by subxact cleanup. */
static uint64 fasttrun_test_subxact_visited = 0;
#endif

#ifdef USE_ASSERT_CHECKING
static uint64 fasttrun_test_locator_opens = 0;
static uint64 fasttrun_test_counter_opens = 0;
static uint64 fasttrun_test_owner_opens = 0;
static uint64 fasttrun_test_plan_invalidations = 0;
static uint64 fasttrun_test_index_hook_calls = 0;
#endif
