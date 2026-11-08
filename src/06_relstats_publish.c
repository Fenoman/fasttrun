/*-------------------------------------------------------------------------
 *
 * src/06_relstats_publish.c
 *      Analyze cache entries: lookup, undo, relstats publication, eviction.
 *
 * What it holds.  Lookup and creation of analyze cache entries, their undo
 * copies for savepoints, publication of relstats and of the delta state, the
 * stats baseline, and eviction of a relation and its indexes.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/06_relstats_publish.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
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
 * (delta-math snapshot) themselves.  The stats baseline starts out as
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
		entry->lazy_check_epoch = 0;
		entry->write_check_epoch = 0;
		entry->aborted_pages = 0;
		entry->aborted_writes = 0;
		entry->probe_size = InvalidBlockNumber;
		entry->probe_size_seq = 0;
		entry->probe_size_epoch = 0;
	}
	return entry;
}

/*
 * An entry without published relstats follows the relation's current file.
 * Not undone: the file a rollback brings back with a published state is handled
 * by the abort path (restored_other_file), and a file-only entry has no state
 * tied to the old file.
 */
static void
fasttrun_cache_follow_file(FasttrunAnalyzeCacheEntry *entry, Relation rel)
{
	Oid			relid = RelationGetRelid(rel);

	if (!entry->state.has_relstats &&
		(entry->state.heap_relid != relid ||
		 !RelFileLocatorEquals(entry->state.probe_rlb.locator, rel->rd_locator)))
	{
		entry->state.heap_relid = relid;
		entry->state.probe_rlb.locator = rel->rd_locator;
		entry->state.probe_rlb.backend = rel->rd_backend;
		entry->state.heap_rlb = entry->state.probe_rlb;
	}
}

/*
 * Column statistics collected without fasttrun_analyze still read the dead
 * pages of the heap file.  The entry points at the current file and holds no
 * relstats and no delta state.
 */
static FasttrunAnalyzeCacheEntry *
fasttrun_cache_ensure_file_entry(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	FasttrunAnalyzeCacheEntry *entry;

	/* An abort of the table's creation removes the entry with the table. */
	fasttrun_xact_mark_created_relation(rel);
	fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_ANALYZE);
	entry = fasttrun_cache_enter(relid);
	fasttrun_cache_follow_file(entry, rel);
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
	saved->aborted_pages = entry->aborted_pages;
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

	fasttrun_xact_mark_created_relation(rel);

	/*
	 * Register this relid so the xact-end callback walks it.  Index entries
	 * land here too (via fasttrun_update_index_relstats) without going
	 * through the top-level touch in fasttrun_analyze or
	 * fasttrun_collect_stats.  An abort restores the old values.  Plans built
	 * on the new ones survive it unless the publication itself invalidated
	 * plans: below fasttrun.invalidate_threshold the old and the new values
	 * are equally close to what the cached plans were built on.  The caller
	 * that invalidates marks the frame for the abort.
	 */
	fasttrun_xact_mark_relid(RelationGetRelid(rel), RelationGetRelid(rel),
								FASTTRUN_TOUCH_ANALYZE);

	entry = fasttrun_cache_enter(RelationGetRelid(rel));
	fasttrun_analyze_save_undo(entry);

	entry->state.has_relstats = true;
	entry->state.cached_locator = rel->rd_locator;
	entry->state.cached_pages = pages;
	entry->state.cached_tuples = tuples;
	entry->state.cached_allvisible = allvisible;
	entry->state.published_dead = entry->aborted_pages;
	entry->state.published_aborted_writes = entry->aborted_writes;
	entry->state.probe_rlb.locator = rel->rd_locator;
	entry->state.probe_rlb.backend = rel->rd_backend;
	/* Default: the entry is its own heap.  Index call sites override. */
	entry->state.heap_relid = RelationGetRelid(rel);
	entry->state.heap_rlb = entry->state.probe_rlb;
	/* cached_pages changed -- the lazy-probe memo is now stale. */
	entry->lazy_check_subid = InvalidSubTransactionId;
	entry->lazy_check_epoch = 0;
	entry->lazy_check_pages = 0;
	/* The pages just published were measured in this transaction. */
	entry->write_check_epoch = fasttrun_xact_epoch;

	return entry;
}

/* Point an index entry's emptiness probe at its owning heap's storage. */
static void
fasttrun_cache_set_owning_heap(FasttrunAnalyzeCacheEntry *entry,
							   Relation heaprel)
{
	Oid			root_relid = RelationGetRelid(heaprel);

	fasttrun_xact_mark_relid(entry->relid, root_relid, FASTTRUN_TOUCH_ANALYZE);

	entry->state.heap_relid = root_relid;
	entry->state.heap_rlb.locator = heaprel->rd_locator;
	entry->state.heap_rlb.backend = heaprel->rd_backend;
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
 * restores it.  Same-subxact overwrites just clobber in place.
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
	entry->lazy_check_epoch = 0;
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

	/* No index publication is left to reach through the heap's entry. */
	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(RelationGetRelid(rel));

		if (entry != NULL)
			entry->state.owns_index_relstats = false;
	}
}

