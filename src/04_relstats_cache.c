/*-------------------------------------------------------------------------
 *
 * src/04_relstats_cache.c
 *      Analyze cache: storage state, dead pages, index owners, commit.
 *
 * What it holds.  Checks of the cached file and its emptiness, the dead pages
 * of rolled-back writes, ownership of index relstats, the effective page count
 * seen by the planner, and the commit-time carry-over of the analyze cache.
 *
 * Also here.  The transaction callback, the plan reset at backend exit and
 * fasttrun_cache_init, which follow the cache commit in the build order.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/04_relstats_cache.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */

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
 * is off-limits (TRANS_ABORT, PG18 asserts inside RelationIdGetRelation).
 * The probe therefore stays at smgr level, addressing storage through the
 * RelFileLocatorBackend captured at store time.  A vanished or replaced
 * storage file (smgrexists false) means the cached identity no longer holds
 * -- bail out to the ordinary undo path.  An index keeps its metapage even
 * after truncate, so index emptiness is judged by the owning heap's blocks.
 * rd_rel is left alone: the planner-hook reinject repairs it on the next
 * plan that touches the relation.
 *
 * truncated: the ending level contains a fasttruncate() of this relation.
 * Every row written after the reset belongs to that level and is rolled back
 * with it, and the rows from before the reset are gone, so the relation has no
 * live rows whatever the file size.  Pages left by a rolled-back refill stay
 * in the published page count, because a scan still reads them.
 */
static bool
fasttrun_cache_file_empty(const RelFileLocatorBackend *rlb)
{
	SMgrRelation reln = smgropen(rlb->locator, rlb->backend);

	return smgrexists(reln, MAIN_FORKNUM) && smgrnblocks(reln, MAIN_FORKNUM) == 0;
}

static bool
fasttrun_cache_make_empty_storage_authoritative(FasttrunAnalyzeCacheEntry *entry,
												bool truncated,
												Oid *plan_relid)
{
	SMgrRelation own;

	*plan_relid = entry->state.heap_relid;

	if (!entry->state.has_relstats || !OidIsValid(entry->state.heap_relid))
		return false;

	own = smgropen(entry->state.probe_rlb.locator, entry->state.probe_rlb.backend);
	if (!smgrexists(own, MAIN_FORKNUM))
		return false;

	if (truncated)
		;
	else if (entry->state.heap_relid != entry->relid)
	{
		/* Index entry: emptiness is the owning heap's emptiness. */
		if (!fasttrun_cache_file_empty(&entry->state.heap_rlb))
			return false;
	}
	else if (smgrnblocks(own, MAIN_FORKNUM) != 0)
		return false;

	/* An empty heap gives 0 pages here, a rebuilt index keeps its metapage. */
	entry->state.has_relstats = true;
	entry->state.cached_pages = smgrnblocks(own, MAIN_FORKNUM);
	entry->state.published_dead = entry->aborted_pages;
	entry->state.published_aborted_writes = entry->aborted_writes;
	entry->state.cached_tuples = 0;
	entry->state.count_exact = true;
	entry->state.cached_allvisible = 0;
	entry->state_subid = InvalidSubTransactionId;
	entry->state.has_delta_state = false;
	/* cached_pages drifted -- drop the lazy memo. */
	entry->lazy_check_subid = InvalidSubTransactionId;
	entry->lazy_check_epoch = 0;
	entry->lazy_check_pages = 0;

	return true;
}

/*
 * Dead pages of rolled-back writes.
 *
 * A rolled-back write leaves its pages in the file while the pgstat counters
 * forget it.  The planner divides reltuples by relpages and multiplies by the
 * actual page count, so pages that hold no live rows would inflate every
 * estimate by the growth factor.
 *
 * What is kept.  aborted_pages counts the heap pages rolled-back writes added
 * since the storage was last reset.  An abort recomputes it from the mark the
 * level left at its first write (the size and the count before that write),
 * so nested aborts of one growth do not add it twice.  It lives outside the
 * undone state: an abort cannot take pages out of the file.  The size comes
 * from smgr through the stored locator, since the abort path has no catalog
 * access.
 *
 * Who reads it.  The published relpages is scaled so that the planner counts
 * only the pages that were there when it was measured or grew since with
 * committed rows (fasttrun_cache_effective_relpages), an analyze with dead
 * pages since the last publication measures the size anew and refreshes the
 * index relstats, and the size backstop of the column statistics discounts
 * them.
 */
/*
 * Dead pages belong to the file, not to a transaction level.  A saved state of
 * the same file takes the new count, so a later restore of it keeps the pages
 * of writes rolled back after it was saved.
 */
static void
fasttrun_cache_share_dead_with_undo(FasttrunAnalyzeCacheEntry *entry)
{
	FasttrunAnalyzeUndo *undo;

	for (undo = entry->undo; undo != NULL; undo = undo->older)
	{
		if (RelFileLocatorEquals(undo->state.probe_rlb.locator,
								 entry->state.probe_rlb.locator))
			undo->aborted_pages = entry->aborted_pages;
	}
}

static void
fasttrun_cache_note_aborted_growth(FasttrunAnalyzeCacheEntry *entry,
								   const FasttrunXactRelEntry *xentry,
								   bool restored, BlockNumber restored_dead,
								   bool restored_other_file)
{
	SMgrRelation reln;
	BlockNumber phys;

	/*
	 * Every heap entry that knows its file counts, published or not: the size
	 * backstop of column statistics reads the count while DDL keeps the
	 * relstats unpublished.
	 */
	if (entry->state.heap_relid != entry->relid)
		return;

	/*
	 * The abort brings back a state of another file: a core TRUNCATE or a
	 * rewrite of the level gave the relation a new one.  The current count may
	 * include dead pages of the new file.  When the level's mark was taken on
	 * the file that comes back, the growth since the mark is that file's own,
	 * and the mark is used below.  Otherwise the file got no rolled-back rows
	 * of this level, and its count is the one saved with the restored state:
	 * a write before the first analyze on the new file leaves no mark at all.
	 */
	if (restored && restored_other_file &&
		!(xentry->level_size_known &&
		  RelFileLocatorEquals(entry->state.probe_rlb.locator,
							   xentry->level_locator)))
		entry->aborted_pages = restored_dead;
	else if (!xentry->level_size_known)
		return;
	else if (!RelFileLocatorEquals(entry->state.probe_rlb.locator,
								   xentry->level_locator))
	{
		/* The mark was taken on another file than the one the state describes. */
		entry->aborted_pages = restored ? restored_dead : xentry->level_dead;
	}
	else
	{
		reln = smgropen(entry->state.probe_rlb.locator,
						entry->state.probe_rlb.backend);
		if (!smgrexists(reln, MAIN_FORKNUM))
			return;
		phys = smgrnblocks(reln, MAIN_FORKNUM);
		entry->aborted_pages = xentry->level_dead +
			(phys > xentry->level_pages ? phys - xentry->level_pages : 0);
		/* The page count the memo checked no longer tells the whole story. */
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_epoch = 0;
		entry->lazy_check_pages = 0;
	}
	fasttrun_cache_share_dead_with_undo(entry);
}

/*
 * An entry without published relstats stays while something still reads it:
 * column samples of the heap, whose size backstop subtracts the dead pages,
 * CREATE STATISTICS data it keeps from the planner, or published index
 * relstats reached through it.  The dead-page count alone keeps nothing: it
 * matters only against a publication or a sample, and the next analyze of an
 * unpublished heap measures the file anew.
 */
static bool
fasttrun_cache_entry_keeps_file(const FasttrunAnalyzeCacheEntry *entry)
{
	return entry->state.has_relstats ||
		entry->state.ext_stats_stale || entry->state.owns_index_relstats ||
		(entry->state.heap_relid == entry->relid &&
		 fasttrun_stats_relid_has_samples(entry->relid));
}

/*
 * The analyze-cache commit runs first and keeps an unpublished entry for
 * column samples that the stats-cache commit may then hide.  It runs again
 * here for that relation.
 */
static void
fasttrun_cache_reclaim_entry(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

	if (entry == NULL || entry->undo != NULL ||
		fasttrun_cache_entry_keeps_file(entry))
		return;
	(void) hash_search(fasttrun_analyze_cache, &relid, HASH_REMOVE, NULL);
	if (hash_get_num_entries(fasttrun_analyze_cache) == 0)
		fasttrun_cache_reset();
}

/* The heap an index entry belongs to, InvalidOid for a heap entry. */
static inline Oid
fasttrun_cache_index_owner(const FasttrunAnalyzeCacheEntry *entry)
{
	return entry->state.heap_relid != entry->relid ?
		entry->state.heap_relid : InvalidOid;
}

/*
 * An index of heap_relid has published relstats.  include_undo: inside an
 * open transaction, a publication that the rollback of an outer level brings
 * back counts too.
 */
static bool
fasttrun_cache_heap_has_index_publication(Oid heap_relid, bool include_undo)
{
	HASH_SEQ_STATUS status;
	FasttrunAnalyzeCacheEntry *entry;

	hash_seq_init(&status, fasttrun_analyze_cache);
	while ((entry = (FasttrunAnalyzeCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		FasttrunAnalyzeUndo *undo;
		bool		found = false;

		if (entry->relid == heap_relid)
			continue;
		if (entry->state.has_relstats &&
			fasttrun_cache_index_owner(entry) == heap_relid)
			found = true;
		for (undo = include_undo ? entry->undo : NULL;
			 undo != NULL && !found; undo = undo->older)
			found = undo->state.has_relstats &&
				undo->state.heap_relid == heap_relid;
		if (found)
		{
			hash_seq_term(&status);
			return true;
		}
	}
	return false;
}

/*
 * A committed index entry of heap_relid ended without relstats.
 *
 * Why.  Every index publication sets owns_index_relstats on the heap, and
 * evicting the heap together with its indexes clears it.  Retiring one index
 * (DROP INDEX) leaves the flag set, and the flag alone would keep a heap
 * entry that has nothing left to reach.
 *
 * What it does.  Unless another index of the heap is still published, the
 * flag goes, and so does the entry when nothing else keeps it.  A heap that
 * the commit walk has not reached yet keeps its undo here and goes there.
 * The check scans the cache, so it runs only for a heap without relstats of
 * its own: only fasttrun_collect_stats leaves one owning index relstats.  A
 * heap with relstats stays anyway, and evicting them clears the flag.
 */
static void
fasttrun_cache_recheck_index_owner(Oid heap_relid)
{
	FasttrunAnalyzeCacheEntry *heap = fasttrun_cache_lookup(heap_relid);

	if (heap == NULL || !heap->state.owns_index_relstats ||
		heap->state.has_relstats ||
		fasttrun_cache_heap_has_index_publication(heap_relid, false))
		return;
	heap->state.owns_index_relstats = false;
	if (heap->undo == NULL && !fasttrun_cache_entry_keeps_file(heap))
		(void) hash_search(fasttrun_analyze_cache, &heap_relid,
						   HASH_REMOVE, NULL);
}

/*
 * Inside an open transaction a sample saved in column undo still reads the
 * dead-page count: the rollback of an outer level brings it back.
 */
static bool
fasttrun_cache_entry_kept_in_xact(const FasttrunAnalyzeCacheEntry *entry)
{
	return fasttrun_cache_entry_keeps_file(entry) ||
		(!OidIsValid(fasttrun_cache_index_owner(entry)) &&
		 fasttrun_stats_relid_has_saved_samples(entry->relid));
}

/*
 * A heap row after an abort walk: the owner flag stays only with an index
 * publication behind it, now or after the rollback of an outer level, and a
 * row that nothing keeps goes.  An index row or a missing one is left alone.
 */
static void
fasttrun_cache_settle_heap_owner(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

	if (entry == NULL || OidIsValid(fasttrun_cache_index_owner(entry)))
		return;
	if (entry->state.owns_index_relstats && !entry->state.has_relstats &&
		!fasttrun_cache_heap_has_index_publication(relid, true))
		entry->state.owns_index_relstats = false;
	if (!fasttrun_cache_entry_kept_in_xact(entry) &&
		!entry->state.has_stats_baseline &&
		entry->undo == NULL)
		(void) hash_search(fasttrun_analyze_cache, &relid,
						   HASH_REMOVE, NULL);
}

/*
 * The end of an abort walk.
 *
 * Why.  Inside the walk an index entry may be removed or keep its relstats
 * after its heap was restored, in an order set by the hash of the frame.
 * Here every entry of the frame holds its final state.
 *
 * What it does.  An index entry with relstats gives its heap the owner flag,
 * since both reinjection paths reach it through the heap.  A heap without
 * relstats of its own then loses a flag that no index entry backs, and a heap
 * row that nothing keeps goes, the rule the walk applies to index rows.  The
 * heaps settled are the heap rows of the frame and the heaps of its index
 * rows (settle_owner): an index created at this level may hold a publication
 * of an inner level whose frame is gone.
 */
static void
fasttrun_cache_settle_abort_owners(FasttrunXactFrame *frame)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_analyze_cache == NULL)
		return;

	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(xentry->relid);
		FasttrunAnalyzeCacheEntry *heap;

		if (entry == NULL || !entry->state.has_relstats ||
			!OidIsValid(fasttrun_cache_index_owner(entry)))
			continue;
		heap = fasttrun_cache_lookup(fasttrun_cache_index_owner(entry));
		if (heap != NULL)
			heap->state.owns_index_relstats = true;
	}

	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		fasttrun_cache_settle_heap_owner(xentry->relid);
		if (OidIsValid(xentry->settle_owner))
			fasttrun_cache_settle_heap_owner(xentry->settle_owner);
	}
}

/* The heap file the entry's dead-page count describes. */
static inline bool
fasttrun_cache_entry_on_file(const FasttrunAnalyzeCacheEntry *entry,
							 Relation rel)
{
	return entry->state.heap_relid == RelationGetRelid(rel) &&
		RelFileLocatorEquals(rel->rd_locator, entry->state.probe_rlb.locator) &&
		rel->rd_backend == entry->state.probe_rlb.backend;
}

static BlockNumber
fasttrun_cache_aborted_pages(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

	return entry != NULL ? entry->aborted_pages : 0;
}

static inline BlockNumber
fasttrun_cache_dead_since_publish(const FasttrunAnalyzeCacheEntry *entry)
{
	return entry->aborted_pages > entry->state.published_dead ?
		entry->aborted_pages - entry->state.published_dead : 0;
}

/*
 * relpages to put in rd_rel: the published value, scaled up by the share of
 * dead pages added since, so that reltuples / relpages x actual pages counts
 * the live-bearing pages only.
 */
static BlockNumber
fasttrun_cache_effective_relpages(Relation rel, FasttrunAnalyzeCacheEntry *entry)
{
	BlockNumber dead = fasttrun_cache_dead_since_publish(entry);
	BlockNumber phys;

	if (dead == 0 || entry->state.cached_pages == 0)
		return entry->state.cached_pages;
	phys = RelationGetNumberOfBlocks(rel);
	if (phys <= dead)
		return entry->state.cached_pages;
	return (BlockNumber) Min((double) (MaxBlockNumber - 1),
							 ceil((double) entry->state.cached_pages * phys /
								  (double) (phys - dead)));
}

/* After an abort-time invalidation every plan is rebuilt on the restored values. */
static void
fasttrun_cache_anchor_abort_invalidation(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

	if (entry == NULL || !entry->state.has_relstats)
		return;
	entry->state.last_inval_pages = entry->state.cached_pages;
	entry->state.last_inval_tuples = (float4) entry->state.cached_tuples;
	entry->state.last_inval_dead = entry->state.published_dead;
	entry->state.last_inval_valid = true;
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
 * The walk covers only the relids this transaction touched, and it probes no
 * storage and sends no invalidations.  Probing storage and walking the plan
 * cache for every cached entry would cost N x (smgr call + plan-cache walk)
 * per COMMIT, which dominates CPU on a backend with many ON COMMIT DELETE
 * ROWS temp tables that get re-analyzed every xact.
 *
 * Empty-storage adoption happens lazily instead, in
 * fasttrun_reinject_relstats() on the planner_hook path: at most once per
 * relation per query, and only when the planner actually touches the table.
 *
 * What this function does:
 *   - walks the per-xact touched-relid list, not the whole cache
 *   - drops entries whose relation has vanished (the long-lived
 *     backend case -- ON COMMIT DROP temp tables die in
 *     PreCommit_on_commit_actions() before this callback fires, so without
 *     explicit cleanup the entry would stay forever and the backend
 *     would slowly grow)
 *   - frees per-subxact undo chains attached to surviving entries
 *   - carries the delta-math snapshot over the commit, see below
 *   - drops entries that lost their planner-visible relstats
 *     (has_relstats == false -- happens after DDL/DROP eviction).
 *
 * Carrying the delta state.  The per-transaction pgstat counters restart from
 * zero in the next transaction, while the counters the delta math was
 * anchored to belong to this one.  This callback runs before
 * AtEOXact_PgStat, so the final counters of the transaction are still
 * readable.  Subtracting them from every counter anchor moves the anchors to
 * the next transaction's origin, and the rows written since the last analyze
 * keep counting: the first analyze of the next transaction takes the delta
 * path instead of a scan, and churn below the refresh threshold adds up across
 * commits.  This holds only if every transaction that writes the relation
 * reaches this callback with it marked, so the executor and COPY paths mark
 * every write target that has a delta state.  Without the final counters, or
 * after a core TRUNCATE since the last analyze, the delta state is dropped and
 * the next analyze scans.
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
	int64		ins;
	int64		upd;
	int64		del;
	bool		truncdropped;

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
			Oid			owner = fasttrun_cache_index_owner(entry);

			fasttrun_analyze_free_undo(entry);
			(void) hash_search(fasttrun_analyze_cache, &relid,
							   HASH_REMOVE, NULL);
			if (OidIsValid(owner) && !fasttrun_xact_relid_dropped(owner))
				fasttrun_cache_recheck_index_owner(owner);
			continue;
		}

		fasttrun_analyze_free_undo(entry);
		entry->state_subid = InvalidSubTransactionId;

		if (entry->state.has_delta_state &&
			fasttrun_read_pgstat_counters_at_commit(relid, &ins, &upd, &del,
													&truncdropped) &&
			truncdropped == entry->state.cached_truncdropped)
		{
			fasttrun_count(FTC_COMMIT_CARRIED);
			entry->state.cached_inserted -= ins;
			entry->state.cached_updated -= upd;
			entry->state.cached_deleted -= del;
			entry->state.cached_truncdropped = false;

			if (entry->state.has_stats_baseline &&
				entry->state.stats_baseline_truncdropped == truncdropped)
			{
				entry->state.stats_baseline_inserted -= ins;
				entry->state.stats_baseline_updated -= upd;
				entry->state.stats_baseline_deleted -= del;
				entry->state.stats_baseline_truncdropped = false;
			}
			else
				entry->state.has_stats_baseline = false;

			if (entry->state.partial_scan_valid)
			{
				entry->state.partial_scan_inserted -= ins;
				entry->state.partial_scan_updated -= upd;
				entry->state.partial_scan_deleted -= del;
			}
		}
		else
		{
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

			fasttrun_cache_reset_partial_scan_anchor(entry);
		}

		/*
		 * Drop the lazy-probe memo.  ON COMMIT DELETE ROWS truncate runs
		 * in PreCommit_on_commit_actions(), before this callback.  The
		 * cached observation no longer holds outside this xact's lifetime.
		 */
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_epoch = 0;
		entry->lazy_check_pages = 0;

		if (!fasttrun_cache_entry_keeps_file(entry))
		{
			Oid			owner = fasttrun_cache_index_owner(entry);

			(void) hash_search(fasttrun_analyze_cache, &relid,
							   HASH_REMOVE, NULL);
			if (OidIsValid(owner) && !fasttrun_xact_relid_dropped(owner))
				fasttrun_cache_recheck_index_owner(owner);
		}
	}

	/*
	 * When the cache is empty, drop the HTAB and its mcxt so the planner-hook
	 * reinject walk returns to the zero-cost `cache == NULL` path.  Re-armed
	 * lazily on the next analyze.
	 */
	if (hash_get_num_entries(fasttrun_analyze_cache) == 0)
		fasttrun_cache_reset();
}

/*
 * At backend exit the core drops the temporary relations in
 * RemoveTempRelationsCallback, and every relcache message of that drop walks
 * all saved plans of the backend.  Once the plans are marked invalid, each
 * visit stops at the is_valid check.  Exit callbacks run in reverse
 * registration order, and the core registers its callback at the commit that
 * creates the temporary namespace, after the XACT_EVENT_COMMIT callbacks.  So
 * the commit callback only notes that the namespace exists, and the next
 * statement that reaches the executor or utility hook registers the fasttrun
 * callback, which then runs before the core cleanup.  A backend with no such
 * statement after that commit keeps the plain exit.  Registration stays out of
 * the commit callback: running out of exit callback slots is FATAL, which must
 * not happen after the commit record.
 */
static bool fasttrun_exit_plan_reset_pending = false;
static bool fasttrun_exit_plan_reset_registered = false;

static void
fasttrun_exit_plan_reset(int code, Datum arg)
{
	/* Fixed text so that scripts/check_fasttrun_exit_plan_reset.sh finds it. */
	elog(DEBUG1, "fasttrun: marking cached plans invalid before temporary relation cleanup");
	ResetPlanCache();
}

static void
fasttrun_exit_plan_reset_note_commit(void)
{
	Oid			temp_namespace;
	Oid			temp_toast_namespace;

	if (fasttrun_exit_plan_reset_registered || fasttrun_exit_plan_reset_pending)
		return;
	GetTempNamespaceState(&temp_namespace, &temp_toast_namespace);
	if (OidIsValid(temp_namespace))
		fasttrun_exit_plan_reset_pending = true;
}

static void
fasttrun_exit_plan_reset_register(void)
{
	if (likely(!fasttrun_exit_plan_reset_pending))
		return;
	fasttrun_exit_plan_reset_pending = false;
	fasttrun_exit_plan_reset_registered = true;
	before_shmem_exit(fasttrun_exit_plan_reset, (Datum) 0);
}

/* Finish analyze + stats caches on xact end.  PRE_COMMIT skipped -- txn may still abort. */
static void
fasttrun_xact_callback(XactEvent event, void *arg)
{
	if (fasttrun_passthrough)
		return;
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			/* A parallel worker's temporary namespace belongs to its leader. */
			if (event == XACT_EVENT_COMMIT)
				fasttrun_exit_plan_reset_note_commit();
			fasttrun_poison_commit_xact();
			fasttrun_cache_commit_xact();
			fasttrun_stats_cache_commit_xact();
			fasttrun_stats_forget_in_flight();
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
			fasttrun_stats_forget_in_flight();
			break;
		case XACT_EVENT_PREPARE:
			/* Prepared xacts cannot retain backend-private temp statistics. */
			fasttrun_cache_reset();
			fasttrun_stats_cache_reset();
			fasttrun_stats_forget_in_flight();
			break;
		default:
			return;
	}

	/* Frame contexts are children of TopTransactionContext. */
	fasttrun_xact_frame = NULL;
	fasttrun_xact_frame_pool = NULL;
	fasttrun_xact_frame_pool_used = 0;
	fasttrun_xact_epoch++;
	fasttrun_unobserved_writers = false;
	fasttrun_counters_flush();
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

