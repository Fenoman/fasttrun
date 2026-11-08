/*-------------------------------------------------------------------------
 *
 * src/07_reinject.c
 *      Reinjection of cached relstats into the relcache.
 *
 * What it holds.  The code that writes cached relpages and reltuples back into
 * rd_rel after a relcache rebuild, for a relation and for the range table
 * entries of a query.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/07_reinject.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
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
	BlockNumber relpages;

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
	 * The truth is observed at planning time: if storage went to zero since
	 * we cached, adopt that fact in place.  One smgrnblocks call per cached
	 * relid per query -- and only for relations the planner actually
	 * touches.  An eager pass in fasttrun_cache_commit_xact() would read
	 * nblocks for every cached entry, O(cache_size) per COMMIT on a backend
	 * holding many ON COMMIT DELETE ROWS temp tables.
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
			entry->lazy_check_epoch == fasttrun_xact_epoch &&
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
				BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

				if (nblocks == 0)
					looks_empty = true;
				entry->probe_size = nblocks;
				entry->probe_size_seq = fasttrun_probe_size_stays(rel) ?
					fasttrun_statement_seq : 0;
				entry->probe_size_epoch = fasttrun_xact_epoch;
			}

			if (looks_empty)
			{
				/* Count a table found emptied, not a known empty state. */
				if (entry->state.heap_relid == RelationGetRelid(rel) &&
					entry->state.cached_tuples > 0)
					fasttrun_count(FTC_STORAGE_FOUND_EMPTY);
				/*
				 * Heap gives 0.  Index gives 1 -- the freshly rebuilt
				 * btree/hash metapage stays put.  One nblocks call returns
				 * the right number for either case.
				 */
				entry->state.cached_pages = RelationGetNumberOfBlocks(rel);
				entry->state.published_dead = entry->aborted_pages;
				entry->state.published_aborted_writes = entry->aborted_writes;
				entry->state.cached_tuples = 0;
				entry->state.count_exact = true;
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
			entry->lazy_check_epoch = fasttrun_xact_epoch;
			entry->lazy_check_pages = entry->state.cached_pages;
		}
	}

	cached_tuples = (float4) entry->state.cached_tuples;

	relpages = fasttrun_cache_effective_relpages(rel, entry);
	if (rel->rd_rel->relpages == relpages &&
		rel->rd_rel->reltuples == cached_tuples &&
		rel->rd_rel->relallvisible == (int32) entry->state.cached_allvisible)
		return;

	rel->rd_rel->relpages = relpages;
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
fasttrun_reinject_relid_relstats(Oid relid)
{
	Relation	rel;
	List	   *index_oids;
	ListCell   *lc;

	/*
	 * Most queries either do not touch temp tables or touch a temp table that
	 * fasttrun never analyzed.  Avoid relation_open on that common miss.
	 */
	if (fasttrun_cache_lookup(relid) == NULL)
		return;

	rel = RelationIdGetRelation(relid);
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

static void
fasttrun_reinject_rte_relstats(RangeTblEntry *rte)
{
	if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
		return;
	fasttrun_reinject_relid_relstats(rte->relid);
}

