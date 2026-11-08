/*-------------------------------------------------------------------------
 *
 * src/15_pgstat_read.c
 *      Reads of pending pgstat counters.
 *
 * What it holds.  The counters of the current transaction for a relation, at
 * plan time and at commit, and the relation size read through smgr at commit.
 *
 * Also here.  fasttrun_relation_has_wide_writers, the check for triggers and
 * stored generated columns.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/15_pgstat_read.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Sum tuples_inserted/_updated/_deleted and OR truncdropped over the
 * full active subxact stack.  Pass upd=NULL on hot paths that don't
 * care about UPDATE (rolling-delta math).  The column-stats freshness
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
	 * relation.  Associate lazily before deciding pgstat is unavailable.
	 * Otherwise a harmless relcache invalidation would make cached column
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

		/*
		 * A truncating level replaces the counters of the levels above it
		 * when it commits instead of adding to them (pgstat's
		 * AtEOSubXact_PgStat_Relations), so those never count.  Summing them
		 * would leave an anchor that no longer matches after RELEASE.
		 */
		if (trans->truncdropped)
		{
			*truncdropped = true;
			break;
		}
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

	/* No pending entry: nothing in this transaction counted a write. */
	ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId, relid);
	if (ref == NULL || ref->pending == NULL)
		return true;

	tabstat = (PgStat_TableStatus *) ref->pending;
	for (trans = tabstat->trans; trans != NULL; trans = trans->upper)
	{
		*ins += trans->tuples_inserted;
		*upd += trans->tuples_updated;
		*del += trans->tuples_deleted;
		/* Same rule as fasttrun_read_pgstat_counters. */
		if (trans->truncdropped)
		{
			*truncdropped = true;
			break;
		}
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
	/*
	 * A local temp locator has one owning backend.  Core truncation leaves
	 * its live SMgrRelation cache at zero.  A later smgrextend either advances
	 * that zero or invalidates it.  Therefore zero is authoritative here and
	 * avoids a filesystem size probe for ON COMMIT DELETE ROWS.  Nonzero and
	 * unknown cache states still take the exact path below.
	 */
	if (reln->smgr_cached_nblocks[MAIN_FORKNUM] == 0)
		return 0;
	if (!smgrexists(reln, MAIN_FORKNUM))
		return 0;

	return smgrnblocks(reln, MAIN_FORKNUM);
}

/*
 * Can this relation change a column that the statement did not name?  A BEFORE
 * ROW trigger may assign to any field of NEW, and a stored generated column is
 * recomputed outside the target list.  Either makes the per-statement UPDATE
 * mask an incomplete account of what changed.
 */
static bool
fasttrun_relation_has_wide_writers(Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);

	return rel->rd_rel->relhastriggers ||
		(tupdesc->constr != NULL && tupdesc->constr->has_generated_stored);
}

static bool
fasttrun_read_pgstat_counters_for_hook(Oid relid, bool *wide_writers,
									   int64 *ins, int64 *upd, int64 *del,
									   bool *truncdropped, BlockNumber *pages)
{
	FasttrunFreshnessCacheEntry *slot = NULL;
	Relation rel;
	bool	have;

	*pages = 0;
	*wide_writers = false;

	if (fasttrun_in_planner)
	{
		slot = fasttrun_freshness_cache_slot(relid);
		if (slot->counters_cached && (upd == NULL || slot->upd_cached))
		{
			*wide_writers = slot->wide_writers;
			*ins = slot->ins;
			if (upd != NULL)
				*upd = slot->upd;
			*del = slot->del;
			*truncdropped = slot->truncdropped;
			*pages = slot->pages;
			return slot->have;
		}
	}

#ifdef USE_ASSERT_CHECKING
	fasttrun_test_counter_opens++;
#endif
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
		 * Physical size backs up the counters against writes the hooks did
		 * not see.  Read it in the same relation open the freshness path
		 * already pays for.
		 */
		if (RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
			*pages = RelationGetNumberOfBlocks(rel);
		/*
		 * Re-read in the relation open the freshness path already pays for: a
		 * trigger created after the last collect must not leave the UPDATE
		 * mask trusted.
		 */
		*wide_writers = fasttrun_relation_has_wide_writers(rel);
		RelationClose(rel);
	}

	if (slot != NULL)
	{
		slot->counters_cached = true;
		slot->wide_writers = *wide_writers;
		slot->have = have;
		slot->ins = *ins;
		slot->upd = (upd != NULL) ? *upd : 0;
		slot->upd_cached = (upd != NULL);
		slot->del = *del;
		slot->truncdropped = *truncdropped;
		slot->pages = *pages;
	}

	return have;
}

