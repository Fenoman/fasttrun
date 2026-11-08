/*-------------------------------------------------------------------------
 *
 * src/10_stats_relid.c
 *      Column stats cache: init, relid entries, memory budget.
 *
 * What it holds.  Creation and reset of the column stats cache, queries over
 * its relid entries, the memory budget check of a collect, and the UPDATE
 * target accounting of fasttrun_stats_note_update_target.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/10_stats_relid.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
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

/* True when the relid holds column samples the planner may be served. */
static bool
fasttrun_stats_relid_has_samples(Oid relid)
{
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL)
		return false;

	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	return entry != NULL && entry->refcount > 0;
}

/* A sample that the rollback of an open level brings back to a column. */
static bool
fasttrun_stats_relid_has_saved_samples(Oid relid)
{
	FasttrunStatsRelidEntry *relentry;
	ListCell   *lc;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return false;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL)
		return false;
	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsEntry *entry;
		FasttrunStatsSavedState *saved;

		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, lfirst(lc), HASH_FIND, NULL);
		if (entry == NULL)
			continue;
		for (saved = entry->undo; saved != NULL; saved = saved->older)
		{
			if (saved->state == FASTTRUN_COLUMN_LOCAL_CANDIDATE &&
				saved->statsTuple != NULL)
				return true;
		}
	}
	return false;
}

/* True when the relid already holds column-stats keys in the cache. */
static bool
fasttrun_stats_relid_has_columns(Oid relid)
{
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL)
		return false;

	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	return entry != NULL && entry->attkeys != NIL;
}

/* Common memory counter: NULL reads as zero. */
static Size
fasttrun_memory_context_bytes(MemoryContext mcxt)
{
	return mcxt != NULL ? MemoryContextMemAllocated(mcxt, true) : 0;
}

/*
 * Soft cap on the column-stats cache: fasttrun.max_stats_memory.
 *
 * Returns true when a first-time collection for rel must be skipped: the
 * cap is set, the relation holds no cached column stats yet, and the
 * cache memory is already over the cap.  The relation then behaves as
 * with fasttrun.auto_collect_stats = off -- relation-level statistics
 * keep working, column stats are absent.  Relations that already hold
 * cached column stats keep refreshing without this check, and nothing is
 * ever evicted.
 *
 * Signals: the auto-collect path warns once per backend, while an explicit
 * fasttrun_collect_stats() call gets a NOTICE every time.
 */
static bool
fasttrun_stats_budget_blocks_collect(Relation rel, bool explicit_collect)
{
	if (fasttrun_max_stats_memory <= 0)
		return false;
	if (fasttrun_stats_relid_has_columns(RelationGetRelid(rel)))
		return false;
	if (fasttrun_memory_context_bytes(fasttrun_stats_mcxt) <=
		(Size) fasttrun_max_stats_memory * 1024)
		return false;

	if (explicit_collect)
		ereport(NOTICE,
				(errmsg("fasttrun_collect_stats: column statistics for \"%s\" not cached, cache exceeds fasttrun.max_stats_memory",
						RelationGetRelationName(rel))));
	else if (!fasttrun_warned_stats_budget)
	{
		ereport(WARNING,
				(errmsg("fasttrun: column statistics cache exceeds fasttrun.max_stats_memory, new tables are left without column statistics"),
				 errdetail("A table without cached column statistics behaves as with fasttrun.auto_collect_stats = off: relation-level statistics keep working, the planner falls back to default selectivity. Tables that already hold cached column statistics keep refreshing, and nothing is evicted."),
				 errhint("Raise fasttrun.max_stats_memory, or recycle the connection to free the cache. fasttrun_cache_stats() reports the current cache size.")));
		fasttrun_warned_stats_budget = true;
	}
	return true;
}

static void
fasttrun_stats_relid_init(FasttrunStatsRelidEntry *entry)
{
	entry->refcount = 0;
	entry->attkeys = NIL;
	entry->updated_cols = NULL;
	entry->updated_cols_exact = false;
	entry->period_unsafe = false;
	entry->collect_seq = 0;
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

/*
 * Record the columns a statement names as UPDATE targets.  pgstat reports
 * tuples_updated per relation, so without this the churn of a single-column
 * UPDATE ages the statistics of every column -- and an UPDATE that only
 * enriches a helper column would blind the planner on the join keys it never
 * touched.  Members are kept in RTEPermissionInfo.updatedCols offset form.
 *
 * A target without permission information (an inheritance child reached
 * through its parent) hides which columns the statement writes, so the mask
 * stops being a full account and the relation falls back to relation-wide
 * churn until the next collect.
 */
static void
fasttrun_stats_note_update_target(Oid relid, RTEPermissionInfo *perminfo)
{
	FasttrunStatsRelidEntry *entry;
	MemoryContext oldcxt;

	if (fasttrun_stats_relid_cache == NULL || fasttrun_stats_mcxt == NULL)
		return;

	/* INSERT and DELETE targets name no columns -- leave before the lookup. */
	if (perminfo != NULL && bms_is_empty(perminfo->updatedCols))
		return;

	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (entry == NULL)
		return;

	if (perminfo == NULL)
	{
		entry->updated_cols_exact = false;
		return;
	}

	oldcxt = MemoryContextSwitchTo(fasttrun_stats_mcxt);
	entry->updated_cols = bms_add_members(entry->updated_cols,
										  perminfo->updatedCols);
	MemoryContextSwitchTo(oldcxt);
}

