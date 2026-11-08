/*-------------------------------------------------------------------------
 *
 * src/12_stats_relid_life.c
 *      Column stats cache: collect periods and relid entry life cycle.
 *
 * What it holds.  The numbered collect period, undo of relid entries,
 * reference counts, column keys, the relation policy and the check whether
 * core statistics exist for a relation.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/12_stats_relid_life.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Open a new collect period: the fresh samples about to be stored carry the new
 * sequence, and the mask starts empty.  Samples left behind by an earlier period
 * -- one the collect skipped, or one a savepoint rollback restores later -- keep
 * their old sequence and stay outside the mask's account.
 *
 * A trigger may write columns the statement does not name, and a stored generated
 * column is recomputed outside the target list.  On such a relation the mask is
 * not a full account.  That check is re-read here and again whenever a plan asks
 * for this relation's statistics, so a trigger created between two collects is
 * covered.
 *
 * An UPDATE of this relation that has started and not finished is the other way
 * the mask misses a write: the samples about to be taken describe rows the
 * statement will still overwrite, and the columns it names were just cleared.
 *
 * Returns true when the relation is being written now: the samples of this
 * period are then stamped unsafe and never serve the planner.
 */
static bool
fasttrun_stats_begin_collect_period(Relation rel)
{
	FasttrunStatsRelidEntry *entry;
	bool		found;
	bool		wide;

	if (fasttrun_stats_relid_cache == NULL)
		return false;

	entry = fasttrun_stats_relid_enter(RelationGetRelid(rel), &found);
	entry->collect_seq++;

	/*
	 * The mask accounts for the new period only when nothing can write outside
	 * it: no UPDATE statement is running underneath this collect (one would
	 * write rows after the sample is taken and never return to the hook), and
	 * the relation cannot change a column no statement names.  A mask left by
	 * an UPDATE that already finished says nothing about the new period and is
	 * simply dropped.
	 */
	if (fasttrun_lazy_loaded && !fasttrun_lazy_loaded_warned)
	{
		/*
		 * Only interactive sessions load session_preload_libraries.  Any other
		 * process -- a background worker above all -- is reached by
		 * shared_preload_libraries alone, and short-lived workers would repeat
		 * the line thousands of times a day, so it reports at DEBUG1.
		 */
		bool		interactive = (MyBackendType == B_BACKEND ||
								   MyBackendType == B_STANDALONE_BACKEND);

		fasttrun_lazy_loaded_warned = true;
		ereport(interactive ? LOG : DEBUG1,
				(errmsg("fasttrun: per-column UPDATE accounting is off in this session"),
				 errdetail("The library was loaded on first use, so statements that had already started were never seen."),
				 interactive ?
				 errhint("Add fasttrun to session_preload_libraries or shared_preload_libraries.") :
				 errhint("This process does not load session_preload_libraries, so add fasttrun to shared_preload_libraries.")));
	}

	wide = fasttrun_relation_has_wide_writers(rel);
	entry->period_unsafe = fasttrun_stats_relation_written_now(rel);
	entry->updated_cols_exact = !fasttrun_lazy_loaded &&
		!entry->period_unsafe && !wide;


	bms_free(entry->updated_cols);
	entry->updated_cols = NULL;
	return entry->period_unsafe;
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
	{
		bms_free(entry->updated_cols);
		entry->updated_cols = NULL;
		(void) hash_search(fasttrun_stats_relid_cache, &relid,
						   HASH_REMOVE, NULL);
	}
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
	FasttrunStatsRelidEntry *volatile entry = NULL;
	bool	found;
	MemoryContext oldcxt = CurrentMemoryContext;
	FasttrunStatsKey *volatile kcopy = NULL;

	if (fasttrun_stats_relid_cache == NULL)
		return;

	PG_TRY();
	{
		entry = fasttrun_stats_relid_enter(key->relid, &found);
		FASTTRUN_TEST_FAILPOINT("after_stats_relid_enter", 0);

		MemoryContextSwitchTo(fasttrun_stats_mcxt);
		kcopy = (FasttrunStatsKey *) palloc(sizeof(*kcopy));
		*((FasttrunStatsKey *) kcopy) = *key;
		entry->attkeys = lappend(entry->attkeys,
								   (FasttrunStatsKey *) kcopy);
		kcopy = NULL;
		MemoryContextSwitchTo(oldcxt);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		if (kcopy != NULL)
			pfree((FasttrunStatsKey *) kcopy);
		if (entry != NULL)
			fasttrun_stats_relid_maybe_drop(key->relid,
										 (FasttrunStatsRelidEntry *) entry);
		PG_RE_THROW();
	}
	PG_END_TRY();
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
			k->inh == key->inh &&
			k->indexoid == key->indexoid)
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
	{
		fasttrun_stats_forget_relid(relid);
		entry = NULL;
	}
	changed = entry == NULL || entry->policy != policy ||
		!entry->heap_rlb_valid || !fasttrun_rlb_equals(entry->heap_rlb, rlb);
	if (!changed)
		return false;

	/*
	 * Mark before inserting or mutating so a mid-operation error stays
	 * conservative and rollback can find every published state.
	 */
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);

	if (entry != NULL)
		fasttrun_stats_relid_save_undo(entry);
	else
	{
		entry = fasttrun_stats_relid_enter(relid, &found);
		Assert(!found);
		entry->state_subid = cur_subid;
	}

	entry->policy = policy;
	entry->heap_rlb = rlb;
	entry->heap_rlb_valid = true;
	return changed;
}

/*
 * Probe the catalog only on an explicit state change, never from hooks.  One
 * index scan by starelid instead of a syscache lookup per column: a temporary
 * table usually has no pg_statistic rows, and every missed lookup would leave
 * a negative catcache entry for the rest of the backend's life.
 */
static bool
fasttrun_relid_has_core_stats(Relation statrel, Oid relid, int natts)
{
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	bool		found = false;

	ScanKeyInit(&key, Anum_pg_statistic_starelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));
	scan = systable_beginscan(statrel, StatisticRelidAttnumInhIndexId, true,
							  NULL, 1, &key);
	while (!found && HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_statistic form = (Form_pg_statistic) GETSTRUCT(tuple);

		found = (!form->stainherit &&
				 form->staattnum >= 1 && form->staattnum <= natts);
	}
	systable_endscan(scan);
	return found;
}

static bool
fasttrun_relation_has_core_stats(Relation rel)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Relation	statrel;
	List	   *index_oids;
	ListCell   *lc;
	bool		found;

	statrel = table_open(StatisticRelationId, AccessShareLock);
	found = fasttrun_relid_has_core_stats(statrel, RelationGetRelid(rel),
										  desc->natts);

	/* Expression statistics are stored under the index OID. */
	index_oids = found ? NIL : RelationGetIndexList(rel);
	foreach(lc, index_oids)
	{
		Oid			index_oid = lfirst_oid(lc);
		HeapTuple	reltuple;
		int			natts;

		reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(index_oid));
		if (!HeapTupleIsValid(reltuple))
			continue;
		natts = ((Form_pg_class) GETSTRUCT(reltuple))->relnatts;
		ReleaseSysCache(reltuple);
		if (fasttrun_relid_has_core_stats(statrel, index_oid, natts))
		{
			found = true;
			break;
		}
	}
	list_free(index_oids);
	table_close(statrel, AccessShareLock);
	return found;
}

/* freefunc for VariableStatData -- we own the tuple, no-op. */
static void
fasttrun_stats_noop_free(HeapTuple tuple)
{
}

