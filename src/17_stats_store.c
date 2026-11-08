/*-------------------------------------------------------------------------
 *
 * src/17_stats_store.c
 *      Column stats entries: state, store, commit, eviction.
 *
 * What it holds.  Candidate and saved states of a column entry, storing a new
 * sample, neutralization, the commit-time carry-over of the column cache,
 * eviction, rebinding after a table rewrite and the visibility notes that
 * drive plan invalidation.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/17_stats_store.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
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
	saved->collected_dead = entry->collected_dead;
	saved->was_usable = entry->was_usable;
	saved->collect_seq = entry->collect_seq;
	saved->unsafe = entry->unsafe;
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
fasttrun_stats_set_entry_state(Relation rel, AttrNumber attnum, bool inh,
							   Oid indexoid,
							   FasttrunColumnStatsState state,
							   HeapTuple tuple,
							   const FasttrunPgstatSnapshot *snapshot)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry = NULL;
	Oid				relid = RelationGetRelid(rel);
	bool				found;
	bool				old_candidate;
	bool				new_candidate;
	bool				changed;
	HeapTuple volatile staged_tuple = NULL;
	MemoryContext		oldcxt = CurrentMemoryContext;
	SubTransactionId	cur_subid = GetCurrentSubTransactionId();

	fasttrun_stats_cache_init();
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_COLSTATS);

	fasttrun_stats_key_init(&key, relid, attnum, inh);
	key.indexoid = indexoid;

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	found = entry != NULL;

	old_candidate = found && fasttrun_stats_entry_is_candidate(entry);
	new_candidate = (state == FASTTRUN_COLUMN_LOCAL_CANDIDATE &&
					 tuple != NULL);
	changed = !found || entry->state != state ||
		old_candidate != new_candidate;

	PG_TRY();
	{
		bool	entered;

		if (new_candidate)
		{
			MemoryContextSwitchTo(fasttrun_stats_mcxt);
			FASTTRUN_TEST_FAILPOINT("before_stats_tuple_copy", 0);
			staged_tuple = heap_copytuple(tuple);
			MemoryContextSwitchTo(oldcxt);
		}

		if (!found)
		{
			entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
													   &key, HASH_ENTER, &entered);
			Assert(!entered);
			entry->statsTuple = NULL;
			entry->collected_ins = 0;
			entry->collected_upd = 0;
			entry->collected_del = 0;
			entry->collected_truncdropped = false;
			entry->collected_pages = 0;
			entry->collected_dead = 0;
			entry->was_usable = false;
			entry->state = FASTTRUN_COLUMN_CORE_ALLOWED;
			memset(&entry->heap_rlb, 0, sizeof(entry->heap_rlb));
			entry->heap_rlb_valid = false;
			entry->state_subid = cur_subid;
			entry->undo = NULL;
			fasttrun_stats_relid_add_key(&key);
		}

		/* A state or tuple refresh changes planner-visible statistics. */
		if (changed || new_candidate)
			fasttrun_xact_mark_relid(relid, relid,
									FASTTRUN_TOUCH_PLAN_INVALIDATE);

		if (old_candidate || new_candidate)
		{
			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid,
							HASH_FIND, NULL);
			if (relentry == NULL)
				elog(ERROR, "fasttrun stats back-reference is missing");
		}

		if (found && entry->state_subid != cur_subid)
			fasttrun_stats_entry_save_undo(entry);
		else if (found && entry->statsTuple != NULL)
		{
			heap_freetuple(entry->statsTuple);
			entry->statsTuple = NULL;
		}

		if (old_candidate && !new_candidate)
		{
			Assert(relentry->refcount > 0);
			if (relentry->refcount > 0)
				relentry->refcount--;
		}
		else if (!old_candidate && new_candidate)
			relentry->refcount++;

		if (new_candidate)
		{
			entry->statsTuple = (HeapTuple) staged_tuple;
			staged_tuple = NULL;
		}
		entry->state = state;
		entry->heap_rlb.locator = rel->rd_locator;
		entry->heap_rlb.backend = rel->rd_backend;
		entry->heap_rlb_valid = true;
		entry->state_subid = cur_subid;
		entry->was_usable = new_candidate;
		/*
		 * Stamp the sample with the relation's current collect period.  A
		 * non-candidate carries no sample and is never asked, so leaving its
		 * old stamp alone is harmless.
		 */
		if (relentry != NULL)
		{
			entry->collect_seq = relentry->collect_seq;
			entry->unsafe = relentry->period_unsafe;
		}
		if (snapshot != NULL)
		{
			entry->collected_ins = snapshot->inserted;
			entry->collected_upd = snapshot->updated;
			entry->collected_del = snapshot->deleted;
			entry->collected_truncdropped = snapshot->truncdropped;
			entry->collected_pages = snapshot->pages;
			entry->collected_dead = fasttrun_cache_aborted_pages(relid);
		}
		else
		{
			entry->collected_ins = 0;
			entry->collected_upd = 0;
			entry->collected_del = 0;
			entry->collected_truncdropped = false;
			entry->collected_pages = 0;
			entry->collected_dead = 0;
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		if (staged_tuple != NULL)
			heap_freetuple((HeapTuple) staged_tuple);
		if (!found)
		{
			(void) hash_search(fasttrun_stats_cache, &key,
							   HASH_REMOVE, NULL);
			fasttrun_stats_relid_drop_key(&key);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return changed;
}

static bool
fasttrun_stats_set_column_state(Relation rel, AttrNumber attnum, bool inh,
								FasttrunColumnStatsState state,
								HeapTuple tuple,
								const FasttrunPgstatSnapshot *snapshot)
{
	return fasttrun_stats_set_entry_state(rel, attnum, inh, InvalidOid, state,
										  tuple, snapshot);
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
	bms_free(relentry->updated_cols);
	relentry->updated_cols = NULL;
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

	/* Hiding stats is planner-visible, so mark before mutating. */
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_COLSTATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);

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
	entry->collected_dead = 0;
	return true;
}

/*
 * COMMIT-time bookkeeping for the per-(relid, attnum) column-stats cache.
 *
 * The cost follows the relations this transaction touched, not the cache
 * size.  Opening the relation, reading pgstat and invalidating plans for
 * every cached entry whose pgstat snapshot has drifted -- the common case
 * for ON COMMIT DELETE ROWS temp tables re-analyzed every xact -- would walk
 * all cached plans once per stale entry, N x M work on every COMMIT.
 *
 * What this function does:
 *   - walks the per-xact touched-relid list, then the attkeys backref
 *     for each one -- O(touched * cached-cols-per-rel), not
 *     O(total-stats-entries)
 *   - reads pgstat counters once per relid, not once per attkey
 *     (a 20-column temp table costs one read, not 20)
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
		List	   *unlink_keys = NIL;
		ListCell   *klc;
		int64		ins_now = 0;
		int64		upd_now = 0;
		int64		del_now = 0;
		bool		truncdropped_now = false;
		bool		have_counters = false;
		bool		counters_attempted = false;
		bool		plan_inval_needed = false;
		bool		storage_nonempty = false;
		bool		neutralized = false;
		bool		had_samples;
		bool		samples_left;

		if ((xentry->flags & (FASTTRUN_TOUCH_DML |
							 FASTTRUN_TOUCH_STATS |
							 FASTTRUN_TOUCH_DROPPED)) == 0)
			continue;

		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
		if (relentry == NULL)
			continue;
		had_samples = relentry->refcount > 0;

		/*
		 * Relation dropped this xact (ON COMMIT DROP fires before this
		 * callback, plus any plain DROP earlier in the xact)?  Drop the
		 * column-stats entries.  The OAT_DROP note replaces the syscache
		 * probe: TRANS_COMMIT forbids catalog access.  Symmetric to the
		 * analyze-cache commit path.
		 */
		if (fasttrun_xact_entry_dropped(xentry))
		{
			foreach(klc, relentry->attkeys)
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
			bms_free(relentry->updated_cols);
			relentry->updated_cols = NULL;
			list_free_deep(relentry->attkeys);
			fasttrun_stats_relid_free_undo(relentry);
			(void) hash_search(fasttrun_stats_relid_cache, &relid,
							   HASH_REMOVE, NULL);
			continue;
		}

		/*
		 * The common candidate path never mutates attkeys.  Walk it in place
		 * and defer the rare stale/handoff unlinks until after the loop.  A
		 * list_copy per touched relation would dominate short COMMITs with
		 * many analyzed temp tables.
		 */
		foreach(klc, relentry->attkeys)
		{
			FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(klc);
			FasttrunStatsKey key = *kptr;	/* value copy -- drop_key may pfree source */
			FasttrunStatsEntry *entry;

			entry = (FasttrunStatsEntry *)
				hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
			if (entry == NULL)
			{
				/* Stale backref -- unlink after this live-list walk. */
				unlink_keys = lappend(unlink_keys, kptr);
				continue;
			}

			/* Nothing reads the entry of a dropped column again. */
			if (!OidIsValid(key.indexoid) &&
				bms_is_member(key.attnum, xentry->dropped_attrs))
			{
				if (fasttrun_stats_entry_is_candidate(entry))
					fasttrun_stats_relid_unref(relid);
				fasttrun_stats_entry_free_undo(entry);
				if (entry->statsTuple != NULL)
					heap_freetuple(entry->statsTuple);
				(void) hash_search(fasttrun_stats_cache, &key,
								   HASH_REMOVE, NULL);
				unlink_keys = lappend(unlink_keys, kptr);
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
				 * An index expression entry without a sample carries nothing:
				 * the next collect recreates it, and an index dropped meanwhile
				 * must not leave it behind.
				 */
				if ((entry->state == FASTTRUN_COLUMN_CORE_ALLOWED &&
					 relentry->policy == FASTTRUN_REL_CORE_ALLOWED) ||
					OidIsValid(key.indexoid))
				{
					fasttrun_stats_entry_free_undo(entry);
					if (entry->statsTuple != NULL)
						heap_freetuple(entry->statsTuple);
					(void) hash_search(fasttrun_stats_cache, &key,
									   HASH_REMOVE, NULL);
					unlink_keys = lappend(unlink_keys, kptr);
					continue;
				}
				fasttrun_stats_entry_free_undo(entry);
				entry->state_subid = InvalidSubTransactionId;
				continue;
			}

			/*
			 * Read pgstat once per relid, not once per attkey: all
			 * statsTuples for this relid are compared against the same
			 * (ins, upd, del, truncdropped) counters.  The read is
			 * relcache-free: this callback runs at TRANS_COMMIT, where
			 * catalog access is forbidden.
			 */
			if (!counters_attempted)
			{
				counters_attempted = true;
				have_counters =
					(xentry->flags & FASTTRUN_TOUCH_UNCOUNTED) == 0 &&
					fasttrun_read_pgstat_counters_at_commit(relid, &ins_now,
															&upd_now, &del_now,
															&truncdropped_now);
			}

			/*
			 * Keep statistics across COMMIT while the churn since the collect
			 * stays within the allowed threshold.  If counters are unavailable
			 * or the distribution has changed too much, hide the statistics.
			 * When DML changes visible statistics to hidden on a nonempty
			 * table, invalidate its plans once after the column loop.
			 */
			/*
			 * The physical-size backstop belongs to the planner hook after
			 * pgstat's temp counters reset.  Here the current xact counters are
			 * still live.  Treat an exact counter match as physically stable so
			 * core ON COMMIT DELETE can take the documented lazy-empty path
			 * without one smgrnblocks probe per analyzed relation.
			 */
			if (!have_counters ||
				!fasttrun_stats_entry_usable(relid, relentry, entry, ins_now, upd_now,
										 del_now, truncdropped_now,
										 entry->collected_pages))
			{
				if (entry->was_usable)
					plan_inval_needed = true;
				neutralized = true;
				fasttrun_stats_entry_free_undo(entry);
				fasttrun_stats_relid_unref(key.relid);
				heap_freetuple(entry->statsTuple);
				entry->statsTuple = NULL;
				entry->state = FASTTRUN_COLUMN_LOCAL_NEUTRAL;
				entry->was_usable = false;
				entry->state_subid = InvalidSubTransactionId;
				continue;
			}

			/*
			 * The next transaction counts from zero: move the collect-time
			 * anchors there, so the churn since the collect keeps counting.
			 */
			fasttrun_stats_entry_free_undo(entry);
			entry->collected_ins -= ins_now;
			entry->collected_upd -= upd_now;
			entry->collected_del -= del_now;
			entry->collected_truncdropped = false;
			entry->state_subid = InvalidSubTransactionId;
		}

		foreach(klc, unlink_keys)
		{
			FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(klc);

			fasttrun_stats_relid_unlink_key(relentry, key);
		}
		list_free(unlink_keys);

		/*
		 * A column hidden here has no sample until the next collect, and
		 * the carried counters may show no churn that would start one.
		 * Drop the delta state: the next analyze scans and collects.
		 */
		if (neutralized)
		{
			FasttrunAnalyzeCacheEntry *aentry = fasttrun_cache_lookup(relid);

			if (aentry != NULL)
			{
				aentry->state.has_delta_state = false;
				aentry->state.has_stats_baseline = false;
				fasttrun_cache_reset_partial_scan_anchor(aentry);
			}
		}
		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
		if (relentry == NULL)
			continue;
		fasttrun_stats_relid_free_undo(relentry);
		relentry->state_subid = InvalidSubTransactionId;
		if (plan_inval_needed)
			storage_nonempty =
				fasttrun_stats_relid_nblocks_at_commit(relentry) > 0;
		samples_left = relentry->refcount > 0;
		fasttrun_stats_relid_maybe_drop(relid, relentry);
		/*
		 * The analyze-cache commit already saw a relation that had no samples
		 * at its start.  Only one that lost its last ones here needs the
		 * second look.
		 */
		if (had_samples && !samples_left)
			fasttrun_cache_reclaim_entry(relid);
		/*
		 * ON COMMIT DELETE ROWS has already truncated local storage before
		 * this callback.  Empty storage gets no invalidation here: no rows
		 * can use the cached plan, while the next explicit refresh owns any
		 * needed invalidation after refill.  This is the hot path that must
		 * not turn N tables into N plan-cache walks per COMMIT.  Ordinary
		 * DELETE keeps heap pages and therefore invalidates.
		 */
		if (plan_inval_needed && storage_nonempty)
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
 * this relid -- O(#cached-cols), not a hash_seq_search over the whole
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
 * statsTuple goes to NULL.  The old version is pushed onto the per-entry
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
fasttrun_stats_neutralize_index_expressions(Oid heap_relid, Oid indexoid)
{
	FasttrunStatsRelidEntry *relentry;
	List	   *keys;
	ListCell   *lc;

	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &heap_relid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->attkeys == NIL)
		return;
	keys = list_copy(relentry->attkeys);
	foreach(lc, keys)
	{
		FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(lc);
		FasttrunStatsEntry *entry;

		if (key->indexoid != indexoid)
			continue;
		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);
		if (entry != NULL)
			(void) fasttrun_stats_entry_neutralize(heap_relid, entry);
	}
	list_free(keys);
}

/* VACUUM cut the heap without changing its rows: anchor the size backstop anew. */
static void
fasttrun_stats_relid_set_size_anchor(Oid relid, BlockNumber pages)
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
		FasttrunStatsEntry *entry;

		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, lfirst(lc), HASH_FIND, NULL);
		if (entry != NULL && entry->statsTuple != NULL)
		{
			entry->collected_pages = pages;
			entry->collected_dead = 0;
		}
	}
}

/*
 * A rewrite with the same rows gave the heap a new file.  The samples and the
 * size backstop move to it.  Entries save their undo first, so ROLLBACK TO
 * SAVEPOINT returns them to the old file together with the relation.
 */
static void
fasttrun_stats_rebind_rewritten(Relation rel, const RelFileLocatorBackend *old)
{
	Oid			relid = RelationGetRelid(rel);
	SubTransactionId cur_subid = GetCurrentSubTransactionId();
	MemoryContext oldcxt = CurrentMemoryContext;
	FasttrunStatsRelidEntry *relentry;
	RelFileLocatorBackend rlb;
	BlockNumber pages;
	BlockNumber dead;
	ListCell   *lc;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL || !relentry->heap_rlb_valid ||
		!fasttrun_rlb_equals(relentry->heap_rlb, *old))
		return;

	rlb.locator = rel->rd_locator;
	rlb.backend = rel->rd_backend;
	pages = RelationGetNumberOfBlocks(rel);
	dead = fasttrun_cache_aborted_pages(relid);
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_COLSTATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	fasttrun_stats_relid_save_undo(relentry);
	relentry->heap_rlb = rlb;

	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsEntry *entry;
		HeapTuple volatile copy = NULL;

		entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, lfirst(lc), HASH_FIND, NULL);
		if (entry == NULL || !entry->heap_rlb_valid ||
			!fasttrun_rlb_equals(entry->heap_rlb, *old))
			continue;
		if (entry->state_subid != cur_subid)
		{
			/* The undo takes the tuple, the entry keeps a copy. */
			PG_TRY();
			{
				if (entry->statsTuple != NULL)
				{
					MemoryContextSwitchTo(fasttrun_stats_mcxt);
					copy = heap_copytuple(entry->statsTuple);
					MemoryContextSwitchTo(oldcxt);
				}
				fasttrun_stats_entry_save_undo(entry);
			}
			PG_CATCH();
			{
				MemoryContextSwitchTo(oldcxt);
				if (copy != NULL)
					heap_freetuple((HeapTuple) copy);
				PG_RE_THROW();
			}
			PG_END_TRY();
			entry->statsTuple = (HeapTuple) copy;
		}
		entry->heap_rlb = rlb;
		if (entry->statsTuple != NULL)
		{
			entry->collected_pages = pages;
			entry->collected_dead = dead;
		}
	}
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
		 * missing CORE_ALLOWED overrides here too.  Relation default may still
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

		/* Core keeps index expression statistics under the index oid. */
		if (OidIsValid(key->indexoid))
		{
			FasttrunStatsEntry *entry = (FasttrunStatsEntry *)
				hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);

			if (entry != NULL)
				(void) fasttrun_stats_entry_neutralize(relid, entry);
			continue;
		}
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
 * planner once churn passes the cardinality-scaled tolerance.  An analyze
 * recollects after any DML, but it cannot when the refresh is deferred for a
 * running writer or blocked by the memory budget, or when the column was
 * hidden for a reason the counters do not show (the physical-size backstop).
 * Then new plans fall back to defaults, and a cached generic plan built on
 * the now-hidden distribution would keep running -- the flip must invalidate
 * it.
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
	FasttrunUsableBaseline rel_baseline = {0};

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
			fasttrun_stats_entry_usable_ext(relid, relentry, entry, ins_now, upd_now,
											del_now, truncdropped_now,
											pages_now, &rel_baseline);
		if (entry->was_usable && !usable_now)
		{
			flipped = true;
			(void) fasttrun_stats_entry_neutralize(relid, entry);
		}
	}

	return flipped;
}

