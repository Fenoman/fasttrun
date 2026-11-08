/*-------------------------------------------------------------------------
 *
 * src/18_subxact_callback.c
 *      The subtransaction callback.
 *
 * What it holds.  fasttrun_subxact_callback, one function that rolls back or
 * promotes the journal frame of a finished subtransaction for both caches.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/18_subxact_callback.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * SubXact callback: savepoint-aware undo/restore for both the per-column
 * stats cache and the analyze-cache stats baseline.
 *
 * Per-column stats cache (statsTuple + freshness counters):
 *   ABORT_SUB:
 *     - entry has undo -> pop top saved state back into entry (full
 *       restore of pre-subxact version)
 *     - no undo -> entry was created inside aborting subxact -> remove.
 *   COMMIT_SUB: re-label sub-local entries with the parent subid.  If
 *     the undo top is already at parent level, collapse it.
 *
 * Analyze-cache stats baseline (used by the refresh-path churn check):
 *   ABORT_SUB:
 *     - baseline was set in this subxact and has undo -> pop
 *     - was set in this subxact and no undo -> mark has_stats_baseline
 *       false (it was created from scratch inside the rolled-back
 *       subxact, there is nothing to restore to).
 *   COMMIT_SUB: re-label + collapse undo, same as the stats cache.
 *
 * Analyze-cache relstats + delta state:
 *   ABORT_SUB restores the previous planner-visible relstats snapshot
 *   and invalidates local generic plans that may have been born against
 *   the aborted subxact's stats.  COMMIT_SUB promotes the snapshot to
 *   the parent subxact.
 */
static void
fasttrun_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	FasttrunXactFrame *frame;
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_passthrough ||
		(event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB))
		return;
	if (event == SUBXACT_EVENT_ABORT_SUB)
		fasttrun_statement_seq++;

	if (fasttrun_in_flight_used > 0)
		fasttrun_stats_in_flight_subxact_end(mySubid, parentSubid,
										 event == SUBXACT_EVENT_ABORT_SUB);

	/*
	 * Hot-path shortcut.  Frames are lazy: a subtransaction gets one only
	 * when a relation is marked at its level, a committing child hands its
	 * entries to its parent's level, and an aborting one releases its frame.
	 * So the ending subtransaction owns the innermost frame exactly when it
	 * or a committed descendant marked something.  Without that frame there
	 * is nothing to undo or promote, and savepoint-heavy code that leaves
	 * fasttrun state alone, such as a PL/pgSQL EXCEPTION block in a loop,
	 * skips the walk.
	 */
	frame = fasttrun_xact_frame;
	if (frame == NULL || frame->subid != mySubid)
		return;

	/*
	 * Walk only the ending frame, not every relid touched by sibling
	 * subtransactions.  For each relid we hit
	 * the stats cache through fasttrun_stats_relid_cache.attkeys (O(K) per
	 * relid) and the analyze cache via a single hash_search by relid.
	 * Sibling subtransactions release their frame context when they abort.
	 */
	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		Oid			relid = xentry->relid;
		FasttrunAnalyzeCacheEntry *aentry;
		bool		restored_state = false;
		bool		restored_other_file = false;
		BlockNumber	restored_dead = 0;

#ifdef USE_ASSERT_CHECKING
		fasttrun_test_subxact_visited++;
#endif

		/* Abort removes a newly created relation without an OAT_DROP hook.
		 * Its still-present empty files must not override this lifetime. */
		if (event == SUBXACT_EVENT_ABORT_SUB &&
			(xentry->flags & FASTTRUN_TOUCH_CREATED) != 0)
		{
			aentry = fasttrun_cache_lookup(relid);
			if (aentry != NULL)
			{
				xentry->settle_owner = fasttrun_cache_index_owner(aentry);
				fasttrun_analyze_free_undo(aentry);
				(void) hash_search(fasttrun_analyze_cache, &relid,
								   HASH_REMOVE, NULL);
			}
			fasttrun_stats_forget_relid(relid);
			fasttrun_poison_forget_relid(relid);
			goto finish_entry;
		}

		/*
		 * Walk the column-stats entries only when this frame actually wrote
		 * some (COLSTATS).  A relid touched only through the relstats path
		 * (pure-delta analyze) has no per-column undo to process, and the
		 * list_copy(attkeys) + per-column lookups would be wasted work.
		 */
		if ((xentry->flags & FASTTRUN_TOUCH_COLSTATS) != 0 &&
			fasttrun_stats_cache != NULL && fasttrun_stats_relid_cache != NULL)
		{
			FasttrunStatsRelidEntry *relentry;
			List	   *keys;
			ListCell   *klc;

			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
			if (relentry != NULL && relentry->attkeys != NIL)
			{
				/*
				 * Snapshot attkeys.  Drop paths below pfree list nodes via
				 * fasttrun_stats_relid_drop_key, mutating the live list.
				 */
				keys = list_copy(relentry->attkeys);

				foreach(klc, keys)
				{
					FasttrunStatsKey *kptr = (FasttrunStatsKey *) lfirst(klc);
					FasttrunStatsKey key = *kptr;
					FasttrunStatsEntry *sentry;

					sentry = (FasttrunStatsEntry *)
						hash_search(fasttrun_stats_cache, &key, HASH_FIND, NULL);
					if (sentry == NULL)
					{
						fasttrun_stats_relid_drop_key(&key);
						continue;
					}
					if (sentry->state_subid != mySubid)
						continue;

					if (event == SUBXACT_EVENT_ABORT_SUB)
					{
						if (sentry->undo != NULL)
						{
							/* Restore pre-subxact version from the undo stack. */
							FasttrunStatsSavedState *popped = sentry->undo;

							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_unref(relid);
							if (sentry->statsTuple != NULL)
							{
								heap_freetuple(sentry->statsTuple);
							}
							sentry->statsTuple = popped->statsTuple;
							sentry->collected_ins = popped->collected_ins;
							sentry->collected_upd = popped->collected_upd;
							sentry->collected_del = popped->collected_del;
							sentry->collected_truncdropped = popped->collected_truncdropped;
							sentry->collected_pages = popped->collected_pages;
							sentry->collected_dead = popped->collected_dead;
							sentry->was_usable = popped->was_usable;
							sentry->collect_seq = popped->collect_seq;
							sentry->unsafe = popped->unsafe;
							sentry->state = popped->state;
							sentry->heap_rlb = popped->heap_rlb;
							sentry->heap_rlb_valid = popped->heap_rlb_valid;
							sentry->state_subid = popped->state_subid;
							sentry->undo = popped->older;
							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_ref(relid);
							pfree(popped);	/* popped->statsTuple now owned by entry */
						}
						else
						{
							/* Created in this subxact -> drop entirely. */
							if (fasttrun_stats_entry_is_candidate(sentry))
								fasttrun_stats_relid_unref(relid);
							if (sentry->statsTuple != NULL)
								heap_freetuple(sentry->statsTuple);
							(void) hash_search(fasttrun_stats_cache, &key,
											   HASH_REMOVE, NULL);
							fasttrun_stats_relid_drop_key(&key);
						}
					}
					else	/* SUBXACT_EVENT_COMMIT_SUB */
					{
						sentry->state_subid = parentSubid;

						/*
						 * If the undo top was also at parent level, the pre-
						 * and post-store versions now sit at the same subxact
						 * -> the undo entry is obsolete (outer rollback would
						 * restore to the same level anyway), collapse it.
						 */
						if (sentry->undo != NULL &&
							sentry->undo->state_subid == parentSubid)
						{
							FasttrunStatsSavedState *obsolete = sentry->undo;

							if (obsolete->statsTuple != NULL)
								heap_freetuple(obsolete->statsTuple);
							sentry->undo = obsolete->older;
							pfree(obsolete);
						}
					}
				}

				list_free(keys);
			}
		}

		if (fasttrun_stats_relid_cache != NULL)
		{
			FasttrunStatsRelidEntry *relentry;

			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
			if (relentry != NULL && relentry->state_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					if (relentry->undo != NULL)
					{
						FasttrunStatsRelidSavedState *popped = relentry->undo;

						relentry->policy = popped->policy;
						relentry->heap_rlb = popped->heap_rlb;
						relentry->heap_rlb_valid = popped->heap_rlb_valid;
						relentry->state_subid = popped->state_subid;
						relentry->undo = popped->older;
						pfree(popped);
					}
					else
					{
						relentry->policy = FASTTRUN_REL_CORE_ALLOWED;
						memset(&relentry->heap_rlb, 0,
							   sizeof(relentry->heap_rlb));
						relentry->heap_rlb_valid = false;
						relentry->state_subid = InvalidSubTransactionId;
					}
				}
				else
				{
					relentry->state_subid = parentSubid;
					if (relentry->undo != NULL &&
						relentry->undo->state_subid == parentSubid)
					{
						FasttrunStatsRelidSavedState *obsolete = relentry->undo;

						relentry->undo = obsolete->older;
						pfree(obsolete);
					}
				}
				fasttrun_stats_relid_maybe_drop(relid, relentry);
			}
		}

		aentry = NULL;
		if (fasttrun_analyze_cache != NULL)
			aentry = (FasttrunAnalyzeCacheEntry *)
				hash_search(fasttrun_analyze_cache, &relid, HASH_FIND, NULL);

		if (aentry != NULL)
		{
			if (event == SUBXACT_EVENT_ABORT_SUB)
				xentry->settle_owner = fasttrun_cache_index_owner(aentry);
			if (aentry->state_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					Oid		plan_relid;
					bool	truncated = (xentry->flags & FASTTRUN_TOUCH_TRUNCATED) != 0;

					if (fasttrun_cache_make_empty_storage_authoritative(aentry,
																		truncated,
																		&plan_relid))
					{
						/*
						 * fasttruncate() is non-transactional for local
						 * storage: after ROLLBACK TO SAVEPOINT the rows are
						 * still gone.  Do not restore the old relstats undo
						 * chain.  Make the observed empty storage the new
						 * outer-subxact truth instead.
						 */
						fasttrun_analyze_free_undo(aentry);

						aentry->state.has_stats_baseline = false;
						aentry->state.stats_baseline_inserted = 0;
						aentry->state.stats_baseline_updated = 0;
						aentry->state.stats_baseline_deleted = 0;
						aentry->state.stats_baseline_truncdropped = false;
						fasttrun_cache_reset_partial_scan_anchor(aentry);

						if (OidIsValid(plan_relid))
							fasttrun_stats_force_neutral_relid(plan_relid);
						aentry->state.last_inval_valid = false;
						goto finish_entry;
					}

					if (truncated)
					{
						/*
						 * Every saved state describes rows the reset
						 * destroyed, but the storage cannot be probed: a
						 * failed reset cleared the publication (the table
						 * stays blocked until it is repaired) or DDL retired
						 * the entry.  Keep nothing, so the next analyze
						 * scans.
						 */
						fasttrun_analyze_free_undo(aentry);
						aentry->state.has_relstats = false;
						aentry->state.has_delta_state = false;
						aentry->state.has_stats_baseline = false;
						aentry->state.last_inval_valid = false;
						fasttrun_cache_reset_partial_scan_anchor(aentry);
						aentry->state_subid = InvalidSubTransactionId;
						fasttrun_stats_force_neutral_relid(xentry->root_relid);
					}
					else if (aentry->undo != NULL)
					{
						FasttrunAnalyzeUndo *popped = aentry->undo;

						restored_other_file =
							!RelFileLocatorEquals(aentry->state.probe_rlb.locator,
												  popped->state.probe_rlb.locator);
						aentry->state = popped->state;
						aentry->state_subid = popped->state_subid;
						aentry->undo = popped->older;
						restored_state = true;
						restored_dead = popped->aborted_pages;
						pfree(popped);
					}
					else
					{
						(void) hash_search(fasttrun_analyze_cache, &relid,
										   HASH_REMOVE, NULL);
						aentry = NULL;
					}
				}
				else
				{
					aentry->state_subid = parentSubid;
					if (aentry->undo != NULL &&
						aentry->undo->state_subid == parentSubid)
					{
						FasttrunAnalyzeUndo *obsolete = aentry->undo;

						aentry->undo = obsolete->older;
						pfree(obsolete);
					}
				}
			}

			/* Writes of the ending level may have grown the file. */
			if (event == SUBXACT_EVENT_ABORT_SUB && aentry != NULL)
			{
				if ((xentry->flags & FASTTRUN_TOUCH_DML) != 0)
					aentry->aborted_writes++;
				fasttrun_cache_note_aborted_growth(aentry, xentry,
												   restored_state, restored_dead,
												   restored_other_file);
			}

			/*
			 * An index row restored to its pre-creation empty state has no
			 * owner.  A heap row waits for fasttrun_cache_settle_abort_owners:
			 * whether it owns index relstats is known only after the whole
			 * walk.
			 */
			if (event == SUBXACT_EVENT_ABORT_SUB && aentry != NULL &&
				OidIsValid(fasttrun_cache_index_owner(aentry)) &&
				!fasttrun_cache_entry_keeps_file(aentry) &&
				!aentry->state.has_stats_baseline &&
				aentry->undo == NULL)
			{
				(void) hash_search(fasttrun_analyze_cache, &relid,
								   HASH_REMOVE, NULL);
				aentry = NULL;
			}

			/*
			 * Lazy-probe memo.  The observation in
			 * fasttrun_reinject_relstats is valid only inside the subxact
			 * that recorded it.  On ABORT_SUB we drop it -- the storage
			 * state we saw is the pre-abort view, and we cannot tell now
			 * whether later DML in this rolled-back subxact moved
			 * cached_pages.  On COMMIT_SUB we promote it to the parent
			 * subxact so plans there keep skipping the probe.
			 */
			if (aentry != NULL && aentry->lazy_check_subid == mySubid)
			{
				if (event == SUBXACT_EVENT_ABORT_SUB)
				{
					aentry->lazy_check_subid = InvalidSubTransactionId;
					aentry->lazy_check_epoch = 0;
					aentry->lazy_check_pages = 0;
				}
				else		/* SUBXACT_EVENT_COMMIT_SUB */
					aentry->lazy_check_subid = parentSubid;
			}
		}

	finish_entry:
		/* The owning-heap entry deduplicates abort-time plan invalidation. */
		if (event == SUBXACT_EVENT_ABORT_SUB &&
			(xentry->flags & FASTTRUN_TOUCH_PLAN_INVALIDATE) != 0 &&
			xentry->relid == xentry->root_relid)
		{
			fasttrun_invalidate_local_plan_cache(xentry->root_relid);
			fasttrun_cache_anchor_abort_invalidation(xentry->root_relid);
		}
	}

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		fasttrun_cache_settle_abort_owners(frame);

		/*
		 * Unique relids from aborted sibling frames must not leave enlarged
		 * empty hash tables in a long-lived backend.  Live session state keeps
		 * either cache nonempty, so deleting an empty cache loses no snapshot.
		 */
		if (fasttrun_analyze_cache != NULL &&
			hash_get_num_entries(fasttrun_analyze_cache) == 0)
			fasttrun_cache_reset();
		if (fasttrun_stats_cache != NULL &&
			fasttrun_stats_relid_cache != NULL &&
			hash_get_num_entries(fasttrun_stats_cache) == 0 &&
			hash_get_num_entries(fasttrun_stats_relid_cache) == 0)
			fasttrun_stats_cache_reset();

		fasttrun_xact_frame = frame->parent;
		fasttrun_xact_frame_release(frame);
	}
	else if (frame->parent == NULL || frame->parent->subid != parentSubid)
	{
		/*
		 * Frames are lazy, so the immediate parent may have no frame while an
		 * older ancestor does.  Keep this frame and retag it.  Merging into the
		 * ancestor would make a later ROLLBACK TO the empty parent invisible.
		 */
		frame->subid = parentSubid;
	}
	else
	{
		FasttrunXactFrame *parent = frame->parent;

		hash_seq_init(&status, frame->entries);
		while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
			fasttrun_xact_merge_entry(parent, xentry);
		fasttrun_xact_frame = parent;
		fasttrun_xact_frame_release(frame);
	}
}

