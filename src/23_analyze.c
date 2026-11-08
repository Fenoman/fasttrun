/*-------------------------------------------------------------------------
 *
 * src/23_analyze.c
 *      fasttrun_analyze and the handoff to core ANALYZE.
 *
 * What it holds.  fasttrun_analyze_relation, the permission checks, extended
 * statistics handling, the core ANALYZE handoff through SPI and the SQL
 * functions fasttrun_analyze, fasttrun_analyze_bulk and fasttrun_relstats.
 *
 * Also here.  fasttrun_passthrough_command, used by fasttruncate as well.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/23_analyze.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * fasttrun_analyze(text)
 *
 * In-memory ANALYZE for a temporary table that does NOT generate any
 * shared invalidation messages.  This is the workhorse function for
 * replacing thousands of explicit "ANALYZE temp_xxx" statements scattered
 * across PL/pgSQL code without paying the catalog-update / sinval-storm
 * cost that an ordinary ANALYZE would incur.
 *
 * Behaviour:
 *
 *   * Resolves the relation name.  Silently returns on a missing
 *     relation, raises ERROR on a non-temp relation or on a
 *     partitioned / inheritance parent (same contract as fasttruncate).
 *
 *   * Reads the actual on-disk page count via RelationGetNumberOfBlocks
 *     when the cache/delta state cannot prove that storage size is
 *     unchanged (this never touches the catalog).
 *
 *   * Consults the lazy-mode cache.  Cache hit computes the row count as
 *     cached_tuples + delta_ins - delta_del without reading the heap and
 *     refreshes the snapshot in place so the next call sees only
 *     the next delta.  After any DML since the last collect it still
 *     samples the heap for a column-stats refresh, as core ANALYZE does,
 *     and for a partial-index rescan, taking the row count from that
 *     scan.
 *
 *   * On cache miss (no delta state in this transaction yet, page count
 *     dropped, truncdropped bit changed, or pgstat off) scans the heap,
 *     optionally collecting per-column stats from a reservoir sample
 *     (see fasttrun.auto_collect_stats GUC), and populates the cache.
 *
 *   * Every heap scan here goes through fasttrun_sample_for_analyze().  The
 *     cold path does an exact full scan up to fasttrun.max_analyze_pages
 *     heap pages and above that samples blocks, estimating the row count
 *     from tuple density.  The churn refresh and the partial-index rescan
 *     already have the exact count from the delta and sample blocks at any
 *     size.  Setting the limit to 0 keeps every scan a full scan.
 *
 * The published reltuples/relpages are visible only inside the current
 * backend's relcache.  pg_class on disk is NOT modified.
 *
 * Column-level coverage: if auto_collect_stats is on (default), the
 * cold scan and the churn refresh collect per-column stats from their
 * sample, and the planner hooks serve them.  With use_typanalyze=on
 * (default) this includes MCV, histogram and correlation, computed as
 * regular ANALYZE computes them from a fasttrun.sample_rows sample
 * (-1 matches the ANALYZE sample size).
 */
/*
 * Workhorse for fasttrun_analyze() and fasttrun_analyze_bulk().
 *
 * Runs the full analyze pipeline on an already-opened Relation:
 * delta-hit cache, refresh-check, cold scan, index relstats update,
 * rd_rel reinjection.  Emits fasttrun_invalidate_local_plan_cache()
 * internally when relstats drift crosses fasttrun.invalidate_threshold.
 *
 * Firing the invalidation BEFORE the final rd_rel mutation is
 * load-bearing.  The invalidation goes through
 * RelationCacheInvalidateEntry().  That rebuilds the relcache entry from
 * pg_class while we still hold AccessShareLock.  We then re-mutate
 * rd_rel afterwards, and the planner observes our cached values -- not
 * the stale 0/0 from pg_class.  Move the invalidate past the mutation
 * (or past table_close) and our values get silently wiped.
 *
 * Caller is responsible for:
 *   - resolving the RangeVar to relOid + table_open(NoLock)
 *   - verifying relpersistence == RELPERSISTENCE_TEMP, isTempNamespace,
 *     heap-AM, no inheritance/partition parent
 *   - recording relOid in the current transaction frame
 *   - table_close(rel, AccessShareLock) afterwards.
 *
 * fasttrun_analyze_bulk() invokes this once per relation in its array.
 * Each invalidation still makes PG's PlanCacheRelCallback walk the whole
 * cached-plan list.  The saving is per-plan, not per-walk -- a plan an
 * earlier message already marked is_valid=false is skipped cheaply rather
 * than re-marked.
 */
static void
fasttrun_analyze_relation(Relation rel)
{
	Oid				relOid = RelationGetRelid(rel);
	BlockNumber		pages_now = 0;
	int64			tuples_count = 0;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;
	bool			have_counters;
	bool			scan_needed = true;
	bool			stats_recollected = false;
	bool			collected_unsafe = false;
	bool			stats_visibility_changed = false;
	bool			index_relstats_changed = false;
	bool			need_plan_inval = false;
	bool			pure_delta_noop = false;
	bool			refresh_deferred = false;
	bool			partial_index_sampled = false;
	bool			pages_now_known = false;
	bool			count_exact = true;
	bool			count_snapshot_stale = false;
	bool			relation_policy_changed;
	BlockNumber		allvisible_now = 0;
	BlockNumber		old_pages;
	int32			old_allvisible;
	float4			old_tuples;
	int64			delta_ins = 0;
	int64			delta_upd = 0;
	int64			delta_del = 0;
	FasttrunAnalyzeCacheEntry *entry = NULL;
	instr_time		started;
	const char	   *path;
	bool			has_extended_stats;

	fasttrun_count(FTC_ANALYZE_CALLS);
	fasttrun_timer_start(&started);
	has_extended_stats = fasttrun_relation_has_extended_stats(rel);
	if (has_extended_stats && fasttrun_may_core_analyze(rel))
	{
		fasttrun_count(FTC_ANALYZE_CORE_HANDOFF);
		fasttrun_run_core_analyze(rel);
		fasttrun_log_if_slow(&started, "fasttrun_analyze", rel, "core ANALYZE");
		return;
	}
	relation_policy_changed = fasttrun_stats_set_relation_policy(rel,
													FASTTRUN_REL_LOCAL_NEUTRAL);
	if (relation_policy_changed && fasttrun_relation_has_core_stats(rel))
		stats_visibility_changed = true;
	old_pages = rel->rd_rel->relpages;
	old_allvisible = rel->rd_rel->relallvisible;
	old_tuples = rel->rd_rel->reltuples;
	have_counters = fasttrun_read_pgstat_counters(rel,
												  &ins_now, &upd_now, &del_now,
												  &truncdropped_now);

	/* Lazy-mode delta hit: see header comment block above. */
	if (have_counters)
	{
		entry = fasttrun_cache_lookup(relOid);
		if (entry != NULL && fasttrun_cache_storage_check_due(entry, relOid))
			(void) fasttrun_cache_check_storage(entry, rel);
		if (entry != NULL
			&& entry->state.has_delta_state
			&& fasttrun_relation_has_same_locator(rel, entry)
			&& entry->state.cached_truncdropped == truncdropped_now)
		{
			int64	new_tuples;

			delta_ins = ins_now - entry->state.cached_inserted;
			delta_upd = upd_now - entry->state.cached_updated;
			delta_del = del_now - entry->state.cached_deleted;
			new_tuples = entry->state.cached_tuples + delta_ins - delta_del;

			if (new_tuples >= 0)
			{
				/*
				 * A rolled-back write since the publication: measure the size
				 * anew and refresh the index relstats too.  Its index entries
				 * stay even when the heap did not grow, as when the new row
				 * versions fit on the old pages.
				 */
				pure_delta_noop =
					(delta_ins == 0 && delta_upd == 0 && delta_del == 0 &&
					 fasttrun_cache_dead_since_publish(entry) == 0 &&
					 entry->aborted_writes == entry->state.published_aborted_writes);
				if (pure_delta_noop)
				{
					/*
					 * No counted DML and no dead pages since the publication,
					 * so the relstats snapshot is authoritative and the hot
					 * no-op fasttrun_analyze() skips one smgrnblocks/lseek.
					 */
					pages_now = entry->state.cached_pages;
					pages_now_known = true;
					tuples_count = new_tuples;
					count_exact = entry->state.count_exact;
					scan_needed = false;
				}
				else
				{
					pages_now = RelationGetNumberOfBlocks(rel);
					pages_now_known = true;
					if (pages_now >= entry->state.cached_pages)
					{
						tuples_count = new_tuples;
						count_exact = entry->state.count_exact;
						scan_needed = false;
					}
				}
			}
		}
	}

	if (!pages_now_known)
	{
		pages_now = RelationGetNumberOfBlocks(rel);
		pages_now_known = true;
	}

	/*
	 * Delta-hit stats refresh: any DML since the last collect resamples, as
	 * core ANALYZE does.  A distribution kept below some churn threshold does
	 * not know the values the DML brought in: a new date, a flag set on part
	 * of the rows or a column filled where it held only NULLs would read as one
	 * row.  The delta already gives the exact row count, so the sample comes
	 * from random blocks as in core ANALYZE, not from a scan of the whole heap.
	 */
	if (!scan_needed && have_counters && fasttrun_auto_collect_stats &&
		fasttrun_sample_rows != 0 &&
		entry != NULL && entry->state.has_stats_baseline)
	{
		int64		churn;

		/* Compute churn BEFORE advancing baseline. */
		churn = (ins_now - entry->state.stats_baseline_inserted)
			+ (upd_now - entry->state.stats_baseline_updated)
			+ (del_now - entry->state.stats_baseline_deleted);
		if (churn < 0)
			churn = -churn;

		if (churn > 0)
		{
			/*
			 * A sample taken while the relation is being written never serves
			 * the planner.  The refresh waits for the writer: the baseline
			 * stays put, so the churn survives to the next analyze, across a
			 * COMMIT too, and that analyze recollects.
			 */
			if (fasttrun_stats_relation_written_now(rel))
				refresh_deferred = true;
			else if (!fasttrun_stats_budget_blocks_collect(rel, false))
			{
				int			sample_target = fasttrun_effective_sample_target(rel);

				if (sample_target > 0)
				{
					HeapTuple  *sample;
					int			sample_count = 0;
					int			i;

					sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
					if (fasttrun_refresh_sample(rel, pages_now, sample,
												sample_target, &tuples_count,
												&sample_count, &count_exact))
						pure_delta_noop = false;

					if (sample_count > 0)
					{
						FasttrunCollectResult collect_result;

						/*
						 * The sample never outnumbers the rows it came from,
						 * and a full reservoir has been reshuffled out of TID
						 * order whatever the count says.
						 */
						collect_result = fasttrun_collect_and_store(rel, sample,
																 sample_count,
																 Max(tuples_count,
																	 (int64) sample_count),
																 tuples_count > sample_target ||
																 sample_count >= sample_target);
						stats_recollected |= collect_result.published_any;
						collected_unsafe |= collect_result.unsafe;
						stats_visibility_changed |= collect_result.became_neutral;
						index_relstats_changed |= collect_result.index_changed;
					}
					else
					{
						if (fasttrun_stats_cache_evict_relid(relOid))
							stats_visibility_changed = true;
						index_relstats_changed |=
							fasttrun_update_index_relstats(rel, NIL, NULL, 0,
														   tuples_count, true);
					}

					for (i = 0; i < sample_count; i++)
						heap_freetuple(sample[i]);
					pfree(sample);
				}
			}

			/*
			 * A deferred or budget-blocked refresh leaves the baseline in
			 * place, so the churn keeps counting toward the freshness check
			 * and the next analyze tries again.
			 */
			if (stats_recollected ||
				(tuples_count == 0 && count_exact && !refresh_deferred))
				fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
												  del_now, truncdropped_now);
		}
	}

	if (!scan_needed && !stats_recollected)
	{
		int64		churn = 0;
		bool		probe_partial;
		bool		trailing_refresh = !pure_delta_noop;

		if (have_counters && fasttrun_sample_rows != 0 &&
			entry != NULL && entry->state.has_stats_baseline)
		{
			/*
			 * Churn is measured from the rescan anchor (the counters at the
			 * last partial-index sync), not from the stats baseline: the
			 * baseline stays put on sub-threshold churn, and measuring
			 * against it would re-sample the partial indexes on every
			 * analyze after a single small DML.
			 */
			if (entry->state.partial_scan_valid)
				churn = (ins_now - entry->state.partial_scan_inserted)
					+ (upd_now - entry->state.partial_scan_updated)
					+ (del_now - entry->state.partial_scan_deleted);
			else
				churn = (ins_now - entry->state.stats_baseline_inserted)
					+ (upd_now - entry->state.stats_baseline_updated)
					+ (del_now - entry->state.stats_baseline_deleted);
			if (churn < 0)
				churn = -churn;
		}
		probe_partial = (churn > 0);

		if (probe_partial || trailing_refresh)
		{
			/* One open per index serves the probe and both refreshes. */
			List	   *index_rels = fasttrun_open_index_rels(rel);

			if (probe_partial)
			{
				/*
				 * Rescan on any new churn when a partial index exists.  A
				 * small DML on the predicate column can change the partial
				 * index's covered tuple count out of proportion to the heap
				 * churn ratio (e.g. flipping a boolean flag on 1% of rows can
				 * double the index), so the heap ratio cannot gate this
				 * rescan.
				 */
				if (fasttrun_index_rels_have_partial(index_rels))
				{
					int			sample_target = fasttrun_effective_sample_target(rel);

					/*
					 * A scan while the relation is being written misses rows
					 * the counters already hold, so the rescan waits and the
					 * anchor stays for the next call.
					 */
					if (sample_target > 0 &&
						!fasttrun_stats_relation_written_now(rel))
					{
						HeapTuple  *sample;
						int			sample_count = 0;
						int			i;

						sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
						if (fasttrun_refresh_sample(rel, pages_now, sample,
													sample_target, &tuples_count,
													&sample_count, &count_exact))
							pure_delta_noop = false;
						index_relstats_changed |=
							fasttrun_update_index_relstats(rel, index_rels,
															   sample, sample_count,
															   tuples_count, true);
						partial_index_sampled = true;
						fasttrun_cache_set_partial_scan_anchor(entry, ins_now,
															   upd_now, del_now);

						for (i = 0; i < sample_count; i++)
							heap_freetuple(sample[i]);
						pfree(sample);
					}
					/*
					 * No rescan now (sample_target <= 0 or a running writer):
					 * leave the anchor alone so a later call still sees this
					 * churn.
					 */
				}
				else
				{
					/* No partial indexes: nothing to sync, anchor the probe. */
					fasttrun_cache_set_partial_scan_anchor(entry, ins_now,
														   upd_now, del_now);
				}
			}

			/*
			 * A trailing sample-less refresh keeps non-partial index relpages
			 * in step with the heap.  Pass partial_upper_bound=false when a
			 * sample-based rescan above already set the partial index tuple
			 * counts, or when there was no churn to change them, so this call
			 * does not overwrite those estimates with the totalrows upper
			 * bound.
			 */
			/*
			 * Without DML since the last analyze (only dead pages since the
			 * publication) the partial index counts still hold: refresh the
			 * pages only.
			 */
			if (trailing_refresh)
				index_relstats_changed |=
					fasttrun_update_index_relstats(rel, index_rels, NULL, 0,
												   tuples_count,
												   (delta_ins != 0 ||
													delta_upd != 0 ||
													delta_del != 0) &&
												   !partial_index_sampled);

			fasttrun_close_index_rels(index_rels);
		}
	}

	if (scan_needed)
	{
		HeapTuple  *sample = NULL;
		int			sample_target = 0;
		int			sample_count = 0;

		/* Reservoir-sample inside the same scan if auto-collect is on. */
		if (pages_now > 0 && fasttrun_auto_collect_stats &&
			fasttrun_sample_rows != 0 &&
			!fasttrun_stats_budget_blocks_collect(rel, false))
		{
			sample_target = fasttrun_effective_sample_target(rel);
			if (sample_target > 0)
				sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);
		}

		if (pages_now > 0)
		{
			/*
			 * Giant temp table: block-sample above max_analyze_pages (estimated
			 * row count, O(sample)), exact full scan at or below.  Same helper
			 * gates the delta-refresh and partial-index rescans.
			 */
			count_exact = fasttrun_sample_for_analyze(rel, pages_now, sample,
													  sample_target,
													  &tuples_count,
													  &sample_count, false);
		}

		if (sample != NULL && sample_count > 0)
		{
			FasttrunCollectResult collect_result;

			/*
			 * Sort needed only when reservoir sampling displaced entries.
			 * published_any stays false when track_counts is off.  Then the
			 * !stats_recollected path below still refreshes index relstats.
			 * became_neutral rides the same single plan-invalidation site.
			 */
			collect_result = fasttrun_collect_and_store(rel, sample,
															  sample_count,
															  tuples_count,
															  tuples_count > sample_target);
			stats_recollected = collect_result.published_any;
			collected_unsafe |= collect_result.unsafe;
			stats_visibility_changed |= collect_result.became_neutral;
			index_relstats_changed |= collect_result.index_changed;
		}

		if (sample != NULL)
		{
			int i;
			for (i = 0; i < sample_count; i++)
				heap_freetuple(sample[i]);
			pfree(sample);
		}

		/*
		 * Re-read counters after the scan.  heap_getnext touches only
		 * non-xact counters, so the values we read here are authoritative
		 * for the tuple DML baseline.
		 */
		if (!have_counters)
			have_counters = fasttrun_read_pgstat_counters(rel,
														  &ins_now, &upd_now,
														  &del_now,
														  &truncdropped_now);

		/*
		 * A scan on a snapshot that cannot see writes the counters already
		 * hold (inside a modifying CTE, under an older command) counts fewer
		 * rows than the counters imply.  Publish what it saw, but anchor no
		 * delta to it: the next analyze scans again.
		 */
		count_snapshot_stale = fasttrun_stats_relation_written_now(rel);

		if (!stats_recollected)
			index_relstats_changed |=
				fasttrun_update_index_relstats(rel, NIL, NULL, 0, tuples_count,
										   true);

		/* Populate stats baseline only when pgstat is usable. */
		if (have_counters)
			fasttrun_cache_set_stats_baseline(relOid, ins_now, upd_now,
											  del_now, truncdropped_now);
	}

	if (pure_delta_noop && entry != NULL && entry->state.has_relstats)
		allvisible_now = entry->state.cached_allvisible;
	else
		allvisible_now = fasttrun_count_allvisible(rel);

	if (!(pure_delta_noop && entry != NULL && entry->state.has_relstats))
	{
		entry = fasttrun_cache_store_relstats(rel, pages_now, tuples_count,
											 allvisible_now);
		entry->state.count_exact = count_exact;
		if (have_counters && !count_snapshot_stale)
			fasttrun_cache_store_delta_state(entry, ins_now, upd_now, del_now,
											 truncdropped_now);
		else
			entry->state.has_delta_state = false;
	}

	/*
	 * Column stats that served plans at the last analyze but are hidden by
	 * the freshness gate now (e.g. a near-unique column whose scaled
	 * tolerance sits below the recollect threshold) force invalidation:
	 * new plans already see defaults, so a cached plan on the stale
	 * distribution must not outlive the flip.  Runs against the state just
	 * published above -- exactly what the planner hook will see next.
	 */
	if (fasttrun_stats_note_visibility(relOid, have_counters, ins_now,
									   upd_now, del_now, truncdropped_now,
									   pages_now))
		stats_visibility_changed = true;

	need_plan_inval = (old_pages != pages_now ||
					   old_tuples != (float4) tuples_count ||
					   old_allvisible != (int32) allvisible_now ||
					   stats_recollected ||
					   stats_visibility_changed ||
					   index_relstats_changed);

	/*
	 * Below-threshold relstats drift does NOT invalidate cached plans.
	 * A few extra rows rarely flip the optimizer's choice.  And walking
	 * the entire plan_cache once per analyze dominates xact CPU on
	 * backends with large SPI/PREPARE caches.
	 *
	 * Drift is measured against the values published at the LAST actual
	 * invalidation -- the values the surviving plans were built on -- not
	 * against the previous call.  Re-anchoring on every call would let a
	 * series of sub-threshold steps accumulate unbounded drift without a
	 * single invalidation.
	 *
	 * Column-stats refresh, stats-visibility flip, and index relstats
	 * change always invalidate.  They signal distribution changes that
	 * the per-row ratio here cannot measure.
	 */
	if (need_plan_inval && fasttrun_invalidate_threshold > 0.0 &&
		!stats_recollected && !stats_visibility_changed &&
		!index_relstats_changed)
	{
		BlockNumber	anchor_pages = old_pages;
		float4		anchor_tuples = old_tuples;
		BlockNumber	live_pages = pages_now;
		double		baseline_tuples;
		double		ratio_tuples;
		double		ratio_pages;
		double		worst;

		if (entry != NULL && entry->state.last_inval_valid)
		{
			anchor_pages = entry->state.last_inval_pages;
			anchor_tuples = entry->state.last_inval_tuples;
			/* Dead pages of rolled-back writes do not move an estimate. */
			if (entry->aborted_pages > entry->state.last_inval_dead)
				live_pages -= Min(live_pages,
								  entry->aborted_pages - entry->state.last_inval_dead);
		}

		baseline_tuples = (anchor_tuples > 0.0f)
			? (double) anchor_tuples : 1.0;
		ratio_tuples =
			fabs((double) tuples_count - (double) anchor_tuples) / baseline_tuples;
		ratio_pages = (anchor_pages > 0)
			? fabs((double) live_pages - (double) anchor_pages) / (double) anchor_pages
			: (live_pages > 0 ? 1.0 : 0.0);
		worst = Max(ratio_tuples, ratio_pages);

		if (worst < fasttrun_invalidate_threshold)
			need_plan_inval = false;
	}

	if (need_plan_inval)
	{
		/* Plans built from here on see the new values: an abort must drop them. */
		fasttrun_xact_mark_relid(relOid, relOid, FASTTRUN_TOUCH_PLAN_INVALIDATE);
		/* Before the invalidation drops the cached index list. */
		fasttrun_anchor_index_invalidation(rel);
		fasttrun_invalidate_local_plan_cache(relOid);
		if (entry != NULL)
		{
			entry->state.last_inval_pages = pages_now;
			entry->state.last_inval_tuples = (float4) tuples_count;
			entry->state.last_inval_dead = entry->aborted_pages;
			entry->state.last_inval_valid = true;
		}
	}

	rel->rd_rel->relpages = pages_now;
	rel->rd_rel->reltuples = (float4) tuples_count;
	rel->rd_rel->relallvisible = (int32) allvisible_now;

	if (refresh_deferred)
		fasttrun_count(FTC_REFRESH_DEFERRED);
	if (partial_index_sampled)
		fasttrun_count(FTC_PARTIAL_INDEX_RESCANS);
	if (scan_needed)
	{
		fasttrun_count(count_exact ? FTC_ANALYZE_COLD_SCAN :
					   FTC_ANALYZE_COLD_BLOCK_SAMPLE);
		path = count_exact ? "full scan" : "block sample";
	}
	else if (pure_delta_noop && !stats_recollected && !partial_index_sampled)
	{
		fasttrun_count(FTC_ANALYZE_NOOP);
		path = "no changes";
	}
	else
	{
		fasttrun_count(FTC_ANALYZE_DELTA);
		path = stats_recollected ? "delta with recollect" : "delta";
	}

	/*
	 * The caller may not run core ANALYZE, so the CREATE STATISTICS data still
	 * describe an earlier fill, and the columns collected here are newer.  A
	 * sample taken while the table is written never serves the planner.
	 */
	if (stats_recollected && !collected_unsafe && has_extended_stats)
	{
		entry = fasttrun_cache_lookup(relOid);
		if (entry != NULL && !entry->state.ext_stats_stale)
		{
			fasttrun_analyze_save_undo(entry);
			entry->state.ext_stats_stale = true;
		}
	}
	fasttrun_log_if_slow(&started, "fasttrun_analyze", rel, path);
}

/*
 * Extended statistics objects (CREATE STATISTICS) are built by core ANALYZE
 * only, from its own sample, into pg_statistic_ext_data.  fasttrun has no
 * planner hook for them, so its own collect would leave the planner with no
 * data for the object, or with the data of an earlier fill.  Such a relation
 * goes to core ANALYZE as a whole: the utility hook hands the column
 * statistics over to core as for any ANALYZE, and the catalog writes and the
 * shared invalidations are the price of the multicolumn estimates.
 */
/*
 * Core VACUUM and ANALYZE skip, with a WARNING, a relation the caller may not
 * maintain, and the utility hook would still hand its statistics over to core:
 * the statistics of an earlier fill would serve the planner.  The same rule as
 * in core: the database owner, and the owner on PG16 or the MAINTAIN privilege
 * on PG17 and later.  Without it fasttrun collects the column statistics
 * itself.
 */
static bool
fasttrun_may_maintain_relid(Oid relid, bool relisshared)
{
	if (!relisshared &&
		object_ownercheck(DatabaseRelationId, MyDatabaseId, GetUserId()))
		return true;
#if PG_VERSION_NUM >= 170000
	/* The owner holds MAINTAIN unless it revoked it from itself. */
	return pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) == ACLCHECK_OK;
#else
	return object_ownercheck(RelationRelationId, relid, GetUserId());
#endif
}

static bool
fasttrun_may_core_analyze(Relation rel)
{
	return fasttrun_may_maintain_relid(RelationGetRelid(rel),
									   rel->rd_rel->relisshared);
}

static bool
fasttrun_relation_has_extended_stats(Relation rel)
{
	List	   *statoids = RelationGetStatExtList(rel);
	bool		found = (statoids != NIL);

	list_free(statoids);
	return found;
}

/* The current version of an object's pg_statistic_ext_data row. */
static FasttrunExtDataVersion
fasttrun_ext_data_version(Oid stxoid)
{
	FasttrunExtDataVersion version;
	HeapTuple	tup;

	version.stxoid = stxoid;
	version.present = false;
	version.xmin = InvalidTransactionId;
	ItemPointerSetInvalid(&version.tid);
	tup = SearchSysCache2(STATEXTDATASTXOID, ObjectIdGetDatum(stxoid),
						  BoolGetDatum(false));
	if (HeapTupleIsValid(tup))
	{
		version.present = true;
#ifdef TANTOR_VERSION
		/* Tantor keeps the 64-bit xmin in HeapTupleData, not in the header. */
		version.xmin = HeapTupleGetXmin(tup);
#else
		version.xmin = HeapTupleHeaderGetXmin(tup->t_data);
#endif
		version.tid = tup->t_self;
		ReleaseSysCache(tup);
	}
	return version;
}

static List *
fasttrun_ext_data_versions(Relation rel)
{
	List	   *statoids = RelationGetStatExtList(rel);
	List	   *versions = NIL;
	ListCell   *lc;

	foreach(lc, statoids)
	{
		FasttrunExtDataVersion *version = palloc(sizeof(*version));

		*version = fasttrun_ext_data_version(lfirst_oid(lc));
		versions = lappend(versions, version);
	}
	list_free(statoids);
	return versions;
}

/*
 * Core ANALYZE writes the data row of every object it builds.  It skips an
 * object with a target of 0, an object over a column it did not sample, all
 * objects of an empty sample, and the whole table when the caller may not
 * analyze it, keeping the earlier row each time.  So the flag goes only when
 * every object now has a row other than the one before the command, or none.
 */
static bool
fasttrun_ext_data_rebuilt(Relation rel, List *before)
{
	List	   *statoids = RelationGetStatExtList(rel);
	ListCell   *lc;
	bool		rebuilt = true;

	foreach(lc, statoids)
	{
		Oid			stxoid = lfirst_oid(lc);
		FasttrunExtDataVersion after = fasttrun_ext_data_version(stxoid);
		ListCell   *blc;
		bool		same = false;

		if (!after.present)
			continue;
		foreach(blc, before)
		{
			FasttrunExtDataVersion *b = (FasttrunExtDataVersion *) lfirst(blc);

			if (b->stxoid == stxoid && b->present &&
				TransactionIdEquals(b->xmin, after.xmin) &&
				ItemPointerEquals(&b->tid, &after.tid))
				same = true;
		}
		if (same)
		{
			rebuilt = false;
			break;
		}
	}
	list_free(statoids);
	return rebuilt;
}

static void
fasttrun_run_core_analyze(Relation rel)
{
	char	   *nspname = get_namespace_name(RelationGetNamespace(rel));
	char	   *sql;
	int			rc;

	sql = psprintf("ANALYZE %s",
				   quote_qualified_identifier(nspname,
											  RelationGetRelationName(rel)));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "fasttrun_analyze: SPI_connect failed");
	rc = SPI_execute(sql, false, 0);
	if (rc != SPI_OK_UTILITY)
		elog(ERROR, "fasttrun_analyze: core ANALYZE failed: %s",
			 SPI_result_code_string(rc));
	SPI_finish();
	pfree(sql);
}

/*
 * fasttrun.passthrough: run the core command on the table.  The caller has
 * closed its own reference: TRUNCATE refuses a table this session holds open.
 */
static void
fasttrun_passthrough_command(const char *command, Oid relid)
{
	char	   *sql;
	int			rc;

	sql = psprintf("%s %s", command,
				   quote_qualified_identifier(get_namespace_name(get_rel_namespace(relid)),
											  get_rel_name(relid)));
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "fasttrun: SPI_connect failed");
	rc = SPI_execute(sql, false, 0);
	if (rc != SPI_OK_UTILITY)
		elog(ERROR, "fasttrun: \"%s\" failed: %s", sql,
			 SPI_result_code_string(rc));
	SPI_finish();
	pfree(sql);
}

/*
 * fasttrun_analyze(text) -- a thin wrapper around
 * fasttrun_analyze_relation().  It resolves the relation name, takes
 * AccessShareLock, runs the helper, and emits any resulting plan-cache
 * invalidation.
 *
 * For the function-level contract -- silent return on missing relation,
 * ERROR on non-temp, non-heap or inheritance/partition parent, what
 * gets cached, when sinval fires --
 * see the comment block above fasttrun_analyze_relation() and the
 * section "Lazy-mode cache for fasttrun_analyze" in
 * src/03_shared_decls.c.
 */
Datum
fasttrun_analyze(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_analyze: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	/*
	 * Inheritance/partition parents are out of contract, same as in
	 * fasttruncate: local inh=false stats would hide core inherited
	 * stats during appendrel planning.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		rel->rd_rel->relhassubclass)
	{
		table_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fasttrun_analyze: relation \"%s\" is partitioned or an inheritance parent",
						RelationGetRelationName(rel)),
				 errhint("Use core SQL ANALYZE for inheritance or partition traversal.")));
	}
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttrun_analyze: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}
	if (fasttrun_passthrough)
	{
		table_close(rel, NoLock);
		fasttrun_passthrough_command("ANALYZE", relOid);
		PG_RETURN_VOID();
	}
	fasttrun_poison_check_relation(rel, "fasttrun_analyze", false);

	/*
	 * No PLAN_INVALIDATE here: the flag is raised where a planner-visible
	 * change invalidates plans (column-stats store/hide, policy flip, relstats
	 * drift past invalidate_threshold).  A pure no-op analyze rolled back by a
	 * savepoint must not cost a plan-cache walk.
	 */
	fasttrun_xact_mark_relid(relOid, relOid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS);
	fasttrun_analyze_relation(rel);
	table_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * fasttrun_analyze_bulk(VARIADIC text[])
 *
 * Run fasttrun_analyze logic over a batch of temp relations in one call.
 * Per-relation behaviour matches fasttrun_analyze() exactly.  One
 * performance note: plan-cache invalidations fire per-relation as the
 * batch progresses.  PG's PlanCacheRelCallback walks the whole cached-plan
 * list once per message, so N invalidations are still N walks.  The only
 * saving is per-plan: a plan an earlier message already marked
 * is_valid=false is skipped cheaply instead of re-marked.  That trims the
 * per-walk body on backends with large SPI/PREPARE caches, not the number
 * of walks.
 *
 * NULL array elements are silently skipped.  Missing relations are
 * silently skipped -- same contract as fasttrun_analyze.  Non-temp,
 * non-heap or parent relations raise an error -- same contract.  The
 * batch aborts at the first failure.  Relations analyzed before it keep
 * their refreshed cache state -- per-xact rollback semantics kick in
 * via the surrounding xact or savepoint.
 */
Datum
fasttrun_analyze_bulk(PG_FUNCTION_ARGS)
{
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();

	arr = PG_GETARG_ARRAYTYPE_P(0);
	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);
	if (nelems == 0)
		PG_RETURN_VOID();

	for (i = 0; i < nelems; i++)
	{
		text	   *name;
		RangeVar   *relvar;
		Oid			relOid;
		Relation	rel;

		if (nulls[i])
			continue;

		name = DatumGetTextPP(elems[i]);
		relvar = fasttrun_make_rangevar(name);
		relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
		if (!OidIsValid(relOid))
			continue;

		rel = table_open(relOid, NoLock);

		if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
			!isTempNamespace(RelationGetNamespace(rel)))
		{
			table_close(rel, NoLock);
			elog(ERROR, "fasttrun_analyze_bulk: relation \"%s\" "
						"is not a local temporary table",
				 RelationGetRelationName(rel));
		}
		if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
			rel->rd_rel->relhassubclass)
		{
			table_close(rel, NoLock);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("fasttrun_analyze_bulk: relation \"%s\" is partitioned or an inheritance parent",
							RelationGetRelationName(rel)),
					 errhint("Use core SQL ANALYZE for inheritance or partition traversal.")));
		}
		if (rel->rd_tableam != GetHeapamTableAmRoutine())
		{
			table_close(rel, NoLock);
			elog(ERROR, "fasttrun_analyze_bulk: relation \"%s\" is not heap-AM",
				 RelationGetRelationName(rel));
		}
		if (fasttrun_passthrough)
		{
			table_close(rel, NoLock);
			fasttrun_passthrough_command("ANALYZE", relOid);
			continue;
		}
		fasttrun_poison_check_relation(rel, "fasttrun_analyze_bulk", false);

		/* PLAN_INVALIDATE comes from the actual mutation sites. */
		fasttrun_xact_mark_relid(relOid, relOid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_STATS);
		fasttrun_analyze_relation(rel);
		table_close(rel, AccessShareLock);
	}

	/*
	 * The helper emits per-relid fasttrun_invalidate_local_plan_cache()
	 * inline.  It has to fire before the rd_rel mutation -- see the
	 * comment above fasttrun_analyze_relation().  Each call still walks the
	 * whole cached-plan list via PG's PlanCacheRelCallback, but a plan already
	 * marked is_valid=false by an earlier call is skipped cheaply, so the
	 * batch trims per-plan work, not the walk count.  The rest is just a
	 * convenience wrapper.
	 */
	PG_RETURN_VOID();
}

/*
 * fasttrun_relstats(text)
 *
 * Inspection helper.  Returns the in-memory rd_rel statistics for the
 * given relation: the same (relpages, reltuples) tuple the planner uses
 * when costing queries.  Mostly useful for verifying that fasttruncate /
 * fasttrun_analyze actually updated the cached values, and as a SQL-level
 * substitute for poking around with gdb.
 *
 * Like fasttruncate, this function silently returns NULL when the
 * relation does not exist.  It raises ERROR when the relation exists but
 * is not a temporary table, because asking about a regular table via this
 * helper is almost certainly a bug.
 */
Datum
fasttrun_relstats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tuple;

	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
	if (!OidIsValid(relOid))
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	rel = relation_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		relation_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		relation_close(rel, NoLock);
		elog(ERROR, "fasttrun_relstats: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}
	fasttrun_poison_check_relation(rel, "fasttrun_relstats", false);

	/*
	 * Re-inject cached relstats after relcache rebuilds so direct SQL
	 * inspection sees the same values the planner hook will publish.
	 */
	if (fasttrun_analyze_cache != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry;

		entry = (FasttrunAnalyzeCacheEntry *)
			hash_search(fasttrun_analyze_cache, &relOid, HASH_FIND, NULL);
		if (entry != NULL)
			fasttrun_reinject_relstats(rel, entry);
	}

	values[0] = Int32GetDatum(rel->rd_rel->relpages);
	values[1] = Float4GetDatum(rel->rd_rel->reltuples);

	relation_close(rel, AccessShareLock);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

