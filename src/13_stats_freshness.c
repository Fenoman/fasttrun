/*-------------------------------------------------------------------------
 *
 * src/13_stats_freshness.c
 *      Freshness check of cached column statistics.
 *
 * What it holds.  The decision whether a cached column sample may still serve
 * the planner after DML, the per-planner-call memo cache and the locator
 * check of a relid entry.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/13_stats_freshness.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Lower bound for the cardinality-scaled freshness tolerance (see
 * fasttrun_stats_entry_usable).  A near-unique column gets a tolerance this
 * tight, since a DML that concentrates rows onto one value can blow that
 * value's "rare" estimate past a plan boundary (Index Scan instead of Seq
 * Scan).  Low-cardinality columns scale back up toward stats_refresh_threshold
 * -- an existing value there still reads as "many rows" and the plan holds.  A
 * value the DML brought in reads as rare until the next analyze recollects,
 * the same as with core statistics between two ANALYZE runs.
 */
#define FASTTRUN_SKEW_REFRESH_FLOOR	0.05

/*
 * Cross-commit physical-size backstop factor (see fasttrun_stats_entry_usable).
 * The backend's pending pgstat counters reset at every transaction boundary,
 * and the shared ones receive them only at the next periodic flush, so neither
 * spans a commit synchronously.  The commit callback carries the counters of
 * every write the executor and COPY hooks saw, but a write past them (a direct
 * table AM write by another extension) leaves the counter-based freshness
 * check blind.  Physical page count survives the commit but is
 * bloat-contaminated, so only a threefold change is trusted as a refill.
 */
#define FASTTRUN_STALE_SIZE_FACTOR	3.0

/*
 * Per-relid input to the churn tolerance below: the analyze cache's row-count
 * baseline.  All columns of a relid share it, so column loops resolve it once
 * and reuse it (lazily -- the no-DML early path never needs it).
 */
typedef struct FasttrunUsableBaseline
{
	bool		resolved;
	bool		available;
	double		tuples;
} FasttrunUsableBaseline;

/*
 * Soft freshness: is this column-stats entry close enough to keep serving?
 * True while the DML churn since collect time stays below the effective
 * tolerance -- stats_refresh_threshold scaled down by the column's cardinality
 * (see FASTTRUN_SKEW_REFRESH_FLOOR).  Within tolerance the cached distribution
 * still beats planner defaults (core PG keeps using pg_statistic between
 * ANALYZE runs the same way).  A truncdropped flip means the storage was
 * emptied or rewritten.  A missing analyze-cache row-count baseline means we
 * cannot bound the drift.  Either makes the entry unusable.  Caller guarantees
 * entry->statsTuple != NULL and passes rel_baseline zero-initialized (or
 * carried over from previous columns of the same relid).
 */
static bool
fasttrun_stats_entry_usable_ext(Oid relid, FasttrunStatsRelidEntry *relentry,
								const FasttrunStatsEntry *entry,
								int64 ins_now, int64 upd_now, int64 del_now,
								bool truncdropped_now, BlockNumber pages_now,
								FasttrunUsableBaseline *rel_baseline)
{
	int64		churn;
	int64		concentrating;
	int64		upd_delta;
	double		baseline;
	double		threshold;
	double		floor_threshold;
	double		stadistinct;
	double		dratio;

	if (truncdropped_now != entry->collected_truncdropped)
		return false;

	/*
	 * A sample taken while the relation was being written describes the
	 * command snapshot its scan started from, while the counters stored beside
	 * it already include those writes.  Nothing about them can be compared, so
	 * it stays out of use for good.  Only a fresh sample replaces it.  The mark
	 * belongs to the sample, not to the relation's current period: a period
	 * ends, and the sample can outlive it through a savepoint rollback or a
	 * collect that skipped its column.
	 */
	if (entry->unsafe)
		return false;

	/*
	 * pgstat counts tuples_updated per relation, not per column, so a single
	 * UPDATE that rewrote one helper column would age the statistics of every
	 * column of the table.  Charge this column only for UPDATEs that named it
	 * in their target list.  Inserts and deletes still count for all of them,
	 * since they change the row count every distribution rests on.
	 */
	upd_delta = upd_now - entry->collected_upd;
	if (upd_delta != 0 &&
		!fasttrun_stats_column_was_updated(relentry, entry))
		upd_delta = 0;

	if (ins_now == entry->collected_ins &&
		upd_now == entry->collected_upd &&
		del_now == entry->collected_del)
	{
		/*
		 * Counters say nothing changed since collect.  For a temp table that
		 * is not enough: a committed write the hooks did not see (a core ON
		 * COMMIT DELETE ROWS truncate, a direct table AM write) leaves both
		 * sides equal, and this branch would serve a distribution the table
		 * no longer has.  The physical page count survives commit, so anchor
		 * freshness to it: if storage changed by an order of size since
		 * collect, the cached MCV/histogram/n_distinct are untrustworthy --
		 * hide them and let the planner fall back to defaults (as core does
		 * with no pg_statistic row).  The counter path below governs soft
		 * freshness.  This check only guards the "counters see nothing" blind
		 * spot.
		 */
		if (pages_now != entry->collected_pages)
		{
			BlockNumber	base_pages = Max(entry->collected_pages, 1);
			BlockNumber	dead = fasttrun_cache_aborted_pages(relid);
			BlockNumber	live_pages = pages_now;
			double		ratio;

			/* Dead pages of rolled-back writes are not a refill. */
			if (dead > entry->collected_dead)
				live_pages -= Min(live_pages, dead - entry->collected_dead);
			ratio = (double) live_pages / (double) base_pages;

			/*
			 * Coarse backstop.  Physical size is bloat-contaminated (a bulk
			 * UPDATE or a rolled-back subxact roughly doubles pages without
			 * changing the data), so it cannot carry the fine
			 * stats_refresh_threshold semantics without defeating soft
			 * freshness and savepoint restore.  Only an order-of-size change
			 * -- the footprint tripled or dropped below a third since collect
			 * -- is taken as "the table was refilled while the counters were
			 * blind", hiding the now-meaningless sample so the planner falls
			 * back to defaults.
			 */
			if (ratio >= FASTTRUN_STALE_SIZE_FACTOR ||
				ratio <= 1.0 / FASTTRUN_STALE_SIZE_FACTOR)
				return false;
		}
		return true;			/* no DML and size stable -- exactly fresh */
	}

	/*
	 * Only UPDATEs of other columns since collect.  Such a rewrite also grows
	 * the page count, which the cross-commit anchor above would read as a
	 * refill -- but here the counters name the cause, so there is nothing to
	 * guess and this column's sample still describes the data.
	 */
	if (ins_now == entry->collected_ins &&
		del_now == entry->collected_del &&
		upd_delta == 0)
		return true;

	if (!rel_baseline->resolved)
	{
		FasttrunAnalyzeCacheEntry *aentry = fasttrun_cache_lookup(relid);

		rel_baseline->resolved = true;
		rel_baseline->available = (aentry != NULL &&
								   aentry->state.has_relstats);
		rel_baseline->tuples = rel_baseline->available
			? (double) Max(aentry->state.cached_tuples, 1) : 1.0;
	}
	if (!rel_baseline->available)
		return false;

	baseline = rel_baseline->tuples;
	concentrating = (ins_now - entry->collected_ins) + upd_delta;
	if (concentrating < 0)
		concentrating = -concentrating;
	churn = del_now - entry->collected_del;
	if (churn < 0)
		churn = -churn;
	churn += concentrating;

	if (((double) churn / baseline) >= fasttrun_stats_refresh_threshold)
		return false;

	/* stadistinct < 0 is a negative fraction of rows, > 0 is an absolute count. */
	stadistinct = ((Form_pg_statistic) GETSTRUCT(entry->statsTuple))->stadistinct;
	dratio = (stadistinct < 0.0) ? -stadistinct : stadistinct / baseline;

	/*
	 * Scale tolerance down with cardinality: a near-unique column (dratio -> 1)
	 * gets FASTTRUN_SKEW_REFRESH_FLOOR, a low-cardinality column keeps the
	 * configured threshold, and everything between scales linearly -- no cliff,
	 * moderate-cardinality columns are protected too.  The floor never loosens
	 * a stricter user setting.
	 *
	 * The scaled tolerance applies to inserted and updated rows only: they can
	 * pile onto one value.  A delete cannot raise the share of any remaining
	 * value by more than 1 / (1 - deleted share), so deleted rows count
	 * against the configured threshold above.
	 */
	floor_threshold = Min(fasttrun_stats_refresh_threshold,
						  FASTTRUN_SKEW_REFRESH_FLOOR);
	threshold = Max(fasttrun_stats_refresh_threshold * (1.0 - dratio),
					floor_threshold);

	return ((double) concentrating / baseline) < threshold;
}

/* Single-column convenience wrapper: resolves the baseline itself. */
static bool
fasttrun_stats_entry_usable(Oid relid, FasttrunStatsRelidEntry *relentry,
							const FasttrunStatsEntry *entry,
							int64 ins_now, int64 upd_now, int64 del_now,
							bool truncdropped_now, BlockNumber pages_now)
{
	FasttrunUsableBaseline rel_baseline = {0};

	return fasttrun_stats_entry_usable_ext(relid, relentry, entry, ins_now,
										   upd_now, del_now, truncdropped_now,
										   pages_now, &rel_baseline);
}

/*
 * Find or create the freshness-cache slot for a relid.  Planner scope only:
 * callers must check fasttrun_in_planner first.  A new slot starts with all
 * memo flags clear.  When the cache is full the slot is claimed round-robin.
 * FIFO is good enough for the planner-scoped lifetime, and an overwritten
 * relid is a miss on its next probe, never a wrong answer.
 */
static FasttrunFreshnessCacheEntry *
fasttrun_freshness_cache_slot(Oid relid)
{
	FasttrunFreshnessCacheEntry *slot;
	int			i;

	for (i = 0; i < fasttrun_freshness_cache_used; i++)
	{
		slot = &fasttrun_freshness_cache[i];
		if (slot->relid == relid)
			return slot;
	}

	if (fasttrun_freshness_cache_used < FASTTRUN_FRESHNESS_CACHE_SLOTS)
	{
		slot = &fasttrun_freshness_cache[fasttrun_freshness_cache_used++];
	}
	else
	{
		slot = &fasttrun_freshness_cache[fasttrun_freshness_cache_next_evict];
		fasttrun_freshness_cache_next_evict =
			(fasttrun_freshness_cache_next_evict + 1) % FASTTRUN_FRESHNESS_CACHE_SLOTS;
	}

	slot->relid = relid;
	slot->locator_checked = false;
	slot->locator_ok = false;
	slot->counters_cached = false;
	slot->upd_cached = false;
	slot->owner_checked = false;
	slot->owner_heap = InvalidOid;
	return slot;
}

/*
 * Does the relid still map to the storage the stats were collected for?
 * Both stats hooks call this once per Var, so while the planner runs the
 * verdict is memoized per relid: one RelationIdGetRelation per relid per
 * plan.  Outside the planner every call probes directly.
 */
static bool
fasttrun_stats_relid_locator_valid(Oid relid,
								   FasttrunStatsRelidEntry *relentry)
{
	Relation	rel;
	RelFileLocatorBackend rlb;
	bool		valid;
	FasttrunFreshnessCacheEntry *slot = NULL;

	if (!relentry->heap_rlb_valid)
		return false;

	if (fasttrun_in_planner)
	{
		slot = fasttrun_freshness_cache_slot(relid);
		if (slot->locator_checked)
			return slot->locator_ok;
	}

	rel = RelationIdGetRelation(relid);
#ifdef USE_ASSERT_CHECKING
	fasttrun_test_locator_opens++;
#endif
	if (!RelationIsValid(rel))
		valid = false;
	else
	{
		rlb.locator = rel->rd_locator;
		rlb.backend = rel->rd_backend;
		valid = fasttrun_rlb_equals(relentry->heap_rlb, rlb);
		RelationClose(rel);
	}
	if (!valid)
		fasttrun_stats_forget_relid(relid);
	if (slot != NULL)
	{
		slot->locator_checked = true;
		slot->locator_ok = valid;
	}
	return valid;
}

