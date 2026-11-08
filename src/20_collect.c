/*-------------------------------------------------------------------------
 *
 * src/20_collect.c
 *      Index relstats, index expressions and fasttrun_collect_and_store.
 *
 * What it holds.  The fraction and relstats of indexes, the drift check that
 * anchors index invalidation, statistics of index expressions and
 * fasttrun_collect_and_store, the entry point of column collection.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/20_collect.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
static double
fasttrun_estimate_index_fraction(Relation heaprel, Relation indexrel,
								 IndexInfo *indexInfo,
								 HeapTuple *sample, int sample_count)
{
	ExprState  *predicate;
	EState	   *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	int			i;
	int			matched = 0;

	if (indexInfo->ii_Predicate == NIL)
		return 1.0;
	if (sample_count <= 0)
		return 0.0;

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heaprel), &TTSOpsHeapTuple);
	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	for (i = 0; i < sample_count; i++)
	{
		ResetExprContext(econtext);
		ExecStoreHeapTuple(sample[i], slot, false);
		if (ExecQual(predicate, econtext))
			matched++;
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	return (double) matched / (double) sample_count;
}

static List *
fasttrun_open_index_rels(Relation rel)
{
	List	   *index_oids = RelationGetIndexList(rel);
	List	   *index_rels = NIL;
	ListCell   *lc;

	foreach(lc, index_oids)
		index_rels = lappend(index_rels,
							 index_open(lfirst_oid(lc), AccessShareLock));
	list_free(index_oids);
	return index_rels;
}

static void
fasttrun_close_index_rels(List *index_rels)
{
	ListCell   *lc;

	foreach(lc, index_rels)
		index_close((Relation) lfirst(lc), AccessShareLock);
	list_free(index_rels);
}

static bool
fasttrun_index_rels_have_partial(List *index_rels)
{
	ListCell   *lc;

	foreach(lc, index_rels)
	{
		if (RelationGetIndexPredicate((Relation) lfirst(lc)) != NIL)
			return true;
	}
	return false;
}

/*
 * Has an index's page count moved past invalidate_threshold since cached
 * plans last saw it?  The anchor is the count published at the owning heap's
 * last invalidation.  Without one (first sync, after a truncate or an
 * eviction) any change counts, as does any change with the threshold at 0.
 */
static bool
fasttrun_index_pages_drifted(Oid index_relid, BlockNumber index_pages)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(index_relid);
	BlockNumber	anchor;

	if (entry == NULL || !entry->state.last_inval_valid ||
		fasttrun_invalidate_threshold <= 0.0)
		return true;
	anchor = entry->state.last_inval_pages;
	if (anchor == 0)
		return index_pages != 0;
	return fabs((double) index_pages - (double) anchor) / (double) anchor >=
		fasttrun_invalidate_threshold;
}

/*
 * After an invalidation every surviving plan was built on the page counts
 * just published, so they become the anchors of the heap's indexes.
 */
static void
fasttrun_anchor_index_invalidation(Relation rel)
{
	List	   *index_oids = RelationGetIndexList(rel);
	ListCell   *lc;

	foreach(lc, index_oids)
	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(lfirst_oid(lc));

		if (entry == NULL || !entry->state.has_relstats)
			continue;
		/* The journal must see every undo push, or a rollback misses it. */
		fasttrun_xact_mark_relid(entry->relid, RelationGetRelid(rel),
								 FASTTRUN_TOUCH_ANALYZE);
		fasttrun_analyze_save_undo(entry);
		entry->state.last_inval_pages = entry->state.cached_pages;
		entry->state.last_inval_tuples = (float4) entry->state.cached_tuples;
		entry->state.last_inval_valid = true;
	}
	list_free(index_oids);
}

/*
 * Refresh relpages/reltuples/relallvisible of every index of `rel`.
 * index_rels is an already-open list from fasttrun_open_index_rels so hot
 * callers pay one open per index per analyze.  NIL means open (and close)
 * here.
 */
static bool
fasttrun_update_index_relstats(Relation rel, List *index_rels,
							   HeapTuple *sample, int sample_count,
							   int64 totalrows, bool partial_upper_bound)
{
	ListCell   *lc;
	bool		changed = false;
	List	   *opened = NIL;

	if (index_rels == NIL)
		index_rels = opened = fasttrun_open_index_rels(rel);

	/*
	 * Both reinjection paths reach index entries through the heap's entry, so
	 * the heap keeps one while its indexes have relstats, also after a collect
	 * without fasttrun_analyze.
	 */
	if (index_rels != NIL)
	{
		FasttrunAnalyzeCacheEntry *heap_entry = fasttrun_cache_ensure_file_entry(rel);

		if (!heap_entry->state.owns_index_relstats)
		{
			fasttrun_analyze_save_undo(heap_entry);
			heap_entry->state.owns_index_relstats = true;
		}
	}

	foreach(lc, index_rels)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		BlockNumber	index_pages;
		double		tuple_fract;
		int64		index_tuples;
		bool		is_partial;
		bool		set_reltuples = true;

		is_partial = (RelationGetIndexPredicate(indexrel) != NIL);

		index_pages = RelationGetNumberOfBlocks(indexrel);
		if (is_partial && sample_count > 0)
		{
			IndexInfo  *indexInfo = BuildIndexInfo(indexrel);

			tuple_fract = fasttrun_estimate_index_fraction(rel, indexrel,
															   indexInfo, sample,
															   sample_count);
		}
		else if (is_partial && !partial_upper_bound)
		{
			/*
			 * Sample-less refresh after a sample-based one already set this
			 * partial index's tuple count this analyze: keep that estimate,
			 * refresh only relpages/relallvisible below.
			 */
			tuple_fract = 0.0;
			set_reltuples = false;
		}
		else
			/*
			 * A full index, or a partial index whose tuple count has no better
			 * source: use totalrows as an upper bound -- better than leaving a
			 * stale under-estimate.
			 */
			tuple_fract = 1.0;
		index_tuples = set_reltuples
			? (int64) ceil(tuple_fract * (double) totalrows)
			: (int64) indexrel->rd_rel->reltuples;

		/*
		 * Report a change only for stats that move independently of the heap
		 * row count: index relpages and relallvisible (which vary with
		 * expression and varlena-keyed indexes and btree dedup), and a partial
		 * index's tuple count (which tracks the predicate, not the heap).  A
		 * plain index's reltuples is a linear copy of the heap row count that
		 * the caller already weighs, so it is not a separate signal here.
		 * relpages passes the same invalidate_threshold as the heap, measured
		 * from the value at the last invalidation: one more page out of a
		 * hundred is not worth a walk over every cached plan.
		 */
		if (indexrel->rd_rel->relallvisible != 0 ||
			(is_partial &&
			 indexrel->rd_rel->reltuples != (float4) index_tuples) ||
			(indexrel->rd_rel->relpages != index_pages &&
			 fasttrun_index_pages_drifted(RelationGetRelid(indexrel),
										  index_pages)))
			changed = true;

		fasttrun_cache_set_owning_heap(fasttrun_cache_store_relstats(indexrel,
														 index_pages,
														 index_tuples,
														 0),
										   rel);
		FASTTRUN_TEST_FAILPOINT("index_publication_boundary", 0);
		indexrel->rd_rel->relpages = index_pages;
		indexrel->rd_rel->reltuples = (float4) index_tuples;
		indexrel->rd_rel->relallvisible = 0;
	}

	if (opened != NIL)
		fasttrun_close_index_rels(opened);

	return changed;
}


/* qsort comparator: order HeapTuples by physical TID (block, offset). */
static int
fasttrun_cmp_heap_tuples_by_tid(const void *a, const void *b)
{
	HeapTuple	ta = *(HeapTuple *) a;
	HeapTuple	tb = *(HeapTuple *) b;

	return ItemPointerCompare(&ta->t_self, &tb->t_self);
}

/*
 * Above this raw width a varlena sample value is not detoasted for the
 * lightweight Haas-Stokes path: materializing multi-MB TOAST payloads for
 * every sampled row would blow up backend memory.  Matches core
 * analyze.c:WIDTH_THRESHOLD -- such values are counted as distinct instead.
 */
#define FASTTRUN_WIDTH_THRESHOLD	1024

/*
 * For every column of rel, compute stats from the sample and store in
 * the session-local cache.  The Haas-Stokes path skips columns without a
 * btree-orderable type (planner falls back to defaults).  Also captures
 * the current pgstat counters as a freshness baseline -- the planner hook
 * compares against them and refuses to return stale stats after DML.
 *
 * Two implementations of "compute the stats" coexist, switched by
 * fasttrun.use_typanalyze (default on):
 *   * on  -> fasttrun_collect_via_typanalyze: full core std_typanalyze
 *           with MCV / histogram / correlation
 *   * off -> fasttrun_collect_via_haas_stokes: lightweight n_distinct /
 *           null_frac / width only.
 * Both branches end at the same fasttrun_stats_cache_store() and
 * publish through the same hooks.
 */
/*
 * Statistics of index expressions, computed as core ANALYZE does in
 * compute_index_stats: each expression of an index is evaluated over the
 * sampled rows and analyzed as a column of the expression's type.  The planner
 * asks for them by index oid and column through get_index_stats_hook.
 *
 * Partial indexes are skipped.  Their rows are a subset of the table, and the
 * planner takes no whole-table statistics from them (examine_variable leaves
 * that decision to the hook), so their expressions stay on defaults.
 *
 * The expressions run as the table owner in a restricted security context,
 * with search_path restricted on PG17 and later, as in core ANALYZE.  An
 * expression that yields no statistics this time loses the old ones.
 *
 * Returns true when anything was published.
 */
static bool
fasttrun_collect_index_expressions(Relation rel, HeapTuple *sample,
								   int sample_count, int64 totalrows,
								   TupleDesc pg_stats_desc,
								   MemoryContext per_col_mcxt,
								   const FasttrunPgstatSnapshot *snapshot)
{
	List	   *index_oids;
	ListCell   *lc;
	bool		published = false;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	if (!rel->rd_rel->relhasindex)
		return false;
	index_oids = RelationGetIndexList(rel);
	if (index_oids == NIL)
		return false;

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(rel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
#if PG_VERSION_NUM >= 170000
	RestrictSearchPath();
#endif

	foreach(lc, index_oids)
	{
		Relation	indexrel = index_open(lfirst_oid(lc), AccessShareLock);
		IndexInfo  *indexInfo;
		ListCell   *expr_item;
		MemoryContext ind_mcxt;
		MemoryContext oldcxt;
		EState	   *estate;
		ExprContext *econtext;
		TupleTableSlot *slot;
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		int			expr_cols[INDEX_MAX_KEYS];
		Node	   *expr_nodes[INDEX_MAX_KEYS];
		int16		expr_typlen[INDEX_MAX_KEYS];
		bool		expr_typbyval[INDEX_MAX_KEYS];
		int			nexpr = 0;
		Datum	   *exprvals;
		bool	   *exprnulls;
		int			i;
		int			row;

		if (RelationGetIndexExpressions(indexrel) == NIL ||
			RelationGetIndexPredicate(indexrel) != NIL)
		{
			index_close(indexrel, AccessShareLock);
			continue;
		}

		ind_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "fasttrun index expressions",
										 ALLOCSET_DEFAULT_SIZES);
		oldcxt = MemoryContextSwitchTo(ind_mcxt);
		indexInfo = BuildIndexInfo(indexrel);
		expr_item = list_head(indexInfo->ii_Expressions);
		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			if (indexInfo->ii_IndexAttrNumbers[i] != 0)
				continue;
			expr_cols[nexpr] = i;
			expr_nodes[nexpr] = (Node *) lfirst(expr_item);
			get_typlenbyval(exprType(expr_nodes[nexpr]), &expr_typlen[nexpr],
							&expr_typbyval[nexpr]);
			expr_item = lnext(indexInfo->ii_Expressions, expr_item);
			nexpr++;
		}

		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);
		econtext->ecxt_scantuple = slot;
		exprvals = (Datum *) palloc(sizeof(Datum) * sample_count * nexpr);
		exprnulls = (bool *) palloc(sizeof(bool) * sample_count * nexpr);

		for (row = 0; row < sample_count; row++)
		{
			CHECK_FOR_INTERRUPTS();
			ResetExprContext(econtext);
			ExecStoreHeapTuple(sample[row], slot, false);
			FormIndexDatum(indexInfo, slot, estate, values, isnull);
			for (i = 0; i < nexpr; i++)
			{
				int			col = expr_cols[i];

				exprnulls[row * nexpr + i] = isnull[col];
				exprvals[row * nexpr + i] = isnull[col] ? (Datum) 0 :
					datumCopy(values[col], expr_typbyval[i], expr_typlen[i]);
			}
		}
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < nexpr; i++)
		{
			AttrNumber	indexattnum = expr_cols[i] + 1;
			VacAttrStats *stats;
			HeapTuple	stats_tuple = NULL;

			MemoryContextReset(per_col_mcxt);
			oldcxt = MemoryContextSwitchTo(per_col_mcxt);
			stats = fasttrun_examine_expression(indexrel, indexattnum,
												expr_nodes[i], per_col_mcxt);
			if (stats != NULL)
			{
				stats->exprvals = exprvals + i;
				stats->exprnulls = exprnulls + i;
				stats->rowstride = nexpr;
				stats->compute_stats(stats, fasttrun_sample_fetch_func,
									 sample_count, (double) totalrows);
				if (stats->stats_valid)
					stats_tuple =
						fasttrun_build_pg_statistic_tuple(pg_stats_desc,
														  RelationGetRelid(indexrel),
														  indexattnum, stats);
			}
			MemoryContextSwitchTo(oldcxt);

			if (stats_tuple != NULL)
			{
				(void) fasttrun_stats_set_entry_state(rel, indexattnum, false,
													  RelationGetRelid(indexrel),
													  FASTTRUN_COLUMN_LOCAL_CANDIDATE,
													  stats_tuple, snapshot);
				published = true;
			}
			else
			{
				FasttrunStatsKey key;
				FasttrunStatsEntry *entry;

				fasttrun_stats_expr_key_init(&key, RelationGetRelid(rel),
											 indexattnum,
											 RelationGetRelid(indexrel));
				entry = fasttrun_stats_cache == NULL ? NULL :
					(FasttrunStatsEntry *) hash_search(fasttrun_stats_cache, &key,
													   HASH_FIND, NULL);
				if (entry != NULL)
					(void) fasttrun_stats_entry_neutralize(RelationGetRelid(rel),
														   entry);
			}
		}

		MemoryContextDelete(ind_mcxt);
		index_close(indexrel, AccessShareLock);
	}

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);
	list_free(index_oids);
	return published;
}

/*
 * The result separates publishing a candidate from a transition to neutral.
 * Callers invalidate only when either changes planner-visible ownership.
 */
static FasttrunCollectResult
fasttrun_collect_and_store(Relation rel, HeapTuple *sample, int sample_count,
						   int64 totalrows, bool sample_needs_tid_sort)
{
	FasttrunCollectResult result = {0};
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Relation	pg_stats_rel;
	TupleDesc	pg_stats_desc;
	int			natts = tupdesc->natts;
	int			a;
	FasttrunSampleColumns cols;
	MemoryContext per_col_mcxt;
	MemoryContext oldcxt;
	int64		snap_ins = 0;
	int64		snap_upd = 0;
	int64		snap_del = 0;
	bool		snap_truncdropped = false;
	BlockNumber	snap_pages;

	if (sample_count <= 0)
		return result;

	result.unsafe = fasttrun_stats_begin_collect_period(rel);

	/*
	 * Physical block count at collect time: the freshness check anchors to it
	 * to detect a refill the counters did not see.
	 */
	snap_pages = RELKIND_HAS_STORAGE(rel->rd_rel->relkind)
		? RelationGetNumberOfBlocks(rel) : 0;

	/*
	 * track_counts=off (or pgstat otherwise unavailable) -> no baseline
	 * for the freshness check -> do not cache at all, the planner will
	 * fall back to defaults.  This is a "silent killer" -- plans
	 * silently degrade without any explicit signal, so we raise a
	 * WARNING once per backend to make DBAs see it in the server log
	 * (and the psql client sees it too).
	 */
	if (!fasttrun_read_pgstat_counters(rel, &snap_ins, &snap_upd, &snap_del,
									   &snap_truncdropped))
	{
		result.became_neutral =
			fasttrun_stats_cache_evict_relid(RelationGetRelid(rel));

		if (!fasttrun_warned_track_counts_off)
		{
			ereport(WARNING,
					(errmsg("fasttrun: pgstat tracking is unavailable, column statistics will not be cached for this session"),
					 errdetail("fasttrun_analyze and fasttrun_collect_stats rely on per-backend pgstat counters to validate cached column statistics after DML. "
							   "With pgstat unavailable (usually because track_counts = off), the planner hook cannot return cached stats and will fall back to default selectivity. "
							   "Plans for temp tables may silently degrade."),
					 errhint("Set track_counts = on in postgresql.conf or per session (SET track_counts = on) to re-enable fasttrun column statistics caching.")));
			fasttrun_warned_track_counts_off = true;
		}
		return result;
	}

	pg_stats_rel = table_open(StatisticRelationId, AccessShareLock);
	pg_stats_desc = RelationGetDescr(pg_stats_rel);

	/* Slices are loaded lazily, after the TID sort below. */
	fasttrun_sample_columns_init(&cols, sample, sample_count, tupdesc);
	per_col_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "fasttrun collect tmp",
										 ALLOCSET_DEFAULT_SIZES);

	if (fasttrun_use_typanalyze)
	{
		/* ----------------------------------------------------------
		 * Branch A: full core std_typanalyze.
		 *
		 * Sort the sample by physical TID first.  compute_scalar_stats
		 * (and other compute_stats variants) use the array position as
		 * the physical-order rank when computing correlation.  Core
		 * acquire_sample_rows() returns rows in block/offset order, while
		 * reservoir sampling places them in random slots.  Without
		 * this sort, correlation degrades to ~0 even on perfectly
		 * ordered data.
		 *
		 * For each non-dropped column we set up a fresh VacAttrStats
		 * inside per_col_mcxt, point its standard fetch fields at our
		 * sample, run compute_stats, serialize the result into a
		 * pg_statistic-shaped HeapTuple and hand it to the cache.
		 * Then reset per_col_mcxt to free everything compute_stats
		 * allocated for this column in one shot.
		 * ---------------------------------------------------------- */
		if (sample_needs_tid_sort && sample_count > 1)
			qsort(sample, sample_count, sizeof(HeapTuple),
				  fasttrun_cmp_heap_tuples_by_tid);

		for (a = 0; a < natts; a++)
		{
			AttrNumber			attnum = a + 1;
			VacAttrStats	   *stats;
			HeapTuple			stats_tuple;

			CHECK_FOR_INTERRUPTS();
			MemoryContextReset(per_col_mcxt);
			oldcxt = MemoryContextSwitchTo(per_col_mcxt);

			stats = fasttrun_examine_attribute(rel, attnum, per_col_mcxt);
			if (stats == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			/* compute_stats reads the deformed column through its fetch func. */
			stats->rows = sample;
			stats->tupDesc = tupdesc;
			fasttrun_sample_column(&cols, a, &stats->exprvals,
								   &stats->exprnulls);
			stats->rowstride = 1;

			stats->compute_stats(stats, fasttrun_sample_fetch_func,
								 sample_count, (double) totalrows);

			if (!stats->stats_valid)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			fasttrun_apply_attribute_options(rel, attnum, false,
											 &stats->stadistinct);

			/*
			 * Build tuple inside per_col_mcxt so intermediates are
			 * reclaimed on the next MemoryContextReset.
			 */
			stats_tuple = fasttrun_build_pg_statistic_tuple(pg_stats_desc,
															RelationGetRelid(rel),
															attnum, stats);
			MemoryContextSwitchTo(oldcxt);

			/* cache_store copies tuple into fasttrun_stats_mcxt */
			fasttrun_stats_cache_store(rel, attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped, snap_pages);
			result.published_any = true;
			heap_freetuple(stats_tuple);
		}

		{
			FasttrunPgstatSnapshot snapshot;

			snapshot.inserted = snap_ins;
			snapshot.updated = snap_upd;
			snapshot.deleted = snap_del;
			snapshot.truncdropped = snap_truncdropped;
			snapshot.pages = snap_pages;
			if (fasttrun_collect_index_expressions(rel, sample, sample_count,
												   totalrows, pg_stats_desc,
												   per_col_mcxt, &snapshot))
				result.published_any = true;
		}
	}
	else
	{
		Datum	   *values;
		Datum	   *col_values;
		bool	   *col_nulls;

		/* ----------------------------------------------------------
		 * Branch B: lightweight Haas-Stokes path.
		 *
		 * Computes only n_distinct / null_frac / width via a per-
		 * column qsort_arg.  No MCV / histogram / correlation, no
		 * type-specific stats -- but ~5x cheaper on wide tables and
		 * available as a fallback if a particular type-specific
		 * typanalyze misbehaves.
		 * ---------------------------------------------------------- */
		values = (Datum *) palloc(sizeof(Datum) * sample_count);

		for (a = 0; a < natts; a++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, a);
			AttrNumber		   attnum = a + 1;
			int				   attstattarget;
			TypeCacheEntry	  *typentry;
			SortSupportData	ssup;
			int				   n_nonnull = 0;
			int				   n_array = 0;
			int				   toowide_cnt = 0;
			int				   n_null = 0;
			int64			   total_width = 0;
			int				   ndistinct = 0;
			int				   nmultiple = 0;
			int				   dups_cnt = 0;
			int				   i;
			float4			   stanullfrac;
			int32			   stawidth;
			float4			   stadistinct;
			HeapTuple		   stats_tuple;

			if (!fasttrun_attribute_is_analyzable(attr))
				continue;
			attstattarget = fasttrun_get_attstattarget(rel, attnum);
			if (attstattarget == 0)
				continue;

			typentry = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);
			if (!OidIsValid(typentry->lt_opr))
				continue;	/* no btree comparator -> skip */

			MemoryContextReset(per_col_mcxt);
			oldcxt = MemoryContextSwitchTo(per_col_mcxt);
			fasttrun_sample_column(&cols, a, &col_values, &col_nulls);

			for (i = 0; i < sample_count; i++)
			{
				bool	isnull = col_nulls[i];
				Datum	d = col_values[i];

				if (isnull)
				{
					n_null++;
					continue;
				}

				/*
				 * Accumulate the average-width sample exactly once per type:
				 * pass-by-value, varlena, and fixed-length by-reference are
				 * mutually exclusive branches, so no width is double-counted.
				 * Only non-toowide values land in values[] for the distinct
				 * sort.  n_nonnull counts every non-null, n_array the sortable
				 * subset.
				 */
				if (attr->attbyval)
				{
					values[n_array++] = d;
					total_width += attr->attlen;
				}
				else if (attr->attlen == -1)
				{
					/*
					 * Excessively wide varlena: don't detoast (would
					 * materialize the full TOAST payload and can OOM the
					 * backend on multi-MB values).  Count it as too-wide --
					 * assumed distinct -- mirroring core analyze.c.  The size
					 * probe reads the raw length from the header without
					 * detoasting.
					 */
					/* Stored (possibly compressed) width, as core: before detoast. */
					total_width += VARSIZE_ANY(DatumGetPointer(d));

					if (toast_raw_datum_size(d) > FASTTRUN_WIDTH_THRESHOLD)
					{
						toowide_cnt++;
						n_nonnull++;
						continue;
					}

					/* In-range varlena: detoast once, for the comparisons only. */
					{
						struct varlena *detoasted = pg_detoast_datum_packed(
							(struct varlena *) DatumGetPointer(d));

						values[n_array++] = PointerGetDatum(detoasted);
					}
				}
				else
				{
					/* Fixed-length by-reference (uuid, name, interval, ...). */
					values[n_array++] = d;
					if (attr->attlen > 0)
						total_width += attr->attlen;
				}

				n_nonnull++;
			}

			if (n_nonnull == 0)
			{
				stanullfrac = 1.0f;
				stawidth = (attr->attlen > 0) ? attr->attlen : 0;
				stadistinct = 0.0f;
			}
			else
			{
				if (n_array > 0)
				{
					memset(&ssup, 0, sizeof(ssup));
					ssup.ssup_cxt = per_col_mcxt;
					ssup.ssup_collation = attr->attcollation;
					ssup.ssup_nulls_first = false;
					PrepareSortSupportFromOrderingOp(typentry->lt_opr, &ssup);

					qsort_arg(values, n_array, sizeof(Datum),
							  fasttrun_datum_cmp, &ssup);

					/* Count distinct values and multiples in the sorted run. */
					dups_cnt = 1;
					for (i = 1; i <= n_array; i++)
					{
						bool	end_of_run;

						if (i == n_array)
							end_of_run = true;
						else
							end_of_run = ApplySortComparator(values[i - 1], false,
															 values[i], false,
															 &ssup) != 0;
						if (end_of_run)
						{
							ndistinct++;
							if (dups_cnt > 1)
								nmultiple++;
							dups_cnt = 1;
						}
						else
							dups_cnt++;
					}
				}

				/*
				 * Too-wide values were never compared, so treat each as its
				 * own distinct singleton (same assumption core makes).
				 */
				stanullfrac = (float4) n_null / (float4) sample_count;
				stawidth = (int32) (total_width / n_nonnull);
				stadistinct = fasttrun_estimate_ndistinct(n_nonnull,
														  ndistinct + toowide_cnt,
														  nmultiple, totalrows,
														  (double) stanullfrac);
			}

			fasttrun_apply_attribute_options(rel, attnum, false,
											 &stadistinct);
			MemoryContextSwitchTo(oldcxt);

			stats_tuple = fasttrun_build_stats_tuple(pg_stats_desc,
													 RelationGetRelid(rel),
													 attnum,
													 stanullfrac, stawidth,
													 stadistinct);

			fasttrun_stats_cache_store(rel, attnum, stats_tuple,
									   snap_ins, snap_upd, snap_del,
									   snap_truncdropped, snap_pages);
			result.published_any = true;
			heap_freetuple(stats_tuple);
		}

		pfree(values);
	}

	MemoryContextDelete(per_col_mcxt);
	fasttrun_sample_columns_free(&cols);
	table_close(pg_stats_rel, AccessShareLock);
	result.index_changed =
		fasttrun_update_index_relstats(rel, NIL, sample, sample_count,
									   totalrows, true);
	fasttrun_stats_relid_remember_locator(rel);

	/*
	 * Note: the stats-collection baseline (used by the delta-hit refresh
	 * path to decide whether DML churn warrants another sample) lives in
	 * the analyze cache entry, not in the per-(relid,attnum) stats cache.
	 * It is set by the callers of this function -- fasttrun_analyze()
	 * cold path and refresh path -- both of which already own (or just
	 * created) an analyze cache entry to attach it to.  fasttrun_collect_
	 * stats() only advances a baseline that already exists and never
	 * creates the entry: it is a "give me column stats now" entry point
	 * that must not seed delta-math state.
	 */
	if (result.published_any)
		fasttrun_count(result.unsafe ? FTC_COLUMN_COLLECTS_DURING_WRITE :
					   FTC_COLUMN_COLLECTS);
	return result;
}

