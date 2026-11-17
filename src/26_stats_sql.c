/*-------------------------------------------------------------------------
 *
 * src/26_stats_sql.c
 *      SQL functions fasttrun_collect_stats and fasttrun_inspect_stats.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/26_stats_sql.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * fasttrun_collect_stats(text)
 *
 * Explicit collect: reservoir sample -> per-column stats -> session-local
 * stats cache.  No catalog write.  fasttrun_collect_and_store() is the
 * same step the cold fasttrun_analyze runs when fasttrun.auto_collect_stats
 * is on (default), so fasttrun.use_typanalyze decides what is computed:
 * the full std_typanalyze output when on (default), only n_distinct,
 * null_frac and width when off.  Unlike fasttrun_analyze, this call always
 * reads the whole heap: fasttrun.max_analyze_pages does not apply.  It is
 * the way to get an exact row count and a row sample drawn from the whole
 * heap on demand where a block sample cannot be trusted: values grouped by
 * page, or a sparse heap after a mass DELETE, where a block sample can miss
 * every live block.
 */
Datum
fasttrun_collect_stats(PG_FUNCTION_ARGS)
{
	text	   *name = PG_GETARG_TEXT_P(0);
	RangeVar   *relvar;
	Oid			relOid;
	Relation	rel;
	int64		tuples_count = 0;
	HeapTuple  *sample;
	int			sample_count = 0;
	int			sample_target;
	int			i;
	int			saved_collect_depth;
	bool		relation_policy_changed;
	bool		planner_fallback_changed;
	FasttrunCollectResult collect_result = {0};

	fasttrun_reject_in_atx("fasttrun_collect_stats");
	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);

	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_collect_stats: relation \"%s\" is not a local temporary table",
						RelationGetRelationName(rel))));
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		rel->rd_rel->relhassubclass)
	{
		table_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fasttrun_collect_stats: relation \"%s\" is partitioned or an inheritance parent",
						RelationGetRelationName(rel)),
				 errhint("Use core SQL ANALYZE for inheritance or partition traversal.")));
	}

	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		table_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_collect_stats: relation \"%s\" is not a heap table", relvar->relname)));
	}
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_collect_stats: relation \"%s\" is not heap-AM",
						RelationGetRelationName(rel))));
	}
	if (fasttrun_passthrough)
	{
		table_close(rel, NoLock);
		fasttrun_passthrough_command("ANALYZE", relOid);
		PG_RETURN_VOID();
	}
	fasttrun_poison_check_relation(rel, "fasttrun_collect_stats", false);

	if (fasttrun_collect_relid_active(relOid))
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_VOID();
	}
	saved_collect_depth = fasttrun_active_collect_depth;
	if (saved_collect_depth >= FASTTRUN_MAX_ACTIVE_COLLECTS)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("fasttrun: nested statistics collection depth exceeds %d",
						FASTTRUN_MAX_ACTIVE_COLLECTS)));
	}
	fasttrun_active_collect_relids[fasttrun_active_collect_depth++] = relOid;

	PG_TRY();
	{
		/* Follow the current file and adopt empty storage before saving undo. */
		{
			FasttrunAnalyzeCacheEntry *aentry = fasttrun_cache_lookup(relOid);

			if (aentry != NULL)
			{
				fasttrun_cache_follow_file(aentry, rel);
				if (fasttrun_cache_storage_check_due(aentry, relOid))
					(void) fasttrun_cache_check_storage(aentry, rel);
			}
		}

		relation_policy_changed = fasttrun_stats_set_relation_policy(rel,
														FASTTRUN_REL_LOCAL_NEUTRAL);
		planner_fallback_changed = relation_policy_changed &&
			fasttrun_relation_has_core_stats(rel);
		if (fasttrun_stats_can_hide_extended(rel) &&
			fasttrun_cache_mark_extended_stale(rel))
			planner_fallback_changed = true;

		/*
		 * Register for per-xact bookkeeping -- callbacks walk this list only.
		 * PLAN_INVALIDATE comes from the actual mutation sites.
		 */
		fasttrun_xact_mark_relid(relOid, relOid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_STATS);

		sample_target = fasttrun_effective_sample_target(rel);

		if (sample_target <= 0 ||
			fasttrun_stats_budget_blocks_collect(rel, true))
		{
			bool		became_neutral = false;

			if (sample_target <= 0)
			{
				if (fasttrun_sample_rows == 0)
					became_neutral = fasttrun_stats_neutralize_core_overrides(relOid);
				else
					became_neutral = fasttrun_stats_cache_evict_relid(relOid);
			}
			if (planner_fallback_changed || became_neutral)
			{
				fasttrun_xact_mark_relid(relOid, relOid,
										 FASTTRUN_TOUCH_PLAN_INVALIDATE);
				fasttrun_invalidate_local_plan_cache(relOid);
			}
		}
		else
		{
			sample = (HeapTuple *) palloc(sizeof(HeapTuple) * sample_target);

			fasttrun_scan_with_sample(rel, &tuples_count,
									  sample, sample_target, &sample_count);

			if (sample_count > 0)
			{
				collect_result = fasttrun_collect_and_store(rel, sample, sample_count,
															tuples_count,
															tuples_count > sample_target);
			}
			else
			{
				collect_result.became_neutral =
					fasttrun_stats_cache_evict_relid(relOid);
				collect_result.index_changed =
					fasttrun_update_index_relstats(rel, NIL, NULL, 0,
												   tuples_count, true);
				/* An empty scan under a running writer says nothing either. */
				collect_result.unsafe = fasttrun_stats_relation_written_now(rel);
			}
			if (planner_fallback_changed || collect_result.published_any ||
				collect_result.became_neutral || collect_result.index_changed)
			{
				fasttrun_xact_mark_relid(relOid, relOid,
										 FASTTRUN_TOUCH_PLAN_INVALIDATE);
				fasttrun_invalidate_local_plan_cache(relOid);
			}

			for (i = 0; i < sample_count; i++)
				heap_freetuple(sample[i]);
			pfree(sample);

			/*
			 * Advance the analyze-cache stats baseline (if an entry exists)
			 * so the next fasttrun_analyze() does not re-enter the refresh
			 * path on stale pre-collect churn.  We deliberately do NOT seed a
			 * fresh entry here: fasttrun_collect_stats() must not invent
			 * delta-math state behind the user's back.  If the user never
			 * called fasttrun_analyze() for this relid, the auto-refresh
			 * machinery stays disengaged for it -- that's the documented
			 * contract.  A sample taken while the relation was being written
			 * leaves the baseline alone: it never serves the planner, and the
			 * churn it leaves standing makes the next analyze recollect.
			 */
			if (!collect_result.unsafe)
			{
				int64	snap_ins = 0;
				int64	snap_upd = 0;
				int64	snap_del = 0;
				bool	snap_truncdropped = false;

				if (fasttrun_read_pgstat_counters(rel, &snap_ins, &snap_upd, &snap_del,
												  &snap_truncdropped))
				{
					fasttrun_cache_update_stats_baseline_if_present(relOid,
																	snap_ins, snap_upd,
																	snap_del,
																	snap_truncdropped);
				}
			}

			/*
			 * The stored statistics read the dead pages of the file through an entry
			 * that knows only the file, so the contract above holds.
			 */
			if (collect_result.published_any)
				(void) fasttrun_cache_ensure_file_entry(rel);
		}
	}
	PG_FINALLY();
	{
		fasttrun_active_collect_depth = saved_collect_depth;
	}
	PG_END_TRY();

	table_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * fasttrun_inspect_stats(text) RETURNS SETOF pg_catalog.pg_statistic
 *
 * Production-debugging entry point.  Returns the per-(relid, attnum)
 * statsTuples that the column-stats cache currently holds for the
 * named table -- in the **same row shape** as core pg_statistic, so
 * the caller can do
 *
 *   SELECT staattnum, stadistinct, stakind1, stanumbers1, stavalues1
 *   FROM   fasttrun_inspect_stats('temp_xxx');
 *
 * and inspect the stored sample. Planner freshness checks can hide it from
 * a later query even while this function still returns the cached tuple.
 *
 * Implementation notes:
 *   * Materialize-mode SRF (we expect a small handful of rows per
 *     call -- one per analysed column -- so building a tuplestore is
 *     simpler than per-call state and almost free).
 *   * The cached statsTuples are already in pg_statistic shape (built
 *     by fasttrun_build_pg_statistic_tuple from VacAttrStats), so we
 *     just stuff each one straight into the tuplestore.
 *   * If the table doesn't exist or has no cached stats yet, the
 *     function returns an empty set -- same defensive behaviour as
 *     fasttrun_relstats() / fasttrun_analyze() on missing tables.  A
 *     table that is not temp raises an ERROR.
 */
Datum
fasttrun_inspect_stats(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	RangeVar	   *relvar;
	Oid				relOid;
	Relation		rel;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_cxt;
	MemoryContext	oldcxt;
	FasttrunStatsRelidEntry *relentry;
	ListCell	   *lc;

	fasttrun_reject_in_atx("fasttrun_inspect_stats");

	/* Check caller is set-aware. */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/* The function is declared as RETURNS SETOF pg_catalog.pg_statistic,
	 * so get_call_result_type will give us the pg_statistic tupdesc. */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	per_query_cxt = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_cxt);

	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcxt);

	/* Resolve relation, missing -> empty result. */
	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, AccessShareLock, true);
	if (!OidIsValid(relOid))
		return (Datum) 0;

	rel = relation_open(relOid, NoLock);
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		relation_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_inspect_stats: relation is not a temporary table")));
	}
	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		relation_close(rel, NoLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_inspect_stats: relation \"%s\" is not a table",
						relvar->relname)));
	}
	fasttrun_poison_check_relation(rel, "fasttrun_inspect_stats", false);
	relation_close(rel, AccessShareLock);

	/* Empty cache -> empty result. */
	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return (Datum) 0;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relOid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->refcount == 0)
		return (Datum) 0;

	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsEntry *entry;

		CHECK_FOR_INTERRUPTS();
		entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
												   lfirst(lc), HASH_FIND, NULL);
		if (entry == NULL || !fasttrun_stats_entry_is_candidate(entry))
			continue;
		tuplestore_puttuple(tupstore, entry->statsTuple);
	}

	return (Datum) 0;
}

