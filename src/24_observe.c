/*-------------------------------------------------------------------------
 *
 * src/24_observe.c
 *      Observability SQL functions.
 *
 * What it holds.  fasttrun_cache_stats, fasttrun_counters,
 * fasttrun_counters_reset, fasttrun_cached_tables and fasttrun_blocked_tables.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/24_observe.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * fasttrun_cache_stats()
 *
 * Capacity monitoring: entry counts and recursively allocated memory of
 * the analyze and column-stats contexts.  The function does not modify
 * catalogs and takes no locks itself.  A missing cache reads as zero.
 */
Datum
fasttrun_cache_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[6];
	bool		nulls[6] = {false, false, false, false, false, false};
	HeapTuple	tuple;
	int64		analyze_entries = 0;
	int64		column_stats_relid_entries = 0;
	int64		column_stats_entries = 0;
	int64		analyze_bytes;
	int64		column_stats_bytes;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	if (fasttrun_analyze_cache != NULL)
		analyze_entries = hash_get_num_entries(fasttrun_analyze_cache);
	if (fasttrun_stats_relid_cache != NULL)
		column_stats_relid_entries =
			hash_get_num_entries(fasttrun_stats_relid_cache);
	if (fasttrun_stats_cache != NULL)
		column_stats_entries = hash_get_num_entries(fasttrun_stats_cache);
	analyze_bytes = (int64) fasttrun_memory_context_bytes(fasttrun_analyze_mcxt);
	column_stats_bytes =
		(int64) fasttrun_memory_context_bytes(fasttrun_stats_mcxt);

	values[0] = Int64GetDatum(analyze_entries);
	values[1] = Int64GetDatum(column_stats_relid_entries);
	values[2] = Int64GetDatum(column_stats_entries);
	values[3] = Int64GetDatum(analyze_bytes);
	values[4] = Int64GetDatum(column_stats_bytes);
	values[5] = Int64GetDatum(analyze_bytes + column_stats_bytes);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* Materialize mode for the observability SRFs below. */
static Tuplestorestate *
fasttrun_begin_srf(FunctionCallInfo fcinfo, TupleDesc *tupdesc_out)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcxt;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	oldcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcxt);
	*tupdesc_out = tupdesc;
	return tupstore;
}

/*
 * fasttrun_counters() RETURNS TABLE(counter text, value bigint, scope text)
 *
 * With shared_preload_libraries the values are cluster totals: the shared
 * sums plus this backend's counts not yet added at a transaction end.  Other
 * backends add theirs at their next transaction end.  Without preloading the
 * values belong to the calling backend, and scope says which it is.
 */
Datum
fasttrun_counters(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore = fasttrun_begin_srf(fcinfo, &tupdesc);
	const char *scope = fasttrun_shared_counters != NULL ? "cluster" : "backend";
	int			i;

	for (i = 0; i < FTC_COUNT; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};
		uint64		value = fasttrun_local_counters[i];

		if (fasttrun_shared_counters != NULL)
		{
			int			s;

			for (s = 0; s < fasttrun_shared_counters->nslots; s++)
				value += pg_atomic_read_u64(&fasttrun_counter_slot(s)->values[i]);
		}
		values[0] = CStringGetTextDatum(fasttrun_counter_names[i]);
		values[1] = Int64GetDatum((int64) value);
		values[2] = CStringGetTextDatum(scope);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	return (Datum) 0;
}

/* Zero the counters this function reports: shared ones and the caller's. */
Datum
fasttrun_counters_reset(PG_FUNCTION_ARGS)
{
	int			i;

	for (i = 0; i < FTC_COUNT; i++)
	{
		fasttrun_local_counters[i] = 0;
		if (fasttrun_shared_counters != NULL)
		{
			int			s;

			for (s = 0; s < fasttrun_shared_counters->nslots; s++)
				pg_atomic_write_u64(&fasttrun_counter_slot(s)->values[i], 0);
		}
	}
	fasttrun_local_counters_dirty = false;
	PG_RETURN_VOID();
}

/*
 * Cached column samples of relid that the planner would not get right now.
 * Read-only: a sample from another file of the relation counts as hidden,
 * without the forgetting the planner path does.  The relid entry is looked up
 * after the relation is open, so nothing the open triggers can leave a stale
 * pointer behind.
 */
static int
fasttrun_count_hidden_columns(Oid relid)
{
	Relation	rel;
	FasttrunStatsRelidEntry *relentry;
	RelFileLocatorBackend rlb;
	int64		ins = 0;
	int64		upd = 0;
	int64		del = 0;
	bool		truncdropped = false;
	bool		usable_source;
	BlockNumber pages;
	FasttrunUsableBaseline rel_baseline = {0};
	ListCell   *lc;
	int			hidden = 0;

	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
		return 0;
	pages = RELKIND_HAS_STORAGE(rel->rd_rel->relkind) ?
		RelationGetNumberOfBlocks(rel) : 0;
	usable_source = fasttrun_read_pgstat_counters(rel, &ins, &upd, &del,
												  &truncdropped);
	rlb.locator = rel->rd_locator;
	rlb.backend = rel->rd_backend;
	relentry = fasttrun_stats_relid_cache == NULL ? NULL :
		(FasttrunStatsRelidEntry *) hash_search(fasttrun_stats_relid_cache,
												&relid, HASH_FIND, NULL);
	if (relentry != NULL)
	{
		usable_source = usable_source && relentry->heap_rlb_valid &&
			fasttrun_rlb_equals(relentry->heap_rlb, rlb);
		foreach(lc, relentry->attkeys)
		{
			FasttrunStatsEntry *entry;

			entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
													   lfirst(lc), HASH_FIND, NULL);
			if (entry == NULL || !fasttrun_stats_entry_is_candidate(entry))
				continue;
			if (!usable_source ||
				!fasttrun_stats_entry_usable_ext(relid, relentry, entry, ins, upd,
												 del, truncdropped, pages,
												 &rel_baseline))
				hidden++;
		}
	}
	relation_close(rel, AccessShareLock);
	return hidden;
}

/*
 * fasttrun_cached_tables() lists the relations this backend keeps state for:
 * the published size estimate, whether a delta state carries the row count,
 * and how many cached column samples there are and how many of them the
 * freshness check hides from the planner right now.
 */
Datum
fasttrun_cached_tables(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore = fasttrun_begin_srf(fcinfo, &tupdesc);
	List	   *relids = NIL;
	HASH_SEQ_STATUS status;
	ListCell   *lc;

	if (fasttrun_analyze_cache != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry;

		hash_seq_init(&status, fasttrun_analyze_cache);
		while ((entry = (FasttrunAnalyzeCacheEntry *) hash_seq_search(&status)) != NULL)
			relids = lappend_oid(relids, entry->relid);
	}
	if (fasttrun_stats_relid_cache != NULL)
	{
		FasttrunStatsRelidEntry *relentry;

		hash_seq_init(&status, fasttrun_stats_relid_cache);
		while ((relentry = (FasttrunStatsRelidEntry *) hash_seq_search(&status)) != NULL)
		{
			/* Keys are unique per hash.  Skip the ones the first walk listed. */
			if (fasttrun_cache_lookup(relentry->relid) == NULL)
				relids = lappend_oid(relids, relentry->relid);
		}
	}

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		char	   *relname = get_rel_name(relid);
		int			hidden = relname != NULL ? fasttrun_count_hidden_columns(relid) : 0;
		FasttrunAnalyzeCacheEntry *entry;
		FasttrunStatsRelidEntry *relentry = NULL;
		bool		is_index;
		Datum		values[9];
		bool		nulls[9];

		/* Catalog access is over: look the entries up now. */
		entry = fasttrun_cache_lookup(relid);
		memset(nulls, 0, sizeof(nulls));
		if (fasttrun_stats_relid_cache != NULL)
			relentry = (FasttrunStatsRelidEntry *)
				hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);

		values[0] = ObjectIdGetDatum(relid);
		if (relname != NULL)
			values[1] = CStringGetTextDatum(relname);
		else
			nulls[1] = true;
		is_index = (entry != NULL && entry->state.heap_relid != relid);
		values[2] = CStringGetTextDatum(is_index ? "index" : "table");
		if (entry != NULL && entry->state.has_relstats)
		{
			values[3] = Int64GetDatum((int64) entry->state.cached_pages);
			values[4] = Float8GetDatum((double) entry->state.cached_tuples);
			/* The row count and its delta belong to the table. */
			values[5] = BoolGetDatum(entry->state.count_exact);
			values[6] = BoolGetDatum(entry->state.has_delta_state);
			nulls[5] = nulls[6] = is_index;
		}
		else
			nulls[3] = nulls[4] = nulls[5] = nulls[6] = true;
		values[7] = Int32GetDatum(relentry != NULL ? relentry->refcount : 0);
		values[8] = Int32GetDatum(hidden);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	list_free(relids);
	return (Datum) 0;
}

/*
 * fasttrun_blocked_tables() lists the tables this backend blocks after an
 * incomplete fasttruncate: the ones that still exist on the damaged file.
 * A pooler can check it before it returns the connection.
 */
Datum
fasttrun_blocked_tables(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore = fasttrun_begin_srf(fcinfo, &tupdesc);
	List	   *relids = NIL;
	HASH_SEQ_STATUS status;
	FasttrunPoisonEntry *entry;
	ListCell   *lc;

	if (fasttrun_poison_cache == NULL)
		return (Datum) 0;
	hash_seq_init(&status, fasttrun_poison_cache);
	while ((entry = (FasttrunPoisonEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->active)
			relids = lappend_oid(relids, entry->root_relid);
	}
	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		Relation	rel;
		Datum		values[2];
		bool		nulls[2] = {false, false};

		rel = try_relation_open(relid, AccessShareLock);
		if (rel == NULL)
			continue;
		entry = fasttrun_poison_find(relid);
		if (entry != NULL && entry->active &&
			RelFileLocatorEquals(entry->root_rlb.locator, rel->rd_locator) &&
			entry->root_rlb.backend == rel->rd_backend)
		{
			values[0] = ObjectIdGetDatum(relid);
			values[1] = CStringGetTextDatum(RelationGetRelationName(rel));
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
		relation_close(rel, AccessShareLock);
	}
	list_free(relids);
	return (Datum) 0;
}

