/*-------------------------------------------------------------------------
 *
 * src/30_tracking_sql.c
 *      Tracking SQL functions.
 *
 * What it holds.  The registry snapshot and selection of the top entries,
 * fasttrun_hot_temp_tables, fasttrun_prewarm and fasttrun_reset_temp_stats.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/30_tracking_sql.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* Build a bounded immutable snapshot without allocation or sorting under lock. */
static FasttrunTrackSnapshot
fasttrun_track_snapshot(int limit)
{
	FasttrunTrackSnapshot snapshot;
	HASH_SEQ_STATUS status;
	FasttrunTrackEntry *entry;
	int			capacity;
	int			worst = 0;
	bool		bounded;

	/* Tiny top-N stays allocation- and copy-bounded.  Large readers copy all. */
	bounded = (limit > 0 && limit <= 32);
	capacity = bounded ? limit : FASTTRUN_TRACK_MAX;
	snapshot.entries = (FasttrunTrackEntry *)
		palloc(sizeof(FasttrunTrackEntry) * capacity);
	snapshot.count = 0;

	LWLockAcquire(fasttrun_track_lock, LW_SHARED);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = (FasttrunTrackEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->create_count <= 0)
			continue;
		if (!bounded)
		{
			Assert(snapshot.count < FASTTRUN_TRACK_MAX);
			snapshot.entries[snapshot.count++] = *entry;
		}
		else if (snapshot.count < capacity)
		{
			snapshot.entries[snapshot.count] = *entry;
			if (snapshot.count == 0 ||
				fasttrun_track_cmp_desc(&snapshot.entries[snapshot.count],
										&snapshot.entries[worst]) > 0)
				worst = snapshot.count;
			snapshot.count++;
		}
		else if (fasttrun_track_cmp_desc(entry,
									  &snapshot.entries[worst]) < 0)
		{
			int			i;

			snapshot.entries[worst] = *entry;
			worst = 0;
			for (i = 1; i < snapshot.count; i++)
			{
				if (fasttrun_track_cmp_desc(&snapshot.entries[i],
										&snapshot.entries[worst]) > 0)
					worst = i;
			}
		}
	}
	LWLockRelease(fasttrun_track_lock);
	return snapshot;
}

static void
fasttrun_track_swap(FasttrunTrackEntry *a, FasttrunTrackEntry *b)
{
	FasttrunTrackEntry tmp = *a;

	*a = *b;
	*b = tmp;
}

/* Keep the worst selected entry at heap[0]. */
static void
fasttrun_track_sift_worst(FasttrunTrackEntry *heap, int count, int root)
{
	for (;;)
	{
		int			left = root * 2 + 1;
		int			worst = root;

		if (left < count &&
			fasttrun_track_cmp_desc(&heap[left], &heap[worst]) > 0)
			worst = left;
		if (left + 1 < count &&
			fasttrun_track_cmp_desc(&heap[left + 1], &heap[worst]) > 0)
			worst = left + 1;
		if (worst == root)
			return;
		fasttrun_track_swap(&heap[root], &heap[worst]);
		root = worst;
	}
}

/* Exact top-N outside LWLock, then deterministic order of selected rows. */
static int
fasttrun_track_select_top(FasttrunTrackEntry *entries, int count, int limit)
{
	int			i;

	if (limit <= 0 || limit >= count)
	{
		qsort(entries, count, sizeof(FasttrunTrackEntry),
			  fasttrun_track_cmp_desc);
		return count;
	}

	for (i = limit / 2; i-- > 0;)
		fasttrun_track_sift_worst(entries, limit, i);
	for (i = limit; i < count; i++)
	{
		if (fasttrun_track_cmp_desc(&entries[i], &entries[0]) < 0)
		{
			entries[0] = entries[i];
			fasttrun_track_sift_worst(entries, limit, 0);
		}
	}
	qsort(entries, limit, sizeof(FasttrunTrackEntry),
		  fasttrun_track_cmp_desc);
	return limit;
}

/* SQL: fasttrun_hot_temp_tables(n) -- returns top-N most created temp tables. */
Datum
fasttrun_hot_temp_tables(PG_FUNCTION_ARGS)
{
	int32				limit = PG_GETARG_INT32(0);
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate	   *tupstore;
	MemoryContext		per_query_cxt, oldcxt;
	FasttrunTrackSnapshot snapshot;
	FasttrunTrackEntry *sorted;
	int					output_count;
	int					i;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

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

	if (fasttrun_track_htab == NULL)
		return (Datum) 0;

	snapshot = fasttrun_track_snapshot(limit);
	sorted = snapshot.entries;
	output_count = fasttrun_track_select_top(sorted, snapshot.count, limit);

	for (i = 0; i < output_count; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		values[0] = CStringGetTextDatum(sorted[i].relname);
		values[1] = Int64GetDatum(sorted[i].create_count);
		values[2] = TimestampTzGetDatum(sorted[i].last_create);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	pfree(sorted);
	return (Datum) 0;
}

/* Complete deterministic order.  Explicit branches avoid subtraction overflow. */
static int
fasttrun_track_cmp_desc(const void *a, const void *b)
{
	const FasttrunTrackEntry *ea = (const FasttrunTrackEntry *) a;
	const FasttrunTrackEntry *eb = (const FasttrunTrackEntry *) b;

	if (ea->create_count != eb->create_count)
		return (ea->create_count < eb->create_count) ? 1 : -1;
	if (ea->last_create != eb->last_create)
		return (ea->last_create < eb->last_create) ? 1 : -1;
	return strcmp(ea->relname, eb->relname);
}

/* SQL: fasttrun_prewarm() -- creates top-N temp tables via create_temp_table. */
Datum
fasttrun_prewarm(PG_FUNCTION_ARGS)
{
	int		limit = fasttrun_prewarm_count;
	int		created = 0;
	int		i;
	FasttrunTrackSnapshot snapshot;
	FasttrunTrackEntry *sorted;

	if (limit == 0 || fasttrun_track_htab == NULL)
		PG_RETURN_INT32(0);

	snapshot = fasttrun_track_snapshot(limit);
	sorted = snapshot.entries;
	limit = fasttrun_track_select_top(sorted, snapshot.count, limit);

	{
		/* Verify dummy schema exists.  If not, skip all prewarm. */
		Oid		nspOid = get_namespace_oid(fasttrun_prewarm_schema, true);
		bool	save_in_prewarm = fasttrun_in_prewarm;

		/*
		 * Suppress tracking of our own re-entrant CREATE TEMP TABLE.  Save and
		 * restore the flag so nesting stays correct.  PG_FINALLY restores it
		 * even if SPI_connect or the SPI query throws.
		 */
		fasttrun_in_prewarm = true;
		PG_TRY();
		{
			/* One SPI session for the whole batch, not one per table. */
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "fasttrun_prewarm: SPI_connect failed");

			for (i = 0; i < limit; i++)
			{
				/* Skip if no matching dummy table in the schema. */
				if (nspOid == InvalidOid ||
					get_relname_relid(sorted[i].relname, nspOid) == InvalidOid)
					continue;

				SPI_execute_with_args(
					"SELECT create_temp_table($1)",
					1,
					(Oid[]) { TEXTOID },
					(Datum[]) { CStringGetTextDatum(sorted[i].relname) },
					NULL, false, 0);

				/* Free this iteration's result so tuptables don't accumulate. */
				if (SPI_tuptable != NULL)
				{
					SPI_freetuptable(SPI_tuptable);
					SPI_tuptable = NULL;
				}

				created++;
			}

			SPI_finish();
		}
		PG_FINALLY();
		{
			fasttrun_in_prewarm = save_in_prewarm;
		}
		PG_END_TRY();
	}

	pfree(sorted);
	PG_RETURN_INT32(created);
}

/* SQL: fasttrun_reset_temp_stats() -- clears all tracking counters. */
Datum
fasttrun_reset_temp_stats(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS		status;
	FasttrunTrackEntry *entry;

	if (fasttrun_track_htab == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
	hash_seq_init(&status, fasttrun_track_htab);
	while ((entry = hash_seq_search(&status)) != NULL)
		hash_search(fasttrun_track_htab, entry->relname, HASH_REMOVE, NULL);
	LWLockRelease(fasttrun_track_lock);

	/* Remove the on-disk file too. */
	(void) unlink(FASTTRUN_TRACK_FILE);

	PG_RETURN_VOID();
}

