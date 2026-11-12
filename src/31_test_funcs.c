/*-------------------------------------------------------------------------
 *
 * src/31_test_funcs.c
 *      Test SQL functions of cassert builds.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/31_test_funcs.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
#ifdef USE_ASSERT_CHECKING
/* Test helper for deterministic tracking-order checks. */
Datum
fasttrun_test_track_set(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char		key[NAMEDATALEN] = {0};
	int64		count = PG_GETARG_INT64(1);
	TimestampTz last_create = PG_GETARG_INT64(2);
	FasttrunTrackEntry *entry;
	bool		found;

	if (fasttrun_track_htab == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun tracking shared memory is unavailable")));
	strlcpy(key, name, sizeof(key));
	LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
	entry = (FasttrunTrackEntry *)
		hash_search(fasttrun_track_htab, key, HASH_ENTER, &found);
	if (!found)
		memcpy(entry->relname, key, sizeof(entry->relname));
	entry->create_count = count;
	entry->last_create = last_create;
	LWLockRelease(fasttrun_track_lock);
	pfree(name);
	PG_RETURN_VOID();
}

/*
 * Test helper: hold the tracking lock exclusively for the given number of
 * seconds, so an immediate shutdown can kill the backend while it holds it.
 */
Datum
fasttrun_test_track_hold_lock(PG_FUNCTION_ARGS)
{
	int32		seconds = PG_GETARG_INT32(0);
	int32		i;

	if (fasttrun_track_htab == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun tracking shared memory is unavailable")));
	LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
	for (i = 0; i < seconds * 10; i++)
		pg_usleep(100000L);
	LWLockRelease(fasttrun_track_lock);
	PG_RETURN_VOID();
}

/* Test function available only in cassert builds. */
Datum
fasttrun_test_subxact_visits(PG_FUNCTION_ARGS)
{
	bool		reset = PG_GETARG_BOOL(0);
	uint64		prior = fasttrun_test_subxact_visited;

	if (reset)
		fasttrun_test_subxact_visited = 0;
	PG_RETURN_INT64((int64) prior);
}

/* Reads rd_rel without substitution from the local cache. */
Datum
fasttrun_test_raw_relpages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	int32		pages;

	rel = relation_open(relid, AccessShareLock);
	pages = rel->rd_rel->relpages;
	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32(pages);
}

/* Returns the relation-policy undo depth for the atomicity test. */
Datum
fasttrun_test_relid_undo_depth(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	FasttrunStatsRelidEntry *entry;
	FasttrunStatsRelidSavedState *saved;
	int64		depth = 0;

	if (fasttrun_stats_relid_cache == NULL)
		PG_RETURN_INT64(0);
	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (entry == NULL)
		PG_RETURN_INT64(0);
	for (saved = entry->undo; saved != NULL; saved = saved->older)
		depth++;
	PG_RETURN_INT64(depth);
}

/* Exercises the defensive cache-miss branch without racing the catalog. */
Datum
fasttrun_test_evict_missing_relid(PG_FUNCTION_ARGS)
{
	fasttrun_evict_missing_relid(PG_GETARG_OID(0));
	PG_RETURN_VOID();
}
#endif

#ifdef USE_ASSERT_CHECKING
/* Returns the planner probe counters.  true also resets them. */
Datum
fasttrun_test_planner_probe(PG_FUNCTION_ARGS)
{
	bool		reset = PG_GETARG_BOOL(0);
	Datum		values[5];

	values[0] = Int64GetDatum((int64) fasttrun_test_locator_opens);
	values[1] = Int64GetDatum((int64) fasttrun_test_counter_opens);
	values[2] = Int64GetDatum((int64) fasttrun_test_owner_opens);
	values[3] = Int64GetDatum((int64) fasttrun_test_plan_invalidations);
	values[4] = Int64GetDatum((int64) fasttrun_test_index_hook_calls);
	if (reset)
	{
		fasttrun_test_locator_opens = 0;
		fasttrun_test_counter_opens = 0;
		fasttrun_test_owner_opens = 0;
		fasttrun_test_plan_invalidations = 0;
		fasttrun_test_index_hook_calls = 0;
	}
	PG_RETURN_ARRAYTYPE_P(construct_array(values, 5, INT8OID,
									  8, true, TYPALIGN_DOUBLE));
}

/* Direct call of the width hook outside the planner. */
Datum
fasttrun_test_call_attavgwidth(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AttrNumber	attnum = PG_GETARG_INT16(1);

	PG_RETURN_INT32(fasttrun_get_attavgwidth_hook(relid, attnum));
}
#endif


/*
 * Test helper: size of the main fork in bytes as smgr reports it.  On
 * PostgreSQL it equals pg_relation_size.  Postgres Pro Enterprise keeps a
 * temporary relation in local buffers until it spills, so the file size
 * there says nothing about the relation.
 */
Datum
fasttrun_test_relation_bytes(PG_FUNCTION_ARGS)
{
	Relation	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	int64		bytes = (int64) RelationGetNumberOfBlocks(rel) * BLCKSZ;

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT64(bytes);
}
