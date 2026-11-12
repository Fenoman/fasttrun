/*-------------------------------------------------------------------------
 *
 * src/22_truncate.c
 *      fasttruncate.
 *
 * What it holds.  Storage reset without sinval, empty btree and ambuild of
 * indexes, the truncate workset and its phases, publication of the empty
 * state and the SQL function fasttruncate.
 *
 * Also here.  fasttrun_invalidate_local_plan_cache, which other slices call.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/22_truncate.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
#ifndef PGPRO_EE
/*
 * Unlink every segment of a relation fork file.
 *
 * heap files are split into 1 GB segments with suffixes .1, .2, ...
 * starting from segment 1.  Segment 0 has no suffix.  We loop until the
 * next segment doesn't exist (ENOENT) and stop.
 *
 * On unlink failure for any other reason we ereport(ERROR) -- a partial
 * unlink is a hard error because it leaves the relation in a half-state.
 */
static void
fasttrun_unlink_fork_segments(const char *base_path)
{
	int			seg = 0;
	char		path[MAXPGPATH];

	for (;;)
	{
		if (seg == 0)
			strlcpy(path, base_path, sizeof(path));
		else
			snprintf(path, sizeof(path), "%s.%d", base_path, seg);

		if (unlink(path) < 0)
		{
			if (errno == ENOENT)
				return;	/* no more segments -- normal end */
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("fasttrun: could not unlink relation segment \"%s\": %m",
							path)));
		}
		seg++;
	}
}

/*
 * Zero-sinval physical truncation of a temp relation.
 *
 * Functionally equivalent to RelationTruncate(rel, 0) -- empties one
 * storage relation to zero blocks and discards local buffers -- but does NOT call
 * smgrtruncate() and therefore never reaches CacheInvalidateSmgr(), the
 * one shared-invalidation message RelationTruncate() sends, so the path
 * is literally zero sinval.
 *
 * How:
 *   1. Discover all forks that currently exist (main / fsm / vm).
 *   2. Drop their local buffers via DropRelationAllLocalBuffers (temp
 *      tables live in the local buffer pool, not shared).
 *   3. Physically unlink every segment of every fork via raw unlink()
 *      on the path returned by relpathbackend().
 *   4. Reset smgr_cached_nblocks (and smgr_targblock) to invalid so
 *      the next access reopens correctly.
 *   5. Recreate empty fork files via smgrcreate(), which has no
 *      built-in invalidation.
 *
 * The whole operation goes through public smgr.h / buf_internals.h /
 * relpath.h API -- no internal smgrsw[] poking.  GetRelationPath returns
 * a palloc'd char* in PG <18 and a stack-allocated RelPathStr in PG 18+,
 * so the wrapping is version-conditional.
 *
 * Caller has already taken ExclusiveLock and verified
 * relpersistence='t' + heap AM.
 */
static void
fasttrun_smgr_bypass_truncate(Relation rel, bool buffers_already_dropped)
{
	SMgrRelation			reln;
	RelFileLocatorBackend	rlocator;
	ForkNumber				forks[MAX_FORKNUM + 1];
	int						nforks = 0;
	int						i;

	/*
	 * Self-defense: this helper unlinks files via raw POSIX unlink() and
	 * uses DropRelationAllLocalBuffers, both of which only make sense on a
	 * temp relation owned by this backend.  fasttruncate() already
	 * checks isTempNamespace + heap-AM in the caller, but a stray direct
	 * call here on a non-temp rel would silently corrupt that rel's
	 * storage.  Re-check explicitly.
	 */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
		elog(ERROR, "fasttrun_smgr_bypass_truncate: relation \"%s\" is not temporary",
			 RelationGetRelationName(rel));

	reln = RelationGetSmgr(rel);

	/*
	 * Snapshot smgr_rlocator now: relpathbackend() needs both the
	 * locator and the backend id, and we'll keep using them after
	 * smgrrelease(). The struct is plain POD, copy by value is safe.
	 */
	rlocator = reln->smgr_rlocator;

	/* 1. Detect existing forks. */
	forks[nforks++] = MAIN_FORKNUM;	/* always exists for a heap */
	if (smgrexists(reln, FSM_FORKNUM))
		forks[nforks++] = FSM_FORKNUM;
	if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
		forks[nforks++] = VISIBILITYMAP_FORKNUM;

	/* 2. Drop local buffers unless already dropped for all opened relations. */
	if (!buffers_already_dropped)
		DropRelationAllLocalBuffers(rlocator.locator);

	/*
	 * 3. Release all cached file descriptors and per-fork state inside the
	 * SMgrRelation BEFORE we touch the files on disk.  Without this, the
	 * MdfdVec arrays inside md.c keep pointing at the inodes we are about
	 * to unlink, and the next mdcreate() would resize those arrays via
	 * repalloc() rather than rebuild them from scratch -- leaving stale fd
	 * entries for any segment beyond segno 0.  Calling smgrrelease() runs
	 * mdclose() for every fork, which properly closes every cached fd and
	 * trims md_seg_fds[] back to zero, so the post-unlink smgrcreate()
	 * starts from a clean slate.  smgrrelease() does NOT call
	 * CacheInvalidateSmgr(), so the path stays sinval-free.
	 *
	 * smgrrelease() does NOT destroy the SMgrRelation in any supported PG
	 * version: in PG 16 the rel is owned via smgrsetowner(), and in PG 17/18
	 * RelationGetSmgr() pinned it via smgrpin().  Either way `reln` stays
	 * a valid pointer through this whole function.
	 */
	smgrrelease(reln);

	/* 4. Physically unlink every segment of every fork. */
	for (i = 0; i < nforks; i++)
	{
#if PG_VERSION_NUM >= 180000
		RelPathStr	rpath = relpathbackend(rlocator.locator,
										   rlocator.backend, forks[i]);
		const char *base = rpath.str;
#else
		char	   *base = relpathbackend(rlocator.locator,
										  rlocator.backend, forks[i]);
#endif

		fasttrun_unlink_fork_segments(base);

#if PG_VERSION_NUM < 180000
		pfree(base);
#endif
	}

	/*
	 * 5. Re-fetch the SMgrRelation pointer.  smgrrelease() left it valid,
	 * but PG style guidance is to re-execute RelationGetSmgr() after
	 * any operation that may have closed underlying file handles, rather
	 * than holding the pointer across the call.  Cheap (likely path is
	 * a single field load).
	 */
	reln = RelationGetSmgr(rel);

	/*
	 * 6. Recreate empty fork files via smgrcreate.  smgrcreate() does NOT
	 * call CacheInvalidateSmgr(), so this entire path stays sinval-free.
	 * smgrrelease above already cleared smgr_cached_nblocks and
	 * smgr_targblock, but we re-set the freshly-created forks to 0 blocks
	 * so the next size query is a hit and not a re-probe via lseek.
	 */
	for (i = 0; i < nforks; i++)
	{
		smgrcreate(reln, forks[i], false);
		reln->smgr_cached_nblocks[forks[i]] = 0;
	}
}
#endif							/* !PGPRO_EE */

/*
 * An empty btree is one metapage with no root, exactly what btbuild() writes
 * for an empty heap.  btbuild() gets there through a heap scan into a
 * tuplesort, an executor state and the xmin horizon taken under
 * ProcArrayLock, once per index per truncate.  The page is written the way
 * btbuildempty() writes the init fork.  A temporary relation needs neither
 * WAL nor fsync, and its local buffers were dropped with the old storage.
 */
static void
fasttrun_write_empty_btree(Relation indexrel)
{
#ifdef PGPRO_EE
	/*
	 * Postgres Pro Enterprise creates the file of a temporary relation only
	 * when its pages no longer fit into temp_buffers.  The metapage goes
	 * through a local buffer, so an index that fits stays without a file.
	 */
	Buffer		buf;

	Assert(RelationUsesLocalBuffers(indexrel));
	buf = ExtendBufferedRel(BMR_REL(indexrel), MAIN_FORKNUM, NULL,
							EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
	if (BufferGetBlockNumber(buf) != BTREE_METAPAGE)
		elog(ERROR, "fasttrun: index \"%s\" is not empty after truncate",
			 RelationGetRelationName(indexrel));
	_bt_initmetapage(BufferGetPage(buf), P_NONE, 0,
					 _bt_allequalimage(indexrel, true));
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
#else
	Page		metapage = (Page) palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, 0);

	Assert(RelationUsesLocalBuffers(indexrel));
	_bt_initmetapage(metapage, P_NONE, 0,
					 _bt_allequalimage(indexrel, true));
	PageSetChecksumInplace(metapage, BTREE_METAPAGE);
	smgrextend(RelationGetSmgr(indexrel), MAIN_FORKNUM, BTREE_METAPAGE,
			   metapage, true);
	pfree(metapage);
#endif
}

/*
 * Phase 2 of the zero-sinval index rebuild: reconstruct one empty index
 * structure (metapage) via the AM's ambuild callback directly -- NOT
 * through index_build(), because index_build calls index_update_stats ->
 * systable_inplace_update_finish on pg_class, which generates shared
 * cache invalidation messages.  The index storage must already be
 * dropped and re-created empty: ambuild requires a zero-block relation.
 */
static BlockNumber
fasttrun_ambuild_empty_index(Relation heaprel, Relation indexrel)
{
	BlockNumber pages;

	if (indexrel->rd_amcache != NULL)
	{
		pfree(indexrel->rd_amcache);
		indexrel->rd_amcache = NULL;
	}
	if (indexrel->rd_rel->relam == BTREE_AM_OID)
		fasttrun_write_empty_btree(indexrel);
	else
	{
		IndexInfo  *indexInfo = BuildDummyIndexInfo(indexrel);
		IndexBuildResult *result;

		result = indexrel->rd_indam->ambuild(heaprel, indexrel, indexInfo);
		pfree(result);
		pfree(indexInfo);
	}
	RelationGetSmgr(indexrel)->smgr_cached_nblocks[MAIN_FORKNUM] =
		InvalidBlockNumber;
	pages = RelationGetNumberOfBlocks(indexrel);
	return pages;
}

static void
fasttrun_truncate_one_storage(Relation rel, bool zero_sinval,
								 bool buffers_already_dropped)
{
#ifdef PGPRO_EE
	/*
	 * Postgres Pro Enterprise keeps the size and the on-disk state of a
	 * temporary relation inside smgr (lazy file creation, CFS maps), so files
	 * must not be unlinked behind it.  Its RelationTruncate sends no shared
	 * invalidation for a temporary relation, which makes the bypass pointless
	 * there.  The last argument asks it to drop the local buffers.
	 */
	RelationTruncate(rel, 0, true);
#else
	if (zero_sinval)
		fasttrun_smgr_bypass_truncate(rel, buffers_already_dropped);
	else
		RelationTruncate(rel, 0);
#endif
}

static FasttrunTruncateResultSlot *
fasttrun_truncate_slot(FasttrunTruncateOperation *operation, Oid relid)
{
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		if (operation->slots[i].relid == relid)
			return &operation->slots[i];
	}
	elog(ERROR, "fasttrun: relation %u was not prepared for truncate", relid);
	return NULL;
}

/* Open the complete relation set before any cache or storage side effect. */
static void
fasttrun_prepare_truncate_workset(Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	List	   *index_oids;
	ListCell   *lc;
	Oid			toastrelid;
	int			i = 0;

	memset(opened, 0, sizeof(*opened));
	if (heaprel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		heaprel->rd_rel->relhassubclass)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fasttruncate does not support partitioned or inheritance-parent temporary tables"),
				 errhint("Use core SQL TRUNCATE for inheritance or partition traversal.")));

	CheckTableNotInUse(heaprel, "fasttruncate");
	index_oids = RelationGetIndexList(heaprel);
	foreach(lc, index_oids)
	{
		Relation	indexrel = index_open(lfirst_oid(lc), ExclusiveLock);

		CheckTableNotInUse(indexrel, "fasttruncate");
		opened->user_indexes = lappend(opened->user_indexes, indexrel);
	}
	list_free(index_oids);

	toastrelid = heaprel->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		opened->toastrel = table_open(toastrelid, ExclusiveLock);
		CheckTableNotInUse(opened->toastrel, "fasttruncate");
		index_oids = RelationGetIndexList(opened->toastrel);
		foreach(lc, index_oids)
		{
			Relation	indexrel = index_open(lfirst_oid(lc),
										 ExclusiveLock);

			CheckTableNotInUse(indexrel, "fasttruncate");
			opened->toast_indexes = lappend(opened->toast_indexes, indexrel);
		}
		list_free(index_oids);
	}

	opened->nslots = list_length(opened->user_indexes) +
		list_length(opened->toast_indexes) +
		(opened->toastrel != NULL ? 1 : 0) + 1;
	opened->slot_relations = (Relation *)
		palloc(sizeof(Relation) * opened->nslots);
	foreach(lc, opened->user_indexes)
		opened->slot_relations[i++] = (Relation) lfirst(lc);
	foreach(lc, opened->toast_indexes)
		opened->slot_relations[i++] = (Relation) lfirst(lc);
	if (opened->toastrel != NULL)
		opened->slot_relations[i++] = opened->toastrel;
	opened->slot_relations[i++] = heaprel;
	Assert(i == opened->nslots);
}

/* Skip file changes only for a fully published empty state, after in-use checks. */
static bool
fasttrun_try_empty_truncate(Relation heaprel, FasttrunOpenedWorkset *opened)
{
	Oid			relid = RelationGetRelid(heaprel);
	FasttrunAnalyzeCacheEntry *heapentry = fasttrun_cache_lookup(relid);
	FasttrunStatsRelidEntry *relentry;
	ListCell   *lc;
	int			nuser_indexes = list_length(opened->user_indexes);
	int			ntoast_indexes = list_length(opened->toast_indexes);
	int			i;
	int64		ins, upd, del;
	bool		truncdropped;
	bool		have_counters;

	/* A physically empty heap can still need repair after a failed rebuild. */
	if (fasttrun_poison_find(relid) != NULL || heapentry == NULL ||
		!fasttrun_relation_has_same_locator(heaprel, heapentry) ||
		heapentry->state.cached_pages != 0 ||
		heapentry->state.cached_tuples != 0 ||
		!heapentry->state.last_inval_valid ||
		heapentry->state.last_inval_pages != 0 ||
		heapentry->state.last_inval_tuples != 0 ||
		RelationGetNumberOfBlocks(heaprel) != 0)
		return false;

	/* Keep the delta seed valid for a later refill in this transaction. */
	have_counters = fasttrun_read_pgstat_counters(heaprel, &ins, &upd, &del,
													 &truncdropped);
	if (heapentry->state.has_delta_state &&
		(!have_counters ||
			ins != heapentry->state.cached_inserted ||
			upd != heapentry->state.cached_updated ||
			del != heapentry->state.cached_deleted ||
			truncdropped != heapentry->state.cached_truncdropped))
		return false;

	if (fasttrun_stats_relid_cache == NULL)
		return false;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL || relentry->policy != FASTTRUN_REL_LOCAL_NEUTRAL ||
		!relentry->heap_rlb_valid ||
		!RelFileLocatorEquals(relentry->heap_rlb.locator, heaprel->rd_locator) ||
		relentry->heap_rlb.backend != heaprel->rd_backend)
		return false;

	/* Partial core ANALYZE can leave CORE_ALLOWED column overrides. */
	foreach(lc, relentry->attkeys)
	{
		FasttrunStatsEntry *entry = (FasttrunStatsEntry *)
			hash_search(fasttrun_stats_cache, lfirst(lc), HASH_FIND, NULL);

		if (entry == NULL || entry->state != FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
			entry->statsTuple != NULL)
			return false;
	}

	for (i = 0; i < opened->nslots; i++)
	{
		Relation	rel = opened->slot_relations[i];
		Relation	owner = i < nuser_indexes ? heaprel :
			(i < nuser_indexes + ntoast_indexes ? opened->toastrel : rel);
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(RelationGetRelid(rel));

		if (entry == NULL || !fasttrun_relation_has_same_locator(rel, entry) ||
			entry->state.probe_rlb.backend != rel->rd_backend ||
			entry->state.heap_relid != RelationGetRelid(owner) ||
			!RelFileLocatorEquals(entry->state.heap_rlb.locator, owner->rd_locator) ||
			entry->state.heap_rlb.backend != owner->rd_backend ||
			entry->state.cached_tuples != 0 || entry->state.cached_allvisible != 0 ||
			(owner == rel && entry->state.cached_pages != 0) ||
			rel->rd_rel->relpages != entry->state.cached_pages ||
			rel->rd_rel->reltuples != 0 || rel->rd_rel->relallvisible != 0 ||
			RelationGetNumberOfBlocks(rel) != entry->state.cached_pages)
			return false;
	}

	/* Anchors lost at a COMMIT without counters are reseeded, relstats kept. */
	if (have_counters &&
		(!heapentry->state.has_delta_state ||
		 !heapentry->state.has_stats_baseline ||
		 heapentry->state.stats_baseline_inserted != ins ||
		 heapentry->state.stats_baseline_updated != upd ||
		 heapentry->state.stats_baseline_deleted != del ||
		 heapentry->state.stats_baseline_truncdropped != truncdropped))
	{
		fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_ANALYZE);
		fasttrun_analyze_save_undo(heapentry);
		fasttrun_cache_store_delta_state(heapentry, ins, upd, del, truncdropped);
		fasttrun_cache_set_stats_baseline(relid, ins, upd, del, truncdropped);
	}
	return true;
}

static void
fasttrun_close_truncate_workset(FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;

	foreach(lc, opened->user_indexes)
		index_close((Relation) lfirst(lc), NoLock);
	list_free(opened->user_indexes);
	opened->user_indexes = NIL;
	foreach(lc, opened->toast_indexes)
		index_close((Relation) lfirst(lc), NoLock);
	list_free(opened->toast_indexes);
	opened->toast_indexes = NIL;
	if (opened->toastrel != NULL)
		table_close(opened->toastrel, NoLock);
	opened->toastrel = NULL;
	if (opened->slot_relations != NULL)
		pfree(opened->slot_relations);
	opened->slot_relations = NULL;
	opened->nslots = 0;
}

static FasttrunTruncateOperation *
fasttrun_allocate_truncate_operation(Relation heaprel,
									FasttrunOpenedWorkset *opened,
									uint64 generation)
{
	FasttrunTruncateOperation *operation;
	Size		size;
	int			i;
	int			toast_index_end;
	int			toast_heap_index = -1;
	RelFileLocatorBackend root_rlb;

	/* Journal every birth before allocating an operation not yet owned by
	 * the caller's error cleanup.  No catalog access is needed on abort. */
	for (i = 0; i < opened->nslots; i++)
		fasttrun_xact_mark_created_relation(opened->slot_relations[i]);

	fasttrun_operation_init();
	size = offsetof(FasttrunTruncateOperation, slots) +
		(sizeof(FasttrunTruncateResultSlot) * opened->nslots);
	operation = (FasttrunTruncateOperation *)
		MemoryContextAllocZero(fasttrun_operation_mcxt, size);
	root_rlb.locator = heaprel->rd_locator;
	root_rlb.backend = heaprel->rd_backend;
	operation->root_relid = RelationGetRelid(heaprel);
	operation->root_rlb = root_rlb;
	operation->generation = generation;
	operation->phase = FASTTRUN_TRUNCATE_PREPARED;
	operation->nuser_indexes = list_length(opened->user_indexes);
	operation->ntoast_indexes = list_length(opened->toast_indexes);
	operation->nslots = opened->nslots;
	toast_index_end = operation->nuser_indexes + operation->ntoast_indexes;
	if (opened->toastrel != NULL)
		toast_heap_index = toast_index_end;

	for (i = 0; i < opened->nslots; i++)
	{
		Relation	rel = opened->slot_relations[i];
		FasttrunTruncateResultSlot *slot = &operation->slots[i];

		slot->relid = RelationGetRelid(rel);
		slot->root_relid = operation->root_relid;
		slot->rlb.locator = rel->rd_locator;
		slot->rlb.backend = rel->rd_backend;
		if (i < operation->nuser_indexes)
		{
			slot->kind = FASTTRUN_TRUNCATE_USER_INDEX;
			slot->heap_relid = operation->root_relid;
			slot->heap_rlb = operation->root_rlb;
		}
		else if (i < toast_index_end)
		{
			slot->kind = FASTTRUN_TRUNCATE_TOAST_INDEX;
			slot->heap_relid = RelationGetRelid(opened->toastrel);
			slot->heap_rlb.locator = opened->toastrel->rd_locator;
			slot->heap_rlb.backend = opened->toastrel->rd_backend;
		}
		else if (i == toast_heap_index)
		{
			slot->kind = FASTTRUN_TRUNCATE_TOAST_HEAP;
			slot->heap_relid = slot->relid;
			slot->heap_rlb = slot->rlb;
		}
		else
		{
			slot->kind = FASTTRUN_TRUNCATE_MAIN_HEAP;
			slot->heap_relid = slot->relid;
			slot->heap_rlb = slot->rlb;
		}
	}
	return operation;
}

/* Save complete undo and switch every result slot to neutral ownership. */
static void
fasttrun_reserve_empty_publication(FasttrunTruncateOperation *operation,
								   Relation heaprel)
{
	int			i;

	fasttrun_xact_mark_relid(operation->root_relid, operation->root_relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_STATS |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry;

		fasttrun_xact_mark_relid(slot->relid, operation->root_relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
		entry = fasttrun_cache_enter(slot->relid);

		fasttrun_analyze_save_undo(entry);
		entry->state.has_relstats = false;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_epoch = 0;
		entry->lazy_check_pages = 0;
		slot->analyze_entry = entry;
		FASTTRUN_TEST_FAILPOINT("reserve_after_state", i + 1);
	}
	(void) fasttrun_stats_neutralize_relation(heaprel);
	operation->have_pgstat_seed = fasttrun_read_pgstat_counters(heaprel,
													&operation->seed_inserted,
													&operation->seed_updated,
													&operation->seed_deleted,
													&operation->seed_truncdropped);
	fasttrun_invalidate_local_plan_cache(operation->root_relid);
}

static void
fasttrun_execute_truncate_storage(FasttrunTruncateOperation *operation,
								  Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;
	int			ordinal = 0;
	int			toast_ordinal = 0;
	int			i;

	/* The rows are gone from here on, whatever the level's outcome. */
	for (i = 0; i < operation->nslots; i++)
		fasttrun_xact_flag_marked_relid(operation->slots[i].relid,
										FASTTRUN_TOUCH_TRUNCATED);
	operation->phase = FASTTRUN_TRUNCATE_MUTATED;

	foreach(lc, opened->user_indexes)
	{
		fasttrun_truncate_one_storage((Relation) lfirst(lc),
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_user_index", ++ordinal);
	}
	foreach(lc, opened->toast_indexes)
	{
		fasttrun_truncate_one_storage((Relation) lfirst(lc),
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_toast_index", ++toast_ordinal);
	}
	if (opened->toastrel != NULL)
	{
		fasttrun_truncate_one_storage(opened->toastrel,
									  fasttrun_zero_sinval_truncate, false);
		FASTTRUN_TEST_FAILPOINT("after_toast_heap", 0);
	}
	fasttrun_truncate_one_storage(heaprel, fasttrun_zero_sinval_truncate,
								 false);
	FASTTRUN_TEST_FAILPOINT("after_main_heap", 0);
}

static void
fasttrun_rebuild_truncate_indexes(FasttrunTruncateOperation *operation,
								  Relation heaprel,
								  FasttrunOpenedWorkset *opened)
{
	ListCell   *lc;
	int			ordinal = 0;

	/* TOAST structures are made usable before user-visible indexes. */
	foreach(lc, opened->toast_indexes)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		FasttrunTruncateResultSlot *slot;

		slot = fasttrun_truncate_slot(operation, RelationGetRelid(indexrel));
		slot->rebuilt_pages =
			fasttrun_ambuild_empty_index(opened->toastrel, indexrel);
		FASTTRUN_TEST_FAILPOINT("after_ambuild", ++ordinal);
	}
	foreach(lc, opened->user_indexes)
	{
		Relation	indexrel = (Relation) lfirst(lc);
		FasttrunTruncateResultSlot *slot;

		slot = fasttrun_truncate_slot(operation, RelationGetRelid(indexrel));
		slot->rebuilt_pages = fasttrun_ambuild_empty_index(heaprel, indexrel);
		FASTTRUN_TEST_FAILPOINT("after_ambuild", ++ordinal);
	}
}

/* No allocation, hash growth, relcache open or pgstat read past this point. */
static void
fasttrun_publish_empty_workset(FasttrunTruncateOperation *operation,
								FasttrunOpenedWorkset *opened)
{
	SubTransactionId subid = GetCurrentSubTransactionId();
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry = slot->analyze_entry;
		Relation	rel = opened->slot_relations[i];

		Assert(entry != NULL);
		entry->state.has_relstats = true;
		entry->state.cached_locator = slot->rlb.locator;
		entry->state.cached_pages = slot->rebuilt_pages;
		/* The storage was reset: no dead pages are left. */
		entry->aborted_pages = 0;
		entry->state.published_dead = 0;
		entry->state.published_aborted_writes = entry->aborted_writes;
		entry->state.last_inval_dead = 0;
		entry->state.cached_tuples = 0;
		entry->state.count_exact = true;
		entry->state.cached_allvisible = 0;
		entry->state.probe_rlb = slot->rlb;
		entry->state.heap_relid = slot->heap_relid;
		entry->state.heap_rlb = slot->heap_rlb;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_pages = 0;
		entry->state.last_inval_tuples = 0;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->state_subid = subid;
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_epoch = 0;
		entry->lazy_check_pages = 0;

		if (slot->kind == FASTTRUN_TRUNCATE_MAIN_HEAP)
		{
			entry->state.last_inval_valid = true;
			if (operation->have_pgstat_seed)
			{
				fasttrun_cache_store_delta_state(entry,
											 operation->seed_inserted,
											 operation->seed_updated,
											 operation->seed_deleted,
											 operation->seed_truncdropped);
				entry->state.has_stats_baseline = true;
				entry->state.stats_baseline_inserted = operation->seed_inserted;
				entry->state.stats_baseline_updated = operation->seed_updated;
				entry->state.stats_baseline_deleted = operation->seed_deleted;
				entry->state.stats_baseline_truncdropped =
					operation->seed_truncdropped;
			}
		}

		rel->rd_rel->relpages = slot->rebuilt_pages;
		rel->rd_rel->reltuples = 0;
		rel->rd_rel->relallvisible = 0;
		slot->published = true;
		operation->published_slots++;
		FASTTRUN_TEST_FAILPOINT("after_publish", operation->published_slots);
	}
}

static void
fasttrun_clear_partial_publication(FasttrunTruncateOperation *operation)
{
	int			i;

	for (i = 0; i < operation->nslots; i++)
	{
		FasttrunTruncateResultSlot *slot = &operation->slots[i];
		FasttrunAnalyzeCacheEntry *entry;

		if (!slot->published)
			continue;
		entry = slot->analyze_entry;
		entry->state.has_relstats = false;
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		entry->state.last_inval_valid = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
		entry->lazy_check_subid = InvalidSubTransactionId;
		entry->lazy_check_epoch = 0;
		entry->lazy_check_pages = 0;
		slot->published = false;
	}
	operation->published_slots = 0;
}

/*
 * Invalidate backend-local cached plans for heap relation `relid`.
 * This reaches `PlanCacheRelCallback` through
 * `LocalExecuteInvalidationMessage(SHAREDINVALRELCACHE_ID)` and does
 * not publish anything into the shared sinval queue.  That preserves
 * the zero-sinval contract: temp tables are private to the current
 * backend, so other backends do not need to hear about them.
 *
 * Why this exists: fasttruncate physically resets the table via
 * unlink+smgrcreate and intentionally emits no shared invalidation,
 * and fasttrun_analyze / fasttrun_collect_stats publish planner stats
 * without touching pg_class or pg_statistic.  Without a local callback,
 * cached SPI / PREPARE plans can keep is_valid=true and stale rowcount
 * or column-stat assumptions.  Marking those plans invalid forces the
 * next EXECUTE to replan and pick up fresh stats through fasttrun hooks.
 *
 * Side effect: before dispatching relcache callbacks,
 * `LocalExecuteInvalidationMessage` calls
 * `RelationCacheInvalidateEntry(relid)`.  For temp relations that
 * becomes `RelationFlushRelation`, i.e. a full relcache rebuild from
 * pg_class.  Therefore fasttruncate must call us BEFORE zeroing
 * `rd_rel->relpages/reltuples`, otherwise the rebuild would overwrite
 * those zeros from pg_class.  After truncate, the authoritative source
 * of refreshed stats is fasttrun_analyze_cache, reinjected by
 * fasttrun_planner_hook.
 *
 * Contract invariant: after fasttruncate, callers must run
 * fasttrun_analyze() before relying on replanning to see real nonzero
 * stats.  The analyze path calls this helper before publishing rd_rel,
 * so any relcache rebuild caused by the callback cannot wipe the final
 * in-memory values.
 */
static void
fasttrun_invalidate_local_plan_cache(Oid relid)
{
	SharedInvalidationMessage msg;

#ifdef USE_ASSERT_CHECKING
	fasttrun_test_plan_invalidations++;
#endif

	/* InvalidOid would target the whole relcache, so defend against that. */
	Assert(OidIsValid(relid));

	/* Fixed text (no OID) so regression tests can count calls. */
	elog(DEBUG1, "fasttrun: invalidating backend-local cached plans");
	fasttrun_count(FTC_PLAN_INVALIDATIONS);

	msg.rc.id = SHAREDINVALRELCACHE_ID;
	msg.rc.dbId = MyDatabaseId;
	msg.rc.relId = relid;
	LocalExecuteInvalidationMessage(&msg);
}

/*
 * fasttruncate(text)
 *
 * Quickly empty a temporary table.
 *
 * Behaviour:
 *   * Resolves the supplied (possibly schema-qualified) name to an OID,
 *     taking ExclusiveLock on the relation.  If the relation does not
 *     exist, the function silently does nothing -- this lets callers drop
 *     the usual `IF EXISTS` / pre-check pattern and simplifies the
 *     recovery logic of bulk PL/pgSQL routines.  ExclusiveLock is enough:
 *     no other session can read a temporary table, and unlike
 *     AccessExclusiveLock it is not WAL-logged for standbys, so the call
 *     assigns no transaction ID and writes no WAL.  The heap, TOAST and
 *     index storage of a temporary relation is never WAL-logged either.
 *   * Verifies the relation lives in a temp namespace and otherwise raises
 *     an error (this prevents accidentally truncating a regular table).
 *   * Then runs these steps in order:
 *       1. fasttrun_prepare_truncate_workset() opens the heap, its indexes,
 *          the TOAST heap and the TOAST indexes before any cache or storage
 *          side effect.  Partition/inheritance traversal is deliberately
 *          unsupported.  If the table's empty state is already fully
 *          published, fasttruncate returns here without file changes.
 *       2. The operation and its registry entry are allocated, the undo
 *          state is saved, the cached stats of the set are reset to
 *          neutral, and local cached plans on the table are invalidated.
 *       3. fasttrun_execute_truncate_storage() empties the user indexes,
 *          then the TOAST indexes, then the TOAST heap, and the main heap
 *          last.  With fasttrun.zero_sinval_truncate on, each one gets
 *          unlink + smgrcreate and no shared invalidation.  With it off,
 *          RelationTruncate() sends one shared SMGR invalidation per
 *          relation.
 *       4. fasttrun_rebuild_truncate_indexes() runs ambuild for the TOAST
 *          indexes, then for the user indexes.
 *       5. fasttrun_publish_empty_workset() publishes the relstats of the
 *          emptied set: zero tuples, and the rebuilt size of each index.
 *     The order is load-bearing.  Each heap is emptied only after its
 *     indexes, so no failure state pairs an empty heap with an index that
 *     still holds old TIDs, which after a refill would point at unrelated
 *     rows.  Every storage drop also finishes before the first ambuild, so
 *     a failure cannot leave an index rebuilt empty over rows the heap
 *     still holds, where index scans would silently miss them.
 *   * An error before step 3 rolls back normally, and a block left by an
 *     earlier failed attempt stays.  An error from step 3 on blocks the
 *     table in this backend: planning a query on it, the other fasttrun
 *     functions and utility commands on it such as COPY, TRUNCATE, VACUUM
 *     or REINDEX fail with SQLSTATE 55000.  The block ends when a later
 *     fasttruncate of the table succeeds, a DROP of the table commits, the
 *     table's creation rolls back, or DISCARD TEMP/ALL runs.
 *
 * IMPORTANT: this function is FOREIGN-KEY-UNSAFE.  fasttruncate does
 * NOT scan pg_constraint.  If you have FKs involving temp tables, use
 * SQL TRUNCATE instead.  Workloads this extension is built for never
 * declare FKs on temp tables -- we skip the check by design.
 *
 * fasttruncate() does NOT call vacuum() or analyze_rel() after the
 * truncate.  On a busy server with many concurrent backends that pattern
 * is catastrophic: each ANALYZE generates dozens of pg_class /
 * pg_statistic invalidation messages, the shared invalidation queue
 * overflows, and every backend ends up spending all of its CPU draining
 * that queue inside ReceiveSharedInvalidMessages() rather than doing
 * useful work.
 *
 * Skipping ANALYZE is functionally safe.  The planner reads the current
 * page count via RelationGetNumberOfBlocks() (which always reflects the
 * real on-disk state, not stale catalogue stats), and falls back to a
 * default density estimate when reltuples / relpages are stale.  For
 * temp tables that are immediately re-filled in the same transaction
 * the catalogue stats would be wrong either way until the next ANALYZE.
 *
 * Warning: this function is NOT transaction-safe.  Storage reset cannot be
 * rolled back.  After surrounding rollback the table remains physically
 * empty.  The session-local cache follows that physical truth.
 */
Datum
fasttruncate(PG_FUNCTION_ARGS)
{
	text		   *name = PG_GETARG_TEXT_P(0);
	RangeVar	   *relvar;
	Oid				relOid;
	Relation		rel;
	uint64			truncate_generation;
	FasttrunOpenedWorkset opened;
	FasttrunTruncateOperation *operation = NULL;
	FasttrunTruncateOperation *volatile cleanup_operation = NULL;
	volatile uint64 cleanup_generation = 0;
	volatile bool cleanup_pending_published = false;
	instr_time		started;

	fasttrun_statement_seq++;
	relvar = fasttrun_make_rangevar(name);
	relOid = RangeVarGetRelid(relvar, ExclusiveLock, true);

	/*
	 * Missing table -- silently bail out (see function header).
	 */
	if (!OidIsValid(relOid))
		PG_RETURN_VOID();

	rel = table_open(relOid, NoLock);

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttruncate: relation \"%s\" is not a local temporary table",
			 RelationGetRelationName(rel));
	}

	/* Heap-AM only. */
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		table_close(rel, NoLock);
		elog(ERROR, "fasttruncate: relation \"%s\" is not heap-AM",
			 RelationGetRelationName(rel));
	}
	if (fasttrun_passthrough)
	{
		table_close(rel, NoLock);
		fasttrun_passthrough_command("TRUNCATE ONLY", relOid);
		PG_RETURN_VOID();
	}

	/* An active marker permits only this repair path or DROP/recreate. */
	fasttrun_poison_check_relation(rel, "fasttruncate", true);
	/* Before any change: the clear mark after the reset must not fail. */
	fasttrun_fresh_reserve(rel);
	fasttrun_count(FTC_TRUNCATE_CALLS);
	fasttrun_timer_start(&started);
	fasttrun_prepare_truncate_workset(rel, &opened);
	if (fasttrun_try_empty_truncate(rel, &opened))
	{
		/* Before the log: ereport may stop the call on a pending cancel. */
		fasttrun_fresh_mark(rel);
		fasttrun_count(FTC_TRUNCATE_EMPTY_FAST_PATH);
		fasttrun_log_if_slow(&started, "fasttruncate", rel, "already empty");
		fasttrun_close_truncate_workset(&opened);
		table_close(rel, ExclusiveLock);
		PG_RETURN_VOID();
	}

	truncate_generation = ++fasttrun_truncate_generation;
	if (truncate_generation == 0)
		truncate_generation = ++fasttrun_truncate_generation;

	PG_TRY();
	{
		operation = fasttrun_allocate_truncate_operation(rel, &opened,
														 truncate_generation);
		cleanup_operation = operation;
		cleanup_generation = truncate_generation;
		(void) fasttrun_poison_reserve(operation);
		cleanup_pending_published = true;
		FASTTRUN_TEST_FAILPOINT("after_prepare", 0);

		/* Reserve all memory and rollback state before changing files. */
		fasttrun_reserve_empty_publication(operation, rel);
		FASTTRUN_TEST_FAILPOINT("after_phase0", 0);

		fasttrun_execute_truncate_storage(operation, rel, &opened);
		fasttrun_rebuild_truncate_indexes(operation, rel, &opened);
		FASTTRUN_TEST_FAILPOINT("before_publish", 0);
		fasttrun_publish_empty_workset(operation, &opened);

		operation->phase = FASTTRUN_TRUNCATE_COMPLETE;
		fasttrun_poison_complete(operation);
		cleanup_operation = NULL;
		cleanup_generation = 0;
		cleanup_pending_published = false;
	}
	PG_CATCH();
	{
		FasttrunTruncateOperation *failed =
			(FasttrunTruncateOperation *) cleanup_operation;

		if (failed != NULL && failed->generation == cleanup_generation)
		{
			if (failed->phase == FASTTRUN_TRUNCATE_PREPARED)
			{
				if (cleanup_pending_published)
					fasttrun_poison_cancel_pending(failed);
				else
				{
					pfree(failed);
					fasttrun_operation_reset_if_empty();
				}
			}
			else
			{
				fasttrun_clear_partial_publication(failed);
				Assert(cleanup_pending_published);
				fasttrun_poison_activate(failed);
				fasttrun_count(FTC_TRUNCATE_FAILED_AFTER_CHANGE);
			}
		}
		else
			fasttrun_operation_reset_if_empty();

		fasttrun_close_truncate_workset(&opened);
		table_close(rel, NoLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_fresh_mark(rel);
	fasttrun_log_if_slow(&started, "fasttruncate", rel, "storage reset");

	/* Release the ExclusiveLock taken by RangeVarGetRelid. */
	fasttrun_close_truncate_workset(&opened);
	table_close(rel, ExclusiveLock);

	PG_RETURN_VOID();
}

