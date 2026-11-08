/*-------------------------------------------------------------------------
 *
 * src/09_writers.c
 *      Storage checks, write marks and executor hooks.
 *
 * What it holds.  The one-off empty-storage check of a transaction, the marks
 * set when a statement writes a temporary relation or its partitions, and the
 * ExecutorStart and ExecutorEnd hooks.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/09_writers.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * The library was loaded on its first use rather than preloaded, so statements
 * that had already started were never offered to the hook.  Which relations
 * they write, and whether they write at all, cannot be recovered afterwards, so
 * the per-column account does not run in such a backend at all: wear from an
 * UPDATE counts for every column of the relation.  Put fasttrun in
 * session_preload_libraries to get it, but a background worker never loads
 * that list and needs shared_preload_libraries.
 */
static bool fasttrun_lazy_loaded = false;
static bool fasttrun_lazy_loaded_warned = false;

/*
 * The heap size just read stays the size until the next statement starts,
 * unless a statement already running can still grow the file: one writing this
 * relation, one the registry may not know (the library loaded during it, or the
 * registry overflowed), or one routing rows into this partition through its
 * parent.  Only then may a later write take the read as its level mark.
 */
static bool
fasttrun_probe_size_stays(Relation rel)
{
	return !fasttrun_unobserved_writers && !rel->rd_rel->relispartition &&
		!fasttrun_stats_relid_in_flight(RelationGetRelid(rel));
}


/*
 * Once per transaction, before the delta state is trusted, check that the
 * heap still has storage.  Core ON COMMIT DELETE ROWS empties tables at commit
 * without counters.  The planner's probe catches it when the relation is
 * planned, and this check covers the first write and the first analyze.
 * Within a transaction nothing outside fasttrun empties storage, and the
 * pages an analyze publishes were measured, so the mark lasts the transaction.
 */
static inline bool
fasttrun_cache_storage_check_due(const FasttrunAnalyzeCacheEntry *entry,
								 Oid relid)
{
	return entry->state.has_delta_state && entry->state.cached_pages > 0 &&
		entry->state.heap_relid == relid &&
		entry->write_check_epoch != fasttrun_xact_epoch;
}

/*
 * The size is read through the relation's own smgr handle, one lseek.  The
 * smgr-level probe of the abort path would close and reopen the files first,
 * and the write that follows would open them once more.  A relation on another
 * file belongs to the analyze locator check, not to this one.  Returns the size
 * read, or InvalidBlockNumber when the relation is on another file.
 */
static BlockNumber
fasttrun_cache_check_storage(FasttrunAnalyzeCacheEntry *entry, Relation rel)
{
	Oid			plan_relid;
	BlockNumber pages;

	entry->write_check_epoch = fasttrun_xact_epoch;
	if (!RelFileLocatorEquals(rel->rd_locator, entry->state.probe_rlb.locator) ||
		rel->rd_backend != entry->state.probe_rlb.backend)
		return InvalidBlockNumber;
	pages = RelationGetNumberOfBlocks(rel);
	if (pages == 0)
	{
		bool		had_rows = entry->state.cached_tuples > 0;

		if (fasttrun_cache_make_empty_storage_authoritative(entry, false,
														   &plan_relid) &&
			had_rows)
			fasttrun_count(FTC_STORAGE_FOUND_EMPTY);
	}
	return pages;
}

/*
 * A statement is about to write the relation.  The commit walk carries the
 * delta state over the commit only for relations marked here, so every write
 * target that has fasttrun state gets the mark, INSERT and DELETE included.
 * With track_counts off the rows are written but not counted: the delta state
 * no longer matches the table and is dropped (with undo, a rollback brings it
 * back), so the next analyze scans.
 */
static void
fasttrun_xact_mark_write(Oid relid)
{
	FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);
	FasttrunXactRelEntry *xentry;
	BlockNumber checked_pages = InvalidBlockNumber;
	RelFileLocator checked_locator;

	/*
	 * An abort of the write takes the grown size from storage, also while DDL
	 * keeps the relstats unpublished and the saved state waits for a restore.
	 */
	if (!fasttrun_stats_relid_exists(relid) &&
		(entry == NULL ||
		 (!entry->state.has_delta_state && !entry->state.has_relstats &&
		  entry->state.heap_relid != relid)))
		return;

	/*
	 * Core ON COMMIT DELETE ROWS empties the table at commit without counters,
	 * and a cached generic plan or COPY writes without the planner's probe, so
	 * the rows written now would add to rows that are gone.
	 */
	if (entry != NULL && fasttrun_cache_storage_check_due(entry, relid))
	{
		Relation	rel = RelationIdGetRelation(relid);

		if (RelationIsValid(rel))
		{
			checked_pages = fasttrun_cache_check_storage(entry, rel);
			checked_locator = rel->rd_locator;
			RelationClose(rel);
		}
	}
	xentry = fasttrun_xact_mark_relid(relid, relid,
									  pgstat_track_counts ? FASTTRUN_TOUCH_DML :
									  FASTTRUN_TOUCH_DML | FASTTRUN_TOUCH_UNCOUNTED);

	/*
	 * The first write at this level marks the heap size and the dead-page
	 * count before it: an abort of the level attributes only the growth since
	 * this mark to its own rolled-back rows.
	 */
	if (xentry != NULL && !xentry->level_size_known && entry != NULL &&
		entry->state.has_relstats && entry->state.heap_relid == relid &&
		checked_pages != InvalidBlockNumber)
	{
		/* The storage check above has just read the size. */
		xentry->level_pages = checked_pages;
		xentry->level_dead = entry->aborted_pages;
		xentry->level_locator = checked_locator;
		xentry->level_size_known = true;
	}
	else if (xentry != NULL && !xentry->level_size_known && entry != NULL &&
			 entry->state.has_relstats && entry->state.heap_relid == relid &&
			 entry->probe_size != InvalidBlockNumber &&
			 entry->probe_size_seq == fasttrun_statement_seq &&
			 entry->probe_size_epoch == fasttrun_xact_epoch)
	{
		/* The planning of this statement read the size, on the cached file. */
		xentry->level_pages = entry->probe_size;
		xentry->level_dead = entry->aborted_pages;
		xentry->level_locator = entry->state.cached_locator;
		xentry->level_size_known = true;
	}
	else if (xentry != NULL && !xentry->level_size_known && entry != NULL &&
			 entry->state.heap_relid == relid)
	{
		Relation	rel = RelationIdGetRelation(relid);

		if (RelationIsValid(rel))
		{
			fasttrun_cache_follow_file(entry, rel);
			if (fasttrun_cache_entry_on_file(entry, rel))
			{
				xentry->level_pages = RelationGetNumberOfBlocks(rel);
				xentry->level_dead = entry->aborted_pages;
				xentry->level_locator = rel->rd_locator;
				xentry->level_size_known = true;
			}
			RelationClose(rel);
		}
	}
	if (!pgstat_track_counts && entry != NULL && entry->state.has_delta_state)
	{
		fasttrun_analyze_save_undo(entry);
		entry->state.has_delta_state = false;
		entry->state.has_stats_baseline = false;
		fasttrun_cache_reset_partial_scan_anchor(entry);
	}
}

/*
 * Rows written through a partitioned table land in partitions the executor or
 * COPY picks per row, after the statement has started.  Mark every partition
 * that carries fasttrun state, so the commit walk carries its counters too, and
 * refuse the statement when a partition is blocked after an incomplete
 * truncate.  Called for temporary partitioned targets only: their partitions
 * are temporary as well.
 */
static void
fasttrun_xact_mark_partitions(Oid parent_relid)
{
	List	   *children;
	ListCell   *lc;

	if (fasttrun_analyze_cache == NULL && fasttrun_stats_relid_cache == NULL &&
		fasttrun_poison_cache == NULL)
		return;
	children = find_all_inheritors(parent_relid, NoLock, NULL);
	foreach(lc, children)
	{
		Oid			child = lfirst_oid(lc);

		if (child == parent_relid)
			continue;
		/* Row routing reaches a partition the plan does not name. */
		fasttrun_poison_check_relid(child, "write through a partitioned table");
		fasttrun_xact_mark_write(child);
	}
	list_free(children);
}

static void
fasttrun_executor_walk_targets(PlannedStmt *stmt, bool starting)
{
	ListCell   *lc;

	if (stmt == NULL ||
		(stmt->commandType == CMD_SELECT && !stmt->hasModifyingCTE))
		return;

	foreach(lc, stmt->resultRelations)
	{
		int			rtindex = lfirst_int(lc);
		RangeTblEntry *rte = rt_fetch(rtindex, stmt->rtable);
		RTEPermissionInfo *perminfo = NULL;
		bool		writes_columns;

		if (rte->rtekind != RTE_RELATION)
			continue;

		/* PG16 moved updatedCols out of RangeTblEntry. */
		if (rte->perminfoindex > 0)
			perminfo = getRTEPermissionInfo(stmt->permInfos, rte);

		/*
		 * An INSERT or DELETE target names no columns, so it leaves the mask
		 * alone -- it still enters the registry below, because its rows are
		 * just as invisible to a scan on the same command's snapshot.  A
		 * target with no permission information hides which it is: treat it as
		 * naming columns, so a collect running underneath stays conservative.
		 */
		writes_columns = (perminfo == NULL ||
						  !bms_is_empty(perminfo->updatedCols));

		/*
		 * Every DML target enters the registry, INSERT and DELETE included:
		 * their rows are invisible to a scan running on the same command's
		 * snapshot just as an UPDATE's are.  The registry is kept even for a
		 * relation fasttrun does not manage yet, because its first collect may
		 * happen from inside this very statement.  Only a temporary relation
		 * can be collected at all, so a permanent target is left out: an
		 * UPDATE or DELETE of a partitioned table lists every unpruned
		 * partition and would overflow the registry for nothing.
		 */
		if (!fasttrun_relid_is_temp(rte->relid))
			continue;
		fasttrun_stats_update_in_flight(rte->relid, starting ? 1 : -1);
		if (starting && rte->relkind == RELKIND_PARTITIONED_TABLE)
			fasttrun_xact_mark_partitions(rte->relid);

		if (starting)
		{
			/*
			 * The commit walk decides from this mark whether the relation
			 * needs a freshness re-check, and carries its delta state over
			 * the commit.
			 */
			fasttrun_xact_mark_write(rte->relid);
			if (writes_columns && fasttrun_stats_relid_exists(rte->relid))
				fasttrun_stats_note_update_target(rte->relid, perminfo);
		}
	}
}

static void
fasttrun_executor_start(QueryDesc *queryDesc, int eflags)
{
	if (!fasttrun_passthrough)
	{
		fasttrun_exit_plan_reset_register();
		/*
		 * A plan can run long after it was made: a SQL function in PG 16 and
		 * 17 plans all its statements before the first one runs, and that one
		 * may be the fasttruncate that fails.
		 */
		if (fasttrun_poison_cache != NULL)
			fasttrun_poison_check_plan(queryDesc->plannedstmt);
		fasttrun_executor_walk_targets(queryDesc->plannedstmt, true);
		fasttrun_statement_seq++;
	}

	if (prev_ExecutorStart != NULL)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void
fasttrun_executor_end(QueryDesc *queryDesc)
{
	if (!fasttrun_passthrough)
		fasttrun_executor_walk_targets(queryDesc->plannedstmt, false);

	if (prev_ExecutorEnd != NULL)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

