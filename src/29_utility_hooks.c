/*-------------------------------------------------------------------------
 *
 * src/29_utility_hooks.c
 *      Object access hook and utility hook.
 *
 * What it holds.  Cache eviction by utility commands, COPY FROM marks,
 * fasttrun_object_access_hook and fasttrun_utility_hook.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/29_utility_hooks.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* Returns the heaps that keep their statistics through the command (FasttrunKeptDdl). */
static List *
fasttrun_evict_utility_caches(Node *parsetree)
{
	List	   *kept_ddl = NIL;

	if (parsetree == NULL)
		return NIL;

	if (fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL)
		return NIL;

	switch (nodeTag(parsetree))
	{
		case T_AlterTableStmt:
			{
				AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
				FasttrunDdlEffect effect = fasttrun_alter_table_effect(stmt);
				Oid			target_oid =
					RangeVarGetRelid(stmt->relation, NoLock, true);
				List	   *relids;
				ListCell   *lc;

				if (OidIsValid(target_oid) &&
					get_rel_relkind(target_oid) == RELKIND_INDEX &&
					effect != FASTTRUN_DDL_EVICTS)
					target_oid = IndexGetRelation(target_oid, true);

				relids = fasttrun_utility_tree(target_oid,
											   fasttrun_alter_table_recurses(stmt));
				foreach(lc, relids)
				{
					if (effect == FASTTRUN_DDL_EVICTS)
						fasttrun_evict_temp_relid(lfirst_oid(lc));
					else
						fasttrun_keep_stats_through_ddl(lfirst_oid(lc), stmt,
													effect == FASTTRUN_DDL_BUILDS_INDEX,
													&kept_ddl);
				}
				list_free(relids);

				foreach(lc, stmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

					if ((cmd->subtype == AT_AttachPartition ||
						 cmd->subtype == AT_DetachPartition) &&
						cmd->def != NULL && IsA(cmd->def, PartitionCmd) &&
						((PartitionCmd *) cmd->def)->name != NULL)
					{
						Oid			part_oid = RangeVarGetRelid(
							((PartitionCmd *) cmd->def)->name, NoLock, true);
						List	   *part_relids = fasttrun_utility_tree(part_oid, true);
						ListCell   *plc;

						foreach(plc, part_relids)
							fasttrun_evict_temp_relid(lfirst_oid(plc));
						list_free(part_relids);
					}
				}
				break;
			}
		case T_CreateTrigStmt:
			fasttrun_stats_note_wide_writer_ddl(
				((CreateTrigStmt *) parsetree)->relation);
			break;
		case T_TruncateStmt:
			{
				TruncateStmt *stmt = (TruncateStmt *) parsetree;
				ListCell   *lc;

				foreach(lc, stmt->relations)
				{
					RangeVar   *rv = (RangeVar *) lfirst(lc);
					List	   *relids = fasttrun_utility_tree(
						RangeVarGetRelid(rv, NoLock, true), rv->inh);
					ListCell   *rlc;

					foreach(rlc, relids)
						fasttrun_evict_temp_relid(lfirst_oid(rlc));
					list_free(relids);
				}
				break;
			}
		case T_IndexStmt:
			{
				IndexStmt  *stmt = (IndexStmt *) parsetree;
				Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);
				bool		builds = fasttrun_index_stmt_builds(stmt);
				List	   *relids = fasttrun_utility_tree(relid,
					stmt->relation->inh && get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE);
				ListCell   *lc;

				foreach(lc, relids)
					fasttrun_keep_stats_through_ddl(lfirst_oid(lc), NULL, builds, &kept_ddl);
				list_free(relids);
			}
			break;
		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;
				ListCell   *lc;

				if (stmt->removeType != OBJECT_TABLE &&
					stmt->removeType != OBJECT_INDEX)
					break;
				foreach(lc, stmt->objects)
				{
					List	   *name = (List *) lfirst(lc);
					RangeVar   *rv = makeRangeVarFromNameList(name);

					fasttrun_evict_rangevar(rv);
				}
				break;
			}
		default:
			break;
	}
	return kept_ddl;
}

/*
 * COPY ... FROM writes rows without going through the executor, so nothing else
 * records it.  Return its target so the caller can count it as a running writer
 * for the duration: a collect called from a COPY expression scans on the same
 * command's snapshot and cannot see the rows the counters already hold.  A
 * permanent target is not returned.  The registry holds temporary relations only.
 */
static Oid
fasttrun_mark_copy_from(Node *parsetree)
{
	CopyStmt   *stmt;
	Oid			relid;

	if (parsetree == NULL || !IsA(parsetree, CopyStmt))
		return InvalidOid;
	stmt = (CopyStmt *) parsetree;
	if (!stmt->is_from || stmt->relation == NULL)
		return InvalidOid;
	relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return InvalidOid;
	fasttrun_xact_mark_write(relid);
	if (get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE &&
		fasttrun_relid_is_temp(relid))
		fasttrun_xact_mark_partitions(relid);
	if (!fasttrun_relid_is_temp(relid))
		return InvalidOid;
	return relid;
}

/*
 * Dependency-machinery drops (DROP ... CASCADE, DROP OWNED BY, DISCARD)
 * delete relations without a per-table DropStmt, so the utility hook never
 * sees them.  Touch the relid and note the drop with its subxact id.  The
 * commit callbacks remove noted entries without probing the syscache
 * (TRANS_COMMIT forbids catalog access).  Rollback semantics stay intact:
 * if the drop aborts, the subxact callback prunes the note and the cache
 * entry stays valid.
 */
static void
fasttrun_object_access_hook(ObjectAccessType access, Oid classId,
							Oid objectId, int subId, void *arg)
{
	if (prev_object_access_hook)
		(*prev_object_access_hook) (access, classId, objectId, subId, arg);

	if (fasttrun_in_atx())
		return;

	if (access == OAT_POST_CREATE && classId == RelationRelationId && subId == 0)
	{
		fasttrun_fresh_note_create(objectId);
		return;
	}

	/* Works in passthrough too: there fasttruncate is a core TRUNCATE. */
	if (access == OAT_TRUNCATE && classId == RelationRelationId)
	{
		fasttrun_fresh_note_core_truncate(objectId);
		if (!fasttrun_passthrough)
		{
			FasttrunPoisonEntry *pentry = fasttrun_poison_find(objectId);

			if (pentry != NULL && pentry->active)
				fasttrun_xact_mark_relid(objectId, objectId,
										 FASTTRUN_TOUCH_POISON_RETIRE);
		}
		return;
	}

	if (access == OAT_DROP && classId == RelationRelationId && subId == 0)
		fasttrun_fresh_note_drop(objectId);

	if (fasttrun_passthrough || access != OAT_DROP)
		return;
	if (classId == StatisticExtRelationId)
	{
		HeapTuple tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(objectId));

		if (HeapTupleIsValid(tuple))
		{
			Oid relid = ((Form_pg_statistic_ext) GETSTRUCT(tuple))->stxrelid;

			if (fasttrun_relid_is_our_temp(relid))
			{
				fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_DROPPED_STATEXT);
				fasttrun_xact_dropped_statext = true;
			}
			ReleaseSysCache(tuple);
		}
		return;
	}
	if (classId != RelationRelationId)
		return;

	/* A dropped temp index can return even before its heap is managed. */
	if (subId == 0 && get_rel_relkind(objectId) == RELKIND_INDEX)
	{
		Oid			heap_relid = IndexGetRelation(objectId, true);

		if (OidIsValid(heap_relid) && fasttrun_relid_is_our_temp(heap_relid))
		{
			Relation	indexrel = try_relation_open(objectId, NoLock);

			if (indexrel == NULL ||
				fasttrun_rel_create_subid(indexrel) != GetCurrentSubTransactionId())
				fasttrun_xact_mark_relid(heap_relid, heap_relid,
										 FASTTRUN_TOUCH_DROPPED_COMPONENT);
			if (indexrel != NULL)
				relation_close(indexrel, NoLock);
		}
	}

	/*
	 * A dropped column: DROP COLUMN, a compound ALTER TABLE or a cascade to a
	 * generated column.  The mark lives at this level of the journal, so a
	 * rollback of the level takes it away, and the commit removes the column's
	 * entries: its attnum is never reused.
	 */
	if (subId != 0)
	{
		if (subId > 0 && fasttrun_stats_relid_exists(objectId))
		{
			FasttrunXactRelEntry *entry;
			MemoryContext oldcxt;

			entry = fasttrun_xact_mark_relid(objectId, objectId,
											 FASTTRUN_TOUCH_STATS);
			oldcxt = MemoryContextSwitchTo(TopTransactionContext);
			entry->dropped_attrs = bms_add_member(entry->dropped_attrs, subId);
			MemoryContextSwitchTo(oldcxt);
		}
		return;
	}

	if (fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL &&
		fasttrun_poison_cache == NULL)
		return;

	/*
	 * The expression statistics of an index live under its heap, so the index
	 * itself may be untracked.  Hide them now, with undo for a rollback of the
	 * drop, and the commit removes them.
	 */
	if (fasttrun_stats_cache != NULL && get_rel_relkind(objectId) == RELKIND_INDEX)
	{
		Oid			heap_relid = IndexGetRelation(objectId, true);

		if (OidIsValid(heap_relid))
			fasttrun_stats_neutralize_index_expressions(heap_relid, objectId);
	}

	/*
	 * Touch only relids we actually track -- keeps the list small on mass
	 * drops of unrelated relations.  DISCARD TEMP/ALL drops every temporary
	 * relation through the dependency machinery and comes here for each: the
	 * commit removes their entries, and a rolled-back DISCARD keeps them with
	 * the table, its statistics and the block of a half-cleared table.
	 */
	if (fasttrun_cache_lookup(objectId) == NULL &&
		!fasttrun_stats_relid_exists(objectId) &&
		fasttrun_poison_find(objectId) == NULL)
		return;

	fasttrun_xact_mark_relid(objectId, objectId, FASTTRUN_TOUCH_DROPPED);
}

/* ProcessUtility hook: track CREATE TEMP TABLE and evict stats on temp-table DDL. */
static void
fasttrun_utility_hook_body(PlannedStmt *pstmt,
					  const char *queryString,
					  bool readOnlyTree,
					  ProcessUtilityContext context,
					  ParamListInfo params,
					  QueryEnvironment *queryEnv,
					  DestReceiver *dest,
					  QueryCompletion *qc)
{
	Node   *parsetree = pstmt->utilityStmt;
	List   *analyze_targets = NIL;
	List   *rewrite_relids = NIL;
	List   *vacuum_relids = NIL;
	List   *kept_ddl;
	List   *statistics_drops;
	Oid		copy_relid = InvalidOid;
	bool	saved_unobserved_writers = fasttrun_unobserved_writers;

	if (fasttrun_passthrough)
	{
		if (prev_utility_hook)
			prev_utility_hook(pstmt, queryString, readOnlyTree, context,
							  params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
									params, queryEnv, dest, qc);
		return;
	}

	fasttrun_statement_seq++;
	if (IsA(parsetree, CreateTableAsStmt))
		fasttrun_unobserved_writers = true;
	else if (IsA(parsetree, ExplainStmt) &&
			 IsA(((ExplainStmt *) parsetree)->query, Query))
	{
		ExplainStmt *estmt = (ExplainStmt *) parsetree;
		Query	   *explained = (Query *) estmt->query;
		ListCell   *olc;
		bool		analyze_opt = false;

		foreach(olc, estmt->options)
		{
			DefElem    *opt = (DefElem *) lfirst(olc);

			if (strcmp(opt->defname, "analyze") == 0)
				analyze_opt = defGetBoolean(opt);
		}
		if (analyze_opt &&
			explained->commandType == CMD_UTILITY &&
			explained->utilityStmt != NULL &&
			IsA(explained->utilityStmt, CreateTableAsStmt))
			fasttrun_unobserved_writers = true;
	}
	fasttrun_exit_plan_reset_register();
	fasttrun_poison_check_utility(parsetree);
	/* Track CREATE TEMP TABLE before execution. */
	if (fasttrun_track_enabled && fasttrun_track_htab != NULL &&
		nodeTag(parsetree) == T_CreateStmt)
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;

		if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
		{
			/*
			 * Only track tables created via LIKE from the dummy schema.
			 * User-created tables (CREATE TEMP TABLE foo (id int, ...))
			 * don't have a matching dummy and would break prewarm.
			 */
			ListCell   *lc;
			bool		from_dummy = false;

			foreach(lc, stmt->tableElts)
			{
				if (IsA(lfirst(lc), TableLikeClause))
				{
					TableLikeClause *like = (TableLikeClause *) lfirst(lc);

					if (like->relation->schemaname != NULL &&
						strcmp(like->relation->schemaname,
							   fasttrun_prewarm_schema) == 0)
					{
						from_dummy = true;
						break;
					}
				}
			}

			if (from_dummy && !fasttrun_in_prewarm &&
				fasttrun_schedule_is_active_now())
			{
				FasttrunTrackEntry *entry;
				bool	found;

				LWLockAcquire(fasttrun_track_lock, LW_EXCLUSIVE);
				entry = hash_search(fasttrun_track_htab,
									stmt->relation->relname,
									hash_get_num_entries(fasttrun_track_htab) >= FASTTRUN_TRACK_MAX
									? HASH_FIND : HASH_ENTER_NULL, &found);
				if (entry != NULL)
				{
					if (!found)
					{
						entry->create_count = 0;
						entry->last_create = 0;
					}
					if (entry->create_count < PG_INT64_MAX)
						entry->create_count++;
					entry->last_create = GetCurrentTimestamp();
				}
				else
					fasttrun_count(FTC_TRACKING_REJECTED);
				LWLockRelease(fasttrun_track_lock);
			}
		}
	}

	copy_relid = fasttrun_mark_copy_from(parsetree);
	if (OidIsValid(copy_relid))
		fasttrun_stats_update_in_flight(copy_relid, 1);
	fasttrun_prepare_utility_handoff(parsetree, &analyze_targets,
									 &rewrite_relids, &vacuum_relids);
	kept_ddl = fasttrun_evict_utility_caches(parsetree);
	statistics_drops = fasttrun_statistics_drop_relids(parsetree);

	PG_TRY();
	{
		/* Chain to next hook or standard ProcessUtility. */
		if (prev_utility_hook)
			prev_utility_hook(pstmt, queryString, readOnlyTree, context,
							  params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
									params, queryEnv, dest, qc);
	}
	PG_FINALLY();
	{
		fasttrun_unobserved_writers = saved_unobserved_writers;
	}
	PG_END_TRY();

	if (OidIsValid(copy_relid))
		fasttrun_stats_update_in_flight(copy_relid, -1);

	fasttrun_note_utility_file_births(parsetree);
	fasttrun_finish_rewrite_handoff(rewrite_relids);
	fasttrun_finish_analyze_handoff(analyze_targets);
	fasttrun_free_analyze_handoff(analyze_targets);
	fasttrun_finish_vacuum(vacuum_relids);
	fasttrun_finish_kept_ddl(kept_ddl);
	fasttrun_finish_statistics_drop(statistics_drops);
	list_free(rewrite_relids);
	fasttrun_free_vacuum_targets(vacuum_relids);
	fasttrun_free_kept_ddl(kept_ddl);
	list_free(statistics_drops);
}

/*
 * A TRUNCATE statement confirms the clear marks its OAT_TRUNCATE set once
 * it has finished.  A nested TRUNCATE, run from a trigger, keeps its own list
 * and level.  Runs in passthrough mode too.
 */
static void
fasttrun_utility_hook(PlannedStmt *pstmt,
					  const char *queryString,
					  bool readOnlyTree,
					  ProcessUtilityContext context,
					  ParamListInfo params,
					  QueryEnvironment *queryEnv,
					  DestReceiver *dest,
					  QueryCompletion *qc)
{
	List	   *outer_pending = NIL;
	SubTransactionId outer_stmt_subid = InvalidSubTransactionId;
	SubTransactionId stmt_subid = InvalidSubTransactionId;
	List	   *outer_creates;
	SubTransactionId outer_create_subid;
	SubTransactionId create_subid;
	MemoryContext outer_top_mcxt;
	uint64		outer_xact_epoch;
	bool		is_truncate;

	if (fasttrun_in_atx())
	{
		if (prev_utility_hook)
			prev_utility_hook(pstmt, queryString, readOnlyTree, context,
							  params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
									params, queryEnv, dest, qc);
		return;
	}

	if (pstmt->utilityStmt == NULL ||
		IsA(pstmt->utilityStmt, TransactionStmt) ||
		IsA(pstmt->utilityStmt, CallStmt) ||
		IsA(pstmt->utilityStmt, DoStmt))
	{
		fasttrun_utility_hook_body(pstmt, queryString, readOnlyTree, context,
								   params, queryEnv, dest, qc);
		return;
	}

	is_truncate = IsA(pstmt->utilityStmt, TruncateStmt);
	outer_creates = fasttrun_create_pending;
	outer_create_subid = fasttrun_create_stmt_subid;
	create_subid = IsTransactionState()
		? GetCurrentSubTransactionId() : InvalidSubTransactionId;
	outer_top_mcxt = TopTransactionContext;
	outer_xact_epoch = fasttrun_xact_epoch;
	fasttrun_create_pending = NIL;
	fasttrun_create_stmt_subid = create_subid;
	if (is_truncate)
	{
		outer_pending = fasttrun_fresh_pending;
		outer_stmt_subid = fasttrun_fresh_stmt_subid;
		stmt_subid = GetCurrentSubTransactionId();
		fasttrun_fresh_pending = NIL;
		fasttrun_fresh_stmt_subid = stmt_subid;
	}
	PG_TRY();
	{
		fasttrun_utility_hook_body(pstmt, queryString, readOnlyTree, context,
								   params, queryEnv, dest, qc);
	}
	PG_CATCH();
	{
		/* The statement failed: its tables get no mark. */
		if (IsTransactionState() && TopTransactionContext == outer_top_mcxt &&
			fasttrun_xact_epoch == outer_xact_epoch)
		{
			list_free(fasttrun_create_pending);
			fasttrun_create_pending = outer_creates;
			fasttrun_create_stmt_subid = outer_create_subid;
			if (is_truncate)
			{
				list_free(fasttrun_fresh_pending);
				fasttrun_fresh_pending = outer_pending;
				fasttrun_fresh_stmt_subid = outer_stmt_subid;
			}
		}
		else
		{
			fasttrun_create_pending = NIL;
			fasttrun_create_stmt_subid = InvalidSubTransactionId;
			if (is_truncate)
			{
				fasttrun_fresh_pending = NIL;
				fasttrun_fresh_stmt_subid = InvalidSubTransactionId;
			}
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (fasttrun_create_pending != NIL)
		fasttrun_fresh_confirm_creates(create_subid);
	if (IsTransactionState() && TopTransactionContext == outer_top_mcxt &&
		fasttrun_xact_epoch == outer_xact_epoch)
	{
		fasttrun_create_pending = outer_creates;
		fasttrun_create_stmt_subid = outer_create_subid;
	}
	else
	{
		fasttrun_create_pending = NIL;
		fasttrun_create_stmt_subid = InvalidSubTransactionId;
	}
	if (is_truncate)
	{
		Assert(GetCurrentSubTransactionId() == stmt_subid);
		fasttrun_fresh_confirm_pending(stmt_subid);
		fasttrun_fresh_pending = outer_pending;
		fasttrun_fresh_stmt_subid = outer_stmt_subid;
	}
}

