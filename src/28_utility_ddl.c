/*-------------------------------------------------------------------------
 *
 * src/28_utility_ddl.c
 *      DDL effects and handoffs to core commands.
 *
 * What it holds.  Eviction after DDL, the effect of ALTER TABLE and index
 * builds on cached statistics, the managed temporary relations of a utility
 * command and the handoffs around VACUUM, CLUSTER and ANALYZE.
 *
 * Also here.  The poison checks of utility commands.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/28_utility_ddl.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */

/* Only utility paths expand trees. Callers filter ownership before locking. */
static List *
fasttrun_utility_tree(Oid relid, bool recurse)
{
	if (!OidIsValid(relid))
		return NIL;
	return recurse ? find_all_inheritors(relid, NoLock, NULL) :
		list_make1_oid(relid);
}

/*
 * Core expands maintenance targets differently across major versions.
 * ANALYZE also samples descendants for inherited statistics, even when those
 * descendants are not individual maintenance targets (including ONLY in 18).
 */
static List *
fasttrun_vacuum_targets(VacuumRelation *vrel, bool inherited_sample)
{
	Oid			relid = vrel->oid;
	bool		recurse = inherited_sample;

	if (!OidIsValid(relid) && vrel->relation != NULL)
	{
		relid = RangeVarGetRelid(vrel->relation, NoLock, true);
#if PG_VERSION_NUM >= 180000
		recurse |= vrel->relation->inh;
#else
		recurse |= get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE;
#endif
	}
	return fasttrun_utility_tree(relid, recurse);
}

static bool fasttrun_vacuum_option_enabled(VacuumStmt *stmt, const char *name);
static void fasttrun_stats_arm_rewrite_undo(Relation rel);

/* These ALTER variants propagate column or constraint work to descendants. */
static bool
fasttrun_alter_table_recurses(AlterTableStmt *stmt)
{
	ListCell   *lc;

	if (!stmt->relation->inh)
		return false;
	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddColumn:
			case AT_DropColumn:
			case AT_ColumnDefault:
			case AT_DropNotNull:
			case AT_SetNotNull:
			case AT_DropExpression:
#if PG_VERSION_NUM >= 170000
			case AT_SetExpression:
#endif
			case AT_SetStatistics:
			case AT_SetStorage:
			case AT_AddConstraint:
			case AT_DropConstraint:
			case AT_AlterConstraint:
			case AT_ValidateConstraint:
			case AT_AlterColumnType:
			case AT_EnableTrig:
			case AT_EnableAlwaysTrig:
			case AT_EnableReplicaTrig:
			case AT_EnableTrigAll:
			case AT_EnableTrigUser:
			case AT_DisableTrig:
			case AT_DisableTrigAll:
			case AT_DisableTrigUser:
				return true;
			default:
				break;
		}
	}
	return false;
}

static void
fasttrun_evict_missing_relid(Oid relid)
{
	/* Mark before mutating so the commit callback reclaims the undo. */
	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE |
							FASTTRUN_TOUCH_DROPPED);
	fasttrun_cache_mark_evicted(relid);
	fasttrun_stats_cache_mark_evicted_relid(relid);
	fasttrun_invalidate_local_plan_cache(relid);
}

static void
fasttrun_evict_temp_relid(Oid relid)
{
	Relation	rel;
	HeapTuple	tp;
	Form_pg_class relform;
	bool		is_our_temp;

	if (!OidIsValid(relid))
		return;
	if (!fasttrun_manages_relid(relid))
		return;

	/*
	 * Filter with one syscache probe BEFORE taking any lock.  The hook runs
	 * for every DDL target, and waiting on somebody else's lock here would
	 * defeat conditional-locking callers such as VACUUM (SKIP_LOCKED).
	 * Only this backend's own temp relations proceed to the open below,
	 * where the AccessShareLock is uncontended.
	 */
	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
	{
		fasttrun_evict_missing_relid(relid);
		return;
	}
	relform = (Form_pg_class) GETSTRUCT(tp);
	is_our_temp = (relform->relpersistence == RELPERSISTENCE_TEMP &&
				   isTempNamespace(relform->relnamespace));
	ReleaseSysCache(tp);
	if (!is_our_temp)
		return;

	/*
	 * relation_open with NoLock would trip the lock-discipline assert on
	 * assert-enabled builds (the DDL has not taken its own lock yet), so
	 * take a transient AccessShareLock and release it at close.
	 */
	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
	{
		fasttrun_evict_missing_relid(relid);
		return;
	}

	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_STATS |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

		/* Save DDL undo only after observing a committed storage emptying. */
		if (entry != NULL && fasttrun_cache_storage_check_due(entry, relid))
			(void) fasttrun_cache_check_storage(entry, rel);
	}
	fasttrun_stats_arm_rewrite_undo(rel);
	(void) fasttrun_stats_neutralize_relation(rel);

	if (rel->rd_rel->relkind == RELKIND_RELATION ||
		rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *lc;

		foreach(lc, index_oids)
			fasttrun_xact_mark_relid(lfirst_oid(lc), relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
		list_free(index_oids);

		fasttrun_cache_mark_rel_and_indexes_evicted(rel);
	}
	else
		fasttrun_cache_mark_evicted(relid);
	relation_close(rel, AccessShareLock);
	fasttrun_invalidate_local_plan_cache(relid);
}

/*
 * DDL that changes neither the rows nor the meaning of the remaining columns.
 * Core keeps pg_statistic through it, and the column statistics fasttrun holds
 * stay as true as core's.  A subcommand that rewrites the heap anyway (a column
 * added with a volatile default or a stored expression) copies the same rows to
 * a new file, and fasttrun_finish_kept_ddl moves the statistics there.
 */
typedef enum FasttrunDdlEffect
{
	FASTTRUN_DDL_EVICTS,		/* types, access method, inheritance: evict */
	FASTTRUN_DDL_KEEPS_STATS,	/* nothing the statistics rest on changes */
	FASTTRUN_DDL_BUILDS_INDEX	/* keeps column statistics, but the index
								 * build writes fresh relstats to pg_class */
} FasttrunDdlEffect;

/* USING INDEX attaches an existing index and builds nothing. */
static bool
fasttrun_constraint_builds_index(Constraint *con)
{
	return (con->contype == CONSTR_PRIMARY ||
			con->contype == CONSTR_UNIQUE ||
			con->contype == CONSTR_EXCLUSION) &&
		con->indexname == NULL;
}

/*
 * CREATE INDEX IF NOT EXISTS with a name a relation of the schema already has
 * builds nothing: core returns before the build and its relstats.
 */
static bool
fasttrun_index_stmt_builds(IndexStmt *stmt)
{
	Oid			relid;

	if (!stmt->if_not_exists || stmt->idxname == NULL || stmt->relation == NULL)
		return true;
	relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return true;
	return !OidIsValid(get_relname_relid(stmt->idxname,
										 get_rel_namespace(relid)));
}

/*
 * A heap that keeps its statistics through the command: its file and, when
 * the command may build an index, its indexes before the command.
 */
typedef struct FasttrunKeptDdl
{
	Oid			relid;
	RelFileLocatorBackend rlb_before;
	bool		builds_index;
	List	   *indexes_before;
} FasttrunKeptDdl;

static FasttrunDdlEffect
fasttrun_alter_table_effect(AlterTableStmt *stmt)
{
	FasttrunDdlEffect effect = FASTTRUN_DDL_KEEPS_STATS;
	ListCell   *lc;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddIndex:
				effect = FASTTRUN_DDL_BUILDS_INDEX;
				break;
			case AT_AddIndexConstraint:
				/* A constraint over an existing index builds nothing. */
				break;
			case AT_AddConstraint:
				if (cmd->def != NULL && IsA(cmd->def, Constraint) &&
					fasttrun_constraint_builds_index((Constraint *) cmd->def))
					effect = FASTTRUN_DDL_BUILDS_INDEX;
				break;
			case AT_AddColumn:
				/*
				 * A column constraint can build an index too.  Whether IF NOT
				 * EXISTS skips the column depends on what the command drops,
				 * cascades included: fasttrun_finish_kept_ddl checks for a
				 * new index instead.
				 */
				if (cmd->def != NULL && IsA(cmd->def, ColumnDef))
				{
					ColumnDef  *coldef = (ColumnDef *) cmd->def;
					ListCell   *clc;

					foreach(clc, coldef->constraints)
					{
						if (IsA(lfirst(clc), Constraint) &&
							fasttrun_constraint_builds_index((Constraint *) lfirst(clc)))
							effect = FASTTRUN_DDL_BUILDS_INDEX;
					}
				}
				break;
			case AT_DropColumn:
				break;
			case AT_ColumnDefault:
			case AT_DropNotNull:
			case AT_SetNotNull:
			case AT_SetStatistics:
			case AT_SetOptions:
			case AT_ResetOptions:
			case AT_SetStorage:
			case AT_SetCompression:
			case AT_ValidateConstraint:
			case AT_DropConstraint:
			case AT_ClusterOn:
			case AT_DropCluster:
			case AT_SetRelOptions:
			case AT_ResetRelOptions:
			case AT_ReplaceRelOptions:
			case AT_EnableTrig:
			case AT_EnableAlwaysTrig:
			case AT_EnableReplicaTrig:
			case AT_DisableTrig:
			case AT_EnableTrigAll:
			case AT_DisableTrigAll:
			case AT_EnableTrigUser:
			case AT_DisableTrigUser:
			case AT_ChangeOwner:
			case AT_ReplicaIdentity:
			case AT_EnableRowSecurity:
			case AT_DisableRowSecurity:
			case AT_ForceRowSecurity:
			case AT_NoForceRowSecurity:
				break;
			default:
				return FASTTRUN_DDL_EVICTS;
		}
	}
	return effect;
}

/*
 * Keep the column statistics through DDL classified above.  A dropped column
 * loses its own statistics, as core removes its pg_statistic rows.  A trigger
 * enabled or a stored generated column added makes the UPDATE column mask an
 * incomplete account.  The heap goes to *kept with its file, and with its
 * indexes before the command when the command may build an index:
 * fasttrun_finish_kept_ddl moves the samples to the file of a rewrite and
 * takes the exact heap relstats an index build writes to pg_class.
 */
static void
fasttrun_keep_stats_through_ddl(Oid relid, AlterTableStmt *alter,
								bool builds_index, List **kept)
{
	Relation	rel;
	HeapTuple	tp;
	bool		is_our_temp;

	if (!OidIsValid(relid) || !fasttrun_manages_relid(relid))
		return;
	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		return;
	is_our_temp = (((Form_pg_class) GETSTRUCT(tp))->relpersistence == RELPERSISTENCE_TEMP &&
				   isTempNamespace(((Form_pg_class) GETSTRUCT(tp))->relnamespace));
	ReleaseSysCache(tp);
	if (!is_our_temp)
		return;
	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
		return;

	if (alter != NULL)
	{
		ListCell   *lc;
		FasttrunStatsRelidEntry *entry = fasttrun_stats_relid_cache == NULL ? NULL :
			hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);

		if (entry != NULL)
			entry->updated_cols_exact = false;
		foreach(lc, alter->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);
			AttrNumber	attnum;

			if (cmd->subtype != AT_DropColumn || cmd->name == NULL)
				continue;
			attnum = get_attnum(relid, cmd->name);
			if (attnum > 0)
				(void) fasttrun_stats_set_column_state(rel, attnum, false,
													 FASTTRUN_COLUMN_LOCAL_NEUTRAL,
													 NULL, NULL);
		}
	}

	if (rel->rd_rel->relkind == RELKIND_RELATION ||
		rel->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		FasttrunKeptDdl *target = palloc0(sizeof(FasttrunKeptDdl));

		target->relid = relid;
		target->rlb_before.locator = rel->rd_locator;
		target->rlb_before.backend = rel->rd_backend;
		target->builds_index = builds_index;
		if (builds_index)
		{
			/*
			 * Core writes pg_class for the heap and the new index only.  The
			 * other indexes keep their relstats: the catalog still has older
			 * values for them.
			 */
			fasttrun_xact_mark_relid(relid, relid,
									FASTTRUN_TOUCH_ANALYZE |
									FASTTRUN_TOUCH_PLAN_INVALIDATE);
			target->indexes_before = RelationGetIndexList(rel);
			fasttrun_invalidate_local_plan_cache(relid);
		}
		*kept = lappend(*kept, target);
	}
	relation_close(rel, AccessShareLock);
}

static void
fasttrun_evict_rangevar(RangeVar *rv)
{
	Oid		relid;

	if (rv == NULL)
		return;
	relid = RangeVarGetRelid(rv, NoLock, true);
	fasttrun_evict_temp_relid(relid);
}

static void
fasttrun_poison_check_relid(Oid relid, const char *operation)
{
	Oid			root_relid = relid;
	FasttrunPoisonEntry *entry;
	Relation	rel;

	if (fasttrun_poison_cache == NULL || !OidIsValid(relid))
		return;
	entry = fasttrun_poison_find(root_relid);
	if (entry == NULL && get_rel_relkind(relid) == RELKIND_INDEX)
	{
		root_relid = IndexGetRelation(relid, true);
		entry = fasttrun_poison_find(root_relid);
	}
	if (entry == NULL || !entry->active)
		return;
	rel = try_relation_open(root_relid, AccessShareLock);
	if (rel == NULL)
		return;
	fasttrun_poison_check_relation(rel, operation, false);
	relation_close(rel, AccessShareLock);
}

static void
fasttrun_poison_check_rangevar(RangeVar *rv, const char *operation)
{
	Oid			relid;

	if (fasttrun_poison_cache == NULL || rv == NULL)
		return;
	relid = RangeVarGetRelid(rv, NoLock, true);
	fasttrun_poison_check_relid(relid, operation);
}

static void
fasttrun_poison_check_tree(RangeVar *rv, bool recurse, const char *operation)
{
	List	   *relids;
	ListCell   *lc;

	if (rv == NULL)
		return;
	relids = fasttrun_utility_tree(RangeVarGetRelid(rv, NoLock, true), recurse);
	foreach(lc, relids)
		fasttrun_poison_check_relid(lfirst_oid(lc), operation);
	list_free(relids);
}

/*
 * Check every blocked table, not only the first one: a blocked table dropped
 * earlier in this transaction keeps its entry until COMMIT and passes the
 * check, while another may still exist.  The relids are copied out first, so
 * an error does not leave the hash scan open.
 */
static void
fasttrun_poison_check_any(const char *operation)
{
	HASH_SEQ_STATUS status;
	FasttrunPoisonEntry *entry;
	Oid		   *relids;
	int			nrelids = 0;
	int			i;

	if (fasttrun_poison_cache == NULL)
		return;
	relids = palloc(sizeof(Oid) * hash_get_num_entries(fasttrun_poison_cache));
	hash_seq_init(&status, fasttrun_poison_cache);
	while ((entry = (FasttrunPoisonEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->active)
			relids[nrelids++] = entry->root_relid;
	}
	for (i = 0; i < nrelids; i++)
		fasttrun_poison_check_relid(relids[i], operation);
	pfree(relids);
}

/* Reject direct utility access before it can mutate or inspect storage. */
static void
fasttrun_poison_check_utility(Node *parsetree)
{
	ListCell   *lc;

	if (fasttrun_poison_cache == NULL || parsetree == NULL)
		return;
	switch (nodeTag(parsetree))
	{
		case T_CopyStmt:
			fasttrun_poison_check_rangevar(((CopyStmt *) parsetree)->relation,
										 "COPY");
			break;
		case T_TruncateStmt:
			foreach(lc, ((TruncateStmt *) parsetree)->relations)
				fasttrun_poison_check_rangevar((RangeVar *) lfirst(lc),
											 "SQL TRUNCATE");
			break;
		case T_VacuumStmt:
			if (((VacuumStmt *) parsetree)->rels == NIL)
				fasttrun_poison_check_any("VACUUM/ANALYZE");
			else
			{
				foreach(lc, ((VacuumStmt *) parsetree)->rels)
				{
					VacuumStmt *stmt = (VacuumStmt *) parsetree;
					VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);
					List	   *relids = fasttrun_vacuum_targets(vrel,
						!stmt->is_vacuumcmd || fasttrun_vacuum_option_enabled(stmt, "analyze"));
					ListCell   *rlc;

					foreach(rlc, relids)
						fasttrun_poison_check_relid(lfirst_oid(rlc), "VACUUM/ANALYZE");
					list_free(relids);
				}
			}
			break;
		case T_ClusterStmt:
			if (((ClusterStmt *) parsetree)->relation == NULL)
				fasttrun_poison_check_any("CLUSTER");
			else
			{
				RangeVar *rv = ((ClusterStmt *) parsetree)->relation;

				fasttrun_poison_check_tree(rv,
					get_rel_relkind(RangeVarGetRelid(rv, NoLock, true)) ==
					RELKIND_PARTITIONED_TABLE, "CLUSTER");
			}
			break;
		case T_AlterTableStmt:
			{
				AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

				fasttrun_poison_check_tree(stmt->relation,
					fasttrun_alter_table_recurses(stmt), "ALTER TABLE");
				foreach(lc, stmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

					if (cmd->subtype == AT_AttachPartition)
					{
						Oid			parent = RangeVarGetRelid(stmt->relation, NoLock, true);
						List	   *defaults = fasttrun_utility_tree(
							get_default_partition_oid(parent), true);
						ListCell   *dlc;

						fasttrun_poison_check_tree(((PartitionCmd *) cmd->def)->name,
							true, "ATTACH PARTITION");
						/* Core can scan the default partition to validate new bounds. */
						foreach(dlc, defaults)
							fasttrun_poison_check_relid(lfirst_oid(dlc), "ATTACH PARTITION");
						list_free(defaults);
					}
				}
			}
			break;
		case T_IndexStmt:
			{
				RangeVar *rv = ((IndexStmt *) parsetree)->relation;

				fasttrun_poison_check_tree(rv, rv->inh &&
					get_rel_relkind(RangeVarGetRelid(rv, NoLock, true)) ==
					RELKIND_PARTITIONED_TABLE, "CREATE INDEX");
			}
			break;
		case T_ReindexStmt:
			{
				ReindexStmt *stmt = (ReindexStmt *) parsetree;

				if (stmt->relation != NULL)
				{
					RangeVar *rv = stmt->relation;
					char kind = get_rel_relkind(RangeVarGetRelid(rv, NoLock, true));

					fasttrun_poison_check_tree(rv,
						kind == RELKIND_PARTITIONED_TABLE || kind == RELKIND_PARTITIONED_INDEX,
						"REINDEX");
				}
				else if (stmt->kind == REINDEX_OBJECT_DATABASE)
					fasttrun_poison_check_any("REINDEX");
				else if (stmt->kind == REINDEX_OBJECT_SCHEMA)
				{
					Oid nspid = get_namespace_oid(stmt->name, true);

					/* Other schemas and system catalogs cannot contain our heap. */
					if (OidIsValid(nspid) && isTempNamespace(nspid))
						fasttrun_poison_check_any("REINDEX");
				}
			}
			break;
		default:
			break;
	}
}

typedef struct FasttrunOidSetEntry
{
	Oid			relid;			/* hash key -- must be first */
} FasttrunOidSetEntry;

static bool
fasttrun_manages_relid(Oid relid)
{
	return fasttrun_cache_lookup(relid) != NULL ||
		fasttrun_stats_relid_exists(relid);
}

/*
 * Snapshot the union of analyze-cache owning heaps and stats-cache relids.
 * ANALYZE/CLUSTER without an explicit relation list must not scan unrelated
 * catalogs just to discover which tables this backend manages.
 */
static List *
fasttrun_managed_temp_relids(void)
{
	HASHCTL		ctl;
	HTAB	   *set;
	HASH_SEQ_STATUS status;
	FasttrunOidSetEntry *setentry;
	List	   *relids = NIL;
	long		initial_size = 16;

	if (fasttrun_analyze_cache != NULL)
		initial_size += hash_get_num_entries(fasttrun_analyze_cache);
	if (fasttrun_stats_relid_cache != NULL)
		initial_size += hash_get_num_entries(fasttrun_stats_relid_cache);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FasttrunOidSetEntry);
	ctl.hcxt = CurrentMemoryContext;
	set = hash_create("fasttrun utility managed relids", initial_size,
					  &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	if (fasttrun_stats_relid_cache != NULL)
	{
		FasttrunStatsRelidEntry *entry;

		hash_seq_init(&status, fasttrun_stats_relid_cache);
		while ((entry = (FasttrunStatsRelidEntry *)
				hash_seq_search(&status)) != NULL)
			(void) hash_search(set, &entry->relid, HASH_ENTER, NULL);
	}

	if (fasttrun_analyze_cache != NULL)
	{
		FasttrunAnalyzeCacheEntry *entry;

		hash_seq_init(&status, fasttrun_analyze_cache);
		while ((entry = (FasttrunAnalyzeCacheEntry *)
				hash_seq_search(&status)) != NULL)
		{
			Oid			root_relid = OidIsValid(entry->state.heap_relid) ?
				entry->state.heap_relid : entry->relid;

			(void) hash_search(set, &root_relid, HASH_ENTER, NULL);
		}
	}

	hash_seq_init(&status, set);
	while ((setentry = (FasttrunOidSetEntry *) hash_seq_search(&status)) != NULL)
		relids = lappend_oid(relids, setentry->relid);
	hash_destroy(set);
	return relids;
}

static Relation
fasttrun_try_open_managed_heap(Oid relid)
{
	Relation	rel;

	if (!OidIsValid(relid) || !fasttrun_manages_relid(relid) ||
		!fasttrun_relid_is_our_temp(relid))
		return NULL;
	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
		return NULL;
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)) ||
		rel->rd_tableam != GetHeapamTableAmRoutine())
	{
		relation_close(rel, AccessShareLock);
		return NULL;
	}
	return rel;
}

/* A repeated option takes its last value, as core reads them. */
static bool
fasttrun_vacuum_option_value(VacuumStmt *stmt, const char *name,
							 bool default_value)
{
	bool		value = default_value;
	ListCell   *lc;

	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, name) == 0)
			value = defGetBoolean(opt);
	}
	return value;
}

static bool
fasttrun_vacuum_option_enabled(VacuumStmt *stmt, const char *name)
{
	return fasttrun_vacuum_option_value(stmt, name, false);
}

/* An option that is on unless the statement turns it off. */
static bool
fasttrun_vacuum_option_default_on(VacuumStmt *stmt, const char *name)
{
	return fasttrun_vacuum_option_value(stmt, name, true);
}

static bool
fasttrun_attribute_target_is_analyzable(Relation rel, AttrNumber attnum)
{
	Form_pg_attribute attr;

	if (attnum <= 0 || attnum > RelationGetDescr(rel)->natts)
		return false;
	attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	if (!fasttrun_attribute_is_analyzable(attr))
		return false;
	/*
	 * Core ANALYZE reads the temporary table's own target only, so the
	 * handoff must too: a template target of 0 would keep a column core
	 * collects away from core.
	 */
	return fasttrun_read_attstattarget(RelationGetRelid(rel), attnum) != 0;
}

/*
 * A non-NULL bitmap denotes selected columns.  Member zero distinguishes a
 * valid list without analyzable columns from a request for all columns.  An
 * invalid name returns NULL deliberately: core will report the error, and if
 * VACUUM already committed an earlier table, its statistics remain hidden.
 */
static Bitmapset *
fasttrun_analyze_target_attnums(Relation rel, List *va_cols)
{
	Bitmapset  *attnums;
	ListCell   *lc;

	if (va_cols == NIL)
		return NULL;
	attnums = bms_make_singleton(0);
	foreach(lc, va_cols)
	{
		const char *attname = strVal(lfirst(lc));
		AttrNumber	attnum = get_attnum(RelationGetRelid(rel), attname);

		if (attnum <= 0 || attnum > RelationGetDescr(rel)->natts)
		{
			bms_free(attnums);
			return NULL;
		}
		if (fasttrun_attribute_target_is_analyzable(rel, attnum))
			attnums = bms_add_member(attnums, attnum);
	}
	return attnums;
}

static void
fasttrun_stats_neutralize_target(Relation rel, Bitmapset *attnums)
{
	int			attnum = -1;

	if (attnums == NULL)
	{
		(void) fasttrun_stats_neutralize_relation(rel);
		return;
	}
	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
	{
		if (attnum == 0)
			continue;
		(void) fasttrun_stats_set_column_state(rel, attnum, false,
										 FASTTRUN_COLUMN_LOCAL_NEUTRAL,
										 NULL, NULL);
	}
}

/* Relstats written by core ANALYZE/rewrite supersede the session snapshot. */
static void
fasttrun_prepare_relstats_handoff(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	List	   *index_oids;
	ListCell   *lc;

	fasttrun_xact_mark_relid(relid, relid,
							FASTTRUN_TOUCH_ANALYZE |
							FASTTRUN_TOUCH_PLAN_INVALIDATE);
	index_oids = RelationGetIndexList(rel);
	foreach(lc, index_oids)
		fasttrun_xact_mark_relid(lfirst_oid(lc), relid,
								FASTTRUN_TOUCH_ANALYZE |
								FASTTRUN_TOUCH_PLAN_INVALIDATE);
	list_free(index_oids);
	fasttrun_cache_mark_rel_and_indexes_evicted(rel);
}

/* Capture relation state before a rewrite changes the locator. */
static void
fasttrun_stats_arm_rewrite_undo(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL)
		return;
	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (entry == NULL || entry->state_subid == GetCurrentSubTransactionId())
		return;
	fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_STATS);
	fasttrun_stats_relid_save_undo(entry);
}

static void
fasttrun_prepare_relation_handoff(Relation rel, Bitmapset *attnums,
								  bool do_analyze, bool do_rewrite,
								  List **analyze_targets,
								  List **rewrite_relids)
{
	Oid			relid = RelationGetRelid(rel);

	Assert(!do_analyze || analyze_targets != NULL);
	Assert(rewrite_relids != NULL);

	/*
	 * Core skips a table the caller may not vacuum or analyze, with a warning,
	 * and computes nothing for it: the local statistics stay in place.
	 */
	if (!fasttrun_may_core_analyze(rel))
		return;

	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

		/* Observe committed emptying before core handoff saves local undo. */
		if (entry != NULL && fasttrun_cache_storage_check_due(entry, relid))
			(void) fasttrun_cache_check_storage(entry, rel);
	}
	if (do_rewrite)
	{
		fasttrun_stats_arm_rewrite_undo(rel);
		(void) fasttrun_stats_neutralize_relation(rel);
	}
	else if (do_analyze)
		fasttrun_stats_neutralize_target(rel, attnums);
	fasttrun_prepare_relstats_handoff(rel);

	if (do_analyze)
	{
		FasttrunAnalyzeHandoffTarget *target;

		target = (FasttrunAnalyzeHandoffTarget *) palloc(sizeof(*target));
		target->relid = relid;
		target->attnums = attnums;
		target->ext_before = NIL;
		{
			FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);

			if (entry != NULL && entry->state.ext_stats_stale)
				target->ext_before = fasttrun_ext_data_versions(rel);
		}
		*analyze_targets = lappend(*analyze_targets, target);
	}
	if (do_rewrite && !list_member_oid(*rewrite_relids, relid))
		*rewrite_relids = lappend_oid(*rewrite_relids, relid);
}

/*
 * VACUUM removes dead rows and may cut the empty tail of the heap without
 * moving the change counters.  The row count of the delta state stays right,
 * but its page and all-visible figures would outlive the command, and a no-op
 * analyze publishes them as they are.  Drop the delta state of every target,
 * so the next analyze takes a fresh pass.  VACUUM cannot run in a transaction
 * block, so no rollback needs the old state back.  The targets are returned
 * for fasttrun_finish_vacuum.
 */
static List *
fasttrun_vacuum_drop_delta(VacuumStmt *stmt)
{
	List	   *relids;
	List	   *permitted = NIL;
	ListCell   *lc;

	if (fasttrun_analyze_cache == NULL)
		return NIL;
	if (stmt->rels == NIL)
		relids = fasttrun_managed_temp_relids();
	else
	{
		relids = NIL;
		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);

			relids = list_concat(relids, fasttrun_vacuum_targets(vrel, false));
		}
	}
	foreach(lc, relids)
	{
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(lfirst_oid(lc));

		if (entry != NULL)
			entry->state.has_delta_state = false;
	}

	/*
	 * Core skips a relation the caller may not vacuum, and a VACUUM that does
	 * not process the heap counts nothing either.
	 */
	if (!fasttrun_vacuum_option_enabled(stmt, "only_database_stats") &&
		fasttrun_vacuum_option_default_on(stmt, "process_main"))
	{
		foreach(lc, relids)
		{
			Oid			relid = lfirst_oid(lc);

			if (fasttrun_cache_lookup(relid) != NULL &&
				fasttrun_relid_is_our_temp(relid) &&
				fasttrun_may_maintain_relid(relid, false))
				permitted = lappend_oid(permitted, relid);
		}
	}
	list_free(relids);
	return permitted;
}

/*
 * VACUUM removes dead rows, may cut the empty tail of the heap and sets
 * visibility map bits, all without counters, and writes the page count, the
 * live row count and the all-visible count it found to pg_class.  That pair is
 * consistent with the file, while the session snapshot is not: the pages VACUUM
 * cut off would shrink every estimate, and rows committed since the last
 * analyze would be lost from it.  So the snapshot takes the pg_class values,
 * marked as an estimate (VACUUM extrapolates over the pages it skipped), and
 * the size backstop of the column statistics moves to the new size: the rows
 * are the same, the cut is not a refill.
 */
static void
fasttrun_finish_vacuum(List *relids)
{
	ListCell   *lc;

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);
		Relation	rel;
		HeapTuple	tp;

		if (entry == NULL)
			continue;
		rel = fasttrun_try_open_managed_heap(relid);
		if (rel == NULL)
			continue;
		if (!entry->state.has_relstats)
		{
			/*
			 * Nothing to adopt, but VACUUM removed the rows of rolled-back
			 * writes and may have cut the empty tail: the count and the size
			 * anchor of the column samples start over, on the current file.
			 */
			fasttrun_cache_follow_file(entry, rel);
			if (fasttrun_cache_entry_on_file(entry, rel))
			{
				entry->aborted_pages = 0;
				fasttrun_stats_relid_set_size_anchor(relid,
													 RelationGetNumberOfBlocks(rel));
			}
			relation_close(rel, AccessShareLock);
			fasttrun_cache_reclaim_entry(relid);
			continue;
		}
		tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (HeapTupleIsValid(tp))
		{
			Form_pg_class form = (Form_pg_class) GETSTRUCT(tp);

			if (fasttrun_relation_has_same_locator(rel, entry) &&
				form->reltuples >= 0)
			{
				entry->state.cached_pages = (BlockNumber) form->relpages;
				entry->state.cached_tuples = (int64) form->reltuples;
				entry->state.cached_allvisible = (BlockNumber) form->relallvisible;
				entry->state.count_exact = false;
				entry->aborted_pages = 0;
				entry->state.published_dead = 0;
				entry->state.published_aborted_writes = entry->aborted_writes;
				entry->lazy_check_subid = InvalidSubTransactionId;
				entry->lazy_check_epoch = 0;
				entry->lazy_check_pages = 0;
				fasttrun_stats_relid_set_size_anchor(relid,
													 RelationGetNumberOfBlocks(rel));
			}
			ReleaseSysCache(tp);
		}
		relation_close(rel, AccessShareLock);
	}
}

static void
fasttrun_prepare_vacuum_handoff(VacuumStmt *stmt,
								List **analyze_targets,
								List **rewrite_relids,
								List **vacuum_relids)
{
	bool		do_analyze = !stmt->is_vacuumcmd ||
		fasttrun_vacuum_option_enabled(stmt, "analyze");
	bool		do_rewrite = stmt->is_vacuumcmd &&
		fasttrun_vacuum_option_enabled(stmt, "full");
	ListCell   *lc;

	if (stmt->is_vacuumcmd)
		*vacuum_relids = fasttrun_vacuum_drop_delta(stmt);

	/* Plain VACUUM changes neither distribution ownership nor statistics. */
	if (!do_analyze && !do_rewrite)
		return;

	/*
	 * Core VACUUM may commit between tables.  Hide local statistics for every
	 * target before calling core, and allow core statistics only after the
	 * whole command succeeds.  If a later table fails, statistics for earlier
	 * tables stay hidden instead of mixing old and partially refreshed values.
	 */

	if (stmt->rels == NIL)
	{
		List	   *relids = fasttrun_managed_temp_relids();

		foreach(lc, relids)
		{
			Oid			relid = lfirst_oid(lc);
			Relation	rel = fasttrun_try_open_managed_heap(relid);

			if (rel == NULL)
				continue;
			fasttrun_prepare_relation_handoff(rel, NULL, do_analyze,
										  do_rewrite, analyze_targets,
										  rewrite_relids);
			relation_close(rel, AccessShareLock);
		}
		list_free(relids);
		return;
	}

	foreach(lc, stmt->rels)
	{
		VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);
		List	   *relids = fasttrun_vacuum_targets(vrel, false);
		ListCell   *rlc;

		foreach(rlc, relids)
		{
			Oid			relid = lfirst_oid(rlc);
			Relation	rel = fasttrun_try_open_managed_heap(relid);
			Bitmapset  *attnums = NULL;

			if (rel == NULL)
				continue;
			if (do_analyze)
				attnums = fasttrun_analyze_target_attnums(rel, vrel->va_cols);
			fasttrun_prepare_relation_handoff(rel, attnums, do_analyze,
										  do_rewrite, analyze_targets,
										  rewrite_relids);
			relation_close(rel, AccessShareLock);
		}
		list_free(relids);
	}
}

static bool
fasttrun_relation_has_clustered_index(Relation rel)
{
	List	   *index_oids = RelationGetIndexList(rel);
	ListCell   *lc;
	bool		has_clustered = false;

	foreach(lc, index_oids)
	{
		if (get_index_isclustered(lfirst_oid(lc)))
		{
			has_clustered = true;
			break;
		}
	}
	list_free(index_oids);
	return has_clustered;
}

static void
fasttrun_prepare_cluster_handoff(ClusterStmt *stmt, List **rewrite_relids)
{
	List	   *relids;
	ListCell   *lc;
	bool		all_relations = stmt->relation == NULL;

	if (all_relations)
		relids = fasttrun_managed_temp_relids();
	else
	{
		Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

		relids = fasttrun_utility_tree(relid,
			get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE);
	}

	foreach(lc, relids)
	{
		Relation	rel = fasttrun_try_open_managed_heap(lfirst_oid(lc));

		if (rel == NULL)
			continue;
		if (!all_relations || fasttrun_relation_has_clustered_index(rel))
			fasttrun_prepare_relation_handoff(rel, NULL, false, true,
										  NULL, rewrite_relids);
		relation_close(rel, AccessShareLock);
	}
	list_free(relids);
}

static void
fasttrun_remember_managed_rebind(RangeVar *relation, bool recurse,
								 List **rewrite_relids)
{
	Oid			relid;
	List	   *relids;
	ListCell   *lc;

	if (relation == NULL)
		return;
	relid = RangeVarGetRelid(relation, NoLock, true);
	relids = fasttrun_utility_tree(relid, recurse);
	foreach(lc, relids)
	{
		relid = lfirst_oid(lc);
		if (fasttrun_manages_relid(relid) &&
			!list_member_oid(*rewrite_relids, relid))
			*rewrite_relids = lappend_oid(*rewrite_relids, relid);
	}
	list_free(relids);
}

static void
fasttrun_prepare_utility_handoff(Node *parsetree,
								 List **analyze_targets,
								 List **rewrite_relids,
								 List **vacuum_relids)
{
	if (parsetree == NULL ||
		(fasttrun_analyze_cache == NULL && fasttrun_stats_cache == NULL))
		return;
	if (IsA(parsetree, VacuumStmt))
		fasttrun_prepare_vacuum_handoff((VacuumStmt *) parsetree,
									  analyze_targets, rewrite_relids,
									  vacuum_relids);
	else if (IsA(parsetree, ClusterStmt))
		fasttrun_prepare_cluster_handoff((ClusterStmt *) parsetree,
									  rewrite_relids);
	else if (IsA(parsetree, AlterTableStmt) &&
			 fasttrun_alter_table_effect((AlterTableStmt *) parsetree) ==
			 FASTTRUN_DDL_EVICTS)
		fasttrun_remember_managed_rebind(
			((AlterTableStmt *) parsetree)->relation,
			fasttrun_alter_table_recurses((AlterTableStmt *) parsetree), rewrite_relids);
	else if (IsA(parsetree, TruncateStmt))
	{
		ListCell   *lc;

		foreach(lc, ((TruncateStmt *) parsetree)->relations)
			fasttrun_remember_managed_rebind((RangeVar *) lfirst(lc),
										 ((RangeVar *) lfirst(lc))->inh,
										 rewrite_relids);
	}
}

/*
 * A heap rewrite changes relfilenode.  Rebind the current neutral state to
 * the new locator without losing the undo nodes that restore the old locator
 * on ROLLBACK TO SAVEPOINT.  Non-rewriting DDL targets share this post-step.
 * For them the locator assignment is an intentional no-op.
 */
static void
fasttrun_finish_rewrite_handoff(List *rewrite_relids)
{
	ListCell   *lc;

	foreach(lc, rewrite_relids)
	{
		Relation	rel = fasttrun_try_open_managed_heap(lfirst_oid(lc));
		FasttrunStatsRelidEntry *relentry;
		ListCell   *klc;
		Oid			relid;

		if (rel == NULL)
			continue;
		relid = RelationGetRelid(rel);
		(void) fasttrun_stats_neutralize_relation(rel);
		relentry = (FasttrunStatsRelidEntry *)
			hash_search(fasttrun_stats_relid_cache,
						&relid, HASH_FIND, NULL);
		if (relentry != NULL)
		{
			foreach(klc, relentry->attkeys)
			{
				FasttrunStatsKey *key = (FasttrunStatsKey *) lfirst(klc);
				FasttrunStatsEntry *entry;

				entry = (FasttrunStatsEntry *)
					hash_search(fasttrun_stats_cache, key, HASH_FIND, NULL);
				if (entry == NULL)
					continue;
				entry->heap_rlb.locator = rel->rd_locator;
				entry->heap_rlb.backend = rel->rd_backend;
				entry->heap_rlb_valid = true;
			}
		}
		relation_close(rel, AccessShareLock);
	}
}

static void
fasttrun_finish_analyze_handoff(List *analyze_targets)
{
	ListCell   *lc;

	foreach(lc, analyze_targets)
	{
		FasttrunAnalyzeHandoffTarget *target =
			(FasttrunAnalyzeHandoffTarget *) lfirst(lc);
		Relation	rel = fasttrun_try_open_managed_heap(target->relid);

		if (rel == NULL)
			continue;
		fasttrun_stats_handoff_columns(rel, target->attnums,
									 target->attnums == NULL);
		/* The flag goes once core ANALYZE rewrote every object's data. */
		{
			FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(target->relid);

			if (entry != NULL && entry->state.ext_stats_stale &&
				fasttrun_ext_data_rebuilt(rel, target->ext_before))
			{
				/* VACUUM may have started a new transaction since. */
				fasttrun_xact_mark_relid(target->relid, target->relid,
										 FASTTRUN_TOUCH_ANALYZE);
				fasttrun_analyze_save_undo(entry);
				entry->state.ext_stats_stale = false;
			}
		}
		relation_close(rel, AccessShareLock);
	}
}

static void
fasttrun_free_analyze_handoff(List *analyze_targets)
{
	ListCell   *lc;

	foreach(lc, analyze_targets)
	{
		FasttrunAnalyzeHandoffTarget *target =
			(FasttrunAnalyzeHandoffTarget *) lfirst(lc);

		if (target->attnums != NULL)
			bms_free(target->attnums);
		list_free_deep(target->ext_before);
		pfree(target);
	}
	list_free(analyze_targets);
}

/*
 * DDL that kept the statistics, after the command.
 *
 * Rewrite.  A column added with a stored generated expression or a volatile
 * default rewrites the heap with the same rows.  Core keeps pg_statistic
 * through it, so the samples move to the new file instead of failing the
 * locator check.
 *
 * Index build.  It writes exact heap relstats to pg_class: the live tuples it
 * indexed and the file size, rows of rolled-back writes included.  The cache
 * takes that pair as its publication, as after VACUUM, so the entry keeps its
 * relstats and the relstats of the other indexes still reach the planner.  The
 * delta state is dropped: the next analyze takes a fresh pass.
 */
static void
fasttrun_finish_kept_ddl(List *kept)
{
	ListCell   *lc;

	foreach(lc, kept)
	{
		FasttrunKeptDdl *target = (FasttrunKeptDdl *) lfirst(lc);
		Oid			relid = target->relid;
		FasttrunAnalyzeCacheEntry *entry;
		Relation	rel;
		HeapTuple	tp;
		List	   *indexes_now;
		ListCell   *ilc;
		bool		built = false;

		rel = fasttrun_try_open_managed_heap(relid);
		if (rel == NULL)
			continue;
		if (!RelFileLocatorEquals(rel->rd_locator, target->rlb_before.locator))
			fasttrun_stats_rebind_rewritten(rel, &target->rlb_before);

		entry = fasttrun_cache_lookup(relid);
		if (!target->builds_index || entry == NULL || !entry->state.has_relstats)
		{
			relation_close(rel, AccessShareLock);
			continue;
		}

		/* Core wrote the pair only if the command added an index. */
		indexes_now = RelationGetIndexList(rel);
		foreach(ilc, indexes_now)
		{
			if (!list_member_oid(target->indexes_before, lfirst_oid(ilc)))
				built = true;
		}
		list_free(indexes_now);
		if (!built)
		{
			relation_close(rel, AccessShareLock);
			continue;
		}
		tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (HeapTupleIsValid(tp))
		{
			Form_pg_class form = (Form_pg_class) GETSTRUCT(tp);

			if (fasttrun_relation_has_same_locator(rel, entry) &&
				form->reltuples >= 0)
			{
				fasttrun_analyze_save_undo(entry);
				entry->state.cached_pages = (BlockNumber) form->relpages;
				entry->state.cached_tuples = (int64) form->reltuples;
				entry->state.cached_allvisible = (BlockNumber) form->relallvisible;
				entry->state.count_exact = false;
				entry->state.published_dead = entry->aborted_pages;
				entry->state.published_aborted_writes = entry->aborted_writes;
				entry->state.has_delta_state = false;
				/* The plans were reset before the build. */
				entry->state.last_inval_pages = entry->state.cached_pages;
				entry->state.last_inval_tuples = (float4) entry->state.cached_tuples;
				entry->state.last_inval_valid = true;
				entry->lazy_check_subid = InvalidSubTransactionId;
				entry->lazy_check_epoch = 0;
				entry->lazy_check_pages = 0;
			}
			ReleaseSysCache(tp);
		}
		relation_close(rel, AccessShareLock);
	}
}

/*
 * The heaps whose CREATE STATISTICS objects the command may drop: DROP
 * STATISTICS, and ALTER TABLE, whose DROP COLUMN drops the objects over the
 * column.
 */
static List *
fasttrun_statistics_drop_relids(Node *parsetree)
{
	DropStmt   *stmt;
	List	   *relids = NIL;
	ListCell   *lc;

	if (fasttrun_analyze_cache == NULL || parsetree == NULL)
		return NIL;
	if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *alter = (AlterTableStmt *) parsetree;
		Oid			relid = alter->relation == NULL ? InvalidOid :
			RangeVarGetRelid(alter->relation, NoLock, true);

		return fasttrun_utility_tree(relid, fasttrun_alter_table_recurses(alter));
	}
	if (!IsA(parsetree, DropStmt))
		return NIL;
	stmt = (DropStmt *) parsetree;
	if (stmt->removeType != OBJECT_STATISTIC_EXT)
		return NIL;
	foreach(lc, stmt->objects)
	{
		Oid			stxoid = get_statistics_object_oid((List *) lfirst(lc), true);
		HeapTuple	tup;

		if (!OidIsValid(stxoid))
			continue;
		tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stxoid));
		if (HeapTupleIsValid(tup))
		{
			relids = list_append_unique_oid(relids,
											((Form_pg_statistic_ext) GETSTRUCT(tup))->stxrelid);
			ReleaseSysCache(tup);
		}
	}
	return relids;
}

/* Once no CREATE STATISTICS object is left, the flag hides nothing. */
static void
fasttrun_finish_statistics_drop(List *relids)
{
	ListCell   *lc;

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		FasttrunAnalyzeCacheEntry *entry = fasttrun_cache_lookup(relid);
		Relation	rel;

		if (entry == NULL || !entry->state.ext_stats_stale)
			continue;
		rel = fasttrun_try_open_managed_heap(relid);
		if (rel == NULL)
			continue;
		if (!fasttrun_relation_has_extended_stats(rel))
		{
			fasttrun_xact_mark_relid(relid, relid, FASTTRUN_TOUCH_ANALYZE);
			fasttrun_analyze_save_undo(entry);
			entry->state.ext_stats_stale = false;
		}
		relation_close(rel, AccessShareLock);
	}
}

/* Observe explicit DDL targets before a later rollback clears core markers. */
static void
fasttrun_note_utility_file_births(Node *parsetree)
{
	RangeVar   *target = NULL;
	bool		recurse = false;
	Oid			relid;
	List	   *relids;
	ListCell   *lc;

	if (fasttrun_passthrough || parsetree == NULL)
		return;
	if (IsA(parsetree, AlterTableStmt))
	{
		target = ((AlterTableStmt *) parsetree)->relation;
		recurse = fasttrun_alter_table_recurses((AlterTableStmt *) parsetree);
	}
	else if (IsA(parsetree, ClusterStmt))
		target = ((ClusterStmt *) parsetree)->relation;
	else if (IsA(parsetree, ReindexStmt))
		target = ((ReindexStmt *) parsetree)->relation;
	else if (IsA(parsetree, IndexStmt))
		target = ((IndexStmt *) parsetree)->relation;
	if (target == NULL)
		return;
	relid = RangeVarGetRelid(target, NoLock, true);
	if (!OidIsValid(relid))
		return;
	if (!IsA(parsetree, AlterTableStmt))
	{
		char kind = get_rel_relkind(relid);

		recurse = kind == RELKIND_PARTITIONED_TABLE || kind == RELKIND_PARTITIONED_INDEX;
		if (IsA(parsetree, IndexStmt))
			recurse &= target->inh;
	}
	relids = fasttrun_utility_tree(relid, recurse);
	foreach(lc, relids)
	{
		Relation	rel;

		relid = lfirst_oid(lc);
		if (get_rel_relkind(relid) == RELKIND_INDEX)
			relid = IndexGetRelation(relid, true);
		if (!fasttrun_relid_is_our_temp(relid))
			continue;
		rel = try_relation_open(relid, AccessShareLock);
		if (rel == NULL)
			continue;
		if (isTempNamespace(RelationGetNamespace(rel)) &&
			(rel->rd_rel->relkind == RELKIND_RELATION ||
			 rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
			fasttrun_note_workset_birth(rel);
		relation_close(rel, AccessShareLock);
	}
	list_free(relids);
}

