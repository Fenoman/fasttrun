/*-------------------------------------------------------------------------
 *
 * src/14_planner.c
 *      Planner hooks.
 *
 * What it holds.  The relation stats, attribute width and index stats hooks,
 * the walk over sublinks and subqueries, get_relation_info_hook with the
 * partial index estimates, and planner_hook.
 *
 * Also here.  The poison checks of the parse tree and of the finished plan,
 * which run inside the planner hook.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/14_planner.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Substitute our cached tuple while it stays within churn tolerance (see
 * fasttrun_stats_entry_usable).  Past the threshold we fall through so the
 * planner uses defaults instead of a distribution that has shifted too far.
 */
static bool
fasttrun_get_relation_stats_hook(PlannerInfo *root, RangeTblEntry *rte,
								 AttrNumber attnum, VariableStatData *vardata)
{
	FasttrunStatsKey key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry;
	int64			ins_now = 0;
	int64			upd_now = 0;
	int64			del_now = 0;
	bool			truncdropped_now = false;
	BlockNumber		pages_now = 0;
	bool			wide_writers = false;

	/* Some core callers do not initialize these output fields. */
	vardata->statsTuple = NULL;
	vardata->freefunc = NULL;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		goto chain;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &rte->relid, HASH_FIND, NULL);
	if (relentry == NULL ||
		!fasttrun_stats_relid_locator_valid(rte->relid, relentry))
		goto chain;

	fasttrun_stats_key_init(&key, rte->relid, attnum, rte->inh);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry != NULL && entry->state == FASTTRUN_COLUMN_CORE_ALLOWED)
		goto chain;
	if (entry == NULL)
	{
		if (relentry->policy == FASTTRUN_REL_LOCAL_NEUTRAL)
			return true;
		goto chain;
	}
	if (entry->state == FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
		entry->statsTuple == NULL)
	{
		fasttrun_count(FTC_STATS_HIDDEN);
		return true;
	}

	/* Freshness check: tolerate DML churn up to stats_refresh_threshold. */
	if (!fasttrun_read_pgstat_counters_for_hook(rte->relid, &wide_writers,
											   &ins_now, &upd_now,
												&del_now, &truncdropped_now,
												&pages_now))
		return true;

	/*
	 * Re-read per plan: a trigger or a stored generated column created after
	 * the last collect makes the UPDATE mask an incomplete account.  The flag
	 * only ever clears here and is re-established by the next collect.
	 */
	if (wide_writers)
		relentry->updated_cols_exact = false;

	if (!fasttrun_stats_entry_usable(rte->relid, relentry, entry, ins_now, upd_now,
									 del_now, truncdropped_now, pages_now))
	{
		fasttrun_count(FTC_STATS_HIDDEN);
		fasttrun_xact_mark_dml_plan_dependency(rte->relid);
		return true;		/* handled: deliberately hide core stats too */
	}

	fasttrun_count(FTC_STATS_SERVED);
	vardata->statsTuple = entry->statsTuple;
	vardata->freefunc = fasttrun_stats_noop_free;
	vardata->acl_ok = true;
	return true;

chain:
	if (prev_get_relation_stats_hook)
		return prev_get_relation_stats_hook(root, rte, attnum, vardata);
	return false;
}

static int32
fasttrun_type_default_width(Oid relid, AttrNumber attnum)
{
	HeapTuple	atttuple;
	Form_pg_attribute attr;
	int32		width;

	atttuple = SearchSysCache2(ATTNUM,
							 ObjectIdGetDatum(relid),
							 Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		return 1;
	attr = (Form_pg_attribute) GETSTRUCT(atttuple);
	width = get_typavgwidth(attr->atttypid, attr->atttypmod);
	ReleaseSysCache(atttuple);
	return Max(width, 1);
}

/*
 * Companion to fasttrun_get_relation_stats_hook for the planner's
 * separate average-width path (lsyscache.c:get_attavgwidth).
 *
 * Without this hook the planner reads stawidth straight from the
 * pg_statistic syscache, which we never write to -- so for temp tables
 * it falls back to get_typavgwidth() and uses the type-default width.
 * For varlena types like text that default is 32 bytes regardless of
 * the real sample, which inflates hash-table / sort / spool costing
 * on temp tables with short text columns.
 *
 * Same ownership contract as the relation-stats hook.  CORE_ALLOWED chains.
 * Neutral, stale, and zero-width local states return the type default here,
 * because returning 0 would make lsyscache read stale pg_statistic.stawidth.
 */
static int32
fasttrun_get_attavgwidth_hook(Oid relid, AttrNumber attnum)
{
	FasttrunStatsKey	key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry;
	int64				ins_now = 0;
	int64				upd_now = 0;
	int64				del_now = 0;
	bool				truncdropped_now = false;
	BlockNumber			pages_now = 0;
	bool				wide_writers = false;
	int32				stawidth;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		goto chain;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL ||
		!fasttrun_stats_relid_locator_valid(relid, relentry))
		goto chain;

	/* matches what fasttrun_stats_cache_store writes */
	fasttrun_stats_key_init(&key, relid, attnum, false);

	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache,
											   &key, HASH_FIND, NULL);
	if (entry != NULL && entry->state == FASTTRUN_COLUMN_CORE_ALLOWED)
		goto chain;
	if (entry == NULL)
	{
		if (relentry->policy == FASTTRUN_REL_LOCAL_NEUTRAL)
			return fasttrun_type_default_width(relid, attnum);
		goto chain;
	}
	if (entry->state == FASTTRUN_COLUMN_LOCAL_NEUTRAL ||
		entry->statsTuple == NULL)
		return fasttrun_type_default_width(relid, attnum);

	if (!fasttrun_read_pgstat_counters_for_hook(relid, &wide_writers,
											   &ins_now, &upd_now,
												&del_now, &truncdropped_now,
												&pages_now))
		return fasttrun_type_default_width(relid, attnum);

	if (wide_writers)
		relentry->updated_cols_exact = false;

	if (!fasttrun_stats_entry_usable(relid, relentry, entry, ins_now, upd_now,
									 del_now, truncdropped_now, pages_now))
	{
		fasttrun_xact_mark_dml_plan_dependency(relid);
		return fasttrun_type_default_width(relid, attnum);
	}

	stawidth = ((Form_pg_statistic) GETSTRUCT(entry->statsTuple))->stawidth;
	if (stawidth > 0)
		return stawidth;
	return fasttrun_type_default_width(relid, attnum);

chain:
	if (prev_get_attavgwidth_hook)
		return (*prev_get_attavgwidth_hook) (relid, attnum);
	return 0;
}

/*
 * Relid-level ownership gate shared by the planner stats hooks: true when
 * fasttrun manages the relation, its storage still matches the cached
 * locator, and the relation policy blocks core pg_statistic rows.  The
 * same condition under which the column hooks hide core stats.
 */
static bool
fasttrun_stats_relid_blocks_core(Oid relid)
{
	FasttrunStatsRelidEntry *relentry;

	if (fasttrun_stats_cache == NULL || fasttrun_stats_relid_cache == NULL)
		return false;
	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (relentry == NULL ||
		!fasttrun_stats_relid_locator_valid(relid, relentry))
		return false;
	return relentry->policy == FASTTRUN_REL_LOCAL_NEUTRAL;
}

/*
 * Owning heap of an index relid, InvalidOid when unresolvable.  Cheap path
 * first: the analyze cache links index entries to their heap.  Fallback is
 * one relcache open -- the hook runs during planning, inside a transaction.
 * While the planner runs the verdict is memoized per index relid.
 */
static Oid
fasttrun_index_owning_heap(Oid indexOid)
{
	FasttrunFreshnessCacheEntry *slot = NULL;
	FasttrunAnalyzeCacheEntry *aentry;
	Oid			heap_relid = InvalidOid;

	if (fasttrun_in_planner)
	{
		slot = fasttrun_freshness_cache_slot(indexOid);
		if (slot->owner_checked)
			return slot->owner_heap;
	}

	aentry = fasttrun_cache_lookup(indexOid);
	if (aentry != NULL && OidIsValid(aentry->state.heap_relid) &&
		aentry->state.heap_relid != indexOid)
		heap_relid = aentry->state.heap_relid;
	else
	{
		Relation	indexrel;

#ifdef USE_ASSERT_CHECKING
		fasttrun_test_owner_opens++;
#endif
		indexrel = RelationIdGetRelation(indexOid);

		if (RelationIsValid(indexrel))
		{
			if (indexrel->rd_index != NULL)
				heap_relid = indexrel->rd_index->indrelid;
			RelationClose(indexrel);
		}
	}

	if (slot != NULL)
	{
		slot->owner_checked = true;
		slot->owner_heap = heap_relid;
	}
	return heap_relid;
}

static bool
fasttrun_index_is_cached(Oid indexOid)
{
	FasttrunAnalyzeCacheEntry *aentry = fasttrun_cache_lookup(indexOid);

	return aentry != NULL && OidIsValid(aentry->state.heap_relid) &&
		aentry->state.heap_relid != indexOid;
}

/*
 * The cached statistics of an index expression of a managed heap, or NULL
 * when there are none or the heap churn since the collect is past their
 * tolerance.  Freshness reads the heap counters, as for its columns.
 */
static HeapTuple
fasttrun_expression_stats_tuple(Oid heap_relid, Oid indexOid,
								AttrNumber indexattnum)
{
	FasttrunStatsKey key;
	FasttrunStatsEntry *entry;
	FasttrunStatsRelidEntry *relentry;
	int64		ins_now = 0;
	int64		upd_now = 0;
	int64		del_now = 0;
	bool		truncdropped_now = false;
	BlockNumber pages_now = 0;
	bool		wide_writers = false;

	relentry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &heap_relid, HASH_FIND, NULL);
	if (relentry == NULL)
		return NULL;
	fasttrun_stats_expr_key_init(&key, heap_relid, indexattnum, indexOid);
	entry = (FasttrunStatsEntry *) hash_search(fasttrun_stats_cache, &key,
											   HASH_FIND, NULL);
	if (entry == NULL || entry->state != FASTTRUN_COLUMN_LOCAL_CANDIDATE ||
		entry->statsTuple == NULL)
		return NULL;
	if (!fasttrun_read_pgstat_counters_for_hook(heap_relid, &wide_writers,
												&ins_now, &upd_now, &del_now,
												&truncdropped_now, &pages_now))
		return NULL;
	if (wide_writers)
		relentry->updated_cols_exact = false;
	if (!fasttrun_stats_entry_usable(heap_relid, relentry, entry, ins_now,
									 upd_now, del_now, truncdropped_now,
									 pages_now))
	{
		fasttrun_xact_mark_dml_plan_dependency(heap_relid);
		return NULL;
	}
	return entry->statsTuple;
}

/*
 * Expression indexes keep their stats in pg_statistic under the INDEX oid,
 * written only by core ANALYZE.  Once the owning heap goes LOCAL_NEUTRAL those
 * rows describe data the table may no longer have, so the hook never chains to
 * them: it serves the expression statistics fasttrun collected, or nothing,
 * and the planner then uses defaults.  Unmanaged and CORE_ALLOWED heaps chain.
 */
static bool
fasttrun_get_index_stats_hook(PlannerInfo *root, Oid indexOid,
							  AttrNumber indexattnum,
							  VariableStatData *vardata)
{
#ifdef USE_ASSERT_CHECKING
	fasttrun_test_index_hook_calls++;
#endif
	/* Some core callers do not initialize these output fields. */
	vardata->statsTuple = NULL;
	vardata->freefunc = NULL;

	/*
	 * Outside the planner frame the query names no managed relation, but an
	 * inlined SQL function body may still reach one.  There only an index the
	 * analyze cache already links to its heap goes on: a hash lookup, so a
	 * query over permanent tables opens no relation here.
	 */
	if (fasttrun_stats_cache != NULL && fasttrun_stats_relid_cache != NULL &&
		(fasttrun_in_planner || fasttrun_index_is_cached(indexOid)))
	{
		Oid			heap_relid = fasttrun_index_owning_heap(indexOid);

		if (OidIsValid(heap_relid) &&
			fasttrun_stats_relid_blocks_core(heap_relid))
		{
			HeapTuple	tuple = fasttrun_expression_stats_tuple(heap_relid,
																indexOid,
																indexattnum);

			if (tuple != NULL)
			{
				fasttrun_count(FTC_STATS_SERVED);
				vardata->statsTuple = tuple;
				vardata->freefunc = fasttrun_stats_noop_free;
				vardata->acl_ok = true;
			}
			else
				fasttrun_count(FTC_STATS_HIDDEN);
			return true;
		}
	}

	if (prev_get_index_stats_hook)
		return prev_get_index_stats_hook(root, indexOid, indexattnum, vardata);
	return false;
}

/*
 * planner hook -- re-inject cached relpages/reltuples into rd_rel for any
 * temp relation whose rd_rel was reset to the on-disk pg_class values
 * by a relcache rebuild (typically triggered by a sinval message from a
 * concurrent backend).
 *
 * fasttrun_analyze() writes (relpages, reltuples) only into rd_rel --
 * never into pg_class -- to keep the operation sinval-free.  But rd_rel
 * is owned by the relcache, and any RelationCacheInvalidateEntry resets
 * it from pg_class on the next RelationIdGetRelation.  Without this
 * hook, that wipes our analyze results: planner sees relpages=0 and
 * picks catastrophic plans (Seq Scan over what looks like a tiny table,
 * nested loop joins on big working sets).
 *
 * We walk the current query's rangetable (recursing into subquery RTEs,
 * CTEs and, when hasSubLinks is set, SubLink subselects) and re-inject
 * cached stats for those temp heaps plus their indexes.  This keeps
 * unrelated cached temp tables at zero cost for plans that don't
 * reference them.
 */
static bool
fasttrun_query_contains_stats_relid(Query *query)
{
	ListCell   *lc;

	if (query == NULL || fasttrun_stats_relid_cache == NULL)
		return false;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			if (fasttrun_stats_relid_exists(rte->relid))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			if (fasttrun_query_contains_stats_relid(rte->subquery))
				return true;
		}
	}

	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query) &&
			fasttrun_query_contains_stats_relid((Query *) cte->ctequery))
			return true;
	}

	/*
	 * Subqueries inside expressions (IN/EXISTS/scalar SubLinks) carry their
	 * Query outside rtable and cteList.  hasSubLinks gates the extra
	 * expression walk, so sublink-free queries pay nothing here.
	 */
	if (query->hasSubLinks &&
		query_tree_walker(query, fasttrun_contains_stats_sublink_walker, NULL,
						  QTW_IGNORE_RT_SUBQUERIES | QTW_IGNORE_CTE_SUBQUERIES))
		return true;

	return false;
}

/* Descend into SubLink subselects looking for cached-stats relids. */
static bool
fasttrun_contains_stats_sublink_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
		return fasttrun_query_contains_stats_relid((Query *) node);
	return expression_tree_walker(node, fasttrun_contains_stats_sublink_walker,
								  context);
}

/*
 * Re-inject cached relstats for every temp relation a query references,
 * descending into subqueries and CTEs.  Mirrors the recursion of
 * fasttrun_query_contains_stats_relid, so a temp table nested below the top
 * level gets its rd_rel repaired just like a top-level one -- otherwise the
 * planner would read relpages/reltuples 0 from pg_class for the nested temp.
 *
 * With detect_stats the same single walk also reports whether any visited
 * relation has cached column stats (the answer fasttrun_query_contains_
 * stats_relid gives), so the planner hook pays one tree walk instead of
 * two.  Probing stops after the first hit, but the reinject part still visits
 * everything.
 */
typedef struct FasttrunReinjectWalkContext
{
	bool		detect_stats;
	bool		found;
} FasttrunReinjectWalkContext;

static bool
fasttrun_reinject_query_relstats(Query *query, bool detect_stats)
{
	ListCell   *lc;
	bool		found = false;

	if (query == NULL)
		return false;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION)
		{
			fasttrun_reinject_rte_relstats(rte);
			if (detect_stats && !found &&
				fasttrun_stats_relid_exists(rte->relid))
				found = true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			if (fasttrun_reinject_query_relstats(rte->subquery, detect_stats))
				found = true;
		}
	}

	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query) &&
			fasttrun_reinject_query_relstats((Query *) cte->ctequery,
											 detect_stats))
			found = true;
	}

	/* Same SubLink descent as fasttrun_query_contains_stats_relid. */
	if (query->hasSubLinks)
	{
		FasttrunReinjectWalkContext ctx;

		ctx.detect_stats = detect_stats;
		ctx.found = false;
		(void) query_tree_walker(query, fasttrun_reinject_sublink_walker, &ctx,
								 QTW_IGNORE_RT_SUBQUERIES |
								 QTW_IGNORE_CTE_SUBQUERIES);
		if (ctx.found)
			found = true;
	}

	return found;
}

/* Descend into SubLink subselects repairing rd_rel for cached temp rels. */
static bool
fasttrun_reinject_sublink_walker(Node *node, void *context)
{
	FasttrunReinjectWalkContext *ctx = (FasttrunReinjectWalkContext *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		if (fasttrun_reinject_query_relstats((Query *) node,
											 ctx->detect_stats))
			ctx->found = true;
		return false;
	}
	return expression_tree_walker(node, fasttrun_reinject_sublink_walker,
								  context);
}

static void fasttrun_poison_check_query(Query *query);

static bool
fasttrun_poison_sublink_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		fasttrun_poison_check_query((Query *) node);
		return false;
	}
	return expression_tree_walker(node, fasttrun_poison_sublink_walker,
								  context);
}

/* Walk only while poison exists.  Hash misses do not open relations. */
static void
fasttrun_poison_check_query(Query *query)
{
	ListCell   *lc;

	if (query == NULL || fasttrun_poison_cache == NULL)
		return;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && OidIsValid(rte->relid))
		{
			FasttrunPoisonEntry *entry = fasttrun_poison_find(rte->relid);

			if (entry != NULL && entry->active)
			{
				Relation	rel = RelationIdGetRelation(rte->relid);

				if (RelationIsValid(rel))
				{
					fasttrun_poison_check_relation(rel, "query planning", false);
					RelationClose(rel);
				}
			}
		}
		else if (rte->rtekind == RTE_SUBQUERY)
			fasttrun_poison_check_query(rte->subquery);
	}
	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (IsA(cte->ctequery, Query))
			fasttrun_poison_check_query((Query *) cte->ctequery);
	}
	if (query->hasSubLinks)
		(void) query_tree_walker(query, fasttrun_poison_sublink_walker, NULL,
								 QTW_IGNORE_RT_SUBQUERIES |
								 QTW_IGNORE_CTE_SUBQUERIES);
}

/*
 * Check the finished plan as well.  Its flat range table holds every relation
 * the plan reads, including the ones the parse tree does not name: the body of
 * an inlined SQL function and the children an inheritance parent expands to.
 */
static void
fasttrun_poison_check_plan(PlannedStmt *plan)
{
	ListCell   *lc;

	if (plan == NULL || fasttrun_poison_cache == NULL)
		return;
	foreach(lc, plan->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		FasttrunPoisonEntry *entry;
		Relation	rel;

		if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
			continue;
		entry = fasttrun_poison_find(rte->relid);
		if (entry == NULL || !entry->active)
			continue;
		rel = RelationIdGetRelation(rte->relid);
		if (RelationIsValid(rel))
		{
			fasttrun_poison_check_relation(rel, "query planning", false);
			RelationClose(rel);
		}
	}
}

static void
fasttrun_restore_planner_frame(bool saved_in_planner,
							  int saved_cache_used, int saved_next_evict)
{
	fasttrun_in_planner = saved_in_planner;
	if (saved_in_planner)
	{
		/* A nested planner overwrote the array: the outer cache re-reads. */
		fasttrun_freshness_cache_used = 0;
		fasttrun_freshness_cache_next_evict = 0;
	}
	else
	{
		fasttrun_freshness_cache_used = saved_cache_used;
		fasttrun_freshness_cache_next_evict = saved_next_evict;
	}
}

/*
 * The planner hook walks the relations of the parse tree.  The body of an
 * inlined SQL function joins the query inside standard_planner, after that
 * walk, so a managed relation reached only through it would keep the pg_class
 * values a relcache rebuild left in rd_rel.  get_relation_info runs for every
 * base relation: when rd_rel there still differs from the cached relstats,
 * reinject them and redo the size estimate core has just made, together with
 * the index tuple counts derived from it.  A relation the walk already
 * reinjected costs one hash lookup and one comparison.
 */
/*
 * A relcache invalidation of an index alone (ALTER INDEX ... SET) rebuilds the
 * index rd_rel from pg_class while the heap keeps the reinjected values.  Only
 * a partial index takes its tuple count from its own rd_rel.
 */
static bool
fasttrun_partial_index_relstats_stale(RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *info = (IndexOptInfo *) lfirst(lc);
		FasttrunAnalyzeCacheEntry *entry;
		Relation	indexrel;
		bool		stale;

		if (info->indpred == NIL)
			continue;
		entry = fasttrun_cache_lookup(info->indexoid);
		if (entry == NULL || !entry->state.has_relstats)
			continue;
		indexrel = index_open(info->indexoid, NoLock);
		stale = fasttrun_relation_has_same_locator(indexrel, entry) &&
			(indexrel->rd_rel->relpages != entry->state.cached_pages ||
			 indexrel->rd_rel->reltuples != (float4) entry->state.cached_tuples);
		index_close(indexrel, NoLock);
		if (stale)
			return true;
	}
	return false;
}

/* A partial index's size and tuple count, as get_relation_info computes them. */
static void
fasttrun_redo_partial_index_estimates(RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *info = (IndexOptInfo *) lfirst(lc);
		Relation	indexrel;
		double		allvisfrac;

		if (info->indpred == NIL)
			continue;
		indexrel = index_open(info->indexoid, NoLock);
		estimate_rel_size(indexrel, NULL, &info->pages, &info->tuples,
						  &allvisfrac);
		if (info->tuples > rel->tuples)
			info->tuples = rel->tuples;
		index_close(indexrel, NoLock);
	}
}

static void
fasttrun_get_relation_info_hook(PlannerInfo *root, Oid relationObjectId,
								bool inhparent, RelOptInfo *rel)
{
	FasttrunAnalyzeCacheEntry *entry;
	Relation	relation;
	ListCell   *lc;

	if (prev_get_relation_info_hook)
		prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);
	if (inhparent || fasttrun_analyze_cache == NULL)
		return;
	entry = fasttrun_cache_lookup(relationObjectId);
	if (entry == NULL)
		return;
	/* Column statistics collected here are newer than CREATE STATISTICS data. */
	if (entry->state.ext_stats_stale)
		rel->statlist = NIL;
	if (!entry->state.has_relstats)
	{
		/*
		 * fasttrun_collect_stats publishes index relstats while the heap has
		 * none of its own.  A partial index still takes its tuple count from
		 * its own rd_rel.
		 */
		if (fasttrun_partial_index_relstats_stale(rel))
		{
			fasttrun_reinject_relid_relstats(relationObjectId);
			fasttrun_redo_partial_index_estimates(rel);
		}
		return;
	}

	relation = table_open(relationObjectId, NoLock);
	if (!fasttrun_relation_has_same_locator(relation, entry) ||
		(relation->rd_rel->relpages ==
		 fasttrun_cache_effective_relpages(relation, entry) &&
		 relation->rd_rel->reltuples == (float4) entry->state.cached_tuples &&
		 relation->rd_rel->relallvisible == (int32) entry->state.cached_allvisible &&
		 !fasttrun_partial_index_relstats_stale(rel)))
	{
		table_close(relation, NoLock);
		return;
	}

	fasttrun_reinject_relid_relstats(relationObjectId);
	estimate_rel_size(relation, rel->attr_widths - rel->min_attr,
					  &rel->pages, &rel->tuples, &rel->allvisfrac);
	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *info = (IndexOptInfo *) lfirst(lc);

		if (info->indpred == NIL)
			info->tuples = rel->tuples;
	}
	fasttrun_redo_partial_index_estimates(rel);
	table_close(relation, NoLock);
}

static PlannedStmt *
fasttrun_planner_hook(Query *parse, const char *query_string,
					  int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool		saved_in_planner = fasttrun_in_planner;
	int			saved_freshness_cache_used = fasttrun_freshness_cache_used;
	int			saved_freshness_cache_next_evict = fasttrun_freshness_cache_next_evict;
	bool		stats_frame_needed = false;

	if (fasttrun_poison_cache != NULL)
		fasttrun_poison_check_query(parse);

	if (fasttrun_analyze_cache != NULL)
	{
		bool		detect = (fasttrun_stats_cache != NULL &&
							  fasttrun_stats_relid_cache != NULL);

		/* One walk: reinject relstats and detect cached column stats. */
		stats_frame_needed = fasttrun_reinject_query_relstats(parse, detect);
	}
	else if (fasttrun_stats_cache != NULL)
	{
		/* Nothing to reinject -- detect-only walk with early exit. */
		stats_frame_needed = fasttrun_query_contains_stats_relid(parse);
	}

	/*
	 * The freshness frame is only useful for cached column statistics
	 * referenced by this query.  Keep unrelated plans to a plain hook call
	 * plus relstats reinjection above.  Avoid PG_TRY/sigsetjmp on ordinary
	 * queries after a backend has used fasttrun stats at least once.
	 */
	if (!stats_frame_needed)
	{
		if (!saved_in_planner)
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
										   boundParams);
			else
				result = standard_planner(parse, query_string, cursorOptions,
										  boundParams);
			fasttrun_poison_check_plan(result);
			return result;
		}

		/* A nested non-routable query does not inherit the outer frame. */
		fasttrun_in_planner = false;
		PG_TRY();
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
									   boundParams);
			else
				result = standard_planner(parse, query_string, cursorOptions,
									  boundParams);
		}
		PG_CATCH();
		{
			fasttrun_restore_planner_frame(saved_in_planner,
										 saved_freshness_cache_used,
										 saved_freshness_cache_next_evict);
			PG_RE_THROW();
		}
		PG_END_TRY();
		fasttrun_restore_planner_frame(saved_in_planner,
									 saved_freshness_cache_used,
									 saved_freshness_cache_next_evict);
		fasttrun_poison_check_plan(result);
		return result;
	}

	fasttrun_in_planner = true;
	fasttrun_freshness_cache_used = 0;
	fasttrun_freshness_cache_next_evict = 0;

	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(parse, query_string, cursorOptions,
									   boundParams);
		else
			result = standard_planner(parse, query_string, cursorOptions,
									  boundParams);
	}
	PG_CATCH();
	{
		fasttrun_restore_planner_frame(saved_in_planner,
									 saved_freshness_cache_used,
									 saved_freshness_cache_next_evict);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_restore_planner_frame(saved_in_planner,
								 saved_freshness_cache_used,
								 saved_freshness_cache_next_evict);
	fasttrun_poison_check_plan(result);

	return result;
}

