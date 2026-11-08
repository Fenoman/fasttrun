/*-------------------------------------------------------------------------
 *
 * src/25_fresh.c
 *      fasttrun_temp_is_ours and fasttrun_temp_same_cols.
 *
 * What it holds.  The truncate marks of the current transaction, their
 * transaction and subtransaction callbacks and the two SQL functions.
 *
 * Also here.  fasttrun_poison_blocks_file.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/25_fresh.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* ----------------------------------------------------------------------
 * Fresh content: fasttrun_temp_is_ours() and fasttrun_temp_same_cols().
 *
 * What it answers.  Whether every row of a temporary table started in the
 * current transaction: the table was created in it, or emptied in it, and no
 * rollback has brought older rows back since.  A caller keeps such a table as
 * it is and clears any other one.
 *
 * Signals:
 *   creation       rd_createSubid of the relcache entry.  Core hands it to the
 *                  parent at RELEASE and drops the table at ROLLBACK TO.
 *   core TRUNCATE  truncdropped of the pgstat transaction counters, read from
 *                  the pending entry, so the current track_counts does not
 *                  matter.  Core sets it right after the rows go, hands it to
 *                  the parent at RELEASE and drops it at ROLLBACK TO, also for
 *                  a TRUNCATE run before the library was loaded.  With
 *                  track_counts = off there are no counters, so OAT_TRUNCATE
 *                  also sets a mark of the same lifetime.  That hook runs
 *                  before the rows go, so the mark waits until the TRUNCATE
 *                  statement has finished: a BEFORE TRUNCATE trigger does not
 *                  see it.  Every form counts: a list, CASCADE, inheritance
 *                  children, partitions.
 *   fasttruncate   a mark set after the reset succeeded or found the table
 *                  already empty.  A rollback does not bring the old rows
 *                  back, so the mark outlives its level, but then only while
 *                  the emptied file is current: the rollback may restore an
 *                  older file from before a TRUNCATE or a rewrite.
 *
 * What is not fresh.  DELETE, a rewrite of older rows (ALTER COLUMN TYPE,
 * CLUSTER, VACUUM FULL), ALTER TABLE ADD COLUMN, the ON COMMIT DELETE ROWS
 * cleanup of the previous transaction.  None of them sets a signal above, and
 * rd_firstRelfilelocatorSubid, which a rewrite does set, is not one.
 * ----------------------------------------------------------------------
 */

/*
 * A file a fasttruncate emptied.  on_file turns on when the level of that
 * reset rolls back: the reset stays done, so the table is fresh whenever this
 * file is current.  The entry is made before the reset, so the rollback only
 * sets the flag.
 */
typedef struct FasttrunFreshFileKey
{
	Oid			relid;
	RelFileLocator locator;
} FasttrunFreshFileKey;

typedef struct FasttrunFreshFile
{
	FasttrunFreshFileKey key;	/* hash key */
	bool		on_file;
} FasttrunFreshFile;

/*
 * Marks of one table in the current top transaction.
 *
 *   reset_subid    the outermost live level whose fasttruncate emptied the
 *                  table.  No level up to it has rolled back since, so the
 *                  rows on whatever file is current were written after the
 *                  reset, a rewrite only carries them over.  A reset at a
 *                  deeper level adds nothing and is not recorded.
 *   reset_locator  the file that reset emptied.
 *   sql_subid      the outermost live level with a finished core TRUNCATE
 *                  seen by OAT_TRUNCATE.
 *   listed_subid   the newest level whose list holds the table.
 */
typedef struct FasttrunFreshEntry
{
	Oid			relid;			/* hash key */
	SubTransactionId reset_subid;
	RelFileLocator reset_locator;
	SubTransactionId sql_subid;
	SubTransactionId listed_subid;
} FasttrunFreshEntry;

/*
 * Tables that got a mark at one level.  The newest level is first, subids
 * never grow towards the bottom, and a RELEASE retags a level to its parent,
 * so the same subid may repeat.
 */
typedef struct FasttrunFreshLevel
{
	SubTransactionId subid;
	List	   *relids;
	struct FasttrunFreshLevel *next;
} FasttrunFreshLevel;

/*
 * All of it lives in TopTransactionContext.
 *
 * The running TRUNCATE statement.  fasttrun_fresh_stmt_subid is its level,
 * fasttrun_fresh_pending the tables its OAT_TRUNCATE saw at that level.  They
 * get sql_subid once the statement has finished.  Outside a TRUNCATE
 * statement the level is invalid.
 */
static HTAB *fasttrun_fresh_marks = NULL;
static HTAB *fasttrun_fresh_files = NULL;
static FasttrunFreshLevel *fasttrun_fresh_levels = NULL;
static List *fasttrun_fresh_pending = NIL;
static SubTransactionId fasttrun_fresh_stmt_subid = InvalidSubTransactionId;

static FasttrunFreshEntry *
fasttrun_fresh_find(Oid relid)
{
	if (fasttrun_fresh_marks == NULL)
		return NULL;
	return (FasttrunFreshEntry *) hash_search(fasttrun_fresh_marks, &relid,
											  HASH_FIND, NULL);
}

static FasttrunFreshFile *
fasttrun_fresh_find_file(Oid relid, const RelFileLocator *locator)
{
	FasttrunFreshFileKey key;

	if (fasttrun_fresh_files == NULL)
		return NULL;
	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.locator = *locator;
	return (FasttrunFreshFile *) hash_search(fasttrun_fresh_files, &key,
											 HASH_FIND, NULL);
}

/*
 * The table's entry, listed at the current level.  It may fail, so callers
 * run it before anything changes: a mark the level list does not know would
 * outlive the rollback of its level.
 */
static FasttrunFreshEntry *
fasttrun_fresh_prepare(Oid relid)
{
	SubTransactionId subid = GetCurrentSubTransactionId();
	FasttrunFreshEntry *entry;
	bool		found;

	if (fasttrun_fresh_marks == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunFreshEntry);
		ctl.hcxt = TopTransactionContext;
		fasttrun_fresh_marks = hash_create("fasttrun fresh marks", 16, &ctl,
										   HASH_ELEM | HASH_BLOBS |
										   HASH_CONTEXT);
	}
	entry = (FasttrunFreshEntry *) hash_search(fasttrun_fresh_marks, &relid,
											   HASH_ENTER, &found);
	if (!found)
	{
		entry->reset_subid = InvalidSubTransactionId;
		entry->sql_subid = InvalidSubTransactionId;
		entry->listed_subid = InvalidSubTransactionId;
	}
	if (entry->listed_subid != subid)
	{
		FasttrunFreshLevel *level = fasttrun_fresh_levels;
		MemoryContext old_mcxt = MemoryContextSwitchTo(TopTransactionContext);

		Assert(level == NULL || level->subid <= subid);
		if (level == NULL || level->subid != subid)
		{
			level = (FasttrunFreshLevel *) palloc(sizeof(FasttrunFreshLevel));
			level->subid = subid;
			level->relids = NIL;
			level->next = fasttrun_fresh_levels;
		}
		level->relids = lappend_oid(level->relids, relid);
		fasttrun_fresh_levels = level;
		MemoryContextSwitchTo(old_mcxt);
		entry->listed_subid = subid;
	}
	return entry;
}

/*
 * Before fasttruncate changes anything: all its mark needs, so that the mark
 * after the reset cannot fail.  A live reset mark makes a new one useless.
 */
static void
fasttrun_fresh_reserve(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	FasttrunFreshEntry *entry = fasttrun_fresh_find(relid);
	FasttrunFreshFileKey key;
	FasttrunFreshFile *file;
	bool		found;

	if (entry != NULL && entry->reset_subid != InvalidSubTransactionId)
		return;
	(void) fasttrun_fresh_prepare(relid);
	if (fasttrun_fresh_files == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(FasttrunFreshFileKey);
		ctl.entrysize = sizeof(FasttrunFreshFile);
		ctl.hcxt = TopTransactionContext;
		fasttrun_fresh_files = hash_create("fasttrun fresh files", 16, &ctl,
										   HASH_ELEM | HASH_BLOBS |
										   HASH_CONTEXT);
	}
	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.locator = rel->rd_locator;
	file = (FasttrunFreshFile *) hash_search(fasttrun_fresh_files, &key,
											 HASH_ENTER, &found);
	if (!found)
		file->on_file = false;
}

/*
 * fasttruncate emptied the table, or found it already empty.  Runs after
 * fasttrun_fresh_reserve and allocates nothing.
 */
static void
fasttrun_fresh_mark(Relation rel)
{
	FasttrunFreshEntry *entry = fasttrun_fresh_find(RelationGetRelid(rel));

	if (entry == NULL || entry->reset_subid != InvalidSubTransactionId)
		return;
	Assert(entry->listed_subid == GetCurrentSubTransactionId());
	Assert(fasttrun_fresh_find_file(RelationGetRelid(rel), &rel->rd_locator) != NULL);
	entry->reset_subid = GetCurrentSubTransactionId();
	entry->reset_locator = rel->rd_locator;
}

/*
 * OAT_TRUNCATE of a core TRUNCATE, before the rows go.  The table waits in
 * fasttrun_fresh_pending until the statement has finished, and nothing in its
 * entry changes before that.  So a TRUNCATE that fails, the core refusing a
 * nested TRUNCATE of a table the outer statement holds included, leaves the
 * marks of other statements as they were.
 *
 * Only an OAT_TRUNCATE at the level of the running TRUNCATE statement counts.
 * A TRUNCATE that reaches the hook without the fasttrun utility hook, run by
 * another extension itself, is left to the pgstat counters.
 */
static void
fasttrun_fresh_note_core_truncate(Oid relid)
{
	FasttrunFreshEntry *entry;
	MemoryContext old_mcxt;

	if (fasttrun_fresh_stmt_subid == InvalidSubTransactionId ||
		fasttrun_fresh_stmt_subid != GetCurrentSubTransactionId())
		return;
	if (!fasttrun_relid_is_temp(relid))
		return;
	entry = fasttrun_fresh_find(relid);
	if (entry != NULL && entry->sql_subid != InvalidSubTransactionId)
		return;
	(void) fasttrun_fresh_prepare(relid);
	old_mcxt = MemoryContextSwitchTo(TopTransactionContext);
	fasttrun_fresh_pending = lappend_oid(fasttrun_fresh_pending, relid);
	MemoryContextSwitchTo(old_mcxt);
}

/*
 * The TRUNCATE statement at level stmt_subid finished: its tables get a mark
 * of that level.  A mark already there is of that level or an outer one, the
 * deeper levels of the statement have ended by now.
 */
static void
fasttrun_fresh_confirm_pending(SubTransactionId stmt_subid)
{
	ListCell   *lc;

	foreach(lc, fasttrun_fresh_pending)
	{
		FasttrunFreshEntry *entry = fasttrun_fresh_find(lfirst_oid(lc));

		if (entry == NULL)
			continue;
		Assert(entry->sql_subid <= stmt_subid);
		if (entry->sql_subid == InvalidSubTransactionId)
			entry->sql_subid = stmt_subid;
	}
	list_free(fasttrun_fresh_pending);
	fasttrun_fresh_pending = NIL;
}

static void
fasttrun_fresh_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			/* The memory goes away with TopTransactionContext. */
			fasttrun_fresh_marks = NULL;
			fasttrun_fresh_files = NULL;
			fasttrun_fresh_levels = NULL;
			fasttrun_fresh_pending = NIL;
			fasttrun_fresh_stmt_subid = InvalidSubTransactionId;
			break;
		default:
			break;
	}
}

/*
 * RELEASE hands the marks of the level to its parent.  ROLLBACK TO drops its
 * core TRUNCATE marks, waiting ones too, and keeps its fasttruncate marks on
 * their files.  The list of waiting marks belongs to the TRUNCATE statement:
 * its wrapper frees it when the error reaches it.  Nothing here allocates.
 */
static void
fasttrun_fresh_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								SubTransactionId parentSubid, void *arg)
{
	bool		aborting = (event == SUBXACT_EVENT_ABORT_SUB);
	FasttrunFreshLevel *level = fasttrun_fresh_levels;

	if (event != SUBXACT_EVENT_COMMIT_SUB && !aborting)
		return;

	/* Every level tagged with mySubid, an earlier RELEASE may have left several. */
	while (level != NULL && level->subid == mySubid)
	{
		FasttrunFreshLevel *next = level->next;
		ListCell   *lc;

		foreach(lc, level->relids)
		{
			Oid			relid = lfirst_oid(lc);
			FasttrunFreshEntry *entry = fasttrun_fresh_find(relid);

			if (entry == NULL)
				continue;
			if (!aborting)
			{
				if (entry->reset_subid == mySubid)
					entry->reset_subid = parentSubid;
				if (entry->sql_subid == mySubid)
					entry->sql_subid = parentSubid;
				if (entry->listed_subid == mySubid)
					entry->listed_subid = parentSubid;
				continue;
			}
			if (entry->sql_subid == mySubid)
				entry->sql_subid = InvalidSubTransactionId;
			if (entry->reset_subid == mySubid)
			{
				FasttrunFreshFile *file =
					fasttrun_fresh_find_file(relid, &entry->reset_locator);

				Assert(file != NULL);
				if (file != NULL)
					file->on_file = true;
				entry->reset_subid = InvalidSubTransactionId;
			}
		}

		if (!aborting)
			level->subid = parentSubid;
		else
		{
			fasttrun_fresh_levels = next;
			list_free(level->relids);
			pfree(level);
		}
		level = next;
	}
}

/* The current file is the one an incomplete fasttruncate left damaged. */
static bool
fasttrun_poison_blocks_file(Relation rel)
{
	FasttrunPoisonEntry *entry = fasttrun_poison_find(RelationGetRelid(rel));

	return entry != NULL && entry->active &&
		RelFileLocatorEquals(entry->root_rlb.locator, rel->rd_locator) &&
		entry->root_rlb.backend == rel->rd_backend;
}

static bool
fasttrun_fresh_marked(Relation rel)
{
	FasttrunFreshEntry *entry = fasttrun_fresh_find(RelationGetRelid(rel));
	FasttrunFreshFile *file;

	if (entry != NULL &&
		(entry->reset_subid != InvalidSubTransactionId ||
		 entry->sql_subid != InvalidSubTransactionId))
		return true;
	file = fasttrun_fresh_find_file(RelationGetRelid(rel), &rel->rd_locator);
	return file != NULL && file->on_file;
}

/*
 * A core TRUNCATE at a live level, as the pgstat transaction counters see it.
 * The pending entry is read directly: with track_counts off now, relation_open
 * has already unlinked it from the relcache entry, though the counters of a
 * TRUNCATE run earlier are still there.
 */
static bool
fasttrun_fresh_core_truncated(Relation rel)
{
	PgStat_EntryRef *ref;
	PgStat_TableXactStatus *trans;

	ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId,
									 RelationGetRelid(rel));
	if (ref == NULL || ref->pending == NULL)
		return false;
	for (trans = ((PgStat_TableStatus *) ref->pending)->trans; trans != NULL;
		 trans = trans->upper)
	{
		if (trans->truncdropped)
			return true;
	}
	return false;
}

/* The temporary table argument: a heap table of this session, not a parent. */
static void
fasttrun_check_local_temp_table(Relation rel, const char *func)
{
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s: relation \"%s\" is a partitioned table",
						func, RelationGetRelationName(rel)),
				 errdetail("Rows live in its partitions, pass a partition.")));
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s: relation \"%s\" is not a table",
						func, RelationGetRelationName(rel))));
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		!isTempNamespace(RelationGetNamespace(rel)))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s: relation \"%s\" is not a local temporary table",
						func, RelationGetRelationName(rel))));
	if (rel->rd_tableam != GetHeapamTableAmRoutine())
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s: relation \"%s\" is not heap-AM",
						func, RelationGetRelationName(rel))));
}

/*
 * fasttrun_temp_is_ours(regclass) returns TRUE when every row of a temporary
 * table of this session started in the current transaction.
 *
 * Returns:
 *   NULL   the table no longer exists
 *   TRUE   created in the transaction, or emptied by TRUNCATE or fasttruncate
 *          at a live level, or emptied by fasttruncate on the current file
 *   FALSE  anything else, and a table an incomplete fasttruncate left
 *          damaged: the caller goes on to fasttruncate, which repairs it
 *
 * Privileges on the table are not checked: the caller is usually a SECURITY
 * DEFINER function, and the table may belong to another role.
 */
Datum
fasttrun_temp_is_ours(PG_FUNCTION_ARGS)
{
	Relation	rel;
	bool		fresh;

	rel = try_relation_open(PG_GETARG_OID(0), AccessShareLock);
	if (rel == NULL)
		PG_RETURN_NULL();
	fasttrun_check_local_temp_table(rel, "fasttrun_temp_is_ours");

	if (fasttrun_poison_blocks_file(rel))
		fresh = false;
	else
		fresh = rel->rd_createSubid != InvalidSubTransactionId ||
			fasttrun_fresh_marked(rel) ||
			fasttrun_fresh_core_truncated(rel);

	relation_close(rel, AccessShareLock);
	PG_RETURN_BOOL(fresh);
}

/*
 * fasttrun_temp_same_cols(temp regclass, template regclass) returns TRUE when
 * the live columns of both tables match.
 *
 * What it compares.  Name, type, typmod and collation in attnum order, dropped
 * columns skipped.  Defaults, constraints, indexes, identity and generation
 * stay out.  The descriptors come from the relcache, which also holds a
 * temporary table whose catalog rows SQL does not see.
 *
 * Arguments:
 *   temp      a temporary table of this session, checked as in
 *             fasttrun_temp_is_ours
 *   template  any table, a partitioned one too
 *
 * Returns NULL when either table no longer exists.
 */
Datum
fasttrun_temp_same_cols(PG_FUNCTION_ARGS)
{
	Relation	temprel;
	Relation	tmplrel;
	TupleDesc	tempdesc;
	TupleDesc	tmpldesc;
	int			i = 0;
	int			j = 0;
	bool		same;

	temprel = try_relation_open(PG_GETARG_OID(0), AccessShareLock);
	if (temprel == NULL)
		PG_RETURN_NULL();
	fasttrun_check_local_temp_table(temprel, "fasttrun_temp_same_cols");

	tmplrel = try_relation_open(PG_GETARG_OID(1), AccessShareLock);
	if (tmplrel == NULL)
	{
		relation_close(temprel, AccessShareLock);
		PG_RETURN_NULL();
	}
	if (tmplrel->rd_rel->relkind != RELKIND_RELATION &&
		tmplrel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("fasttrun_temp_same_cols: template \"%s\" is not a table",
						RelationGetRelationName(tmplrel))));

	tempdesc = RelationGetDescr(temprel);
	tmpldesc = RelationGetDescr(tmplrel);
	for (;;)
	{
		Form_pg_attribute a;
		Form_pg_attribute b;

		while (i < tempdesc->natts && TupleDescAttr(tempdesc, i)->attisdropped)
			i++;
		while (j < tmpldesc->natts && TupleDescAttr(tmpldesc, j)->attisdropped)
			j++;
		if (i >= tempdesc->natts || j >= tmpldesc->natts)
		{
			same = (i >= tempdesc->natts && j >= tmpldesc->natts);
			break;
		}
		a = TupleDescAttr(tempdesc, i++);
		b = TupleDescAttr(tmpldesc, j++);
		if (strcmp(NameStr(a->attname), NameStr(b->attname)) != 0 ||
			a->atttypid != b->atttypid ||
			a->atttypmod != b->atttypmod ||
			a->attcollation != b->attcollation)
		{
			same = false;
			break;
		}
	}

	relation_close(tmplrel, AccessShareLock);
	relation_close(temprel, AccessShareLock);
	PG_RETURN_BOOL(same);
}

