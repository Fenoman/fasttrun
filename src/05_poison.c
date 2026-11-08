/*-------------------------------------------------------------------------
 *
 * src/05_poison.c
 *      Truncate operations and the poison registry.
 *
 * What it holds.  The registry of a running fasttruncate and the per-backend
 * block of tables whose truncate failed after storage changed: reservation,
 * activation, completion, lookup and commit handling.
 *
 * Also here.  The cassert test function fasttrun_test_poison_locator_mismatch.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/05_poison.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* Publish the operation registry only after its context and HTAB exist. */
static void
fasttrun_operation_init(void)
{
	HASHCTL		ctl;
	MemoryContext volatile new_mcxt = NULL;
	HTAB	   *new_cache = NULL;

	if (fasttrun_poison_cache != NULL)
		return;

	/* The planner hook enforces active blocks. */
	fasttrun_ensure_planner_hook();

	PG_TRY();
	{
		new_mcxt = AllocSetContextCreate(TopMemoryContext,
										 "fasttrun operation context",
										 ALLOCSET_SMALL_SIZES);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunPoisonEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_cache = hash_create("fasttrun poison cache", 8, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	PG_CATCH();
	{
		if (new_mcxt != NULL)
			MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	fasttrun_operation_mcxt = (MemoryContext) new_mcxt;
	fasttrun_poison_cache = new_cache;
}

static void
fasttrun_operation_reset_if_empty(void)
{
	MemoryContext old_mcxt;

	if (fasttrun_poison_cache == NULL ||
		hash_get_num_entries(fasttrun_poison_cache) != 0)
		return;

	old_mcxt = fasttrun_operation_mcxt;
	fasttrun_poison_cache = NULL;
	fasttrun_operation_mcxt = NULL;
	MemoryContextDelete(old_mcxt);
}

static FasttrunPoisonEntry *
fasttrun_poison_find(Oid root_relid)
{
	if (fasttrun_poison_cache == NULL || !OidIsValid(root_relid))
		return NULL;
	return (FasttrunPoisonEntry *) hash_search(fasttrun_poison_cache,
												  &root_relid, HASH_FIND, NULL);
}

/*
 * Reserve the registry slot before mutation.  The final pointer assignment
 * is the publication boundary: errors before it leave no pending operation.
 */
static FasttrunPoisonEntry *
fasttrun_poison_reserve(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry;
	bool		found;

	fasttrun_operation_init();
	entry = (FasttrunPoisonEntry *) hash_search(fasttrun_poison_cache,
												   &operation->root_relid,
												   HASH_ENTER, &found);
	if (!found)
	{
		entry->root_rlb = operation->root_rlb;
		entry->generation = 0;
		entry->active = false;
		entry->active_operation = NULL;
		entry->pending_operation = NULL;
	}
	if (entry->pending_operation != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun: truncate is already pending for relation %u",
						operation->root_relid)));

	operation->registry_entry = entry;
	entry->root_rlb = operation->root_rlb;
	entry->generation = operation->generation;
	entry->pending_operation = operation;
	return entry;
}

/* A preparation error detaches this attempt and preserves an older block. */
static void
fasttrun_poison_cancel_pending(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	Oid			root_relid = operation->root_relid;

	if (entry != NULL && entry->pending_operation == operation &&
		entry->generation == operation->generation)
		entry->pending_operation = NULL;
	pfree(operation);

	if (entry != NULL && entry->pending_operation == NULL)
	{
		if (entry->active && entry->active_operation != NULL)
		{
			entry->root_rlb = entry->active_operation->root_rlb;
			entry->generation = entry->active_operation->generation;
		}
		else
			(void) hash_search(fasttrun_poison_cache, &root_relid,
								 HASH_REMOVE, NULL);
	}
	fasttrun_operation_reset_if_empty();
}

/* MUTATED failed: no allocation or catalog access is allowed here. */
static void
fasttrun_poison_activate(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	FasttrunTruncateOperation *old_active;

	Assert(entry != NULL);
	Assert(entry->pending_operation == operation);
	old_active = entry->active_operation;
	entry->pending_operation = NULL;
	entry->active_operation = operation;
	entry->active = true;
	entry->root_rlb = operation->root_rlb;
	entry->generation = operation->generation;
	operation->phase = FASTTRUN_TRUNCATE_POISONED;
	if (old_active != NULL && old_active != operation)
		pfree(old_active);
}

/* COMPLETE is the only successful repair boundary. */
static void
fasttrun_poison_complete(FasttrunTruncateOperation *operation)
{
	FasttrunPoisonEntry *entry = operation->registry_entry;
	FasttrunTruncateOperation *old_active;
	Oid			root_relid = operation->root_relid;

	Assert(entry != NULL);
	Assert(entry->pending_operation == operation);
	old_active = entry->active_operation;
	entry->pending_operation = NULL;
	entry->active_operation = NULL;
	entry->active = false;
	if (old_active != NULL && old_active != operation)
		pfree(old_active);
	pfree(operation);
	(void) hash_search(fasttrun_poison_cache, &root_relid,
						 HASH_REMOVE, NULL);
	fasttrun_operation_reset_if_empty();
}

static void
fasttrun_poison_forget_relid(Oid root_relid)
{
	FasttrunPoisonEntry *entry = fasttrun_poison_find(root_relid);
	FasttrunTruncateOperation *active;
	FasttrunTruncateOperation *pending;

	if (entry == NULL)
		return;
	active = entry->active_operation;
	pending = entry->pending_operation;
	entry->active_operation = NULL;
	entry->pending_operation = NULL;
	entry->active = false;
	if (active != NULL)
		pfree(active);
	if (pending != NULL && pending != active)
		pfree(pending);
	(void) hash_search(fasttrun_poison_cache, &root_relid,
						 HASH_REMOVE, NULL);
	fasttrun_operation_reset_if_empty();
}

static void
fasttrun_poison_check_relation(Relation rel, const char *operation,
								bool allow_repair)
{
	Oid			root_relid = RelationGetRelid(rel);
	FasttrunPoisonEntry *entry = fasttrun_poison_find(root_relid);

	if (entry == NULL || !entry->active)
		return;

	/*
	 * A relation on another file is not the damaged one.  The file change can
	 * still roll back: core TRUNCATE through an inheritance parent gives the
	 * child a new file, and an abort brings the damaged one back.  So the
	 * block retires only at COMMIT.
	 */
	if (!RelFileLocatorEquals(entry->root_rlb.locator, rel->rd_locator) ||
		entry->root_rlb.backend != rel->rd_backend)
	{
		fasttrun_xact_mark_relid(root_relid, root_relid,
								 FASTTRUN_TOUCH_POISON_RETIRE);
		return;
	}
	if (allow_repair)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("fasttrun: relation \"%s\" is unavailable after an incomplete truncate",
					RelationGetRelationName(rel)),
			 errdetail("The failed %s had already changed table files.",
					   operation),
			 errhint("Retry fasttruncate, or DROP and recreate the temporary table.")));
}

/*
 * DROP and a change of the relation's file are transactional, so only COMMIT
 * removes an active block.
 */
static void
fasttrun_poison_commit_xact(void)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;

	if (fasttrun_poison_cache == NULL || fasttrun_xact_frame == NULL)
		return;
	hash_seq_init(&status, fasttrun_xact_frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		if ((fasttrun_xact_entry_dropped(xentry) ||
			 (xentry->flags & FASTTRUN_TOUCH_POISON_RETIRE) != 0) &&
			fasttrun_poison_find(xentry->relid) != NULL)
			fasttrun_poison_forget_relid(xentry->relid);
	}
}

#ifdef USE_ASSERT_CHECKING
Datum
fasttrun_test_poison_locator_mismatch(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	FasttrunPoisonEntry *entry = fasttrun_poison_find(relid);

	if (entry == NULL || !entry->active)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("fasttrun test poison is not active")));
	entry->root_rlb.locator.relNumber++;
	PG_RETURN_VOID();
}
#endif

