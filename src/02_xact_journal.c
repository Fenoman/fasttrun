/*-------------------------------------------------------------------------
 *
 * src/02_xact_journal.c
 *      Subtransaction journal frames and relation marks.
 *
 * What it holds.  The touch flags, the bounded journal frame of each
 * subtransaction that changes local state, the frame pool, and the marks that
 * record which relations a level created, wrote or dropped.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/02_xact_journal.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */

/* One bounded journal frame per subtransaction that mutates local state. */
typedef enum FasttrunTouchFlags
{
	FASTTRUN_TOUCH_ANALYZE = 1 << 0,
	FASTTRUN_TOUCH_STATS = 1 << 1,
	FASTTRUN_TOUCH_DML = 1 << 2,
	FASTTRUN_TOUCH_DROPPED = 1 << 3,
	/* Planner-visible mutation: abort must invalidate local cached plans. */
	FASTTRUN_TOUCH_PLAN_INVALIDATE = 1 << 4,
	/* Column-stats entries were written: subxact end must walk attkeys. */
	FASTTRUN_TOUCH_COLSTATS = 1 << 5,
	/* Recorded at the relation's birth level, not its first cache mutation. */
	FASTTRUN_TOUCH_CREATED = 1 << 6,
	/*
	 * fasttruncate() reset the storage at this level.  An abort of the level
	 * cannot bring the old rows back, so no state saved before the reset
	 * applies any more.
	 */
	FASTTRUN_TOUCH_TRUNCATED = 1 << 7,
	/* Written while track_counts was off: the counters missed the rows. */
	FASTTRUN_TOUCH_UNCOUNTED = 1 << 8,
	/*
	 * The relation was seen on a file other than the one its incomplete
	 * truncate damaged.  COMMIT removes the block, and an abort drops the
	 * mark, because it may bring the damaged file back.
	 */
	FASTTRUN_TOUCH_POISON_RETIRE = 1 << 9,
	/* A recorded file birth can retire its failed operation on abort. */
	FASTTRUN_TOUCH_POISON_FILE = 1 << 10,
	/* An omitted index can return when this level rolls back. */
	FASTTRUN_TOUCH_DROPPED_COMPONENT = 1 << 11,
	/* A statistics object can return on rollback after a local storage reset. */
	FASTTRUN_TOUCH_DROPPED_STATEXT = 1 << 12
} FasttrunTouchFlags;

typedef struct FasttrunXactRelEntry
{
	Oid			relid;			/* hash key */
	Oid			root_relid;		/* owning heap */
	uint32		flags;
	bool		level_size_known;	/* the first write at this level recorded */
	BlockNumber level_pages;	/* heap pages before that write */
	BlockNumber level_dead;		/* aborted_pages before that write */
	RelFileLocator level_locator;	/* the file level_pages was measured on */
	RelFileLocator file_birth_locator;	/* file owned by this level */
	Oid			settle_owner;	/* abort walk: the heap of the index row it
								 * reached, settled after the walk */
	Bitmapset  *dropped_attrs;	/* columns dropped at this level, in
								 * TopTransactionContext */
} FasttrunXactRelEntry;

typedef struct FasttrunSubcommitDropped
{
	Oid			relid;
	Bitmapset  *merged;
} FasttrunSubcommitDropped;

typedef struct FasttrunXactFrame
{
	SubTransactionId subid;
	MemoryContext mcxt;
	HTAB	   *entries;
	FasttrunSubcommitDropped *subcommit_dropped;
	int			subcommit_ndropped;
	Oid		   *subcommit_inserted;
	int			subcommit_ninserted;
	struct FasttrunXactFrame *parent;
} FasttrunXactFrame;

static FasttrunXactFrame *fasttrun_xact_frame = NULL;
/* Avoid ancestor lookups on truncates in transactions without statistics DDL. */
static bool fasttrun_xact_dropped_statext = false;

/*
 * Frames released by a finished subtransaction are kept here for reuse instead
 * of being deleted.  A frame costs a memory context plus a hash table, and a
 * PL/pgSQL EXCEPTION block opens a subtransaction on every pass of its loop, so
 * without the pool a loop over a tracked relation built and tore that pair down
 * once per pass.  Frame contexts are children of TopTransactionContext, so at
 * the end of the top transaction the list only has to be cleared: the memory
 * goes away with the parent.
 *
 * A frame that grew wide is not pooled: dynahash never shrinks, so keeping it
 * would hold the enlarged table until the top transaction ends.  Deleting it
 * returns that memory at the end of the subtransaction.
 */
#define FASTTRUN_XACT_FRAME_POOL_MAX 4
#define FASTTRUN_XACT_FRAME_POOL_ENTRIES_MAX 32

/* Set on a pooled frame's context.  A live frame carries no identifier. */
#define FASTTRUN_XACT_FRAME_POOLED_IDENT "pooled"

static FasttrunXactFrame *fasttrun_xact_frame_pool = NULL;
static int	fasttrun_xact_frame_pool_used = 0;

static uint64 fasttrun_truncate_generation = 0;

#ifdef USE_ASSERT_CHECKING
/* The counter and the list must not drift apart. */
static int
fasttrun_xact_frame_pool_length(void)
{
	FasttrunXactFrame *frame;
	int			n = 0;

	for (frame = fasttrun_xact_frame_pool; frame != NULL; frame = frame->parent)
		n++;
	return n;
}
#endif

static FasttrunXactFrame *
fasttrun_xact_frame_for_subid(SubTransactionId subid, bool create)
{
	FasttrunXactFrame **link = &fasttrun_xact_frame;
	FasttrunXactFrame *volatile new_frame = NULL;
	MemoryContext volatile new_mcxt = NULL;
	MemoryContext oldcxt;
	HASHCTL		ctl;

	/* Active ancestors have smaller IDs.  Core rejects subid wraparound. */
	while (*link != NULL && (*link)->subid > subid)
		link = &(*link)->parent;
	if (*link != NULL && (*link)->subid == subid)
		return *link;
	if (!create)
		return NULL;

	if (fasttrun_xact_frame_pool != NULL)
	{
		FasttrunXactFrame *reused = fasttrun_xact_frame_pool;

		Assert(fasttrun_xact_frame_pool_length() == fasttrun_xact_frame_pool_used);
		fasttrun_xact_frame_pool = reused->parent;
		fasttrun_xact_frame_pool_used--;
		MemoryContextSetIdentifier(reused->mcxt, NULL);
		reused->subid = subid;
		reused->subcommit_dropped = NULL;
		reused->subcommit_ndropped = 0;
		reused->subcommit_inserted = NULL;
		reused->subcommit_ninserted = 0;
		reused->parent = *link;
		*link = reused;
		return *link;
	}

	new_mcxt = AllocSetContextCreate(TopTransactionContext,
									 "fasttrun xact frame",
									 ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo((MemoryContext) new_mcxt);
	PG_TRY();
	{
		new_frame = (FasttrunXactFrame *) palloc0(sizeof(*new_frame));
		new_frame->subid = subid;
		new_frame->mcxt = (MemoryContext) new_mcxt;
		new_frame->parent = *link;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(FasttrunXactRelEntry);
		ctl.hcxt = (MemoryContext) new_mcxt;
		new_frame->entries = hash_create("fasttrun xact frame relids", 16,
										 &ctl,
										 HASH_ELEM | HASH_BLOBS |
										 HASH_CONTEXT);
		MemoryContextSwitchTo(oldcxt);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextDelete((MemoryContext) new_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	*link = (FasttrunXactFrame *) new_frame;
	return *link;
}

/* Discard preallocated parent merge state when a prepared frame aborts. */
static void
fasttrun_xact_discard_prepared_subcommit(FasttrunXactFrame *frame)
{
	int			i;

	if (frame->subcommit_inserted != NULL)
	{
		if (frame->parent != NULL && frame->parent->entries != NULL)
		{
			for (i = 0; i < frame->subcommit_ninserted; i++)
			{
				FasttrunXactRelEntry *dst;

				dst = (FasttrunXactRelEntry *)
					hash_search(frame->parent->entries,
								&frame->subcommit_inserted[i],
								HASH_FIND, NULL);
				if (dst != NULL && dst->flags == 0 &&
					!dst->level_size_known && dst->dropped_attrs == NULL)
					(void) hash_search(frame->parent->entries,
									   &frame->subcommit_inserted[i],
									   HASH_REMOVE, NULL);
			}
		}
		pfree(frame->subcommit_inserted);
		frame->subcommit_inserted = NULL;
		frame->subcommit_ninserted = 0;
	}
	if (frame->subcommit_dropped == NULL)
		return;
	for (i = 0; i < frame->subcommit_ndropped; i++)
		bms_free(frame->subcommit_dropped[i].merged);
	pfree(frame->subcommit_dropped);
	frame->subcommit_dropped = NULL;
	frame->subcommit_ndropped = 0;
}

/*
 * Release the frame of a finished subtransaction.  Its entries are cleared and
 * the empty frame, context and hash table included, goes to the pool.  A frame
 * that is too wide, or one arriving at a full pool, is deleted.
 */
static void
fasttrun_xact_frame_release(FasttrunXactFrame *frame)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;
	bool		pool_frame;

	fasttrun_xact_discard_prepared_subcommit(frame);

	pool_frame =
		(fasttrun_xact_frame_pool_used < FASTTRUN_XACT_FRAME_POOL_MAX &&
		 hash_get_num_entries(frame->entries) <=
		 FASTTRUN_XACT_FRAME_POOL_ENTRIES_MAX);

	/*
	 * dropped_attrs bitmaps live in TopTransactionContext rather than
	 * frame->mcxt, so release them whether the frame is pooled or deleted.
	 * Deleting the just-returned entry during a seq scan is allowed.
	 */
	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		if (xentry->dropped_attrs != NULL)
		{
			bms_free(xentry->dropped_attrs);
			xentry->dropped_attrs = NULL;
		}
		if (pool_frame)
			(void) hash_search(frame->entries, &xentry->relid,
							   HASH_REMOVE, NULL);
	}

	if (!pool_frame)
	{
		MemoryContextDelete(frame->mcxt);
		return;
	}

	/*
	 * A pooled frame keeps the context name, so mark it instead: the memory
	 * check tells a live frame from a pooled one by this identifier, and can
	 * still demand zero live frames after a rollback.
	 */
	MemoryContextSetIdentifier(frame->mcxt,
							   FASTTRUN_XACT_FRAME_POOLED_IDENT);
	frame->subid = InvalidSubTransactionId;
	frame->parent = fasttrun_xact_frame_pool;
	fasttrun_xact_frame_pool = frame;
	fasttrun_xact_frame_pool_used++;
	Assert(fasttrun_xact_frame_pool_length() == fasttrun_xact_frame_pool_used);
}

static FasttrunXactFrame *
fasttrun_xact_frame_for_current(bool create)
{
	return fasttrun_xact_frame_for_subid(GetCurrentSubTransactionId(), create);
}

/* Mark the level that owns a table or storage lifetime, not its first use. */
static FasttrunXactRelEntry *
fasttrun_xact_mark_relation_at_level(Oid relid, SubTransactionId subid,
								   uint32 flags)
{
	FasttrunXactFrame *frame;
	FasttrunXactRelEntry *entry;
	bool		found;

	if (subid == InvalidSubTransactionId)
		return NULL;

	frame = fasttrun_xact_frame_for_subid(subid, true);
	entry = (FasttrunXactRelEntry *)
		hash_search(frame->entries, &relid, HASH_ENTER, &found);
	if (!found)
	{
		entry->root_relid = relid;
		entry->flags = 0;
		entry->level_size_known = false;
		entry->settle_owner = InvalidOid;
		entry->dropped_attrs = NULL;
	}
	entry->flags |= flags;
	return entry;
}

/* Preserve an observed birth before a deeper rollback clears rd_new*. */
static void
fasttrun_xact_note_file_birth(Relation rel)
{
	FasttrunXactRelEntry *entry;
	SubTransactionId subid = rel->rd_newRelfilelocatorSubid;
	SubTransactionId create_file_subid = InvalidSubTransactionId;

	if (subid == InvalidSubTransactionId &&
		fasttrun_rel_on_create_file(rel, &create_file_subid))
		subid = create_file_subid;

	entry = fasttrun_xact_mark_relation_at_level(RelationGetRelid(rel),
											subid,
											FASTTRUN_TOUCH_POISON_FILE);
	if (entry != NULL)
		entry->file_birth_locator = rel->rd_locator;
}

static inline TransactionId
fasttrun_tuple_xmin(HeapTuple tup)
{
#if defined(TANTOR_VERSION) || defined(PGPRO_EE)
	return HeapTupleGetXmin(tup);
#else
	return HeapTupleHeaderGetXmin(tup->t_data);
#endif
}

static bool
fasttrun_tuple_committed_before_xact(HeapTuple tup)
{
	HeapTupleHeader hdr = tup->t_data;
	TransactionId xmin;

	if (HeapTupleHeaderXminInvalid(hdr))
		return false;
	if (HeapTupleHeaderXminFrozen(hdr))
		return true;
	xmin = fasttrun_tuple_xmin(tup);
	if (!TransactionIdIsValid(xmin) ||
		TransactionIdIsCurrentTransactionId(xmin))
		return false;
	if (HeapTupleHeaderXminCommitted(hdr))
		return true;
	if (!TransactionIdIsNormal(xmin))
		return TransactionIdEquals(xmin, BootstrapTransactionId) ||
			TransactionIdEquals(xmin, FrozenTransactionId);
	if (TransactionIdIsInProgress(xmin))
		return false;
	return TransactionIdDidCommit(xmin);
}

static bool
fasttrun_pg_class_matches_locator(Form_pg_class form, RelFileLocator locator)
{
	Oid			tup_spc = OidIsValid(form->reltablespace)
		? form->reltablespace : MyDatabaseTableSpace;
	RelFileNumber tup_num = OidIsValid(form->relfilenode)
		? form->relfilenode : (RelFileNumber) form->oid;

	return tup_spc == locator.spcOid && tup_num == locator.relNumber;
}

static bool
fasttrun_rel_locator_committed_before_xact(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	HeapTuple	tp;
	Relation	pg_class_rel;
	SysScanDesc scan;
	ScanKeyData skey;
	bool		committed = false;

	if (fasttrun_rel_create_subid(rel) != InvalidSubTransactionId ||
		rel->rd_newRelfilelocatorSubid != InvalidSubTransactionId ||
		rel->rd_firstRelfilelocatorSubid != InvalidSubTransactionId)
		return false;
	if (!fasttrun_outer_xact_saw_atx)
		return true;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class form = (Form_pg_class) GETSTRUCT(tp);
		bool		ok = fasttrun_tuple_committed_before_xact(tp) &&
			fasttrun_pg_class_matches_locator(form, rel->rd_locator);

		ReleaseSysCache(tp);
		if (ok)
			return true;
	}

	/*
	 * Non-rewriting DDL in the outer transaction (e.g. CREATE INDEX, GRANT or
	 * RENAME) updates pg_class with the current XID while keeping the old
	 * RelFileLocator.  Check committed pg_class versions of this OID.
	 */
	pg_class_rel = table_open(RelationRelationId, AccessShareLock);
	ScanKeyInit(&skey, Anum_pg_class_oid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(pg_class_rel, ClassOidIndexId, true,
							  SnapshotAny, 1, &skey);
	while (HeapTupleIsValid(tp = systable_getnext(scan)))
	{
		Form_pg_class form = (Form_pg_class) GETSTRUCT(tp);

		if (fasttrun_tuple_committed_before_xact(tp) &&
			fasttrun_pg_class_matches_locator(form, rel->rd_locator))
		{
			committed = true;
			break;
		}
	}
	systable_endscan(scan);
	table_close(pg_class_rel, AccessShareLock);
	return committed;
}

static SubTransactionId
fasttrun_xact_file_birth(Relation rel, bool *known)
{
	FasttrunXactFrame *frame;
	Oid			relid = RelationGetRelid(rel);
	SubTransactionId create_subid = fasttrun_rel_create_subid(rel);
	SubTransactionId create_file_subid = InvalidSubTransactionId;

	*known = true;
	/* Committed files need no journal lookup on the ordinary truncate path. */
	if (!fasttrun_outer_xact_saw_atx &&
		rel->rd_newRelfilelocatorSubid == InvalidSubTransactionId &&
		rel->rd_firstRelfilelocatorSubid == InvalidSubTransactionId &&
		create_subid == InvalidSubTransactionId)
		return InvalidSubTransactionId;
	for (frame = fasttrun_xact_frame; frame != NULL; frame = frame->parent)
	{
		FasttrunXactRelEntry *entry;

		entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &relid, HASH_FIND, NULL);
		if (entry != NULL &&
			(entry->flags & FASTTRUN_TOUCH_POISON_FILE) != 0 &&
			RelFileLocatorEquals(entry->file_birth_locator, rel->rd_locator))
			return frame->subid;
	}
	fasttrun_xact_note_file_birth(rel);
	if (rel->rd_newRelfilelocatorSubid != InvalidSubTransactionId)
		return rel->rd_newRelfilelocatorSubid;
	if (fasttrun_rel_on_create_file(rel, &create_file_subid))
		return create_file_subid;
	if (fasttrun_outer_xact_saw_atx &&
		fasttrun_rel_locator_committed_before_xact(rel))
		return InvalidSubTransactionId;
	*known = false;
	return InvalidSubTransactionId;
}

static void
fasttrun_xact_mark_created_relation(Relation rel)
{
	fasttrun_xact_mark_relation_at_level(RelationGetRelid(rel),
										fasttrun_rel_create_subid(rel),
										FASTTRUN_TOUCH_CREATED);
}

static bool
fasttrun_xact_component_can_return(Oid relid, SubTransactionId file_subid)
{
	FasttrunXactFrame *frame;

	for (frame = fasttrun_xact_frame; frame != NULL && frame->subid > file_subid;
		 frame = frame->parent)
	{
		FasttrunXactRelEntry *entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &relid, HASH_FIND, NULL);

		if (entry != NULL &&
			(entry->flags & FASTTRUN_TOUCH_DROPPED_COMPONENT) != 0)
			return true;
	}
	return false;
}

static bool
fasttrun_xact_statistics_can_return(Oid relid)
{
	FasttrunXactFrame *frame;

	if (!fasttrun_xact_dropped_statext)
		return false;
	for (frame = fasttrun_xact_frame; frame != NULL; frame = frame->parent)
	{
		FasttrunXactRelEntry *entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &relid, HASH_FIND, NULL);

		if (entry != NULL && (entry->flags & FASTTRUN_TOUCH_DROPPED_STATEXT) != 0)
			return true;
	}
	return false;
}

static FasttrunXactRelEntry *
fasttrun_xact_mark_relid(Oid relid, Oid root_relid, uint32 flags)
{
	FasttrunXactFrame *frame;
	FasttrunXactRelEntry *entry;
	bool		found;

	if (!OidIsValid(relid))
		return NULL;
	if (!OidIsValid(root_relid))
		root_relid = relid;

	frame = fasttrun_xact_frame_for_current(true);
	entry = (FasttrunXactRelEntry *)
		hash_search(frame->entries, &relid, HASH_ENTER, &found);
	if (!found)
	{
		entry->root_relid = root_relid;
		entry->flags = 0;
		entry->level_size_known = false;
		entry->settle_owner = InvalidOid;
		entry->dropped_attrs = NULL;
	}
	else if (OidIsValid(root_relid))
		entry->root_relid = root_relid;
	entry->flags |= flags;

	/* A root entry makes abort-time plan invalidation allocation-free. */
	if ((flags & FASTTRUN_TOUCH_PLAN_INVALIDATE) != 0 &&
		root_relid != relid)
	{
		FasttrunXactRelEntry *root_entry;

		root_entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &root_relid, HASH_ENTER, &found);
		if (!found)
		{
			root_entry->root_relid = root_relid;
			root_entry->flags = 0;
			root_entry->level_size_known = false;
			root_entry->settle_owner = InvalidOid;
			root_entry->dropped_attrs = NULL;
		}
		root_entry->flags |= FASTTRUN_TOUCH_PLAN_INVALIDATE;
	}

	return entry;
}

/*
 * Add flags to a relid this subtransaction has already marked.  Allocation-free
 * for callers past the point where an error can still be rolled back.
 */
static void
fasttrun_xact_flag_marked_relid(Oid relid, uint32 flags)
{
	FasttrunXactFrame *frame = fasttrun_xact_frame_for_current(false);
	FasttrunXactRelEntry *entry = NULL;

	if (frame != NULL)
		entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &relid, HASH_FIND, NULL);
	Assert(entry != NULL);
	if (entry != NULL)
		entry->flags |= flags;
}

/* The plan saw statistics hidden by DML at these levels. */
static void
fasttrun_xact_mark_dml_plan_dependency(Oid relid)
{
	FasttrunXactFrame *frame;

	if (!fasttrun_in_planner)
		return;
	for (frame = fasttrun_xact_frame; frame != NULL; frame = frame->parent)
	{
		FasttrunXactRelEntry *entry;

		entry = (FasttrunXactRelEntry *)
			hash_search(frame->entries, &relid, HASH_FIND, NULL);
		if (entry == NULL || (entry->flags & FASTTRUN_TOUCH_DML) == 0)
			continue;
		entry->flags |= FASTTRUN_TOUCH_PLAN_INVALIDATE;
		if (entry->root_relid != entry->relid)
		{
			FasttrunXactRelEntry *root_entry;

			root_entry = (FasttrunXactRelEntry *)
				hash_search(frame->entries, &entry->root_relid,
							HASH_FIND, NULL);
			if (root_entry != NULL)
				root_entry->flags |= FASTTRUN_TOUCH_PLAN_INVALIDATE;
		}
		return;
	}
}

/*
 * Reserve parent journal entries and merged dropped-attribute bitmaps during
 * SUBXACT_EVENT_PRE_COMMIT_SUB while the subtransaction is still in
 * TRANS_INPROGRESS, before CommitSubTransaction() records subcommit or merges
 * local invalidation lists.  Newly inserted parent entries are removed if
 * preparation fails or a later pre-commit step aborts the subtransaction.
 */
static void
fasttrun_xact_prepare_subcommit(FasttrunXactFrame *parent,
								FasttrunXactFrame *child)
{
	HASH_SEQ_STATUS status;
	volatile bool seq_open = false;
	FasttrunSubcommitDropped * volatile dropped = NULL;
	int volatile ndropped = 0;
	int			drop_cap = 0;
	Oid		   * volatile inserted = NULL;
	int volatile ninserted = 0;
	int			ins_cap = 0;
	MemoryContext caller_cxt = CurrentMemoryContext;

	fasttrun_xact_discard_prepared_subcommit(child);

	PG_TRY();
	{
		FasttrunXactRelEntry *xentry;

		hash_seq_init(&status, child->entries);
		seq_open = true;
		while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
		{
			FasttrunXactRelEntry *dst;
			bool		found;

			dst = (FasttrunXactRelEntry *)
				hash_search(parent->entries, &xentry->relid, HASH_FIND, NULL);
			if (dst == NULL)
			{
				if (ninserted == ins_cap)
				{
					MemoryContext oldcxt =
						MemoryContextSwitchTo(TopTransactionContext);

					ins_cap = (ins_cap == 0) ? 8 : ins_cap * 2;
					if (inserted == NULL)
						inserted = (Oid *) palloc(sizeof(Oid) * ins_cap);
					else
						inserted = (Oid *)
							repalloc(inserted, sizeof(Oid) * ins_cap);
					MemoryContextSwitchTo(oldcxt);
				}
				dst = (FasttrunXactRelEntry *)
					hash_search(parent->entries, &xentry->relid,
								HASH_ENTER, &found);
				Assert(!found);
				if (!found)
				{
					memset(dst, 0, sizeof(*dst));
					dst->relid = xentry->relid;
					dst->root_relid = xentry->relid;
					inserted[ninserted] = xentry->relid;
					ninserted++;
				}
			}
			if (xentry->dropped_attrs != NULL)
			{
				MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);
				Bitmapset  *merged;

				if (ndropped == drop_cap)
				{
					drop_cap = (drop_cap == 0) ? 4 : drop_cap * 2;
					if (dropped == NULL)
						dropped = (FasttrunSubcommitDropped *)
							palloc(sizeof(FasttrunSubcommitDropped) * drop_cap);
					else
						dropped = (FasttrunSubcommitDropped *)
							repalloc(dropped,
									 sizeof(FasttrunSubcommitDropped) * drop_cap);
				}
				merged = bms_union(dst->dropped_attrs, xentry->dropped_attrs);
				dropped[ndropped].relid = xentry->relid;
				dropped[ndropped].merged = merged;
				ndropped++;
				MemoryContextSwitchTo(oldcxt);
			}
			FASTTRUN_TEST_FAILPOINT("subcommit_merge", 0);
		}
		seq_open = false;
	}
	PG_CATCH();
	{
		int			i;

		MemoryContextSwitchTo(caller_cxt);
		if (seq_open)
			hash_seq_term(&status);
		for (i = 0; i < ninserted; i++)
			(void) hash_search(parent->entries, &inserted[i],
							   HASH_REMOVE, NULL);
		if (inserted != NULL)
			pfree(inserted);
		for (i = 0; i < ndropped; i++)
			bms_free(dropped[i].merged);
		if (dropped != NULL)
			pfree(dropped);
		PG_RE_THROW();
	}
	PG_END_TRY();

	child->subcommit_dropped = dropped;
	child->subcommit_ndropped = ndropped;
	child->subcommit_inserted = inserted;
	child->subcommit_ninserted = ninserted;
}

/* Allocation-free commit merge after fasttrun_xact_prepare_subcommit. */
static void
fasttrun_xact_apply_subcommit(FasttrunXactFrame *parent,
							  FasttrunXactFrame *child)
{
	HASH_SEQ_STATUS status;
	FasttrunXactRelEntry *xentry;
	FasttrunSubcommitDropped *dropped = child->subcommit_dropped;
	int			ndropped = child->subcommit_ndropped;
	int			drop_idx = 0;

	if (child->subcommit_inserted != NULL)
	{
		pfree(child->subcommit_inserted);
		child->subcommit_inserted = NULL;
		child->subcommit_ninserted = 0;
	}
	child->subcommit_dropped = NULL;
	child->subcommit_ndropped = 0;

	hash_seq_init(&status, child->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
	{
		FasttrunXactRelEntry *dst;

		dst = (FasttrunXactRelEntry *)
			hash_search(parent->entries, &xentry->relid, HASH_FIND, NULL);
		Assert(dst != NULL);
		if (dst == NULL)
		{
			if (xentry->dropped_attrs != NULL)
			{
				bms_free(xentry->dropped_attrs);
				xentry->dropped_attrs = NULL;
			}
			continue;
		}

		/* A component's birth mark does not replace an established owning heap. */
		if (xentry->root_relid != xentry->relid || dst->root_relid == dst->relid)
			dst->root_relid = xentry->root_relid;
		dst->flags |= xentry->flags;
		if ((xentry->flags & FASTTRUN_TOUCH_POISON_FILE) != 0)
			dst->file_birth_locator = xentry->file_birth_locator;
		if (xentry->dropped_attrs != NULL)
		{
			Assert(dropped != NULL && drop_idx < ndropped &&
				   dropped[drop_idx].relid == xentry->relid);
			if (dropped != NULL && drop_idx < ndropped &&
				dropped[drop_idx].relid == xentry->relid)
			{
				if (dst->dropped_attrs != NULL)
					bms_free(dst->dropped_attrs);
				bms_free(xentry->dropped_attrs);
				xentry->dropped_attrs = NULL;
				dst->dropped_attrs = dropped[drop_idx].merged;
				drop_idx++;
			}
			else if (dst->dropped_attrs == NULL)
			{
				dst->dropped_attrs = xentry->dropped_attrs;
				xentry->dropped_attrs = NULL;
			}
			else
			{
				bms_free(xentry->dropped_attrs);
				xentry->dropped_attrs = NULL;
			}
		}
		/* The parent level's first write may have happened in the child. */
		if (!dst->level_size_known && xentry->level_size_known)
		{
			dst->level_size_known = true;
			dst->level_pages = xentry->level_pages;
			dst->level_dead = xentry->level_dead;
			dst->level_locator = xentry->level_locator;
		}
	}
	while (dropped != NULL && drop_idx < ndropped)
	{
		bms_free(dropped[drop_idx].merged);
		drop_idx++;
	}
	if (dropped != NULL)
		pfree(dropped);
}

static bool
fasttrun_xact_entry_dropped(const FasttrunXactRelEntry *entry)
{
	return (entry->flags & FASTTRUN_TOUCH_DROPPED) != 0;
}

/* The commit walk removes the entries of this relid itself. */
static bool
fasttrun_xact_relid_dropped(Oid relid)
{
	FasttrunXactRelEntry *entry;

	entry = (FasttrunXactRelEntry *)
		hash_search(fasttrun_xact_frame->entries, &relid, HASH_FIND, NULL);
	return entry != NULL && fasttrun_xact_entry_dropped(entry);
}

