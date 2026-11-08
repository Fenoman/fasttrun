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
	FASTTRUN_TOUCH_POISON_RETIRE = 1 << 9
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
	Oid			settle_owner;	/* abort walk: the heap of the index row it
								 * reached, settled after the walk */
	Bitmapset  *dropped_attrs;	/* columns dropped at this level, in
								 * TopTransactionContext */
} FasttrunXactRelEntry;

typedef struct FasttrunXactFrame
{
	SubTransactionId subid;
	MemoryContext mcxt;
	HTAB	   *entries;
	struct FasttrunXactFrame *parent;
} FasttrunXactFrame;

static FasttrunXactFrame *fasttrun_xact_frame = NULL;

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

	if (fasttrun_xact_frame_pool_used >= FASTTRUN_XACT_FRAME_POOL_MAX ||
		hash_get_num_entries(frame->entries) >
		FASTTRUN_XACT_FRAME_POOL_ENTRIES_MAX)
	{
		MemoryContextDelete(frame->mcxt);
		return;
	}

	/* Deleting the just-returned entry during a seq scan is allowed. */
	hash_seq_init(&status, frame->entries);
	while ((xentry = (FasttrunXactRelEntry *) hash_seq_search(&status)) != NULL)
		(void) hash_search(frame->entries, &xentry->relid, HASH_REMOVE, NULL);

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

/* A descendant can abort its first use while the table's birth stays live. */
static void
fasttrun_xact_mark_created_relation(Relation rel)
{
	FasttrunXactFrame *frame;
	FasttrunXactRelEntry *entry;
	Oid			relid = RelationGetRelid(rel);
	bool		found;

	if (rel->rd_createSubid == InvalidSubTransactionId)
		return;

	frame = fasttrun_xact_frame_for_subid(rel->rd_createSubid, true);
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
	entry->flags |= FASTTRUN_TOUCH_CREATED;
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

static void
fasttrun_xact_merge_entry(FasttrunXactFrame *parent,
						  FasttrunXactRelEntry *child)
{
	FasttrunXactRelEntry *dst;
	bool		found;

	dst = (FasttrunXactRelEntry *)
		hash_search(parent->entries, &child->relid, HASH_ENTER, &found);
	if (!found)
	{
		*dst = *child;
		return;
	}

	dst->root_relid = child->root_relid;
	dst->flags |= child->flags;
	if (child->dropped_attrs != NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		dst->dropped_attrs = bms_add_members(dst->dropped_attrs,
											 child->dropped_attrs);
		MemoryContextSwitchTo(oldcxt);
	}
	/* The parent level's first write may have happened in the child. */
	if (!dst->level_size_known && child->level_size_known)
	{
		dst->level_size_known = true;
		dst->level_pages = child->level_pages;
		dst->level_dead = child->level_dead;
		dst->level_locator = child->level_locator;
	}
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

