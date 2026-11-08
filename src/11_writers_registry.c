/*-------------------------------------------------------------------------
 *
 * src/11_writers_registry.c
 *      Registry of started writing statements.
 *
 * What it holds.  The short registry of pairs (temporary relation,
 * subtransaction) for statements that started writing and have not finished,
 * its overflow flag and the subtransaction-end handling.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/11_writers_registry.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Registry of statements writing a temporary relation that have started and
 * not finished.
 *
 * It has to live outside the stats cache: the very first collect of a relation
 * happens while no cache entry exists yet, and that collect still must not take
 * the column mask as a full account if an UPDATE is running underneath it.
 * Permanent relations are never collected and never take a slot.  A statement
 * can have several targets (a modifying CTE, an inheritance or partition
 * tree), and statements nest when a trigger or a function runs one inside
 * another.  Both are usually small, so a short array beats a hash.  Overflow
 * is answered conservatively rather than grown.
 */
#define FASTTRUN_IN_FLIGHT_SLOTS	16

typedef struct FasttrunInFlightSlot
{
	Oid			relid;
	int			depth;
	SubTransactionId subid;		/* subtransaction the statement started in */
} FasttrunInFlightSlot;

static FasttrunInFlightSlot fasttrun_in_flight[FASTTRUN_IN_FLIGHT_SLOTS];
static int	fasttrun_in_flight_used = 0;

/*
 * The registry ran out of slots, so it does not hold the whole truth for this
 * transaction.  Every relation is then answered as written.
 */
static bool fasttrun_in_flight_incomplete = false;


/*
 * The registry filter.  It must answer the same at a statement's start and end
 * so the count balances.  No relation becomes or stops being temporary in
 * between.  A relation the syscache does not know is not temporary.
 */
static bool
fasttrun_relid_is_temp(Oid relid)
{
	HeapTuple	tp;
	bool		result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		return false;
	result = (((Form_pg_class) GETSTRUCT(tp))->relpersistence ==
			  RELPERSISTENCE_TEMP);
	ReleaseSysCache(tp);
	return result;
}

static void
fasttrun_stats_update_in_flight(Oid relid, int delta)
{
	SubTransactionId subid = GetCurrentSubTransactionId();
	int			i;
	int			fallback = -1;

	/*
	 * One slot per (relation, subtransaction).  Keeping the subtransaction in
	 * the key is what lets a nested statement that raised an error be dropped
	 * on its own, without touching the outer statement writing the same
	 * relation.  A statement normally starts and ends in the same
	 * subtransaction.  The fallback covers the case where its subtransaction
	 * committed while it was still running.
	 */
	for (i = 0; i < fasttrun_in_flight_used; i++)
	{
		if (fasttrun_in_flight[i].relid != relid)
			continue;
		if (fasttrun_in_flight[i].subid != subid)
		{
			if (fallback < 0)
				fallback = i;
			continue;
		}
		fasttrun_in_flight[i].depth += delta;
		if (fasttrun_in_flight[i].depth <= 0)
			fasttrun_in_flight[i] = fasttrun_in_flight[--fasttrun_in_flight_used];
		return;
	}

	if (delta <= 0)
	{
		if (fallback < 0)
			return;				/* end of a statement whose start overflowed */
		fasttrun_in_flight[fallback].depth += delta;
		if (fasttrun_in_flight[fallback].depth <= 0)
			fasttrun_in_flight[fallback] =
				fasttrun_in_flight[--fasttrun_in_flight_used];
		return;
	}

	if (fasttrun_in_flight_used >= FASTTRUN_IN_FLIGHT_SLOTS)
	{
		if (!fasttrun_in_flight_incomplete)
			fasttrun_count(FTC_WRITER_REGISTRY_OVERFLOWS);
		fasttrun_in_flight_incomplete = true;
		return;
	}

	fasttrun_in_flight[fasttrun_in_flight_used].relid = relid;
	fasttrun_in_flight[fasttrun_in_flight_used].depth = 1;
	fasttrun_in_flight[fasttrun_in_flight_used].subid = subid;
	fasttrun_in_flight_used++;
}

static bool
fasttrun_stats_relid_in_flight(Oid relid)
{
	int			i;

	if (fasttrun_in_flight_incomplete)
		return true;

	for (i = 0; i < fasttrun_in_flight_used; i++)
	{
		if (fasttrun_in_flight[i].relid == relid)
			return true;
	}
	return false;
}

/*
 * True when this relation's data and the counters beside it describe different
 * moments, so a sample taken now cannot be compared with anything later.
 *
 * Two ways that happens:
 *
 *   * a statement writing the relation is running.  The registry knows every
 *     statement whose start was observed, nested ones included, and every
 *     temporary DML target of it.  Permanent targets are left out, since only
 *     a temporary relation is ever sampled.  The observed starts are every
 *     statement at all, because the per-column account only runs in a backend
 *     that preloaded the library.
 *   * a write already finished under a snapshot older than it.  The scan then
 *     runs on that older snapshot and does not see rows the counters count:
 *     an inner function's UPDATE, or an earlier command of this transaction
 *     when the current statement kept the old snapshot.
 */
static bool
fasttrun_stats_relation_written_now(Relation rel)
{
	int64		ins = 0;
	int64		upd = 0;
	int64		del = 0;
	bool		truncdropped = false;

	if (fasttrun_stats_relid_in_flight(RelationGetRelid(rel)))
		return true;

	/*
	 * Writes of this transaction that the scan's snapshot cannot show.  Both
	 * halves are needed: without the counters an unrelated statement moving
	 * the command id would forbid a perfectly good sample, and without the
	 * snapshot age an ordinary collect after a finished write of this relation
	 * would be forbidden although the scan sees every row of it.
	 */
	return (ActiveSnapshotSet() &&
			GetActiveSnapshot()->curcid < GetCurrentCommandId(false) &&
			fasttrun_read_pgstat_counters(rel, &ins, &upd, &del, &truncdropped) &&
			(ins != 0 || upd != 0 || del != 0 || truncdropped));
}

/* Nothing is in flight once the top-level transaction is over. */
static void
fasttrun_stats_forget_in_flight(void)
{
	fasttrun_in_flight_used = 0;
	fasttrun_in_flight_incomplete = false;
}

/*
 * A subtransaction ending takes its statements with it.  On abort that is the
 * statement caught by a PL/pgSQL EXCEPTION block: it never reaches the end
 * hook, so without this its entry would hold every later collect of that
 * relation at the conservative answer for the rest of the transaction.  On
 * commit the entry moves to the parent, which is the subtransaction that owns
 * it from then on.  Statements started outside the ending subtransaction and
 * still running keep their entries.
 *
 * Entries are per (relation, subtransaction), so a nested statement on a
 * relation an outer statement is already writing is dropped on its own.
 */
static void
fasttrun_stats_in_flight_subxact_end(SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 bool aborted)
{
	int			i = 0;

	while (i < fasttrun_in_flight_used)
	{
		if (fasttrun_in_flight[i].subid != mySubid)
		{
			i++;
			continue;
		}
		if (!aborted)
		{
			fasttrun_in_flight[i].subid = parentSubid;
			i++;
			continue;
		}
		fasttrun_in_flight[i] = fasttrun_in_flight[--fasttrun_in_flight_used];
	}
}

/*
 * DDL that can add a writer working outside a statement's target list must take
 * trust away from the column mask before the next UPDATE runs -- after COMMIT
 * the counters are gone and the evidence with them.  CREATE TRIGGER calls it,
 * and so does every ALTER TABLE that keeps the statistics
 * (fasttrun_keep_stats_through_ddl), since it may enable a trigger or add a
 * stored generated column.  An ALTER TABLE that evicts needs nothing more.  The
 * planner-path re-read stays as a second line.
 */
static void
fasttrun_stats_note_wide_writer_ddl(RangeVar *rv)
{
	Oid			relid;
	FasttrunStatsRelidEntry *entry;

	if (fasttrun_stats_relid_cache == NULL || rv == NULL)
		return;

	relid = RangeVarGetRelid(rv, NoLock, true);
	if (!OidIsValid(relid))
		return;

	entry = (FasttrunStatsRelidEntry *)
		hash_search(fasttrun_stats_relid_cache, &relid, HASH_FIND, NULL);
	if (entry != NULL)
		entry->updated_cols_exact = false;
}

/*
 * True when an UPDATE may have written this column since its sample was taken.
 * Answers conservatively -- an over-reported column merely loses its statistics
 * -- whenever the mask cannot account for the change:
 *
 *   * the relation can change columns a statement does not name.
 *   * the sample predates the current collect period, so the mask, which was
 *     cleared at that collect, says nothing about it.  This covers a sample
 *     restored by savepoint rollback and a column the collect skipped.
 *   * the mask is empty while the counters say an UPDATE happened, so the
 *     write did not reach the executor hook.  A write that mixes with an
 *     already recorded UPDATE is not detected -- see the limitation on
 *     writes that bypass the executor in docs/ru/limitations.md and
 *     docs/en/limitations.md.
 */
static bool
fasttrun_stats_column_was_updated(FasttrunStatsRelidEntry *relentry,
								  const FasttrunStatsEntry *entry)
{
	if (relentry == NULL || !relentry->updated_cols_exact)
		return true;

	/* An index expression may read any column: every UPDATE counts. */
	if (OidIsValid(entry->key.indexoid))
		return true;

	if (entry->collect_seq != relentry->collect_seq)
		return true;

	if (relentry->updated_cols == NULL)
		return true;

	/* A whole-row reference names every column. */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
					  relentry->updated_cols))
		return true;

	return bms_is_member(entry->key.attnum - FirstLowInvalidHeapAttributeNumber,
						 relentry->updated_cols);
}

