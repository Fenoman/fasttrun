/*-------------------------------------------------------------------------
 *
 * src/21_sample.c
 *      Full scans, block sampling and refresh.
 *
 * What it holds.  The full scan with a reservoir, the block sampler built on
 * the core BlockSampler, the choice between them and the refresh of a sample.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/21_sample.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/*
 * Sequential scan + reservoir sampling.  Returns exact row count via
 * *tuples_out and up to sample_target tuples via the pre-allocated
 * sample[] array (heap_copytuple'd in the current memory context).
 *
 * A fasttrun_analyze scan that has to count rows, at or below
 * fasttrun.max_analyze_pages, comes here through fasttrun_sample_for_analyze(),
 * and fasttrun_collect_stats() calls it directly at any size.
 */
static void
fasttrun_scan_with_sample(Relation rel, int64 *tuples_out,
						  HeapTuple *sample, int sample_target,
						  int *sample_count_out)
{
	TableScanDesc	scan;
	Snapshot		snap;
	HeapTuple		tuple;
	int64			tuples_count = 0;
	int				sample_count = 0;
	double			samplerows = 0;
	double			rowstoskip = -1;
	ReservoirStateData rstate;

	if (sample != NULL && sample_target > 0)
		reservoir_init_selection_state(&rstate, sample_target);

	snap = GetActiveSnapshot();
	if (snap == NULL)
		snap = GetTransactionSnapshot();

	scan = table_beginscan(rel, snap, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		tuples_count++;

		if (sample == NULL || sample_target <= 0)
			continue;

		if (sample_count < sample_target)
			sample[sample_count++] = heap_copytuple(tuple);
		else
		{
			/* Vitter skip distances: one RNG draw per replacement, not per row. */
			if (rowstoskip < 0)
				rowstoskip = reservoir_get_next_S(&rstate, samplerows,
												  sample_target);

			if (rowstoskip <= 0)
			{
				int		k = (int) (sample_target *
								   sampler_random_fract(&rstate.randstate));

				Assert(k >= 0 && k < sample_target);
				heap_freetuple(sample[k]);
				sample[k] = heap_copytuple(tuple);
			}
			rowstoskip -= 1;
		}
		samplerows += 1;
	}
	table_endscan(scan);

	*tuples_out = tuples_count;
	*sample_count_out = sample_count;
}

/*
 * Reservoir bookkeeping for one analyzed block: pull every live tuple the
 * analyze scan yields (which also advances *liverows for the density
 * estimate) and, when the caller wants a column-stats sample, keep a Vitter
 * reservoir of sample_target rows.  Shared by the PG16/17 and PG18 outer
 * loops, which only differ in how blocks are fed to the scan.
 */
static void
fasttrun_reservoir_collect_block(TableScanDesc scan, TransactionId OldestXmin,
								 TupleTableSlot *slot, HeapTuple *sample,
								 int sample_target, int *numrows,
								 double *samplerows, double *rowstoskip,
								 double *liverows, double *deadrows,
								 ReservoirState rstate)
{
	while (table_scan_analyze_next_tuple(scan, OldestXmin,
										 liverows, deadrows, slot))
	{
		if (sample != NULL)
		{
			if (*numrows < sample_target)
				sample[(*numrows)++] = ExecCopySlotHeapTuple(slot);
			else
			{
				if (*rowstoskip < 0)
					*rowstoskip = reservoir_get_next_S(rstate, *samplerows,
													   sample_target);

				if (*rowstoskip <= 0)
				{
					int		k = (int) (sample_target *
									   sampler_random_fract(&rstate->randstate));

					Assert(k >= 0 && k < sample_target);
					heap_freetuple(sample[k]);
					sample[k] = ExecCopySlotHeapTuple(slot);
				}
				*rowstoskip -= 1;
			}
		}
		*samplerows += 1;
	}
}

#if PG_VERSION_NUM >= 170000
/* ReadStream callback: hand out BlockSampler-chosen blocks (PG17+ analyze). */
static BlockNumber
fasttrun_block_sample_stream_next(ReadStream *stream, void *callback_private_data,
								  void *per_buffer_data)
{
	BlockSampler	bs = (BlockSampler) callback_private_data;

	return BlockSampler_HasMore(bs) ? BlockSampler_Next(bs) : InvalidBlockNumber;
}
#endif

/*
 * Block-sampled variant of fasttrun_scan_with_sample for giant temp tables
 * (heap pages > fasttrun.max_analyze_pages).  Reads only a bounded random
 * sample of blocks instead of the whole relation and estimates the row count
 * from tuple density, like core acquire_sample_rows(), so the analyze cost
 * stays O(sample) rather than O(table).  *tuples_out is an ESTIMATE (like a
 * regular ANALYZE), not an exact count.
 *
 * Ported from commands/analyze.c on the public block-sampling API
 * (BlockSampler_* + table_scan_analyze_next_block/tuple).  PG17+ feeds the
 * blocks through a ReadStream as core does, PG16 reads them one by one
 * without core's prefetch.  Progress reporting is dropped.  The reservoir
 * and density math match core.
 */
static void
fasttrun_block_sample_rows(Relation rel, int64 *tuples_out,
						   HeapTuple *sample, int sample_target,
						   int *sample_count_out)
{
	int				numrows = 0;
	double			samplerows = 0;
	double			liverows = 0;
	double			deadrows = 0;
	double			rowstoskip = -1;
	uint32			randseed;
	BlockNumber		totalblocks;
	TransactionId	OldestXmin;
	BlockSamplerData bs;
	ReservoirStateData rstate;
	TupleTableSlot *slot;
	TableScanDesc	scan;
	BufferAccessStrategy strategy;

	elog(DEBUG1, "fasttrun: block-sampling analyze scan");

	totalblocks = RelationGetNumberOfBlocks(rel);
	OldestXmin = GetOldestNonRemovableTransactionId(rel);
	/*
	 * The core seeds the global state in every process at its start, and
	 * ANALYZE takes its seed from it too.  A state seeded in _PG_init would
	 * be inherited from the postmaster under shared_preload_libraries on fork
	 * builds, the same sequence in every backend.
	 */
	randseed = pg_prng_uint32(&pg_global_prng_state);
	(void) BlockSampler_Init(&bs, totalblocks, sample_target, randseed);
	reservoir_init_selection_state(&rstate, sample_target);
	strategy = GetAccessStrategy(BAS_VACUUM);

	scan = table_beginscan_analyze(rel);
	slot = table_slot_create(rel, NULL);

#if PG_VERSION_NUM >= 170000
	{
		/* PG17+ drives the sampled-block scan through a ReadStream. */
		ReadStream *stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE,
														strategy, rel,
														MAIN_FORKNUM,
														fasttrun_block_sample_stream_next,
														&bs, 0);

		while (table_scan_analyze_next_block(scan, stream))
		{
			CHECK_FOR_INTERRUPTS();
			fasttrun_reservoir_collect_block(scan, OldestXmin, slot, sample,
											 sample_target, &numrows,
											 &samplerows, &rowstoskip,
											 &liverows, &deadrows, &rstate);
		}

		read_stream_end(stream);
	}
#else
	while (BlockSampler_HasMore(&bs))
	{
		BlockNumber		targblock = BlockSampler_Next(&bs);

		CHECK_FOR_INTERRUPTS();

		if (!table_scan_analyze_next_block(scan, targblock, strategy))
			continue;

		fasttrun_reservoir_collect_block(scan, OldestXmin, slot, sample,
										 sample_target, &numrows,
										 &samplerows, &rowstoskip,
										 &liverows, &deadrows, &rstate);
	}
#endif

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	FreeAccessStrategy(strategy);

	/* Density extrapolation over the sampled blocks (core's estimator). */
	if (bs.m > 0)
		*tuples_out = (int64) floor((liverows / bs.m) * totalblocks + 0.5);
	else
		*tuples_out = 0;

	*sample_count_out = numrows;
}

/*
 * Sample rows for analyze.  Shared by the cold path, the delta-refresh and the
 * partial-index rescan.  fasttrun_collect_stats() does not come here: it always
 * reads the whole heap.
 *
 * Block sampling, as core ANALYZE does, reads at most sample_target blocks
 * and estimates the row count from their density.  The full scan reads every
 * block and counts rows exactly.  The full scan runs when the row count is
 * still unknown and the heap is at or below fasttrun.max_analyze_pages.  When
 * the caller already knows the count exactly (delta-refresh, partial rescan),
 * only the sample is needed, and blocks are sampled once the heap has more of
 * them than the sample target.  A smaller heap would be read whole either
 * way, and the block sampler takes ProcArrayLock to compute the removal
 * horizon, walking every backend, so it is read with the full scan.  A
 * threshold of 0 keeps the full scan everywhere.
 *
 * Returns true when *tuples_out is an exact count from a full scan.
 */
static bool
fasttrun_sample_for_analyze(Relation rel, BlockNumber pages_now,
							HeapTuple *sample, int sample_target,
							int64 *tuples_out, int *sample_count_out,
							bool count_known)
{
	int			block_target = (sample_target > 0) ? sample_target : 3000;

	if (fasttrun_max_analyze_pages > 0 &&
		(count_known ? pages_now > (BlockNumber) block_target :
		 pages_now > (BlockNumber) fasttrun_max_analyze_pages))
	{
		fasttrun_block_sample_rows(rel, tuples_out, sample,
								   block_target, sample_count_out);
		return false;
	}
	/* Fixed text so regression tests can see where a resample read the heap. */
	if (count_known)
		elog(DEBUG1, "fasttrun: full-scan resample");
	fasttrun_scan_with_sample(rel, tuples_out, sample,
							  sample_target, sample_count_out);
	return true;
}

/*
 * Resample for the delta-refresh and the partial-index rescan.  An exact
 * count from the delta needs only the sample: blocks are sampled when the heap
 * is larger than the sample target, and the count stays.  A count that is itself an estimate (a cold pass
 * that sampled blocks) cannot stand in for the scan: it is replaced by
 * whatever the sampler measured, exact or estimated.  Returns true when the
 * count or its exactness changed, so the caller publishes it.
 */
static bool
fasttrun_refresh_sample(Relation rel, BlockNumber pages_now,
						HeapTuple *sample, int sample_target,
						int64 *tuples_count, int *sample_count,
						bool *count_exact)
{
	int64		scanned = 0;
	bool		counted;
	bool		changed;

	counted = fasttrun_sample_for_analyze(rel, pages_now, sample,
										  sample_target, &scanned,
										  sample_count, *count_exact);
	if (!counted && *count_exact)
		return false;
	changed = (*tuples_count != scanned || *count_exact != counted);
	*tuples_count = scanned;
	*count_exact = counted;
	return changed;
}

