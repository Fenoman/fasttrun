/*-------------------------------------------------------------------------
 *
 * src/16_stats_build.c
 *      Haas-Stokes n_distinct and the stats tuple of the light path.
 *
 * What it holds.  The comparator, the n_distinct estimate and the pg_statistic
 * tuple built when fasttrun.use_typanalyze is off.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/16_stats_build.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* ---- Stats sample collection: Haas-Stokes n_distinct + null_frac + width ---- */

/* qsort_arg comparator wrapping a SortSupport. */
static int
fasttrun_datum_cmp(const void *a, const void *b, void *arg)
{
	SortSupport	ssup = (SortSupport) arg;
	Datum		da = *(const Datum *) a;
	Datum		db = *(const Datum *) b;

	return ApplySortComparator(da, false, db, false, ssup);
}

/*
 * Haas-Stokes Duj1 estimator, ported from analyze.c.  Returns
 * stadistinct in pg_statistic semantics (positive = absolute count,
 * negative = -(distinct/N) ratio when close to total).
 */
static float4
fasttrun_estimate_ndistinct(int n_nonnull, int ndistinct, int nmultiple,
							int64 totalrows, double stanullfrac)
{
	int		f1;
	double	n;
	double	N;
	double	estimate;

	if (n_nonnull <= 0)
		return 0.0f;
	if (nmultiple == 0)
	{
		/* No repeats -- assume unique, discounted for nulls (as core). */
		return (float4) (-1.0 * (1.0 - stanullfrac));
	}
	if (ndistinct == nmultiple)
	{
		/* Bounded set: the sample says the column has just these values. */
		estimate = (double) ndistinct;
	}
	else
	{
		/*
		 * Haas-Stokes Duj1, exactly as core compute_scalar_stats():
		 * n*d / (n - f1 + f1*n/N), nulls excluded from both n and N.
		 */
		f1 = ndistinct - nmultiple;
		n = (double) n_nonnull;
		N = (double) totalrows * (1.0 - stanullfrac);

		if (N > 0)
			estimate = (n * (double) ndistinct) /
				((n - (double) f1) + (double) f1 * n / N);
		else
			estimate = 0;

		if (estimate < (double) ndistinct)
			estimate = (double) ndistinct;
		if (estimate > N)
			estimate = N;
	}
	estimate = floor(estimate + 0.5);

	if (estimate > 0.1 * (double) totalrows)
		return (float4) -(estimate / (double) totalrows);

	return (float4) estimate;
}

/*
 * Build a pg_statistic-shaped HeapTuple with only stanullfrac /
 * stawidth / stadistinct filled in.  All slots are empty.
 */
static HeapTuple
fasttrun_build_stats_tuple(TupleDesc pg_stats_desc, Oid relid, AttrNumber attnum,
						   float4 stanullfrac, int32 stawidth, float4 stadistinct)
{
	Datum	values[Natts_pg_statistic];
	bool	nulls[Natts_pg_statistic];
	int		i;

	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stanullfrac);
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stawidth);
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stadistinct);

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		values[Anum_pg_statistic_stakind1 - 1 + i] = Int16GetDatum(0);
		values[Anum_pg_statistic_staop1  - 1 + i] = ObjectIdGetDatum(InvalidOid);
		values[Anum_pg_statistic_stacoll1 - 1 + i] = ObjectIdGetDatum(InvalidOid);
		nulls [Anum_pg_statistic_stanumbers1 - 1 + i] = true;
		nulls [Anum_pg_statistic_stavalues1  - 1 + i] = true;
	}

	return heap_form_tuple(pg_stats_desc, values, nulls);
}

