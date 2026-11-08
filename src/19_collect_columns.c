/*-------------------------------------------------------------------------
 *
 * src/19_collect_columns.c
 *      Column sampling inputs and pg_statistic tuples.
 *
 * What it holds.  Column options and statistics targets (including the target
 * taken from the template table), the column matrix of a sample, examine
 * callbacks for columns and expressions, the pg_statistic tuple and the
 * sample size.
 *
 * Part of fasttrun.c: compiled only through its #include list, the order
 * is part of the build.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTTRUN_UNITY
#error "src/19_collect_columns.c is included only from fasttrun.c"
#endif /* FASTTRUN_UNITY */
/* ----------------------------------------------------------------------
 * Core std_typanalyze integration.
 *
 * When fasttrun.use_typanalyze=on (default), fasttrun reuses PostgreSQL's
 * own per-type typanalyze callbacks to compute n_distinct, MCV,
 * histogram, correlation and any type-specific statistics -- exactly the
 * same code path that a regular ANALYZE would take, only without ever
 * touching pg_statistic on disk.  Output goes into the same per-(relid,
 * attnum) HeapTuple cache that the lightweight Haas-Stokes path uses,
 * and is read by the same get_relation_stats_hook / get_attavgwidth_hook.
 *
 * The three helpers below replicate the parts of commands/analyze.c we
 * need: examine_attribute (set up VacAttrStats + run typanalyze), the
 * standard fetch func, and the formatting logic from update_attstats
 * (build a pg_statistic-shaped HeapTuple, sans catalog write).
 * ---------------------------------------------------------------------- */

/*
 * The n_distinct override of the column, as core ANALYZE applies it.  The
 * options are parsed from the pg_attribute tuple, not taken from the core
 * attoptions cache: on PG16 and PG17 every column invalidation another backend
 * sends walks that cache whole, and a collect over hundreds of columns would
 * keep it large for the rest of the session.
 */
static void
fasttrun_apply_attribute_options(Relation rel, AttrNumber attnum,
								 bool inh, float4 *stadistinct)
{
	HeapTuple	atttuple;
	Datum		datum;
	bool		isnull;
	AttributeOpts *aopt;

	atttuple = SearchSysCache2(ATTNUM,
							   ObjectIdGetDatum(RelationGetRelid(rel)),
							   Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		return;
	datum = SysCacheGetAttr(ATTNUM, atttuple, Anum_pg_attribute_attoptions,
							&isnull);
	aopt = isnull ? NULL : (AttributeOpts *) attribute_reloptions(datum, false);
	ReleaseSysCache(atttuple);
	if (aopt != NULL)
	{
		float8		n_distinct;

		n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
		if (n_distinct != 0.0)
			*stadistinct = (float4) n_distinct;
		pfree(aopt);
	}
}

/* -1 is the default target: the attribute is missing or was never set. */
static int
fasttrun_read_attstattarget(Oid relid, AttrNumber attnum)
{
	HeapTuple	atttuple;
	Datum		datum;
	bool		isnull;
	int			attstattarget = -1;

	atttuple = SearchSysCache2(ATTNUM,
							   ObjectIdGetDatum(relid),
							   Int16GetDatum(attnum));
	if (!HeapTupleIsValid(atttuple))
		return -1;

	datum = SysCacheGetAttr(ATTNUM, atttuple,
							Anum_pg_attribute_attstattarget,
							&isnull);
	if (!isnull)
		attstattarget = DatumGetInt16(datum);
	ReleaseSysCache(atttuple);

	return attstattarget;
}

/*
 * CREATE TABLE ... (LIKE template) does not copy SET STATISTICS.  A column the
 * temporary table leaves at the default takes the target of the same-named
 * column of the same-named table in fasttrun.prewarm_schema.  The setting is
 * then made once on the template, without an ALTER TABLE on every temporary
 * table, which would write the catalog and send a shared invalidation.
 */
/*
 * The template of the relation being collected.  A collect asks for every
 * column, and the template cannot change within one command, so the lookup
 * runs once per relation per command.
 */
static Oid	fasttrun_template_memo_relid = InvalidOid;
static Oid	fasttrun_template_memo_template = InvalidOid;
static uint64 fasttrun_template_memo_epoch = 0;
static CommandId fasttrun_template_memo_cid = InvalidCommandId;

static int
fasttrun_template_attstattarget(Relation rel, AttrNumber attnum)
{
	Oid			relid = RelationGetRelid(rel);
	CommandId	cid = GetCurrentCommandId(false);
	Oid			template_relid;
	AttrNumber	template_attnum;
	Form_pg_attribute attr;
	HeapTuple	atttuple;

	if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP ||
		fasttrun_prewarm_schema == NULL || fasttrun_prewarm_schema[0] == '\0' ||
		attnum <= 0 || attnum > RelationGetNumberOfAttributes(rel))
		return -1;
	if (fasttrun_template_memo_relid != relid ||
		fasttrun_template_memo_epoch != fasttrun_xact_epoch ||
		fasttrun_template_memo_cid != cid)
	{
		Oid			nspid = get_namespace_oid(fasttrun_prewarm_schema, true);

		fasttrun_template_memo_template = OidIsValid(nspid) ?
			get_relname_relid(RelationGetRelationName(rel), nspid) : InvalidOid;
		fasttrun_template_memo_relid = relid;
		fasttrun_template_memo_epoch = fasttrun_xact_epoch;
		fasttrun_template_memo_cid = cid;
	}
	template_relid = fasttrun_template_memo_template;
	if (!OidIsValid(template_relid))
		return -1;

	/* LIKE copies the columns in order, so the same number usually matches. */
	attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	atttuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(template_relid),
							   Int16GetDatum(attnum));
	if (HeapTupleIsValid(atttuple))
	{
		Form_pg_attribute tattr = (Form_pg_attribute) GETSTRUCT(atttuple);

		if (!tattr->attisdropped &&
			namestrcmp(&tattr->attname, NameStr(attr->attname)) == 0)
		{
			Datum		datum;
			bool		isnull;
			int			attstattarget = -1;

			datum = SysCacheGetAttr(ATTNUM, atttuple,
									Anum_pg_attribute_attstattarget, &isnull);
			if (!isnull)
				attstattarget = DatumGetInt16(datum);
			ReleaseSysCache(atttuple);
			return attstattarget;
		}
		ReleaseSysCache(atttuple);
	}
	template_attnum = get_attnum(template_relid, NameStr(attr->attname));
	if (template_attnum == InvalidAttrNumber)
		return -1;
	return fasttrun_read_attstattarget(template_relid, template_attnum);
}

static int
fasttrun_get_attstattarget(Relation rel, AttrNumber attnum)
{
	int			attstattarget = fasttrun_read_attstattarget(RelationGetRelid(rel),
															attnum);

	if (attstattarget < 0 && fasttrun_template_stattarget)
		attstattarget = fasttrun_template_attstattarget(rel, attnum);
	return attstattarget;
}

/* Core ANALYZE has no stored sample value for a PG18 virtual column. */
static inline bool
fasttrun_attribute_is_analyzable(Form_pg_attribute attr)
{
	if (attr->attisdropped)
		return false;
#if PG_VERSION_NUM >= 180000
	if (attr->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
		return false;
#endif
	return true;
}

/*
 * Sample values deformed once per row instead of once per (row, column).
 *
 * Why: heap_getattr() can use cached attribute offsets only up to the first
 * variable-width or null column.  Past it every fetch walks the tuple from the
 * start, so reading the sample column by column costs O(natts^2) per row.  On
 * a 300-column table with a leading text column that is most of the collect.
 *
 * What it holds: a column-major slice of the sample, values and null flags,
 * so each column is one contiguous run read with stride 1, the same layout
 * core ANALYZE builds for index expressions (exprvals, exprnulls, rowstride).
 * Datums point into the sample tuples, which outlive the collect.
 *
 * Memory: a slice holds as many columns as fit in FASTTRUN_DEFORM_BUDGET.  A
 * wider sample is deformed again for each further slice, which still reads
 * each row natts times per slice instead of natts^2 / 2 times in total.
 */
#define FASTTRUN_DEFORM_BUDGET	(16 * 1024 * 1024)

typedef struct FasttrunSampleColumns
{
	HeapTuple  *sample;
	int			sample_count;
	TupleDesc	tupdesc;
	int			width;			/* columns per slice */
	int			first;			/* 0-based column at the slice start, -1 empty */
	int			ncols;			/* columns in the current slice */
	Datum	   *values;			/* width x sample_count */
	bool	   *nulls;
	Datum	   *row_values;		/* natts, one deformed row */
	bool	   *row_nulls;
} FasttrunSampleColumns;

static void
fasttrun_sample_columns_init(FasttrunSampleColumns *cols, HeapTuple *sample,
							 int sample_count, TupleDesc tupdesc)
{
	Size		per_column = (Size) sample_count * (sizeof(Datum) + sizeof(bool));

	cols->sample = sample;
	cols->sample_count = sample_count;
	cols->tupdesc = tupdesc;
	cols->width = (int) Min((Size) tupdesc->natts,
							Max(FASTTRUN_DEFORM_BUDGET / per_column, (Size) 1));
	cols->first = -1;
	cols->ncols = 0;
	cols->values = palloc(sizeof(Datum) * cols->width * sample_count);
	cols->nulls = palloc(sizeof(bool) * cols->width * sample_count);
	cols->row_values = palloc(sizeof(Datum) * tupdesc->natts);
	cols->row_nulls = palloc(sizeof(bool) * tupdesc->natts);
}

static void
fasttrun_sample_columns_free(FasttrunSampleColumns *cols)
{
	pfree(cols->values);
	pfree(cols->nulls);
	pfree(cols->row_values);
	pfree(cols->row_nulls);
}

/* Point *values and *nulls at column attidx (0-based), loading its slice. */
static void
fasttrun_sample_column(FasttrunSampleColumns *cols, int attidx,
					   Datum **values, bool **nulls)
{
	if (cols->first < 0 || attidx < cols->first ||
		attidx >= cols->first + cols->ncols)
	{
		int			i;
		int			a;

		cols->first = attidx;
		cols->ncols = Min(cols->width, cols->tupdesc->natts - attidx);
		for (i = 0; i < cols->sample_count; i++)
		{
			heap_deform_tuple(cols->sample[i], cols->tupdesc,
							  cols->row_values, cols->row_nulls);
			for (a = 0; a < cols->ncols; a++)
			{
				cols->values[a * cols->sample_count + i] =
					cols->row_values[cols->first + a];
				cols->nulls[a * cols->sample_count + i] =
					cols->row_nulls[cols->first + a];
			}
		}
	}
	*values = cols->values + (attidx - cols->first) * cols->sample_count;
	*nulls = cols->nulls + (attidx - cols->first) * cols->sample_count;
}

/* Fetch function for compute_stats over a column of FasttrunSampleColumns. */
static Datum
fasttrun_sample_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			i = rownum * stats->rowstride;

	*isNull = stats->exprnulls[i];
	return stats->exprvals[i];
}

/*
 * Build a VacAttrStats for one column and run its typanalyze callback.
 * Returns NULL when the column should be skipped (dropped, no usable
 * typanalyze).  All allocations live in anl_context, so the caller can
 * release them in one MemoryContextReset / Delete.
 *
 * Mirrors examine_attribute() in commands/analyze.c (static there).
 *
 * PG 17 reshaped VacAttrStats: it dropped the embedded
 * `Form_pg_attribute attr` and lifted `attstattarget` into a top-level
 * field on the struct.  Keep the core semantics: attstattarget=0 means
 * "do not collect stats for this column", negative means default target.
 */
static VacAttrStats *
fasttrun_examine_attribute(Relation rel, int attnum, MemoryContext anl_context)
{
	Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attnum - 1);
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			attstattarget;
	int			i;
	bool		ok;
	MemoryContext oldcxt;

	if (!fasttrun_attribute_is_analyzable(attr))
		return NULL;
	attstattarget = fasttrun_get_attstattarget(rel, attnum);
	if (attstattarget == 0)
		return NULL;

	oldcxt = MemoryContextSwitchTo(anl_context);

	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));

#if PG_VERSION_NUM < 170000
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);
	stats->attr->attstattarget = attstattarget;
#else
	stats->attstattarget = attstattarget;
#endif

	stats->attrtypid = attr->atttypid;
	stats->attrtypmod = attr->atttypmod;
	stats->attrcollid = attr->attcollation;

	typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
	{
		MemoryContextSwitchTo(oldcxt);
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	}
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	MemoryContextSwitchTo(oldcxt);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
		return NULL;

	return stats;
}

/*
 * fasttrun_examine_attribute for an expression column of an index, as core
 * examine_attribute does it: the type, typmod and collation come from the
 * expression (the column type may be an opclass storage type), the target
 * from the index column.
 */
static VacAttrStats *
fasttrun_examine_expression(Relation indexrel, int attnum, Node *expr,
							MemoryContext anl_context)
{
	Form_pg_attribute attr = TupleDescAttr(indexrel->rd_att, attnum - 1);
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			attstattarget;
	int			i;
	bool		ok;
	MemoryContext oldcxt;

	attstattarget = fasttrun_read_attstattarget(RelationGetRelid(indexrel),
												attnum);
	if (attstattarget == 0)
		return NULL;

	oldcxt = MemoryContextSwitchTo(anl_context);

	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
#if PG_VERSION_NUM < 170000
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);
	stats->attr->attstattarget = attstattarget;
#else
	stats->attstattarget = attstattarget;
#endif
	stats->attrtypid = exprType(expr);
	stats->attrtypmod = exprTypmod(expr);
	stats->attrcollid = OidIsValid(indexrel->rd_indcollation[attnum - 1]) ?
		indexrel->rd_indcollation[attnum - 1] : exprCollation(expr);

	typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
	{
		MemoryContextSwitchTo(oldcxt);
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	}
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	MemoryContextSwitchTo(oldcxt);

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
		return NULL;
	(void) attr;
	return stats;
}

/*
 * Build a pg_statistic-shaped HeapTuple from a fully populated
 * VacAttrStats, mirroring the formatting half of update_attstats() in
 * commands/analyze.c -- but without ever opening pg_statistic for write.
 *
 * Returned tuple is allocated in CurrentMemoryContext.  Caller is
 * expected to feed it to fasttrun_stats_cache_store(), which deep-copies
 * it into fasttrun_stats_mcxt.
 */
static HeapTuple
fasttrun_build_pg_statistic_tuple(TupleDesc pg_stats_desc, Oid relid,
								  AttrNumber attnum, VacAttrStats *stats)
{
	Datum	values[Natts_pg_statistic];
	bool	nulls[Natts_pg_statistic];
	int		i,
			k,
			n;

	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);

	i = Anum_pg_statistic_stakind1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = Int16GetDatum(stats->stakind[k]);

	i = Anum_pg_statistic_staop1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = ObjectIdGetDatum(stats->staop[k]);

	i = Anum_pg_statistic_stacoll1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		values[i++] = ObjectIdGetDatum(stats->stacoll[k]);

	i = Anum_pg_statistic_stanumbers1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		int			nnum = stats->numnumbers[k];

		if (nnum > 0)
		{
			Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
			ArrayType  *arry;

			for (n = 0; n < nnum; n++)
				numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
			arry = construct_array_builtin(numdatums, nnum, FLOAT4OID);
			values[i++] = PointerGetDatum(arry);
		}
		else
		{
			nulls[i] = true;
			values[i++] = (Datum) 0;
		}
	}

	i = Anum_pg_statistic_stavalues1 - 1;
	for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		if (stats->numvalues[k] > 0)
		{
			ArrayType  *arry;

			arry = construct_array(stats->stavalues[k],
								   stats->numvalues[k],
								   stats->statypid[k],
								   stats->statyplen[k],
								   stats->statypbyval[k],
								   stats->statypalign[k]);
			values[i++] = PointerGetDatum(arry);
		}
		else
		{
			nulls[i] = true;
			values[i++] = (Datum) 0;
		}
	}

	return heap_form_tuple(pg_stats_desc, values, nulls);
}

/*
 * Compute the maximum sample size that std_typanalyze (or any custom
 * type-specific typanalyze) would request across all columns of rel.
 *
 * Used when fasttrun.sample_rows = -1 ("auto") to size our reservoir
 * sample exactly like core ANALYZE would: each typanalyze callback sets
 * stats->minrows during examine_attribute, and core picks the largest
 * value across all attributes (analyze.c:do_analyze_rel).  We do the
 * same so that the chosen sample is large enough for every column's
 * MCV / histogram / correlation slots.
 *
 * The temporary VacAttrStats objects live in a private mcxt that we
 * delete on the way out -- this is just a sizing query, the real
 * collect_and_store path will examine_attribute again.  Each call is
 * O(natts) syscache lookups + a typanalyze function call per column,
 * usually a few hundred microseconds total even on wide tables.  Cheap
 * compared to the actual heap scan.
 *
 * Returns the maximum minrows seen, or 0 if no column has a usable
 * typanalyze (in which case the caller will skip stats collection).
 * A floor of 100 is enforced for sanity -- even degenerate cases get
 * a non-trivial sample.
 */
static int
fasttrun_compute_max_minrows(Relation rel)
{
	TupleDesc		tupdesc = RelationGetDescr(rel);
	int				natts = tupdesc->natts;
	int				max_minrows = 0;
	int				a;
	MemoryContext	tmp_ctx;
	MemoryContext	oldcxt;

	tmp_ctx = AllocSetContextCreate(CurrentMemoryContext,
									"fasttrun minrows probe",
									ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(tmp_ctx);

	for (a = 0; a < natts; a++)
	{
		VacAttrStats *stats;

		stats = fasttrun_examine_attribute(rel, a + 1, tmp_ctx);
		if (stats == NULL)
			continue;
		if (stats->minrows > max_minrows)
			max_minrows = stats->minrows;
	}

	/* Expressions of the indexes the collect analyzes too, as core does. */
	if (rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *lc;

		foreach(lc, index_oids)
		{
			Relation	indexrel = index_open(lfirst_oid(lc), AccessShareLock);
			List	   *exprs = RelationGetIndexExpressions(indexrel);
			ListCell   *expr_item = list_head(exprs);
			int			i;

			if (exprs != NIL && RelationGetIndexPredicate(indexrel) == NIL)
			{
				for (i = 0; i < indexrel->rd_index->indnkeyatts; i++)
				{
					VacAttrStats *stats;

					if (indexrel->rd_index->indkey.values[i] != 0)
						continue;
					stats = fasttrun_examine_expression(indexrel, i + 1,
														(Node *) lfirst(expr_item),
														tmp_ctx);
					expr_item = lnext(exprs, expr_item);
					if (stats != NULL && stats->minrows > max_minrows)
						max_minrows = stats->minrows;
				}
			}
			index_close(indexrel, AccessShareLock);
		}
		list_free(index_oids);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmp_ctx);

	if (max_minrows > 0 && max_minrows < 100)
		max_minrows = 100;

	return max_minrows;
}

/*
 * Resolve fasttrun.sample_rows GUC into the actual reservoir size.
 *   < 0 -> autosize via fasttrun_compute_max_minrows() (full ANALYZE
 *         parity, follows default_statistics_target)
 *   = 0 -> caller treats as "stats collection disabled"
 *   > 0 -> explicit override.
 */
static int
fasttrun_effective_sample_target(Relation rel)
{
	if (fasttrun_sample_rows < 0)
	{
		/* Auto sizing via typanalyze minrows. If use_typanalyze=off,
		 * don't call typanalyze callbacks -- fall back to default. */
		if (!fasttrun_use_typanalyze)
			return 3000;
		return fasttrun_compute_max_minrows(rel);
	}
	return fasttrun_sample_rows;
}

