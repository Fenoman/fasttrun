/* Test hook module used only by check_fasttrun_hook_chain.sh. */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/params.h"
#include "optimizer/planner.h"
#include "tcop/utility.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fasttrun_hook_probe_counts);
PG_FUNCTION_INFO_V1(fasttrun_hook_probe_reset);
PG_FUNCTION_INFO_V1(fasttrun_hook_probe_arm_nested);

PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void _PG_fini(void);

static planner_hook_type prev_planner = NULL;
static get_relation_stats_hook_type prev_relation_stats = NULL;
static get_attavgwidth_hook_type prev_attavgwidth = NULL;
static get_index_stats_hook_type prev_index_stats = NULL;
static ProcessUtility_hook_type prev_utility = NULL;
static object_access_hook_type prev_object_access = NULL;

static uint64 planner_calls = 0;
static uint64 relation_stats_calls = 0;
static uint64 attavgwidth_calls = 0;
static uint64 index_stats_calls = 0;
static uint64 utility_calls = 0;
static uint64 object_access_calls = 0;
static uint64 nested_plans = 0;
static bool nested_armed = false;
static bool nested_active = false;
static bool nested_managed = true;

static void
probe_plan_nested_query(void)
{
	SPIPlanPtr	plan;
	const char *query;

	if (!nested_armed || nested_active)
		return;
	nested_armed = false;
	nested_active = true;
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		nested_active = false;
		elog(ERROR, "fasttrun hook probe could not connect to SPI");
	}
	PG_TRY();
	{
		query = nested_managed
			? "SELECT * FROM ft_nested_memo WHERE grp = 42 AND payload = 'x'"
			: "SELECT * FROM ft_perm_expr WHERE lower(name) = 'dup'";
		plan = SPI_prepare(query, 0, NULL);
		if (plan == NULL)
			elog(ERROR, "fasttrun hook probe could not prepare nested query");
		if (SPI_execute_plan(plan, NULL, NULL, true, 0) != SPI_OK_SELECT)
			elog(ERROR, "fasttrun hook probe could not execute nested query");
		SPI_freeplan(plan);
		SPI_finish();
		nested_plans++;
		nested_active = false;
	}
	PG_CATCH();
	{
		SPI_finish();
		nested_active = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static PlannedStmt *
probe_planner(Query *parse, const char *query_string,
			  int cursor_options, ParamListInfo bound_params)
{
	planner_calls++;
	if (prev_planner)
		return prev_planner(parse, query_string, cursor_options, bound_params);
	return standard_planner(parse, query_string, cursor_options, bound_params);
}

static bool
probe_relation_stats(PlannerInfo *root, RangeTblEntry *rte,
				 AttrNumber attnum, VariableStatData *vardata)
{
	bool		handled = false;

	relation_stats_calls++;
	if (prev_relation_stats)
		handled = prev_relation_stats(root, rte, attnum, vardata);
	probe_plan_nested_query();
	return handled;
}

static int32
probe_attavgwidth(Oid relid, AttrNumber attnum)
{
	attavgwidth_calls++;
	if (prev_attavgwidth)
		return prev_attavgwidth(relid, attnum);
	return 0;
}

static bool
probe_index_stats(PlannerInfo *root, Oid index_oid,
				  AttrNumber index_attnum, VariableStatData *vardata)
{
	index_stats_calls++;
	if (prev_index_stats)
		return prev_index_stats(root, index_oid, index_attnum, vardata);
	return false;
}

static void
probe_utility(PlannedStmt *pstmt, const char *query_string,
			  bool read_only_tree, ProcessUtilityContext context,
			  ParamListInfo params, QueryEnvironment *query_env,
			  DestReceiver *dest, QueryCompletion *qc)
{
	utility_calls++;
	if (prev_utility)
		prev_utility(pstmt, query_string, read_only_tree, context, params,
					 query_env, dest, qc);
	else
		standard_ProcessUtility(pstmt, query_string, read_only_tree, context,
							params, query_env, dest, qc);
}

static void
probe_object_access(ObjectAccessType access, Oid class_id,
				Oid object_id, int sub_id, void *arg)
{
	object_access_calls++;
	if (prev_object_access)
		prev_object_access(access, class_id, object_id, sub_id, arg);
}

PGDLLEXPORT void
_PG_init(void)
{
	prev_planner = planner_hook;
	planner_hook = probe_planner;
	prev_relation_stats = get_relation_stats_hook;
	get_relation_stats_hook = probe_relation_stats;
	prev_attavgwidth = get_attavgwidth_hook;
	get_attavgwidth_hook = probe_attavgwidth;
	prev_index_stats = get_index_stats_hook;
	get_index_stats_hook = probe_index_stats;
	prev_utility = ProcessUtility_hook;
	ProcessUtility_hook = probe_utility;
	prev_object_access = object_access_hook;
	object_access_hook = probe_object_access;
}

PGDLLEXPORT void
_PG_fini(void)
{
	if (planner_hook == probe_planner)
		planner_hook = prev_planner;
	if (get_relation_stats_hook == probe_relation_stats)
		get_relation_stats_hook = prev_relation_stats;
	if (get_attavgwidth_hook == probe_attavgwidth)
		get_attavgwidth_hook = prev_attavgwidth;
	if (get_index_stats_hook == probe_index_stats)
		get_index_stats_hook = prev_index_stats;
	if (ProcessUtility_hook == probe_utility)
		ProcessUtility_hook = prev_utility;
	if (object_access_hook == probe_object_access)
		object_access_hook = prev_object_access;
}

Datum
fasttrun_hook_probe_counts(PG_FUNCTION_ARGS)
{
	Datum		values[7];

	values[0] = Int64GetDatum((int64) planner_calls);
	values[1] = Int64GetDatum((int64) relation_stats_calls);
	values[2] = Int64GetDatum((int64) attavgwidth_calls);
	values[3] = Int64GetDatum((int64) index_stats_calls);
	values[4] = Int64GetDatum((int64) utility_calls);
	values[5] = Int64GetDatum((int64) object_access_calls);
	values[6] = Int64GetDatum((int64) nested_plans);
	PG_RETURN_ARRAYTYPE_P(construct_array(values, 7, INT8OID,
									  8, true, TYPALIGN_DOUBLE));
}

Datum
fasttrun_hook_probe_reset(PG_FUNCTION_ARGS)
{
	planner_calls = 0;
	relation_stats_calls = 0;
	attavgwidth_calls = 0;
	index_stats_calls = 0;
	utility_calls = 0;
	object_access_calls = 0;
	nested_plans = 0;
	nested_armed = false;
	PG_RETURN_VOID();
}

Datum
fasttrun_hook_probe_arm_nested(PG_FUNCTION_ARGS)
{
	nested_managed = PG_GETARG_BOOL(0);
	nested_armed = true;
	PG_RETURN_VOID();
}
