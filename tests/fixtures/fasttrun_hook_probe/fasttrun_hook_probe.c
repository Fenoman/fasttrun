/* Test hook module used only by check_fasttrun_hook_chain.sh. */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_type_d.h"
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

PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void _PG_fini(void);

static planner_hook_type prev_planner = NULL;
static get_relation_stats_hook_type prev_relation_stats = NULL;
static get_attavgwidth_hook_type prev_attavgwidth = NULL;
static ProcessUtility_hook_type prev_utility = NULL;
static object_access_hook_type prev_object_access = NULL;

static uint64 planner_calls = 0;
static uint64 relation_stats_calls = 0;
static uint64 attavgwidth_calls = 0;
static uint64 utility_calls = 0;
static uint64 object_access_calls = 0;

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
	relation_stats_calls++;
	if (prev_relation_stats)
		return prev_relation_stats(root, rte, attnum, vardata);
	return false;
}

static int32
probe_attavgwidth(Oid relid, AttrNumber attnum)
{
	attavgwidth_calls++;
	if (prev_attavgwidth)
		return prev_attavgwidth(relid, attnum);
	return 0;
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
	if (ProcessUtility_hook == probe_utility)
		ProcessUtility_hook = prev_utility;
	if (object_access_hook == probe_object_access)
		object_access_hook = prev_object_access;
}

Datum
fasttrun_hook_probe_counts(PG_FUNCTION_ARGS)
{
	Datum		values[5];

	values[0] = Int64GetDatum((int64) planner_calls);
	values[1] = Int64GetDatum((int64) relation_stats_calls);
	values[2] = Int64GetDatum((int64) attavgwidth_calls);
	values[3] = Int64GetDatum((int64) utility_calls);
	values[4] = Int64GetDatum((int64) object_access_calls);
	PG_RETURN_ARRAYTYPE_P(construct_array(values, 5, INT8OID,
									  8, true, TYPALIGN_DOUBLE));
}

Datum
fasttrun_hook_probe_reset(PG_FUNCTION_ARGS)
{
	planner_calls = 0;
	relation_stats_calls = 0;
	attavgwidth_calls = 0;
	utility_calls = 0;
	object_access_calls = 0;
	PG_RETURN_VOID();
}
