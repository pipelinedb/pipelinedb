#include "postgres.h"
#include "pipeline/cont_worker_util.h"

#include "nodes/nodeFuncs.h"
#include "utils/snapmgr.h"
#include "executor/executor.h"
	
EState *
create_estate(QueryDesc *query_desc)
{
	EState *estate;

	estate = CreateExecutorState();
	estate->es_param_list_info = query_desc->params;
	estate->es_snapshot = RegisterSnapshot(query_desc->snapshot);
	estate->es_crosscheck_snapshot = 
		RegisterSnapshot(query_desc->crosscheck_snapshot);
	estate->es_instrument = query_desc->instrument_options;
	estate->es_range_table = query_desc->plannedstmt->rtable;
	estate->es_continuous = query_desc->plannedstmt->is_continuous;
	estate->es_lastoid = InvalidOid;
	estate->es_processed = estate->es_filtered = 0;

	if (query_desc->plannedstmt->nParamExec > 0)
		estate->es_param_exec_vals = (ParamExecData *)
			palloc0(query_desc->plannedstmt->nParamExec * 
					sizeof(ParamExecData));

	estate->es_top_eflags |= EXEC_FLAG_SKIP_TRIGGERS;

	return estate;
}

void
set_snapshot(EState *estate, ResourceOwner owner)
{
	estate->es_snapshot = GetTransactionSnapshot();
	estate->es_snapshot->active_count++;
	estate->es_snapshot->copied = true;
	PushActiveSnapshot(estate->es_snapshot);
}

void
unset_snapshot(EState *estate, ResourceOwner owner)
{
	PopActiveSnapshot();
	estate->es_snapshot = NULL;
}
