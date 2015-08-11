/*-------------------------------------------------------------------------
 *
 * cont_adhoc.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_adhoc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pipeline/cont_adhoc.h"

#include "catalog/pipeline_query_fn.h"
#include "executor/execdesc.h"
#include "pgstat.h"
#include "tcop/dest.h"
#include "utils/palloc.h"
#include "utils/resowner.h"
#include "utils/memutils.h"
#include "pipeline/cont_plan.h"
#include "utils/snapmgr.h"
#include "executor/executor.h"
#include <unistd.h>
#include "catalog/pipeline_query.h"
#include "access/htup_details.h"
#include "pipeline/cont_scheduler.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "executor/tstoreReceiver.h"
#include "nodes/print.h"
#include "executor/tupletableReceiver.h"
#include "lib/stringinfo.h"
#include "utils/syscache.h"
#include "pipeline/miscutils.h"

#include "parser/parse_node.h"
#include "pipeline/cont_analyze.h"
#include "access/xact.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqanalyze.h"
#include "parser/analyze.h"
#include "nodes/nodeFuncs.h"
#include "catalog/pg_type.h"
#include "commands/pipelinecmds.h"
#include "pipeline/cqwindow.h"

typedef struct {
	TupleBufferBatchReader  *reader;
	Oid                     view_id;
	ContinuousView          *view;
	DestReceiver            *dest;
	QueryDesc               *query_desc;
	TupleDesc               tup_desc;
	TupleTableSlot          *slot;
	CQStatEntry             stats;

	TimestampTz 			last_processed;
	uint32 					num_processed;
} AdhocWorkerState;

typedef struct {
	QueryDesc               *query_desc;

	TupleDesc               tup_desc;
	TupleTableSlot          *slot;

	Tuplestorestate *batch;      /* to hold pre combine rows */
	TupleHashTable existing; 	 /* only needed if is_agg is true */
	Tuplestorestate *result; 	 /* holds the combined result */

	DestReceiver            *dest;

	bool is_agg;

} AdhocCombinerState;

static TupleHashTable
create_agg_hash(int num_cols, Oid *grp_ops, AttrNumber *group_atts)
{
	FmgrInfo *eq_funcs = 0;
	FmgrInfo *hash_funcs = 0;
	MemoryContext hash_tmp_cxt;

	execTuplesHashPrepare(num_cols, grp_ops,
						  &eq_funcs, &hash_funcs);

	/* this tmp cxt is reqd by hash table, it will reset it internally */

	hash_tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
			"AdhocCombinerQueryHashTableTempCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	return BuildTupleHashTable(num_cols, group_atts,
							   eq_funcs, hash_funcs, 1000,
							   sizeof(HeapTupleEntryData),
							   CurrentMemoryContext, hash_tmp_cxt);
}

/* read everything */
static bool
should_read_fn(TupleBufferReader *reader, TupleBufferSlot *slot)
{
	return true;
}

static void
init_cont_query_batch_context()
{
	ContQueryBatchContext = AllocSetContextCreate(CurrentMemoryContext,
			"ContQueryBatchContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
}

static void
set_reader(PlanState *planstate, TupleBufferBatchReader *reader)
{
	if (planstate == NULL)
		return;

	if (IsA(planstate, StreamScanState))
	{
		StreamScanState *scan = (StreamScanState *) planstate;
		scan->reader = reader;
		return;
	}
	else if (IsA(planstate, SubqueryScanState))
	{
		set_reader(((SubqueryScanState *) planstate)->subplan, reader);
		return;
	}

	set_reader(planstate->lefttree, reader);
	set_reader(planstate->righttree, reader);
}

static EState *
create_estate(QueryDesc *query_desc)
{
	EState *estate;

	estate = CreateExecutorState();
	estate->es_param_list_info = query_desc->params;
	estate->es_snapshot = RegisterSnapshot(query_desc->snapshot);
	estate->es_crosscheck_snapshot = RegisterSnapshot(query_desc->crosscheck_snapshot);
	estate->es_instrument = query_desc->instrument_options;
	estate->es_range_table = query_desc->plannedstmt->rtable;
	estate->es_continuous = query_desc->plannedstmt->is_continuous;
	estate->es_lastoid = InvalidOid;
	estate->es_processed = estate->es_filtered = 0;

	if (query_desc->plannedstmt->nParamExec > 0)
		estate->es_param_exec_vals = (ParamExecData *)
			palloc0(query_desc->plannedstmt->nParamExec * sizeof(ParamExecData));

	estate->es_top_eflags |= EXEC_FLAG_SKIP_TRIGGERS;

	return estate;
}

static void
set_snapshot(EState *estate, ResourceOwner owner)
{
	estate->es_snapshot = GetTransactionSnapshot();
	estate->es_snapshot->active_count++;
	estate->es_snapshot->copied = true;
	PushActiveSnapshot(estate->es_snapshot);
}

static void
unset_snapshot(EState *estate, ResourceOwner owner)
{
	PopActiveSnapshot();
	estate->es_snapshot = NULL;
}

static Oid
get_cont_view_id(const char* name)
{
	RangeVar var = {T_RangeVar,0x0,0x0,(char*) name,INH_DEFAULT,'p', 0x0, 0x0};
	HeapTuple tuple = GetPipelineQueryTuple(&var);
	Form_pipeline_query row;

	Assert(HeapTupleIsValid(tuple));
	row = (Form_pipeline_query) GETSTRUCT(tuple);

	return row->id;
}

static ContQueryProc *
get_cont_query_proc()
{
	Oid key = MyDatabaseId;
	ContQueryProc *proc = ContQueryGetAdhoc(key);

	Assert(proc);

	/* TODO - manage cq procs for adhoc queries correctly.
	 * cont_scheduler.c:start_group is currently doing the rest of this.
	 * id can't be too high - if the bms tries to resize it will die
	 */
	
	proc->id = 23; /* HACK */
	proc->latch = &MyProc->procLatch;
	proc->active = true;

	return proc;
}

void
dump_tuplestore(Tuplestorestate *batch, TupleTableSlot *slot);

void
dump_tuplestore(Tuplestorestate *batch, TupleTableSlot *slot)
{
	size_t ctr = 0;
	elog(LOG, "******************************");

	tuplestore_rescan(batch);

	foreach_tuple(slot, batch)
	{
		elog(LOG, "dump store %zu", ctr++);
		print_slot(slot);
	}
}

typedef struct{
	Oid view_id;
	ContinuousView *view;
	TupleDesc desc;
} ContinuousViewData;

static AdhocWorkerState*
init_adhoc_worker(ContinuousViewData data, DestReceiver *receiver)
{
	ResourceOwner owner = CurrentResourceOwner;
	PlannedStmt *pstmt = 0;
	AdhocWorkerState *state = palloc0(sizeof(AdhocWorkerState));

	init_cont_query_batch_context();

	state->reader = 
		TupleBufferOpenBatchReader(WorkerTupleBuffer, &should_read_fn);

	state->view_id = data.view_id;
	state->view = data.view;

	Assert(state->view);

	(void) (owner);
	state->dest = receiver;

	PushActiveSnapshot(GetTransactionSnapshot());
	pstmt = GetContPlanType(state->view, Worker, data.desc);
	PopActiveSnapshot();

	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, state->dest, NULL, 0);

	/* TODO - handle snapshots correctly */

/*	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;
	RegisterSnapshotOnOwner(state->query_desc->snapshot, owner); */

	ExecutorStart(state->query_desc, 0);

	/* not really used by the worker, but this is a convenient place to 
	   create them. */
	
	state->tup_desc = CreateTupleDescCopyConstr(state->query_desc->tupDesc);
	state->slot = MakeSingleTupleTableSlot(state->tup_desc);

/*	state->query_desc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(state->query_desc->snapshot, owner);
	UnregisterSnapshotFromOwner(state->query_desc->estate->es_snapshot, owner);
	state->query_desc->snapshot = NULL; */

	set_reader(state->query_desc->planstate, state->reader);

	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, 
			state->query_desc->operation, state->query_desc->tupDesc);

	cq_stat_init(&state->stats, state->view->id, 0);

	/*
	 * The main loop initializes and ends plans across plan executions, so 
	 * it expects the plan to be uninitialized
	 */
	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	state->last_processed = GetCurrentTimestamp();

	return state;
}

static AdhocCombinerState*
init_adhoc_combiner(ContinuousViewData data,
					TupleDesc tup_desc,
					TupleTableSlot *slot,
					Tuplestorestate *batch)
{

	AdhocCombinerState *state = palloc0(sizeof(AdhocWorkerState));
	PlannedStmt *pstmt = 0;

	PushActiveSnapshot(GetTransactionSnapshot());
	pstmt = GetContPlanType(data.view, Combiner, data.desc);
	PopActiveSnapshot();

	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, state->dest,
										NULL, 0);

	state->tup_desc = tup_desc;
	state->slot = slot;

	{
		TuplestoreScan *scan;

		(void) (scan);
		scan = SetCombinerPlanTuplestorestate(pstmt, batch);
	}

	state->batch = batch;

	state->result = tuplestore_begin_heap(true, true,
							continuous_query_combiner_work_mem);

	state->dest = CreateDestReceiver(DestTuplestore);

	SetTuplestoreDestReceiverParams(state->dest, state->result,
									CurrentMemoryContext, true);

	if (IsA(pstmt->planTree, Agg))
	{
		Agg *agg = (Agg *) pstmt->planTree;

		state->is_agg = true;
		state->existing = create_agg_hash(agg->numCols, agg->grpOperators,
										  agg->grpColIdx);
	}

	return state;
}

static void
exec_adhoc_worker(AdhocWorkerState* state)
{
	/* TODO - fix up memory context usage
	 * 		- handle transactions correctly
	 */

	ResourceOwner owner = CurrentResourceOwner;
	struct Plan *plan = 0;
	bool has_tup = false;
	EState *estate = 0;

	TupleBufferBatchReaderTrySleep(state->reader, state->last_processed);

	has_tup = 
		TupleBufferBatchReaderHasTuplesForCQId(state->reader, state->view_id);

	if (!has_tup)
	{
		TupleBufferBatchReaderRewind(state->reader);
		return;
	}

	state->query_desc->estate = create_estate(state->query_desc);
	estate = state->query_desc->estate;

	TupleBufferBatchReaderSetCQId(state->reader, state->view_id);

	set_snapshot(estate, owner);

	plan = state->query_desc->plannedstmt->planTree;

	state->query_desc->planstate = 
		ExecInitNode(plan, state->query_desc->estate, 0);

	set_reader(state->query_desc->planstate, state->reader);

	ExecutePlan(estate, state->query_desc->planstate, 
				state->query_desc->operation,
				true, 0, 0, ForwardScanDirection, state->dest);

	ExecEndNode(state->query_desc->planstate);
	state->query_desc->planstate = NULL;

	state->num_processed = estate->es_processed + estate->es_filtered;

	if (state->num_processed)
	{
		state->last_processed = GetCurrentTimestamp();
	}

	unset_snapshot(estate, owner);
	state->query_desc->estate = NULL;
	estate = state->query_desc->estate;

	TupleBufferBatchReaderRewind(state->reader);
	
	TupleBufferBatchReaderReset(state->reader);
	MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
}

static void
exec_adhoc_combiner(AdhocCombinerState* state)
{
	ResourceOwner owner = CurrentResourceOwner;

	struct Plan *plan = 0;
	EState *estate = 0;

	/* put all existing groups in with batch. */

	if (state->is_agg)
	{
		foreach_tuple(state->slot, state->batch)
		{
			HeapTupleEntry entry = (HeapTupleEntry) 
				LookupTupleHashEntry(state->existing, state->slot, NULL);

			if (entry)
			{
				/* if the group exists, copy it into 'result'
				 * we are using 'result' as scratch space here. */

				tuplestore_puttuple(state->result, entry->tuple);
			}
		}

		foreach_tuple(state->slot, state->result)
		{
			tuplestore_puttupleslot(state->batch, state->slot);
		}

		tuplestore_clear(state->result);
		tuplestore_rescan(state->batch);
	}

	state->query_desc->estate = create_estate(state->query_desc);
	estate = state->query_desc->estate;

	set_snapshot(estate, owner);

	plan = state->query_desc->plannedstmt->planTree;

	state->query_desc->planstate = 
		ExecInitNode(plan, state->query_desc->estate, 0);

	ExecutePlan(estate, state->query_desc->planstate, 
				state->query_desc->operation,
				true, 0, 0, ForwardScanDirection, state->dest);

	ExecEndNode(state->query_desc->planstate);
	state->query_desc->planstate = NULL;

	unset_snapshot(estate, owner);

	state->query_desc->estate = NULL;
	estate = state->query_desc->estate;

	MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
}

static void
adhoc_sync_combine(AdhocCombinerState* state)
{
	tuplestore_clear(state->batch);
	tuplestore_rescan(state->result);

	foreach_tuple(state->slot, state->result)
	{
		if (state->is_agg)
		{
			bool isnew = false;

			HeapTupleEntry entry = (HeapTupleEntry) 
				LookupTupleHashEntry(state->existing, state->slot, &isnew);

			if (!isnew)
			{
				elog(LOG, "update existing in adhoc_sync_combine");
				print_slot(state->slot);

				heap_freetuple(entry->tuple);
				entry->tuple = ExecCopySlotTuple(state->slot);
			}
			else
			{
				elog(LOG, "insert new in adhoc_sync_combine");
				print_slot(state->slot);
				entry->tuple = ExecCopySlotTuple(state->slot);
			}
		}
	}

	tuplestore_clear(state->result);
}

static RangeVar
get_unique_adhoc_view_name()
{
	char *relname = palloc0(NAMEDATALEN);
	RangeVar var = {T_RangeVar,0x0,0x0,relname,INH_DEFAULT,'p', 0x0, 0x0};

	int i = 0;

	while (true)
	{
		sprintf(relname, "adhoc%d", i++);

		if (!IsAContinuousView(&var))
		{
			break;
		}
	}

	return var;
}

#define DEFAULT_TYPEMOD -1

typedef struct
{
	SelectStmt *workerselect;
	SelectStmt *viewselect;
	TupleDesc desc;
} ViewDetails;

/* TODO - refactor with ExecCreateContViewStmt */

static ViewDetails
build_view_details(SelectStmt *stmt, const char* querystring)
{
	ViewDetails details;

	SelectStmt *workerselect;
	SelectStmt *viewselect;

	Query *query;
	List *tableElts = NIL;
	List *tlist;
	ListCell *col;
	CQAnalyzeContext context;

	workerselect =
		GetSelectStmtForCQWorker(copyObject(stmt), &viewselect);
	InitializeCQAnalyzeContext(workerselect, NULL, &context);

	query = parse_analyze(copyObject(workerselect), querystring, 0, 0);
	tlist = query->targetList;

	foreach(col, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(col);
		ColumnDef   *coldef;
		char		*colname;
		Oid			hiddentype;
		Oid type;

		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);

		type = exprType((Node *) tle->expr);

		if (type == VOIDOID)
			type = BOOLOID;
		coldef = make_cv_columndef(colname, type,
					exprTypmod((Node *) tle->expr));
		tableElts = lappend(tableElts, coldef);

		hiddentype = GetCombineStateColumnType(tle->expr);

		if (OidIsValid(hiddentype))
		{
			char *hiddenname = GetUniqueInternalColname(&context);
			ColumnDef *hidden = 
				make_cv_columndef(hiddenname, hiddentype, DEFAULT_TYPEMOD);
			tableElts = lappend(tableElts, hidden);
		}
	}

	details.workerselect = workerselect;
	details.viewselect = viewselect;
	details.desc = BuildDescForRelation(tableElts);

	return details;
}

static ContinuousViewData
init_cont_view(SelectStmt *stmt, const char* querystring)
{
	ViewDetails details;
	ContinuousViewData data;
	Relation pipeline_query;
	RangeVar var;
	RangeVar *view = &var;
	bool saveAllowSystemTableMods;
	Oid cvoid = 0;

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);

	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	var = get_unique_adhoc_view_name();
	view = &var;

	CreateInferredStreams(stmt);
	ValidateContQuery(view, (Node*) stmt, querystring);

	details = build_view_details(stmt, querystring);

	cvoid = DefineContinuousView(view, 
			querystring,
			0,
			IsSlidingWindowSelectStmt(details.viewselect),
			!SelectsFromStreamOnly(details.workerselect));

	(void) (cvoid);

	CommandCounterIncrement();
	allowSystemTableMods = saveAllowSystemTableMods;
	heap_close(pipeline_query, NoLock);

	data.view_id = get_cont_view_id(var.relname);
	data.view = GetContinuousView(data.view_id);
	data.desc = details.desc;

	CommitTransactionCommand();
	StartTransactionCommand();

	return data;
}

void
ExecAdhocQuery(SelectStmt* stmt, const char *s)
{
	AdhocWorkerState *worker_state = 0;
	AdhocCombinerState *combiner_state = 0;
	ContinuousViewData data = init_cont_view(stmt, s);

	DestReceiver *worker_receiver = CreateDestReceiver(DestTuplestore);

	Tuplestorestate *batch = tuplestore_begin_heap(true, true,
			continuous_query_combiner_work_mem);

	SetTuplestoreDestReceiverParams(worker_receiver, batch,
			CurrentMemoryContext, true);

	MyContQueryProc = get_cont_query_proc();

	worker_state = init_adhoc_worker(data, worker_receiver);

	combiner_state = init_adhoc_combiner(data,
										 worker_state->tup_desc,
										 worker_state->slot,
										 batch);

	while (true)
	{
		/* TODO - create a mem context here */

		exec_adhoc_worker(worker_state);
		exec_adhoc_combiner(combiner_state);

		adhoc_sync_combine(combiner_state);

		/* TODO - and tear it down here */
	}
}
