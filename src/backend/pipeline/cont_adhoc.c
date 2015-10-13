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
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "executor/tupletableReceiver.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/print.h"
#include "pgstat.h"
#include "pipeline/cont_adhoc.h"
#include "pipeline/cont_adhoc_sender.h"
#include "pipeline/cont_adhoc_mgr.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "pipeline/tuplebuf.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include <unistd.h>

#include <sys/time.h>
#include <sys/resource.h>

/* This module performs work analogous to the bg workers and combiners
 * (and a view select), but does it in memory using tuple stores.
 *
 * The flow of data is roughly like so:
 *
 * WorkerTupleBuffer -> Worker -> Combiner -> View -> Frontend
 */

typedef struct {
	TupleBufferBatchReader  *reader;
	Oid                     view_id;
	ContinuousView          *view;
	DestReceiver            *dest;
	QueryDesc               *query_desc;
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

typedef struct {

	Tuplestorestate *batch;
	QueryDesc *query_desc;
	DestReceiver *dest;
	TupleTableSlot          *slot;

} AdhocViewState;

static TupleHashTable
create_agg_hash(int num_cols, Oid *grp_ops, AttrNumber *group_atts)
{
	FmgrInfo *eq_funcs = 0;
	FmgrInfo *hash_funcs = 0;
	MemoryContext hash_tmp_cxt;

	execTuplesHashPrepare(num_cols, grp_ops,
						  &eq_funcs, &hash_funcs);

	/* this tmp cxt is required by hash table, it will reset it internally */

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

static bool
should_read_fn(TupleBufferReader *reader, TupleBufferSlot *slot)
{
	return slot->tuple->group_hash == reader->proc->group_id;
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

/* util func to set reader on stream scan nodes */ 
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
get_cont_view_id(RangeVar *name)
{
	HeapTuple tuple = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	Oid row_id;

	Assert(HeapTupleIsValid(tuple));
	row = (Form_pipeline_query) GETSTRUCT(tuple);

	row_id = row->id;
	ReleaseSysCache(tuple);

	return row_id;
}

static void
acquire_cont_query_proc()
{
	MyContQueryProc = AdhocMgrGetProc();
}

static void
release_cont_query_proc()
{
	AdhocMgrReleaseProc(MyContQueryProc);
	MyContQueryProc = 0;
}

/* generate a unique name for the adhoc view */
static char *
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

	return relname;
}

typedef struct{
	Oid view_id;
	ContinuousView *view;
} ContinuousViewData;

/* reuse most of the machinery of ExecCreateContViewStmt to make the view.
 * The view will be flagged as adhoc because SetAmContQueryAdhoc(true) has been
 * called earlier - (it is passed into DefineContinuousView) */

static ContinuousViewData
init_cont_view(SelectStmt *stmt, const char * querystring)
{
	ContinuousViewData view_data;
	RangeVar *view_name = makeRangeVar(0, get_unique_adhoc_view_name(), -1);

	CreateContViewStmt *create_stmt	= makeNode(CreateContViewStmt);

	create_stmt->into = makeNode(IntoClause);
	create_stmt->into->rel = view_name;
	create_stmt->query = (Node *) stmt;

	stmt->forContinuousView = true;

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecCreateContViewStmt(create_stmt, querystring);
	PopActiveSnapshot();

	view_data.view_id = get_cont_view_id(view_name);
	view_data.view = GetContinuousView(view_data.view_id);

	MyContQueryProc->group_id = view_data.view_id;

	return view_data;
}

/* get the worker plan and set up the worker state for execution */
static AdhocWorkerState *
init_adhoc_worker(ContinuousViewData data, DestReceiver *receiver)
{
	PlannedStmt *pstmt = 0;
	AdhocWorkerState *state = palloc0(sizeof(AdhocWorkerState));
	ResourceOwner owner = CurrentResourceOwner;

	init_cont_query_batch_context();

	state->reader = 
		TupleBufferOpenBatchReader(AdhocTupleBuffer, &should_read_fn);

	state->view_id = data.view_id;
	state->view = data.view;

	Assert(state->view);
	state->dest = receiver;

	PushActiveSnapshot(GetTransactionSnapshot());
	pstmt = GetContPlan(state->view, Worker);
	PopActiveSnapshot();

	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, state->dest, NULL, 0);

	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;

	RegisterSnapshotOnOwner(state->query_desc->snapshot, owner);

	ExecutorStart(state->query_desc, 0);

	state->query_desc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(state->query_desc->snapshot, owner);
	UnregisterSnapshotFromOwner(state->query_desc->estate->es_snapshot, owner);
	state->query_desc->snapshot = NULL;

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


/* Prepare the combiner plan to read from the batch */
static TupleDesc
prepare_plan_for_reading(RangeVar *matrel, PlannedStmt *plan,
					 	 Tuplestorestate *batch)
{
	TuplestoreScan *scan;

	Relation rel = heap_openrv(matrel, AccessShareLock);

	plan->is_continuous = false;

	scan = SetCombinerPlanTuplestorestate(plan, batch);
	scan->desc = CreateTupleDescCopy(RelationGetDescr(rel));

	heap_close(rel, AccessShareLock);

	return scan->desc;

}

/* Get the combiner plan, and prepare the combiner state for execution */
static AdhocCombinerState *
init_adhoc_combiner(ContinuousViewData data,
					Tuplestorestate *batch)
{
	AdhocCombinerState *state = palloc0(sizeof(AdhocWorkerState));
	PlannedStmt *pstmt = 0;

	PushActiveSnapshot(GetTransactionSnapshot());
	pstmt = GetContPlan(data.view, Combiner);
	PopActiveSnapshot();

	state->tup_desc = prepare_plan_for_reading(data.view->matrel, pstmt, batch);
	state->dest = CreateDestReceiver(DestTuplestore);
	 
	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, state->dest,
										NULL, 0);

	state->slot = MakeSingleTupleTableSlot(state->tup_desc);

	state->batch = batch;

	state->result = tuplestore_begin_heap(true, true,
							continuous_query_combiner_work_mem);

	SetTuplestoreDestReceiverParams(state->dest, state->result,
									CurrentMemoryContext, true);

	/* if this is an aggregating query, set up a hash table to store the
	 * groups */

	if (IsA(pstmt->planTree, Agg))
	{
		Agg *agg = (Agg *) pstmt->planTree;
		state->is_agg = true;

		state->existing = create_agg_hash(agg->numCols, agg->grpOperators,
										  agg->grpColIdx);
	}

	ExecutorStart(state->query_desc, 0);
	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, 
			state->query_desc->operation, state->query_desc->tupDesc);

	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	return state;
}

/* run the worker - this will block for one sec waiting for tuples in the worker tuple
 * buffer. 
 *
 * If it has no tuples, it will return false.
 *
 * Otherwise, tuples are processed with the worker plan and sent to the dest receiver
 */
static bool
exec_adhoc_worker(AdhocWorkerState * state)
{
	struct Plan *plan = 0;
	bool has_tup = false;
	EState *estate = 0;
	ResourceOwner owner = CurrentResourceOwner;

	TupleBufferBatchReaderTrySleepTimeout(state->reader, state->last_processed, 1000);

	has_tup = 
		TupleBufferBatchReaderHasTuplesForCQId(state->reader, state->view_id);

	if (!has_tup)
	{
		TupleBufferBatchReaderRewind(state->reader);
		return false;
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

	return state->num_processed != 0;
}

static void
adhoc_sync_combine(AdhocCombinerState *state);

/* 
 * combine rows in state->batch (from worker)
 */
static void
exec_adhoc_combiner(AdhocCombinerState *state)
{
	ResourceOwner owner = CurrentResourceOwner;
	struct Plan *plan = 0;
	EState *estate = 0;

	/* if this is an agg query, check for any existing groups, and put them in the
	 * batch to be processed also */

	if (state->is_agg)
	{
		foreach_tuple(state->slot, state->batch)
		{
			HeapTupleEntry entry = (HeapTupleEntry) 
				LookupTupleHashEntry(state->existing, state->slot, NULL);

			if (entry)
			{
				/* we are using 'result' as scratch space here. */
				tuplestore_puttuple(state->result, entry->tuple);
			}
			else
			{
			}
		}

		foreach_tuple(state->slot, state->result)
		{
			tuplestore_puttupleslot(state->batch, state->slot);
		}

		tuplestore_clear(state->result);
		tuplestore_rescan(state->batch);
	}

	/* run the combine plan */

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

	/* sync */
	adhoc_sync_combine(state);
}

/* update results in hash table if need be */
static void
adhoc_sync_combine(AdhocCombinerState *state)
{
	MemoryContext old_cxt;

	tuplestore_clear(state->batch);
	tuplestore_rescan(state->result);

	if (!state->is_agg)
		return;

	foreach_tuple(state->slot, state->result)
	{
		bool isnew = false;

		HeapTupleEntry entry = (HeapTupleEntry) 
			LookupTupleHashEntry(state->existing, state->slot, &isnew);

		if (!isnew)
		{
			heap_freetuple(entry->tuple);

			old_cxt = MemoryContextSwitchTo(state->existing->tablecxt);
			entry->tuple = ExecCopySlotTuple(state->slot);
			MemoryContextSwitchTo(old_cxt);
		}
		else
		{
			old_cxt = MemoryContextSwitchTo(state->existing->tablecxt);
			entry->tuple = ExecCopySlotTuple(state->slot);
			MemoryContextSwitchTo(old_cxt);
		}
	}
}

/* get the view plan and set up the view state for execution */
static AdhocViewState *
init_adhoc_view(ContinuousViewData data,
				Tuplestorestate *batch,
				DestReceiver *receiver)
{
	AdhocViewState *state = palloc0(sizeof(AdhocViewState));
	PlannedStmt *pstmt = 0;
	TupleDesc desc;

	PushActiveSnapshot(GetTransactionSnapshot());
	pstmt = GetContinuousViewOverlayPlan(data.view);
	PopActiveSnapshot();

	desc = prepare_plan_for_reading(data.view->matrel, pstmt, batch);
	(void) desc;

	state->batch = batch;
	state->dest = receiver;

	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, state->dest,
										NULL, 0);

	ExecutorStart(state->query_desc, 0);
	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, 
			state->query_desc->operation, state->query_desc->tupDesc);

	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	return state;
}

/* execute the view and send rows to adhoc dest receiver */
static void
exec_adhoc_view(AdhocViewState *state)
{
	ResourceOwner owner = CurrentResourceOwner;
	struct Plan *plan = 0;
	EState *estate = 0;

	tuplestore_rescan(state->batch);

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

	tuplestore_clear(state->batch);
}

/* Execute a drop view statement to cleanup the adhoc view */
void cleanup_cont_view(ContinuousView *view)
{
	DestReceiver *receiver = 0;
	DropStmt *stmt = makeNode(DropStmt);
	List *querytree_list;
	Node *plan;
	Portal portal;

	stmt->objects = list_make1(list_make1(makeString(view->name.data)));
	stmt->removeType = OBJECT_CONTINUOUS_VIEW;

	querytree_list = pg_analyze_and_rewrite((Node *) stmt, "DROP",
			NULL, 0);

	plan = ((Query *) linitial(querytree_list))->utilityStmt;
	PushActiveSnapshot(GetTransactionSnapshot());

	portal = CreatePortal("__cleanup_cont_view__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			"DROP",
			"DROP",
			list_make1(plan),
			NULL);

	receiver = CreateDestReceiver(DestNone);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	(*receiver->rDestroy) (receiver);
	PortalDrop(portal, false);
	PopActiveSnapshot();
}

typedef struct CleanupData
{
	ContinuousViewData *cont_view_data;
	AdhocWorkerState *worker_state;
} CleanupData;

/* To be called when adhoc query is finished */
static void
cleanup(int code, Datum arg)
{
	AbortCurrentTransaction();

	CleanupData *cleanup = (CleanupData*)(arg);

	TupleBufferBatchReaderReset(cleanup->worker_state->reader);
	TupleBufferCloseBatchReader(cleanup->worker_state->reader);

	release_cont_query_proc();

	StartTransactionCommand();
	cleanup_cont_view(cleanup->cont_view_data->view);
	CommitTransactionCommand();
}

/* Initialize the worker, combiner and view modules, then loop until done.
 * The current exit conditions are when the query is cancelled, or when the frontend
 * goes away (the heartbeat will fail to send) */
static void
exec_adhoc_query(SelectStmt *stmt, const char *s)
{
	int numCols = 1;
	AttrNumber one = 1;
	AttrNumber *keyColIdx = &one;

	AdhocWorkerState *worker_state = 0;
	AdhocCombinerState *combiner_state = 0;
	AdhocViewState *view_state = 0;
	AdhocDestReceiver *adhoc_receiver = 0;
	DestReceiver *worker_receiver = 0;

	MemoryContext run_cxt = CurrentMemoryContext;
	ContinuousViewData view_data;
	Tuplestorestate *batch = 0;

	ResourceOwner owner = 
			ResourceOwnerCreate(CurrentResourceOwner, "WorkerResourceOwner");

	acquire_cont_query_proc();

	if (!MyContQueryProc)
	{
		elog(ERROR, "too many adhoc processes");
	}

	StartTransactionCommand();
	CurrentResourceOwner = owner;
	MemoryContextSwitchTo(run_cxt);
	view_data = init_cont_view(stmt, s);
	CommitTransactionCommand();

	StartTransactionCommand();
	CurrentResourceOwner = owner;
	MemoryContextSwitchTo(run_cxt);
	worker_receiver = CreateDestReceiver(DestTuplestore);

	batch = tuplestore_begin_heap(true, true, 
				continuous_query_combiner_work_mem);

	SetTuplestoreDestReceiverParams(worker_receiver, batch,
			CurrentMemoryContext, true);

	worker_state = init_adhoc_worker(view_data, worker_receiver);
	combiner_state = init_adhoc_combiner(view_data, batch);

	if (combiner_state->existing)
	{
		keyColIdx = combiner_state->existing->keyColIdx;
		numCols = combiner_state->existing->numCols;
	}

	adhoc_receiver = CreateAdhocDestReceiver(combiner_state->is_agg,
										 keyColIdx,
										 numCols);

	view_state = init_adhoc_view(view_data, combiner_state->result,
				 (DestReceiver *) adhoc_receiver);
	view_state->slot = combiner_state->slot;

	CommitTransactionCommand();

	CleanupData cleanup_data = { &view_data, worker_state };

	PG_ENSURE_ERROR_CLEANUP(cleanup, PointerGetDatum(&cleanup_data));
	{
		while (true)
		{
			bool new_rows = false;

			/* use normal transaction machinery to cleanup any 'run' allocs */
			StartTransactionCommand();
			CurrentResourceOwner = owner;

			new_rows = exec_adhoc_worker(worker_state);

			if (new_rows)
			{
				exec_adhoc_combiner(combiner_state);
				exec_adhoc_view(view_state);
			}
			else
			{
				/* TODO - check deadline timeout */
				AdhocDestReceiverHeartbeat(adhoc_receiver);
			}

			CommitTransactionCommand();
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cleanup, PointerGetDatum(&cleanup_data));
}

static const char* get_stmt_sql(Node* node)
{
	/* assumes IsAdhocQuery has returned true on node already. */

	SelectStmt *stmt = (SelectStmt*) (node);
	Node *first_node = 0;
	ResTarget *target = 0;
	FuncCall *func = 0;
	A_Const *sql_arg = 0;

	first_node = (Node *) linitial(stmt->targetList);
	target = (ResTarget*) (first_node);
	func = (FuncCall*) (target->val);
	sql_arg = linitial(func->args);

	return sql_arg->val.val.str;
}

static void set_nice_priority()
{
	int default_priority = getpriority(PRIO_PROCESS, MyProcPid);
	int priority = Max(default_priority, 20 - ceil(continuous_query_proc_priority * (20 - default_priority)));

	priority = nice(priority);
}

static void set_default_priority()
{
}

void
ExecAdhocQuery(Node *stmt, const char *s)
{
	/* Break out of the top level xact (from exec_simple_query) */
	AbortCurrentTransaction();

	{
		/* Set up our own top level mem ctx for cleanup */
		MemoryContext adhoc_cxt = AllocSetContextCreate(CurrentMemoryContext,
				"AdhocQueryContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);
		
		MemoryContext oldcontext = MemoryContextSwitchTo(adhoc_cxt);

		/* This must be set so that newly created views are tagged properly */
		SetAmContQueryAdhoc(true);

		set_nice_priority();

		/* At present, the queries always exit through the error path */
		/* The 'errors' are from query cancel, or the frontend not responding to
		 * a heartbeat */
		PG_TRY();
		{
			List *parsetree_list;
			const char* inner_sql = get_stmt_sql(stmt);
			parsetree_list = pg_parse_query(inner_sql);

			exec_adhoc_query(linitial(parsetree_list), inner_sql);
		}
		PG_CATCH();
		{
			SetAmContQueryAdhoc(false);

			set_default_priority();
			MemoryContextSwitchTo(oldcontext);
			MemoryContextResetAndDeleteChildren(adhoc_cxt);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
}

static bool check_func(FuncCall* func,
					   const char* name)
{
	Node* node = 0;
	Value* value = 0;
	A_Const *sql_arg = 0;

	if (list_length(func->funcname) != 1)
		return false;

	node = linitial(func->funcname);

	if (!IsA(node, String))
		return false;

	value = (Value*) (node);

	if (strcmp(value->val.str, name) != 0)
		return false;

	if (!func->args)
		return false;

	if (list_length(func->args) != 1)
		return false;

	sql_arg = linitial(func->args);

	if (!IsA(sql_arg, A_Const))
		return false;

	return true;
}

/* returns true iff the query is an adhoc select */
bool IsAdhocQuery(Node *node)
{
	SelectStmt *stmt = (SelectStmt*) (node);
	Node *first_node = 0;
	ResTarget *target = 0;
	FuncCall *func = 0;

	if (!IsA(node, SelectStmt))
		return false;

	if (list_length(stmt->targetList) != 1)
		return false;

	first_node = (Node *) linitial(stmt->targetList);

	if (!IsA(first_node, ResTarget))
		return false;

	target = (ResTarget*)(first_node);

	if (!target->val)
		return false;

	if (!IsA(target->val, FuncCall))
		return false;

	func = (FuncCall*)(target->val);

	if (!check_func(func, "pipeline_exec_adhoc_query"))
		return false;

	return true;
}

Datum pipeline_exec_adhoc_query(PG_FUNCTION_ARGS)
{
	elog(ERROR, "pipeline_exec_adhoc_query intercept failure");
	PG_RETURN_TEXT_P(CStringGetTextDatum(""));
}
