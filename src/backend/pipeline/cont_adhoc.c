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

#include <unistd.h>

#include <sys/time.h>
#include <sys/resource.h>

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
#include "pipeline/adhocReceiver.h"
#include "pipeline/cont_adhoc.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/dsm_cqueue.h"
#include "pipeline/miscutils.h"
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

#define SLEEP_MS 1
#define ADHOC_TIMEOUT_MS 1000

/* 
 * This module performs work analogous to the bg workers and combiners
 * (and a view select), but does it in memory using tuple stores.
 *
 * The flow of data is roughly like so:
 *
 * WorkerTupleBuffer -> Worker -> Combiner -> View -> Frontend
 */

struct AdhocExecutor
{
	dsm_cqueue *cqueue;
	int nitems;
	Timestamp start_time;
};

typedef struct
{
	Oid view_id;
	ContinuousView *view;
	DestReceiver *dest;
	QueryDesc *query_desc;
	TimestampTz last_processed;
	AdhocExecutor exec;
} AdhocWorkerState;

typedef struct
{
	QueryDesc *query_desc;

	TupleDesc tup_desc;
	TupleTableSlot *slot;

	Tuplestorestate *batch;      /* to hold pre combine rows */
	TupleHashTable existing; 	 /* only needed if is_agg is true */
	Tuplestorestate *result; 	 /* holds the combined result */

	DestReceiver *dest;

	bool is_agg;
} AdhocCombinerState;

typedef struct
{
	Tuplestorestate *batch;
	QueryDesc *query_desc;
	DestReceiver *dest;
	TupleTableSlot *slot;
} AdhocViewState;

StreamTupleState *
AdhocExecutorYieldItem(AdhocExecutor *exec, int *len)
{
	ContQueryRunParams *params = GetContQueryRunParams();

	if (exec->start_time == 0)
		exec->start_time = GetCurrentTimestamp();

	for (;;)
	{
		void *ptr;

		/* We've read a full batch or waited long enough? */
		if (exec->nitems == params->batch_size ||
				TimestampDifferenceExceeds(exec->start_time, GetCurrentTimestamp(), params->max_wait) ||
				MyContQueryProc->db_meta->terminate)
		{
			exec->start_time = 0;
			exec->nitems = 0;
			return NULL;
		}

		ptr = dsm_cqueue_peek_next(exec->cqueue, len);

		if (ptr)
		{
			exec->nitems++;
			return ptr;
		}
		else
			pg_usleep(SLEEP_MS * 1000);
	}

	return NULL;
}

static TupleHashTable
create_agg_hash(int num_cols, Oid *grp_ops, AttrNumber *group_atts)
{
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
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

static void
init_cont_query_batch_context()
{
	ContQueryBatchContext = AllocSetContextCreate(CurrentMemoryContext,
			"ContQueryBatchContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
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
	MyContQueryProc = AdhocContQueryProcGet();
}

static void
release_cont_query_proc()
{
	AdhocContQueryProcRelease(MyContQueryProc);
	MyContQueryProc = NULL;
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
			break;
	}

	return relname;
}

typedef struct
{
	Oid view_id;
	ContinuousView *view;
} ContinuousViewData;

/*
 * Reuse most of the machinery of ExecCreateContViewStmt to make the view.
 * The view will be flagged as adhoc because SetAmContQueryAdhoc(true) has been
 * called earlier - (it is passed into DefineContinuousView)
 */
static void
init_cont_view(ContinuousViewData *view_data, SelectStmt *stmt,
			   const char * querystring)
{
	RangeVar *view_name = makeRangeVar(0, get_unique_adhoc_view_name(), -1);
	CreateContViewStmt *create_stmt	= makeNode(CreateContViewStmt);

	create_stmt->into = makeNode(IntoClause);
	create_stmt->into->rel = view_name;
	create_stmt->query = (Node *) stmt;

	stmt->forContinuousView = true;

	PushActiveSnapshot(GetTransactionSnapshot());
	ExecCreateContViewStmt(create_stmt, querystring);
	PopActiveSnapshot();

	memset(view_data, 0, sizeof(ContinuousViewData));

	view_data->view_id = get_cont_view_id(view_name);
	view_data->view = GetContinuousView(view_data->view_id);

	MyContQueryProc->group_id = view_data->view_id;
}

/* get the worker plan and set up the worker state for execution */
static AdhocWorkerState *
init_adhoc_worker(ContinuousViewData data, DestReceiver *receiver, dsm_segment *segment)
{
	PlannedStmt *pstmt = 0;
	AdhocWorkerState *state = palloc0(sizeof(AdhocWorkerState));

	init_cont_query_batch_context();

	MemSet(&state->exec, 0, sizeof(AdhocExecutor));
	state->exec.cqueue = (dsm_cqueue *) dsm_segment_address(segment);

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

	RegisterSnapshot(state->query_desc->snapshot);

	ExecutorStart(state->query_desc, EXEC_NO_STREAM_LOCKING);

	state->query_desc->snapshot->active_count++;
	UnregisterSnapshot(state->query_desc->snapshot);
	UnregisterSnapshot(state->query_desc->estate->es_snapshot);
	state->query_desc->snapshot = NULL;

	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, 
			state->query_desc->operation, state->query_desc->tupDesc);

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

	plan->isContinuous = false;

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

	/* if this is an aggregating query, set up a hash table to store the groups */
	if (IsA(pstmt->planTree, Agg))
	{
		Agg *agg = (Agg *) pstmt->planTree;
		state->is_agg = true;

		state->existing = create_agg_hash(agg->numCols, agg->grpOperators,
										  agg->grpColIdx);
	}

	ExecutorStart(state->query_desc, EXEC_NO_STREAM_LOCKING);
	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, 
			state->query_desc->operation, state->query_desc->tupDesc);

	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	return state;
}

static void
set_adhoc_executor(PlanState *planstate, AdhocExecutor *exec)
{
	if (planstate == NULL)
		return;

	if (IsA(planstate, ForeignScanState))
	{
		ForeignScanState *fss = (ForeignScanState *) planstate;
		if (IsStream(RelationGetRelid(fss->ss.ss_currentRelation)))
		{
			StreamScanState *scan = (StreamScanState *) fss->fdw_state;
			scan->adhoc_executor = exec;
		}

		return;
	}
	else if (IsA(planstate, SubqueryScanState))
	{
		set_adhoc_executor(((SubqueryScanState *) planstate)->subplan, exec);
		return;
	}

	set_adhoc_executor(planstate->lefttree, exec);
	set_adhoc_executor(planstate->righttree, exec);
}

/* run the worker - this will block for one sec waiting for tuples in the worker tuple
 * buffer. 
 *
 * If it has no tuples, it will return false.
 *
 * Otherwise, tuples are processed with the worker plan and sent to the dest receiver
 */
static bool
exec_adhoc_worker(AdhocWorkerState *state)
{
	struct Plan *plan;
	EState *estate;
	int num_processed = 0;

	dsm_cqueue_wait_non_empty(state->exec.cqueue, ADHOC_TIMEOUT_MS);

	CHECK_FOR_INTERRUPTS();

	state->query_desc->estate = CreateEState(state->query_desc);
	estate = state->query_desc->estate;

	SetEStateSnapshot(estate);
	plan = state->query_desc->plannedstmt->planTree;

	state->query_desc->planstate = 
		ExecInitNode(plan, state->query_desc->estate, EXEC_NO_STREAM_LOCKING);
	set_adhoc_executor(state->query_desc->planstate, &state->exec);

	ExecutePlan(estate, state->query_desc->planstate, 
				state->query_desc->operation,
				true, 0, 0, ForwardScanDirection, state->dest);

	ExecEndNode(state->query_desc->planstate);
	state->query_desc->planstate = NULL;
	num_processed = estate->es_processed + estate->es_filtered;

	if (num_processed)
		state->last_processed = GetCurrentTimestamp();

	UnsetEStateSnapshot(estate);

	state->query_desc->estate = NULL;
	estate = state->query_desc->estate;

	MemoryContextResetAndDeleteChildren(ContQueryBatchContext);

	dsm_cqueue_pop_peeked(state->exec.cqueue);

	return num_processed > 0;
}

static void adhoc_sync_combine(AdhocCombinerState *state);

/* 
 * combine rows in state->batch (from worker)
 */
static void
exec_adhoc_combiner(AdhocCombinerState *state)
{
	struct Plan *plan = 0;
	EState *estate = 0;

	/* 
	 * if this is an agg query, check for any existing groups, 
	 * and put them in the batch to be processed also 
	 */
	if (state->is_agg)
	{
		foreach_tuple(state->slot, state->batch)
		{
			HeapTupleEntry entry = (HeapTupleEntry) 
				LookupTupleHashEntry(state->existing, state->slot, NULL);

			if (entry)
				tuplestore_puttuple(state->result, entry->tuple);
		}

		foreach_tuple(state->slot, state->result)
			tuplestore_puttupleslot(state->batch, state->slot);

		tuplestore_clear(state->result);
		tuplestore_rescan(state->batch);
	}

	/* run the combine plan */

	state->query_desc->estate = CreateEState(state->query_desc);
	estate = state->query_desc->estate;

	SetEStateSnapshot(estate);

	plan = state->query_desc->plannedstmt->planTree;

	state->query_desc->planstate = 
		ExecInitNode(plan, state->query_desc->estate, 0);

	ExecutePlan(estate, state->query_desc->planstate, 
				state->query_desc->operation,
				true, 0, 0, ForwardScanDirection, state->dest);

	ExecEndNode(state->query_desc->planstate);
	state->query_desc->planstate = NULL;

	UnsetEStateSnapshot(estate);

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
	struct Plan *plan = 0;
	EState *estate = 0;

	tuplestore_rescan(state->batch);

	state->query_desc->estate = CreateEState(state->query_desc);
	estate = state->query_desc->estate;
	SetEStateSnapshot(estate);

	plan = state->query_desc->plannedstmt->planTree;

	state->query_desc->planstate = 
		ExecInitNode(plan, state->query_desc->estate, 0);

	ExecutePlan(estate, state->query_desc->planstate, 
				state->query_desc->operation,
				true, 0, 0, ForwardScanDirection, state->dest);

	ExecEndNode(state->query_desc->planstate);
	state->query_desc->planstate = NULL;

	UnsetEStateSnapshot(estate);

	state->query_desc->estate = NULL;
	estate = state->query_desc->estate;

	tuplestore_clear(state->batch);
}

/* Execute a drop view statement to cleanup the adhoc view */
void
CleanupAdhocContinuousView(ContinuousView *view)
{
	DestReceiver *receiver = 0;
	DropStmt *stmt = makeNode(DropStmt);
	List *querytree_list;
	Node *plan;
	Portal portal;

	stmt->objects = list_make1(list_make1(makeString(NameStr(view->name))));
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
	dsm_segment *segment;
	ResourceOwner owner;
} CleanupData;

/* To be called when adhoc query is finished */
static void
cleanup(int code, Datum arg)
{
	CleanupData *cleanup = (CleanupData *) arg;

	if (IsTransactionState())
		AbortCurrentTransaction();

	XactReadOnly = false;
	
	release_cont_query_proc();

	dsm_detach(cleanup->segment);
	ResourceOwnerDelete(cleanup->owner);

	StartTransactionCommand();
	CleanupAdhocContinuousView(cleanup->cont_view_data->view);
	CommitTransactionCommand();
}

static dsm_segment *
create_dsm_cqueue(ResourceOwner owner)
{
	dsm_segment *segment;
	dsm_handle handle;
	Size size;
	void *ptr;

	CurrentResourceOwner = owner;

	/* Create dsm_segment and pin it. */
	size = continuous_query_ipc_shared_mem * 1024;
	segment = dsm_create(size);
	dsm_pin_mapping(segment);
	handle = dsm_segment_handle(segment);

	/* Initialize dsm_cqueue. */
	ptr = dsm_segment_address(segment);
	dsm_cqueue_init(ptr, size, GetContProcTrancheId());
	dsm_cqueue_set_handlers((dsm_cqueue *) ptr, StreamTupleStatePeekFn, NULL, StreamTupleStateCopyFn);

	MyContQueryProc->dsm_handle = handle;

	return segment;
}

/*
 * Initialize the worker, combiner and view modules, then loop until done.
 * The current exit conditions are when the query is cancelled, or when the frontend
 * goes away (the heartbeat will fail to send)
 */
static void
exec_adhoc_query(SelectStmt *stmt, const char *s)
{
	int numCols = 1;
	AttrNumber one = 1;
	AttrNumber *keyColIdx = &one;

	AdhocWorkerState *worker_state;
	AdhocCombinerState *combiner_state;
	AdhocViewState *view_state;
	DestReceiver *adhoc_receiver;
	DestReceiver *worker_receiver;

	MemoryContext run_cxt = CurrentMemoryContext;
	ContinuousViewData view_data;
	Tuplestorestate *batch;
	CleanupData cleanup_data;
	dsm_segment *segment;

	ResourceOwner owner = ResourceOwnerCreate(CurrentResourceOwner, "LongTermResOwner");

	acquire_cont_query_proc();

	if (!MyContQueryProc)
		elog(ERROR, "too many adhoc processes");

	/* Create dsm_cqueue */
	segment = create_dsm_cqueue(owner);

	StartTransactionCommand();
	MemoryContextSwitchTo(run_cxt);
	init_cont_view(&view_data, stmt, s);
	CommitTransactionCommand();

	worker_receiver = CreateDestReceiver(DestTuplestore);
	batch = tuplestore_begin_heap(true, true, 
				continuous_query_combiner_work_mem);
	SetTuplestoreDestReceiverParams(worker_receiver, batch,
			CurrentMemoryContext, true);
	adhoc_receiver = CreateAdhocDestReceiver();

	StartTransactionCommand();
	MemoryContextSwitchTo(run_cxt);
	CurrentResourceOwner = owner;
	worker_state = init_adhoc_worker(view_data, worker_receiver, segment);
	combiner_state = init_adhoc_combiner(view_data, batch);

	if (combiner_state->existing)
	{
		keyColIdx = combiner_state->existing->keyColIdx;
		numCols = combiner_state->existing->numCols;
	}

	SetAdhocDestReceiverParams(adhoc_receiver, combiner_state->is_agg, keyColIdx, numCols);

	view_state = init_adhoc_view(view_data, combiner_state->result, adhoc_receiver);
	view_state->slot = combiner_state->slot;

	CommitTransactionCommand();

	XactReadOnly = true;

	cleanup_data.cont_view_data = &view_data;
	cleanup_data.worker_state = worker_state;
	cleanup_data.owner = owner;
	cleanup_data.segment = segment;

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
				AdhocDestReceiverHeartbeat((AdhocDestReceiver *) adhoc_receiver);
			}

			CommitTransactionCommand();
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(cleanup, PointerGetDatum(&cleanup_data));
}

/* 
 * grabs the sql out of a statement like 
 * select pipeline_exec_adhoc_query('select avg(x::int) from stream');
 */
static const char *
get_stmt_sql(Node *node)
{
	/* assumes IsAdhocQuery has returned true on node already. */

	SelectStmt *stmt = (SelectStmt *) (node);
	ResTarget *target;
	FuncCall *func = 0;
	A_Const *sql_arg = 0;

	target = (ResTarget *) linitial(stmt->targetList);
	func = (FuncCall *) target->val;
	sql_arg = linitial(func->args);

	return strVal(&sql_arg->val);
}

void
ExecAdhocQuery(Node *select)
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
		
		MemoryContext old = MemoryContextSwitchTo(adhoc_cxt);

		/* This must be set so that newly created views are tagged properly */
		SetAmContQueryAdhoc(true);

		/* give up some cpu */
		SetNicePriority();

		/* At present, the queries always exit through the error path
		 * The 'errors' are from query cancel, or the frontend not 
		 * responding to a heartbeat
		 */
		PG_TRY();
		{
			List *parsetree_list;
			const char *inner_sql = get_stmt_sql(select);
			parsetree_list = pg_parse_query(inner_sql);

			exec_adhoc_query(linitial(parsetree_list), inner_sql);
		}
		PG_CATCH();
		{
			SetAmContQueryAdhoc(false);
			SetDefaultPriority();

			MemoryContextSwitchTo(old);
			MemoryContextResetAndDeleteChildren(adhoc_cxt);

			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	StartTransactionCommand();
}

/* 
 * Functions for intercepting the select pipeline_exec_adhoc_query(...) call.
 *
 * We have to do it this way rather than executing a regular function call, 
 * because the environment inside a portal is not conducive to sending
 * msgs async to the client.
 */
#define PIPELINE_FN_NAME "pipeline_exec_adhoc_query"

static bool
check_adhoc_func(FuncCall *func)
{
	Node *node;
	A_Const *sql_arg;

	if (list_length(func->funcname) != 1)
		return false;

	node = linitial(func->funcname);

	if (strcmp(strVal(node), PIPELINE_FN_NAME) != 0)
		return false;

	if (!func->args || list_length(func->args) != 1)
		return false;

	sql_arg = linitial(func->args);

	if (!IsA(sql_arg, A_Const))
		return false;

	return true;
}

bool
IsAdhocQuery(Node *node)
{
	SelectStmt *stmt = (SelectStmt *) (node);
	ResTarget *target;
	FuncCall *func;

	if (!continuous_queries_adhoc_enabled)
		return false;

	if (!IsA(node, SelectStmt))
		return false;

	if (list_length(stmt->targetList) != 1)
		return false;

	target = (ResTarget *) linitial(stmt->targetList);

	if (!IsA(target->val, FuncCall))
		return false;

	func = (FuncCall *) target->val;

	if (!check_adhoc_func(func))
		return false;

	return true;
}

/* 
 * Placeholder function to prevent people from defining this function.
 * This function is meant to be intercepted, not executed, hence the elog
 * error if it does get executed.
 */
Datum
pipeline_exec_adhoc_query(PG_FUNCTION_ARGS)
{
	if (!continuous_queries_adhoc_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("adhoc continuous queries are disabled"),
				 errhint("Enable adhoc continuous queries using the \"continuous_queries_adhoc_enabled\" parameter.")));
	else
		elog(ERROR, "pipeline_exec_adhoc_query intercept failure");

	PG_RETURN_TEXT_P(CStringGetTextDatum(""));
}

AdhocInsertState *
AdhocInsertStateCreate(Bitmapset *queries)
{
	int nqueries = bms_num_members(queries);
	AdhocInsertState *astate;
	ContQueryProc *procs;
	int i;
	int idx = 0;

	if (nqueries == 0)
		return NULL;

	astate = palloc0(sizeof(AdhocInsertState) +
			(sizeof(AdhocQueryState) * nqueries));
	astate->nqueries = nqueries;

	procs = GetContQueryAdhocProcs();
	for (i = 0; i < max_worker_processes; i++)
	{
		ContQueryProc *proc = &procs[i];
		dsm_segment *segment;
		dsm_handle handle = proc->dsm_handle;
		AdhocQueryState *qstate;

		if (proc->group_id == 0 || handle == 0 || !bms_is_member(proc->group_id, queries))
			continue;

		segment = dsm_find_or_attach(handle);
		if (segment == NULL)
			continue;

		qstate = &astate->queries[idx];
		qstate->active = &proc->group_id;
		qstate->segment = segment;
		qstate->cqueue = dsm_segment_address(segment);

		idx++;
	}

	if (idx == 0)
	{
		pfree(astate);
		return NULL;
	}

	return astate;
}

static void
adhoc_insert_state_resync(AdhocInsertState *astate)
{
	int i;

	for (i = 0; i < astate->nqueries; i++)
	{
		AdhocQueryState *qstate = &astate->queries[i];

		if (*qstate->active == 0)
		{
			dsm_detach(qstate->segment);
			qstate->segment = NULL;
			qstate->cqueue = NULL;
		}
	}
}

void
AdhocInsertStateSend(AdhocInsertState *astate, StreamTupleState *sts, int len)
{
	int i;
	InsertBatchAck *ack = sts->ack;
	Bitmapset *queries = sts->queries;

	if (astate == NULL)
		return;

	/* Adhoc queries don't care about ack and queries */
	if (ack)
	{
		sts->ack = NULL;
		len -= sizeof(InsertBatchAck);
	}

	if (queries)
	{
		sts->queries = NULL;
		len -= BITMAPSET_SIZE(queries->nwords);
	}

	adhoc_insert_state_resync(astate);

	for (i = 0; i < astate->nqueries; i++)
	{
		AdhocQueryState *qstate = &astate->queries[i];

		if (qstate->segment == NULL)
			continue;

		dsm_cqueue_push(qstate->cqueue, sts, len);
	}

	sts->ack = ack;
	sts->queries = queries;
}

void
AdhocInsertStateDestroy(AdhocInsertState *astate)
{
	int i;

	if (astate == NULL)
		return;

	for (i = 0; i < astate->nqueries; i++)
	{
		AdhocQueryState *qstate = &astate->queries[i];

		if (qstate->segment)
			dsm_detach(qstate->segment);
	}

	pfree(astate);
}
