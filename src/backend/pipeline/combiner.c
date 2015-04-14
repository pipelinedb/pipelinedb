/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 *	  Combiner process functionality
 *
 * src/backend/pipeline/combiner.c
 *
 *-------------------------------------------------------------------------
 */
#include <sys/socket.h>
#include <time.h>
#include <sys/un.h>
#include <sys/unistd.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/tupletableReceiver.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "pipeline/combiner.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cqplan.h"
#include "pipeline/cqproc.h"
#include "pipeline/tuplebuf.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

/* duration in seconds after which to replan the cached groups retrieval plan */
#define GROUPS_PLAN_LIFESPAN 10

static TupleBufferReader *reader = NULL;

int combiner_work_mem = 16384;
int combiner_synchronous_commit = SYNCHRONOUS_COMMIT_OFF;

typedef struct CombineState
{
	/* combine plan */
	PlannedStmt *plan;
	/* plan for selecting existing groups */
	PlannedStmt *groupsplan;
	/* timestamp of the last time groupsplan was generated */
	TimestampTz lastgroupsplan;
	/* continuous view this combiner is running for */
	RangeVar *cv;
	/* materialization that this combiner is updating */
	RangeVar *matrel;
	/* descriptor of the materialization table */
	TupleDesc cvdesc;
	/* temporary context for combiner */
	MemoryContext tmpcontext;
	/* context that lives for the duration of the combiner proc */
	MemoryContext context;
	/* true if the combine plan aggregates */
	bool isagg;
	/* slot for combine plan result tuples */
	TupleTableSlot *resultslot;
	/* number of attributes the combine plan is grouping on */
	int ngroupatts;
	/* attribute numbers that the combine plan is grouping on */
	AttrNumber *groupatts;
	/* equality operators for group attributes */
	Oid *groupops;
} CombineState;

/*
 * receive_tuple
 */
static bool
receive_tuple(TupleTableSlot *slot)
{
	TupleBufferSlot *tbs;

	if (!TupIsNull(slot))
		ExecClearTuple(slot);

	tbs = TupleBufferPinNextSlot(reader);

	if (tbs == NULL)
		return false;

	IncrementCQRead(1, tbs->size);

	ExecStoreTuple(heap_copytuple(tbs->tuple->heaptup), slot, InvalidBuffer, false);
	TupleBufferUnpinSlot(reader, tbs);

	return true;
}

/*
 * prepare_combine_plan
 *
 * Retrieves a the cached combine plan for a continuous view, creating it if necessary
 */
static PlannedStmt *
prepare_combine_plan(PlannedStmt *plan, Tuplestorestate *store, TupleDesc *desc)
{
	TuplestoreScan *scan = NULL;

	/*
	 * Mark plan as not continuous now because we'll be repeatedly
	 * executing it in a new portal. We also need to set its batch
	 * size to 0 so that TuplestoreScans don't return early. Since
	 * they're not being executed continuously, they'd never
	 * see anything after the first batch was consumed.
	 *
	 */
	plan->is_continuous = false;
	plan->cq_state->batchsize = 0;

	if (IsA(plan->planTree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree;
	else if ((IsA(plan->planTree, Agg) || IsA(plan->planTree, ContinuousUnique)) &&
			IsA(plan->planTree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree;
	else if (IsA(plan->planTree, Agg) &&
			IsA(plan->planTree->lefttree, Sort) &&
			IsA(plan->planTree->lefttree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree->lefttree;
	else
		elog(ERROR, "couldn't find TuplestoreScan node in combiner's plan");

	scan->store = store;

	*desc = ExecTypeFromTL(((Plan *) scan)->targetlist, false);

	return plan;
}

/*
 * get_values
 *
 * Given an incoming batch, returns a VALUES clause containing each tuple's
 * group columns that can be joined against with the matrel's existing groups.
 */
static List *
get_values(CombineState *cstate, Tuplestorestate *batch)
{
	TupleTableSlot *slot = cstate->resultslot;
	List *values = NIL;
	int i;

	/*
	 * Generate a VALUES list of the incoming groups
	 */
	foreach_tuple(slot, batch)
	{
		List *tup = NIL;

		for (i = 0; i < cstate->ngroupatts; i++)
		{
			AttrNumber groupattr = cstate->groupatts[i];
			Form_pg_attribute attr = cstate->cvdesc->attrs[groupattr - 1];
			Type typeinfo;
			bool isnull;
			Datum d;
			Const *c;
			int length;

			typeinfo = typeidType(attr->atttypid);
			length = typeLen(typeinfo);
			ReleaseSysCache((HeapTuple) typeinfo);

			d = slot_getattr(slot, groupattr, &isnull);
			c = makeConst(attr->atttypid, attr->atttypmod,
					attr->attcollation, length, d, isnull, attr->attbyval);

			tup = lappend(tup, c);
		}

		values = lappend(values, tup);
	}

	assign_expr_collations(NULL, (Node *) values);

	return values;
}

/*
 * set_values
 *
 * Given an existing group retrieval plan, attach the VALLUES list that
 * contains the groups to be looked up in the matrel.
 */
static void
set_values(PlannedStmt *plan, List *values)
{
	PhysicalGroupLookup *lookup;
	NestLoop *nl;
	ValuesScan *scan;

	if (!IsA(plan->planTree, PhysicalGroupLookup))
		elog(ERROR, "unexpected group retrieval plan: %d", nodeTag(plan->planTree));

	lookup = (PhysicalGroupLookup *) plan->planTree;

	if (!IsA(lookup->plan.lefttree, NestLoop))
		elog(ERROR, "unexpected join type found in group retrieval plan: %d", nodeTag(lookup->plan.lefttree));

	nl = (NestLoop *) lookup->plan.lefttree;

	if (!IsA(nl->join.plan.lefttree, ValuesScan))
		elog(ERROR, "could not find values scan in group retrieval plan");

	scan = (ValuesScan *) nl->join.plan.lefttree;
	scan->values_lists = values;
}

/*
 * get_groups
 *
 * Given a tuplestore containing incoming tuples to combine with
 * on-disk tuples, generates a VALUES list that can be joined
 * against with on-disk tuples.
 */
static Node*
get_groups(CombineState *cstate, List *values, ParseState *ps)
{
	Node *where;
	SubLink *sub = makeNode(SubLink);
	SelectStmt *sel = makeNode(SelectStmt);
	RowExpr *row = makeNode(RowExpr);
	int i;

	sel->valuesLists = values;

	/*
	 * Now create a subquery to join the matrel against, which
	 * will result in a retrieval of existing groups to update.
	 */
	row->args = NIL;

	for (i = 0; i < cstate->ngroupatts; i++)
	{
		AttrNumber groupattr = cstate->groupatts[i];
		Form_pg_attribute attr = cstate->cvdesc->attrs[groupattr - 1];
		ColumnRef *cref = makeNode(ColumnRef);

		cref->fields = list_make1(makeString(NameStr(attr->attname)));
		cref->location = -1;
		row->args = lappend(row->args, cref);
	}

	sub->subLinkType = ANY_SUBLINK;
	sub->testexpr = (Node *) row;
	sub->operName = list_make1(makeString("="));
	sub->subselect = (Node *) sel;

	where = transformExpr(ps, (Node *) sub, EXPR_KIND_WHERE);

	return (Node *) where;
}

/*
 * get_cached_groups_plan
 *
 * Plans and caches the combiner's existing groups retrieval plan,
 * or simply returns the cached plan if it's still valid.
 */
static PlannedStmt *
get_cached_groups_plan(CombineState *cstate, List *values)
{
	MemoryContext old;
	ParseState *ps;
	Query *query;
	List *qlist;
	SelectStmt *sel;
	ResTarget *res;
	A_Star *star;
	ColumnRef *cref;
	PlannedStmt *plan;

	if (cstate->groupsplan != NULL &&
			!TimestampDifferenceExceeds(cstate->lastgroupsplan, GetCurrentTimestamp(), GROUPS_PLAN_LIFESPAN * 1000))
	{
		if (values)
			set_values(cstate->groupsplan, values);

		/* use a fresh copy of the plan, as it may be modified by the executor */
		old = MemoryContextSwitchTo(cstate->context);
		plan = copyObject(cstate->groupsplan);
		MemoryContextSwitchTo(old);

		return plan;
	}

	/* cache miss, plan the query */

	if (cstate->groupsplan != NULL)
		pfree(cstate->groupsplan);

	sel = makeNode(SelectStmt);
	res = makeNode(ResTarget);
	star = makeNode(A_Star);
	cref = makeNode(ColumnRef);

	cref->fields = list_make1(star);
	res->val = (Node *) cref;
	sel->targetList = list_make1(res);
	sel->fromClause = list_make1(cstate->matrel);

	/* populate the ParseState's p_varnamespace member */
	ps = make_parsestate(NULL);
	transformFromClause(ps, sel->fromClause);

	qlist = pg_analyze_and_rewrite((Node *) sel, NULL, NULL, 0);
	query = (Query *) linitial(qlist);

	if (cstate->ngroupatts > 0)
	{
		Node *groups = get_groups(cstate, values, ps);
		query->jointree = makeFromExpr(query->jointree->fromlist, groups);
		query->hasSubLinks = true;
	}

	plan = pg_plan_query(query, 0, NULL);

	old = MemoryContextSwitchTo(cstate->context);
	cstate->groupsplan = copyObject(plan);
	MemoryContextSwitchTo(old);

	cstate->lastgroupsplan = GetCurrentTimestamp();

	return plan;
}

/*
 * select_existing_groups
 *
 * Adds all existing groups in the matrel to the combine input set
 */
static void
select_existing_groups(CombineState *cstate, Tuplestorestate *batch, TupleHashTable existing)
{
	PlannedStmt *plan = NULL;
	Portal portal;
	DestReceiver *dest;
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = cstate->resultslot;
	List *tups = NIL;
	ListCell *lc;
	List *values = NIL;

	/*
	 * If we're not grouping on any columns, then there's only one row to look up
	 * so we don't need to do a VALUES-matrel join.
	 */
	if (cstate->isagg && cstate->ngroupatts > 0)
		values = get_values(cstate, batch);


	plan = get_cached_groups_plan(cstate, values);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	dest = CreateDestReceiver(DestTupleTable);
	SetTupleTableDestReceiverParams(dest, existing, CurrentMemoryContext, true);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	tuplestore_rescan(batch);
	foreach_tuple(slot, batch)
	{
		HeapTuple tup = ExecCopySlotTuple(slot);
		tups = lappend(tups, tup);
	}
	tuplestore_clear(batch);

	/*
	 * Now add the merge targets that already exist in the continuous view's table
	 * to the input of the final merge query
	 */
	hash_seq_init(&status, existing->hashtab);
	while ((entry = (HeapTupleEntry) hash_seq_search(&status)) != NULL)
		tuplestore_puttuple(batch, entry->tuple);

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(batch, tup);
	}

	PortalDrop(portal, false);
}

/*
 * sync_combine
 *
 * Writes the combine results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
static void
sync_combine(CombineState *cstate, Tuplestorestate *results, TupleHashTable existing)
{
	TupleTableSlot *slot = cstate->resultslot;
	Relation rel = heap_openrv(cstate->matrel, RowExclusiveLock);
	int size = sizeof(bool) * slot->tts_tupleDescriptor->natts;
	bool *replace_all = palloc0(size);
	ResultRelInfo *ri = CQMatViewOpen(rel);

	MemSet(replace_all, true, size);

	foreach_tuple(slot, results)
	{
		HeapTupleEntry update = NULL;
		slot_getallattrs(slot);

		if (existing)
			update = (HeapTupleEntry) LookupTupleHashEntry(existing, slot, NULL);

		if (update)
		{
			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			HeapTuple updated = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);

			ExecStoreTuple(updated, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot);
			IncrementCQUpdate(1, HEAPTUPLESIZE + updated->t_len);
		}
		else
		{
			/* No existing tuple found, so it's an INSERT */
			ExecCQMatRelInsert(ri, slot);
			IncrementCQWrite(1, HEAPTUPLESIZE + slot->tts_tuple->t_len);
		}
	}
	CQMatViewClose(ri);
	relation_close(rel, RowExclusiveLock);
}

/*
 * combine
 *
 * Combines partial results of a continuous query with existing rows in the continuous view
 */
static void
combine(CombineState *cstate, Tuplestorestate *batch)
{
	Portal portal;
	DestReceiver *dest = CreateDestReceiver(DestTuplestore);
	Tuplestorestate *result = NULL;
	TupleHashTable existing = NULL;

	PushActiveSnapshot(GetTransactionSnapshot());

	if (cstate->isagg)
	{
		FmgrInfo *eq_funcs;
		FmgrInfo *hash_funcs;

		execTuplesHashPrepare(cstate->ngroupatts, cstate->groupops, &eq_funcs, &hash_funcs);
		existing = BuildTupleHashTable(cstate->ngroupatts, cstate->groupatts, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CurrentMemoryContext, cstate->tmpcontext);
		select_existing_groups(cstate, batch, existing);
	}

	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(cstate->plan),
					  NULL);

	result = tuplestore_begin_heap(true, true, combiner_work_mem);
	SetTuplestoreDestReceiverParams(dest, result, CurrentMemoryContext, true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	PopActiveSnapshot();

	tuplestore_clear(batch);

	sync_combine(cstate, result, existing);

	if (existing)
		hash_destroy(existing->hashtab);

	tuplestore_end(result);
	PortalDrop(portal, false);
}

/*
 * init_combine_state
 *
 * Initialize state to be used by repeated calls to combine
 */
static void
init_combine_state(CombineState *cstate, char *cvname, PlannedStmt *plan,
		TupleDesc desc, MemoryContext context, MemoryContext tmpcontext)
{
	MemSet(cstate, 0, sizeof(CombineState));

	cstate->plan = plan;
	cstate->cv = plan->cq_target;
	cstate->matrel = makeRangeVar(NULL, NameStr(plan->cq_state->matrelname), -1);
	cstate->cvdesc = desc;
	cstate->context = context;
	cstate->tmpcontext = tmpcontext;
	cstate->resultslot = MakeSingleTupleTableSlot(cstate->cvdesc);
	cstate->isagg = false;
	cstate->groupsplan = NULL;

	if (IsA(plan->planTree, Agg))
	{
		Agg *agg = (Agg *) plan->planTree;

		cstate->groupatts = agg->grpColIdx;
		cstate->ngroupatts = agg->numCols;
		cstate->groupops = agg->grpOperators;
		cstate->isagg = true;
	}
}

/*
 * ContinuousQueryCombinerRun
 */
void
ContinuousQueryCombinerRun(Portal portal, ContinuousViewState *state, QueryDesc *queryDesc, ResourceOwner owner)
{
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	ResourceOwner save = CurrentResourceOwner;
	TupleTableSlot *slot;
	TupleDesc workerdesc;
	Tuplestorestate *batch;
	long count = 0;
	int batchsize = queryDesc->plannedstmt->cq_state->batchsize;
	int timeout = queryDesc->plannedstmt->cq_state->maxwaitms;
	char *cvname = rv->relname;
	PlannedStmt *combineplan;
	CQProcEntry *entry = GetCQProcEntry(MyCQId);
	MemoryContext oldcontext;
	MemoryContext runctx = AllocSetContextCreate(TopMemoryContext,
			"CombinerRunContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext combinectx;
	MemoryContext tmpctx;
	CombineState cstate;

	cq_stat_initialize(state->viewid, MyProcPid);

	CQExecutionContext = AllocSetContextCreate(runctx, "CQExecutionContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	combinectx = AllocSetContextCreate(CQExecutionContext,
			"CombineContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	tmpctx = AllocSetContextCreate(CQExecutionContext,
			"CombineTmpContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	CurrentResourceOwner = owner;

	elog(LOG, "\"%s\" combiner %d running", cvname, MyProcPid);

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(runctx);

	MarkCombinerAsRunning(MyCQId);
	pgstat_report_activity(STATE_RUNNING, queryDesc->sourceText);

	TupleBufferInitLatch(CombinerTupleBuffer, MyCQId, 0, &MyProc->procLatch);
	reader = TupleBufferOpenReader(CombinerTupleBuffer, MyCQId, 0, 1);

	/*
	 * Create tuple store and slot outside of combinectx and tmpctx,
	 * so that we don't lose received tuples in case of errors in the loop
	 * below.
	 */
	batch = tuplestore_begin_heap(true, true, combiner_work_mem);
	combineplan = prepare_combine_plan(queryDesc->plannedstmt, batch, &workerdesc);
	slot = MakeSingleTupleTableSlot(workerdesc);
	init_combine_state(&cstate, cvname, combineplan, workerdesc, runctx, tmpctx);

	MemoryContextSwitchTo(oldcontext);

	CommitTransactionCommand();

	MemoryContextSwitchTo(combinectx);

retry:
	PG_TRY();
	{
		bool workers_done = false;
		TimestampTz last_combine = GetCurrentTimestamp();
		TimestampTz last_receive = GetCurrentTimestamp();

		for (;;)
		{
			bool force = false;
			bool found_tuple;

			if (count == 0 && entry->active && !TupleBufferHasUnreadSlots())
			{
				if (TimestampDifferenceExceeds(last_receive, GetCurrentTimestamp(), empty_tuple_buffer_wait_time))
				{
					/* force stats flush */
					cq_stat_report(true);

					pgstat_report_activity(STATE_IDLE, queryDesc->sourceText);
					TupleBufferWait(CombinerTupleBuffer, MyCQId, 0);
					pgstat_report_activity(STATE_RUNNING, queryDesc->sourceText);
				}
				else
					pg_usleep(CQ_DEFAULT_SLEEP_MS * 1000);
			}

			TupleBufferResetNotify(CombinerTupleBuffer, MyCQId, 0);

			CurrentResourceOwner = owner;

			found_tuple = receive_tuple(slot);

			/*
			 * If we get a null tuple, we either want to combine the current batch
			 * or wait a little while longer for more tuples before forcing the batch
			 */
			if (!found_tuple)
			{
				if (timeout > 0)
				{
					if (!TimestampDifferenceExceeds(last_combine, GetCurrentTimestamp(), timeout))
						continue; /* timeout not reached yet, keep scanning for new tuples to arrive */
				}

				force = true;
			}
			else
			{
				last_receive = GetCurrentTimestamp();
				tuplestore_puttupleslot(batch, slot);
				count++;
			}

			if (count > 0 && (count == batchsize || force))
			{
				SyncCommitLevel save = synchronous_commit;
				/*
				 * Start/CommitTransactionCommand put us in TopTransactionContext,
				 * so we need to immediately switch back to the combine context
				 */
				StartTransactionCommand();
				MemoryContextSwitchTo(combinectx);

				combine(&cstate, batch);

				/* commit asynchronously for better performance */
				synchronous_commit = combiner_synchronous_commit;
				CommitTransactionCommand();
				synchronous_commit = save;

				MemoryContextSwitchTo(combinectx);

				TupleBufferClearPinnedSlots();

				MemoryContextReset(CQExecutionContext);

				last_combine = GetCurrentTimestamp();
				count = 0;

				IncrementCQExecutions(1);

				/*
				 * Send stats to the collector
				 */
				cq_stat_report(false);
			}


			/*
			 * If we received a tuple in this iteration, poll the buffer again.
			 */
			if (found_tuple)
				continue;

			/* Has the CQ been deactivated? */
			if (!entry->active)
			{
				/*
				 * Ensure that all workers have terminated. There is a continue here
				 * because we wanna poll the buffer one last time after the workers have
				 * terminated.
				 */
				if (!workers_done)
				{
					workers_done = AreCQWorkersStopped(MyCQId);
					continue;
				}

				/*
				 * By this point the worker processes have terminated and
				 * we received no new tuples in the previous iteration.
				 * If there are some unmerged tuples, force merge them.
				 */
				if (count)
				{
					force = true;
					continue;
				}

				break;
			}
		}
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();

		/*
		 * If we were in a transaction state, then abort it and dump
		 * anything in this batch.
		 */
		if (IsTransactionState())
		{
			AbortCurrentTransaction();
			ExecClearTuple(slot);
			tuplestore_clear(batch);
			count = 0;
		}

		TupleBufferUnpinAllPinnedSlots();

		MemoryContextReset(CQExecutionContext);

		IncrementCQErrors(1);

		if (continuous_query_crash_recovery)
			goto retry;
	}
	PG_END_TRY();

	TupleBufferCloseReader(reader);
	TupleBufferClearReaders();

	MemoryContextDelete(runctx);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Remove proc-level stats
	 */
	cq_stat_report(true);
	cq_stat_send_purge(state->viewid, MyProcPid, CQ_STAT_COMBINER);

	CurrentResourceOwner = save;
}
