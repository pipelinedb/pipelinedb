/* Copyright (c) 2013-2015 PipelineDB */
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

static TupleBufferReader *reader = NULL;

int combiner_work_mem = 16384;
int combiner_synchronous_commit = SYNCHRONOUS_COMMIT_OFF;

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
	TuplestoreScan *scan;

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
	else if (IsA(plan->planTree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree;
	else
		elog(ERROR, "couldn't find TuplestoreScan node in combiner's plan");

	scan->store = store;

	*desc = ExecTypeFromTL(((Plan *) scan)->targetlist, false);

	return plan;
}

/*
 * get_retrieval_where_clause
 *
 * Given a tuplestore containing incoming tuples to combine with
 * on-disk tuples, generates a WHERE clause that can be used to
 * retrieve all corresponding on-disk tuples with a single query.
 */
static Node*
get_retrieval_where_clause(Tuplestorestate *incoming, TupleDesc desc,
		AttrNumber *merge_attrs, int num_merge_attrs, ParseState *ps)
{
	List *name = list_make1(makeString("="));
	Node *where;
	Expr *dnf = NULL;
	List *args = NIL;
	TupleTableSlot *slot = MakeSingleTupleTableSlot(desc);

	foreach_tuple(slot, incoming)
	{
		List *cnfExprs = NIL;
		Node *arg;
		int i;

		for (i = 0; i < num_merge_attrs; i++)
		{
			AttrNumber merge_attr = merge_attrs[i];
			Form_pg_attribute attr = desc->attrs[merge_attr - 1];
			ColumnRef *cref;
			Type typeinfo;
			int length;
			Expr *expr;
			bool isnull;
			Datum d;
			Const *c;
			NullTest *null = makeNode(NullTest);

			null->argisrow = false;
			null->nulltesttype = IS_NULL;

			typeinfo = typeidType(attr->atttypid);
			length = typeLen(typeinfo);
			ReleaseSysCache((HeapTuple) typeinfo);

			d = slot_getattr(slot, merge_attr, &isnull);

			cref = makeNode(ColumnRef);
			cref->fields = list_make1(makeString(NameStr(attr->attname)));
			cref->location = -1;

			if (isnull)
			{
				null->arg = (Expr *) cref;
				expr = (Expr *) copyObject(null);
			}
			else
			{
				c = makeConst(attr->atttypid, attr->atttypmod,
						attr->attcollation, length, d, isnull, attr->attbyval);

				expr = (Expr *) makeA_Expr(AEXPR_OP, name, (Node *) cref, (Node *) c, -1);
			}
			/*
			 * If we're grouping on multiple columns, the WHERE predicate must
			 * include a conjunction of all GROUP BY column values for each incoming
			 * tuple. For example, consider the query:
			 *
			 *   SELECT col0, col1, COUNT(*) FROM stream GROUP BY col0, col1
			 *
			 * If we have incoming tuples,
			 *
			 *   (0, 1, 10), (4, 3, 200)
			 *
			 * then we need to look for any tuples on disk for which the following
			 * predicate evaluates to true:
			 *
			 *   (col0 = 0 AND col1 = 1) OR (col0 = 4 AND col1 = 3)
			 *
			 * Her we're creating the conjunction clauses, and we pass them all in
			 * as a list of arguments to an OR expression at the end.
			 */
			cnfExprs = lappend(cnfExprs, transformExpr(ps, (Node *) expr, AEXPR_OP));
		}

		if (list_length(cnfExprs) == 1)
			arg = (Node *) linitial(cnfExprs);
		else
			arg = transformExpr(ps, (Node *) makeBoolExpr(AND_EXPR, cnfExprs, -1), AEXPR_OP);

		args = lappend(args, arg);
	}

	/* this is one big disjunction of conjunctions that match GROUP BY criteria */
	dnf = makeBoolExpr(OR_EXPR, args, -1);
	assign_expr_collations(ps, (Node *) dnf);

	where = transformExpr(ps, (Node *) dnf, EXPR_KIND_WHERE);

	return (Node *) where;
}

/*
 * get_tuples_to_combine_with
 *
 * Gets the plan for retrieving all of the existing tuples that are going
 * to be combined with the incoming tuples
 */
static void
get_tuples_to_combine_with(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber *merge_attrs,
		int num_merge_attrs, TupleHashTable merge_targets)
{
	Node *raw_parse_tree;
	List *parsetree_list;
	List *query_list;
	PlannedStmt *plan;
	Query *query;
	SelectStmt *stmt;
	Portal portal;
	ParseState *ps;
	DestReceiver *dest;
	char base_select[14 + strlen(cvname) + 1];
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = MakeSingleTupleTableSlot(desc);
	List *tups = NIL;
	ListCell *lc;

	sprintf(base_select, "SELECT * FROM %s", cvname);

	ps = make_parsestate(NULL);

	dest = CreateDestReceiver(DestTupleTable);

	parsetree_list = pg_parse_query(base_select);
	Assert(parsetree_list->length == 1);

	/*
	 * We need to do this to populate the ParseState's p_varnamespace member
	 */
	stmt = (SelectStmt *) linitial(parsetree_list);
	transformFromClause(ps, stmt->fromClause);

	raw_parse_tree = (Node *) linitial(parsetree_list);
	query_list = pg_analyze_and_rewrite(raw_parse_tree, base_select, NULL, 0);

	Assert(query_list->length == 1);
	query = (Query *) linitial(query_list);

	if (num_merge_attrs > 0)
	{
		Node *where = get_retrieval_where_clause(incoming_merges, desc, merge_attrs,
				num_merge_attrs, ps);
		query->jointree = makeFromExpr(query->jointree->fromlist, where);
	}

	plan = pg_plan_query(query, 0, NULL);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("__merge_retrieval__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	SetTupleTableDestReceiverParams(dest, merge_targets, CurrentMemoryContext, true);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	tuplestore_rescan(incoming_merges);
	foreach_tuple(slot, incoming_merges)
	{
		HeapTuple tup = ExecCopySlotTuple(slot);
		tups = lappend(tups, tup);
	}
	tuplestore_clear(incoming_merges);

	/*
	 * Now add the merge targets that already exist in the continuous view's table
	 * to the input of the final merge query
	 */
	hash_seq_init(&status, merge_targets->hashtab);
	while ((entry = (HeapTupleEntry) hash_seq_search(&status)) != NULL)
		tuplestore_puttuple(incoming_merges, entry->tuple);

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(incoming_merges, tup);
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
sync_combine(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, TupleHashTable merge_targets)
{
	Relation rel = heap_openrv(makeRangeVar(NULL, cvname, -1), RowExclusiveLock);
	int size = sizeof(bool) * slot->tts_tupleDescriptor->natts;
	bool *replace_all = palloc0(size);
	ResultRelInfo *ri = CQMatViewOpen(rel);

	MemSet(replace_all, true, size);

	foreach_tuple(slot, results)
	{
		HeapTupleEntry update = NULL;
		slot_getallattrs(slot);

		if (merge_targets)
			update = (HeapTupleEntry) LookupTupleHashEntry(merge_targets, slot, NULL);

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
combine(PlannedStmt *plan, TupleDesc cvdesc,
		Tuplestorestate *store, MemoryContext tmpctx)
{
	TupleTableSlot *slot;
	Portal portal;
	DestReceiver *dest = CreateDestReceiver(DestTuplestore);
	Tuplestorestate *merge_output = NULL;
	AttrNumber *merge_attrs = NULL;
	Oid *merge_attr_ops;
	TupleHashTable merge_targets = NULL;
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	int num_merge_attrs = 0;
	char *matrelname = NameStr(plan->cq_state->matrelname);
	Agg *agg = NULL;

	PushActiveSnapshot(GetTransactionSnapshot());

	slot = MakeSingleTupleTableSlot(cvdesc);

	if (IsA(plan->planTree, Agg))
	{
		agg = (Agg *) plan->planTree;
		merge_attrs = agg->grpColIdx;
		num_merge_attrs = agg->numCols;
		merge_attr_ops = agg->grpOperators;
	}

	if (agg != NULL)
	{
		execTuplesHashPrepare(num_merge_attrs, merge_attr_ops, &eq_funcs, &hash_funcs);
		merge_targets = BuildTupleHashTable(num_merge_attrs, merge_attrs, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CurrentMemoryContext, tmpctx);
		get_tuples_to_combine_with(matrelname, cvdesc, store, merge_attrs, num_merge_attrs, merge_targets);
	}

	portal = CreatePortal("__merge__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  NULL,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	merge_output = tuplestore_begin_heap(true, true, combiner_work_mem);
	SetTuplestoreDestReceiverParams(dest, merge_output, CurrentMemoryContext, true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE, GetActiveSnapshot());

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	PopActiveSnapshot();

	tuplestore_clear(store);

	sync_combine(matrelname, merge_output, slot, merge_targets);

	if (merge_targets)
		hash_destroy(merge_targets->hashtab);

	tuplestore_end(merge_output);
	PortalDrop(portal, false);
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
	Tuplestorestate *store;
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
	store = tuplestore_begin_heap(true, true, combiner_work_mem);
	combineplan = prepare_combine_plan(queryDesc->plannedstmt, store, &workerdesc);
	slot = MakeSingleTupleTableSlot(workerdesc);

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
				tuplestore_puttupleslot(store, slot);
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

				combine(combineplan, workerdesc, store, tmpctx);

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
			tuplestore_clear(store);
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
