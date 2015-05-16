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
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
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
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "pipeline/combiner.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cqplan.h"
#include "pipeline/cqproc.h"
#include "pipeline/groupcache.h"
#include "pipeline/tuplebuf.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

/* duration in seconds after which to replan the cached groups retrieval plan */
#define GROUPS_PLAN_LIFESPAN 10
#define WAIT_SLEEP_MS 5

static TupleBufferReader *reader = NULL;

int combiner_work_mem = 16384;
int combiner_cache_mem = 0;
int combiner_synchronous_commit = SYNCHRONOUS_COMMIT_OFF;

static GroupCache *cache = NULL;

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
	/* hash function expression used by the matrel, or NULL if no grouping is used */
	FuncExpr *hashfunc;
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
get_values(CombineState *cstate, Tuplestorestate *batch, TupleHashTable existing)
{
	TupleTableSlot *slot = cstate->resultslot;
	List *values = NIL;
	int i;

	/*
	 * Generate a VALUES list of the incoming groups
	 *
	 * We wrap each incoming group in a call to hash_group because that's
	 * what we index matrels on. This gives the index entries a high cardinality
	 * without having to know anything about the actual selectivity of the groups,
	 * which keeps the matrel index fast.
	 */
	foreach_tuple(slot, batch)
	{
		List *tup = NIL;
		FuncExpr *hash;
		List *args = NIL;
		Oid hashoid = HASH_GROUP_OID;

		/*
		 * If we already have the physical tuple cached from a previous combine
		 * call, then we don't need to look for it on disk. Instead, just add it
		 * to the output that the on-disk tuples will eventually go to.
		 */
		if (cache)
		{
			bool isnew;
			HeapTuple cached = GroupCacheGet(cache, slot);
			if (cached)
			{
				HeapTupleEntry ex = (HeapTupleEntry) LookupTupleHashEntry(existing, slot, &isnew);

				ex->tuple = cached;
				continue;
			}
		}

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

			if (TypeCategory(attr->atttypid) == TYPCATEGORY_DATETIME)
				hashoid = LS_HASH_GROUP_OID;

			d = slot_getattr(slot, groupattr, &isnull);
			c = makeConst(attr->atttypid, attr->atttypmod,
					attr->attcollation, length, d, isnull, attr->attbyval);

			args = lappend(args, c);
			tup = lappend(tup, c);
		}

		hash = makeFuncExpr(hashoid, get_func_rettype(hashoid), args, 0, 0, COERCE_EXPLICIT_CALL);
		values = lappend(values, list_make1(hash));
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
	int i;
	FuncExpr *func;
	List *args = NIL;

	sel->valuesLists = values;

	/*
	 * Now create a subquery to join the matrel against, which
	 * will result in a retrieval of existing groups to update.
	 */
	if (cstate->hashfunc)
	{
		func = copyObject(cstate->hashfunc);
	}
	else
	{
		/*
		 * In the event that something strange happened, such as a user
		 * deleting the hash index, we can still try our best to rebuild
		 * it here.
		 */
		for (i=0; i<cstate->ngroupatts; i++)
		{
			AttrNumber groupattr = cstate->groupatts[i];
			Form_pg_attribute attr = cstate->cvdesc->attrs[groupattr - 1];

			Var *v = makeVar(1, groupattr, attr->atttypid,
					attr->atttypmod, attr->attcollation, 0);

			args = lappend(args, v);
		}

		func = makeFuncExpr(HASH_GROUP_OID, INT4OID, args, 0, 0, COERCE_EXPLICIT_CALL);
	}

	sub->subLinkType = ANY_SUBLINK;
	sub->testexpr = (Node *) func;
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
		plan = copyObject(cstate->groupsplan);

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
	query->is_combine_lookup = true;

	if (cstate->ngroupatts > 0 && values)
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
 * hash_groups
 *
 * Stores all of the tuples in the given tuplestore into a hashtable, keyed
 * by their grouping columns
 */
static TupleHashTable
hash_groups(CombineState *cstate, Tuplestorestate *batch, TupleHashTable existing)
{
	TupleHashTable groups = NULL;
	bool isnew = false;
	TupleTableSlot *slot = cstate->resultslot;

	groups = BuildTupleHashTable(existing->numCols, existing->keyColIdx,
			existing->tab_eq_funcs, existing->tab_hash_funcs, 1000,
			existing->entrysize, existing->tablecxt, existing->tempcxt);

	foreach_tuple(slot, batch)
	{
		LookupTupleHashEntry(groups, slot, &isnew);
	}
	tuplestore_rescan(batch);

	return groups;
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
	Portal portal = NULL;
	DestReceiver *dest;
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = cstate->resultslot;
	List *tups = NIL;
	ListCell *lc;
	List *values = NIL;
	TupleHashTable batchgroups = hash_groups(cstate, batch, existing);

	/*
	 * If we're not grouping on any columns, then there's only one row to look up
	 * so we don't need to do a VALUES-matrel join.
	 */
	if (cstate->isagg && cstate->ngroupatts > 0)
	{
		values = get_values(cstate, batch, existing);
		/*
		 * If we're grouping and there aren't any uncached values to look up,
		 * there is no need to execute a query.
		 */
		if (!values)
			goto finish;
	}

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

	PortalDrop(portal, false);

finish:

	tuplestore_rescan(batch);
	foreach_tuple(slot, batch)
	{
		HeapTuple tup = ExecCopySlotTuple(slot);
		tups = lappend(tups, tup);
	}
	tuplestore_clear(batch);

	/*
	 * Now add the existing rows to the input of the final combine query
	 */
	hash_seq_init(&status, existing->hashtab);
	while ((entry = (HeapTupleEntry) hash_seq_search(&status)) != NULL)
	{
		/*
		 * Our index can potentially have collisions, so we filter out any
		 * tuples that were returned that aren't related to the batch we're
		 * currently processing. This is just a matter of intersecting the
		 * retrieved groups with the batch's groups.
		 */
		ExecStoreTuple(entry->tuple, slot, InvalidBuffer, false);
		if (LookupTupleHashEntry(batchgroups, slot, NULL))
			tuplestore_puttuple(batch, entry->tuple);
	}

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(batch, tup);
	}

	hash_destroy(batchgroups->hashtab);
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
	EState *estate = CreateExecutorState();

	MemSet(replace_all, true, size);

	foreach_tuple(slot, results)
	{
		HeapTupleEntry update = NULL;
		HeapTuple tup = NULL;

		slot_getallattrs(slot);

		if (existing)
			update = (HeapTupleEntry) LookupTupleHashEntry(existing, slot, NULL);

		if (update)
		{
			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			tup = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);

			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot, estate);
			IncrementCQUpdate(1, HEAPTUPLESIZE + tup->t_len);
		}
		else
		{

			/* No existing tuple found, so it's an INSERT */
			ExecCQMatRelInsert(ri, slot, estate);
			IncrementCQWrite(1, HEAPTUPLESIZE + slot->tts_tuple->t_len);
		}

		if (cache)
			GroupCachePut(cache, slot);

		ResetPerTupleExprContext(estate);
	}

	FreeExecutorState(estate);
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
		TupleDesc desc, TupleTableSlot *slot, MemoryContext context, MemoryContext tmpcontext)
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
		Relation matrel;
		ResultRelInfo *ri;
		int i;

		cstate->groupatts = agg->grpColIdx;
		cstate->ngroupatts = agg->numCols;
		cstate->groupops = agg->grpOperators;
		cstate->isagg = true;

		/*
		 * In order for the hashed group index to be usable, we must use an expression
		 * that is equivalent to the index expression in the group lookup. The best way
		 * to do this is to just copy the actual index expression. If something strange
		 * happens, such as a user deleting the hash index, we still try our best to
		 * reconstruct this expression later.
		 */
		matrel = heap_openrv(cstate->matrel, NoLock);
		ri = CQMatViewOpen(matrel);

		for (i=0; i<ri->ri_NumIndices; i++)
		{
			IndexInfo *idx = ri->ri_IndexRelationInfo[i];
			Node *n;
			FuncExpr *func;

			if (!idx->ii_Expressions || list_length(idx->ii_Expressions) != 1)
				continue;

			n = linitial(idx->ii_Expressions);
			if (!IsA(n, FuncExpr))
				continue;

			func = (FuncExpr *) n;
			if ((func->funcid != HASH_GROUP_OID && func->funcid != LS_HASH_GROUP_OID) ||
					list_length(func->args) != cstate->ngroupatts)
				continue;

			cstate->hashfunc = copyObject(func);
			break;
		}

		heap_close(matrel, NoLock);
		CQMatViewClose(ri);
		cache = GroupCacheCreate(combiner_cache_mem * 1024, cstate->ngroupatts, cstate->groupatts,
				cstate->groupops, slot, CurrentMemoryContext, cstate->tmpcontext);
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
	init_combine_state(&cstate, cvname, combineplan, workerdesc, slot, runctx, tmpctx);

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
				if (TimestampDifferenceExceeds(last_receive, GetCurrentTimestamp(), state->emptysleepms))
				{
					/* force stats flush */
					cq_stat_report(true);

					pgstat_report_activity(STATE_IDLE, queryDesc->sourceText);
					TupleBufferWait(CombinerTupleBuffer, MyCQId, 0);
					pgstat_report_activity(STATE_RUNNING, queryDesc->sourceText);
				}
				else
					pg_usleep(Min(WAIT_SLEEP_MS, state->emptysleepms) * 1000);
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
