/*-------------------------------------------------------------------------
 *
 * cont_combiner.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 * 
 * IDENTIFICATION
 *    src/include/pipeline/cont_combiner.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "executor/tupletableReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/paths.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cont_plan.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/groupcache.h"
#include "pipeline/tuplebuf.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define GROUPS_PLAN_LIFESPAN (10 * 1000)

static bool
should_read_fn(TupleBufferReader *reader, TupleBufferSlot *slot)
{
	/* combiner tuples are guaranteed to target only a single CQ */
	return bms_singleton_member(slot->queries) % continuous_query_num_combiners == reader->proc->group_id;
}

typedef struct {
	Oid view_id;
	ContinuousView *view;
	PlannedStmt *combine_plan;
	PlannedStmt *groups_plan;
	TimestampTz last_groups_plan;
	TupleDesc desc;
	MemoryContext state_cxt;
	MemoryContext plan_cache_cxt;
	MemoryContext tmp_cxt;
	Tuplestorestate *batch;
	TupleTableSlot *slot;
	bool isagg;
	int ngroupatts;
	AttrNumber *groupatts;
	Oid *groupops;
	FuncExpr *hashfunc;
	GroupCache *cache;
	Relation matrel;
	ResultRelInfo *ri;
	CQStatEntry stats;
} ContQueryCombinerState;

/*
 * prepare_combine_plan
 */
static void
prepare_combine_plan(ContQueryCombinerState *state, PlannedStmt *plan)
{
	TuplestoreScan *scan;

	/*
	 * Mark combine_plan as not continuous now because we'll be repeatedly
	 * executing it in a new portal. We also need to set its batch
	 * size to 0 so that TuplestoreScans don't return early. Since
	 * they're not being executed continuously, they'd never
	 * see anything after the first batch was consumed.
	 *
	 */
	plan->is_continuous = false;

	scan = SetCombinerPlanTuplestorestate(plan, state->batch);

	state->desc = ExecTypeFromTL(((Plan *) scan)->targetlist, false);
	state->combine_plan = plan;
}

/*
 * get_values
 *
 * Given an incoming batch, returns a VALUES clause containing each tuple's
 * group columns that can be joined against with the matrel's existing groups.
 */
static List *
get_values(ContQueryCombinerState *state, TupleHashTable existing)
{
	TupleTableSlot *slot = state->slot;
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
	foreach_tuple(slot, state->batch)
	{
		FuncExpr *hash;
		List *args = NIL;
		Oid hashoid = HASH_GROUP_OID;

		/*
		 * If we already have the physical tuple cached from a previous combine
		 * call, then we don't need to look for it on disk. Instead, just add it
		 * to the output that the on-disk tuples will eventually go to.
		 */
		if (state->cache)
		{
			bool isnew;
			HeapTuple cached = GroupCacheGet(state->cache, slot);
			if (cached)
			{
				HeapTupleEntry ex = (HeapTupleEntry) LookupTupleHashEntry(existing, slot, &isnew);

				ex->tuple = cached;
				continue;
			}
		}

		for (i = 0; i < state->ngroupatts; i++)
		{
			AttrNumber groupattr = state->groupatts[i];
			Form_pg_attribute attr = state->desc->attrs[groupattr - 1];
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
get_groups(ContQueryCombinerState *state, List *values, ParseState *ps)
{
	Node *where;
	SubLink *sub = makeNode(SubLink);
	SelectStmt *sel = makeNode(SelectStmt);

	sel->valuesLists = values;

	/*
	 * Now create a subquery to join the matrel against, which
	 * will result in a retrieval of existing groups to update.
	 */
	sub->subLinkType = ANY_SUBLINK;
	sub->testexpr = (Node *) copyObject(state->hashfunc);
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
get_cached_groups_plan(ContQueryCombinerState *state, List *values)
{
	MemoryContext old_cxt;
	ParseState *ps;
	Query *query;
	List *qlist;
	SelectStmt *sel;
	ResTarget *res;
	A_Star *star;
	ColumnRef *cref;
	PlannedStmt *plan;

	if (state->groups_plan != NULL &&
			!TimestampDifferenceExceeds(state->last_groups_plan, GetCurrentTimestamp(), GROUPS_PLAN_LIFESPAN))
	{
		if (values)
			set_values(state->groups_plan, values);

		/* use a fresh copy of the plan, as it may be modified by the executor */
		plan = copyObject(state->groups_plan);
		return plan;
	}

	/* cache miss, plan the query */
	MemoryContextReset(state->plan_cache_cxt);

	sel = makeNode(SelectStmt);
	res = makeNode(ResTarget);
	star = makeNode(A_Star);
	cref = makeNode(ColumnRef);

	cref->fields = list_make1(star);
	res->val = (Node *) cref;
	sel->targetList = list_make1(res);
	sel->fromClause = list_make1(state->view->matrel);
	sel->forCombineLookup = true;

	/* populate the ParseState's p_varnamespace member */
	ps = make_parsestate(NULL);
	ps->p_no_locking = true;
	transformFromClause(ps, sel->fromClause);

	qlist = pg_analyze_and_rewrite((Node *) sel, state->view->matrel->relname, NULL, 0);
	query = (Query *) linitial(qlist);

	if (state->ngroupatts > 0 && list_length(values))
	{
		Node *groups = get_groups(state, values, ps);
		query->jointree = makeFromExpr(query->jointree->fromlist, groups);
		query->hasSubLinks = true;
	}

	PushActiveSnapshot(GetTransactionSnapshot());
	plan = pg_plan_query(query, 0, NULL);
	PopActiveSnapshot();

	old_cxt = MemoryContextSwitchTo(state->plan_cache_cxt);
	state->groups_plan = copyObject(plan);
	state->last_groups_plan = GetCurrentTimestamp();
	MemoryContextSwitchTo(old_cxt);

	return plan;
}

/*
 * hash_groups
 *
 * Stores all of the tuples in the given tuplestore into a hashtable, keyed
 * by their grouping columns
 */
static TupleHashTable
hash_groups(ContQueryCombinerState *state, TupleHashTable existing)
{
	TupleHashTable groups = NULL;
	bool isnew = false;
	TupleTableSlot *slot = state->slot;

	groups = BuildTupleHashTable(existing->numCols, existing->keyColIdx,
			existing->tab_eq_funcs, existing->tab_hash_funcs, 1000,
			existing->entrysize, existing->tablecxt, existing->tempcxt);

	foreach_tuple(slot, state->batch)
		LookupTupleHashEntry(groups, slot, &isnew);

	tuplestore_rescan(state->batch);

	return groups;
}

/*
 * select_existing_groups
 *
 * Adds all existing groups in the matrel to the combine input set
 */
static void
select_existing_groups(ContQueryCombinerState *state, TupleHashTable existing)
{
	PlannedStmt *plan = NULL;
	Portal portal = NULL;
	DestReceiver *dest;
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = state->slot;
	List *tups = NIL;
	ListCell *lc;
	List *values = NIL;
	TupleHashTable batchgroups = hash_groups(state, existing);

	/*
	 * If we're not grouping on any columns, then there's only one row to look up
	 * so we don't need to do a VALUES-matrel join.
	 */
	if (state->isagg && state->ngroupatts > 0)
	{
		values = get_values(state, existing);
		/*
		 * If we're grouping and there aren't any uncached values to look up,
		 * there is no need to execute a query.
		 */
		if (!values)
			goto finish;
	}

	plan = get_cached_groups_plan(state, values);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			state->view->matrel->relname,
			"SELECT",
			list_make1(plan),
			NULL);

	dest = CreateDestReceiver(DestTupleTable);
	SetTupleTableDestReceiverParams(dest, existing, state->tmp_cxt, true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE_LOOKUP, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	PortalDrop(portal, false);

finish:
	tuplestore_rescan(state->batch);
	foreach_tuple(slot, state->batch)
	{
		HeapTuple tup = ExecCopySlotTuple(slot);
		tups = lappend(tups, tup);
	}
	tuplestore_clear(state->batch);

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
			tuplestore_puttuple(state->batch, entry->tuple);
	}

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(state->batch, tup);
	}

	list_free(tups);

	hash_destroy(batchgroups->hashtab);
}


/*
 * sync_combine
 *
 * Writes the combine results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
static void
sync_combine(ContQueryCombinerState *state, Tuplestorestate *results, TupleHashTable existing)
{
	TupleTableSlot *slot = state->slot;
	int size = sizeof(bool) * slot->tts_tupleDescriptor->natts;
	bool *replace_all = palloc0(size);
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
			ExecCQMatRelUpdate(state->ri, slot, estate);
			IncrementCQUpdate(1, HEAPTUPLESIZE + tup->t_len);
		}
		else
		{

			/* No existing tuple found, so it's an INSERT */
			ExecCQMatRelInsert(state->ri, slot, estate);
			IncrementCQWrite(1, HEAPTUPLESIZE + slot->tts_tuple->t_len);
		}

		ResetPerTupleExprContext(estate);
	}

	/*
	 * We need to wait until we've completed all updates/inserts before caching everything, otherwise
	 * we may free a cached tuple before trying to read it
	 */
	if (state->cache)
	{
		foreach_tuple(slot, results)
		{
			GroupCachePut(state->cache, slot);
		}
	}

	FreeExecutorState(estate);
}

/*
 * combine
 *
 * Combines partial results of a continuous query with existing rows in the continuous view
 */
static void
combine(ContQueryCombinerState *state)
{
	Portal portal;
	DestReceiver *dest;
	Tuplestorestate *result;
	TupleHashTable existing = NULL;

	state->matrel = heap_openrv_extended(state->view->matrel, RowExclusiveLock, true);

	if (state->matrel == NULL)
		return;

	state->ri = CQMatViewOpen(state->matrel);

	if (state->isagg)
	{
		FmgrInfo *eq_funcs;
		FmgrInfo *hash_funcs;
		MemoryContext hash_tmp_cxt = AllocSetContextCreate(state->tmp_cxt, "CombinerQueryHashTableTempCxt",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

		execTuplesHashPrepare(state->ngroupatts, state->groupops, &eq_funcs, &hash_funcs);
		existing = BuildTupleHashTable(state->ngroupatts, state->groupatts, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CurrentMemoryContext, hash_tmp_cxt);

		select_existing_groups(state, existing);
	}

	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  state->view->matrel->relname,
					  "SELECT",
					  list_make1(state->combine_plan),
					  NULL);

	result = tuplestore_begin_heap(true, true, continuous_query_combiner_work_mem);
	dest = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(dest, result, state->tmp_cxt, true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	tuplestore_clear(state->batch);

	sync_combine(state, result, existing);

	tuplestore_end(result);

	if (existing)
		hash_destroy(existing->hashtab);

	PortalDrop(portal, false);

	CQMatViewClose(state->ri);
	heap_close(state->matrel, RowExclusiveLock);
}

static void
init_query_state(ContQueryCombinerState *state, Oid id, MemoryContext context)
{
	PlannedStmt *pstmt;
	MemoryContext state_cxt;
	MemoryContext old_cxt;
	MemoryContext cache_tmp_cxt;

	MemSet(state, 0, sizeof(ContQueryCombinerState));

	state_cxt = AllocSetContextCreate(context, "CombinerQueryStateCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old_cxt = MemoryContextSwitchTo(state_cxt);

	state->view_id = id;
	state->state_cxt = state_cxt;
	state->tmp_cxt = AllocSetContextCreate(state_cxt, "CombinerQueryTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	state->plan_cache_cxt = AllocSetContextCreate(state_cxt, "CombinerQueryPlanCacheCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	cache_tmp_cxt = AllocSetContextCreate(state_cxt, "CombinerQueryTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	state->view = GetContinuousView(id);

	if (state->view == NULL)
		return;

	pstmt = GetContPlan(state->view);

	state->batch = tuplestore_begin_heap(true, true, continuous_query_combiner_work_mem);
	/* this also sets the state's desc field */
	prepare_combine_plan(state, pstmt);
	state->slot = MakeSingleTupleTableSlot(state->desc);
	state->groups_plan = NULL;

	if (IsA(state->combine_plan->planTree, Agg))
	{
		Agg *agg = (Agg *) state->combine_plan->planTree;
		int i;
		Relation matrel;
		ResultRelInfo *ri;

		state->groupatts = agg->grpColIdx;
		state->ngroupatts = agg->numCols;
		state->groupops = agg->grpOperators;
		state->isagg = true;

		matrel = heap_openrv_extended(state->view->matrel, AccessShareLock, true);

		if (matrel == NULL)
		{
			state->view = NULL;
			return;
		}

		ri = CQMatViewOpen(matrel);

		if (state->ngroupatts)
		{
			/*
			 * In order for the hashed group index to be usable, we must use an expression
			 * that is equivalent to the index expression in the group lookup. The best way
			 * to do this is to just copy the actual index expression.
			 */
			for (i = 0; i < ri->ri_NumIndices; i++)
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
						list_length(func->args) != state->ngroupatts)
					continue;

				state->hashfunc = copyObject(func);
				break;
			}

			Assert(state->hashfunc);
		}

		CQMatViewClose(ri);
		heap_close(matrel, AccessShareLock);

		state->cache = GroupCacheCreate(continuous_query_combiner_cache_mem * 1024, state->ngroupatts, state->groupatts,
				state->groupops, state->slot, state_cxt, cache_tmp_cxt);
	}

	cq_stat_init(&state->stats, state->view->id, 0);

	MemoryContextSwitchTo(old_cxt);
}

static void
cleanup_query_state(ContQueryCombinerState **states, Oid id)
{
	ContQueryCombinerState *state = states[id];

	if (state == NULL)
		return;

	MemoryContextDelete(state->state_cxt);
	pfree(state);
	states[id] = NULL;
}

static ContQueryCombinerState **
init_query_states_array(MemoryContext context)
{
	MemoryContext old_cxt = MemoryContextSwitchTo(context);
	ContQueryCombinerState **states = palloc0(MAXALIGN(mul_size(sizeof(ContQueryCombinerState *), MAX_CQS)));
	MemoryContextSwitchTo(old_cxt);

	return states;
}

static ContQueryCombinerState *
get_query_state(ContQueryCombinerState **states, Oid id, MemoryContext context)
{
	ContQueryCombinerState *state = states[id];
	HeapTuple tuple;

	MyCQStats = NULL;

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	tuple = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tuple))
	{
		PopActiveSnapshot();
		cleanup_query_state(states, id);
		return NULL;
	}

	if (state != NULL)
	{
		/* Was the continuous view modified? In our case this means remove the old view and add a new one */
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		if (row->hash != state->view->hash)
		{
			cleanup_query_state(states, id);
			state = NULL;
		}
	}

	ReleaseSysCache(tuple);

	if (state == NULL)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(context);
		state = palloc0(sizeof(ContQueryCombinerState));
		init_query_state(state, id, context);
		states[id] = state;
		MemoryContextSwitchTo(old_cxt);

		if (state->view == NULL)
		{
			PopActiveSnapshot();
			cleanup_query_state(states, id);
			return NULL;
		}
	}

	PopActiveSnapshot();

	MyCQStats = &state->stats;

	return state;
}

static int
read_batch(ContQueryCombinerState *state, TupleBufferBatchReader *reader)
{
	TupleBufferSlot *tbs;
	int count = 0;

	while ((tbs = TupleBufferBatchReaderNext(reader)) != NULL)
	{
		if (!TupIsNull(state->slot))
			ExecClearTuple(state->slot);

		ExecStoreTuple(heap_copytuple(tbs->tuple->heaptup), state->slot, InvalidBuffer, false);
		tuplestore_puttupleslot(state->batch, state->slot);

		IncrementCQRead(1, tbs->size);
		count++;
	}

	if (!TupIsNull(state->slot))
		ExecClearTuple(state->slot);

	return count;
}

static bool
has_queries_to_process(Bitmapset *queries)
{
	Bitmapset *tmp = bms_copy(queries);
	int id;
	bool has_queries = false;

	while ((id = bms_first_member(tmp)) >= 0)
		if (id % continuous_query_num_combiners == MyContQueryProc->group_id)
		{
			has_queries = true;
			break;
		}

	bms_free(tmp);

	return has_queries;
}

void
ContinuousQueryCombinerMain(void)
{
	TupleBufferBatchReader *reader = TupleBufferOpenBatchReader(CombinerTupleBuffer, &should_read_fn);
	MemoryContext run_cxt = AllocSetContextCreate(TopMemoryContext, "CombinerRunCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	ContQueryCombinerState **states = init_query_states_array(run_cxt);
	ContQueryCombinerState *state = NULL;
	Bitmapset *queries;
	TimestampTz last_processed = GetCurrentTimestamp();
	bool has_queries;
	int id;

	/* Set the commit level */
	synchronous_commit = continuous_query_combiner_synchronous_commit;

	ContQueryBatchContext = AllocSetContextCreate(run_cxt, "ContQueryBatchContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* Bootstrap the query ids we should process. */
	StartTransactionCommand();
	MemoryContextSwitchTo(run_cxt);
	queries = GetAllContinuousViewIds();
	CommitTransactionCommand();

	has_queries = has_queries_to_process(queries);

	MemoryContextSwitchTo(run_cxt);

	for (;;)
	{
		uint32 num_processed = 0;
		Bitmapset *tmp;
		int id;
		bool updated_queries = false;

		sleep_if_deactivated();

		TupleBufferBatchReaderTrySleep(reader, last_processed);

		if (MyContQueryProc->group->terminate)
			break;

		/* If we had no queries, then rescan the catalog. */
		if (!has_queries)
		{
			Bitmapset *new, *removed;
			StartTransactionCommand();
			MemoryContextSwitchTo(run_cxt);
			new = GetAllContinuousViewIds();
			CommitTransactionCommand();

			removed = bms_difference(queries, new);
			bms_free(queries);
			queries = new;

			while ((id = bms_first_member(removed)) >= 0)
				cleanup_query_state(states, id);

			bms_free(removed);

			has_queries = has_queries_to_process(queries);
		}

		StartTransactionCommand();

		MemoryContextSwitchTo(ContQueryBatchContext);

		tmp = bms_copy(queries);
		while ((id = bms_first_member(tmp)) >= 0)
		{
			/* skip eagerly for continuous queries that we don't own */
			if (id % continuous_query_num_combiners != MyContQueryProc->group_id)
				continue;

			PG_TRY();
			{
				int count = 0;

				state = get_query_state(states, id, run_cxt);

				if (state == NULL)
				{
					queries = bms_del_member(queries, id);
					has_queries = has_queries_to_process(queries);
					goto next;
				}

				/* No need to process queries which we don't have tuples for. */
				if (!TupleBufferBatchReaderHasTuplesForCQId(reader, id))
					goto next;

				debug_query_string = NameStr(state->view->name);
				MemoryContextSwitchTo(state->tmp_cxt);

				TupleBufferBatchReaderSetCQId(reader, id);

				count = read_batch(state, reader);

				if (count)
				{
					num_processed += count;
					combine(state);
				}

				MemoryContextResetAndDeleteChildren(state->tmp_cxt);

				IncrementCQExecutions(1);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (ActiveSnapshotSet())
					PopActiveSnapshot();

				MemoryContextSwitchTo(ContQueryBatchContext);

				if (state)
					cleanup_query_state(states, id);

				IncrementCQErrors(1);

				if (!continuous_query_crash_recovery)
					exit(1);

				/*
				 * In case the error was thrown inside `cont_plan.c:get_combiner_plan`, we might not have
				 * reset the join_search_hook which would wrongly make the `get_cached_groups_plan` invoke the
				 * hook.
				 */
				join_search_hook = NULL;
			}
			PG_END_TRY();

next:
			TupleBufferBatchReaderRewind(reader);

			/* after reading a full batch, update query bitset with any new queries seen */
			if (reader->batch_done && !updated_queries)
			{
				Bitmapset *new;

				updated_queries = true;

				new = bms_difference(reader->queries_seen, queries);

				if (!bms_is_empty(new))
				{
					MemoryContextSwitchTo(ContQueryBatchContext);
					tmp = bms_add_members(tmp, new);

					MemoryContextSwitchTo(run_cxt);
					queries = bms_add_members(queries, new);

					has_queries = has_queries_to_process(queries);
				}

				bms_free(new);
			}

			if (state)
				cq_stat_report(false);
			else
				cq_stat_send_purge(id, 0, CQ_STAT_COMBINER);

			debug_query_string = NULL;
		}

		CommitTransactionCommand();

		if (num_processed)
			last_processed = GetCurrentTimestamp();

		TupleBufferBatchReaderReset(reader);
		MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
	}

	for (id = 0; id < MAX_CQS; id++)
	{
		ContQueryCombinerState *state = states[id];

		if (state == NULL)
			continue;

		MyCQStats = &state->stats;
		cq_stat_report(true);
	}

	TupleBufferCloseBatchReader(reader);
	pfree(states);
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(run_cxt);
}

/*
 * GetCombinerLookupPlan
 */
PlannedStmt *
GetCombinerLookupPlan(ContinuousView *view)
{
	ContQueryCombinerState state;
	PlannedStmt *plan;
	List *values = NIL;

	init_query_state(&state, view->id, CurrentMemoryContext);

	if (state.isagg && state.ngroupatts > 0)
	{
		TupleHashTable existing;
		FmgrInfo *eq_funcs;
		FmgrInfo *hash_funcs;
		Relation rel;

		execTuplesHashPrepare(state.ngroupatts, state.groupops, &eq_funcs, &hash_funcs);
		existing = BuildTupleHashTable(state.ngroupatts, state.groupatts, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CurrentMemoryContext, CurrentMemoryContext);

		rel = heap_openrv_extended(view->matrel, AccessShareLock, true);

		if (rel)
		{
			HeapTuple tuple;
			HeapScanDesc scan = heap_beginscan(rel, GetActiveSnapshot(), 0, NULL);

			while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
			{
				if (!TupIsNull(state.slot))
					ExecClearTuple(state.slot);

				ExecStoreTuple(heap_copytuple(tuple), state.slot, InvalidBuffer, false);
				tuplestore_puttupleslot(state.batch, state.slot);
				break;
			}

			heap_endscan(scan);
			heap_close(rel, AccessShareLock);
		}

		values = get_values(&state, existing);
	}

	plan = get_cached_groups_plan(&state, values);

	tuplestore_end(state.batch);

	return plan;
}
