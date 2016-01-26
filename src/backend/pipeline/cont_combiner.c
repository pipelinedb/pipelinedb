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
#include "commands/sequence.h"
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
#include "pipeline/sw_vacuum.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define GROUPS_PLAN_LIFESPAN (10 * 1000)

/*
 * Flag that indicates whether or not an on-disk tuple has already been added to
 * an ongoing combine result.
 */
#define EXISTING_ADDED 0x1

typedef struct
{
	ContQueryState base;
	PlannedStmt *combine_plan;
	PlannedStmt *groups_plan;
	TimestampTz last_groups_plan;
	TupleDesc desc;
	MemoryContext plan_cache_cxt;
	MemoryContext combine_cxt;
	Tuplestorestate *batch;
	TupleTableSlot *slot;
	TupleTableSlot *prev_slot;
	bool isagg;
	int ngroupatts;
	AttrNumber *groupatts;
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	Oid *groupops;
	FuncExpr *hashfunc;
	TupleHashTable existing;
	Tuplestorestate *combined;
	long pending_tuples;

	/* Stores the hashes of the current batch, in parallel to the order of the batch's tuplestore */
	int64 *group_hashes;
	int group_hashes_len;
	AttrNumber pk;
	bool seq_pk;
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
	Relation rel = heap_openrv(state->base.view->matrel, AccessShareLock);

	plan->isContinuous = false;

	scan = SetCombinerPlanTuplestorestate(plan, state->batch);
	scan->desc = CreateTupleDescCopy(RelationGetDescr(rel));

	state->combine_plan = plan;
	state->desc = scan->desc;

	heap_close(rel, AccessShareLock);
}

/*
 * get_values
 *
 * Given an incoming batch, returns a VALUES clause containing each tuple's
 * group columns that can be joined against with the matrel's existing groups.
 */
static List *
get_values(ContQueryCombinerState *state)
{
	TupleHashTable existing = state->existing;
	TupleTableSlot *slot = state->slot;
	List *values = NIL;
	int pos = 0;

	Assert(state->hashfunc->funcresulttype == INT8OID || state->hashfunc->funcresulttype == INT4OID);

	/*
	 * Generate a VALUES list based on the incoming groups
	 *
	 * We use the hash of the grouping columns, because that's what we index matrels on.
	 * This gives the index entries a high cardinality without having to know anything about
	 * the actual selectivity of the groups, which keeps the matrel index fast. The hash
	 * was already generated by the worker when determining which combiner process to
	 * send tuples to since we shard on groups.
	 */
	foreach_tuple(slot, state->batch)
	{
		Type typeinfo;
		Form_pg_type typ;
		Datum d;
		Const *hash;

		/*
		 * If the input set already has this group, we don't need to look it up
		 */
		if (LookupTupleHashEntry(existing, slot, NULL))
		{
			pos++;
			continue;
		}

		typeinfo = typeidType(state->hashfunc->funcresulttype);
		typ = (Form_pg_type) GETSTRUCT(typeinfo);

		/* these are parallel to this tuplestore's underlying array of tuples */
		d = DatumGetInt64(state->group_hashes[pos]);
		pos++;

		hash = makeConst(state->hashfunc->funcresulttype,
				typ->typtypmod, typ->typcollation, typ->typlen, d, false, typ->typbyval);

		ReleaseSysCache((HeapTuple) typeinfo);

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
	sel->fromClause = list_make1(state->base.view->matrel);
	sel->forCombineLookup = true;

	/* populate the ParseState's p_varnamespace member */
	ps = make_parsestate(NULL);
	ps->p_no_locking = true;
	transformFromClause(ps, sel->fromClause);

	qlist = pg_analyze_and_rewrite((Node *) sel, state->base.view->matrel->relname, NULL, 0);
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
hash_groups(ContQueryCombinerState *state)
{
	TupleHashTable existing = state->existing;
	TupleHashTable groups = NULL;
	bool isnew = false;
	TupleTableSlot *slot = state->slot;
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "CombinerGroupsHashTableCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext tmp_cxt = AllocSetContextCreate(cxt, "CombinerGroupsHashTableTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	groups = BuildTupleHashTable(existing->numCols, existing->keyColIdx,
			existing->tab_eq_funcs, existing->tab_hash_funcs, 1000,
			existing->entrysize, cxt, tmp_cxt);

	tuplestore_rescan(state->batch);

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
select_existing_groups(ContQueryCombinerState *state)
{
	TupleHashTable existing = state->existing;
	PlannedStmt *plan = NULL;
	Portal portal = NULL;
	DestReceiver *dest;
	HASH_SEQ_STATUS status;
	HeapTupleEntry entry;
	TupleTableSlot *slot = state->slot;
	List *tups = NIL;
	ListCell *lc;
	List *values = NIL;
	TupleHashTable batchgroups;
	Relation matrel;

	/*

	 */
	if (state->isagg && state->ngroupatts > 0)
	{
		Assert(state->existing);

		values = get_values(state);

		/*
		 * If we're grouping and there aren't any uncached values to look up,
		 * there is no need to execute a query.
		 */
		if (!values)
			goto finish;
	}
	else if (state->isagg && state->ngroupatts == 0)
	{
		/*
		 * If we're not grouping on any columns, then there's only one row to look up
		 * so we don't need to do a VALUES-matrel join. If it's already in existing, we're done
		 */
		if (hash_get_num_entries(state->existing->hashtab))
			return;
	}

	matrel = heap_openrv(state->base.view->matrel, AccessExclusiveLock);

	plan = get_cached_groups_plan(state, values);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			state->base.view->matrel->relname,
			"SELECT",
			list_make1(plan),
			NULL);

	dest = CreateDestReceiver(DestTupleTable);
	SetTupleTableDestReceiverParams(dest, existing, existing->tablecxt, true);

	PortalStart(portal, NULL, EXEC_NO_MATREL_LOCKING, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);
	PortalDrop(portal, false);

	heap_close(matrel, NoLock);

finish:
	batchgroups = hash_groups(state);
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
		 * We only need to add on-disk tuples to the input once, because they'll
		 * be accumulated upon within the ongoing combine result until we sync.
		 */
		if (entry->flags & EXISTING_ADDED)
			continue;

		/*
		 * Our index can potentially have collisions, so we filter out any
		 * tuples that were returned that aren't related to the batch we're
		 * currently processing. This is just a matter of intersecting the
		 * retrieved groups with the batch's groups.
		 */
		ExecStoreTuple(entry->tuple, slot, InvalidBuffer, false);
		if (LookupTupleHashEntry(batchgroups, slot, NULL))
		{
			tuplestore_puttuple(state->batch, entry->tuple);
			entry->flags |= EXISTING_ADDED;
		}
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
 * reset_existing_hashtable
 *
 * Free any memory associated with the given state's existing hashtable and
 * create a new hashtable.
 */
static TupleHashTable
build_existing_hashtable(ContQueryCombinerState *state)
{
	MemoryContext existing_cxt = AllocSetContextCreate(state->combine_cxt, "CombinerExistingGroupsCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext existing_tmp_cxt = AllocSetContextCreate(existing_cxt, "CombinerExistingGroupsTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext old = MemoryContextSwitchTo(state->combine_cxt);
	TupleHashTable result;

	result = BuildTupleHashTable(state->ngroupatts, state->groupatts, state->eq_funcs, state->hash_funcs, 1000,
			sizeof(HeapTupleEntryData), existing_cxt, existing_tmp_cxt);

	MemoryContextSwitchTo(old);

	return result;
}

/*
 * sync_combine
 *
 * Writes the combine results to a continuous view's table, performing
 * UPDATES or INSERTS as necessary
 */
static void
sync_combine(ContQueryCombinerState *state)
{
	TupleHashTable existing = state->existing;
	TupleTableSlot *slot = state->slot;
	Size size = sizeof(bool) * slot->tts_tupleDescriptor->natts;
	bool *replace_all = palloc0(size);
	EState *estate = CreateExecutorState();
	int i;
	int natts;
	Relation matrel;
	ResultRelInfo *ri;

	matrel = heap_openrv_extended(state->base.view->matrel, RowExclusiveLock, true);
	if (matrel == NULL)
		return;

	ri = CQMatRelOpen(matrel);
	natts = state->prev_slot->tts_tupleDescriptor->natts;

	foreach_tuple(slot, state->combined)
	{
		HeapTupleEntry update = NULL;
		HeapTuple tup = NULL;
		AttrNumber att = 1;
		int replaces = 0;

		/* Only replace values for non-group attributes */
		MemSet(replace_all, true, size);
		for (i = 0; i < state->ngroupatts; i++)
			replace_all[state->groupatts[i] - 1] = false;

		/* Never replace pkey */
		replace_all[state->pk - 1] = false;

		slot_getallattrs(slot);

		if (existing)
			update = (HeapTupleEntry) LookupTupleHashEntry(existing, slot, NULL);

		if (update)
		{
			ExecStoreTuple(update->tuple, state->prev_slot, InvalidBuffer, false);

			/*
			 * Figure out which columns actually changed, as those are the only values
			 * that we want to write to disk.
			 */
			for (att = 1; att <= natts; att++)
			{
				Datum prev;
				Datum new;
				bool prev_null;
				bool new_null;
				Form_pg_attribute attr = state->prev_slot->tts_tupleDescriptor->attrs[att - 1];

				if (att == state->pk)
					continue;

				prev = slot_getattr(state->prev_slot, att, &prev_null);
				new = slot_getattr(slot, att, &new_null);

				/*
				 * Note that this equality check only does a byte-by-byte comparison, which may yield false
				 * negatives. This is fine, because a false negative will just cause an extraneous write, which
				 * seems like a reasonable balance compared to looking up and invoking actual equality operators.
				 */
				if ((!prev_null && !new_null) && datumIsEqual(prev, new, attr->attbyval, attr->attlen))
					replace_all[att - 1] = false;
				else
					replaces++;
			}

			if (replaces == 0)
				continue;

			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			tup = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			if (ExecCQMatRelUpdate(ri, slot, estate))
				IncrementCQUpdate(1, HEAPTUPLESIZE + tup->t_len);
		}
		else
		{
			/* No existing tuple found, so it's an INSERT. Also generate a primary key for it if necessary. */
			if (state->seq_pk)
				slot->tts_values[state->pk - 1] = nextval_internal(state->base.view->seqrel);
			slot->tts_isnull[state->pk - 1] = false;
			tup = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelInsert(ri, slot, estate);
			IncrementCQWrite(1, HEAPTUPLESIZE + slot->tts_tuple->t_len);
		}

		ResetPerTupleExprContext(estate);
	}

	tuplestore_clear(state->combined);

	CQMatRelClose(ri);
	heap_close(matrel, NoLock);
	FreeExecutorState(estate);
}

/*
 * sync_all
 */
static void
sync_all(ContExecutor *cont_exec)
{
	Bitmapset *tmp = bms_copy(cont_exec->queries);
	ContQueryCombinerState **states = (ContQueryCombinerState **) cont_exec->states;
	int id;

	while ((id = bms_first_member(tmp)) >= 0)
	{
		ContQueryCombinerState *state = states[id];
		if (!state)
			continue;

		MyCQStats = &state->base.stats;

		PG_TRY();
		{
			if (state->pending_tuples > 0)
				sync_combine(state);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			AbortCurrentTransaction();
			StartTransactionCommand();
		}
		PG_END_TRY();

		state->pending_tuples = 0;
		state->existing = NULL;
		MemSet(state->group_hashes, 0, state->group_hashes_len);
		MemoryContextResetAndDeleteChildren(state->combine_cxt);
	}
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

	if (state->isagg)
	{
		if (state->existing == NULL)
			state->existing = build_existing_hashtable(state);
		select_existing_groups(state);
	}

	foreach_tuple(state->slot, state->combined)
	{
		tuplestore_puttupleslot(state->batch, state->slot);
	}
	tuplestore_clear(state->combined);

	portal = CreatePortal("", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  state->base.view->matrel->relname,
					  "SELECT",
					  list_make1(state->combine_plan),
					  NULL);

	dest = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(dest, state->combined, state->combine_cxt, true);

	PortalStart(portal, NULL, EXEC_FLAG_COMBINE, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 dest,
					 dest,
					 NULL);

	PortalDrop(portal, false);
	tuplestore_clear(state->batch);
}

static ContQueryState *
init_query_state(ContExecutor *cont_exec, ContQueryState *base)
{
	PlannedStmt *pstmt;
	ContQueryCombinerState *state;
	Relation matrel;
	List *indices = NIL;
	ListCell *lc;

	state = (ContQueryCombinerState *) palloc0(sizeof(ContQueryCombinerState));
	memcpy(&state->base, base, sizeof(ContQueryState));
	pfree(base);
	base = (ContQueryState *) state;

	state->plan_cache_cxt = AllocSetContextCreate(base->state_cxt, "CombinerQueryPlanCacheCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	state->combine_cxt = AllocSetContextCreate(base->state_cxt, "CombinerQueryCombineCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	pstmt = GetContPlan(base->view, Combiner);

	state->batch = tuplestore_begin_heap(true, true, continuous_query_combiner_work_mem);
	state->combined = tuplestore_begin_heap(false, false, continuous_query_combiner_work_mem);

	/* this also sets the state's desc field */
	prepare_combine_plan(state, pstmt);
	state->slot = MakeSingleTupleTableSlot(state->desc);
	state->prev_slot = MakeSingleTupleTableSlot(state->desc);
	state->groups_plan = NULL;

	/* this will grow dynamically when needed, but this is a good starting size */
	state->group_hashes_len = continuous_query_batch_size;
	state->group_hashes = palloc0(state->group_hashes_len * sizeof(int64));

	matrel = heap_openrv_extended(base->view->matrel, AccessShareLock, true);
	if (matrel == NULL)
	{
		base->view = NULL;
		return base;
	}

	if (IsA(state->combine_plan->planTree, Agg))
	{
		Agg *agg = (Agg *) state->combine_plan->planTree;
		ResultRelInfo *ri;

		state->groupatts = agg->grpColIdx;
		state->ngroupatts = agg->numCols;
		state->groupops = agg->grpOperators;
		state->isagg = true;

		ri = CQMatRelOpen(matrel);

		if (state->ngroupatts)
			state->hashfunc = GetGroupHashIndexExpr(state->ngroupatts, ri);

		CQMatRelClose(ri);

		execTuplesHashPrepare(state->ngroupatts, state->groupops, &state->eq_funcs, &state->hash_funcs);
		state->existing = build_existing_hashtable(state);
	}

	/*
	 * Find the primary key column
	 */
	indices = RelationGetIndexList(matrel);
	foreach(lc, indices)
	{
		Oid indexoid = lfirst_oid(lc);
		HeapTuple tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		Form_pg_index form;

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "index %u for matrel %u not found", indexoid, RelationGetRelid(matrel));

		form = (Form_pg_index) GETSTRUCT(tup);
		if (form->indisprimary)
		{
			TupleDesc desc = RelationGetDescr(matrel);
			Assert(form->indnatts == 1);
			state->pk = form->indkey.values[0];
			if (pg_strcasecmp(NameStr(desc->attrs[state->pk - 1]->attname), "$pk") == 0)
				state->seq_pk = true;
		}
		ReleaseSysCache(tup);
	}
	heap_close(matrel, AccessShareLock);

	return base;
}

/*
 * set_group_hash
 *
 * Set the group hash at the given index, increasing the size of the group hashes array if necessary
 */
static void
set_group_hash(ContQueryCombinerState *state, int index, int64 hash)
{
	int start = state->group_hashes_len;

	while (index >= state->group_hashes_len)
		state->group_hashes_len *= 2;

	if (start != state->group_hashes_len)
	{
		MemoryContext old = MemoryContextSwitchTo(state->base.state_cxt);
		state->group_hashes = repalloc(state->group_hashes, state->group_hashes_len);
		MemoryContextSwitchTo(old);
	}

	Assert(index < state->group_hashes_len);
	state->group_hashes[index] = hash;
}

static int
read_batch(ContQueryCombinerState *state, ContExecutor *cont_exec)
{
	PartialTupleState *pts;
	int len;
	int count = 0;

	while ((pts = (PartialTupleState *) ContExecutorYieldItem(cont_exec, &len)) != NULL)
	{
		if (!TupIsNull(state->slot))
			ExecClearTuple(state->slot);

		ExecStoreTuple(heap_copytuple(pts->tup), state->slot, InvalidBuffer, false);
		tuplestore_puttupleslot(state->batch, state->slot);

		set_group_hash(state, count, pts->hash);

		IncrementCQRead(1, len);
		count++;

	}

	if (!TupIsNull(state->slot))
		ExecClearTuple(state->slot);

	return count;
}

/*
 * need_sync
 */
static bool
need_sync(ContExecutor *cont_exec, TimestampTz last_sync)
{
	Bitmapset *tmp = bms_copy(cont_exec->queries);
	ContQueryCombinerState **states = (ContQueryCombinerState **) cont_exec->states;
	int id;

	while ((id = bms_first_member(tmp)) >= 0)
	{
		ContQueryCombinerState *state = states[id];
		if (state == NULL)
			continue;
	}

	if (synchronous_stream_insert || continuous_query_commit_interval == 0)
		return true;

	return TimestampDifferenceExceeds(last_sync, GetCurrentTimestamp(), continuous_query_commit_interval);
}

void
ContinuousQueryCombinerMain(void)
{
	ContExecutor *cont_exec = ContExecutorNew(Combiner, &init_query_state);
	Oid query_id;
	TimestampTz first_seen = GetCurrentTimestamp();
	bool do_commit = false;
	long total_pending = 0;

	/* Set the commit level */
	synchronous_commit = continuous_query_combiner_synchronous_commit;

	for (;;)
	{
		ContExecutorStartBatch(cont_exec);

		if (ShouldTerminateContQueryProcess())
			break;

		while ((query_id = ContExecutorStartNextQuery(cont_exec)) != InvalidOid)
		{
			int count = 0;
			ContQueryCombinerState *state = (ContQueryCombinerState *) cont_exec->current_query;

			PG_TRY();
			{
				if (state == NULL)
					goto next;

				CHECK_FOR_INTERRUPTS();

				MemoryContextSwitchTo(state->base.tmp_cxt);

				count = read_batch(state, cont_exec);

				if (count)
				{
					state->pending_tuples += count;
					total_pending += count;

					combine(state);

					if (first_seen == 0)
						first_seen = GetCurrentTimestamp();
				}

				MemoryContextResetAndDeleteChildren(state->base.tmp_cxt);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (ActiveSnapshotSet())
					PopActiveSnapshot();

				ContExecutorPurgeQuery(cont_exec);

				if (!continuous_query_crash_recovery)
					exit(1);

				MemoryContextSwitchTo(cont_exec->exec_cxt);
			}
			PG_END_TRY();

next:
			ContExecutorEndQuery(cont_exec);
		}

		if (total_pending == 0)
			do_commit = true;
		else if (need_sync(cont_exec, first_seen))
		{
			sync_all(cont_exec);
			do_commit = true;
			first_seen = 0;
			total_pending = 0;
		}
		else
			do_commit = false;

		ContExecutorEndBatch(cont_exec, do_commit);
	}

	for (query_id = 0; query_id < MAX_CQS; query_id++)
	{
		ContQueryCombinerState *state = (ContQueryCombinerState *) cont_exec->states[query_id];

		if (state == NULL)
			continue;

		MyCQStats = &state->base.stats;
		cq_stat_report(true);
	}

	MemoryContextSwitchTo(TopMemoryContext);
	ContExecutorDestroy(cont_exec);
}

/*
 * GetCombinerLookupPlan
 */
PlannedStmt *
GetCombinerLookupPlan(ContinuousView *view)
{
	ContQueryCombinerState *state;
	ContQueryState *base;
	PlannedStmt *plan;
	List *values = NIL;
	ContExecutor exec;

	exec.cxt = CurrentMemoryContext;
	exec.current_query_id = view->id;

	base = palloc0(sizeof(ContQueryState));

	base->view_id = view->id;
	base->view = view;
	base->state_cxt = CurrentMemoryContext;
	base->tmp_cxt = CurrentMemoryContext;

	state = (ContQueryCombinerState *) init_query_state(&exec, base);
	state->group_hashes = palloc0(continuous_query_batch_size * sizeof(uint64));

	if (state->isagg && state->ngroupatts > 0)
	{
		TupleHashTable existing;
		FmgrInfo *eq_funcs;
		FmgrInfo *hash_funcs;
		Relation rel;

		execTuplesHashPrepare(state->ngroupatts, state->groupops, &eq_funcs, &hash_funcs);
		existing = BuildTupleHashTable(state->ngroupatts, state->groupatts, eq_funcs, hash_funcs, 1000,
				sizeof(HeapTupleEntryData), CurrentMemoryContext, CurrentMemoryContext);

		rel = heap_openrv_extended(view->matrel, AccessShareLock, true);

		if (rel)
		{
			HeapTuple tuple;
			HeapScanDesc scan = heap_beginscan(rel, GetActiveSnapshot(), 0, NULL);

			while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
			{
				if (!TupIsNull(state->slot))
					ExecClearTuple(state->slot);

				ExecStoreTuple(heap_copytuple(tuple), state->slot, InvalidBuffer, false);
				tuplestore_puttupleslot(state->batch, state->slot);
				break;
			}

			heap_endscan(scan);
			heap_close(rel, AccessShareLock);
		}

		state->existing = existing;
		values = get_values(state);
	}

	plan = get_cached_groups_plan(state, values);

	tuplestore_end(state->batch);

	return plan;
}
