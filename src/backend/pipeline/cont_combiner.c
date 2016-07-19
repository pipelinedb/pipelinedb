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
#include "catalog/namespace.h"
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
#include "pipeline/stream.h"
#include "pipeline/stream_fdw.h"
#include "pipeline/sw_vacuum.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pipelinefuncs.h"
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
	TupleTableSlot *os_slot;
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
	Relation rel = heap_openrv(state->base.query->matrel, AccessShareLock);

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
	sel->fromClause = list_make1(state->base.query->matrel);
	sel->forCombineLookup = true;

	/* populate the ParseState's p_varnamespace member */
	ps = make_parsestate(NULL);
	ps->p_no_locking = true;
	transformFromClause(ps, sel->fromClause);

	qlist = pg_analyze_and_rewrite((Node *) sel, state->base.query->matrel->relname, NULL, 0);
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

	matrel = heap_openrv(state->base.query->matrel, RowShareLock);

	plan = get_cached_groups_plan(state, values);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("select_existing_groups", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			state->base.query->matrel->relname,
			"SELECT",
			list_make1(plan),
			NULL);

	dest = CreateDestReceiver(DestTupleTable);
	SetTupleTableDestReceiverParams(dest, existing, existing->tablecxt, true);

	PortalStart(portal, NULL, 0, NULL);

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
	Relation osrel;
	ResultRelInfo *ri;
	ResultRelInfo *osri;
	Size nbytes_inserted = 0;
	Size nbytes_updated = 0;
	int ntups_inserted = 0;
	int ntups_updated = 0;
	StreamInsertState *sis = NULL;
	Bitmapset *os_targets = NULL;

	matrel = heap_openrv_extended(state->base.query->matrel, RowExclusiveLock, true);
	if (matrel == NULL)
		return;

	osrel = heap_openrv_extended(state->base.query->osrel, RowExclusiveLock, true);
	if (osrel == NULL)
		return;

	osri = CQOSRelOpen(osrel);
	BeginStreamModify(NULL, osri, NIL, 0, 0);
	sis = (StreamInsertState *) osri->ri_FdwState;
	Assert(sis);

	os_targets = sis->targets;

	ri = CQMatRelOpen(matrel);
	natts = state->prev_slot->tts_tupleDescriptor->natts;

	foreach_tuple(slot, state->combined)
	{
		HeapTupleEntry update = NULL;
		HeapTuple tup = NULL;
		HeapTuple os_tup;
		Datum os_values[2];
		bool os_nulls[2];
		AttrNumber att = 1;
		int replaces = 0;

		MemSet(os_nulls, 0, sizeof(os_nulls));

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

			if (os_targets)
				os_values[0] = heap_copy_tuple_as_datum(update->tuple, slot->tts_tupleDescriptor);

			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			tup = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot, estate);

			if (os_targets)
			{
				os_values[1] = heap_copy_tuple_as_datum(tup, slot->tts_tupleDescriptor);
				os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, os_values, os_nulls);
			}

			ntups_updated++;
			nbytes_updated += HEAPTUPLESIZE + slot->tts_tuple->t_len;
		}
		else
		{
			/* No existing tuple found, so it's an INSERT. Also generate a primary key for it if necessary. */
			if (state->seq_pk)
				slot->tts_values[state->pk - 1] = nextval_internal(state->base.query->seqrelid);
			slot->tts_isnull[state->pk - 1] = false;
			tup = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelInsert(ri, slot, estate);

			if (os_targets)
			{
				os_nulls[0] = true;
				os_nulls[1] = false;

				os_values[0] = (Datum) NULL;
				os_values[1] = heap_copy_tuple_as_datum(tup, slot->tts_tupleDescriptor);
			}

			ntups_inserted++;
			nbytes_inserted += HEAPTUPLESIZE + slot->tts_tuple->t_len;
		}

		if (os_targets && (os_nulls[0] == false || os_nulls[1] == false))
		{
			os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, os_values, os_nulls);
			ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, osri, state->os_slot, NULL);
		}

		ResetPerTupleExprContext(estate);
	}

	tuplestore_clear(state->combined);

	pgstat_increment_cq_update(ntups_updated, nbytes_updated);
	pgstat_increment_cq_write(ntups_inserted, nbytes_inserted);

	CQMatRelClose(ri);
	heap_close(matrel, NoLock);








	EndStreamModify(NULL, osri);
	CQOSRelClose(osri);
	heap_close(osrel, NoLock);

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

		MyStatCQEntry = (PgStat_StatCQEntry *) &state->base.stats;

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

		pgstat_report_cqstat(false);

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

	portal = CreatePortal("combine", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  state->base.query->matrel->relname,
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
	Relation osrel ;

	if (base->query->type != CONT_VIEW)
	{
		base->query = NULL;
		return base;
	}

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

	matrel = heap_openrv_extended(base->query->matrel, AccessShareLock, true);
	pstmt = GetContPlan(base->query, COMBINER);

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

	if (matrel == NULL)
	{
		base->query = NULL;
		return base;
	}

	osrel = heap_openrv_extended(base->query->osrel, AccessShareLock, true);

	if (osrel  == NULL)
	{
		base->query = NULL;
		return base;
	}

	state->os_slot = MakeSingleTupleTableSlot(CreateTupleDescCopy(RelationGetDescr(osrel)));
	heap_close(osrel, AccessShareLock);

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
			state->hashfunc = GetGroupHashIndexExpr(ri);

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
			Assert(form->indnatts == 1);
			state->pk = form->indkey.values[0];
		}
		ReleaseSysCache(tup);
	}

	heap_close(matrel, AccessShareLock);

	Assert(AttributeNumberIsValid(state->pk));
	state->seq_pk = OidIsValid(base->query->seqrelid);

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
	Size nbytes = 0;
	int count = 0;

	while ((pts = (PartialTupleState *) ContExecutorYieldNextMessage(cont_exec, &len)) != NULL)
	{
		if (!TupIsNull(state->slot))
			ExecClearTuple(state->slot);

		ExecStoreTuple(heap_copytuple(pts->tup), state->slot, InvalidBuffer, false);
		tuplestore_puttupleslot(state->batch, state->slot);

		set_group_hash(state, count, pts->hash);

		nbytes += len;
		count++;

	}

	if (!TupIsNull(state->slot))
		ExecClearTuple(state->slot);

	pgstat_increment_cq_read(count, nbytes);

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
	ContExecutor *cont_exec = ContExecutorNew(COMBINER, &init_query_state);
	Oid query_id;
	TimestampTz first_seen = GetCurrentTimestamp();
	bool do_commit = false;
	long total_pending = 0;

	/* Set the commit level */
	synchronous_commit = continuous_query_combiner_synchronous_commit;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (ShouldTerminateContQueryProcess())
			break;

		ContExecutorStartBatch(cont_exec);

		while ((query_id = ContExecutorStartNextQuery(cont_exec)) != InvalidOid)
		{
			int count = 0;
			ContQueryCombinerState *state = (ContQueryCombinerState *) cont_exec->current_query;

			PG_TRY();
			{
				if (state == NULL)
					goto next;

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
				pgstat_increment_cq_error(1);

				if (!continuous_query_crash_recovery)
					proc_exit(1);

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

		MyStatCQEntry = (PgStat_StatCQEntry *) &state->base.stats;
		pgstat_report_cqstat(true);
	}

	MemoryContextSwitchTo(TopMemoryContext);
	ContExecutorDestroy(cont_exec);
}

/*
 * GetCombinerLookupPlan
 */
PlannedStmt *
GetCombinerLookupPlan(ContQuery *view)
{
	ContQueryCombinerState *state;
	ContQueryState *base;
	PlannedStmt *plan;
	List *values = NIL;
	ContExecutor exec;
	bool save = am_cont_combiner;

	exec.cxt = CurrentMemoryContext;
	exec.current_query_id = view->id;

	base = palloc0(sizeof(ContQueryState));

	base->query_id = view->id;
	base->query = view;
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

	am_cont_combiner = true;
	plan = get_cached_groups_plan(state, values);
	am_cont_combiner = save;

	tuplestore_end(state->batch);

	return plan;
}

static uint64
hash_group(ContQueryCombinerState *state, FunctionCallInfo fcinfo)
{
	ListCell *lc;
	Datum result;
	int i = 0;

	foreach(lc, state->hashfunc->args)
	{
		AttrNumber attno = ((Var *) lfirst(lc))->varattno;
		bool isnull;
		Datum d;

		d = slot_getattr(state->slot, attno, &isnull);
		fcinfo->arg[i] = d;
		fcinfo->argnull[i] = isnull;
		i++;
	}

	result = FunctionCallInvoke(fcinfo);

	return DatumGetInt64(result);
}

/*
 * equal_tupdesc
 *
 * This is less strict than equalTupleDescs and enforces enough similarity that we can merge tuples.
 */
static bool
equal_tupdesc(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	int	i;

	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
		return false;

	for (i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = tupdesc1->attrs[i];
		Form_pg_attribute attr2 = tupdesc2->attrs[i];

		if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->attstattarget != attr2->attstattarget)
			return false;
		if (attr1->attndims != attr2->attndims)
			return false;
		if (attr1->attstorage != attr2->attstorage)
			return false;
		if (attr1->atthasdef != attr2->atthasdef)
			return false;
		if (attr1->attisdropped != attr2->attisdropped)
			return false;
		if (attr1->attcollation != attr2->attcollation)
			return false;
		/* attacl, attoptions and attfdwoptions are not even present... */
	}

	return true;
}

Datum
pipeline_combine_table(PG_FUNCTION_ARGS)
{
	text *cv_name = PG_GETARG_TEXT_P(0);
	RangeVar *cv_rv = makeRangeVarFromNameList(textToQualifiedNameList(cv_name));
	text *relname = PG_GETARG_TEXT_P(1);
	RangeVar *rel_rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	ContQuery *cv = GetContQueryForView(cv_rv);
	Relation matrel;
	Relation srcrel;
	ContExecutor exec;
	ContQueryState *base;
	ContQueryCombinerState *state;
	HeapScanDesc scan;
	HeapTuple tup;
	FunctionCallInfo hashfcinfo = palloc0(sizeof(FunctionCallInfoData));

	if (cv == NULL)
		elog(ERROR, "continuous view \"%s\" does not exist", text_to_cstring(cv_name));

	matrel = heap_openrv(cv->matrel, ExclusiveLock);
	srcrel = heap_openrv(rel_rv, AccessShareLock);

	if (!equal_tupdesc(RelationGetDescr(matrel), RelationGetDescr(srcrel)))
		elog(ERROR, "schema of \"%s\" does not match the schema of \"%s\"",
				text_to_cstring(relname), quote_qualified_identifier(cv->matrel->schemaname, cv->matrel->relname));

	exec.cxt = CurrentMemoryContext;
	exec.current_query_id = cv->id;
	exec.queries = bms_make_singleton(cv->id);

	base = palloc0(sizeof(ContQueryState));

	base->query_id = cv->id;
	base->query = cv;
	base->state_cxt = CurrentMemoryContext;
	base->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "pipeline_combine_table temp cxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	state = (ContQueryCombinerState *) init_query_state(&exec, base);
	base = &state->base;
	exec.states[cv->id] = base;

	hashfcinfo->flinfo = palloc0(sizeof(FmgrInfo));
	hashfcinfo->flinfo->fn_mcxt = base->tmp_cxt;

	if (state->hashfunc)
	{
		fmgr_info(state->hashfunc->funcid, hashfcinfo->flinfo);
		fmgr_info_set_expr((Node *) state->hashfunc, hashfcinfo->flinfo);

		hashfcinfo->fncollation = state->hashfunc->funccollid;
		hashfcinfo->nargs = list_length(state->hashfunc->args);
	}

	scan = heap_beginscan(srcrel, GetTransactionSnapshot(), 0, NULL);
	state->pending_tuples = 0;

	Assert(base->tmp_cxt);
	MemoryContextSwitchTo(base->tmp_cxt);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		ExecStoreTuple(heap_copytuple(tup), state->slot, InvalidBuffer, false);
		tuplestore_puttupleslot(state->batch, state->slot);

		if (state->hashfunc)
			set_group_hash(state, state->pending_tuples, hash_group(state, hashfcinfo));

		if (++state->pending_tuples < continuous_query_batch_size)
			continue;

		combine(state);
		sync_all(&exec);

		MemoryContextResetAndDeleteChildren(base->tmp_cxt);

		state->pending_tuples = 0;
	}

	if (state->pending_tuples)
	{
		combine(state);
		sync_all(&exec);
	}

	heap_endscan(scan);

	heap_close(srcrel, NoLock);
	heap_close(matrel, NoLock);

	PG_RETURN_BOOL(true);
}
