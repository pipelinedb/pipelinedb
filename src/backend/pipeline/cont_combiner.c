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
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/stream_fdw.h"
#include "pipeline/sw_vacuum.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/hashfuncs.h"
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
#define MURMUR_SEED 0x155517D2

/*
 * Flag that indicates whether or not an on-disk tuple has already been added to
 * an ongoing combine result.
 */
#define EXISTING_ADDED 0x1

#define OLD_TUPLE 0
#define NEW_TUPLE 1

typedef struct
{
	AttrNumber arrival_ts_attr;
	TupleHashTable overlay_groups;
	TupleHashTable step_groups;
	PlannedStmt *overlay_plan;
	Tuplestorestate *overlay_input;
	DestReceiver *overlay_dest;
	Tuplestorestate *overlay_output;
	MemoryContext context;
	TimestampTz last_tick;
	TimestampTz last_matrel_sync;
} SWOutputState;

typedef struct
{
	HeapTupleEntryData base;
	TimestampTz last_touched;
} OverlayTupleEntry;

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

	/* Projection to execute on output stream tuples */
	ProjectionInfo *output_stream_proj;
	TupleTableSlot *proj_input_slot;
	TupleDesc overlay_desc;
	TupleTableSlot *overlay_prev_slot;
	TupleTableSlot *overlay_slot;
	AttrNumber output_stream_arrival_ts;

	/* Sliding-window state */
	SWOutputState *sw;

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
 * build_projection
 */
static ProjectionInfo *
build_projection(List *tlist, EState *estate, ExprContext *econtext,
		TupleDesc input_desc)
{
	TupleTableSlot *result_slot;
	TupleDesc result_desc;
	List *targetlist;

	result_desc = ExecTypeFromTL(tlist, false);
	result_slot = MakeSingleTupleTableSlot(result_desc);
	targetlist = (List *) ExecPrepareExpr((Expr *) tlist, estate);

	return ExecBuildProjectionInfo(targetlist, econtext,
								   result_slot, input_desc);
}

/*
 * project_overlay
 */
static Datum
project_overlay(ContQueryCombinerState *state, HeapTuple tup, bool *isnull)
{
	TupleTableSlot *slot;
	HeapTuple projected;

	if (!state->output_stream_proj)
		return heap_copy_tuple_as_datum(tup, state->desc);

	Assert(state->proj_input_slot);

	/* Should we copy the input tuple so it doesn't get modified? */
	ExecStoreTuple(tup, state->proj_input_slot, InvalidBuffer, false);
	slot = ExecProject(state->output_stream_proj, NULL);

	if (TupIsNull(slot))
	{
		*isnull = true;
		return (Datum) NULL;
	}

	*isnull = false;
	projected = ExecMaterializeSlot(slot);

	return heap_copy_tuple_as_datum(projected, state->overlay_desc);
}

/*
 * load_sw_matrel_groups
 *
 * Sync sliding-window matrel rows newer than the given timestamp
 * from disk into the local cache. If the cache already contains rows
 * covering the given timestamp, the matrel doesn't need to be scanned.
 */
static void
sync_sw_matrel_groups(ContQueryCombinerState *state, Relation matrel)
{
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	ScanKeyData skey[1];
	TimestampTz oldest;
	FunctionCallInfoData hashfcinfo;
	FmgrInfo flinfo;
	int64 cv_name_hash;
	bool close_matrel = matrel == NULL;

	/*
	 * We only need to sync once, all other groups will be cached
	 * from the combiner's output.
	 */
	if (state->sw->last_matrel_sync)
		return;

	/* If a matrel didn't get passed to us, we need to lock one ourselves */
	if (matrel == NULL)
		matrel = heap_openrv_extended(state->base.query->matrel, AccessShareLock, true);

	if (matrel == NULL)
		return;

	Assert(state->sw);
	Assert(state->sw->arrival_ts_attr);
	oldest = GetCurrentTimestamp() - (1000 * state->base.query->sw_interval_ms);

	/* We only need to scan for in-window rows */
	ScanKeyInit(&skey[0],
				state->sw->arrival_ts_attr,
				BTGreaterEqualStrategyNumber, F_TIMESTAMP_GE, TimestampTzGetDatum(oldest));

	scan = heap_beginscan(matrel, GetTransactionSnapshot(), 1, skey);

	if (state->hashfunc)
	{
		InitFunctionCallInfoData(hashfcinfo, &flinfo,
				list_length(state->hashfunc->args), state->hashfunc->funccollid, NULL, NULL);

		fmgr_info(state->hashfunc->funcid, hashfcinfo.flinfo);
		fmgr_info_set_expr((Node *) state->hashfunc, hashfcinfo.flinfo);
	}
	else
	{
		cv_name_hash = MurmurHash3_64(state->base.query->name->relname, strlen(state->base.query->name->relname), MURMUR_SEED);
	}

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool isnew;
		HeapTupleEntry entry;
		MemoryContext old;
		int64 hash;

		ExecStoreTuple(tup, state->slot, InvalidBuffer, false);

		if (state->hashfunc)
			hash = hash_group_for_combiner(state->slot, state->hashfunc, &hashfcinfo);
		else
			hash = cv_name_hash;

		/*
		 * Shard groups across combiners.
		 *
		 * Note that this sharding logic must match exactly with what the worker
		 * uses to shard partials across combiners. This combiner will act on whatever
		 * tuples it receives from the worker, but these are the only tuples that will
		 * be used as input into the overlay plan, so we need to ensure that we sync
		 * all groups that this combiner will receive. Loading all matrel rows would
		 * technically work as well, but we'd get duplicate output stream writes and
		 * obviously waste memory.
		 */
		if (!is_group_hash_mine(hash))
			continue;

		entry = (HeapTupleEntry) LookupTupleHashEntry(state->sw->step_groups, state->slot, &isnew);

		old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
		entry->tuple = heap_copytuple(tup);
		MemoryContextSwitchTo(old);

		state->sw->last_matrel_sync = GetCurrentTimestamp();
	}

	heap_endscan(scan);

	if (close_matrel)
		heap_close(matrel, AccessShareLock);
}

/*
 * compare_slots
 */
static int
compare_slots(TupleTableSlot *lslot, TupleTableSlot *rslot,
		AttrNumber pk, bool *equal)
{
	AttrNumber att;
	TupleDesc desc;
	int natts;
	int num_changed = 0;

	Assert(equalTupleDescs(lslot->tts_tupleDescriptor, rslot->tts_tupleDescriptor));

	desc = lslot->tts_tupleDescriptor;
	natts = desc->natts;

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
		Form_pg_attribute attr = desc->attrs[att - 1];

		if (att == pk)
			continue;

		prev = slot_getattr(lslot, att, &prev_null);
		new = slot_getattr(rslot, att, &new_null);

		/*
		 * Note that this equality check only does a byte-by-byte comparison, which may yield false
		 * negatives. This is fine, because a false negative will just cause an extraneous write, which
		 * seems like a reasonable balance compared to looking up and invoking actual equality operators.
		 */
		if ((!prev_null && !new_null) && datumIsEqual(prev, new, attr->attbyval, attr->attlen))
			equal[att - 1] = false;
		else
			num_changed++;
	}

	return num_changed;
}

/*
 * add_matrel_tuples_to_overlay_input
 *
 * Add all existing cached matrel groups to the input of the overlay plan
 * we're about to execute. Note that we still add matrel rows that may
 * not have been modified by the most recent combine call, because their
 * corresponding values in the overlay output are dependent on the current
 * time.
 */
static List *
add_cached_sw_tuples_to_overlay_input(ContQueryCombinerState *state)
{
	HASH_SEQ_STATUS seq;
	HeapTupleEntry matrel_entry;
	List *to_delete = NIL;

	hash_seq_init(&seq, state->sw->step_groups->hashtab);
	while ((matrel_entry = (HeapTupleEntry) hash_seq_search(&seq)) != NULL)
	{
		Datum d;
		bool isnull;
		TimestampTz ts;

		ExecStoreTuple(matrel_entry->tuple, state->slot, InvalidBuffer, false);
		d = slot_getattr(state->slot, state->sw->arrival_ts_attr, &isnull);
		ts = DatumGetTimestampTz(d);
		Assert(!isnull);

		if ((GetCurrentTimestamp() - ts) / 1000 > state->base.query->sw_interval_ms)
		{
			to_delete = lappend(to_delete, matrel_entry->tuple);
			continue;
		}

		tuplestore_puttuple(state->sw->overlay_input, matrel_entry->tuple);
	}
	tuplestore_rescan(state->sw->overlay_input);

	return to_delete;
}

/*
 * gc_cached_matrel_tuples
 *
 * Garbage collect any out-of-window step tuples
 */
static void
gc_cached_matrel_tuples(ContQueryCombinerState *state, List *expired)
{
	ListCell *lc;

	Assert(state->sw);
	Assert(state->sw->step_groups);

	foreach(lc, expired)
	{
		bool isnew;
		HeapTuple tup = (HeapTuple) lfirst(lc);
		HeapTupleEntry entry;

		ExecStoreTuple(tup, state->slot, InvalidBuffer, false);
		entry = (HeapTupleEntry) LookupTupleHashEntry(state->sw->step_groups, state->slot, &isnew);
		Assert(!isnew);

		RemoveTupleHashEntry(state->sw->step_groups, state->slot);
		heap_freetuple(entry->tuple);
	}
}

/*
 * gc_cached_overlay_tuples
 *
 * GC any out-of-window tuples in the overlay cache. We indicate an
 * out-of-window tuple in the output stream by writing a null new tuple:
 *
 * INSERT INTO osrel (old, new) VALUES (<old tuple>, <null>)
 */
static void
gc_cached_overlay_tuples(ContQueryCombinerState *state,
		TimestampTz this_tick, ResultRelInfo *osri)
{
	HASH_SEQ_STATUS seq;
	OverlayTupleEntry *overlay_entry;
	List *to_delete = NIL;
	ListCell *lc;

	hash_seq_init(&seq, state->sw->overlay_groups->hashtab);
	while ((overlay_entry = (OverlayTupleEntry *) hash_seq_search(&seq)) != NULL)
	{
		Datum values[3];
		bool nulls[3];
		HeapTuple tup;
		HeapTuple os_tup;

		if (overlay_entry->last_touched == this_tick)
			continue;

		tup = overlay_entry->base.tuple;
		to_delete = lappend(to_delete, overlay_entry->base.tuple);

		MemSet(nulls, false, sizeof(nulls));

		nulls[state->output_stream_arrival_ts] = true;
		nulls[NEW_TUPLE] = true;
		values[NEW_TUPLE] = (Datum) 0;
		values[OLD_TUPLE] = heap_copy_tuple_as_datum(tup, state->overlay_desc);

		os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, values, nulls);
		ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
		ExecStreamInsert(NULL, osri, state->os_slot, NULL);
	}

	foreach(lc, to_delete)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);

		ExecStoreTuple(tup, state->overlay_slot, InvalidBuffer, false);
		RemoveTupleHashEntry(state->sw->overlay_groups, state->overlay_slot);
	}
}

/*
 * execute_sw_overlay_plan
 *
 * Execute the overlay plan to compute the instantaneous values over the sliding window
 */
static void
execute_sw_overlay_plan(ContQueryCombinerState *state)
{
	QueryDesc *query_desc;
	struct Plan *plan;

	Assert(state->sw);
	Assert(state->sw->overlay_plan);
	Assert(state->sw->overlay_dest);

	tuplestore_clear(state->sw->overlay_output);
	query_desc = CreateQueryDesc(state->sw->overlay_plan,
			NULL, InvalidSnapshot, InvalidSnapshot, state->sw->overlay_dest, NULL, 0);
	query_desc->estate = CreateEState(query_desc);

	plan = state->sw->overlay_plan->planTree;
	query_desc->planstate = ExecInitNode(plan, query_desc->estate, 0);
	ExecutePlan(query_desc->estate, query_desc->planstate,
			query_desc->operation,
			true, 0, ForwardScanDirection, state->sw->overlay_dest);
	ExecEndNode(query_desc->planstate);

	query_desc->planstate = NULL;
	query_desc->estate = NULL;

	tuplestore_rescan(state->sw->overlay_output);
}

/*
 * tick_sw_groups
 *
 * Since sliding-window values can change over time without receiving
 * new rows, we need to keep recomputing instantaneous values to write
 * to this CV's output stream if anything is reading it.
 */
static void
tick_sw_groups(ContQueryCombinerState *state, Relation matrel, bool force)
{
	TimestampTz this_tick = GetCurrentTimestamp();
	OverlayTupleEntry *overlay_entry;
	Relation osrel;
	ResultRelInfo *osri;
	StreamInsertState *sis;
	List *to_delete = NIL;

	/* Ensure matrel rows are synced into memory */
	sync_sw_matrel_groups(state, matrel);

	if (!force && !TimestampDifferenceExceeds(state->sw->last_tick,
			GetCurrentTimestamp(), state->base.query->sw_step_ms))
		return;

	if (!hash_get_num_entries(state->sw->step_groups->hashtab))
		return;

	osrel = try_relation_open(state->base.query->osrelid, RowExclusiveLock);
	if (osrel == NULL)
		return;

	osri = CQOSRelOpen(osrel);

	BeginStreamModify(NULL, osri, NIL, 0, REENTRANT_STREAM_INSERT);
	sis = (StreamInsertState *) osri->ri_FdwState;
	Assert(sis);

	/* If nothing is reading from this output stream, there is nothing to do */
	if (sis->targets == NULL)
	{
		EndStreamModify(NULL, osri);
		CQOSRelClose(osri);
		heap_close(osrel, NoLock);
		return;
	}

	/*
	 * Compute instantaneous sliding-window values
	 */
	to_delete = add_cached_sw_tuples_to_overlay_input(state);
	gc_cached_matrel_tuples(state, to_delete);
	execute_sw_overlay_plan(state);

	/*
	 * Write out any changed sliding-window values to the output stream
	 */
	tuplestore_rescan(state->sw->overlay_output);
	foreach_tuple(state->overlay_slot, state->sw->overlay_output)
	{
		bool isnew;
		Datum values[3];
		bool nulls[3];
		bool replaces[3];
		HeapTuple new_tup = ExecMaterializeSlot(state->overlay_slot);
		HeapTuple old_tup = NULL;
		HeapTuple os_tup;
		MemoryContext old;

		Assert(!TupIsNull(state->overlay_slot));
		overlay_entry = (OverlayTupleEntry *) LookupTupleHashEntry(state->sw->overlay_groups, state->overlay_slot, &isnew);
		overlay_entry->last_touched = this_tick;

		if (!isnew)
		{
			MemSet(replaces, false, sizeof(replaces));
			ExecStoreTuple(overlay_entry->base.tuple, state->overlay_prev_slot, InvalidBuffer, false);

			/*
			 * Similarly to how we don't sync unchanged tuples to disk when combining,
			 * we don't write unchanged tuples to output streams.
			 */
			if (!compare_slots(state->overlay_slot, state->overlay_prev_slot, state->pk, replaces))
				continue;

			old_tup = overlay_entry->base.tuple;
		}

		old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
		overlay_entry->base.tuple = heap_copytuple(new_tup);
		MemoryContextSwitchTo(old);

		MemSet(nulls, true, sizeof(nulls));

		if (old_tup)
		{
			nulls[OLD_TUPLE] = false;
			values[OLD_TUPLE] = heap_copy_tuple_as_datum(old_tup, state->overlay_desc);
		}

		nulls[state->output_stream_arrival_ts] = true;
		nulls[NEW_TUPLE] = false;
		values[NEW_TUPLE] = heap_copy_tuple_as_datum(new_tup, state->overlay_desc);

		/* Finally write the old and new tuple to the output stream */
		os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, values, nulls);
		ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
		ExecStreamInsert(NULL, osri, state->os_slot, NULL);

		if (old_tup)
			heap_freetuple(old_tup);
	}

	/*
	 * Expire any out-of-window tuples in the overlay cache
	 */
	gc_cached_overlay_tuples(state, this_tick, osri);

	/* Done, cleanup */
	EndStreamModify(NULL, osri);
	CQOSRelClose(osri);
	heap_close(osrel, NoLock);

	tuplestore_clear(state->sw->overlay_input);
	tuplestore_clear(state->sw->overlay_output);

	state->sw->last_tick = GetCurrentTimestamp();
}

/*
 * find_attr
 *
 * Find the attribute with the given name in the given TupleDesc
 */
static AttrNumber
find_attr(TupleDesc desc, char *name)
{
	int i;
	AttrNumber result = InvalidAttrNumber;

	for (i = 0; i < desc->natts; i++)
	{
		if (pg_strcasecmp(name, NameStr(desc->attrs[i]->attname)) == 0)
		{
			result = i + 1;
			break;
		}
	}

	return result;
}

/*
 * init_sw_state
 *
 * Initialize the state necessary to write SW results to output streams
 */
static void
init_sw_state(ContQueryCombinerState *state, Relation matrel)
{
	MemoryContext tmp_cxt;
	TuplestoreScan *scan;
	MemoryContext old;
	Oid *group_ops;
	AttrNumber *group_idx;
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	ColumnRef *cref;
	int n_group_attr = 0;

	if (!state->isagg)
		return;

	cref = GetWindowTimeColumn(state->base.query->name);
	Assert(cref);
	Assert(list_length(cref->fields) == 1);
	Assert(IsA(linitial(cref->fields), String));

	state->sw = palloc0(sizeof(SWOutputState));
	state->sw->arrival_ts_attr = find_attr(state->desc, strVal(linitial(cref->fields)));

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "SWOutputTmpCxt",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	state->sw->step_groups = BuildTupleHashTable(state->ngroupatts,
			state->groupatts, state->eq_funcs, state->hash_funcs, 1000,
			sizeof(HeapTupleEntryData), CurrentMemoryContext, tmp_cxt);

	state->sw->overlay_plan = GetContinuousViewOverlayPlan(state->base.query);
	state->sw->context = AllocSetContextCreate(CurrentMemoryContext, "SWOutputCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(state->sw->context);
	state->sw->overlay_input = tuplestore_begin_heap(true, true, work_mem);
	state->sw->overlay_output = tuplestore_begin_heap(true, true, work_mem);
	MemoryContextSwitchTo(old);

	state->sw->overlay_plan->isContinuous = false;
	scan = SetCombinerPlanTuplestorestate(state->sw->overlay_plan, state->sw->overlay_input);
	scan->desc = state->desc;

	state->sw->overlay_dest = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(state->sw->overlay_dest, state->sw->overlay_output, state->sw->context, true);

	if (IsA(state->sw->overlay_plan->planTree, Agg))
	{
		Agg *agg = (Agg *) state->sw->overlay_plan->planTree;
		int i;

		group_ops = agg->grpOperators;
		n_group_attr = agg->numCols;

		/*
		 * Since this is a view over a matrel, these grouping attributes actually
		 * refer to matrel attributes so we need to offset them by 1 to account for
		 * the matrel's additional arrival_timestamp column.
		 *
		 * There is probably a better way to do this.
		 */
		group_idx = palloc0(sizeof(AttrNumber) * n_group_attr);
		memcpy(group_idx, agg->grpColIdx, sizeof(AttrNumber) * n_group_attr);
		for (i = 0; i < n_group_attr; i++)
		{
			Assert(group_idx[i] >= 1);
			group_idx[i] -= 1;
		}
	}

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "SWOverlayOutputTmpCxt",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	execTuplesHashPrepare(n_group_attr, group_ops, &eq_funcs, &hash_funcs);
	state->sw->overlay_groups = BuildTupleHashTable(n_group_attr,
			group_idx, eq_funcs, hash_funcs, 1000, sizeof(OverlayTupleEntry), CurrentMemoryContext, tmp_cxt);
}

/*
 * project_sw_overlay_into_ostream
 *
 * Execute the sliding-window overlay plan on step-sized matrel groups
 * and write the result to the CV's output stream
 */
static void
project_sw_overlay_into_ostream(ContQueryCombinerState *state, Relation matrel)
{
	Assert(state->sw);
	Assert(state->sw->overlay_input);
	Assert(state->sw->overlay_output);
	Assert(state->sw->overlay_plan);
	Assert(state->sw->overlay_dest);

	/*
	 * Add or replace any tuples in the cache based on the combiner's most recent output
	 */
	tuplestore_rescan(state->combined);
	foreach_tuple(state->slot, state->combined)
	{
		bool isnew;
		MemoryContext old;
		HeapTupleEntry entry = (HeapTupleEntry)
				LookupTupleHashEntry(state->sw->step_groups, state->slot, &isnew);

		if (!isnew)
			heap_freetuple(entry->tuple);

		old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
		entry->tuple = ExecCopySlotTuple(state->slot);
		MemoryContextSwitchTo(old);
	}

	/* Force a tick */
	tick_sw_groups(state, matrel, true);
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

	/*
	 * We'll handle output stream writes separately for SWs since
	 * they require the execution of an overlay plan
	 */
	if (!state->base.query->is_sw)
	{
		osrel = try_relation_open(state->base.query->osrelid, RowExclusiveLock);
		if (osrel == NULL)
			return;

		osri = CQOSRelOpen(osrel);

		BeginStreamModify(NULL, osri, NIL, 0, REENTRANT_STREAM_INSERT);
		sis = (StreamInsertState *) osri->ri_FdwState;
		Assert(sis);

		os_targets = sis->targets;

		/* If nothing is reading from the output stream, close it immediately */
		if (os_targets == NULL)
		{
			EndStreamModify(NULL, osri);
			CQOSRelClose(osri);
			heap_close(osrel, NoLock);
			sis = NULL;
		}
	}

	ri = CQMatRelOpen(matrel);

	estate->es_per_tuple_exprcontext = CreateStandaloneExprContext();
	estate->es_per_tuple_exprcontext->ecxt_scantuple = state->proj_input_slot;

	if (state->output_stream_proj)
		state->output_stream_proj->pi_exprContext = estate->es_per_tuple_exprcontext;

	foreach_tuple(slot, state->combined)
	{
		HeapTupleEntry update = NULL;
		HeapTuple tup = NULL;
		HeapTuple os_tup;
		Datum os_values[3];
		bool os_nulls[3];
		int replaces = 0;

		MemSet(os_nulls, false, sizeof(os_nulls));

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
			replaces = compare_slots(state->prev_slot, state->slot,
					state->pk, replace_all);

			if (replaces == 0)
				continue;

			if (os_targets)
				os_values[OLD_TUPLE] = project_overlay(state, update->tuple, &os_nulls[OLD_TUPLE]);

			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			tup = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot, estate);

			if (os_targets)
				os_values[NEW_TUPLE] = project_overlay(state, tup, &os_nulls[NEW_TUPLE]);

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
				os_nulls[OLD_TUPLE] = true;
				os_nulls[NEW_TUPLE] = false;
				os_values[OLD_TUPLE] = (Datum) NULL;
				os_values[NEW_TUPLE] = project_overlay(state, tup, &os_nulls[NEW_TUPLE]);
			}

			ntups_inserted++;
			nbytes_inserted += HEAPTUPLESIZE + slot->tts_tuple->t_len;
		}

		/*
		 * If anything is reading this CV's output stream, write out the
		 * old and new rows to it
		 */
		if (os_targets &&
				(os_nulls[OLD_TUPLE] == false || os_nulls[NEW_TUPLE] == false))
		{
			os_nulls[state->output_stream_arrival_ts] = true;
			os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, os_values, os_nulls);
			ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, osri, state->os_slot, NULL);
		}

		ResetPerTupleExprContext(estate);
	}

	if (sis)
	{
		EndStreamModify(NULL, osri);
		CQOSRelClose(osri);
		heap_close(osrel, NoLock);
		sis = NULL;
	}

	/*
	 * We handle SW output stream writes here so we can execute the
	 * overlay plan on the whole batch all at once in order to to
	 * compute output stream tuples.
	 */
	if (state->sw)
		project_sw_overlay_into_ostream(state, matrel);

	tuplestore_clear(state->combined);

	pgstat_increment_cq_update(ntups_updated, nbytes_updated);
	pgstat_increment_cq_write(ntups_inserted, nbytes_inserted);

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

/*
 * assign_output_stream_projection
 *
 * If this query's output stream requires a projection, assign the projection here.
 * This is necessary when any aggregates have a final function that isn't applied until
 * read time, as consumers of the output stream expect a finalized aggregate value.
 */
static void
assign_output_stream_projection(ContQueryCombinerState *state)
{
	ListCell *lc;
	bool needs_proj = false;
	PlannedStmt *overlay;
	EState *estate;
	ExprContext *context;
	Relation rel;

	rel = heap_open(state->base.query->relid, NoLock);
	state->overlay_desc = CreateTupleDescCopy(RelationGetDescr(rel));
	state->overlay_slot = MakeSingleTupleTableSlot(state->overlay_desc);
	state->overlay_prev_slot = MakeSingleTupleTableSlot(state->overlay_desc);
	heap_close(rel, NoLock);

	/*
	 * Sliding windows are set up in init_sw_state,
	 * and non-sliding windows aren't supported
	 */
	if (GetWindowTimeColumn(state->base.query->name))
		return;

	overlay = GetContinuousViewOverlayPlan(state->base.query);
	estate = CreateExecutorState();
	context = CreateStandaloneExprContext();

	foreach(lc, overlay->planTree->targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (IsA(te->expr, FuncExpr))
		{
			needs_proj = true;
			break;
		}
	}

	if (!needs_proj)
		return;

	state->output_stream_proj = build_projection(overlay->planTree->targetlist, estate, context, NULL);
	state->proj_input_slot = MakeSingleTupleTableSlot(state->desc);
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

	osrel = try_relation_open(base->query->osrelid, AccessShareLock);
	state->os_slot = MakeSingleTupleTableSlot(CreateTupleDescCopy(RelationGetDescr(osrel)));
	heap_close(osrel, AccessShareLock);

	state->output_stream_arrival_ts = find_attr(state->os_slot->tts_tupleDescriptor, ARRIVAL_TIMESTAMP);
	Assert(state->output_stream_arrival_ts);

	assign_output_stream_projection(state);

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

	if (state->base.query->is_sw)
		init_sw_state(state, matrel);

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
	int min_tick_ms = 0;
	Bitmapset *queries;
	int id;

	/*
	 * Get the minimum timeout we can use to ensure SW queries
	 * tick up-to-date results to any output streams at roughly an interval
	 * equal to their step size.
	 *
	 * Note that if no SW queries are present, the timeout is set to 0 and
	 * thus there is no ticking.
	 */
	StartTransactionCommand();
	queries = GetContinuousViewIds();

	while ((id = bms_first_member(queries)) >= 0)
	{
		ContQuery *q = GetContQueryForViewId(id);
		if (!q->is_sw)
			continue;
		Assert(q->sw_step_ms);
		min_tick_ms = min_tick_ms ? Min(min_tick_ms, q->sw_step_ms) : q->sw_step_ms;
	}

	CommitTransactionCommand();

	/* Set the commit level */
	synchronous_commit = continuous_query_combiner_synchronous_commit;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (ShouldTerminateContQueryProcess())
			break;

		ContExecutorStartBatch(cont_exec, min_tick_ms);

		while ((query_id = ContExecutorStartNextQuery(cont_exec, min_tick_ms)) != InvalidOid)
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

				if (state->sw)
				{
					tick_sw_groups(state, NULL, false);
					min_tick_ms = min_tick_ms ? Min(min_tick_ms, state->base.query->sw_step_ms) : state->base.query->sw_step_ms;
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
		{
			int64 hash =  hash_group_for_combiner(state->slot, state->hashfunc, hashfcinfo);
			set_group_hash(state, state->pending_tuples, hash);
		}

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
