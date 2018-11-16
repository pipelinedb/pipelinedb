/*-------------------------------------------------------------------------
 *
 * combiner.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "analyzer.h"
#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "combiner_receiver.h"
#include "commands/sequence.h"
#include "commands/lockcmds.h"
#include "compat.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "hashfuncs.h"
#include "matrel.h"
#include "miscadmin.h"
#include "miscutils.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/paths.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "physical_group_lookup.h"
#include "pipeline_query.h"
#include "planner.h"
#include "rewrite/rewriteHandler.h"
#include "scheduler.h"
#include "stats.h"
#include "stream_fdw.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

#define GROUPS_PLAN_LIFESPAN (10 * 1000)
#define MURMUR_SEED 0x155517D2

#define SHOULD_UPDATE(state) ((state)->base.query->cvdef->distinctClause == NIL)

/*
 * Flag that indicates whether or not an on-disk tuple has already been added to
 * an ongoing combine result.
 */
#define EXISTING_ADDED 0x1

#define OLD_TUPLE 		0
#define NEW_TUPLE 		1
#define DELTA_TUPLE		2

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
	PhysicalTupleData base;
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
	Tuplestorestate *combined;
	TupleTableSlot *slot;
	TupleTableSlot *delta_slot;
	TupleTableSlot *prev_slot;
	TupleTableSlot *os_slot;
	bool isagg;
	int ngroupatts;
	AttrNumber *groupatts;
#if (PG_VERSION_NUM < 110000)
	FmgrInfo *eq_funcs;
#else
	Oid *eq_funcs;
#endif
	FmgrInfo *hash_funcs;
	Oid *groupops;
	FuncExpr *hashfunc;
	TupleHashTable existing;
	TupleHashTable deltas;
	long pending_tuples;

	/* Stores the hashes of the current batch, in parallel to the order of the batch's tuplestore */
	int64 *group_hashes;
	int group_hashes_len;
	AttrNumber pk;
	bool seq_pk;
	FunctionCallInfo hash_fcinfo;

	/* Projection to execute on output stream tuples */
	ProjectionInfo *output_stream_proj;
	TupleTableSlot *proj_input_slot;
	TupleDesc overlay_desc;
	TupleTableSlot *overlay_prev_slot;
	TupleTableSlot *overlay_slot;
	AttrNumber output_stream_arrival_ts;

	Node *lookup_query;
	RangeTblEntry *lookup_rte;

	/* Sliding-window state */
	SWOutputState *sw;

	List *acks;
} ContQueryCombinerState;

/*
 * prepare_combine_plan
 */
static void
prepare_combine_plan(ContQueryCombinerState *state, PlannedStmt *plan)
{
	/*
	 * Mark combine_plan as not continuous now because we'll be repeatedly
	 * executing it in a new portal. We also need to set its batch
	 * size to 0 so that TuplestoreScans don't return early. Since
	 * they're not being executed continuously, they'd never
	 * see anything after the first batch was consumed.
	 */
	Relation rel = heap_open(state->base.query->matrelid, AccessShareLock);

	SetCombinerPlanTuplestorestate(plan, state->batch);

	state->combine_plan = plan;
	state->desc = CreateTupleDescCopy(RelationGetDescr(rel));

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
	CustomScan *lookup;
	NestLoop *nl;
	ValuesScan *scan;

	if (!IsA(plan->planTree, CustomScan))
		elog(ERROR, "unexpected group retrieval plan: %d", nodeTag(plan->planTree));

	lookup = (CustomScan *) plan->planTree;

	if (!IsA(lookup->scan.plan.lefttree, NestLoop))
		elog(ERROR, "unexpected join type found in group retrieval plan: %d", nodeTag(lookup->scan.plan.lefttree));

	nl = (NestLoop *) lookup->scan.plan.lefttree;

	if (!IsA(nl->join.plan.lefttree, ValuesScan))
		elog(ERROR, "unexpected outer plan found in group retrieval plan: %d",
				nodeTag(nl->join.plan.lefttree));

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
	state->lookup_rte->values_lists = values;

	return copyObject(state->lookup_query);
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
	RawStmt *raw;
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

	/* we can't use the matrel RangeVar here because the matre's schema may have changed */
	sel->fromClause = list_make1(RelidGetRangeVar(state->base.query->matrelid));

	/* populate the ParseState's p_varnamespace member */
	ps = make_parsestate(NULL);
	transformFromClause(ps, sel->fromClause);

	raw = makeNode(RawStmt);
	raw->stmt = (Node *) sel;

	PipelineContextSetCombinerLookup();
	qlist = pg_analyze_and_rewrite(raw, state->base.query->matrel->relname, NULL, 0, NULL);
	ClearPipelineContext();
	query = (Query *) linitial(qlist);

	if (state->ngroupatts > 0 && list_length(values))
	{
		Node *groups = get_groups(state, values, ps);
		query->jointree = makeFromExpr(query->jointree->fromlist, groups);
		query->hasSubLinks = true;
	}

	plan = GetGroupsLookupPlan(query);

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

	groups = CompatBuildTupleHashTable(state->desc, existing->numCols, existing->keyColIdx,
			state->eq_funcs, existing->tab_hash_funcs, 1000,
			existing->entrysize, cxt, tmp_cxt, false);

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
	PhysicalTuple pt;
	TupleTableSlot *slot = state->slot;
	List *tups = NIL;
	ListCell *lc;
	List *values = NIL;
	TupleHashTable batchgroups;
	Relation matrel;
	TupleHashIterator status;
	TupleHashEntry entry;

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
		if (state->existing->hashtab->members)
			return;
	}

	matrel = heap_open(state->base.query->matrelid, RowShareLock);

	plan = get_cached_groups_plan(state, values);

	/*
	 * Group lookups should always be underneath a physical group lookup CustomScan
	 */
	Assert(IsA(plan->planTree, CustomScan));
	SetPhysicalGroupLookupOutput(existing);

	/*
	 * Now run the query that retrieves existing tuples to merge this merge request with.
	 * This query outputs to the tuplestore currently holding the incoming merge tuples.
	 */
	portal = CreatePortal("select_existing_groups", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			RelationGetRelationName(matrel),
			"SELECT",
			list_make1(plan),
			NULL);

	/*
	 * The plan we're about to run returns physical tuples that can be updated in place,
	 * making the combiner sync as efficient as possible. Because of this, these plans
	 * return tuples at a lower level than a DestReceiever, so we don't even need one.
	 */
	dest = CreateDestReceiver(DestNone);

	PortalStart(portal, NULL, 0, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
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
	InitTupleHashIterator(existing, &status);
	while ((entry = ScanTupleHashTable(existing, &status)) != NULL)
	{
		pt = entry->additional;
		/*
		 * We only need to add on-disk tuples to the input once, because they'll
		 * be accumulated upon within the ongoing combine result until we sync.
		 */
		if (pt->flags & EXISTING_ADDED)
			continue;

		/*
		 * Our index can potentially have collisions, so we filter out any
		 * tuples that were returned that aren't related to the batch we're
		 * currently processing. This is just a matter of intersecting the
		 * retrieved groups with the batch's groups.
		 */
		ExecStoreTuple(pt->tuple, slot, InvalidBuffer, false);
		Assert(!TupIsNull(slot));

		if (LookupTupleHashEntry(batchgroups, slot, NULL))
		{
			tuplestore_puttuple(state->batch, pt->tuple);
			pt->flags |= EXISTING_ADDED;
		}
	}

	foreach(lc, tups)
	{
		HeapTuple tup = (HeapTuple) lfirst(lc);
		tuplestore_puttuple(state->batch, tup);
	}

	list_free(tups);
}

/*
 * reset_existing_hashtable
 *
 * Free any memory associated with the given state's existing hashtable and
 * create a new hashtable.
 */
static TupleHashTable
build_existing_hashtable(ContQueryCombinerState *state, char *name)
{
	MemoryContext existing_cxt;
	MemoryContext existing_tmp_cxt;
	MemoryContext old;
	TupleHashTable result;

	existing_cxt = AllocSetContextCreate(state->combine_cxt, "CombinerExistingContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	existing_tmp_cxt = AllocSetContextCreate(existing_cxt, "CombinerExistingTmpContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	old = MemoryContextSwitchTo(state->combine_cxt);

	result = CompatBuildTupleHashTable(state->desc, state->ngroupatts, state->groupatts, state->eq_funcs, state->hash_funcs, 1000,
			sizeof(PhysicalTupleData), existing_cxt, existing_tmp_cxt, false);

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

	result_desc = ExecTypeFromTL(tlist, false);
	result_slot = MakeSingleTupleTableSlot(result_desc);

	return ExecBuildProjectionInfo(tlist, econtext, result_slot, NULL, input_desc);
}

/*
 * project_overlay
 */
static Datum
project_overlay(ContQueryCombinerState *state, ExprContext *econtext, HeapTuple tup, bool *isnull)
{
	TupleTableSlot *slot;
	HeapTuple projected;
	TupleTableSlot *prev = econtext->ecxt_scantuple;

	Assert(state->output_stream_proj);
	Assert(state->proj_input_slot);

	/* Should we copy the input tuple so it doesn't get modified? */
	ExecStoreTuple(tup, state->proj_input_slot, InvalidBuffer, false);
	econtext->ecxt_scantuple = state->proj_input_slot;
	slot = ExecProject(state->output_stream_proj);
	econtext->ecxt_scantuple = prev;

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
	uint64 cv_name_hash = InvalidOid;
	bool close_matrel = matrel == NULL;
	Size size = 0;

	/*
	 * We only need to sync once, all other groups will be cached
	 * from the combiner's output.
	 */
	if (state->sw->last_matrel_sync)
		return;

	/* If a matrel didn't get passed to us, we need to lock one ourselves */
	if (matrel == NULL)
		matrel = try_relation_open(state->base.query->matrelid, AccessShareLock);

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
		TupleHashEntry entry;
		bool isnew;
		OverlayTupleEntry *ot;
		MemoryContext old;
		uint64 hash;

		ExecStoreTuple(tup, state->slot, InvalidBuffer, false);

		if (state->hashfunc)
			hash = slot_hash_group_skip_attr(state->slot, state->base.query->sw_attno, state->hashfunc, &hashfcinfo);
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

		entry = LookupTupleHashEntry(state->sw->step_groups, state->slot, &isnew);

		if (!isnew)
		{
			Assert(entry->additional);
			pfree(entry->additional);
		}

		size += state->sw->step_groups->entrysize + HEAPTUPLESIZE + tup->t_len;

		if (size > continuous_query_combiner_work_mem)
			elog(ERROR, "not enough continuous_query_combiner_work_mem to sync sliding-window groups");

		old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
		ot = palloc0(sizeof(OverlayTupleEntry));
		ot->base.tuple = heap_copytuple(tup);
		entry->additional = ot;
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
		Form_pg_attribute attr = TupleDescAttr(desc, att - 1);

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
	TupleHashEntry matrel_entry;
	List *to_delete = NIL;
	TupleHashIterator seq;

	InitTupleHashIterator(state->sw->step_groups, &seq);
	while ((matrel_entry = ScanTupleHashTable(state->sw->step_groups, &seq)) != NULL)
	{
		Datum d;
		bool isnull;
		TimestampTz ts;
		OverlayTupleEntry *ot = (OverlayTupleEntry *) matrel_entry->additional;
		PhysicalTuple matrel_tup = &ot->base;

		ExecStoreTuple(matrel_tup->tuple, state->slot, InvalidBuffer, false);
		d = slot_getattr(state->slot, state->sw->arrival_ts_attr, &isnull);
		ts = DatumGetTimestampTz(d);
		Assert(!isnull);

		if ((GetCurrentTimestamp() - ts) / 1000 > state->base.query->sw_interval_ms)
		{
			to_delete = lappend(to_delete, ot);
			continue;
		}

		tuplestore_puttuple(state->sw->overlay_input, matrel_tup->tuple);
	}
	tuplestore_rescan(state->sw->overlay_input);

	return to_delete;
}

/*
 * tuplehash_remove
 *
 * Remove the given tuple from the given TupleHashTable. The standard PG implementation
 * doesn't support this operation so we need our own implementation.
 */
static bool
tuplehash_remove(TupleHashTable hash, TupleTableSlot *slot)
{
	TupleTableSlot *save = hash->inputslot;
	bool result;

	/*
	 * The TupleHashTable is a bit different in that it uses this slot to get the hash value from,
	 * rather than being given an input key.
	 */
	hash->inputslot = slot;
	result = tuplehash_delete(hash->hashtab, NULL);

	/* This may be paranoid, but preserve the state of the TupleHashTable as it was given to us */
	hash->inputslot = save;

	return result;
}

/*
 * destroy_overlay_tuple_entry
 */
static void
destroy_overlay_tuple_entry(OverlayTupleEntry *ot)
{
	Assert(ot);
	pfree(ot->base.tuple);
	pfree(ot);
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
		OverlayTupleEntry *ot = (OverlayTupleEntry *) lfirst(lc);
		HeapTuple tup = ot->base.tuple;
#ifdef USE_ASSERT_CHECKING
		bool removed;
#endif

		ExecStoreTuple(tup, state->slot, InvalidBuffer, false);
#ifdef USE_ASSERT_CHECKING
		removed = tuplehash_remove(state->sw->step_groups, state->slot);
#else
		tuplehash_remove(state->sw->step_groups, state->slot);
#endif

		Assert(removed);
		destroy_overlay_tuple_entry(ot);
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
	TupleHashEntry entry;
	List *to_delete = NIL;
	ListCell *lc;
	TupleHashIterator seq;

	InitTupleHashIterator(state->sw->overlay_groups, &seq);
	while ((entry = (TupleHashEntry) ScanTupleHashTable(state->sw->overlay_groups, &seq)) != NULL)
	{
		Datum values[4];
		bool nulls[4];
		HeapTuple tup;
		HeapTuple os_tup;
		OverlayTupleEntry *overlay_entry = (OverlayTupleEntry *) entry->additional;

		if (overlay_entry->last_touched == this_tick)
			continue;

		tup = overlay_entry->base.tuple;
		to_delete = lappend(to_delete, overlay_entry);

		MemSet(nulls, false, sizeof(nulls));

		nulls[state->output_stream_arrival_ts - 1] = true;
		nulls[NEW_TUPLE] = true;
		values[NEW_TUPLE] = (Datum) 0;
		values[OLD_TUPLE] = heap_copy_tuple_as_datum(tup, state->overlay_desc);

		os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, values, nulls);
		ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
		ExecStreamInsert(NULL, osri, state->os_slot, NULL);
	}

	foreach(lc, to_delete)
	{
		OverlayTupleEntry *ot = (OverlayTupleEntry *) lfirst(lc);
		HeapTuple tup = ot->base.tuple;
#ifdef USE_ASSERT_CHECKING
		bool removed;
#endif

		ExecStoreTuple(tup, state->overlay_slot, InvalidBuffer, false);
#ifdef USE_ASSERT_CHECKING
		removed = tuplehash_remove(state->sw->overlay_groups, state->overlay_slot);
#else
		tuplehash_remove(state->sw->overlay_groups, state->overlay_slot);
#endif

		Assert(removed);
		destroy_overlay_tuple_entry(ot);
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
			NULL, InvalidSnapshot, InvalidSnapshot, state->sw->overlay_dest, NULL, NULL, 0);
	query_desc->estate = CreateEState(query_desc);

	plan = state->sw->overlay_plan->planTree;
	query_desc->planstate = ExecInitNode(plan, query_desc->estate, 0);
	ExecuteContPlan(query_desc->estate, query_desc->planstate, false,
			query_desc->operation,
			true, 0, ForwardScanDirection, state->sw->overlay_dest, true);
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
	Relation osrel;
	ResultRelInfo *osri;
	StreamInsertState *sis;
	List *to_delete = NIL;
	List *fdw_private;

	if (!force && !TimestampDifferenceExceeds(state->sw->last_tick,
			GetCurrentTimestamp(), state->base.query->sw_step_ms))
		return;

	if (!state->sw->step_groups->hashtab->members)
		return;

	osrel = try_relation_open(state->base.query->osrelid, RowExclusiveLock);
	if (osrel == NULL)
		return;

	osri = CQOSRelOpen(osrel);

	/*
	 * No acks for non-forced ticks since they don't get triggered by
	 * any tuple insertion.
	 */
	fdw_private = list_make1(force ? state->acks : NIL);

	BeginStreamModify(NULL, osri, fdw_private, 0, REENTRANT_STREAM_INSERT);
	sis = (StreamInsertState *) osri->ri_FdwState;
	Assert(sis);

	/* If nothing is reading from this output stream, there is nothing to do */
	if (sis->queries == NULL)
	{
		EndStreamModify(NULL, osri);
		CQOSRelClose(osri);
		heap_close(osrel, RowExclusiveLock);
		return;
	}

	/* Ensure matrel rows are synced into memory */
	sync_sw_matrel_groups(state, matrel);

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
		Datum values[4];
		bool nulls[4];
		bool replaces[state->overlay_desc->natts];
		HeapTuple new_tup = ExecMaterializeSlot(state->overlay_slot);
		HeapTuple old_tup = NULL;
		HeapTuple os_tup;
		MemoryContext old;
		TupleHashEntry entry;
		OverlayTupleEntry *overlay_entry = NULL;

		Assert(!TupIsNull(state->overlay_slot));
		entry = (TupleHashEntry) LookupTupleHashEntry(state->sw->overlay_groups, state->overlay_slot, &isnew);

		/*
		 * We must create our additional data ourselves
		 */
		if (isnew)
		{
			old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
			overlay_entry = palloc0(sizeof(OverlayTupleEntry));
			entry->additional = overlay_entry;
			MemoryContextSwitchTo(old);
		}

		Assert(entry->additional);
		overlay_entry = (OverlayTupleEntry *) entry->additional;
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
		overlay_entry->last_touched = this_tick;
		MemoryContextSwitchTo(old);

		entry->additional = overlay_entry;
		MemSet(nulls, true, sizeof(nulls));

		if (old_tup)
		{
			nulls[OLD_TUPLE] = false;
			values[OLD_TUPLE] = heap_copy_tuple_as_datum(old_tup, state->overlay_desc);
		}

		nulls[state->output_stream_arrival_ts - 1] = true;
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
	heap_close(osrel, RowExclusiveLock);

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
		if (pg_strcasecmp(name, NameStr(TupleDescAttr(desc, i)->attname)) == 0)
		{
			result = i + 1;
			break;
		}
	}

	return result;
}

/*
 * set_overlay_group_attrs
 *
 * Set the attribute numbers that define the SW overlay group. The aggregate
 * plan refers to the matrel, which will have an extra step column. Since this
 * column is aggregated out of the overlay plan's output, we need to adjust
 * the aggregate plan's attribute numbers to reflect their positions in the
 * overlay plan.
 */
static void
set_overlay_group_attrs(Agg *agg, AttrNumber *overlay_group_idx)
{
	ListCell *lc;

	foreach(lc, agg->plan.targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (te->ressortgroupref)
		{
			Assert(te->ressortgroupref <= agg->numCols);
			overlay_group_idx[te->ressortgroupref - 1] = te->resno;
		}
	}
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
	MemoryContext old;
	Oid *group_ops = NULL;
	AttrNumber *group_idx = NULL;
#if (PG_VERSION_NUM < 110000)
	FmgrInfo *eq_funcs;
#else
	Oid *eq_funcs;
#endif
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

	state->sw->step_groups = CompatBuildTupleHashTable(state->desc, state->ngroupatts,
			state->groupatts, state->eq_funcs, state->hash_funcs, 1000,
			sizeof(OverlayTupleEntry), CurrentMemoryContext, tmp_cxt, false);

	state->sw->overlay_plan = GetContViewOverlayPlan(state->base.query);
	state->sw->context = AllocSetContextCreate(CurrentMemoryContext, "SWOutputCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(state->sw->context);
	state->sw->overlay_input = tuplestore_begin_heap(true, true, work_mem);
	state->sw->overlay_output = tuplestore_begin_heap(true, true, work_mem);
	MemoryContextSwitchTo(old);

	SetCombinerPlanTuplestorestate(state->sw->overlay_plan, state->sw->overlay_input);

	state->sw->overlay_dest = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(state->sw->overlay_dest, state->sw->overlay_output, state->sw->context, true);

	if (IsA(state->sw->overlay_plan->planTree, Agg))
	{
		Agg *agg = (Agg *) state->sw->overlay_plan->planTree;

		group_ops = agg->grpOperators;
		n_group_attr = agg->numCols;

		/*
		 * Since this is a view over a matrel, these grouping attributes actually
		 * refer to matrel attributes so we need to offset them by 1 to account for
		 * the matrel's additional arrival_timestamp column.
		 */
		group_idx = palloc0(sizeof(AttrNumber) * n_group_attr);
		memcpy(group_idx, agg->grpColIdx, sizeof(AttrNumber) * n_group_attr);
		set_overlay_group_attrs(agg, group_idx);
	}

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "SWOverlayOutputTmpCxt",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	CompatExecTuplesHashPrepare(n_group_attr, group_ops, &eq_funcs, &hash_funcs);
	state->sw->overlay_groups = CompatBuildTupleHashTable(state->overlay_desc, n_group_attr,
			group_idx, eq_funcs, hash_funcs, 1000, sizeof(OverlayTupleEntry), CurrentMemoryContext, tmp_cxt, false);
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
		TupleHashEntry entry = LookupTupleHashEntry(state->sw->step_groups, state->slot, &isnew);
		OverlayTupleEntry *ot;

		if (isnew)
		{
			old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
			ot = palloc0(sizeof(OverlayTupleEntry));
			entry->additional = ot;
			MemoryContextSwitchTo(old);
		}

		Assert(entry->additional);
		ot = (OverlayTupleEntry *) entry->additional;

		if (!isnew)
			heap_freetuple(ot->base.tuple);

		old = MemoryContextSwitchTo(state->sw->step_groups->tablecxt);
		ot->base.tuple = ExecCopySlotTuple(state->slot);
		MemoryContextSwitchTo(old);
	}

	/* Force a tick */
	tick_sw_groups(state, matrel, true);
}

/*
 * combine
 *
 * Combines partial results of a continuous query with existing rows in the continuous view
 */
static void
combine(ContQueryCombinerState *state, bool lookup)
{
	Portal portal;
	DestReceiver *dest;

	if (state->isagg && lookup)
	{
		if (state->existing == NULL)
			state->existing = build_existing_hashtable(state, "CombinerExistingGroups");
		select_existing_groups(state);
	}

	foreach_tuple(state->slot, state->combined)
	{
		tuplestore_puttupleslot(state->batch, state->slot);
	}
	tuplestore_clear(state->combined);

	portal = CreatePortal("combine", true, true);
	portal->visible = false;

	/*
	 * Note: we can use the matrel name here because only its schema can potentially change
	 */
	PortalDefineQuery(portal,
					  NULL,
					  state->base.query->matrel->relname,
					  "SELECT",
					  list_make1(state->combine_plan),
					  NULL);

	dest = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(dest, state->combined, state->combine_cxt, false);

	PortalStart(portal, NULL, 0, NULL);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 true,
					 dest,
					 dest,
					 NULL);

	PortalDrop(portal, false);
	tuplestore_clear(state->batch);
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
		state->group_hashes = repalloc(state->group_hashes, state->group_hashes_len * sizeof(int64));
		MemoryContextSwitchTo(old);
	}

	Assert(index < state->group_hashes_len);
	state->group_hashes[index] = hash;
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
	Bitmapset *orig_targets = NULL;
	int pending = 0;

	estate->es_range_table = state->combine_plan->rtable;

	osrel = try_relation_open(state->base.query->osrelid, RowExclusiveLock);
	if (osrel == NULL)
		return;

	matrel = try_relation_open(state->base.query->matrelid, RowExclusiveLock);
	if (matrel == NULL)
	{
		heap_close(osrel, RowExclusiveLock);
		return;
	}

	/*
	 * We haven't combined anything with on-disk groups yet, so what's
	 * in the combined store is the deltas that are about to be applied
	 * to existing groups. We load this into a hashtable so they can be
	 * written to the output stream if necessary, in the order that they're
	 * sync'd to disk.
	 */
	state->deltas = build_existing_hashtable(state, "CombinerDeltas");
	foreach_tuple(state->slot, state->combined)
	{
		bool new;
		TupleHashEntry entry;
		PhysicalTuple pt;

		entry = LookupTupleHashEntry(state->deltas, state->slot, &new);

		pt = palloc0(sizeof(PhysicalTupleData));
		pt->tuple = ExecCopySlotTuple(state->slot);
		entry->additional = pt;

		tuplestore_puttupleslot(state->batch, state->slot);

		if (state->hashfunc)
		{
			uint64 hash =  slot_hash_group(state->slot, state->hashfunc, state->hash_fcinfo);
			set_group_hash(state, pending++, hash);
		}
	}

	/* Do a final combine with existing on-disk groups */
	combine(state, true);

	osri = CQOSRelOpen(osrel);

	BeginStreamModify(NULL, osri, list_make1(state->acks), 0, REENTRANT_STREAM_INSERT);
	sis = (StreamInsertState *) osri->ri_FdwState;
	Assert(sis);

	os_targets = orig_targets = sis->queries;

	/*
	 * If nothing is reading from the output stream, close it immediately.
	 *
	 * We'll also handle output stream writes separately for SWs since
	 * they require the execution of an overlay plan.
	 */
	if (state->base.query->is_sw || os_targets == NULL)
	{
		EndStreamModify(NULL, osri);
		CQOSRelClose(osri);
		heap_close(osrel, RowExclusiveLock);
		sis = NULL;
		os_targets = NULL;
	}

	ri = CQMatRelOpen(matrel);

	estate->es_per_tuple_exprcontext = CreateStandaloneExprContext();

	if (state->output_stream_proj)
		state->output_stream_proj->pi_exprContext = estate->es_per_tuple_exprcontext;

	foreach_tuple(slot, state->combined)
	{
		TupleHashEntry entry;
		PhysicalTuple update = NULL;
		HeapTuple tup = NULL;
		HeapTuple os_tup;
		Datum os_values[4];
		bool os_nulls[4];
		int replaces = 0;
		ExprContext *econtext = estate->es_per_tuple_exprcontext;

		MemSet(os_nulls, false, sizeof(os_nulls));

		/* Only replace values for non-group attributes */
		MemSet(replace_all, true, size);
		for (i = 0; i < state->ngroupatts; i++)
			replace_all[state->groupatts[i] - 1] = false;

		/* Never replace pkey */
		replace_all[state->pk - 1] = false;

		slot_getallattrs(slot);

		if (state->existing)
		{
			entry = LookupTupleHashEntry(state->existing, slot, NULL);
			if (entry)
			{
				Assert(entry->additional);
				update = (PhysicalTuple) entry->additional;
			}
		}

		if (update && SHOULD_UPDATE(state))
		{
			ExecStoreTuple(update->tuple, state->prev_slot, InvalidBuffer, false);
			replaces = compare_slots(state->prev_slot, state->slot,
					state->pk, replace_all);

			if (replaces == 0)
				continue;

			if (os_targets)
				os_values[OLD_TUPLE] = project_overlay(state, econtext, update->tuple, &os_nulls[OLD_TUPLE]);

			/*
			 * The slot has the updated values, so store them in the updatable physical tuple
			 */
			tup = heap_modify_tuple(update->tuple, slot->tts_tupleDescriptor,
					slot->tts_values, slot->tts_isnull, replace_all);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelUpdate(ri, slot, estate);

			if (os_targets)
				os_values[NEW_TUPLE] = project_overlay(state, econtext, tup, &os_nulls[NEW_TUPLE]);

			ntups_updated++;
			nbytes_updated += HEAPTUPLESIZE + slot->tts_tuple->t_len;
		}
		else
		{
			/* No existing tuple found, so it's an INSERT. Also generate a primary key for it if necessary. */
			if (state->seq_pk)
				slot->tts_values[state->pk - 1] = nextval_internal(state->base.query->seqrelid, true);
			slot->tts_isnull[state->pk - 1] = false;
			tup = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecCQMatRelInsert(ri, slot, estate);

			if (os_targets)
			{
				os_nulls[OLD_TUPLE] = true;
				os_nulls[NEW_TUPLE] = false;
				os_values[OLD_TUPLE] = (Datum) NULL;
				os_values[NEW_TUPLE] = project_overlay(state, econtext, tup, &os_nulls[NEW_TUPLE]);
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
			TupleHashEntry e = LookupTupleHashEntry(state->deltas, slot, NULL);

			if (e != NULL)
			{
				PhysicalTuple pt = (PhysicalTuple) e->additional;
				os_values[DELTA_TUPLE] = heap_copy_tuple_as_datum(pt->tuple, state->desc);
				os_nulls[DELTA_TUPLE] = false;
				os_nulls[state->output_stream_arrival_ts - 1] = true;
				os_tup = heap_form_tuple(state->os_slot->tts_tupleDescriptor, os_values, os_nulls);
				ExecStoreTuple(os_tup, state->os_slot, InvalidBuffer, false);
				ExecStreamInsert(NULL, osri, state->os_slot, NULL);
			}
			else
			{
				elog(WARNING, "tuple not found in delta store");
			}
		}

		ResetPerTupleExprContext(estate);
	}

	if (sis)
	{
		EndStreamModify(NULL, osri);
		CQOSRelClose(osri);
		heap_close(osrel, RowExclusiveLock);
	}

	/*
	 * We handle SW output stream writes here so we can execute the
	 * overlay plan on the whole batch all at once in order to to
	 * compute output stream tuples.
	 *
	 * Note that this only happens if the output stream currently
	 * has any readers.
	 */
	if (orig_targets && state->sw)
		project_sw_overlay_into_ostream(state, matrel);

	tuplestore_clear(state->combined);

	StatsIncrementCQUpdate(ntups_updated, nbytes_updated);
	StatsIncrementCQWrite(ntups_inserted, nbytes_inserted);

	CQMatRelClose(ri);
	heap_close(matrel, RowExclusiveLock);

	FreeExecutorState(estate);
}

/*
 * sync_all
 */
static void
sync_all(ContExecutor *cont_exec)
{
	Bitmapset *tmp = bms_copy(cont_exec->all_queries);
	ContQueryCombinerState **states = (ContQueryCombinerState **) cont_exec->states;
	int id;
	TimestampTz start_time;
	long secs;
	int usecs;

	PushActiveSnapshot(GetTransactionSnapshot());

	while ((id = bms_first_member(tmp)) >= 0)
	{
		volatile bool error = false;
		ContQueryCombinerState *state = states[id];

		if (!state)
			continue;

		start_time = GetCurrentTimestamp();
		debug_query_string = state->base.query->name->relname;
		MyProcStatCQEntry = state->base.stats;

		PG_TRY();
		{
			if (state->pending_tuples > 0)
				sync_combine(state);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			error = true;
		}
		PG_END_TRY();

		if (error)
			ContExecutorAbortQuery(cont_exec);

		TimestampDifference(start_time, GetCurrentTimestamp(), &secs, &usecs);
		StatsIncrementCQExecMs(secs * 1000 + (usecs / 1000));

		state->pending_tuples = 0;
		state->existing = NULL;
		debug_query_string = NULL;
		MyProcStatCQEntry = NULL;

		/*
		 * Set acks to NIL here so we don't try to access them for future ticks
		 * that happen in a different transaction.
		 */
		state->acks = NIL;

		MemSet(state->group_hashes, 0, state->group_hashes_len);
		MemoryContextResetAndDeleteChildren(state->combine_cxt);
		MemoryContextResetAndDeleteChildren(ErrorContext);
	}

	if (ActiveSnapshotSet())
		PopActiveSnapshot();
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
	Relation overlayrel;
	EState *estate;
	ExprContext *context;
	Relation rel;
	Query *overlay;

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

	estate = CreateExecutorState();
	context = CreateStandaloneExprContext();
	overlayrel = heap_openrv(state->base.query->name, NoLock);
	overlay = get_view_query(overlayrel);
	heap_close(overlayrel, NoLock);

	state->output_stream_proj = build_projection(overlay->targetList, estate, context, NULL);
	state->proj_input_slot = MakeSingleTupleTableSlot(state->desc);
}

/*
 * init_lookup_query
 *
 * Initialize the groups lookup Query we'll use to generate the lookup plan.
 * The only thing that changes across planning it is the VALUES list, so we just
 * keep a reference to that and set it for each new planning.
 */
static void
init_lookup_query(ContQueryCombinerState *state)
{
	Query *result;
	SubLink *sub;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	TypeCacheEntry *tc;
	Param *param;
	OpExpr *op;

	tc = lookup_type_cache(state->hashfunc->funcresulttype, TYPECACHE_EQ_OPR);

	param = makeNode(Param);
	param->paramkind = PARAM_SUBLINK;
	param->paramid = 1;
	param->paramtype = state->hashfunc->funcresulttype;
	param->paramtypmod = -1;

	op = makeNode(OpExpr);
	op->opno = tc->eq_opr;
	op->opfuncid = get_opcode(op->opno);
	op->opresulttype = BOOLOID;
	op->args = list_make2(copyObject(state->hashfunc), param);

	rte = makeNode(RangeTblEntry);
	rte->eref = makeAlias("*VALUES*", list_make1(makeString("hashvalue")));
	rte->requiredPerms = ACL_SELECT;
	rte->rtekind = RTE_VALUES;
	rte->coltypmods = list_make1_oid(0);
	rte->inFromCl = true;
	rte->coltypes = list_make1_oid(state->hashfunc->funcresulttype);

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = 1;

	/*
	 * Now create a subquery of VALUES to join the matrel against, which
	 * will result in a retrieval of existing groups to update.
	 */
	result = makeNode(Query);
	result->commandType = CMD_SELECT;

	/*
	 * We know what the best lookup plan is, which is a JOIN against a VALUES scan.
	 * However, a VALUES scan with only a single tuple may be optimized by the planner
	 * into a Result plan selecting the tuple as constants. This optimization will only
	 * happen when the Query has multiple RTEs, so we just append a dummy RTE to prevent
	 * that optimizer from performing this optimization :/
	 *
	 * See prejointree.c:is_simple_values
	 */
	result->rtable = lappend(result->rtable, rte);
	result->rtable = lappend(result->rtable, rte);
	result->targetList = list_make1(makeTargetEntry((Expr *) makeVar(1, 1, state->hashfunc->funcresulttype, -1, 0, 0), 1, "hashvalue", false));
	result->jointree = makeFromExpr(list_make1(rtr), NULL);
	result->canSetTag = true;

	sub = makeNode(SubLink);
	sub->subLinkType = ANY_SUBLINK;
	sub->testexpr = (Node *) op;
	sub->operName = list_make1(makeString("="));
	sub->subselect = (Node *) result;

	state->lookup_rte = rte;
	state->lookup_query = (Node *) sub;
}

/*
 * init_query_state
 */
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

	matrel = try_relation_open(base->query->matrelid, AccessShareLock);
	pstmt = GetContPlan(base->query, Combiner);

	state->batch = tuplestore_begin_heap(true, true, continuous_query_combiner_work_mem);
	state->combined = tuplestore_begin_heap(false, true, continuous_query_combiner_work_mem);

	/* this also sets the state's desc field */
	prepare_combine_plan(state, pstmt);

	state->slot = MakeSingleTupleTableSlot(state->desc);
	state->delta_slot = MakeSingleTupleTableSlot(state->desc);
	state->prev_slot = MakeSingleTupleTableSlot(state->desc);
	state->groups_plan = NULL;

	/* this will grow dynamically when needed, but this is a good starting size */
	state->group_hashes_len = continuous_query_batch_size;
	state->group_hashes = palloc0(state->group_hashes_len * sizeof(int64));

	/* process/query level statistics */
	state->base.stats = ProcStatsInit(state->base.query->id, MyProcPid);

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
		{
			state->hashfunc = GetGroupHashIndexExpr(ri);
			state->hash_fcinfo = palloc0(sizeof(FunctionCallInfoData));
			state->hash_fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
			state->hash_fcinfo->flinfo->fn_mcxt = state->base.tmp_cxt;

			fmgr_info(state->hashfunc->funcid, state->hash_fcinfo->flinfo);
			fmgr_info_set_expr((Node *) state->hashfunc, state->hash_fcinfo->flinfo);

			state->hash_fcinfo->fncollation = state->hashfunc->funccollid;
			state->hash_fcinfo->nargs = list_length(state->hashfunc->args);

			init_lookup_query(state);
		}

		CQMatRelClose(ri);

		CompatExecTuplesHashPrepare(state->ngroupatts, state->groupops, &state->eq_funcs, &state->hash_funcs);
		state->existing = build_existing_hashtable(state, "CombinerExistingGroups");
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
 * read_batch
 */
static int
read_batch(ContExecutor *exec, ContQueryCombinerState *state, Oid query_id)
{
	ipc_tuple *itup;
	Size nbytes = 0;
	int ntups = 0;

	if (!exec->batch)
		return 0;

	while ((itup = ipc_tuple_reader_next(query_id)) != NULL)
	{
		if (!TupIsNull(state->slot))
			ExecClearTuple(state->slot);
		ExecStoreTuple(heap_copytuple(itup->tup), state->slot, InvalidBuffer, false);
		tuplestore_puttupleslot(state->batch, state->slot);

		set_group_hash(state, ntups, itup->hash);

		nbytes += itup->tup->t_len + HEAPTUPLESIZE;
		ntups++;
	}

	state->acks = exec->batch->sync_acks;
	ipc_tuple_reader_rewind();

	if (!TupIsNull(state->slot))
		ExecClearTuple(state->slot);

	StatsIncrementCQRead(ntups, nbytes);

	return ntups;
}

/*
 * need_sync
 */
static bool
need_sync(ContExecutor *exec, TimestampTz last_sync)
{
	if ((exec->batch && exec->batch->has_acks) || !continuous_query_commit_interval)
		return true;

	return TimestampDifferenceExceeds(last_sync, GetCurrentTimestamp(), continuous_query_commit_interval);
}

/*
 * get_min_tick_ms
 */
static uint64
get_min_tick_ms(void)
{
	Bitmapset *queries;
	uint64 min_tick_ms = 0;
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
	InitPipelineCatalog();

	queries = GetContViewIds();

	while ((id = bms_first_member(queries)) >= 0)
	{
		ContQuery *q = GetContViewForId(id);
		if (!q || !q->is_sw)
			continue;
		Assert(q->sw_step_ms);
		min_tick_ms = min_tick_ms ? Min(min_tick_ms, q->sw_step_ms) : q->sw_step_ms;
	}

	CommitTransactionCommand();

	return min_tick_ms;
}

/*
 * ContinuousQueryCombinerMain
 */
void
ContinuousQueryCombinerMain(void)
{
	ContExecutor *cont_exec = ContExecutorNew(&init_query_state);
	Oid query_id;
	TimestampTz first_seen = 0;
	bool do_commit = false;
	long total_pending = 0;
	uint64 min_tick_ms;

	min_tick_ms = get_min_tick_ms();

	/* Set the commit level */
	synchronous_commit = continuous_query_combiner_synchronous_commit;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (get_sigterm_flag())
			break;

		ContExecutorStartBatch(cont_exec, min_tick_ms);

		while ((query_id = ContExecutorStartNextQuery(cont_exec, min_tick_ms)) != InvalidOid)
		{
			int count = 0;
			ContQueryCombinerState *state = (ContQueryCombinerState *) cont_exec->curr_query;
			volatile bool error = false;

			PG_TRY();
			{
				TimestampTz start_time;
				long secs;
				int usecs;

				if (state == NULL)
					goto next;

				start_time = GetCurrentTimestamp();
				MemoryContextSwitchTo(state->base.tmp_cxt);

				count = read_batch(cont_exec, state, query_id);
				if (count)
				{
					state->pending_tuples += count;
					total_pending += count;

					combine(state, false);

					if (!first_seen)
						first_seen = GetCurrentTimestamp();
				}

				if (state->sw)
				{
					tick_sw_groups(state, NULL, false);
					min_tick_ms = min_tick_ms ? Min(min_tick_ms, state->base.query->sw_step_ms) :
							state->base.query->sw_step_ms;
				}

				MemoryContextResetAndDeleteChildren(state->base.tmp_cxt);

				TimestampDifference(start_time, GetCurrentTimestamp(), &secs, &usecs);
				StatsIncrementCQExecMs(secs * 1000 + (usecs / 1000));
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (ActiveSnapshotSet())
					PopActiveSnapshot();

				/*
				 * Modifying anything within a PG_CATCH block can have unpredictable behavior
				 * when optimization is enabled, so we do the remaining error handling later.
				 */
				error = true;
			}
			PG_END_TRY();

			if (error)
				StatsIncrementCQError(1);

next:
			ContExecutorEndQuery(cont_exec);

			/*
			 * Clear analyzer/planner context flags
			 */
			ClearPipelineContext();

			/*
			 * We wait to purge until we're done incrementing all stats, because this will
			 * free the stats object
			 */
			if (error)
				ContExecutorPurgeQuery(cont_exec);
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

		if (!state)
			continue;

		MemoryContextDelete(state->base.state_cxt);
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
	exec.curr_query_id = view->id;

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
#if (PG_VERSION_NUM < 110000)
		FmgrInfo *eq_funcs;
#else
		Oid *eq_funcs;
#endif
		FmgrInfo *hash_funcs;
		Relation rel;

		CompatExecTuplesHashPrepare(state->ngroupatts, state->groupops, &eq_funcs, &hash_funcs);
		existing = CompatBuildTupleHashTable(state->desc, state->ngroupatts, state->groupatts, eq_funcs, hash_funcs, 1000,
				sizeof(PhysicalTupleData), CurrentMemoryContext, CurrentMemoryContext, false);

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
 * combine_table
 */
PG_FUNCTION_INFO_V1(combine_table);
Datum
combine_table(PG_FUNCTION_ARGS)
{
	text *cv_name = PG_GETARG_TEXT_P(0);
	RangeVar *cv_rv = makeRangeVarFromNameList(textToQualifiedNameList(cv_name));
	text *relname = PG_GETARG_TEXT_P(1);
	RangeVar *rel_rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	ContQuery *cv = RangeVarGetContView(cv_rv);
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

	if (!equalTupleDescsWeak(RelationGetDescr(matrel), RelationGetDescr(srcrel), true))
		elog(ERROR, "schema of \"%s\" does not match the schema of \"%s\"",
				text_to_cstring(relname), quote_qualified_identifier(cv->matrel->schemaname, cv->matrel->relname));

	exec.cxt = CurrentMemoryContext;
	exec.curr_query_id = cv->id;
	exec.all_queries = bms_make_singleton(cv->id);

	base = palloc0(sizeof(ContQueryState));

	base->query_id = cv->id;
	base->query = cv;
	base->state_cxt = CurrentMemoryContext;
	base->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "pipeline_combine_table temp cxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	PipelineContextSetCombineTable();
	state = (ContQueryCombinerState *) init_query_state(&exec, base);
	ClearPipelineContext();

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
			uint64 hash =  slot_hash_group(state->slot, state->hashfunc, hashfcinfo);
			set_group_hash(state, state->pending_tuples, hash);
		}

		if (++state->pending_tuples < continuous_query_batch_size)
			continue;

		combine(state, false);
		sync_all(&exec);

		MemoryContextResetAndDeleteChildren(base->tmp_cxt);

		state->pending_tuples = 0;
	}

	if (state->pending_tuples)
	{
		combine(state, true);
		sync_all(&exec);
	}

	heap_endscan(scan);

	heap_close(srcrel, NoLock);
	heap_close(matrel, NoLock);

	PG_RETURN_BOOL(true);
}
