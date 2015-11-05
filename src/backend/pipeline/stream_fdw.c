/*-------------------------------------------------------------------------
 *
 * stream_fdw.c
 *
 *	  Stream FDW implementation
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/stream_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_stream_fn.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "pipeline/cont_adhoc.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/dsm_cqueue.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/stream_fdw.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

typedef struct StreamFdwInfo
{
	List *colnames;
} StreamFdwInfo;

typedef struct StreamInsertState
{
	Bitmapset *targets;

	long count;
	long bytes;

	InsertBatch *batch;
	InsertBatchAck *ack;

	TupleDesc desc;
	bytea *packed_desc;

	dsm_cqueue *worker_queue;
	AdhocInsertState *adhoc_state;
} StreamInsertState;

/*
 * stream_fdw_handler
 */
Datum
stream_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Stream SELECTS (only used by continuous query procs) */
	routine->GetForeignRelSize = GetStreamSize;
	routine->GetForeignPaths = GetStreamPaths;
	routine->GetForeignPlan = GetStreamScanPlan;
	routine->BeginForeignScan = BeginStreamScan;
	routine->IterateForeignScan = IterateStreamScan;
	routine->ReScanForeignScan = ReScanStreamScan;
	routine->EndForeignScan = EndStreamScan;

	/* Streams INSERTs */
	routine->PlanForeignModify = PlanStreamModify;
	routine->BeginForeignModify = BeginStreamModify;
	routine->ExecForeignInsert = ExecStreamInsert;
	routine->EndForeignModify = EndStreamModify;

	routine->ExplainForeignScan = NULL;
	routine->ExplainForeignModify = NULL;

	PG_RETURN_POINTER(routine);
}

/*
 * GetStreamSize
 */
void
GetStreamSize(PlannerInfo *root, RelOptInfo *rel, Oid streamid)
{
	StreamFdwInfo *sinfo = (StreamFdwInfo *) palloc0(sizeof(StreamFdwInfo));
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

	sinfo->colnames = rte->eref->colnames;
	rel->fdw_private = (void *) sinfo;
}

/*
 * GetStreamPaths
 */
void
GetStreamPaths(PlannerInfo *root, RelOptInfo *baserel, Oid streamid)
{
	ForeignPath *path;

	if (!IsContQueryProcess())
	{
		Relation rel;
		PlannerInfo *parent = root;

		/* If the root query is continuous, we can read from streams */
		while (parent->parent_root != NULL)
			parent = parent->parent_root;

		if (!parent->parse->isContinuous)
		{
			rel = heap_open(streamid, NoLock);

			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a stream", RelationGetRelationName(rel)),
					 errhint("Streams can only be read by a continuous view's FROM clause.")));
		}
	}

	path = create_foreignscan_path(root, baserel, 0, 0, 0, NIL, NULL, NIL);
	add_path(baserel, (Path *) path);
}

/*
 * GetStreamScanPlan
 */
ForeignScan *
GetStreamScanPlan(PlannerInfo *root, RelOptInfo *baserel,
		Oid streamid, ForeignPath *best_path, List *tlist, List *scan_clauses)
{
	StreamFdwInfo *sinfo = (StreamFdwInfo *) baserel->fdw_private;
	List *physical_tlist = build_physical_tlist(root, baserel);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist, scan_clauses, baserel->relid,
							NIL, list_make2(sinfo->colnames, physical_tlist));
}

/*
 * BeginStreamScan
 */
void
BeginStreamScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	StreamScanState *state;
	ListCell *lc;
	List *colnames = (List *) linitial(plan->fdw_private);
	List *physical_tlist = (List *) lsecond(plan->fdw_private);
	int i = 0;

	state = makeNode(StreamScanState);
	state->ss.ps.plan = (Plan *) node;

	state->pi = palloc(sizeof(StreamProjectionInfo));
	state->pi->ctxt = AllocSetContextCreate(CurrentMemoryContext,
													 "ExecProjectContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	state->pi->econtext = CreateStandaloneExprContext();
	state->pi->resultdesc = ExecTypeFromTL(physical_tlist, false);
	state->pi->raweventdesc = NULL;

	Assert(state->pi->resultdesc->natts == list_length(colnames));

	foreach(lc, colnames)
	{
		Value *v = (Value *) lfirst(lc);
		namestrcpy(&(state->pi->resultdesc->attrs[i++]->attname), strVal(v));
	}

	ExecInitResultTupleSlot(node->ss.ps.state, &state->ss.ps);
	ExecInitScanTupleSlot(node->ss.ps.state, &state->ss);

	ExecAssignScanType(&node->ss, state->pi->resultdesc);

	/*
	 * Override result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&node->ss.ps);
	ExecAssignScanProjectionInfo(&node->ss);

	node->fdw_state = (void *) state;
}

/*
 * ReScanStreamScan
 */
void
ReScanStreamScan(ForeignScanState *node)
{

}

/*
 * EndStreamScan
 */
void
EndStreamScan(ForeignScanState *node)
{
	StreamScanState *ss = (StreamScanState *) node->fdw_state;

	MemoryContextReset(ss->pi->ctxt);

	/* the next event's descriptor will be used if this is NULL */
	ss->pi->raweventdesc = NULL;

	reset_record_type_cache();
}

/*
 * Maps the positions of attribute names in the first TupleDesc to the corresponding
 * attribute names in the second TupleDesc
 */
static int *
map_field_positions(TupleDesc evdesc, TupleDesc desc)
{
	int i;
	int *result = palloc(sizeof(int) * evdesc->natts);

	for (i = 0; i < evdesc->natts; i++)
	{
		int j;

		result[i] = -1;
		for (j = 0; j < desc->natts; j++)
		{
			if (pg_strcasecmp(NameStr(evdesc->attrs[i]->attname), NameStr(desc->attrs[j]->attname)) == 0)
			{
				result[i] = j;
				break;
			}
		}
	}

	return result;
}

/*
 * Initializes the given StreamProjectionInfo for the given
 * Tuple. This allows us to cache descriptor-level information, which
 * may only change after many event projections.
 */
static void
init_proj_info(StreamProjectionInfo *pi, StreamTupleState *sts)
{
	MemoryContext old;

	old = MemoryContextSwitchTo(pi->ctxt);

	pi->eventdesc = UnpackTupleDesc(sts->desc);
	pi->attrmap = map_field_positions(pi->eventdesc, pi->resultdesc);
	pi->curslot = MakeSingleTupleTableSlot(pi->eventdesc);

	pi->raweventdesc = palloc0(VARSIZE(sts->desc) + VARHDRSZ);
	memcpy(pi->raweventdesc, sts->desc, VARSIZE(sts->desc) + VARHDRSZ);

	/*
	 * Load RECORDOID tuple descriptors in the cache.
	 */
	if (sts->num_record_descs)
	{
		int i;

		for (i = 0; i < sts->num_record_descs; i++)
		{
			RecordTupleDesc *rdesc = &sts->record_descs[i];
			set_record_type_typemod(rdesc->typmod, UnpackTupleDesc(rdesc->desc));
		}
	}

	MemoryContextSwitchTo(old);
}

/*
 * Given a value, convert it to its original user input representation,
 * then attempt to read it in as the target output type
 */
static Datum
coerce_raw_input(Datum value, Oid intype, Oid outtype)
{
	char *orig;
	Oid outfn;
	Oid infn;
	Oid ioparam;
	bool isvlen;
	Datum result;

	getTypeOutputInfo(intype, &outfn, &isvlen);
	orig = OidOutputFunctionCall(outfn, value);

	getTypeInputInfo(outtype, &infn, &ioparam);
	result = OidInputFunctionCall(infn, orig, ioparam, -1);

	return result;
}

static HeapTuple
exec_stream_project(StreamTupleState *sts, StreamScanState *node)
{
	HeapTuple decoded;
	MemoryContext oldcontext;
	Datum *values;
	bool *nulls;
	int i;
	StreamProjectionInfo *pi = node->pi;
	TupleDesc evdesc = pi->eventdesc;
	TupleDesc desc = pi->resultdesc;

	values = palloc0(sizeof(Datum) * desc->natts);
	nulls = palloc0(sizeof(bool) * desc->natts);

	/* assume every element in the output tuple is null until we actually see values */
	MemSet(nulls, true, desc->natts);

	ExecStoreTuple(sts->tup, pi->curslot, InvalidBuffer, false);

	/*
	 * For each field in the event, place it in the corresponding field in the
	 * output tuple, coercing types if necessary.
	 */
	for (i = 0; i < evdesc->natts; i++)
	{
		Datum v;
		bool isnull;
		int outatt = pi->attrmap[i];
		Form_pg_attribute evatt;

		if (outatt < 0)
			continue;

		/* this is the append-time value */
		v = slot_getattr(pi->curslot, i + 1, &isnull);

		if (isnull)
			continue;

		evatt = evdesc->attrs[i];
		nulls[outatt] = false;

		/* if the append-time value's type is different from the target type, try to coerce it */
		if (evatt->atttypid != desc->attrs[outatt]->atttypid)
		{
			Const *c = makeConst(evatt->atttypid, evatt->atttypmod, evatt->attcollation,
					evatt->attlen, v, false, evatt->attbyval);
			Node *n = coerce_to_target_type(NULL, (Node *) c, evatt->atttypid, desc->attrs[outatt]->atttypid,
					desc->attrs[outatt]->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

			/* if the coercion is possible, do it */
			if (n != NULL)
			{
				ExprState *estate = ExecInitExpr((Expr *) n, NULL);
				v = ExecEvalExpr(estate, pi->econtext, &nulls[outatt], NULL);
			}
			else
			{
				/*
				 * Slow path, fall back to the original user input and try to
				 * coerce that to the target type
				 */
				v = coerce_raw_input(v, evatt->atttypid,
						desc->attrs[outatt]->atttypid);
			}
		}

		values[outatt] = v;
	}

	/* If arrival_timestamp is requested, pull value from StreamEvent and
	 * update the HeapTuple. */
	for (i = 0; i < desc->natts; i++)
	{
		if (pg_strcasecmp(NameStr(desc->attrs[i]->attname), ARRIVAL_TIMESTAMP) == 0)
		{
			values[i] = TimestampGetDatum(sts->arrival_time);
			nulls[i] = false;
			break;
		}
	}

	oldcontext = MemoryContextSwitchTo(ContQueryBatchContext);

	/* our result tuple needs to live for the duration of this query execution */
	decoded = heap_form_tuple(desc, values, nulls);

	MemoryContextSwitchTo(oldcontext);

	return decoded;
}

/*
 * PlanStreamModify
 */
List *
PlanStreamModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	return list_make1(root->parse->targetList);
}


/*
 * IterateStreamScan
 */
TupleTableSlot *
IterateStreamScan(ForeignScanState *node)
{
	StreamTupleState *sts;
	int len;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	StreamScanState *state = (StreamScanState *) node->fdw_state;

	HeapTuple tup;
	bytea *piraw;
	bytea *tupraw;

	if (state->cont_executor)
		sts = (StreamTupleState *) ContExecutorYieldItem(state->cont_executor, &len);
	else if (state->adhoc_executor)
		sts = AdhocExecutorYieldItem(state->adhoc_executor, &len);
	else
		elog(ERROR, "streams can only be read from worker or adhoc processes");

	if (sts == NULL)
		return NULL;

	IncrementCQRead(1, len);

	/*
	 * Check if the incoming event descriptor is different from the one we're
	 * currently using before fully unpacking it.
	 */
	piraw = state->pi->raweventdesc;
	tupraw = sts->desc;

	if (piraw == NULL || VARSIZE(piraw) != VARSIZE(tupraw) ||
			memcmp(VARDATA(piraw), VARDATA(tupraw), VARSIZE(piraw)))
		init_proj_info(state->pi, sts);

	tup = exec_stream_project(sts, state);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	return slot;
}

/*
 * BeginStreamModify
 */
void
BeginStreamModify(ModifyTableState *mtstate, ResultRelInfo *result_info,
						   List *fdw_private, int subplan_index, int eflags)
{
	Relation stream = result_info->ri_RelationDesc;
	Oid streamid = RelationGetRelid(stream);
	StreamInsertState *sis = palloc0(sizeof(StreamInsertState));
	Bitmapset *all_targets = GetStreamReaders(streamid);
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();
	Bitmapset *worker_targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = continuous_queries_adhoc_enabled ?
			bms_difference(all_targets, worker_targets) : NULL;
	InsertBatchAck *ack = NULL;
	InsertBatch *batch = NULL;
	List *insert_tl;

	Assert(fdw_private);
	insert_tl = linitial(fdw_private);

	if (!bms_is_empty(worker_targets))
	{
		if (synchronous_stream_insert)
		{
			batch = InsertBatchCreate();
			ack = palloc0(sizeof(InsertBatchAck));

			ack->batch_id = batch->id;
			ack->batch = batch;
		}

		sis->worker_queue = GetWorkerQueue();
		Assert(sis->worker_queue);
	}

	if (!bms_is_empty(adhoc_targets))
		sis->adhoc_state = AdhocInsertStateCreate(adhoc_targets);

	sis->targets = worker_targets;
	sis->ack = ack;
	sis->batch = batch;
	sis->count = 0;
	sis->bytes = 0;
	sis->desc = is_inferred_stream_relation(stream) ? ExecTypeFromTL(insert_tl, false) : RelationGetDescr(stream);
	sis->packed_desc = PackTupleDesc(sis->desc);

	result_info->ri_FdwState = sis;
}

/*
 * ExecStreamInsert
 */
TupleTableSlot *
ExecStreamInsert(EState *estate, ResultRelInfo *result_info,
						  TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	StreamInsertState *sis = (StreamInsertState *) result_info->ri_FdwState;
	HeapTuple tup = ExecMaterializeSlot(slot);
	StreamTupleState *sts;
	int len;

	sts = StreamTupleStateCreate(tup, sis->desc, sis->packed_desc, sis->targets, sis->ack, &len);

	if (sis->worker_queue)
	{
		/*
		 * If we've written a batch to a worker process, start writing to
		 * the next worker process.
		 */
		if (sis->count && (sis->count % continuous_query_batch_size == 0))
		{
			dsm_cqueue_unlock(sis->worker_queue);
			sis->worker_queue = GetWorkerQueue();
		}

		dsm_cqueue_push_nolock(sis->worker_queue, sts, len);
	}

	if (sis->adhoc_state)
		AdhocInsertStateSend(sis->adhoc_state, sts, len);

	pfree(sts);

	sis->count++;
	sis->bytes += tup->t_len + HEAPTUPLESIZE;

	return slot;
}

/*
 * EndStreamModify
 */
void
EndStreamModify(EState *estate, ResultRelInfo *result_info)
{
	StreamInsertState *sis = (StreamInsertState *) result_info->ri_FdwState;

	stream_stat_report(RelationGetRelid(result_info->ri_RelationDesc), sis->count, 1, sis->bytes);

	if (sis->worker_queue)
	{
		dsm_cqueue_unlock(sis->worker_queue);
		if (synchronous_stream_insert)
			InsertBatchWaitAndRemove(sis->batch, sis->count);
	}

	if (sis->adhoc_state)
		AdhocInsertStateDestroy(sis->adhoc_state);
}
