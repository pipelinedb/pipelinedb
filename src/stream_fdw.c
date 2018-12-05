/*-------------------------------------------------------------------------
 *
 * stream_fdw.c
 *
 *	  Stream FDW implementation
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "analyzer.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "compat.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "pipeline_stream.h"
#include "scheduler.h"
#include "pzmq.h"
#include "reader.h"
#include "miscutils.h"
#include "stream_fdw.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/varlena.h"

int stream_insert_level;

typedef struct StreamFdwInfo
{
	List *colnames;
} StreamFdwInfo;

struct StreamProjectionInfo {
	/*
	 * Temporary context to use during stream projections,
	 * reset after each stream scan batch
	 */
	MemoryContext mcxt;

	/* expression context for evaluating stream event cast expressions */
	ExprContext *ecxt;

	/* slot to store the current stream event in */
	TupleTableSlot *slot;

	/*
	 * Mapping from event attribute to result attribute position,
	 * may be cached across projections
	 */
	int *attrmap;

	/*
	 * Descriptor for the incoming tuples, will change between micro batches. We need to
	 * reset the projection info every time this changes
	 */
	TupleDesc indesc;

	/* Descriptor for the tuples being output by this scan */
	TupleDesc outdesc;
};

/*
 * stream_fdw_handler
 */
PG_FUNCTION_INFO_V1(stream_fdw_handler);
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
GetStreamSize(PlannerInfo *root, RelOptInfo *baserel, Oid streamid)
{
	StreamFdwInfo *sinfo = (StreamFdwInfo *) palloc0(sizeof(StreamFdwInfo));
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	sinfo->colnames = rte->eref->colnames;
	baserel->fdw_private = (void *) sinfo;

	/* We'll have at most continuous_query_batch_size stream rows per plan execution */
	baserel->rows = (double) continuous_query_batch_size;
}

/*
 * GetStreamPaths
 */
void
GetStreamPaths(PlannerInfo *root, RelOptInfo *baserel, Oid relid)
{
	ForeignPath *path;
	Cost startup_cost;
	Cost total_cost;
	double rows;

	if (!IsContQueryProcess() && !PipelineContextIsContPlan())
	{
		PlannerInfo *parent = root;

		/* If the root query is continuous, we can read from streams */
		while (parent->parent_root != NULL)
			parent = parent->parent_root;

		if (!PipelineContextIsCombineTable() && (!(QueryIsContinuous(parent->parse) && !IsContQueryProcess()) ||
				!PipelineContextIsDDL()))
		{
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a stream", get_rel_name(relid)),
					 errhint("Streams can only be read by a continuous view's FROM clause.")));
		}
	}

	rows = Min(100, continuous_query_batch_size * 0.25);
	startup_cost = baserel->baserestrictcost.startup;
	total_cost = startup_cost + (cpu_tuple_cost * rows);

	path = create_foreignscan_path(root, baserel, NULL, rows, startup_cost, total_cost,
			NIL, NULL, NULL, NIL);
	add_path(baserel, (Path *) path);
}

/*
 * GetStreamScanPlan
 */
ForeignScan *
GetStreamScanPlan(PlannerInfo *root, RelOptInfo *baserel,
		Oid relid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	StreamFdwInfo *sinfo = (StreamFdwInfo *) baserel->fdw_private;
	List *physical_tlist = build_physical_tlist(root, baserel);
	RangeTblEntry *rte = NULL;
	int i;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	for (i = 1; i <= root->simple_rel_array_size; i++)
	{
		rte = root->simple_rte_array[i];
		if (rte && rte->relid == relid)
			break;
	}

	if (!rte || rte->relid != relid)
		elog(ERROR, "stream RTE missing");

	return make_foreignscan(tlist, scan_clauses, baserel->relid,
							NIL, list_make2(sinfo->colnames, physical_tlist), NIL, NIL, outer_plan);
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

	state = palloc0(sizeof(StreamScanState));

	state->pi = palloc(sizeof(StreamProjectionInfo));
	state->pi->mcxt = AllocSetContextCreate(CurrentMemoryContext,
			"ExecProjectContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	state->pi->ecxt = CreateStandaloneExprContext();
	state->pi->outdesc = ExecTypeFromTL(physical_tlist, false);
	state->pi->indesc = NULL;

	Assert(state->pi->outdesc->natts == list_length(colnames));

	foreach(lc, colnames)
	{
		Value *v = (Value *) lfirst(lc);
		namestrcpy(&(TupleDescAttr(state->pi->outdesc, i++)->attname), strVal(v));
	}

	/*
	 * Override result tuple type and projection info.
	 */
	ComaptExecAssignResultTypeFromTL(&node->ss.ps);
	ExecAssignScanProjectionInfo(&node->ss);

	node->fdw_state = (void *) state;
}

/*
 * ReScanStreamScan
 */
void
ReScanStreamScan(ForeignScanState *node)
{
	/* Ephemeral data anyone? x_x */
}

/*
 * EndStreamScan
 */
void
EndStreamScan(ForeignScanState *node)
{
	StreamScanState *ss = (StreamScanState *) node->fdw_state;

	MemoryContextReset(ss->pi->mcxt);

	/* the next event's descriptor will be used if this is NULL */
	ss->pi->indesc = NULL;

	StatsIncrementCQRead(ss->ntuples, ss->nbytes);
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
			if (pg_strcasecmp(NameStr(TupleDescAttr(evdesc, i)->attname), NameStr(TupleDescAttr(desc, j)->attname)) == 0)
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
init_proj_info(StreamProjectionInfo *pi, ipc_tuple *itup)
{
	MemoryContext old;
	ListCell *lc;

	old = MemoryContextSwitchTo(pi->mcxt);

	pi->indesc = itup->desc;
	pi->attrmap = map_field_positions(pi->indesc, pi->outdesc);
	pi->slot = MakeSingleTupleTableSlot(pi->indesc);

	/*
	 * Load RECORDOID tuple descriptors in the cache.
	 */
	foreach(lc, itup->record_descs)
	{
		/* TODO */
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
exec_stream_project(StreamScanState *node, ipc_tuple *itup)
{
	HeapTuple decoded;
	MemoryContext old;
	Datum *values;
	bool *nulls;
	int i;
	StreamProjectionInfo *pi = node->pi;
	TupleDesc indesc = pi->indesc;
	TupleDesc outdesc = pi->outdesc;

	values = palloc0(sizeof(Datum) * outdesc->natts);
	nulls = palloc0(sizeof(bool) * outdesc->natts);

	/* assume every element in the output tuple is null until we actually see values */
	MemSet(nulls, true, outdesc->natts);

	ExecStoreTuple(itup->tup, pi->slot, InvalidBuffer, false);

	/*
	 * For each field in the event, place it in the corresponding field in the
	 * output tuple, coercing types if necessary.
	 */
	for (i = 0; i < indesc->natts; i++)
	{
		Datum v;
		bool isnull;
		int outattno = pi->attrmap[i];
		Form_pg_attribute inattr;

		if (outattno < 0)
			continue;

		/* this is the append-time value */
		v = slot_getattr(pi->slot, i + 1, &isnull);

		if (isnull)
			continue;

		inattr = TupleDescAttr(indesc, i);
		nulls[outattno] = false;

		/* if the append-time value's type is different from the target type, try to coerce it */
		if (inattr->atttypid != TupleDescAttr(outdesc, outattno)->atttypid)
		{
			Const *c = makeConst(inattr->atttypid, inattr->atttypmod, inattr->attcollation,
					inattr->attlen, v, false, inattr->attbyval);
			Node *n = coerce_to_target_type(NULL, (Node *) c, inattr->atttypid, TupleDescAttr(outdesc, outattno)->atttypid,
					TupleDescAttr(outdesc, outattno)->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

			/* if the coercion is possible, do it */
			if (n != NULL)
			{
				ExprState *estate = ExecInitExpr((Expr *) n, NULL);
				v = ExecEvalExpr(estate, pi->ecxt, &nulls[outattno]);
			}
			else
			{
				/*
				 * Slow path, fall back to the original user input and try to
				 * coerce that to the target type
				 */
				v = coerce_raw_input(v, inattr->atttypid, TupleDescAttr(outdesc, outattno)->atttypid);
			}
		}

		values[outattno] = v;
	}

	/* Assign arrival_timestamp to this tuple if it hasn't been explicitly provided */
	for (i = 0; i < outdesc->natts; i++)
	{
		if (nulls[i] && pg_strcasecmp(NameStr(TupleDescAttr(outdesc, i)->attname), ARRIVAL_TIMESTAMP) == 0)
		{
			values[i] = TimestampGetDatum(GetCurrentTimestamp());
			nulls[i] = false;
			break;
		}
	}

	old = MemoryContextSwitchTo(ContQueryBatchContext);

	/* our result tuple needs to live for the duration of this query execution */
	decoded = heap_form_tuple(outdesc, values, nulls);

	MemoryContextSwitchTo(old);

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
	ipc_tuple *itup = NULL;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	StreamScanState *state = (StreamScanState *) node->fdw_state;
	HeapTuple tup;

	itup = (ipc_tuple *) ipc_tuple_reader_next(state->cont_executor->curr_query_id);

	if (itup == NULL)
		return NULL;

	state->ntuples++;
	state->nbytes += itup->tup->t_len + HEAPTUPLESIZE;

	if (state->pi->indesc != itup->desc)
		init_proj_info(state->pi, itup);

	tup = exec_stream_project(state, itup);
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
	Relation rel = result_info->ri_RelationDesc;
	StreamInsertState *sis = palloc0(sizeof(StreamInsertState));
	Bitmapset *queries = GetLocalStreamReaders(RelationGetRelid(rel));
	List *acks = NIL;

	sis->flags = eflags;
	sis->queries = queries;
	sis->nbatches = 1;
	sis->start_generation = 0;
	sis->db_meta = NULL;
	sis->desc = RelationGetDescr(rel);

	result_info->ri_FdwState = sis;

	if (bms_is_empty(queries))
		return;

	if (eflags & REENTRANT_STREAM_INSERT)
	{
		Assert(list_length(fdw_private) == 1 || list_length(fdw_private) == 2);

		sis->ack = NULL;
		acks = linitial(fdw_private);
	}
	else
	{
		sis->db_meta = GetMyContQueryDatabaseMetadata();
		sis->start_generation = pg_atomic_read_u64(&sis->db_meta->generation);
		if (stream_insert_level == STREAM_INSERT_ASYNCHRONOUS)
			sis->ack = NULL;
		else
			sis->ack = microbatch_ack_new(stream_insert_level);
	}

	sis->batch = microbatch_new(WorkerTuple, queries, sis->desc);

	if (sis->ack)
	{
		Assert(!acks);
		microbatch_add_ack(sis->batch, sis->ack);
	}
	else if (acks)
	{
		Assert(!sis->ack);
		microbatch_add_acks(sis->batch, acks);
	}

	if (!IsContQueryProcess())
		microbatch_ipc_init();
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

	if (bms_is_empty(sis->queries))
		return slot;

	if (!microbatch_add_tuple(sis->batch, tup, 0))
	{
		microbatch_send_to_worker(sis->batch, -1);
		microbatch_add_tuple(sis->batch, tup, 0);
		sis->nbatches++;
	}

	sis->nbytes += HEAPTUPLESIZE + tup->t_len;
	sis->ntups++;

	return slot;
}

/*
 * EndStreamModify
 */
void
EndStreamModify(EState *estate, ResultRelInfo *result_info)
{
	StreamInsertState *sis = (StreamInsertState *) result_info->ri_FdwState;

	if (bms_is_empty(sis->queries))
		return;

	if (!microbatch_is_empty(sis->batch))
		microbatch_send_to_worker(sis->batch, -1);

	StatsIncrementStreamInsert(RelationGetRelid(result_info->ri_RelationDesc), sis->ntups, sis->nbatches, sis->nbytes);
	microbatch_acks_check_and_exec(sis->batch->acks, microbatch_ack_increment_wtups, sis->ntups);

	if (sis->ack)
	{
		bool success = microbatch_ack_wait(sis->ack, sis->db_meta, sis->start_generation);

		if (!success)
			ereport(WARNING,
					(errmsg("a background worker crashed while processing this batch"),
					errhint("Some of the tuples inserted in this batch might have been lost.")));

		microbatch_ack_free(sis->ack);
	}

	microbatch_destroy(sis->batch);
}

PG_FUNCTION_INFO_V1(insert_into_stream);
Datum
insert_into_stream(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger *trig = trigdata->tg_trigger;
	HeapTuple tup;
	List *fdw_private;
	int i;
	ResultRelInfo rinfo;

	if (trig->tgnargs < 1)
		elog(ERROR, "pipeline_stream_insert: must be provided a stream name");

	/* make sure it's called as a trigger */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called as trigger")));

	/* and that it's called on update or insert */
	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && !TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called on insert or update")));

	/* and that it's called for each row */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called for each row")));

	/* and that it's called after insert or update */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called after insert or update")));

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		tup = trigdata->tg_newtuple;
	else
		tup = trigdata->tg_trigtuple;

	fdw_private = list_make1(RelationGetDescr(trigdata->tg_relation));

	MemSet(&rinfo, 0, sizeof(ResultRelInfo));
	rinfo.ri_RangeTableIndex = 1; /* dummy */
	rinfo.ri_TrigDesc = NULL;

	for (i = 0; i < trig->tgnargs; i++)
	{
		RangeVar *stream;
		Relation rel;
		StreamInsertState *sis;

		stream = makeRangeVarFromNameList(textToQualifiedNameList(cstring_to_text(trig->tgargs[i])));
		rel = heap_openrv(stream, AccessShareLock);

		rinfo.ri_RelationDesc = rel;

		BeginStreamModify(NULL, &rinfo, fdw_private, 0, 0);
		sis = (StreamInsertState *) rinfo.ri_FdwState;
		Assert(sis);

		if (sis->queries)
		{
			TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, &rinfo, slot, NULL);
			ExecClearTuple(slot);

			ExecDropSingleTupleTableSlot(slot);
		}

		EndStreamModify(NULL, &rinfo);
		heap_close(rel, AccessShareLock);
	}

	return PointerGetDatum(tup);
}
