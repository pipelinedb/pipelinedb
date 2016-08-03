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
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/pzmq.h"
#include "pipeline/ipc/reader.h"
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
}

/*
 * GetStreamPaths
 */
void
GetStreamPaths(PlannerInfo *root, RelOptInfo *baserel, Oid streamid)
{
	ForeignPath *path;
	Cost startup_cost;
	Cost total_cost;
	double rows;

	if (!IsContQueryProcess())
	{
		PlannerInfo *parent = root;

		/* If the root query is continuous, we can read from streams */
		while (parent->parent_root != NULL)
			parent = parent->parent_root;

		if (!parent->parse->isContinuous)
		{
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a stream", get_rel_name(streamid)),
					 errhint("Streams can only be read by a continuous view's FROM clause.")));
		}
	}

	rows = Min(100, continuous_query_batch_size * 0.25);
	startup_cost = baserel->baserestrictcost.startup;
	total_cost = startup_cost + (cpu_tuple_cost * rows);

	path = create_foreignscan_path(root, baserel, rows, startup_cost, total_cost,
			NIL, NULL, NULL, NIL);
	add_path(baserel, (Path *) path);
}

/*
 * GetStreamScanPlan
 */
ForeignScan *
GetStreamScanPlan(PlannerInfo *root, RelOptInfo *baserel,
		Oid streamid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	StreamFdwInfo *sinfo = (StreamFdwInfo *) baserel->fdw_private;
	List *physical_tlist = build_physical_tlist(root, baserel);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

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
		namestrcpy(&(state->pi->outdesc->attrs[i++]->attname), strVal(v));
	}

	ExecAssignScanType(&node->ss, state->pi->outdesc);

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

	reset_record_type_cache();
	pgstat_increment_cq_read(ss->ntuples, ss->nbytes);
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

		inattr = indesc->attrs[i];
		nulls[outattno] = false;

		/* if the append-time value's type is different from the target type, try to coerce it */
		if (inattr->atttypid != outdesc->attrs[outattno]->atttypid)
		{
			Const *c = makeConst(inattr->atttypid, inattr->atttypmod, inattr->attcollation,
					inattr->attlen, v, false, inattr->attbyval);
			Node *n = coerce_to_target_type(NULL, (Node *) c, inattr->atttypid, outdesc->attrs[outattno]->atttypid,
					outdesc->attrs[outattno]->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

			/* if the coercion is possible, do it */
			if (n != NULL)
			{
				ExprState *estate = ExecInitExpr((Expr *) n, NULL);
				v = ExecEvalExpr(estate, pi->ecxt, &nulls[outattno], NULL);
			}
			else
			{
				/*
				 * Slow path, fall back to the original user input and try to
				 * coerce that to the target type
				 */
				v = coerce_raw_input(v, inattr->atttypid, outdesc->attrs[outattno]->atttypid);
			}
		}

		values[outattno] = v;
	}

	/* Assign arrival_timestamp to this tuple! */
	for (i = 0; i < outdesc->natts; i++)
	{
		if (pg_strcasecmp(NameStr(outdesc->attrs[i]->attname), ARRIVAL_TIMESTAMP) == 0)
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
	ipc_tuple *itup;
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
	sis->sync = false;
	sis->start_generation = 0;
	sis->db_meta = NULL;

	if (eflags & REENTRANT_STREAM_INSERT)
	{
		Assert(list_length(fdw_private) == 1 || list_length(fdw_private) == 2);

		sis->ack = NULL;
		acks = linitial(fdw_private);

		if (list_length(fdw_private) == 2)
		{
			Assert(IsContQueryWorkerProcess());
			sis->desc = lsecond(fdw_private);
		}
		else
		{
			Assert(!is_inferred_stream_relation(rel));
			sis->desc = RelationGetDescr(rel);
		}
	}
	else
	{
		if (is_inferred_stream_relation(rel))
		{
			void *incoming;

			Assert(fdw_private);
			incoming = linitial(fdw_private);

			if (IsA(incoming, List))
				sis->desc = ExecTypeFromTL(incoming, false);
			else
				sis->desc = (TupleDesc) incoming;
		}
		else
			sis->desc = RelationGetDescr(rel);

		if (synchronous_stream_insert)
		{
			sis->db_meta = GetContQueryDatabaseMetadata(MyDatabaseId);
			sis->start_generation = pg_atomic_read_u64(&sis->db_meta->generation);

			sis->ack = microbatch_ack_new();
			sis->sync = true;
		}
	}

	if (!bms_is_empty(queries))
	{
		sis->batch = microbatch_new(WorkerTuple, queries, sis->desc);

		if (sis->ack)
		{
			Assert(!acks);
			microbatch_add_ack(sis->batch, sis->ack);
		}
		else if (acks)
		{
			Assert(!sis->ack);
			Assert(!sis->sync);
			microbatch_add_acks(sis->batch, acks);
		}
	}

	result_info->ri_FdwState = sis;

	if (!IsContQueryProcess())
		pzmq_init();
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
		microbatch_send_to_worker(sis->batch);
		microbatch_add_tuple(sis->batch, tup, 0);
		sis->nbatches++;
	}

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
	Relation rel = result_info->ri_RelationDesc;

	if (bms_is_empty(sis->queries))
		return;

	if (!microbatch_is_empty(sis->batch))
		microbatch_send_to_worker(sis->batch);

	pgstat_increment_stream_insert(RelationGetRelid(rel), sis->ntups, sis->nbatches, sis->nbytes);
	microbatch_acks_check_and_exec(sis->batch->acks, microbatch_ack_increment_wtups, sis->ntups);

	if (sis->sync)
	{
		bool success = false;
		int64 generation = -1;

		Assert(sis->ack);
		Assert(sis->db_meta);

		while (true)
		{
			if (microbatch_ack_is_acked(sis->ack))
			{
				success = true;
				break;
			}

			generation = pg_atomic_read_u64(&sis->db_meta->generation);
			if (generation != sis->start_generation)
			{
				Assert(generation > sis->start_generation);
				break;
			}

			/* TODO(usmanm): exponential backoff? */
			pg_usleep(1000);
			CHECK_FOR_INTERRUPTS();
		}

		if (!success)
			ereport(WARNING,
					(errmsg("a background worker crashed while processing this batch"),
					errhint("Some of the tuples inserted in this batch might have been lost.")));

		microbatch_ack_destroy(sis->ack);
	}

	microbatch_destroy(sis->batch);

	/* This ensures that we wait till any data in our local ZMQ queues has been flushed! */
	if (!IsContQueryProcess())
		pzmq_pollout();
}
