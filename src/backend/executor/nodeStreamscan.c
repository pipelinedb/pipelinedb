/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 * Support for scanning the stream buffer
 *
 * src/backend/executor/nodeStreamscan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/nodeStreamscan.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "catalog/pipeline_stream_fn.h"
#include "pipeline/tuplebuf.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


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
			if (strcmp(NameStr(evdesc->attrs[i]->attname), NameStr(desc->attrs[j]->attname)) == 0)
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
init_proj_info(StreamProjectionInfo *pi, Tuple *tuple)
{
	MemoryContext old;

	MemoryContextReset(pi->ctxt);

	old = MemoryContextSwitchTo(pi->ctxt);

	pi->eventdesc = UnpackTupleDesc(tuple->desc);
	pi->attrmap = map_field_positions(pi->eventdesc, pi->resultdesc);
	pi->curslot = MakeSingleTupleTableSlot(pi->eventdesc);

	MemoryContextSwitchTo(old);
}

static TupleTableSlot *
StreamScanNext(StreamScanState *node)
{
	TupleBufferSlot *tbs;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple tup;
	MemoryContext old;

	/*
	 * The TupleBuffer needs this slot until it gets unpinned, which we don't
	 * know when will happen so we need to keep it around for a full CQ execution.
	 */
	old = MemoryContextSwitchTo(CQExecutionContext);
	tbs = TupleBufferPinNextSlot(node->reader);
	MemoryContextSwitchTo(old);

	if (tbs == NULL)
		return NULL;

	/* update the projection info if the event descriptor has changed */
	init_proj_info(node->pi, tbs->tuple);

	tup = ExecStreamProject(tbs->tuple, node);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	/*
	 * We don't necessarily know when parent nodes will be done with this
	 * event, so only unpin it if we're configured to do so.
	 */
	if (node->unpin)
		TupleBufferUnpinSlot(node->reader, tbs);
	else
		node->pinned = lappend(node->pinned, tbs);

	return slot;
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

/*
 * ExecStreamProject
 *
 * Project a stream event onto a physical tuple
 */
HeapTuple
ExecStreamProject(Tuple *event, StreamScanState *node)
{
	HeapTuple decoded;
	MemoryContext oldcontext;
	Datum *values;
	bool *nulls;
	int i;
	StreamProjectionInfo *pi = node->pi;
	TupleDesc evdesc = pi->eventdesc;
	TupleDesc desc = pi->resultdesc;

	oldcontext = MemoryContextSwitchTo(pi->ctxt);

	values = palloc0(sizeof(Datum) * desc->natts);
	nulls = palloc0(sizeof(bool) * desc->natts);

	/* assume every element in the output tuple is null until we actually see values */
	MemSet(nulls, true, desc->natts);

	ExecStoreTuple(event->heaptup, pi->curslot, InvalidBuffer, false);

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
			Node *n = coerce_to_target_type(NULL, (Node *) copyObject(c), evatt->atttypid, desc->attrs[outatt]->atttypid,
					desc->attrs[outatt]->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

			/* if the coercion is possible, do it */
			if (n != NULL)
			{
				/*
				 * TODO(derekjn) an easy optimization here would be to cache the result
				 * of this conversion to avoid extraneous function calls by another reader
				 */
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
			values[i] = TimestampGetDatum(event->arrivaltime);
			nulls[i] = false;
			break;
		}
	}

	MemoryContextSwitchTo(CQExecutionContext);

	/* our result tuple needs to live for the duration of this query execution */
	decoded = heap_form_tuple(desc, values, nulls);

	MemoryContextSwitchTo(oldcontext);

	return decoded;
}

StreamScanState *
ExecInitStreamScan(StreamScan *node, EState *estate, int eflags)
{
	StreamScanState *state;

	state = makeNode(StreamScanState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;

	state->pi = palloc(sizeof(StreamProjectionInfo));
	state->pi->ctxt = AllocSetContextCreate(CurrentMemoryContext,
													 "ExecProjectContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	state->pi->econtext = CreateStandaloneExprContext();
	state->pi->resultdesc = node->desc;
	state->unpin = true;
	state->pinned = NIL;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &state->ss.ps);

	/*
	 * initialize child expressions
	 */
	state->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist, &state->ss.ps);
	state->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual, &state->ss.ps);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->ss.ps);
	ExecInitScanTupleSlot(estate, &state->ss);

	state->ss.ps.ps_TupFromTlist = false;

	ExecAssignScanType(&state->ss, node->desc);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);

	state->reader = TupleBufferOpenReader(WorkerTupleBuffer, node->cqid, MyWorkerId, NUM_WORKERS(GetCQProcEntry(node->cqid)));

	return state;
}


TupleTableSlot *
ExecStreamScan(StreamScanState *node)
{
	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) StreamScanNext,
					(ExecScanRecheckMtd) StreamScanNext);
}

void
ExecEndStreamScan(StreamScanState *node)
{
	TupleBufferCloseReader(node->reader);
}

void
ExecEndBatchStreamScan(StreamScanState *node)
{
	MemoryContextReset(node->pi->ctxt);
}

void
ExecReScanStreamScan(StreamScanState *node)
{

}
