/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 * Support for scanning the stream buffer
 *
 * src/include/executor/nodeStreamscan.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/nodeStreamscan.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "pipeline/streambuf.h"
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

static TupleTableSlot *
StreamScanNext(StreamScanState *node)
{
	StreamBufferSlot *sbs = PinNextStreamEvent(node->reader);
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple tup;

	if (sbs == NULL)
		return NULL;

	tup = ExecStreamProject(sbs->event, node->pi);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	UnpinStreamEvent(node->reader, sbs);

	return slot;
}


/*
 * ExecStreamProject
 *
 * Project a stream event onto a physical tuple
 */
HeapTuple
ExecStreamProject(StreamEvent event, StreamProjectionInfo *pi)
{
	HeapTuple decoded;
	MemoryContext oldcontext;
	Datum *values;
	bool *nulls;
	int i;
	int *intoout;
	TupleTableSlot *evslot;
	TupleDesc desc = pi->desc;

	oldcontext = MemoryContextSwitchTo(pi->ctxt);

	values = palloc0(sizeof(Datum) * desc->natts);
	nulls = palloc0(sizeof(bool) * desc->natts);

	intoout = map_field_positions(event->desc, desc);

	evslot = MakeSingleTupleTableSlot(event->desc);
	ExecStoreTuple(event->raw, evslot, InvalidBuffer, false);

	/*
	 * For each field in the event, place it in the corresponding field in the
	 * output tuple, coercing types if necessary.
	 */
	for (i = 0; i < event->desc->natts; i++)
	{
		Datum v;
		int outatt = intoout[i];
		if (outatt < 0)
			continue;

		/* this is the append-time value */
		v = slot_getattr(evslot, i + 1, &nulls[i]);

		/* if the append-time value's type is different from the target type, try to coerce it */
		if (nulls[i] == false &&
				event->desc->attrs[i]->atttypid != desc->attrs[outatt]->atttypid)
		{
			Form_pg_attribute attr = event->desc->attrs[i];
			Const *c = makeConst(attr->atttypid, attr->atttypmod, attr->attcollation,
					attr->attlen, v, nulls[outatt], attr->attbyval);
			Node *n = coerce_to_target_type(NULL, (Node *) c, attr->atttypid, desc->attrs[outatt]->atttypid,
					desc->attrs[outatt]->atttypmod, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);

			/* if the coercion is possible, do it */
			if (n != NULL)
			{
				/*
				 * TODO(derekjn) an easy optimization here would be to cache the result
				 * of this conversion to avoid a subsequent function call by another reader
				 */
				ExprState *estate = ExecInitExpr((Expr *) n, NULL);

//				ReScanExprContext(pi->econtext);

				v = ExecEvalExpr(estate, pi->econtext, &nulls[outatt], NULL);
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
			break;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	decoded = heap_form_tuple(desc, values, nulls);

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
													 "DecoderContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	state->pi->econtext = CreateStandaloneExprContext();
	state->pi->desc = node->desc;

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

	state->reader = OpenStreamBufferReader(GlobalStreamBuffer, node->cqid);

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
	CloseStreamBufferReader(node->reader);
}

void
ExecEndStreamScanBatch(StreamScanState *node)
{
	MemoryContextReset(node->pi->ctxt);
}
