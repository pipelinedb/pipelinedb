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

#include "executor/executor.h"
#include "executor/nodeStreamscan.h"
#include "events/decode.h"
#include "events/streambuf.h"


static TupleTableSlot *
StreamScanNext(StreamScanState *node)
{
	StreamBufferSlot *sbs = NextStreamEvent(node->reader);
	StreamEventDecoder *decoder;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple tup;
	StreamScan *scan;
	if (sbs == NULL)
	{
		return NULL;
	}
	scan = (StreamScan *) node->ss.ps.plan;
	decoder = GetStreamEventDecoder(sbs->encoding);
	tup = DecodeStreamEvent(sbs->event, decoder, scan->desc);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	return slot;
}

StreamScanState *
ExecInitStreamScan(StreamScan *node, EState *estate, int eflags)
{
	StreamScanState *state;

	state = makeNode(StreamScanState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;

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

	/*
	 * There is probably a better place for this, but we may as well wait to initialize
	 * it until we actually need it.
	 */
	if (!GlobalStreamBuffer)
		InitGlobalStreamBuffer();

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
