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
#include "pipeline/decode.h"
#include "pipeline/streambuf.h"
 #include "access/htup_details.h"


int writeIdx = 0;
int readIdx = 0;
HeapTuple tempTupleStore[100];
int tempIntCrap[100];
Datum datumStore[10][10];
HeapTuple t1,t2,t3;
bool scanning_join_cache = false;
bool
IsScanningJoinCache()
{
	return scanning_join_cache;
}

void
ClearStreamJoinCache()
{
	elog(LOG,"*************************** CLEARING CACHE **************************");
	writeIdx = 0;
	scanning_join_cache = false;
}

static TupleTableSlot *
StreamScanNext(StreamScanState *node)
{
	StreamBufferSlot *sbs = PinNextStreamEvent(node->reader);
	StreamEventDecoder *decoder;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple slot_copy;
	HeapTuple tup;
	StreamScan *scan;
	PlanState* ps = (PlanState*) node;
	elog(LOG,"SCANNEXT IDX %d writeIdx %d cqi_batch_progres %d",readIdx, writeIdx,ps->cq_batch_progress);

	if ((sbs == NULL) && (!IsScanningJoinCache()))
	{
		scanning_join_cache = true;
		return NULL;
	}

 	// Everythin in the stream batch has been read AND 
 	// 	there is nothing waiting in the stream
 	// Read from the cache if there is anything there at all
 	if ((sbs == NULL) && (readIdx <= writeIdx))
 	{
 		HeapTuple tempHeapTuple;	
		TupleDesc	typeinfo = slot->tts_tupleDescriptor;
		int			natts = typeinfo->natts;
		bool 		nulls[natts];

 		if (readIdx == writeIdx)
 		{
 			elog(LOG,"READ EVERYTHING IN TUPLE CACHE");
 			return NULL;
 		}
 		elog(LOG,"READING FROM CACHE readIdx %d",readIdx);
		MemSet(nulls, false, natts);
		tempHeapTuple = heap_form_tuple(slot->tts_tupleDescriptor, datumStore[readIdx++], nulls);
 		ExecClearTuple(slot);
 		ExecStoreTuple(tempHeapTuple, slot, InvalidBuffer, false);
 		//print_slot(slot);
 		//elog(LOG,"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

 		return slot;
 	}	
 	
	// Nothing in the stream, return NULL
 	// Read from the stream
	scan = (StreamScan *) node->ss.ps.plan;
	decoder = GetStreamEventDecoder(sbs->encoding);
	tup = DecodeStreamEvent(sbs->event, decoder, scan->desc);
	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	UnpinStreamEvent(node->reader, sbs);

	// Store the entry in the join cache 
	elog(LOG,"COPYING TO CACHE SLOT******************");
	{
		TupleDesc	typeinfo = slot->tts_tupleDescriptor;
		int			natts = typeinfo->natts;
		// Copy out the slot contents
		Datum values[natts];
		bool nulls[natts];
		bool replaces[natts];
		HeapTuple newtup;
		int			i;
		Datum		attr;
		char	   *value;
		bool		isnull;
		Oid			typoutput;
		bool		typisvarlena;

		// INIT	
		MemSet(replaces, false, sizeof(replaces));
		MemSet(nulls, false, natts);
		MemSet(values, 0, sizeof(values));
		// Set VALUES
		slot_getallattrs(slot); // All the attr,val pairs  in the row
		for (i = 0; i < natts; ++i)
		{
			datumStore[writeIdx][i] = (Datum *) heap_getattr(slot->tts_tuple, i+1, slot->tts_tupleDescriptor, &isnull);
		}
		writeIdx++;
		if (writeIdx == 99)
			elog(LOG,"ERROR tuplestore FUL #################################L");
	}


	return slot;
}

void
ExecReScanStreamScan(StreamScanState* node)
{
	elog(LOG,"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% RESCANNING TUPLE STORE");
	readIdx = 0;
}

StreamScanState *
ExecInitStreamScan(StreamScan *node, EState *estate, int eflags)
{
	elog(LOG,"%%%%%%%%%%%%%%%% INIT STREAM SCAN");
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
