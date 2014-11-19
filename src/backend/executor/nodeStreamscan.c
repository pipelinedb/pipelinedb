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
#include "access/xact.h"
#include "utils/memutils.h"

ListCell   *cachedRow;
// Create a linnked list of Datum pointers
bool scanning_join_cache = false;
List	   *datumCache;

bool
IsScanningJoinCache()
{
	return scanning_join_cache;
}

void
ClearStreamJoinCache()
{
	ListCell *l;
	elog(LOG,"*************************** CLEARING JOIN CACHE **************************");
	scanning_join_cache = false;
	list_free_deep(datumCache);

	/* Initialize the cache for the next join */
	InitStreamJoinCache();
}

void
InitStreamJoinCache()
{
	datumCache = NIL;
	/* More init stuff goes here */
}

static TupleTableSlot *
StreamScanNext(StreamScanState *node)
{
	StreamBufferSlot *sbs = PinNextStreamEvent(node->reader);
	StreamEventDecoder *decoder;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple tup;
	StreamScan *scan;
	ListCell   *l;
	PlanState* ps = (PlanState*) node;
	//pg_usleep(1000000);

	MemoryContext oldcontext;

	if ((sbs == NULL) && (cachedRow == NIL))
	{
		elog(LOG,"RETURN NOTHING IN STREAM OR CACHE *********************************");
		return NULL;
	}


 	if ((sbs == NULL) && (cachedRow != NIL))
 	{
 		HeapTuple tempHeapTuple;	
		TupleDesc	typeinfo = slot->tts_tupleDescriptor;
		int			natts = typeinfo->natts;
		bool 		nulls[natts];

		oldcontext = MemoryContextSwitchTo(CacheMemoryContext);	
 		elog(LOG,"READING FROM CACHE");
		MemSet(nulls, false, natts);
		tempHeapTuple = heap_form_tuple(slot->tts_tupleDescriptor, lfirst(cachedRow), nulls);
		cachedRow = lnext(cachedRow);


 		ExecClearTuple(slot);
 		ExecStoreTuple(tempHeapTuple, slot, InvalidBuffer, false);
		MemoryContextSwitchTo(oldcontext);	
 		return slot;
 	}	
 	else if ((sbs == NULL) && (cachedRow == NIL))
 	{
		elog(LOG,"READ EVERYTHING IN JOIN CACHE");
		return NULL;
 	}
 	
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
		bool nulls[natts];
		bool replaces[natts];
		HeapTuple newtup;
		int			i;
		Datum		attr;
		char	   *value;
		bool		isnull;
		Oid			typoutput;
		bool		typisvarlena;
		Datum       *row;

		oldcontext = MemoryContextSwitchTo(CacheMemoryContext);	

		/* Init cache row */
		MemSet(replaces, false, sizeof(replaces));
		MemSet(nulls, false, natts);
		row = (Datum *)palloc(natts * sizeof(Datum));

		/* populate cache row */
		slot_getallattrs(slot); // All the attr,val pairs  in the row
		for (i = 0; i < natts; 	++i)
		{
			row[i] = (Datum *) heap_getattr(slot->tts_tuple, i+1, slot->tts_tupleDescriptor, &isnull);
		}

		/* Add to the cache */
		datumCache = lappend(datumCache, row);
		elog(LOG,"cache size %d",list_length(datumCache));
		MemoryContextSwitchTo(oldcontext);	

	}

	return slot;
}

void
ExecReScanStreamScan(StreamScanState* node)
{
	elog(LOG,"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% RESCANNING TUPLE STORE");
	if (datumCache != NIL)
	{
		elog(LOG,"cache size %d",list_length(datumCache));
		cachedRow =list_head(datumCache);
	}
}

StreamScanState *
ExecInitStreamScan(StreamScan *node, EState *estate, int eflags)
{
	elog(LOG,"%%%%%%%%%%%%%%%% INIT STREAM SCAN");
	StreamScanState *state;

	state = makeNode(StreamScanState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;
	InitStreamJoinCache();

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
