/*-------------------------------------------------------------------------
 *
 * nodeStreamTablescan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeStreamTableScan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecStreamTableScan				sequentially scans a relation.
 *		ExecStreamTableNext				retrieve next tuple in sequential order.
 *		ExecInitStreamTableScan			creates and initializes a seqscan node.
 *		ExecEndStreamTableScan			releases any storage allocated.
 *		ExecReScanStreamTableScan		rescans the relation
 *		ExecStreamTableMarkPos			marks scan position
 *		ExecStreamTableRestrPos			restores scan position
 */
#include "postgres.h"

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeStreamTablescan.h"
#include "utils/rel.h"

static bool IsHeapOpen(StreamTableScanState* node);
static void InitScanRelation(StreamTableScanState *node);
static TupleTableSlot *StreamTableNext(StreamTableScanState *node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		StreamTableNext
 *
 *		This is a workhorse for ExecStreamTableScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
StreamTableNext(StreamTableScanState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss_currentScanDesc;
	estate = node->ps.state;
	direction = estate->es_direction;
	slot = node->ss_ScanTupleSlot;

	/*
	 * get the next tuple from the table
	 */
	tuple = heap_getnext(scandesc, direction);

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreTuple(tuple,	/* tuple to store */
					   slot,	/* slot to store in */
					   scandesc->rs_cbuf,		/* buffer associated with this
												 * tuple */
					   false);	/* don't pfree this pointer */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * StreamTableRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
StreamTableRecheck(StreamTableScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}



/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitScanRelation(StreamTableScanState *node)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(node->ss_savedScanInfo.estate,
									  ((SeqScan *) node->ps.plan)->scanrelid,
										   node->ss_savedScanInfo.eflags);

	/* Set the right snap shot */
	node->ss_currentRelation = currentRelation;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(node, RelationGetDescr(currentRelation));
}

/* ----------------------------------------------------------------
 *		ExecStreamTableScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecStreamTableScan(StreamTableScanState *node)
{
	TupleTableSlot* temp;

	temp = ExecScan((StreamTableScanState *) node,
					(ExecScanAccessMtd) StreamTableNext,
					(ExecScanRecheckMtd) StreamTableRecheck);

	return temp;
}

/* ----------------------------------------------------------------
 *		ExecInitStreamTableScan
 * ----------------------------------------------------------------
 */
StreamTableScanState *
ExecInitStreamTableScan(StreamTableScan *node, EState *estate, int eflags)
{
	StreamTableScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(StreamTableScanState);
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;
	scanstate->ss_savedScanInfo.estate = estate;
	scanstate->ss_savedScanInfo.eflags = eflags;
	scanstate->ss_savedScanInfo.targetlist = (Expr *) node->plan.targetlist;
	scanstate->ss_savedScanInfo.qual = (Expr *) node->plan.qual;

	return scanstate;
}

/*
* Create and set a new Heap descriptor
*/
void
OpenHeapScan(StreamTableScanState *node)
{
	HeapScanDesc currentScanDesc;

	/* initialize a heapscan */
	currentScanDesc = heap_beginscan(node->ss_currentRelation,
									 GetTransactionSnapshot(),
									 0,
									 NULL);

	node->ss_currentScanDesc = currentScanDesc;
}

/*
 * Close the nodes heap descriptor
 */
void
CloseHeapScan(StreamTableScanState *node)
{
	heap_endscan(node->ss_currentScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecEndStreamTableScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndStreamTableScan(StreamTableScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss_currentRelation;
	scanDesc = node->ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss_ScanTupleSlot);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */
 static
 void InitMiscInfo(StreamTableScanState *node)
 {

	node->ps.ps_TupFromTlist = false;

	/*
	* Initialize result tuple type and projection info.
	*/
	ExecAssignResultTypeFromTL(&node->ps);
	ExecAssignScanProjectionInfo(node);
 }

 static
 void InitExpressionInfo(StreamTableScanState *node)
 {
	List	   *targetlist = node->ss_savedScanInfo.targetlist;	
	EState	   *estate = node->ss_savedScanInfo.estate;
	List	   *qual = node->ss_savedScanInfo.qual;	

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &node->ps);
	/*
	 * initialize child expressions
	 */
	node->ps.targetlist = (List *)
		ExecInitExpr(targetlist,
					 (PlanState *) node);
	node->ps.qual = (List *)
		ExecInitExpr(qual,
					 (PlanState *) node);
	/*
	 * tuple table initialization
	 */
	 ExecInitResultTupleSlot(estate, &node->ps);
	 ExecInitScanTupleSlot(estate, node);
 }

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanStreamTableScan(StreamTableScanState *node)
{
	static bool isFirstRescan = true;
	HeapScanDesc scan;

	/*
	 * Some info needs tbe initialized only
	 * once per scan node, that goes in here
	 */
	if (isFirstRescan)
	{
		/* (Jay) XXX
		 * These methods need to be called in order
		 * and cant really be merged into 1 or 2 methods
		 * and looks a little hacky.
		 */
	 	InitExpressionInfo(node);
	 	InitScanRelation(node);
	 	InitMiscInfo(node);
		isFirstRescan = false;
	} 

	/*
	 * Heaps are opened every rescan
	 * and closed in ExecEndBatch
	 */
	OpenHeapScan(node);
	scan = node->ss_currentScanDesc;

	heap_rescan(scan,			/* sn desc */
				NULL);			/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *		ExecSeqMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void
ExecStreamTableMarkPos(StreamTableScanState *node)
{
	HeapScanDesc scan = node->ss_currentScanDesc;

	heap_markpos(scan);
}

/* ----------------------------------------------------------------
 *		ExecSeqRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void
ExecStreamTableRestrPos(StreamTableScanState *node)
{
	HeapScanDesc scan = node->ss_currentScanDesc;

	/*
	 * Clear any reference to the previously returned tuple.  This is needed
	 * because the slot is simply pointing at scan->rs_cbuf, which
	 * heap_restrpos will change; we'd have an internally inconsistent slot if
	 * we didn't do this.
	 */
	ExecClearTuple(node->ss_ScanTupleSlot);

	heap_restrpos(scan);
}
