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
static void InitScanRelation(StreamTableScanState *node, EState *estate, int eflags);
static TupleTableSlot *StreamTableNext(StreamTableScanState *node);

EState	   *global_estate = NULL;
int 		global_eflags;
StreamTableScanState *global_scanstate;
List	   *global_targetlist;		/* target list to be computed at this node */
List	   *global_qual;			/* implicitly-ANDed qual conditions */
Relation	global_currentRelation;

struct ScanInfo
{
	EState	   			 *estate;
	int 				 eflags;
	StreamTableScanState *scanstate;
} ScanInfo;
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
	elog(LOG,"NEXT\n");
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
InitScanRelation(StreamTableScanState *node, EState *estate, int eflags)
{
	elog(LOG,"INITSCANRELATION\n");
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
									  ((SeqScan *) node->ps.plan)->scanrelid,
										   eflags);
	global_currentRelation = currentRelation;

	/* Set the right snap shot */
	//estate->es_snapshot = GetTransactionSnapshot();
	node->ss_currentRelation = currentRelation;
	//node->is_heap_open = true; //XXX find a beter place for this flag, this line should not be necessary
	//node->ss_currentScanDesc = currentScanDesc;

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
	elog(LOG,"EXECSTTBLSCAN\n");
	TupleTableSlot* temp;

	temp = ExecScan((StreamTableScanState *) node,
					(ExecScanAccessMtd) StreamTableNext,
					(ExecScanRecheckMtd) StreamTableRecheck);

	print_slot(temp);
	return temp;
}
/* ----------------------------------------------------------------
 *		ExecInitStreamTableScan
 * ----------------------------------------------------------------
 */
StreamTableScanState *
ExecInitStreamTableScan(StreamTableScan *node, EState *estate, int eflags)
{
	elog(LOG,"INITSTREAMTABLSCANyy\n");
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
	scanstate->is_heap_open = false;

	global_estate = estate;
	global_eflags = eflags;
	global_scanstate = scanstate;
	global_targetlist = (Expr *) node->plan.targetlist;
	global_qual = (Expr *) node->plan.qual;

	/******************* POSTPONE the scan relation initialization to the ExecProc stage *************************/
	/* to prevent tuple leaks and snapshot leaks */
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
	currentScanDesc = heap_beginscan(global_currentRelation,
									 GetTransactionSnapshot(),
									 0,
									 NULL);

	node->is_heap_open = true; //XXX find a beter place for this flag, this line should not be necessary
	node->ss_currentScanDesc = currentScanDesc;
}

/*
 * Close the nodes heap descriptor
 */

void
CloseHeapScan(StreamTableScanState *node)
{
	heap_endscan(node->ss_currentScanDesc);
	node->is_heap_open = false;
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
	elog(LOG,"ENDSTREAMTABLESCAN\n");
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
	 * close heap scan
	 */
	/*
	if (IsHeapOpen(node))
	{
		elog(LOG,"CLOSING EVERYTHING\n");
		CloseHeapScan(node);
	}
	*/
	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

static bool
IsHeapOpen(StreamTableScanState* node)
{
	return (node->is_heap_open == true);
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
	static done = false;
	elog(LOG,"RESCAN make this work within a trasaction\n");
	HeapScanDesc scan;

	if (!done)
	{
	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(global_estate, &global_scanstate->ps);
	/*
	 * initialize child expressions
	 */
	global_scanstate->ps.targetlist = (List *)
		ExecInitExpr(global_targetlist,
					 (PlanState *) global_scanstate);
	global_scanstate->ps.qual = (List *)
		ExecInitExpr(global_qual,
					 (PlanState *) global_scanstate);
	/*
	 * tuple table initialization
	 */
	 ExecInitResultTupleSlot(global_estate, &global_scanstate->ps);
	 ExecInitScanTupleSlot(global_estate, global_scanstate);

	/*
	 * initialize scan relation
	 */
	 InitScanRelation(node, global_estate, global_eflags);
	 OpenHeapScan(node);
	 node->ps.ps_TupFromTlist = false;
	/*
	* Initialize result tuple type and projection info.
	*/
	ExecAssignResultTypeFromTL(&node->ps);
	ExecAssignScanProjectionInfo(node);
	done = true;
	} else
	{
		OpenHeapScan(node);
	}
	scan = node->ss_currentScanDesc;

	heap_rescan(scan,			/* scan desc */
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
	elog(LOG,"MARKPOSyy\n");
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
	elog(LOG,"RESTRPOS\n");
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
