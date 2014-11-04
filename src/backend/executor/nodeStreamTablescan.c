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
 *		ExecTreamTableNext				retrieve next tuple in sequential order.
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

static void InitStreamTableScanRelation(StreamTableScanState *node, EState *estate, int eflags);
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
	 // XXX TODO can we make this better for streams? (Jay)
	return true;
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
	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) StreamTableNext,
					(ExecScanRecheckMtd) StreamTableRecheck);
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
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
									  ((SeqScan *) node->ps.plan)->scanrelid,
										   eflags);

	/* initialize a heapscan */
	currentScanDesc = heap_beginscan(currentRelation,
									 estate->es_snapshot,
									 0,
									 NULL);

	node->ss_currentRelation = currentRelation;
	node->ss_currentScanDesc = currentScanDesc;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(node, RelationGetDescr(currentRelation));
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

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ps);
	ExecInitScanTupleSlot(estate, scanstate);

	/*
	 * initialize scan relation
	 */
	InitScanRelation(scanstate, estate, eflags);

	scanstate->ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ps);
	ExecAssignScanProjectionInfo(scanstate);

	return scanstate;
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
	 * close heap scan
	 */
	heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanStreamTableScan(StreamTableScanState *node)
{
	HeapScanDesc scan;

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
ExecSTreamTableMarkPos(StreamTableScanState *node)
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
