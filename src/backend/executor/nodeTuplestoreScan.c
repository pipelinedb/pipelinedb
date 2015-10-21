/*-------------------------------------------------------------------------
 *
 * nodeTuplestoreScan.c
 *	  Support routines for sequential scans of tuple stores.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeTuplestoreScan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecTuplestoreScan				sequentially scans a tuplestore.
 *		ExecInitTuplestoreScan			creates and initializes a tuplestore scan node.
 *		ExecEndTuplestoreScan			releases any storage allocated.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeTuplestoreScan.h"
#include "utils/tuplestore.h"

static TupleTableSlot *TuplestoreNext(TuplestoreScanState * node);
static bool TuplestoreRecheck(TuplestoreScanState * node, TupleTableSlot *slot);

extern TuplestoreScanState *
ExecInitTuplestoreScan(TuplestoreScan *node, EState *estate, int eflags)
{
	TuplestoreScanState *scanstate;
	ScanState *nscan;
	Scan *scan;

	scanstate = makeNode(TuplestoreScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	ExecAssignExprContext(estate, &scanstate->ss.ps);

	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	nscan = (ScanState *) (scanstate);
	scan = (Scan *) nscan->ps.plan;
	scan->scanrelid = 1;

	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, (ScanState *) scanstate);
	ExecSetSlotDescriptor(((ScanState *) scanstate)->ss_ScanTupleSlot, node->desc);

	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo((ScanState *) scanstate);

	return scanstate;
}

static TupleTableSlot *
TuplestoreNext(TuplestoreScanState * node)
{
	TuplestoreScan *scan = (TuplestoreScan *) node->ss.ps.plan;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (!tuplestore_gettupleslot(scan->store, true, false, slot))
		return NULL;

	return slot;
}

static bool
TuplestoreRecheck(TuplestoreScanState * node, TupleTableSlot *slot)
{
	return true;
}

extern TupleTableSlot *
ExecTuplestoreScan(TuplestoreScanState *node)
{
	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) TuplestoreNext,
					(ExecScanRecheckMtd) TuplestoreRecheck);
}

extern void
ExecEndTuplestoreScan(TuplestoreScanState *node)
{

}
