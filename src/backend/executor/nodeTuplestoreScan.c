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
	TuplestoreScanState *tss = makeNode(TuplestoreScanState);
	tss->ss.ps.plan = (Plan *) node;
	tss->ss.ps.state = estate;
	tss->ss.ps.ps_ExprContext = CreateExprContext(estate);

	ExecInitResultTupleSlot(estate, &tss->ss.ps);
	ExecInitScanTupleSlot(estate, &tss->ss);

	ExecSetSlotDescriptor(tss->ss.ss_ScanTupleSlot, node->desc);
	ExecSetSlotDescriptor(tss->ss.ps.ps_ResultTupleSlot, node->desc);

	tss->ss.ps.targetlist = node->scan.plan.targetlist;

	tuplestore_rescan(node->store);

	return tss;
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
