/*-------------------------------------------------------------------------
 *
 * tuplestore_scan.c
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/tuplestore_scan.c
 */
#include "postgres.h"

#include "access/heapam.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "pipeline/tuplestore_scan.h"
#include "utils/rel.h"

static CustomPathMethods path_methods = {
	.CustomName = "TuplestoreScan",
	.PlanCustomPath = CreateTuplestoreScanPlan
};

/*
 * CreateTuplestoreScanPath
 */
Node *
CreateTuplestoreScanPath(RelOptInfo *parent)
{
	CustomPath *path = makeNode(CustomPath);

	path->path.parent = parent;
	path->path.pathtype = T_CustomScan;
	path->custom_paths = NIL;
	path->custom_private = NIL;
	path->methods = &path_methods;

	return (Node *) path;
}

/*
 * BeginTuplestoreScan
 */
static void
BeginTuplestoreScan(CustomScanState *cscan, EState *estate, int eflags)
{

}

typedef struct CustomTuplestoreScanState
{
	CustomScanState cstate;
	Tuplestorestate *store;
} CustomTuplestoreScanState;

#include "utils/memutils.h"
static TupleTableSlot *
TuplestoreNext(struct CustomScanState *node)
{
	CustomTuplestoreScanState *state = (CustomTuplestoreScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

//	elog(LOG, "context=%s", CurrentMemoryContext->name);
	if (!tuplestore_gettupleslot(state->store, true, false, slot))
		return NULL;

//	print_slot(slot);

	return slot;
}

static void
EndTuplestoreScan(struct CustomScanState *node)
{

}

static void
ReScanTuplestoreScan(struct CustomScanState *node)
{

}

static void
MarkPosTuplestoreScan(struct CustomScanState *node)
{

}

static void
RestrPosTuplestoreScan(struct CustomScanState *node)
{

}

static CustomExecMethods exec_methods = {
	.CustomName = "TuplestoreScan",
	.BeginCustomScan = BeginTuplestoreScan,
	.ExecCustomScan = TuplestoreNext,
	.EndCustomScan = EndTuplestoreScan,
	.ReScanCustomScan = ReScanTuplestoreScan,
	.MarkPosCustomScan = MarkPosTuplestoreScan,
	.RestrPosCustomScan = RestrPosTuplestoreScan
};

/*
 * CreateTuplestoreScanState
 */
Node *
CreateTuplestoreScanState(struct CustomScan *cscan)
{
	CustomTuplestoreScanState *scanstate = (CustomTuplestoreScanState *) palloc0(sizeof(CustomTuplestoreScanState));

	scanstate->cstate.ss.ps.type = T_CustomScanState;
	scanstate->cstate.methods = &exec_methods;

	// this thing still needs to be copyable, this is ghetto
	// but it's kind of a weird usage anyways because we want to scan a specific tuplestore in local memory

	scanstate->store = linitial(cscan->custom_private);

	return (Node *) scanstate;
}

static CustomScanMethods planner_methods = {
	.CustomName = "TuplestoreScan",
	.CreateCustomScanState = CreateTuplestoreScanState,
};

/*
 * CreateTuplestoreScanPlan
 */
Plan *
CreateTuplestoreScanPlan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *scan = makeNode(CustomScan);
	Plan *plan = &scan->scan.plan;
	ContQuery *cv = GetContQueryForViewId(root->parse->cqId);
	Relation matrel = heap_openrv(cv->matrel, NoLock);
	TupleDesc desc = RelationGetDescr(matrel);
	AttrNumber attrno;

	scan->methods = &planner_methods;
	scan->scan.scanrelid = 1;
	tlist = NIL;

	// should be a function to create a TL from a TupleDesc...

	for (attrno = 1; attrno <= desc->natts; attrno++)
	{
		Form_pg_attribute att_tup = desc->attrs[attrno - 1];
		/* We're reading from precisely one source, which is the tuplestore associated with this TuplestoreScan,
		 * so we always use a varno of 1.
		 */
		Var *var = makeVar(1, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);

		tlist = lappend(tlist, makeTargetEntry((Expr *) var, attrno, NULL, false));
	}

	heap_close(matrel, NoLock);

	plan->targetlist = tlist;

	return (Plan *) scan;
}
