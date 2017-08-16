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

static Plan *create_tuplestore_scan_plan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans);
static Node *create_tuplestore_scan_state(struct CustomScan *cscan);
static void begin_tuplestore_scan(CustomScanState *cscan, EState *estate, int eflags);
static TupleTableSlot *tuplestore_next(struct CustomScanState *node);
static void end_tuplestore_scan(struct CustomScanState *node);
static void rescan_tuplestore_scan(struct CustomScanState *node);

typedef struct CustomTuplestoreScanState
{
	CustomScanState cstate;
	Tuplestorestate *store;
} CustomTuplestoreScanState;

/*
 * Methods for creating a TuplestoreScan plan, attached to a CustomPath
 */
static CustomPathMethods path_methods = {
	.CustomName = "TuplestoreScan",
	.PlanCustomPath = create_tuplestore_scan_plan
};

/*
 * Methods for executing a TuplestoreScan plan, attached to a CustomTuplestoreScanState
 */
static CustomExecMethods exec_methods = {
	.CustomName = "TuplestoreScan",
	.BeginCustomScan = begin_tuplestore_scan,
	.ExecCustomScan = tuplestore_next,
	.EndCustomScan = end_tuplestore_scan,
	.ReScanCustomScan = rescan_tuplestore_scan
};

/*
 * Methods for creating a TuplestoreScan's executor state, attached to a TuplestoreScan plan node
 */
static CustomScanMethods plan_methods = {
	.CustomName = "TuplestoreScan",
	.CreateCustomScanState = create_tuplestore_scan_state,
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
 * create_tuplestore_scan_plan
 */
static Plan *
create_tuplestore_scan_plan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *scan = makeNode(CustomScan);
	Plan *plan = &scan->scan.plan;
	ContQuery *cv = GetContQueryForViewId(QueryGetContQueryId(root->parse));
	Relation matrel = heap_openrv(cv->matrel, NoLock);
	TupleDesc desc = RelationGetDescr(matrel);
	AttrNumber attrno;

	scan->methods = &plan_methods;
	scan->scan.scanrelid = 1;
	tlist = NIL;

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
	scan->custom_scan_tlist = tlist;

	return (Plan *) scan;
}

/*
 * create_tuplestore_scan_state
 */
static Node *
create_tuplestore_scan_state(struct CustomScan *cscan)
{
	CustomTuplestoreScanState *scanstate = palloc0(sizeof(CustomTuplestoreScanState));
	Value *ptr;

	scanstate->cstate.ss.ps.type = T_CustomScanState;
	scanstate->cstate.methods = &exec_methods;

	/* A local-memory pointer to the tuplestore is encoded as a copyable string :/ */
	ptr = linitial(cscan->custom_private);
	memcpy(&scanstate->store, ptr->val.str, sizeof(Tuplestorestate *));

	return (Node *) scanstate;
}

/*
 * begin_tuplestore_scan
 */
static void
begin_tuplestore_scan(CustomScanState *cscan, EState *estate, int eflags)
{
	/* Called at the end of a CustomScan's initialization */
}

/*
 * tuplestore_next
 */
static TupleTableSlot *
tuplestore_next(struct CustomScanState *node)
{
	CustomTuplestoreScanState *state = (CustomTuplestoreScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (!tuplestore_gettupleslot(state->store, true, false, slot))
		return NULL;

	return slot;
}

/*
 * end_tuplestore_scan
 */
static void
end_tuplestore_scan(struct CustomScanState *node)
{

}

/*
 * rescan_tuplestore_scan
 */
static void
rescan_tuplestore_scan(struct CustomScanState *node)
{

}
