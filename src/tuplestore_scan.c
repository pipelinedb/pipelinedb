/*-------------------------------------------------------------------------
 *
 * tuplestore_scan.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "analyzer.h"
#include "matrel.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "optimizer/tlist.h"
#include "pipeline_query.h"
#include "tuplestore_scan.h"
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
CreateTuplestoreScanPath(PlannerInfo *root, RelOptInfo *parent, RangeTblEntry *rte)
{
	CustomPath *path = makeNode(CustomPath);

	path->path.parent = parent;
	path->path.pathtype = T_CustomScan;
	path->custom_paths = NIL;
	path->custom_private = NIL;
	path->methods = &path_methods;
	path->path.pathtarget = parent->reltarget;

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
	ContQuery *cv = GetContViewForId(QueryGetContQueryId(root->parse));
	Relation matrel = heap_open(cv->matrelid, NoLock);
	TupleDesc desc = RelationGetDescr(matrel);
	AttrNumber attrno;
	Index groupref[desc->natts];
	ListCell *lc;
	int i = 0;
	bool physical_tlist = true;

	MemSet(groupref, 0, sizeof(groupref));

	/*
	 * The grouping/sorting positions are stored in parallel to the path target's expressions, which
	 * may be in a different order than the matrel's columns so we map matrel attributes to their
	 * grouping/sorting positions for when we build the final matrel-ordered target list below.
	 */
	foreach(lc, best_path->path.pathtarget->exprs)
	{
		Expr *e = (Expr *) lfirst(lc);
		Var *v;

		/*
		 * If the path expressions are not bare references to columns, we don't want to use a physical targetlist
		 *
		 * TODO(derekjn) This feels hacky, look for a cleaner way to build this scan's targetlist without special casing.
		 */
		if (!IsA(e, Var))
		{
			physical_tlist = false;
			continue;
		}
		Assert(IsA(e, Var));
		v = (Var *) lfirst(lc);
		groupref[v->varattno - 1] = get_pathtarget_sortgroupref(best_path->path.pathtarget, i);
		i++;
	}

	scan->methods = &plan_methods;
	scan->scan.scanrelid = rel->relid;

	if (physical_tlist)
	{
		tlist = NIL;
		best_path->path.pathtarget->exprs = NIL;
		best_path->path.pathtarget->sortgrouprefs = NULL;

		for (attrno = 1; attrno <= desc->natts; attrno++)
		{
			Form_pg_attribute att_tup = TupleDescAttr(desc, attrno - 1);
			Var *var;
			TargetEntry *te;

			/*
			 * TuplestoreScans will never read the auto-created primary-key column
			 */
			if (!pg_strcasecmp(NameStr(att_tup->attname), CQ_MATREL_PKEY))
				continue;

			/* We're reading from precisely one source, which is the tuplestore associated with this TuplestoreScan,
			 * so we always use a varno of 1.
			 */
			var = makeVar(1, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
			te = makeTargetEntry((Expr *) var, attrno, NameStr(att_tup->attname), false);

			te->ressortgroupref = groupref[attrno - 1];
			tlist = lappend(tlist, te);
			add_column_to_pathtarget(best_path->path.pathtarget, (Expr *) var, te->ressortgroupref);
		}
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
