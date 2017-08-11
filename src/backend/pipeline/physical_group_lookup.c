/*-------------------------------------------------------------------------
 *
 * physical_group_lookup.c
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/physical_group_lookup.c
 */
#include "postgres.h"

#include "access/heapam.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "pipeline/physical_group_lookup.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

typedef struct LookupScanState
{
	CustomScanState cstate;
} LookupScanState;

static Plan *create_lookup_plan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans);
static Node *create_lookup_scan_state(struct CustomScan *cscan);
static void begin_lookup_scan(CustomScanState *cscan, EState *estate, int eflags);
static TupleTableSlot *lookup_next(struct CustomScanState *node);
static void end_lookup_scan(struct CustomScanState *node);
static void rescan_lookup_scan(struct CustomScanState *node);

/*
 * Methods for creating a physical lookup plan, attached to a CustomPath
 */
static CustomPathMethods path_methods = {
	.CustomName = "PhysicalGroupLookup",
	.PlanCustomPath = create_lookup_plan
};

/*
 * Methods for creating a the executor state, attached to a physical lookup's plan node
 */
static CustomScanMethods plan_methods = {
	.CustomName = "PhysicalGroupLookup",
	.CreateCustomScanState = create_lookup_scan_state,
};

/*
 * Methods for executing a lookup plan, attached to the plan state
 */
static CustomExecMethods exec_methods = {
	.CustomName = "PhysicalGroupLookup",
	.BeginCustomScan = begin_lookup_scan,
	.ExecCustomScan = lookup_next,
	.EndCustomScan = end_lookup_scan,
	.ReScanCustomScan = rescan_lookup_scan
};

/*
 * create_lookup_plan
 */
static Plan *
create_lookup_plan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *scan = makeNode(CustomScan);

	/*
	 * Lookup plans are nested-loop JOINs, and the inner and outer plans should have
	 * already been planned
	 */
	Assert(list_length(custom_plans) == 1);

	outerPlan(scan) = linitial(custom_plans);

	/* physical group lookups always scan one relation: the outer plan, which is a nested loop JOIN */
	scan->scan.scanrelid = 1;
	scan->methods = &plan_methods;

	return (Plan *) scan;
}

/*
 * create_lookup_scan_state
 */
static Node *
create_lookup_scan_state(struct CustomScan *cscan)
{
	LookupScanState *state = palloc0(sizeof(LookupScanState));

	state->cstate.ss.ps.type = T_CustomScanState;
	state->cstate.methods = &exec_methods;

	return (Node *) state;
}

/*
 * begin_lookup_scan
 */
List *PHYSICAL_TUPLES = NIL;

#include "utils/memutils.h"
static void
begin_lookup_scan(CustomScanState *cscan, EState *estate, int eflags)
{
	Plan *outer = outerPlan(cscan->ss.ps.plan);
	NestLoop *nl;

	if (!IsA(outer, NestLoop))
		elog(ERROR, "unexpected join node found in physical group lookup: %d", nodeTag(outer));

	nl = (NestLoop *) outer;
	nl->join.jointype = JOIN_INNER;

	outerPlanState(cscan) = ExecInitNode(outer, estate, eflags);
}

/*
 * lookup_next
 */
static TupleTableSlot *
lookup_next(struct CustomScanState *node)
{
	NestLoopState *outer = (NestLoopState *) outerPlanState(node);
	TupleTableSlot *result;
	TupleTableSlot *inner;
	bool res;
	EState *estate = outer->js.ps.state;
	Buffer buffer;
	HeapUpdateFailureData hufd;
	HeapTuple tup;
	Relation rel;
	TupleTableSlot *slot = NULL;
	ScanState *scan;

	/* Get next tuple from subplan, if any. */
lnext:
	result = ExecProcNode((PlanState *) outer);
	if (TupIsNull(result))
		return NULL;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/* this slot contains the physical matrel tuple that was joined on */
	inner = outer->js.ps.ps_ExprContext->ecxt_innertuple;

	tup = inner->tts_tuple;
	Assert(inner->tts_tuple);

	scan = (ScanState *) outer->js.ps.righttree;
	rel = scan->ss_currentRelation;

	/* lock the physical tuple for update */
	res = heap_lock_tuple(rel, tup, estate->es_output_cid,
			LockTupleExclusive, LockWaitBlock, true, &buffer, &hufd);
	ReleaseBuffer(buffer);

	switch (res)
	{
		case HeapTupleSelfUpdated:
			/*
			 * The target tuple was already updated or deleted by the
			 * current command, or by a later command in the current
			 * transaction.
			 *
			 * This should NEVER happen.
			 */
			elog(ERROR, "tuple updated again in the same transaction");
			break;

		case HeapTupleMayBeUpdated:
			/* Got the lock successfully! */
			slot = inner;
			break;

		case HeapTupleUpdated:
			/* Was tuple deleted? */
			if (ItemPointerEquals(&hufd.ctid, &tup->t_self))
				goto lnext;

			/* Tuple was updated, so fetch and lock the updated version */
			tup = EvalPlanQualFetch(estate, rel, LockTupleExclusive, false, &hufd.ctid, hufd.xmax);
			if (tup == NULL)
				goto lnext;

			slot = ExecStoreTuple(tup, node->ss.ps.ps_ResultTupleSlot, InvalidBuffer, true);
			break;

		default:
			elog(ERROR, "unrecognized heap_lock_tuple status: %u", res);
	}

	// what's the cleanest way to put these somewhere for retrieval?
	// we probably want to pass the plan a ref to a tuplestore like we do with TuplestoreScan
	// should we just have a tuplestore pointer to write them into right here? Might as well!
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	PHYSICAL_TUPLES = lappend(PHYSICAL_TUPLES, ExecCopySlotTuple(slot));
	MemoryContextSwitchTo(old);

	return slot;
}

/*
 * end_lookup_scan
 */
static void
end_lookup_scan(struct CustomScanState *node)
{
	ExecEndNode(outerPlanState(node));
}

/*
 * rescan_lookup_scan
 */
static void
rescan_lookup_scan(struct CustomScanState *node)
{

}

/*
 * CreatePhysicalGroupLookupPath
 */
Node *
CreatePhysicalGroupLookupPath(RelOptInfo *joinrel, NestPath *nlpath)
{
	CustomPath *path = makeNode(CustomPath);

	path->path.pathtype = T_CustomScan;
	path->methods = &path_methods;
	path->path.parent = joinrel;
	path->custom_paths = list_make1(nlpath);

	return (Node *) path;
}
