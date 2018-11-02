/*-------------------------------------------------------------------------
 *
 * physical_group_lookup.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/extensible.h"
#include "optimizer/planmain.h"
#include "physical_group_lookup.h"
#include "scheduler.h"
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
static TupleTableSlot *nestloop_lookup_next(struct CustomScanState *node);
static TupleTableSlot *seqscan_lookup_next(struct CustomScanState *node);
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
 * Methods for executing a lookup plan on top of a NestLoop JOIN, attached to the plan state
 */
static CustomExecMethods nestloop_exec_methods = {
	.CustomName = "PhysicalGroupLookup",
	.BeginCustomScan = begin_lookup_scan,
	.ExecCustomScan = nestloop_lookup_next,
	.EndCustomScan = end_lookup_scan,
	.ReScanCustomScan = rescan_lookup_scan
};

/*
 * Methods for executing a lookup plan on top of a SeqScan, attached to the plan state
 */
static CustomExecMethods seqscan_exec_methods = {
	.CustomName = "PhysicalGroupLookup",
	.BeginCustomScan = begin_lookup_scan,
	.ExecCustomScan = seqscan_lookup_next,
	.EndCustomScan = end_lookup_scan,
	.ReScanCustomScan = rescan_lookup_scan
};

static TupleHashTable lookup_result = NULL;

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

	scan->scan.plan.targetlist = tlist;
	scan->custom_scan_tlist = tlist;

	return (Plan *) scan;
}

/*
 * SetPhysicalGroupLookupOutput
 *
 * Sets the tuplestore that the next physical group lookup will output to.
 */
void
SetPhysicalGroupLookupOutput(TupleHashTable output)
{
	Assert(lookup_result == NULL);
	lookup_result = output;
}

/*
 * CreatePhysicalGroupLookupPlan
 */
Plan *
CreatePhysicalGroupLookupPlan(Plan *outer)
{
	CustomScan *scan = makeNode(CustomScan);

	scan->scan.plan.targetlist = copyObject(outer->targetlist);
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
	Plan *outer = outerPlan(cscan);

	state->cstate.ss.ps.type = T_CustomScanState;

	/*
	 * Outer plan can either be a NestLoop, or a SeqScan for the special case of single-row-CVs
	 */
	if (IsA(outer, NestLoop))
		state->cstate.methods = &nestloop_exec_methods;
	else if (IsA(outer, SeqScan))
		state->cstate.methods = &seqscan_exec_methods;
	else
		elog(ERROR, "unexpected outer plan type: %d", nodeTag(outer));

	return (Node *) state;
}

/*
 * begin_lookup_scan
 */
static void
begin_lookup_scan(CustomScanState *cscan, EState *estate, int eflags)
{
	Plan *outer = outerPlan(cscan->ss.ps.plan);
	NestLoop *nl;

	if (IsA(outer, NestLoop))
	{
		nl = (NestLoop *) outer;
		nl->join.jointype = JOIN_INNER;
	}
	else if (!IsA(outer, SeqScan))
	{
		elog(ERROR, "unexpected outer plan type: %d", nodeTag(outer));
	}

	outerPlanState(cscan) = ExecInitNode(outer, estate, eflags);
}

/*
 * output_physical_tuple
 */
static void
output_physical_tuple(TupleTableSlot *slot)
{
	bool isnew;
	PhysicalTuple pt;
	MemoryContext old;
	TupleHashEntry entry;

	Assert(lookup_result);

	old = MemoryContextSwitchTo(lookup_result->tablecxt);

	pt = palloc0(sizeof(PhysicalTupleData));
	pt->tuple = ExecCopySlotTuple(slot);
	entry = LookupTupleHashEntry(lookup_result, slot, &isnew);
	entry->additional = pt;

	MemoryContextSwitchTo(old);
}

/*
 * seqscan_lookup_next
 */
static TupleTableSlot *
seqscan_lookup_next(struct CustomScanState *node)
{
	TupleTableSlot *result = ExecProcNode((PlanState *) outerPlanState(node));

	if (TupIsNull(result))
		return NULL;

	output_physical_tuple(result);

	return result;
}

/*
 * nestloop_lookup_next
 */
static TupleTableSlot *
nestloop_lookup_next(struct CustomScanState *node)
{
	NestLoopState *outer = (NestLoopState *) outerPlanState(node);
	TupleTableSlot *result;
	TupleTableSlot *inner;
	HTSU_Result res;
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

	output_physical_tuple(slot);

	return slot;
}

/*
 * end_lookup_scan
 */
static void
end_lookup_scan(struct CustomScanState *node)
{
	lookup_result = NULL;
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
CreatePhysicalGroupLookupPath(RelOptInfo *joinrel, Path *child)
{
	CustomPath *path = makeNode(CustomPath);

	path->path.pathtype = T_CustomScan;
	path->methods = &path_methods;
	path->path.parent = joinrel;
	path->custom_paths = list_make1(child);
	path->path.pathtarget = joinrel->reltarget;

	return (Node *) path;
}
