/*-------------------------------------------------------------------------
 *
 * tuplestore_scan.h
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/tuplestore_scan.h
 *
 */
#ifndef TUPLESTORE_SCAN_H
#define TUPLESTORE_SCAN_H

#include "postgres.h"

#include "optimizer/planner.h"

extern Node *CreateTuplestoreScanPath(RelOptInfo *parent);
extern Node *CreateTuplestoreScanState(struct CustomScan *cscan);
extern Plan *CreateTuplestoreScanPlan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
		List *tlist, List *clauses, List *custom_plans);

#endif
