/*-------------------------------------------------------------------------
 *
 * tuplestore_scan.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef TUPLESTORE_SCAN_H
#define TUPLESTORE_SCAN_H

#include "postgres.h"

#include "optimizer/planner.h"

extern Node *CreateTuplestoreScanPath(PlannerInfo *root, RelOptInfo *parent, RangeTblEntry *rte);

#endif
