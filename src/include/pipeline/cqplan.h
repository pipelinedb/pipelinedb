/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqplan.h
 * 		Interface for generating/modifying CQ plans
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cqplan.h
 *
 */
#ifndef CQPLAN_H
#define CQPLAN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#define IS_STREAM_RTE(relid, root) ((planner_rt_fetch(relid, root)) && \
	((planner_rt_fetch(relid, root))->streamdesc))
#define IS_STREAM_TREE(plan) (IsA((plan), StreamScan) || \
		IsA((plan), StreamTableJoin))

PlannedStmt *GetCQPlan(ContinuousView *view);

#endif
