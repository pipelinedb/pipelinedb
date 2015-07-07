/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cont_plan.h
 * 		Interface for generating/modifying CQ plans
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cont_plan.h
 *
 */
#ifndef CONT_PLAN_H
#define CONT_PLAN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#define IS_STREAM_RTE(relid, root) ((planner_rt_fetch(relid, root)) && \
	((planner_rt_fetch(relid, root))->streamdesc))
#define IS_STREAM_TREE(plan) (IsA((plan), StreamScan) || \
		IsA((plan), StreamTableJoin))

PlannedStmt *GetContPlan(ContinuousView *view);

#endif
