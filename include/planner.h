/*-------------------------------------------------------------------------
 *
 * planner.h
 * 		Interface for generating/modifying CQ plans
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 */
#ifndef CONT_PLAN_H
#define CONT_PLAN_H

#include "executor/execdesc.h"
#include "optimizer/planner.h"
#include "pipeline_query.h"
#include "scheduler.h"

extern bool debug_simulate_continuous_query;

extern void InstallPlannerHooks(void);

extern PlannedStmt *GetContPlan(ContQuery *view, ContQueryProcType type);
extern PlannedStmt *GetGroupsLookupPlan(Query *query);
extern CustomScan *SetCombinerPlanTuplestorestate(PlannedStmt *plan, Tuplestorestate *tupstore);
extern FuncExpr *GetGroupHashIndexExpr(ResultRelInfo *ri);
extern PlannedStmt *GetCombinerLookupPlan(ContQuery *view);
extern PlannedStmt *GetContViewOverlayPlan(ContQuery *view);

extern FuncExpr *GetGroupHashIndexExpr(ResultRelInfo *ri);

extern EState *CreateEState(QueryDesc *query_desc);
extern void SetEStateSnapshot(EState *estate);
extern void UnsetEStateSnapshot(EState *estate);

extern PlannedStmt *PipelinePlanner(Query *parse, int cursorOptions, ParamListInfo boundParams);

#endif
