/*-------------------------------------------------------------------------
 *
 * planner.h
 * 		Interface for generating/modifying CQ plans
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/planner.h
 *
 */
#ifndef CONT_PLAN_H
#define CONT_PLAN_H

#include "catalog/pipeline_stream_fn.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "pipeline/scheduler.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"

extern ProcessUtility_hook_type SaveUtilityHook;

#define IS_STREAM_RTE(relid, root) ((planner_rt_fetch(relid, root)) && \
	((planner_rt_fetch(relid, root))->relkind == RELKIND_STREAM))

extern PlannedStmt *GetContPlan(ContQuery *view, ContQueryProcType type);
extern PlannedStmt *GetGroupsLookupPlan(Query *query);
extern CustomScan *SetCombinerPlanTuplestorestate(PlannedStmt *plan, Tuplestorestate *tupstore);
extern FuncExpr *GetGroupHashIndexExpr(ResultRelInfo *ri);
extern PlannedStmt *GetCombinerLookupPlan(ContQuery *view);
extern PlannedStmt *GetContinuousViewOverlayPlan(ContQuery *view);

typedef void (*ViewModFunction)(SelectStmt *sel);
extern PlannedStmt *GetContinuousViewOverlayPlanMod(ContQuery *view,
										 	        ViewModFunction fn);

extern EState *CreateEState(QueryDesc *query_desc);
extern void SetEStateSnapshot(EState *estate);
extern void UnsetEStateSnapshot(EState *estate);

extern void PipelineProcessUtility(Node *parsetree, const char *sql, ProcessUtilityContext context,
													  ParamListInfo params, DestReceiver *dest, char *tag);

#endif
