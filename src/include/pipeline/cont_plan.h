/*-------------------------------------------------------------------------
 *
 * cont_plan.h
 * 		Interface for generating/modifying CQ plans
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cont_plan.h
 *
 */
#ifndef CONT_PLAN_H
#define CONT_PLAN_H

#include "catalog/pipeline_stream_fn.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "pipeline/cont_scheduler.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"

extern ProcessUtility_hook_type SaveUtilityHook;

#define IS_STREAM_RTE(relid, root) ((planner_rt_fetch(relid, root)) && \
	((planner_rt_fetch(relid, root))->relkind == RELKIND_STREAM))

#define IS_STREAM_TREE(node) ((IsA((node), ForeignScanState) && \
		(((ForeignScanState *) (node))->ss.ss_currentRelation->rd_rel->relkind == RELKIND_STREAM)) || \
		IsA((node), StreamTableJoinState))

extern PlannedStmt *GetContPlan(ContinuousView *view, ContQueryProcType type);
extern TuplestoreScan *SetCombinerPlanTuplestorestate(PlannedStmt *plan, Tuplestorestate *tupstore);
extern FuncExpr *GetGroupHashIndexExpr(ResultRelInfo *ri);
extern PlannedStmt *GetCombinerLookupPlan(ContinuousView *view);
extern PlannedStmt *GetContinuousViewOverlayPlan(ContinuousView *view);

typedef void (*ViewModFunction)(SelectStmt *sel);
extern PlannedStmt *GetContinuousViewOverlayPlanMod(ContinuousView *view,
										 	        ViewModFunction fn);

extern EState *CreateEState(QueryDesc *query_desc);
extern void SetEStateSnapshot(EState *estate);
extern void UnsetEStateSnapshot(EState *estate);

extern void ProcessUtilityOnContView(Node *parsetree, const char *sql, ProcessUtilityContext context,
													  ParamListInfo params, DestReceiver *dest, char *tag);

#endif
