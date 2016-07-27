/*-------------------------------------------------------------------------
 *
 * pipelinecmds.h
 *	  prototypes for pipelinecmds.c.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/commands/pipelinecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PIPELINECMDS_H
#define PIPELINECMDS_H

#include "access/tupdesc.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "nodes/primnodes.h"

/* guc parameter */
extern int continuous_view_fillfactor;

/* hooks */
extern bool use_ls_hash_group_index;

extern void ExecCreateContViewStmt(CreateContViewStmt *stmt, const char *querystring);
extern void ExecTruncateContViewStmt(TruncateStmt *stmt);

extern TupleDesc ExplainContViewResultDesc(ExplainContQueryStmt *stmt);
extern void ExecExplainContQueryStmt(ExplainContQueryStmt *stmt, const char *queryString,
			 ParamListInfo params, DestReceiver *dest);

extern void ExecActivateStmt(ActivateStmt *stmt);
extern void ExecDeactivateStmt(DeactivateStmt *stmt);

extern void ExecCreateContTransformStmt(CreateContTransformStmt *stmt, const char *querystring);

#endif   /* PIPELINECMDS_H */
