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

/* guc parameter */
extern int continuous_view_fillfactor;

extern void ExecCreateContViewStmt(CreateContViewStmt *stmt, const char *querystring);
extern void ExecTruncateContViewStmt(TruncateStmt *stmt);

extern void ExecActivateStmt(ActivateStmt *stmt);
extern void ExecDeactivateStmt(DeactivateStmt *stmt);

extern TupleDesc ExplainContViewResultDesc(ExplainContViewStmt *stmt);
extern void ExecExplainContViewStmt(ExplainContViewStmt *stmt, const char *queryString,
			 ParamListInfo params, DestReceiver *dest);


extern ColumnDef *
make_cv_columndef(char *name, Oid type, Oid typemod);

#endif   /* PIPELINECMDS_H */
