/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipelinecmds.h
 *	  prototypes for pipelinecmds.c.
 *
 * src/include/commands/pipelinecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PIPELINECMDS_H
#define PIPELINECMDS_H

#include "nodes/parsenodes.h"

/* guc parameter */
extern int continuous_view_fillfactor;

extern void ExecCreateContinuousViewStmt(CreateContinuousViewStmt *stmt, const char *querystring);
extern void ExecTruncateContinuousViewStmt(TruncateStmt *stmt);

extern void ExecActivateStmt(ActivateStmt *stmt);
extern void ExecDeactivateStmt(DeactivateStmt *stmt);

#endif   /* PIPELINECMDS_H */
