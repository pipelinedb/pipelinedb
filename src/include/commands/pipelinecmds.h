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

extern void ExecCreateContViewStmt(RangeVar *view, Node *query, List *options, const char *querystring);
extern void ExecCreateContTransformStmt(RangeVar *transform, Node *stmt, List *options, const char *querystring);
extern void ReconcilePipelineObjects(void);

#endif   /* PIPELINECMDS_H */
