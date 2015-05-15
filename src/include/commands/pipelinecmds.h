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

/* GUC parameter */
extern int continuous_view_fillfactor;

void ExecCreateContinuousViewStmt(CreateContinuousViewStmt *stmt, const char *querystring);
void ExecDropContinuousViewStmt(DropStmt *stmt);
int ExecActivateContinuousViewStmt(ActivateContinuousViewStmt *stmt, bool recovery);
int ExecDeactivateContinuousViewStmt(DeactivateContinuousViewStmt *stmt);
void ExecTruncateContinuousViewStmt(TruncateStmt *stmt);

#endif   /* PIPELINECMDS_H */
