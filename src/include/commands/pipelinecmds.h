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

void ExecCreateContinuousViewStmt(CreateContinuousViewStmt *stmt, const char *querystring);
void ExecDropContinuousViewStmt(DropStmt *stmt);
void ExecDumpStmt(DumpStmt *stmt);
int ExecActivateContinuousViewStmt(ActivateContinuousViewStmt *stmt);
int ExecDeactivateContinuousViewStmt(DeactivateContinuousViewStmt *stmt);
void ExecTruncateContinuousViewStmt(TruncateStmt *stmt);

#endif   /* PIPELINECMDS_H */
