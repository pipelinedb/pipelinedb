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

void CreateEncoding(CreateEncodingStmt *stmt);
void CreateContinuousView(CreateContinuousViewStmt *stmt, const char *querystring);
void DropContinuousView(DropStmt *stmt);
void DumpState(DumpStmt *stmt);
void DeactivateContinuousView(DeactivateContinuousViewStmt *stmt);
void ClearContinuousView(ClearContinuousViewStmt *stmt);

#endif   /* PIPELINECMDS_H */
