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

extern void CreateEncoding(CreateEncodingStmt *stmt);
extern void CreateContinuousView(CreateContinuousViewStmt *stmt, const char *querystring);
extern void DropContinuousView(DropStmt *stmt);
extern void DumpState(DumpStmt *stmt);

#endif   /* PIPELINECMDS_H */
