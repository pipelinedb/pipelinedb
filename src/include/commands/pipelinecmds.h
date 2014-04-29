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
extern void CreateContinuousView(CreateContinuousViewStmt *stmt);
extern void DropContinuousView(DropStmt *stmt);
extern void RegisterQuery(RangeVar *name, const char *rawquery);
extern void ActivateQuery(RangeVar *name);
extern void DeactivateQuery(RangeVar *name);

#endif   /* PIPELINECMDS_H */
