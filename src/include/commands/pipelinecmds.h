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

extern void RegisterQuery(const char *name, const char *source);

#endif   /* PIPELINECMDS_H */
