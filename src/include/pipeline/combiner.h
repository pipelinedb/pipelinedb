/*-------------------------------------------------------------------------
 *
 * combiner.h
 *
 * Interface to the combiner process functionality
 *
 * src/include/pipeline/combiner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMBINER_H
#define COMBINER_H

#include "executor/execdesc.h"
#include "utils/resowner.h"


typedef struct CombinerDesc
{
	char *name;
	int sock;
} CombinerDesc;

extern CombinerDesc *CreateCombinerDesc(const char *name);
extern void ContinuousQueryCombinerRun(CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner);

#endif
