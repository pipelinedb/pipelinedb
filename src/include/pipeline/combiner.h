/* Copyright (c) 2013-2015 PipelineDB */
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
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/resowner.h"


typedef struct CombinerDesc
{
	char *name;
	int sock;
	int recvtimeoutms;
} CombinerDesc;

CombinerDesc *CreateCombinerDesc(QueryDesc *query);
void ContinuousQueryCombinerRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner);

#endif /* COMBINER_H */
