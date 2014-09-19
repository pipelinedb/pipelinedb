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

extern void ContinuousQueryCombinerRun(QueryDesc *queryDesc, ResourceOwner owner);

#endif
