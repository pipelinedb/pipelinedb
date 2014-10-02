/*-------------------------------------------------------------------------
 *
 * gh.h
 *
 * Interface to the CQ garbage collection process functionality
 *
 * src/include/pipeline/gc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GC_H
#define GC_H

#include "nodes/plannodes.h"
#include "pipeline/combiner.h"
#include "utils/portal.h"

void ExecutePlannedStmt(PlannedStmt *plannedstmt);
void ContinuousQueryGarbageCollectorRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner);

#endif /* GC_H */
