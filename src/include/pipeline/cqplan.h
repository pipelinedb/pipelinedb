/*-------------------------------------------------------------------------
 *
 * cqplan.h
 * 		Interface for generating/modifying CQ plans
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cqplan.h
 *
 */
#ifndef CQPLAN_H
#define CQPLAN_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

void SetCQPlanRefs(PlannedStmt *plan);

#endif
