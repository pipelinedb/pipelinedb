/*-------------------------------------------------------------------------
 *
 * cqvacuum.h
 *
 *   Support for vacuuming materialization relations for sliding window
 *   continuous views.
 *
 * src/include/pipeline/cqvacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQVACUUM_H
#define CQVACUUM_H

#include "postgres.h"
#include "executor/executor.h"
#include "utils/relcache.h"

typedef struct CQVacuumContext
{
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	List *predicate;
} CQVacuumContext;

bool RelationNeedsCQVacuum(Oid relid);
CQVacuumContext *CreateCQVacuumContext(Relation relation);
void FreeCQVacuumContext(CQVacuumContext *context);
bool ShouldVacuumCQTuple(CQVacuumContext *context, HeapTupleData *tuple);

#endif /* CQVACUUM_H */
