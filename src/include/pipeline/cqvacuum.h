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
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "utils/relcache.h"

typedef struct CQVacuumContext
{
	TupleTableSlot *slot;
	ExprContext *econtext;
	List *predicate;
} CQVacuumContext;

uint64_t NumCQVacuumTuples(Oid relid);
CQVacuumContext *CreateCQVacuumContext(Relation relation);
void FreeCQVacuumContext(CQVacuumContext *context);
bool ShouldVacuumCQTuple(CQVacuumContext *context, HeapTupleData *tuple);

#endif /* CQVACUUM_H */
