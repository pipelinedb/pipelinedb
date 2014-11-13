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
#include "utils/relcache.h"

bool NeedsCQVacuum(Relation relation);
bool CQVacuumTuple(Relation rel, HeapTupleData *tuple);

#endif /* CQVACUUM_H */
