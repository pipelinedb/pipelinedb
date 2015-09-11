/*-------------------------------------------------------------------------
 *
 * sw_vacuum.h
 *
 *   Support for vacuuming discarded tuples for sliding window
 *   continuous views.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/sw_vacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SW_VACUUM_H
#define SW_VACUUM_H

#include "postgres.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "utils/relcache.h"

typedef struct SWVacuumContext
{
	TupleTableSlot *slot;
	ExprContext *econtext;
	List *predicate;
	List *expired;
} SWVacuumContext;

extern uint64_t NumSWVacuumTuples(Oid relid);
extern SWVacuumContext *CreateSWVacuumContext(Relation relation);
extern void FreeSWVacuumContext(SWVacuumContext *context);
extern bool ShouldVacuumSWTuple(SWVacuumContext *context, HeapTuple tuple);

#endif /* SW_VACUUM_H */
