/*-------------------------------------------------------------------------
 *
 * sw_vacuum.h
 *
 *   Support for vacuuming discarded tuples for continuous views with TTLs.
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * src/include/pipeline/sw_vacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TTL_VACUUM_H
#define TTL_VACUUM_H

#include "postgres.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "utils/relcache.h"

extern uint64_t NumTTLExpiredTuples(Oid relid);
extern void DeleteTTLExpiredTuples(Oid relid);

#endif /* TTL_VACUUM_H */
