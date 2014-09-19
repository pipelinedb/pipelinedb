/*-------------------------------------------------------------------------
 *
 * worker.h
 *
 * Interface to the worker process functionality
 *
 * src/include/pipeline/worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WORKER_H
#define WORKER_H

#include "executor/execdesc.h"
#include "utils/resowner.h"

extern void ContinuousQueryWorkerRun(QueryDesc *queryDesc, ResourceOwner owner);

#endif
