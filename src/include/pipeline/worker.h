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
#include "pipeline/combiner.h"
#include "utils/resowner.h"

void ContinuousQueryWorkerRun(Portal portal, ContinuousViewState *state, QueryDesc *queryDesc, ResourceOwner owner);

#endif
