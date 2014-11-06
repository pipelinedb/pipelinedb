/*-------------------------------------------------------------------------
 *
 * cqrun.h
 * 		Interface for running process groups for continuous queries
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cqrun.h
 *
 */
#ifndef CQRUN_H
#define CQRUN_H

#include "catalog/pipeline_queries_fn.h"
#include "postmaster/bgworker.h"

typedef enum
{
	CQCombiner,
	CQWorker,
	CQGarbageCollector
} CQProcessType;

bool RunContinuousQueryProcess(CQProcessType ptype, const char *cvname, struct ContinuousViewState *state, BackgroundWorkerHandle **bg_handle);

#endif
