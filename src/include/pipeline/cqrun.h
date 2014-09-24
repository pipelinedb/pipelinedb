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

typedef enum
{
	CQCombiner,
	CQWorker
} CQProcessType;

int RunContinuousQueryProcess(CQProcessType ptype, const char *cvname, struct ContinuousViewState state);

#endif
