/*-------------------------------------------------------------------------
 *
 * pipelineinit.c
 *	  routines to support initialization of pipeline stuff
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/pipelineinit.c
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>
#include <stdlib.h>
#include "postgres.h"

#include "miscadmin.h"
#include "pipeline/cont_scheduler.h"
#include "storage/shm_alloc.h"

/*
 * InitPipeline
 *
 * This is called whenever a new backend is starting up.
 */
void PipelineShmemInit()
{
	srand(time(NULL) ^ MyProcPid);

	ShmemDynAllocShmemInit();
	ContQuerySchedulerShmemInit();
}
