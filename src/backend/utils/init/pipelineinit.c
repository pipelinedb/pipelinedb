/*-------------------------------------------------------------------------
 *
 * pipelineinit.c
 *	  routines to support initialization of pipeline stuff
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
#include "pipeline/cqproc.h"
#include "pipeline/tuplebuf.h"
#include "storage/spalloc.h"

/*
 * InitPipeline
 *
 * This is called whenever a new backend is starting up.
 */
void InitPipeline()
{
	srand(time(NULL));
	InitSPalloc();
	TupleBuffersInit();
	InitCQProcState();
}
