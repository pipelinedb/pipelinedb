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
#include "parser/analyze.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/analyzer.h"
#include "pipeline/planner.h"
#include "pipeline/scheduler.h"
#include "pipeline/syscache.h"
#include "tcop/utility.h"

/*
 * InitPipeline
 *
 * This is called whenever a new backend is starting up.
 */
void
PipelineShmemInit()
{
	srand(time(NULL) ^ MyProcPid);
	ContQuerySchedulerShmemInit();
	MicrobatchAckShmemInit();
}

/*
 * PipelineInstallHooks
 */
void
PipelineInstallHooks()
{
	InitPipelineSysCache();

	if (ProcessUtility_hook == NULL)
		ProcessUtility_hook = PipelineProcessUtility;

	if (post_parse_analyze_hook == NULL)
		post_parse_analyze_hook = PostParseAnalyzeHook;
}
