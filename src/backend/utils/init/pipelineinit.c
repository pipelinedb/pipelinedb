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
#include "postgres.h"

#include "catalog/pipeline_queries_fn.h"
#include "miscadmin.h"
#include "pipeline/cvmetadata.h"
#include "pipeline/streambuf.h"
#include "storage/lwlock.h"

/*
 * init_pipeline_once
 *
 * Perform any initialization work that needs to happen when the postmaster process
 * starts.
 *
 * XXX(usmanm): This is technically a hack. We hook into postinit.c:InitPostgres
 * which is called before spawning every client backend. We (re)use the CVMetadata
 * lock to initialize a shmem bool which indicates whether we've initialized
 * PipelineDB or not. This gives the fake behavior of the initialization code
 * only running once, but doesn't happen when the postgres process starts, rather
 * it executes when the first client connects.
 */
static void
init_pipeline_once()
{
	MarkAllContinuousViewsAsInactive();
}

/*
 * init_pipeline
 *
 * Perform any initialization work whenever a new client backend is spawned.
 */
static void
init_pipeline()
{
	InitGlobalStreamBuffer();
	InitCVMetadataTable();
	InitStreamBufferLatch();
}

/*
 * InitPipeline
 */
void InitPipeline()
{
	bool found = false;
	Size shmemSize = MAXALIGN(sizeof(char));

	LWLockAcquire(CVMetadataLock, LW_EXCLUSIVE);

	ShmemInitStruct("IsPipelineInitialized", shmemSize, &found);

	if (!found)
	{
		init_pipeline_once();
	}

	LWLockRelease(CVMetadataLock);

	init_pipeline();
}
