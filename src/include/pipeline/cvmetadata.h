/*-------------------------------------------------------------------------
 *
 * cvmetadata.h
 *	  prototypes for cvmetadata.c.
 *
 * src/include/pipeline/cvmetadata.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CVMETADATA_H
#define CVMETADATA_H

#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"

/*
   CV Metadata, elements of which are stored
   in shared memory
 */
typedef struct CVMetadata
{
	uint32 key; /* key must be the first field */
	int32 pg_count;
	int32 pg_size;
	bool active;
	BackgroundWorkerHandle combiner;
	/* TODO(usmanm): Make this dynamic to support parallelism */
	BackgroundWorkerHandle worker;
	BackgroundWorkerHandle gc;
	bool worker_done;
} CVMetadata;

extern void InitCVMetadataTable(void);

extern CVMetadata* GetCVMetadata(int32 id);
extern int GetProcessGroupSize(int32 id);
extern int GetProcessGroupSizeFromCatalog(RangeVar* rv);
extern int GetProcessGroupCount(int32 id);
extern void DecrementProcessGroupCount(int32 id);
extern void IncrementProcessGroupCount(int32 id);
extern bool *GetActiveFlagPtr(int32 id);
extern void SetActiveFlag(int32 id, bool flag);

extern CVMetadata* EntryAlloc(int32 key, int pg_size);
extern void EntryRemove(int32 key);

extern bool WaitForCQProcessesToStart(int32 id);
extern void WaitForCQProcessesToTerminate(int32 id);
extern void TerminateCQProcesses(int32 id);
extern bool DidCQWorkerCrash(int32 id);
extern void SetCQWorkerDoneFlag(int32 cq_id);

#endif   /* CVMETADATA_H */
