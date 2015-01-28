/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqproc.h
 *	  prototypes for cqproc.c.
 *
 * src/include/pipeline/cqproc.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CQPROC_H
#define CQPROC_H

#include "datatype/timestamp.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"

typedef enum
{
	CQCombiner,
	CQWorker
} CQProcessType;

typedef struct SocketDesc
{
	NameData name;
	TimestampTz created_at;
	bool conn_waiting;
} SocketDesc;

typedef struct CQProcTableEntry
{
	uint32 key; /* key must be the first field */
	int32 pg_count;
	int32 pg_size;
	bool active;
	BackgroundWorkerHandle combiner;
	BackgroundWorkerHandle *workers;
	char *shm_query;
	SocketDesc socket_desc;
} CQProcEntry;

extern void InitCQProcState(void);

extern CQProcEntry* GetCQProcEntry(int32 id);
extern int GetProcessGroupSize(int32 id);
extern int GetProcessGroupSizeFromCatalog(RangeVar* rv);
extern int GetProcessGroupCount(int32 id);
extern void DecrementProcessGroupCount(int32 id);
extern void IncrementProcessGroupCount(int32 id);
extern bool *GetActiveFlagPtr(int32 id);
extern void SetActiveFlag(int32 id, bool flag);

extern CQProcEntry* CQProcEntryCreate(int32 key, int pg_size);
extern void CQProcEntryRemove(int32 key);

extern SocketDesc *GetSocketDesc(int32 id);

extern bool WaitForCQProcsToStart(int32 id);
extern void WaitForCQProcsToTerminate(int32 id);
extern void TerminateCQProcs(int32 id);
extern bool AreCQWorkersStopped(int32 id);
extern void EnableCQProcsRecovery(int32 id);

extern void RunCQProcs(const char *cvname, void *state, CQProcEntry *procentry);

#endif   /* CQPROC_H */
