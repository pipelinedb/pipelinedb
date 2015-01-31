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

typedef struct
{
	BackgroundWorkerHandle handle;
	pid_t last_pid;
} CQBackgroundWorkerHandle;

typedef struct
{
	int id;
	int pg_size;
	bool active;
	CQBackgroundWorkerHandle combiner;
	CQBackgroundWorkerHandle *workers;
	char *shm_query;
	char sock_name[20];
} CQProcEntry;

extern bool ContinuousQueryCrashRecovery;

extern void InitCQProcState(void);

extern CQProcEntry* GetCQProcEntry(int id);
extern int GetProcessGroupSize(int id);
extern int GetProcessGroupSizeFromCatalog(RangeVar* rv);
extern bool *GetActiveFlagPtr(int id);
extern void SetActiveFlag(int id, bool flag);
extern void MarkCombinerAsRunning(int id);
extern void MarkWorkerAsRunning(int id, int worker_id);

extern CQProcEntry* CQProcEntryCreate(int key, int pg_size);
extern void CQProcEntryRemove(int key);

/* IPC */
extern char *GetSocketName(int id);
extern pid_t GetCombinerPid(int id);
extern pid_t *GetWorkerPids(int id);

/* Resource Management */
extern bool WaitForCQProcsToStart(int id);
extern void WaitForCQProcsToTerminate(int id);
extern void TerminateCQProcs(int id);
extern bool IsCombinerRunning(int id);
extern bool AreCQWorkersStopped(int id);
extern void EnableCQProcsRecovery(int id);
extern void DisableCQProcsRecovery(int id);

extern void RunCQProcs(const char *cvname, void *state, CQProcEntry *procentry);

#endif   /* CQPROC_H */
