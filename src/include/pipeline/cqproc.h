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
#include "signal.h"
#include "storage/spin.h"

#define NUM_WORKERS(entry) ((entry)->pg_size - 1)

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
	slock_t mutex;
} CQProcEntry;

/* GUC parameters */
extern bool continuous_query_crash_recovery;

extern void InitCQProcState(void);

extern CQProcEntry* GetCQProcEntry(int id);
extern int GetProcessGroupSize(int id);
extern int GetProcessGroupSizeFromCatalog(RangeVar* rv);
extern void SetActiveFlag(int id, bool flag);
extern void MarkCombinerAsRunning(int id);
extern void MarkWorkerAsRunning(int id, int worker_id);

extern CQProcEntry* CQProcEntryCreate(int key, int pg_size);
extern void CQProcEntryRemove(int key);

/* IPC */
extern pid_t GetCombinerPid(int id);
extern pid_t *GetWorkerPids(int id);

/* Resource Management */
extern bool WaitForCQProcsToStart(int id);
extern void WaitForCQProcsToTerminate(int id);
extern void TerminateCQProcs(int id);
extern bool AreCQWorkersStopped(int id);
extern void EnableCQProcsRecovery(int id);
extern void DisableCQProcsRecovery(int id);

extern void RunCQProcs(const char *cvname, void *state, CQProcEntry *procentry, Oid dboid);
extern void RestartContinuousQueryProcs(List *databases);

#endif   /* CQPROC_H */
