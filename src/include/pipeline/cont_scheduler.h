/*-------------------------------------------------------------------------
 *
 * cont_scheduler.h
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_scheduler.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONT_SCHEDULER_H
#define CONT_SCHEDULER_H

#include "storage/latch.h"
#include "postmaster/bgworker.h"
#include "storage/spin.h"

#define MAX_CQS 1024

typedef enum
{
	Combiner,
	Worker,
	Scheduler /* unused */
} ContQueryProcType;

typedef struct ContQueryProcGroup ContQueryProcGroup;

typedef struct
{
	ContQueryProcType type;
	int id; /* unique across all cont query processes */
	int group_id; /* unqiue [0, n) for each db_oid, type pair */
	Latch *latch;
	sig_atomic_t active;
	BackgroundWorkerHandle handle;
	ContQueryProcGroup *group;
} ContQueryProc;

struct ContQueryProcGroup
{
	Oid db_oid;
	NameData db_name;
	slock_t mutex;
	sig_atomic_t active;
	sig_atomic_t terminate;
	ContQueryProc procs[1]; /* number of slots is equal to continuous_query_num_combiners + continuous_query_num_workers */
};

typedef struct
{
	int batch_size;
	int max_wait;
} ContQueryRunParams;

/* per proc structures */
extern ContQueryProc *MyContQueryProc;

extern char *GetContQueryProcName(ContQueryProc *proc);

/* guc parameters */
extern bool continuous_query_crash_recovery;
extern int continuous_query_num_combiners;
extern int continuous_query_num_workers;
extern int continuous_query_batch_size;
extern int continuous_query_max_wait;
extern int continuous_query_combiner_work_mem;
extern int continuous_query_combiner_cache_mem;
extern int continuous_query_combiner_synchronous_commit;
extern double continuous_query_proc_priority;

/* shared memory stuff */
extern Size ContQuerySchedulerShmemSize(void);
extern void ContQuerySchedulerShmemInit(void);
extern ContQueryRunParams *GetContQueryRunParams(void);

/* status inquiry functions */
extern bool IsContQuerySchedulerProcess(void);
extern bool IsContQueryWorkerProcess(void);
extern bool IsContQueryCombinerProcess(void);
extern bool IsContQueryAdhocProcess(void);

#define IsContQueryProcess() \
	(IsContQueryWorkerProcess() || IsContQueryCombinerProcess())

/* functions to start the scheduler process */
extern pid_t StartContQueryScheduler(void);
#ifdef EXEC_BACKEND
void ContQuerySchedulerIAm(void);
extern void ContQuerySchedulerMain(int argc, char *argv[]) __attribute__((noreturn));
#endif

extern void ContinuousQueryCombinerMain(void);
extern void ContinuousQueryWorkerMain(void);

extern void sleep_if_deactivated(void);
extern bool ContQuerySetStateAndWait(bool state, int waitms);
extern bool *ContQueryGetActiveFlag(void);

extern void SignalContQuerySchedulerTerminate(Oid db_oid);
extern void SignalContQuerySchedulerRefresh(void);

extern ContQueryProc* ContQueryGetAdhoc(Oid db_oid);
extern void SetAmContQueryAdhoc(bool s);

#endif   /* CONT_SCHEDULER_H */
