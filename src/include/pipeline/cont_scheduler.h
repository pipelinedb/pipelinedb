/*-------------------------------------------------------------------------
 *
 * cont_scheduler.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_scheduler.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONT_SCHEDULER_H
#define CONT_SCHEDULER_H

#include "storage/latch.h"
#include "pipeline/dsm_cqueue.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/spin.h"

#define MAX_CQS 1024
#define BGWORKER_IS_CONT_QUERY_PROC 0x1000

typedef enum
{
	Combiner,
	Worker,
	Adhoc,
	Scheduler /* unused */
} ContQueryProcType;

typedef struct ContQueryDatabaseMetadata ContQueryDatabaseMetadata;

typedef struct
{
	ContQueryProcType type;

	int   id; /* unique across all cont query processes */
	int   group_id; /* unqiue [0, n) for each db_oid, type pair */
	Latch *latch;

	volatile dsm_handle dsm_handle;
	dsm_cqueue_handle *cq_handle;
	BackgroundWorkerHandle *bgw_handle;

	ContQueryDatabaseMetadata *db_meta;
} ContQueryProc;

struct ContQueryDatabaseMetadata
{
	Oid      db_oid;
	NameData db_name;
	slock_t  mutex;
	sig_atomic_t terminate;

	/* Number of entries is equal to continuous_query_num_combiners + continuous_query_num_workers. */
	ContQueryProc *db_procs;

	int adhoc_counter;
	/* Number of entries is equal to max_worker_processes. */
	ContQueryProc *adhoc_procs;
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
extern bool continuous_queries_enabled;
extern bool continuous_query_crash_recovery;
extern bool continuous_queries_adhoc_enabled;
extern int  continuous_query_num_combiners;
extern int  continuous_query_num_workers;
extern int  continuous_query_batch_size;
extern int  continuous_query_max_wait;
extern int  continuous_query_combiner_work_mem;
extern int  continuous_query_combiner_cache_mem;
extern int  continuous_query_combiner_synchronous_commit;
extern int  continuous_query_ipc_shared_mem;
extern double continuous_query_proc_priority;

#define ContQueriesEnabled() (continuous_queries_enabled)
#define ShouldTerminateContQueryProcess() (MyContQueryProc->db_meta->terminate)
#define MyDSMCQueue (MyContQueryProc->cq_handle->cqueue)

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
	(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryAdhocProcess())

/* functions to start the scheduler process */
extern pid_t StartContQueryScheduler(void);
#ifdef EXEC_BACKEND
void ContQuerySchedulerIAm(void);
extern void ContQuerySchedulerMain(int argc, char *argv[]) __attribute__((noreturn));
#endif

extern void ContinuousQueryCombinerMain(void);
extern void ContinuousQueryWorkerMain(void);

extern void SignalContQuerySchedulerTerminate(Oid db_oid);
extern void SignalContQuerySchedulerRefresh(void);

/* Adhoc Process Management */
extern void SetAmContQueryAdhoc(bool value);
extern ContQueryProc *AdhocContQueryProcGet(void);
extern void AdhocContQueryProcRelease(ContQueryProc *proc);

extern ContQueryProc *GetContQueryWorkerProcs(void);

#endif   /* CONT_SCHEDULER_H */
