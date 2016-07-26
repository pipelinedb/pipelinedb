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
#include "pipeline/ipc/broker.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/spin.h"

#define MAX_CQS 1024
#define BGWORKER_IS_CONT_QUERY_PROC 0x1000

typedef enum
{
	COMBINER = 0,
	WORKER,
	ADHOC,
	SCHEDULER /* unused */
} ContQueryProcType;

typedef struct ContQueryDatabaseMetadata ContQueryDatabaseMetadata;

typedef struct ContQueryProc
{
	ContQueryProcType type;

	Latch *latch;

	int id; /* unique across all cont query processes */
	volatile int group_id; /* unqiue [0, n) for each db_oid, type pair */

	dsm_handle dsm_handle; /* only valid for adhoc processes */

	BackgroundWorkerHandle *bgw_handle;

	ContQueryDatabaseMetadata *db_meta;
} ContQueryProc;

struct ContQueryDatabaseMetadata
{
	Oid      db_oid;
	NameData db_name;

	slock_t mutex;

	sig_atomic_t running;
	sig_atomic_t dropdb;
	sig_atomic_t terminate;

	int lock_idx; /* ContQuerySchedulerShmem->locks index where the locks for this DB's workers start */

	/* Number of entries is equal to continuous_query_num_combiners + continuous_query_num_workers. */
	ContQueryProc *db_procs;

	int adhoc_counter;
	/* Number of entries is equal to max_worker_processes. */
	ContQueryProc *adhoc_procs;

	ContQueryProc trigger_proc;
	int alert_server_port;
};

typedef struct ContQueryRunParams
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
extern int  continuous_query_combiner_synchronous_commit;

extern int continuous_query_commit_interval;
extern double continuous_query_proc_priority;

#define MyDSMCQueue (MyContQueryProc->cq_handle->cqueue)

/* shared memory stuff */
extern Size ContQuerySchedulerShmemSize(void);
extern void ContQuerySchedulerShmemInit(void);
extern ContQueryRunParams *GetContQueryRunParams(void);
extern int GetContSchedulerTrancheId(void);

extern bool am_cont_combiner;

/* status inquiry functions */
extern bool IsContQuerySchedulerProcess(void);
extern bool IsContQueryWorkerProcess(void);
extern bool IsContQueryCombinerProcess(void);
extern bool IsContQueryAdhocProcess(void);
extern bool IsContQueryTriggerProcess(void);

#define IsContQueryProcess() \
	(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryAdhocProcess())

/* functions to start the scheduler process */
extern pid_t StartContQueryScheduler(void);

extern void ContinuousQueryCombinerMain(void);
extern void ContinuousQueryWorkerMain(void);
extern bool ShouldTerminateContQueryProcess(void);

extern void SignalContQuerySchedulerDropDB(Oid db_oid);
extern void SignalContQuerySchedulerRefreshDBList(void);

extern ContQueryDatabaseMetadata *GetContQueryDatabaseMetadata(Oid db_oid);

/* Adhoc Process Management */
extern void SetAmContQueryAdhoc(bool value);
extern void AdhocContQueryProcAcquire(void);
extern void AdhocContQueryProcRelease(void);

extern ContQueryProc *GetContQueryAdhocProcs(void);

extern bool CheckContinuousTriggerRequirements(int elevel);

#endif   /* CONT_SCHEDULER_H */
