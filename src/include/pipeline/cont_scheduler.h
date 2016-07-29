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
#include "pipeline/ipc/microbatch.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/spin.h"

#define MAX_CQS 1024
#define BGWORKER_IS_CONT_QUERY_PROC 0x1000

typedef enum
{
	Combiner = 1,
	Worker,
	AdhocVacuumer,
	Scheduler /* unused */
} ContQueryProcType;

typedef struct ContQueryDatabaseMetadata ContQueryDatabaseMetadata;

typedef struct ContQueryProc
{
	ContQueryProcType type;
	Latch *latch;

	volatile int pzmq_id;
	volatile int group_id; /* unqiue [0, n) for each db_oid, type pair */

	BackgroundWorkerHandle *bgw_handle;
	ContQueryDatabaseMetadata *db_meta;
} ContQueryProc;

struct ContQueryDatabaseMetadata
{
	Oid      db_id;
	NameData db_name;

	slock_t mutex;

	sig_atomic_t running;
	sig_atomic_t dropdb;
	sig_atomic_t terminate;

	int lock_idx; /* ContQuerySchedulerShmem->locks index where the locks for this DB's workers start */

	/* Number of entries is equal to continuous_query_num_combiners + continuous_query_num_workers. */
	ContQueryProc *db_procs;

	ContQueryProc adhoc_vacuumer;
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
extern int  continuous_query_num_combiners;
extern int  continuous_query_num_workers;
extern int  continuous_query_max_wait;
extern int  continuous_query_combiner_work_mem;
extern int  continuous_query_combiner_synchronous_commit;

extern int continuous_query_commit_interval;
extern double continuous_query_proc_priority;

#define MyDSMCQueue (MyContQueryProc->cq_handle->cqueue)

/* shared memory stuff */
extern Size ContQuerySchedulerShmemSize(void);
extern void ContQuerySchedulerShmemInit(void);

extern bool am_cont_combiner;

/* status inquiry functions */
extern bool IsContQuerySchedulerProcess(void);
extern bool IsContQueryWorkerProcess(void);
extern bool IsContQueryCombinerProcess(void);
extern bool IsContQueryTriggerProcess(void);

#define IsContQueryProcess() \
	(IsContQueryWorkerProcess() || IsContQueryCombinerProcess())

/* functions to start the scheduler process */
extern pid_t StartContQueryScheduler(void);

extern void ContinuousQueryCombinerMain(void);
extern void ContinuousQueryWorkerMain(void);
extern bool ShouldTerminateContQueryProcess(void);

extern void SignalContQuerySchedulerDropDB(Oid db_oid);
extern void SignalContQuerySchedulerRefreshDBList(void);

extern ContQueryDatabaseMetadata *GetContQueryDatabaseMetadata(Oid db_oid);

#endif   /* CONT_SCHEDULER_H */
