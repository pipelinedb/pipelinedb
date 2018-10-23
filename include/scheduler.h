/*-------------------------------------------------------------------------
 *
 * scheduler.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_SCHEDULER_H
#define CONT_SCHEDULER_H

#include "storage/latch.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/spin.h"

#define MAX_CQS 1024
#define BGWORKER_IS_CONT_QUERY_PROC 0x1000
#define NUM_BG_WORKERS_PER_DB (num_workers + num_combiners + num_queues + num_reapers)

#define PIPELINE_EXEC_CONTINUOUS 0x100000

#define NUM_BG_WORKERS_PER_DB (num_workers + num_combiners + num_queues + num_reapers)

typedef enum
{
	Combiner = 0,
	Worker,
	Queue,
	Reaper,
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

	pg_atomic_uint64 generation;

	slock_t mutex;

	sig_atomic_t running;
	sig_atomic_t dropdb;
	sig_atomic_t extexists;
	sig_atomic_t terminate;

	/* Number of entries is equal to continuous_query_num_combiners + continuous_query_num_workers. */
	ContQueryProc *db_procs;
};

typedef struct ContQueryRunParams
{
	int batch_size;
	int max_wait;
} ContQueryRunParams;

/* per proc structures */
extern ContQueryProc *MyContQueryProc;

extern void cont_bgworker_main(Datum arg);
extern char *GetContQueryProcName(ContQueryProc *proc);

extern void debug_segfault(SIGNAL_ARGS);

/* guc parameters */
extern int num_workers;
extern int num_combiners;
extern int num_queues;
extern int num_reapers;

extern bool continuous_queries_enabled;
extern int  continuous_query_queue_mem;
extern int  continuous_query_max_wait;
extern int  continuous_query_combiner_work_mem;
extern int  continuous_query_combiner_synchronous_commit;

extern int continuous_query_commit_interval;
extern double continuous_query_proc_priority;

#define MyDSMCQueue (MyContQueryProc->cq_handle->cqueue)

extern void ContQuerySchedulerMain(Datum arg);

/* shared memory stuff */
extern Size ContQuerySchedulerShmemSize(void);
extern void ContQuerySchedulerShmemInit(void);

extern bool am_cont_combiner;

/* status inquiry functions */
extern bool IsContQuerySchedulerProcess(void);
extern bool IsContQueryWorkerProcess(void);
extern bool IsContQueryCombinerProcess(void);
extern bool IsContQueryQueueProcess(void);
extern bool IsContQueryTriggerProcess(void);

#define IsContQueryProcess() \
	(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryQueueProcess())

/* functions to start the scheduler process */
extern pid_t StartContQueryScheduler(void);

extern void ContinuousQueryCombinerMain(void);
extern void ContinuousQueryWorkerMain(void);
extern void ContinuousQueryQueueMain(void);
extern void ContinuousQueryReaperMain(void);

extern void ContQuerySchedulerCanCreateDB(void);

extern void SignalContQuerySchedulerDropDB(Oid db_oid);
extern void SignalContQuerySchedulerCreatePipelineDB(Oid db_oid);
extern void SignalContQuerySchedulerDropPipelineDB(Oid db_oid);
extern void SignalContQuerySchedulerRefreshDBList(void);

extern ContQueryDatabaseMetadata *GetContQueryDatabaseMetadata(Oid db_oid);
extern ContQueryDatabaseMetadata *GetMyContQueryDatabaseMetadata(void);

#endif   /* CONT_SCHEDULER_H */
