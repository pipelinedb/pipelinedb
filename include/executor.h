/*-------------------------------------------------------------------------
 *
 * executor.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PIPELINEDB_EXECUTOR
#define PIPELINEDB_EXECUTOR

#include "postgres.h"

#include "nodes/execnodes.h"
#include "pipeline_query.h"
#include "reader.h"
#include "scheduler.h"
#include "stats.h"
#include "storage/lockdefs.h"
#include "tcop/dest.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"

extern Oid PipelineExecLockRelationOid;

extern MemoryContext ContQueryTransactionContext;
extern MemoryContext ContQueryBatchContext;

typedef struct ContQueryState
{
	Oid query_id;
	ContQuery *query;
	MemoryContext state_cxt;
	MemoryContext tmp_cxt;
	ProcStatsEntry *stats;
} ContQueryState;

typedef struct BatchReceiver
{
	Tuplestorestate *buffer;
	void (*flush) (struct BatchReceiver *self, TupleTableSlot *slot);
} BatchReceiver;

typedef struct ContExecutor ContExecutor;
typedef ContQueryState *(*ContQueryStateInit) (ContExecutor *exec, ContQueryState *state);
typedef Relation ContExecutionLock;

struct ContExecutor
{
	MemoryContext cxt;

	ContQueryProcType ptype;
	char *pname;

	Bitmapset *all_queries;
	Bitmapset *exec_queries;

	ipc_tuple_reader_batch *batch;

	Oid curr_query_id;
	ContQueryState *curr_query;
	ContQueryState *states[MAX_CQS];
	ContQueryStateInit initfn;
	ContExecutionLock lock;
};

extern ContExecutionLock AcquireContExecutionLock(LOCKMODE mode);
extern void ReleaseContExecutionLock(ContExecutionLock lock);

extern ContExecutor *ContExecutorNew(ContQueryStateInit initfn);
extern void ContExecutorDestroy(ContExecutor *exec);
extern void ContExecutorStartBatch(ContExecutor *exec, int timeout);
extern Oid ContExecutorStartNextQuery(ContExecutor *exec, int timeout);
extern void ContExecutorPurgeQuery(ContExecutor *exec);
extern void *ContExecutorIterate(ContExecutor *exec, int *len);
extern void ContExecutorEndQuery(ContExecutor *exec);
extern void ContExecutorEndBatch(ContExecutor *exec, bool commit);
extern void ContExecutorAbortQuery(ContExecutor *exec);

extern void ExecuteContPlan(EState *estate, PlanState *planstate,
			bool use_parallel_mode,
			CmdType operation,
			bool sendTuples,
			uint64 numberTuples,
			ScanDirection direction,
			DestReceiver *dest,
			bool execute_once);

#endif
