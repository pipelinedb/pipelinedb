/*-------------------------------------------------------------------------
 *
 * cvmetadata.c
 *	  commands specific to PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cvmetadata.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/toasting.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqproc.h"
#include "pipeline/cqwindow.h"
#include "pipeline/streambuf.h"
#include "postmaster/bgworker.h"
#include "regex/regex.h"
#include "storage/spin.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "miscadmin.h"

#define SLEEP_TIMEOUT 2000

static HTAB *CQProcTable = NULL;
static slock_t CQProcTableMutex;

/*
 * cv_metadata_hash_function
 *
 * Identity hash function
 */
static
uint32
cqproc_state_hash_function(const void *key, Size keysize)
{
	uint32 id = *((uint32*) key);
	return id;
}

/*
 * InitCQMetadataTable
 *
 * Initialize global shared-memory buffer that stores the number of processes
 * in each CQ process group
 */
void
InitCQProcTable(void)
{
	HASHCTL		info;
	/*
	 * Each continuous view has at least 2 concurrent processes (1 worker and 1 combiner)
	 * num_concurrent_cv is set to half that value.
	 * max_concurrent_processes is set as a conf parameter
	*/
	int num_concurrent_cv = max_worker_processes / 2;

	info.keysize = sizeof(uint32);
	info.hash = cqproc_state_hash_function;
	info.entrysize = sizeof(CQProcState);

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	/*
	 * We don't need to acquire CVMetadataLock lock around this because there is
	 * no initialization work we need to do. ShmemInitHash does all of that
	 * atomically for us.
	 */
	CQProcTable = ShmemInitHash("CQProcStateHash",
							  num_concurrent_cv, num_concurrent_cv,
							  &info,
							  HASH_ELEM | HASH_FUNCTION);
	SpinLockInit(&CQProcTableMutex);

	LWLockRelease(PipelineMetadataLock);

}

/*
 * GetProcessGroupSizeFromCatalog
 */
int
GetProcessGroupSizeFromCatalog(RangeVar* rv)
{
	HeapTuple tuple;
	Form_pipeline_query row;
	/* Initialize the counter to 1 for the combiner proc. */
	int pg_size = 1;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "continuous view \"%s\" does not exist",
				rv->relname);

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	/* Add number of worker processes. */
	pg_size += row->parallelism;

	ReleaseSysCache(tuple);

	/* Add GC process, if needed */
	if (IsSlidingWindowContinuousView(rv))
		pg_size += 1;

	return pg_size;
}

/*
 * GetProcessGroupSize
 *
 * Returns the number of processes that form
 * the process group for a continuous query
 *
 */
int
GetProcessGroupSize(int32 id)
{
	CQProcState *entry;

	entry = GetCQProcState(id);
	Assert(entry);
	return entry->pg_size;
}

/*
 * EntryAlloc
 *
 * Allocate an entry in the shared memory
 * hash table. Returns the entry if it exists
 *
 */
CQProcState*
EntryAlloc(int32 id, int pg_size)
{
	CQProcState	*entry;
	bool		found;

	/* Find or create an entry with desired hash code */
	entry = (CQProcState *) hash_search(CQProcTable, &id, HASH_ENTER, &found);
	Assert(entry);

	if (found)
		return NULL;

	/* New entry, initialize it with the process group size */
	entry->pg_size = pg_size;
	/* New entry, No processes are active on creation */
	entry->pg_count = 0;
	/* New entry, and is active the moment this entry is created */
	entry->active = true;

	return entry;
}

/*
 * EntryRemove
 *
 * Remove an entry in the shared memory
 * hash table.
 *
 */
void
EntryRemove(int32 id)
{
	/* Remove the entry from the hash table. */
	hash_search(CQProcTable, &id, HASH_REMOVE, NULL);
}

/*
 * GetCVMetadata
 *
 * Return the entry based on a key
 */
CQProcState*
GetCQProcState(int32 id)
{
	CQProcState  *entry;
	bool found;

	entry = (CQProcState *) hash_search(CQProcTable, &id, HASH_FIND, &found);
	if (!entry)
	{
		elog(LOG,"entry for cvid %d not found in the metadata hash table", id);
		return NULL;
	}
	return entry;
}

/*
 * GetProcessGroupCount
 *
 * Return the current
 * process group count for the given cv
 *
 */
int
GetProcessGroupCount(int32 id)
{
	CQProcState  *entry;

	entry = GetCQProcState(id);
	if (entry == NULL)
	{
		return -1;
	}
	return entry->pg_count;
}

/*
 * DecrementProcessGroupCount
 *
 * Decrement the process group count
 * Called from sub processes
 */
void
DecrementProcessGroupCount(int32 id)
{
	CQProcState *entry;

	SpinLockAcquire(&CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->pg_count--;
	SpinLockRelease(&CQProcTableMutex);
}

/*
 * IncrementProcessGroupCount
 *
 * Increment the process group count
 * Called from caller processes
 */
void
IncrementProcessGroupCount(int32 id)
{
	CQProcState *entry;

	SpinLockRelease(&CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->pg_count++;
	SpinLockRelease(&CQProcTableMutex);
}

/*
 * SetActiveFlag
 *
 * Sets the flag insicating whether the process is active
 * or not
 */
void
SetActiveFlag(int32 id, bool flag)
{
	CQProcState *entry;

	SpinLockRelease(&CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->active = flag;
	SpinLockRelease(&CQProcTableMutex);
}

/*
 * GetActiveFlagPtr
 */
bool *
GetActiveFlagPtr(int32 id)
{
	CQProcState *entry;

	entry = GetCQProcState(id);
	Assert(entry);
	return &entry->active;
}

/*
 * get_stopped_proc_count
 */
static int
get_stopped_proc_count(CQProcState *entry)
{
	int count = 0;
	pid_t pid;
	count += WaitForBackgroundWorkerStartup(&entry->combiner, &pid) == BGWH_STOPPED;
	count += WaitForBackgroundWorkerStartup(&entry->worker, &pid) == BGWH_STOPPED;
	return count;
}

/*
 * WaitForCQProcessStart
 *
 * Block on the process group count till
 * it reaches 0. This enables the activate cv
 * to be synchronous
 */
bool
WaitForCQProcsToStart(int32 id)
{
	CQProcState *entry = GetCQProcState(id);
	int err_count;

	while (true)
	{
		err_count = 0;
		if (entry->pg_count == entry->pg_size)
			break;
		err_count = get_stopped_proc_count(entry);
		if (entry->pg_count + err_count == entry->pg_size)
			break;
		pg_usleep(SLEEP_TIMEOUT);
	}

	return err_count == 0;
}

/*
 * WaitForCQProcessEnd
 *
 * Block on the process group count till
 * it reaches pg_size. This enables the deactivate cv
 * to be synchronous
 */
void
WaitForCQProcsToTerminate(int32 id)
{
	CQProcState *entry = GetCQProcState(id);
	while (true)
	{
		if (entry->pg_count == 0)
			break;
		if (get_stopped_proc_count(entry) == entry->pg_size)
			break;
		pg_usleep(SLEEP_TIMEOUT);
	}
}

/*
 * TerminateCQProcesses
 */
void
TerminateCQProcs(int32 id)
{
	CQProcState *entry = GetCQProcState(id);
	TerminateBackgroundWorker(&entry->combiner);
	TerminateBackgroundWorker(&entry->worker);
	TerminateBackgroundWorker(&entry->gc);
}

/*
 * DidCQWorkerCrash
 */
bool
DidCQWorkerCrash(int32 id)
{
	CQProcState *entry = GetCQProcState(id);
	pid_t pid;
	return WaitForBackgroundWorkerStartup(&entry->worker, &pid) == BGWH_STOPPED && !entry->worker_done;
}

/*
 * SetCQWorkerDoneFlag
 */
void
SetCQWorkerDoneFlag(int32 id)
{
	CQProcState *entry = GetCQProcState(id);
	entry->worker_done = true;
}
