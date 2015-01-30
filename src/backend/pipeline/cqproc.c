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
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/toasting.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/paths.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqplan.h"
#include "pipeline/cqproc.h"
#include "pipeline/cqwindow.h"
#include "pipeline/miscutils.h"
#include "pipeline/streambuf.h"
#include "postmaster/bgworker.h"
#include "regex/regex.h"
#include "storage/spalloc.h"
#include "storage/spin.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define SOCKET_PREFIX "pipeline_"
#define SLEEP_TIMEOUT (2 * 1000)
#define RECOVERY_TIME 1
#define NUM_WORKERS(entry) ((entry)->pg_size - 1)

typedef struct CQProcRunArgs
{
	NameData cvname;
	NameData dbname;
	CQProcessType ptype;
	ContinuousViewState state;
	char *query;
	int worker_id;
} CQProcRunArgs;

static HTAB *CQProcTable = NULL;
static slock_t *CQProcMutex = NULL;

/*
 * InitCQProcState
 *
 * Initialize global shared-memory buffer that stores the number of processes
 * in each CQ process group
 */
void
InitCQProcState(void)
{
	HASHCTL info;
	bool found;
	/*
	 * Each continuous view has at least 2 concurrent processes (1 worker and 1 combiner)
	 * num_concurrent_cv is set to half that value.
	 * max_concurrent_processes is set as a conf parameter
	*/
	int num_concurrent_cv = max_worker_processes / 2;

	info.keysize = sizeof(int);
	info.entrysize = sizeof(CQProcEntry);

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	CQProcTable = ShmemInitHash("CQProcTable",
			num_concurrent_cv, num_concurrent_cv,
			&info,
			HASH_ELEM);
	CQProcMutex = ShmemInitStruct("CQProcMutex", sizeof(slock_t), &found);

	if (!found)
		SpinLockInit(CQProcMutex);

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
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	/* Add number of worker processes. */
	pg_size += row->parallelism;

	ReleaseSysCache(tuple);

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
GetProcessGroupSize(int id)
{
	CQProcEntry *entry;

	entry = GetCQProcEntry(id);
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
CQProcEntry*
CQProcEntryCreate(int id, int pg_size)
{
	CQProcEntry	*entry;
	bool		found;

	/* Find or create an entry with desired hash code */
	entry = (CQProcEntry *) hash_search(CQProcTable, &id, HASH_ENTER, &found);
	Assert(entry);

	if (found)
		return NULL;

	/* New entry, initialize it with the process group size */
	entry->pg_size = pg_size;
	/* New entry, and is active the moment this entry is created */
	entry->active = true;

	entry->shm_query = NULL;

	entry->combiner.last_pid = 0;
	entry->workers = spalloc0(sizeof(CQBackgroundWorkerHandle) * NUM_WORKERS(entry));

	/* socket names are "pipeline_<hex>" */
	strcpy(entry->sock_name, SOCKET_PREFIX);
	strcpy(&entry->sock_name[strlen(SOCKET_PREFIX)], random_hex(10));

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
CQProcEntryRemove(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);

	if (entry)
	{
		spfree(entry->workers);
		spfree(entry->shm_query);
	}

	/* Remove the entry from the hash table. */
	hash_search(CQProcTable, &id, HASH_REMOVE, NULL);
}

/*
 * GetCVMetadata
 *
 * Return the entry based on a key
 */
CQProcEntry*
GetCQProcEntry(int id)
{
	bool found;
	return (CQProcEntry *) hash_search(CQProcTable, &id, HASH_FIND, &found);
}

/*
 * SetActiveFlag
 *
 * Sets the flag insicating whether the process is active
 * or not
 */
void
SetActiveFlag(int id, bool flag)
{
	CQProcEntry *entry;

	SpinLockAcquire(CQProcMutex);
	entry = GetCQProcEntry(id);
	Assert(entry);
	entry->active = flag;
	SpinLockRelease(CQProcMutex);
}

/*
 * GetActiveFlagPtr
 */
bool *
GetActiveFlagPtr(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	Assert(entry);
	return &entry->active;
}

void
MarkCombinerAsRunning(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	entry->combiner.last_pid = MyProcPid;
}

void
MarkWorkerAsRunning(int id, int worker_id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	entry->workers[worker_id].last_pid = MyProcPid;
}

/*
 * get_stopped_proc_count
 */
static int
get_stopped_proc_count(CQProcEntry *entry)
{
	int count = 0;
	int i;
	pid_t pid;

	count += WaitForBackgroundWorkerStartup((BackgroundWorkerHandle *) &entry->combiner, &pid) == BGWH_STOPPED;
	for (i = 0; i < NUM_WORKERS(entry); i++)
		count += WaitForBackgroundWorkerStartup((BackgroundWorkerHandle *) &entry->workers[i], &pid) == BGWH_STOPPED;

	return count;
}

/*
 * WaitForCQProcsToStart
 *
 * Block on the process group count till
 * it reaches 0. This enables the ACTIVATE
 * to be synchronous
 */
bool
WaitForCQProcsToStart(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	int err_count;
	int succ_count;
	pid_t *worker_pids;
	int i;

	while (true)
	{
		err_count = succ_count = 0;
		succ_count += GetCombinerPid(id) == entry->combiner.last_pid;
		worker_pids = GetWorkerPids(id);

		for (i = 0; i < NUM_WORKERS(entry); i++)
			succ_count += worker_pids[i] == entry->workers[i].last_pid;

		pfree(worker_pids);

		if (succ_count == entry->pg_size)
			break;

		err_count = get_stopped_proc_count(entry);

		if (err_count + succ_count == entry->pg_size)
			break;

		pg_usleep(SLEEP_TIMEOUT);
	}

	return err_count == 0;
}

/*
 * WaitForCQProcsToTerminate
 *
 * Block on the process group count till
 * it reaches pg_size. This enables the DEACTIVATE
 * to be synchronous
 */
void
WaitForCQProcsToTerminate(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	while (true)
	{
		if (get_stopped_proc_count(entry) == entry->pg_size)
			break;
		pg_usleep(SLEEP_TIMEOUT);
	}
}

/*
 * TerminateCQProcs
 */
void
TerminateCQProcs(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	int i;

	TerminateBackgroundWorker((BackgroundWorkerHandle *) &entry->combiner);
	for (i = 0; i < NUM_WORKERS(entry); i++)
		TerminateBackgroundWorker((BackgroundWorkerHandle *) &entry->workers[i]);
}

/*
 * IsCombinerRunning
 */
bool
IsCombinerRunning(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	pid_t pid;
	if (WaitForBackgroundWorkerStartup((BackgroundWorkerHandle *) &entry->combiner, &pid) != BGWH_STARTED)
		return false;
	return pid == entry->combiner.last_pid;
}

/*
 * AreCQWorkersStopped
 */
bool
AreCQWorkersStopped(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	pid_t pid;
	int i;

	for (i = 0; i < NUM_WORKERS(entry); i++)
		if (WaitForBackgroundWorkerStartup((BackgroundWorkerHandle *) &entry->workers[i], &pid) != BGWH_STOPPED)
			return false;

	return true;
}

/*
 * EnableCQProcsRecovery
 */
void
EnableCQProcsRecovery(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	int i;

	ChangeBackgroundWorkerRestartState((BackgroundWorkerHandle *) &entry->combiner, false, RECOVERY_TIME);
	for (i = 0; i < NUM_WORKERS(entry); i++)
		ChangeBackgroundWorkerRestartState((BackgroundWorkerHandle *) &entry->workers[i], false, RECOVERY_TIME);
}

/*
 * DisableCQProcsRecovery
 */
void
DisableCQProcsRecovery(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	int i;

	ChangeBackgroundWorkerRestartState((BackgroundWorkerHandle *) &entry->combiner, true, 0);
	for (i = 0; i < NUM_WORKERS(entry); i++)
		ChangeBackgroundWorkerRestartState((BackgroundWorkerHandle *) &entry->workers[i], true, 0);
}

/*
 * cq_bg_main
 */
static void
cq_bg_main(Datum d, char *additional, Size additionalsize)
{
	MemoryContext oldcontext;
	CommandDest dest = DestRemote;
	DestReceiver *receiver;
	Portal portal;
	CQProcRunArgs *args;
	int16 format;
	PlannedStmt *plan;
	char *sql;
	char completionTag[COMPLETION_TAG_BUFSIZE];
	char *cvname;
	char *matrelname;
	MemoryContext planctxt = AllocSetContextCreate(TopMemoryContext,
													"RunCQPlanContext",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	args = (CQProcRunArgs *) additional;

	/*
	 * This will happen when the BG worker is being respawned after a full
	 * reset cycle.
	 */
	if (!IsValidSPallocMemory(args->query))
		return;

	/* Set all globals variables */
	IsCombiner = args->ptype == CQCombiner;
	IsWorker = args->ptype == CQWorker;
	MyCQId = args->state.id;
	MyWorkerId = args->worker_id;

	/*
	 * XXX(usmanm): Is this kosher to do? We need this so in case of failed
	 * ACTIVATEs, the postmaster can kill the CQ's bg procs. Previously
	 * we would only unblock signals when waiting on the process' GlobalStreamBuffer
	 * latch.
	 */
	BackgroundWorkerUnblockSignals();

	/*
	 * 0. Give this process access to the database
	 */
	BackgroundWorkerInitializeConnection(NameStr(args->dbname), NULL);

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(planctxt);

	/*
	 * 1. Plan the continuous query
	 */
	cvname = NameStr(args->cvname);
	matrelname = NameStr(args->state.matrelname);

	sql = pstrdup(args->query);
	plan = GetCQPlan(cvname, sql, &args->state, matrelname);

	/*
	 * 2. Set up the portal to run it in
	 */
	portal = CreatePortal(cvname, true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
					  NULL,
					  sql,
					  "SELECT",
					  list_make1(plan),
					  NULL);

	receiver = CreateDestReceiver(dest);
	format = 0;
	PortalStart(portal, NULL, 0, InvalidSnapshot);
	PortalSetResultFormat(portal, 1, &format);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * 3. Run the continuous query until it's deactivated
	 */
	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 receiver,
					 receiver,
					 completionTag);


	(*receiver->rDestroy) (receiver);

	PortalDrop(portal, false);

	CommitTransactionCommand();
}

static char *
get_cq_proc_type_name(CQProcessType ptype)
{
	switch(ptype)
	{
	case CQCombiner:
		return " [combiner]";
	case CQWorker:
		return " [worker]";
	default:
		elog(ERROR, "unknown CQProcessType %d", ptype);
	}

	return NULL;
}

static bool
run_cq_proc(CQProcessType ptype, const char *cvname, ContinuousViewState *state, BackgroundWorkerHandle *bg_handle, char *query, int worker_id)
{
	BackgroundWorker worker;
	CQProcRunArgs args;
	BackgroundWorkerHandle *worker_handle;
	char *procName = get_cq_proc_type_name(ptype);
	bool success;

	strcpy(worker.bgw_name, cvname);
	append_suffix(worker.bgw_name, procName, NAMEDATALEN);

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = cq_bg_main;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_let_crash = true;
	worker.bgw_additional_size = sizeof(CQProcRunArgs);
	worker.bgw_cvid = state->id;

	args.ptype = state->ptype = ptype;
	args.state = *state;
	namestrcpy(&args.cvname, cvname);
	namestrcpy(&args.dbname, MyProcPort->database_name);

	args.query = query;
	args.worker_id = worker_id;

	memcpy(worker.bgw_additional_arg, &args, worker.bgw_additional_size);

	success = RegisterDynamicBackgroundWorker(&worker, &worker_handle);

	*bg_handle = *worker_handle;

	return success;
}

/*
 * RunCQProcs
 */
void
RunCQProcs(const char *cvname, void *_state, CQProcEntry *entry)
{
	ContinuousViewState *state = (ContinuousViewState *) _state;
	int i;

	if (entry->shm_query == NULL)
	{
		char *q = GetQueryString((char *) cvname, true);
		entry->shm_query = spalloc(strlen(q) + 1);
		strcpy(entry->shm_query, q);
	}

	run_cq_proc(CQCombiner, cvname, state, (BackgroundWorkerHandle *) &entry->combiner, entry->shm_query, -1);
	for (i = 0; i < NUM_WORKERS(entry); i++)
		run_cq_proc(CQWorker, cvname, state, (BackgroundWorkerHandle *) &entry->workers[i], entry->shm_query, i);
}

/*
 * GetSocketName
 */
char *
GetSocketName(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	return entry->sock_name;
}

/*
 * GetCombinerPid
 */
pid_t
GetCombinerPid(int id)
{
	CQProcEntry *entry = GetCQProcEntry(id);
	pid_t pid;
	GetBackgroundWorkerPid((BackgroundWorkerHandle *) &entry->combiner, &pid);
	return pid;
}

/*
 * GetWorkerPids
 */
pid_t *
GetWorkerPids(int id)
{
	int i;
	CQProcEntry *entry = GetCQProcEntry(id);
	pid_t *pids = palloc0(NUM_WORKERS(entry) * sizeof(pid_t));

	for (i = 0; i < NUM_WORKERS(entry); i++)
		GetBackgroundWorkerPid((BackgroundWorkerHandle *) &entry->workers[i], &pids[i]);

	return pids;
}
