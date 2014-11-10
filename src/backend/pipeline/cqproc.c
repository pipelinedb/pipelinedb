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
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqplan.h"
#include "pipeline/cqproc.h"
#include "pipeline/cqwindow.h"
#include "pipeline/decode.h"
#include "pipeline/streambuf.h"
#include "postmaster/bgworker.h"
#include "regex/regex.h"
#include "storage/spalloc.h"
#include "storage/spin.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define SLEEP_TIMEOUT 2000

typedef struct CQProcArgs
{
	NameData cvname;
	NameData dbname;
	CQProcessType ptype;
	ContinuousViewState state;
	char *query;
} CQProcArgs;

static HTAB *CQProcTable = NULL;
static slock_t *CQProcTableMutex = NULL;

/*
 * InitCQMetadataTable
 *
 * Initialize global shared-memory buffer that stores the number of processes
 * in each CQ process group
 */
void
InitCQProcTable(void)
{
	HASHCTL info;
	bool found;
	/*
	 * Each continuous view has at least 2 concurrent processes (1 worker and 1 combiner)
	 * num_concurrent_cv is set to half that value.
	 * max_concurrent_processes is set as a conf parameter
	*/
	int num_concurrent_cv = max_worker_processes / 2;

	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(CQProcState);

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	CQProcTable = ShmemInitHash("CQProcStateHash",
							  num_concurrent_cv, num_concurrent_cv,
							  &info,
							  HASH_ELEM);
	CQProcTableMutex = ShmemInitStruct("CQProcTableMutex", sizeof(slock_t) , &found);
	if (!found)
		SpinLockInit(CQProcTableMutex);

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

	SpinLockAcquire(CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->pg_count--;
	SpinLockRelease(CQProcTableMutex);
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

	SpinLockRelease(CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->pg_count++;
	SpinLockRelease(CQProcTableMutex);
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

	SpinLockRelease(CQProcTableMutex);
	entry = GetCQProcState(id);
	Assert(entry);
	entry->active = flag;
	SpinLockRelease(CQProcTableMutex);
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

static PlannedStmt *
get_plan_from_stmt(char *cvname, Node *node, const char *sql, ContinuousViewState *state, bool is_combine)
{
	List		*querytree_list;
	List		*plantree_list;
	Query		*query;
	PlannedStmt	*plan;

	querytree_list = pg_analyze_and_rewrite(node, sql, NULL, 0);
	Assert(list_length(querytree_list) == 1);

	query = linitial(querytree_list);

	query->is_continuous = IsA(node, SelectStmt);
	query->is_combine = is_combine;
	query->cq_target = makeRangeVar(NULL, cvname, -1);
	query->cq_state = state;

	plantree_list = pg_plan_queries(querytree_list, 0, NULL);
	Assert(list_length(plantree_list) == 1);
	plan = (PlannedStmt *) linitial(plantree_list);

	plan->is_continuous = true;
	plan->cq_target = makeRangeVar(NULL, cvname, -1);
	plan->cq_state = palloc(sizeof(ContinuousViewState));
	memcpy(plan->cq_state, query->cq_state, sizeof(ContinuousViewState));

	return plan;
}

static PlannedStmt*
get_gc_plan(char *cvname, const char *sql, ContinuousViewState *state)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;
	DeleteStmt	*delete_stmt;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	delete_stmt = GetDeleteStmtForGC(cvname, selectstmt);

	if (delete_stmt == NULL)
		return NULL;

	return get_plan_from_stmt(cvname, (Node *) delete_stmt, NULL, state, false);
}

static PlannedStmt*
get_worker_plan(char *cvname, const char *sql, ContinuousViewState *state)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = GetSelectStmtForCQWorker(selectstmt, NULL);
	selectstmt->forContinuousView = true;

	return get_plan_from_stmt(cvname, (Node *) selectstmt, sql, state, false);
}

static PlannedStmt*
get_combiner_plan(char *cvname, const char *sql, ContinuousViewState *state)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = GetSelectStmtForCQCombiner(selectstmt);
	selectstmt->forContinuousView = true;

	return get_plan_from_stmt(cvname, (Node *) selectstmt, sql, state, true);
}

/*
 * Run CQ combiner or worker in a background process with the postmaster as its parent
 */
static void
run_cq(Datum d, char *additional, Size additionalsize)
{
	MemoryContext oldcontext;
	CommandDest dest = DestRemote;
	DestReceiver *receiver;
	Portal portal;
	CQProcArgs args;
	int16 format;
	PlannedStmt *plan;
	ContinuousViewState state;
	char *sql;
	char completionTag[COMPLETION_TAG_BUFSIZE];
	char *cvname;
	MemoryContext planctxt = AllocSetContextCreate(TopMemoryContext,
													"RunCQPlanContext",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * XXX(usmanm): Is this kosher to do? We need this so in case of failed
	 * ACTIVATEs, the postmaster can kill the CQ's bg procs. Previously
	 * we would only unblock signals when waiting on the process' GlobalStreamBuffer
	 * latch.
	 */
	BackgroundWorkerUnblockSignals();

	memcpy(&args, additional, additionalsize);
	state = args.state;
	state.ptype = args.ptype;

	/*
	 * 0. Give this process access to the database
	 */
	BackgroundWorkerInitializeConnection(NameStr(args.dbname), NULL);
	InitDecoderCache();

	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(planctxt);

	/*
	 * 1. Plan the continuous query
	 */
	cvname = NameStr(args.cvname);
	sql = pstrdup(args.query);
	spfree(args.query);

	switch(state.ptype)
	{
		case CQCombiner:
			plan = get_combiner_plan(cvname, sql, &state);
			break;
		case CQWorker:
			plan = get_worker_plan(cvname, sql, &state);
			break;
		case CQGarbageCollector:
			plan = get_gc_plan(cvname, sql, &state);
			break;
		default:
			elog(ERROR, "unrecognized CQ process type: %d", state.ptype);
	}

	/* No plan? Terminate CQ process. */
	if (plan == NULL)
	{
		return;
	}

	SetCQPlanRefs(plan);

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
	case CQGarbageCollector:
		return " [gc]";
	default:
		elog(ERROR, "unknown CQProcessType %d", ptype);
	}

	return NULL;
}

bool
RunContinuousQueryProcess(CQProcessType ptype, const char *cvname, ContinuousViewState *state, BackgroundWorkerHandle *bg_handle)
{
	BackgroundWorker worker;
	CQProcArgs args;
	BackgroundWorkerHandle *worker_handle;
	char *procName = get_cq_proc_type_name(ptype);
	char *query = GetQueryString(cvname, true);
	char *shmem_query;
	bool success;

	/* TODO(usmanm): Make sure the name doesn't go beyond 64 bytes */
	memcpy(worker.bgw_name, cvname, strlen(cvname) + 1);
	memcpy(&worker.bgw_name[strlen(cvname)], procName, strlen(procName) + 1);

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = run_cq;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_let_crash = true;
	worker.bgw_additional_size = sizeof(CQProcArgs);
	worker.bgw_cvid = state->id;

	args.state = *state;
	args.ptype = ptype;
	namestrcpy(&args.cvname, cvname);
	namestrcpy(&args.dbname, MyProcPort->database_name);

	shmem_query = spalloc(strlen(query) + 1);
	args.query = shmem_query;
	memcpy(args.query, query, strlen(query) + 1);

	memcpy(worker.bgw_additional_arg, &args, worker.bgw_additional_size);

	success = RegisterDynamicBackgroundWorker(&worker, &worker_handle);

	*bg_handle = *worker_handle;

	return success;
}
