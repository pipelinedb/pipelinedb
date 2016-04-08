/*-------------------------------------------------------------------------
 *
 * cont_scheduler.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/backend/pipeline/cont_scheduler.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pipeline_database.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/dsm_cqueue.h"
#include "pipeline/miscutils.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/shm_alloc.h"
#include "storage/sinval.h"
#include "storage/spin.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "pipeline/trigger/trigger.h"
#include "pipeline/trigger/triggerfuncs.h"

#define NUM_BG_WORKERS_PER_DB (continuous_query_num_workers + continuous_query_num_combiners)
#define NUM_LOCKS_PER_DB (NUM_BG_WORKERS_PER_DB + 1) /* single lock for all adhoc processes */

#define BG_PROC_STATUS_TIMEOUT 1000
#define CQ_STATE_CHANGE_TIMEOUT 5000

typedef struct DatabaseEntry
{
	Oid oid;
	NameData name;
	bool active;
} DatabaseEntry;

static List *DatabaseList = NIL;

/* per-process structures */
ContQueryProc *MyContQueryProc = NULL;

/* flags to tell if we are in a continuous query process */
static bool am_cont_scheduler = false;
static bool am_cont_worker = false;
static bool am_cont_adhoc = false;
static bool am_cont_trigger = false;
bool am_cont_combiner = false;

/* guc parameters */
bool continuous_queries_enabled;
bool continuous_queries_adhoc_enabled;
bool continuous_query_crash_recovery;
int  continuous_query_num_combiners;
int  continuous_query_num_workers;
int  continuous_query_batch_size;
int  continuous_query_max_wait;
int  continuous_query_combiner_work_mem;
int  continuous_query_combiner_synchronous_commit;
int  continuous_query_ipc_shared_mem;
int continuous_query_commit_interval;
double continuous_query_proc_priority;

/* memory context for long-lived data */
static MemoryContext ContQuerySchedulerContext;

/* flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGINT = false;

typedef struct LWLockSlot
{
	LWLock lock;
	bool   used;
} LWLockSlot;

/* the main continuous process scheduler shmem struct */
typedef struct ContQuerySchedulerShmemStruct
{
	pid_t pid;

	HTAB *db_table;

	ContQueryRunParams params;

	int tranche_id;
	LWLockTranche tranche;
	LWLockSlot *locks;
} ContQuerySchedulerShmemStruct;

static ContQuerySchedulerShmemStruct *ContQuerySchedulerShmem;

NON_EXEC_STATIC void ContQuerySchedulerMain(int argc, char *argv[]) __attribute__((noreturn));

static void
update_run_params(void)
{
	ContQueryRunParams *params = GetContQueryRunParams();
	params->batch_size = continuous_query_batch_size;
	params->max_wait = continuous_query_max_wait;
}

static Size
ContQueryDatabaseMetadataSize(void)
{
	return (sizeof(ContQueryDatabaseMetadata) +
			(sizeof(ContQueryProc) * NUM_BG_WORKERS_PER_DB) +
			(sizeof(ContQueryProc) * max_worker_processes));
}

/* shared memory stuff */
Size
ContQuerySchedulerShmemSize(void)
{
	return MAXALIGN(sizeof(ContQuerySchedulerShmemStruct) + (sizeof(LWLockSlot) * max_worker_processes));
}

void
ContQuerySchedulerShmemInit(void)
{
	bool found;
	Size size = ContQuerySchedulerShmemSize();

	ContQuerySchedulerShmem = ShmemInitStruct("ContQuerySchedulerShmem", size, &found);

	if (!found)
	{
		HASHCTL ctl;
		char *ptr;
		int i;

		MemSet(ContQuerySchedulerShmem, 0, size);

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = ContQueryDatabaseMetadataSize();
		ctl.hash = oid_hash;

		ContQuerySchedulerShmem->db_table = ShmemInitHash("ContQueryDatabaseMetadata", 4, 16, &ctl, HASH_ELEM | HASH_FUNCTION);

		update_run_params();

		ContQuerySchedulerShmem->tranche_id = LWLockNewTrancheId();

		ptr = (void *) ContQuerySchedulerShmem;
		ptr += sizeof(ContQuerySchedulerShmemStruct);
		ContQuerySchedulerShmem->locks = (LWLockSlot *) ptr;

		ContQuerySchedulerShmem->tranche.name = "ContProcLWLocks";
		ContQuerySchedulerShmem->tranche.array_base = ptr;
		ContQuerySchedulerShmem->tranche.array_stride = sizeof(LWLockSlot);

		for (i = 0; i < max_worker_processes; i++)
		{
			LWLockSlot *slot = &ContQuerySchedulerShmem->locks[i];
			LWLockInitialize(&slot->lock, ContQuerySchedulerShmem->tranche_id);
		}
	}

	LWLockRegisterTranche(ContQuerySchedulerShmem->tranche_id, &ContQuerySchedulerShmem->tranche);
}

ContQueryRunParams *
GetContQueryRunParams(void)
{
	return &ContQuerySchedulerShmem->params;
}

char *
GetContQueryProcName(ContQueryProc *proc)
{
	char *buf = palloc0(NAMEDATALEN);

	switch (proc->type)
	{
		case COMBINER:
			sprintf(buf, "combiner%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case WORKER:
			sprintf(buf, "worker%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case ADHOC:
			sprintf(buf, "adhoc [%s]", NameStr(proc->db_meta->db_name));
			break;
		case TRIG:
			sprintf(buf, "trigger [%s]", NameStr(proc->db_meta->db_name));
			break;
		case SCHEDULER:
			return pstrdup("scheduler");
			break;
	}

	return buf;
}

/* status inquiry functions */
bool
IsContQuerySchedulerProcess(void)
{
	return am_cont_scheduler;
}

bool
IsContQueryWorkerProcess(void)
{
	return am_cont_worker;
}

bool
IsContQueryAdhocProcess(void)
{
	return am_cont_adhoc;
}

bool
IsContQueryTriggerProcess(void)
{
	return am_cont_trigger;
}

bool
IsContQueryCombinerProcess(void)
{
	return am_cont_combiner;
}

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the continuous query launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
cqscheduler_forkexec(void)
{
	char *av[10];
	int ac = 0;

	av[ac++] = "pipeline-server";
	av[ac++] = "--forkcqscheduler";
	av[ac++] = NULL; /* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
ContQuerySchdulerIAm(void)
{
	am_cont_scheduler = true;
}
#endif

pid_t
StartContQueryScheduler(void)
{
	pid_t pid;

#ifdef EXEC_BACKEND
	switch ((pid = cqscheduler_forkexec()))
#else
	switch ((pid = fork_process()))
#endif
	{
	case -1:
		ereport(LOG,
				(errmsg("could not fork continuous query scheduler process: %m")));
		return 0;

#ifndef EXEC_BACKEND
	case 0:
		InitPostmasterChild();
		/* in postmaster child ... */
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Lose the postmaster's on-exit routines */
		on_exit_reset();

		ContQuerySchedulerMain(0, NULL);
		break;
#endif
	default:
		return pid;
	}

	/* shouldn't get here */
	return 0;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
sighup_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	if (MyProc)
		SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: time to die */
static void
sigterm_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc)
		SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR2: wake up */
static void
sigusr2_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	if (MyProc)
		SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGINT: refresh database list */
static void
sigint_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGINT = true;
	if (MyProc)
		SetLatch(MyLatch);

	errno = save_errno;
}

static void
refresh_database_list(void)
{
	List *dbs = NIL;
	Relation pg_database;
	Relation pipeline_database;
	HeapScanDesc scan;
	HeapTuple tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	/* We take a AccessExclusiveLock so we don't conflict with any DATABASE commands */
	pg_database = heap_open(DatabaseRelationId, AccessExclusiveLock);
	scan = heap_beginscan_catalog(pg_database, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		MemoryContext oldcxt;
		Form_pg_database row = (Form_pg_database) GETSTRUCT(tup);
		DatabaseEntry *db_entry;

		/* Ignore template databases or ones that don't allow connections. */
		if (row->datistemplate || !row->datallowconn)
			continue;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);

		db_entry = palloc0(sizeof(DatabaseEntry));
		db_entry->oid = HeapTupleGetOid(tup);
		StrNCpy(NameStr(db_entry->name), NameStr(row->datname), NAMEDATALEN);
		dbs = lappend(dbs, db_entry);

		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);

	/* Get enabled flags */
	pipeline_database = heap_open(PipelineDatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(pipeline_database, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pipeline_database row = (Form_pipeline_database) GETSTRUCT(tup);
		ListCell *lc;

		foreach(lc, dbs)
		{
			DatabaseEntry *db_entry = (DatabaseEntry *) lfirst(lc);

			if (db_entry->oid == row->dbid)
				db_entry->active = row->cq_enabled;
		}
	}

	heap_endscan(scan);
	heap_close(pipeline_database, NoLock);

	/* Unlock pg_database at the very end. */
	heap_close(pg_database, NoLock);

	CommitTransactionCommand();

	if (DatabaseList)
		list_free_deep(DatabaseList);

	DatabaseList = dbs;
}

static void
purge_adhoc_queries(void)
{
	int id;
	Bitmapset *view_ids;

	StartTransactionCommand();

	view_ids = GetAdhocContinuousViewIds();

	while ((id = bms_first_member(view_ids)) >= 0)
	{
		ContQuery *cv = GetContQueryForViewId(id);
		CleanupAdhocContinuousView(cv);
	}

	CommitTransactionCommand();
}

static void
cont_bgworker_main(Datum arg)
{
	void (*run) (void);
	ContQueryProc *proc;

#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	proc = MyContQueryProc = (ContQueryProc *) DatumGetPointer(arg);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(NameStr(MyContQueryProc->db_meta->db_name), NULL);

	/* if we got a cancel signal in prior command, quit */
	CHECK_FOR_INTERRUPTS();

	proc->latch = MyLatch;

	switch (proc->type)
	{
		case COMBINER:
			am_cont_combiner = true;
			run = &ContinuousQueryCombinerMain;
			break;
		case WORKER:
			am_cont_worker = true;
			run = &ContinuousQueryWorkerMain;
			break;
		case TRIG:
			am_cont_trigger = true;
			run = &trigger_main;
			break;
		case ADHOC:
			/* Clean up and die. */
			purge_adhoc_queries();
			return;
		default:
			elog(ERROR, "invalid continuous query process type: %d", proc->type);
	}

	if (proc->type != TRIG)
	{
		StartTransactionCommand();

		proc->segment = dsm_attach(proc->db_meta->handle);
		dsm_pin_mapping(proc->segment);

		CommitTransactionCommand();
	}

	elog(LOG, "continuous query process \"%s\" running with pid %d", GetContQueryProcName(proc), MyProcPid);
	pgstat_report_activity(STATE_RUNNING, GetContQueryProcName(proc));

	/* Be nice! Give up some CPU. */
	SetNicePriority();

	/* Initialize process level CQ stats. */
	cq_stat_init(&MyProcCQStats, 0, MyProcPid);

	/* Run the continuous execution function. */
	run();

	/* Purge process level CQ stats. */
	cq_stat_send_purge(0, MyProcPid, IsContQueryWorkerProcess() ? CQ_STAT_WORKER : CQ_STAT_COMBINER);

	if (proc->type != TRIG)
		dsm_detach(proc->segment);

	/* If this isn't a clean termination, exit with a non-zero status code */
	if (!proc->db_meta->terminate)
	{
		elog(LOG, "continuous query process \"%s\" was killed", GetContQueryProcName(proc));
		proc_exit(1);
	}
	else
		elog(LOG, "continuous query process \"%s\" shutting down", GetContQueryProcName(proc));
}

/*
 * wait_for_bg_worker_state
 *
 * We can't use WaitForBackgroundWorkerStartup/WaitForBackgroundWorkerShutdown because
 * the continuous query scheduler isn't a normal backend and so cannot be signaled by
 * the postmaster.
 */
static bool
wait_for_bg_worker_state(BackgroundWorkerHandle *handle, BgwHandleStatus state, int timeoutms)
{
	TimestampTz start = GetCurrentTimestamp();
	pid_t pid;
	bool success = false;

	if (!handle)
		return true;

	while (!TimestampDifferenceExceeds(start, GetCurrentTimestamp(), timeoutms))
	{
		if (GetBackgroundWorkerPid(handle, &pid) == state)
		{
			success = true;
			break;
		}

		pg_usleep(10 * 1000); /* 10ms */
	}

	return success;
}

static void
dsm_cqueue_setup(ContQueryProc *proc)
{
	dsm_cqueue_peek_fn peek_fn;
	dsm_cqueue_pop_fn pop_fn;
	dsm_cqueue_copy_fn cpy_fn;
	void *ptr;
	Size size;
	LWLock *lock;
	int lock_idx;

	if (proc->type == COMBINER)
	{
		peek_fn = &PartialTupleStatePeekFn;
		pop_fn = &PartialTupleStatePopFn;
		cpy_fn = &PartialTupleStateCopyFn;
	}
	else if (proc->type == WORKER)
	{
		peek_fn = &StreamTupleStatePeekFn;
		pop_fn = &StreamTupleStatePopFn;
		cpy_fn = &StreamTupleStateCopyFn;
	}
	else
		return;

	lock_idx = proc->id + proc->db_meta->lock_idx;
	Assert(lock_idx >= 0);

	lock = (LWLock *) &ContQuerySchedulerShmem->locks[lock_idx];

	size = continuous_query_ipc_shared_mem * 1024;
	ptr = (char *) dsm_segment_address(proc->db_meta->segment) + (size * proc->id);
	dsm_cqueue_init(ptr, size, lock);
	dsm_cqueue_set_handlers((dsm_cqueue *) ptr, peek_fn, pop_fn, cpy_fn);
}

static bool
run_cont_bgworker(ContQueryProc *proc)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	bool success;

	dsm_cqueue_setup(proc);

	strcpy(worker.bgw_name, GetContQueryProcName(proc));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_IS_CONT_QUERY_PROC;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = cont_bgworker_main;
	worker.bgw_notify_pid = 0;
	worker.bgw_restart_time = 1; /* recover in 1s */
	worker.bgw_let_crash = false;
	worker.bgw_main_arg = PointerGetDatum(proc);

	success = RegisterDynamicBackgroundWorker(&worker, &handle);

	if (success)
		proc->bgw_handle = handle;
	else
		proc->bgw_handle = NULL;

	return success;
}

static int
get_unused_lock_idx(void)
{
	int i;
	int lock_idx = -1;

	LWLockAcquire(ContQuerySchedulerLock, LW_EXCLUSIVE);

	for (i = 0; i < max_worker_processes; i++)
	{
		LWLockSlot *slot = &ContQuerySchedulerShmem->locks[i];
		if (!slot->used)
		{
			lock_idx = i;
			break;
		}
	}

	Assert(lock_idx != -1);

	for (i = 0; i < NUM_LOCKS_PER_DB; i++)
	{
		LWLockSlot *slot = &ContQuerySchedulerShmem->locks[lock_idx + i];
		Assert(!slot->used);
		slot->used = true;
	}

	LWLockRelease(ContQuerySchedulerLock);

	return lock_idx;
}

static void
release_locks(ContQueryDatabaseMetadata *db_meta)
{
	int i;

	LWLockAcquire(ContQuerySchedulerLock, LW_EXCLUSIVE);

	for (i = 0; i < NUM_LOCKS_PER_DB; i++)
	{
		LWLockSlot *slot = &ContQuerySchedulerShmem->locks[db_meta->lock_idx + i];
		Assert(slot->used);
		slot->used = false;
	}

	LWLockRelease(ContQuerySchedulerLock);
}

static void
wait_for_db_workers(ContQueryDatabaseMetadata *db_meta, BgwHandleStatus state)
{
	ContQueryProc *cqproc;
	int i;

	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
	{
		cqproc = &db_meta->db_procs[i];
		if (!wait_for_bg_worker_state(cqproc->bgw_handle, state, BG_PROC_STATUS_TIMEOUT))
			elog(WARNING, "timed out waiting for continuous query process \"%s\" to reach state %d",
					GetContQueryProcName(cqproc), state);
	}

	cqproc = &db_meta->trigger_proc;
	if (!wait_for_bg_worker_state(cqproc->bgw_handle, state, BG_PROC_STATUS_TIMEOUT))
		elog(WARNING, "timed out waiting for continuous query process \"%s\" to reach state %d",
				GetContQueryProcName(cqproc), state);
}

static void
terminate_database_workers(ContQueryDatabaseMetadata *db_meta)
{
	int i;

	Assert(db_meta->running);

	elog(LOG, "terminating continuous query processes for database: \"%s\"", NameStr(db_meta->db_name));

	db_meta->terminate = true;

	SpinLockAcquire(&db_meta->mutex);

	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
		TerminateBackgroundWorker(db_meta->db_procs[i].bgw_handle);

	if (db_meta->trigger_proc.bgw_handle)
		TerminateBackgroundWorker(db_meta->trigger_proc.bgw_handle);

	wait_for_db_workers(db_meta, BGWH_STOPPED);

	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
		pfree(db_meta->db_procs[i].bgw_handle);

	if (db_meta->trigger_proc.bgw_handle)
		pfree(db_meta->trigger_proc.bgw_handle);

	/* XXX: Force delete segment? */
	dsm_detach(db_meta->segment);
	db_meta->handle = 0;

	release_locks(db_meta);

	db_meta->terminate = false;
	db_meta->running = false;

	SpinLockRelease(&db_meta->mutex);
}

static void
start_database_workers(ContQueryDatabaseMetadata *db_meta)
{
	int slot_idx;
	int group_id;
	ContQueryProc *proc;
	ResourceOwner res;
	bool success = true;

	Assert(!db_meta->running);

	elog(LOG, "starting continuous query processes for database: \"%s\"", NameStr(db_meta->db_name));

	SpinLockAcquire(&db_meta->mutex);

	/* Create dsm_segment for all worker queues */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Database dsm_segment ResourceOwner");

	db_meta->segment = dsm_create(continuous_query_ipc_shared_mem * 1024 * NUM_BG_WORKERS_PER_DB, 0);
	dsm_pin_mapping(db_meta->segment);
	db_meta->handle = dsm_segment_handle(db_meta->segment);

	res = CurrentResourceOwner;
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(res);

	db_meta->terminate = false;

	db_meta->lock_idx = get_unused_lock_idx();

	/* Start worker processes. */
	for (slot_idx = 0, group_id = 0; slot_idx < continuous_query_num_workers; slot_idx++, group_id++)
	{
		proc = &db_meta->db_procs[slot_idx];
		MemSet(proc, 0, sizeof(ContQueryProc));

		proc->type = WORKER;
		proc->id = slot_idx;
		proc->group_id = group_id;
		proc->db_meta = db_meta;

		success &= run_cont_bgworker(proc);
	}

	/* Start combiner processes. */
	for (group_id = 0; slot_idx < NUM_BG_WORKERS_PER_DB; slot_idx++, group_id++)
	{
		proc = &db_meta->db_procs[slot_idx];
		MemSet(proc, 0, sizeof(ContQueryProc));

		proc->type = COMBINER;
		proc->id = slot_idx;
		proc->group_id = group_id;
		proc->db_meta = db_meta;

		success &= run_cont_bgworker(proc);
	}

	/* Start a single adhoc process for initial state clean up. */
	proc = db_meta->adhoc_procs;
	MemSet(proc, 0, sizeof(ContQueryProc));

	proc->type = ADHOC;
	proc->group_id = 1;
	proc->db_meta = db_meta;

	success &= run_cont_bgworker(proc);

	/* Start a single trigger process */
	proc = &db_meta->trigger_proc;
	MemSet(proc, 0, sizeof(ContQueryProc));
	if (continuous_triggers_enabled && XLogLogicalInfoActive())
	{
		proc->type = TRIG;
		proc->group_id = 1;
		proc->db_meta = db_meta;

		success &= run_cont_bgworker(proc);
	}

	SpinLockRelease(&db_meta->mutex);

	if (!success)
	{
		terminate_database_workers(db_meta);
		elog(ERROR, "failed to start some continuous query background workers");
	}

	wait_for_db_workers(db_meta, BGWH_STARTED);
	db_meta->running = true;
}

static void
reaper(void)
{
	HASH_SEQ_STATUS status;
	ContQueryDatabaseMetadata *db_meta;
	ListCell *lc;

	/*
	 * Check if any database is being dropped, remove it from the DatabaseList and terminate its
	 * worker processes.
	 */
	hash_seq_init(&status, ContQuerySchedulerShmem->db_table);
	while ((db_meta = (ContQueryDatabaseMetadata *) hash_seq_search(&status)) != NULL)
	{
		ListCell *lc;

		if (!db_meta->dropdb)
			continue;

		foreach(lc, DatabaseList)
		{
			DatabaseEntry *entry = lfirst(lc);
			if (entry->oid == db_meta->db_oid)
			{
				DatabaseList = list_delete_ptr(DatabaseList, entry);
				pfree(entry);
				break;
			}
		}

		if (db_meta->running)
			terminate_database_workers(db_meta);
	}

	/*
	 * Go over the list of databases and ensure that any database with continuous queries activated
	 * has background workers running and any database with continuous queries deactivated has
	 * no background workers running.
	 */
	foreach(lc, DatabaseList)
	{
		DatabaseEntry *db_entry = lfirst(lc);
		bool found;

		db_meta = hash_search(ContQuerySchedulerShmem->db_table,
				&db_entry->oid, HASH_FIND, &found);

		if (!found)
		{
			char *pos;

			db_meta = hash_search(ContQuerySchedulerShmem->db_table,
					&db_entry->oid, HASH_ENTER, &found);

			MemSet(db_meta, 0, ContQueryDatabaseMetadataSize());

			db_meta->db_oid = db_entry->oid;
			namestrcpy(&db_meta->db_name, NameStr(db_entry->name));
			SpinLockInit(&db_meta->mutex);

			pos = (char *) db_meta;
			pos += sizeof(ContQueryDatabaseMetadata);
			db_meta->db_procs = (ContQueryProc *) pos;
			pos += sizeof(ContQueryProc) * NUM_BG_WORKERS_PER_DB;
			db_meta->adhoc_procs = (ContQueryProc *) pos;
		}

		/* Continuous queries activated but no workers running? */
		if (db_entry->active && !db_meta->running && !db_meta->dropdb)
			start_database_workers(db_meta);
		/* Continuous queries disabled but workers still running? */
		else if (!db_entry->active && db_meta->running)
			terminate_database_workers(db_meta);
	}
}


void
ContQuerySchedulerMain(int argc, char *argv[])
{
	sigjmp_buf local_sigjmp_buf;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	am_cont_scheduler = true;

	/* reset MyProcPid */
	MyProcPid = getpid();
	MyPMChildSlot = AssignPostmasterChildSlot();

	/* record Start Time for logging */
	MyStartTime = time(NULL);

	/* Identify myself via ps */
	init_ps_display("continuous query scheduler process", "", "", "");

	ereport(LOG, (errmsg("continuous query scheduler started")));

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, sighup_handler);
	pqsignal(SIGINT, sigint_handler);
	pqsignal(SIGTERM, sigterm_handler);

	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts(); /* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	/* Early initialization */
	BaseInit();

	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	ContQuerySchedulerContext = AllocSetContextCreate(TopMemoryContext,
			"ContQuerySchedulerContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ContQuerySchedulerContext);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(ContQuerySchedulerContext);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(ContQuerySchedulerContext);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000 * 1000);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_database_list */
	PG_SETMASK(&UnBlockSig);

	ContQuerySchedulerShmem->pid = MyProcPid;

	refresh_database_list();
	if (list_length(DatabaseList) * (NUM_BG_WORKERS_PER_DB + 1) > max_worker_processes)
		ereport(ERROR,
				(errmsg("%d background worker slots are required but there are only %d available",
						list_length(DatabaseList) * NUM_BG_WORKERS_PER_DB, max_worker_processes),
				errhint("Each database requires %d background worker slots. Increase max_worker_processes to enable more capacity.", NUM_BG_WORKERS_PER_DB)));

	/* Loop forever */
	for (;;)
	{
		int rc;

		reaper();

		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 1000);
		ResetLatch(MyLatch);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;

			ProcessConfigFile(PGC_SIGHUP);
			update_run_params();
		}

		/* refresh database list? */
		if (got_SIGINT)
		{
			got_SIGINT = false;

			refresh_database_list();
		}
	}

	/* Normal exit from the continuous query scheduler is here */
	ereport(LOG, (errmsg("continuous query scheduler shutting down")));

	ContQuerySchedulerShmem->pid = 0;

	proc_exit(0); /* done */
}

static void
signal_cont_query_scheduler(int signal)
{
	int ntries;

	/*
	 * Send signal to request updating the db list.  It's possible that the scheduler
	 * hasn't started yet, or is in process of restarting, so we will retry a
	 * few times if needed.
	 */
	for (ntries = 0;; ntries++)
	{
		if (ContQuerySchedulerShmem->pid == 0)
		{
			if (ntries >= 20) /* max wait 2.0 sec */
			{
				ereport(NOTICE, (errmsg("could not signal continuous query scheduler because it is not running")));
				break;
			}
		}
		else if (kill(ContQuerySchedulerShmem->pid, signal) != 0)
		{
			if (ntries >= 20) /* max wait 2.0 sec */
			{
				ereport(NOTICE, (errmsg("could not signal continuous query scheduler for: %m")));
				break;
			}
		}
		else
			return; /* signal sent successfully */

		CHECK_FOR_INTERRUPTS();
		pg_usleep(100 * 1000); /* wait 100ms, then retry */
	}
}

bool
WaitForContQueryActivation(void)
{
	ContQueryDatabaseMetadata *db_meta;
	TimestampTz start = GetCurrentTimestamp();

	while ((db_meta = GetContQueryDatabaseMetadata(MyDatabaseId)) == NULL ||
			!db_meta->running)
	{
		pg_usleep(10 * 1000);

		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), CQ_STATE_CHANGE_TIMEOUT))
			return false;
	}

	return true;
}

static bool
wait_for_db_procs_to_stop(Oid dbid)
{
	ContQueryDatabaseMetadata *db_meta = GetContQueryDatabaseMetadata(dbid);
	TimestampTz start = GetCurrentTimestamp();

	/* No metadata? This means its already been deactivated. */
	if (db_meta == NULL)
		return true;

	while (db_meta->running)
	{
		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), CQ_STATE_CHANGE_TIMEOUT))
			return false;

		pg_usleep(10 * 1000);
	}

	return true;
}

bool
WaitForContQueryDeactivation(void)
{
	return wait_for_db_procs_to_stop(MyDatabaseId);
}

/*
 * SignalContQuerySchedulerDropDB
 */
void
SignalContQuerySchedulerDropDB(Oid db_oid)
{
	bool found;
	ContQueryDatabaseMetadata *db_meta = (ContQueryDatabaseMetadata *) hash_search(
			ContQuerySchedulerShmem->db_table, &db_oid, HASH_FIND, &found);

	if (found)
	{
		db_meta->dropdb = true;
		signal_cont_query_scheduler(SIGUSR2);
		wait_for_db_procs_to_stop(db_oid);
	}
}

extern ContQueryDatabaseMetadata *
GetContQueryDatabaseMetadata(Oid db_oid)
{
	bool found;

	ContQueryDatabaseMetadata *db_meta =
		(ContQueryDatabaseMetadata *) hash_search(
			ContQuerySchedulerShmem->db_table, &db_oid, HASH_FIND, &found);

	if (!found)
		return NULL;

	return db_meta;
}

/*
 * SignalContQuerySchedulerRefreshDBList
 */
void
SignalContQuerySchedulerRefreshDBList(void)
{
	signal_cont_query_scheduler(SIGINT);
}

void
SetAmContQueryAdhoc(bool value)
{
	am_cont_adhoc = value;
}

ContQueryProc *
AdhocContQueryProcGet(void)
{
	bool found;
	ContQueryDatabaseMetadata *db_meta;
	int i;
	int idx;
	ContQueryProc *proc = NULL;

	db_meta = (ContQueryDatabaseMetadata *) hash_search(
				ContQuerySchedulerShmem->db_table, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "failed to find database metadata for continuous queries");

	SpinLockAcquire(&db_meta->mutex);

	for (i = 0; i < max_worker_processes; i++)
	{
		idx = (db_meta->adhoc_counter + i) % max_worker_processes;

		/* We use group_idx > 0 as a slot occupied check. */
		if (db_meta->adhoc_procs[idx].group_id == 0)
		{
			proc = &db_meta->adhoc_procs[idx];
			proc->group_id = 0xdeadbeef;
			db_meta->adhoc_counter++;
			break;
		}
	}

	SpinLockRelease(&db_meta->mutex);

	if (proc)
	{
		MemSet(proc, 0, sizeof(ContQueryProc));

		proc->type = ADHOC;
		proc->id = rand();
		proc->latch = MyLatch;
		proc->db_meta = db_meta;

		proc->dsm_handle = 0;
		proc->bgw_handle = NULL;
	}
	else
		elog(ERROR, "no free slot for running adhoc continuous query process");

	return proc;
}

void
AdhocContQueryProcRelease(ContQueryProc *proc)
{
	bool found;
	ContQueryDatabaseMetadata *db_meta;

	Assert(proc->type == ADHOC);
	Assert(proc->group_id > 0);

	db_meta = (ContQueryDatabaseMetadata *) hash_search(
				ContQuerySchedulerShmem->db_table, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "failed to find database metadata for continuous queries");

	SpinLockAcquire(&db_meta->mutex);

	/* Mark ContQueryProc struct as free. */
	MemSet(proc, 0, sizeof(ContQueryProc));

	SpinLockRelease(&db_meta->mutex);
}

dsm_handle
GetDatabaseDSMHandle(char *dbname)
{
	bool found = false;
	ContQueryDatabaseMetadata *db_meta;
	dsm_handle handle;

	if (dbname == NULL)
		db_meta = (ContQueryDatabaseMetadata *) hash_search(
				ContQuerySchedulerShmem->db_table, &MyDatabaseId, HASH_FIND, &found);
	else
	{
		HASH_SEQ_STATUS scan;
		hash_seq_init(&scan, ContQuerySchedulerShmem->db_table);
		while ((db_meta = (ContQueryDatabaseMetadata *) hash_seq_search(&scan)))
		{
			if (pg_strcasecmp(dbname, NameStr(db_meta->db_name)) == 0)
			{
				found = true;
				break;
			}
		}
		hash_seq_term(&scan);
	}

	if (!found)
		elog(ERROR, "failed to find database metadata for continuous queries");

	SpinLockAcquire(&db_meta->mutex);
	handle = db_meta->handle;
	SpinLockRelease(&db_meta->mutex);

	return handle;
}

ContQueryProc *
GetContQueryAdhocProcs(void)
{
	bool found;
	ContQueryDatabaseMetadata *db_meta;
	ContQueryProc *procs;

	db_meta = (ContQueryDatabaseMetadata *) hash_search(
			ContQuerySchedulerShmem->db_table, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "failed to find database metadata for continuous queries");

	SpinLockAcquire(&db_meta->mutex);
	procs = db_meta->adhoc_procs;
	SpinLockRelease(&db_meta->mutex);

	return procs;
}

LWLock *
GetContAdhocProcLWLock(void)
{
	bool found;
	ContQueryDatabaseMetadata *db_meta;
	int lock_idx = -1;

	db_meta = (ContQueryDatabaseMetadata *) hash_search(
			ContQuerySchedulerShmem->db_table, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "failed to find database metadata for continuous queries");

	SpinLockAcquire(&db_meta->mutex);
	lock_idx = db_meta->lock_idx;
	SpinLockRelease(&db_meta->mutex);

	return (LWLock *) &ContQuerySchedulerShmem->locks[lock_idx + NUM_BG_WORKERS_PER_DB];
}

bool
AreContQueriesEnabled(void)
{
	HeapTuple tup = SearchSysCache1(PIPELINEDATABASEDBID, ObjectIdGetDatum(MyDatabaseId));
	bool enabled;

	Assert(HeapTupleIsValid(tup));

	enabled = ((Form_pipeline_database) GETSTRUCT(tup))->cq_enabled;
	ReleaseSysCache(tup);
	return enabled;
}
