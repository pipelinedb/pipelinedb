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
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/pzmq.h"
#include "pipeline/miscutils.h"
#include "postmaster/bgworker_internals.h"
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

#define NUM_BG_WORKERS_PER_DB (continuous_query_num_workers + continuous_query_num_combiners)
#define NUM_LOCKS_PER_DB NUM_BG_WORKERS_PER_DB

#define BG_PROC_STATUS_TIMEOUT 1000
#define CQ_STATE_CHANGE_TIMEOUT 5000

typedef struct DatabaseEntry
{
	Oid id;
	NameData name;
} DatabaseEntry;

static List *DatabaseList = NIL;

/* per-process structures */
ContQueryProc *MyContQueryProc = NULL;

/* flags to tell if we are in a continuous query process */
static bool am_cont_scheduler = false;
static bool am_cont_worker = false;
bool am_cont_combiner = false;

/* guc parameters */
bool continuous_queries_enabled;
int  continuous_query_num_combiners;
int  continuous_query_num_workers;
int  continuous_query_max_wait;
int  continuous_query_combiner_work_mem;
int  continuous_query_combiner_synchronous_commit;
int continuous_query_commit_interval;
double continuous_query_proc_priority;

/* memory context for long-lived data */
static MemoryContext ContQuerySchedulerContext;

/* flags set by signal handlers */
static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGINT = false;

/* the main continuous process scheduler shmem struct */
typedef struct ContQuerySchedulerShmemStruct
{
	pid_t pid;
	HTAB *db_table;
} ContQuerySchedulerShmemStruct;

static ContQuerySchedulerShmemStruct *ContQuerySchedulerShmem;

NON_EXEC_STATIC void ContQuerySchedulerMain(int argc, char *argv[]) __attribute__((noreturn));

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
	Size		size;

	size = MAXALIGN(sizeof(ContQuerySchedulerShmemStruct));
	size = add_size(size, hash_estimate_size(16, ContQueryDatabaseMetadataSize()));

	return size;
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

		MemSet(ContQuerySchedulerShmem, 0, size);

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = ContQueryDatabaseMetadataSize();
		ctl.hash = oid_hash;

		ContQuerySchedulerShmem->db_table = ShmemInitHash("ContQueryDatabaseMetadata", 4, 16, &ctl, HASH_ELEM | HASH_FUNCTION);
	}
}

char *
GetContQueryProcName(ContQueryProc *proc)
{
	char *buf = palloc0(NAMEDATALEN);

	switch (proc->type)
	{
		case Combiner:
			snprintf(buf, NAMEDATALEN, "combiner%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case Worker:
			snprintf(buf, NAMEDATALEN, "worker%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case AdhocVacuumer:
			snprintf(buf, NAMEDATALEN, "adhoc vacuumer [%s]", NameStr(proc->db_meta->db_name));
			break;
		case Scheduler:
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
IsContQueryCombinerProcess(void)
{
	return am_cont_combiner;
}

pid_t
StartContQueryScheduler(void)
{
	pid_t pid;

	switch ((pid = fork_process()))
	{
	case -1:
		ereport(LOG,
				(errmsg("could not fork continuous query scheduler process: %m")));
		return 0;

	case 0:
		InitPostmasterChild();
		/* in postmaster child ... */
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Lose the postmaster's on-exit routines */
		on_exit_reset();

		ContQuerySchedulerMain(0, NULL);
		break;
	default:
		return pid;
	}

	/* shouldn't get here */
	return 0;
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

bool
ShouldTerminateContQueryProcess(void)
{
	return (bool) got_SIGTERM;
}

static void
refresh_database_list(void)
{
	List *dbs = NIL;
	Relation pg_database;
	HeapScanDesc scan;
	HeapTuple tup;
	MemoryContext cxt;

	/* This is the context that we will allocate our output data in */
	cxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 */
	StartTransactionCommand();
	GetTransactionSnapshot();

	/* We take a AccessExclusiveLock so we don't conflict with any DATABASE commands */
	pg_database = heap_open(DatabaseRelationId, AccessExclusiveLock);
	scan = heap_beginscan_catalog(pg_database, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		MemoryContext old;
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
		old = MemoryContextSwitchTo(cxt);

		db_entry = palloc0(sizeof(DatabaseEntry));
		db_entry->id = HeapTupleGetOid(tup);
		namestrcpy(&db_entry->name, NameStr(row->datname));

		dbs = lappend(dbs, db_entry);

		MemoryContextSwitchTo(old);
	}

	heap_endscan(scan);
	heap_close(pg_database, NoLock);

	CommitTransactionCommand();

	if (DatabaseList)
		list_free_deep(DatabaseList);

	DatabaseList = dbs;
}

static void
purge_adhoc_query(ContQuery *view)
{
	DestReceiver *receiver = 0;
	DropStmt *stmt = makeNode(DropStmt);
	List *querytree_list;
	Node *plan;
	Portal portal;

	if (!view)
		return;

	stmt->objects = list_make1(
			list_make2(makeString(view->name->schemaname), makeString(view->name->relname)));
	stmt->removeType = OBJECT_CONTVIEW;

	querytree_list = pg_analyze_and_rewrite((Node *) stmt, "DROP",
			NULL, 0);

	plan = ((Query *) linitial(querytree_list))->utilityStmt;
	PushActiveSnapshot(GetTransactionSnapshot());

	portal = CreatePortal("__cleanup_cont_view__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
			NULL,
			"DROP",
			"DROP",
			list_make1(plan),
			NULL);

	receiver = CreateDestReceiver(DestNone);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	(*receiver->rDestroy) (receiver);
	PortalDrop(portal, false);
	PopActiveSnapshot();
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
		purge_adhoc_query(cv);
	}

	CommitTransactionCommand();
}

static void
cont_bgworker_main(Datum arg)
{
	void (*run) (void);
	ContQueryProc *proc;

	proc = MyContQueryProc = (ContQueryProc *) DatumGetPointer(arg);

	pqsignal(SIGTERM, sigterm_handler);
#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(proc->db_meta->db_id, InvalidOid);

	/* if we got a cancel signal in prior command, quit */
	CHECK_FOR_INTERRUPTS();

	proc->latch = MyLatch;

	switch (proc->type)
	{
		case Combiner:
			am_cont_combiner = true;
			run = &ContinuousQueryCombinerMain;
			break;
		case Worker:
			am_cont_worker = true;
			run = &ContinuousQueryWorkerMain;
			break;
		case AdhocVacuumer:
			/* Clean up and die. */
			purge_adhoc_queries();
			return;
		default:
			elog(ERROR, "unknown continuous process type: %d", proc->type);
	}

	elog(LOG, "continuous query process \"%s\" running with pid %d", GetContQueryProcName(proc), MyProcPid);

	pgstat_init_cqstat(MyProcStatCQEntry, 0, MyProcPid);
	pzmq_init();
	pzmq_bind(MyContQueryProc->pzmq_id);

	set_nice_priority();

	run();

	pzmq_destroy();
	pgstat_send_cqpurge(0, MyProcPid, proc->type);

	/* If this isn't a clean termination, exit with a non-zero status code */
	if (!proc->db_meta->terminate)
	{
		elog(LOG, "continuous query process \"%s\" was killed", GetContQueryProcName(proc));
		proc_exit(1);
	}

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

	if (!handle)
		return true;

	while (!TimestampDifferenceExceeds(start, GetCurrentTimestamp(), timeoutms))
	{
		if (GetBackgroundWorkerPid(handle, &pid) == state)
			return true;

		pg_usleep(10 * 1000); /* 10ms */
	}

	return false;
}

static bool
run_cont_bgworker(ContQueryProc *proc)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	bool success;

	strcpy(worker.bgw_name, GetContQueryProcName(proc));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_IS_CONT_QUERY_PROC;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = cont_bgworker_main;
	worker.bgw_notify_pid = 0;
	worker.bgw_restart_time = 1; /* recover in 1s */
	worker.bgw_main_arg = PointerGetDatum(proc);

	success = RegisterDynamicBackgroundWorker(&worker, &handle);

	if (success)
		proc->bgw_handle = handle;
	else
		proc->bgw_handle = NULL;

	return success;
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

	wait_for_db_workers(db_meta, BGWH_STOPPED);

	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
		pfree(db_meta->db_procs[i].bgw_handle);

	db_meta->terminate = false;
	db_meta->running = false;

	SpinLockRelease(&db_meta->mutex);
}

static void
start_database_workers(ContQueryDatabaseMetadata *db_meta)
{
	int i;
	ContQueryProc *proc;
	bool success = true;

	Assert(!db_meta->running);

	elog(LOG, "starting continuous query processes for database: \"%s\"", NameStr(db_meta->db_name));

	SpinLockAcquire(&db_meta->mutex);

	db_meta->terminate = false;

	/* Start background processes */
	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
	{
		proc = &db_meta->db_procs[i];
		MemSet(proc, 0, sizeof(ContQueryProc));
		proc->db_meta = db_meta;
		proc->pzmq_id = rand() ^ MyProcPid;

		if (i >= continuous_query_num_workers)
		{
			proc->type = Combiner;
			proc->group_id = i - continuous_query_num_workers;
		}
		else
		{
			proc->type = Worker;
			proc->group_id = i;
		}

		success &= run_cont_bgworker(proc);
	}

	/* Start a single adhoc process for initial state clean up. */
	proc = &db_meta->adhoc_vacuumer;
	MemSet(proc, 0, sizeof(ContQueryProc));

	proc->type = AdhocVacuumer;
	proc->group_id = -1;
	proc->db_meta = db_meta;

	success &= run_cont_bgworker(proc);

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
			if (entry->id == db_meta->db_id)
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
	 * Go over the list of databases and ensure that all databases have background workers running.
	 */
	foreach(lc, DatabaseList)
	{
		DatabaseEntry *db_entry = lfirst(lc);
		bool found;

		db_meta = hash_search(ContQuerySchedulerShmem->db_table,
				&db_entry->id, HASH_FIND, &found);

		if (!found)
		{
			char *pos;

			db_meta = hash_search(ContQuerySchedulerShmem->db_table,
					&db_entry->id, HASH_ENTER, &found);

			MemSet(db_meta, 0, ContQueryDatabaseMetadataSize());

			db_meta->db_id = db_entry->id;
			namestrcpy(&db_meta->db_name, NameStr(db_entry->name));
			SpinLockInit(&db_meta->mutex);

			pos = (char *) db_meta;
			pos += sizeof(ContQueryDatabaseMetadata);
			db_meta->db_procs = (ContQueryProc *) pos;

			start_database_workers(db_meta);
		}

		Assert(db_meta->running);
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
		if (IsTransactionState())
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

/*
 * SignalContQuerySchedulerDropDB
 */
void
SignalContQuerySchedulerDropDB(Oid db_oid)
{
	ContQueryDatabaseMetadata *db_meta = GetContQueryDatabaseMetadata(db_oid);

	if (!db_meta)
		return;

	db_meta->dropdb = true;
	signal_cont_query_scheduler(SIGUSR2);
	wait_for_db_procs_to_stop(db_oid);
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
TerminateContQueryProcesses(void)
{
	static TimestampTz last_kill = 0;
	slist_iter siter;

	if (!TimestampDifferenceExceeds(last_kill, GetCurrentTimestamp(), 2000))
		return;

	last_kill = GetCurrentTimestamp();

	/*
	 * The continuous query scheduler process handles the launching of its child procs,
	 * so remove its workers from local memory so we don't try to launch them from
	 * the Postmaster.
	 */
	slist_foreach(siter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = slist_container(RegisteredBgWorker, rw_lnode, siter.cur);
		if (rw->rw_worker.bgw_flags & BGWORKER_IS_CONT_QUERY_PROC)
		{
			if (rw->rw_pid == 0)
				continue;
			kill(rw->rw_pid, SIGTERM);
		}
	}
}
