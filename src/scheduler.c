/*-------------------------------------------------------------------------
 *
 * scheduler.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <execinfo.h>
#include <math.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "compat.h"
#include "config.h"
#include "libpq/pqsignal.h"
#include "microbatch.h"
#include "miscadmin.h"
#include "miscutils.h"
#include "nodes/print.h"
#include "pgstat.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "pzmq.h"
#include "scheduler.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/sinval.h"
#include "storage/spin.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "update.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#define MAX_PRIORITY 20 /* XXX(usmanm): can we get this from some sys header? */
#define NUM_LOCKS_PER_DB NUM_BG_WORKERS_PER_DB

#define BG_PROC_STATUS_TIMEOUT 10000
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
static bool am_cont_queue = false;
static bool am_cont_reaper = false;

bool am_cont_combiner = false;

int num_workers;
int num_combiners;
int num_queues;
int num_reapers;

/* guc parameters */
bool continuous_queries_enabled;
int continuous_query_queue_mem;
int  continuous_query_max_wait;
int  continuous_query_combiner_work_mem;
int  continuous_query_combiner_synchronous_commit;
int continuous_query_commit_interval;
double continuous_query_proc_priority;

/* flags set by signal handlers */
static volatile sig_atomic_t got_SIGINT = false;

/* the main continuous process scheduler shmem struct */
typedef struct ContQuerySchedulerShmemStruct
{
	pid_t pid;
	HTAB *db_table;
} ContQuerySchedulerShmemStruct;

static ContQuerySchedulerShmemStruct *ContQuerySchedulerShmem;

/*
 * ContQueryDatabaseMetadataSize
 */
static Size
ContQueryDatabaseMetadataSize(void)
{
	return (sizeof(ContQueryDatabaseMetadata) +
			(sizeof(ContQueryProc) * NUM_BG_WORKERS_PER_DB) +
			(sizeof(ContQueryProc) * max_worker_processes));
}

/*
 * debug_segfault
 * 		SIGSEGV handler that prints a stack trace
 *
 * 		Enable by #defining BACKTRACE_SEGFAULTS.
 *
 * 		Note, the output of backtrace uses instruction addresses
 * 		instead of line numbers, which can be hard to read. To translate
 * 		the addresses into something more human readble, use addr2line:
 *
 * 		addr2line -e path/to/postgres <address>
 */
void
debug_segfault(SIGNAL_ARGS)
{
	void *array[32];
	size_t size = backtrace(array, 32);
	fprintf(stderr, "Segmentation fault (PID %d)\n", MyProcPid);
	fprintf(stderr, "PostgreSQL version: %s\n", PG_VERSION);
	fprintf(stderr, "PipelineDB version: %s at revision %s\n", pipeline_version_str, pipeline_revision_str);
	fprintf(stderr, "query: %s\n", debug_query_string);
	fprintf(stderr, "backtrace:\n");
	backtrace_symbols_fd(array, size, STDERR_FILENO);

#ifdef SLEEP_ON_ASSERT

	/*
	 * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
	 * minutes, which seems too short.
	 */
	sleep(1000000);
#endif

#ifdef DUMP_CORE
	abort();
#else
	exit(1);
#endif
}

/*
 * ContQuerySchedulerShmemSize
 */
Size
ContQuerySchedulerShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(ContQuerySchedulerShmemStruct));
	size = add_size(size, hash_estimate_size(16, ContQueryDatabaseMetadataSize()));

	return size;
}

/*
 * ContQuerySchedulerShmemInit
 */
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

/*
 * GetContQueryProcName
 */
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
		case Queue:
			snprintf(buf, NAMEDATALEN, "queue%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case Reaper:
			snprintf(buf, NAMEDATALEN, "reaper%d [%s]", proc->group_id, NameStr(proc->db_meta->db_name));
			break;
		case Scheduler:
			return pstrdup("scheduler");
			break;
	}

	return buf;
}

/*
 * IsContQuerySchedulerProcess
 */
bool
IsContQuerySchedulerProcess(void)
{
	return am_cont_scheduler;
}

/*
 * IsContQueryWorkerProcess
 */
bool
IsContQueryWorkerProcess(void)
{
	return am_cont_worker;
}

/*
 * IsContQueryCombinerProcess
 */
bool
IsContQueryCombinerProcess(void)
{
	return am_cont_combiner;
}

/*
 * IsContQueryQueueProcess
 */
bool
IsContQueryQueueProcess(void)
{
	return am_cont_queue;
}

/* SIGTERM: time to die */
static void
sigterm_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	set_sigterm_flag();
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

/*
 * refresh_database_list
 */
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
	if (!IsTransactionState())
		StartTransactionCommand();

	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

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

	PopActiveSnapshot();

	heap_endscan(scan);
	heap_close(pg_database, NoLock);

	CommitTransactionCommand();

	if (DatabaseList)
		list_free_deep(DatabaseList);

	DatabaseList = dbs;
}

/*
 * set_nice_priority
 */
static int
set_nice_priority()
{
	int priority = 0;
	int default_priority = getpriority(PRIO_PROCESS, MyProcPid);
	priority = Max(default_priority, MAX_PRIORITY -
			ceil(continuous_query_proc_priority * (MAX_PRIORITY - default_priority)));

	return nice(priority);
}

/*
 * cont_bgworker_main
 */
void
cont_bgworker_main(Datum arg)
{
	void (*run) (void);
	ContQueryProc *proc;
	bool exists = false;
	TimestampTz start;

	pqsignal(SIGTERM, sigterm_handler);
	pqsignal(SIGSEGV, debug_segfault);

	proc = MyContQueryProc = (ContQueryProc *) DatumGetPointer(arg);

	pg_atomic_fetch_add_u64(&MyContQueryProc->db_meta->generation, 1);

	BackgroundWorkerUnblockSignals();

#if (PG_VERSION_NUM < 110000)
	BackgroundWorkerInitializeConnectionByOid(proc->db_meta->db_id, InvalidOid);
#else
	BackgroundWorkerInitializeConnectionByOid(proc->db_meta->db_id, InvalidOid, 0);
#endif

	/*
	 * We must keep checking for the extension's existence for a short duration,
	 * because we'll get here as soon as CREATE EXTENSION pipelinedb is executed
	 * but before it's results are actually committed.
	 *
	 * The scheduler can't wait until CREATE EXTENSION commits, because it doesn't
	 * have catalog access so we launch workers for all databases, and any that don't
	 * have the extension will just cleanly exit as soon as possible here.
	 */
	start = GetCurrentTimestamp();
	while (!TimestampDifferenceExceeds(start, GetCurrentTimestamp(), CQ_STATE_CHANGE_TIMEOUT))
	{
		StartTransactionCommand();
		exists = PipelineDBExists();
		CommitTransactionCommand();
		if (exists)
			break;
		pg_usleep(10 * 1000);
	}

	if (!exists)
	{
		SpinLockAcquire(&MyContQueryProc->db_meta->mutex);
		MyContQueryProc->db_meta->extexists = false;
		SpinLockRelease(&MyContQueryProc->db_meta->mutex);
		proc_exit(0);
	}

	/*
	 * Tell the scheduler that the extension exists for this database so that it will restart
	 * any processes as necessary.
	 */
	SpinLockAcquire(&MyContQueryProc->db_meta->mutex);
	MyContQueryProc->db_meta->extexists = true;
	SpinLockRelease(&MyContQueryProc->db_meta->mutex);

	/* if we got a cancel signal in prior command, quit */
	CHECK_FOR_INTERRUPTS();

	proc->latch = MyLatch;

	switch (proc->type)
	{
		case Combiner:
			am_cont_combiner = true;
			run = &ContinuousQueryCombinerMain;
			break;
		case Queue:
			am_cont_queue = true;
			run = &ContinuousQueryQueueMain;
			break;
		case Worker:
			am_cont_worker = true;
			run = &ContinuousQueryWorkerMain;
			break;
		case Reaper:
			am_cont_reaper = true;
			run = &ContinuousQueryReaperMain;
			break;
		default:
			elog(ERROR, "unknown pipelinedb process type: %d", proc->type);
	}

	elog(LOG, "pipelinedb process \"%s\" running with pid %d", GetContQueryProcName(proc), MyProcPid);

	microbatch_ipc_init();
	pzmq_bind(MyContQueryProc->pzmq_id);

	set_nice_priority();

	run();

	pg_atomic_fetch_add_u64(&MyContQueryProc->db_meta->generation, 1);
	pzmq_destroy();

	/* If this isn't a clean termination, exit with a non-zero status code */
	if (!proc->db_meta->terminate)
	{
#if (PG_VERSION_NUM < 110000)
		elog(LOG, "pipelinedb process \"%s\" was killed", GetContQueryProcName(proc));
#endif
		proc_exit(1);
	}

	elog(LOG, "pipelinedb process \"%s\" shutting down", GetContQueryProcName(proc));
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

/*
 * run_cont_bgworker
 */
static bool
run_cont_bgworker(ContQueryProc *proc)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	bool success;
	char *name = GetContQueryProcName(proc);

	strcpy(worker.bgw_name, GetContQueryProcName(proc));
#if (PG_VERSION_NUM >= 110000)
	strcpy(worker.bgw_type, GetContQueryProcName(proc));
#endif

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_IS_CONT_QUERY_PROC;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	strcpy(worker.bgw_name, name);
	strcpy(worker.bgw_library_name, PIPELINEDB_EXTENSION_NAME);
	strcpy(worker.bgw_function_name, "cont_bgworker_main");

	worker.bgw_notify_pid = 0;
	worker.bgw_restart_time = BGW_NEVER_RESTART; /* the scheduler will restart procs as necessary */
	worker.bgw_main_arg = PointerGetDatum(proc);

	success = RegisterDynamicBackgroundWorker(&worker, &handle);

	if (success)
		proc->bgw_handle = handle;
	else
		proc->bgw_handle = NULL;

	pfree(name);

	return success;
}

/*
 * wait_for_db_workers
 */
static void
wait_for_db_workers(ContQueryDatabaseMetadata *db_meta, BgwHandleStatus state)
{
	ContQueryProc *cqproc;
	int i;

	for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
	{
		cqproc = &db_meta->db_procs[i];
		if (!wait_for_bg_worker_state(cqproc->bgw_handle, state, BG_PROC_STATUS_TIMEOUT))
			elog(WARNING, "timed out waiting for pipelinedb process \"%s\" to reach state %d",
					GetContQueryProcName(cqproc), state);
	}
}

/*
 * terminate_database_workers
 */
static void
terminate_database_workers(ContQueryDatabaseMetadata *db_meta)
{
	int i;

	if (!db_meta->running)
		return;

	elog(LOG, "terminating pipelinedb processes for database: \"%s\"", NameStr(db_meta->db_name));

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

/*
 * start_database_workers
 */
static void
start_database_workers(ContQueryDatabaseMetadata *db_meta)
{
	int i;
	int offset = 0;
	ContQueryProc *proc;
	bool success = true;

	Assert(!db_meta->running);

	SpinLockAcquire(&db_meta->mutex);

	db_meta->terminate = false;

	/* Start background processes */
	for (i = 0; i < num_workers; i++)
	{
		proc = &db_meta->db_procs[i];
		MemSet(proc, 0, sizeof(ContQueryProc));
		proc->db_meta = db_meta;
		proc->pzmq_id = rand() ^ MyProcPid;

		proc->type = Worker;
		proc->group_id = i;

		success &= run_cont_bgworker(proc);
	}
	offset = num_workers;

	for (i = 0; i < num_combiners; i++)
	{
		proc = &db_meta->db_procs[i + offset];
		MemSet(proc, 0, sizeof(ContQueryProc));
		proc->db_meta = db_meta;
		proc->pzmq_id = rand() ^ MyProcPid;

		proc->type = Combiner;
		proc->group_id = i;

		success &= run_cont_bgworker(proc);
	}
	offset += num_combiners;

	for (i = 0; i < num_queues; i++)
	{
		proc = &db_meta->db_procs[i + offset];
		MemSet(proc, 0, sizeof(ContQueryProc));
		proc->db_meta = db_meta;
		proc->pzmq_id = rand() ^ MyProcPid;

		proc->type = Queue;
		proc->group_id = i;

		success &= run_cont_bgworker(proc);
	}
	offset += num_queues;

	for (i = 0; i < num_reapers; i++)
	{
		proc = &db_meta->db_procs[i + offset];
		MemSet(proc, 0, sizeof(ContQueryProc));
		proc->db_meta = db_meta;
		proc->pzmq_id = rand() ^ MyProcPid;

		proc->type = Reaper;
		proc->group_id = i;

		success &= run_cont_bgworker(proc);
	}

	SpinLockRelease(&db_meta->mutex);

	if (!success)
	{
		terminate_database_workers(db_meta);
		elog(ERROR, "failed to start some pipelinedb processes");
	}

	wait_for_db_workers(db_meta, BGWH_STARTED);
	db_meta->running = true;
}

/*
 * reaper
 */
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
		if (db_meta->extexists)
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
			db_meta->extexists = false;
			namestrcpy(&db_meta->db_name, NameStr(db_entry->name));
			SpinLockInit(&db_meta->mutex);

			pos = (char *) db_meta;
			pos += sizeof(ContQueryDatabaseMetadata);
			db_meta->db_procs = (ContQueryProc *) pos;

			pg_atomic_init_u64(&db_meta->generation, 0);

			start_database_workers(db_meta);
		}

		Assert(db_meta->running);
	}
}

/*
 * restart_dead_procs
 */
static void
restart_dead_procs(void)
{
	HASH_SEQ_STATUS status;
	ContQueryDatabaseMetadata *db_meta;

	hash_seq_init(&status, ContQuerySchedulerShmem->db_table);
	while ((db_meta = (ContQueryDatabaseMetadata *) hash_seq_search(&status)) != NULL)
	{
		int i;
		ContQueryProc *cqproc;

		if (db_meta->dropdb)
			continue;
		if (!db_meta->extexists)
			continue;

		for (i = 0; i < NUM_BG_WORKERS_PER_DB; i++)
		{
			pid_t pid;
			BgwHandleStatus status;

			cqproc = &db_meta->db_procs[i];
			status = GetBackgroundWorkerPid(cqproc->bgw_handle, &pid);

			if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			{
				if (!run_cont_bgworker(cqproc))
					elog(WARNING, "failed to restart %s", GetContQueryProcName(cqproc));
			}
		}
	}
}

/*
 * ContQuerySchedulerMain
 */
void
ContQuerySchedulerMain(Datum arg)
{
	int required_workers;

	pqsignal(SIGINT, sigint_handler);
	pqsignal(SIGTERM, sigterm_handler);
	pqsignal(SIGUSR2, sigusr2_handler);
	pqsignal(SIGSEGV, debug_segfault);

	BackgroundWorkerUnblockSignals();

#if (PG_VERSION_NUM < 110000)
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL);
#else
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);
#endif

	ContQuerySchedulerShmem->pid = MyProcPid;

	ereport(LOG, (errmsg("pipelinedb scheduler started")));

	refresh_database_list();
  required_workers = list_length(DatabaseList) * (NUM_BG_WORKERS_PER_DB) + 1 + 1;

	/*
	 * 1 scheduler + 1 logical decoding launcher for PG
	 */
	if (required_workers > max_worker_processes)
		ereport(FATAL,
				(errmsg("%d background worker slots are required but there are only %d available", required_workers, max_worker_processes),
				errhint("Each database requires num_combiners + num_workers + num_queues + num_reapers (%d) background worker slots. Increase max_worker_processes to enable more capacity.", required_workers)));

	for (;;)
	{
		int rc;

		reaper();

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 1000, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		if (get_sigterm_flag())
			break;

		/* refresh database list? */
		if (got_SIGINT)
		{
			got_SIGINT = false;
			refresh_database_list();
		}

		/* check for dead procs and restart them if necessary */
		restart_dead_procs();

		if (anonymous_update_checks)
		{
			/* only check at startup and a maximum of once per hour */
			static bool first = true;
			static TimestampTz last_check = 0;
			if (first || TimestampDifferenceExceeds(last_check, GetCurrentTimestamp(), 60 * 60 * 1000))
			{
				UpdateCheck(first);
				last_check = GetCurrentTimestamp();
				first = false;
			}
		}

		if (VerifyConfigurationHook != NULL)
			VerifyConfigurationHook();
	}
}

/*
 * signal_cont_query_scheduler
 */
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
				ereport(NOTICE, (errmsg("could not signal pipelinedb scheduler because it is not running")));
				break;
			}
		}
		else if (kill(ContQuerySchedulerShmem->pid, signal) != 0)
		{
			if (ntries >= 20) /* max wait 2.0 sec */
			{
				ereport(NOTICE, (errmsg("could not signal pipelinedb scheduler for: %m")));
				break;
			}
		}
		else
			return; /* signal sent successfully */

		CHECK_FOR_INTERRUPTS();
		pg_usleep(100 * 1000); /* wait 100ms, then retry */
	}
}

/*
 * wait_for_db_procs_to_stop
 */
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

	hash_search(ContQuerySchedulerShmem->db_table, &dbid, HASH_REMOVE, NULL);

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

	/*
	 * If the extension doesn't exist for the target DB, nothing to do
	 */
	if (!db_meta->extexists)
		return;

	SignalContQuerySchedulerDropPipelineDB(db_oid);
	db_meta->dropdb = true;
	signal_cont_query_scheduler(SIGUSR2);
	wait_for_db_procs_to_stop(db_oid);
}

/*
 * SignalContQuerySchedulerCreatePipelineDB
 */
void
SignalContQuerySchedulerCreatePipelineDB(Oid db_oid)
{
	ContQueryDatabaseMetadata *db_meta;

	SignalContQuerySchedulerRefreshDBList();
	db_meta = GetContQueryDatabaseMetadata(db_oid);

	if (!db_meta)
		return;

	db_meta->extexists = true;
	SignalContQuerySchedulerRefreshDBList();
}

/*
 * SignalContQuerySchedulerDropPipelineDB
 */
void
SignalContQuerySchedulerDropPipelineDB(Oid db_oid)
{
	ContQueryDatabaseMetadata *db_meta = GetContQueryDatabaseMetadata(db_oid);

	if (!db_meta)
		return;

	db_meta->extexists = false;
	db_meta->dropdb = true;
	signal_cont_query_scheduler(SIGUSR2);
	wait_for_db_procs_to_stop(db_oid);
}

/*
 * GetContQueryDatabaseMetadata
 */
ContQueryDatabaseMetadata *
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

ContQueryDatabaseMetadata *
GetMyContQueryDatabaseMetadata(void)
{
	static ContQueryDatabaseMetadata *db_meta = NULL;

	if (!db_meta)
		db_meta = GetContQueryDatabaseMetadata(MyDatabaseId);

	if (!db_meta)
		elog(ERROR, "pipelinedb workers have not started up yet");

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

/*
 * ContQuerySchedulerCanCreateDB
 */
void
ContQuerySchedulerCanCreateDB(void)
{
	int required;

	refresh_database_list();
	required = (list_length(DatabaseList)) * NUM_BG_WORKERS_PER_DB + 1 + 1;

	StartTransactionCommand();

	if (required > max_worker_processes)
		ereport(ERROR,
				(errmsg("%d background worker slots are required but there are only %d available",
						required, max_worker_processes),
				errhint("Each database requires (combiners + workers + queues + reapers + 1) background worker slots. Increase max_worker_processes to enable more capacity.")));

}
