/*-------------------------------------------------------------------------
 *
 * cont_scheduler.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_scheduler.c
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
#include "pipeline/cont_scheduler.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/shm_alloc.h"
#include "storage/shm_array.h"
#include "storage/sinval.h"
#include "storage/spin.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"

#define MAX_PROC_TABLE_SZ 16 /* an entry exists per database */
#define INIT_PROC_TABLE_SZ 4
#define TOTAL_SLOTS (continuous_query_num_workers + continuous_query_num_combiners)
#define MIN_WAIT_TERMINATE_MS 250
#define MAX_PRIORITY 20 /* XXX(usmanm): can we get this from some sys header? */

static Throttler throttlers[MAX_CQS];

typedef struct
{
	Oid oid;
	NameData name;
} DatabaseEntry;

/* per proc structures */
ContQueryProc *MyContQueryProc = NULL;

/* flags to tell if we are in a continuous query process */
static bool am_cont_scheduler = false;
static bool am_cont_worker = false;
static bool am_cont_combiner = false;

/* guc parameters */
bool continuous_query_crash_recovery;
int continuous_query_num_combiners;
int continuous_query_num_workers;
int continuous_query_batch_size;
int continuous_query_max_wait;
int continuous_query_combiner_work_mem;
int continuous_query_combiner_cache_mem;
int continuous_query_combiner_synchronous_commit;
double continuous_query_proc_priority;

/* memory context for long-lived data */
static MemoryContext ContQuerySchedulerMemCxt;

/* flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_SIGINT = false;

/* the main continuous process scheduler shmem struct */
typedef struct
{
	pid_t scheduler_pid;
	HTAB *proc_table;
	ContQueryRunParams params;
} ContQuerySchedulerShmemStruct;

static ContQuerySchedulerShmemStruct *ContQuerySchedulerShmem;

NON_EXEC_STATIC void ContQuerySchedulerMain(int argc, char *argv[]) __attribute__((noreturn));

/* shared memory stuff */
Size
ContQuerySchedulerShmemSize(void)
{
	return MAXALIGN(sizeof(ContQuerySchedulerShmemStruct));
}

static void
update_tuning_params(void)
{
	ContQuerySchedulerShmem->params.batch_size = continuous_query_batch_size;
	ContQuerySchedulerShmem->params.max_wait = continuous_query_max_wait;
}

void
ContQuerySchedulerShmemInit(void)
{
	bool found;
	Size size = ContQuerySchedulerShmemSize();

	ContQuerySchedulerShmem = ShmemInitStruct("ContQueryScheduler Data", size, &found);

	if (!found)
	{
		HASHCTL info;

		MemSet(ContQuerySchedulerShmem, 0, ContQuerySchedulerShmemSize());

		info.keysize = sizeof(Oid);
		info.entrysize = MAXALIGN(add_size(sizeof(ContQueryProcGroup), mul_size(sizeof(ContQueryProc), TOTAL_SLOTS)));
		info.hash = oid_hash;

		ContQuerySchedulerShmem->proc_table = ShmemInitHash("ContQueryScheduler Proc Table", INIT_PROC_TABLE_SZ,
				MAX_PROC_TABLE_SZ, &info, HASH_ELEM | HASH_FUNCTION);

		update_tuning_params();
	}
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
	case Combiner:
		sprintf(buf, "%s [%s]", "combiner", NameStr(proc->group->db_name));
		break;
	case Worker:
		sprintf(buf, "%s [%s]", "worker", NameStr(proc->group->db_name));
		break;
	case Scheduler:
		return "scheduler";
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

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the continuous query launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
cqschduler_forkexec(void)
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
	switch ((scheduler_pid = cqschduler_forkexec()))
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
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGTERM: time to die */
static void
sigterm_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGUSR2: terminate DB conns */
static void
sigusr2_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGUSR2 = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGINT: refresh db list */
static void
sigint_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGINT = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * get_database_oids
 *
 * Returns a list of all database OIDs found in pg_database.
 */
static List *
get_database_list(void)
{
	List *dbs = NIL;
	Relation rel;
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
	rel = heap_open(DatabaseRelationId, AccessExclusiveLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		MemoryContext oldcxt;
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		DatabaseEntry *db_entry;

		/* Ignore template databases or ones that don't allow connections. */
		if (pgdatabase->datistemplate || !pgdatabase->datallowconn)
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
		StrNCpy(NameStr(db_entry->name), NameStr(pgdatabase->datname), NAMEDATALEN);
		dbs = lappend(dbs, db_entry);

		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);
	heap_close(rel, AccessExclusiveLock);

	CommitTransactionCommand();

	return dbs;
}

static void
cq_bgproc_main(Datum arg)
{
	void (*run) (void);
	int default_priority = getpriority(PRIO_PROCESS, MyProcPid);
	int priority;

	MyContQueryProc = (ContQueryProc *) DatumGetPointer(arg);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(NameStr(MyContQueryProc->group->db_name), NULL);

	/* if we got a cancel signal in prior command, quit */
	CHECK_FOR_INTERRUPTS();

	MyContQueryProc->latch = &MyProc->procLatch;
	MemSet(throttlers, 0, sizeof(throttlers));

	ereport(LOG, (errmsg("continuous query process \"%s\" running with pid %d", GetContQueryProcName(MyContQueryProc), MyProcPid)));
	pgstat_report_activity(STATE_RUNNING, GetContQueryProcName(MyContQueryProc));

	/*
	 * The BackgroundWorkerHandle's slot is always in the range [0, max_worker_processes)
	 * and will be unique for any background process being run. We use this knowledge to
	 * assign our continuous query processes's a unique ID that fits within any TupleBuffer's
	 * waiters Bitmapset.
	 */
	MyContQueryProc->id = MyContQueryProc->handle.slot;

	/*
	 * Be nice!
	 *
	 * More is less here. A higher number indicates a lower scheduling priority.
	 */
	priority = Max(default_priority, MAX_PRIORITY - ceil(continuous_query_proc_priority * (MAX_PRIORITY - default_priority)));
	nice(priority);

	switch (MyContQueryProc->type)
	{
	case Combiner:
		am_cont_combiner = true;
		run = &ContinuousQueryCombinerMain;
		break;
	case Worker:
		am_cont_worker = true;
		run = &ContinuousQueryWorkerMain;
		break;
	default:
		ereport(ERROR, (errmsg("continuous queries can only be run as worker or combiner processes")));
	}

	/* initialize process level CQ stats */
	cq_stat_init(&MyProcCQStats, 0, MyProcPid);

	run();

	/* purge proc level CQ stats */
	cq_stat_send_purge(0, MyProcPid, IsContQueryWorkerProcess() ? CQ_STAT_WORKER : CQ_STAT_COMBINER);
}

static bool
run_background_proc(ContQueryProc *proc)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	bool success;

	strcpy(worker.bgw_name, GetContQueryProcName(proc));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = cq_bgproc_main;
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_restart_time = 1; /* recover in 1s */
	worker.bgw_let_crash = false;
	worker.bgw_main_arg = PointerGetDatum(proc);

	success = RegisterDynamicBackgroundWorker(&worker, &handle);

	if (success)
		proc->handle = *handle;

	return success;
}

static void
start_group(ContQueryProcGroup *grp)
{
	int slot_idx;
	int group_id;

	SpinLockInit(&grp->mutex);
	SpinLockAcquire(&grp->mutex);

	grp->active = true;
	grp->terminate = false;

	/* Start workers */
	for (slot_idx = 0, group_id = 0; slot_idx < continuous_query_num_workers; slot_idx++, group_id++)
	{
		ContQueryProc *proc = &grp->procs[slot_idx];

		MemSet(proc, 0, sizeof(ContQueryProc));

		proc->type = Worker;
		proc->group_id = group_id;
		proc->group = grp;

		run_background_proc(proc);
	}

	/* Start combiners */
	for (group_id = 0; slot_idx < TOTAL_SLOTS; slot_idx++, group_id++)
	{
		ContQueryProc *proc = &grp->procs[slot_idx];

		MemSet(proc, 0, sizeof(ContQueryProc));

		proc->type = Combiner;
		proc->group_id = group_id;
		proc->group = grp;

		run_background_proc(proc);
	}

	SpinLockRelease(&grp->mutex);
}

static void
terminate_group(ContQueryProcGroup *grp)
{
	bool found;
	int i;

	grp->active = true;

	for (i = 0; i < TOTAL_SLOTS; i++)
	{
		ContQueryProc *proc = &grp->procs[i];

		/* Wake up processes, so they can see the terminate flag. */
		SetLatch(proc->latch);

		/* Let workers crash now as well in case we force terminate them. */
		ChangeBackgroundWorkerRestartState(&proc->handle, true, 0);
	}

	/* Wait for a bit and then force terminate any processes that are still alive. */
	pg_usleep(Max(ContQuerySchedulerShmem->params.max_wait, MIN_WAIT_TERMINATE_MS) * 1000);

	for (i = 0; i < TOTAL_SLOTS; i++)
	{
		ContQueryProc *proc = &grp->procs[i];
		TerminateBackgroundWorker(&proc->handle);
	}

	hash_search(ContQuerySchedulerShmem->proc_table, &grp->db_oid, HASH_REMOVE, &found);

	Assert(found);

	TupleBufferDrain(WorkerTupleBuffer, grp->db_oid);
	TupleBufferDrain(CombinerTupleBuffer, grp->db_oid);
}

void
ContQuerySchedulerMain(int argc, char *argv[])
{
	sigjmp_buf local_sigjmp_buf;
	List *dbs = NIL;

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
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too. This is only for consistency sake, we
	 * never fork the scheduler process. Instead dynamic bgworkers are used.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

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

	InitPostgres(NULL, InvalidOid, NULL, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	ContQuerySchedulerMemCxt = AllocSetContextCreate(TopMemoryContext,
			"ContQuerySchedulerCtx",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ContQuerySchedulerMemCxt);

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
		MemoryContextSwitchTo(ContQuerySchedulerMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(ContQuerySchedulerMemCxt);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
	}
	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_database_list */
	PG_SETMASK(&UnBlockSig);

	ContQuerySchedulerShmem->scheduler_pid = MyProcPid;

	dbs = get_database_list();

	/* Loop forever */
	for (;;)
	{
		ListCell *lc;
		int rc;

		foreach(lc, dbs)
		{
			DatabaseEntry *db_entry = lfirst(lc);
			bool found;
			ContQueryProcGroup *grp = hash_search(ContQuerySchedulerShmem->proc_table, &db_entry->oid, HASH_ENTER, &found);

			/* If we don't have an entry for this dboid, initialize a new one and fire off bg procs */
			if (!found)
			{
				grp->db_oid = db_entry->oid;
				namestrcpy(&grp->db_name, NameStr(db_entry->name));

				start_group(grp);
			}
		}

		/* Allow sinval catchup interrupts while sleeping */
		EnableCatchupInterrupt();

		/*
		 * Wait until naptime expires or we get some type of signal (all the
		 * signal handlers will wake us by calling SetLatch).
		 */
		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		ResetLatch(&MyProc->procLatch);

		DisableCatchupInterrupt();

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;

		/* update config? */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* update tuning parameters, so that they can be read downstream by background processes */
			update_tuning_params();
		}

		/* terminate a proc group? */
		if (got_SIGUSR2)
		{
			HASH_SEQ_STATUS status;
			ContQueryProcGroup *grp;

			got_SIGUSR2 = false;

			hash_seq_init(&status, ContQuerySchedulerShmem->proc_table);
			while ((grp = (ContQueryProcGroup *) hash_seq_search(&status)) != NULL)
			{
				ListCell *lc;

				if (!grp->terminate)
					continue;

				foreach(lc, dbs)
				{
					DatabaseEntry *entry = lfirst(lc);
					if (entry->oid == grp->db_oid)
					{
						dbs = list_delete(dbs, entry);
						break;
					}
				}

				terminate_group(grp);
			}
		}

		/* refresh db list? */
		if (got_SIGINT)
		{
			list_free_deep(dbs);
			dbs = get_database_list();
		}
	}

	/* Normal exit from the continuous query scheduler is here */
	ereport(LOG, (errmsg("continuous query scheduler shutting down")));

	ContQuerySchedulerShmem->scheduler_pid = 0;

	proc_exit(0); /* done */
}

/*
 * sleep_if_cqs_deactivated
 */
void
sleep_if_deactivated(void)
{
	char *name;

	if (MyContQueryProc->group->active)
		return;

	name = GetContQueryProcName(MyContQueryProc);
	pgstat_report_activity(STATE_DISABLED, name);

	while (!MyContQueryProc->group->active)
	{
		MyContQueryProc->active = false;
		WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		MyContQueryProc->active = true;
		ResetLatch(&MyProc->procLatch);
	}

	pgstat_report_activity(STATE_RUNNING, name);
	pfree(name);
}

/*
 * ContQuerySetStateAndWait
 */
bool
ContQuerySetStateAndWait(bool state, int waitms)
{
	TimestampTz start = GetCurrentTimestamp();
	bool found;
	ContQueryProcGroup *grp = (ContQueryProcGroup *) hash_search(ContQuerySchedulerShmem->proc_table, &MyDatabaseId, HASH_FIND, &found);
	int i;
	int num_affected = 0;

	if (!found)
		ereport(ERROR,
				(errmsg("couldn't find entry for database %d", MyDatabaseId)));

	SpinLockAcquire(&grp->mutex);

	/* Already in the right state? Noop. */
	if (grp->active == state)
	{
		SpinLockRelease(&grp->mutex);
		return true;
	}

	grp->active = state;

	/* Set latches so any sleeping processes wake up and see the unsetting of the active flag. */
	for (i = 0; i < TOTAL_SLOTS; i++)
		SetLatch(grp->procs[i].latch);

	while (!TimestampDifferenceExceeds(start, GetCurrentTimestamp(), waitms) && num_affected != TOTAL_SLOTS)
	{
		num_affected = 0;

		for (i = 0; i < TOTAL_SLOTS; i++)
		{
			if (grp->procs[i].active == state)
				num_affected++;
		}
	}

	/* If all processes failed to register state, reset to old state. */
	if (num_affected != TOTAL_SLOTS)
	{
		grp->active = !state;
		for (i = 0; i < TOTAL_SLOTS; i++)
			SetLatch(grp->procs[i].latch);
	}

	SpinLockRelease(&grp->mutex);

	if (num_affected == TOTAL_SLOTS && !state)
	{
		/* Successful deactivation? Drain the tuple buffers. */
		TupleBufferDrain(WorkerTupleBuffer, MyDatabaseId);
		TupleBufferDrain(CombinerTupleBuffer, MyDatabaseId);
	}

	return num_affected == TOTAL_SLOTS;
}

/*
 * ContQueryGetActiveFlag
 */
bool *ContQueryGetActiveFlag(void)
{
	bool found;
	ContQueryProcGroup *grp = (ContQueryProcGroup *) hash_search(ContQuerySchedulerShmem->proc_table, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
		ereport(ERROR,
				(errmsg("couldn't find entry for database %d", MyDatabaseId)));

	return (bool *) &grp->active;
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
		if (ContQuerySchedulerShmem->scheduler_pid == 0)
		{
			if (ntries >= 20) /* max wait 2.0 sec */
			{
				ereport(NOTICE, (errmsg("could not signal continuous query scheduler because it is not running")));
				break;
			}
		}
		else if (kill(ContQuerySchedulerShmem->scheduler_pid, signal) != 0)
		{
			if (ntries >= 20) /* max wait 2.0 sec */
			{
				ereport(NOTICE, (errmsg("could not signal continuous query scheduler for: %m")));
				break;
			}
		}
		else
			break; /* signal sent successfully */

		CHECK_FOR_INTERRUPTS();
		pg_usleep(100000L); /* wait 0.1 sec, then retry */
	}
}

/*
 * SignalContQuerySchedulerTerminate
 */
void
SignalContQuerySchedulerTerminate(Oid db_oid)
{
	bool found;
	ContQueryProcGroup *grp = (ContQueryProcGroup *) hash_search(ContQuerySchedulerShmem->proc_table, &db_oid, HASH_FIND, &found);

	if (found)
	{
		grp->terminate = true;
		signal_cont_query_scheduler(SIGUSR2);
	}
}

/*
 * SignalContQuerySchedulerRefresh
 */
void
SignalContQuerySchedulerRefresh(void)
{
	signal_cont_query_scheduler(SIGINT);
}

/*
 * ThrottlerRecordError
 */
void
ThrottlerRecordError(Oid cq_id)
{
	Throttler *t = &throttlers[cq_id];

	if (t->err_delay == 0)
		t->err_delay = MIN_ERR_DELAY;
	else
		t->err_delay = Min(MAX_ERR_DELAY, t->err_delay * 2);

	t->last_run = GetCurrentTimestamp();

	IncrementCQErrors();
}

/*
 * ThrottlerRecordSuccess
 */
void
ThrottlerRecordSuccess(Oid cq_id)
{
	Throttler *t = &throttlers[cq_id];

	t->err_delay = 0;
	t->last_run = GetCurrentTimestamp();

	IncrementCQExecutions();
}

/*
 * ThrottlerShouldSkip
 */
bool
ThrottlerShouldSkip(Oid cq_id)
{
	Throttler *t = &throttlers[cq_id];

	if (t->err_delay == 0)
		return false;

	if (TimestampDifferenceExceeds(t->last_run, GetCurrentTimestamp(), t->err_delay))
		return false;

	return true;
}
