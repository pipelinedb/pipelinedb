/*-------------------------------------------------------------------------
 *
 * broker.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include <time.h>
#include <unistd.h>

#include "postgres.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "pipeline/ipc/broker.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "signal.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"

#define NUM_INTERNAL_QUEUES = 4;

/* guc */
int continuous_query_ipc_shared_mem;

/* flag to tell if we're in IPC broker process */
static bool am_ipc_msg_broker = false;

/* flags set by signal handlers */
static volatile sig_atomic_t got_SIGTERM = false;

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

static void
ipc_msg_broker_main(int argc, char *argv[])
{
	sigjmp_buf local_sigjmp_buf;
	MemoryContext work_ctx;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	am_ipc_msg_broker = true;

	/* reset MyProcPid */
	MyProcPid = getpid();
	MyPMChildSlot = AssignPostmasterChildSlot();

	/* record Start Time for logging */
	MyStartTime = time(NULL);

	/* Identify myself via ps */
	init_ps_display("ipc message broker", "", "", "");

	elog(LOG, "ipc message broker started");

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.
	 */
	pqsignal(SIGTERM, sigterm_handler);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts(); /* establishes SIGALRM handler */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);
#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	BaseInit();
	InitProcess();
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	work_ctx = AllocSetContextCreate(TopMemoryContext,
			"IPCBrokerMemoryContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(work_ctx);

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

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(work_ctx);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(work_ctx);

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

	/* Loop forever */
	for (;;)
	{
		int rc;

		/* wait until timeout or signaled */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 1000);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* normal shutdown case */
		if (got_SIGTERM)
			break;
	}

	elog(LOG, "ipc broker shutting down");

	proc_exit(0); /* done */
}

pid_t
StartIPCMessageBroker(void)
{
	pid_t pid;

	switch ((pid = fork_process()))
	{
	case -1:
		ereport(LOG,
				(errmsg("could not fork ipc message broker process: %m")));
		return 0;

	case 0:
		InitPostmasterChild();
		/* in postmaster child ... */
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Lose the postmaster's on-exit routines */
		on_exit_reset();

		ipc_msg_broker_main(0, NULL);
		break;
	default:
		return pid;
	}

	/* shouldn't get here */
	return 0;
}

bool
IsIPCMessageBrokerProcess(void)
{
	return am_ipc_msg_broker;
}
