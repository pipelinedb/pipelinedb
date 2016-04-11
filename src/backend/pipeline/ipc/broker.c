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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/broker.h"
#include "pipeline/ipc/queue.h"
#include "pipeline/miscutils.h"
#include "port/atomics.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "signal.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"

#define num_bg_workers_per_db (continuous_query_num_workers + continuous_query_num_combiners)
#define num_locks_per_db (num_bg_workers_per_db + 1) /* +1 for all ephemeral ipc queues */
#define ipc_queue_size (continuous_query_ipc_shared_mem * 1024)
#define db_dsm_segment_size (ipc_queue_size * num_bg_workers_per_db * 2)
#define broker_db_meta_size (sizeof(broker_db_meta) + (max_worker_processes * sizeof(dsm_segment_slot)))

typedef struct dsm_segment_slot
{
	pg_atomic_flag used;
	dsm_segment *segment;
	dsm_handle handle;
} dsm_segment_slot;

typedef struct lw_lock_slot
{
	bool used;
	LWLock lock;
} lw_lock_slot;

typedef struct ipc_meta
{
	dsm_segment *segment;
	dsm_segment_slot *seg_slot;

	ipc_queue *queue;
} ipc_meta;

static ipc_meta *my_ipc_meta = NULL;

typedef struct broker_db_meta
{
	Oid dbid;

	/* index into broker_meta->locks array */
	int lock_idx;

	/* segment for ipc queues of all database worker and combiner processes */
	dsm_segment_slot seg_slot;

	/* ephemeral segments used by adhoc query processes */
	dsm_segment_slot ephemeral_seg_slots[1]; /* max_worker_processes total slots */
} broker_db_meta;

typedef struct BrokerMeta
{
	pid_t pid;
	Latch *latch;

	pg_atomic_flag waiting;

	HTAB *db_meta_hash;

	int tranche_id;
	LWLockTranche tranche;
	lw_lock_slot locks[1]; /* max_worker_processes total slots */
} BrokerMeta;

/* guc */
int continuous_query_ipc_shared_mem;

/* flag to tell if we're in IPC broker process */
static bool am_ipc_msg_broker = false;

/* metadata for managing dsm segments and ipc queues */
static BrokerMeta *broker_meta = NULL;

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

/*
 * IPCMessageBrokerShmemInit
 */
void
IPCMessageBrokerShmemInit(void)
{
	bool found;
	Size size = sizeof(BrokerMeta) + (max_worker_processes * sizeof(lw_lock_slot));

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_EXCLUSIVE);

	broker_meta = ShmemInitStruct("BrokerMeta", size, &found);

	if (!found)
	{
		HASHCTL ctl;
		int i;

		MemSet(broker_meta, 0, size);

		pg_atomic_init_flag(&broker_meta->waiting);

		MemSet(&ctl, 0, sizeof(HASHCTL));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = broker_db_meta_size;
		ctl.hash = oid_hash;

		broker_meta->db_meta_hash = ShmemInitHash("BrokerDBMetaHash", 4,	16, &ctl, HASH_ELEM | HASH_FUNCTION);

		/* Initialize LWLocks that we'll use for all IPC queues */
		broker_meta->tranche_id = LWLockNewTrancheId();

		broker_meta->tranche.name = "BrokerMetaLWLocks";
		broker_meta->tranche.array_base = (void *) broker_meta->locks;
		broker_meta->tranche.array_stride = sizeof(lw_lock_slot);

		for (i = 0; i < max_worker_processes; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[i];
			LWLockInitialize(&slot->lock, broker_meta->tranche_id);
			slot->used = false;
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);
}

/*
 * get_database_oids
 */
static List *
get_database_oids(void)
{
	List *db_oids = NIL;
	Relation pg_database;
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
		MemoryContext old;
		Form_pg_database row = (Form_pg_database) GETSTRUCT(tup);

		/* Ignore template databases or ones that don't allow connections. */
		if (row->datistemplate || !row->datallowconn)
			continue;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		old = MemoryContextSwitchTo(resultcxt);

		db_oids = lappend_oid(db_oids, HeapTupleGetOid(tup));

		MemoryContextSwitchTo(old);
	}

	heap_endscan(scan);
	heap_close(pg_database, NoLock);

	CommitTransactionCommand();

	return db_oids;
}

/*
 * purge_dropped_db_segments
 */
static void
purge_dropped_db_segments(void)
{
	static TimestampTz last_gc = 0;
	List *db_oids;
	List *dbs_to_remove = NIL;
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;

	if (!TimestampDifferenceExceeds(last_gc, GetCurrentTimestamp(), 30 * 1000)) /* 30s */
		return;

	db_oids = get_database_oids();

	hash_seq_init(&status, broker_meta->db_meta_hash);
	while ((db_meta = (broker_db_meta *) hash_seq_search(&status)) != NULL)
	{
		bool found = false;
		ListCell *lc;

		foreach(lc, db_oids)
		{
			if (lfirst_oid(lc) == db_meta->dbid)
			{
				found = true;
				break;
			}
		}

		if (!found)
			dbs_to_remove = lappend_oid(dbs_to_remove, db_meta->dbid);
	}

	if (list_length(dbs_to_remove))
	{
		ListCell *lc;

		foreach(lc, dbs_to_remove)
		{
			Oid dbid = lfirst_oid(lc);
			bool found;
			broker_db_meta *meta;
			int i;

			meta = hash_search(broker_meta->db_meta_hash, &dbid, HASH_FIND, &found);
			Assert(found);

			Assert(!pg_atomic_unlocked_test_flag(&meta->seg_slot.used));
			Assert(meta->seg_slot.segment);
			Assert(meta->seg_slot.handle > 0);

			/* detach from main db segment */
			dsm_detach(meta->seg_slot.segment);

			/* detach from any adhoc proc segments */
			for (i = 0; i < max_worker_processes; i++)
			{
				dsm_segment_slot *slot = &meta->ephemeral_seg_slots[i];

				if (pg_atomic_unlocked_test_flag(&slot->used))
					continue;

				Assert(slot->handle > 0);

				/* we might have not attached to the segment yet */
				if (!slot->segment)
					continue;

				dsm_detach(slot->segment);
			}

			hash_search(broker_meta->db_meta_hash, &dbid, HASH_REMOVE, &found);
			Assert(found);
		}
	}

	last_gc = GetCurrentTimestamp();
}

static int
copy_messages(void)
{
	return 0;
}

static bool
have_pending_messages(void)
{
	return false;
}

/*
 * ipc_msg_broker_main
 */
static void
ipc_msg_broker_main(int argc, char *argv[])
{
	sigjmp_buf local_sigjmp_buf;
	MemoryContext work_ctx;

	IsUnderPostmaster = true;
	am_ipc_msg_broker = true;

	MyProcPid = getpid();
	MyPMChildSlot = AssignPostmasterChildSlot();
	MyStartTime = time(NULL);

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

	broker_meta->pid = MyProcPid;
	broker_meta->latch = MyLatch;

	/* Loop forever */
	for (;;)
	{
		int rc;
		int num_copied;

		purge_dropped_db_segments();
		num_copied = copy_messages();

		/* If we didn't copy any messages, try to sleep and wait for new ones to arrive. */
		if (num_copied == 0)
		{
			/* Mark as waiting */
			pg_atomic_test_set_flag(&broker_meta->waiting);

			/* If we have no pending messages, sleep till we get signaled. */
			if (!have_pending_messages())
				rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 1000);

			pg_atomic_clear_flag(&broker_meta->waiting);
			ResetLatch(MyLatch);
		}

		/* Emergency bail out if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* Shutdown signaled? */
		if (got_SIGTERM)
			break;
	}

	elog(LOG, "ipc message broker shutting down");

	proc_exit(0); /* done */
}

/*
 * StartIPCMessageBroker
 */
pid_t
StartIPCMessageBroker(void)
{
	pid_t pid;

	switch ((pid = fork_process()))
	{
		/* Error? */
		case -1:
			ereport(LOG,
					(errmsg("could not fork ipc message broker process: %m")));
			return 0;
		/* Child case */
		case 0:
			InitPostmasterChild();
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Lose the postmaster's on-exit routines */
			on_exit_reset();

			ipc_msg_broker_main(0, NULL);
			break;
		/* Parent case */
		default:
			return pid;
	}

	Assert(false); /* unreachable */
	return 0;
}

/*
 * IsIPCMessageBrokerProcess
 */
bool
IsIPCMessageBrokerProcess(void)
{
	return am_ipc_msg_broker;
}

static dsm_segment *
dsm_create_and_pin(Size size)
{
	ResourceOwner res;
	ResourceOwner old;
	dsm_segment *segment;

	old = CurrentResourceOwner;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "BrokerDBMeta dsm_segment ResourceOwner");

	segment = dsm_create(size, 0);
	dsm_pin_mapping(segment);

	res = CurrentResourceOwner;
	CurrentResourceOwner = old;
	ResourceOwnerDelete(res);

	return segment;
}

static dsm_segment *
dsm_attach_and_pin(dsm_handle handle)
{
	ResourceOwner res;
	ResourceOwner old;
	dsm_segment *segment;

	segment = dsm_find_mapping(handle);
	if (segment != NULL)
		return segment;

	old = CurrentResourceOwner;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "BrokerDBMeta dsm_segment ResourceOwner");

	segment = dsm_attach(handle);
	dsm_pin_mapping(segment);

	res = CurrentResourceOwner;
	CurrentResourceOwner = old;
	ResourceOwnerDelete(res);

	return segment;
}

static broker_db_meta *
get_db_meta(Oid dbid)
{
	bool found;
	broker_db_meta *db_meta;

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);

	db_meta = hash_search(broker_meta->db_meta_hash, &MyDatabaseId, HASH_FIND, &found);

	if (!found)
	{
		LWLockRelease(IPCMessageBrokerIndexLock);
		LWLockAcquire(IPCMessageBrokerIndexLock, LW_EXCLUSIVE);

		db_meta = hash_search(broker_meta->db_meta_hash, &MyDatabaseId, HASH_ENTER, &found);
	}

	/*
	 * We need to do this check again, in case someone snuck in between and created the metadata for
	 * our database.
	 */
	if (!found)
	{
		int i;
		dsm_segment_slot *seg_slot;
		dsm_segment *segment;
		char *ptr;

		MemSet(db_meta, 0, broker_db_meta_size);

		db_meta->dbid = MyDatabaseId;
		db_meta->lock_idx = -1;

		seg_slot = &db_meta->seg_slot;
		pg_atomic_init_flag(&seg_slot->used);
		pg_atomic_test_set_flag(&seg_slot->used);

		for (i = 0; i < max_worker_processes; i++)
		{
			dsm_segment_slot *seg_slot = &db_meta->ephemeral_seg_slots[i];
			pg_atomic_init_flag(&seg_slot->used);
		}

		/* Create per-database dsm_segment used by all worker and combiner processes. */
		segment = dsm_create_and_pin(db_dsm_segment_size);

		/* Set handle on the dsm_segment_slot. */
		seg_slot->handle = dsm_segment_handle(segment);

		/* Pick a lock index for this database. */
		for (i = 0; i < max_worker_processes; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[i];

			if (slot->used)
				continue;

			db_meta->lock_idx = i;
			break;
		}

		/* XXX(usmanm): Is this totally kosher? */
		Assert(db_meta->lock_idx != -1);

		for (i = 0; i < num_locks_per_db; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[db_meta->lock_idx + i];
			Assert(!slot->used);
			slot->used = true;
		}

		/* Initialize all ipc_queues for worker and combiner processes. */
		ptr = dsm_segment_address(segment);
		MemSet(ptr, 0, db_dsm_segment_size);

		for (i = 0; i < num_bg_workers_per_db; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[db_meta->lock_idx + i];

			/*
			 * We have two queues per process, one is a multi producer queue which requires a LWLock,
			 * the other one is a single producer queue and doesn't require a LWLock. The single producer
			 * queue is used by the continuous query process to read messages being sent to it while the
			 * multi producer queue is used other other processes to send messages to the continuous query
			 * process. The IPC broker process moves messages between these two queues.
			 */
			ipc_queue_init(ptr, ipc_queue_size, NULL);
			ptr += ipc_queue_size;

			ipc_queue_init(ptr, ipc_queue_size, &slot->lock);
			ptr += ipc_queue_size;
		}

		LWLockRelease(IPCMessageBrokerIndexLock);
		LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return db_meta;
}

static inline int
get_ipc_queue_idx_in_segment(int group_id, bool is_worker)
{
	int idx = group_id;
	if (!is_worker)
		idx += continuous_query_num_workers;
	return idx;
}

ipc_queue *
acquire_my_ipc_consumer_queue(void)
{
	broker_db_meta *db_meta;
	MemoryContext old;

	Assert(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryAdhocProcess());

	if (my_ipc_meta)
		return my_ipc_meta->queue;

	db_meta = get_db_meta(MyDatabaseId);

	old = MemoryContextSwitchTo(TopMemoryContext);

	my_ipc_meta = palloc0(sizeof(ipc_meta));

	if (IsContQueryAdhocProcess())
	{
		int i;
		dsm_segment *segment;
		char *ptr;

		for (i = 0; i < max_worker_processes; i++)
		{
			dsm_segment_slot *slot = &db_meta->ephemeral_seg_slots[i];

			if (pg_atomic_test_set_flag(&slot->used))
			{
				my_ipc_meta->seg_slot = slot;
				break;
			}
		}

		/* XXX(usmanm): Is this totally kosher? */
		Assert(my_ipc_meta->seg_slot);

		segment = dsm_create_and_pin(ipc_queue_size * 2);
		my_ipc_meta->seg_slot->handle = dsm_segment_handle(segment);
		my_ipc_meta->segment = segment;

		ptr = dsm_segment_address(my_ipc_meta->segment);
		ipc_queue_init(ptr, ipc_queue_size, NULL);
		my_ipc_meta->queue = (ipc_queue *) ptr;

		ptr += ipc_queue_size;
		ipc_queue_init(ptr, ipc_queue_size,
				&broker_meta->locks[db_meta->lock_idx + num_bg_workers_per_db].lock);
	}
	else
	{
		int idx;
		char *ptr;

		/*
		 * For worker and combiner processes, we simply connect to the DB's primary dsm_segment
		 * and find the correct ipc_queue within that dsm_segment.
		 */
		idx = get_ipc_queue_idx_in_segment(MyContQueryProc->group_id, IsContQueryWorkerProcess());

		my_ipc_meta->seg_slot = &db_meta->seg_slot;
		my_ipc_meta->segment = dsm_attach_and_pin(db_meta->seg_slot.handle);

		ptr = dsm_segment_address(my_ipc_meta->segment);
		ptr += ipc_queue_size * idx * 2;
		my_ipc_meta->queue = (ipc_queue *) ptr;
	}

	MemoryContextSwitchTo(old);

	return my_ipc_meta->queue;
}

void
release_my_ipc_consumer_queue()
{
	Assert(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryAdhocProcess());
	Assert(my_ipc_meta);
	Assert(!pg_atomic_unlocked_test_flag(&my_ipc_meta->seg_slot->used));

	dsm_detach(my_ipc_meta->segment);

	if (IsContQueryAdhocProcess())
		pg_atomic_clear_flag(&my_ipc_meta->seg_slot->used);

	pfree(my_ipc_meta);
	my_ipc_meta = NULL;
}

static ipc_queue *
get_bg_worker_queue_for_producers(dsm_segment *segment, int group_id, bool is_worker)
{
	int idx = get_ipc_queue_idx_in_segment(group_id, is_worker);
	char *ptr = dsm_segment_address(segment);
	ipc_queue *ipcq;

	ptr += ipc_queue_size * idx * 2;
	ptr += ipc_queue_size; /* skip over to the producer queue */

	ipcq = (ipc_queue *) ptr;
	Assert(ipcq->lock);

	return ipcq;
}

ipc_queue *
get_worker_producer_queue_with_lock(void)
{
	static long idx = -1;
	int ntries = 0;
	ipc_queue *ipcq = NULL;
	broker_db_meta *db_meta = get_db_meta(MyDatabaseId);
	dsm_segment *segment = dsm_attach_and_pin(db_meta->seg_slot.handle);

	if (idx == -1)
		idx = rand() % continuous_query_num_workers;

	idx = (idx + 1) % continuous_query_num_workers;

	for (;;)
	{
		ipcq = get_bg_worker_queue_for_producers(segment, idx, true);

		/*
		 * Try to lock dsm_cqueue of any worker that is not already locked in
		 * a round robin fashion.
		 */
		if (ntries < continuous_query_num_workers)
		{
			if (ipc_queue_lock(ipcq, false))
				break;
		}
		else
		{
			/* If all workers are locked, then just wait for the lock. */
			ipc_queue_lock(ipcq, true);
			break;
		}

		ntries++;
		idx = (idx + 1) % continuous_query_num_workers;
	}

	Assert(ipcq);
	Assert(LWLockHeldByMe(ipcq->lock));

	return ipcq;
}

ipc_queue *
get_combiner_producer_queue_with_lock(int idx)
{
	broker_db_meta *db_meta = get_db_meta(MyDatabaseId);
	dsm_segment *segment = dsm_attach_and_pin(db_meta->seg_slot.handle);
	ipc_queue *ipcq = get_bg_worker_queue_for_producers(segment, idx, false);
	ipc_queue_lock(ipcq, true);
	return ipcq;
}
