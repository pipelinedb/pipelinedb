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
#include "pipeline/cont_execute.h"
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

#define MAX_LOOPS_PER_QUEUE 5

#define num_bg_workers_per_db (continuous_query_num_workers + continuous_query_num_combiners)
#define num_locks_per_db (num_bg_workers_per_db + 1) /* +1 for all ephemeral ipc queues */
#define ipc_queue_size (continuous_query_ipc_shared_mem * 1024)
#define db_dsm_segment_size (ipc_queue_size * ((continuous_query_num_workers * 2) + continuous_query_num_combiners))
#define broker_db_meta_size (sizeof(broker_db_meta) + (max_worker_processes * sizeof(dsm_segment_slot)))

typedef struct local_buffer
{
	Size size;
	List *slots;
} local_buffer;

static local_buffer *my_local_buffers = NULL;

typedef struct dsm_segment_slot
{
	pg_atomic_flag used;
	dsm_handle handle;
} dsm_segment_slot;

typedef struct lw_lock_slot
{
	Oid dbid;
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
	dsm_segment *segment;
	dsm_handle handle;

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
 * IPCMessageBrokerShmemSize
 */
Size
IPCMessageBrokerShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(BrokerMeta));
	size = add_size(size, max_worker_processes * MAXALIGN(sizeof(lw_lock_slot)));
	size = add_size(size, hash_estimate_size(16, broker_db_meta_size));

	return size;
}

/*
 * IPCMessageBrokerShmemInit
 */
void
IPCMessageBrokerShmemInit(void)
{
	bool found;
	Size size = IPCMessageBrokerShmemSize();

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

		broker_meta->db_meta_hash = ShmemInitHash("BrokerDBMetaHash", 4, 16, &ctl, HASH_ELEM | HASH_FUNCTION);

		/* Initialize LWLocks that we'll use for all IPC queues */
		broker_meta->tranche_id = LWLockNewTrancheId();

		broker_meta->tranche.name = "BrokerMetaLWLocks";
		broker_meta->tranche.array_base = (void *) broker_meta->locks;
		broker_meta->tranche.array_stride = sizeof(lw_lock_slot);

		for (i = 0; i < max_worker_processes; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[i];
			LWLockInitialize(&slot->lock, broker_meta->tranche_id);
			slot->dbid = InvalidOid;
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	LWLockRegisterTranche(broker_meta->tranche_id, &broker_meta->tranche);
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

static void
mark_unused_locks_as_free(List *db_oids)
{
	int i;

	for (i = 0; i < max_worker_processes; i++)
	{
		lw_lock_slot *slot = &broker_meta->locks[i];
		ListCell *lc;
		bool found = false;

		if (!OidIsValid(slot->dbid))
			continue;

		foreach(lc, db_oids)
		{
			if (slot->dbid == lfirst_oid(lc))
			{
				found = true;
				break;
			}
		}

		if (!found)
			slot->dbid = InvalidOid;
	}
}

/*
 * purge_dropped_db_segments
 */
static void
purge_dropped_db_segments(void)
{
	static TimestampTz last_purge_time = 0;
	List *db_oids;
	List *dbs_to_remove = NIL;
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;

	if (!TimestampDifferenceExceeds(last_purge_time, GetCurrentTimestamp(), 30 * 1000)) /* 30s */
		return;

	db_oids = get_database_oids();

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);

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

	LWLockRelease(IPCMessageBrokerIndexLock);

	if (list_length(dbs_to_remove))
	{
		ListCell *lc;

		LWLockAcquire(IPCMessageBrokerIndexLock, LW_EXCLUSIVE);

		foreach(lc, dbs_to_remove)
		{
			Oid dbid = lfirst_oid(lc);
			bool found;

			db_meta = hash_search(broker_meta->db_meta_hash, &dbid, HASH_FIND, &found);
			Assert(found);

			Assert(db_meta->handle > 0);

			/* detach from main db segment */
			if (db_meta->segment)
				dsm_detach(db_meta->segment);

			hash_search(broker_meta->db_meta_hash, &dbid, HASH_REMOVE, &found);
			Assert(found);
		}

		mark_unused_locks_as_free(db_oids);

		LWLockRelease(IPCMessageBrokerIndexLock);
	}

	last_purge_time = GetCurrentTimestamp();
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

static int
copy_messages(void)
{
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;
	int num_copied = 0;

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);

	hash_seq_init(&status, broker_meta->db_meta_hash);
	while ((db_meta = (broker_db_meta *) hash_seq_search(&status)) != NULL)
	{
		int i;
		char *ptr;

		if (db_meta->segment == NULL)
			db_meta->segment = dsm_attach_and_pin(db_meta->handle);

		ptr = dsm_segment_address(db_meta->segment);

		for (i = 0; i < continuous_query_num_workers; i++)
		{
			ipc_queue *dest;
			ipc_queue *src;
			local_buffer *local_buf = &my_local_buffers[i];
			uint64 last_dest_tail = UINT64_MAX;
			int nloops = MAX_LOOPS_PER_QUEUE;
			uint64 dest_head;
			int ncopied_to = 0;
			int ncopied_from = 0;
			bool not_enough_space = false;
			uint64 src_tail;

			dest = (ipc_queue *) ptr;
			ptr += ipc_queue_size;
			src = (ipc_queue *) ptr;
			ptr += ipc_queue_size;

			Assert(src->peek_fn == NULL);
			Assert(src->pop_fn == NULL);

			dest_head = pg_atomic_read_u64(&dest->head);
			src_tail = src->cursor;

			Assert(src->cursor == pg_atomic_read_u64(&src->tail));

			/* Try to copy data into dest ipc_queue */
			while (--nloops)
			{
				uint64 dest_tail;
				Size dest_free;

				dest_tail = pg_atomic_read_u64(&dest->tail);

				/* Has the reader not read anything since the last iteration? Skip. */
				if (not_enough_space && dest_tail == last_dest_tail)
					continue;

				not_enough_space = false;
				last_dest_tail = dest_tail;
				dest_free = dest->size - (dest_head - dest_tail);

				/* If we have locally cached data, write that first so order remains same */
				if (local_buf->slots)
				{
					ListCell *lc;
					int nremoved = 0;

					foreach(lc, local_buf->slots)
					{
						ipc_queue_slot *src_slot = lfirst(lc);
						ipc_queue_slot *dest_slot = ipc_queue_slot_get(dest, dest_head);
						int len_needed;

						if (src_slot->wraps)
							len_needed = src_slot->len + dest->size - ipc_queue_offset(dest, dest_head);
						else
							len_needed = sizeof(ipc_queue_slot) + src_slot->len;

						Assert(len_needed > 0);

						if (len_needed > dest_free)
						{
							not_enough_space = true;
							break;
						}

						dest_free -= len_needed;

						if (src_slot->wraps)
						{
							*dest_slot = *src_slot;
							memcpy(dest->bytes, src_slot->bytes, src_slot->len);
						}
						else
							memcpy(dest_slot, src_slot, sizeof(ipc_queue_slot) + src_slot->len - 1);

						dest_head += len_needed;

						if (src_slot->next != dest_head)
						{

							elog(LOG, "FUCK src slot %ld, dest head %ld, len_needed %d, slots %d",
									src_slot->next, dest_head, len_needed, list_length(local_buf->slots));

							elog(LOG, "sleeping %d", MyProcPid);

							pg_usleep(20 * 1000 * 1000);
						}
						Assert(src_slot->next == dest_head);
						dest_slot->next = dest_head;

						pfree(src_slot);

						local_buf->size -= src_slot->len;

						nremoved++;
					}

					ncopied_to += nremoved;

					while (nremoved--)
						local_buf->slots = list_delete_first(local_buf->slots);
				}

				/* Still locally cached slots, try again */
				if (local_buf->slots)
					continue;
				/* Otherwise try to copy directly from src ipc_queue to dest ipc_queue */
				else
				{
					uint64 src_head;
					int ncopied = 0;

					while (true)
					{
						ipc_queue_slot *src_slot = ipc_queue_slot_get(src, src_tail);
						ipc_queue_slot *dest_slot = ipc_queue_slot_get(dest, dest_head);
						int len_needed;

						src_head = pg_atomic_read_u64(&src->head);

						/* No data in src buffer? */
						if (src_tail == src_head)
							break;

						len_needed = src_slot->next - src_tail;

						if (len_needed > dest_free)
						{
							not_enough_space = true;
							break;
						}

						dest_free -= len_needed;

						if (src_slot->wraps)
						{
							*dest_slot = *src_slot;
							memcpy(dest->bytes, src->bytes, src_slot->len);
						}
						else
							memcpy(dest_slot, src_slot, sizeof(ipc_queue_slot) + src_slot->len - 1);

						dest_head += len_needed;
						Assert(dest_head == src_slot->next);
						dest_slot->next = dest_head;

						src_tail = src_slot->next;

						ncopied++;
					}

					ncopied_to += ncopied;
					ncopied_from += ncopied;
				}
			}

			if (ncopied_to)
			{
				Latch *consumer_latch;

				Assert(dest_head);

				pg_atomic_write_u64(&dest->head, dest_head);
				consumer_latch = (Latch *) pg_atomic_read_u64(&dest->consumer_latch);
				if (consumer_latch)
					SetLatch(consumer_latch);
			}

			/* Try to copy data from src ipc_queue into local buffers */
			if (ipc_queue_has_unread(src))
			{
				while (local_buf->size < ipc_queue_size / 2)
				{
					ipc_queue_slot *src_slot = ipc_queue_slot_get(src, src_tail);
					uint64 src_head = pg_atomic_read_u64(&src->head);
					MemoryContext old =  MemoryContextSwitchTo(CacheMemoryContext);
					ipc_queue_slot *local_slot;

					/* No data in src buffer? */
					if (src_tail == src_head)
						break;

					local_slot = palloc0(sizeof(ipc_queue_slot) + src_slot->len);

					if (src_slot->wraps)
					{
						*local_slot = *src_slot;
						memcpy(local_slot->bytes, src->bytes, src_slot->len);
					}
					else
						memcpy(local_slot, src_slot, sizeof(ipc_queue_slot) + src_slot->len - 1);

					local_buf->slots = lappend(local_buf->slots, local_slot);
					local_buf->size += local_slot->len;

					ncopied_from++;

					src_tail = src_slot->next;

					MemoryContextSwitchTo(old);
				}
			}

			if (ncopied_from)
			{
				Latch *producer_latch;

				Assert(src_tail);

				pg_atomic_write_u64(&src->tail, src_tail);
				src->cursor = src_tail;
				producer_latch = (Latch *) pg_atomic_read_u64(&src->producer_latch);
				if (producer_latch)
					SetLatch(producer_latch);
			}
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return num_copied;
}

static bool
have_pending_messages(void)
{
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);

	hash_seq_init(&status, broker_meta->db_meta_hash);
	while ((db_meta = (broker_db_meta *) hash_seq_search(&status)) != NULL)
	{
		int i;
		char *ptr;

		Assert(db_meta->segment);
		ptr = dsm_segment_address(db_meta->segment);

		for (i = 0; i < continuous_query_num_workers; i++)
		{
			ipc_queue *src;
			local_buffer *local_buf = &my_local_buffers[i];

			if (list_length(local_buf->slots))
			{
				hash_seq_term(&status);
				LWLockRelease(IPCMessageBrokerIndexLock);
				return true;
			}

			ptr += ipc_queue_size;
			src = (ipc_queue *) ptr;
			ptr += ipc_queue_size;

			if (!ipc_queue_is_empty(src))
			{
				hash_seq_term(&status);
				LWLockRelease(IPCMessageBrokerIndexLock);

				return true;
			}
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return false;
}

static void
disconnect_from_all_segments(void)
{
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;

	LWLockAcquire(IPCMessageBrokerIndexLock, LW_SHARED);

	hash_seq_init(&status, broker_meta->db_meta_hash);
	while ((db_meta = (broker_db_meta *) hash_seq_search(&status)) != NULL)
	{
		if (db_meta->segment)
			dsm_detach(db_meta->segment);
	}

	LWLockRelease(IPCMessageBrokerIndexLock);
}

/*
 * ipc_msg_broker_main
 */
static void
ipc_msg_broker_main(int argc, char *argv[])
{
	sigjmp_buf local_sigjmp_buf;
	MemoryContext work_ctx;
	MemoryContext old;

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

		/* Abort the current transaction in order to recover */
		if (IsTransactionState())
			AbortCurrentTransaction();

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

	old = MemoryContextSwitchTo(CacheMemoryContext);
	my_local_buffers = (local_buffer *) palloc0(sizeof(local_buffer) * continuous_query_num_workers);
	MemoryContextSwitchTo(old);

	/* Loop forever */
	for (;;)
	{
		int num_copied;

		MemoryContextSwitchTo(work_ctx);
		MemoryContextReset(work_ctx);

		purge_dropped_db_segments();

		num_copied = copy_messages();

		/* If we didn't copy any messages, try to sleep and wait for new ones to arrive. */
		if (num_copied == 0)
		{
			int rc;

			/* Mark as waiting */
			pg_atomic_test_set_flag(&broker_meta->waiting);

			/* If we have no pending messages, sleep till we get signaled. */
			if (!have_pending_messages())
			{
				rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 1000);
				ResetLatch(MyLatch);

				/* Emergency bail out if postmaster has died */
				if (rc & WL_POSTMASTER_DEATH)
					proc_exit(1);
			}

			pg_atomic_clear_flag(&broker_meta->waiting);
		}

		/* Shutdown signaled? */
		if (got_SIGTERM)
			break;
	}

	disconnect_from_all_segments();

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
		dsm_segment *segment;
		char *ptr;
		lw_lock_slot *lock_slot;

		MemSet(db_meta, 0, broker_db_meta_size);

		db_meta->dbid = MyDatabaseId;
		db_meta->lock_idx = -1;

		for (i = 0; i < max_worker_processes; i++)
		{
			dsm_segment_slot *seg_slot = &db_meta->ephemeral_seg_slots[i];
			pg_atomic_init_flag(&seg_slot->used);
		}

		/* Create per-database dsm_segment used by all worker and combiner processes. */
		segment = dsm_create_and_pin(db_dsm_segment_size);

		/* Set handle on the dsm_segment_slot. */
		db_meta->handle = dsm_segment_handle(segment);

		/* Pick a lock index for this database. */
		mark_unused_locks_as_free(get_database_oids());
		for (i = 0; i < max_worker_processes; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[i];

			if (OidIsValid(slot->dbid))
				continue;

			db_meta->lock_idx = i;
			break;
		}

		/* XXX(usmanm): Is this totally kosher? */
		Assert(db_meta->lock_idx != -1);
		lock_slot = &broker_meta->locks[db_meta->lock_idx];

		for (i = 0; i < num_locks_per_db; i++)
		{
			lw_lock_slot *slot = &broker_meta->locks[db_meta->lock_idx + i];
			Assert(!OidIsValid(slot->dbid));
			slot->dbid = MyDatabaseId;
		}

		/* Initialize all ipc_queues for worker and combiner processes. */
		ptr = dsm_segment_address(segment);
		MemSet(ptr, 0, db_dsm_segment_size);

		for (i = 0; i < continuous_query_num_workers; i++)
		{
			ipc_queue_pop_fn popfn = synchronous_stream_insert ? StreamTupleStatePopFn : NULL;

			/*
			 * We have two queues per worker process, one is a multi producer queue which requires a LWLock,
			 * the other one is a single producer queue and doesn't require a LWLock. The single producer
			 * queue is used by the worker process to read messages being sent to it while the
			 * multi producer queue is used other other processes to send messages to the worker process.
			 * The IPC broker process moves messages between these two queues.
			 */
			ipc_queue_init(ptr, ipc_queue_size, NULL, false);
			ipc_queue_set_handlers((ipc_queue *) ptr, StreamTupleStatePeekFn, popfn, NULL);
			ptr += ipc_queue_size;

			ipc_queue_init(ptr, ipc_queue_size, &lock_slot->lock, true);
			ipc_queue_set_handlers((ipc_queue *) ptr, NULL, NULL, StreamTupleStateCopyFn);
			ptr += ipc_queue_size;

			lock_slot++;
		}

		for (i = 0; i < continuous_query_num_combiners; i++)
		{
			ipc_queue_pop_fn popfn = synchronous_stream_insert ? PartialTupleStatePopFn : NULL;

			ipc_queue_init(ptr, ipc_queue_size, &lock_slot->lock, false);
			ipc_queue_set_handlers((ipc_queue *) ptr, PartialTupleStatePeekFn, popfn, PartialTupleStateCopyFn);
			ptr += ipc_queue_size;

			lock_slot++;
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return db_meta;
}

static ipc_queue *
get_bg_worker_queue(dsm_segment *segment, int group_id, bool is_worker, bool for_producers)
{
	char *ptr = dsm_segment_address(segment);
	ipc_queue *ipcq;

	if (is_worker)
	{
		ptr += ipc_queue_size * group_id * 2;
		if (for_producers)
			ptr += ipc_queue_size; /* skip over to the producer queue */
	}
	else
	{
		ptr += ipc_queue_size * continuous_query_num_workers * 2;
		ptr += ipc_queue_size * group_id;
	}

	ipcq = (ipc_queue *) ptr;

	return ipcq;
}

ipc_queue *
acquire_my_ipc_queue(void)
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

		segment = dsm_create_and_pin(ipc_queue_size);
		my_ipc_meta->seg_slot->handle = dsm_segment_handle(segment);
		my_ipc_meta->segment = segment;
		my_ipc_meta->queue = (ipc_queue *) dsm_segment_address(my_ipc_meta->segment);

		ipc_queue_init(my_ipc_meta->queue, ipc_queue_size,
				&broker_meta->locks[db_meta->lock_idx + num_bg_workers_per_db].lock, false);
		ipc_queue_set_handlers(my_ipc_meta->queue, StreamTupleStatePeekFn, StreamTupleStatePopFn, StreamTupleStateCopyFn);

		MyContQueryProc->dsm_handle = dsm_segment_handle(segment);
	}
	else
	{
		/*
		 * For worker and combiner processes, we simply connect to the DB's primary dsm_segment
		 * and find the correct ipc_queue within that dsm_segment.
		 */
		my_ipc_meta->segment = dsm_attach_and_pin(db_meta->handle);
		my_ipc_meta->queue = get_bg_worker_queue(my_ipc_meta->segment, MyContQueryProc->group_id, IsContQueryWorkerProcess(), false);
		my_ipc_meta->seg_slot = NULL;
	}

	MemoryContextSwitchTo(old);

	return my_ipc_meta->queue;
}

void
release_my_ipc_queue()
{
	Assert(IsContQueryWorkerProcess() || IsContQueryCombinerProcess() || IsContQueryAdhocProcess());
	Assert(my_ipc_meta);

	dsm_detach(my_ipc_meta->segment);

	if (IsContQueryAdhocProcess())
	{
		Assert(!pg_atomic_unlocked_test_flag(&my_ipc_meta->seg_slot->used));
		pg_atomic_clear_flag(&my_ipc_meta->seg_slot->used);
	}

	pfree(my_ipc_meta);
	my_ipc_meta = NULL;
}

ipc_queue *
get_worker_queue_with_lock(void)
{
	static long idx = -1;
	int ntries = 0;
	ipc_queue *ipcq = NULL;
	broker_db_meta *db_meta = get_db_meta(MyDatabaseId);
	dsm_segment *segment = dsm_attach_and_pin(db_meta->handle);

	if (idx == -1)
		idx = rand() % continuous_query_num_workers;

	idx = (idx + 1) % continuous_query_num_workers;

	for (;;)
	{
		/*
		 * If we have multiple workers and we're trying to write from a continuous transform, never write to our
		 * own queue.
		 */
		if (IsContQueryWorkerProcess() && continuous_query_num_workers > 1 && idx == MyContQueryProc->id)
		{
			ntries++;
			idx = (idx + 1) % continuous_query_num_workers;
		}

		ipcq = get_bg_worker_queue(segment, idx, true, true);

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
get_combiner_queue_with_lock(int idx)
{
	broker_db_meta *db_meta = get_db_meta(MyDatabaseId);
	dsm_segment *segment = dsm_attach_and_pin(db_meta->handle);
	ipc_queue *ipcq = get_bg_worker_queue(segment, idx, false, true);
	ipc_queue_lock(ipcq, true);
	return ipcq;
}

void
signal_ipc_broker_process(void)
{
	if (!pg_atomic_unlocked_test_flag(&broker_meta->waiting))
		SetLatch(broker_meta->latch);
}
