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
#include "pipeline/stream.h"
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
#define num_queues_per_db (continuous_query_num_workers * 3 + continuous_query_num_combiners)
#define num_locks_per_db \
	((continuous_query_num_workers * 2) + continuous_query_num_combiners + 1) /* +1 for all ephemeral ipc queues */
#define ipc_queue_size MAXALIGN(continuous_query_ipc_shared_mem * 1024 / num_queues_per_db)
#define db_dsm_segment_size (ipc_queue_size * num_queues_per_db)
#define broker_db_meta_size (sizeof(broker_db_meta) + (max_worker_processes * sizeof(dsm_segment_slot)))
#define num_queues_per_worker 3
#define num_queues_per_combiner 1
#define max_locks (max_worker_processes * 2)

typedef struct local_queue
{
	uint64 size;
	List *slots;
} local_queue;

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

	/* in local memory */
	local_queue *lqueues;

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
	lw_lock_slot locks[1]; /* num_locks total slots */
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
	size = add_size(size, max_locks * MAXALIGN(sizeof(lw_lock_slot)));
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

	if (!TimestampDifferenceExceeds(last_purge_time, GetCurrentTimestamp(), 10 * 1000)) /* 10s */
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

			if (db_meta->lqueues)
			{
				int i;

				for (i = 0; i < continuous_query_num_workers; i++)
				{
					local_queue *local_buf = &db_meta->lqueues[i];
					if (local_buf->slots)
						list_free_deep(local_buf->slots);
				}

				pfree(db_meta->lqueues);
			}

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
	if (segment)
		dsm_pin_mapping(segment);

	res = CurrentResourceOwner;
	CurrentResourceOwner = old;
	ResourceOwnerDelete(res);

	return segment;
}

static int
copy_lq_to_bwq(local_queue *local_buf, ipc_queue *bwq, uint64 *bwq_head, uint64 bwq_tail, bool *isfull)
{
	ListCell *lc;
	int nremoved = 0;
	uint64 head = *bwq_head;
	int free = ipc_queue_free_size(bwq, head, bwq_tail);
	int copied = 0;

	foreach(lc, local_buf->slots)
	{
		ipc_queue_slot *src_slot = lfirst(lc);
		ipc_queue_slot *dest_slot = ipc_queue_slot_get(bwq, head);
		int len_needed = src_slot->next - head;
		char *dest_bytes;

		if (len_needed > free)
		{
			*isfull = true;
			break;
		}

		free -= len_needed;

		*dest_slot = *src_slot;
		dest_slot->peeked = false;

		dest_bytes = src_slot->wraps ? bwq->bytes : dest_slot->bytes;
		ipc_queue_check_overflow(bwq, dest_bytes, src_slot->len);

		memcpy(dest_bytes, src_slot->bytes, src_slot->len);

		head = src_slot->next;
		dest_slot->next = src_slot->next;

		local_buf->size -= src_slot->len;

		pfree(src_slot);
		nremoved++;
	}

	copied = nremoved;

	while (nremoved--)
		local_buf->slots = list_delete_first(local_buf->slots);

	*bwq_head = head;

	return copied;
}

static uint64
copy_wbq_to_bwq(ipc_queue *wbq, ipc_queue *bwq, uint64 *bwq_head, uint64 bwq_tail, bool *isfull)
{
	uint64 wbq_head = pg_atomic_read_u64(&wbq->head);
	uint64 wbq_tail = wbq->cursor;
	int free;
	int count = 0;

	Assert(wbq->consumed_by_broker);
	Assert(wbq_tail == *bwq_head);

	free = ipc_queue_free_size(bwq, wbq_tail, bwq_tail);
	Assert(free >= 0);

	while (true)
	{
		ipc_queue_slot *src_slot;
		ipc_queue_slot *dest_slot;
		int len_needed;
		char *src_bytes;
		char *dest_bytes;

		Assert(wbq_tail <= wbq_head);

		/* no data in src queue? update head and recheck */
		if (wbq_tail == wbq_head)
		{
			wbq_head = pg_atomic_read_u64(&wbq->head);
			if (wbq_tail == wbq_head)
				break;
		}

		src_slot = ipc_queue_slot_get(wbq, wbq_tail);
		dest_slot =  ipc_queue_slot_get(bwq, wbq_tail);

		len_needed = src_slot->next - wbq_tail;

		if (len_needed > free)
		{
			*isfull = true;
			break;
		}

		free -= len_needed;

		*dest_slot = *src_slot;
		dest_slot->peeked = false;

		src_bytes = src_slot->wraps ? wbq->bytes : src_slot->bytes;
		dest_bytes = src_slot->wraps ? bwq->bytes : dest_slot->bytes;
		ipc_queue_check_overflow(bwq, dest_bytes, src_slot->len);

		memcpy(dest_bytes, src_bytes, src_slot->len);

		wbq_tail = src_slot->next;

		count++;
	}

	wbq->cursor = wbq_tail;
	*bwq_head = wbq_tail;

	return count;
}

static int
copy_wbq_to_lq(ipc_queue *wbq, local_queue *local_buf)
{
	uint64 tail = wbq->cursor;
	uint64 head = pg_atomic_read_u64(&wbq->head);
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	int count = 0;

	while (local_buf->size < ipc_queue_size)
	{
		ipc_queue_slot *src_slot;
		ipc_queue_slot *local_slot;

		/* is the wbq empty? */
		if (tail == head)
		{
			head = pg_atomic_read_u64(&wbq->head);
			if (tail == head)
				break;
		}

		src_slot = ipc_queue_slot_get(wbq, tail);
		local_slot = palloc0(sizeof(ipc_queue_slot) + src_slot->len);

		*local_slot = *src_slot;

		if (src_slot->wraps)
			memcpy(local_slot->bytes, wbq->bytes, src_slot->len);
		else
			memcpy(local_slot->bytes, src_slot->bytes, src_slot->len);

		local_buf->slots = lappend(local_buf->slots, local_slot);
		local_buf->size += local_slot->len;

		tail = src_slot->next;

		count++;
	}

	MemoryContextSwitchTo(old);

	wbq->cursor = tail;

	return count;
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

		if (db_meta->lqueues == NULL)
			db_meta->lqueues = MemoryContextAllocZero(CacheMemoryContext,
					sizeof(local_queue) * continuous_query_num_workers);

		if (db_meta->segment == NULL)
			continue;

		ptr = dsm_segment_address(db_meta->segment);

		for (i = 0; i < continuous_query_num_workers; i++)
		{
			ipc_queue *bwq; /* broker->worker queue */
			ipc_queue *wbq; /* worker->broker queue */
			local_queue *local_buf = &db_meta->lqueues[i];
			uint64 last_wbq_cur;
			uint64 bwq_head;
			int bwq_inserted = 0;
			uint64 bwq_tail;
			bool bwq_is_full = false;

			bwq = (ipc_queue *) ptr;
			ptr += ipc_queue_size;
			wbq = (ipc_queue *) ptr;
			ptr += 2 * ipc_queue_size;

			/* check some invariants */
			Assert(bwq->produced_by_broker);
			Assert(bwq->copy_fn == NULL);
			Assert(wbq->consumed_by_broker);
			Assert(wbq->peek_fn == NULL);
			Assert(wbq->pop_fn == NULL);
			Assert(wbq->cursor == pg_atomic_read_u64(&wbq->tail));

			bwq_head = pg_atomic_read_u64(&bwq->head);
			last_wbq_cur = wbq->cursor;

			bwq_tail = pg_atomic_read_u64(&bwq->tail);

			/* locally cache slots? insert them first */
			if (local_buf->slots)
				bwq_inserted += copy_lq_to_bwq(local_buf, bwq, &bwq_head, bwq_tail, &bwq_is_full);

			/* is there no more local data? */
			if (!bwq_is_full && !local_buf->slots)
				/* try copying from wbq to bwq */
				bwq_inserted += copy_wbq_to_bwq(wbq, bwq, &bwq_head, bwq_tail, &bwq_is_full);

			if (bwq_inserted)
			{
				Assert(bwq_head);
				ipc_queue_update_head(bwq, bwq_head);
				num_copied += bwq_inserted;
			}

			num_copied += copy_wbq_to_lq(wbq, local_buf);

			if (wbq->cursor > last_wbq_cur)
			{
				Latch *latch;
				pg_atomic_write_u64(&wbq->tail, wbq->cursor);
				latch = (Latch *) pg_atomic_read_u64(&wbq->producer_latch);
				if (latch)
					SetLatch(latch);
			}
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return num_copied;
}

static bool
have_no_pending_messages_or_out_of_space(void)
{
	HASH_SEQ_STATUS status;
	broker_db_meta *db_meta;
	bool success = true;

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
			ipc_queue *dst;
			ipc_queue *src;
			char *pos;
			uint64 dst_head;
			uint64 dst_tail;
			int free;
			ipc_queue_slot *slot = NULL;

			pos = ptr + i * num_queues_per_worker * ipc_queue_size;
			dst = (ipc_queue *) pos;
			pos += ipc_queue_size;
			src = (ipc_queue *) pos;

			Assert(dst->produced_by_broker);
			Assert(src->consumed_by_broker);

			dst_head = pg_atomic_read_u64(&dst->head);
			dst_tail = pg_atomic_read_u64(&dst->tail);
			free = ipc_queue_free_size(dst, dst_head, dst_tail);

			if (db_meta->lqueues[i].size)
				slot = (ipc_queue_slot *) linitial(db_meta->lqueues[i].slots);
			else if (!ipc_queue_is_empty(src))
				slot = (ipc_queue_slot *) ipc_queue_slot_get(src, src->cursor);

			if (slot)
			{
				int len_needed = slot->next - dst_head;
				if (free > len_needed)
				{
					success = false;
					goto end;
				}
			}
		}
	}

end:
	if (!success)
		hash_seq_term(&status);

	LWLockRelease(IPCMessageBrokerIndexLock);

	return success;
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

	/* Loop forever */
	for (;;)
	{
		int num_copied;

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(work_ctx);
		MemoryContextReset(work_ctx);

		purge_dropped_db_segments();

		num_copied = copy_messages();

		/* If we didn't copy any messages, try to sleep and wait for new ones to arrive. */
		if (!num_copied)
		{
			/* Mark as waiting */
			pg_atomic_test_set_flag(&broker_meta->waiting);

			/* If we have no pending messages or out of space is dest ipc_queues, sleep till we get signaled. */
			if (have_no_pending_messages_or_out_of_space())
			{
				int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
				pg_atomic_clear_flag(&broker_meta->waiting);
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
		Assert(db_meta->lock_idx + num_locks_per_db < max_locks);
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
			ipc_queue *ipcq;

			/*
			 * We have three queues per worker process, two are multi producer queues which requires a LWLock,
			 * the other two are single producer queues and don't require a LWLock. The MP queue is used by the
			 * insert processes to write tuples to the worker process. One SP queue is used by the worker process to
			 * write data to itself (or by other worker processes to write data to it). The last SP queue is used
			 * by the IPC broker to copy data from the first two queues so that the worker can read the data being
			 * sent to it.
			 */

			/* broker->worker queue */
			ipcq = (ipc_queue *) ptr;
			ipc_queue_init(ptr, ipc_queue_size, NULL);
			ipc_queue_set_handlers(ipcq, StreamTupleStatePeekFn, popfn, NULL);
			ipcq->produced_by_broker = true;
			ptr += ipc_queue_size;

			/* We use the same lock for these two producer queues */

			/* worker->broker queue */
			ipcq = (ipc_queue *) ptr;
			ipc_queue_init(ptr, ipc_queue_size, &lock_slot->lock);
			ipc_queue_set_handlers(ipcq, NULL, NULL, StreamTupleStateCopyFn);
			ipcq->consumed_by_broker = true;
			ptr += ipc_queue_size;
			lock_slot++;

			/* insert->worker queue */
			ipc_queue_init(ptr, ipc_queue_size, &lock_slot->lock);
			ipc_queue_set_handlers((ipc_queue *) ptr, StreamTupleStatePeekFn, popfn, StreamTupleStateCopyFn);
			ptr += ipc_queue_size;
			lock_slot++;
		}

		for (i = 0; i < continuous_query_num_combiners; i++)
		{
			ipc_queue_pop_fn popfn = synchronous_stream_insert ? PartialTupleStatePopFn : NULL;

			/* worker->combiner queue */
			ipc_queue_init(ptr, ipc_queue_size, &lock_slot->lock);
			ipc_queue_set_handlers((ipc_queue *) ptr, PartialTupleStatePeekFn, popfn, PartialTupleStateCopyFn);
			ptr += ipc_queue_size;
			lock_slot++;
		}
	}

	LWLockRelease(IPCMessageBrokerIndexLock);

	return db_meta;
}


static ipc_queue *
get_combiner_ipcq(dsm_segment *segment, int group_id)
{
	char *ptr = dsm_segment_address(segment);
	ipc_queue *ipcq;

	ptr += ipc_queue_size * continuous_query_num_workers * num_queues_per_worker;
	ptr += ipc_queue_size * group_id * num_queues_per_combiner;

	ipcq = (ipc_queue *) ptr;

	return ipcq;
}

static ipc_queue *
get_worker_ipcq(dsm_segment *segment, int group_id, bool for_producer, bool broker_handled)
{
	char *ptr = dsm_segment_address(segment);

	ptr += ipc_queue_size * group_id * num_queues_per_worker;

	if (!broker_handled)
		ptr += 2 * ipc_queue_size; /* skip over to insert -> worker queue */
	else if (for_producer)
		ptr += ipc_queue_size; /* skip over to the worker -> broker queue */

	return (ipc_queue *) ptr;
}

ipc_queue *
acquire_my_broker_ipc_queue(void)
{
	Assert(my_ipc_meta);
	Assert(IsContQueryWorkerProcess());

	return get_worker_ipcq(my_ipc_meta->segment, MyContQueryProc->group_id, false, true);
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
				&broker_meta->locks[db_meta->lock_idx + num_bg_workers_per_db].lock);
		ipc_queue_set_handlers(my_ipc_meta->queue, StreamTupleStatePeekFn, StreamTupleStatePopFn, StreamTupleStateCopyFn);

		MyContQueryProc->dsm_handle = dsm_segment_handle(segment);
	}
	else
	{
		/*
		 * For worker and combiner processes, we simply connect to the DB's primary dsm_segment
		 * and find the correct ipc_queue(s) within that dsm_segment.
		 */
		my_ipc_meta->segment = dsm_attach_and_pin(db_meta->handle);
		my_ipc_meta->seg_slot = NULL;

		if (IsContQueryCombinerProcess())
			my_ipc_meta->queue = get_combiner_ipcq(my_ipc_meta->segment, MyContQueryProc->group_id);
		else
			my_ipc_meta->queue = get_worker_ipcq(my_ipc_meta->segment, MyContQueryProc->group_id, false, false);

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
	static bool broker_handled;
	int ntries = 0;
	ipc_queue *ipcq = NULL;
	broker_db_meta *db_meta = get_db_meta(MyDatabaseId);
	dsm_segment *segment = dsm_attach_and_pin(db_meta->handle);

	if (idx == -1)
	{
		idx = rand() % continuous_query_num_workers;
		broker_handled = IsContQueryWorkerProcess();
	}

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

		ipcq = get_worker_ipcq(segment, idx, true, broker_handled);

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
	ipc_queue *ipcq = get_combiner_ipcq(segment, idx);
	ipc_queue_lock(ipcq, true);
	return ipcq;
}

void
signal_ipc_broker_process(void)
{
	if (!pg_atomic_unlocked_test_flag(&broker_meta->waiting))
		SetLatch(broker_meta->latch);
}
