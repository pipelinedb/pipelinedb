/*-------------------------------------------------------------------------
 *
 * microbatch.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "microbatch.h"
#include "pzmq.h"
#include "miscutils.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

int continuous_query_batch_size;
int continuous_query_batch_mem;
int continuous_query_ipc_hwm;

#define MAX_PACKED_SIZE (MAX_MICROBATCH_SIZE - 2048) /* subtract 2kb for buffer for acks */
#define MAX_TUPDESC_SIZE(desc) ((desc)->natts * (sizeof(NameData) + (3 * sizeof(int))))
#define MAX_MICROBATCHES MaxBackends

typedef struct MicrobatchAckShmemStruct
{
	pg_atomic_uint64 counter;
	microbatch_ack_t acks[1];
} MicrobatchAckShmemStruct;

static MicrobatchAckShmemStruct *MicrobatchAckShmem = NULL;

/*
 * MicrobatchAckShmemSize
 */
Size
MicrobatchAckShmemSize(void)
{
	Size size;

	size = sizeof(MicrobatchAckShmemStruct);
	size = mul_size(sizeof(microbatch_ack_t), MAX_MICROBATCHES);

	return size;
}

/*
 * MicrobatchAckShmemInit
 */
void
MicrobatchAckShmemInit(void)
{
	bool found;

	MicrobatchAckShmem = (MicrobatchAckShmemStruct *)
			ShmemInitStruct("MicrobatchAckShmem", MicrobatchAckShmemSize(), &found);

	if (!found)
	{
		int i;

		MemSet(MicrobatchAckShmem, 0, MicrobatchAckShmemSize());
		pg_atomic_init_u64(&MicrobatchAckShmem->counter, 0);

		for (i = 0; i < MAX_MICROBATCHES; i++)
		{
			microbatch_ack_t *ack = &MicrobatchAckShmem->acks[i];
			pg_atomic_init_u64(&ack->id, 0);
			pg_atomic_init_u32(&ack->num_cacks, 0);
			pg_atomic_init_u32(&ack->num_cacks, 0);
			pg_atomic_init_u32(&ack->num_ctups, 0);
			pg_atomic_init_u32(&ack->num_wacks, 0);
			pg_atomic_init_u32(&ack->num_wrecv, 0);
			pg_atomic_init_u32(&ack->num_wtups, 0);
		}
	}
}

/*
 * microbatch_ipc_init
 */
void
microbatch_ipc_init(void)
{
	pzmq_init(MAX_MICROBATCH_SIZE,
			continuous_query_ipc_hwm,
			num_workers + num_combiners,
			!IsContQueryProcess());
}

/*
 * microbatch_ack_new
 */
microbatch_ack_t *
microbatch_ack_new(StreamInsertLevel level)
{
	microbatch_ack_t *ack;
	uint64 id;
	id = level;
	id <<= 62L;
	id |= (rand() ^ (int) MyProcPid) & 0x3fffffffffffffff;

	for (;;)
	{
		int i = pg_atomic_fetch_add_u64(&MicrobatchAckShmem->counter, 1) % MAX_MICROBATCHES;
		uint64 zero = 0;

		ack = &MicrobatchAckShmem->acks[i];
		if (!pg_atomic_compare_exchange_u64(&ack->id, &zero, id))
			continue;

		pg_atomic_write_u32(&ack->num_cacks, 0);
		pg_atomic_write_u32(&ack->num_cacks, 0);
		pg_atomic_write_u32(&ack->num_ctups, 0);
		pg_atomic_write_u32(&ack->num_wacks, 0);
		pg_atomic_write_u32(&ack->num_wrecv, 0);
		pg_atomic_write_u32(&ack->num_wtups, 0);

		/*
		 * TODO(usmanm): If MAX_MICROBATCHES insert procs crash before freeing their ack,
		 * we could run out out acks and keep on spinning here. Seems unlikely, but a workaround
		 * is eventually needed.
		 */
		break;
	}

	return ack;
}

/*
 * microbatch_ack_free
 */
void
microbatch_ack_free(microbatch_ack_t *ack)
{
	pg_atomic_write_u64(&ack->id, 0);
}

/*
 * microbatch_ack_wait
 */
bool
microbatch_ack_wait(microbatch_ack_t *ack, ContQueryDatabaseMetadata *db_meta, uint64 start_generation)
{
	bool success = false;
	uint64 generation;
	StreamInsertLevel level = microbatch_ack_get_level(ack);

	if (level == STREAM_INSERT_ASYNCHRONOUS)
		return true;

	for (;;)
	{
		if (level == STREAM_INSERT_SYNCHRONOUS_RECEIVE && microbatch_ack_is_received(ack))
		{
			success = true;
			break;
		}
		else if ((level == STREAM_INSERT_SYNCHRONOUS_COMMIT || level == STREAM_INSERT_FLUSH) && microbatch_ack_is_acked(ack))
		{
			success = true;
			break;
		}

		/*
		 * If start_generation is 0, it means the microbatch was sent before workers have finished starting up,
		 * so in that case we'll just keep waiting.
		 */
		generation = pg_atomic_read_u64(&db_meta->generation);
		if (start_generation != 0 && generation != start_generation)
		{
			Assert(generation > start_generation);
			break;
		}

		/* TODO(usmanm): exponential backoff? */
		pg_usleep(1000);
		CHECK_FOR_INTERRUPTS();
	}

	return success;
}

/*
 * microbatch_new
 */
microbatch_t *
microbatch_new(microbatch_type_t type, Bitmapset *queries, TupleDesc desc)
{
	microbatch_t *mb = palloc0(sizeof(microbatch_t));

	mb->type = type;
	mb->queries = queries;
	mb->desc = desc;
	mb->buf = makeStringInfo();

	mb->packed_size = sizeof(microbatch_type_t);
	mb->packed_size += sizeof(int); /* number of tuples */
	mb->packed_size += sizeof(int); /* number of acks */

	if (type == WorkerTuple)
	{
		int i;

		Assert(bms_num_members(queries));

		mb->packed_size += BITMAPSET_SIZE(queries->nwords); /* queries */
		mb->packed_size += sizeof(int); /* number of record descs */

		mb->packed_size += MAX_TUPDESC_SIZE(desc); /* upper bound on packed size */

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(desc, i);
			TupleDesc rdesc;
			tagged_ref_t *ref;

			if (attr->atttypid != RECORDOID)
				continue;

			Assert(attr->atttypmod != -1);
			rdesc = lookup_rowtype_tupdesc(RECORDOID, attr->atttypmod);

			ref = palloc(sizeof(tagged_ref_t));
			ref->tag = attr->atttypmod;
			ref->ptr = rdesc;

			mb->record_descs = lappend(mb->record_descs, ref);
			mb->packed_size += sizeof(int); /* typMod */
			mb->packed_size += MAX_TUPDESC_SIZE(desc);
		}
	}
	else if (type == CombinerTuple)
	{
		Assert(!desc);
		Assert(type == CombinerTuple);
		Assert(bms_num_members(queries) == 1);

		mb->packed_size += sizeof(Oid); /* query id */
	}
	else
	{
		Assert(type == FlushTuple);
		Assert(!queries);
		Assert(!desc);
	}

	return mb;
}

/*
 * microbatch_reset
 */
void
microbatch_reset(microbatch_t *mb)
{
	if (mb->tups)
		pfree(mb->tups);

	mb->tups = NULL;
	mb->ntups = 0;

	resetStringInfo(mb->buf);
}

/*
 * microbatch_destroy
 */
void
microbatch_destroy(microbatch_t *mb)
{
	microbatch_reset(mb);

	list_free_deep(mb->record_descs);
	list_free_deep(mb->acks);
	pfree(mb->buf->data);
	pfree(mb->buf);
	pfree(mb);
}

/*
 * microbatch_add_ack
 */
void
microbatch_add_ack(microbatch_t *mb, microbatch_ack_t *ack)
{
	tagged_ref_t *ref = palloc(sizeof(tagged_ref_t));
	ref->tag = pg_atomic_read_u64(&ack->id);
	ref->ptr = ack;

	mb->acks = lappend(mb->acks, ref);
	mb->packed_size += sizeof(tagged_ref_t);
}

/*
 * microbatch_add_tuple
 */
bool
microbatch_add_tuple(microbatch_t *mb, HeapTuple tup, uint64 hash)
{
	int tup_size = HEAPTUPLESIZE + tup->t_len;

	if (tup_size > MAX_PACKED_SIZE)
		elog(ERROR, "tuple is too large to fit in a microbatch");

	if (mb->allow_iter)
		elog(ERROR, "microbatch is read only, can't add tuples");

	if (mb->ntups >= continuous_query_batch_size)
		return false;

	if (tup_size + mb->packed_size + mb->buf->len >= MAX_PACKED_SIZE)
		return false;

	appendBinaryStringInfo(mb->buf, (char *) tup, HEAPTUPLESIZE);
	appendBinaryStringInfo(mb->buf, (char *) tup->t_data, tup->t_len);

	if (mb->type == CombinerTuple)
	{
		Assert(hash);
		appendBinaryStringInfo(mb->buf, (char *) &hash, sizeof(uint64));
	}

	mb->ntups++;

	return true;
}

/*
 * pack_tupdesc
 */
static char *
pack_tupdesc(char *buf, TupleDesc desc)
{
	int i;

	memcpy(buf, &desc->natts, sizeof(int));
	buf += sizeof(int);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		int len;

		len = strlen(NameStr(attr->attname)) + 1;
		memcpy(buf, &NameStr(attr->attname), len);
		buf += len;

		memcpy(buf, &attr->atttypid, sizeof(Oid));
		buf += sizeof(Oid);
		memcpy(buf, &attr->atttypmod, sizeof(int));
		buf += sizeof(int);
		memcpy(buf, &attr->attcollation, sizeof(int));
		buf += sizeof(int);
	}

	return buf;
}

/*
 * unpack_tupdesc
 */
static char *
unpack_tupdesc(char *buf, TupleDesc *desc)
{
	int nattrs;
	int i;
	List *names = NIL;
	List *types = NIL;
	List *mods = NIL;
	List *collations = NIL;

	memcpy(&nattrs, buf, sizeof(int));
	buf += sizeof(int);

	for (i = 0; i < nattrs; i++)
	{
		Oid typid;
		int typmod;
		int collation;
		char *name;

		name = buf;
		buf += strlen(name) + 1;

		memcpy(&typid, buf, sizeof(Oid));
		buf += sizeof(Oid);
		memcpy(&typmod, buf, sizeof(int));
		buf += sizeof(int);
		memcpy(&collation, buf, sizeof(int));
		buf += sizeof(int);

		names = lappend(names, makeString(name));
		types = lappend_int(types, typid);
		mods = lappend_int(mods, typmod);
		collations = lappend_int(collations, collation);
	}

	*desc = BuildDescFromLists(names, types, mods, collations);

	list_free_deep(names);
	list_free(types);
	list_free(mods);
	list_free(collations);

	return buf;
}

/*
 * microbatch_pack_for_queue
 */
char *
microbatch_pack_for_queue(uint64 recv_id, char *packed, int *len)
{
	int new_len;

	Assert(len);

	new_len = *len;
	new_len += sizeof(uint64);

	/*
	 * We prefix the microbatch with the recv_id so that the queue proc knows where to send it
	 */
	packed = repalloc(packed, new_len);
	memmove(packed + sizeof(uint64), packed, *len);
	memcpy(packed, &recv_id, sizeof(uint64));

	*len = new_len;

	return packed;
}

/*
 * microbatch_pack
 */
char *
microbatch_pack(microbatch_t *mb, int *len)
{
	char *buf = palloc0(mb->packed_size + mb->buf->len);
	char *pos = buf;
	int nacks = list_length(mb->acks);
	int packed_size;
	ListCell *lc;

	Assert(!mb->allow_iter);
	Assert(mb->tups == NULL);

	Assert(mb->packed_size + mb->buf->len <= MAX_PACKED_SIZE);

	memcpy(pos, &mb->type, sizeof(microbatch_type_t));
	pos += sizeof(microbatch_type_t);

	/* Pack acks */
	memcpy(pos, &nacks, sizeof(int));
	pos += sizeof(int);

	foreach(lc, mb->acks)
	{
		tagged_ref_t *ref = lfirst(lc);
		memcpy(pos, ref, sizeof(tagged_ref_t));
		pos += sizeof(tagged_ref_t);
	}

	/* Pack tuples */
	memcpy(pos, &mb->ntups, sizeof(int));
	pos += sizeof(int);
	memcpy(pos, mb->buf->data, mb->buf->len);
	pos += mb->buf->len;

	if (mb->type == WorkerTuple)
	{
		int nrecord_descs;

		/* Pack desc */
		pos = pack_tupdesc(pos, mb->desc);

		/* Pack record descs */
		nrecord_descs = list_length(mb->record_descs);
		memcpy(pos, &nrecord_descs, sizeof(int));
		pos += sizeof(int);

		foreach(lc, mb->record_descs)
		{
			tagged_ref_t *ref = lfirst(lc);

			/* Pack typMod */
			memcpy(pos, &ref->tag, sizeof(int));
			pos += sizeof(int);

			pos = pack_tupdesc(pos, (TupleDesc) ref->ptr);
		}

		/* Pack queries */
		memcpy(pos, mb->queries, BITMAPSET_SIZE(mb->queries->nwords));
		pos += BITMAPSET_SIZE(mb->queries->nwords);
	}
	else if (mb->type == CombinerTuple)
	{
		/* Pack query id */
		Oid id = bms_next_member(mb->queries, -1);
		memcpy(pos, &id, sizeof(Oid));
		pos += sizeof(Oid);
	}

	packed_size = (uintptr_t) pos - (uintptr_t) buf;
	Assert(packed_size <= mb->packed_size + mb->buf->len);
	Assert(packed_size <= MAX_MICROBATCH_SIZE);
	mb->packed_size = packed_size;
	*len = packed_size;

	return buf;
}

/*
 * microbatch_unpack
 */
microbatch_t *
microbatch_unpack(char *buf, int len)
{
	microbatch_t *mb = palloc0(sizeof(microbatch_t));
	char *pos = buf;
	int nacks;
	int i;

	mb->allow_iter = true;

	memcpy(&mb->type, pos, sizeof(microbatch_type_t));
	pos += sizeof(microbatch_type_t);

	/* Unpack acks */
	memcpy(&nacks, pos, sizeof(int));
	pos += sizeof(int);

	for (i = 0; i < nacks; i++)
	{
		tagged_ref_t *ref = (tagged_ref_t *) pos;
		pos += sizeof(tagged_ref_t);
		mb->acks = lappend(mb->acks, ref);
	}

	/* Unpack tuples */
	memcpy(&mb->ntups, pos, sizeof(int));
	pos += sizeof(int);

	mb->tups = palloc(sizeof(tagged_ref_t) * mb->ntups);

	for (i = 0; i < mb->ntups; i++)
	{
		tagged_ref_t *ref = &mb->tups[i];
		HeapTuple tup = (HeapTuple) pos;
		pos += HEAPTUPLESIZE;
		tup->t_data = (HeapTupleHeader) pos;
		pos += tup->t_len;

		ref->ptr = tup;

		if (mb->type == CombinerTuple)
		{
			memcpy(&ref->tag, pos, sizeof(uint64));
			pos += sizeof(uint64);
		}
		else
			ref->tag = 0;
	}

	if (mb->type == WorkerTuple)
	{
		int nrecord_descs;

		/* Unpack desc */
		pos = unpack_tupdesc(pos, &mb->desc);

		/* Unpack record descs */
		memcpy(&nrecord_descs, pos, sizeof(int));
		pos += sizeof(int);

		for (i = 0; i < nrecord_descs; i++)
		{
			tagged_ref_t *ref = palloc(sizeof(tagged_ref_t));

			/* Unpack typMod */
			memcpy(&ref->tag, pos, sizeof(int));
			pos += sizeof(int);

			pos = unpack_tupdesc(pos, (TupleDesc *) &ref->ptr);
		}

		/* Unpack queries */
		mb->queries = (Bitmapset *) pos;
		pos += BITMAPSET_SIZE(mb->queries->nwords);
	}
	else if (mb->type == CombinerTuple)
	{
		/* Unpack query id */
		Oid query_id;
		memcpy(&query_id, pos, sizeof(Oid));
		pos += sizeof(Oid);
		mb->queries = bms_make_singleton(query_id);
	}

	mb->packed_size = (uintptr_t) pos - (uintptr_t) buf;
	Assert(mb->packed_size <= MAX_PACKED_SIZE);
	Assert(mb->packed_size == len);

	return mb;
}

/*
 * microbatch_send
 */
void
microbatch_send(microbatch_t *mb, uint64 recv_id, bool async, ContQueryDatabaseMetadata *db_meta)
{
	int len;
	char *buf = microbatch_pack(mb, &len);

	pzmq_connect(recv_id);

	if (!async)
	{
		/*
		 * Simple blocking write
		 */
		for (;;)
		{
			if (pzmq_send(recv_id, buf, len, true))
				break;

			if (get_sigterm_flag())
				break;
		}
	}
	else if (!pzmq_send(recv_id, buf, len, false))
	{
		/*
		 * It's an asynchronous write, which works as follows:
		 *
		 * 1) Attempt a nonblocking write to the given socket, if it succeeds, we're done
		 * 2) The nonblocking write failed, so we do a blocking write to the queue process, which
		 *    will eventually write the batch to the target receiver.
		 */
		int queue_id = rand() % num_queues;
		int offset = num_workers + num_combiners;

		/* TODO(derekjn) encapsulate this offset arithmetic in a function */
		queue_id = db_meta->db_procs[offset + queue_id].pzmq_id;
		buf = microbatch_pack_for_queue(recv_id, buf, &len);

		pzmq_connect(queue_id);

		/*
		 * Async writes are used to prevent blocking write cycles between processes,
		 * so it might seem strange to do a blocking write here. However, the queue process
		 * by design will never block indefinitely, so this is fine.
		 */
		for (;;)
		{
			if (pzmq_send(queue_id, buf, len, true))
				break;

			if (get_sigterm_flag())
				break;
		}
	}

	pfree(buf);
}

/*
 * microbatch_add_acks
 */
void
microbatch_add_acks(microbatch_t *mb, List *acks)
{
	ListCell *lc;
	MemoryContext old;

	old = MemoryContextSwitchTo(ContQueryBatchContext);

	foreach(lc, acks)
	{
		tagged_ref_t *ref = lfirst(lc);
		if (microbatch_ack_ref_is_valid(ref))
			microbatch_add_ack(mb, (microbatch_ack_t *) ref->ptr);
	}

	MemoryContextSwitchTo(old);
}

/*
 * microbatch_send_to_worker
 */
void
microbatch_send_to_worker(microbatch_t *mb, int worker_id)
{
	ContQueryDatabaseMetadata *db_meta = GetMyContQueryDatabaseMetadata();
	int recv_id;
	bool async = false;

	if (worker_id == -1)
	{
		if (IsContQueryCombinerProcess())
		{
			/*
			 * Combiners need to shard over workers so that updates to a specific group are always
			 * written in order to the output stream.
			 */
			worker_id = MyContQueryProc->group_id % num_workers;

			/*
			 * It's a combiner -> worker (output stream) write, so we need the write to be asynchronous
			 * to prevent blocking write cycles between combiner and worker procs.
			 */
			async = true;
		}
		else if (IsContQueryWorkerProcess())
		{
			worker_id = rand() % num_workers;

			/*
			 * It's a worker -> worker write, which means we're a transform writing to a stream.
			 * We make these writes asynchronous to prevent blocking write cycles between worker procs.
			 */
			async = true;
		}
		else
		{
			/*
			 * We're a client write process (INSERT or COPY), so we can do a blocking write to the worker
			 * proc because blocking write cycles are not possible in this case.
			 */

			/* TODO(usmanm): Poll all workers and send to first non-blocking one? */
			worker_id = rand() % num_workers;
		}
	}

	recv_id = db_meta->db_procs[worker_id].pzmq_id;

	microbatch_send(mb, recv_id, async, db_meta);
	microbatch_reset(mb);
}

/*
 * microbatch_send_to_combiner
 */
void
microbatch_send_to_combiner(microbatch_t *mb, int combiner_id)
{
	static ContQueryDatabaseMetadata *db_meta = NULL;
	int recv_id;

	if (!db_meta)
		db_meta = GetContQueryDatabaseMetadata(MyDatabaseId);

	recv_id = db_meta->db_procs[num_workers + combiner_id].pzmq_id;

	microbatch_send(mb, recv_id, true, db_meta);
	microbatch_reset(mb);
}
