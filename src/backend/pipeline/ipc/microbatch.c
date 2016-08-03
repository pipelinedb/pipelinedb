/*-------------------------------------------------------------------------
 *
 * microbatch.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/ipc/pzmq.h"
#include "pipeline/miscutils.h"
#include "storage/shm_alloc.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

int continuous_query_num_batch;
int continuous_query_batch_size;

#define MAX_PACKED_SIZE (MAX_MICROBATCH_SIZE - 2048) /* subtract 2kb for buffer for acks */
#define MAX_TUPDESC_SIZE(desc) ((desc)->natts * (sizeof(NameData) + (3 * sizeof(int))))

microbatch_ack_t *
microbatch_ack_new(void)
{
	microbatch_ack_t *ack = (microbatch_ack_t *) ShmemDynAlloc0(sizeof(microbatch_ack_t));
	ack->id = rand() ^ (int) MyProcPid;
	pg_atomic_init_flag(&ack->read);
	pg_atomic_init_u32(&ack->num_cacks, 0);
	pg_atomic_init_u32(&ack->num_ctups, 0);
	pg_atomic_init_u32(&ack->num_wacks, 0);
	pg_atomic_init_u32(&ack->num_wtups, 0);
	return ack;
}

void
microbatch_ack_destroy(microbatch_ack_t *ack)
{
	ShmemDynFree(ack);
}

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

		Assert(bms_num_members(queries) >= 1);

		mb->packed_size += BITMAPSET_SIZE(queries->nwords); /* queries */
		mb->packed_size += sizeof(int); /* number of record descs */

		mb->packed_size += MAX_TUPDESC_SIZE(desc); /* upper bound on packed size */

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute attr = desc->attrs[i];
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
	else
	{
		Assert(desc == NULL);
		Assert(type == CombinerTuple);
		Assert(bms_num_members(queries) == 1);

		mb->packed_size += sizeof(Oid); /* query id */
	}

	return mb;
}

void
microbatch_reset(microbatch_t *mb)
{
	if (mb->tups)
		pfree(mb->tups);

	mb->tups = NULL;
	mb->ntups = 0;

	resetStringInfo(mb->buf);
}

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

void
microbatch_add_ack(microbatch_t *mb, microbatch_ack_t *ack)
{
	tagged_ref_t *ref = palloc(sizeof(tagged_ref_t));
	ref->tag = ack->id;
	ref->ptr = ack;

	mb->acks = lappend(mb->acks, ref);
	mb->packed_size += sizeof(tagged_ref_t);
}

bool
microbatch_add_tuple(microbatch_t *mb, HeapTuple tup, uint64 hash)
{
	int tup_size = HEAPTUPLESIZE + tup->t_len;

	if (tup_size > MAX_PACKED_SIZE)
		elog(ERROR, "tuple is too large to fit in a microbatch");

	if (mb->allow_iter)
		elog(ERROR, "microbatch is read only, can't add more tuples");

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

static char *
pack_tupdesc(char *buf, TupleDesc desc)
{
	int i;

	memcpy(buf, &desc->natts, sizeof(int));
	buf += sizeof(int);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = desc->attrs[i];
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
	else
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
	else
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

void
microbatch_send(microbatch_t *mb, uint64 recv_id)
{
	int len;
	char *buf = microbatch_pack(mb, &len);

	pzmq_connect(recv_id);
	/* TODO(usmanm): What to do if interrupted? */
	pzmq_send(recv_id, buf, len, true);

	pfree(buf);
}

void
microbatch_add_acks(microbatch_t *mb, List *acks)
{
	ListCell *lc;
	MemoryContext old;

	old = MemoryContextSwitchTo(ContQueryBatchContext);

	foreach(lc, acks)
	{
		tagged_ref_t *ref = lfirst(lc);
		microbatch_ack_t *ack = (microbatch_ack_t *) ref->ptr;
		if (ref->tag == ack->id)
			microbatch_add_ack(mb, ack);
	}

	MemoryContextSwitchTo(old);
}

void
microbatch_send_to_worker(microbatch_t *mb)
{
	ContQueryDatabaseMetadata *db_meta = GetMyContQueryDatabaseMetadata();
	int recv_id;
	int worker_id;

	if (IsContQueryCombinerProcess())
	{
		/*
		 * Combiners need to shard over works so that updates to a specific group are always
		 * written in order to the output stream.
		 */
		worker_id = MyContQueryProc->group_id % continuous_query_num_workers;
	}
	else
	{
		/* TODO(usmanm): Poll all workers and send to first non-blocking one? */
		worker_id = rand() % continuous_query_num_workers;
	}

	recv_id = db_meta->db_procs[worker_id].pzmq_id;

	microbatch_send(mb, recv_id);
	microbatch_reset(mb);
}
