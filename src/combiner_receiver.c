/*-------------------------------------------------------------------------
 *
 * combiner_receiver.c
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "access/htup_details.h"
#include "access/printtup.h"
#include "catalog/pg_type.h"
#include "combiner_receiver.h"
#include "hashfuncs.h"
#include "microbatch.h"
#include "miscadmin.h"
#include "miscutils.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define MURMUR_SEED 0x155517D2

CombinerReceiveFunc CombinerReceiveHook = NULL;
CombinerFlushFunc CombinerFlushHook = NULL;

static void
combiner_receive(CombinerReceiver *c, TupleTableSlot *slot)
{
	MemoryContext old = MemoryContextSwitchTo(ContQueryBatchContext);
	tagged_ref_t *ref;
	uint32 shard_hash;
	bool received = false;

	if (!c->cont_query)
		c->cont_query = c->cont_exec->curr_query->query;

	Assert(c->cont_query->type == CONT_VIEW);

	ref = palloc(sizeof(tagged_ref_t));
	ref->ptr = ExecCopySlotTuple(slot);

	/* Shard by groups or name if no grouping. */
	if (c->hash_fcinfo)
	{
		ref->tag = slot_hash_group(slot, c->hashfn, c->hash_fcinfo);
		shard_hash = slot_hash_group_skip_attr(slot, c->cont_query->sw_attno, c->hashfn, c->hash_fcinfo);
	}
	else
	{
		ref->tag = c->name_hash;
		shard_hash = c->name_hash;
	}

	if (CombinerReceiveHook)
		received = CombinerReceiveHook(c->cont_query, shard_hash, ref->tag, ref->ptr);

	if (!received)
	{
		int i = get_combiner_for_shard_hash(shard_hash);
		c->tups_per_combiner[i] = lappend(c->tups_per_combiner[i], ref);
	}

	MemoryContextSwitchTo(old);
}

static void
flush_to_combiner(BatchReceiver *receiver, TupleTableSlot *slot)
{
	int i;
	int ntups = 0;
	Size size = 0;
	microbatch_t *mb;
	CombinerReceiver *c = (CombinerReceiver *) receiver;

	foreach_tuple(slot, c->base.buffer)
	{
		combiner_receive(c, slot);
	}

	if (CombinerFlushHook)
		CombinerFlushHook();

	mb = microbatch_new(CombinerTuple, bms_make_singleton(c->cont_query->id), NULL);
	microbatch_add_acks(mb, c->cont_exec->batch->sync_acks);

	for (i = 0; i < num_combiners; i++)
	{
		List *tups = c->tups_per_combiner[i];
		ListCell *lc;

		if (tups == NIL)
			continue;

		ntups += list_length(tups);

		foreach(lc, tups)
		{
			tagged_ref_t *ref = lfirst(lc);
			HeapTuple tup = (HeapTuple) ref->ptr;
			uint64 hash = ref->tag;

			if (!microbatch_add_tuple(mb, tup, hash))
			{
				microbatch_send_to_combiner(mb, i);
				microbatch_add_tuple(mb, tup, hash);
			}

			size += HEAPTUPLESIZE + tup->t_len;
		}

		if (!microbatch_is_empty(mb))
		{
			microbatch_send_to_combiner(mb, i);
			microbatch_reset(mb);
		}

		list_free_deep(tups);
		c->tups_per_combiner[i] = NIL;
	}

	microbatch_acks_check_and_exec(mb->acks, microbatch_ack_increment_ctups, ntups);
	microbatch_destroy(mb);

	StatsIncrementCQWrite(ntups, size);
}

BatchReceiver *
CreateCombinerReceiver(ContExecutor *cont_exec, ContQuery *query, Tuplestorestate *buffer)
{
	CombinerReceiver *self = (CombinerReceiver *) palloc0(sizeof(CombinerReceiver));
	char *relname = get_rel_name(query->relid);

	self->tups_per_combiner = palloc0(sizeof(List *) * num_combiners);
	self->cont_exec = cont_exec;
	self->cont_query = query;
	self->name_hash = MurmurHash3_64(relname, strlen(relname), MURMUR_SEED);

	pfree(relname);

	self->base.flush = &flush_to_combiner;
	self->base.buffer = buffer;

	return (BatchReceiver *) self;
}

/*
 * SetCombinerDestReceiverHashFunc
 *
 * Initializes the hash function to use to determine which combiner should read a given tuple
 */
void
SetCombinerDestReceiverHashFunc(BatchReceiver *receiver, FuncExpr *hash)
{
	CombinerReceiver *c = (CombinerReceiver *) receiver;
	FunctionCallInfo fcinfo = palloc0(sizeof(FunctionCallInfoData));

	fcinfo->flinfo = palloc0(sizeof(FmgrInfo));
	fcinfo->flinfo->fn_mcxt = ContQueryBatchContext;

	fmgr_info(hash->funcid, fcinfo->flinfo);
	fmgr_info_set_expr((Node *) hash, fcinfo->flinfo);

	fcinfo->fncollation = hash->funccollid;
	fcinfo->nargs = list_length(hash->args);

	c->hash_fcinfo = fcinfo;
	c->hashfn = hash;
}
