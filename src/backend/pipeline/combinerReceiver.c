/*-------------------------------------------------------------------------
 *
 * combinerReceiver.c
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/combinerReceiver.c
 *
 */
#include "postgres.h"
#include "pgstat.h"

#include "catalog/pg_type.h"
#include "access/htup_details.h"
#include "access/printtup.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cont_execute.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "miscadmin.h"
#include "utils/hashfuncs.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define MURMUR_SEED 0x155517D2

typedef struct
{
	DestReceiver pub;
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	FunctionCallInfo hash_fcinfo;
	FuncExpr *hashfn;

	uint64 name_hash;
	List **tups_per_combiner;
} CombinerState;

static void
combiner_shutdown(DestReceiver *self)
{

}

static void
combiner_startup(DestReceiver *self, int operation,
		TupleDesc typeinfo)
{

}

static void
combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	MemoryContext old = MemoryContextSwitchTo(ContQueryBatchContext);
	int i;
	tagged_ref_t *ref;


	if (!c->cont_query)
		c->cont_query = c->cont_exec->curr_query->query;

	Assert(c->cont_query->type == CONT_VIEW);

	ref = palloc(sizeof(tagged_ref_t));
	ref->ptr = ExecCopySlotTuple(slot);

	/* Shard by groups or name if no grouping. */
	if (c->hash_fcinfo)
	{
		uint64 hash;

		ref->tag = slot_hash_group(slot, c->hashfn, c->hash_fcinfo);

		hash = slot_hash_group_skip_attr(slot, c->cont_query->sw_attno, c->hashfn, c->hash_fcinfo);
		i = get_combiner_for_group_hash(hash);
	}
	else
	{
		ref->tag = c->name_hash;
		i = get_combiner_for_group_hash(c->name_hash);
	}

	c->tups_per_combiner[i] = lappend(c->tups_per_combiner[i], ref);

	MemoryContextSwitchTo(old);
}

static void
combiner_destroy(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	if (c->hash_fcinfo)
		pfree(c->hash_fcinfo);
	pfree(c->tups_per_combiner);
	pfree(c);
}

DestReceiver *
CreateCombinerDestReceiver(void)
{
	CombinerState *self = (CombinerState *) palloc0(sizeof(CombinerState));

	self->pub.receiveSlot = combiner_receive;
	self->pub.rStartup = combiner_startup;
	self->pub.rShutdown = combiner_shutdown;
	self->pub.rDestroy = combiner_destroy;
	self->pub.mydest = DestCombiner;

	self->tups_per_combiner = palloc0(sizeof(List *) * continuous_query_num_combiners);

	return (DestReceiver *) self;
}

/*
 * SetCombinerDestReceiverParams
 *
 * Set parameters for a CombinerDestReceiver
 */
void
SetCombinerDestReceiverParams(DestReceiver *self, ContExecutor *exec, ContQuery *query)
{
	CombinerState *c = (CombinerState *) self;
	char *relname = get_rel_name(query->relid);

	c->cont_exec = exec;
	c->cont_query = query;
	c->name_hash = MurmurHash3_64(relname, strlen(relname), MURMUR_SEED);

	pfree(relname);
}

/*
 * SetCombinerDestReceiverHashFunc
 *
 * Initializes the hash function to use to determine which combiner should read a given tuple
 */
void
SetCombinerDestReceiverHashFunc(DestReceiver *self, FuncExpr *hash)
{
	CombinerState *c = (CombinerState *) self;
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

static void
microbatch_send_to_combiner(microbatch_t *mb, int combiner_id)
{
	static ContQueryDatabaseMetadata *db_meta = NULL;
	int recv_id;

	if (!db_meta)
		db_meta = GetContQueryDatabaseMetadata(MyDatabaseId);

	recv_id = db_meta->db_procs[continuous_query_num_workers + combiner_id].pzmq_id;

	microbatch_send(mb, recv_id);
	microbatch_reset(mb);
}

void
CombinerDestReceiverFlush(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	int i;
	int ntups = 0;
	Size size = 0;
	microbatch_t *mb;

	mb = microbatch_new(CombinerTuple, bms_make_singleton(c->cont_query->id), NULL);
	microbatch_add_acks(mb, c->cont_exec->batch->acks);

	for (i = 0; i < continuous_query_num_combiners; i++)
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
	pgstat_increment_cq_write(ntups, size);
}
