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
#include "storage/shm_alloc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define MURMUR_SEED 0x155517D2

CombinerReceiveFunc CombinerReceiveHook = NULL;

typedef struct
{
	DestReceiver pub;
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	FunctionCallInfo hash_fcinfo;
	FuncExpr *hash;
	int64 query_hash;
	List **partials;
	int ntups;
	int nacks;
	InsertBatchAck *acks;
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

static uint64
hash_group(TupleTableSlot *slot, CombinerState *state)
{
	ListCell *lc;
	Datum result;
	int i = 0;

	foreach(lc, state->hash->args)
	{
		AttrNumber attno = ((Var *) lfirst(lc))->varattno;
		bool isnull;
		Datum d;

		d = slot_getattr(slot, attno, &isnull);
		state->hash_fcinfo->arg[i] = d;
		state->hash_fcinfo->argnull[i] = isnull;
		i++;
	}

	result = FunctionCallInvoke(state->hash_fcinfo);

	return DatumGetInt64(result);
}

static void
combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	MemoryContext old = MemoryContextSwitchTo(ContQueryBatchContext);
	PartialTupleState *pts = palloc0(sizeof(PartialTupleState));
	InsertBatchAck *acks = NULL;
	int nacks = 0;
	int idx;

	if (c->cont_query == NULL)
		c->cont_query = c->cont_exec->current_query->query;

	Assert(c->cont_query->type == CONT_VIEW);

	if (synchronous_stream_insert)
	{
		if (c->acks == NULL)
			c->acks = InsertBatchAckCreate(c->cont_exec->yielded_msgs, &c->nacks);

		c->ntups++;
		acks = c->acks;
		nacks = c->nacks;
	}

	pts->tup = ExecCopySlotTuple(slot);
	pts->query_id = c->cont_query->id;
	pts->nacks = nacks;
	pts->acks = acks;

	/* Shard by groups or id if no grouping. */
	if (c->hash_fcinfo)
		pts->hash = hash_group(slot, c);
	else
		pts->hash = MurmurHash3_64(&c->cont_query->name, sizeof(NameData), MURMUR_SEED);

	idx = pts->hash % continuous_query_num_combiners;
	c->partials[idx] = lappend(c->partials[idx], pts);

	MemoryContextSwitchTo(old);
}

static void
combiner_destroy(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	if (c->hash_fcinfo)
		pfree(c->hash_fcinfo);
	pfree(c->partials);
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

	self->partials = palloc0(sizeof(List *) * continuous_query_num_combiners);

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
	c->cont_exec = exec;
	c->cont_query = query;
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
	fcinfo->flinfo->fn_mcxt = c->cont_exec->exec_cxt;

	fmgr_info(hash->funcid, fcinfo->flinfo);
	fmgr_info_set_expr((Node *) hash, fcinfo->flinfo);

	fcinfo->fncollation = hash->funccollid;
	fcinfo->nargs = list_length(hash->args);

	c->hash_fcinfo = fcinfo;
	c->hash = hash;
}

void
CombinerDestReceiverFlush(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	int i;

	if (CombinerReceiveHook)
	{
		for (i = 0; i < continuous_query_num_combiners; i++)
		{
			List *partials = c->partials[i];
			ListCell *lc;

			if (partials == NIL)
				continue;

			foreach(lc, partials)
			{
				PartialTupleState *pts = (PartialTupleState *) lfirst(lc);
				int len = (sizeof(PartialTupleState) +
						HEAPTUPLESIZE + pts->tup->t_len +
						(pts->nacks * sizeof(InsertBatchAck)));

				CombinerReceiveHook(pts, len);
			}

			list_free_deep(partials);
			c->partials[i] = NIL;
		}
	}
	else
	{
		int ninserted = 0;
		Size size = 0;

		for (i = 0; i < continuous_query_num_combiners; i++)
		{
			List *partials = c->partials[i];
			ListCell *lc;
			ipc_queue *ipcq = NULL;

			if (partials == NIL)
				continue;

			foreach(lc, partials)
			{
				PartialTupleState *pts = (PartialTupleState *) lfirst(lc);
				int len = (sizeof(PartialTupleState) +
						HEAPTUPLESIZE + pts->tup->t_len +
						(pts->nacks * sizeof(InsertBatchAck)));

				if (ipcq == NULL)
					ipcq = get_combiner_queue_with_lock(pts->hash % continuous_query_num_combiners);

				Assert(ipcq);
				ipc_queue_push_nolock(ipcq, pts, len, true);

				size += len;
				ninserted++;
			}

			Assert(ipcq);
			ipc_queue_unlock(ipcq);

			list_free_deep(partials);
			c->partials[i] = NIL;
		}

		pgstat_increment_cq_write(ninserted, size);
	}

	if (c->acks)
	{
		int i;

		for (i = 0; i < c->nacks; i++)
		{
			InsertBatchAck *ack = &c->acks[i];
			InsertBatchIncrementNumCTuples(ack->batch, c->ntups);
		}

		pfree(c->acks);
		c->acks = NULL;
		c->nacks = 0;
		c->ntups = 0;
	}
}
