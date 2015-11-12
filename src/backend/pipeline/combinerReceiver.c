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
	ContExecutor *cont_executor;
	Bitmapset *queries;
	FunctionCallInfo hash_fcinfo;
	FuncExpr *hash;
	int64 query_hash;
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
	PartialTupleState pts;
	InsertBatchAck *acks = NULL;
	int nacks = 0;
	int len;

	if (synchronous_stream_insert)
	{
		List *acks_list = NIL;
		ListCell *lc;
		int i = 0;

		/* Generate acks list from yielded tuples */
		foreach(lc, c->cont_executor->yielded)
		{
			StreamTupleState *sts = lfirst(lc);
			InsertBatchAck *ack = sts->ack;
			ListCell *lc2;
			bool found = false;

			if (!ShmemDynAddrIsValid(ack->batch) || ack->batch_id != ack->batch->id)
				continue;

			foreach(lc2, acks_list)
			{
				InsertBatchAck *ack2 = lfirst(lc2);

				if (ack->batch_id == ack2->batch_id)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				InsertBatchAck *ack2 = (InsertBatchAck *) palloc(sizeof(InsertBatchAck));
				memcpy(ack2, ack, sizeof(InsertBatchAck));
				acks_list = lappend(acks_list, ack2);
				nacks++;
			}
		}

		acks = (InsertBatchAck *) palloc(sizeof(InsertBatchAck) * nacks);
		foreach(lc, acks_list)
		{
			InsertBatchAck *ack = lfirst(lc);

			InsertBatchIncrementNumCTuples(ack->batch);

			acks[i].batch_id = ack->batch_id;
			acks[i].batch = ack->batch;
			i++;
		}

		list_free_deep(acks_list);
	}

	pts.tup = ExecMaterializeSlot(slot);
	pts.query_id = c->cont_executor->current_query_id;
	pts.nacks = nacks;
	pts.acks = acks;

	len = (sizeof(PartialTupleState) +
			HEAPTUPLESIZE + pts.tup->t_len +
			(nacks * sizeof(InsertBatchAck)));

	/* Shard by groups or id if no grouping. */
	if (c->hash_fcinfo)
		pts.hash = hash_group(slot, c);
	else
		pts.hash = MurmurHash3_64(&c->cont_executor->current_query->view->name, sizeof(NameData), MURMUR_SEED);

	if (CombinerReceiveHook)
		CombinerReceiveHook(&pts, len);
	else
	{
		dsm_cqueue *cq = GetCombinerQueue(&pts);
		dsm_cqueue_push_nolock(cq, &pts, len);
		dsm_cqueue_unlock(cq);
	}

	IncrementCQWrite(1, len);

	if (nacks)
		pfree(acks);

	MemoryContextSwitchTo(old);
}

static void combiner_destroy(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	if (c->hash_fcinfo)
		pfree(c->hash_fcinfo);
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

	return (DestReceiver *) self;
}

/*
 * SetCombinerDestReceiverParams
 *
 * Set parameters for a CombinerDestReceiver
 */
void
SetCombinerDestReceiverParams(DestReceiver *self, ContExecutor *exec)
{
	CombinerState *c = (CombinerState *) self;
	c->cont_executor = exec;
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
	fcinfo->flinfo->fn_mcxt = c->cont_executor->exec_cxt;

	fmgr_info(hash->funcid, fcinfo->flinfo);
	fmgr_info_set_expr((Node *) hash, fcinfo->flinfo);

	fcinfo->fncollation = hash->funccollid;
	fcinfo->nargs = list_length(hash->args);

	c->hash_fcinfo = fcinfo;
	c->hash = hash;
}
