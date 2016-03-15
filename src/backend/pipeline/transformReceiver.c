/*-------------------------------------------------------------------------
 *
 * transformReceiver.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/transformReceiver.c
 *
 */
#include "postgres.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/printtup.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "pipeline/transformReceiver.h"
#include "pipeline/cont_execute.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "miscadmin.h"
#include "storage/shm_alloc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct TransformState
{
	DestReceiver pub;
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	Relation tg_rel;
	FunctionCallInfo trig_fcinfo;

	/* only used by the optimized code path for pipeline_stream_insert */
	List *tups;
	int nacks;
	InsertBatchAck *acks;
} TransformState;

static void
transform_shutdown(DestReceiver *self)
{

}

static void
transform_startup(DestReceiver *self, int operation,
		TupleDesc typeinfo)
{

}

static void
transform_receive(TupleTableSlot *slot, DestReceiver *self)
{
	TransformState *t = (TransformState *) self;
	MemoryContext old = MemoryContextSwitchTo(ContQueryBatchContext);

	if (t->tg_rel == NULL)
		t->tg_rel = heap_open(t->cont_query->matrelid, AccessShareLock);

	if (t->cont_query->tgfn != PIPELINE_STREAM_INSERT_OID)
	{
		TriggerData *cxt = (TriggerData *) t->trig_fcinfo->context;

		Assert(t->trig_fcinfo);

		cxt->tg_relation = t->tg_rel;
		cxt->tg_trigtuple = ExecCopySlotTuple(slot);

		FunctionCallInvoke(t->trig_fcinfo);

		heap_freetuple(cxt->tg_trigtuple);
		cxt->tg_newtuple = NULL;
	}
	else
	{
		t->tups = lappend(t->tups, ExecCopySlotTuple(slot));

		if (t->acks == NULL)
			t->acks = InsertBatchAckCreate(t->cont_exec->yielded, &t->nacks);
	}

	MemoryContextSwitchTo(old);
}

static void
transform_destroy(DestReceiver *self)
{
	TransformState *t = (TransformState *) self;
	pfree(t);
}

DestReceiver *
CreateTransformDestReceiver(void)
{
	TransformState *self = (TransformState *) palloc0(sizeof(TransformState));

	self->pub.receiveSlot = transform_receive;
	self->pub.rStartup = transform_startup;
	self->pub.rShutdown = transform_shutdown;
	self->pub.rDestroy = transform_destroy;
	self->pub.mydest = DestTransform;

	return (DestReceiver *) self;
}

/*
 * SetTransformDestReceiverParams
 *
 * Set parameters for a TransformDestReceiver
 */
void
SetTransformDestReceiverParams(DestReceiver *self, ContExecutor *exec, ContQuery *query)
{
	TransformState *t = (TransformState *) self;

	t->cont_exec = exec;

	Assert(query->type == CONT_TRANSFORM);
	t->cont_query = query;

	Assert(OidIsValid(query->tgfn));

	if (query->tgfn != PIPELINE_STREAM_INSERT_OID)
	{
		FunctionCallInfo fcinfo = palloc0(sizeof(FunctionCallInfoData));
		FmgrInfo *finfo = palloc0(sizeof(FmgrInfo));
		TriggerData *cxt = palloc0(sizeof(TriggerData));
		Trigger *trig = palloc0(sizeof(Trigger));

		finfo->fn_mcxt = exec->exec_cxt;
		fmgr_info(query->tgfn, finfo);

		/* Create mock TriggerData and Trigger */
		trig->tgname = NameStr(query->name);
		trig->tgenabled = TRIGGER_FIRES_ALWAYS;
		trig->tgfoid = query->tgfn;
		trig->tgnargs = query->tgnargs;
		trig->tgargs = query->tgargs;
		TRIGGER_SETT_ROW(trig->tgtype);
		TRIGGER_SETT_AFTER(trig->tgtype);
		TRIGGER_SETT_INSERT(trig->tgtype);

		cxt->type = T_TriggerData;
		cxt->tg_event = TRIGGER_EVENT_ROW;
		cxt->tg_newtuplebuf = InvalidBuffer;
		cxt->tg_trigtuplebuf = InvalidBuffer;
		cxt->tg_trigger = trig;

		fcinfo->flinfo = finfo;
		fcinfo->context = (fmNodePtr) cxt;

		t->trig_fcinfo = fcinfo;
	}
}

void
TransformDestReceiverFlush(DestReceiver *self)
{
	TransformState *t = (TransformState *) self;
	bool wait = continuous_query_num_workers > 1;

	/* Optimized path for stream insertions */
	if (t->cont_query->tgfn == PIPELINE_STREAM_INSERT_OID)
	{
		int i;
		ListCell *lc;

		if (list_length(t->tups) == 0)
			return;

		Assert(t->tg_rel);

		for (i = 0; i < t->cont_query->tgnargs; i++)
		{
			RangeVar *rv = makeRangeVarFromNameList(stringToQualifiedNameList(t->cont_query->tgargs[i]));
			Relation rel = heap_openrv(rv, AccessShareLock);
			Oid relid = RelationGetRelid(rel);
			Bitmapset *targets;
			bytea *packed_desc;
			Size size = 0;
			dsm_cqueue *cq;
			int batch = 0;
			int nbatches = 1;

			targets = bms_difference(GetStreamReaders(relid), GetAdhocContinuousViewIds());

			if (bms_num_members(targets) == 0)
				continue;

			if (t->acks)
			{
				int i;

				for (i = 0; i < t->nacks; i++)
				{
					InsertBatchAck *ack = &t->acks[i];
					InsertBatchIncrementNumWTuples(ack->batch, list_length(t->tups));
				}
			}

			packed_desc = PackTupleDesc(RelationGetDescr(t->tg_rel));
			cq = GetWorkerQueue();

			foreach(lc, t->tups)
			{
				HeapTuple tup = (HeapTuple) lfirst(lc);
				int len;
				StreamTupleState *sts = StreamTupleStateCreate(tup, RelationGetDescr(t->tg_rel), packed_desc, targets,
						t->acks, t->nacks, &len);

				if (batch == continuous_query_batch_size)
				{
					dsm_cqueue_unlock(cq);
					cq = GetWorkerQueue();
					batch = 0;
					nbatches++;
				}

				if (dsm_cqueue_push_nolock(cq, sts, len, wait))
				{
					batch++;
					size += len;
				}
				else
					StreamTupleStateBuffer(sts, len);
			}

			pfree(packed_desc);
			dsm_cqueue_unlock(cq);
			heap_close(rel, NoLock);

			stream_stat_increment(relid, list_length(t->tups), nbatches, size);
		}

		if (t->acks)
		{
			pfree(t->acks);
			t->acks = NULL;
			t->nacks = 0;
		}

		foreach(lc, t->tups)
		{
			HeapTuple tup = (HeapTuple) lfirst(lc);
			heap_freetuple(tup);
		}

		list_free(t->tups);
		t->tups = NIL;
	}
	else
	{
		TriggerData *cxt = (TriggerData *) t->trig_fcinfo->context;
		cxt->tg_relation = NULL;
	}

	if (t->tg_rel)
	{
		heap_close(t->tg_rel, AccessShareLock);
		t->tg_rel = NULL;
	}
}
