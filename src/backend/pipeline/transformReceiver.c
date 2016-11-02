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
#include "pipeline/cqmatrel.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/stream_fdw.h"
#include "miscadmin.h"
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
	bool os_has_readers;
	FunctionCallInfo trig_fcinfo;

	/* only used by the optimized code path for pipeline_stream_insert */
	HeapTuple *tups;
	int nmaxtups;
	int ntups;
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
	{
		Bitmapset *readers = GetAllStreamReaders(t->cont_query->osrelid);

		t->os_has_readers = !bms_is_empty(readers);
		t->tg_rel = heap_open(t->cont_query->matrelid, AccessShareLock);
	}

	if (OidIsValid(t->cont_query->tgfn) &&
			t->cont_query->tgfn != PIPELINE_STREAM_INSERT_OID)
	{
		TriggerData *cxt = (TriggerData *) t->trig_fcinfo->context;

		Assert(t->trig_fcinfo);

		cxt->tg_relation = t->tg_rel;
		cxt->tg_trigtuple = ExecCopySlotTuple(slot);

		FunctionCallInvoke(t->trig_fcinfo);

		heap_freetuple(cxt->tg_trigtuple);
		cxt->tg_trigtuple = NULL;
	}

	if (t->cont_query->tgfn == PIPELINE_STREAM_INSERT_OID || t->os_has_readers)
	{
		if (t->tups == NULL)
		{
			Assert(t->nmaxtups == 0);
			Assert(t->ntups == 0);

			t->nmaxtups = continuous_query_batch_size / 8;
			t->tups = palloc(sizeof(HeapTuple) * t->nmaxtups);
		}

		if (t->ntups == t->nmaxtups)
		{
			t->nmaxtups *= 2;
			t->tups = repalloc(t->tups, sizeof(HeapTuple) * t->nmaxtups);
		}

		t->tups[t->ntups++] = ExecCopySlotTuple(slot);
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

	if (OidIsValid(query->tgfn) && query->tgfn != PIPELINE_STREAM_INSERT_OID)
	{
		FunctionCallInfo fcinfo = palloc0(sizeof(FunctionCallInfoData));
		FmgrInfo *finfo = palloc0(sizeof(FmgrInfo));
		TriggerData *cxt = palloc0(sizeof(TriggerData));
		Trigger *trig = palloc0(sizeof(Trigger));

		finfo->fn_mcxt = ContQueryBatchContext;
		fmgr_info(query->tgfn, finfo);

		/* Create mock TriggerData and Trigger */
		trig->tgname = query->name->relname;
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

static void
insert_into_rel(TransformState *t, Relation rel)
{
	ResultRelInfo *rinfo = CQOSRelOpen(rel);
	StreamInsertState *sis;

	BeginStreamModify(NULL, rinfo, list_make2(t->cont_exec->batch->sync_acks, RelationGetDescr(t->tg_rel)),
			0, REENTRANT_STREAM_INSERT);
	sis = (StreamInsertState *) rinfo->ri_FdwState;
	Assert(sis);

	if (sis->queries)
	{
		TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
		int j;

		for (j = 0; j < t->ntups; j++)
		{
			ExecStoreTuple(t->tups[j], slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, rinfo, slot, NULL);
			ExecClearTuple(slot);
		}

		ExecDropSingleTupleTableSlot(slot);
		pgstat_increment_cq_write(t->ntups, sis->nbytes);
	}

	EndStreamModify(NULL, rinfo);
	CQOSRelClose(rinfo);
	pgstat_report_streamstat(false);
}

static void
pipeline_stream_insert_batch(TransformState *t)
{
	int i;

	if (t->ntups == 0)
		return;

	Assert(t->tg_rel);

	for (i = 0; i < t->cont_query->tgnargs; i++)
	{
		RangeVar *rv = makeRangeVarFromNameList(stringToQualifiedNameList(t->cont_query->tgargs[i]));
		Relation rel = heap_openrv(rv, AccessShareLock);

		insert_into_rel(t, rel);

		heap_close(rel, NoLock);
	}

	if (t->os_has_readers)
	{
		Relation rel = heap_open(t->cont_query->osrelid, AccessShareLock);

		insert_into_rel(t, rel);

		heap_close(rel, NoLock);
	}

	if (t->ntups)
	{
		for (i = 0; i < t->ntups; i++)
			heap_freetuple(t->tups[i]);

		pfree(t->tups);
		t->tups = NULL;
		t->ntups = 0;
		t->nmaxtups = 0;
	}
	else
	{
		Assert(t->tups == NULL);
		Assert(t->nmaxtups == 0);
	}
}

void
TransformDestReceiverFlush(DestReceiver *self)
{
	TransformState *t = (TransformState *) self;

	/* Optimized path for stream insertions */
	if (t->cont_query->tgfn == PIPELINE_STREAM_INSERT_OID || t->os_has_readers)
		pipeline_stream_insert_batch(t);

	if (OidIsValid(t->cont_query->tgfn) && t->cont_query->tgfn != PIPELINE_STREAM_INSERT_OID)
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
