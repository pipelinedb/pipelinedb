/*-------------------------------------------------------------------------
 *
 * transform_receiver.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/printtup.h"
#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "pipeline_stream.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "transform_receiver.h"
#include "matrel.h"
#include "miscutils.h"
#include "stream_fdw.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

TransformFlushFunc TransformFlushHook = NULL;

/*
 * transform_receive
 */
static void
transform_receive(TransformReceiver *t, TupleTableSlot *slot)
{
	MemoryContext old = MemoryContextSwitchTo(ContQueryBatchContext);
	Oid insertoid = GetInsertIntoStreamOid();

	if (t->tg_rel == NULL)
	{
		Bitmapset *readers = GetAllStreamReaders(t->cont_query->osrelid);

		t->os_has_readers = !bms_is_empty(readers);
		t->tg_rel = heap_open(t->cont_query->matrelid, AccessShareLock);
	}

	if (OidIsValid(t->cont_query->tgfn) &&
			t->cont_query->tgfn != insertoid)
	{
		TriggerData *cxt = (TriggerData *) t->trig_fcinfo->context;
		StreamInsertLevel save_stream_insert_level = stream_insert_level;

		Assert(t->trig_fcinfo);

		cxt->tg_relation = t->tg_rel;
		cxt->tg_trigtuple = ExecCopySlotTuple(slot);

		/*
		 * If it's a trigger function that inserts into another stream, we need to make
		 * sure that the insert is fully asynchronous. Otherwise, we could end up stalled
		 * if this worker were to wait for an ack that we ourselves are responsible for producing.
		 */
		stream_insert_level = STREAM_INSERT_ASYNCHRONOUS;
		FunctionCallInvoke(t->trig_fcinfo);
		stream_insert_level = save_stream_insert_level;

		heap_freetuple(cxt->tg_trigtuple);
		cxt->tg_trigtuple = NULL;
	}

	if (t->cont_query->tgfn == insertoid || t->os_has_readers)
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

/*
 * align_tuple
 */
static HeapTuple
align_tuple(TransformReceiver *t, HeapTuple tup, TupleTableSlot *slot, TupleDesc osrel)
{
	int i;
	int j;
	TupleDesc event = slot->tts_tupleDescriptor;
	Datum values[osrel->natts];
	bool nulls[osrel->natts];

	/*
	 * We need to write out rows using the correct ordering of attributes, which is just
	 * the output stream's descriptor.
	 *
	 * In most cases they'll already be in the correct order, but in some cases we'll need
	 * to realign them, such as when a column is referenced in a transform's WHERE clause
	 * but not its target list.
	 */
	if (t->needs_alignment && t->osrel_attrs == NULL)
	{
		/*
		 * We only need to determine whether or not alignment is needed a single time,
		 * since each query state has its own TransformReceiver.
		 */
		MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);

		t->needs_alignment = false;
		t->osrel_attrs = (AttrNumber *) palloc0(sizeof(AttrNumber) * osrel->natts);

		for (i = 0; i < osrel->natts; i++)
		{
			Form_pg_attribute osrel_attr = TupleDescAttr(osrel, i);
			for (j = 0; j < event->natts; j++)
			{
				Form_pg_attribute event_attr = TupleDescAttr(event, j);
				if (pg_strcasecmp(NameStr(osrel_attr->attname), NameStr(event_attr->attname)) == 0)
				{
					t->osrel_attrs[osrel_attr->attnum - 1] = event_attr->attnum;
					if (i != j)
						t->needs_alignment = true;
				}
			}
		}

		MemoryContextSwitchTo(old);
	}

	/*
	 * The below realignment will be a noop in this case, so just bail
	 */
	if (!t->needs_alignment)
		return tup;

	MemSet(nulls, false, sizeof(nulls));
	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	for (i = 0; i < osrel->natts; i++)
	{
		values[i] = slot_getattr(slot, t->osrel_attrs[i], &nulls[i]);
	}


	return heap_form_tuple(osrel, values, nulls);
}

/*
 * insert_into_rel
 */
static void
insert_into_rel(TransformReceiver *t, Relation rel, TupleTableSlot *event_slot)
{
	ResultRelInfo *rinfo = CQOSRelOpen(rel);
	StreamInsertState *sis;

	BeginStreamModify(NULL, rinfo, list_make2(t->cont_exec->batch->sync_acks, RelationGetDescr(t->tg_rel)),
			0, REENTRANT_STREAM_INSERT);
	sis = (StreamInsertState *) rinfo->ri_FdwState;
	Assert(sis);

	if (sis->queries)
	{
		TupleDesc osreldesc = RelationGetDescr(rel);
		TupleTableSlot *slot = MakeSingleTupleTableSlot(osreldesc);
		int j;

		for (j = 0; j < t->ntups; j++)
		{
			HeapTuple tup = align_tuple(t, t->tups[j], event_slot, osreldesc);
			ExecStoreTuple(tup, slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, rinfo, slot, NULL);
			ExecClearTuple(slot);
		}

		ExecDropSingleTupleTableSlot(slot);
	}

	EndStreamModify(NULL, rinfo);
	CQOSRelClose(rinfo);
}

/*
 * pipeline_stream_insert_batch
 */
static void
pipeline_stream_insert_batch(TransformReceiver *t, TupleTableSlot *slot)
{
	int i;

	if (t->ntups == 0)
		return;

	Assert(t->tg_rel);

	for (i = 0; i < t->cont_query->tgnargs; i++)
	{
		RangeVar *rv = makeRangeVarFromNameList(stringToQualifiedNameList(t->cont_query->tgargs[i]));
		Relation rel = heap_openrv(rv, AccessShareLock);

		insert_into_rel(t, rel, slot);

		heap_close(rel, NoLock);
	}

	if (t->os_has_readers)
	{
		Relation rel = heap_open(t->cont_query->osrelid, AccessShareLock);

		insert_into_rel(t, rel, slot);

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

/*
 * flush_to_transform
 */
static void
flush_to_transform(struct BatchReceiver *receiver, TupleTableSlot *slot)
{
	TransformReceiver *t = (TransformReceiver *) receiver;
	int save_batch_size = continuous_query_batch_size;
	int save_batch_mem = continuous_query_batch_mem;
	Oid insertoid = GetInsertIntoStreamOid();

	foreach_tuple(slot, t->base.buffer)
	{
		transform_receive(t, slot);
	}

	if (TransformFlushHook)
		TransformFlushHook();

	continuous_query_batch_size = continuous_query_batch_mem = INT_MAX / 1024;

	/* Optimized path for stream insertions */
	if (t->cont_query->tgfn == insertoid || t->os_has_readers)
		pipeline_stream_insert_batch(t, slot);

	if (OidIsValid(t->cont_query->tgfn) && t->cont_query->tgfn != insertoid)
	{
		TriggerData *cxt = (TriggerData *) t->trig_fcinfo->context;
		cxt->tg_relation = NULL;
	}

	if (t->tg_rel)
	{
		heap_close(t->tg_rel, AccessShareLock);
		t->tg_rel = NULL;
	}

	continuous_query_batch_size = save_batch_size;
	continuous_query_batch_mem = save_batch_mem;
}

/*
 * CreateTransformReceiver
 */
BatchReceiver *
CreateTransformReceiver(ContExecutor *exec, ContQuery *query, Tuplestorestate *buffer)
{
	TransformReceiver *t = (TransformReceiver *) palloc0(sizeof(TransformReceiver));

	t->cont_exec = exec;

	Assert(query->type == CONT_TRANSFORM);

	t->cont_query = query;
	t->base.buffer = buffer;
	t->base.flush = &flush_to_transform;
	t->needs_alignment = true;
	t->osrel_attrs = NULL;

	if (OidIsValid(query->tgfn) && query->tgfn != GetInsertIntoStreamOid())
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

	return (BatchReceiver *) t;
}
