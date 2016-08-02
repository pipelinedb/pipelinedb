/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_target.h"
#include "pgstat.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/stream.h"
#include "pipeline/stream_fdw.h"
#include "storage/shm_alloc.h"
#include "storage/ipc.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets;

int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread) = NULL;
void *copy_iter_arg = NULL;

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
void
CopyIntoStream(Relation rel, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	bool snap = ActiveSnapshotSet();
	ResultRelInfo rinfo;
	StreamInsertState *sis;

	MemSet(&rinfo, 0, sizeof(ResultRelInfo));
	rinfo.ri_RangeTableIndex = 1; /* dummy */
	rinfo.ri_TrigDesc = NULL;
	rinfo.ri_RelationDesc = rel;

	if (snap)
		PopActiveSnapshot();

	BeginStreamModify(NULL, &rinfo, list_make1(desc), 0, 0);
	sis = (StreamInsertState *) rinfo.ri_FdwState;
	Assert(sis);

	if (sis->queries)
	{
		TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
		int i;

		for (i = 0; i < ntuples; i++)
		{
			ExecStoreTuple(tuples[i], slot, InvalidBuffer, false);
			ExecStreamInsert(NULL, &rinfo, slot, NULL);
			ExecClearTuple(slot);
		}

		ExecDropSingleTupleTableSlot(slot);
		pgstat_increment_cq_write(ntuples, sis->nbytes);
	}

	EndStreamModify(NULL, &rinfo);

	if (snap)
		PushActiveSnapshot(GetTransactionSnapshot());
}

Datum
pipeline_stream_insert(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger *trig = trigdata->tg_trigger;
	HeapTuple tup;
	TupleDesc desc;
	int i;

	if (trig->tgnargs < 1)
		elog(ERROR, "pipeline_stream_insert: must be provided a stream name");

	/* make sure it's called as a trigger */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called as trigger")));

	/* and that it's called on update or insert */
	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && !TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called on insert or update")));

	/* and that it's called for each row */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called for each row")));

	/* and that it's called after insert or update */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("pipeline_stream_insert: must be called after insert or update")));

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		tup = trigdata->tg_newtuple;
	else
		tup = trigdata->tg_trigtuple;

	desc = RelationGetDescr(trigdata->tg_relation);

	for (i = 0; i < trig->tgnargs; i++)
	{
		RangeVar *stream;
		Relation rel;
		HeapTuple tups[1];

		stream = makeRangeVarFromNameList(textToQualifiedNameList(cstring_to_text(trig->tgargs[i])));
		rel = heap_openrv(stream, AccessShareLock);

		tups[0] = tup;
//		SendTuplesToContWorkers(rel, desc, tups, 1, NIL);

		heap_close(rel, AccessShareLock);
	}

	return PointerGetDatum(tup);
}
