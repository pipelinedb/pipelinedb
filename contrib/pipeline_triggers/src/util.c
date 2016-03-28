/*-------------------------------------------------------------------------
 *
 * util.c
 *	  Functionality for util
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "util.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "utils/memutils.h"
#include "pipeline/cont_plan.h"

/* these trigger_plugin_xxx funcs are boilerplate that need to exist
 * for the WAL decoder plugin */

void
trigger_plugin_decode_startup(LogicalDecodingContext *ctx,
					OutputPluginOptions *opt,
				  	bool is_init)
{
}

void
trigger_plugin_decode_shutdown(LogicalDecodingContext *ctx)
{
}

void
trigger_prepare_write(LogicalDecodingContext *ctx,
				   XLogRecPtr lsn,
				   TransactionId xid, bool last_write)
{
}

void
trigger_write_data(LogicalDecodingContext *ctx,
				XLogRecPtr lsn,
				TransactionId xid,
				bool last_write)
{
}

/*
 * map_rb_change_to_trig
 */
TriggerProcessChangeType
map_rb_change_to_trig(enum ReorderBufferChangeType in)
{
	enum TriggerProcessChangeType out;

	switch (in)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			out = TRIGGER_PROCESS_CHANGE_INSERT;
			break;

		case REORDER_BUFFER_CHANGE_UPDATE:
			out = TRIGGER_PROCESS_CHANGE_UPDATE;
			break;

		case REORDER_BUFFER_CHANGE_DELETE:
			out = TRIGGER_PROCESS_CHANGE_DELETE;
			break;

		default:
			out = TRIGGER_PROCESS_CHANGE_NOOP;
			break;
	}

	return out;
}

/*
 * map_trig_change_to_event
 */
int
map_trig_change_to_event(enum TriggerProcessChangeType in)
{
	int out;

	switch (in)
	{
		case TRIGGER_PROCESS_CHANGE_INSERT:
			out = TRIGGER_EVENT_INSERT;
			break;

		case TRIGGER_PROCESS_CHANGE_UPDATE:
			out = TRIGGER_EVENT_UPDATE;
			break;

		case TRIGGER_PROCESS_CHANGE_DELETE:
			out = TRIGGER_EVENT_DELETE;
			break;

		default:
			elog(ERROR, "invalid trigger process chg type %d", in);
			break;
	}

	return out;
}

/*
 * CopyTrigger
 *
 * Copy a Trigger into a newly palloc'd structure and return it.
 */
Trigger*
CopyTrigger(Trigger *src)
{
	Trigger *trigger = palloc0(sizeof(Trigger));
	memcpy(trigger, src, sizeof(Trigger));

	trigger->tgname = pstrdup(trigger->tgname);

	if (trigger->tgnattr > 0)
	{
		int16	   *newattr;

		newattr = (int16 *) palloc(trigger->tgnattr * sizeof(int16));
		memcpy(newattr, trigger->tgattr,
				trigger->tgnattr * sizeof(int16));
		trigger->tgattr = newattr;
	}

	if (trigger->tgnargs > 0)
	{
		char	  **newargs;
		int16		j;

		newargs = (char **) palloc(trigger->tgnargs * sizeof(char *));
		for (j = 0; j < trigger->tgnargs; j++)
			newargs[j] = pstrdup(trigger->tgargs[j]);
		trigger->tgargs = newargs;
	}

	if (trigger->tgqual)
		trigger->tgqual = pstrdup(trigger->tgqual);

	return trigger;
}

/*
 * build_projection
 */
ProjectionInfo *
build_projection(List *tlist, EState *estate, ExprContext *econtext,
		TupleDesc input_desc)
{
	TupleTableSlot *result_slot;
	TupleDesc result_desc;
	List *targetlist;

	result_desc = ExecTypeFromTL(tlist, false);
	result_slot = MakeSingleTupleTableSlot(result_desc);
	targetlist = (List *) ExecPrepareExpr((Expr *) tlist, estate);

	return ExecBuildProjectionInfo(targetlist, econtext,
								   result_slot, input_desc);
}

/*
 * create_agg_hash
 */
TupleHashTable
create_agg_hash(const char *name, int num_cols, Oid *grp_ops,
		AttrNumber *group_atts, size_t entrysize)
{
	FmgrInfo *eq_funcs;
	FmgrInfo *hash_funcs;
	MemoryContext hash_tmp_cxt;

	execTuplesHashPrepare(num_cols, grp_ops,
						  &eq_funcs, &hash_funcs);

	/* this tmp cxt is required by hash table, it will reset it internally */

	hash_tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
			name,
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	return BuildTupleHashTable(num_cols, group_atts,
							   eq_funcs, hash_funcs, 1000,
							   entrysize,
							   CurrentMemoryContext, hash_tmp_cxt);
}

/*
 * PreparePlanForReading
 *
 * Prepare a plan for reading from a tuplestore
 */
TupleDesc
PreparePlanForReading(RangeVar *matrel, PlannedStmt *plan,
					  Tuplestorestate *batch)
{
	TuplestoreScan *scan;

	Relation rel = heap_openrv(matrel, NoLock);

	plan->isContinuous = false;

	scan = SetCombinerPlanTuplestorestate(plan, batch);
	scan->desc = CreateTupleDescCopy(RelationGetDescr(rel));

	heap_close(rel, NoLock);

	return scan->desc;
}
