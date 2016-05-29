/*-------------------------------------------------------------------------
 *
 * sliding_window.c
 *	  Functionality for sliding window
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "parser/parse_oper.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "pipeline/stream.h"
#include "pipeline/trigger/batching.h"
#include "pipeline/trigger/sliding_window.h"
#include "pipeline/trigger/tuple_formatter.h"
#include "pipeline/trigger/util.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define TRIGGER_SW_VACUUM_INTERVAL 1 * 1000 /* 1s */

static TriggerCacheEntry *find_min_timestamp(TriggerProcessState *state);
static TimestampTz first_timestamp(TriggerCacheEntry *entry);
static TimestampTz calc_expiry_time(TimestampTz tz, int msec);

/*
 * get_tup_expiry_time
 */
static TimestampTz
get_tup_expiry_time(TriggerCacheEntry *entry, TupleTableSlot *slot)
{
	bool isnull;
	Datum d = slot_getattr(slot, entry->sw_arrival_ts_attr, &isnull);
	TimestampTz tup_arr_ts = DatumGetTimestampTz(d);
	return calc_expiry_time(tup_arr_ts, entry->sw_interval_ms);
}

static HTAB*
create_timestamp_hash(const char *name, MemoryContext hcxt);

/*
 * get_recalc_tups
 *
 * Get input tuples required to recalculate a sw group result
 */
static void
get_recalc_tups(ResultEntry *res_entry,
				Tuplestorestate *input)
{
	dlist_iter iter;

	dlist_foreach(iter, &res_entry->list_head)
	{
		MatrelEntry *mrel_entry =
			dlist_container(MatrelEntry, list_node, iter.cur);
		tuplestore_puttuple(input, mrel_entry->tuple);
	}
}

/*
 * run_plan
 */
static void
run_plan(TriggerCacheEntry *tc_entry)
{
	struct Plan *plan;
	TriggerViewExec *exec = tc_entry->sw_view_exec;
	tuplestore_clear(exec->output);

	exec->query_desc->estate = CreateEState(exec->query_desc);

	/*
	 * set estate snapshot is invalid in the wal context
	 * SetEStateSnapshot(exec->query_desc->estate);
	 */

	plan = exec->query_desc->plannedstmt->planTree;

	exec->query_desc->planstate =
		ExecInitNode(plan, exec->query_desc->estate, 0);

	ExecutePlan(exec->query_desc->estate, exec->query_desc->planstate,
			exec->query_desc->operation,
			true, 0, ForwardScanDirection, exec->dest);

	ExecEndNode(exec->query_desc->planstate);
	exec->query_desc->planstate = NULL;

	/* ditto for unset estate snapshot
	 * UnsetEStateSnapshot(exec->query_desc->estate);
	 */

	exec->query_desc->estate = NULL;
}

/*
 * execute_sw_view
 *
 * Run the view overlay plan for the sw after a change to the matrel and
 * fire any relevant triggers
 *
 * This currently executes the view overlay plan for every change.
 */
static void
execute_sw_view(TriggerCacheEntry *trg_entry, Relation cvrel,
		TupleTableSlot *key_slot)
{
	ResultEntry *res_entry;
	MemoryContext old_cxt;
	TriggerEvent event_type;
	TriggerViewExec *exec = trg_entry->sw_view_exec;
	bool ok = false;
	HeapTuple old_tup = NULL;
	HeapTuple new_tup = NULL;

	tuplestore_clear(exec->input);

	res_entry = (ResultEntry *)
		LookupTupleHashEntry(trg_entry->sw_res_table, key_slot, NULL);

	Assert(res_entry);
	old_tup = res_entry->result;

	if (dlist_is_empty(&res_entry->list_head))
	{
		new_tup = NULL;
		event_type = TRIGGER_PROCESS_CHANGE_DELETE;
	}
	else
	{
		get_recalc_tups(res_entry, exec->input);
		run_plan(trg_entry);

		ok = tuplestore_gettupleslot(exec->output, true,
				false, exec->output_slot);

		Assert(ok);

		old_cxt = MemoryContextSwitchTo(trg_entry->sw_res_table->tablecxt);
		new_tup = ExecCopySlotTuple(exec->output_slot);
		MemoryContextSwitchTo(old_cxt);

		event_type = old_tup ? TRIGGER_PROCESS_CHANGE_UPDATE :
			TRIGGER_PROCESS_CHANGE_INSERT;
	}

	fire_triggers(trg_entry, cvrel, event_type, old_tup, new_tup);

	if (old_tup)
		heap_freetuple(old_tup);

	res_entry->result = new_tup;
}

/*
 * sw_vacuum_entry
 *
 * Find expiring tuples, and add to a batch of deletes.
 */
static void
sw_vacuum_entry(TriggerProcessState *state,
		TriggerCacheEntry *trg_entry, XactBatch *batch,
		TimestampTz cmp_ts)
{
	dlist_iter iter;
	bool found = false;

	TimestampEntry *ts_entry =
		hash_search(trg_entry->sw_timestamp_table, &cmp_ts,
				HASH_FIND, &found);

	Assert(found);

	if (!dlist_is_empty(&ts_entry->list_head))
	{
		ChangeList *changes =
			get_changelist(state, batch, trg_entry->matrelid);

		dlist_foreach(iter, &ts_entry->list_head)
		{
			MatrelEntry *mrel_entry = dlist_container(MatrelEntry,
					ts_list_node, iter.cur);

			add_change(state, changes, TRIGGER_PROCESS_CHANGE_DELETE,
					mrel_entry->tuple, NULL);
		}
	}
}

/*
 * sw_vacuum
 *
 * Use the ordered timestamp set and sw_timestamp_table to find
 * expired groups of tuples, and create a batch of deletes
 */
void
sw_vacuum(TriggerProcessState *state)
{
	XactBatch *batch;
	TimestampTz cur_time = GetCurrentTimestamp();

	if (!TimestampDifferenceExceeds(state->last_sw_vacuum,
			cur_time, TRIGGER_SW_VACUUM_INTERVAL))
		return;

	state->dirty_syscache = true;

	batch = create_batch(state, "vacuum", state->last_xid,
			state->last_commit_time);

	while (true)
	{
		TimestampTz first_ts;
		TriggerCacheEntry *entry = find_min_timestamp(state);

		if (!entry)
			break;

		first_ts = first_timestamp(entry);

		if (cur_time < first_ts)
			break;

		sw_vacuum_entry(state, entry, batch, first_ts);
		timestamp_set_pop(&entry->timestamps);
	}

	state->last_sw_vacuum = cur_time;
	batch->finished = true;

	if (dlist_is_empty(&batch->cl_list))
		pfree(batch);
	else
		add_batch(state, batch);
}

/*
 * create_sw_view_exec
 *
 * Create a context to execute the sw view overlay plan
 */
static TriggerViewExec *
create_sw_view_exec(PlannedStmt *pstmt, ContQuery *cv)
{
	TriggerViewExec *exec = palloc0(sizeof(TriggerViewExec));

	exec->input = tuplestore_begin_heap(true, true,
							continuous_query_combiner_work_mem);

	exec->output = tuplestore_begin_heap(true, true,
							continuous_query_combiner_work_mem);

	exec->input_desc = PreparePlanForReading(cv->matrel, pstmt, exec->input);

	exec->dest = CreateDestReceiver(DestTuplestore);
	exec->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot,
										InvalidSnapshot, exec->dest,
										NULL, 0);

	SetTuplestoreDestReceiverParams(exec->dest,
			exec->output, CurrentMemoryContext, true);

	ExecutorStart(exec->query_desc, 0);

	exec->output_slot =
		MakeSingleTupleTableSlot(
				CreateTupleDescCopy(exec->query_desc->tupDesc));

	exec->query_desc->estate->es_lastoid = InvalidOid;

	(exec->dest->rStartup) (exec->dest,
			exec->query_desc->operation,
			exec->query_desc->tupDesc);

	ExecEndNode(exec->query_desc->planstate);
	FreeExecutorState(exec->query_desc->estate);

	return exec;
}

/*
 * add_to_result_group
 *
 * Add the given mrel entry to the appropriate group
 */
static void
add_to_result_group(TriggerCacheEntry *trg_entry,
			 TupleTableSlot *slot,
			 MatrelEntry *mrel_entry)
{
	bool isnew = false;

	ResultEntry *res_entry = (ResultEntry *)
		LookupTupleHashEntry(trg_entry->sw_res_table, slot, &isnew);

	if (isnew)
	{
		res_entry->result = NULL;
		MemSet(&res_entry->list_head, 0, sizeof(dlist_head));
	}

	dlist_push_tail(&res_entry->list_head, &mrel_entry->list_node);
}

/*
 * add_to_ts_group
 *
 * Add the given mrel entry to the appropriate expiring timestamp entry
 */
static void
add_to_ts_group(TriggerCacheEntry *trg_entry,
			 TupleTableSlot *slot,
			 MatrelEntry *mrel_entry)
{
	bool found = false;
	TimestampTz tup_exp_time = get_tup_expiry_time(trg_entry, slot);

	TimestampEntry *ts_entry =
		hash_search(trg_entry->sw_timestamp_table, &tup_exp_time,
				HASH_ENTER, &found);

	if (!found)
	{
		memset(ts_entry, 0, sizeof(TimestampEntry));
		ts_entry->timestamp = tup_exp_time;
	}

	dlist_push_tail(&ts_entry->list_head, &mrel_entry->ts_list_node);
}

/*
 * track_sw_change
 *
 * Track changes to underlying matrel for sw queries
 * Executes the sw view and fires triggers if anything changes
 */
static void
track_sw_change(TriggerCacheEntry *trg_entry,
				Relation cvrel,
				enum TriggerProcessChangeType action,
				HeapTuple old_tup, HeapTuple new_tup)
{
	MemoryContext old_cxt;
	MatrelEntry *mrel_entry;
	HeapTuple to_del;
	bool isnew = false;

	TupleTableSlot *slot = trg_entry->sw_slot;

	bool touched = false;

	switch(action)
	{
		case TRIGGER_PROCESS_CHANGE_INSERT:
		case TRIGGER_PROCESS_CHANGE_UPDATE:

			ExecStoreTuple(new_tup, slot, InvalidBuffer, false);

			mrel_entry = (MatrelEntry *)
				LookupTupleHashEntry(trg_entry->sw_table,
						slot, &isnew);

			timestamp_set_insert(&trg_entry->timestamps,
					get_tup_expiry_time(trg_entry, slot));

			if (isnew)
			{
				mrel_entry->tuple = NULL;
				MemSet(&mrel_entry->list_node, 0, sizeof(dlist_node));
				MemSet(&mrel_entry->ts_list_node, 0, sizeof(dlist_node));

				add_to_result_group(trg_entry, slot, mrel_entry);
				add_to_ts_group(trg_entry, slot, mrel_entry);
			}
			else
				heap_freetuple(mrel_entry->tuple);

			old_cxt = MemoryContextSwitchTo(trg_entry->sw_table->tablecxt);
			mrel_entry->tuple = ExecCopySlotTuple(slot);
			MemoryContextSwitchTo(old_cxt);

			touched = true;

			break;

		case TRIGGER_PROCESS_CHANGE_DELETE:
			ExecStoreTuple(old_tup, slot, InvalidBuffer, false);

			mrel_entry = (MatrelEntry *)
				LookupTupleHashEntry(trg_entry->sw_table,
						slot, NULL);

			if (mrel_entry)
			{
				TimestampTz cmp_ts = get_tup_expiry_time(trg_entry, slot);
				TimestampEntry *ts_entry =
					hash_search(trg_entry->sw_timestamp_table, &cmp_ts,
							HASH_FIND, NULL);

				Assert(ts_entry);

				to_del = mrel_entry->tuple;

				dlist_delete(&mrel_entry->list_node);
				dlist_delete(&mrel_entry->ts_list_node);

				if (dlist_is_empty(&ts_entry->list_head))
				{
					hash_search(trg_entry->sw_timestamp_table, &cmp_ts,
							HASH_REMOVE, NULL);
				}

				heap_freetuple(to_del);
				RemoveTupleHashEntry(trg_entry->sw_table, slot);
				touched = true;
			}

			break;
		default:
			break;
	}

	if (touched)
		execute_sw_view(trg_entry, cvrel, slot);
}

/*
 * table_scan
 *
 * Scan the underlying matrel and synchronize the internal state of the
 * TriggerCacheEntry with the tuples.
 */
static void
table_scan(TriggerCacheEntry *trg_entry, Relation rel)
{
	TupleTableSlot *slot;
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	MatrelEntry *mrel_entry;
	MemoryContext old_cxt;

	bool isnew = false;
	scan = heap_beginscan(rel, GetTransactionSnapshot(), 0, NULL);
	slot = trg_entry->sw_slot;

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		ExecStoreTuple(tup, slot, InvalidBuffer, false);

		mrel_entry = (MatrelEntry *)
			LookupTupleHashEntry(trg_entry->sw_table,
					slot, &isnew);

		Assert(isnew);
		mrel_entry->tuple = NULL;

		MemSet(&mrel_entry->list_node, 0, sizeof(dlist_node));
		add_to_result_group(trg_entry, slot, mrel_entry);
		add_to_ts_group(trg_entry, slot, mrel_entry);

		timestamp_set_insert(&trg_entry->timestamps,
					get_tup_expiry_time(trg_entry, slot));

		old_cxt = MemoryContextSwitchTo(trg_entry->sw_table->tablecxt);
		mrel_entry->tuple = ExecCopySlotTuple(slot);
		MemoryContextSwitchTo(old_cxt);
	}

	heap_endscan(scan);
}

/*
 * find_pk_attr
 *
 * Find the pk column. 1 based.
 */
static AttrNumber
find_pk_attr(Relation matrel, TupleDesc desc)
{
	List *indices = NIL;
	ListCell *lc;
	AttrNumber pk = InvalidAttrNumber;

	indices = RelationGetIndexList(matrel);

	foreach(lc, indices)
	{
		Oid indexoid = lfirst_oid(lc);
		HeapTuple tup = SearchSysCache1(INDEXRELID,
				ObjectIdGetDatum(indexoid));
		Form_pg_index form;

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "index %u for matrel %u not found",
					indexoid, RelationGetRelid(matrel));

		form = (Form_pg_index) GETSTRUCT(tup);

		if (form->indisprimary)
		{
			Assert(form->indnatts == 1);
			pk = form->indkey.values[0];
		}

		ReleaseSysCache(tup);
	}

	return pk;
}

/*
 * lookup_eq_func_oid
 *
 * Get the pk field eq opr, so we can use it for hashing.
 * If the tuple desc doesn't match what we are expecting, we return InvalidOid
 */
static Oid
lookup_eq_func_oid(TupleDesc desc, AttrNumber pk)
{
	bool is_hash;
	Form_pg_attribute attr;
	int pki = pk - 1;
	Oid eqOpr = InvalidOid;

	Assert(pki >= 0 && pki < desc->natts);

	attr = desc->attrs[pki];

	get_sort_group_operators(attr->atttypid,
							 false, true, false,
							 NULL, &eqOpr, NULL,
							 &is_hash);

	if (!is_hash)
		return InvalidOid;

	return eqOpr;
}

/*
 * extract_epoch_from_interval
 */
static TimestampTz
extract_epoch_from_interval(Interval *i)
{
	Datum d;
	float8 f;

	d = DirectFunctionCall2(interval_part,
			CStringGetTextDatum("epoch"), (Datum) i);
	f = DatumGetFloat8(d);

	return (TimestampTz) f;
}

/*
 * find_arrival_ts_attr
 *
 * Find the arrival_timestamp column of a SW matrel
 */
static AttrNumber
find_arrival_ts_attr(TupleDesc desc)
{
	int i;
	AttrNumber result = InvalidAttrNumber;

	for (i = 0; i < desc->natts; i++)
	{
		if (pg_strcasecmp(ARRIVAL_TIMESTAMP,
					NameStr(desc->attrs[i]->attname)) == 0)
		{
			result = i + 1;
			break;
		}
	}

	Assert(result);

	return result;
}

/*
 * get_view_desc
 */
static TupleDesc
get_view_desc(TriggerCacheEntry *entry)
{
	return entry->sw_view_exec->output_slot->tts_tupleDescriptor;
}

/*
 * init_sw
 *
 * Initialize the relevant parts of a TriggerCacheEntry for sw queries
 */
void
init_sw(TriggerCacheEntry *entry, Relation rel)
{
	TupleDesc input_desc = CreateTupleDescCopy(RelationGetDescr(rel));
	AttrNumber pk = find_pk_attr(rel, input_desc);
	Oid eq_func_oid = lookup_eq_func_oid(input_desc, pk);

	Oid *res_grp_ops;
	AttrNumber *res_grp_idx;
	int res_grp_n;
	Interval *i;

	Assert(OidIsValid(eq_func_oid));

	i = GetSWInterval(entry->cv_name);

	entry->sw_interval_ms = 1000 * extract_epoch_from_interval(i);
	entry->sw_arrival_ts_attr = find_arrival_ts_attr(input_desc);

	entry->sw_grp = RelationGetDescr(rel)->natts;

	/* tracks the raw tuples */

	entry->sw_table = create_agg_hash("sw_table", 1,
			&eq_func_oid, &entry->sw_grp,
			sizeof(MatrelEntry));

	entry->sw_slot = MakeSingleTupleTableSlot(input_desc);
	entry->spare_slot = MakeSingleTupleTableSlot(input_desc);
	entry->trig_func = track_sw_change;

	/* setup the matching code required to find tups that need to be
	 * pulled in when the contents of a sliding window changes
	 *
	 * note - this group key is the group key of the result, i.e.
	 * 		  without the arrival timestamp
	 */

	if (IsA(entry->pstmt->planTree, Agg))
	{
		/*
		 * use the group key
		 *
		 * This handles normal sw grouping queries like
		 * select x::int,count(*) from s where (sw cond)
		 *
		 * numcols = 0 case as well
		 * (i.e. select sum(*) from s where (sw cond)*/

		Agg *agg = (Agg *) entry->pstmt->planTree;

		res_grp_ops = agg->grpOperators;
		res_grp_idx = agg->grpColIdx;
		res_grp_n = agg->numCols;
	}
	else
	{
		/*
		 * use the matrel's $pk field.
		 *
		 * This is to handle queries like
		 * select x::int from s where (sw cond)
		 */

		res_grp_ops = palloc0(sizeof(Oid));
		res_grp_ops[0] = eq_func_oid;

		res_grp_idx = palloc0(sizeof(AttrNumber));
		res_grp_idx[0] = pk;
		res_grp_n = 1;
	}

	entry->sw_res_table =
		create_agg_hash("sw_res_table", res_grp_n,
				res_grp_ops,
				res_grp_idx,
				sizeof(ResultEntry));

	entry->sw_timestamp_table = create_timestamp_hash("sw_timestamp_table",
													  CurrentMemoryContext);

	entry->sw_view_exec = create_sw_view_exec(entry->pstmt, entry->cv);
	entry->output_desc = CreateTupleDescCopy(get_view_desc(entry));

	init_timestamp_set(&entry->timestamps, CurrentMemoryContext);

	register_formatter(entry->cvrelid, entry->output_desc);
	table_scan(entry, rel);
}

/*
 * find_min_timestamp
 *
 * Find the TriggerCacheEntry with the next expiring timestamp
 */
static TriggerCacheEntry *
find_min_timestamp(TriggerProcessState *state)
{
	TriggerCacheEntry *entry = NULL;
	TriggerCacheEntry *min_entry = NULL;
	HASH_SEQ_STATUS seq;
	Timestamp ts;
	TimestampTz min_ts = DT_NOEND;

	hash_seq_init(&seq, state->trigger_cache);

	while ((entry = (TriggerCacheEntry*) hash_seq_search(&seq)) != NULL)
	{
		if (timestamp_set_is_empty(&entry->timestamps))
			continue;

		ts = first_timestamp(entry);

		if (ts < min_ts)
		{
			min_ts = ts;
			min_entry = entry;
		}
	}

	return min_entry;
}

/*
 * first_timestamp
 */
static TimestampTz
first_timestamp(TriggerCacheEntry *entry)
{
	Assert(!timestamp_set_is_empty(&entry->timestamps));
	return timestamp_set_first(&entry->timestamps);
}

/*
 * calc_expiry_time
 */
static TimestampTz
calc_expiry_time(TimestampTz tz, int msec)
{
#ifdef HAVE_INT64_TIMESTAMP
	return (tz + (msec * INT64CONST(1000)));
#else
	return (tz + 1000.0 * msec);
#endif
}

/*
 * create_timestamp_hash
 *
 * Create a hash table for tracking MatrelEntry by timestamp
 */
static HTAB*
create_timestamp_hash(const char *name, MemoryContext hcxt)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(TimestampTz);
	ctl.entrysize = sizeof(TimestampEntry);
	ctl.hash = tag_hash;
	ctl.hcxt = hcxt;

	return hash_create(name, 32, &ctl, HASH_ELEM | HASH_FUNCTION |
			HASH_CONTEXT);
}
