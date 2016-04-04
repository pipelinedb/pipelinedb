/*-------------------------------------------------------------------------
 *
 * batching.c
 *	  Functionality for batching
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "pipeline/trigger/batching.h"
#include "access/heapam.h"
#include "storage/lock.h"
#include "access/xact.h"

/*
 * find_change_list
 */
static ChangeList *
find_change_list(XactBatch *batch, Oid relid)
{
	dlist_iter iter;

	dlist_foreach(iter, &batch->cl_list)
	{
		ChangeList *cl =
			dlist_container(ChangeList, list_node, iter.cur);

		if (cl->relid == relid)
			return cl;
	}

	return NULL;
}

/*
 * get_changelist
 *
 * Find a change list, create if it does not exist
 */
ChangeList *
get_changelist(TriggerProcessState *state,
				XactBatch *batch,
				Oid relid)
{
	ChangeList *cl = find_change_list(batch, relid);
	MemoryContext cache_cxt = state->cache_cxt;

	if (cl == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(cache_cxt);
		cl = palloc0(sizeof(ChangeList));
		cl->relid = relid;

		dlist_push_tail(&batch->cl_list, &cl->list_node);
		MemoryContextSwitchTo(old);
	}

	return cl;
}

/*
 * create_batch
 *
 * Allocate and return a new XactBatch (not linked to anything)
 */
XactBatch *
create_batch(TriggerProcessState *state,
			 const char *desc,
			 TransactionId xmin,
			 TimestampTz commit_time)
{
	MemoryContext old = MemoryContextSwitchTo(state->cache_cxt);

	XactBatch *batch = palloc0(sizeof(XactBatch));

	batch->batch_id = 0;
	batch->desc = desc;
	batch->xmin = xmin;
	batch->commit_time = commit_time;

	MemoryContextSwitchTo(old);

	return batch;
}

/*
 * add_batch
 *
 * Append a premade batch to the batch list
 */
void
add_batch(TriggerProcessState *state, XactBatch *batch)
{
	batch->batch_id = state->batch_id++;
	dlist_push_tail(&state->xact_batches, &batch->list_node);
}

/*
 * start_new_batch
 *
 * Create a new batch and append it to the batch list
 */
XactBatch *
start_new_batch(TriggerProcessState *state,
				const char *desc,
				TransactionId xmin,
				TimestampTz commit_time)
{
	XactBatch *batch = create_batch(state, desc, xmin, commit_time);
	batch->batch_id = state->batch_id++;

	dlist_push_tail(&state->xact_batches, &batch->list_node);

	return batch;
}

/*
 * add_change
 *
 * Create and append a new Change to a ChangeList
 * old/new tuples are copied
 */
void
add_change(TriggerProcessState *state, ChangeList *cl,
		TriggerProcessChangeType action,
		HeapTuple old_tup,
		HeapTuple new_tup)
{
	MemoryContext old_cxt = MemoryContextSwitchTo(state->cache_cxt);

	Change *c = palloc0(sizeof(Change));
	c->action = action;

	if (old_tup)
		c->old_tup = heap_copytuple(old_tup);

	if (new_tup)
		c->new_tup = heap_copytuple(new_tup);

	dlist_push_tail(&cl->changes, &c->list_node);
	MemoryContextSwitchTo(old_cxt);
}

/*
 * cleanup_changelist
 *
 * Deep free the changelist, and pfree it.
 */
void
cleanup_changelist(ChangeList *cl)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &cl->changes)
	{
		Change *chg =
			dlist_container(Change, list_node, iter.cur);

		if (chg->old_tup)
			heap_freetuple(chg->old_tup);

		if (chg->new_tup)
			heap_freetuple(chg->new_tup);

		pfree(chg);
	}

	pfree(cl);
}

/*
 * cleanup_batch
 *
 * Deep free the batch, and pfree it
 */
void
cleanup_batch(XactBatch *batch)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &batch->cl_list)
	{
		ChangeList *cl =
			dlist_container(ChangeList, list_node, iter.cur);

		cleanup_changelist(cl);
	}

	pfree(batch);
}

/*
 * should_process_batch
 *
 * Determine if this batch is visible
 */
static bool
should_process_batch(TriggerProcessState *state,
		TriggerCacheEntry *entry, TransactionId batch_xid, Relation rel,
		Relation cvrel)
{
	if (entry->numtriggers == 0)
		return false;

	if (entry->xmin && TransactionIdPrecedes(batch_xid, entry->xmin))
		return false;

	return true;
}

/*
 * process_changelist
 *
 * Update the TriggerCacheEntry state, and call the trig func for each change
 */
void
process_changelist(TriggerProcessState *state,
		ChangeList *cl, TransactionId xid, Relation rel,
		Relation cvrel, TriggerCacheEntry *entry)
{
	dlist_iter iter;
	diff_triggers(state, entry, rel, cvrel->trigdesc);

	if (!should_process_batch(state, entry, xid, rel, cvrel))
		return;

	dlist_foreach(iter, &cl->changes)
	{
		Change *chg = dlist_container(Change, list_node, iter.cur);
		entry->trig_func(entry, rel, cvrel, chg->action,
				chg->old_tup, chg->new_tup);
	}
}

/*
 * process_batch
 *
 * For each changelist in the batch, open the necessary relations and
 * call process_changelist
 */
void
process_batch(TriggerProcessState *state, XactBatch *batch)
{
	dlist_iter iter;

	dlist_foreach(iter, &batch->cl_list)
	{
		Relation cvrel = NULL;
		Relation rel = NULL;

		ChangeList *cl =
			dlist_container(ChangeList, list_node, iter.cur);

		TriggerCacheEntry *entry = hash_search(state->trigger_cache,
				&cl->relid, HASH_FIND, NULL);

		/* Could have just been cleaned up */
		if (!entry)
			continue;

		cvrel = try_relation_open(entry->cvrelid, AccessShareLock);

		if (cvrel)
			rel = try_relation_open(cl->relid, AccessShareLock);

		if (rel && cvrel)
			process_changelist(state, cl, batch->xmin, rel, cvrel, entry);

		if (cvrel)
			relation_close(cvrel, AccessShareLock);

		if (rel)
			relation_close(rel, AccessShareLock);
	}
}

/*
 * first_batch
 */
static XactBatch *
first_batch(TriggerProcessState *state)
{
	if (dlist_is_empty(&state->xact_batches))
		return NULL;

	return dlist_container(XactBatch, list_node,
			dlist_head_node(&state->xact_batches));
}

/*
 * process_batches
 *
 * Pop and process any finished batches
 */
void
process_batches(TriggerProcessState *state)
{
	while (true)
	{
		XactBatch *batch = first_batch(state);

		if (!batch || !batch->finished)
			return;

		dlist_pop_head_node(&state->xact_batches);

		XactReadOnly = true;
		state->dirty_syscache = true;

		StartTransactionCommand();
		process_batch(state, batch);
		CommitTransactionCommand();

		cleanup_batch(batch);
	}
}
