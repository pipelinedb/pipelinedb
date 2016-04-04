/*-------------------------------------------------------------------------
 *
 * timestamp_set.c
 *	  Functionality for timestamp set
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "pipeline/trigger/timestamp_set.h"

/*
 * This is a very simple set of routines for keeping an ordered set of
 * timestamps.
 *
 * It could be made much more efficient, currently we are expecting low N, and
 * a low update rate
 */

/*
 * init_timestamp_set
 *
 * cxt is the MemoryContext that new list items are to be created in.
 */
void
init_timestamp_set(TimestampSet *ts_set, MemoryContext cxt)
{
	ts_set->cxt = cxt;
	memset(&ts_set->items, 0, sizeof(ts_set->items));
}

/*
 * cleanup_timestamp_set
 *
 * Deep free the ts_set item list, and set it to null.
 */
void
cleanup_timestamp_set(TimestampSet *ts_set)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &ts_set->items)
	{
		TimestampSetNode *tsnode =
			dlist_container(TimestampSetNode, list_node, iter.cur);

		pfree(tsnode);
	}

	memset(&ts_set->items, 0, sizeof(ts_set->items));
}

/*
 * timestamp_set_lower_bound
 *
 * Find the first node that compares gte the given timestamp
 */
static TimestampSetNode*
timestamp_set_lower_bound(TimestampSet *ts_set, TimestampTz ts)
{
	dlist_iter iter;

	dlist_foreach(iter, &ts_set->items)
	{
		TimestampSetNode *tsnode =
			dlist_container(TimestampSetNode, list_node, iter.cur);

		if (tsnode->timestamp >= ts)
			return tsnode;
	}

	return NULL;
}

/*
 * timestamp_set_insert
 *
 * Inserts ts into set if it is not already present
 */
void
timestamp_set_insert(TimestampSet *ts_set, TimestampTz ts)
{
	MemoryContext old;
	TimestampSetNode *new_node = NULL;
	TimestampSetNode *ts_node = timestamp_set_lower_bound(ts_set, ts);

	if (ts_node && ts_node->timestamp == ts)
		return;

	old = MemoryContextSwitchTo(ts_set->cxt);
	new_node = palloc0(sizeof(TimestampSetNode));
	new_node->timestamp = ts;
	MemoryContextSwitchTo(old);

	if (ts_node == 0)
	{
		dlist_push_tail(&ts_set->items, &new_node->list_node);
		return;
	}

	dlist_insert_before(&ts_node->list_node, &new_node->list_node);
}

/*
 * timestamp_set_pop
 *
 * Pop and return the min timestamp. (Must exist)
 */
TimestampTz
timestamp_set_pop(TimestampSet *ts_set)
{
	TimestampTz ts;
	TimestampSetNode *tsnode;
	Assert(!timestamp_set_is_empty(ts_set));

	tsnode = dlist_container(TimestampSetNode, list_node,
			dlist_pop_head_node(&ts_set->items));

	ts = tsnode->timestamp;
	pfree(tsnode);

	return ts;
}

/*
 * timestamp_set_first
 *
 * Return the min timestamp. (Must exist)
 */
TimestampTz
timestamp_set_first(TimestampSet *ts_set)
{
	TimestampSetNode *tsnode;
	Assert(!timestamp_set_is_empty(ts_set));

	tsnode = dlist_container(TimestampSetNode, list_node,
			dlist_head_node(&ts_set->items));

	return tsnode->timestamp;
}

/*
 * timestamp_set_is_empty
 */
bool
timestamp_set_is_empty(TimestampSet *ts_set)
{
	return dlist_is_empty(&ts_set->items);
}
