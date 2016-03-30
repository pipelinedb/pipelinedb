/*-------------------------------------------------------------------------
 *
 * timestamp_set.h
 *	  Interface for timestamp set
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#ifndef TIMESTAMP_SET_H
#define TIMESTAMP_SET_H

#include "postgres.h"
#include "datatype/timestamp.h"
#include "lib/ilist.h"

typedef struct TimestampSetNode
{
	TimestampTz timestamp;
	dlist_node list_node;
} TimestampSetNode;

typedef struct TimestampSet
{
	MemoryContext cxt;
	dlist_head items;
} TimestampSet;

extern void init_timestamp_set(TimestampSet *ts_set, MemoryContext cxt);
extern void cleanup_timestamp_set(TimestampSet *ts_set);
extern void timestamp_set_insert(TimestampSet *ts_set, TimestampTz ts);
extern TimestampTz timestamp_set_pop(TimestampSet *ts_set);
extern TimestampTz timestamp_set_first(TimestampSet *ts_set);
extern bool timestamp_set_is_empty(TimestampSet *ts_set);

#endif
