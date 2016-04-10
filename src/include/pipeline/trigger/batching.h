/*-------------------------------------------------------------------------
 *
 * batching.h
 *	  Interface for batching
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TRIGGER_BATCHING_H
#define PIPELINE_TRIGGER_BATCHING_H

#include "postgres.h"
#include "access/htup.h"
#include "lib/ilist.h"
#include "datatype/timestamp.h"
#include "trigger.h"

typedef struct Change
{
	enum TriggerProcessChangeType action;
	HeapTuple old_tup;
	HeapTuple new_tup;
	dlist_node list_node;
} Change;

typedef struct ChangeList
{
	Oid relid;
	dlist_head changes;
	dlist_node list_node;
} ChangeList;

typedef struct XactBatch
{
	int batch_id;
	const char *desc;
	TransactionId xmin;
	TimestampTz commit_time;
	bool finished;
	dlist_head cl_list;
	dlist_node list_node;
} XactBatch;

extern ChangeList *get_changelist(TriggerProcessState *state, XactBatch *batch,
		Oid relid);

extern XactBatch *create_batch(TriggerProcessState *state, const char *desc,
		TransactionId xmin,
		TimestampTz commit_time);

extern void add_batch(TriggerProcessState *state, XactBatch *batch);

extern XactBatch *start_new_batch(TriggerProcessState *state,
		const char *desc,
		TransactionId xmin,
		TimestampTz commit_time);

extern void add_change(TriggerProcessState *state, ChangeList *cl,
		TriggerProcessChangeType action,
		HeapTuple old_tup,
		HeapTuple new_tup);

extern void cleanup_changelist(ChangeList *cl);

extern void cleanup_batch(XactBatch *batch);

extern void process_changelist(TriggerProcessState *state,
		ChangeList *cl, TransactionId xid,
		Relation cvrel, TriggerCacheEntry *entry);

extern void process_batch(TriggerProcessState *state, XactBatch *batch);
extern void process_batches(TriggerProcessState *state);

#endif
