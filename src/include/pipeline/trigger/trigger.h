/*-------------------------------------------------------------------------
 *
 * trigger.h
 *	  Interface for continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TRIGGERS_H
#define PIPELINE_TRIGGERS_H

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"

#include "alert_server.h"
#include "utils/tuplestore.h"
#include "tcop/dest.h"
#include "executor/execdesc.h"
#include "timestamp_set.h"

extern int alert_socket_mem;
extern int alert_server_port;
extern bool continuous_triggers_enabled;

typedef enum TriggerProcessChangeType
{
	TRIGGER_PROCESS_CHANGE_INSERT,
	TRIGGER_PROCESS_CHANGE_UPDATE,
	TRIGGER_PROCESS_CHANGE_DELETE,
	TRIGGER_PROCESS_CHANGE_NOOP
} TriggerProcessChangeType;

typedef struct TriggerProcessState
{
	AlertServer *server;

	TimestampTz last_trigger_cache_cleanup;
	TimestampTz last_sw_vacuum;
	HTAB *trigger_cache;
	StringInfo check_buf;
	MemoryContext cache_cxt;
	dlist_head xact_batches;

	TransactionId last_xid;
	TimestampTz last_commit_time;

	struct XactBatch *cur_wal_batch;
	int batch_id;
	bool dirty_syscache;

} TriggerProcessState;

typedef struct TriggerViewExec
{
	Tuplestorestate *input;
	Tuplestorestate *output;

	TupleDesc input_desc;
	DestReceiver *dest;

	QueryDesc *query_desc;
	TupleTableSlot *output_slot;

} TriggerViewExec;

/* keyed with the raw tuple group (e.g. $pk) */

typedef struct MatrelEntry
{
	TupleHashEntryData shared;
	HeapTuple tuple;	/* mrel tuple */
	dlist_node list_node;
	dlist_node ts_list_node;

} MatrelEntry;

/* keyed with the result group */

typedef struct ResultEntry
{
	TupleHashEntryData shared;
	HeapTuple result;	/* sw result tuple */
	dlist_head list_head; /* list of MatrelEntry */

} ResultEntry;

typedef struct TimestampEntry
{
	TimestampTz timestamp;
	dlist_head list_head; /* list of MatrelEntry (by ts_list_node) */
} TimestampEntry;

typedef struct TriggerCacheEntry TriggerCacheEntry;

typedef void (*TrigFunc)(TriggerCacheEntry *entry, Relation rel,
			Relation cvrel,
			TriggerProcessChangeType action,
			HeapTuple old_tup,
			HeapTuple new_tup);

struct TriggerCacheEntry
{
	Oid matrelid;
	Oid cvrelid;
	Oid cvid;
	bool is_sw;
	bool is_adhoc;

	RangeVar *cv_name;
	ContQuery *cv;
	PlannedStmt *pstmt;

	ProjectionInfo *proj_inner;
	ProjectionInfo *proj_outer;

	TupleHashTable sw_table;
	TupleTableSlot *sw_slot;
	TupleTableSlot *spare_slot;

	AttrNumber sw_grp;

 	TupleHashTable sw_res_table;
	TriggerViewExec *sw_view_exec;

	AttrNumber sw_arrival_ts_attr;
	TimestampTz sw_interval_ms;

	EState *estate;
	ExprContext *econtext;
	TupleDesc output_desc;

	TransactionId xmin;
	HTAB *sw_timestamp_table;
	TimestampSet timestamps;

	dlist_head triggers;
	int numtriggers;

	TrigFunc trig_func;

};

typedef struct ViewInfo
{
	Oid matrel_id;
	List *target_list;
	TupleDesc matrel_desc;
} ViewInfo;

typedef struct WalStream
{
	LogicalDecodingContext *logical_decoding_ctx;
	XLogRecPtr startptr;
	XLogRecPtr end_of_wal;
} WalStream;

typedef struct TrigInfo
{
	Oid tgoid;
	List *when;
	FmgrInfo fmgr_info;

	Trigger *trigger;
	dlist_node list_node;

} TrigInfo;

extern void trigger_plugin_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
extern void trigger_plugin_decode_shutdown(LogicalDecodingContext *ctx);
extern void trigger_plugin_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
extern void trigger_plugin_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void trigger_plugin_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation matrel, ReorderBufferChange *change);

extern void diff_triggers(TriggerProcessState *state, TriggerCacheEntry *entry,
		Relation rel, TriggerDesc *newdesc);

extern void trigger_check_catalog(void *data, const char *name);
extern AlertServer *MyAlertServer;

extern void fire_triggers(TriggerCacheEntry *entry, Relation rel,
		Relation cvrel,
		TriggerProcessChangeType action,
		HeapTuple old_tup, HeapTuple new_tup);

extern void trigger_main(void);

#endif
