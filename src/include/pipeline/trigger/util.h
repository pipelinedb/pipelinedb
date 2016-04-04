/*-------------------------------------------------------------------------
 *
 * util.h
 *	  Interface for util
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRIGGER_UTIL_H
#define TRIGGER_UTIL_H

#include "postgres.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "trigger.h"

extern void trigger_plugin_decode_startup(LogicalDecodingContext *ctx,
					OutputPluginOptions *opt,
				  	bool is_init);

extern void trigger_plugin_decode_shutdown(LogicalDecodingContext *ctx);

extern void trigger_prepare_write(LogicalDecodingContext *ctx,
				   XLogRecPtr lsn,
				   TransactionId xid, bool last_write);

extern void trigger_write_data(LogicalDecodingContext *ctx,
				XLogRecPtr lsn,
				TransactionId xid,
				bool last_write);

extern TriggerProcessChangeType map_rb_change_to_trig(enum ReorderBufferChangeType in);
extern int map_trig_change_to_event(TriggerProcessChangeType in);

extern ProjectionInfo *build_projection(List *tlist, EState *estate,
		ExprContext *econtext, TupleDesc input_desc);

extern TupleHashTable create_agg_hash(const char* name, int num_cols,
		Oid *grp_ops, AttrNumber *group_atts, size_t entrysize);

extern TupleDesc PreparePlanForReading(RangeVar *matrel, PlannedStmt *plan,
		Tuplestorestate *batch);

extern Trigger* CopyTrigger(Trigger *src);

#endif
