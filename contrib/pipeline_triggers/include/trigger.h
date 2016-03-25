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

extern void RegisterTriggerProcess(void);

extern void trigger_plugin_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
extern void trigger_plugin_decode_shutdown(LogicalDecodingContext *ctx);
extern void trigger_plugin_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
extern void trigger_plugin_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void trigger_plugin_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation matrel, ReorderBufferChange *change);

#endif
