/*-------------------------------------------------------------------------
 *
 * config.c
 *
 *	  PipelineDB Enterprise extension configuration
 *
 * Copyright (c) 2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "replication/logical.h"
#include "pipeline/trigger/trigger.h"
#include "replication/output_plugin.h"
#include "fmgr.h"

#define PIPELINEDB_TRIGGERS "pipelinedb_triggers"

/* These must be available to pg_dlsym() */

/* guc */
int alert_socket_mem;
int alert_socket_port;
bool is_trigger_process;
bool continuous_triggers_enabled;

static void
output_plugin_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	Assert(is_trigger_process);
	trigger_plugin_decode_startup(ctx, opt, is_init);
}

static void
output_plugin_decode_shutdown(LogicalDecodingContext *ctx)
{
	Assert(is_trigger_process);
	trigger_plugin_decode_shutdown(ctx);
}

static void
output_plugin_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	Assert(is_trigger_process);
	trigger_plugin_decode_begin_txn(ctx, txn);
}

static void
output_plugin_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	Assert(is_trigger_process);
	trigger_plugin_decode_commit_txn(ctx, txn, commit_lsn);
}

static void
output_plugin_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
		Relation rel, ReorderBufferChange *change)
{
	Assert(is_trigger_process);
	trigger_plugin_decode_change(ctx, txn, rel, change);
}

static void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = output_plugin_decode_startup;
	cb->begin_cb = output_plugin_decode_begin_txn;
	cb->change_cb = output_plugin_decode_change;
	cb->commit_cb = output_plugin_decode_commit_txn;
	cb->shutdown_cb = output_plugin_decode_shutdown;
}
