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
#include "trigger.h"
#include "replication/output_plugin.h"
#include "fmgr.h"

#define PIPELINEDB_TRIGGERS "pipelinedb_triggers"

int alert_socket_mem;
bool is_trigger_process;

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/*
 * Declare configuration parameters and install hooks. We can't do too much work here
 * because almost nothing is initialized yet.
 */
void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system. (We don't throw error here because it seems useful to
	 * allow the pipelinedb_enterprise functions to be created even when the
	 * module isn't active. The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(WARNING, "pipelinedb_triggers must be loaded via shared_preload_libraries");
		return;
	}

	DefineCustomIntVariable("pipelinedb_triggers.alert_socket_mem",
		 gettext_noop("Amount of memory in KB to buffer alert server writes per socket."),
		 NULL,
		 &alert_socket_mem,
		 1024, 1024, 8192,
		 PGC_POSTMASTER, 0,
		 NULL, NULL, NULL);

	EmitWarningsOnPlaceholders(PIPELINEDB_TRIGGERS);

	RegisterTriggerProcess();
}

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

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = output_plugin_decode_startup;
	cb->begin_cb = output_plugin_decode_begin_txn;
	cb->change_cb = output_plugin_decode_change;
	cb->commit_cb = output_plugin_decode_commit_txn;
	cb->shutdown_cb = output_plugin_decode_shutdown;
}
