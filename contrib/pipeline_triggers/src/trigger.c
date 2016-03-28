/*-------------------------------------------------------------------------
 *
 * trigger.c
 *	  Functionality for continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "trigger.h"
#include "postmaster/bgworker.h"
#include "config.h"
#include "utils/hax.h"

#define TRIGGER_PROC_NAME "pipelinedb_enterprise trigger"

static void
trigger_main(Datum main_arg)
{
	elog(LOG, "trig main");
}

void
RegisterTriggerProcess(void)
{
	elog(LOG, "trig enabled %d", triggers_enabled);

	BackgroundWorker worker;

	if (!triggers_enabled)
		return;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_main = trigger_main;
	worker.bgw_notify_pid = 0;
	worker.bgw_restart_time = 1; /* recover in 1s */
	worker.bgw_let_crash = false;
	worker.bgw_main_arg = PointerGetDatum(0);

	snprintf(worker.bgw_name, BGW_MAXLEN, TRIGGER_PROC_NAME);

	RegisterBackgroundWorker(&worker);
}

void trigger_plugin_decode_startup(LogicalDecodingContext *ctx,
		OutputPluginOptions *opt, bool is_init) {}
void trigger_plugin_decode_shutdown(LogicalDecodingContext *ctx) {}
void trigger_plugin_decode_begin_txn(LogicalDecodingContext *ctx,
		ReorderBufferTXN *txn) {}
void trigger_plugin_decode_commit_txn(LogicalDecodingContext *ctx,
		ReorderBufferTXN *txn, XLogRecPtr commit_lsn) {}
void trigger_plugin_decode_change(LogicalDecodingContext *ctx,
		ReorderBufferTXN *txn, Relation matrel, ReorderBufferChange *change) {}

