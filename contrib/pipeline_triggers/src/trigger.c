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

void
RegisterTriggerProcess(void)
{
//	BackgroundWorker worker;
//
//	if (!triggers_enabled)
//		return;
//
//	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
//	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
//	worker.bgw_main = trigger_main;
//	worker.bgw_notify_pid = 0;
//	worker.bgw_restart_time = 1; /* recover in 1s */
//	worker.bgw_let_crash = false;
//	worker.bgw_main_arg = PointerGetDatum(0);
//
//	snprintf(worker.bgw_name, BGW_MAXLEN, TRIGGER_PROC_NAME);
//	RegisterBackgroundWorker(&worker);
}
