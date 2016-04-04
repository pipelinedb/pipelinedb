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
bool continuous_triggers_enabled;

