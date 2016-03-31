/*-------------------------------------------------------------------------
 *
 * triggerfuncs.h
 *	  Functions to call from continuous triggers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TRIGGERFUNCS_H
#define PIPELINE_TRIGGERFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum pipeline_send_alert_new_row(PG_FUNCTION_ARGS);
extern Datum pipeline_test_alert_new_row(PG_FUNCTION_ARGS);

extern Datum pipeline_get_alert_server_conn(PG_FUNCTION_ARGS);

extern Datum pipeline_trigger_debug(PG_FUNCTION_ARGS);

#endif
