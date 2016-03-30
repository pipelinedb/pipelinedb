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

extern Datum send_alert_new_row(PG_FUNCTION_ARGS);
extern Datum test_alert_new_row(PG_FUNCTION_ARGS);
extern Datum trigger_testing_setup(PG_FUNCTION_ARGS);
extern Datum trigger_testing_sync(PG_FUNCTION_ARGS);

#endif
