/*-------------------------------------------------------------------------
 *
 * pipelinefuncs.h
 *
 *	  Interface for PipelineDB functions
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/utils/pipelinefuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQSTATFUNCS_H
#define CQSTATFUNCS_H


#define DISPLAY_OVERLAY_VIEW -2

/* continuous query process stats */
extern Datum cq_proc_stat_get(PG_FUNCTION_ARGS);

/* continuous query stats */
extern Datum cq_stat_get(PG_FUNCTION_ARGS);

/* stream stats */
extern Datum stream_stat_get(PG_FUNCTION_ARGS);

/* pipeline queries */
extern Datum pipeline_queries(PG_FUNCTION_ARGS);

/* pipeline streams */
extern Datum pipeline_streams(PG_FUNCTION_ARGS);

/* global pipeline stats */
extern Datum pipeline_stat_get(PG_FUNCTION_ARGS);

/* matrel overlay view definition */
extern Datum pipeline_get_overlay_viewdef(PG_FUNCTION_ARGS);

/* PipelineDB version string */
extern Datum pipeline_version(PG_FUNCTION_ARGS);

extern Datum pipeline_get_worker_querydef(PG_FUNCTION_ARGS);

extern Datum pipeline_get_combiner_querydef(PG_FUNCTION_ARGS);

extern Datum pipeline_exec_adhoc_query(PG_FUNCTION_ARGS);

#endif
