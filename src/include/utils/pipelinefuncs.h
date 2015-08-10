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

#endif
