/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqstatfuncs.h

 *	  Interface for CQ stats functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQSTATFUNCS_H
#define CQSTATFUNCS_H

/* CQ process-level stats */
extern Datum cq_stat_proc_get(PG_FUNCTION_ARGS);

/* CQ all-time stats */
extern Datum cq_stat_get(PG_FUNCTION_ARGS);

#endif
