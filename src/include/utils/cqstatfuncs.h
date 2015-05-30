/*-------------------------------------------------------------------------
 *
 * cqstatfuncs.h

 *	  Interface for CQ stats functions
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

#endif
