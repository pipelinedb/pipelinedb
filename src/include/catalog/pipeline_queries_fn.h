/*-------------------------------------------------------------------------
 *
 * pipeline_queries_fn.h
 *	 prototypes for functions in catalog/pipeline_queries.c
 *
 *
 * src/include/catalog/pipeline_queries_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERIES_FN_H
#define PIPELINE_QUERIES_FN_H

#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

typedef struct ContinuousViewParams
{
	int8 		batchsize;
	int32 		maxwaitms;
	int32		emptysleepms;
	int16		parallelism;

} ContinuousViewParams;

void RegisterContinuousView(RangeVar *name, const char *query_string);
void DeregisterContinuousView(RangeVar *name);
bool MarkContinuousViewAsActive(RangeVar *name);
bool MarkContinuousViewAsInactive(RangeVar *name);
void SetContinousViewParams(RangeVar *name, List *parameters);
void GetContinousViewParams(RangeVar *name, ContinuousViewParams *cv_params);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryString(RangeVar *name, int *cqid, bool selectonly);

#endif /* PIPELINE_QUERIES_H */
