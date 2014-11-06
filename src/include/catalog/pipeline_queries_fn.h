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
#include "pipeline/cqrun.h"
#include "utils/relcache.h"

typedef struct ContinuousViewState
{
	int32	id;
	char	state;
	int32 	batchsize;
	int32 	maxwaitms;
	int32	emptysleepms;
	int16	parallelism;
	CQProcessType ptype;
} ContinuousViewState;

List *GetAllContinuousViewNames(void);
void RegisterContinuousView(RangeVar *name, const char *query_string);
bool MarkContinuousViewAsActive(RangeVar *name, Relation pipeline_queries);
bool MarkContinuousViewAsInactive(RangeVar *name, Relation pipeline_queries);
void SetContinousViewState(RangeVar *name, ContinuousViewState *cv_state, Relation pipeline_queries);
void GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryStringOrNull(const char *cvname, bool selectonly);
char *GetQueryString(const char *cvname, bool selectonly);
bool IsAContinuousView(RangeVar *name);
void MarkAllContinuousViewsAsInactive(void);

#endif
