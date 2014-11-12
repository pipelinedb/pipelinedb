/*-------------------------------------------------------------------------
 *
 * pipeline_query_fn.h
 *	 prototypes for functions in catalog/pipeline_query.c
 *
 *
 * src/include/catalog/pipeline_query_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERY_FN_H
#define PIPELINE_QUERY_FN_H

#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "pipeline/cqproc.h"
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
void RegisterContinuousView(RangeVar *name, const char *query_string, bool gc);
bool MarkContinuousViewAsActive(RangeVar *name, Relation pipeline_query);
bool MarkContinuousViewAsInactive(RangeVar *name, Relation pipeline_query);
void SetContinousViewState(RangeVar *name, ContinuousViewState *cv_state, Relation pipeline_query);
void GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryStringOrNull(const char *cvname, bool selectonly);
char *GetQueryString(const char *cvname, bool selectonly);
bool IsAContinuousView(RangeVar *name);
void MarkAllContinuousViewsAsInactive(void);

#endif
