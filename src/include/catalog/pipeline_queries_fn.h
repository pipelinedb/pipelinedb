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

typedef struct ContinuousViewState
{
	int32	id;
	char	state;
	int64 	batchsize;
	int32 	maxwaitms;
	int32	emptysleepms;
	int16	parallelism;
	CQProcessType ptype;
} ContinuousViewState;

void RegisterContinuousView(RangeVar *name, const char *query_string);
void DeregisterContinuousView(RangeVar *name);
void ActivateContinuousView(ActivateContinuousViewStmt *stmt);
bool MarkContinuousViewAsActive(RangeVar *name);
bool MarkContinuousViewAsInactive(RangeVar *name);
void SetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
void GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryString(const char *cvname, bool selectonly);

#endif
