/* Copyright (c) 2013-2015 PipelineDB */
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
	int32 id;
	char state;
	int32 batchsize;
	int32 maxwaitms;
	int32 emptysleepms;
	int16 parallelism;
	NameData matrelname;
	CQProcessType ptype;
} ContinuousViewState;

List *GetAllContinuousViewNames(void);
void RegisterContinuousView(RangeVar *name, const char *query_string, RangeVar *matrelname, bool gc);
bool MarkContinuousViewAsActive(RangeVar *name, Relation pipeline_query);
bool MarkContinuousViewAsInactive(RangeVar *name, Relation pipeline_query);
void SetContinousViewState(RangeVar *name, ContinuousViewState *cv_state, Relation pipeline_query);
void GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state);
bool IsContinuousViewActive(RangeVar *name);
char *GetQueryStringOrNull(char *cvname, bool selectonly);
char *GetQueryString(char *cvname, bool selectonly);
bool IsAContinuousView(RangeVar *name);
bool IsAMatRel(RangeVar *name, RangeVar **cvname);
bool GetGCFlag(RangeVar *name);
void MarkAllContinuousViewsAsInactive(void);
char *GetMatRelationName(char *cvname);
char *GetCVNameForMatRelationName(char *matrelname);
Query *GetContinuousQuery(RangeVar *rv);
void InvalidateContinuousQuery(RangeVar *rv);

#endif
