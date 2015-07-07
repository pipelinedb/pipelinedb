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

#include "datatype/timestamp.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "utils/relcache.h"

typedef struct {
	Oid id;
	NameData name;
	NameData matrelname;
	char *query;
	bool needs_xact;
	int32 hash;
} ContinuousView;

void CreateContinuousView(RangeVar *name, const char *query_string, RangeVar *matrelname, bool gc, bool long_xact);
char *GetQueryString(char *cvname);
bool IsAContinuousView(RangeVar *name);
bool IsAMatRel(RangeVar *name, RangeVar **cvname);
bool GetGCFlag(RangeVar *name);
char *GetMatRelationName(char *cvname);
char *GetCVNameForMatRelationName(char *matrelname);
Query *GetContinuousQuery(RangeVar *rv);

extern ContinuousView *GetContinuousView(Oid id);
extern Bitmapset *GetAllContinuousViewIds(void);

#endif
