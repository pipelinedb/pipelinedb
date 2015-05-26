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
	RangeVar *matrel;
	char *query;
	bool needs_xact;
	int32 hash;
} ContinuousView;

Oid DefineContinuousView(RangeVar *name, const char *query_string, RangeVar *matrelname, bool gc, bool long_xact);
HeapTuple GetPipelineQueryTuple(RangeVar *name);
char *GetQueryString(RangeVar *name);
bool IsAContinuousView(RangeVar *name);
bool IsAMatRel(RangeVar *name, RangeVar **cvname);
bool GetGCFlag(RangeVar *name);
RangeVar *GetMatRelationName(RangeVar *cv);
RangeVar *GetCVNameFromMatRelName(RangeVar *matrel);
Query *GetContinuousQuery(RangeVar *rv);
void RemoveContinuousViewById(Oid oid);

extern ContinuousView *GetContinuousView(Oid id);
extern Bitmapset *GetAllContinuousViewIds(void);

#endif
