/*-------------------------------------------------------------------------
 *
 * pipeline_query_fn.h
 *	 prototypes for functions in catalog/pipeline_query.c
 *
 * Copyright (c) 2013-2015, PipelineDB
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

typedef struct ContinuousView
{
	Oid id;
	Oid namespace;
	NameData name;
	RangeVar *matrel;
	char *query;
	int sw_step_factor;
	Oid oid; /* OID in pipeline_query table */
} ContinuousView;

extern Oid DefineContinuousView(RangeVar *name, Query *query, Oid matrel, bool gc, bool adhoc, Oid *pq_id);
extern HeapTuple GetPipelineQueryTuple(RangeVar *name);
extern SelectStmt *GetContSelectStmt(RangeVar *rv);
extern bool IsAContinuousView(RangeVar *name);
extern bool ContainsSlidingWindowContinuousView(List *nodes);
extern bool IsAMatRel(RangeVar *name, RangeVar **cvname);
extern bool RelIdIsAMatRel(Oid relid);
extern bool GetGCFlag(RangeVar *name);
extern RangeVar *GetMatRelationName(RangeVar *cv);
extern RangeVar *GetCVNameFromMatRelName(RangeVar *matrel);
extern void RemoveContinuousViewById(Oid oid);

extern ContinuousView *GetContinuousView(Oid id);
extern Bitmapset *GetAllContinuousViewIds(void);
extern Bitmapset *GetAdhocContinuousViewIds(void);

#endif
