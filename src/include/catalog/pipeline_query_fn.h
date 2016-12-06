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

#include "access/htup.h"
#include "datatype/timestamp.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

typedef enum ContQueryType
{
	CONT_VIEW,
	CONT_TRANSFORM
} ContQueryType;

typedef struct ContQuery
{
	Oid id;
	RangeVar *name;
	Oid oid; /* OID in pipeline_query table */
	bool active;
	ContQueryType type;

	/* meta */
	Oid relid;
	char *sql;
	Oid matrelid;
	Oid osrelid;

	/* for view */
	RangeVar *matrel;
	Oid seqrelid;
	Oid pkidxid;
	Oid lookupidxid;
	int sw_step_factor;
	int sw_step_ms;
	uint64 sw_interval_ms;
	bool is_sw;
	Oid sw_attno;

	/* for transform */
	Oid tgfn;
	int tgnargs;
	char **tgargs;
} ContQuery;

extern HeapTuple GetPipelineQueryTuple(RangeVar *name);
extern void RemovePipelineQueryById(Oid oid);

extern Oid DefineContinuousView(Oid relid, Query *query, Oid matrel, Oid seqrel, int ttl, AttrNumber ttl_attno, Oid *pq_id);
extern void UpdateContViewRelIds(Oid cvid, Oid cvrelid, Oid osrelid);
extern void UpdateContViewIndexIds(Oid cvid, Oid pkindid, Oid lookupindid);
extern Oid DefineContinuousTransform(Oid relid, Query *query, Oid typoid, Oid osrelid, Oid fnoid, List *args);

extern Relation OpenCVRelFromMatRel(Relation matrel, LOCKMODE lockmode);
extern bool IsAContinuousView(RangeVar *name);
extern RangeVar *GetSWContinuousViewRangeVar(List *nodes);
extern bool IsAMatRel(RangeVar *name, RangeVar **cvname);
extern bool RelIdIsForMatRel(Oid relid, Oid *id);
extern bool IsSWContView(RangeVar *name);
extern bool IsTTLContView(RangeVar *name);
extern RangeVar *GetMatRelName(RangeVar *cv);
extern RangeVar *GetCVNameFromMatRelName(RangeVar *matrel);

extern Bitmapset *GetContinuousQueryIds(void);
extern Bitmapset *GetContinuousViewIds(void);
extern Bitmapset *GetContinuousTransformIds(void);

extern Oid GetContQueryId(RangeVar *name);

extern ContQuery *GetContQueryForId(Oid id);
extern ContQuery *GetContQueryForView(RangeVar *cv_name);
extern ContQuery *GetContQueryForViewId(Oid id);
extern ContQuery *GetContQueryForTransformId(Oid id);

extern bool ContQuerySetActive(Oid id, bool active);

extern void GetTTLInfo(RangeVar *cvname, char **ttl_col, int *ttl);

#endif
