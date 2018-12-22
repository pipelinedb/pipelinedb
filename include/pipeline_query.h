/*-------------------------------------------------------------------------
 *
 * pipeline_query.h
 * 		Interface for pipelinedb.cont_query catalog
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERY_H
#define PIPELINE_QUERY_H

#include "postgres.h"

#include "access/htup_details.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_node.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"

typedef struct FormData_pipeline_query
{
	int32		id;
	char 		type;
	Oid			relid;
	Oid     defrelid;
	bool 		active;
	Oid 		osrelid;
	Oid 		streamrelid;

	/* valid for views only */
	Oid			matrelid;
	Oid  	  seqrelid;
	Oid  	  pkidxid;
	Oid 	  lookupidxid;
	int16	step_factor;
	int32		ttl;
	int16		ttl_attno;

	/* valid for transforms only */
	int16		tgnargs;

#ifdef CATALOG_VARLEN
	bytea       tgargs;
#endif
} FormData_pipeline_query;

typedef FormData_pipeline_query *Form_pipeline_query;

#define Natts_pipeline_query             16
#define Anum_pipeline_query_id           1
#define Anum_pipeline_query_type         2
#define Anum_pipeline_query_relid  	    3
#define Anum_pipeline_query_defrelid  	  4
#define Anum_pipeline_query_active       5
#define Anum_pipeline_query_osrelid      6
#define Anum_pipeline_query_streamrelid  7
#define Anum_pipeline_query_matrelid     8
#define Anum_pipeline_query_seqrelid     9
#define Anum_pipeline_query_pkidxid      10
#define Anum_pipeline_query_lookupidxid  11
#define Anum_pipeline_query_step_factor  12
#define Anum_pipeline_query_ttl  		    13
#define Anum_pipeline_query_ttl_attno    14
#define Anum_pipeline_query_tgnargs	    15
#define Anum_pipeline_query_tgargs       16

#define PIPELINE_QUERY_VIEW 		    'v'
#define PIPELINE_QUERY_TRANSFORM 	't'

#define OPTION_FILLFACTOR "fillfactor"
#define OPTION_SLIDING_WINDOW "sw"
#define OPTION_SLIDING_WINDOW_COLUMN "sw_column"
#define OPTION_PK "pk"
#define OPTION_STEP_FACTOR "step_factor"
#define OPTION_TTL "ttl"
#define OPTION_TTL_COLUMN "ttl_column"
#define OPTION_TTL_ATTNO "ttl_attno"

#define OPTION_CV "cv"
#define OPTION_TRANSFORM "transform"
#define OPTION_STREAM "stream"
#define OPTION_MATREL "matrel"
#define OPTION_SEQREL "seqrel"
#define OPTION_OSREL "osrel"
#define OPTION_OVERLAY "overlay"
#define OPTION_PKINDEX "pkindex"
#define OPTION_LOOKUPINDEX "lookupindex"

#define OPTION_ACTION "action"
#define OPTION_OUTPUTFUNC "outputfunc"
#define OPTION_MATRELID "matrelid"
#define OPTION_OSRELID "osrelid"
#define OPTION_OSRELTYPE "osreltype"
#define OPTION_OSRELATYPE "osrelatype"
#define OPTION_TABLESPACE "tablespace"
#define OPTION_TGFNID "tgfnid"
#define OPTION_TGFN "tgfn"
#define OPTION_TGNARGS "tgnargs"
#define OPTION_TGARGS "tgargs"

#define OPTION_VIEWRELID "viewrelid"
#define OPTION_VIEWTYPE "viewtype"
#define OPTION_VIEWATYPE "viewatype"
#define OPTION_MATRELID "matrelid"
#define OPTION_MATRELTYPE "matreltype"
#define OPTION_MATRELATYPE "matrelatype"
#define OPTION_MATRELTOASTRELID "matreltoastrelid"
#define OPTION_MATRELTOASTTYPE "matreltoasttype"
#define OPTION_MATRELTOASTINDID "matreltoastindid"

#define OPTION_SEQRELID "seqrelid"
#define OPTION_SEQRELTYPE "seqreltype"

#define OPTION_PKINDID "pkindid"
#define OPTION_LOOKUPINDID "lookupindid"

#define ACTION_TRANSFORM "transform"
#define ACTION_MATERIALIZE "materialize"
#define ACTION_DUMPED "dumped"

typedef Relation PipelineDDLLock;

typedef enum ContQueryType
{
	CONT_VIEW,
	CONT_TRANSFORM
} ContQueryType;

typedef struct ContQuery
{
	Oid id;
	RangeVar *name;
	bool active;
	ContQueryType type;

	/* meta */
	Oid relid;
	Oid defrelid;
	char *sql;
	Oid matrelid;
	Oid osrelid;
	Oid streamrelid;
	Query *cvdef;

	/* for view */
	RangeVar *matrel;
	Oid seqrelid;
	Oid pkidxid;
	Oid lookupidxid;
	int sw_step_factor;
	uint64 sw_step_ms;
	uint64 sw_interval_ms;
	bool is_sw;
	AttrNumber ttl_attno;
	AttrNumber sw_attno;
	int ttl;

	/* for transform */
	Oid tgfn;
	int tgnargs;
	char **tgargs;
} ContQuery;

typedef enum ContQueryAction
{
	NONE,
	DUMPED,
	TRANSFORM,
	MATERIALIZE
} ContQueryAction;

extern Oid PipelineQueryRelationOid;

extern int continuous_view_fillfactor;

extern PipelineDDLLock AcquirePipelineDDLLock(void);
extern void ReleasePipelineDDLLock(PipelineDDLLock lock);

extern void SyncPipelineQuery(void);
extern void SyncContView(RangeVar *name);
extern void SyncContQueryDefRel(Oid cqrelid);
extern void SyncAllContQueryDefRels(void);
extern void SyncStreamReaderDefRels(Oid streamrelid);
extern void SyncContQuerySchema(Oid cqrelid, char *schema);

extern Oid DefineContView(Relation pipeline_query, Oid relid, Oid streamrelid, Oid matrel, Oid seqrel, int ttl, AttrNumber ttl_attno, double step_factor, Oid *pq_id);
extern Oid DefineContTransform(Oid relid, Oid defrelid, Oid streamrelid, Oid typoid, Oid osrelid, List **optionsp, Oid *ptgfnid);
extern void UpdateContViewRelIds(Relation pipeline_query, Oid cvid, Oid cvrelid, Oid defrelid, Oid osrelid, List *options);
extern void UpdateContViewIndexIds(Relation pipeline_query, Oid cvid, Oid pkindid, Oid lookupindid, Oid seqrelid);

extern void ExecCreateContViewStmt(RangeVar *view, Node *sel, List *options, const char *querystring);
extern void ExecCreateContTransformStmt(RangeVar *transform, Node *stmt, List *options, const char *querystring);

extern void StorePipelineQueryReloptions(Oid relid, List *options);

extern Query *GetContQueryDef(Oid defrelid);

extern HeapTuple GetPipelineQueryTuple(RangeVar *name);
extern bool RangeVarIsContView(RangeVar *name);
extern bool RangeVarIsContTransform(RangeVar *name);
extern bool RangeVarIsContQuery(RangeVar *name);

extern bool RangeVarIsPipelineObject(RangeVar *name);
extern bool RangeVarIsMatRel(RangeVar *name, RangeVar **cvname);
extern bool RangeVarIsSWContView(RangeVar *name);
extern bool RangeVarIsTTLContView(RangeVar *name);

extern Oid RangeVarGetContQueryId(RangeVar *name);

extern RangeVar *RangeVarGetMatRelName(RangeVar *cv);

RangeVar *RelidGetRangeVar(Oid relid);
extern bool RelidIsMatRel(Oid relid, Oid *id);
extern bool RelidIsContView(Oid relid);
extern bool RelidIsContTransform(Oid relid);
extern bool RelidIsContQuery(Oid relid);
extern bool RelidIsDefRel(Oid relid);
extern bool RelidIsContViewIndex(Oid relid);
extern bool RelidIsContViewSequence(Oid relid);
extern bool RelidIsContQueryInternalRelation(Oid relid);
extern ContQuery * RelidGetContQuery(Oid relid);


extern RangeVar *GetSWContViewRelName(List *nodes);

extern Bitmapset *GetContQueryIds(void);
extern Bitmapset *GetContViewIds(void);
extern Bitmapset *GetContTransformIds(void);

extern bool ContQuerySetActive(Oid id, bool active);

extern ContQuery *GetContQueryForId(Oid id);
extern ContQuery *RangeVarGetContView(RangeVar *cv_name);
extern ContQuery *RangeVarGetContQuery(RangeVar *cq_name);
extern ContQuery *GetContViewForId(Oid id);
extern ContQuery *GetContTransformForId(Oid id);

extern Relation OpenPipelineQuery(LOCKMODE mode);
extern void ClosePipelineQuery(Relation rel, LOCKMODE mode);

extern void FinalizeOverlayStmtAggregates(SelectStmt *overlay, Query *worker_query);
extern void AnalyzeCreateViewForTransform(ViewStmt *stmt);
extern void AnalyzeDumped(ViewStmt *stmt);
extern void PostParseAnalyzeHook(ParseState *pstate, Query *query);

extern void RangeVarGetTTLInfo(RangeVar *cvname, char **ttl_col, int *ttl);

extern Oid GetTriggerFnOid(Oid defrelid);

#endif
