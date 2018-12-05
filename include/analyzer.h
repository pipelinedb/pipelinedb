/*-------------------------------------------------------------------------
 *
 * analyzer.h
 *	  Interface for for parsing and analyzing continuous queries
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINEDB_ANALYZE_H
#define PIPELINEDB_ANALYZE_H

#include "postgres.h"

#include "datatype/timestamp.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "pipeline_query.h"
#include "scheduler.h"

extern double sliding_window_step_factor;

#define OUTPUT_OF "output_of"
#define CLOCK_TIMESTAMP "clock_timestamp"
#define ARRIVAL_TIMESTAMP "arrival_timestamp"
#define MATREL_COMBINE "combine"
#define SW_COMBINE "sw_combine"
#define MATREL_FINALIZE "finalize"

#define COMBINE_AGG_DUMMY "combine_dummy"
#define SW_COMBINE_AGG_DUMMY "sw_combine_dummy"

typedef struct ContAnalyzeContext
{
	ParseState *pstate;
	int colno;
	List *colnames;
	List *types;
	List *cols;
	List *targets;
	List *rels;
	List *streams;
	List *funcs;
	List *windows;
	Node *expr;
	int location;
	bool is_sw;
	bool stream_only;
	bool view_combines;
	char *hoisted_name;
	ContQueryProcType proc_type;
} ContAnalyzeContext;

typedef Oid (*GetCombineTargetFunc) (RangeTblEntry *rte);
extern GetCombineTargetFunc GetCombineTargetHook;

extern ContAnalyzeContext *MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select, ContQueryProcType type);

extern void PipelineContextSetIsDDL(void);
extern void PipelineContextSetIsDefRel(bool defrel);
extern void PipelineContextSetStepFactor(double sf);
extern double PipelineContextGetStepFactor(void);

extern bool PipelineContextIsDefRel(void);
extern bool PipelineContextIsDDL(void);

extern void PipelineContextSetCombineTable(void);
extern bool PipelineContextIsCombineTable(void);

extern void PipelineContextSetContPlan(bool allowed);
extern bool PipelineContextIsContPlan(void);

extern void PipelineContextSetCombinerLookup(void);
extern bool PipelineContextIsCombinerLookup(void);
extern void ClearPipelineContext(void);

extern bool QueryIsContinuous(Query *query);
double QueryGetSWStepFactor(Query *query);
extern void QuerySetSWStepFactor(Query *query, double sf);
extern Oid QueryGetContQueryId(Query *query);
extern void QuerySetContQueryId(Query *query, Oid id);

extern bool QueryHasStream(Node *node);

extern bool collect_rels_and_streams(Node *node, ContAnalyzeContext *context);
extern bool collect_cols(Node *node, ContAnalyzeContext *context);

extern void ValidateSubselect(Node *subquery, char *objdesc);
extern void ValidateParsedContQuery(RangeVar *name, Node *node, const char *sql);
extern void ValidateContQuery(Query *query);

extern AttrNumber FindSWTimeColumnAttrNo(SelectStmt *viewselect, Oid matrel, int *ttl, char **ttl_column);
extern AttrNumber FindTTLColumnAttrNo(char *colname, Oid matrelid);
extern Node *GetSWExpr(RangeVar *rv);
extern Node *GetTTLExpiredExpr(RangeVar *cv);

extern ColumnRef *GetSWTimeColumn(RangeVar *rv);
extern Interval *GetSWInterval(RangeVar *rv);
extern ColumnRef *GetWindowTimeColumn(RangeVar *cv);

extern DefElem *GetContQueryOption(List *options, char *name);
extern bool GetOptionAsString(List *options, char *option, char **result);
extern bool GetOptionAsInteger(List *options, char *option, int *result);
extern bool GetOptionAsDouble(List *options, char *option, double *result);
extern void ApplySlidingWindow(SelectStmt *stmt, DefElem *max_age, DefElem *column, int *ttl);
extern int IntervalToEpoch(Interval *i);
extern List *ApplyStorageOptions(SelectStmt *select, List *options, bool *has_sw, int *ttl, char **ttl_column);

extern Query *GetContViewQuery(RangeVar *rv);

extern Query *RangeVarGetContWorkerQuery(RangeVar *rv);
extern Query *RangeVarGetContCombinerQuery(RangeVar *rv);

extern void RewriteFromClause(SelectStmt *stmt);
extern SelectStmt *TransformSelectStmtForContProcess(RangeVar *mat_relation, SelectStmt *stmt, SelectStmt **viewptr, double sw_step_factor, ContQueryProcType proc_type);
extern void FinalizeOverlayAggrefs(Query *query);

extern ContQueryAction GetContQueryAction(ViewStmt *stmt);

RawStmt *GetContWorkerSelectStmt(ContQuery* view, SelectStmt** viewptr);
RawStmt *GetContViewOverlayStmt(ContQuery *view);
extern Query *GetContWorkerQuery(ContQuery *view);

extern Oid AggGetInitialArgType(FunctionCallInfo fcinfo);

extern bool OidIsCombineAgg(Oid oid);
extern void RewriteCombineAggs(Query *q);

extern void InstallAnalyzerHooks(void);

#endif
