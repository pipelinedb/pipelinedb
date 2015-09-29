/*-------------------------------------------------------------------------
 *
 * cont_analyze.h
 *	  Interface for for parsing and analyzing continuous queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline/cont_analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_ANALYZE_H
#define CONT_ANALYZE_H

#include "parser/parse_node.h"
#include "pipeline/cont_scheduler.h"

extern int sliding_window_step_factor;

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

#define MATREL_COMBINE "combine"
#define MATREL_FINALIZE "finalize"

#define OPTION_FILLFACTOR "fillfactor"
#define OPTION_MAX_AGE "max_age"
#define OPTION_STEP_FACTOR "step_factor"

#define SW_TIMESTAMP_REF 65100
#define IS_SW_TIMESTAMP_REF(var) (IsA((var), Var) && ((Var *) (var))->varno >= SW_TIMESTAMP_REF)

#define IsMatRelCombine(proname) (pg_strcasecmp(NameStr(proname), MATREL_COMBINE) == 0)
#define IsMatRelFinalize(proname) (pg_strcasecmp(NameStr(proname), MATREL_FINALIZE) == 0)

extern bool collect_rels_and_streams(Node *node, ContAnalyzeContext *context);
extern bool collect_types_and_cols(Node *node, ContAnalyzeContext *context);

extern ContAnalyzeContext *MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select, ContQueryProcType type);
extern void CreateInferredStreams(SelectStmt *stmt);
extern void MakeSelectsContinuous(SelectStmt *stmt);
extern void ValidateContQuery(RangeVar *name, Node *node, const char *sql);

extern void transformContSelectStmt(ParseState *pstate, SelectStmt *select);
extern List *transformContSelectTargetList(ParseState *pstate, List *tlist);
extern List *transformContViewOverlayTargetList(ParseState *pstate, List *tlist);
extern void transformCreateStreamStmt(CreateStreamStmt *stmt);
extern SelectStmt *TransformSelectStmtForContProcess(RangeVar *mat_relation, SelectStmt *stmt, SelectStmt **viewptr, ContQueryProcType type);

extern TupleDesc parserGetStreamDescr(Oid relid, ContAnalyzeContext *context);
extern Node *ParseCombineFuncCall(ParseState *pstate, List *args, List *order, Expr *filter, WindowDef *over, int location);
extern Node *ParseFinalizeFuncCall(ParseState *pstate, List *args, int location);
extern Query *RewriteContinuousViewSelect(Query *query, Query *rule, Relation cv, int rtindex);
extern List *pipeline_rewrite(List *raw_parsetree_list);

extern Query *GetContQuery(RangeVar *rv);
extern Query *GetContWorkerQuery(RangeVar *rv);
extern Query *GetContCombinerQuery(RangeVar *rv);

extern Node *GetSWExpr(RangeVar *rv);
extern ColumnRef *GetSWTimeColumn(RangeVar *rv);
extern ColumnRef *GetWindowTimeColumn(RangeVar *cv);
extern Node *CreateOuterSWTimeColumnRef(ParseState *pstate, ColumnRef *cref, Node *var);

extern DefElem *GetContinuousViewOption(List *options, char *name);
extern void ApplyMaxAge(SelectStmt *stmt, DefElem *max_age);
extern void ApplyStorageOptions(CreateContViewStmt *stmt);

/* Deparsing */
extern char *deparse_query_def(Query *query);
extern char *get_inv_streaming_agg(char *name, bool *is_distinct);

#endif
