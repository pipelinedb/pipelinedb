/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqanalyze.h
 *	  Interface for analyzing continuous view statements, mainly to support
 *	  schema inference
 *
 *
 * src/include/catalog/pipeline/cqanalyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQANALYZE_H
#define CQANALYZE_H

#include "parser/parse_node.h"

typedef struct CQAnalyzeContext
{
	ParseState *pstate;
	int colNum;
	List *colNames;
	List *types;
	List *cols;
	List *streams;
	List *tables;
	List *targets;
	List *funcCalls;
	Node *swExpr;
	Node *stepNode;
	List *windows;
	int location;
	char *stepSize;
} CQAnalyzeContext;

#define USER_COMBINE "combine"

TupleDesc InferStreamScanTupleDescriptor(ParseState *pstate, RangeTblEntry *rte);
void AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **stmt);
RangeTblEntry *TransformStreamEntry(ParseState *pstate, StreamDesc *stream);

void RewriteStreamingAggs(SelectStmt *stmt);
SelectStmt *GetSelectStmtForCQWorker(SelectStmt *stmt, SelectStmt **viewstmtptr);
SelectStmt *GetSelectStmtForCQCombiner(SelectStmt *stmt);

AttrNumber GetHiddenAttrNumber(char *attname, TupleDesc matrel);
Oid GetCombineStateColumnType(Expr *expr);
bool IsUserCombine(NameData proname);
Node *ParseCombineFuncCall(ParseState *pstate, List *args, List *order, Expr *filter, WindowDef *over, int location);

void InitializeCQAnalyzeContext(SelectStmt *stmt, ParseState *pstate, CQAnalyzeContext *context);
char *GetUniqueInternalColname(CQAnalyzeContext *context);
bool FindColumnRefsWithTypeCasts(Node *node, CQAnalyzeContext *context);
bool ContainsColumnRef(Node *node, ColumnRef *cref);
ColumnRef *GetColumnRef(Node *node);
ResTarget *IsColumnRefInTargetList(SelectStmt *stmt, Node *node);
bool IsAColumnRef(Node *node);
bool AreColumnRefsEqual(Node *cr1, Node *cr2);
Node *HoistNode(SelectStmt *stmt, Node *node, CQAnalyzeContext *context);
bool CollectFuncs(Node *node, CQAnalyzeContext *context);
bool CollectAggrefs(Node *node, CQAnalyzeContext *context);
bool CollectAggFuncs(Node *node, CQAnalyzeContext *context);
ResTarget *CreateResTargetForNode(Node *node);
ResTarget *CreateUniqueResTargetForNode(Node *node, CQAnalyzeContext *context);
ColumnRef *CreateColumnRefFromResTarget(ResTarget *res);
bool HasAggOrGroupBy(SelectStmt *stmt);
bool AddStreams(Node *node, CQAnalyzeContext *context);
List *pipeline_rewrite(List *raw_parsetree_list);
Query *RewriteContinuousViewSelect(Query *query, Query *rule, Relation cv);

#endif
