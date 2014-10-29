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
	Node *stepColumn;
	int location;
	char *stepSize;
} CQAnalyzeContext;

TupleDesc InferStreamScanTupleDescriptor(ParseState *pstate, RangeTblEntry *rte);
void AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **stmt);
RangeTblEntry *TransformStreamEntry(ParseState *pstate, StreamDesc *stream);

SelectStmt *GetSelectStmtForCQWorker(SelectStmt *stmt, SelectStmt **viewselect);
SelectStmt *GetSelectStmtForCQCombiner(SelectStmt *stmt);

Oid GetCombineStateColumnType(TargetEntry *te);

void InitializeCQAnalyzeContext(SelectStmt *stmt, ParseState *pstate, CQAnalyzeContext *context);
char *GetUniqueInternalColname(CQAnalyzeContext *context);
bool FindColumnRefsWithTypeCasts(Node *node, CQAnalyzeContext *context);
bool IsColumnRefInTargetList(SelectStmt *stmt, Node *node);
void ReplaceTargetListWithColumnRefs(SelectStmt *stmt, bool replaceAggs);
bool AreColumnRefsEqual(Node *cr1, Node *cr2);
bool CollectAggFuncs(Node *node, CQAnalyzeContext *context);
ResTarget *CreateResTargetForNode(Node *node);
ResTarget *CreateUniqueResTargetForNode(Node *node, CQAnalyzeContext *context);
ColumnRef *CreateColumnRefFromResTarget(ResTarget *res);
bool HasAggOrGroupBy(SelectStmt *stmt);

#endif
