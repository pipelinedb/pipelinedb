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
	Node *matchExpr;
	int location;
	char *stepSize;
} CQAnalyzeContext;

TupleDesc InferStreamScanTupleDescriptor(ParseState *pstate, RangeTblEntry *rte);
void AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **stmt);
RangeTblEntry *TransformStreamEntry(ParseState *pstate, StreamDesc *stream);

SelectStmt *GetSelectStmtForCQWorker(SelectStmt *stmt);
SelectStmt *GetSelectStmtForCQCombiner(SelectStmt *stmt);
SelectStmt *GetSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel);

Oid GetCombineStateColumnType(TargetEntry *te);

void InitializeCQAnalyzeContext(SelectStmt *stmt, ParseState *pstate, CQAnalyzeContext *context);
char *GetUniqueInternalColname(CQAnalyzeContext *context);
bool FindColumnRefsWithTypeCasts(Node *node, CQAnalyzeContext *context);
bool AreColumnRefsEqual(ColumnRef *cr1, ColumnRef *cr2);
bool ContainsColumnRef(Node *node, ColumnRef *cref);
bool IsColumnRef(Node *node);
bool IsColumnRefInTargetList(SelectStmt *stmt, Node *node);

#endif
