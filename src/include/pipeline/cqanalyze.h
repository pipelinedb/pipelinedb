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

TupleDesc InferStreamScanTupleDescriptor(ParseState *pstate, RangeTblEntry *rte);
void AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **stmt);
RangeTblEntry *TransformStreamEntry(ParseState *pstate, StreamDesc *stream);

Node *GetSlidingWindowMatchExpr(SelectStmt *stmt, ParseState *pstate);

SelectStmt *GetSelectStmtForCQWorker(SelectStmt *stmt);
SelectStmt *GetSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel);

bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
bool IsSlidingWindowContinuousView(RangeVar *cvname);

Oid GetCombineStateColumnType(TargetEntry *te);

#endif
