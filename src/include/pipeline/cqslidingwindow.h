/*-------------------------------------------------------------------------
 *
 * cqslidingwindow.h
 *	  Interface for analyzing sliding window queries
 *
 *
 * src/include/catalog/pipeline/cqslidingwindow.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQSLIDINGWINDOWE_H
#define CQSLIDINGWINDOWE_H

#include "parser/parse_node.h"

void ValidateSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate);

bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
bool IsSlidingWindowContinuousView(RangeVar *cvname);

SelectStmt *AddProjectionAndAddGroupByForSlidingWindow(SelectStmt *stmt, SelectStmt *viewselect, bool hasAggOrGroupBy, CQAnalyzeContext *context);
void TransformAggNodeForCQView(SelectStmt *viewselect, Node *agg, ResTarget *aggres, bool hasAggOrGroupBy);
void FixAggArgForCQView(SelectStmt *viewselect, SelectStmt *workerselect, RangeVar *matrelation);
DeleteStmt *GetDeleteStmtForGC(char *cvname, SelectStmt *stmt);

#endif
