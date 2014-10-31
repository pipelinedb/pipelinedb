/*-------------------------------------------------------------------------
 *
 * cqwindow.h
 *	  Interface for analyzing window queries
 *
 *
 * src/include/catalog/pipeline/cqwindow.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQWINDOW_H
#define CQWINDOW_H

#include "parser/parse_node.h"

void ValidateSlidingWindowExpr(SelectStmt *stmt, CQAnalyzeContext *context);

ColumnRef *GetColumnRefInSlidingWindowExpr(SelectStmt *stmt);
bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
bool IsSlidingWindowContinuousView(RangeVar *cvname);

bool DoesViewAggregate(SelectStmt *stmt, CQAnalyzeContext *context);

void AddProjectionsAndGroupBysForWindows(SelectStmt *stmt, SelectStmt *viewselect, bool hasAggOrGroupBy, CQAnalyzeContext *context);
void TransformAggNodeForCQView(SelectStmt *viewselect, Node *agg, ResTarget *aggres, bool doesViewAggregate);
void FixAggArgForCQView(SelectStmt *viewselect, SelectStmt *workerselect, RangeVar *matrelation);
DeleteStmt *GetDeleteStmtForGC(char *cvname, SelectStmt *stmt);

#endif
