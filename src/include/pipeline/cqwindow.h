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

bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
ColumnRef *GetColumnRefInSlidingWindowExpr(SelectStmt *stmt);
Node *GetSlidingWindowExpr(SelectStmt *stmt, CQAnalyzeContext *context);

bool DoesViewAggregate(SelectStmt *stmt, CQAnalyzeContext *context);

ResTarget *AddProjectionsAndGroupBysForWindows(SelectStmt *workerstmt, SelectStmt *viewstmt,
		bool doesViewAggregate, CQAnalyzeContext *context, AttrNumber *timeAttr);
void TransformAggNodeForCQView(SelectStmt *viewselect, Node *agg, ResTarget *aggres, bool doesViewAggregate);
Node* GetCQVacuumExpr(RangeVar *cvname);

#endif
