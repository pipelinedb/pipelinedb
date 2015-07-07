/* Copyright (c) 2013-2015 PipelineDB */
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
void ValidateWindows(SelectStmt *stmt, CQAnalyzeContext *context);

bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
bool IsSlidingWindowContinuousView(RangeVar *cvname);
ColumnRef *GetColumnRefInSlidingWindowExpr(SelectStmt *stmt);
Node *GetSlidingWindowExpr(SelectStmt *stmt, CQAnalyzeContext *context);

bool DoesViewAggregate(SelectStmt *stmt, CQAnalyzeContext *context);

ResTarget *AddProjectionsAndGroupBysForWindows(SelectStmt *workerstmt, SelectStmt *viewstmt,
		bool doesViewAggregate, CQAnalyzeContext *context, AttrNumber *timeAttr);
void TransformAggNodeForCQView(SelectStmt *viewselect, Node *agg, ResTarget *aggres, bool doesViewAggregate);
Node* GetCQVacuumExpr(char *cvname);

#endif
