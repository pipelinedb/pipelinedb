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

Node *GetSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate);
void ValidateSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate);

SelectStmt *TransformSWSelectStmtForCQWorker(SelectStmt *stmt);
SelectStmt *TransformSWSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel);

bool IsSlidingWindowSelectStmt(SelectStmt *stmt);
bool IsSlidingWindowContinuousView(RangeVar *cvname);

DeleteStmt *GetDeleteStmtForGC(char *cvname, SelectStmt *stmt);

#endif
