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

TupleDesc inferStreamScanTupleDescriptor(ParseState *pstate, RangeTblEntry *rte);
void analyzeContinuousSelectStmt(ParseState *pstate, SelectStmt **stmt);
RangeTblEntry *transformStreamEntry(ParseState *pstate, StreamDesc *stream);

DeleteStmt *getGarbageTupleDeleteStmt(char *cv_name, SelectStmt *stmt);
List *getResTargetsForGarbageCollection(SelectStmt *stmt);

#endif
