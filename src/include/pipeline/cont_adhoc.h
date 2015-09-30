/*-------------------------------------------------------------------------
 *
 * cont_adhoc.h
 *
 * Functions for adhoc queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/cont_adhoc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_ADHOC_H
#define CONT_ADHOC_H

#include "nodes/parsenodes.h"

/* This module is for performing adhoc in-memory continuous queries. Rows are not 
 * persisted in a matrel, but are streamed down to the client in adhoc format */

extern void ExecAdhocQuery(SelectStmt *stmt, const char *s);

/* returns true iff this parsetree is an adhoc query */
extern bool IsAdhocQuery(Node *node);

#endif
