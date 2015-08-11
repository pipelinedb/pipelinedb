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

extern void ExecAdhocQuery(SelectStmt* stmt, const char* s);

#endif
