/*-------------------------------------------------------------------------
 *
 * tuplestore_scan.h
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/tuplestore_scan.h
 *
 */
#ifndef TUPLESTORE_SCAN_H
#define TUPLESTORE_SCAN_H

#include "postgres.h"

#include "optimizer/planner.h"

extern Node *CreateTuplestoreScanPath(RelOptInfo *parent);

#endif
