/*-------------------------------------------------------------------------
 *
 * cont_adhoc_mgr.h
 *
 * Adhoc Query process management
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/cont_adhoc_mgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_ADHOC_MGR_H
#define CONT_ADHOC_MGR_H

#include "pipeline/cont_scheduler.h"

extern void AdhocShmemInit(void);
extern ContQueryProc* AdhocMgrGetProc(void);
extern void AdhocMgrReleaseProc(ContQueryProc*);
extern void AdhocMgrPeriodicCleanup(void);

extern int* AdhocMgrGetActiveFlag(int cq_id);

#endif
