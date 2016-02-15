/*-------------------------------------------------------------------------
 *
 * update.h
 *	  Interface for anonymous update checks
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * src/include/pipeline/update.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_UPDATE_H
#define PIPELINE_UPDATE_H

/* guc */
extern bool anonymous_update_checks;

typedef char *(*GetInstallationIdFunc) (void);
extern GetInstallationIdFunc GetInstallationIdHook;

extern void UpdateCheck(HTAB *all_dbs, bool startup);

#endif
