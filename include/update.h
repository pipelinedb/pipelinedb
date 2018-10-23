/*-------------------------------------------------------------------------
 *
 * update.h
 *	  Interface for anonymous update checks
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_UPDATE_H
#define PIPELINE_UPDATE_H

#include "utils/hsearch.h"

/* guc */
extern bool anonymous_update_checks;

typedef char *(*GetInstallationIdFunc) (void);
extern GetInstallationIdFunc GetInstallationIdHook;

typedef void (*VerifyConfigurationFunc) (void);
extern VerifyConfigurationFunc VerifyConfigurationHook;

extern void UpdateCheck(bool startup);
extern int AnonymousPost(char *path, char *payload, char *resp_buf, int resp_len);

#endif
