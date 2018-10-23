/*-------------------------------------------------------------------------
 *
 * commands.h
 * 		Interface for processing commands
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMANDS_H
#define COMMANDS_H

#include "tcop/utility.h"

extern void InstallCommandHooks(void);

extern void PipelineProcessUtility(PlannedStmt *pstmt, const char *queryString, ProcessUtilityContext context,
		ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest, char *completionTag);

#endif
