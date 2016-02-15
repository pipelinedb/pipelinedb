/*-------------------------------------------------------------------------
 *
 * adhoc_options.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADHOC_OPTIONS_H
#define ADHOC_OPTIONS_H

#include "libpq-fe.h"

typedef struct adhoc_opts
{
	char	   *dbname;
	char	   *host;
	char	   *port;
	char	   *username;
	char	   *action_string;
	PGconn 	   *db;
} adhoc_opts;

extern int handle_options(int argc, char** argv, adhoc_opts *options);

#endif
