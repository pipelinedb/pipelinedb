/*-------------------------------------------------------------------------
 *
 * json.h
 *	  Declarations for JSON data type support.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PIPELINE_JSON_H
#define PIPELINE_JSON_H

#include "lib/stringinfo.h"

/* functions in json.c */
extern void escape_json(StringInfo buf, const char *str);

#endif							/* PIPELINE_JSON_H */
