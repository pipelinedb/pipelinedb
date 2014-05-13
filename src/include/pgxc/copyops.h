/*--------------------------------------------------------------------------
 *
 * copyops.h
 *		Routines for manipulation of remote COPY data
 *
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/include/pgxc/copyops.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COPYOPS_H
#define COPYOPS_H

#include "access/tupdesc.h"

/* Type of data delimiter used for data redistribution using remote COPY */
#define COPYOPS_DELIMITER	'\t'

extern char **CopyOps_RawDataToArrayField(TupleDesc tupdesc, char *message, int len);
extern char *CopyOps_BuildOneRowTo(TupleDesc tupdesc, Datum *values, bool *nulls, int *len);

#endif
