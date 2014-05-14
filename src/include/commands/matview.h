/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern void ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   ParamListInfo params, char *completionTag);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid);
#ifdef PGXC
extern void pgxc_send_matview_data(RangeVar *matview_rv, const char *query_string);
extern void pgxc_fill_matview_by_copy(DestReceiver *mv_dest, bool skipdata,
										int operation, TupleDesc tupdesc);
#endif /* PGXC */

#endif   /* MATVIEW_H */
