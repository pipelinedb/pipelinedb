/*-------------------------------------------------------------------------
 *
 * merge.h
 *
 *	  Interface for incremental updates of continuous views
 *
 * src/include/pipeline/merge.h
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * merge.c
 *
 *	  Support for incremental updates of continuous views
 *
 * src/backend/pipeline/merge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "commands/prepare.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/tstoreReceiver.h"
#include "executor/tupletableReceiver.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#ifdef PGXC
#include "parser/parse_type.h"
#endif /* PGXC */
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tcop/fastpath.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "mb/pg_wchar.h"

#ifdef PGXC
#include "storage/procarray.h"
#include "pgxc/pgxc.h"
#include "access/gtm.h"
/* PGXC_COORD */
#include "pgxc/execRemote.h"
#include "pgxc/barrier.h"
#include "optimizer/pgxcplan.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pgxcnode.h"
#include "commands/copy.h"
/* PGXC_DATANODE */
#include "access/transam.h"

#endif

/*
 * exec_merge_retrieval
 *
 * Gets the plan for retrieving all of the existing tuples that
 * this merge request will merge with
 */
extern void exec_merge_retrieval(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber merge_attr,
		List *group_clause, TupleHashTable merge_targets);
extern void sync_merge_results(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, AttrNumber merge_attr, TupleHashTable merge_targets);

extern List *get_merge_columns(Query *query);

extern CachedPlan *get_merge_plan(char *cvname, CachedPlanSource **src);

extern void DoRemoteMerge(RemoteMergeState mergeState);
