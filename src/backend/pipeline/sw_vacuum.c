/*-------------------------------------------------------------------------
 *
 * sw_vacuum.c
 *
 *   Support for vacuuming discarded tuples for sliding window
 *   continuous views.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/sw_vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_expr.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/sw_vacuum.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static Node *
get_sw_vacuum_expr(RangeVar *rv)
{
	Node *expr = GetSWExpr(rv);
	Assert(expr);
	return (Node *) make_notclause((Expr *) expr);
}


/*
 * DeleteSWExpiredTuples
 */
void
DeleteSWExpiredTuples(Oid relid)
{
	char *relname;
	char *namespace;
	RangeVar *matrel;
	RangeVar *cvname;
	DeleteStmt *stmt;
	CommandDest dest = DestNone;
	List *querytree_list;
	PlannedStmt *plan;
	Portal portal;
	DestReceiver *receiver;
	MemoryContext oldcxt;
	MemoryContext runctx;
	bool save_continuous_query_materialization_table_updatable = continuous_query_materialization_table_updatable;

	continuous_query_materialization_table_updatable = true;

	StartTransactionCommand();

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"DeleteSWExpiredTuplesContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(runctx);

	relname = get_rel_name(relid);

	if (!relname)
		goto end;

	namespace = get_namespace_name(get_rel_namespace(relid));
	matrel = makeRangeVar(namespace, relname, -1);

	cvname = GetCVNameFromMatRelName(matrel);

	if (!cvname)
		goto end;

	if (!GetGCFlag(cvname))
		goto end;

	/* Now we're certain relid if for a SW continuous view's matrel */

	/*
	 * TODO(usmanm): Use lock contention free strategy here. See https://news.ycombinator.com/item?id=9018129
	 */
	stmt = makeNode(DeleteStmt);
	stmt->relation = matrel;
	stmt->whereClause = get_sw_vacuum_expr(cvname);

	PushActiveSnapshot(GetTransactionSnapshot());

	querytree_list = pg_analyze_and_rewrite((Node *) stmt, "DELETE",
			NULL, 0);
	plan = pg_plan_query((Query *) linitial(querytree_list), 0, NULL);

	portal = CreatePortal("__sw_delete_expired__", true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
			NULL,
			"DELETE",
			"DELETE",
			list_make1(plan),
			NULL);

	receiver = CreateDestReceiver(dest);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());
	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	(*receiver->rDestroy) (receiver);
	PortalDrop(portal, false);

	PopActiveSnapshot();

end:
	continuous_query_materialization_table_updatable = save_continuous_query_materialization_table_updatable;

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(runctx);

	CommitTransactionCommand();
}

/*
 * NumSWVacuumTuples
 */
uint64_t
NumSWExpiredTuples(Oid relid)
{
	uint64_t count;
	char *relname = get_rel_name(relid);
	char *namespace = get_namespace_name(get_rel_namespace(relid));
	RangeVar *matrel = makeRangeVar(namespace, relname, -1);
	RangeVar *cvname;
	SelectStmt *stmt;
	CommandDest dest = DestTuplestore;
	Tuplestorestate *store = NULL;
	List *querytree_list;
	List *parsetree_list;
	PlannedStmt *plan;
	Portal portal;
	DestReceiver *receiver;
	TupleTableSlot *slot;
	QueryDesc *queryDesc;
	StringInfoData sql;
	Datum *datum;
	bool isnull;
	MemoryContext oldcontext;
	MemoryContext runctx;
	bool locked;

	if (!relname)
		return 0;

	locked = ConditionalLockRelationOid(PipelineQueryRelationId, AccessShareLock);
	if (!locked)
		return 0;

	cvname = GetCVNameFromMatRelName(matrel);
	UnlockRelationOid(PipelineQueryRelationId, AccessShareLock);

	if (!cvname)
		return 0;

	if (!GetGCFlag(cvname))
		return 0;

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"NumSWExpiredTuplesContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(runctx);

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT COUNT(*) FROM %s.%s", namespace, relname);

	parsetree_list = pg_parse_query(sql.data);
	Assert(parsetree_list->length == 1);
	resetStringInfo(&sql);

	stmt = (SelectStmt *) linitial(parsetree_list);
	stmt->whereClause = get_sw_vacuum_expr(cvname);

	PushActiveSnapshot(GetTransactionSnapshot());

	querytree_list = pg_analyze_and_rewrite((Node *) stmt, sql.data,
			NULL, 0);
	plan = pg_plan_query((Query *) linitial(querytree_list), 0, NULL);

	portal = CreatePortal("__sw_num_expired__", true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
			NULL,
			sql.data,
			"SELECT",
			list_make1(plan),
			NULL);

	store = tuplestore_begin_heap(true, true, work_mem);
	receiver = CreateDestReceiver(dest);
	SetTuplestoreDestReceiverParams(receiver, store, PortalGetHeapMemory(portal), true);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());
	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	queryDesc = PortalGetQueryDesc(portal);
	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc);
	tuplestore_gettupleslot(store, true, false, slot);

	if (TupIsNull(slot))
		count = 0;
	else
	{
		slot_getallattrs(slot);
		datum = (Datum *) heap_getattr(slot->tts_tuple, 1, slot->tts_tupleDescriptor, &isnull);
		count = DatumGetInt64(datum);
	}

	(*receiver->rDestroy) (receiver);
	tuplestore_end(store);
	PortalDrop(portal, false);

	PopActiveSnapshot();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(runctx);

	return count;
}
