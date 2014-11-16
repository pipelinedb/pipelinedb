/*-------------------------------------------------------------------------
 *
 * cqvacuum.c
 *
 *   Support for vacuuming materialization relations for sliding window
 *   continuous views.
 *
 * src/backend/pipeline/cqvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query_fn.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqvacuum.h"
#include "pipeline/cqwindow.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/*
 * NumCQVacuumTuples
 */
uint64_t
NumCQVacuumTuples(Oid relid)
{
	uint64_t count;
	char *relname = get_rel_name(relid);
	char *cvname;
	SelectStmt *stmt;
	ResTarget *res;
	FuncCall *fcall;
	CommandDest dest = DestTuplestore;
	Tuplestorestate *store = NULL;
	List *querytree_list;
	PlannedStmt *plan;
	Portal portal;
	DestReceiver *receiver;
	TupleTableSlot *slot;
	QueryDesc *queryDesc;
	MemoryContext oldcontext;
	MemoryContext runctx;

	cvname = GetCVNameForMatRelationName(relname);
	if (!cvname)
		return 0;

	if (!GetGCFlag(makeRangeVar(NULL, cvname, -1)))
		return 0;

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"CQAutoVacuumContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(runctx);

	fcall = makeNode(FuncCall);
	fcall->agg_star = true;
	fcall->funcname = list_make1(makeString("count"));
	fcall->location = -1;
	res = makeNode(ResTarget);
	res->val = (Node *) fcall;
	res->location = -1;
	stmt = makeNode(SelectStmt);
	stmt->fromClause = list_make1(makeRangeVar(NULL, relname, -1));
	stmt->whereClause = GetCQVacuumExpr(cvname);
	stmt->targetList = list_make1(res);

	PushActiveSnapshot(GetTransactionSnapshot());

	querytree_list = pg_analyze_and_rewrite((Node *) stmt, NULL,
			NULL, 0);
	plan = pg_plan_query((Query *) linitial(querytree_list), 0, NULL);

	portal = CreatePortal("__cq_auto_vacuum__", true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
			NULL,
			NULL,
			"SELECT",
			list_make1(plan),
			NULL);

	store = tuplestore_begin_heap(true, true, work_mem);
	receiver = CreateDestReceiver(dest);
	SetTuplestoreDestReceiverParams(receiver, store, PortalGetHeapMemory(portal), true);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	/*
	 * Run the portal to completion, and then drop it (and the receiver).
	 */
	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	queryDesc = PortalGetQueryDesc(portal);
	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc);
	foreach_tuple(slot, store)
	{
		Datum *datum;
		bool isnull;

		slot_getallattrs(slot);
		datum = (Datum *) heap_getattr(slot->tts_tuple, 1, slot->tts_tupleDescriptor, &isnull);
		count = DatumGetInt64(datum);
	}

	(*receiver->rDestroy) (receiver);
	tuplestore_end(store);
	PortalDrop(portal, false);

	PopActiveSnapshot();

	MemoryContextReset(runctx);
	MemoryContextSwitchTo(oldcontext);

	return count;
}

/*
 * CreateCQVacuumContext
 */
CQVacuumContext *
CreateCQVacuumContext(Relation rel)
{
	char *relname = RelationGetRelationName(rel);
	char *cvname;
	Expr *expr;
	ParseState *ps;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;
	List *colnames = NIL;
	int i;
	CQVacuumContext *context;

	cvname = GetCVNameForMatRelationName(relname);
	if (!cvname)
		return NULL;

	if (!GetGCFlag(makeRangeVar(NULL, cvname, -1)))
		return NULL;

	/* Copy colnames from the relation's TupleDesc */
	for (i = 0; i < RelationGetDescr(rel)->natts; i++)
		colnames = lappend(colnames, makeString(NameStr(RelationGetDescr(rel)->attrs[i]->attname)));

	nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
	nsitem->p_cols_visible = true;
	nsitem->p_lateral_only = false;

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->alias = NULL;
	rte->inFromCl = true;
	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid; /* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->modifiedCols = NULL;
	rte->eref = makeAlias(relname, colnames);
	rte->relid = rel->rd_id;
	nsitem->p_rte = rte;

	ps = make_parsestate(NULL);
	ps->p_namespace = list_make1(nsitem);
	ps->p_rtable = list_make1(rte);

	expr = (Expr *) transformExpr(ps, GetCQVacuumExpr(cvname), EXPR_KIND_WHERE);

	context = (CQVacuumContext *) palloc(sizeof(CQVacuumContext));
	context->estate = CreateExecutorState();

	context->econtext = GetPerTupleExprContext(context->estate);
	context->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	context->econtext->ecxt_scantuple = context->slot;
	context->predicate = list_make1(ExecPrepareExpr(expr, context->estate));

	return context;
}

/*
 * FreeCQVacuumContext
 */
void
FreeCQVacuumContext(CQVacuumContext *context)
{
	if (!context)
		return;
	ExecDropSingleTupleTableSlot(context->slot);
	FreeExecutorState(context->estate);
	pfree(context);
}

/*
 * ShouldVacuumCQTuple
 */
bool
ShouldVacuumCQTuple(CQVacuumContext *context, HeapTupleData *tuple)
{
	bool vacuum;

	if (!context)
		return false;

	ExecStoreTuple(tuple, context->slot, InvalidBuffer, false);
	vacuum = ExecQual(context->predicate, context->econtext, false);
	ExecClearTuple(context->slot);
	return vacuum;
}
