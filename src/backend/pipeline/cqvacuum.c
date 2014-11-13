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

#include "catalog/pipeline_query_fn.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqvacuum.h"
#include "pipeline/cqwindow.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#define CQ_TABLE_SUFFIX "_pdb"

static HTAB *disqualification_plans;

static void
get_cv_name(Relation rel, char* cvname)
{
	char *relname = get_rel_name(rel->rd_id);

	/*
	 * TODO(usmanm): This isn't entirely correct. We should simply
	 * store the materialization table name in the pipeline_query catalog
	 * and look that up or iterate through the catalog and see if we have
	 * a match.
	 */
	memset(cvname, 0, NAMEDATALEN);
	memcpy(cvname, relname, strlen(relname) - strlen(CQ_TABLE_SUFFIX));
}

bool
NeedsCQVacuum(Relation relation)
{
	char cvname[NAMEDATALEN];
	get_cv_name(relation, cvname);
	return GetGCFlag(makeRangeVar(NULL, cvname, -1));
}

bool
CQVacuumTuple(Relation rel, HeapTuple tuple)
{
	Expr *expr;
	char *relname = get_rel_name(rel->rd_id);
	char cvname[NAMEDATALEN];
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	List *predicate;
	ParseState *ps = make_parsestate(NULL);
	ParseNamespaceItem *nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
	RangeTblEntry *rte;
	List *colnames = NIL;
	int i;
	bool vacuum;

	for (i = 0; i < RelationGetDescr(rel)->natts; i++)
		colnames = lappend(colnames, makeString(NameStr(RelationGetDescr(rel)->attrs[i]->attname)));

	get_cv_name(rel, cvname);
	expr = GetCQVacuumExpr(cvname);

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
	ps->p_namespace = list_make1(nsitem);
	ps->p_rtable = list_make1(rte);

	expr = transformExpr(ps, (Node *) expr, EXPR_KIND_WHERE);
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);

	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	econtext->ecxt_scantuple = slot;
	predicate = list_make1(ExecPrepareExpr(expr, estate));
	vacuum = ExecQual(predicate, econtext, false);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	return vacuum;
}
