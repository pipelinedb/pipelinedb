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

static bool
get_cv_name(char *relname, char *cvname)
{
	/*
	 * TODO(usmanm): This isn't entirely correct. We should simply
	 * store the materialization table name in the pipeline_query catalog
	 * and look that up or iterate through the catalog and see if we have
	 * a match.
	 */
	if (strlen(relname) <= strlen(CQ_TABLE_SUFFIX))
		return false;
	memset(cvname, 0, NAMEDATALEN);
	memcpy(cvname, relname, strlen(relname) - strlen(CQ_TABLE_SUFFIX));
	return true;
}

/*
 * NeedsCQVacuum
 */
bool
RelationNeedsCQVacuum(Oid relid)
{
	char *relname = get_rel_name(relid);
	char cvname[NAMEDATALEN];

	if (!get_cv_name(relname, cvname))
		return false;

	if (!GetGCFlag(makeRangeVar(NULL, cvname, -1)))
		return false;

	/*
	 * TODO(usmanm): Make this smarter. Figure out the %age of disqualified tuples
	 * and based on that return true/false.
	 */

	return true;
}

/*
 * CreateCQVacuumContext
 */
CQVacuumContext *
CreateCQVacuumContext(Relation rel)
{
	char *relname = RelationGetRelationName(rel);
	char cvname[NAMEDATALEN];
	Expr *expr;
	ParseState *ps;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;
	List *colnames = NIL;
	int i;
	CQVacuumContext *context;

	if (!get_cv_name(relname, cvname))
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
