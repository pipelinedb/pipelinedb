/*-------------------------------------------------------------------------
 *
 * pipelinecmds.c
 *	  commands specific to PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/commands/pipelinecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "catalog/toasting.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "regex/regex.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * CreateContinuousView
 *
 * Creates a table for backing the result of the continuous query,
 * and stores the query in a catalog table.
 */
void
CreateContinuousView(CreateContinuousViewStmt *stmt, const char *querystring)
{
	CreateStmt *create_stmt;
	Query *query;
	RangeVar *relation;
	IntoClause *into;
	List *tableElts = NIL;
	List *tlist;
	ListCell *lc;
	ListCell *col;
	Oid reloid;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

	relation = stmt->into->rel;

	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = relation;
	into = stmt->into;

	query = parse_analyze(stmt->query, NULL, 0, 0);
	tlist = query->targetList;

	/*
	 * Build a list of columns from the SELECT statement that we
	 * can use to create a table with
	 */
	lc = list_head(into->colNames);
	foreach(col, tlist)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(col);
		ColumnDef   *coldef;
		TypeName    *typename;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		coldef = makeNode(ColumnDef);
		typename = makeNode(TypeName);

		/* Take the column name specified if any */
		if (lc)
		{
			coldef->colname = strVal(lfirst(lc));
			lc = lnext(lc);
		}
		else
			coldef->colname = pstrdup(tle->resname);

		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->constraints = NIL;

		/*
		 * Set typeOid and typemod. The name of the type is derived while
		 * generating query
		 */
		typename->typeOid = exprType((Node *)tle->expr);
		typename->typemod = exprTypmod((Node *)tle->expr);

		coldef->typeName = typename;

		tableElts = lappend(tableElts, coldef);
	}

	/*
	 * Set column information and the distribution mechanism (which will be
	 * NULL for SELECT INTO and the default mechanism will be picked)
	 */
	create_stmt->tableElts = tableElts;
	create_stmt->distributeby = stmt->into->distributeby;
	create_stmt->subcluster = stmt->into->subcluster;

	create_stmt->tablespacename = stmt->into->tableSpaceName;
	create_stmt->oncommit = stmt->into->onCommit;
	create_stmt->options = stmt->into->options;

	/*
	 * Actually create the relation
	 */
	reloid = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid);
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);

	AlterTableCreateToastTable(reloid, toast_options);

	/*
	 * Now save the underlying query for ACTIVATION/DEACTIVATION
	 */
	RegisterContinuousView(relation, querystring);
}

/*
 * DumpState
 *
 * Dumps the state of a given object by sending tuples which describe
 * the state back to the client
 */
void
DumpState(DumpStmt *stmt)
{
	char *name = NULL;
	if (stmt->name)
		name = stmt->name->relname;

	elog(LOG, "DUMP \"%s\"", name);
}

/*
 * DropContinuousView
 *
 * Drops the query row in the pipeline_queries catalog table.
 */
void
DropContinuousView(DropStmt *stmt)
{
	ListCell *item;
	foreach(item, stmt->objects)
	{
		RangeVar *view_name = makeRangeVarFromNameList((List *) lfirst(item));
		DeregisterContinuousView(view_name);
	}
}

void
DeactivateContinuousView(DeactivateContinuousViewStmt *stmt)
{
	MarkContinuousViewAsInactive((RangeVar *) linitial(stmt->views));
}
