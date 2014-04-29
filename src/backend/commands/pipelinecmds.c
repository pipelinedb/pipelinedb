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
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "catalog/toasting.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "regex/regex.h"

#define QUERY_BEGINS_HERE " AS "

/*
 * CreateContinuousView
 *
 * Creates a table for backing the result of the continuous query,
 * and stores the query in a catalog table.
 */
void
CreateContinuousView(CreateContinuousViewStmt *stmt)
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
	StringInfoData deparsed;
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
	initStringInfo(&deparsed);
	deparse_query(query, &deparsed, NIL, false, false);
	AddQuery(relation->relname, deparsed.data, PIPELINE_QUERY_STATE_INACTIVE);
}

/*
 * DropContinuousView
 *
 * Drops the continuous view's underlying table and query row in
 * the pipeline_queries catalog table.
 */
void
DropContinuousView(DropStmt *stmt)
{

}

/*
 * RegisterQuery
 *
 * Registers a continuous query. This is essentially a matter of putting
 * it in the pipeline_queries catalog table and marking it as inactive.
 */
void
RegisterQuery(RangeVar *name, const char *rawquery)
{
	/*
	 * This is a hacky way to extract the original registered query,
	 * but it should always work since the grammar is of the form:
	 *
	 * REGISTER <name> AS ( ... )
	 *
	 * We don't just blindly include the ( ... ) component as part of the
	 * RegisterStmt, because we want it to be parsed for correctness.
	 * However, we still want to store the original ( ... ) in the catalog,
	 * assuming it's correct.
	 *
	 * Deparsing isn't sufficient for recovering the original query, because
	 * wildcards will have already been expanded, and thus the original
	 * query may not work as intended over time.
	 */
	char *upperquery = pstrdup(rawquery);
	char *uppername = pstrdup(name->relname);
	int offset = 0;
	int i;
	char *query_to_register;
	for (i=0; upperquery[i] != '\0'; i++) upperquery[i] = toupper(upperquery[i]);
	for (i=0; uppername[i] != '\0'; i++) uppername[i] = toupper(uppername[i]);

	/* Move past the query name part of the query before we look for AS */
	query_to_register = strstr(upperquery, uppername) + strlen(uppername);
	query_to_register = strstr(query_to_register, QUERY_BEGINS_HERE) + strlen(QUERY_BEGINS_HERE);

	/* We want to take the substring from the original string */
	offset = query_to_register - upperquery;

	AddQuery(name->relname, rawquery + offset, PIPELINE_QUERY_STATE_INACTIVE);
}


/*
 * ActivateQuery
 *
 * Activates a REGISTERed continuous query. If the query with the given name
 * is already active, it will be re-activated with whatever the current
 * query payload is.
 */
void
ActivateQuery(RangeVar *name)
{
//	SetQueryState(name, PIPELINE_QUERY_STATE_ACTIVE);
}


/*
 * DeactivateQuery
 *
 * Deactivates a REGISTERed and ACTIVE continuous query. If the query isn't
 * ACTIVE, this is a noop.
 */
void
DeactivateQuery(RangeVar *name)
{
//	SetQueryState(name, PIPELINE_QUERY_STATE_INACTIVE);
}
