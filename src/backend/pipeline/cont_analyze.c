	/*-------------------------------------------------------------------------
 *
 * cont_analyze.c
 *	  Support for parsing and analyzing continuous queries
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "utils/syscache.h"

#define INTERNAL_COLUMN_PREFIX "_"

typedef struct ColumRefWithTypeCast
{
	ColumnRef cref;
	TypeCast tc;
} ColumnRefWithTypeCast;

static bool
collect_column_names(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL)
			context->colnames = lappend(context->colnames, res->name);
	}
	else if (IsA(node, ColumnRef))
		context->colnames = lappend(context->colnames, FigureColname(node));

	return raw_expression_tree_walker(node, collect_column_names, (void *) context);
}

ContAnalyzeContext *
MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select)
{
	ContAnalyzeContext *context = palloc0(sizeof(ContAnalyzeContext));

	context->pstate = pstate;

	/*
	 * Collect any column names being used, so we don't clobber them when generating
	 * internal column names for the materialization table.
	 */
	collect_column_names((Node *) select, context);

	return context;
}

static bool
collect_rels_and_streams(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeVar))
	{
		Oid reloid = RangeVarGetRelid((RangeVar *) node, NoLock, true);
		if (reloid != InvalidOid)
			context->rels = lappend(context->rels, node);
		else
			context->streams = lappend(context->streams, node);

		return false;
	}

	return raw_expression_tree_walker(node, collect_rels_and_streams, (void *) context);
}

static bool
collect_types_and_cols(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
		{
			ListCell* lc;

			foreach(lc, context->types)
			{
				TypeCast *t = (TypeCast *) lfirst(lc);
				if (equal(tc->arg, t->arg) && !equal(tc, t))
				{
					/* a column can only be assigned one type */
					ColumnRef *cr = (ColumnRef *) tc->arg;
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("column has an ambiguous type because of a conflicting previous type cast"),
							parser_errposition(context->pstate, cr->location)));
				}
			}

			context->types = lappend(context->types, tc);
		}
	}
	else if (IsA(node, ColumnRef))
		context->cols = lappend(context->cols, node);

	return raw_expression_tree_walker(node, collect_types_and_cols, (void *) context);
}

static bool
contains_node(Node *node, Node *child)
{
	if (node == NULL || child == NULL)
		return false;

	if (equal(node, child))
		return true;

	return raw_expression_tree_walker(node, contains_node, (void *) child);
}

static bool
collect_agg_funcs(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *func = (FuncCall *) node;
		HeapTuple ftup;
		Form_pg_proc pform;
		bool is_agg = func->agg_within_group;
		FuncCandidateList clist;

		if (!func->agg_within_group)
		{
			clist = FuncnameGetCandidates(func->funcname, list_length(func->args), NIL, true, false, true);
			while (clist)
			{
				ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
				if (!HeapTupleIsValid(ftup))
					break;

				pform = (Form_pg_proc) GETSTRUCT(ftup);
				is_agg = pform->proisagg;
				ReleaseSysCache(ftup);

				if (is_agg)
					break;

				clist = clist->next;
			}
		}

		if (is_agg)
			context->funcs = lappend(context->funcs, func);
	}

	return raw_expression_tree_walker(node, collect_agg_funcs, context);
}

static void
collect_windows(SelectStmt *stmt, ContAnalyzeContext *context)
{
	List *windows = NIL;
	ListCell *lc;

	/*
	 * Copy over all WINDOW clauses.
	 */
	foreach(lc, stmt->windowClause)
	{
		windows = lappend(windows, lfirst(lc));
	}

	context->funcs = NIL;
	collect_agg_funcs((Node *) stmt->targetList, context);

	/*
	 * Copy over all inline OVER clauses.
	 */
	foreach(lc, context->funcs)
	{
		FuncCall *func = (FuncCall *) lfirst(lc);
		/*
		 * Ignore entries that reference names windows. Such
		 * windows should already be copied in the above loop.
		 */
		if (func->over == NULL || func->over->name != NULL)
			continue;
		windows = lappend(windows, func->over);
	}

	context->windows = windows;
}

/*
 * ValidateContinuousQuery
 */
void
ValidateContinuousQuery(CreateContinuousViewStmt *stmt, const char *sql)
{
	SelectStmt *select = (SelectStmt *) stmt->query;
	ContAnalyzeContext *context = MakeContAnalyzeContext(make_parsestate(NULL), select);
	ListCell *lc;

	context->pstate->p_sourcetext = sql;

	collect_rels_and_streams((Node *) select->fromClause, context);
	collect_types_and_cols((Node *) select, context);
	collect_agg_funcs((Node *) select, context);

	/* Ensure that we're reading from at least one stream */
	if (!context->streams)
	{
		RangeVar *t = (RangeVar *) linitial(context->rels);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("continuous queries must include a stream in the FROM clause"),
						errhint("To include a relation in a continuous query, JOIN it with a stream."),
						parser_errposition(context->pstate, t->location)));
	}

	/* Ensure that we're not trying to read from ourselves, which right now would be treated as a stream. */
	foreach(lc, context->streams)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		if (equal(rv, stmt->into->rel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("continuous queries cannot read from themselves"),
					errhint("Remove \"%s\" from the FROM clause.", rv->relname),
					parser_errposition(context->pstate, rv->location)));
	}

	/* Ensure that each column being read from a stream is type-casted */
	foreach(lc, context->cols)
	{
		ListCell *lc2;
		ColumnRef *cref = lfirst(lc);
		bool needs_type = true;
		bool has_type = false;
		bool refs_target;
		char *colname = FigureColname((Node *) cref);
		char *qualname = NameListToString(cref->fields);

		/*
		 * ARRIVAL_TIMESTAMP doesn't require an explicit type cast. We don't care about the
		 * qualified name because even if it's a column for a table, we don't need an explicit
		 * type.
		 */
		if (pg_strcasecmp(colname, ARRIVAL_TIMESTAMP) == 0)
			continue;

		/*
		 * Ensure that we have no `*` in the target list.
		 *
		 * We can't SELECT * from streams because we don't know the schema of
		 * streams ahead of time.
		 *
		 * XXX(usmanm): The decision to disallow wildcards for relations was taken
		 * because relations can be ALTERed in the future which would require us to
		 * ALTER the CQ's underlying materialization table and the VIEW. This can probably
		 * be accomplished by triggers, but lets just punt on this for now.
		 */
		if (IsA(lfirst(cref->fields->tail), A_Star))
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("can't select \"%s\"", qualname),
					 errhint("Explicitly state the columns you want to SELECT."),
					 parser_errposition(context->pstate, cref->location)));
		}

		/*
		 * If the column is for a relation, we don't need any explicit type case.
		 */
		if (list_length(cref->fields) == 2)
		{
			char *col_relname = strVal(linitial(cref->fields));
			needs_type = false;

			foreach(lc2, context->streams)
			{
				RangeVar *rv = (RangeVar *) lfirst(lc2);
				char *streamname = rv->alias ? rv->alias->aliasname : rv->relname;

				if (pg_strcasecmp(col_relname, streamname) == 0)
				{
					needs_type = true;
					break;
				}
			}

			if (!needs_type)
				continue;
		}

		/* Do we have a TypeCast for this ColumnRef? */
		foreach(lc2, context->types)
		{
			TypeCast *tc = (TypeCast *) lfirst(lc2);
			if (equal(tc->arg, cref))
			{
				has_type = true;
				break;
			}
		}

		if (has_type)
			continue;

		/*
		 * If the column refers to a named target then it doesn't need an explicit type.
		 *
		 * This doesn't work in cases where target references the column being checked.
		 * For example:
		 *
		 *   SELECT date_trunc('hour', x) AS x FROM stream
		 *   SELECT substring(y::text, 1, 2) as x, substring(x, 1, 2) FROM stream
		 *
		 * In both these examples we need an explicit cast for `x`.
		 */
		needs_type = false;
		refs_target = false;

		foreach(lc2, select->targetList)
		{
			ResTarget *res = (ResTarget *) lfirst(lc2);

			if (contains_node((Node *) res, (Node *) cref))
			{
				needs_type = true;
				break;
			}

			if (res->name && pg_strcasecmp(qualname, res->name) == 0)
				refs_target = true;
		}

		if (refs_target && !needs_type)
			continue;

		/* If it doesn't reference a target or is contained in a target, it needs a type */
		if (needs_type || !refs_target)
		{
			/*
			 * If it's a stream-table join, try to do some extra work to make the error
			 * informative, as the user may be trying to join against a nonexistent table.
			 */
			bool has_join = false;
			RangeVar *rel = NULL;

			foreach(lc, select->fromClause)
			{
				Node *n = (Node *) lfirst(lc);
				if (IsA(n, JoinExpr))
				{
					has_join = true;
					break;
				}
			}

			if (list_length(cref->fields) > 1)
			{
				Value *alias = linitial(cref->fields);
				foreach(lc, context->streams)
				{
					RangeVar *rv = (RangeVar *) lfirst(lc);
					if (pg_strcasecmp(rv->relname, strVal(alias)) == 0 || pg_strcasecmp(rv->alias->aliasname, strVal(alias)) == 0)
					{
						rel = rv;
						break;
					}
				}
			}

			if (has_join)
			{
				if (rel)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" has an ambiguous type", qualname),
							 errhint("Explicitly cast to the desired type. For example, %s::integer. "
									 "If \"%s\" is supposed to be a relation, create it first.", qualname, rel->relname),
							 parser_errposition(context->pstate, cref->location)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" has an ambiguous type", qualname),
							 errhint("Explicitly cast to the desired type. For example, %s::integer. "
									 "If \"%s\" is supposed to belong to a relation, create the relation first.", qualname, qualname),
							 parser_errposition(context->pstate, cref->location)));
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						errmsg("column reference \"%s\" has an ambiguous type", qualname),
						errhint("Explicitly cast to the desired type. For example, %s::integer.", qualname),
						parser_errposition(context->pstate, cref->location)));
		}
	}

	/* Ensure that any WINDOWs are legal */
	collect_windows(select, context);
	foreach(lc, context->windows)
	{
		WindowDef *window = (WindowDef *) lfirst(lc);

		if (list_length(window->orderClause) > 1)
		{
			/* Can't ORDER BY multiple columns. */
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("only a single ORDER BY is allowed for WINDOWs"),
					parser_errposition(context->pstate, window->location)));
		}
		else if (list_length(window->orderClause) == 1)
		{
			/* ORDER BY must be on a time-like column/expression */
			/* TODO(usmanm): Check that type of this column is time-like */
		}
	}

	/* Ensure that the sliding window is legal */
}
