/*-------------------------------------------------------------------------
 *
 * cqanalyze.c
 *	  Support for analyzing continuous view statements, mainly to support
 *	  schema inference
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqanalyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pipeline_combine_fn.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqslidingwindow.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

typedef struct CQAnalyzeContext
{
	ParseState *pstate;
	List *types;
	List *cols;
	List *streams;
	List *tables;
	List *targets;
} CQAnalyzeContext;

/*
 * GetCombineStateColumnType
 *
 * Retrieves the additional hidden columns that will be
 * required to store transition states for the given target entry
 */
Oid
GetCombineStateColumnType(TargetEntry *te)
{
	Oid result = InvalidOid;

	if (IsA(te->expr, Aggref))
	{
		Aggref *agg = (Aggref *) te->expr;

		result = GetCombineStateType(agg->aggfnoid);
	}

	return result;
}

/*
 * find_col_refs
 *
 * Recursively search this node and find all ColRefs in it.
 */
static bool
find_col_refs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
		{
			context->cols = lappend(context->cols, node);
			return false;
		}

	}
	else if (IsA(node, ColumnRef))
	{
		context->cols = lappend(context->cols, node);
	}

	return raw_expression_tree_walker(node, find_col_refs, (void *) context);
}

/*
 * does_colref_match_res_target
 *
 * Does this ColRef match the ResTarget? This is used
 * so that TypeCasted ColRefs are also considered matches.
 */
static bool
does_colref_match_res_target(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL &&
				strcmp(res->name, FigureColname((Node *) cref)) == 0)
			return true;
	}

	if (IsA(node, ColumnRef))
	{
		ColumnRef *test_cref = (ColumnRef *) node;
		ListCell *lc1;
		ListCell *lc2;
		bool match = true;

		if (list_length(test_cref->fields) != list_length(cref->fields))
			return false;

		forboth(lc1, test_cref->fields, lc2, cref->fields)
		{
			if (strcmp(strVal(lfirst(lc1)), strVal(lfirst(lc2))) != 0)
			{
				match = false;
				break;
			}
		}

		return match;
	}

	/*
	 * Even if a FuncCall has cref as an argument, its value
	 * can be different from the column, so we need to project
	 * the column.
	 */
	if (IsA(node, FuncCall))
		return false;

	return raw_expression_tree_walker(node, does_colref_match_res_target, (void *) cref);
}

/*
 * add_res_targets_for_missing_cols
 *
 * Add ResTargets for any ColRef that is missing in the
 * targetList of the SelectStmt.
 */
static void
add_res_targets_for_missing_cols(SelectStmt *stmt, List *cols)
{
	ListCell *clc;
	ListCell *tlc;
	ResTarget *res;

	foreach(clc, cols)
	{
		Node *node = (Node *) lfirst(clc);
		ColumnRef *cref;
		int location;
		bool found = false;

		if (IsA(node, TypeCast))
		{
			TypeCast *tc = (TypeCast *) node;
			cref = (ColumnRef *) tc->arg;
			location = tc->location;
		}
		else
		{
			cref = (ColumnRef *) lfirst(clc);
			location = cref->location;
		}

		foreach(tlc, stmt->targetList)
		{
			res = (ResTarget *) lfirst(tlc);
			found = does_colref_match_res_target((Node *) res, cref);

			if (found)
				break;
		}

		if (!found)
		{
			res = makeNode(ResTarget);
			res->name = NULL;
			res->indirection = NIL;
			res->val = node;
			res->location = location;
			stmt->targetList = lappend(stmt->targetList, res);
		}
	}
}

/*
 * GetSelectStmtForCQWorker
 *
 * Get the SelectStmt that should be executed by the
 * CQ worker on micro-batches.
 *
 * The targetList of this SelectStmt is also used to determine
 * the columns that must be created in the CQ's underlying
 * materialization table.
 */
SelectStmt *
GetSelectStmtForCQWorker(SelectStmt *stmt)
{
	CQAnalyzeContext context;

	stmt = (SelectStmt *) copyObject(stmt);

	/*
	 * Find all ColRefs in the groupClause and add them to the targetList
	 * if they're missing.
	 *
	 * These columns need to be projected by the worker so that the combiner
	 * can determine which row to merge the partial results with.
	 */
	context.cols = NIL;
	find_col_refs((Node *) stmt->groupClause, &context);
	add_res_targets_for_missing_cols(stmt, context.cols);

	if (IsSlidingWindowSelectStmt(stmt))
	{
		return TransformSWSelectStmtForCQWorker(stmt);
	}

	return stmt;
}

/*
 * GetSelectStmtForCQView
 *
 * Get the SelectStmt that should be passed to the VIEW we
 * create for this CQ.
 */
SelectStmt *
GetSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel)
{
	List		*origTargetList = stmt->targetList;
	ListCell	*lc;

	stmt = (SelectStmt *) copyObject(stmt);

	/*
	 * Is this a sliding window CQ? If so create a SELECT over
	 * the materialization table that filters based on the
	 * `matchExpr` rather than the original `whereClause`.
	 */
	if (IsSlidingWindowSelectStmt(stmt))
	{
		return TransformSWSelectStmtForCQView(stmt, cqrel);
	}

	/*
	 * Create a SelectStmt that only projects fields that
	 * the user expects in the continuous view.
	 */
	stmt = makeNode(SelectStmt);
	stmt->fromClause = list_make1(cqrel);
	stmt->targetList = NIL;

	/*
	 * Create a ResTarget to wrap a ColumnRef for each column
	 * name that we expect in the continuous view.
	 */
	foreach(lc, origTargetList)
	{
		ResTarget *origRes = lfirst(lc);
		ResTarget *res;
		ColumnRef *cref;
		char *colname;

		if (origRes->name != NULL)
			colname = origRes->name;
		else
			colname = FigureColname(origRes->val);

		cref = makeNode(ColumnRef);
		cref->fields = list_make1(makeString(colname));

		res = makeNode(ResTarget);
		res->name = NULL;
		res->indirection = NIL;
		res->val = (Node *) cref;
		res->location = origRes->location;

		stmt->targetList = lappend(stmt->targetList, res);
	}

	return stmt;
}

/*
 * find_colref_types
 *
 * Walk the parse tree and associate a single type with each inferred column
 */
static bool
find_colref_types(Node *node, CQAnalyzeContext *context)
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
	{
		context->cols = lappend(context->cols, (ColumnRef *) node);
	}
	else if (IsA(node, ResTarget))
	{
		context->targets = lappend(context->targets, (ResTarget *) node);
	}

	return raw_expression_tree_walker(node, find_colref_types, (void *) context);
}

/*
 * add_streams
 *
 * Figure out which relations are streams that we'll need to infer types for
 */
static bool
add_streams(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeVar))
	{
		Oid reloid = RangeVarGetRelid((RangeVar *) node, NoLock, true);
		if (reloid != InvalidOid)
			context->tables = lappend(context->tables, node);
		else
			context->streams = lappend(context->streams, node);

		return false;
	}

	return raw_expression_tree_walker(node, add_streams, (void *) context);
}

/*
 * make_streamdesc
 *
 * Create a StreamDesc and build and attach a TupleDesc to it
 */
static Node *
make_streamdesc(RangeVar *rv, CQAnalyzeContext *context)
{
	List *attrs = NIL;
	ListCell *lc;
	StreamDesc *desc = makeNode(StreamDesc);
	TupleDesc tupdesc;
	bool onestream = false;
	AttrNumber attnum = 1;
	bool sawArrivalTime = false;

	desc->name = rv;

	if (list_length(context->streams) == 1 && list_length(context->tables) == 0)
		onestream = true;

	foreach(lc, context->types)
	{
		Oid oid;
		TypeCast *tc = (TypeCast *) lfirst(lc);
		ColumnRef *ref = (ColumnRef *) tc->arg;
		Form_pg_attribute attr;
		char *colname;
		if (onestream)
		{
			/* all of the inferred columns belong to this stream desc */
			RangeVar *s = linitial(context->streams);
			if (s->alias)
				colname = strVal(lsecond(ref->fields));
			else
				colname = strVal(linitial(ref->fields));
		}
		else if (list_length(ref->fields) == 2)
		{
			/* find the columns that belong to this stream desc */
			char *colrelname = strVal(linitial(ref->fields));
			char *relname = rv->alias ? rv->alias->aliasname : rv->relname;
			if (strcmp(relname, colrelname) != 0)
				continue;

			colname = strVal(lsecond(ref->fields));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference is ambiguous"),
					 parser_errposition(context->pstate, ref->location)));
		}

		if (strcmp(colname, ARRIVAL_TIMESTAMP) == 0)
				sawArrivalTime = true;

		oid = LookupTypeNameOid(NULL, tc->typeName, false);
		attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attnum = attnum++;
		attr->atttypid = oid;

		namestrcpy(&attr->attname, colname);

		attrs = lappend(attrs, attr);
	}

	if (!sawArrivalTime)
	{
		Oid oid = LookupTypeNameOid(NULL, makeTypeName("timestamptz"), false);
		Form_pg_attribute attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attnum = attnum++;
		attr->atttypid = oid;
		namestrcpy(&attr->attname, ARRIVAL_TIMESTAMP);
		attrs = lappend(attrs, attr);
	}

	tupdesc = CreateTemplateTupleDesc(list_length(attrs), false);

	foreach(lc, attrs)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);

		/* PipelineDB XXX: should we be able to handle non-zero dimensions here? */
		TupleDescInitEntry(tupdesc, attr->attnum, NameStr(attr->attname),
				attr->atttypid, InvalidOid, 0);
	}

	desc->desc = tupdesc;

	return (Node *) desc;
}

/*
 * analyze_from_item
 *
 * Replaces RangeVar nodes that correspond to streams with StreamDesc nodes.
 *
 * Doing this as early as possible simplifies the rest of the analyze path.
 */
static Node*
analyze_from_item(Node *node, CQAnalyzeContext *context)
{
	if (IsA(node, RangeVar))
	{
		ListCell *lc;
		foreach(lc, context->streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			if (equal(rv, node))
				return make_streamdesc(rv, context);
		}

		/* not a stream */
		return node;
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *join = (JoinExpr *) node;
		join->larg = analyze_from_item(join->larg, context);
		join->rarg = analyze_from_item(join->rarg, context);

		return (Node *) join;
	}
	else
	{
		return node;
	}
}

/*
 * AnalyzeContinuousSelectStmt
 *
 * This is mainly to prepare a CV SELECT's FROM clause, which may involve streams
 */
void
AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **topselect)
{
	SelectStmt *stmt = *topselect;
	ListCell *lc;
	CQAnalyzeContext context;
	List *newfrom = NIL;

	if (stmt->sortClause != NULL)
	{
		elog(ERROR, "continuous view select can't have any sorting");
	}

	context.pstate = pstate;
	context.types = NIL;
	context.cols = NIL;
	context.streams = NIL;
	context.tables = NIL;
	context.targets = NIL;

	/* make sure that we can infer types for every column that appears anywhere in the statement */
	find_colref_types((Node *) stmt, &context);

	/* now indicate which relations are actually streams */
	add_streams((Node *) stmt->fromClause, &context);

	foreach(lc, context.cols)
	{
		ListCell *tlc;
		ListCell *slc;
		ColumnRef *cref = lfirst(lc);
		bool needstype = true;
		bool hastype = false;
		char *colname;

		/*
		 * Ensure that we have no '*' for a stream or relation target.
		 *
		 * We can't SELECT * from streams because we don't know the schema of
		 * streams ahead of time.
		 *
		 * XXX(usmanm): What if we have created a stream using the CREATE STREAM
		 * syntax? We should probably allow wildcards for such streams.
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
					 errmsg("can't select %s", NameListToString(cref->fields)),
					 errhint("Explicitly state the fields you want to select"),
					 parser_errposition(pstate, cref->location)));
		}

		if (list_length(cref->fields) == 2)
		{
			char *colrelname = strVal(linitial(cref->fields));

			colname = strVal(lsecond(cref->fields));
			needstype = false;

			foreach(slc, context.streams)
			{
				RangeVar *r = (RangeVar *) lfirst(slc);
				char *sname = r->alias ? r->alias->aliasname : r->relname;

				/* if it's a legit relation, the column doesn't need to have a type yet */
				if (strcmp(colrelname, sname) == 0)
				{
					needstype = true;
					break;
				}
			}
			if (!needstype)
				continue;
		}
		else
		{
			colname = strVal(linitial(cref->fields));
		}

		/* verify that we have a type for the column if it needs one */
		foreach(tlc, context.types)
		{
			TypeCast *tc = (TypeCast *) lfirst(tlc);
			if (equal(tc->arg, cref))
			{
				hastype = true;
				break;
			}
		}

		/*
		 * If the ColRef refers to a named ResTarget then it doesn't need an explicit type.
		 * XXX(usmanm): This doesn't work in cases where the ResTarget contains the ColumnRef
		 * being checked against. For example:
		 *   SELECT date_trunc('hour', ts) AS ts FROM stream
		 * Here we need as explicit for ts.
		 */
		foreach(tlc, context.targets)
		{
			ResTarget *rt = (ResTarget *) lfirst(tlc);
			CQAnalyzeContext context;
			ListCell *lc;
			ColumnRef *rt_cref;
			bool is_matching = false;

			context.pstate = pstate;
			context.types = NIL;
			context.cols = NIL;
			context.streams = NIL;
			context.tables = NIL;
			context.targets = NIL;

			find_colref_types(rt->val, &context);

			foreach(lc, context.cols)
			{
				rt_cref = (ColumnRef *) lfirst(lc);
				if (equal(rt_cref, cref))
				{
					is_matching = true;
					break;
				}
			}

			if (is_matching)
			{
				needstype = true;
				break;
			}

			if (rt->name != NULL && strcmp(strVal(linitial(cref->fields)), rt->name) == 0)
			{
				needstype = false;
				break;
			}
		}

		if (strcmp(colname, "arrival_timestamp") == 0)
			needstype = false;

		if (needstype && !hastype)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference \"%s\" has an ambiguous type", NameListToString(cref->fields)),
					 errhint("Explicitly cast to the desired type. For example, %s::integer.", NameListToString(cref->fields)),
					 parser_errposition(pstate, cref->location)));
		}
	}

	foreach(lc, stmt->fromClause)
	{
		Node *n = (Node *) lfirst(lc);
		Node *newnode = analyze_from_item(n, &context);

		newfrom = lappend(newfrom, newnode);
	}

	stmt->fromClause = newfrom;

	ValidateSlidingWindowExpr(stmt, pstate);
}

/*
 * TransformStreamEntry
 *
 * Transform a StreamDesc to a RangeTblEntry
 */
RangeTblEntry *
TransformStreamEntry(ParseState *pstate, StreamDesc *stream)
{
	RangeVar *relation = stream->name;
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	char *refname = relation->alias ? relation->alias->aliasname : relation->relname;

	rte->rtekind = RTE_RELATION;
	rte->alias = relation->alias;
	rte->inFromCl = true;
	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid;		/* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->modifiedCols = NULL;
	rte->relname = refname;

	rte->eref = makeAlias(refname, NIL);
	rte->streamdesc = stream;

	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
}
