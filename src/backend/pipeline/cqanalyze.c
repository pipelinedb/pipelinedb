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

#define INTERNAL_COLNAME_PREFIX "_"

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
 * FindColNames
 */
static bool
FindColNames(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL)
			context->colNames = lappend(context->colNames, res->name);
	}
	else if (IsA(node, ColumnRef))
	{
		context->colNames = lappend(context->colNames, FigureColname(node));
	}

	return raw_expression_tree_walker(node, FindColNames, (void *) context);
}

/*
 * InitializeCQAnalyzeContext
 */
void
InitializeCQAnalyzeContext(SelectStmt *stmt, ParseState *pstate, CQAnalyzeContext *context)
{
	memset(context, 0, sizeof(CQAnalyzeContext));
	context->location = -1;
	context->pstate = pstate;

	FindColNames((Node *) stmt, context);

	context->cols = NIL;
	context->colNum = 0;
}

/*
 * GetUniqueInternalColname
 */
char *
GetUniqueInternalColname(CQAnalyzeContext *context)
{
	/* XXX(usmanm): 8 bytes should be hella enough? */
	char *colname = palloc(8);

	while (1)
	{
		ListCell *lc;
		bool alreadyExists = false;

		sprintf(colname, "%s%d", INTERNAL_COLNAME_PREFIX, context->colNum);
		context->colNum++;

		foreach(lc, context->colNames)
		{
			char *colname2 = lfirst(lc);
			if (strcmp(colname, colname2) == 0)
			{
				alreadyExists = true;
				break;
			}
		}

		if (!alreadyExists)
			break;
	}

	context->colNames = lappend(context->colNames, colname);

	return colname;
}

/*
 * is_column_ref
 */
static bool
is_column_ref(Node *node)
{
	TypeCast *tc = (TypeCast *) node;

	return (IsA(node, ColumnRef) ||
			(IsA(node, TypeCast) && (IsA(tc->arg, ColumnRef))));
}

/*
 * FindColRefsWithTypeCasts
 */
bool
FindColumnRefsWithTypeCasts(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, FindColumnRefsWithTypeCasts, (void *) context);
}

/*
 * are_column_refs_equal
 */
static bool
are_column_refs_equal(ColumnRef *cr1, ColumnRef *cr2)
{
	/* In our land ColumnRef locations are matter */
	return equal(cr1->fields, cr2->fields);
}

/*
 * contains_column_ref
 */
static bool
contains_column_ref(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		ColumnRef *cref2 = (ColumnRef *) node;
		return are_column_refs_equal(cref, cref2);
	}

	return raw_expression_tree_walker(node, contains_column_ref, (void *) cref);
}

/*
 * IsResTargetForColumnRef
 */
static bool
IsResTargetForColumnRef(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL &&
				strcmp(res->name, FigureColname((Node *) cref)) == 0)
		{
			/*
			 * Is this ResTarget overriding a column it references?
			 * Example: substring(key::text, 1, 2) AS key
			 */
			if (contains_column_ref((Node *) res, cref))
				return false;
			return true;
		}
	}
	else if (IsA(node, ColumnRef))
	{
		ColumnRef *cref2 = (ColumnRef *) node;
		return are_column_refs_equal(cref, cref2);
	}
	else if (IsA(node, FuncCall))
	{
		/*
		 * Even if a FuncCall has cref as an argument, its value
		 * can be different from the column, so we need to project
		 * the column.
		 */
		return false;
	}

	return raw_expression_tree_walker(node, IsResTargetForColumnRef, (void *) cref);
}

/*
 * IsColumnRefInTargetList
 */
bool
IsColumnRefInTargetList(SelectStmt *stmt, Node *node)
{
	ColumnRef *cref;
	ListCell *lc;
	bool found = false;

	if (IsA(node, TypeCast))
		node = ((TypeCast *) node)->arg;

	Assert(IsA(node, ColumnRef));

	cref = (ColumnRef *) node;

	foreach(lc, stmt->targetList)
	{
		node = lfirst(lc);

		if (IsResTargetForColumnRef(node, cref))
		{
			found = true;
			break;
		}
	}

	return found;
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
	ListCell *lc;
	List *newGroupClause = NIL;

	InitializeCQAnalyzeContext(stmt, NULL, &context);

	stmt = (SelectStmt *) copyObject(stmt);

	/*
	 * Check to see if we need to project any columns
	 * that are required to evaluate the sliding window
	 * match expression.
	 */
	if (IsSlidingWindowSelectStmt(stmt))
	{
		stmt = TransformSWSelectStmtForCQWorker(stmt, &context);
	}

	/*
	 * Rewrite the groupClause.
	 */
	foreach(lc, stmt->groupClause)
	{
		Node *node = (Node *) lfirst(lc);
		char *name = NULL;
		ResTarget *res;
		ColumnRef *cref;

		if (is_column_ref(node) && IsColumnRefInTargetList(stmt, node))
		{
			newGroupClause = lappend(newGroupClause, node);
			continue;
		}

		/*
		 * Any new node we add to the targetList should have a unique
		 * name. This isn't always necessary, but its always safe.
		 * Safety over everything else yo.
		 */
		name = GetUniqueInternalColname(&context);
		cref = makeNode(ColumnRef);
		cref->fields = list_make1(makeString(name));
		cref->location = -1;

		res = makeNode(ResTarget);
		res->name = name;
		res->indirection = NIL;
		res->val = node;
		res->location = -1;

		stmt->targetList = lappend(stmt->targetList, res);

		newGroupClause = lappend(newGroupClause, cref);
	}

	stmt->groupClause = newGroupClause;

	return stmt;
}

/*
 * GetSelectStmtForCQCombiner
 */
SelectStmt *
GetSelectStmtForCQCombiner(SelectStmt *stmt)
{
	stmt = GetSelectStmtForCQWorker(stmt);

	/*
	 * Combiner shouldn't have to check for the
	 * whereClause conditionals. The worker has already
	 * done that.
	 */
	stmt->whereClause = NULL;

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
	CQAnalyzeContext context;
	List *origTargetList = stmt->targetList;
	ListCell *lc;

	InitializeCQAnalyzeContext(stmt, NULL, &context);

	stmt = (SelectStmt *) copyObject(stmt);
	stmt->forContinuousView = false;

	if (IsSlidingWindowSelectStmt(stmt))
		return TransformSWSelectStmtForCQView(stmt, cqrel, &context);

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
		cref->location = origRes->location;

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
associate_types_to_colrefs(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, associate_types_to_colrefs, (void *) context);
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
 * replace_stream_rangevar_with_streamdesc
 *
 * Doing this as early as possible simplifies the rest of the analyze path.
 */
static Node*
replace_stream_rangevar_with_streamdesc(Node *node, CQAnalyzeContext *context)
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
		join->larg = replace_stream_rangevar_with_streamdesc(join->larg, context);
		join->rarg = replace_stream_rangevar_with_streamdesc(join->rarg, context);

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
	CQAnalyzeContext context;
	SelectStmt *stmt = *topselect;
	ListCell *lc;
	List *newfrom = NIL;

	InitializeCQAnalyzeContext(stmt, pstate, &context);

	if (list_length(stmt->sortClause) > 0)
	{
		SortBy *sortby = (SortBy *) linitial(stmt->sortClause);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("continuous queries don't support ORDER BY"),
						parser_errposition(pstate, sortby->location)));
	}

	/* make sure that we can infer types for every column that appears anywhere in the statement */
	associate_types_to_colrefs((Node *) stmt, &context);

	/* now indicate which relations are actually streams */
	add_streams((Node *) stmt->fromClause, &context);

	foreach(lc, context.cols)
	{
		ListCell *tlc;
		ListCell *slc;
		ColumnRef *cref = lfirst(lc);
		bool needsType = true;
		bool hasType = false;
		bool matchesResTarget = false;
		char *colName = FigureColname((Node *) cref);

		/*
		 * arrival_timestamp doesn't require an explicit TypeCast
		 */
		if (pg_strcasecmp(colName, ARRIVAL_TIMESTAMP) == 0)
			continue;

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

		/*
		 * If the ColumnRef is for a relation, we don't need any explicit
		 * TypeCast.
		 */
		if (list_length(cref->fields) == 2)
		{
			char *colRelname = strVal(linitial(cref->fields));
			needsType = false;

			foreach(slc, context.streams)
			{
				RangeVar *rv = (RangeVar *) lfirst(slc);
				char *streamName = rv->alias ? rv->alias->aliasname : rv->relname;

				if (strcmp(colRelname, streamName) == 0)
				{
					needsType = true;
					break;
				}
			}

			if (!needsType)
				continue;
		}

		/* Do we have a TypeCast for this ColumnRef? */
		foreach(tlc, context.types)
		{
			TypeCast *tc = (TypeCast *) lfirst(tlc);
			if (equal(tc->arg, cref))
			{
				hasType = true;
				break;
			}
		}

		if (hasType)
			continue;

		/*
		 * If the ColRef refers to a named ResTarget then it doesn't need an explicit type.
		 *
		 * This doesn't work in cases where a ResTarget contains the ColumnRef
		 * being checked against. For example:
		 *
		 *   SELECT date_trunc('hour', x) AS x FROM stream
		 *   SELECT substring(y::text, 1, 2) as x, substring(x, 1, 2) FROM stream
		 *
		 * In both these examples we need an explicit TypeCast for `x`.
		 */
		needsType = false;

		foreach(tlc, context.targets)
		{
			ResTarget *rt = (ResTarget *) lfirst(tlc);
			if (contains_column_ref((Node *) rt, cref))
			{
				needsType = true;
				break;
			}

			if (rt->name != NULL && strcmp(strVal(linitial(cref->fields)), rt->name) == 0)
			{
				matchesResTarget = true;
			}
		}

		if (matchesResTarget && !needsType)
			continue;

		needsType = needsType || !matchesResTarget;

		if (needsType && !hasType)
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
		Node *newnode = replace_stream_rangevar_with_streamdesc(n, &context);

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
