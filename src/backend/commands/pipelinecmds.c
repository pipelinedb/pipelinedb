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
#include "pgstat.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pipeline_tstate_fn.h"
#include "catalog/toasting.h"
#include "executor/execdesc.h"
#include "executor/tstoreReceiver.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cqwindow.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/tuplebuf.h"
#include "regex/regex.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define CQ_MATREL_INDEX_TYPE "btree"
#define DEFAULT_TYPEMOD -1

#define OPTION_FILLFACTOR "fillfactor"

int continuous_view_fillfactor = 50;

static ColumnDef *
make_cv_columndef(char *name, Oid type, Oid typemod)
{
	ColumnDef *result;
	TypeName *typename;

	typename = makeNode(TypeName);
	typename->typeOid = type;
	typename->typemod = typemod;

	result = makeNode(ColumnDef);
	result->colname = name;
	result->inhcount = 0;
	result->is_local = true;
	result->is_not_null = false;
	result->raw_default = NULL;
	result->cooked_default = NULL;
	result->constraints = NIL;
	result->typeName = typename;

	return result;
}

/*
 * has_fillfactor
 *
 * Returns true if a fillfactor option is included in the given WITH options
 */
static bool
has_fillfactor(List *options)
{
	ListCell *lc;

	foreach(lc, options)
	{
		DefElem *de;

		if (!IsA(lfirst(lc), DefElem))
			continue;

		de = (DefElem *) lfirst(lc);
		if (de->defname && pg_strcasecmp(de->defname, OPTION_FILLFACTOR) == 0)
			return true;
	}

	return false;
}

/*
 * make_default_fillfactor
 *
 * Return a fillfactor DefElem with the default fillfactor
 */
static List *
add_default_fillfactor(List *options)
{
	DefElem *ff = makeDefElem(OPTION_FILLFACTOR, (Node *) makeInteger(continuous_view_fillfactor));

	options = lappend(options, (Node *) ff);

	return options;
}

/*
 * make_hashed_index_expr
 *
 * Create an index expression that hashes the grouping columns into a single
 * 32-bit value
 */
static Node *
make_hashed_index_expr(Query *query, TupleDesc desc)
{
	ListCell *lc;
	List *args = NIL;
	FuncExpr *hash;
	Oid hashoid = HASH_GROUP_OID;

	foreach(lc, query->groupClause)
	{
		SortGroupClause *g = (SortGroupClause *) lfirst(lc);
		TargetEntry *te = (TargetEntry *) get_sortgroupref_tle(g->tleSortGroupRef, query->targetList);
		Form_pg_attribute attr;
		Var *var;
		bool found = false;
		int i;

		/*
		 * Instead of using the expression itself as an argument, we use a variable that
		 * points to the column that stores the result of the expression.
		 */
		for (i=0; i<desc->natts; i++)
		{
			attr = (Form_pg_attribute) desc->attrs[i];
			if (pg_strcasecmp(te->resname, NameStr(attr->attname)) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
			elog(ERROR, "could not find index attribute in tuple descriptor");

		if (TypeCategory(attr->atttypid) == TYPCATEGORY_DATETIME)
			hashoid = LS_HASH_GROUP_OID;

		var = makeVar(1, attr->attnum, attr->atttypid, attr->atttypmod,
				attr->attcollation, 0);

		args = lappend(args, var);
	}

	/*
	 * We can only index on expressions having immutable results, so if any of the
	 * grouping expressions are mutable, we can't use a hashed index.
	 */
	hash = makeFuncExpr(hashoid, get_func_rettype(hashoid), args, 0, 0, COERCE_EXPLICIT_CALL);

	return (Node *) hash;
}

/*
 * create_index_on_mat_relation
 *
 * If feasible, create an index on the new materialization table to make
 * combine retrievals on it as efficient as possible. Sometimes this may be
 * impossible to do automatically in a smart way, but for some queries,
 * such as single-column GROUP BYs, it's straightforward.
 */
static void
create_index_on_mat_relation(Oid matreloid, RangeVar *matrelname, Query *query,
		SelectStmt *workerstmt, SelectStmt *viewstmt)
{
	IndexStmt *index;
	IndexElem *indexcol;
	Node *expr = NULL;
	bool sliding = IsSlidingWindowSelectStmt(workerstmt);
	char *indexcolname = NULL;

	if (query->groupClause == NIL && !sliding)
		return;

	if (query->groupClause == NIL && sliding)
	{
		/*
		 * We still want an index on the timestamp column for sliding window
		 * queries without any grouping, because there is an implicit WHERE clause
		 * used in queries against sliding window CVs.
		 */
		ColumnRef *col;
		Node *node = NULL;
		char *namespace;

		node = (Node *) GetColumnRefInSlidingWindowExpr(viewstmt);

		if (!IsA(node, ColumnRef))
			elog(ERROR, "unexpected sliding window expression type found: %d", nodeTag(node));

		col = (ColumnRef *) node;
		DeconstructQualifiedName(col->fields, &namespace, &indexcolname);
	}
	else
	{
		Relation matrel = heap_open(matreloid, NoLock);

		expr = make_hashed_index_expr(query, RelationGetDescr(matrel));
		heap_close(matrel, NoLock);
	}

	indexcol = makeNode(IndexElem);
	indexcol->name = indexcolname;
	indexcol->expr = expr;
	indexcol->indexcolname = NULL;
	indexcol->collation = NULL;
	indexcol->opclass = NULL;
	indexcol->ordering = SORTBY_DEFAULT;
	indexcol->nulls_ordering = SORTBY_NULLS_DEFAULT;

	index = makeNode(IndexStmt);
	index->idxname = NULL;
	index->relation = matrelname;
	index->accessMethod = CQ_MATREL_INDEX_TYPE;
	index->tableSpace = NULL;
	index->indexParams = list_make1(indexcol);
	index->unique = false;
	index->primary = false;
	index->isconstraint = false;
	index->deferrable = false;
	index->initdeferred = false;
	index->concurrent = false;

	DefineIndex(matreloid, index, InvalidOid, false, false, false, false);
	CommandCounterIncrement();
}

static void
record_dependencies(Oid cvoid, Oid matreloid, Oid viewoid, List *from)
{
	ObjectAddress parent;
	ObjectAddress child;
	ListCell *lc;
	ContAnalyzeContext cxt;

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));

	/*
	 * Record a dependency between the matrel and the view, so when we drop the view
	 * the matrel is automatically dropped as well. The user will enter the view name
	 * when dropping, so the alternative is to rewrite the drop target to the matrel.
	 * This seems simpler.
	 */
	child.classId = RelationRelationId;
	child.objectId = matreloid;
	child.objectSubId = 0;

	parent.classId = RelationRelationId;
	parent.objectId = viewoid;
	parent.objectSubId = 0;

	recordDependencyOn(&child, &parent, DEPENDENCY_INTERNAL);

	/*
	 * Record a dependency between the matrel and a pipeline_query entry so that when
	 * the matrel is dropped the pipeline_query metadata cleanup hook is invoked.
	 */
	child.classId = PipelineQueryRelationId;
	child.objectId = cvoid;
	child.objectSubId = 0;

	parent.classId = RelationRelationId;
	parent.objectId = viewoid;
	parent.objectSubId = 0;

	recordDependencyOn(&child, &parent, DEPENDENCY_INTERNAL);

	collect_rels_and_streams((Node *) from, &cxt);

	/*
	 * Record a dependency between any typed streams and a pipeline_query object,
	 * so that it is not possible to drop a stream that is being read by a CV.
	 */
	foreach(lc, cxt.streams)
	{
		RangeVar *rv;

		if (!IsA(lfirst(lc), RangeVar))
			continue;

		rv = (RangeVar *) lfirst(lc);
		if (RangeVarIsForTypedStream(rv))
		{
			Relation rel = heap_openrv(rv, AccessShareLock);

			parent.classId = PipelineQueryRelationId;
			parent.objectId = cvoid;
			parent.objectSubId = 0;

			child.classId = RelationRelationId;
			child.objectId = rel->rd_id;
			child.objectSubId = 0;

			recordDependencyOn(&parent, &child, DEPENDENCY_NORMAL);

			heap_close(rel, AccessShareLock);
		}
	}

	/* Record dependency between relations and continuous views if there is a stream-table join */
	foreach(lc, cxt.rels)
	{
		Relation rel;

		if (!IsA(lfirst(lc), RangeVar))
			continue;

		rel = heap_openrv((RangeVar *) lfirst(lc), AccessShareLock);

		parent.classId = PipelineQueryRelationId;
		parent.objectId = cvoid;
		parent.objectSubId = 0;

		child.classId = RelationRelationId;
		child.objectId = rel->rd_id;
		child.objectSubId = 0;

		recordDependencyOn(&parent, &child, DEPENDENCY_NORMAL);

		heap_close(rel, AccessShareLock);
	}
}

/*
 * ExecCreateContViewStmt
 *
 * Creates a table for backing the result of the continuous query,
 * and stores the query in a catalog table.
 */
void
ExecCreateContViewStmt(CreateContViewStmt *stmt, const char *querystring)
{
	CreateStmt *create_stmt;
	ViewStmt *view_stmt;
	Query *query;
	RangeVar *mat_relation;
	RangeVar *view;
	List *tableElts = NIL;
	List *tlist;
	ListCell *col;
	Oid matreloid;
	Oid viewoid;
	Oid cvoid;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	SelectStmt *workerselect;
	SelectStmt *viewselect;
	SelectStmt *cont_select;
	char *cont_select_sql;
	Query *cont_query;
	CQAnalyzeContext context;
	bool saveAllowSystemTableMods;

	Assert(((SelectStmt *) stmt->query)->forContinuousView);

	view = stmt->into->rel;

	/*
	 * Check if CV already exists?
	 */
	if (IsAContinuousView(view))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" already exists", view->relname)));

	mat_relation = makeRangeVar(view->schemaname, GetUniqueMatRelName(view->relname, view->schemaname), -1);

	/*
	 * allowSystemTableMods is a global flag that, when true, allows certain column types
	 * to be created. We need it set to true to create some hidden state columns. In particular,
	 * ones with a type of anyarray.
	 */
	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	ValidateContQuery(stmt, querystring);

	/* Deparse query so that workers always see the same SelectStmt */
	cont_query = parse_analyze(copyObject(stmt->query), querystring, NULL, 0);
	cont_select_sql = deparse_cont_query_def(cont_query);
	cont_select = (SelectStmt *) linitial(pg_parse_query(cont_select_sql));
	cont_select->forContinuousView = true;

	/*
	 * Get the transformed SelectStmt used by CQ workers. We do this
	 * because the targetList of this SelectStmt contains all columns
	 * that need to be created in the underlying materialization table.
	 */
	workerselect = GetSelectStmtForCQWorker(copyObject(cont_select), &viewselect);
	InitializeCQAnalyzeContext(workerselect, NULL, &context);

	query = parse_analyze(copyObject(workerselect), cont_select_sql, NULL, 0);
	tlist = query->targetList;

	/*
	 * Build a list of columns from the SELECT statement that we
	 * can use to create a table with
	 */
	foreach(col, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(col);
		ColumnDef   *coldef;
		char		*colname;
		Oid			hiddentype;
		Oid type;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);

		/*
		 * Set typeOid and typemod. The name of the type is derived while
		 * generating query
		 */
		type = exprType((Node *) tle->expr);
		/* Replace void type with a bool type. We need this because of the use of pg_sleep in some CQ tests */
		if (type == VOIDOID)
			type = BOOLOID;
		coldef = make_cv_columndef(colname, type, exprTypmod((Node *) tle->expr));
		tableElts = lappend(tableElts, coldef);

		/*
		 * If this column requires state to support incremental transitions, create it. Note: since this
		 * column isn't in the target list, it won't be visible when selecting from this CV,
		 * which will have an overlay view that only exposes target list columns.
		 */
		hiddentype = GetCombineStateColumnType(tle->expr);
		if (OidIsValid(hiddentype))
		{
			char *hiddenname = GetUniqueInternalColname(&context);
			ColumnDef *hidden = make_cv_columndef(hiddenname, hiddentype, DEFAULT_TYPEMOD);
			tableElts = lappend(tableElts, hidden);
		}
	}

	if (!has_fillfactor(stmt->into->options))
		stmt->into->options = add_default_fillfactor(stmt->into->options);

	/*
	 * Create the actual underlying materialzation relation.
	 */
	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = mat_relation;
	create_stmt->tableElts = tableElts;
	create_stmt->tablespacename = stmt->into->tableSpaceName;
	create_stmt->oncommit = stmt->into->onCommit;
	create_stmt->options = stmt->into->options;

	matreloid = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid);
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);

	AlterTableCreateToastTable(matreloid, toast_options, AccessExclusiveLock);

	/*
	 * Now save the underlying query in the `pipeline_query` catalog
	 * relation.
	 */
	cvoid = DefineContinuousView(view, cont_query, mat_relation,
				IsSlidingWindowSelectStmt(cont_select), !SelectsFromStreamOnly(cont_select));
	CommandCounterIncrement();

	/*
	 * Create a VIEW over the CQ materialization relation which exposes
	 * only the columns that users expect. This is needed primarily for three
	 * reasons:
	 *
	 * 1. Sliding window queries. For such queries, this VIEW filters events out
	 *    of the window (that have not been GC'd).
	 * 2. Some aggregate operators require storing some additional state along
	 *    with partial results and this VIEW filters out such hidden
	 *    columns.
	 * 3. View also computes expressions on aggregates.
	 */
	viewselect->fromClause = list_make1(mat_relation);
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;

	viewoid = DefineView(view_stmt, cont_select_sql);
	CommandCounterIncrement();
	allowSystemTableMods = saveAllowSystemTableMods;

	record_dependencies(cvoid, matreloid, viewoid, workerselect->fromClause);

	/*
	 * Record a dependency between the matrel and the view, so when we drop the view
	 * the matrel is automatically dropped as well. The user will enter the view name
	 * when dropping, so the alternative is to rewrite the drop target to the matrel.
	 * This seems simpler.
	 */
	allowSystemTableMods = saveAllowSystemTableMods;
	create_index_on_mat_relation(matreloid, mat_relation, query, workerselect, viewselect);
}

/*
 * ExecTruncateContViewStmt
 */
void
ExecTruncateContViewStmt(TruncateStmt *stmt)
{
	ListCell *lc;
	Relation pipeline_query;
	List *views = NIL;

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	/* Ensure that all *relations* are CQs. */
	foreach(lc, stmt->relations)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		RangeVar *matrel;
		HeapTuple tuple = GetPipelineQueryTuple(rv);
		Form_pipeline_query row;

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
					errmsg("continuous view \"%s\" does not exist", rv->relname)));

		row = (Form_pipeline_query) GETSTRUCT(tuple);

		ReleaseSysCache(tuple);

		views = lappend_oid(views, row->id);
		matrel = GetMatRelationName(rv);

		rv->relname = matrel->relname;
	}

	/* Reset all CQ level transition state */
	foreach(lc, views)
		ResetTStateEntry(lfirst_oid(lc));

	/* Call TRUNCATE on the backing view table(s). */
	stmt->objType = OBJECT_TABLE;
	ExecuteTruncate(stmt);

	heap_close(pipeline_query, NoLock);
}

void
ExecActivateStmt(ActivateStmt *stmt)
{
	if (!ContQuerySetStateAndWait(true, 1000))
		ereport(ERROR,
				(errmsg("failed to activate continuous views")));
}

void
ExecDeactivateStmt(DeactivateStmt *stmt)
{
	if (!ContQuerySetStateAndWait(false, 1000))
		ereport(ERROR,
				(errmsg("failed to deactivate continuous views")));
}

void
ExecExplainContViewStmt(ExplainContViewStmt *stmt, const char *queryString,
			 ParamListInfo params, DestReceiver *dest)
{
	/* TODO */
}
