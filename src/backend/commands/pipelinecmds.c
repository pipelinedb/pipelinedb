/*-------------------------------------------------------------------------
 *
 * pipelinecmds.c
 *	  commands specific to PipelineDB
 *
 * Copyright (c) 2013-2015, PipelineDB
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
#include "catalog/binary_upgrade.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/pipelinecmds.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "commands/view.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream_fn.h"
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
#include "parser/parse_func.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/matrel.h"
#include "pipeline/analyzer.h"
#include "pipeline/planner.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "regex/regex.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/pipelinefuncs.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "storage/lmgr.h"

#define CQ_MATREL_INDEX_TYPE "btree"
#define DEFAULT_TYPEMOD -1

/* guc params */
int continuous_view_fillfactor;

/* for binary upgrades */
static Oid next_matrel_type = InvalidOid;
static Oid next_matrel_array_type = InvalidOid;
static Oid next_matrel_toast_type = InvalidOid;
static Oid next_matrel_class = InvalidOid;
static Oid next_matrel_toast_class = InvalidOid;
static Oid next_matrel_toast_index_class = InvalidOid;

static Oid next_seqrel_type = InvalidOid;
static Oid next_seqrel_class = InvalidOid;

static Oid next_pk_index_class = InvalidOid;
static Oid next_lookup_index_class = InvalidOid;

static Oid next_overlay_type = InvalidOid;
static Oid next_overlay_array_type = InvalidOid;
static Oid next_overlay_class = InvalidOid;

static Oid next_osrel_type = InvalidOid;
static Oid next_osrel_array_type = InvalidOid;
static Oid next_osrel_class = InvalidOid;

static void
reset_next_oids()
{
	binary_upgrade_next_pg_type_oid = InvalidOid;
	binary_upgrade_next_array_pg_type_oid = InvalidOid;
	binary_upgrade_next_toast_pg_type_oid = InvalidOid;
	binary_upgrade_next_heap_pg_class_oid = InvalidOid;
	binary_upgrade_next_toast_pg_class_oid = InvalidOid;
	binary_upgrade_next_index_pg_class_oid = InvalidOid;
}

static void
set_next_oids_for_matrel(void)
{
	reset_next_oids();
	binary_upgrade_next_pg_type_oid = next_matrel_type;
	binary_upgrade_next_array_pg_type_oid = next_matrel_array_type;
	binary_upgrade_next_toast_pg_type_oid = next_matrel_toast_type;

	binary_upgrade_next_heap_pg_class_oid = next_matrel_class;
	binary_upgrade_next_toast_pg_class_oid = next_matrel_toast_class;
	binary_upgrade_next_index_pg_class_oid = next_matrel_toast_index_class;
}

static void
set_next_oids_for_seqrel(void)
{
	reset_next_oids();
	binary_upgrade_next_pg_type_oid = next_seqrel_type;
	binary_upgrade_next_heap_pg_class_oid = next_seqrel_class;
}

static void
set_next_oids_for_pk_index(void)
{
	reset_next_oids();
	binary_upgrade_next_index_pg_class_oid = next_pk_index_class;
}

static void
set_next_oids_for_lookup_index(void)
{
	reset_next_oids();
	binary_upgrade_next_index_pg_class_oid = next_lookup_index_class;
}

static void
set_next_oids_for_overlay(void)
{
	reset_next_oids();
	binary_upgrade_next_pg_type_oid = next_overlay_type;
	binary_upgrade_next_array_pg_type_oid = next_overlay_array_type;
	binary_upgrade_next_heap_pg_class_oid = next_overlay_class;
}

static void
set_next_oids_for_osrel(void)
{
	reset_next_oids();
	binary_upgrade_next_pg_type_oid = next_osrel_type;
	binary_upgrade_next_array_pg_type_oid = next_osrel_array_type;
	binary_upgrade_next_heap_pg_class_oid = next_osrel_class;
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
make_hashed_index_expr(RangeVar *cv, SelectStmt *select, TupleDesc desc)
{
	ListCell *lc;
	List *args = NIL;
	FuncExpr *hash;
	ColumnRef *t_col = GetWindowTimeColumn(cv);
	Oid hashoid = HASH_GROUP_OID;
	char *t_colname = NULL;

	if (t_col)
		t_colname = NameListToString(t_col->fields);

	foreach(lc, select->groupClause)
	{
		Node *node = lfirst(lc);
		char *name;
		bool found = false;
		Form_pg_attribute attr;
		Var *var;
		int i;

		Assert(IsA(node, ColumnRef));

		name = FigureColname(node);

		/*
		 * Instead of using the expression itself as an argument, we use a variable that
		 * points to the column that stores the result of the expression.
		 */
		for (i = 0; i < desc->natts; i++)
		{
			attr = (Form_pg_attribute) desc->attrs[i];
			if (pg_strcasecmp(name, NameStr(attr->attname)) == 0)
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

		/*
		 * Always insert time column in the beginning so it is correctly picked for locality-sensitive
		 * hashing by ls_hash_group.
		 */
		if (t_colname && pg_strcasecmp(t_colname, name) == 0)
			args = list_concat(list_make1(var), args);
		else
			args = lappend(args, var);
	}

	/*
	 * We can only index on expressions having immutable results, so if any of the
	 * grouping expressions are mutable, we can't use a hashed index.
	 */
	hash = makeFuncExpr(hashoid, get_func_rettype(hashoid), args, 0, 0, COERCE_EXPLICIT_CALL);

	return (Node *) hash;
}

static Oid
create_lookup_index(RangeVar *cv, Oid matrelid, RangeVar *matrel, SelectStmt *select, bool is_sw)
{
	IndexStmt *index;
	IndexElem *indexcol;
	Node *expr = NULL;
	char *indexcolname = NULL;
	Oid index_oid;
	ObjectAddress address;

	if (select->groupClause == NIL && !is_sw)
		return InvalidOid;

	if (select->groupClause == NIL && is_sw)
	{
		/*
		 * We still want an index on the timestamp column for sliding window
		 * queries without any grouping, because there is an implicit WHERE clause
		 * used in queries against sliding window CVs.
		 */
		ColumnRef *col;
		char *namespace;

		col = GetSWTimeColumn(cv);

		if (!IsA(col, ColumnRef))
			elog(ERROR, "unexpected sliding window expression type found: %d", nodeTag(col));

		DeconstructQualifiedName(col->fields, &namespace, &indexcolname);
	}
	else
	{
		Relation rel = heap_open(matrelid, NoLock);
		expr = make_hashed_index_expr(cv, select, RelationGetDescr(rel));
		heap_close(rel, NoLock);
	}

	indexcol = makeNode(IndexElem);
	indexcol->name = indexcolname;
	indexcol->expr = expr;
	indexcol->ordering = SORTBY_DEFAULT;
	indexcol->nulls_ordering = SORTBY_NULLS_DEFAULT;

	index = makeNode(IndexStmt);
	index->relation = matrel;
	index->accessMethod = CQ_MATREL_INDEX_TYPE;
	index->indexParams = list_make1(indexcol);

	address = DefineIndex(matrelid, index, InvalidOid, false, false, false, false);
	index_oid = address.objectId;
	CommandCounterIncrement();

	return index_oid;
}

static Oid
create_pkey_index(RangeVar *cv, Oid matrelid, RangeVar *matrel, char *colname)
{
	IndexStmt *index;
	IndexElem *indexcol;
	Oid index_oid;
	ObjectAddress address;

	indexcol = makeNode(IndexElem);
	indexcol->name = colname;
	indexcol->ordering = SORTBY_DEFAULT;
	indexcol->nulls_ordering = SORTBY_NULLS_DEFAULT;

	index = makeNode(IndexStmt);
	index->relation = matrel;
	index->accessMethod = CQ_MATREL_INDEX_TYPE;
	index->indexParams = list_make1(indexcol);
	index->primary = true;
	index->unique = true;
	index->isconstraint = true;

	address = DefineIndex(matrelid, index, InvalidOid, false, false, false, false);
	index_oid = address.objectId;
	CommandCounterIncrement();

	return index_oid;
}

static void
record_cv_dependencies(Oid cvoid, Oid matreloid, Oid osreloid, Oid seqreloid, Oid viewoid,
		Oid indexoid, Oid pkey_idxoid, SelectStmt *stmt, Query *query)
{
	ObjectAddress referenced;
	ObjectAddress dependent;
	ListCell *lc;
	ContAnalyzeContext cxt;

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));

	/* Record a dependency between matrel and view. */
	dependent.classId = RelationRelationId;
	dependent.objectId = matreloid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = viewoid;
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	/* Record dependency between output stream and view */
	dependent.classId = RelationRelationId;
	dependent.objectId = osreloid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = viewoid;
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	/* Record dependency between sequence relation and view. */
	if (OidIsValid(seqreloid))
	{
		dependent.classId = RelationRelationId;
		dependent.objectId = seqreloid;
		dependent.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);
	}

	/*
	 * Record a dependency between the view its pipeline_query entry so that when
	 * the view is dropped the pipeline_query metadata cleanup hook is invoked.
	 */
	dependent.classId = PipelineQueryRelationOid;
	dependent.objectId = cvoid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = viewoid;
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	/*
	 * Record a dependency between the matrel and the group lookup index so that the
	 * index can never be dropped.
	 */
	if (OidIsValid(indexoid))
	{
		dependent.classId = RelationRelationId;
		dependent.objectId = indexoid;
		dependent.objectSubId = 0;

		referenced.classId = RelationRelationId;
		referenced.objectId = matreloid;
		referenced.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);
	}

	/* Record dependency on pkey index */
	dependent.classId = RelationRelationId;
	dependent.objectId = pkey_idxoid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = matreloid;
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	collect_rels_and_streams((Node *) stmt->fromClause, &cxt);

	/*
	 * Record a dependency between any strongly typed streams and a pipeline_query object,
	 * so that it is not possible to drop a stream that is being read by a CV.
	 */
	foreach(lc, cxt.streams)
	{
		RangeVar *rv;
		Oid relid;

		if (!IsA(lfirst(lc), RangeVar))
			continue;

		rv = (RangeVar *) lfirst(lc);
		relid = RangeVarGetRelid(rv, NoLock, false);

		referenced.classId = RelationRelationId;
		referenced.objectId = relid;
		referenced.objectSubId = 0;

		dependent.classId = RelationRelationId;
		dependent.objectId = viewoid;
		dependent.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);
	}

	referenced.classId = RelationRelationId;
	referenced.objectId = viewoid;
	referenced.objectSubId = 0;
	recordDependencyOnExpr(&referenced, (Node *) query, NIL, DEPENDENCY_NORMAL);
}

static ColumnDef *
make_coldef(char *name, Oid type, Oid typemod)
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

static List *
create_coldefs_from_tlist(Query *query)
{
	ListCell *lc;
	List *defs = NIL;

	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		char *colname;
		Oid type;
		ColumnDef *coldef;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);

		type = exprType((Node *) tle->expr);

		/* Replace void type with a bool type. We need this because of the use of pg_sleep in some CQ tests */
		if (type == VOIDOID)
			type = BOOLOID;

		coldef = make_coldef(colname, type, exprTypmod((Node *) tle->expr));
		defs = lappend(defs, coldef);
	}

	return defs;
}

static void
check_relation_already_exists(RangeVar *rv)
{
	Oid namespace = RangeVarGetCreationNamespace(rv);

	if (OidIsValid(get_relname_relid(rv->relname, namespace)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", rv->relname)));
}

/*
 * extract_ttl_params
 */
static void
extract_ttl_params(List **options, List *coldefs, bool has_sw, int *ttl, char **ttl_column)
{
	ListCell *lc;
	DefElem *opt_ttl = NULL;
	DefElem *opt_ttl_col = NULL;

	Assert(ttl);

	foreach(lc, *options)
	{
		DefElem *e = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(e->defname, OPTION_TTL) == 0)
		{
			Interval *ttli;

			if (!IsA(e->arg, String))
				elog(ERROR, "ttl must be expressed as an interval");

			ttli = (Interval *) DirectFunctionCall3(interval_in,
					(Datum) strVal(e->arg), 0, (Datum) -1);

			opt_ttl = e;
			*ttl = IntervalToEpoch(ttli);
		}
		else if (pg_strcasecmp(e->defname, OPTION_TTL_COLUMN) == 0)
		{
			Oid type;
			ListCell *clc;

			if (!IsA(e->arg, String))
				elog(ERROR, "ttl_column must be expressed as a column name");

			/*
			 * Verify that ttl_column refers to a valid column. We'll figure out the attribute number later.
			 */
			foreach(clc, coldefs)
			{
				ColumnDef *def = (ColumnDef *) lfirst(clc);

				type = typenameTypeId(NULL, def->typeName);

				if (pg_strcasecmp(def->colname, strVal(e->arg)) == 0 &&
						(type == TIMESTAMPOID || type == TIMESTAMPTZOID))
				{
					*ttl_column = strVal(e->arg);
					break;
				}
			}

			if (!*ttl_column)
				elog(ERROR, "ttl_column must refer to a timestamp or timestamptz column");

			opt_ttl_col = e;
		}
	}

	if (!opt_ttl && !opt_ttl_col)
		return;

	if (has_sw && (opt_ttl || opt_ttl_col))
		elog(ERROR, "TTLs cannot be specified in conjunction with sliding windows");

	if (opt_ttl && !opt_ttl_col)
		elog(ERROR, "ttl_column must be specified in conjunction with ttl");

	if (opt_ttl_col && !opt_ttl)
		elog(ERROR, "ttl must be specified in conjunction with ttl_column");

	Assert(opt_ttl);
	Assert(opt_ttl_col);

	*options = list_delete(*options, opt_ttl);
	*options = list_delete(*options, opt_ttl_col);
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
	CreateSeqStmt *create_seq_stmt;
	ViewStmt *view_stmt;
	Query *query;
	RangeVar *matrel_name;
	RangeVar *seqrel_name;
	RangeVar *view;
	List *tableElts = NIL;
	ListCell *lc;
	Oid matrelid = InvalidOid;
	Oid seqrelid = InvalidOid;
	Oid overlayid = InvalidOid;
	Oid pqoid = InvalidOid;
	Oid lookup_idx_oid = InvalidOid;
	Oid pkey_idx_oid = InvalidOid;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	SelectStmt *workerselect;
	SelectStmt *viewselect;
	SelectStmt *select;
	char *cont_select_sql;
	Query *cont_query;
	bool saveAllowSystemTableMods;
	Relation pipeline_query;
	Relation overlayrel;
	Relation matrel;
	ContQuery *cv;
	Oid cvid;
	Constraint *pkey;
	DefElem *pk;
	ColumnDef *pk_coldef = NULL;
	ObjectAddress address;
	ColumnDef *old;
	ColumnDef *new;
	ColumnDef *delta;
	CreateForeignTableStmt *create_osrel;
	Oid osrelid = InvalidOid;
	bool has_sw = false;
	int ttl = -1;
	AttrNumber ttl_attno = InvalidAttrNumber;
	char *ttl_column = NULL;
	double sf = 0;

	view = stmt->into->rel;

	check_relation_already_exists(view);

	matrel_name = makeRangeVar(view->schemaname, CVNameToMatRelName(view->relname), -1);
	matrel_name->inhOpt = INH_NO;
	seqrel_name = makeRangeVar(view->schemaname, CVNameToSeqRelName(view->relname), -1);

	/*
	 * allowSystemTableMods is a global flag that, when true, allows certain column types
	 * to be created. We need it set to true to create some hidden state columns. In particular,
	 * ones with a type of anyarray.
	 */
	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	pipeline_query = heap_open(PipelineQueryRelationOid, ExclusiveLock);

	RewriteFromClause((SelectStmt *) stmt->query);

	/* Apply any CQ storage options like sw, step_factor */
	ApplyStorageOptions(stmt, &has_sw, &ttl, &ttl_column);

	ValidateParsedContQuery(stmt->into->rel, stmt->query, querystring);

	/* Deparse query so that analyzer always see the same canonicalized SelectStmt */
	cont_query = parse_analyze(copyObject(stmt->query), querystring, NULL, 0);
	cont_select_sql = deparse_query_def(cont_query);
	select = (SelectStmt *) linitial(pg_parse_query(cont_select_sql));

	sf = QueryGetSWStepFactor(cont_query);
	/*
	 * Get the transformed SelectStmt used by CQ workers. We do this
	 * because the targetList of this SelectStmt contains all columns
	 * that need to be created in the underlying matrel.
	 */
	workerselect = TransformSelectStmtForContProcess(matrel_name, copyObject(select), &viewselect, sf, Worker);

	query = parse_analyze(copyObject(workerselect), cont_select_sql, 0, 0);
	ValidateContQuery(query);

	foreach(lc, query->groupClause)
	{
		SortGroupClause *g = (SortGroupClause *) lfirst(lc);

		if (!OidIsValid(g->eqop) || !g->hashable)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("each grouping column type must be associated with an operator class"),
					 errhint("Define an operator class using CREATE OPERATOR CLASS.")));
		}
	}

	tableElts = create_coldefs_from_tlist(query);
	extract_ttl_params(&stmt->into->options, tableElts, has_sw, &ttl, &ttl_column);

	pk = GetContinuousViewOption(stmt->into->options, OPTION_PK);
	if (pk)
	{
		if (IsA(pk->arg, String))
			stmt->into->options = list_delete(stmt->into->options, pk);
		else
			elog(ERROR, "continuous view primary keys must be specified with a valid column name");

		foreach(lc, tableElts)
		{
			ColumnDef *cdef = (ColumnDef *) lfirst(lc);

			if (pg_strcasecmp(strVal(pk->arg), cdef->colname) == 0)
			{
				pk_coldef = cdef;
				break;
			}
		}

		if (!pk_coldef)
			elog(ERROR, "primary key column \"%s\" not found", strVal(pk->arg));
	}
	else
	{
		/* Add primary key column */
		pk_coldef = make_coldef(CQ_MATREL_PKEY, INT8OID, InvalidOid);
		pk_coldef->is_not_null = true;
		tableElts = lappend(tableElts, pk_coldef);
	}

	pkey = makeNode(Constraint);
	pkey->contype = CONSTR_PRIMARY;
	pk_coldef->constraints = list_make1(pkey);

	if (!GetContinuousViewOption(stmt->into->options, OPTION_FILLFACTOR))
		stmt->into->options = add_default_fillfactor(stmt->into->options);

	/*
	 * Create the actual underlying materialization relation.
	 */
	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = matrel_name;
	create_stmt->tableElts = tableElts;
	create_stmt->tablespacename = stmt->into->tableSpaceName;
	create_stmt->oncommit = stmt->into->onCommit;
	create_stmt->options = stmt->into->options;

	if (IsBinaryUpgrade)
		set_next_oids_for_matrel();
	address = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid, NULL);

	matrelid = address.objectId;
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	AlterTableCreateToastTable(matrelid, toast_options, AccessExclusiveLock);

	/* Create the sequence for primary keys */
	if (!pk)
	{
		create_seq_stmt = makeNode(CreateSeqStmt);
		create_seq_stmt->sequence = seqrel_name;

		if (IsBinaryUpgrade)
			set_next_oids_for_seqrel();
		address = DefineSequence(create_seq_stmt);

		seqrelid = address.objectId;
		CommandCounterIncrement();
	}
	else
		seqrelid = InvalidOid;

	if (ttl_column)
	{
		ttl_attno = FindTTLColumnAttrNo(ttl_column, matrelid);
	}
	else
	{
		ttl_attno = FindSWTimeColumnAttrNo(viewselect, matrelid, &ttl);
		/*
		 * has_sw will already be true if the sw storage parameter was set,
		 * but a sliding-window can still be expressed as a WHERE predicate,
		 * which will be detected here.
		 */
		if (AttributeNumberIsValid(ttl_attno))
		{
			double sf;

			has_sw = true;
			sf = QueryGetSWStepFactor(cont_query);

			if (!sf)
				sf = sliding_window_step_factor;

			QuerySetSWStepFactor(cont_query, sf);
		}
	}

	/*
	 * Now save the underlying query in the `pipeline_query` catalog relation. We don't have relid for
	 * the continuous view yet, since we need this entry for the DefineView call below to succeed.
	 * We'll update it afterwards.
	 *
	 * pqoid is the oid of the row in pipeline_query,
	 * cvid is the id of the continuous view (used in reader bitmaps)
	 */
	pqoid = DefineContinuousView(InvalidOid, cont_query, matrelid, seqrelid, ttl, ttl_attno, &cvid);
	CommandCounterIncrement();

	/* Create the view on the matrel */
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;
	view_stmt->options = lappend(view_stmt->options, makeDefElem("forcv", (Node *) makeInteger(1)));

	if (IsBinaryUpgrade)
		set_next_oids_for_overlay();

	address = DefineView(view_stmt, cont_select_sql);

	CommandCounterIncrement();

	overlayid = address.objectId;

	/*
	 * Create the output stream
	 */
	overlayrel = heap_open(overlayid, NoLock);

	old = makeNode(ColumnDef);
	old->colname = "old";
	old->typeName = makeNode(TypeName);
	old->typeName->typeOid = overlayrel->rd_rel->reltype;
	old->typeName->typemod = -1;

	new = copyObject(old);
	new->colname = "new";

	heap_close(overlayrel, NoLock);

	delta = NULL;
	if (!has_sw)
	{
		/* Only non-SW output streams have delta tuples */
		matrel = heap_open(matrelid, NoLock);

		delta = makeNode(ColumnDef);
		delta->colname = "delta";
		delta->typeName = makeNode(TypeName);
		delta->typeName->typeOid = matrel->rd_rel->reltype;
		delta->typeName->typemod = -1;

		heap_close(matrel, NoLock);
	}

	create_osrel = makeNode(CreateForeignTableStmt);
	create_osrel->servername = PIPELINEDB_SERVER;
	create_osrel->base.tableElts = list_make2(old, new);

	if (delta)
		create_osrel->base.tableElts = lappend(create_osrel->base.tableElts, delta);

	create_osrel->base.relation = makeRangeVar(view->schemaname, CVNameToOSRelName(view->relname), -1);
	transformCreateStreamStmt(create_osrel);

	if (IsBinaryUpgrade)
		set_next_oids_for_osrel();
	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_FOREIGN_TABLE, InvalidOid, NULL);

	CreateForeignTable(create_osrel, address.objectId);
	CreatePipelineStreamEntry(create_osrel, address.objectId);

	osrelid = address.objectId;

	UpdateContViewRelIds(cvid, overlayid, osrelid);
	CommandCounterIncrement();

	/* Create group look up index and record dependencies */
	if (IsBinaryUpgrade)
		set_next_oids_for_lookup_index();
	select = TransformSelectStmtForContProcess(matrel_name, copyObject(select), NULL, sf, Combiner);
	lookup_idx_oid = create_lookup_index(view, matrelid, matrel_name, select, has_sw);

	if (IsBinaryUpgrade)
		set_next_oids_for_pk_index();
	pkey_idx_oid = create_pkey_index(view, matrelid, matrel_name, pk ? strVal(pk->arg) : CQ_MATREL_PKEY);

	UpdateContViewIndexIds(cvid, pkey_idx_oid, lookup_idx_oid);
	CommandCounterIncrement();

	record_cv_dependencies(pqoid, matrelid, osrelid, seqrelid, overlayid, lookup_idx_oid, pkey_idx_oid, workerselect, query);
	allowSystemTableMods = saveAllowSystemTableMods;

	/*
	 * Run the combiner and worker queries through the planner, so that if something goes wrong
	 * we know now rather than at execution time.
	 */
	cv = GetContQueryForViewId(cvid);
	GetContPlan(cv, Combiner);
	GetContPlan(cv, Worker);
	GetCombinerLookupPlan(cv);

	heap_close(pipeline_query, NoLock);

	pgstat_report_create_drop_cv(true);
}

static void
record_ct_dependencies(Oid pqoid, Oid relid, Oid osrelid, Oid fnoid, SelectStmt *stmt, Query *query, List *args)
{
	ObjectAddress referenced;
	ObjectAddress dependent;
	ListCell *lc;
	ContAnalyzeContext cxt;

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));

	if (OidIsValid(fnoid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = fnoid;
		referenced.objectSubId = 0;

		dependent.classId = RelationRelationId;
		dependent.objectId = relid;
		dependent.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);
	}

	/*
	 * Record a dependency between the type its pipeline_query entry so that when
	 * the view is dropped the pipeline_query meta data cleanup hook is invoked.
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;

	dependent.classId = PipelineQueryRelationOid;
	dependent.objectId = pqoid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	/* Record dependency with output stream */
	dependent.classId = RelationRelationId;
	dependent.objectId = osrelid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	collect_rels_and_streams((Node *) stmt->fromClause, &cxt);

	/*
	 * Record a dependency between any strongly typed streams and a pipeline_query object,
	 * so that it is not possible to drop a stream that is being read by a CV.
	 */
	foreach(lc, cxt.streams)
	{
		RangeVar *rv;
		Oid relid;

		if (!IsA(lfirst(lc), RangeVar))
			continue;

		rv = (RangeVar *) lfirst(lc);
		relid = RangeVarGetRelid(rv, NoLock, false);

		referenced.classId = RelationRelationId;
		referenced.objectId = relid;
		referenced.objectSubId = 0;

		dependent.classId = RelationRelationId;
		dependent.objectId = relid;
		dependent.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);
	}

	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;
	recordDependencyOnExpr(&referenced, (Node *) query, NIL, DEPENDENCY_NORMAL);

	if (fnoid == PIPELINE_STREAM_INSERT_OID)
	{
		ListCell *lc;
		Relation rel = heap_open(relid, AccessShareLock);

		foreach(lc, args)
		{
			Value *v = (Value *) lfirst(lc);
			RangeVar *rv = makeRangeVarFromNameList(stringToQualifiedNameList(strVal(v)));
			Relation srel;
			TupleDesc desc;

			if (!RangeVarIsForStream(rv, true))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a stream", strVal(v)),
						errhint("Arguments to pipeline_stream_insert must be streams.")));

			srel = heap_openrv(rv, NoLock);
			desc = CreateTupleDescCopy(RelationGetDescr(srel));

			/* HACK(usmanm): Ignore arrival_timestamp. Should be fixed by #1616. */
			desc->natts--;

			if (!equalTupleDescsWeak(RelationGetDescr(rel), desc, false))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" must have the same schema as the transform", strVal(v))));

			referenced.classId = RelationRelationId;
			referenced.objectId = RelationGetRelid(srel);
			referenced.objectSubId = 0;

			dependent.classId = RelationRelationId;
			dependent.objectId = relid;
			dependent.objectSubId = 0;

			recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

			heap_close(srel, NoLock);
		}

		heap_close(rel, NoLock);
	}
}

void
ExecCreateContTransformStmt(CreateContTransformStmt *stmt, const char *querystring)
{
	RangeVar *transform;
	Relation pipeline_query;
	Query *query;
	Oid pqoid;
	ObjectAddress address;
	Oid relid;
	Oid fargtypes[1];	/* dummy */
	Oid tgfnid = InvalidOid;
	Oid funcrettype = InvalidOid;
	CreateStmt *create;
	CreateForeignTableStmt *create_osrel;
	Oid osrelid;

	transform = stmt->into->rel;
	check_relation_already_exists(transform);

	/* Find and validate the transform output function. */
	if (stmt->funcname)
	{
		tgfnid = LookupFuncName(stmt->funcname, 0, fargtypes, false);
		funcrettype = get_func_rettype(tgfnid);
		if (funcrettype != TRIGGEROID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("function %s must return type \"trigger\"",
					NameListToString(stmt->funcname))));
	}

	pipeline_query = heap_open(PipelineQueryRelationOid, ExclusiveLock);

	RewriteFromClause((SelectStmt *) stmt->query);

	ValidateParsedContQuery(stmt->into->rel, stmt->query, querystring);
	ValidateSubselect(stmt->query, "continuous transforms");

	query = parse_analyze(copyObject(stmt->query), querystring, NULL, 0);
	ValidateContQuery(query);

	create = makeNode(CreateStmt);
	create->relation = stmt->into->rel;
	create->tableElts = create_coldefs_from_tlist(query);
	create->tablespacename = stmt->into->tableSpaceName;
	create->oncommit = stmt->into->onCommit;
	create->options = stmt->into->options;

	if (IsBinaryUpgrade)
		set_next_oids_for_matrel();

	ViewStmt *vstmt = makeNode(ViewStmt);
	vstmt->view = create->relation;
	vstmt->query = stmt->query;

//	address = DefineRelation(create, RELKIND_VIEW, InvalidOid, NULL);
	address = DefineView(vstmt, querystring);
	relid = address.objectId;
	CommandCounterIncrement();

	/* Create output stream */
	create_osrel = makeNode(CreateForeignTableStmt);
	create_osrel->servername = PIPELINEDB_SERVER;
	create_osrel->base.tableElts = create_coldefs_from_tlist(query);
	create_osrel->base.relation = makeRangeVar(transform->schemaname, CVNameToOSRelName(transform->relname), -1);
	transformCreateStreamStmt(create_osrel);

	if (IsBinaryUpgrade)
		set_next_oids_for_osrel();
	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_FOREIGN_TABLE, InvalidOid, NULL);
	osrelid = address.objectId;
	CommandCounterIncrement();

	pqoid = DefineContinuousTransform(relid, query, relid, osrelid, tgfnid, stmt->args);
	CommandCounterIncrement();

	CreateForeignTable(create_osrel, address.objectId);
	CreatePipelineStreamEntry(create_osrel, address.objectId);
	CommandCounterIncrement();

	record_ct_dependencies(pqoid, relid, osrelid, tgfnid, (SelectStmt *) stmt->query, query, stmt->args);
	CommandCounterIncrement();

	heap_close(pipeline_query, NoLock);
}

/*
 * create_cq_set_next_oids_for_matrel
 */
Datum
create_cq_set_next_oids_for_matrel(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_matrel_class = PG_GETARG_OID(0);
	next_matrel_type = PG_GETARG_OID(1);
	next_matrel_array_type = PG_GETARG_OID(2);


	/* Toast (if necessary) */
	next_matrel_toast_class = PG_GETARG_OID(3);
	next_matrel_toast_type = PG_GETARG_OID(4);
	next_matrel_toast_index_class = PG_GETARG_OID(5);

	PG_RETURN_VOID();
}

/*
 * create_cv_set_next_oids_for_seqrel
 */
Datum
create_cv_set_next_oids_for_seqrel(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_seqrel_class = PG_GETARG_OID(0);
	next_seqrel_type = PG_GETARG_OID(1);

	PG_RETURN_VOID();
}

/*
 * create_cv_set_next_oids_for_pk_index
 */
Datum
create_cv_set_next_oids_for_pk_index(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_pk_index_class = PG_GETARG_OID(0);

	PG_RETURN_VOID();
}

/*
 * create_cv_set_next_oids_for_lookup_index
 */
Datum
create_cv_set_next_oids_for_lookup_index(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_lookup_index_class = PG_GETARG_OID(0);

	PG_RETURN_VOID();
}

/*
 * create_cv_set_next_oids_for_overlay
 */
Datum
create_cv_set_next_oids_for_overlay(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_overlay_type = PG_GETARG_OID(0);
	next_overlay_array_type = PG_GETARG_OID(1);
	next_overlay_class = PG_GETARG_OID(2);

	PG_RETURN_VOID();
}

/*
 * create_cq_set_next_oids_for_osrel
 */
Datum
create_cq_set_next_oids_for_osrel(PG_FUNCTION_ARGS)
{
	if (!IsBinaryUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
				 (errmsg("function can only be called when server is in binary upgrade mode"))));

	next_osrel_type = PG_GETARG_OID(0);
	next_osrel_array_type = PG_GETARG_OID(1);
	next_osrel_class = PG_GETARG_OID(2);

	PG_RETURN_VOID();
}
