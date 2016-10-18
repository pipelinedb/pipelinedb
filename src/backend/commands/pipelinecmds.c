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
#include "parser/parse_func.h"
#include "parser/parse_target.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
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
	dependent.classId = PipelineQueryRelationId;
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
	RangeVar *matrel;
	RangeVar *seqrel;
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
	ContQuery *cv;
	Oid cvid;
	Constraint *pkey;
	DefElem *pk;
	ColumnDef *pk_coldef = NULL;
	ObjectAddress address;
	ColumnDef *old;
	ColumnDef *new;
	CreateStreamStmt *create_osrel;
	Oid osrelid = InvalidOid;
	AttrNumber sw_attno = InvalidAttrNumber;

	Assert(((SelectStmt *) stmt->query)->forContinuousView);

	view = stmt->into->rel;

	check_relation_already_exists(view);

	matrel = makeRangeVar(view->schemaname, CVNameToMatRelName(view->relname), -1);
	matrel->inhOpt = INH_NO;
	seqrel = makeRangeVar(view->schemaname, CVNameToSeqRelName(view->relname), -1);

	/*
	 * allowSystemTableMods is a global flag that, when true, allows certain column types
	 * to be created. We need it set to true to create some hidden state columns. In particular,
	 * ones with a type of anyarray.
	 */
	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);

	RewriteFromClause((SelectStmt *) stmt->query);
	MakeSelectsContinuous((SelectStmt *) stmt->query);

	/* Apply any CQ storage options like max_age, step_factor */
	ApplyStorageOptions(stmt);

	ValidateParsedContQuery(stmt->into->rel, stmt->query, querystring);

	/* Deparse query so that analyzer always see the same canonicalized SelectStmt */
	cont_query = parse_analyze(copyObject(stmt->query), querystring, NULL, 0);
	cont_select_sql = deparse_query_def(cont_query);
	select = (SelectStmt *) linitial(pg_parse_query(cont_select_sql));
	select->swStepFactor = ((SelectStmt *) stmt->query)->swStepFactor;

	/*
	 * Get the transformed SelectStmt used by CQ workers. We do this
	 * because the targetList of this SelectStmt contains all columns
	 * that need to be created in the underlying matrel.
	 */
	workerselect = TransformSelectStmtForContProcess(matrel, copyObject(select), &viewselect, Worker);

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
	create_stmt->relation = matrel;
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

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);
	AlterTableCreateToastTable(matrelid, toast_options, AccessExclusiveLock);

	/* Create the sequence for primary keys */
	if (!pk)
	{
		create_seq_stmt = makeNode(CreateSeqStmt);
		create_seq_stmt->sequence = seqrel;

		if (IsBinaryUpgrade)
			set_next_oids_for_seqrel();
		address = DefineSequence(create_seq_stmt);

		seqrelid = address.objectId;
		CommandCounterIncrement();
	}
	else
		seqrelid = InvalidOid;

	sw_attno = FindSWTimeColumnAttrNo(viewselect, matrelid);

	/*
	 * Now save the underlying query in the `pipeline_query` catalog relation. We don't have relid for
	 * the continuous view yet, since we need this entry for the DefineView call below to succeed.
	 * We'll update it afterwards.
	 *
	 * pqoid is the oid of the row in pipeline_query,
	 * cvid is the id of the continuous view (used in reader bitmaps)
	 */
	pqoid = DefineContinuousView(InvalidOid, cont_query, matrelid, seqrelid, sw_attno, false, &cvid);
	CommandCounterIncrement();

	/* Create the view on the matrel */
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;
	viewselect->forContinuousView = true;

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

	create_osrel = makeNode(CreateStreamStmt);
	create_osrel->servername = PIPELINE_STREAM_SERVER;
	create_osrel->base.stream = true;
	create_osrel->base.tableElts = list_make2(old, new);
	create_osrel->base.relation = makeRangeVar(view->schemaname, CVNameToOSRelName(view->relname), -1);
	transformCreateStreamStmt(create_osrel);

	if (IsBinaryUpgrade)
		set_next_oids_for_osrel();
	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_STREAM, InvalidOid, NULL);

	CreateForeignTable((CreateForeignTableStmt *) create_osrel, address.objectId);
	CreatePipelineStreamEntry((CreateStreamStmt *) create_osrel, address.objectId);

	osrelid = address.objectId;

	UpdateContViewRelIds(cvid, overlayid, osrelid);
	CommandCounterIncrement();

	/* Create group look up index and record dependencies */
	if (IsBinaryUpgrade)
		set_next_oids_for_lookup_index();
	select = TransformSelectStmtForContProcess(matrel, copyObject(select), NULL, Combiner);
	lookup_idx_oid = create_lookup_index(view, matrelid, matrel, select, AttributeNumberIsValid(sw_attno));

	if (IsBinaryUpgrade)
		set_next_oids_for_pk_index();
	pkey_idx_oid = create_pkey_index(view, matrelid, matrel, pk ? strVal(pk->arg) : CQ_MATREL_PKEY);

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

/*
 * ExecTruncateContViewStmt
 */
void
ExecTruncateContViewStmt(TruncateContViewStmt *stmt)
{
	ListCell *lc;
	Relation pipeline_query;
	List *views = NIL;
	TruncateStmt *trunc = makeNode(TruncateStmt);

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
		matrel = GetMatRelName(rv);

		trunc->relations = lappend(trunc->relations, matrel);
	}

	trunc->restart_seqs = stmt->restart_seqs;
	trunc->behavior = stmt->behavior;

	/* Reset all CQ level transition state */
	foreach(lc, views)
		ResetTStateEntry(lfirst_oid(lc));

	/* Call TRUNCATE on the backing view table(s). */
	ExecuteTruncate(trunc);

	heap_close(pipeline_query, NoLock);
}

static void
explain_cont_plan(char *name, PlannedStmt *plan, ExplainState *base_es, TupleDesc desc, DestReceiver *dest)
{
	TupOutputState *tstate;
	ExplainState es;

	Assert(plan);

	memcpy(&es, base_es, sizeof(ExplainState));
	es.str = makeStringInfo();
	es.indent = 1;
	appendStringInfoString(es.str, name);
	appendStringInfoString(es.str, ":\n");

	/* emit opening boilerplate */
	ExplainBeginOutput(&es);

	ExplainOnePlan(plan, NULL, &es, NULL, NULL, NULL);

	/* emit closing boilerplate */
	ExplainEndOutput(&es);
	Assert(es.indent == 1);

	/* Non text formats only return single line inputs so do have a new line at the end */
	if (es.format != EXPLAIN_FORMAT_TEXT)
		appendStringInfoChar(es.str, '\n');

	/* output tuples */
	tstate = begin_tup_output_tupdesc(dest, desc);
	do_text_output_multiline(tstate, es.str->data);
	end_tup_output(tstate);

	pfree(es.str->data);
	pfree(es.str);
}

/*
 * ExplainContViewResultDesc
 */
TupleDesc
ExplainContViewResultDesc(ExplainContQueryStmt *stmt)
{
	ExplainStmt *explain = makeNode(ExplainStmt);
	TupleDesc desc;

	explain->options = stmt->options;
	desc = ExplainResultDesc(explain);
	pfree(explain);

	Assert(desc->natts == 1);
	namestrcpy(&(desc->attrs[0]->attname), "CONTINUOUS QUERY PLANS");

	return desc;
}

/*
 * ExecExplainContViewStmt
 */
void
ExecExplainContQueryStmt(ExplainContQueryStmt *stmt, const char *queryString,
			 ParamListInfo params, DestReceiver *dest)
{
	ExplainState *es = NewExplainState();
	ListCell *lc;
	TupleDesc desc;
	ContQuery *cq;
	HeapTuple tuple = GetPipelineQueryTuple(stmt->view);
	Oid cq_id;
	Form_pipeline_query row;
	PlannedStmt *plan;
	Tuplestorestate *tupstore;
	TuplestoreScan *scan;
	Relation rel;
	char *objname;

	Assert(stmt->objType == OBJECT_CONTVIEW || stmt->objType == OBJECT_CONTTRANSFORM);
	objname = stmt->objType == OBJECT_CONTVIEW ? "continuous view" : "continuous transform";

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("%s \"%s\" does not exist", objname, stmt->view->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);
	cq_id = row->id;
	ReleaseSysCache(tuple);

	if (stmt->objType == OBJECT_CONTVIEW && row->type != PIPELINE_QUERY_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("\"%s\" is not a continuous view", stmt->view->relname)));

	if (stmt->objType == OBJECT_CONTTRANSFORM && row->type != PIPELINE_QUERY_TRANSFORM)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("\"%s\" is not a continuous transform", stmt->view->relname)));

	/* Initialize ExplainState. */
	es->format = EXPLAIN_FORMAT_TEXT;
	pfree(es->str);

	/* Parse options list. */
	foreach(lc, stmt->options)
	{
		DefElem *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "verbose") == 0)
			es->verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "costs") == 0)
			es->costs = defGetBoolean(opt);
		else if (strcmp(opt->defname, "format") == 0)
		{
			char *p = defGetString(opt);

			if (strcmp(p, "text") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unrecognized value for EXPLAIN CONTINUOUS VIEW option \"%s\": \"%s\"",
								opt->defname, p)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized EXPLAIN CONTINUOUS VIEW option \"%s\"",
							opt->defname)));
	}

	desc = ExplainContViewResultDesc(stmt);

	cq = GetContQueryForId(cq_id);

	explain_cont_plan("Worker Plan", GetContPlan(cq, Worker), es, desc, dest);

	if (stmt->objType == OBJECT_CONTVIEW)
	{
		plan = GetContPlan(cq, Combiner);
		tupstore = tuplestore_begin_heap(false, false, work_mem);
		scan = SetCombinerPlanTuplestorestate(plan, tupstore);
		rel = relation_openrv(cq->matrel, NoLock);
		scan->desc = CreateTupleDescCopy(RelationGetDescr(rel));
		relation_close(rel, NoLock);
		explain_cont_plan("Combiner Plan", plan, es, desc, dest);
		tuplestore_end(tupstore);

		explain_cont_plan("Combiner Lookup Plan", GetCombinerLookupPlan(cq), es, desc, dest);
	}
}

static Bitmapset *
get_query_ids(List *queries)
{
	Bitmapset *ids = NULL;
	ListCell *lc;

	if (queries == NIL)
		return GetContinuousQueryIds();

	foreach(lc, queries)
	{
		RangeVar *rv = lfirst(lc);
		Oid id = GetContQueryId(rv);

		if (!OidIsValid(id))
			ereport(ERROR,
					(errmsg("continuous query \"%s\" does not exist", rv->relname)));

		ids = bms_add_member(ids, id);
	}

	return ids;
}

static void
set_cq_enabled(List *queries, bool activate)
{
	bool changed = false;
	int query_id;
	Bitmapset *query_ids;
	Relation pipeline_query;

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);

	query_ids = get_query_ids(queries);

	while ((query_id = bms_first_member(query_ids)) >= 0)
	{
		Assert(OidIsValid(query_id));
		changed |= ContQuerySetActive(query_id, activate);
	}

	if (changed)
		UpdatePipelineStreamCatalog();

	heap_close(pipeline_query, NoLock);
}

void
ExecActivateStmt(ActivateStmt *stmt)
{
	set_cq_enabled(stmt->queries, true);
}

void
ExecDeactivateStmt(DeactivateStmt *stmt)
{
	set_cq_enabled(stmt->queries, false);
}

static void
record_ct_dependencies(Oid pqoid, Oid relid, Oid osrelid, Oid fnoid, SelectStmt *stmt, Query *query, List *args)
{
	ObjectAddress referenced;
	ObjectAddress dependent;
	ListCell *lc;
	ContAnalyzeContext cxt;

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));

	referenced.classId = ProcedureRelationId;
	referenced.objectId = fnoid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = relid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	/*
	 * Record a dependency between the type its pipeline_query entry so that when
	 * the view is dropped the pipeline_query meta data cleanup hook is invoked.
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;

	dependent.classId = PipelineQueryRelationId;
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

			if (!RangeVarIsForStream(rv))
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
	Oid tgfnid;
	Oid funcrettype;
	CreateStmt *create;
	CreateStreamStmt *create_osrel;
	Oid osrelid;

	Assert(((SelectStmt *) stmt->query)->forContinuousView);

	transform = stmt->into->rel;
	check_relation_already_exists(transform);

	/* Find and validate the transform output function. */
	tgfnid = LookupFuncName(stmt->funcname, 0, fargtypes, false);
	funcrettype = get_func_rettype(tgfnid);
	if (funcrettype != TRIGGEROID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("function %s must return type \"trigger\"",
				NameListToString(stmt->funcname))));

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);

	RewriteFromClause((SelectStmt *) stmt->query);
	MakeSelectsContinuous((SelectStmt *) stmt->query);

	ValidateParsedContQuery(stmt->into->rel, stmt->query, querystring);
	ValidateSubselect(stmt->query, "continuous transforms");

	query = parse_analyze(copyObject(stmt->query), querystring, NULL, 0);

	create = makeNode(CreateStmt);
	create->relation = stmt->into->rel;
	create->tableElts = create_coldefs_from_tlist(query);
	create->tablespacename = stmt->into->tableSpaceName;
	create->oncommit = stmt->into->onCommit;
	create->options = stmt->into->options;

	if (IsBinaryUpgrade)
		set_next_oids_for_matrel();
	address = DefineRelation(create, RELKIND_CONTTRANSFORM, InvalidOid, NULL);
	relid = address.objectId;
	CommandCounterIncrement();

	/* Create output stream */
	create_osrel = makeNode(CreateStreamStmt);
	create_osrel->servername = PIPELINE_STREAM_SERVER;
	create_osrel->base.stream = true;
	create_osrel->base.tableElts = create_coldefs_from_tlist(query);
	create_osrel->base.relation = makeRangeVar(transform->schemaname, CVNameToOSRelName(transform->relname), -1);
	transformCreateStreamStmt(create_osrel);

	if (IsBinaryUpgrade)
		set_next_oids_for_osrel();
	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_STREAM, InvalidOid, NULL);
	osrelid = address.objectId;
	CommandCounterIncrement();

	pqoid = DefineContinuousTransform(relid, query, relid, osrelid, tgfnid, false, stmt->args);
	CommandCounterIncrement();

	CreateForeignTable((CreateForeignTableStmt *) create_osrel, address.objectId);
	CreatePipelineStreamEntry((CreateStreamStmt *) create_osrel, address.objectId);
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
