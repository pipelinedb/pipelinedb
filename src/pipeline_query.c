/*-------------------------------------------------------------------------
 *
 * pipeline_query.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "analyzer.h"
#include "catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/alter.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "compat.h"
#include "executor/spi.h"
#include "matrel.h"
#include "miscadmin.h"
#include "miscutils.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "planner.h"
#include "rewrite/rewriteHandler.h"
#include "ruleutils.h"
#include "stats.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#define CQ_MATREL_INDEX_TYPE "btree"
#define is_sw(row) ((row)->step_factor > 0)

Oid PipelineQueryRelationOid;

int continuous_view_fillfactor;

/*
 * compare_oid
 */
static int
compare_oid(const void *a, const void *b)
{
  const Oid *ia = (const Oid *) a;
  const Oid *ib = (const Oid *) b;

  return (*ia > *ib) - (*ia < *ib);
}

/*
 * get_next_id
 *
 * Gets the smallest possible id to assign to the next continuous view.
 * We keep this minimal so that we can minimize the size of bitmaps used
 * to tag stream buffer events with.
 */
static Oid
get_next_id(Relation rel)
{
	HeapScanDesc scandesc;
	HeapTuple tup;
	List *ids_list = NIL;
	int num_ids;

	Assert(MAX_CQS % 32 == 0);

	scandesc = heap_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		ids_list = lappend_oid(ids_list, row->id);
	}

	heap_endscan(scandesc);

	num_ids = list_length(ids_list);

	if (num_ids)
	{
		Oid ids[num_ids];
		int counts_per_combiner[num_combiners];
		int i = 0;
		Oid max;
		ListCell *lc;
		int j;
		int target_combiner;
		List *potential_ids;

		MemSet(counts_per_combiner, 0, sizeof(counts_per_combiner));

		foreach(lc, ids_list)
		{
			ids[i] = lfirst_oid(lc);
			counts_per_combiner[ids[i] % num_combiners] += 1;
			i++;
		}

		qsort(ids, num_ids, sizeof(Oid), &compare_oid);

		if (num_ids == MAX_CQS - 1) /* -1 because 0 is an invalid id */
			elog(ERROR, "maximum number of continuous queries exceeded");

		max = ids[num_ids - 1];
		Assert(max >= num_ids);

		/*
		 * FIXME(usmanm): We do some randomization of ID generation here to make sure that CQs that
		 * are created and dropped in quick succession don't read an event that was not for them.
		 */

		/*
		 * Collect any unused ids in [1, max].
		 */
		list_free(ids_list);
		ids_list = NIL;

		for (i = 1, j = 0; j < num_ids; i++)
		{
			if (ids[j] > i)
				ids_list = lappend_oid(ids_list, (Oid) i);
			else
				j++;
		}

		/*
		 * Add all IDs between max and the next multiple of 32.
		 */
		j = Min((max / 32 + 1) * 32, MAX_CQS);
		for (i = max + 1; i < j; i++)
			ids_list = lappend_oid(ids_list, (Oid) i);

		/*
		 * Less than 16 options? Throw in some more.
		 */
		if (list_length(ids_list) < 16 && j < MAX_CQS)
			for (i = j; i < j + 32; i++)
				ids_list = lappend_oid(ids_list, (Oid) i);

		/*
		 * Figure out the target combiner (one with least IDs allocated) and try to allocate
		 * an ID that belongs to it.
		 */
		target_combiner = 0;
		for (i = 0; i < num_combiners; i++)
			if (counts_per_combiner[i] < counts_per_combiner[target_combiner])
				target_combiner = i;

		potential_ids = NIL;
		foreach(lc, ids_list)
		{
			Oid id = lfirst_oid(lc);
			if (id % num_combiners == target_combiner)
				potential_ids = lappend_oid(potential_ids, id);
		}

		if (list_length(potential_ids))
			return list_nth_oid(potential_ids, rand() % list_length(potential_ids));

		return list_nth_oid(ids_list, rand() % list_length(ids_list));
	}

	/*
	 * No CVs exist, give any id in [1, 16).
	 */
	return (rand() % 15) + 1;
}

/*
 * define_defrel
 */
static Oid
define_defrel(ViewStmt *vstmt, const char *sql)
{
	ObjectAddress address;
	Oid result = InvalidOid;

	PG_TRY();
	{
		PipelineContextSetIsDefRel(true);
		address = DefineView(vstmt, sql, -1, 0);
		result = address.objectId;
		PipelineContextSetIsDefRel(false);
	}
	PG_CATCH();
	{
		PipelineContextSetIsDefRel(false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * set_option
 */
static List *
set_option(List *options, char *option, Node *value)
{
	ListCell *lc;
	DefElem *opt = NULL;

	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, option) == 0)
		{
			opt = def;
			break;
		}
	}

	if (opt)
		opt->arg = value;
	else
		options = lappend(options, makeDefElem(option, value, -1));

	return options;
}

/*
 * get_qualified_relname
 */
static char *
get_qualified_relname(Oid relid)
{
	StringInfoData buf;

	char *relname = get_rel_name(relid);
	char *namespace;

	if (!relname)
		return NULL;

	namespace = get_namespace_name(get_rel_namespace(relid));

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s.%s", namespace, relname);

	return buf.data;
}

/*
 * add_options_for_dump
 *
 * Add the OIDs necessary to support future dump/restore if they aren't already present in the given options List
 */
static List *
add_options_for_dump(char *cvname, Oid streamrelid, Oid viewrelid, Oid matrelid, Oid seqrelid, Oid osrelid,
		Oid pkindexid, Oid lookupindexid, int ttl, AttrNumber ttl_attno, double step_factor, List *options)
{
	char *relname;

	/* We should always have these options */
	options = set_option(options, OPTION_CV, (Node *) makeString(pstrdup(cvname)));

	relname = get_rel_name(streamrelid);
	Assert(relname);

	/*
	 * The input stream can be in a different schema from all of the CV's dependent relations,
	 * so we make sure it's qualified here.
	 */
	relname = get_qualified_relname(streamrelid);
	options = set_option(options, OPTION_STREAM, (Node *) makeString(pstrdup(relname)));

	relname = get_rel_name(matrelid);
	Assert(relname);
	options = set_option(options, OPTION_MATREL, (Node *) makeString(pstrdup(relname)));

	relname = get_rel_name(viewrelid);
	Assert(relname);
	options = set_option(options, OPTION_OVERLAY, (Node *) makeString(pstrdup(relname)));

	relname = get_rel_name(osrelid);
	Assert(relname);
	options = set_option(options, OPTION_OSREL, (Node *) makeString(pstrdup(relname)));

	/* We may not always have these though */
	relname = get_rel_name(seqrelid);
	if (relname)
		options = set_option(options, OPTION_SEQREL, (Node *) makeString(pstrdup(relname)));

	relname = get_rel_name(pkindexid);
	if (relname)
		options = set_option(options, OPTION_PKINDEX, (Node *) makeString(pstrdup(relname)));

	relname = get_rel_name(lookupindexid);
	if (relname)
		options = set_option(options, OPTION_LOOKUPINDEX, (Node *) makeString(pstrdup(relname)));

	if (ttl > 0)
	{
		options = set_option(options, OPTION_TTL, (Node *) makeInteger(ttl));
		options = set_option(options, OPTION_TTL_ATTNO, (Node *) makeInteger(ttl_attno));

		/*
		 * Only sliding windows have step_factor
		 */
		if (step_factor)
		{
			char *s = float8out_internal(step_factor);
			options = set_option(options, OPTION_STEP_FACTOR, (Node *) makeFloat(s));
		}
	}

	return options;
}

/*
 * GetContQueryDef
 */
Query *
GetContQueryDef(Oid defrelid)
{
	Relation rel;
	Query *result;

	rel = heap_open(defrelid, NoLock);
	result = copyObject(get_view_query(rel));
	heap_close(rel, NoLock);

	return result;
}

/*
 * AcquirePipelineDDLLock
 */
PipelineDDLLock
AcquirePipelineDDLLock(void)
{
	if (IsBinaryUpgrade)
		return NULL;
	return heap_open(GetPipelineQueryOid(), AccessExclusiveLock);
}

/*
 * ReleasePipelineDDLLock
 */
void
ReleasePipelineDDLLock(PipelineDDLLock lock)
{
	heap_close(lock, NoLock);
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
			if (*ttl == 0)
				elog(ERROR, "ttl must be a minimum of 1 second");
		}
		else if (pg_strcasecmp(e->defname, OPTION_TTL_COLUMN) == 0)
		{
			Oid type;
			ListCell *clc;

			if (!IsA(e->arg, String) && !IsA(e->arg, TypeName))
				elog(ERROR, "ttl_column must be expressed as a column name");

			/*
			 * Verify that ttl_column refers to a valid column. We'll figure out the attribute number later.
			 */
			foreach(clc, coldefs)
			{
				ColumnDef *def = (ColumnDef *) lfirst(clc);
				char *name;

				type = typenameTypeId(NULL, def->typeName);

				if (IsA(e->arg, TypeName))
				{
					TypeName *t = (TypeName *) e->arg;
					name = NameListToString(t->names);
				}
				else
				{
					name = strVal(e->arg);
				}

				if (pg_strcasecmp(def->colname, name) == 0 &&
						(type == TIMESTAMPOID || type == TIMESTAMPTZOID))
				{
					*ttl_column = name;
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
 * RelidIsMatRel
 *
 * Returns true if the given oid represents a materialization table
 */
bool
RelidIsMatRel(Oid relid, Oid *id)
{
	HeapTuple tup;

	tup = PipelineCatalogLookup(PIPELINEQUERYMATRELID, 1, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
	{
		if (id)
			*id = InvalidOid;
		return false;
	}

	if (id)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		*id = row->id;
	}

	return true;
}

/*
 * StorePipelineQueryReloptions
 *
 * Stores the given custom options in the pg_class catalog
 */
void
StorePipelineQueryReloptions(Oid relid, List *options)
{
	Datum classopts;
	HeapTuple classtup;
	HeapTuple newtup;
	Relation pgclass;
	Datum class_values[Natts_pg_class];
	bool class_null[Natts_pg_class];
	bool class_repl[Natts_pg_class];

	pgclass = heap_open(RelationRelationId, RowExclusiveLock);
	classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for OID %u", relid);

	classopts = transformRelOptions((Datum) 0, options, NULL, NULL, false, false);
	view_reloptions(classopts, false);

	MemSet(class_values, 0, sizeof(class_values));
	MemSet(class_null, 0, sizeof(class_null));
	MemSet(class_repl, 0, sizeof(class_repl));

	class_values[Anum_pg_class_reloptions - 1] = classopts;
	class_repl[Anum_pg_class_reloptions - 1] = true;

	newtup = heap_modify_tuple(classtup, RelationGetDescr(pgclass), class_values, class_null, class_repl);
	PipelineCatalogTupleUpdate(pgclass, &newtup->t_self, newtup);

	heap_freetuple(newtup);
	heap_close(pgclass, NoLock);
	ReleaseSysCache(classtup);
}

/*
 * record_cv_dependencies
 */
static void
record_cv_dependencies(Oid matreloid, Oid osreloid, Oid viewoid,
		Oid indexoid, Oid pkey_idxoid, Oid defreloid, Oid seqreloid, SelectStmt *stmt, Query *query)
{
	ObjectAddress referenced;
	ObjectAddress dependent;

	/* This CV depends on its corresponding pipelinedb.cont_query row */
	dependent.classId = RelationRelationId;
	dependent.objectId = viewoid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineQueryOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	/*
	 * We'll also have an entry as a reader in pipelinedb.stream so we're dependent on that catalog as well
	 */
	dependent.classId = RelationRelationId;
	dependent.objectId = viewoid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineStreamOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	/*
	 * The view will already have a dependency on the underlying matrel it reads,
	 * but creating an explicit internal dependency here ensures our dump output
	 * ordering is exactly how we want it
	 */
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

	/* Record dependency on sequence used for pkey, if there is one */
	if (OidIsValid(seqreloid))
	{
		dependent.classId = RelationRelationId;
		dependent.objectId = pkey_idxoid;
		dependent.objectSubId = 0;

		referenced.classId = RelationRelationId;
		referenced.objectId = seqreloid;
		referenced.objectSubId = 0;

		recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);
	}

	/*
	 * Record a dependency between the CV and all referenced objects in its query definition
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = viewoid;
	referenced.objectSubId = 0;
	recordDependencyOnExpr(&referenced, (Node *) query, NIL, DEPENDENCY_NORMAL);

	/*
	 * These dependencies ensure that these relations are dumped before the defrel for dump/restore
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = matreloid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = defreloid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	referenced.classId = RelationRelationId;
	referenced.objectId = osreloid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = defreloid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);
}

/*
 * make_coldef
 */
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

/*
 * create_coldefs_from_tlist
 */
static List *
create_coldefs_from_tlist(Query *query)
{
	ListCell *lc;
	List *defs = NIL;
	int count = 0;

	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		char *colname;
		Oid type;
		ColumnDef *coldef;

		count++;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);

		if (!pg_strcasecmp(colname, CQ_MATREL_PKEY))
			elog(ERROR, "\"%s\" is a reserved column name", colname);

		type = exprType((Node *) tle->expr);

		/* Replace void type with a bool type. We need this because of the use of pg_sleep in some CQ tests */
		if (type == VOIDOID)
			type = BOOLOID;

		coldef = make_coldef(colname, type, exprTypmod((Node *) tle->expr));
		defs = lappend(defs, coldef);
	}

	return defs;
}

/*
 * make_default_fillfactor
 *
 * Return a fillfactor DefElem with the default fillfactor
 */
static List *
add_default_fillfactor(List *options)
{
	DefElem *ff = makeDefElem(OPTION_FILLFACTOR, (Node *) makeInteger(continuous_view_fillfactor), -1);

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
	Oid hashoid = GetHashGroupOid();
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
			attr = (Form_pg_attribute) TupleDescAttr(desc, i);
			if (pg_strcasecmp(name, NameStr(attr->attname)) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
			elog(ERROR, "could not find index attribute in tuple descriptor");

		if (TypeCategory(attr->atttypid) == TYPCATEGORY_DATETIME)
			hashoid = GetLSHashGroupOid();

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

/*
 * create_lookup_index
 */
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

#if (PG_VERSION_NUM < 110000)
	address = DefineIndex(matrelid, index, InvalidOid, false, false, false, false, false);
#else
	address = DefineIndex(matrelid, index, InvalidOid, InvalidOid, InvalidOid, false, false, false, false, false);
#endif
	index_oid = address.objectId;

	return index_oid;
}

/*
 * create_pkey_index
 */
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

#if (PG_VERSION_NUM < 110000)
	address = DefineIndex(matrelid, index, InvalidOid, false, false, false, false, false);
#else
	address = DefineIndex(matrelid, index, InvalidOid, InvalidOid, InvalidOid, false, false, false, false, false);
#endif

	index_oid = address.objectId;

	return index_oid;
}

/*
 * DefineContView
 *
 * Adds a CV to the `pipeline_query` catalog table.
 */
Oid
DefineContView(Relation pipeline_query, Oid relid, Oid streamrelid, Oid matrelid, Oid seqrelid, int ttl,
		AttrNumber ttl_attno, double step_factor, Oid *pq_id)
{
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	Oid id;
	Oid result;

	id = get_next_id(pipeline_query);

	Assert(OidIsValid(id));

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_query_type - 1] = CharGetDatum(PIPELINE_QUERY_VIEW);

	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pipeline_query_active - 1] = BoolGetDatum(continuous_queries_enabled);
	values[Anum_pipeline_query_streamrelid - 1] = ObjectIdGetDatum(streamrelid);
	values[Anum_pipeline_query_matrelid - 1] = ObjectIdGetDatum(matrelid);
	values[Anum_pipeline_query_seqrelid - 1] = ObjectIdGetDatum(seqrelid);
	values[Anum_pipeline_query_ttl - 1] = Int32GetDatum(ttl);
	values[Anum_pipeline_query_ttl_attno - 1] = Int16GetDatum(ttl_attno);
	values[Anum_pipeline_query_step_factor - 1] = Int16GetDatum((int16) step_factor);

	/* unused */
	values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(0);
	values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(""));

	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);
	result = PipelineCatalogTupleInsert(pipeline_query, tup);

	*pq_id = id;

	return result;
}

/*
 * DefineContTransform
 */
Oid
DefineContTransform(Oid relid, Oid defrelid, Oid streamrelid, Oid typoid, Oid osrelid, List **optionsp, Oid *ptgfnid)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	Oid id;
	Oid result;
	char *tgs;
	int nargs;
	char *funcname;
	char *relname;
	List *options = *optionsp;

	pipeline_query = heap_open(PipelineQueryRelationOid, RowExclusiveLock);

	id = get_next_id(pipeline_query);

	Assert(OidIsValid(id));

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_query_type - 1] = Int8GetDatum(PIPELINE_QUERY_TRANSFORM);

	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pipeline_query_defrelid - 1] = ObjectIdGetDatum(defrelid);
	values[Anum_pipeline_query_active - 1] = BoolGetDatum(continuous_queries_enabled);
	values[Anum_pipeline_query_streamrelid - 1] = ObjectIdGetDatum(streamrelid);
	values[Anum_pipeline_query_matrelid - 1] = ObjectIdGetDatum(typoid); /* HACK(usmanm): So matrel index works */
	values[Anum_pipeline_query_osrelid - 1] = ObjectIdGetDatum(osrelid);
	values[Anum_pipeline_query_pkidxid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pipeline_query_lookupidxid - 1] = ObjectIdGetDatum(InvalidOid);

	options = lappend(options, makeDefElem(OPTION_ACTION, (Node *) makeString(ACTION_TRANSFORM), -1));

	relname = get_rel_name(osrelid);
	Assert(relname);
	options = lappend(options, makeDefElem(OPTION_OSREL, (Node *) makeString(pstrdup(relname)), -1));

	relname = get_qualified_relname(streamrelid);
	Assert(relname);
	options = lappend(options, makeDefElem(OPTION_STREAM, (Node *) makeString(pstrdup(relname)), -1));

	relname = get_rel_name(relid);
	Assert(relname);
	options = lappend(options, makeDefElem(OPTION_TRANSFORM, (Node *) makeString(pstrdup(relname)), -1));

	if (ptgfnid)
		*ptgfnid = InvalidOid;

	if (GetOptionAsString(options, OPTION_TGFN, &funcname))
	{
		Oid fargtypes[1];
		Oid tgfnoid = InvalidOid;

		tgfnoid = LookupFuncName(textToQualifiedNameList((text *) CStringGetTextDatum(funcname)), 0, fargtypes, false);

		/*
		 * We store the trigger function by qualified name for dump/restore support.
		 * The OID will change across restores (even for binary upgrades) and there isn't a good place in which
		 * to sync it with the new OID in these cases so we just retrieve the OID via function name when we need to use it.
		 */
		if (OidIsValid(tgfnoid))
		{
			Form_pg_proc form;
			HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(tgfnoid));
			List *qualified = NIL;
			DefElem *opt;

			form = (Form_pg_proc) GETSTRUCT(tup);

			qualified = list_make2(makeString(get_namespace_name(form->pronamespace)), makeString(NameStr(form->proname)));
			opt = GetContQueryOption(options, OPTION_TGFN);
			Assert(opt);

			opt->arg = (Node *) makeString(NameListToQuotedString(qualified));
			ReleaseSysCache(tup);
		}
		*ptgfnid = tgfnoid;
	}

	if (GetOptionAsString(options, OPTION_TGARGS, &tgs))
		values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(tgs));
	else
		values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(""));

	if (GetOptionAsInteger(options, OPTION_TGNARGS, &nargs))
		values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(nargs);
	else
		values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(0);

	/* unused */
	values[Anum_pipeline_query_seqrelid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pipeline_query_ttl - 1] = Int32GetDatum(-1);
	values[Anum_pipeline_query_ttl_attno - 1] = Int16GetDatum(InvalidAttrNumber);
	values[Anum_pipeline_query_step_factor - 1] = Int16GetDatum(0);

	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	result = PipelineCatalogTupleInsert(pipeline_query, tup);
	CommandCounterIncrement();

	heap_freetuple(tup);

	SyncPipelineStreamReaders();

	heap_close(pipeline_query, NoLock);

	CommandCounterIncrement();

	*optionsp = options;

	return result;
}

/*
 * UpdateContViewRelIds
 */
void
UpdateContViewRelIds(Relation pipeline_query, Oid cvid, Oid cvrelid, Oid defrelid, Oid osrelid, List *options)
{
	bool replace[Natts_pipeline_query];
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	HeapTuple tup;
	HeapTuple new;

	MemSet(replace, 0 , sizeof(replace));
	MemSet(nulls, 0 , sizeof(nulls));
	replace[Anum_pipeline_query_relid - 1] = true;
	replace[Anum_pipeline_query_defrelid - 1] = true;
	replace[Anum_pipeline_query_osrelid - 1] = true;
	replace[Anum_pipeline_query_pkidxid - 1] = true;
	replace[Anum_pipeline_query_lookupidxid - 1] = true;

	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(cvrelid);
	values[Anum_pipeline_query_defrelid - 1] = ObjectIdGetDatum(defrelid);
	values[Anum_pipeline_query_osrelid - 1] = ObjectIdGetDatum(osrelid);

	tup = PipelineCatalogLookupForUpdate(pipeline_query, PIPELINEQUERYID, ObjectIdGetDatum(cvid));
	Assert(HeapTupleIsValid(tup));

	new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);
	PipelineCatalogTupleUpdate(pipeline_query, &new->t_self, new);
}

/*
 * UpdateContViewIndexIds
 */
void
UpdateContViewIndexIds(Relation pipeline_query, Oid cvid, Oid pkindid, Oid lookupindid, Oid seqrelid)
{
	bool replace[Natts_pipeline_query];
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	HeapTuple tup;
	HeapTuple new;

	MemSet(replace, 0 , sizeof(replace));
	MemSet(nulls, 0 , sizeof(nulls));
	replace[Anum_pipeline_query_pkidxid - 1] = true;
	replace[Anum_pipeline_query_lookupidxid - 1] = true;
	replace[Anum_pipeline_query_seqrelid - 1] = true;

	values[Anum_pipeline_query_pkidxid - 1] = ObjectIdGetDatum(pkindid);
	values[Anum_pipeline_query_lookupidxid - 1] = ObjectIdGetDatum(lookupindid);
	values[Anum_pipeline_query_seqrelid - 1] = ObjectIdGetDatum(seqrelid);

	tup = PipelineCatalogLookupForUpdate(pipeline_query, PIPELINEQUERYID, ObjectIdGetDatum(cvid));
	Assert(HeapTupleIsValid(tup));

	new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);
	PipelineCatalogTupleUpdate(pipeline_query, &new->t_self, new);
}

/*
 * is_dumped_cv_defrel
 */
static bool
is_dumped_cv_defrel(List *options)
{
	return GetContQueryOption(options, OPTION_CV) != NULL;
}

/*
 * get_relid_from_qualified_relname
 */
static Oid
get_relid_from_qualified_relname(char *qualified)
{
	List *name = textToQualifiedNameList((text *) CStringGetTextDatum(qualified));
	RangeVar *rv = makeRangeVarFromNameList(name);

	return RangeVarGetRelid(rv, NoLock, false);
}

/*
 * create_cv_row_from_dump
 */
static Oid
create_cv_row_from_dump(Relation pq, RangeVar *name, Query *query, List *options)
{
	Oid nspid;
	Oid overlayrelid = InvalidOid;
	Oid streamrelid = InvalidOid;
	Oid matrelid = InvalidOid;
	Oid osrelid = InvalidOid;
	Oid seqrelid = InvalidOid;
	Oid pkrelid = InvalidOid;
	Oid lookuprelid = InvalidOid;
	Oid defrelid = InvalidOid;
	int ttl_attno = InvalidAttrNumber;
	int ttl = -1;
	double sf = 0.0;
	char *relname;
	Oid pqid = InvalidOid;

	if (!name->schemaname)
		nspid = RangeVarGetCreationNamespace(name);
	else
		nspid = get_namespace_oid(name->schemaname, false);

	/*
	 * We should always have the following options
	 */
	if (!GetOptionAsString(options, OPTION_STREAM, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_STREAM);
	streamrelid = get_relid_from_qualified_relname(relname);

	/*
	 * The remaining dependent relations will always be in the same schema as the CV
	 */
	if (!GetOptionAsString(options, OPTION_MATREL, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_MATREL);
	matrelid = get_relname_relid(relname, nspid);

	if (!GetOptionAsString(options, OPTION_OVERLAY, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_OVERLAY);
	overlayrelid = get_relname_relid(relname, nspid);

	if (!GetOptionAsString(options, OPTION_OSREL, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_OSREL);
	osrelid = get_relname_relid(relname, nspid);

	defrelid = RangeVarGetRelid(name, NoLock, false);

	/*
	 * But these options won't always be present
	 */
	if (GetOptionAsString(options, OPTION_SEQREL, &relname))
		seqrelid = get_relname_relid(relname, nspid);

	if (GetOptionAsString(options, OPTION_PKINDEX, &relname))
		pkrelid = get_relname_relid(relname, nspid);

	if (GetOptionAsString(options, OPTION_LOOKUPINDEX, &relname))
		lookuprelid = get_relname_relid(relname, nspid);

	/*
	 * TTL options
	 */
	if (GetOptionAsInteger(options, OPTION_TTL, &ttl))
	{
		if (!GetOptionAsInteger(options, OPTION_TTL_ATTNO, &ttl_attno))
			elog(ERROR, "TTL specified without TTL column");

		if (GetOptionAsDouble(options, OPTION_STEP_FACTOR, &sf))
			QuerySetSWStepFactor(query, sf);
	}

	DefineContView(pq, overlayrelid, streamrelid, matrelid, seqrelid, ttl, ttl_attno, sf, &pqid);
	CommandCounterIncrement();

	UpdateContViewRelIds(pq, pqid, overlayrelid, defrelid, osrelid, options);
	CommandCounterIncrement();

	UpdateContViewIndexIds(pq, pqid, pkrelid, lookuprelid, seqrelid);
	CommandCounterIncrement();

	return InvalidOid;
}

/*
 * create_dumped_cv
 */
static void
create_dumped_cv(Relation pq, RangeVar *name, const char *sql, SelectStmt *stmt, Query *query, List *options)
{
	ViewStmt *vstmt;
	Oid defrelid;

	/* Create the actual view  */
	vstmt = makeNode(ViewStmt);
	vstmt->view = name;
	vstmt->query = (Node *) stmt;

	defrelid = define_defrel(vstmt, sql);
	CommandCounterIncrement();

	StorePipelineQueryReloptions(defrelid, options);

	/*
	 * Create the cont_query row. If we're doing a binary upgrade the row will already be present.
	 */
	if (!IsBinaryUpgrade)
		create_cv_row_from_dump(pq, name, query, options);
}


/*
 * record_ct_dependencies
 */
static void
record_ct_dependencies(Oid relid, Oid osrelid, Oid fnoid, Oid defrelid, SelectStmt *stmt, Query *query, List *args)
{
	ObjectAddress referenced;
	ObjectAddress dependent;

	/* This CT depends on its corresponding pipelinedb.cont_query row */
	dependent.classId = RelationRelationId;
	dependent.objectId = relid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineQueryOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	/*
	 * We'll also have an entry as a reader in pipelinedb.stream so we're dependent on that catalog as well
	 */
	dependent.classId = RelationRelationId;
	dependent.objectId = relid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineStreamOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

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
	 * Record a dependency between the transform and its output stream
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = osrelid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	/*
	 * Record a dependency between the CT and all referenced objects in its query definition
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;
	recordDependencyOnExpr(&referenced, (Node *) query, NIL, DEPENDENCY_NORMAL);

	/*
	 * If our output function is pipelinedb.insert_into_stream, record dependencies between
	 * this CT and all target streams given to pipelinedb.insert_into_stream
	 */
	if (fnoid == GetInsertIntoStreamOid())
	{
		ListCell *lc;
		Relation rel = heap_open(relid, AccessShareLock);

		foreach(lc, args)
		{
			Value *v = (Value *) lfirst(lc);
			RangeVar *rv = makeRangeVarFromNameList(stringToQualifiedNameList(strVal(v)));
			Relation srel;
			TupleDesc desc;

			if (!RangeVarIsStream(rv, true))
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

	/*
	 * These dependencies ensure that these relations are dumped before the defrel for dump/restore
	 */
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = defrelid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	referenced.classId = RelationRelationId;
	referenced.objectId = osrelid;
	referenced.objectSubId = 0;

	dependent.classId = RelationRelationId;
	dependent.objectId = defrelid;
	dependent.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);
}

/*
 * create_dumped_ct
 */
static void
create_dumped_ct(Relation pq, RangeVar *name, const char *sql, SelectStmt *stmt, List *options)
{
	ViewStmt *vstmt;
	ObjectAddress address;
	Oid defrelid;
	RawStmt *raw;
	Query *q;
	Oid nspid;
	Oid viewid = InvalidOid;
	Oid streamrelid = InvalidOid;
	Oid osrelid = InvalidOid;
	Oid tgfnid;
	char *relname;
	List *dumped;

	raw = makeNode(RawStmt);
	raw->stmt = (Node *) copyObject(stmt);

	if (!name->schemaname)
		nspid = RangeVarGetCreationNamespace(name);
	else
		nspid = get_namespace_oid(name->schemaname, false);

	/*
	 * We should always have the following options
	 */
	if (!GetOptionAsString(options, OPTION_STREAM, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_STREAM);
	streamrelid = get_relid_from_qualified_relname(relname);

	if (!GetOptionAsString(options, OPTION_TRANSFORM, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_TRANSFORM);
	viewid = get_relname_relid(relname, nspid);

	if (!GetOptionAsString(options, OPTION_OSREL, &relname))
		elog(ERROR, "option \"%s\" expected but not found", OPTION_OSREL);
	osrelid = get_relname_relid(relname, nspid);

	/* Create the actual view  */
	vstmt = makeNode(ViewStmt);
	vstmt->view = name;
	vstmt->query = (Node *) stmt;

	address = DefineView(vstmt, sql, -1, 0);
	defrelid = address.objectId;
	CommandCounterIncrement();

	q = GetContQueryDef(defrelid);

	options = lappend(options, makeDefElem(OPTION_ACTION, (Node *) makeString(ACTION_TRANSFORM), -1));
	record_ct_dependencies(viewid, osrelid, tgfnid, defrelid, stmt, q, NIL);
	StorePipelineQueryReloptions(defrelid, options);

	dumped = list_make1(makeDefElem(OPTION_ACTION, (Node *) makeString(ACTION_DUMPED), -1));
	StorePipelineQueryReloptions(viewid, dumped);

	CommandCounterIncrement();

	/*
	 * Create the cont_query row. If we're doing a binary upgrade the row will already be present.
	 */
	if (!IsBinaryUpgrade)
		DefineContTransform(viewid, defrelid, streamrelid, viewid, osrelid, &options, &tgfnid);
}

/*
 * SyncContView
 */
void
SyncContView(RangeVar *name)
{
	HeapTuple tup;
	Datum d;
	bool isnull;
	Oid defrelid = InvalidOid;
	int noptions;
	int i;
	ArrayType *options;
	Datum *values;
	bool *nulls;
	Oid pkindexid = InvalidOid;
	Oid lookupindexid = InvalidOid;
	Oid seqrelid = InvalidOid;
	Oid matrelid = InvalidOid;
	Oid overlayid = InvalidOid;
	Oid osrelid = InvalidOid;
	Relation pq;
	ContQuery *cq;
	RawStmt *raw;

	cq = RangeVarGetContView(name);
	defrelid = cq->defrelid;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(defrelid));
	Assert(HeapTupleIsValid(tup));

	d = SysCacheGetAttr(RELOID, tup, Anum_pg_class_reloptions, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		elog(ERROR, "no reloptions found for OID %u", defrelid);
	}

	options = DatumGetArrayTypeP(d);
	deconstruct_array(options, TEXTOID, -1, false, 'i', &values, &nulls, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char *raw = TextDatumGetCString(values[i]);
		char *key;
		char *value;
		List *split = NIL;
		RangeVar *rv;

		if (!SplitIdentifierString(raw, '=', &split))
			elog(ERROR, "failed to parse option \"%s\"", raw);

		if (list_length(split) != 2)
			elog(ERROR, "failed to parse option \"%s\"", raw);

		key = (char *) linitial(split);
		value = (char *) lsecond(split);
		rv = makeRangeVar(name->schemaname, value, -1);

		if (!pg_strcasecmp(key, OPTION_LOOKUPINDEX))
			lookupindexid = RangeVarGetRelid(rv, NoLock, false);
		else if (!pg_strcasecmp(key, OPTION_PKINDEX))
			pkindexid = RangeVarGetRelid(rv, NoLock, false);
		else if (!pg_strcasecmp(key, OPTION_SEQREL))
			seqrelid = RangeVarGetRelid(rv, NoLock, false);
		else if (!pg_strcasecmp(key, OPTION_MATREL))
			matrelid = RangeVarGetRelid(rv, NoLock, false);
		else if (!pg_strcasecmp(key, OPTION_OSREL))
			osrelid = RangeVarGetRelid(rv, NoLock, false);
		else if (!pg_strcasecmp(key, OPTION_OVERLAY))
			overlayid = RangeVarGetRelid(rv, NoLock, false);
	}

	pq = OpenPipelineQuery(NoLock);
	ReleaseSysCache(tup);
	UpdateContViewIndexIds(pq, cq->id, pkindexid, lookupindexid, seqrelid);

	raw = (RawStmt *) linitial(pg_parse_query(cq->sql));
	Assert(IsA(raw->stmt, SelectStmt));

	record_cv_dependencies(matrelid, osrelid, overlayid,
			lookupindexid, pkindexid, defrelid, seqrelid, (SelectStmt *) raw->stmt, cq->cvdef);

	ClosePipelineQuery(pq, NoLock);

	/*
	 * This CV is ready to start reading events now
	 */
	SyncPipelineStreamReaders();
}

/*
 * ExecCreateContViewStmt
 */
void
ExecCreateContViewStmt(RangeVar *view, Node *sel, List *options, const char *querystring)
{
	CreateStmt *create_stmt;
	CreateSeqStmt *create_seq_stmt;
	ViewStmt *view_stmt;
	Query *query;
	RangeVar *matrel_name;
	RangeVar *seqrel_name;
	List *tableElts = NIL;
	ListCell *lc;
	Oid matrelid = InvalidOid;
	Oid seqrelid = InvalidOid;
	Oid overlayid = InvalidOid;
	Oid lookup_idx_oid = InvalidOid;
	Oid pkey_idx_oid = InvalidOid;
	Oid defrelid = InvalidOid;
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
	char *tsname;
	List *matrel_options = NIL;
	DefElem *ff = NULL;
	Oid streamrelid;
	RawStmt *raw;
	ContAnalyzeContext cxt;
	ContQuery *cv;
	ObjectAddress typaddr;
	RawStmt *def;
	SelectStmt *defstmt;
	char *given_ttl_column = NULL;

	check_relation_already_exists(view);

	matrel_name = makeRangeVar(view->schemaname, CVNameToMatRelName(view->relname), -1);
	matrel_name->inh = false;
	seqrel_name = makeRangeVar(view->schemaname, CVNameToSeqRelName(view->relname), -1);

	/*
	 * allowSystemTableMods is a global flag that, when true, allows certain column types
	 * to be created. We need it set to true to create some hidden state columns. In particular,
	 * ones with a type of anyarray.
	 */
	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	pipeline_query = OpenPipelineQuery(ExclusiveLock);

	RewriteFromClause((SelectStmt *) sel);

	/* Apply any CQ storage options like sw, step_factor */
	options = ApplyStorageOptions((SelectStmt *) sel, options, &has_sw, &ttl, &ttl_column);

	ValidateParsedContQuery(view, sel, querystring);

	raw = makeNode(RawStmt);
	raw->stmt = copyObject(sel);

	/* Deparse query so that analyzer always see the same canonicalized SelectStmt */
	cont_query = parse_analyze(raw, querystring, NULL, 0, NULL);

	/*
	 * Detect if we're restoring a dumped CV, which requires its own definition path
	 */
	if (is_dumped_cv_defrel(options))
	{
		create_dumped_cv(pipeline_query, view, querystring, (SelectStmt *) sel, cont_query, options);
		heap_close(pipeline_query, NoLock);
		allowSystemTableMods = saveAllowSystemTableMods;
		return;
	}

	cont_select_sql = deparse_query_def(cont_query);
	raw = (RawStmt *) linitial(pg_parse_query(cont_select_sql));
	select = (SelectStmt *) raw->stmt;

	def = makeNode(RawStmt);
	def->stmt = (Node *) copyObject(select);

	sf = QueryGetSWStepFactor(cont_query);

	/*
	 * Get the transformed SelectStmt used by CQ workers. We do this
	 * because the targetList of this SelectStmt contains all columns
	 * that need to be created in the underlying matrel.
	 */
	workerselect = TransformSelectStmtForContProcess(matrel_name, copyObject(select), &viewselect, sf, Worker);

	raw->stmt = (Node *) copyObject(workerselect);
	query = parse_analyze(raw, cont_select_sql, NULL, 0, NULL);

	ValidateContQuery(query);

	/*
	 * We need to rewrite any combine aggregates so that we don't attempt to create a matrel with polymorphic columns
	 */
	RewriteCombineAggs(query);

	FinalizeOverlayStmtAggregates(viewselect, query);

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
	extract_ttl_params(&options, tableElts, has_sw, &ttl, &ttl_column);

	pk = GetContQueryOption(options, OPTION_PK);
	if (pk)
	{
		if (IsA(pk->arg, String))
			options = list_delete(options, pk);
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

	ff = GetContQueryOption(options, OPTION_FILLFACTOR);
	if (ff)
	{
		options = list_delete(options, ff);
		matrel_options = lappend(matrel_options, ff);
	}
	else
	{
		matrel_options = add_default_fillfactor(matrel_options);
	}

	/*
	 * Create the actual underlying materialization relation.
	 */
	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = matrel_name;
	create_stmt->tableElts = tableElts;
	create_stmt->options = matrel_options;

	if (GetOptionAsString(options, OPTION_TABLESPACE, &tsname))
		create_stmt->tablespacename = tsname;

	address = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid, &typaddr, NULL);

	matrelid = address.objectId;
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	AlterTableCreateToastTable(matrelid, toast_options, AccessExclusiveLock);

	/* Create the sequence for primary keys */
	if (!pk)
	{
		List *ob = NIL;

		if (view->schemaname)
			ob = lappend(ob, makeString(view->schemaname));

		ob = lappend(ob, makeString(matrel_name->relname));
		ob = lappend(ob, makeString(CQ_MATREL_PKEY));

		create_seq_stmt = makeNode(CreateSeqStmt);
		create_seq_stmt->options = lappend(create_seq_stmt->options, makeDefElem("owned_by", (Node *) ob, -1));
		create_seq_stmt->sequence = seqrel_name;

		address = DefineSequence(NULL, create_seq_stmt);

		seqrelid = address.objectId;
		CommandCounterIncrement();
	}
	else
	{
		seqrelid = InvalidOid;
	}

	/*
	 * Any TTL column given at this point was passed via storage options, so we'll use this
	 * to ensure both a SW and explicit TTL column aren't both given.
	 */
	given_ttl_column = ttl_column;

	ttl_attno = FindSWTimeColumnAttrNo(viewselect, matrelid, &ttl, &ttl_column);
	/*
	 * has_sw will already be true if the sw storage parameter was set,
	 * but a sliding-window can still be expressed as a WHERE predicate,
	 * which will be detected here.
	 */
	if (AttributeNumberIsValid(ttl_attno))
	{
		has_sw = true;
		sf = QueryGetSWStepFactor(cont_query);

		if (!sf)
			sf = sliding_window_step_factor;

		QuerySetSWStepFactor(cont_query, sf);
	}

	if (has_sw && ttl == 0)
		elog(ERROR, "sliding window must be at least 1 second");
	if (has_sw && given_ttl_column)
		elog(ERROR, "TTLs cannot be specified in conjunction with sliding windows");

	if (given_ttl_column)
	{
		if (has_sw)
			elog(ERROR, "TTLs cannot be specified in conjunction with sliding windows");
		ttl_attno = FindTTLColumnAttrNo(given_ttl_column, matrelid);
	}

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));
	collect_rels_and_streams((Node *) workerselect->fromClause, &cxt);

	Assert(list_length(cxt.streams) == 1);
	streamrelid = RangeVarGetRelid((RangeVar *) linitial(cxt.streams), NoLock, false);

	/*
	 * Now save the underlying query in the `pipelinedb.cont_query` catalog relation. We don't have relid for
	 * the continuous view yet, since we need this entry for the DefineView call below to succeed.
	 * We'll update it afterwards.
	 *
	 * pqoid is the oid of the row in pipeline_query,
	 * cvid is the id of the continuous view (used in reader bitmaps)
	 */
	DefineContView(pipeline_query, InvalidOid, streamrelid, matrelid, seqrelid, ttl, ttl_attno, sf, &cvid);
	CommandCounterIncrement();

	SyncPipelineStreamReaders();

	/* Create the view on the matrel */
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;

	address = DefineView(view_stmt, cont_select_sql, -1, 0);
	CommandCounterIncrement();

	overlayid = address.objectId;

	defstmt = (SelectStmt *) copyObject(def->stmt);

	/*
	 * We rewrite all DISTINCT clauses to explicit DISTINCT ON, so use the rewritten one
	 *
	 * Note that as per PG, the singleton list of [NIL] signifies a non-explicit DISTINCT clause
	 */
	if (equal(defstmt->distinctClause, lcons(NIL, NIL)))
		defstmt->distinctClause = workerselect->distinctClause;

	view_stmt->view = makeRangeVar(view->schemaname, CVNameToDefRelName(view->relname), -1);
	view_stmt->query = (Node *) defstmt;

	/*
	 * Store the CQ definition
	 */
	defrelid = define_defrel(view_stmt, cont_select_sql);
	CommandCounterIncrement();

	/*
	 * Create the output stream
	 */
	overlayrel = heap_open(overlayid, NoLock);

	old = makeNode(ColumnDef);
	old->colname = OSREL_OLD_ROW;
	old->typeName = makeNode(TypeName);
	old->typeName->typeOid = overlayrel->rd_rel->reltype;
	old->typeName->typemod = -1;
	old->is_local = true;

	new = copyObject(old);
	new->colname = OSREL_NEW_ROW;

	heap_close(overlayrel, NoLock);

	delta = NULL;
	if (!has_sw)
	{
		/* Only non-SW output streams have delta tuples */
		matrel = heap_open(matrelid, NoLock);

		delta = makeNode(ColumnDef);
		delta->colname = OSREL_DELTA_ROW;
		delta->typeName = makeNode(TypeName);
		delta->typeName->typeOid = matrel->rd_rel->reltype;
		delta->typeName->typemod = -1;
		delta->is_local = true;

		heap_close(matrel, NoLock);
	}

	create_osrel = makeNode(CreateForeignTableStmt);
	create_osrel->servername = PIPELINEDB_SERVER;
	create_osrel->base.tableElts = list_make2(old, new);

	if (delta)
		create_osrel->base.tableElts = lappend(create_osrel->base.tableElts, delta);

	create_osrel->base.relation = makeRangeVar(view->schemaname, CVNameToOSRelName(view->relname), -1);
	transformCreateStreamStmt(create_osrel);

	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_FOREIGN_TABLE, InvalidOid, &typaddr, NULL);

	CreateForeignTable(create_osrel, address.objectId);
	CreatePipelineStreamEntry(create_osrel, address.objectId);

	osrelid = address.objectId;

	UpdateContViewRelIds(pipeline_query, cvid, overlayid, defrelid, osrelid, options);
	CommandCounterIncrement();

	/* Create group look up index and record dependencies */
	select = TransformSelectStmtForContProcess(matrel_name, copyObject(select), NULL, sf, Combiner);
	lookup_idx_oid = create_lookup_index(view, matrelid, matrel_name, select, has_sw);
	CommandCounterIncrement();

	pkey_idx_oid = create_pkey_index(view, matrelid, matrel_name, pk ? strVal(pk->arg) : CQ_MATREL_PKEY);
	CommandCounterIncrement();

	UpdateContViewIndexIds(pipeline_query, cvid, pkey_idx_oid, lookup_idx_oid, seqrelid);
	CommandCounterIncrement();

	record_cv_dependencies(matrelid, osrelid, overlayid, lookup_idx_oid, pkey_idx_oid, defrelid, seqrelid, workerselect, cont_query);
	allowSystemTableMods = saveAllowSystemTableMods;

	options = add_options_for_dump(view->relname, streamrelid, overlayid, matrelid, seqrelid,
			osrelid, pkey_idx_oid, lookup_idx_oid, ttl, ttl_attno, sf, options);

	StorePipelineQueryReloptions(defrelid, options);

	/*
	 * Run the combiner and worker queries through the planner, so that if something goes wrong
	 * we know now rather than at execution time.
	 */
	cv = GetContViewForId(cvid);
	GetContPlan(cv, Combiner);
	GetContPlan(cv, Worker);
	GetCombinerLookupPlan(cv);

	ClosePipelineQuery(pipeline_query, NoLock);
}

/*
 * GetPipelineQueryTuple
 */
HeapTuple
GetPipelineQueryTuple(RangeVar *name)
{
	HeapTuple tuple;
	Oid namespace = InvalidOid;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, true);

	if (!OidIsValid(namespace))
		return NULL;

	Assert(OidIsValid(namespace));

	tuple = PipelineCatalogLookup(PIPELINEQUERYRELID, 1, ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	return tuple;
}

/*
 * RangeVarIsPipelineObject
 */
bool
RangeVarIsPipelineObject(RangeVar *name)
{
	HeapTuple tup;

	/* Is it a stream? */
	if (RangeVarIsStream(name, true))
		return true;

	/* Is it a CV/CT? */
	tup = GetPipelineQueryTuple(name);
	if (!HeapTupleIsValid(tup))
		return false;

	return true;
}

/*
 *
 */
Relation
OpenPipelineQuery(LOCKMODE mode)
{
	return heap_open(GetPipelineQueryOid(), mode);
}

/*
 * ClosePipelineQuery
 */
void
ClosePipelineQuery(Relation rel, LOCKMODE mode)
{
	heap_close(rel, mode);
}


/*
 * RangeVarGetMatRelName
 */
RangeVar *
RangeVarGetMatRelName(RangeVar *cvname)
{
	HeapTuple tuple;
	Form_pipeline_query row;
	RangeVar *result;

	tuple = GetPipelineQueryTuple(cvname);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "continuous view \"%s\" does not exist", cvname->relname);

	row = (Form_pipeline_query) GETSTRUCT(tuple);
	result = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);

	return result;
}

/*
 * RangeVarIsContView
 *
 * Returns true if the RangeVar represents a registered
 * continuous view.
 */
bool
RangeVarIsContView(RangeVar *name)
{
	HeapTuple tup = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	bool success;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	success = row->type == PIPELINE_QUERY_VIEW;

	return success;
}

/*
 * RangeVarIsContTransform
 *
 * Returns true if the RangeVar represents a registered
 * continuous view.
 */
bool
RangeVarIsContTransform(RangeVar *name)
{
	HeapTuple tup = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	bool success;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	success = row->type == PIPELINE_QUERY_TRANSFORM;

	return success;
}

/*
 * RangeVarIsContQuery
 */
bool
RangeVarIsContQuery(RangeVar *name)
{
	return RangeVarIsContView(name) || RangeVarIsContTransform(name);
}

/*
 * RelidIsContQuery
 */
bool
RelidIsContQuery(Oid relid)
{
	return RelidIsContView(relid) || RelidIsContTransform(relid);
}

/*
 * GetSWContViewRelName
 *
 * Returns true if any of the given nodes represents a
 * sliding window continuous view
 */
RangeVar *
GetSWContViewRelName(List *nodes)
{
	ListCell *lc;
	foreach(lc, nodes)
	{
		if (IsA(lfirst(lc), RangeVar))
		{
			RangeVar *rv = lfirst(lc);

			if (RangeVarIsContView(rv) && RangeVarIsSWContView(rv))
				return rv;
		}
	}
	return NULL;
}

/*
 * RangeVarIsMatRel
 *
 * Returns true if the RangeVar represents a materialization table,
 * and also assigns the given string (if it's non-NULL) to the name
 * of the corresponding continuous view
 */
bool
RangeVarIsMatRel(RangeVar *name, RangeVar **cvname)
{
	HeapTuple tup;
	Oid namespace;
	Form_pipeline_query row;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, false);

	Assert(OidIsValid(namespace));

	tup = PipelineCatalogLookup(PIPELINEQUERYMATRELID, 1, ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	if (!HeapTupleIsValid(tup))
	{
		if (cvname)
			*cvname = NULL;
		return false;
	}

	row = (Form_pipeline_query) GETSTRUCT(tup);

	if (cvname)
		*cvname = makeRangeVar(get_namespace_name(namespace), get_rel_name(row->relid), -1);

	return true;
}

/*
 * RelidIsContView
 *
 * * Returns true if the given oid represents a continuous view
 */
bool
RelidIsContView(Oid relid)
{
	HeapTuple tup;
	bool result = false;
	Form_pipeline_query row;

	tup = PipelineCatalogLookup(PIPELINEQUERYRELID, 1, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	if (row->type == PIPELINE_QUERY_VIEW)
		result = true;

	return result;
}

/*
 * RelidIsContTransform
 *
 * * Returns true if the given oid represents a continuous transform
 */
bool
RelidIsContTransform(Oid relid)
{
	HeapTuple tup;
	bool result = false;
	Form_pipeline_query row;

	tup = PipelineCatalogLookup(PIPELINEQUERYRELID, 1, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	if (row->type == PIPELINE_QUERY_TRANSFORM)
		result = true;

	return result;
}

/*
 * RangeVarIsSWContView
 */
bool
RangeVarIsSWContView(RangeVar *name)
{
	bool sw = false;
	HeapTuple tuple = GetPipelineQueryTuple(name);

	if (HeapTupleIsValid(tuple))
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		sw = is_sw(row);
	}

	return sw;
}

/*
 * RangeVarIsTTLContView
 */
bool
RangeVarIsTTLContView(RangeVar *name)
{
	bool ttl = false;
	HeapTuple tuple = GetPipelineQueryTuple(name);

	if (HeapTupleIsValid(tuple))
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		ttl = AttributeNumberIsValid(row->ttl_attno);
	}

	return ttl;
}

/*
 * GetTriggerFnOid
 */
Oid
GetTriggerFnOid(Oid defrelid)
{
	HeapTuple tup;
	Datum d;
	bool isnull;
	int i;
	ArrayType *options;
	Datum *values;
	bool *nulls;
	int noptions;
	Oid fargtypes[1];
	Oid result = InvalidOid;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(defrelid));
	Assert(HeapTupleIsValid(tup));

	d = SysCacheGetAttr(RELOID, tup, Anum_pg_class_reloptions, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		elog(ERROR, "no reloptions found for OID %u", defrelid);
	}

	options = DatumGetArrayTypeP(d);
	deconstruct_array(options, TEXTOID, -1, false, 'i', &values, &nulls, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char *raw = TextDatumGetCString(values[i]);
		char *key;
		char *value;
		List *split = NIL;

		if (!SplitIdentifierString(raw, '=', &split))
			elog(ERROR, "failed to parse option \"%s\"", raw);

		if (list_length(split) != 2)
			elog(ERROR, "failed to parse option \"%s\"", raw);

		key = (char *) linitial(split);
		if (pg_strcasecmp(key, OPTION_TGFN))
			continue;

		value = (char *) lsecond(split);
		result = LookupFuncName(textToQualifiedNameList((text *) CStringGetTextDatum(value)), 0, fargtypes, false);
		break;
	}

	ReleaseSysCache(tup);

	return result;
}

/*
 * GetContQueryForId
 */
ContQuery *
GetContQueryForId(Oid id)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYID, 1, ObjectIdGetDatum(id));
	ContQuery *cq;
	Form_pipeline_query row;
	bool isnull;
	Query *query;
	Oid tgfnid = InvalidOid;
	char *relname;

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	relname = get_rel_name(row->relid);

	if (relname == NULL)
		return NULL;

	cq = palloc0(sizeof(ContQuery));
	cq->id = id;

	Assert(row->type == PIPELINE_QUERY_TRANSFORM || row->type == PIPELINE_QUERY_VIEW);
	cq->type = row->type == PIPELINE_QUERY_TRANSFORM ? CONT_TRANSFORM : CONT_VIEW;

	cq->relid = row->relid;
	cq->defrelid = row->defrelid;
	cq->name = makeRangeVar(get_namespace_name(get_rel_namespace(row->relid)), relname, -1);
	cq->seqrelid = row->seqrelid;
	cq->matrelid = row->matrelid;
	cq->streamrelid = row->streamrelid;
	cq->active = row->active;
	cq->osrelid = row->osrelid;
	cq->pkidxid = row->pkidxid;
	cq->lookupidxid = row->lookupidxid;
	cq->ttl_attno = row->ttl_attno;
	cq->ttl = row->ttl;

	if (cq->type == CONT_VIEW)
	{
		cq->matrel = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);
		/* Ignore inherited tables when working with the matrel */
		cq->matrel->inh = false;
	}
	else
		cq->matrel = NULL;

	/*
	 * Retrieve the Query from the defrel
	 */
	query = GetContQueryDef(row->defrelid);

	cq->sql = deparse_query_def(query);
	cq->cvdef = query;

	if (is_sw(row))
	{
		Interval *i;

		cq->is_sw = true;
		cq->sw_attno = row->ttl_attno;
		cq->sw_step_factor = row->step_factor;
		i = GetSWInterval(cq->name);
		cq->sw_interval_ms = 1000 * (uint64) DatumGetFloat8(
				DirectFunctionCall2(interval_part, CStringGetTextDatum("epoch"), (Datum) i));
		cq->sw_step_ms = (uint64) (cq->sw_interval_ms * cq->sw_step_factor / 100.0);
	}

	/*
	 * We store the output function by name instead of OID for better dump/restore usability
	 */
	if (cq->type == CONT_TRANSFORM)
		tgfnid = GetTriggerFnOid(cq->defrelid);

	if (OidIsValid(tgfnid))
	{
		cq->tgfn = tgfnid;
		cq->tgnargs = row->tgnargs;
	}

	/* This code is copied from trigger.c:RelationBuildTriggers */
	if (cq->tgnargs > 0)
	{
		bytea *val;
		char *p;
		int i;

		val = DatumGetByteaP(PipelineCatalogGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_tgargs, &isnull));
		Assert(!isnull);

		p = (char *) VARDATA(val);
		cq->tgargs = (char **) palloc(cq->tgnargs * sizeof(char *));

		for (i = 0; i < cq->tgnargs; i++)
		{
			cq->tgargs[i] = pstrdup(p);
			p += strlen(p) + 1;
		}
	}
	else
		cq->tgargs = NULL;

	return cq;
}

/*
 * GetContViewForId
 */
ContQuery *
GetContViewForId(Oid id)
{
	ContQuery *cq = GetContQueryForId(id);

	if (cq)
	{
		if (cq->type != CONT_VIEW)
			elog(ERROR, "continuous query with id %d is not a continuous view", id);

		return cq;
	}

	return NULL;
}

/*
 * GetContTransformForId
 */
ContQuery *
GetContTransformForId(Oid id)
{
	ContQuery *cq = GetContQueryForId(id);

	if (cq)
	{
		if (cq->type != CONT_TRANSFORM)
			elog(ERROR, "continuous query with id %d is not a continuous transform", id);

		return cq;
	}
	return NULL;
}

/*
 * RangeVarGetContView
 */
ContQuery *
RangeVarGetContView(RangeVar *cv_name)
{
	Oid id = RangeVarGetContQueryId(cv_name);

	if (!OidIsValid(id))
		return NULL;

	return GetContViewForId(id);
}

/*
 * RangeVarGetContQuery
 */
ContQuery *
RangeVarGetContQuery(RangeVar *name)
{
	Oid id = RangeVarGetContQueryId(name);

	if (!OidIsValid(id))
		return NULL;

	return GetContQueryForId(id);
}

/*
 * get_cont_query_ids
 */
static Bitmapset *
get_cont_query_ids(char type)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationOid, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;
	Bitmapset *result = NULL;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Oid id = row->id;

		if (type && row->type != type)
			continue;

		result = bms_add_member(result, id);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);

	return result;
}

/*
 * GetContViewIds
 */
Bitmapset *
GetContViewIds(void)
{
	return get_cont_query_ids(PIPELINE_QUERY_VIEW);
}

/*
 * GetContTransformIds
 */
Bitmapset *
GetContTransformIds(void)
{
	return get_cont_query_ids(PIPELINE_QUERY_TRANSFORM);
}

/*
 * GetContQueryIds
 */
Bitmapset *
GetContQueryIds(void)
{
	return get_cont_query_ids(0);
}

/*
 * RangeVarGetContQueryId
 */
Oid
RangeVarGetContQueryId(RangeVar *name)
{
	HeapTuple tuple = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	Oid row_id = InvalidOid;

	if (HeapTupleIsValid(tuple))
	{
		row = (Form_pipeline_query) GETSTRUCT(tuple);
		row_id = row->id;
	}

	return row_id;
}

/*
 * SyncPipelineQuery
 */
void
SyncPipelineQuery(void)
{
	HeapTuple tup;
	Relation pipeline_query;
	HeapScanDesc scan_desc;

	if (pg_class_aclcheck(PipelineQueryRelationOid, GetUserId(), ACL_DELETE) != ACLCHECK_OK)
		return;

	pipeline_query = heap_open(PipelineQueryRelationOid, NoLock);
	scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		if (!get_rel_name(row->relid))
		{
			PipelineCatalogTupleDelete(pipeline_query, &tup->t_self);
			PurgeDeadProcStats(row->id);
		}
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, NoLock);
}

/*
 * ContQuerySetActive
 */
bool
ContQuerySetActive(Oid id, bool active)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationOid, RowExclusiveLock);
	HeapTuple tup = PipelineCatalogLookupForUpdate(pipeline_query, PIPELINEQUERYID, ObjectIdGetDatum(id));
	Form_pipeline_query row;
	bool changed = false;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);

	if (row->active != active)
	{
		bool replace[Natts_pipeline_query];
		bool nulls[Natts_pipeline_query];
		Datum values[Natts_pipeline_query];
		HeapTuple new;

		MemSet(replace, 0 , sizeof(replace));
		MemSet(nulls, 0 , sizeof(nulls));
		replace[Anum_pipeline_query_active - 1] = true;
		values[Anum_pipeline_query_active - 1] = BoolGetDatum(active);

		new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);

		PipelineCatalogTupleUpdate(pipeline_query, &tup->t_self, new);
		CommandCounterIncrement();

		changed = true;
	}

	heap_close(pipeline_query, NoLock);

	return changed;
}

/*
 * RangeVarGetTTLInfo
 */
void RangeVarGetTTLInfo(RangeVar *cvname, char **ttl_col, int *ttl)
{
	HeapTuple tup = GetPipelineQueryTuple(cvname);
	Form_pipeline_query row;
	Relation rel;
	TupleDesc desc;
	int i;

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "continuous view \"%s\" does not exist", cvname->relname);

	Assert(ttl_col);
	Assert(ttl);

	row = (Form_pipeline_query) GETSTRUCT(tup);
	Assert(AttributeNumberIsValid(row->ttl_attno));
	Assert(row->ttl > 0);
	*ttl = row->ttl;

	rel = heap_open(row->matrelid, NoLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		if (TupleDescAttr(desc, i)->attnum == row->ttl_attno)
		{
			*ttl_col = pstrdup(NameStr(TupleDescAttr(desc, i)->attname));
			break;
		}
	}

	Assert(*ttl_col);

	heap_close(rel, NoLock);
}

/*
 * parse_outputfunc_args
 */
static void
parse_outputfunc_args(List *options, List **args)
{
	int nargs;
	char *raw;
	bytea *val;
	char *p;
	int i;

	*args = NIL;

	if (!GetOptionAsInteger(options, OPTION_TGNARGS, &nargs))
		return;

	if (nargs <= 0)
		return;

	if (!GetOptionAsString(options, OPTION_TGARGS, &raw))
		return;

	val = (bytea *) DirectFunctionCall1(byteain, (Datum) CStringGetDatum(raw));
	p = (char *) VARDATA(val);

	/* Trigger function arguments will always be an array of strings */
	for (i = 0; i < nargs; i++)
	{
		*args = lappend(*args, makeString(pstrdup(p)));
		p += strlen(p) + 1;
	}
}

/*
 * is_dumped_ct_defrel
 */
static bool
is_dumped_ct_defrel(List *options)
{
	return GetContQueryOption(options, OPTION_TRANSFORM) != NULL;
}

/*
 * ExecCreateContTransformStmt
 */
void
ExecCreateContTransformStmt(RangeVar *transform, Node *stmt, List *options, const char *querystring)
{
	Relation pipeline_query;
	Query *query;
	ObjectAddress address;
	Oid relid;
	Oid tgfnid = InvalidOid;
	CreateForeignTableStmt *create_osrel;
	Oid osrelid;
	ViewStmt *vstmt;
	List *args = NIL;
	ContAnalyzeContext cxt;
	Oid streamrelid;
	RawStmt *raw;
	ObjectAddress typaddr;
	Oid defrelid;
	List *dumped;

	check_relation_already_exists(transform);

	pipeline_query = OpenPipelineQuery(ExclusiveLock);

	RewriteFromClause((SelectStmt *) stmt);

	ValidateParsedContQuery(transform, stmt, querystring);
	ValidateSubselect(stmt, "continuous transforms");

	raw = makeNode(RawStmt);
	raw->stmt = copyObject(stmt);
	query = parse_analyze(raw, querystring, NULL, 0, NULL);
	ValidateContQuery(query);

	/*
	 * Detect if we're restoring a dumped CT, which requires its own definition path
	 */
	if (is_dumped_ct_defrel(options))
	{
		create_dumped_ct(pipeline_query, transform, querystring, (SelectStmt *) stmt, options);
		ClosePipelineQuery(pipeline_query, NoLock);
		return;
	}

	vstmt = makeNode(ViewStmt);
	vstmt->view = transform;
	vstmt->query = stmt;

	address = DefineView(vstmt, querystring, -1, 0);
	relid = address.objectId;
	CommandCounterIncrement();

	vstmt = makeNode(ViewStmt);
	vstmt->view =  makeRangeVar(transform->schemaname, CVNameToDefRelName(transform->relname), -1);
	vstmt->query = stmt;

	/*
	 * Store the CQ definition
	 */
	defrelid = define_defrel(vstmt, querystring);

	/* Create output stream */
	create_osrel = makeNode(CreateForeignTableStmt);
	create_osrel->servername = PIPELINEDB_SERVER;
	create_osrel->base.tableElts = create_coldefs_from_tlist(query);
	create_osrel->base.relation = makeRangeVar(transform->schemaname, CVNameToOSRelName(transform->relname), -1);
	transformCreateStreamStmt(create_osrel);

	address = DefineRelation((CreateStmt *) create_osrel, RELKIND_FOREIGN_TABLE, InvalidOid, &typaddr, NULL);
	osrelid = address.objectId;
	CommandCounterIncrement();

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));
	collect_rels_and_streams((Node *) ((SelectStmt *) stmt)->fromClause, &cxt);

	Assert(list_length(cxt.streams) == 1);
	streamrelid = RangeVarGetRelid((RangeVar *) linitial(cxt.streams), NoLock, false);

	DefineContTransform(relid, defrelid, streamrelid, relid, osrelid, &options, &tgfnid);
	CommandCounterIncrement();

	CreateForeignTable(create_osrel, address.objectId);
	CreatePipelineStreamEntry(create_osrel, address.objectId);
	CommandCounterIncrement();

	parse_outputfunc_args(options, &args);
	record_ct_dependencies(relid, osrelid, tgfnid, defrelid, (SelectStmt *) stmt, query, args);

	CommandCounterIncrement();

	StorePipelineQueryReloptions(defrelid, options);

	/*
	 * Add a dumped flag to this CT's reloptions to designate it as dumped for restores
	 */
	dumped = list_make1(makeDefElem(OPTION_ACTION, (Node *) makeString(ACTION_DUMPED), -1));
	StorePipelineQueryReloptions(relid, dumped);

	ClosePipelineQuery(pipeline_query, NoLock);
}

/*
 * RelidIsDefRel
 */
bool
RelidIsDefRel(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYDEFRELID, 1, ObjectIdGetDatum(relid));

	return HeapTupleIsValid(tup);
}

/*
 * RelidIsContViewIndex
 */
bool
RelidIsContViewIndex(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYPKIDXID, 1, ObjectIdGetDatum(relid));

	/*
	 * If the given relation is either the lookup index or the $pk index, return true
	 */
	if (HeapTupleIsValid(tup))
		return true;

	tup = PipelineCatalogLookup(PIPELINEQUERYLOOKUPIDXID, 1, ObjectIdGetDatum(relid));

	return HeapTupleIsValid(tup);
}

/*
 * RelidIsContViewSequence
 */
bool
RelidIsContViewSequence(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYSEQRELID, 1, ObjectIdGetDatum(relid));

	return HeapTupleIsValid(tup);
}

/*
 * RelidIsContQueryInternalRelation
 */
bool
RelidIsContQueryInternalRelation(Oid relid)
{
	if (RelidIsDefRel(relid) ||
			RelidIsContViewIndex(relid) ||
			RelidIsContViewSequence(relid) ||
			RelidIsOutputStream(relid))
		return true;

	return false;
}

/*
 * RelidGetContQuery
 */
ContQuery *
RelidGetContQuery(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYRELID, 1, ObjectIdGetDatum(relid));
	Form_pipeline_query row;

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);

	return GetContQueryForId(row->id);
}

/*
 * SyncContQueryDefRel
 */
void
SyncContQueryDefRel(Oid cqrelid)
{
	ContQuery *cq = RelidGetContQuery(cqrelid);
	char *name_opt = NULL;
	char *cq_relname;
	char *stream_name;
	Datum *values;
	bool *nulls;
	HeapTuple tup;
	HeapTuple newtup;
	bool isnull;
	ArrayType *options;
	int noptions;
	int i;
	Datum d;
	Relation pgclass;
	Datum class_values[Natts_pg_class];
	bool class_null[Natts_pg_class];
	bool class_repl[Natts_pg_class];
	StringInfoData buf;
	int opt_index = -1;
	int overlay_index = -1;
	int stream_index = -1;

	Assert(cq);

	if (cq->type == CONT_VIEW)
		name_opt = OPTION_CV;
	else if (cq->type == CONT_TRANSFORM)
		name_opt = OPTION_TRANSFORM;

	/*
	 * The schema is implied by whatever schema this CQ exists in,
	 * so we only need to deal with relation names.
	 */
	cq_relname = get_rel_name(cqrelid);
	stream_name = get_qualified_relname(cq->streamrelid);

	pgclass = heap_open(RelationRelationId, RowExclusiveLock);
	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(cq->defrelid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for OID %u", cq->defrelid);

	d = SysCacheGetAttr(RELOID, tup, Anum_pg_class_reloptions, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		elog(ERROR, "no reloptions found for OID %u", cq->defrelid);
	}

	options = DatumGetArrayTypeP(d);
	deconstruct_array(options, TEXTOID, -1, false, 'i', &values, &nulls, &noptions);

	/*
	 * Find the reloptions we must update and save their indices in the reloptions array
	 */
	for (i = 0; i < noptions; i++)
	{
		char *raw = TextDatumGetCString(values[i]);
		char *key;
		List *split = NIL;

		if (!SplitIdentifierString(raw, '=', &split))
			elog(ERROR, "failed to parse option \"%s\"", raw);

		if (list_length(split) != 2)
			elog(ERROR, "failed to parse option \"%s\"", raw);

		key = (char *) linitial(split);

		/*
		 * All CQs will have a stream and either a cv or transform option.
		 * Only CVs will have an overlay option.
		 */
		if (!pg_strcasecmp(key, name_opt))
			opt_index = i + 1;
		else if (!pg_strcasecmp(key, OPTION_STREAM))
			stream_index = i + 1;
		else if (cq->type == CONT_VIEW && !pg_strcasecmp(key, OPTION_OVERLAY))
			overlay_index = i + 1;
	}

	/*
	 * All CQs should either have a transform or continuous view name
	 */
	if (opt_index < 0)
		elog(ERROR, "defrel option \"%s\" not found", name_opt);

	/*
	 * All CQs should also have a stream
	 */
	if (stream_index < 0)
		elog(ERROR, "defrel option \"%s\" not found", OPTION_STREAM);

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s=%s", name_opt, cq_relname);
	d = array_set_element(d, 1, &opt_index, CStringGetTextDatum(buf.data),
			false, -1, -1, false, 'i');

	resetStringInfo(&buf);
	appendStringInfo(&buf, "%s=%s", OPTION_STREAM, stream_name);
	d = array_set_element(d, 1, &stream_index, CStringGetTextDatum(buf.data),
			false, -1, -1, false, 'i');

	/*
	 * Only continuous views will have an overlay
	 */
	if (overlay_index > 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf, "%s=%s", OPTION_OVERLAY, cq_relname);
		d = array_set_element(d, 1, &overlay_index, CStringGetTextDatum(buf.data),
				false, -1, -1, false, 'i');
	}

	MemSet(class_values, 0, sizeof(class_values));
	MemSet(class_null, 0, sizeof(class_null));
	MemSet(class_repl, 0, sizeof(class_repl));

	class_values[Anum_pg_class_reloptions - 1] = d;
	class_repl[Anum_pg_class_reloptions - 1] = true;

	newtup = heap_modify_tuple(tup, RelationGetDescr(pgclass), class_values, class_null, class_repl);
	PipelineCatalogTupleUpdate(pgclass, &newtup->t_self, newtup);

	pfree(buf.data);
	heap_freetuple(newtup);
	heap_close(pgclass, NoLock);
	ReleaseSysCache(tup);
}

/*
 * SyncStreamReaderDefRels
 */
void
SyncStreamReaderDefRels(Oid streamrelid)
{
	HeapTuple tup;
	Relation pipeline_query;
	HeapScanDesc scan_desc;
	ScanKeyData key[1];

	ScanKeyInit(&key[0],
			Anum_pipeline_query_streamrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(streamrelid));

	pipeline_query = heap_open(PipelineQueryRelationOid, NoLock);
	scan_desc = heap_beginscan_catalog(pipeline_query, 1, key);

	/*
	 * Find all readers of this stream and update their defrel reloptions to reference the new name
	 */
	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		SyncContQueryDefRel(row->relid);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, NoLock);
}

/*
 * SyncAllContQueryDefRels
 */
void
SyncAllContQueryDefRels(void)
{
	HeapTuple tup;
	Relation pipeline_query;
	HeapScanDesc scan_desc;

	pipeline_query = heap_open(PipelineQueryRelationOid, NoLock);
	scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	/*
	 * Find all readers of this stream and update their defrel reloptions to reference the new name
	 */
	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		SyncContQueryDefRel(row->relid);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, NoLock);
}

/*
 * RelidGetRangeVar
 */
RangeVar *
RelidGetRangeVar(Oid relid)
{
	return makeRangeVar(get_namespace_name(get_rel_namespace(relid)), get_rel_name(relid), -1);
}

/*
 * SyncContQuerySchema
 */
void
SyncContQuerySchema(Oid cqrelid, char *schema)
{
	AlterObjectSchemaStmt *stmt;
	ContQuery *cq = RelidGetContQuery(cqrelid);

	Assert(cq);
	stmt = makeNode(AlterObjectSchemaStmt);

	/* defrel */
	stmt->relation = RelidGetRangeVar(cq->defrelid);
	stmt->objectType = OBJECT_VIEW;
	stmt->newschema = schema;
	ExecAlterObjectSchemaStmt(stmt, NULL);

	/* osrel */
	stmt->relation = RelidGetRangeVar(cq->osrelid);
	stmt->objectType = OBJECT_FOREIGN_TABLE;
	ExecAlterObjectSchemaStmt(stmt, NULL);

	if (cq->type == CONT_VIEW)
	{
		/* matrel */
		stmt->relation = RelidGetRangeVar(cq->matrelid);
		stmt->objectType = OBJECT_TABLE;
		ExecAlterObjectSchemaStmt(stmt, NULL);
	}

	CommandCounterIncrement();
	SyncStreamReaderDefRels(cq->streamrelid);
}
