/*-------------------------------------------------------------------------
 *
 * pipeline_stream.c
 *	  routines to support manipulation of the pipeline_stream relation
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_target.h"
#include "parser/analyze.h"
#include "pipeline/cont_analyze.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define NUMERIC_OID 1700

typedef struct StreamColumnsEntry
{
	char name[NAMEDATALEN];
	List *types;
} StreamColumnsEntry;

typedef struct StreamTargetsEntry
{
	Oid relid; /* hash key --- MUST BE FIRST */
	Bitmapset *queries;
	bool inferred;
	HTAB *colstotypes;
	TupleDesc desc;
} StreamTargetsEntry;

/*
 * infer_tupledesc
 *
 * Given a stream, infer a TupleDesc based on the supertype of all
 * the casted types for each of the stream's columns
 */
static void
infer_tupledesc(StreamTargetsEntry *stream)
{
	HASH_SEQ_STATUS status;
	StreamColumnsEntry *entry;
	List *names = NIL;
	List *types = NIL;
	List *mods = NIL;
	List *collations = NIL;
	Const *preferred = makeConst(NUMERIC_OID, -1, 0, -1, 0, false, false);

	hash_seq_init(&status, stream->colstotypes);
	while ((entry = (StreamColumnsEntry *) hash_seq_search(&status)) != NULL)
	{
		char err[128];
		Oid supertype;
		char category;
		bool typispreferred;
		Oid t = exprType(linitial(entry->types));

		/*
		 * If there are any numeric types in our target types, we prepend a float8
		 * to the list of types to select from, as that is our preferred type when
		 * there is any ambiguity about how to interpret numeric types.
		 */
		get_type_category_preferred(t, &category, &typispreferred);
		if (category == TYPCATEGORY_NUMERIC)
			entry->types = lcons(preferred, entry->types);

		sprintf(err, "type conflict with stream \"%s\":", get_rel_name(stream->relid));
		supertype = select_common_type(NULL, entry->types, err, NULL);

		names = lappend(names, makeString(entry->name));
		types = lappend_int(types, supertype);
		mods = lappend_int(mods, -1);
		collations = lappend_int(collations, 0);
	}

	stream->desc = BuildDescFromLists(names, types, mods, collations);
}

/*
 * add_coltypes
 *
 * Given a target list, add the types the entries to a list of types seen
 * for the same column across all streams
 */
static void
add_coltypes(StreamTargetsEntry *stream, List *targetlist)
{
	ListCell *lc;

	foreach(lc, targetlist)
	{
		bool found;
		TypeCast *tc = (TypeCast *) lfirst(lc);
		StreamColumnsEntry *entry;
		char *name = FigureColname((Node *) tc);

		entry = (StreamColumnsEntry *)  hash_search(stream->colstotypes, name, HASH_ENTER, &found);

		if (!found)
			entry->types = NIL;

		entry->types = lappend(entry->types, tc);
	}
}

/*
 * streams_to_meta
 *
 * Scans the pipeline_query catalog table and reads the FROM clause of
 * every active query to determine which streams each query is reading
 * from and what the TupleDesc is for each stream, based on the supertype
 * of all casts for each stream column.
 *
 * A mapping from stream name to this metadata is returned.
 */
static HTAB *
streams_to_meta(Relation pipeline_query)
{
	HeapScanDesc scandesc;
	HASHCTL ctl;
	HTAB *targets;
	StreamTargetsEntry *entry;
	HeapTuple tup;
	HASH_SEQ_STATUS status;

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(StreamTargetsEntry);
	ctl.hash = oid_hash;

	targets = hash_create("streams_to_targets_and_desc", 32, &ctl, HASH_ELEM | HASH_FUNCTION);
	scandesc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		char *querystring;
		ListCell *lc;
		List *parsetree_list;
		Node *parsetree;
		SelectStmt *sel;
		Datum tmp;
		bool isnull;
		Form_pipeline_query catrow = (Form_pipeline_query) GETSTRUCT(tup);
		ContAnalyzeContext *context;

		tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tup, Anum_pipeline_query_query, &isnull);
		querystring = deparse_query_def((Query *) stringToNode(TextDatumGetCString(tmp)));

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		sel = (SelectStmt *) parsetree;

		context = MakeContAnalyzeContext(make_parsestate(NULL), sel, Worker);
		collect_rels_and_streams((Node *) sel->fromClause, context);
		collect_types_and_cols((Node *) sel, context);

		foreach(lc, context->streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			bool found;
			Oid key;
			bool is_inferred = false;

			key = RangeVarGetRelid(rv, NoLock, false);
			entry = (StreamTargetsEntry *) hash_search(targets, &key, HASH_ENTER, &found);

			if (!found)
			{
				HASHCTL colsctl;

				MemSet(&colsctl, 0, sizeof(ctl));
				colsctl.keysize = NAMEDATALEN;
				colsctl.entrysize = sizeof(StreamColumnsEntry);

				entry->colstotypes = hash_create(rv->relname, 8, &colsctl, HASH_ELEM);
				entry->queries = NULL;
				entry->desc = NULL;
				entry->inferred = false;
			}

			RangeVarIsForStream(rv, &is_inferred);

			/* if it's a typed stream, we can just set the descriptor right away */
			if (is_inferred)
			{
				entry->inferred = true;
				add_coltypes(entry, context->types);
			}

			entry->queries = bms_add_member(entry->queries, catrow->id);
		}
	}

	heap_endscan(scandesc);

	/*
	 * Now we have enough information to infer a TupleDesc for each stream
	 */
	hash_seq_init(&status, targets);
	while ((entry = (StreamTargetsEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->inferred)
			infer_tupledesc(entry);
	}

	return targets;
}

/*
 * Deserialize a TupleDesc from a bytea *
 */
TupleDesc
UnpackTupleDesc(bytea *bytes)
{
	StringInfoData buf;
	List *names = NIL;
	List *types = NIL;
	List *mods = NIL;
	List *collations = NIL;
	int nbytes;
	int i;
	int natts = 0;

	nbytes = VARSIZE(bytes);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	natts = pq_getmsgint(&buf, 4);
	for (i=0; i<natts; i++)
	{
		const char *name = pq_getmsgstring(&buf);
		int type = pq_getmsgint(&buf, 4);
		int mod = pq_getmsgint(&buf, 4);
		int collation = pq_getmsgint(&buf, 4);

		names = lappend(names, makeString((char *) name));
		types = lappend_int(types, type);
		mods = lappend_int(mods, mod);
		collations = lappend_int(collations, collation);
	}

	return BuildDescFromLists(names, types, mods, collations);
}

/*
 * Serialize a TupleDesc to a bytea *
 */
bytea *
PackTupleDesc(TupleDesc desc)
{
	StringInfoData buf;
	bytea *result;
	int nbytes;
	int i;

	if (desc == NULL)
		return NULL;

	initStringInfo(&buf);

	pq_sendint(&buf, desc->natts, 4);

	for (i=0; i<desc->natts; i++)
	{
		Form_pg_attribute attr = desc->attrs[i];

		pq_sendstring(&buf, NameStr(attr->attname));
		pq_sendint(&buf, attr->atttypid, 4);
		pq_sendint(&buf, attr->atttypmod, 4);
		pq_sendint(&buf, attr->attcollation, 4);
	}

	nbytes = buf.len - buf.cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pq_copymsgbytes(&buf, VARDATA(result), nbytes);

	return result;
}

/*
 * update_pipeline_stream_targets_and_desc
 *
 * Serializes the given stream-to-queries mapping into the
 * pipeline_stream catalog table along with the given stream TupleDesc.
 */
static List *
update_pipeline_stream_catalog(Relation pipeline_stream, HTAB *hash)
{
	StreamTargetsEntry *entry;
	HASH_SEQ_STATUS scan;
	List *keys = NIL;

	hash_seq_init(&scan, hash);
	while ((entry = (StreamTargetsEntry *) hash_seq_search(&scan)) != NULL)
	{
		Bitmapset *queries = entry->queries;
		Size targetssize = queries->nwords * sizeof(bitmapword) + VARHDRSZ;
		bytea *targetsbytes = palloc0(targetssize);
		HeapTuple tup;
		Datum values[Natts_pipeline_stream];
		bool nulls[Natts_pipeline_stream];
		bool replaces[Natts_pipeline_stream];
		HeapTuple newtup;

		MemSet(nulls, false, Natts_pipeline_stream);
		MemSet(replaces, false, sizeof(replaces));

		memcpy(VARDATA(targetsbytes), queries->words, queries->nwords * sizeof(bitmapword));
		SET_VARSIZE(targetsbytes, targetssize);

		values[Anum_pipeline_stream_queries - 1] = PointerGetDatum(targetsbytes);
		replaces[Anum_pipeline_stream_queries - 1] = true;

		tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(entry->relid));
		Assert(HeapTupleIsValid(tup));

		/* Only update desc for inferred streams */
		if (entry->inferred)
		{
			values[Anum_pipeline_stream_desc - 1] = PointerGetDatum(PackTupleDesc(entry->desc));
			replaces[Anum_pipeline_stream_desc - 1] = true;
		}

		newtup = heap_modify_tuple(tup, pipeline_stream->rd_att,
				values, nulls, replaces);

		simple_heap_update(pipeline_stream, &newtup->t_self, newtup);
		CatalogUpdateIndexes(pipeline_stream, newtup);

		ReleaseSysCache(tup);

		CommandCounterIncrement();

		keys = lappend_oid(keys, entry->relid);
	}

	return keys;
}

/*
 * delete_nonexistent_streams
 */
static void
mark_nonexistent_streams(Relation pipeline_stream, List *keys)
{
	HeapScanDesc scandesc;
	HeapTuple tup;

	/* delete from the catalog any streams that aren't in the given list */
	scandesc = heap_beginscan_catalog(pipeline_stream, 0, NULL);
	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		ListCell *lc;
		bool found = false;
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);

		foreach(lc, keys)
		{
			Oid relid = lfirst_oid(lc);

			if (relid == row->relid)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			Datum values[Natts_pipeline_stream];
			bool nulls[Natts_pipeline_stream];
			bool replaces[Natts_pipeline_stream];
			HeapTuple newtup;
			bool isnull;

			SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

			/* If queries is already NULL, this is a noop */
			if (isnull)
				continue;

			MemSet(nulls, false, Natts_pipeline_stream);
			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pipeline_stream_queries - 1] = true;
			nulls[Anum_pipeline_stream_queries - 1] = true;
			replaces[Anum_pipeline_stream_desc - 1] = true;
			nulls[Anum_pipeline_stream_desc - 1] = true;

			newtup = heap_modify_tuple(tup, pipeline_stream->rd_att,
					values, nulls, replaces);

			simple_heap_update(pipeline_stream, &newtup->t_self, newtup);
			CatalogUpdateIndexes(pipeline_stream, newtup);

			CommandCounterIncrement();
		}
	}

	heap_endscan(scandesc);
}

/*
 * UpdatePipelineStreamCatalog
 */
void
UpdatePipelineStreamCatalog(void)
{
	Relation pipeline_query;
	Relation pipeline_stream;
	HTAB *hash;
	List *keys = NIL;

	pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	pipeline_stream = heap_open(PipelineStreamRelationId, RowExclusiveLock);

	hash = streams_to_meta(pipeline_query);
	keys = update_pipeline_stream_catalog(pipeline_stream, hash);
	mark_nonexistent_streams(pipeline_stream, keys);

	heap_close(pipeline_stream, NoLock);
	heap_close(pipeline_query, NoLock);
}

/*
 * GetStreamReaders
 *
 * Gets a bitmap indexed by continuous query id that represents which
 * queries are reading from the given stream.
 */
Bitmapset *
GetAllStreamReaders(Oid relid)
{
	HeapTuple tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(relid));
	bool isnull;
	Datum raw;
	bytea *bytes;
	int nbytes;
	int nwords;
	Bitmapset *result;

	if (!HeapTupleIsValid(tup))
		return NULL;

	raw = SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

	if (isnull)
		return NULL;

	bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(raw));
	nbytes = VARSIZE(bytes) - VARHDRSZ;
	nwords = nbytes / sizeof(bitmapword);

	result = palloc0(BITMAPSET_SIZE(nwords));
	result->nwords = nwords;

	memcpy(result->words, VARDATA(bytes), nbytes);

	ReleaseSysCache(tup);

	return result;
}

Bitmapset *
GetLocalStreamReaders(Oid relid)
{
	Bitmapset *readers = GetAllStreamReaders(relid);

	if (stream_targets && readers)
	{
		Bitmapset *local_readers = NULL;
		HeapTuple tuple;
		Form_pipeline_query row;
		int i = 0;
		Datum str = PointerGetDatum(cstring_to_text(stream_targets));
		Datum split = PointerGetDatum(cstring_to_text(","));

		while (++i)
		{
			Datum view_datum = DirectFunctionCall3(split_text, str, split, Int32GetDatum(i));
			char *view_name = text_to_cstring((const text *) DatumGetPointer(view_datum));
			RangeVar *rv;
			List *name = NIL;
			int j = 0;

			if (!strlen(view_name))
				break;

			view_datum = DirectFunctionCall1(btrim1, view_datum);
			view_name = text_to_cstring((const text *) DatumGetPointer(view_datum));

			/* deconstruct possible qualifiers */
			while (++j)
			{
				Datum qual_split = PointerGetDatum(cstring_to_text("."));
				Datum quals_datum = DirectFunctionCall3(split_text, view_datum, qual_split, Int32GetDatum(j));
				char *qual = text_to_cstring((const text *) DatumGetPointer(quals_datum));

				if (!strlen(qual))
					break;

				name = lappend(name, makeString(qual));
			}

			rv = makeRangeVarFromNameList(name);
			tuple = GetPipelineQueryTuple(rv);

			if (!HeapTupleIsValid(tuple))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
						errmsg("continuous view \"%s\" does not exist", view_name)));

			row = (Form_pipeline_query) GETSTRUCT(tuple);
			local_readers = bms_add_member(local_readers, row->id);

			ReleaseSysCache(tuple);
		}

		readers = bms_intersect(readers, local_readers);
		bms_free(local_readers);
	}

	return readers;
}

TupleDesc
GetInferredStreamTupleDesc(Oid relid, List *colnames)
{
	HeapTuple tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(relid));
	bool isnull;
	Datum raw;
	bytea *bytes;
	TupleDesc desc;
	ListCell *lc;
	List *attlist = NIL;
	Form_pg_attribute *attrs;
	AttrNumber attno = InvalidAttrNumber;
	int i;
	Form_pipeline_stream row;

	Assert (HeapTupleIsValid(tup));

	row = (Form_pipeline_stream) GETSTRUCT(tup);

	Assert(row->inferred);
	raw = SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_desc, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		return NULL;
	}

	bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(raw));
	desc = UnpackTupleDesc(bytes);

	ReleaseSysCache(tup);

	/*
	 * Now we need to build a new TupleDesc based on the subset and
	 * ordering of the columns we're interested in
	 */
	attno = 1;
	foreach(lc, colnames)
	{
		Value *colname = (Value *) lfirst(lc);
		int i;
		for (i=0; i<desc->natts; i++)
		{
			/*
			 * It's ok if no matching column was found in the all-streams TupleDesc,
			 * it just means nothing is reading that column and we can ignore it
			 */
			if (pg_strcasecmp(strVal(colname), NameStr(desc->attrs[i]->attname)) == 0)
			{
				Form_pg_attribute att = (Form_pg_attribute) desc->attrs[i];
				att->attnum = attno++;
				attlist = lappend(attlist, att);
				break;
			}
		}
	}

	attrs = palloc0(list_length(attlist) * sizeof(Form_pg_attribute));
	i = 0;
	foreach(lc, attlist)
		attrs[i++] = (Form_pg_attribute) lfirst(lc);

	return CreateTupleDesc(list_length(attlist), false, attrs);
}

/*
 * RangeVarIsForStream
 */
bool
RangeVarIsForStream(RangeVar *rv, bool *is_inferred)
{
	Relation rel = heap_openrv_extended(rv, NoLock, true);
	char relkind;
	Oid relid;
	HeapTuple tup;
	Form_pipeline_stream row;

	if (rel == NULL)
		return false;

	relkind = rel->rd_rel->relkind;
	relid = rel->rd_id;
	heap_close(rel, NoLock);

	if (relkind != RELKIND_STREAM)
		return false;

	if (is_inferred != NULL)
	{
		tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(relid));
		Assert(HeapTupleIsValid(tup));
		row = (Form_pipeline_stream) GETSTRUCT(tup);
		*is_inferred = row->inferred;
		ReleaseSysCache(tup);
	}

	return true;
}

/*
 * IsInferredStream
 */
bool
IsInferredStream(Oid relid)
{
	HeapTuple tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(relid));
	Form_pipeline_stream row;
	bool is_inferred;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_stream) GETSTRUCT(tup);
	is_inferred = row->inferred;
	ReleaseSysCache(tup);

	return is_inferred;
}

/*
 * IsStream
 */
bool IsStream(Oid relid)
{
	Relation rel = try_relation_open(relid, NoLock);
	char relkind;

	if (rel == NULL)
		return false;

	relkind = rel->rd_rel->relkind;
	heap_close(rel, NoLock);

	return relkind == RELKIND_STREAM;
}

/*
 * CreateInferredStream
 */
void
CreateInferredStream(RangeVar *rv)
{
	Oid relid;
	CreateStreamStmt *stmt;

	stmt = makeNode(CreateStreamStmt);
	stmt->base.relation = rv;
	stmt->base.tableElts = NIL;
	stmt->base.if_not_exists = false;
	stmt->is_inferred = true;

	transformCreateStreamStmt(stmt);

	relid = DefineRelation((CreateStmt *) stmt,
							RELKIND_STREAM,
							InvalidOid);
	CreatePipelineStreamEntry(stmt, relid);
}

/*
 * CreatePipelineStreamCatalogEntry
 */
void
CreatePipelineStreamEntry(CreateStreamStmt *stmt, Oid relid)
{
	Relation pipeline_stream = heap_open(PipelineStreamRelationId, RowExclusiveLock);
	Datum values[Natts_pipeline_stream];
	bool nulls[Natts_pipeline_stream];
	HeapTuple tup;
	ObjectAddress referenced;
	ObjectAddress dependent;
	Oid entry_oid;

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_stream_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pipeline_stream_inferred - 1] = BoolGetDatum(stmt->is_inferred);
	nulls[Anum_pipeline_stream_queries - 1] = true;
	nulls[Anum_pipeline_stream_desc - 1] = true;

	tup = heap_form_tuple(pipeline_stream->rd_att, values, nulls);
	simple_heap_insert(pipeline_stream, tup);
	CatalogUpdateIndexes(pipeline_stream, tup);

	entry_oid = HeapTupleGetOid(tup);

	CommandCounterIncrement();

	/* Record dependency between tuple in pipeline_stream and the relation */
	dependent.classId = PipelineStreamRelationId;
	dependent.objectId = entry_oid;
	dependent.objectSubId = 0;
	referenced.classId = RelationRelationId;
	referenced.objectId = relid;
	referenced.objectSubId = 0;
	recordDependencyOn(&dependent, &referenced, DEPENDENCY_INTERNAL);

	CommandCounterIncrement();

	heap_close(pipeline_stream, NoLock);
}

/*
 * RemovePipelineStreamById
 */
void
RemovePipelineStreamById(Oid oid)
{
	Relation pipeline_stream;
	HeapTuple tuple;

	pipeline_stream = heap_open(PipelineStreamRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(PIPELINESTREAMOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for stream with OID %u", oid);

	simple_heap_delete(pipeline_stream, &tuple->t_self);

	ReleaseSysCache(tuple);

	CommandCounterIncrement();

	heap_close(pipeline_stream, RowExclusiveLock);
}

/*
 * inferred_stream_open
 */
Relation
inferred_stream_open(ParseState *pstate, Relation rel)
{
	TupleDesc desc = NULL;
	Relation stream_rel;

	Assert(pstate->p_allow_streams);
	Assert(is_inferred_stream_relation(rel));

	if (pstate->p_cont_view_context)
		desc = parserGetStreamDescr(rel->rd_id, pstate->p_cont_view_context);
	else if (pstate->p_ins_cols)
		desc = GetInferredStreamTupleDesc(rel->rd_id, pstate->p_ins_cols);
	else
		elog(ERROR, "inferred_stream_open called in an invalid context");

	/* Create a dummy Relation for the inferred stream */
	stream_rel = (Relation) palloc0(sizeof(RelationData));
	stream_rel->rd_att = desc;
	stream_rel->rd_rel = palloc0(sizeof(FormData_pg_class));
	stream_rel->rd_rel->relnatts = stream_rel->rd_att->natts;
	namestrcpy(&stream_rel->rd_rel->relname, NameStr(rel->rd_rel->relname));
	stream_rel->rd_rel->relkind = RELKIND_STREAM;
	stream_rel->rd_id = rel->rd_id;
	stream_rel->rd_rel->relnamespace = rel->rd_rel->relnamespace;
	stream_rel->rd_refcnt = 1; /* needs for copy */

	heap_close(rel, NoLock);

	return stream_rel;
}

/*
 * inferred_stream_close
 */
void
inferred_stream_close(Relation rel)
{
	Assert(is_inferred_stream_relation(rel));

	pfree(rel->rd_rel);
	pfree(rel);
}
