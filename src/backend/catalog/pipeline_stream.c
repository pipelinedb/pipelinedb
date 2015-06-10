/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipeline_stream.c
 *	  routines to support manipulation of the pipeline_stream relation
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
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
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

typedef struct Key
{
	Oid namespace;
	char name[NAMEDATALEN];
} Key;

typedef struct StreamColumnsEntry
{
	char name[NAMEDATALEN];
	List *types;
} StreamColumnsEntry;

typedef struct StreamTargetsEntry
{
	Key key; /* hash key --- MUST BE FIRST */
	Bitmapset *queries;
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

		sprintf(err, "type conflict with stream \"%s\":", stream->key.name);
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

	ctl.keysize = sizeof(Key);
	ctl.entrysize = sizeof(StreamTargetsEntry);
	ctl.match = memcmp;
	ctl.keycopy = memcpy;

	targets = hash_create("streams_to_targets_and_desc", 32, &ctl, HASH_ELEM | HASH_COMPARE | HASH_KEYCOPY);
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
		querystring = TextDatumGetCString(tmp);

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		sel = (SelectStmt *) parsetree;

		context = MakeContAnalyzeContext(make_parsestate(NULL), sel);
		collect_rels_and_streams((Node *) sel->fromClause, context);
		collect_types_and_cols((Node *) sel, context);

		foreach(lc, context->streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			bool found;
			Key key;

			MemSet(&key, 0, sizeof(Key));

			/* if the stream doesn't have a namespace, assume it's in the same namespace as this CV */
			if (rv->schemaname)
				key.namespace = get_namespace_oid(rv->schemaname, false);
			else
				key.namespace = catrow->namespace;

			strcpy(key.name, rv->relname);

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
			}

			/* if it's a typed stream, we can just set the descriptor right away */
			if (RangeVarIsForTypedStream(rv))
			{
				Relation rel = heap_openrv(rv, AccessShareLock);
				entry->desc = CreateTupleDescCopyConstr(RelationGetDescr(rel));

				heap_close(rel, AccessShareLock);
			}
			else
			{
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
		if (!entry->desc)
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
		Oid namespace = entry->key.namespace;
		char *sname = entry->key.name;
		Bitmapset *queries = entry->queries;
		Size targetssize = queries->nwords * sizeof(bitmapword) + VARHDRSZ;
		bytea *targetsbytes = palloc0(targetssize);
		HeapTuple tup;
		Datum values[Natts_pipeline_stream];
		bool nulls[Natts_pipeline_stream];

		MemSet(values, 0, Natts_pipeline_stream);
		MemSet(nulls, false, Natts_pipeline_stream);

		memcpy(VARDATA(targetsbytes), queries->words, queries->nwords * sizeof(bitmapword));
		SET_VARSIZE(targetsbytes, targetssize);

		values[Anum_pipeline_stream_queries - 1] = PointerGetDatum(targetsbytes);
		values[Anum_pipeline_stream_desc - 1] = PointerGetDatum(PackTupleDesc(entry->desc));

		tup = SearchSysCache2(PIPELINESTREAMNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(sname));

		if (HeapTupleIsValid(tup))
		{
			/* catalog entry exists, update it */
			bool replaces[Natts_pipeline_stream];
			HeapTuple newtup;

			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pipeline_stream_queries - 1] = true;
			replaces[Anum_pipeline_stream_desc - 1] = true;

			newtup = heap_modify_tuple(tup, pipeline_stream->rd_att,
					values, nulls, replaces);

			simple_heap_update(pipeline_stream, &newtup->t_self, newtup);
			CatalogUpdateIndexes(pipeline_stream, newtup);

			ReleaseSysCache(tup);
		}
		else
		{
			/* create a new catalog entry */
			NameData name;

			namestrcpy(&name, sname);
			values[Anum_pipeline_stream_namespace - 1] = ObjectIdGetDatum(namespace);
			values[Anum_pipeline_stream_name - 1] = NameGetDatum(&name);

			tup = heap_form_tuple(pipeline_stream->rd_att, values, nulls);

			simple_heap_insert(pipeline_stream, tup);
			CatalogUpdateIndexes(pipeline_stream, tup);
		}

		CommandCounterIncrement();

		keys = lappend(keys, &entry->key);
	}

	return keys;
}

/*
 * delete_nonexistent_streams
 */
static int
delete_nonexistent_streams(Relation pipeline_stream, List *keys)
{
	HeapScanDesc scandesc;
	HeapTuple tup;
	int deleted = 0;

	/* delete from the catalog any streams that aren't in the given list */
	scandesc = heap_beginscan_catalog(pipeline_stream, 0, NULL);
	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		ListCell *lc;
		bool found = false;
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);

		foreach(lc, keys)
		{
			Key *key = lfirst(lc);
			if (key->namespace == row->namespace && pg_strcasecmp(key->name, NameStr(row->name)) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			simple_heap_delete(pipeline_stream, &tup->t_self);
			deleted++;
			CommandCounterIncrement();
		}
	}

	heap_endscan(scandesc);

	return deleted;
}

/*
 * UpdatePipelineStreamCatalog
 */
void
UpdatePipelineStreamCatalog(Relation pipeline_query)
{
	Relation pipeline_stream;
	HTAB *hash;
	List *keys = NIL;

	hash = streams_to_meta(pipeline_query);

	pipeline_stream = heap_open(PipelineStreamRelationId, ExclusiveLock);
	keys = update_pipeline_stream_catalog(pipeline_stream, hash);
	delete_nonexistent_streams(pipeline_stream, keys);
	heap_close(pipeline_stream, NoLock);
}

/*
 * GetStreamReaders
 *
 * Gets a bitmap indexed by continuous query id that represents which
 * queries are reading from the given stream.
 */
Bitmapset *
GetAllStreamReaders(Oid namespace, char *name)
{
	HeapTuple tup = SearchSysCache2(PIPELINESTREAMNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(name));
	bool isnull;
	Datum raw;
	bytea *bytes;
	int nbytes;
	int nwords;
	Bitmapset *result;

	if (!HeapTupleIsValid(tup))
		return NULL;

	raw = SysCacheGetAttr(PIPELINESTREAMNAMESPACENAME, tup, Anum_pipeline_stream_queries, &isnull);

	Assert(!isnull);

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
GetLocalStreamReaders(Oid namespace, char *name)
{
	Bitmapset *readers = GetAllStreamReaders(namespace, name);

	if (stream_targets != NULL)
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
GetStreamTupleDesc(Oid namespace, char *name, List *colnames)
{
	HeapTuple tup = SearchSysCache2(PIPELINESTREAMNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(name));
	bool isnull;
	Datum raw;
	bytea *bytes;
	TupleDesc alldesc;
	ListCell *lc;
	List *attlist = NIL;
	Form_pg_attribute *attrs;
	AttrNumber attno = InvalidAttrNumber;
	int i;

	if (!HeapTupleIsValid(tup))
	{
		RangeVar *rv = makeRangeVar(get_namespace_name(namespace), name, -1);
		if (RangeVarIsForTypedStream(rv))
		{
			/*
			 * Technically this can happen with a prepared insert against a typed stream,
			 * which has a use for the descriptor even before the stream is being read by
			 * anything. If the prepared insert is executed against a stream with no readers,
			 * an error will still be thrown.
			 */
			Relation rel = heap_openrv(rv, NoLock);
			alldesc = CreateTupleDescCopy(RelationGetDescr(rel));
			heap_close(rel, NoLock);
			return alldesc;
		}
		return NULL;
	}

	raw = SysCacheGetAttr(PIPELINESTREAMNAMESPACENAME, tup, Anum_pipeline_stream_desc, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		return NULL;
	}

	bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(raw));
	alldesc = UnpackTupleDesc(bytes);

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
		for (i=0; i<alldesc->natts; i++)
		{
			/*
			 * It's ok if no matching column was found in the all-streams TupleDesc,
			 * it just means nothing is reading that column and we can ignore it
			 */
			if (pg_strcasecmp(strVal(colname), NameStr(alldesc->attrs[i]->attname)) == 0)
			{
				Form_pg_attribute att = (Form_pg_attribute) alldesc->attrs[i];
				att->attnum = attno++;
				attlist = lappend(attlist, att);
				break;
			}
		}
	}

	attrs = palloc0(list_length(attlist) * sizeof(Form_pg_attribute));
	i = 0;
	foreach(lc, attlist)
	{
		attrs[i++] = (Form_pg_attribute) lfirst(lc);
	}

	return CreateTupleDesc(list_length(attlist), false, attrs);
}

/*
 * RangeVarIsTypedStream
 *
 * Is the given relation a typed stream (meaning it was created with CREATE STREAM)?
 */
bool
RangeVarIsForTypedStream(RangeVar *rv)
{
	Relation rel = heap_openrv_extended(rv, AccessShareLock, true);
	char relkind;

	if (rel == NULL)
		return false;

	relkind = rel->rd_rel->relkind;
	relation_close(rel, AccessShareLock);

	return (relkind == RELKIND_STREAM);
}

/*
 * RangeVarIsForStream
 */
bool
RangeVarIsForStream(RangeVar *stream)
{

	return RangeVarIsForTypedStream(stream) || RangeVarIsForInferredStream(stream);
}

/*
 * RangeVarIsForInferredStream
 */
bool
RangeVarIsForInferredStream(RangeVar *rv)
{
	Oid namespace = RangeVarGetCreationNamespace(rv);
	HeapTuple tup;

	tup = SearchSysCache2(PIPELINESTREAMNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tup))
		return false;

	ReleaseSysCache(tup);
	return true;
}

/*
 * GetStreamRelId
 *
 * This returns the *relid* for a stream RangeVar. The relid for a static stream is
 * the actual relid in the pg_class catalog, while the relid for an inferred stream is
 * it's OID in the pipeline_stream catalog.
 */
Oid
GetStreamRelId(RangeVar *stream)
{
	Relation rel = heap_openrv_extended(stream, AccessShareLock, true);
	Oid namespace;
	HeapTuple tup;
	Oid relid;

	if (rel)
	{
		char relkind = rel->rd_rel->relkind;
		relid = rel->rd_id;

		relation_close(rel, AccessShareLock);

		Assert(relkind == RELKIND_STREAM);

		return relid;
	}

	namespace = RangeVarGetAndCheckCreationNamespace(stream, NoLock, NULL);
	tup = SearchSysCache2(PIPELINESTREAMNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(stream->relname));

	if (!HeapTupleIsValid(tup))
		return InvalidOid;

	relid = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);

	return relid;
}

/*
 * GetStreamNamespace
 */
Oid
GetStreamNamespace(Oid stream_relid)
{
	Oid namespace;
	HeapTuple tup;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(stream_relid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_class reltup;
		reltup = (Form_pg_class) GETSTRUCT(tup);

		Assert(reltup->relkind == RELKIND_STREAM);

		namespace = reltup->relnamespace;
		ReleaseSysCache(tup);
		return namespace;
	}

	tup = SearchSysCache1(PIPELINESTREAMOID, ObjectIdGetDatum(stream_relid));

	if (HeapTupleIsValid(tup))
	{
		Form_pipeline_stream row;
		row = (Form_pipeline_stream) GETSTRUCT(tup);
		namespace = row->namespace;
		ReleaseSysCache(tup);
		return namespace;
	}

	return InvalidOid;
}

/*
 * RelIdIsForTypedStream
 */
bool
RelIdIsForTypedStream(Oid stream_relid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(stream_relid));
	if (HeapTupleIsValid(tp))
	{
		Assert(((Form_pg_class) GETSTRUCT(tp))->relkind == RELKIND_STREAM);
		ReleaseSysCache(tp);
		return true;
	}

	return false;
}

/*
 * RelIdIsForInferredStream
 */
bool
RelIdIsForInferredStream(Oid stream_relid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(PIPELINESTREAMOID, ObjectIdGetDatum(stream_relid));
	if (HeapTupleIsValid(tp))
	{
		ReleaseSysCache(tp);
		return true;
	}

	return false;
}

/*
 * GetRelationForStream
 */
Relation
GetRelationForStream(RangeVar *rv, List *cols)
{
	Oid stream_id = GetStreamRelId(rv);
	Oid namespace = GetStreamNamespace(stream_id);
	Relation rel = (Relation) palloc0(sizeof(RelationData));
	rel->rd_att = GetStreamTupleDesc(namespace, rv->relname, cols);
	rel->rd_rel = palloc0(sizeof(FormData_pg_class));
	rel->rd_rel->relnatts = rel->rd_att->natts;
	namestrcpy(&rel->rd_rel->relname, rv->relname);
	rel->rd_rel->relkind = RELKIND_STREAM;
	rel->rd_id = stream_id;
	rel->rd_rel->relnamespace = namespace;
	return rel;
}
