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
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_target.h"
#include "parser/analyze.h"
#include "pipeline/analyzer.h"
#include "pipeline/stream.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
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
} StreamTargetsEntry;

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
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		ContAnalyzeContext *context;

		tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
		querystring = deparse_query_def((Query *) stringToNode(TextDatumGetCString(tmp)));

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		sel = (SelectStmt *) parsetree;

		context = MakeContAnalyzeContext(make_parsestate(NULL), sel, Worker);
		collect_rels_and_streams((Node *) sel->fromClause, context);

		foreach(lc, context->streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			bool found;
			Oid key;

			key = RangeVarGetRelid(rv, NoLock, false);
			entry = (StreamTargetsEntry *) hash_search(targets, &key, HASH_ENTER, &found);

			if (!found)
			{
				HASHCTL colsctl;

				MemSet(&colsctl, 0, sizeof(ctl));
				colsctl.keysize = NAMEDATALEN;
				colsctl.entrysize = sizeof(StreamColumnsEntry);

				entry->queries = NULL;
			}

			if (row->active)
				entry->queries = bms_add_member(entry->queries, row->id);
		}
	}

	heap_endscan(scandesc);

	return targets;
}

bool
is_stream_relid(Oid relid)
{
	Relation rel = heap_open(relid, NoLock);
	bool result = rel->rd_rel->relkind == RELKIND_STREAM;

	heap_close(rel, NoLock);

	return result;
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
	TupleDesc desc;

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

	desc = BuildDescFromLists(names, types, mods, collations);
	pfree(buf.data);

	return desc;
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

	pfree(buf.data);

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
		Size targetssize;
		bytea *targetsbytes;
		HeapTuple tup;
		Datum values[Natts_pipeline_stream];
		bool nulls[Natts_pipeline_stream];
		bool replaces[Natts_pipeline_stream];
		HeapTuple newtup;

		MemSet(nulls, false, Natts_pipeline_stream);
		MemSet(replaces, false, sizeof(replaces));

		if (queries)
		{
			targetssize = queries->nwords * sizeof(bitmapword) + VARHDRSZ;
			targetsbytes = palloc0(targetssize);
			memcpy(VARDATA(targetsbytes), queries->words, queries->nwords * sizeof(bitmapword));
			SET_VARSIZE(targetsbytes, targetssize);
			values[Anum_pipeline_stream_queries - 1] = PointerGetDatum(targetsbytes);
		}
		else
			nulls[Anum_pipeline_stream_queries - 1] = true;

		replaces[Anum_pipeline_stream_queries - 1] = true;

		tup = SearchSysCache1(PIPELINESTREAMRELID, ObjectIdGetDatum(entry->relid));
		if (!HeapTupleIsValid(tup))
			continue;

		newtup = heap_modify_tuple(tup, pipeline_stream->rd_att, values, nulls, replaces);

		simple_heap_update(pipeline_stream, &newtup->t_self, newtup);
		CatalogUpdateIndexes(pipeline_stream, newtup);

		ReleaseSysCache(tup);

		CommandCounterIncrement();

		keys = lappend_oid(keys, entry->relid);
	}

	return keys;
}

/*
 * mark_nonexistent_streams
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

	raw = SysCacheGetAttr(PIPELINESTREAMRELID, tup,
						  Anum_pipeline_stream_queries, &isnull);

	if (isnull)
	{
		ReleaseSysCache(tup);
		return NULL;
	}

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

	if (stream_targets && strlen(stream_targets) && readers)
	{
		Bitmapset *local_readers = NULL;
		HeapTuple tuple;
		Form_pipeline_query row;
		char *targets = pstrdup(stream_targets);
		List *streams;
		ListCell *lc;

		if (!SplitIdentifierString(targets, ',', &streams))
		{
			/* syntax error in stream_targets */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"stream_targets\"")));
		}

		foreach(lc, streams)
		{
			char *view_name = (char *) lfirst(lc);
			RangeVar *rv;
			List *name = NIL;
			int i = 0;

			/* deconstruct possible qualifiers */
			while (++i)
			{
				Datum qual_split = PointerGetDatum(cstring_to_text("."));
				Datum view_datum = PointerGetDatum(cstring_to_text(view_name));
				Datum quals_datum = DirectFunctionCall3(split_text, view_datum, qual_split, Int32GetDatum(i));
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
		pfree(targets);
		list_free(streams);
	}

	return readers;
}

/*
 * RangeVarIsForStream
 */
bool
RangeVarIsForStream(RangeVar *rv)
{
	Relation rel = heap_openrv(rv, NoLock);
	bool is_stream;

	is_stream = rel->rd_rel->relkind == RELKIND_STREAM;
	heap_close(rel, NoLock);

	return is_stream;
}

/*
 * IsStream
 */
bool IsStream(Oid relid)
{
	Relation rel = try_relation_open(relid, NoLock);
	bool result;

	if (rel == NULL)
		return false;

	result = rel->rd_rel->relkind == RELKIND_STREAM;
	heap_close(rel, NoLock);

	return result;
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
	nulls[Anum_pipeline_stream_queries - 1] = true;

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
