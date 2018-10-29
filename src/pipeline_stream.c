/*-------------------------------------------------------------------------
 *
 * pipeline_query.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "config.h"
#include "commands/dbcommands.h"
#include "nodes/parsenodes.h"
#include "miscadmin.h"
#include "miscutils.h"
#include "parser/parse_type.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "scheduler.h"
#include "stats.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

Oid PipelineStreamRelationOid = InvalidOid;

char *stream_targets;

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
 * validate_stream_constraints
 *
 * We allow some stuff that is technically supported by the grammar
 * for CREATE STREAM, so we do some validation here so that we can generate more
 * informative errors than simply syntax errors.
 */
static void
validate_stream_constraints(CreateStmt *stmt)
{
	ListCell *lc;

	foreach(lc, stmt->tableElts)
	{
		Node *n = (Node *) lfirst(lc);
		ColumnDef *cdef;

		if (!IsA(n, ColumnDef))
			continue;

		cdef = (ColumnDef *) n;
		if (cdef->constraints)
		{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot create constraint on stream %s", stmt->relation->relname),
						 errhint("Constraints are currently unsupported on streams.")));
		}
	}
}

/*
 * transformCreateStreamStmt
 */
void
transformCreateStreamStmt(CreateForeignTableStmt *stmt)
{
	ListCell *lc;
	bool saw_atime = false;

	validate_stream_constraints((CreateStmt *) stmt);

	foreach(lc, stmt->base.tableElts)
	{
		ColumnDef *coldef = (ColumnDef *) lfirst(lc);

		if (pg_strcasecmp(coldef->colname, ARRIVAL_TIMESTAMP) == 0)
		{
			Oid typ;
			int typmod;

			typenameTypeIdAndMod(NULL, coldef->typeName, &typ, &typmod);

			/*
			 * HACK(usmanm): If arrival_timestamp is the last column and has the correct type, then let it slide. This
			 * it for making CREATE STREAM ... (LIKE ...) and pg_dump/pg_restore to work. Should be fixed by #1616.
			 */
			if (!lnext(lc) && typ == TIMESTAMPTZOID)
				saw_atime = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column name \"%s\" conflicts with a system column name",
								ARRIVAL_TIMESTAMP)));
		}
	}

	if (!saw_atime)
	{
		ColumnDef *coldef;
		TypeName *typename;

		typename = makeNode(TypeName);
		typename->typeOid = TIMESTAMPTZOID;
		typename->typemod = -1;

		coldef = makeNode(ColumnDef);
		coldef->colname = ARRIVAL_TIMESTAMP;
		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->constraints = NIL;
		coldef->typeName = typename;

		stmt->base.tableElts = lappend(stmt->base.tableElts, coldef);
	}
}

/*
 * has_pipeline_stream_row
 */
static bool
has_pipeline_stream_row(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINESTREAMRELID, 1, relid);

	if (!HeapTupleIsValid(tup))
		return false;

	return true;
}

/*
 * RangeVarIsStream
 */
bool
RangeVarIsStream(RangeVar *rv, bool missing_ok)
{
	Oid relid;

	/* Ignore cross-database references */
	if (rv->catalogname && pg_strcasecmp(rv->catalogname, get_database_name(MyDatabaseId)) != 0)
		return false;

	relid = RangeVarGetRelid(rv, NoLock, missing_ok);
	if (!OidIsValid(relid))
		return false;

	if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
		return false;

	return has_pipeline_stream_row(relid);
}

/*
 * RelidIsStream
 */
bool
RelidIsStream(Oid relid)
{
	return has_pipeline_stream_row(relid);
}

/*
 * RelidIsOutputStream
 */
bool
RelidIsOutputStream(Oid relid)
{
	HeapTuple tup = PipelineCatalogLookup(PIPELINEQUERYOSRELID, 1, ObjectIdGetDatum(relid));

	return HeapTupleIsValid(tup);
}

/*
 * OpenPipelineStream
 */
Relation
OpenPipelineStream(LOCKMODE mode)
{
	return heap_open(GetPipelineStreamOid(), mode);
}

void
ClosePipelineStream(Relation ps, LOCKMODE mode)
{
	heap_close(ps, mode);
}

/*
 * CreatePipelineStreamCatalogEntry
 */
void
CreatePipelineStreamEntry(CreateForeignTableStmt *stmt, Oid relid)
{
	Relation ps = OpenPipelineStream(RowExclusiveLock);
	Datum values[Natts_pipeline_stream];
	bool nulls[Natts_pipeline_stream];
	HeapTuple tup;
	ObjectAddress referenced;
	ObjectAddress dependent;

	/*
	 * The stream will depend on its corresponding pipelinedb.stream catalog row
	 */
	dependent.classId = RelationRelationId;
	dependent.objectId = relid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineStreamOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	/*
	 * This stream will CQs reading from it so it depends on pipelinedb.cont_query
	 */
	dependent.classId = RelationRelationId;
	dependent.objectId = relid;
	dependent.objectSubId = 0;

	referenced.classId = RelationRelationId;
	referenced.objectId = GetPipelineQueryOid();
	referenced.objectSubId = 0;

	recordDependencyOn(&dependent, &referenced, DEPENDENCY_NORMAL);

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_stream_relid - 1] = ObjectIdGetDatum(relid);
	nulls[Anum_pipeline_stream_queries - 1] = true;

	tup = heap_form_tuple(RelationGetDescr(ps), values, nulls);

	PipelineCatalogTupleInsert(ps, tup);
	CommandCounterIncrement();

	ClosePipelineStream(ps, NoLock);
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

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(StreamTargetsEntry);
	ctl.hash = oid_hash;

	targets = hash_create("streams_to_targets_and_desc", 32, &ctl, HASH_ELEM | HASH_FUNCTION);
	scandesc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		bool found;

		entry = (StreamTargetsEntry *) hash_search(targets, &row->streamrelid, HASH_ENTER, &found);

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

	heap_endscan(scandesc);

	return targets;
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
		Bitmapset *current;
		Datum raw;
		bytea *bytes;
		Size targetssize;
		bytea *targetsbytes;
		HeapTuple tup;
		Datum values[Natts_pipeline_stream];
		bool nulls[Natts_pipeline_stream];
		bool replaces[Natts_pipeline_stream];
		HeapTuple newtup;
		bool isnull;

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

		tup = PipelineCatalogLookupForUpdate(pipeline_stream, PIPELINESTREAMRELID, ObjectIdGetDatum(entry->relid));

		if (!HeapTupleIsValid(tup))
			continue;

		raw = PipelineCatalogGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

		if (!isnull)
		{
			int nbytes;
			int nwords;

			bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(raw));
			nbytes = VARSIZE(bytes) - VARHDRSZ;
			nwords = nbytes / sizeof(bitmapword);

			current = palloc0(BITMAPSET_SIZE(nwords));
			current->nwords = nwords;

			memcpy(current->words, VARDATA(bytes), nbytes);
		}

		/* If the queries changed, replace the value */
		if (isnull || !bms_equal(current, queries))
		{
			replaces[Anum_pipeline_stream_queries - 1] = true;
			newtup = heap_modify_tuple(tup, pipeline_stream->rd_att, values, nulls, replaces);
			PipelineCatalogTupleUpdate(pipeline_stream, &newtup->t_self, newtup);
		}

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

			PipelineCatalogGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

			/* If queries is already NULL, this is a noop */
			if (isnull)
				continue;

			MemSet(nulls, false, Natts_pipeline_stream);
			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pipeline_stream_queries - 1] = true;
			nulls[Anum_pipeline_stream_queries - 1] = true;

			newtup = heap_modify_tuple(tup, pipeline_stream->rd_att,
					values, nulls, replaces);

			PipelineCatalogTupleUpdate(pipeline_stream, &newtup->t_self, newtup);
			CommandCounterIncrement();
		}
	}

	heap_endscan(scandesc);
}

/*
 * SyncPipelineStreamReaders
 *
 * Sync the active stream readers' Bitmapsets for all streams
 */
void
SyncPipelineStreamReaders(void)
{
	Relation pipeline_query;
	Relation pipeline_stream;
	HTAB *hash;
	List *keys = NIL;
	MemoryContext old;
	MemoryContext tmp_cxt;

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext, "UpdatePipelineStreamCatalogCxt",
					ALLOCSET_DEFAULT_MINSIZE,
					ALLOCSET_DEFAULT_INITSIZE,
					ALLOCSET_DEFAULT_MAXSIZE);

	pipeline_query = heap_open(PipelineQueryRelationOid, NoLock);
	pipeline_stream = heap_open(PipelineStreamRelationOid, RowExclusiveLock);

	/*
	 * The following series of function calls can consume quite a bit
	 * of memory and UpdatePipelineStreamCatalog can be called may times
	 * in one transaction, so we free everything aggressively.
	 */
	old = MemoryContextSwitchTo(tmp_cxt);

	hash = streams_to_meta(pipeline_query);
	keys = update_pipeline_stream_catalog(pipeline_stream, hash);
	mark_nonexistent_streams(pipeline_stream, keys);

	hash_destroy(hash);

	MemoryContextSwitchTo(old);
	MemoryContextDelete(tmp_cxt);

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
	HeapTuple tup = PipelineCatalogLookup(PIPELINESTREAMRELID, 1, ObjectIdGetDatum(relid));
	bool isnull;
	Datum raw;
	bytea *bytes;
	int nbytes;
	int nwords;
	Bitmapset *result;

	if (!HeapTupleIsValid(tup))
		return NULL;

	raw = PipelineCatalogGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

	if (isnull)
		return NULL;

	bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(raw));
	nbytes = VARSIZE(bytes) - VARHDRSZ;
	nwords = nbytes / sizeof(bitmapword);

	result = palloc0(BITMAPSET_SIZE(nwords));
	result->nwords = nwords;

	memcpy(result->words, VARDATA(bytes), nbytes);

	return result;
}

/*
 * GetLocalStreamReaders
 */
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
				elog(ERROR, "continuous view \"%s\" does not exist", view_name);

			row = (Form_pipeline_query) GETSTRUCT(tuple);
			local_readers = bms_add_member(local_readers, row->id);

		}

		readers = bms_intersect(readers, local_readers);
		bms_free(local_readers);
		pfree(targets);
		list_free(streams);
	}

	return readers;
}

/*
 * SyncPipelineStream
 */
void
SyncPipelineStream(void)
{
	HeapTuple tup;
	Relation pipeline_stream;
	HeapScanDesc scan_desc;

	if (pg_class_aclcheck(GetPipelineStreamOid(), GetUserId(), ACL_DELETE) != ACLCHECK_OK)
		return;

	pipeline_stream = OpenPipelineStream(NoLock);
	scan_desc = heap_beginscan_catalog(pipeline_stream, 0, NULL);

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);
		if (!get_rel_name(row->relid))
		{
			PipelineCatalogTupleDelete(pipeline_stream, &tup->t_self);
			PurgeDeadStreamStats(row->relid);
		}
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_stream, NoLock);
}
