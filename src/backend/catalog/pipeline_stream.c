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
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "funcapi.h"
#include "pipeline/cqanalyze.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct StreamTargetsEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	Bitmapset *targets;
} StreamTargetsEntry;

/*
 * streams_to_cq_ids
 *
 * Scans the pipeline_query catalog table and reads the FROM clause of
 * every active query to determined which streams each query is
 * reading from. A mapping from stream name to a all of the pipeline_query
 * ids reading from that stream is returned.
 */
static HTAB *
streams_to_cq_ids(Relation pipeline_query)
{
	HeapScanDesc scandesc;
	HASHCTL ctl;
	HTAB *targets;
	StreamTargetsEntry *entry;
	HeapTuple tup;

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(StreamTargetsEntry);

	targets = hash_create("streams_to_cq_ids", 32, &ctl, HASH_ELEM);
	scandesc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		char *querystring;
		ListCell *lc;
		List *parsetree_list;
		Node *parsetree;
		CreateContinuousViewStmt *cv;
		SelectStmt *select;
		Datum tmp;
		bool isnull;
		Form_pipeline_query catrow = (Form_pipeline_query) GETSTRUCT(tup);
		CQAnalyzeContext context;

		if (catrow->state != PIPELINE_QUERY_STATE_ACTIVE)
			continue;

		tmp = SysCacheGetAttr(PIPELINEQUERYNAME, tup, Anum_pipeline_query_query, &isnull);
		querystring = TextDatumGetCString(tmp);

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		cv = (CreateContinuousViewStmt *) parsetree;
		select = (SelectStmt *) cv->query;

		context.tables = NIL;
		context.streams = NIL;
		AddStreams((Node *) select->fromClause, &context);

		foreach(lc, context.streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			bool found;

			entry = (StreamTargetsEntry *) hash_search(targets, (void *) rv->relname, HASH_ENTER, &found);

			if (!found)
				entry->targets = NULL;

			entry->targets = bms_add_member(entry->targets, catrow->id);
		}
	}

	heap_endscan(scandesc);

	return targets;
}

/*
 * cache_stream_targets
 *
 * Serializes the given stream-to-queries mapping into the
 * pipeline_stream catalog table. Existing streams are updated,
 * and new streams are inserted.
 */
static List *
cache_stream_targets(Relation pipeline_stream, HTAB *targets)
{
	HASH_SEQ_STATUS scan;
	StreamTargetsEntry *entry;
	List *names = NIL;

	hash_seq_init(&scan, targets);
	while ((entry = (StreamTargetsEntry *) hash_seq_search(&scan)) != NULL)
	{
		char *sname = entry->key;
		Bitmapset *bms = entry->targets;
		int nbytes = bms->nwords * sizeof(bitmapword);
		Size size = nbytes + VARHDRSZ;
		bytea *bytes = palloc0(size);
		HeapTuple tup;
		Datum values[Natts_pipeline_stream];
		bool nulls[Natts_pipeline_stream];

		if (bms_is_empty(bms))
			continue;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, Natts_pipeline_stream);

		memcpy(VARDATA(bytes), bms->words, nbytes);
		SET_VARSIZE(bytes, size);

		values[Anum_pipeline_stream_targets - 1] = PointerGetDatum(bytes);

		tup = SearchSysCache1(PIPELINESTREAMNAME, CStringGetDatum(sname));
		if (HeapTupleIsValid(tup))
		{
			/* catalog entry exists, update it */
			bool replaces[Natts_pipeline_stream];
			HeapTuple newtup;

			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pipeline_stream_targets - 1] = true;

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
			values[Anum_pipeline_stream_name - 1] = NameGetDatum(&name);
			values[Anum_pipeline_stream_targets - 1] = PointerGetDatum(bytes);

			tup = heap_form_tuple(pipeline_stream->rd_att, values, nulls);

			simple_heap_insert(pipeline_stream, tup);
			CatalogUpdateIndexes(pipeline_stream, tup);
		}

		CommandCounterIncrement();
		names = lappend(names, sname);
	}

	return names;
}

/*
 * delete_nonexistent_streams
 */
static int
delete_nonexistent_streams(Relation pipeline_stream, List *added)
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

		foreach(lc, added)
		{
			char *name = (char *) lfirst(lc);
			if (strcmp(name, NameStr(row->name)) == 0)
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
 * UpdateStreamTargets
 *
 * Updates the pipeline_stream catalog to reflect which continuous queries
 * are reading from each stream, and removes any streams that aren't being
 * read from.
 */
void
UpdateStreamTargets(Relation pipeline_query)
{
	Relation pipeline_stream;
	List *added = NIL;
	HTAB *targets;

	targets = streams_to_cq_ids(pipeline_query);

	pipeline_stream = heap_open(PipelineStreamRelationId, ExclusiveLock);
	added = cache_stream_targets(pipeline_stream, targets);
	delete_nonexistent_streams(pipeline_stream, added);
	heap_close(pipeline_stream, NoLock);

	hash_destroy(targets);
}

/*
 * GetTargetsFor
 *
 * Gets a bitmap indexed by continuous query id that represents which
 * queries are reading from the given stream.
 */
Bitmapset *
GetTargetsFor(const char *stream)
{
	HeapTuple tup = SearchSysCache1(PIPELINESTREAMNAME, CStringGetDatum(stream));
	bool isnull;
	Datum raw;
	bytea *bytes;
	int nbytes;
	int nwords;
	Bitmapset *result;

	if (!HeapTupleIsValid(tup))
		return NULL;

	raw = SysCacheGetAttr(PIPELINESTREAMNAME, tup, Anum_pipeline_stream_targets, &isnull);

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
