/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_queries.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "parser/analyze.h"
#include "pipeline/decode.h"
#include "pipeline/stream.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#define SEND_EVENTS_RESPONSE_COMPLETE 0
#define SEND_EVENTS_RESPONSE_MISMATCH 1
#define SEND_EVENTS_RESPONSE_FAILED	2

typedef struct StreamTagsEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	Bitmapset *tags;
} StreamTagsEntry;

/*
 * respond_send_events
 *
 * Sends a response from a datanode to a coordinator, signifying
 * that the given number of events were received
 */
int
RespondSendEvents(int numevents)
{
	StringInfoData resp;
	pq_beginmessage(&resp, '#');
	pq_sendint(&resp, numevents, 4);
	pq_endmessage(&resp);

	return pq_flush();
}

/*
 * close_stream
 *
 * Closes datanode connections and cleans up stream state
 */
void
CloseStream(EventStream stream)
{

}

/*
 * GetStreamTargets
 *
 * Builds a mapping from stream name to continuous view tagindexes that read from the stream
 */
StreamTargets *
CreateStreamTargets(void)
{
	HASHCTL ctl;
	StreamTargets *targets;
	Relation rel;
	HeapScanDesc scandesc;
	Form_pipeline_queries catrow;
	HeapTuple tup;
	MemoryContext oldcontext;

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(StreamTagsEntry);

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	targets = hash_create("StreamTargets", 32, &ctl, HASH_ELEM);

	rel = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);
	scandesc = heap_beginscan(rel, SnapshotAny, 0, NULL);

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

		catrow = (Form_pipeline_queries) GETSTRUCT(tup);
		tmp = SysCacheGetAttr(PIPELINEQUERIESNAME, tup, Anum_pipeline_queries_query, &isnull);
		querystring = TextDatumGetCString(tmp);

		parsetree_list = pg_parse_query(querystring);
		parsetree = (Node *) lfirst(parsetree_list->head);
		cv = (CreateContinuousViewStmt *) parsetree;
		select = (SelectStmt *) cv->query;

		foreach(lc, select->fromClause)
		{
			Node *node = (Node *) lfirst(lc);
			if (IsA(node, JoinExpr))
			{
				JoinExpr *j = (JoinExpr *) node;
				char *relname = ((RangeVar *) j->larg)->relname;
				bool found;
				StreamTagsEntry *entry =
						(StreamTagsEntry *) hash_search(targets, (void *) relname, HASH_ENTER, &found);

				if (!found)
					entry->tags = NULL;

				entry->tags = bms_add_member(entry->tags, catrow->id);

				relname = ((RangeVar *) j->rarg)->relname;
				entry = (StreamTagsEntry *) hash_search(targets, (void *) relname, HASH_ENTER, NULL);
				entry->tags = bms_add_member(entry->tags, catrow->id);
			}
			else if (IsA(node, RangeVar))
			{
				RangeVar *rv = (RangeVar *) node;
				bool found;
				StreamTagsEntry *entry =
						(StreamTagsEntry *) hash_search(targets, (void *) rv->relname, HASH_ENTER, &found);

				if (!found)
					entry->tags = NULL;

				entry->tags = bms_add_member(entry->tags, catrow->id);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("unrecognized node type found when determining stream targets: %d", nodeTag(node))));
			}
		}
	}

	heap_endscan(scandesc);
	heap_close(rel, AccessExclusiveLock);

	MemoryContextSwitchTo(oldcontext);

	return targets;
}

/*
 * CopyStreamTargets
 *
 * Copies the bitmap of the given stream's target CVs into the specified address.
 * This is used to load the bitmap into the stream buffer's shared memory.
 */
Bitmapset *
GetTargetsFor(const char *stream, StreamTargets *s)
{
	bool found = false;
	StreamTagsEntry *entry =
			(StreamTagsEntry *) hash_search(s, stream, HASH_FIND, &found);
	if (!found)
		return NULL;

	return entry->tags;
}

/*
 * DestroyStreamTargets
 *
 * Cleans up a StreamTargets object
 */
void
DestroyStreamTargets(StreamTargets *s)
{
	hash_destroy(s);
}

/*
 * InsertTargetIsStream
 *
 * Is the given INSERT statements target relation a stream?
 * We assume it is if the relation doesn't exist in the catalog as a normal relation.
 */
bool InsertTargetIsStream(InsertStmt *ins)
{
	Oid reloid = RangeVarGetRelid(ins->relation, NoLock, true);

	if (reloid != InvalidOid)
		return false;

	return IsInputStream(ins->relation->relname);
}

/*
 * InsertIntoStream
 *
 * Send INSERT-encoded events to the given stream
 *
 *
 */
int
InsertIntoStream(InsertStmt *ins)
{
	SelectStmt *sel = (SelectStmt *) ins->selectStmt;
	ListCell *lc;
	Size size = 0;
	int numcols = list_length(ins->cols);
	char **sharedfields = NULL;
	int i;
	int count = 0;

	/* make sure all tuples are of the correct length before sending any */
	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		if (list_length(vals) < numcols)
			elog(ERROR, "VALUES tuples must have at least %d values", numcols);
	}

	sharedfields = ShmemAlloc(numcols * sizeof(char *));

	/* build header */
	for (i=0; i<numcols; i++)
	{
		ListCell *rtc;
		ResTarget *res = (ResTarget *) list_nth(ins->cols, i);
		int count = 0;

		/* verify that each column only appears once */
		foreach(rtc, ins->cols)
		{
			ResTarget *r = (ResTarget *) lfirst(rtc);
			if (strcmp(r->name, res->name) == 0)
				count++;
		}

		if (count > 1)
			elog(ERROR, "column \"%s\" appears more than once in columns list", res->name);

		size += strlen(res->name) + 1;
		sharedfields[i] = ShmemAlloc(size);
		memcpy(sharedfields[i], res->name, size);
	}

	OpenStreamBuffer(GlobalStreamBuffer);

	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		A_Const *c;
		Value *v;
		List *values = NIL;
		Size size = 0;
		StreamEvent ev = (StreamEvent) palloc(STREAMEVENTSIZE);
		ListCell *vlc;
		int offset = 0;

		for (i=0; i<numcols; i++)
		{
			char *sval;

			c = (A_Const *) list_nth(vals, i);
			v = &(c->val);

			if (IsA(v, Integer))
			{
				/* longs have a maximum of 20 digits */
				sval = palloc(20);
				sprintf(sval, "%ld", intVal(v));
			}
			else
			{
				sval = strVal(v);
			}

			values = lappend(values, sval);
			size += strlen(sval) + 1;
		}

		ev->raw = palloc(size);
		ev->len = size;
		ev->fields = sharedfields;
		ev->nfields = numcols;

		foreach (vlc, values)
		{
			char *value = (char *) lfirst(vlc);
			int len = strlen(value) + 1;

			memcpy(ev->raw + offset, value, len);
			offset += len;
		}

		AppendStreamEvent(ins->relation->relname, VALUES_ENCODING, GlobalStreamBuffer, ev);
		count++;
	}

	CloseStreamBuffer(GlobalStreamBuffer);

	return count;
}
