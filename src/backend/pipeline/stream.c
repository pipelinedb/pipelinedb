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

typedef struct StreamTagsEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	Bitmapset *tags;
} StreamTagsEntry;

static StreamTargets *targets = NULL;

/* Whether or not to block till the events are consumed by a cv*/
bool DebugSyncStreamInsert;

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
 * Builds a mapping from stream name to continuous views that need to read from the stream
 */
void
CreateStreamTargets(void)
{
	HASHCTL ctl;
	Relation rel;
	HeapScanDesc scandesc;
	HeapTuple tup;
	MemoryContext oldcontext;

	if (targets != NULL)
		hash_destroy(targets);

	MemSet(&ctl, 0, sizeof(ctl));

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(StreamTagsEntry);

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	targets = hash_create("StreamTargets", 32, &ctl, HASH_ELEM);

	rel = heap_open(PipelineQueriesRelationId, AccessShareLock);
	scandesc = heap_beginscan_catalog(rel, 0, NULL);

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
		Form_pipeline_queries catrow = (Form_pipeline_queries) GETSTRUCT(tup);

		if (catrow->state != PIPELINE_QUERY_STATE_ACTIVE)
			continue;

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
				entry = (StreamTagsEntry *) hash_search(targets, (void *) relname, HASH_ENTER, &found);

				if (!found)
					entry->tags = NULL;

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
	heap_close(rel, NoLock);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * CopyStreamTargets
 *
 * Copies the bitmap of the given stream's target CVs into the specified address.
 * This is used to load the bitmap into the stream buffer's shared memory.
 */
Bitmapset *
GetTargetsFor(const char *stream)
{
	bool found = false;
	StreamTagsEntry *entry;

	if (targets == NULL)
		CreateStreamTargets();

	entry = (StreamTagsEntry *) hash_search(targets, stream, HASH_FIND, &found);
	if (!found)
		return NULL;

	return entry->tags;
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
 */
int
InsertIntoStream(InsertStmt *ins)
{
	SelectStmt *sel = (SelectStmt *) ins->selectStmt;
	ListCell *lc;
	StreamBufferSlot* sbs = NULL;
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
	for (i = 0; i < numcols; i++)
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

	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		A_Const *c;
		Value *v;
		List *values = NIL;
		Size size = 0;
		StreamEvent ev = (StreamEvent) palloc(sizeof(StreamEventData));
		ListCell *vlc;
		int offset = 0;

		for (i = 0; i < numcols; i++)
		{
			Node *val = list_nth(vals, i);
			char *sval;

			if (IsA(val, TypeCast))
			{
				TypeCast *tc = (TypeCast *) val;
				val = tc->arg;
			}

			if (IsA(val, A_Const))
			{
				c = (A_Const *) val;
			}
			else
			{
				/*
				 * XXX(usmanm): For now we only allow inserting constant values into streams.
				 * This should be fixed eventually to support arbitrary expressions.
				 */
				elog(ERROR, "only literal values are allowed when inserting into streams");
			}

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
		ev->arrivaltime = GetCurrentTimestamp();

		foreach (vlc, values)
		{
			char *value = (char *) lfirst(vlc);
			int len = strlen(value) + 1;

			memcpy(ev->raw + offset, value, len);
			offset += len;
		}

		sbs = AppendStreamEvent(ins->relation->relname, VALUES_ENCODING, GlobalStreamBuffer, ev);
		Assert(sbs);
		count++;
	}

	/*
		#392 Wait till the last event has been consumed by a CV before returning
		Used for testing, based on a config setting.
	*/
	if (DebugSyncStreamInsert)
	{
		WaitForOverwrite(GlobalStreamBuffer, sbs, 5);
	}
	/* Print out the binary variable that tells you how many cvs are waiting on this stream */

	return count;
}
