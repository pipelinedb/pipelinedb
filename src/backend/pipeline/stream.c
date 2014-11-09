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
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
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

typedef struct StreamTargetsEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	Bitmapset *targets;
} StreamTargetsEntry;

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
