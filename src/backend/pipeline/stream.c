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
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_target.h"
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
	int numcols = list_length(ins->cols);
	int i;
	int count = 0;
	int remaining = 0;
	ParseState *ps = make_parsestate(NULL);
	List *colnames = NIL;
	TupleDesc desc = NULL;
	ExprContext *econtext = CreateStandaloneExprContext();
	TupleDesc byrefdesc = NULL;
	Bitmapset **coltypes = NULL;
	Oid *commontypes = NULL;
	List *exprlists = NULL;
	List **alltypes = NULL;

	/* build header of column names */
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

		colnames = lappend(colnames, makeString(res->name));
	}

	coltypes = (Bitmapset **) palloc0(numcols * sizeof(Bitmapset *));
	alltypes = (List **) palloc0(numcols * sizeof(List *));
	commontypes = palloc0(numcols * sizeof(Oid));

	/*
	 * For each column in each VALUE tuple, we need to figure
	 * out what their common supertype is and coerce them to it later
	 * if necessary. This allows us to perform the same coercion on
	 * each column when projecting.
	 */
	foreach (lc, sel->valuesLists)
	{
		List *vals = (List *) lfirst(lc);
		int col = 0;
		List *exprlist = transformExpressionList(ps, vals, EXPR_KIND_VALUES);
		ListCell *l;

		if (list_length(vals) != numcols)
			elog(ERROR, "VALUES tuples must have %d values", numcols);

		/*
		 * For each column in the tuple, add its type to a set of types
		 * seen for that column across all tuples.
		 */
		foreach(l, exprlist)
		{
			Expr *expr = (Expr *) lfirst(l);
			Bitmapset *types = coltypes[col];
			Oid etype = exprType((Node *) expr);

			if (!bms_is_member(etype, types))
			{
				bms_add_member(types, etype);
				alltypes[col] = lappend(alltypes[col], expr);
			}

			col++;
		}

		exprlists = lappend(exprlists, exprlist);
	}

	for (i=0; i<numcols; i++)
	{
		commontypes[i] = select_common_type(NULL, alltypes[i], "VALUES", NULL);
	}

	remaining = list_length(exprlists);

	/* append each VALUES tuple to the stream buffer */
	foreach (lc, exprlists)
	{
		StreamEvent ev = (StreamEvent) palloc0(sizeof(StreamEventData));
		List *exprlist = (List *) lfirst(lc);
		List *exprstatelist;
		ListCell *l;
		Datum *values;
		bool *nulls;
		int col = 0;

		/* coerce column to common supertype */
		foreach (l, exprlist)
		{
			Node *n = (Node *) lfirst(l);
			lfirst(l) = coerce_to_common_type(NULL, n, commontypes[col], "VALUES");
			col++;
		}

		exprstatelist = (List *) ExecInitExpr((Expr *) exprlist, NULL);

		/*
		 * Each tuple in an insert batch has the same descriptor,
		 * so we only have to generate it once. All the other tuples in
		 * this batch reference the same descriptor.
		 */
		if (desc == NULL)
		{
			desc = ExecTypeFromExprList(exprlist, colnames);
			ev->desc = desc;
			ev->desc->tdrefcount = 1;
			ev->flags |= TUPLEDESC_BYVAL;
		}
		else
		{
			/* descriptor will be passed byref because we've already stored it byval */
			ev->desc = byrefdesc;
		}

		remaining--;

		/*
		 * If this is the last event in this insert, indicate that the byref
		 * descriptor can be unpinned when this event is done being read.
		 */
		if (remaining == 0)
			ev->flags |= TUPLEDESC_UNPIN;

		values = palloc0(desc->natts * sizeof(Datum));
		nulls = palloc0(desc->natts * sizeof(bool));

		col = 0;
		foreach(l, exprstatelist)
		{
			ExprState  *estate = (ExprState *) lfirst(l);

			values[col] = ExecEvalExpr(estate, econtext, &nulls[col], NULL);
			col++;
		}

		ev->raw = heap_form_tuple(desc, values, nulls);
		ev->arrivaltime = GetCurrentTimestamp();

		sbs = AppendStreamEvent(ins->relation->relname, GlobalStreamBuffer, ev);

		/*
		 * Instead of copying the entire attribute array into each event, we just
		 * keep a pointer to the first array and keep it pinned until no events need
		 * to read it anymore.
		 */
		if (ev->flags & TUPLEDESC_BYVAL)
			byrefdesc = sbs->event->desc;

		Assert(sbs);
		count++;
	}

	/*
		Wait till the last event has been consumed by a CV before returning
		Used for testing, based on a config setting.
	*/
	if (DebugSyncStreamInsert)
		WaitForOverwrite(GlobalStreamBuffer, sbs, 5);

	return count;
}
