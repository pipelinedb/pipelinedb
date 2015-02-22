/* Copyright (c) 2013-2015 PipelineDB */
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
#include "catalog/pipeline_stream_fn.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_target.h"
#include "pipeline/stream.h"
#include "storage/ipc.h"
#include "storage/spalloc.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/* Whether or not to block till the events are consumed by a cv*/
bool DebugSyncStreamInsert;

/*
 * InsertTargetIsStream
 *
 * Is the given INSERT statements target relation a stream?
 * We assume it is if the relation doesn't exist in the catalog as a normal relation.
 */
bool
InsertTargetIsStream(InsertStmt *ins)
{
	Oid reloid = RangeVarGetRelid(ins->relation, NoLock, true);

	if (reloid != InvalidOid)
		return false;

	return IsInputStream(ins->relation->relname);
}

/*
 * IsInputStream
 *
 * Returns true if at least one continuous query is reading from the given stream
 */
bool
IsInputStream(const char *stream)
{
	return GetStreamTargets(stream) != NULL;
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
	TupleBufferSlot* tbs = NULL;
	int numcols = list_length(ins->cols);
	int i;
	int count = 0;
	ParseState *ps = make_parsestate(NULL);
	List *colnames = NIL;
	TupleDesc desc = NULL;
	ExprContext *econtext = CreateStandaloneExprContext();
	Bitmapset *targets = GetStreamTargets(ins->relation->relname);

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

	desc = GetStreamTupleDesc(ins->relation->relname, colnames);

	/* append each VALUES tuple to the stream buffer */
	foreach (lc, sel->valuesLists)
	{
		List *raw = (List *) lfirst(lc);
		List *exprlist = transformExpressionList(ps, raw, EXPR_KIND_VALUES);
		List *exprstatelist;
		ListCell *l;
		Datum *values;
		bool *nulls;
		int col = 0;
		int attindex = 0;
		int evindex = 0;
		List *filteredvals = NIL;
		List *filteredcols = NIL;
		Tuple *tuple;

		assign_expr_collations(NULL, (Node *) exprlist);

		/*
		 * Extract all fields from the current tuple that are actually being
		 * read by something.
		 */
		while (attindex < desc->natts && evindex < list_length(exprlist))
		{
			Value *colname = (Value *) list_nth(colnames, evindex);
			Node *n = (Node *) list_nth(exprlist, evindex);

			if (strcmp(strVal(colname), NameStr(desc->attrs[attindex]->attname)) == 0)
			{
				/* coerce column to common supertype */
				Node *c = coerce_to_specific_type(NULL, n, desc->attrs[attindex]->atttypid, "VALUES");

				filteredvals = lappend(filteredvals, c);
				filteredcols = lappend(filteredcols, strVal(colname));
				attindex++;
			}
			evindex++;
		}

		/*
		 * If we haven't read any fields then we need to add the last one
		 * available because a COUNT(*) might need it (annoying). We arbitrarily
		 * use the first value in the INSERT tuple in this case.
		 */
		if (!filteredvals)
		{
			filteredvals = lappend(filteredvals, linitial(exprlist));
			filteredcols = lappend(filteredcols, strVal(linitial(colnames)));
		}

		exprstatelist = (List *) ExecInitExpr((Expr *) filteredvals, NULL);

		values = palloc0(list_length(exprstatelist) * sizeof(Datum));
		nulls = palloc0(list_length(exprstatelist) * sizeof(bool));

		/*
		 * Eval expressions to create a tuple of constants
		 */
		col = 0;
		foreach(l, exprstatelist)
		{
			ExprState  *estate = (ExprState *) lfirst(l);

			values[col] = ExecEvalExpr(estate, econtext, &nulls[col], NULL);
			col++;
		}

		/*
		 * Now write the tuple of constants to the TupleBuffer
		 */
		tuple = MakeTuple(heap_form_tuple(desc, values, nulls), desc);
		tbs = TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		Assert(tbs);
		count++;
	}

	FreeExprContext(econtext, false);

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (DebugSyncStreamInsert)
		TupleBufferWaitOnSlot(tbs, 5);

	return count;
}
