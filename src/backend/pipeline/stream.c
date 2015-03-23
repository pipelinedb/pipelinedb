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
#include "utils/typcache.h"

/* Whether or not to block till the events are consumed by a cv*/
bool debug_sync_stream_insert;

static HTAB *prepared_stream_inserts = NULL;

/*
 * StorePreparedStreamInsert
 *
 * Create a PreparedStreamInsertStmt with the given column names
 */
PreparedStreamInsertStmt *
StorePreparedStreamInsert(const char *name, const char *stream, List *cols)
{
	PreparedStreamInsertStmt *result;
	bool found;

	if (prepared_stream_inserts == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(PreparedStreamInsertStmt);

		prepared_stream_inserts = hash_create("prepared_stream_inserts", 2, &ctl, HASH_ELEM);
	}

	result = (PreparedStreamInsertStmt *)
			hash_search(prepared_stream_inserts, (void *) name, HASH_ENTER, &found);

	result->inserts = NIL;
	result->stream = (char *) stream;
	result->cols = cols;
	result->desc = GetStreamTupleDesc(stream, cols);

	return result;
}

/*
 * AddPreparedStreamInsert
 *
 * Add a Datum tuple to the PreparedStreamInsertStmt
 */
void
AddPreparedStreamInsert(PreparedStreamInsertStmt *stmt, ParamListInfoData *params)
{
	stmt->inserts = lappend(stmt->inserts, params);
}

/*
 * FetchPreparedStreamInsert
 *
 * Retrieve the given PreparedStreamInsertStmt, or NULL of it doesn't exist
 */
PreparedStreamInsertStmt *
FetchPreparedStreamInsert(const char *name)
{
	if (prepared_stream_inserts == NULL)
		return NULL;

	return (PreparedStreamInsertStmt *)
			hash_search(prepared_stream_inserts, (void *) name, HASH_FIND, NULL);
}

/*
 * DropPreparedStreamInsert
 *
 * Remove a PreparedStreamInsertStmt
 */
void
DropPreparedStreamInsert(const char *name)
{
	if (prepared_stream_inserts)
		hash_search(prepared_stream_inserts, (void *) name, HASH_REMOVE, NULL);
}

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

	return IsStream(ins->relation->relname);
}

/*
 * InsertIntoStreamPrepared
 *
 * Send Datum-encoded events to the given stream
 */
int
InsertIntoStreamPrepared(PreparedStreamInsertStmt *pstmt)
{
	ListCell *lc;
	int count = 0;
	Bitmapset *targets = GetStreamReaders(pstmt->stream);
	TupleBufferSlot* tbs = NULL;
	TupleDesc desc = GetStreamTupleDesc(pstmt->stream, pstmt->cols);

	foreach(lc, pstmt->inserts)
	{
		int i;
		ParamListInfoData *params = (ParamListInfoData *) lfirst(lc);
		Datum *values = palloc0(params->numParams * sizeof(Datum));
		bool *nulls = palloc0(params->numParams * sizeof(bool));
		Tuple *tuple;

		for (i=0; i<params->numParams; i++)
		{
			TypeCacheEntry *type = lookup_type_cache(params->params[i].ptype, 0);
			/*
			 * The incoming param may have a different but cast-compatible type with
			 * the target, so we change the TupleDesc before it gets physically packed
			 * in with the event. Eventually it will be casted to the correct type.
			 */
			desc->attrs[i]->atttypid = params->params[i].ptype;
			desc->attrs[i]->attbyval = type->typbyval;
			desc->attrs[i]->attalign = type->typalign;
			desc->attrs[i]->attlen = type->typlen;

			values[i] = params->params[i].value;
			nulls[i] = params->params[i].isnull;
		}

		tuple = MakeTuple(heap_form_tuple(desc, values, nulls), desc);
		tbs = TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		count++;
	}

	if (debug_sync_stream_insert)
		TupleBufferWaitOnSlot(tbs, 5);

	pstmt->inserts = NIL;

	return count;
}

/*
 * InsertIntoStream
 *
 * Send INSERT-encoded events to the given stream
 */
int
InsertIntoStream(InsertStmt *ins, List *values)
{
	ListCell *lc;
	TupleBufferSlot* tbs = NULL;
	int numcols = list_length(ins->cols);
	int i;
	int count = 0;
	ParseState *ps = make_parsestate(NULL);
	List *colnames = NIL;
	TupleDesc desc = NULL;
	ExprContext *econtext = CreateStandaloneExprContext();
	Bitmapset *targets = GetStreamReaders(ins->relation->relname);

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
	foreach (lc, values)
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

			if (pg_strcasecmp(strVal(colname), NameStr(desc->attrs[attindex]->attname)) == 0)
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
	if (debug_sync_stream_insert)
		TupleBufferWaitOnSlot(tbs, 5);

	return count;
}

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
uint64
CopyIntoStream(const char *stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	Bitmapset *targets = GetStreamReaders(stream);
	TupleBufferSlot* tbs = NULL;
	uint64 count = 0;
	int i;

	for (i=0; i<ntuples; i++)
	{
		HeapTuple htup = tuples[i];
		Tuple *tuple;

		tuple = MakeTuple(htup, desc);
		tbs = TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		count++;
	}

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (debug_sync_stream_insert)
		TupleBufferWaitOnSlot(tbs, 5);

	return count;
}
