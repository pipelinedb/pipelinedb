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
	return GetTargetsFor(stream) != NULL;
}

static TupleDesc
spalloc_tupledesc(TupleDesc desc)
{
	Size size = TUPLEDESC_FIXED_SIZE + (desc->natts * sizeof(Form_pg_attribute)) + (desc->natts * ATTRIBUTE_FIXED_PART_SIZE);
	char *addr = spalloc(size);
	char *attr_addr = addr + TUPLEDESC_FIXED_SIZE + (desc->natts * sizeof(Form_pg_attribute));
	TupleDesc shared = (TupleDesc) addr;
	int i;

	memcpy(addr, desc, TUPLEDESC_FIXED_SIZE);

	shared->attrs = (Form_pg_attribute *) (addr + TUPLEDESC_FIXED_SIZE);

	for (i = 0; i < desc->natts; i++)
	{
		shared->attrs[i] = (Form_pg_attribute) attr_addr;
		memcpy(shared->attrs[i], desc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
		attr_addr += ATTRIBUTE_FIXED_PART_SIZE;
	}

	return shared;
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

	assign_expr_collations(NULL, (Node *) exprlists);

	/* append each VALUES tuple to the stream buffer */
	foreach (lc, exprlists)
	{
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
		 * so we only have to generate it once and put it in shared
		 * memory.
		 */
		if (desc == NULL)
		{
			TupleDesc src = ExecTypeFromExprList(exprlist);

			ExecTypeSetColNames(src, colnames);
			desc = spalloc_tupledesc(src);

			/*
			 * We use a negative counter and consider the TupleDesc unneeded by further
			 * events when it reaches 0. We do this because the TupleDesc is considered
			 * refcounted when tdrefcount >= 0, which causes the refcount to be incremented
			 * in other places, which we don't need and it complicates things for the purpose
			 * of stream inserts.
			 */
			desc->tdrefcount = -list_length(exprlists);
		}

		values = palloc0(desc->natts * sizeof(Datum));
		nulls = palloc0(desc->natts * sizeof(bool));

		col = 0;
		foreach(l, exprstatelist)
		{
			ExprState  *estate = (ExprState *) lfirst(l);

			values[col] = ExecEvalExpr(estate, econtext, &nulls[col], NULL);
			col++;
		}

		tbs = TupleBufferInsert(WorkerTupleBuffer, MakeTuple(heap_form_tuple(desc, values, nulls), desc), GetTargetsFor(ins->relation->relname));

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
