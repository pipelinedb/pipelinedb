/*-------------------------------------------------------------------------
 *
 * orderedsetaggs.c
 *		Ordered-set aggregate functions.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2013-2015, PipelineDB
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/orderedsetaggs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeContinuousUnique.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "pipeline/hll.h"
#include "pipeline/miscutils.h"
#include "pipeline/tdigest.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"


typedef struct CQHSPerQueryState
{
	/* slot for inserting elements into the HLL */
	TupleTableSlot *curslot;
	/* slot containing the tuple representing the direct args to this HS agg */
	TupleTableSlot *directslot;
	SortSupport sort;
} CQHSPerQueryState;


/*
 * Generic support for ordered-set aggregates
 *
 * The state for an ordered-set aggregate is divided into a per-group struct
 * (which is the internal-type transition state datum returned to nodeAgg.c)
 * and a per-query struct, which contains data and sub-objects that we can
 * create just once per query because they will not change across groups.
 * The per-query struct and subsidiary data live in the executor's per-query
 * memory context, and go away implicitly at ExecutorEnd().
 */

typedef struct OSAPerQueryState
{
	/* Aggref for this aggregate: */
	Aggref	   *aggref;
	/* Memory context containing this struct and other per-query data: */
	MemoryContext qcontext;

	/* These fields are used only when accumulating tuples: */

	/* Tuple descriptor for tuples inserted into sortstate: */
	TupleDesc	tupdesc;
	/* Tuple slot we can use for inserting/extracting tuples: */
	TupleTableSlot *tupslot;
	/* Per-sort-column sorting information */
	int			numSortCols;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *eqOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirsts;
	/* Equality operator call info, created only if needed: */
	FmgrInfo   *equalfns;

	/* These fields are used only when accumulating datums: */

	/* Info about datatype of datums being sorted: */
	Oid			sortColType;
	int16		typLen;
	bool		typByVal;
	char		typAlign;
	/* Info about sort ordering: */
	Oid			sortOperator;
	Oid			eqOperator;
	Oid			sortCollation;
	bool		sortNullsFirst;
	/* Equality operator call info, created only if needed: */
	FmgrInfo	equalfn;
} OSAPerQueryState;

typedef struct OSAPerGroupState
{
	/* Link to the per-query state for this aggregate: */
	OSAPerQueryState *qstate;
	/* Memory context containing per-group data: */
	MemoryContext gcontext;
	/* Sort object we're accumulating data in: */
	Tuplesortstate *sortstate;
	/* Number of normal rows inserted into sortstate: */
	int64		number_of_rows;
} OSAPerGroupState;

static void ordered_set_shutdown(Datum arg);
static int compare_slots(TupleTableSlot *s0, TupleTableSlot *s1, SortSupport sort);

/*
 * Set up working state for an ordered-set aggregate
 */
static OSAPerGroupState *
ordered_set_startup(FunctionCallInfo fcinfo, bool use_tuples)
{
	OSAPerGroupState *osastate;
	OSAPerQueryState *qstate;
	MemoryContext gcontext;
	MemoryContext oldcontext;

	/*
	 * Check we're called as aggregate (and not a window function), and get
	 * the Agg node's group-lifespan context (which might change from group to
	 * group, so we shouldn't cache it in the per-query state).
	 */
	if (AggCheckCallContext(fcinfo, &gcontext) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "ordered-set aggregate called in non-aggregate context");

	/*
	 * We keep a link to the per-query state in fn_extra; if it's not there,
	 * create it, and do the per-query setup we need.
	 */
	qstate = (OSAPerQueryState *) fcinfo->flinfo->fn_extra;
	if (qstate == NULL)
	{
		Aggref	   *aggref;
		MemoryContext qcontext;
		List	   *sortlist;
		int			numSortCols;

		/* Get the Aggref so we can examine aggregate's arguments */
		aggref = AggGetAggref(fcinfo);
		if (!aggref)
			elog(ERROR, "ordered-set aggregate called in non-aggregate context");
		if (!AGGKIND_IS_ORDERED_SET(aggref->aggkind))
			elog(ERROR, "ordered-set aggregate support function called for non-ordered-set aggregate");

		/*
		 * Prepare per-query structures in the fn_mcxt, which we assume is the
		 * executor's per-query context; in any case it's the right place to
		 * keep anything found via fn_extra.
		 */
		qcontext = fcinfo->flinfo->fn_mcxt;
		oldcontext = MemoryContextSwitchTo(qcontext);

		qstate = (OSAPerQueryState *) palloc0(sizeof(OSAPerQueryState));
		qstate->aggref = aggref;
		qstate->qcontext = qcontext;

		/* Extract the sort information */
		sortlist = aggref->aggorder;
		numSortCols = list_length(sortlist);

		if (use_tuples)
		{
			bool		ishypothetical = (aggref->aggkind == AGGKIND_HYPOTHETICAL);
			ListCell   *lc;
			int			i;

			if (ishypothetical)
				numSortCols++;	/* make space for flag column */
			qstate->numSortCols = numSortCols;
			qstate->sortColIdx = (AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
			qstate->sortOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->eqOperators = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->sortCollations = (Oid *) palloc(numSortCols * sizeof(Oid));
			qstate->sortNullsFirsts = (bool *) palloc(numSortCols * sizeof(bool));

			i = 0;
			foreach(lc, sortlist)
			{
				SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl,
														   aggref->args);

				/* the parser should have made sure of this */
				Assert(OidIsValid(sortcl->sortop));

				qstate->sortColIdx[i] = tle->resno;
				qstate->sortOperators[i] = sortcl->sortop;
				qstate->eqOperators[i] = sortcl->eqop;
				qstate->sortCollations[i] = exprCollation((Node *) tle->expr);
				qstate->sortNullsFirsts[i] = sortcl->nulls_first;
				i++;
			}

			if (ishypothetical)
			{
				/* Add an integer flag column as the last sort column */
				qstate->sortColIdx[i] = list_length(aggref->args) + 1;
				qstate->sortOperators[i] = Int4LessOperator;
				qstate->eqOperators[i] = Int4EqualOperator;
				qstate->sortCollations[i] = InvalidOid;
				qstate->sortNullsFirsts[i] = false;
				i++;
			}

			Assert(i == numSortCols);

			/*
			 * Get a tupledesc corresponding to the aggregated inputs
			 * (including sort expressions) of the agg.
			 */
			qstate->tupdesc = ExecTypeFromTL(aggref->args, false);

			/* If we need a flag column, hack the tupledesc to include that */
			if (ishypothetical)
			{
				TupleDesc	newdesc;
				int			natts = qstate->tupdesc->natts;

				newdesc = CreateTemplateTupleDesc(natts + 1, false);
				for (i = 1; i <= natts; i++)
					TupleDescCopyEntry(newdesc, i, qstate->tupdesc, i);

				TupleDescInitEntry(newdesc,
								   (AttrNumber) ++natts,
								   "flag",
								   INT4OID,
								   -1,
								   0);

				FreeTupleDesc(qstate->tupdesc);
				qstate->tupdesc = newdesc;
			}

			/* Create slot we'll use to store/retrieve rows */
			qstate->tupslot = MakeSingleTupleTableSlot(qstate->tupdesc);
		}
		else
		{
			/* Sort single datums */
			SortGroupClause *sortcl;
			TargetEntry *tle;

			if (numSortCols != 1 || aggref->aggkind == AGGKIND_HYPOTHETICAL)
				elog(ERROR, "ordered-set aggregate support function does not support multiple aggregated columns");

			sortcl = (SortGroupClause *) linitial(sortlist);
			tle = get_sortgroupclause_tle(sortcl, aggref->args);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			/* Save sort ordering info */
			qstate->sortColType = exprType((Node *) tle->expr);
			qstate->sortOperator = sortcl->sortop;
			qstate->eqOperator = sortcl->eqop;
			qstate->sortCollation = exprCollation((Node *) tle->expr);
			qstate->sortNullsFirst = sortcl->nulls_first;

			/* Save datatype info */
			get_typlenbyvalalign(qstate->sortColType,
								 &qstate->typLen,
								 &qstate->typByVal,
								 &qstate->typAlign);
		}

		fcinfo->flinfo->fn_extra = (void *) qstate;

		MemoryContextSwitchTo(oldcontext);
	}

	/* Now build the stuff we need in group-lifespan context */
	oldcontext = MemoryContextSwitchTo(gcontext);

	osastate = (OSAPerGroupState *) palloc(sizeof(OSAPerGroupState));
	osastate->qstate = qstate;
	osastate->gcontext = gcontext;

	/* Initialize tuplesort object */
	if (use_tuples)
		osastate->sortstate = tuplesort_begin_heap(qstate->tupdesc,
												   qstate->numSortCols,
												   qstate->sortColIdx,
												   qstate->sortOperators,
												   qstate->sortCollations,
												   qstate->sortNullsFirsts,
												   work_mem, false);
	else
		osastate->sortstate = tuplesort_begin_datum(qstate->sortColType,
													qstate->sortOperator,
													qstate->sortCollation,
													qstate->sortNullsFirst,
													work_mem, false);

	osastate->number_of_rows = 0;

	/* Now register a shutdown callback to clean things up at end of group */
	AggRegisterCallback(fcinfo,
						ordered_set_shutdown,
						PointerGetDatum(osastate));

	MemoryContextSwitchTo(oldcontext);

	return osastate;
}

/*
 * Clean up when evaluation of an ordered-set aggregate is complete.
 *
 * We don't need to bother freeing objects in the per-group memory context,
 * since that will get reset anyway by nodeAgg.c; nor should we free anything
 * in the per-query context, which will get cleared (if this was the last
 * group) by ExecutorEnd.  But we must take care to release any potential
 * non-memory resources.
 *
 * This callback is arguably unnecessary, since we don't support use of
 * ordered-set aggs in AGG_HASHED mode and there is currently no non-error
 * code path in non-hashed modes wherein nodeAgg.c won't call the finalfn
 * after calling the transfn one or more times.  So in principle we could rely
 * on the finalfn to delete the tuplestore etc.  However, it's possible that
 * such a code path might exist in future, and in any case it'd be
 * notationally tedious and sometimes require extra data copying to ensure
 * we always delete the tuplestore in the finalfn.
 */
static void
ordered_set_shutdown(Datum arg)
{
	OSAPerGroupState *osastate = (OSAPerGroupState *) DatumGetPointer(arg);

	/* Tuplesort object might have temp files. */
	if (osastate->sortstate)
		tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;
	/* The tupleslot probably can't be holding a pin, but let's be safe. */
	if (osastate->qstate->tupslot)
		ExecClearTuple(osastate->qstate->tupslot);
}


/*
 * Generic transition function for ordered-set aggregates
 * with a single input column in which we want to suppress nulls
 */
Datum
ordered_set_transition(PG_FUNCTION_ARGS)
{
	OSAPerGroupState *osastate;

	/* If first call, create the transition state workspace */
	if (PG_ARGISNULL(0))
		osastate = ordered_set_startup(fcinfo, false);
	else
		osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* Load the datum into the tuplesort object, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		tuplesort_putdatum(osastate->sortstate, PG_GETARG_DATUM(1), false);
		osastate->number_of_rows++;
	}

	PG_RETURN_POINTER(osastate);
}

/*
 * Generic transition function for ordered-set aggregates
 * with (potentially) multiple aggregated input columns
 */
Datum
ordered_set_transition_multi(PG_FUNCTION_ARGS)
{
	OSAPerGroupState *osastate;
	TupleTableSlot *slot;
	int			nargs;
	int			i;

	/* If first call, create the transition state workspace */
	if (PG_ARGISNULL(0))
		osastate = ordered_set_startup(fcinfo, true);
	else
		osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* Form a tuple from all the other inputs besides the transition value */
	slot = osastate->qstate->tupslot;
	ExecClearTuple(slot);
	nargs = PG_NARGS() - 1;
	for (i = 0; i < nargs; i++)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	if (osastate->qstate->aggref->aggkind == AGGKIND_HYPOTHETICAL)
	{
		/* Add a zero flag value to mark this row as a normal input row */
		slot->tts_values[i] = Int32GetDatum(0);
		slot->tts_isnull[i] = false;
		i++;
	}
	Assert(i == slot->tts_tupleDescriptor->natts);
	ExecStoreVirtualTuple(slot);

	/* Load the row into the tuplesort object */
	tuplesort_puttupleslot(osastate->sortstate, slot);
	osastate->number_of_rows++;

	PG_RETURN_POINTER(osastate);
}

/*
 * percentile_disc(float8) within group(anyelement) - discrete percentile
 */
Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
	OSAPerGroupState *osastate;
	double		percentile;
	Datum		val;
	bool		isnull;
	int64		rownum;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* Get and check the percentile argument */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(1);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1",
						percentile)));

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/*----------
	 * We need the smallest K such that (K/N) >= percentile.
	 * N>0, therefore K >= N*percentile, therefore K = ceil(N*percentile).
	 * So we skip K-1 rows (if K>0) and return the next row fetched.
	 *----------
	 */
	rownum = (int64) ceil(percentile * osastate->number_of_rows);
	Assert(rownum <= osastate->number_of_rows);

	if (rownum > 1)
	{
		if (!tuplesort_skiptuples(osastate->sortstate, rownum - 1, true))
			elog(ERROR, "missing row in percentile_disc");
	}

	if (!tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
		elog(ERROR, "missing row in percentile_disc");

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
	 * to be returned is allocated inside its sortcontext.  We could use
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

	/* We shouldn't have stored any nulls, but do the right thing anyway */
	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}


/*
 * For percentile_cont, we need a way to interpolate between consecutive
 * values. Use a helper function for that, so that we can share the rest
 * of the code between types.
 */
typedef Datum (*LerpFunc) (Datum lo, Datum hi, double pct);

static Datum
float8_lerp(Datum lo, Datum hi, double pct)
{
	double		loval = DatumGetFloat8(lo);
	double		hival = DatumGetFloat8(hi);

	return Float8GetDatum(loval + (pct * (hival - loval)));
}

static Datum
interval_lerp(Datum lo, Datum hi, double pct)
{
	Datum		diff_result = DirectFunctionCall2(interval_mi, hi, lo);
	Datum		mul_result = DirectFunctionCall2(interval_mul,
												 diff_result,
												 Float8GetDatumFast(pct));

	return DirectFunctionCall2(interval_pl, mul_result, lo);
}

/*
 * Continuous percentile
 */
static Datum
percentile_cont_final_common(FunctionCallInfo fcinfo,
							 Oid expect_type,
							 LerpFunc lerpfunc)
{
	OSAPerGroupState *osastate;
	double		percentile;
	int64		first_row = 0;
	int64		second_row = 0;
	Datum		val;
	Datum		first_val;
	Datum		second_val;
	double		proportion;
	bool		isnull;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* Get and check the percentile argument */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(1);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1",
						percentile)));

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	Assert(expect_type == osastate->qstate->sortColType);

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	first_row = floor(percentile * (osastate->number_of_rows - 1));
	second_row = ceil(percentile * (osastate->number_of_rows - 1));

	Assert(first_row < osastate->number_of_rows);

	if (!tuplesort_skiptuples(osastate->sortstate, first_row, true))
		elog(ERROR, "missing row in percentile_cont");

	if (!tuplesort_getdatum(osastate->sortstate, true, &first_val, &isnull))
		elog(ERROR, "missing row in percentile_cont");
	if (isnull)
		PG_RETURN_NULL();

	if (first_row == second_row)
	{
		val = first_val;
	}
	else
	{
		if (!tuplesort_getdatum(osastate->sortstate, true, &second_val, &isnull))
			elog(ERROR, "missing row in percentile_cont");

		if (isnull)
			PG_RETURN_NULL();

		proportion = (percentile * (osastate->number_of_rows - 1)) - first_row;
		val = lerpfunc(first_val, second_val, proportion);
	}

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
	 * to be returned may be allocated inside its sortcontext.  We could use
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

	PG_RETURN_DATUM(val);
}

/*
 * percentile_cont(float8) within group (float8)	- continuous percentile
 */
Datum
percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, FLOAT8OID, float8_lerp);
}

/*
 * percentile_cont(float8) within group (interval)	- continuous percentile
 */
Datum
percentile_cont_interval_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, INTERVALOID, interval_lerp);
}


/*
 * Support code for handling arrays of percentiles
 *
 * Note: in each pct_info entry, second_row should be equal to or
 * exactly one more than first_row.
 */
struct pct_info
{
	int64		first_row;		/* first row to sample */
	int64		second_row;		/* possible second row to sample */
	double		proportion;		/* interpolation fraction */
	int			idx;			/* index of this item in original array */
};

/*
 * Sort comparator to sort pct_infos by first_row then second_row
 */
static int
pct_info_cmp(const void *pa, const void *pb)
{
	const struct pct_info *a = (const struct pct_info *) pa;
	const struct pct_info *b = (const struct pct_info *) pb;

	if (a->first_row != b->first_row)
		return (a->first_row < b->first_row) ? -1 : 1;
	if (a->second_row != b->second_row)
		return (a->second_row < b->second_row) ? -1 : 1;
	return 0;
}

/*
 * Construct array showing which rows to sample for percentiles.
 */
static struct pct_info *
setup_pct_info(int num_percentiles,
			   Datum *percentiles_datum,
			   bool *percentiles_null,
			   int64 rowcount,
			   bool continuous)
{
	struct pct_info *pct_info;
	int			i;

	pct_info = (struct pct_info *) palloc(num_percentiles * sizeof(struct pct_info));

	for (i = 0; i < num_percentiles; i++)
	{
		pct_info[i].idx = i;

		if (percentiles_null[i])
		{
			/* dummy entry for any NULL in array */
			pct_info[i].first_row = 0;
			pct_info[i].second_row = 0;
			pct_info[i].proportion = 0;
		}
		else
		{
			double		p = DatumGetFloat8(percentiles_datum[i]);

			if (p < 0 || p > 1 || isnan(p))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("percentile value %g is not between 0 and 1",
								p)));

			if (continuous)
			{
				pct_info[i].first_row = 1 + floor(p * (rowcount - 1));
				pct_info[i].second_row = 1 + ceil(p * (rowcount - 1));
				pct_info[i].proportion = (p * (rowcount - 1)) - floor(p * (rowcount - 1));
			}
			else
			{
				/*----------
				 * We need the smallest K such that (K/N) >= percentile.
				 * N>0, therefore K >= N*percentile, therefore
				 * K = ceil(N*percentile); but not less than 1.
				 *----------
				 */
				int64		row = (int64) ceil(p * rowcount);

				row = Max(1, row);
				pct_info[i].first_row = row;
				pct_info[i].second_row = row;
				pct_info[i].proportion = 0;
			}
		}
	}

	/*
	 * The parameter array wasn't necessarily in sorted order, but we need to
	 * visit the rows in order, so sort by first_row/second_row.
	 */
	qsort(pct_info, num_percentiles, sizeof(struct pct_info), pct_info_cmp);

	return pct_info;
}

/*
 * percentile_disc(float8[]) within group (anyelement)	- discrete percentiles
 */
Datum
percentile_disc_multi_final(PG_FUNCTION_ARGS)
{
	OSAPerGroupState *osastate;
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	struct pct_info *pct_info;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int64		rownum = 0;
	Datum		val = (Datum) 0;
	bool		isnull = true;
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Deconstruct the percentile-array input */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	param = PG_GETARG_ARRAYTYPE_P(1);

	deconstruct_array(param, FLOAT8OID,
	/* hard-wired info on type float8 */
					  8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum,
					  &percentiles_null,
					  &num_percentiles);

	if (num_percentiles == 0)
		PG_RETURN_POINTER(construct_empty_array(osastate->qstate->sortColType));

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  osastate->number_of_rows,
							  false);

	result_datum = (Datum *) palloc(num_percentiles * sizeof(Datum));
	result_isnull = (bool *) palloc(num_percentiles * sizeof(bool));

	/*
	 * Start by dealing with any nulls in the param array - those are sorted
	 * to the front on row=0, so set the corresponding result indexes to null
	 */
	for (i = 0; i < num_percentiles; i++)
	{
		int			idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the input
	 * and extract the needed values
	 */
	if (i < num_percentiles)
	{
		/* Finish the sort */
		tuplesort_performsort(osastate->sortstate);

		for (; i < num_percentiles; i++)
		{
			int64		target_row = pct_info[i].first_row;
			int			idx = pct_info[i].idx;

			/* Advance to target row, if not already there */
			if (target_row > rownum)
			{
				if (!tuplesort_skiptuples(osastate->sortstate, target_row - rownum - 1, true))
					elog(ERROR, "missing row in percentile_disc");

				if (!tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
					elog(ERROR, "missing row in percentile_disc");

				rownum = target_row;
			}

			result_datum[idx] = val;
			result_isnull[idx] = isnull;
		}
	}

	/*
	 * We could clean up the tuplesort object after forming the array, but
	 * probably not worth the trouble.
	 */

	/* We make the output array the same shape as the input */
	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param),
										 ARR_LBOUND(param),
										 osastate->qstate->sortColType,
										 osastate->qstate->typLen,
										 osastate->qstate->typByVal,
										 osastate->qstate->typAlign));
}

/*
 * percentile_cont(float8[]) within group ()	- continuous percentiles
 */
static Datum
percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
								   Oid expect_type,
								   int16 typLen, bool typByVal, char typAlign,
								   LerpFunc lerpfunc)
{
	OSAPerGroupState *osastate;
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	struct pct_info *pct_info;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int64		rownum = 0;
	Datum		first_val = (Datum) 0;
	Datum		second_val = (Datum) 0;
	bool		isnull;
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	Assert(expect_type == osastate->qstate->sortColType);

	/* Deconstruct the percentile-array input */
	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	param = PG_GETARG_ARRAYTYPE_P(1);

	deconstruct_array(param, FLOAT8OID,
	/* hard-wired info on type float8 */
					  8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum,
					  &percentiles_null,
					  &num_percentiles);

	if (num_percentiles == 0)
		PG_RETURN_POINTER(construct_empty_array(osastate->qstate->sortColType));

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  osastate->number_of_rows,
							  true);

	result_datum = (Datum *) palloc(num_percentiles * sizeof(Datum));
	result_isnull = (bool *) palloc(num_percentiles * sizeof(bool));

	/*
	 * Start by dealing with any nulls in the param array - those are sorted
	 * to the front on row=0, so set the corresponding result indexes to null
	 */
	for (i = 0; i < num_percentiles; i++)
	{
		int			idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the input
	 * and extract the needed values
	 */
	if (i < num_percentiles)
	{
		/* Finish the sort */
		tuplesort_performsort(osastate->sortstate);

		for (; i < num_percentiles; i++)
		{
			int64		first_row = pct_info[i].first_row;
			int64		second_row = pct_info[i].second_row;
			int			idx = pct_info[i].idx;

			/*
			 * Advance to first_row, if not already there.  Note that we might
			 * already have rownum beyond first_row, in which case first_val
			 * is already correct.  (This occurs when interpolating between
			 * the same two input rows as for the previous percentile.)
			 */
			if (first_row > rownum)
			{
				if (!tuplesort_skiptuples(osastate->sortstate, first_row - rownum - 1, true))
					elog(ERROR, "missing row in percentile_cont");

				if (!tuplesort_getdatum(osastate->sortstate, true, &first_val, &isnull) || isnull)
					elog(ERROR, "missing row in percentile_cont");

				rownum = first_row;
				/* Always advance second_val to be latest input value */
				second_val = first_val;
			}
			else if (first_row == rownum)
			{
				/*
				 * We are already at the desired row, so we must previously
				 * have read its value into second_val (and perhaps first_val
				 * as well, but this assignment is harmless in that case).
				 */
				first_val = second_val;
			}

			/* Fetch second_row if needed */
			if (second_row > rownum)
			{
				if (!tuplesort_getdatum(osastate->sortstate, true, &second_val, &isnull) || isnull)
					elog(ERROR, "missing row in percentile_cont");
				rownum++;
			}
			/* We should now certainly be on second_row exactly */
			Assert(second_row == rownum);

			/* Compute appropriate result */
			if (second_row > first_row)
				result_datum[idx] = lerpfunc(first_val, second_val,
											 pct_info[i].proportion);
			else
				result_datum[idx] = first_val;

			result_isnull[idx] = false;
		}
	}

	/*
	 * We could clean up the tuplesort object after forming the array, but
	 * probably not worth the trouble.
	 */

	/* We make the output array the same shape as the input */
	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param), ARR_LBOUND(param),
										 expect_type,
										 typLen,
										 typByVal,
										 typAlign));
}

/*
 * percentile_cont(float8[]) within group (float8)	- continuous percentiles
 */
Datum
percentile_cont_float8_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  FLOAT8OID,
	/* hard-wired info on type float8 */
											  8, FLOAT8PASSBYVAL, 'd',
											  float8_lerp);
}

/*
 * percentile_cont(float8[]) within group (interval)  - continuous percentiles
 */
Datum
percentile_cont_interval_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  INTERVALOID,
	/* hard-wired info on type interval */
											  16, false, 'd',
											  interval_lerp);
}


/*
 * mode() within group (anyelement) - most common value
 */
Datum
mode_final(PG_FUNCTION_ARGS)
{
	OSAPerGroupState *osastate;
	Datum		val;
	bool		isnull;
	Datum		mode_val = (Datum) 0;
	int64		mode_freq = 0;
	Datum		last_val = (Datum) 0;
	int64		last_val_freq = 0;
	bool		last_val_is_mode = false;
	FmgrInfo   *equalfn;
	bool		shouldfree;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (osastate->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Look up the equality function for the datatype, if we didn't already */
	equalfn = &(osastate->qstate->equalfn);
	if (!OidIsValid(equalfn->fn_oid))
		fmgr_info_cxt(get_opcode(osastate->qstate->eqOperator), equalfn,
					  osastate->qstate->qcontext);

	shouldfree = !(osastate->qstate->typByVal);

	/* Finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/* Scan tuples and count frequencies */
	while (tuplesort_getdatum(osastate->sortstate, true, &val, &isnull))
	{
		/* we don't expect any nulls, but ignore them if found */
		if (isnull)
			continue;

		if (last_val_freq == 0)
		{
			/* first nonnull value - it's the mode for now */
			mode_val = last_val = val;
			mode_freq = last_val_freq = 1;
			last_val_is_mode = true;
		}
		else if (DatumGetBool(FunctionCall2(equalfn, val, last_val)))
		{
			/* value equal to previous value, count it */
			if (last_val_is_mode)
				mode_freq++;	/* needn't maintain last_val_freq */
			else if (++last_val_freq > mode_freq)
			{
				/* last_val becomes new mode */
				if (shouldfree)
					pfree(DatumGetPointer(mode_val));
				mode_val = last_val;
				mode_freq = last_val_freq;
				last_val_is_mode = true;
			}
			if (shouldfree)
				pfree(DatumGetPointer(val));
		}
		else
		{
			/* val should replace last_val */
			if (shouldfree && !last_val_is_mode)
				pfree(DatumGetPointer(last_val));
			last_val = val;
			last_val_freq = 1;
			last_val_is_mode = false;
		}

		CHECK_FOR_INTERRUPTS();
	}

	if (shouldfree && !last_val_is_mode)
		pfree(DatumGetPointer(last_val));

	/*
	 * Note: we *cannot* clean up the tuplesort object here, because the value
	 * to be returned is allocated inside its sortcontext.  We could use
	 * datumCopy to copy it out of there, but it doesn't seem worth the
	 * trouble, since the cleanup callback will clear the tuplesort later.
	 */

	if (mode_freq)
		PG_RETURN_DATUM(mode_val);
	else
		PG_RETURN_NULL();
}


/*
 * Common code to sanity-check args for hypothetical-set functions. No need
 * for friendly errors, these can only happen if someone's messing up the
 * aggregate definitions. The checks are needed for security, however.
 */
static void
hypothetical_check_argtypes(FunctionCallInfo fcinfo, int nargs,
							TupleDesc tupdesc)
{
	int			i;

	/* check that we have an int4 flag column */
	if (!tupdesc ||
		(nargs + 1) != tupdesc->natts ||
		tupdesc->attrs[nargs]->atttypid != INT4OID)
		elog(ERROR, "type mismatch in hypothetical-set function");

	/* check that direct args match in type with aggregated args */
	for (i = 0; i < nargs; i++)
	{
		if (get_fn_expr_argtype(fcinfo->flinfo, i + 1) != tupdesc->attrs[i]->atttypid)
			elog(ERROR, "type mismatch in hypothetical-set function");
	}
}

/*
 * compute rank of hypothetical row
 *
 * flag should be -1 to sort hypothetical row ahead of its peers, or +1
 * to sort behind.
 * total number of regular rows is returned into *number_of_rows.
 */
static int64
hypothetical_rank_common(FunctionCallInfo fcinfo, int flag,
						 int64 *number_of_rows)
{
	int			nargs = PG_NARGS() - 1;
	int64		rank = 1;
	OSAPerGroupState *osastate;
	TupleTableSlot *slot;
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the rank is always 1 */
	if (PG_ARGISNULL(0))
	{
		*number_of_rows = 0;
		return 1;
	}

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);
	*number_of_rows = osastate->number_of_rows;

	/* Adjust nargs to be the number of direct (or aggregated) args */
	if (nargs % 2 != 0)
		elog(ERROR, "wrong number of arguments in hypothetical-set function");
	nargs /= 2;

	hypothetical_check_argtypes(fcinfo, nargs, osastate->qstate->tupdesc);

	/* insert the hypothetical row into the sort */
	slot = osastate->qstate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->tts_values[i] = Int32GetDatum(flag);
	slot->tts_isnull[i] = false;
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(osastate->sortstate, slot);

	/* finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/* iterate till we find the hypothetical row */
	while (tuplesort_gettupleslot(osastate->sortstate, true, slot))
	{
		bool		isnull;
		Datum		d = slot_getattr(slot, nargs + 1, &isnull);

		if (!isnull && DatumGetInt32(d) != 0)
			break;

		rank++;

		CHECK_FOR_INTERRUPTS();
	}

	ExecClearTuple(slot);

	/* Might as well clean up the tuplesort object immediately */
	tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;

	return rank;
}

/*
 * rank()  - rank of hypothetical row
 */
Datum
hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;

	rank = hypothetical_rank_common(fcinfo, -1, &rowcount);

	PG_RETURN_INT64(rank);
}

/*
 * percent_rank()	- percentile rank of hypothetical row
 */
Datum
hypothetical_percent_rank_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;
	double		result_val;

	rank = hypothetical_rank_common(fcinfo, -1, &rowcount);

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	result_val = (double) (rank - 1) / (double) (rowcount);

	PG_RETURN_FLOAT8(result_val);
}

/*
 * cume_dist()	- cumulative distribution of hypothetical row
 */
Datum
hypothetical_cume_dist_final(PG_FUNCTION_ARGS)
{
	int64		rank;
	int64		rowcount;
	double		result_val;

	rank = hypothetical_rank_common(fcinfo, 1, &rowcount);

	result_val = (double) (rank) / (double) (rowcount + 1);

	PG_RETURN_FLOAT8(result_val);
}

/*
 * dense_rank() - rank of hypothetical row without gaps in ranking
 */
Datum
hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
{
	int			nargs = PG_NARGS() - 1;
	int64		rank = 1;
	int64		duplicate_count = 0;
	OSAPerGroupState *osastate;
	int			numDistinctCols;
	AttrNumber *sortColIdx;
	FmgrInfo   *equalfns;
	TupleTableSlot *slot;
	TupleTableSlot *extraslot;
	TupleTableSlot *slot2;
	MemoryContext tmpcontext;
	int			i;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the rank is always 1 */
	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(rank);

	osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

	/* Adjust nargs to be the number of direct (or aggregated) args */
	if (nargs % 2 != 0)
		elog(ERROR, "wrong number of arguments in hypothetical-set function");
	nargs /= 2;

	hypothetical_check_argtypes(fcinfo, nargs, osastate->qstate->tupdesc);

	/*
	 * When comparing tuples, we can omit the flag column since we will only
	 * compare rows with flag == 0.
	 */
	numDistinctCols = osastate->qstate->numSortCols - 1;

	/* Look up the equality function(s), if we didn't already */
	equalfns = osastate->qstate->equalfns;
	if (equalfns == NULL)
	{
		MemoryContext qcontext = osastate->qstate->qcontext;

		equalfns = (FmgrInfo *)
			MemoryContextAlloc(qcontext, numDistinctCols * sizeof(FmgrInfo));
		for (i = 0; i < numDistinctCols; i++)
		{
			fmgr_info_cxt(get_opcode(osastate->qstate->eqOperators[i]),
						  &equalfns[i],
						  qcontext);
		}
		osastate->qstate->equalfns = equalfns;
	}
	sortColIdx = osastate->qstate->sortColIdx;

	/* Get short-term context we can use for execTuplesMatch */
	tmpcontext = AggGetTempMemoryContext(fcinfo);

	/* insert the hypothetical row into the sort */
	slot = osastate->qstate->tupslot;
	ExecClearTuple(slot);
	for (i = 0; i < nargs; i++)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	slot->tts_values[i] = Int32GetDatum(-1);
	slot->tts_isnull[i] = false;
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(osastate->sortstate, slot);

	/* finish the sort */
	tuplesort_performsort(osastate->sortstate);

	/*
	 * We alternate fetching into tupslot and extraslot so that we have the
	 * previous row available for comparisons.  This is accomplished by
	 * swapping the slot pointer variables after each row.
	 */
	extraslot = MakeSingleTupleTableSlot(osastate->qstate->tupdesc);
	slot2 = extraslot;

	/* iterate till we find the hypothetical row */
	while (tuplesort_gettupleslot(osastate->sortstate, true, slot))
	{
		bool		isnull;
		Datum		d = slot_getattr(slot, nargs + 1, &isnull);
		TupleTableSlot *tmpslot;

		if (!isnull && DatumGetInt32(d) != 0)
			break;

		/* count non-distinct tuples */
		if (!TupIsNull(slot2) &&
			execTuplesMatch(slot, slot2,
							numDistinctCols,
							sortColIdx,
							equalfns,
							tmpcontext))
			duplicate_count++;

		tmpslot = slot2;
		slot2 = slot;
		slot = tmpslot;

		rank++;

		CHECK_FOR_INTERRUPTS();
	}

	ExecClearTuple(slot);
	ExecClearTuple(slot2);

	ExecDropSingleTupleTableSlot(extraslot);

	/* Might as well clean up the tuplesort object immediately */
	tuplesort_end(osastate->sortstate);
	osastate->sortstate = NULL;

	rank = rank - duplicate_count;

	PG_RETURN_INT64(rank);
}

/*
 * Set up query-level working state for continuous hypothetical-set aggregates
 */
static CQHSPerQueryState *
cq_hypothetical_set_per_query_startup(FunctionCallInfo fcinfo)
{
	CQHSPerQueryState *qstate;
	MemoryContext old;
	Aggref *aggref = AggGetAggref(fcinfo);
	ExprContext *econtext = CreateStandaloneExprContext();
	TupleDesc desc;
	ListCell *lc;
	List *sortlist = aggref->aggorder;
	List *directargs;
	Datum *values;
	bool *nulls;
	int ndirectargs = list_length(aggref->aggdirectargs);
	int i;

	/* our state needs to live for the duration of the query */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	values = palloc0(ndirectargs * sizeof(Datum));
	nulls = palloc0(ndirectargs * sizeof(bool));

	desc = ExecTypeFromTL(aggref->args, false);
	qstate = palloc0(sizeof(CQHSPerQueryState));

	/*
	 * Evaluate our direct arguments and build a tuple out
	 * of the resulting values
	 */
	directargs = (List *) ExecInitExpr((Expr *) aggref->aggdirectargs, NULL);
	i = 0;
	foreach(lc, directargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		values[i] = ExecEvalExpr(expr, econtext, &nulls[i], NULL);
		i++;
	}
	qstate->curslot = MakeSingleTupleTableSlot(desc);
	qstate->directslot = MakeSingleTupleTableSlot(desc);
	qstate->directslot->tts_values = values;
	qstate->directslot->tts_isnull = nulls;

	ExecStoreVirtualTuple(qstate->directslot);

	/*
	 * Build our comparator for determining how an input tuple compares to our
	 * direct argument tuple
	 */
	qstate->sort = (SortSupport) palloc0(desc->natts * sizeof(SortSupportData));
	i = 0;
	foreach(lc, sortlist)
	{
		SortSupport sortkey = qstate->sort + i;
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl,
												   aggref->args);

		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = exprCollation((Node *) tle->expr);
		sortkey->ssup_nulls_first = sortcl->nulls_first;
		sortkey->ssup_attno = tle->resno;

		PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);
		i++;
	}

	fcinfo->flinfo->fn_extra = (void *) qstate;
	MemoryContextSwitchTo(old);

	FreeExprContext(econtext, false);

	return qstate;
}

static ArrayType *
cq_hypothetical_set_startup(FunctionCallInfo fcinfo)
{
	ArrayType *state;
	CQHSPerQueryState *qstate;
	Datum *values = palloc0(3 * sizeof(Datum));

	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	if (qstate == NULL)
		qstate = cq_hypothetical_set_per_query_startup(fcinfo);

	state = construct_array(values, 3, 20, 8, true, 'i');

	return state;
}

static HyperLogLog *
hll_hypothetical_set_startup(FunctionCallInfo fcinfo)
{
	CQHSPerQueryState *qstate;

	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	if (qstate == NULL)
		qstate = cq_hypothetical_set_per_query_startup(fcinfo);

	return HLLCreate();
}

/*
 * Transition function for streaming ordered-set aggregates
 * with (potentially) multiple input columns. It uses HyperLogLog
 * instead of actually sorting the input to determine input value uniqueness.
 */
Datum
hll_hypothetical_set_transition_multi(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *hll;
	CQHSPerQueryState *qstate;
	int nargs;
	int i;
	int result;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		hll = hll_hypothetical_set_startup(fcinfo);
	else
		hll = (HyperLogLog *)  PG_GETARG_POINTER(0);

	/* this is created once by the first call to cq_hypothetical_set_startup */
	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	/* load the input values into our slot and add it to our HLL */
	ExecClearTuple(qstate->curslot);
	nargs = PG_NARGS() - 1;
	for (i = 0; i < nargs; i++)
	{
		qstate->curslot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		qstate->curslot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	ExecStoreVirtualTuple(qstate->curslot);

	result = compare_slots(qstate->curslot, qstate->directslot, qstate->sort);

	if (result < 0)
	{
		int unique;
		StringInfo buf = makeStringInfo();

		SlotAttrsToBytes(qstate->curslot, -1, NULL, buf);

		/*
		 * dense rank is only increased once for each lower-ranking tuple we see,
		 * so that the step size to the next highest-ranking tuple is always 1.
		 */
		hll = HLLAdd(hll, buf->data, buf->len, &unique);
	}

	SET_VARSIZE(hll, sizeof(HyperLogLog) + hll->mlen);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(hll);
}

static int
compare_slots(TupleTableSlot *s0, TupleTableSlot *s1, SortSupport sort)
{
	int i;
	for (i=0; i<s0->tts_tupleDescriptor->natts; i++)
	{
		bool n0;
		bool n1;
		Datum d0 = slot_getattr(s0, i + 1, &n0);
		Datum d1 = slot_getattr(s1, i + 1, &n1);
		SortSupport sortkey = sort + i;

		int result = ApplySortComparator(d0, n0, d1, n1, sortkey);

		if (result != 0)
			return result;
	}

	/* they must be equal */
	return 0;
}

/*
 * Generic transition function for streaming ordered-set aggregates
 * with (potentially) multiple input columns
 */
Datum
cq_hypothetical_set_transition_multi(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	ArrayType *state;
	CQHSPerQueryState *qstate;
	uint64 *transvalues;
	int nargs;
	int i;
	int result;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cq_hypothetical_set_startup(fcinfo);
	else
		state = (ArrayType *)  PG_GETARG_POINTER(0);

	/* this is created once by the first call to cq_hypothetical_set_startup */
	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	/* load the input values into our slot and add it to our HLL */
	ExecClearTuple(qstate->curslot);
	nargs = PG_NARGS() - 1;
	for (i = 0; i < nargs; i++)
	{
		qstate->curslot->tts_values[i] = PG_GETARG_DATUM(i + 1);
		qstate->curslot->tts_isnull[i] = PG_ARGISNULL(i + 1);
	}
	ExecStoreVirtualTuple(qstate->curslot);

	MemoryContextSwitchTo(old);

	result = compare_slots(qstate->curslot, qstate->directslot, qstate->sort);
	transvalues = (uint64 *) ARR_DATA_PTR(state);

	/* row count */
	transvalues[0]++;

	/* rank */
	if (result < 0)
		transvalues[1]++;
	else if (result == 0)
		transvalues[2]++;

	PG_RETURN_ARRAYTYPE_P(state);
}

/*
 * combines multiple continuous hypothetical-set transition states into one
 */
Datum
cq_hypothetical_set_combine_multi(PG_FUNCTION_ARGS)
{
	ArrayType *statearr;
	uint64 *state;
	ArrayType *incomingarr = (ArrayType *) PG_GETARG_ARRAYTYPE_P(1);
	uint64 *incoming = (uint64 *) ARR_DATA_PTR(incomingarr);

	if (!AggCheckCallContext(fcinfo, NULL))
			elog(ERROR, "aggregate function called in non-aggregate context");

	if (PG_ARGISNULL(0))
	{
		statearr = (ArrayType *) PG_GETARG_ARRAYTYPE_P_COPY(1);

		PG_RETURN_POINTER(statearr);
	}

	statearr = (ArrayType *) PG_GETARG_ARRAYTYPE_P(0);
	state = (uint64 *) ARR_DATA_PTR(statearr);

	state[0] += incoming[0];
	state[1] += incoming[1];
	state[2] += incoming[2];

	PG_RETURN_POINTER(statearr);
}

/*
 * cq_rank()  - streaming rank of hypothetical row
 */
Datum
cq_hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(1);

	state = (ArrayType *) PG_GETARG_POINTER(0);
	values = (uint64 *) ARR_DATA_PTR(state);


	/* minimum rank is always 1 */
	PG_RETURN_INT64(values[1] + 1);
}

/*
 * cq_percent_rank()  - streaming percent_rank of hypothetical row
 */
Datum
cq_hypothetical_percent_rank_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;
	uint64 rowcount;
	uint64 rank;
	double result;

	if (PG_ARGISNULL(0))
		PG_RETURN_FLOAT8(0);

	state = (ArrayType *) PG_GETARG_POINTER(0);
	values = (uint64 *) ARR_DATA_PTR(state);
	rowcount = values[0];
	rank = values[1] + 1;

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	result = (double) (rank - 1) / (double) (rowcount);

	PG_RETURN_FLOAT8(result);
}

/*
 * cume_dist()  - streaming cume_dist of hypothetical row
 */
Datum
cq_hypothetical_cume_dist_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;
	uint64 rowcount;
	uint64 rank;
	uint64 peers;
	double result;

	if (PG_ARGISNULL(0))
		PG_RETURN_FLOAT8(0);

	state = (ArrayType *) PG_GETARG_POINTER(0);
	values = (uint64 *) ARR_DATA_PTR(state);
	rowcount = values[0];
	rank = values[1] + 1;
	peers = values[2];

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	/* cume_dist counts peer values as increases in rank */
	rank += peers;
	result = (double) (rank) / (double) (rowcount + 1);

	PG_RETURN_FLOAT8(result);
}

/*
 * hll_dense_rank()  - dense rank of hypothetical row using HyperLogLog
 */
Datum
hll_hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(1);

	hll = (HyperLogLog *) PG_GETARG_POINTER(0);

	PG_RETURN_INT64(HLLCardinality(hll) + 1);
}

typedef struct CQOSAAggState
{
	TDigest *tdigest;
	bool is_multiple;
	bool is_descending;
	int num_percentiles;
	float8 *percentiles;
	bool *nulls;
} CQOSAAggState;

/*
 * cqosastatesend
 */
Datum
cqosastatesend(PG_FUNCTION_ARGS)
{
	CQOSAAggState *state = (CQOSAAggState *) PG_GETARG_POINTER(0);
	TDigest *t = state->tdigest;
	bytea *result;
	int nbytes;
	char *pos;

	TDigestCompress(t);

	nbytes = (sizeof(CQOSAAggState) + sizeof(float8) * state->num_percentiles + sizeof(bool) * state->num_percentiles +
			TDigestSize(t));

	result = (bytea *) palloc0(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pos = VARDATA(result);
	memcpy(pos, state, sizeof(CQOSAAggState));
	pos += sizeof(CQOSAAggState);

	memcpy(pos, state->percentiles, sizeof(float8) * state->num_percentiles);
	pos += sizeof(float8) * state->num_percentiles;
	memcpy(pos, state->nulls, sizeof(bool) * state->num_percentiles);
	pos += sizeof(bool) * state->num_percentiles;

	memcpy(pos, t, TDigestSize(t));

	PG_RETURN_BYTEA_P(result);
}

/*
 * cqosastaterecv
 */
Datum
cqosastaterecv(PG_FUNCTION_ARGS)
{
	bytea *bytes = (bytea *) PG_GETARG_BYTEA_P(0);
	char *pos = VARDATA(bytes);
	CQOSAAggState *state = palloc(sizeof(CQOSAAggState));
	TDigest *t;

	memcpy(state, pos, sizeof(CQOSAAggState));
	pos += sizeof(CQOSAAggState);

	state->percentiles = palloc(sizeof(float8) * state->num_percentiles);
	memcpy(state->percentiles, pos, sizeof(float8) * state->num_percentiles);
	pos += sizeof(float8) * state->num_percentiles;
	state->nulls = palloc(sizeof(bool) * state->num_percentiles);
	memcpy(state->nulls, pos, sizeof(bool) * state->num_percentiles);
	pos += sizeof(bool) * state->num_percentiles;

	t = (TDigest *) pos;
	state->tdigest = palloc(TDigestSize(t));
	memcpy(state->tdigest, t, TDigestSize(t));

	PG_RETURN_POINTER(state);
}

static CQOSAAggState *
cq_percentile_cont_float8_startup(PG_FUNCTION_ARGS, bool is_multiple)
{
	CQOSAAggState *aggstate;
	MemoryContext old;
	Aggref *aggref = AggGetAggref(fcinfo);
	Datum value;
	bool isnull;
	ExprContext *econtext;
	ExprState *expr;
	SortGroupClause *sortcl = (SortGroupClause *) linitial(aggref->aggorder);

	/* our state needs to live for the duration of the query */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	/*
	 * We need to store the arguments to this aggregate function in the AggState because
	 * only the transition state is passed to the final_fn for CQ aggregates. Unlike regular
	 * aggregates where we can just read the incoming directargs in the final_fn.
	 */
	aggstate = palloc0(sizeof(CQOSAAggState));
	aggstate->tdigest = TDigestCreate();
	aggstate->is_multiple = is_multiple;
	/* HACK: nulls_first implies descending */
	aggstate->is_descending = sortcl->nulls_first;

	econtext = CreateStandaloneExprContext();
	expr = (ExprState *) linitial((List *) ExecInitExpr((Expr *) aggref->aggdirectargs, NULL));
	value = ExecEvalExpr(expr, econtext, &isnull, NULL);

	if (isnull)
		aggstate->num_percentiles = 0;
	else if (is_multiple)
	{
		ArrayType *param = DatumGetArrayTypeP(value);
		Datum *percentiles_datum;
		int i;

		deconstruct_array(param, FLOAT8OID,
				/* hard-wired info on type float8 */
				8, FLOAT8PASSBYVAL, 'd',
				&percentiles_datum,
				&aggstate->nulls,
				&aggstate->num_percentiles);

		aggstate->percentiles = (float8 *) palloc(sizeof(float8) * aggstate->num_percentiles);

		for (i = 0; i < aggstate->num_percentiles; i++)
			aggstate->percentiles[i] = DatumGetFloat8(percentiles_datum[i]);
	}
	else
	{
		aggstate->num_percentiles = 1;
		aggstate->percentiles = (float8 *) palloc(sizeof(float8));
		aggstate->nulls = (bool *) palloc(sizeof(bool));
		aggstate->percentiles[0] = DatumGetFloat8(value);
		aggstate->nulls[0] = false;
	}

	MemoryContextSwitchTo(old);

	FreeExprContext(econtext, false);

	return aggstate;
}

/*
 * Transition function for percentile_cont aggregates with a single
 * numeric column. It uses t-digest to estimate quantiles.
 */
static Datum
cq_percentile_cont_float8_transition_common(PG_FUNCTION_ARGS, bool is_multiple)
{
	MemoryContext old;
	MemoryContext context;
	CQOSAAggState *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cq_percentile_cont_float8_startup(fcinfo, is_multiple);
	else
		state = (CQOSAAggState *)  PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		Datum d = PG_GETARG_DATUM(1);
		TDigestAdd(state->tdigest, DatumGetFloat8(d), 1);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
cq_percentile_cont_float8_transition(PG_FUNCTION_ARGS)
{
	return cq_percentile_cont_float8_transition_common(fcinfo, false);
}

Datum
cq_percentile_cont_float8_transition_multi(PG_FUNCTION_ARGS)
{
	return cq_percentile_cont_float8_transition_common(fcinfo, true);
}

/*
 * combines multiple t-digest transition states into one
 */
Datum
cq_percentile_cont_float8_combine(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	CQOSAAggState *state;
	CQOSAAggState *incoming = (CQOSAAggState *) PG_GETARG_POINTER(1);

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = (CQOSAAggState *) palloc0(sizeof(CQOSAAggState));
		memcpy(state, incoming, sizeof(CQOSAAggState));
		state->tdigest = TDigestCopy(incoming->tdigest);
		state->percentiles = (float8 *) palloc(sizeof(float8) * state->num_percentiles);
		state->nulls = (bool *) palloc(sizeof(bool) * state->num_percentiles);
		memcpy(state->percentiles, incoming->percentiles, sizeof(float8) * state->num_percentiles);
		memcpy(state->nulls, incoming->nulls, sizeof(bool) * state->num_percentiles);
	}
	else
	{
		state = (CQOSAAggState *) PG_GETARG_POINTER(0);
		TDigestMerge(state->tdigest, incoming->tdigest);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
  * percentile_cont(float8) within group (float8) - streaming continuous percentile
 */
Datum
cq_percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
	CQOSAAggState *state;
	float8 percentile;
	int i;
	Datum *result_datum;

	Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (CQOSAAggState *) PG_GETARG_POINTER(0);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (state->tdigest->total_weight == 0)
		PG_RETURN_NULL();

	if (state->num_percentiles == 0)
	{
		if (state->is_multiple)
			PG_RETURN_POINTER(construct_empty_array(FLOAT8OID));
		else
			PG_RETURN_NULL();
	}

	result_datum = (Datum *) palloc(state->num_percentiles * sizeof(Datum));

	for (i = 0; i < state->num_percentiles; i++)
	{
		percentile = state->percentiles[i];

		if (percentile < 0 || percentile > 1 || isnan(percentile))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							errmsg("percentile value %g is not between 0 and 1",
									percentile)));

		if (state->is_descending)
			percentile = 1.0 - percentile;

		if (state->nulls[i])
			continue;

		result_datum[i] = Float8GetDatum(TDigestQuantile(state->tdigest, percentile));
	}

	if (state->is_multiple)
	{
		/* We make the output array the same shape as the input;
		 * hard-wired info on type float8 */
		int dims[1];
		int lbs[1];
		dims[0] = state->num_percentiles;
		lbs[0] = 1;
		PG_RETURN_POINTER(construct_md_array(result_datum, state->nulls,
											 1,
											 dims,
											 lbs,
											 FLOAT8OID,
											 8,
											 FLOAT8PASSBYVAL,
											 'd'));
	}

	PG_RETURN_DATUM(result_datum[0]);
}
