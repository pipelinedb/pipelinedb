/*-------------------------------------------------------------------------
 *
 * cmsfuncs.c
 *		Count-Min Sketch Filter functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cmsfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/cms.h"
#include "utils/datum.h"
#include "utils/cmsfuncs.h"
#include "utils/typcache.h"

Datum
cms_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "user-specified count-min sketches are not supported");
	PG_RETURN_NULL();
}

Datum
cms_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	CountMinSketch *cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ d = %d, w = %d, size = %ldk }", cms->w, cms->d, CountMinSketchSize(cms) / 1024);

	PG_RETURN_CSTRING(buf.data);
}

static CountMinSketch *
cms_startup(FunctionCallInfo fcinfo, float8 eps, float8 p)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	TargetEntry *te = (TargetEntry *) linitial(aggref->args);
	CountMinSketch *cms;
	Oid type = exprType((Node *) te->expr);

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p && eps)
		cms = CountMinSketchCreateWithEpsAndP(eps, p);
	else
		cms = CountMinSketchCreate();

	SET_VARSIZE(cms, CountMinSketchSize(cms));

	return cms;
}

static CountMinSketch *
cms_add(FunctionCallInfo fcinfo, CountMinSketch *cms, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		CountMinSketchAdd(cms, (char *) &elem, size, 1);
	else
		CountMinSketchAdd(cms, DatumGetPointer(elem), size, 1);

	return cms;
}

/*
 * cms_agg transition function -
 * 	adds the given element to the transition cms
 */
Datum
cms_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cms_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cms_startup(fcinfo, 0, 0);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	state = cms_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * cms_agg transition function -
 *
 * 	adds the given element to the transition cms using the given value for p and n
 */
Datum
cms_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	Datum incoming = PG_GETARG_DATUM(1);
	float8 eps = PG_GETARG_FLOAT8(2);
	float8 p = PG_GETARG_FLOAT8(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cms_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cms_startup(fcinfo, eps, p);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	state = cms_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * cms_merge_agg transition function -
 *
 * 	returns the merge of the transition state and the given cms
 */
Datum
cms_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	CountMinSketch *incoming = (CountMinSketch *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cms_merge_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = CountMinSketchCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	state = CountMinSketchMerge(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * Returns the estimate count of the item
 */
Datum
cms_count(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	Datum elem = PG_GETARG_DATUM(1);
	uint32_t count = false;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	Size size;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(count);

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);
	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		count = CountMinSketchEstimateCount(cms, (char *) &elem, size);
	else
		count = CountMinSketchEstimateCount(cms, DatumGetPointer(elem), size);

	PG_RETURN_INT32(count);
}
