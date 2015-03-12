/*-------------------------------------------------------------------------
 *
 * gcsfuncs.c
 *		Golomb-coded set functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/gcsfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/gcs.h"
#include "utils/datum.h"
#include "utils/gcsfuncs.h"
#include "utils/typcache.h"

Datum
gcs_send(PG_FUNCTION_ARGS)
{
	GolombCodedSet *gcs = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);
	gcs = GolombCodedSetCompress(gcs);
	SET_VARSIZE(gcs, GolombCodedSetSize(gcs));
	PG_RETURN_POINTER(gcs);
}

Datum
gcs_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "user-specified golomb-coded sets are not supported");
	PG_RETURN_NULL();
}

Datum
gcs_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	GolombCodedSet *gcs = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ n = %d, p = %f, fill = %f, size = %ldkB }", gcs->n, 1.0 / gcs->p, GolombCodedSetFillRatio(gcs), GolombCodedSetSize(gcs) / 1024);

	PG_RETURN_CSTRING(buf.data);
}

static GolombCodedSet *
gcs_startup(FunctionCallInfo fcinfo, float8 p, uint64_t n)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	TargetEntry *te = (TargetEntry *) linitial(aggref->args);
	GolombCodedSet *gcs;
	Oid type = exprType((Node *) te->expr);

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p && n)
		gcs = GolombCodedSetCreateWithPAndN(p, n);
	else
		gcs = GolombCodedSetCreate();

	SET_VARSIZE(gcs, GolombCodedSetSize(gcs));

	return gcs;
}

static GolombCodedSet *
gcs_add(FunctionCallInfo fcinfo, GolombCodedSet *gcs, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size;

	if (!typ->typbyval && !elem)
		return gcs;

	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		GolombCodedSetAdd(gcs, (char *) &elem, size);
	else
		GolombCodedSetAdd(gcs, DatumGetPointer(elem), size);

	return gcs;
}

/*
 * gcs_agg transition function -
 * 	adds the given element to the transition Golomb-coded set
 */
Datum
gcs_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	GolombCodedSet *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "gcs_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = gcs_startup(fcinfo, 0, 0);
	else
		state = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);

	state = gcs_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * gcs_agg transition function -
 *
 * 	adds the given element to the transition Gcs Filter using the given value for p and n
 */
Datum
gcs_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	GolombCodedSet *state;
	Datum incoming = PG_GETARG_DATUM(1);
	float8 p = PG_GETARG_FLOAT8(2);
	uint64_t n = PG_GETARG_INT64(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "gcs_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = gcs_startup(fcinfo, p, n);
	else
		state = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);

	state = gcs_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * gcs_union_agg transition function -
 *
 * 	returns the union of the transition state and the given Golomb-coded set
 */
Datum
gcs_union_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	GolombCodedSet *state;
	GolombCodedSet *incoming = (GolombCodedSet *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "gcs_union_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = GolombCodedSetCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);
	state = GolombCodedSetShallowUnion(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * gcs_intersection_agg transition function -
 *
 * 	returns the intersection of the transition state and the given Golomb-coded set
 */
Datum
gcs_intersection_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	GolombCodedSet *state;
	GolombCodedSet *incoming = (GolombCodedSet *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "gcs_union_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = GolombCodedSetCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);
	state = GolombCodedSetIntersection(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * Returns whether the Golomb-coded set contains the item or not
 */
Datum
gcs_contains(PG_FUNCTION_ARGS)
{
	GolombCodedSet *gcs;
	Datum elem = PG_GETARG_DATUM(1);
	bool contains = false;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	Size size;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(contains);

	gcs = (GolombCodedSet *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);
	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		contains = GolombCodedSetContains(gcs, (char *) &elem, size);
	else
		contains = GolombCodedSetContains(gcs, DatumGetPointer(elem), size);

	PG_RETURN_BOOL(contains);
}
