/*-------------------------------------------------------------------------
 *
 * tdigestfuncs.c
 *		t-digest functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tdigestfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/tdigest.h"
#include "utils/datum.h"
#include "utils/tdigestfuncs.h"
#include "utils/typcache.h"

static TDigest *
tdigest_unpack(bytea *bytes)
{
	StringInfoData buf;
	float8 x;
	int64_t w;
	int64_t count;
	TDigest *t;

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), VARSIZE(bytes) - VARHDRSZ);

	t = TDigestCreateWithCompression(pq_getmsgfloat8(&buf));
	count = pq_getmsgint(&buf, 4);

	while(count--)
	{
		x = pq_getmsgfloat8(&buf);
		w = pq_getmsgint64(&buf);
		TDigestAdd(t, x, w);
	}

	return t;
}

Datum
tdigest_send(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_POINTER(0);
	StringInfoData buf;
	bytea *result;
	int nbytes;
	AVLNodeIterator *it;
	Centroid *c;

	initStringInfo(&buf);

	pq_sendfloat8(&buf, t->compression);
	pq_sendint(&buf, t->summary->size, 4);

	it = AVLNodeIteratorCreate(t->summary, NULL);


	while ((c = AVLNodeNext(it)))
	{
		pq_sendfloat8(&buf, c->mean);
		pq_sendint64(&buf, c->count);
	}

	AVLNodeIteratorDestroy(it);

	nbytes = buf.len - buf.cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pq_copymsgbytes(&buf, VARDATA(result), nbytes);

	PG_RETURN_POINTER(result);
}

Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "user-specified t-digests are not supported");
	PG_RETURN_NULL();
}

Datum
tdigest_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	TDigest *t = tdigest_unpack(PG_GETARG_BYTEA_P(0));

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ c = %ld, k = %d }", t->count, (int) t->compression);

	PG_RETURN_CSTRING(buf.data);
}

static TDigest *
tdigest_startup(FunctionCallInfo fcinfo, uint32_t k)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	TargetEntry *te = (TargetEntry *) linitial(aggref->args);
	TDigest *t;
	Oid type = exprType((Node *) te->expr);
	TypeCacheEntry *typ = lookup_type_cache(type, 0);

	fcinfo->flinfo->fn_extra = typ;

	if (!typ->typbyval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid input data type for t-digests")));

	if (k)
		t = TDigestCreateWithCompression(k);
	else
		t = TDigestCreate();

	return t;
}

/*
 * tdigest_agg transition function -
 * 	adds the given element to the transition tdigest
 */
Datum
tdigest_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;
	float8 incoming = PG_GETARG_FLOAT8(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "tdigest_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = tdigest_startup(fcinfo, 0);
	else
		state = (TDigest *) PG_GETARG_POINTER(0);

	TDigestAddSingle(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * tdigest_agg transition function -
 *
 * 	adds the given element to the transition tdigest using the given value for compression
 */
Datum
tdigest_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;
	float8 incoming = PG_GETARG_FLOAT8(1);
	uint64_t k = PG_GETARG_INT32(2);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "tdigest_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = tdigest_startup(fcinfo, k);
	else
		state = (TDigest *) PG_GETARG_POINTER(0);

	TDigestAddSingle(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * tdigest_merge_agg transition function -
 *
 * 	returns the union of the transition state and the given tdigest
 */
Datum
tdigest_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;
	TDigest *incoming = tdigest_unpack(PG_GETARG_BYTEA_P(1));

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "tdigest_merge_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = TDigestCreateWithCompression(incoming->compression);
	else
		state = (TDigest *) PG_GETARG_POINTER(0);

	state = TDigestMerge(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * tdigest_quantile
 */
Datum
tdigest_quantile(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 q;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	t = tdigest_unpack(PG_GETARG_BYTEA_P(0));
	q = PG_GETARG_FLOAT8(1);

	PG_RETURN_FLOAT8(TDigestQuantile(t, q));
}

/*
 * tdigest_cdf
 */
Datum
tdigest_cdf(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 x;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	t = tdigest_unpack(PG_GETARG_BYTEA_P(0));
	x = PG_GETARG_FLOAT8(1);

	PG_RETURN_FLOAT8(TDigestCDF(t, x));
}
