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

Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "user-specified tdigest filters are not supported");
	PG_RETURN_NULL();
}

Datum
tdigest_out(PG_FUNCTION_ARGS)
{

}

static TDigest *
tdigest_startup(FunctionCallInfo fcinfo, uint64_t m, uint16_t k, float8 p, uint64_t n)
{

}

static TDigest *
tdigest_add(FunctionCallInfo fcinfo, TDigest *tdigest, Datum elem)
{

}

/*
 * tdigest_agg transition function -
 * 	adds the given element to the transition tdigest filter
 */
Datum
tdigest_agg_trans(PG_FUNCTION_ARGS)
{

}

/*
 * tdigest_agg transition function -
 *
 * 	adds the given element to the transition tdigest Filter using the given value for p and n
 */
Datum
tdigest_agg_transp(PG_FUNCTION_ARGS)
{

}

/*
 * tdigest_merge_agg transition function -
 *
 * 	returns the union of the transition state and the given tdigest filter
 */
Datum
tdigest_merge_agg_trans(PG_FUNCTION_ARGS)
{

}

/*
 * tdigest_quantile
 */
Datum
tdigest_quantile(PG_FUNCTION_ARGS)
{

}

/*
 * tdigest_cdf
 */
Datum
tdigest_cdf(PG_FUNCTION_ARGS)
{

}
