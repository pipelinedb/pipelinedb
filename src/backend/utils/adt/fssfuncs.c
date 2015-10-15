/*-------------------------------------------------------------------------
 *
 * fssfuncs.c
 *		Filtered Space Saving functions
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/fssfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/fss.h"
#include "utils/array.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fssfuncs.h"
#include "utils/typcache.h"

#define DEFAULT_K 5

Datum
fss_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	FSS *fss;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ m = %d, h = %d, count = %ld, size = %ldkB }", fss->m, fss->h, fss->count, FSSSize(fss) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
}

Datum
fss_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "fss_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		uint16_t k = PG_GETARG_UINT16(2);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreate(k, typ);
	}
	else
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	state = FSSIncrement(state, incoming, PG_ARGISNULL(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_agg_weighted_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	Datum incoming = PG_GETARG_DATUM(1);
	int64 weight = PG_GETARG_INT64(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "fss_agg_weighted_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		uint16_t k = PG_GETARG_UINT16(2);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreate(k, typ);
	}
	else
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	state = FSSIncrementWeighted(state, incoming, PG_ARGISNULL(1), weight);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "fss_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		uint16_t k = PG_GETARG_UINT16(2);
		uint16_t m = PG_GETARG_UINT16(3);
		uint16_t h = PG_GETARG_UINT16(4);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;

		if (k > m || k > USHRT_MAX)
			elog(ERROR, "maximum value of k exceeded");
		if (m > USHRT_MAX)
			elog(LOG, "maximum value of m exceeded");
		if (h > USHRT_MAX)
			elog(ERROR, "maximum value of h exceeded");

		state = FSSCreateWithMAndH(k, typ, m, h);
	}
	else
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	state = FSSIncrement(state, incoming, PG_ARGISNULL(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	FSS *incoming = FSSFromBytes(PG_GETARG_VARLENA_P(1));

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "fss_merge_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = FSSCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = FSSFromBytes(PG_GETARG_VARLENA_P(0));
	state = FSSMerge(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_topk(PG_FUNCTION_ARGS)
{
	FSS *fss;
	Datum *datums;
	uint64_t *freqs;
	bool *null_k;
	uint16_t found;
	Tuplestorestate *store;
	ReturnSetInfo *rsi;
	TupleDesc desc;
	int i;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));
	desc= CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(desc, (AttrNumber) 1, "value", fss->typ.typoid, -1, 0);
	TupleDescInitEntry(desc, (AttrNumber) 2, "frequency", INT8OID, -1, 0);

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	rsi->returnMode = SFRM_Materialize;
	rsi->setDesc = BlessTupleDesc(desc);

	store = tuplestore_begin_heap(false, false, work_mem);
	datums = FSSTopK(fss, fss->k, &null_k, &found);
	freqs = FSSTopKCounts(fss, fss->k, &found);

	for (i = 0; i < found; i++)
	{
		Datum values[2];
		bool nulls[2];
		HeapTuple tup;

		MemSet(nulls, false, sizeof(nulls));

		nulls[0] = null_k[i];
		values[0] = datums[i];
		values[1] = freqs[i];

		tup = heap_form_tuple(rsi->setDesc, values, nulls);
		tuplestore_puttuple(store, tup);
	}

	rsi->setResult = store;

	PG_RETURN_NULL();
}

Datum
fss_topk_values(PG_FUNCTION_ARGS)
{
	FSS *fss;
	Datum *datums;
	ArrayType *arr;
	TypeCacheEntry *typ;
	uint16_t found;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	datums = FSSTopK(fss, fss->k, NULL, &found);
	typ = lookup_type_cache(fss->typ.typoid, 0);
	arr = construct_array(datums, found, typ->type_id, typ->typlen, typ->typbyval, typ->typalign);

	PG_RETURN_ARRAYTYPE_P(arr);
}

Datum
fss_topk_freqs(PG_FUNCTION_ARGS)
{
	FSS *fss;
	Datum *datums;
	ArrayType *arr;
	uint16_t found;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	datums = FSSTopKCounts(fss, fss->k, &found);
	arr = construct_array(datums, found, INT8OID, 8, true, 'd');

	PG_RETURN_ARRAYTYPE_P(arr);
}

Datum
fss_empty(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	uint16_t k = PG_GETARG_INT64(1);
	FSS *fss = FSSCreate(k, lookup_type_cache(typid, 0));
	PG_RETURN_POINTER(fss);
}

Datum
fss_emptyp(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	uint16_t k = PG_GETARG_UINT16(1);
	uint16_t m = PG_GETARG_UINT16(2);
	uint16_t h = PG_GETARG_UINT16(3);
	FSS *fss = FSSCreateWithMAndH(k, lookup_type_cache(typid, 0), m, h);
	PG_RETURN_POINTER(fss);
}

Datum
fss_increment(PG_FUNCTION_ARGS)
{
	FSS *fss;
	TypeCacheEntry *typ = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);

	if (PG_ARGISNULL(0))
		fss = FSSCreate(DEFAULT_K, typ);
	else
	{
		fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

		if (fss->typ.typoid != typ->type_id)
			elog(ERROR, "type mismatch for incoming value");
	}

	fss = FSSIncrement(fss, PG_GETARG_DATUM(1), PG_ARGISNULL(1));

	PG_RETURN_POINTER(fss);
}
