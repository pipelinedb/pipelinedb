/*-------------------------------------------------------------------------
 *
 * topkfuncs.c
 *		Filtered Space Saving top-k functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "analyzer.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "fss.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "topkfuncs.h"
#include "utils/array.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/typcache.h"

#define DEFAULT_K 5

/*
 * topk_print
 */
PG_FUNCTION_INFO_V1(topk_print);
Datum
topk_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	FSS *fss;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ k = %d, m = %d, h = %d, count = %ld, size = %ldkB }", fss->k, fss->m, fss->h, fss->count, FSSSize(fss) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
}

/*
 * topk_agg_trans
 */
PG_FUNCTION_INFO_V1(topk_agg_trans);
Datum
topk_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "topk_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		/*
		 * We use 32-bit integers as arguments so users don't have to cast integers in their invocations
		 */
		int k = PG_GETARG_INT32(2);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreate(k, typ);
	}
	else
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	if (!PG_ARGISNULL(1))
		state = FSSIncrement(state, PG_GETARG_DATUM(1), PG_ARGISNULL(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(topk_agg_weighted_trans);
Datum
topk_agg_weighted_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	int64 weight = PG_GETARG_INT64(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "topk_agg_weighted_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		/*
		 * We use 32-bit integers as arguments so users don't have to cast integers in their invocations
		 */
		int k = PG_GETARG_INT32(2);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreate(k, typ);
	}
	else
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));

	if (!PG_ARGISNULL(1))
		state = FSSIncrementWeighted(state, PG_GETARG_DATUM(1), PG_ARGISNULL(1), weight);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * topk_agg_transp
 */
PG_FUNCTION_INFO_V1(topk_agg_transp);
Datum
topk_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "topk_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		/*
		 * We use 32-bit integers as arguments so users don't have to cast integers in their invocations
		 */
		int k = PG_GETARG_INT32(2);
		int m = PG_GETARG_INT32(3);
		int h = PG_GETARG_INT32(4);
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

	if (!PG_ARGISNULL(1))
		state = FSSIncrement(state, PG_GETARG_DATUM(1), PG_ARGISNULL(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * topk_merge_agg_trans
 */
PG_FUNCTION_INFO_V1(topk_merge_agg_trans);
Datum
topk_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	FSS *incoming;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "topk_merge_agg_trans called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		incoming = FSSFromBytes(PG_GETARG_VARLENA_P(1));
		state = FSSCopy(incoming);
	}
	else if (PG_ARGISNULL(1))
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));
	else
	{
		state = FSSFromBytes(PG_GETARG_VARLENA_P(0));
		incoming = FSSFromBytes(PG_GETARG_VARLENA_P(1));
		state = FSSMerge(state, incoming);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * topk
 */
PG_FUNCTION_INFO_V1(topk);
Datum
topk(PG_FUNCTION_ARGS)
{
	FSS *fss;
	Datum *datums;
	uint64_t *freqs;
	bool *null_k;
	uint16_t found;
	Tuplestorestate *store;
	ReturnSetInfo *rsi;
	TupleDesc desc;
	MemoryContext old;
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

	old = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
	store = tuplestore_begin_heap(false, false, work_mem);
	MemoryContextSwitchTo(old);

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

/*
 * topk_values
 */
PG_FUNCTION_INFO_V1(topk_values);
Datum
topk_values(PG_FUNCTION_ARGS)
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

/*
 * topk_freqs
 */
PG_FUNCTION_INFO_V1(topk_freqs);
Datum
topk_freqs(PG_FUNCTION_ARGS)
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

PG_FUNCTION_INFO_V1(topk_empty);
Datum
topk_empty(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	int k = PG_GETARG_INT32(1);
	FSS *fss;

	/*
	 * We use a 32-bit integer as our argument so users don't have to explicitly
	 * cast it to something smaller in their invocations
	 */
	if (k > USHRT_MAX)
		elog(ERROR, "maximum value of k exceeded");

	fss = FSSCreate(k, lookup_type_cache(typid, 0));
	PG_RETURN_POINTER(fss);
}

/*
 * topk_emptyp
 */
PG_FUNCTION_INFO_V1(topk_emptyp);
Datum
topk_emptyp(PG_FUNCTION_ARGS)
{
	Oid typid = PG_GETARG_OID(0);
	int k = PG_GETARG_INT32(1);
	int m = PG_GETARG_INT32(2);
	int h = PG_GETARG_INT32(3);
	FSS *fss;

	/*
	 * We use 32-bit integers as arguments so users don't have explicitly cast integers
	 * into smaller integers in their invocations
	 */
	if (k > m || k > USHRT_MAX)
		elog(ERROR, "maximum value of k exceeded");
	if (m > USHRT_MAX)
		elog(LOG, "maximum value of m exceeded");
	if (h > USHRT_MAX)
		elog(ERROR, "maximum value of h exceeded");

	fss = FSSCreateWithMAndH(k, lookup_type_cache(typid, 0), m, h);
	PG_RETURN_POINTER(fss);
}

/*
 * topk_increment
 */
PG_FUNCTION_INFO_V1(topk_increment);
Datum
topk_increment(PG_FUNCTION_ARGS)
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

/*
 * topk_increment_weighted
 */
PG_FUNCTION_INFO_V1(topk_increment_weighted);
Datum
topk_increment_weighted(PG_FUNCTION_ARGS)
{
	FSS *fss;
	TypeCacheEntry *typ = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	int64 weight = PG_GETARG_INT64(2);

	if (PG_ARGISNULL(0))
		fss = FSSCreate(DEFAULT_K, typ);
	else
	{
		fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));

		if (fss->typ.typoid != typ->type_id)
			elog(ERROR, "type mismatch for incoming value");
	}

	fss = FSSIncrementWeighted(fss, PG_GETARG_DATUM(1), PG_ARGISNULL(1), weight);

	PG_RETURN_POINTER(fss);
}

/*
 * topk_in
 */
PG_FUNCTION_INFO_V1(topk_in);
Datum
topk_in(PG_FUNCTION_ARGS)
{
	char *raw = PG_GETARG_CSTRING(0);
	Datum result = DirectFunctionCall1(byteain, (Datum) raw);

	PG_RETURN_POINTER(result);
}

/*
 * topk_out
 */
PG_FUNCTION_INFO_V1(topk_out);
Datum
topk_out(PG_FUNCTION_ARGS)
{
	FSS *fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));
	char *s = (char *) DirectFunctionCall1(byteaout, (Datum) fss);

	PG_RETURN_CSTRING(s);
}

/*
 * topk_send
 */
PG_FUNCTION_INFO_V1(topk_send);
Datum
topk_send(PG_FUNCTION_ARGS)
{
	FSS *fss = FSSFromBytes(PG_GETARG_VARLENA_P(0));
	bytea *result = (bytea *) DirectFunctionCall1(byteasend, (Datum) fss);

	PG_RETURN_BYTEA_P(result);
}

/*
 * topk_recv
 */
PG_FUNCTION_INFO_V1(topk_recv);
Datum
topk_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea *result = (bytea *) DirectFunctionCall1(bytearecv, (Datum) buf);

	PG_RETURN_BYTEA_P(result);
}
