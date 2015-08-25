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
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/fss.h"
#include "utils/array.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fssfuncs.h"
#include "utils/typcache.h"

#define DEFAULT_K 5

static FSS *
fss_fix_ptrs(struct varlena *bytes)
{
	char *pos;
	FSS *fss = (FSS *) bytes;

	/* Fix pointers */
	pos = (char *) fss;
	pos += sizeof(FSS);
	fss->bitmap_counter = (Counter *) pos;
	pos += sizeof(Counter) * fss->h;
	fss->monitored_elements = (MonitoredElement *) pos;

	if (FSS_STORES_DATUMS(fss))
	{
		pos += sizeof(MonitoredElement) * fss->m;
		fss->top_k = (Datum *) pos;
	}
	else
		fss->top_k = NULL;

	return fss;
}

Datum
fss_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	FSS *fss;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));

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
		uint16_t k = PG_GETARG_INT64(2);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreate(k, typ);
	}
	else
		state = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));

	FSSIncrement(state, incoming);

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
		uint16_t k = PG_GETARG_INT64(2);
		uint16_t m = PG_GETARG_INT64(3);
		uint16_t h = PG_GETARG_INT64(4);
		Oid type = AggGetInitialArgType(fcinfo);
		TypeCacheEntry *typ = lookup_type_cache(type, 0);
		fcinfo->flinfo->fn_extra = typ;
		state = FSSCreateWithMAndH(k, typ, m, h);
	}
	else
		state = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));

	FSSIncrement(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FSS *state;
	FSS *incoming = fss_fix_ptrs(PG_GETARG_VARLENA_P(1));

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "fss_merge_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = FSSCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));
	state = FSSMerge(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
fss_topk(PG_FUNCTION_ARGS)
{
	FSS *fss;
	Datum *datums;
	ArrayType *arr;
	TypeCacheEntry *typ;
	uint16_t found;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fss = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));

	datums = FSSTopK(fss, fss->k, &found);
	typ = lookup_type_cache(fss->typ.typoid, 0);
	arr = construct_array(datums, found, typ->type_id, typ->typlen, typ->typbyval, typ->typalign);

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
	uint16_t k = PG_GETARG_INT64(1);
	uint16_t m = PG_GETARG_INT64(2);
	uint16_t h = PG_GETARG_INT64(3);
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
		fss = fss_fix_ptrs(PG_GETARG_VARLENA_P(0));

		if (fss->typ.typoid != typ->type_id)
			elog(ERROR, "type mismatch for incoming value");
	}

	FSSIncrement(fss, PG_GETARG_DATUM(1));

	PG_RETURN_POINTER(fss);
}
