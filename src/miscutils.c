/*-------------------------------------------------------------------------
 *
 * Miscellaneous utilities
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <math.h>
#include <unistd.h>

#include "analyzer.h"
#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "commands/tablecmds.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "microbatch.h"
#include "miscutils.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "port.h"
#include "reaper.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/varlena.h"

volatile sig_atomic_t pipeline_got_SIGTERM = false;

#define round_down(value, base) ((value) - ((value) % (base)))

/*
 * append_suffix
 */
void
append_suffix(char *str, char *suffix, int max_len)
{
	strcpy(&str[Min(strlen(str), max_len - strlen(suffix))], suffix);
}

/*
 * rotl64
 */
static inline uint64_t
rotl64 (uint64_t x, int8_t r)
{
	return (x << r) | (x >> (64 - r));
}

/*
 * getblock64
 */
static inline uint64_t
getblock64(const uint64_t *p, int i)
{
	return p[i];
}

/*
 * fmix64
 */
static inline
uint64_t fmix64(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccd;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53;
	k ^= k >> 33;

	return k;
}

/*
 * Murmur3 Hash Function
 * http://web.mit.edu/mmadinot/Desktop/mmadinot/MacData/afs/athena.mit.edu/software/julia_v0.2.0/julia/src/support/MurmurHash3.c
 */
void
MurmurHash3_128(const void *key, const Size len, const uint64_t seed, void *out)
{
	const uint8_t * data = (const uint8_t*)key;
	const int nblocks = len / 16;

	uint64_t h1 = seed;
	uint64_t h2 = seed;

	uint64_t c1 = 0x87c37b91114253d5;
	uint64_t c2 = 0x4cf5ad432745937f;

	/* body */
	int i;
	const uint64_t * blocks = (const uint64_t *)(data);

	const uint8_t * tail;
	uint64_t k1 = 0;
	uint64_t k2 = 0;

	for(i = 0; i < nblocks; i++)
	{
		uint64_t k1 = getblock64(blocks,i*2+0);
		uint64_t k2 = getblock64(blocks,i*2+1);

		k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;

		h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

		k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

		h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
	}

	/* tail */
	tail = (const uint8_t*)(data + nblocks*16);

	switch(len & 15)
	{
	case 15: k2 ^= ((uint64_t)(tail[14])) << 48;
	case 14: k2 ^= ((uint64_t)(tail[13])) << 40;
	case 13: k2 ^= ((uint64_t)(tail[12])) << 32;
	case 12: k2 ^= ((uint64_t)(tail[11])) << 24;
	case 11: k2 ^= ((uint64_t)(tail[10])) << 16;
	case 10: k2 ^= ((uint64_t)(tail[ 9])) << 8;
	case  9: k2 ^= ((uint64_t)(tail[ 8])) << 0;
	k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

	case  8: k1 ^= ((uint64_t)(tail[ 7])) << 56;
	case  7: k1 ^= ((uint64_t)(tail[ 6])) << 48;
	case  6: k1 ^= ((uint64_t)(tail[ 5])) << 40;
	case  5: k1 ^= ((uint64_t)(tail[ 4])) << 32;
	case  4: k1 ^= ((uint64_t)(tail[ 3])) << 24;
	case  3: k1 ^= ((uint64_t)(tail[ 2])) << 16;
	case  2: k1 ^= ((uint64_t)(tail[ 1])) << 8;
	case  1: k1 ^= ((uint64_t)(tail[ 0])) << 0;
	k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
	};

	/* finalization */
	h1 ^= len; h2 ^= len;

	h1 += h2;
	h2 += h1;

	h1 = fmix64(h1);
	h2 = fmix64(h2);

	h1 += h2;
	h2 += h1;

	((uint64_t*)out)[0] = h1;
	((uint64_t*)out)[1] = h2;
}

/*
 * MurmurHash3_64
 */
uint64_t
MurmurHash3_64(const void *key, const Size len, const uint64_t seed)
{
	uint64_t hash[2];
	MurmurHash3_128(key, len, seed, &hash);
	return hash[0];
}

/*
 * DatumToBytes
 */
void
DatumToBytes(Datum d, TypeCacheEntry *typ, StringInfo buf)
{
	if (typ->type_id != RECORDOID && typ->typtype != TYPTYPE_COMPOSITE)
	{
		Size size;

		if (typ->typlen == -1) /* varlena */
			size = VARSIZE_ANY_EXHDR(DatumGetPointer(d));
		else
			size = datumGetSize(d, typ->typbyval, typ->typlen);

		if (typ->typbyval)
			appendBinaryStringInfo(buf, (char *) &d, size);
		else if (typ->typlen == -1)
			appendBinaryStringInfo(buf, VARDATA_ANY(d), size);
		else
			appendBinaryStringInfo(buf, (char *) DatumGetPointer(d), size);
	}
	else
	{
		/* For composite/RECORD types, we need to serialize all attrs */
		HeapTupleHeader rec = DatumGetHeapTupleHeader(d);
		TupleDesc desc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(rec), HeapTupleHeaderGetTypMod(rec));
		HeapTupleData tmptup;
		int i;

		tmptup.t_len = HeapTupleHeaderGetDatumLength(rec);
		tmptup.t_data = rec;

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);
			bool isnull;
			Datum tmp = heap_getattr(&tmptup, i + 1, desc, &isnull);

			if (isnull)
			{
				appendStringInfoChar(buf, '0');
				continue;
			}

			appendStringInfoChar(buf, '1');
			DatumToBytes(tmp, lookup_type_cache(att->atttypid, 0), buf);
		}
	}
}

/*
 * SlotAttrsToBytes
 */
void
SlotAttrsToBytes(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, StringInfo buf)
{
	TupleDesc desc = slot->tts_tupleDescriptor;
	int i;

	num_attrs = num_attrs == -1 ? desc->natts : num_attrs;

	for (i = 0; i < num_attrs; i++)
	{
		bool isnull;
		AttrNumber attno = attrs == NULL ? i + 1 : attrs[i];
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, attno - 1);
		Datum d = slot_getattr(slot, attno, &isnull);
		TypeCacheEntry *typ = lookup_type_cache(att->atttypid, 0);

		if (isnull)
		{
			appendStringInfoChar(buf, '0');
			continue;
		}

		appendStringInfoChar(buf, '1');
		DatumToBytes(d, typ, buf);
	}
}

/*
 * equalTupleDescsWeak
 *
 * This is less strict than equalTupleDescs and enforces enough similarity that we can merge tuples.
 */
bool
equalTupleDescsWeak(TupleDesc tupdesc1, TupleDesc tupdesc2, bool check_names)
{
	int	i;

	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
		return false;

	for (i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

		if (check_names && strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->attstattarget != attr2->attstattarget)
			return false;
		if (attr1->attndims != attr2->attndims)
			return false;
		if (attr1->attstorage != attr2->attstorage)
			return false;
		if (attr1->atthasdef != attr2->atthasdef)
			return false;
		if (attr1->attisdropped != attr2->attisdropped)
			return false;
		if (attr1->attcollation != attr2->attcollation)
			return false;
		/* attacl, attoptions and attfdwoptions are not even present... */
	}

	return true;
}

PG_FUNCTION_INFO_V1(timestamptz_round);
Datum
timestamptz_round(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	TimestampTz interval_ts;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (TIMESTAMP_NOT_FINITE(timestamp) || PG_ARGISNULL(1))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	/*
	 * Add the interval to a 0-valued timestamp (TIMESTAMP '2000-01-01 00:00:00')
	 * to an int64 value of just the interval.
	 */
	interval_ts = DatumGetTimestamp(
			DirectFunctionCall2(timestamp_pl_interval, TimestampGetDatum(0), PG_GETARG_DATUM(1)));
	timestamp = round_down(timestamp, interval_ts);

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

/*
 * combine_trans_dummy
 */
PG_FUNCTION_INFO_V1(combine_trans_dummy);
Datum
combine_trans_dummy(PG_FUNCTION_ARGS)
{
	elog(ERROR, "no combine aggregate found");
	PG_RETURN_DATUM(PG_GETARG_DATUM(1));
}

/*
 * pipeline_finalize
 */
PG_FUNCTION_INFO_V1(pipeline_finalize);
Datum
pipeline_finalize(PG_FUNCTION_ARGS)
{
	Datum deserialized;
	Datum finalized;
	FmgrInfo *deserfn;
	FmgrInfo *finalfn;
	FunctionCallInfo deserinfo;
	FunctionCallInfo finalinfo;
	List *fns = NIL;

	if (!fcinfo->flinfo->fn_extra)
	{
		Oid proid;
		MemoryContext old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		AggState *context;
		HeapTuple tup;
		Form_pg_aggregate agg;
		text *name = PG_GETARG_TEXT_PP(0);
		ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
		ArrayIterator iter;
		Datum value;
		bool isnull;
		Oid *argtypes;
		List *names;
		int n;
		int i;
		char *nsp;
		char *fn;
		Oid nspoid;
		oidvector *vec;

		n = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
		argtypes = palloc0(sizeof(Oid) * n);

		/*
		 * Create an oidvector of argument types from an array of schema-qualified type names
		 */
		i = 0;
		iter = array_create_iterator(arr, 0, NULL);

		while (array_iterate(iter, &value, &isnull))
		{
			text *t = (text *) DatumGetPointer(value);

			names = textToQualifiedNameList(t);
			argtypes[i++] = LookupTypeNameOid(NULL, makeTypeNameFromNameList(names), false);
		}

		array_free_iterator(iter);
		vec = buildoidvector(argtypes, n);

		/*
		 * We always qualify the name we end up with here, so it's safe to assume we'll have a schema and name
		 */
		names = textToQualifiedNameList(name);
		DeconstructQualifiedName(names, &nsp, &fn);

		nspoid = get_namespace_oid(nsp, false);
		tup = SearchSysCache3(PROCNAMEARGSNSP, CStringGetDatum(fn), PointerGetDatum(vec), ObjectIdGetDatum(nspoid));

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "no pg_proc row found for function \"%s\"", TextDatumGetCString(name));

		proid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(proid));

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "no pg_aggregate row found for OID %u", proid);

		agg = (Form_pg_aggregate) GETSTRUCT(tup);

		old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

		context = (AggState *) makeNode(AggState);
		context->curaggcontext = CreateStandaloneExprContext();

		deserfn = palloc0(sizeof(FmgrInfo));
		deserinfo = palloc0(sizeof(FunctionCallInfoData));

		fmgr_info(agg->aggdeserialfn, deserfn);
		InitFunctionCallInfoData(*deserinfo, deserfn, 2, InvalidOid, (Node *) context, NULL);

		finalfn = palloc0(sizeof(FmgrInfo));
		finalinfo = palloc0(sizeof(FunctionCallInfoData));
		fmgr_info(agg->aggfinalfn, finalfn);

		InitFunctionCallInfoData(*finalinfo, finalfn, 1, InvalidOid, NULL, NULL);

		fcinfo->flinfo->fn_extra = (void *) list_make2(deserinfo, finalinfo);

		ReleaseSysCache(tup);

		MemoryContextSwitchTo(old);
	}

	fns = (List *) fcinfo->flinfo->fn_extra;
	deserinfo = (FunctionCallInfo) linitial(fns);
	deserinfo->arg[0] = PG_GETARG_DATUM(2);
	deserinfo->argnull[0] = PG_ARGISNULL(2);
	deserinfo->argnull[1] = false;

	if (deserinfo->argnull[0] && deserinfo->flinfo->fn_strict)
		PG_RETURN_NULL();

	deserialized = FunctionCallInvoke(deserinfo);

	finalinfo = (FunctionCallInfo) lsecond(fns);
	finalinfo->arg[0] = deserialized;
	finalinfo->argnull[0] = false;

	finalized = FunctionCallInvoke(finalinfo);

	if (finalinfo->isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(finalized);
}

/*
 * pipeline_deserialize
 */
PG_FUNCTION_INFO_V1(pipeline_deserialize);
Datum
pipeline_deserialize(PG_FUNCTION_ARGS)
{
	Datum deserialized;
	FmgrInfo *deserfn;
	FunctionCallInfo deserinfo;

	if (!fcinfo->flinfo->fn_extra)
	{
		RegProcedure proid = PG_GETARG_OID(0);
		MemoryContext old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		AggState *context;

		context = (AggState *) makeNode(AggState);
		context->curaggcontext = CreateStandaloneExprContext();

		deserfn = palloc0(sizeof(FmgrInfo));
		deserinfo = palloc0(sizeof(FunctionCallInfoData));

		fmgr_info(proid, deserfn);
		InitFunctionCallInfoData(*deserinfo, deserfn, 2, InvalidOid, (Node *) context, NULL);

		fcinfo->flinfo->fn_extra = (void *) deserinfo;
		MemoryContextSwitchTo(old);
	}

	deserinfo = (FunctionCallInfo) fcinfo->flinfo->fn_extra;
	deserinfo->arg[0] = PG_GETARG_DATUM(1);
	deserinfo->argnull[0] = PG_ARGISNULL(1);
	deserinfo->argnull[1] = false;

	/*
	 * If the deserialize function is strict, be careful not to even call it with a NULL input
	 */
	if (deserinfo->flinfo->fn_strict && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	deserialized = FunctionCallInvoke(deserinfo);

	if (deserinfo->isnull)
		PG_RETURN_NULL();

	PG_RETURN_POINTER(DatumGetPointer(deserialized));
}

/*
 * GetTypeOid
 */
Oid
GetTypeOid(char *name)
{
	TypeName *t = makeTypeNameFromNameList(list_make1(makeString(name)));

	return LookupTypeNameOid(NULL, t, false);
}

/*
 * print_tupledesc
 * 		print out the given tuple descriptor
 */
void
print_tupledesc(TupleDesc desc)
{
	int i;
	for (i=0; i<desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		printf("\t%2d: \"%s\"\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
			   attr->attnum,
			   NameStr(attr->attname),
			   (unsigned int) (attr->atttypid),
			   attr->attlen,
			   attr->atttypmod,
			   attr->attbyval ? 't' : 'f');
	}
}

/*
 * timestamptz_truncate_by
 */
static Datum
timestamptz_truncate_by(const char *units, PG_FUNCTION_ARGS)
{
	Datum arg = PG_GETARG_TIMESTAMPTZ(0);
	Datum result = DirectFunctionCall2(timestamptz_trunc,
			(Datum) CStringGetTextDatum(units), arg);

	PG_RETURN_TIMESTAMPTZ(result);
}

/*
 * Truncate to year
 */
PG_FUNCTION_INFO_V1(timestamptz_year);
Datum
timestamptz_year(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("year", fcinfo);
}

/*
 * Truncate to month
 */
PG_FUNCTION_INFO_V1(timestamptz_month);
Datum
timestamptz_month(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("month", fcinfo);
}

/*
 * Truncate to day
 */
PG_FUNCTION_INFO_V1(timestamptz_day);
Datum
timestamptz_day(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("day", fcinfo);
}

/*
 * Truncate to hour
 */
PG_FUNCTION_INFO_V1(timestamptz_hour);
Datum
timestamptz_hour(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("hour", fcinfo);
}

/*
 * Truncate to minute
 */
PG_FUNCTION_INFO_V1(timestamptz_minute);
Datum
timestamptz_minute(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("minute", fcinfo);
}

/*
 * Truncate to second
 */
PG_FUNCTION_INFO_V1(timestamptz_second);
Datum
timestamptz_second(PG_FUNCTION_ARGS)
{
	return timestamptz_truncate_by("second", fcinfo);
}

/*
 * pipeline_truncate_continuous_view
 *
 * Truncate all rows from a continuous view's matrel
 */
PG_FUNCTION_INFO_V1(pipeline_truncate_continuous_view);
Datum
pipeline_truncate_continuous_view(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	TruncateStmt *trunc = makeNode(TruncateStmt);
	Relation pipeline_query = OpenPipelineQuery(RowExclusiveLock);

	RangeVar *matrel;
	HeapTuple tuple = GetPipelineQueryTuple(rv);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	matrel = RangeVarGetMatRelName(rv);
	trunc->relations = lappend(trunc->relations, matrel);

	ClosePipelineQuery(pipeline_query, NoLock);

	/* Call TRUNCATE on the backing view table(s). */
	ExecuteTruncate(trunc);

	PG_RETURN_NULL();
}

#define UPDATE_TTL_TEMPLATE "UPDATE pipelinedb.cont_query SET ttl = %d, ttl_attno = %d WHERE id = %d;"

/*
 * pipeline_set_ttl
 *
 * Set a continuous view's TTL info
 */
PG_FUNCTION_INFO_V1(pipeline_set_ttl);
Datum
pipeline_set_ttl(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	Datum result;
	RangeVar *cv_name;
	Datum values[2];
	bool nulls[2];
	TupleDesc	tupdesc;
	MemoryContext oldcontext;
	Interval *ttli = PG_ARGISNULL(1) ? NULL : PG_GETARG_INTERVAL_P(1);
	text *ttl_col = PG_ARGISNULL(2) ? NULL : PG_GETARG_TEXT_P(2);
	TupleDesc desc;
	ContQuery *cv;
	Relation matrel;
	int ttl = -1;
	char *ttl_colname = ttl_col ? TextDatumGetCString(ttl_col) : NULL;
	AttrNumber ttl_attno = InvalidAttrNumber;
	int i;
	StringInfoData buf;

	if (PG_ARGISNULL(0))
		elog(ERROR, "continuous view name is null");

	cv_name = makeRangeVarFromNameList(textToQualifiedNameList(PG_GETARG_TEXT_P(0)));

	if (!SRF_IS_FIRSTCALL())
	{
		funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
		SRF_RETURN_DONE(funcctx);
	}

	/* create a function context for cross-call persistence */
	funcctx = SRF_FIRSTCALL_INIT();

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	/* build tupdesc for result tuples */
	tupdesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "ttl", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ttl_attno", INT2OID, -1, 0);

	funcctx->tuple_desc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcontext);

	cv = RangeVarGetContView(cv_name);

	if (cv == NULL)
		elog(ERROR, "continuous view \"%s\" does not exist", cv_name->relname);

	if (RangeVarIsSWContView(cv_name))
		elog(ERROR, "the ttl of a sliding-window continuous view cannot be changed");

	if (ttl_colname)
	{
		matrel = heap_openrv(cv->matrel, NoLock);

		/*
		 * Find which attribute number corresponds to the given column name
		 */
		desc = RelationGetDescr(matrel);
		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);
			if (pg_strcasecmp(ttl_colname, NameStr(att->attname)) == 0)
			{
				ttl_attno = att->attnum;
				if (att->atttypid != TIMESTAMPOID && att->atttypid != TIMESTAMPTZOID)
					elog(ERROR, "ttl_column must refer to a timestamp or timestamptz column");
				break;
			}
		}
		heap_close(matrel, NoLock);

		if (!AttributeNumberIsValid(ttl_attno))
			elog(ERROR, "column \"%s\" does not exist", ttl_colname);
	}

	ttl = ttli ? IntervalToEpoch(ttli) : -1;

	initStringInfo(&buf);
	appendStringInfo(&buf, UPDATE_TTL_TEMPLATE, ttl, ttl_attno, cv->id);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI manager");

	if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute failed: %s", buf.data);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	values[0] = Int32GetDatum(ttl);

	if (!AttributeNumberIsValid(ttl_attno))
		nulls[1] = true;

	values[1] = Int16GetDatum(ttl_attno);

	MemSet(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tup);

	PipelineCatalogInvalidate(PIPELINEQUERYID);
	PipelineCatalogInvalidate(PIPELINEQUERYRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYMATRELID);
	PipelineCatalogInvalidate(PIPELINEQUERYOSRELID);

	SRF_RETURN_NEXT(funcctx, result);
}

/*
 * pipeline_ttl_expire
 */
PG_FUNCTION_INFO_V1(pipeline_ttl_expire);
Datum
pipeline_ttl_expire(PG_FUNCTION_ARGS)
{
	RangeVar *cv_name;
	RangeVar *matrel;
	int result;
	int save_batch_size = ttl_expiration_batch_size;

	if (PG_ARGISNULL(0))
		elog(ERROR, "continuous view name cannot be NULL");

	cv_name = makeRangeVarFromNameList(textToQualifiedNameList(PG_GETARG_TEXT_P(0)));
	matrel = RangeVarGetMatRelName(cv_name);

	if (!RangeVarIsTTLContView(cv_name))
		elog(ERROR, "continuous view \"%s\" does not have a TTL", cv_name->relname);

	/*
	 * DELETE everything
	 */
	ttl_expiration_batch_size = 0;
	result = DeleteTTLExpiredRows(cv_name, matrel);
	ttl_expiration_batch_size = save_batch_size;

	PG_RETURN_INT32(result);
}

/*
 * set_cq_enabled
 */
static bool
set_cq_enabled(RangeVar *name, bool activate)
{
	bool changed = false;
	Oid query_id;
	Relation pipeline_query;

	pipeline_query = OpenPipelineQuery(ExclusiveLock);
	query_id = RangeVarGetContQueryId(name);

	if (!OidIsValid(query_id))
		elog(ERROR, "\"%s\" does not exist", name->relname);

	changed = ContQuerySetActive(query_id, activate);
	if (changed)
		SyncPipelineStreamReaders();

	ClosePipelineQuery(pipeline_query, NoLock);

	return changed;
}

/*
 * pipeline_activate
 *
 * Activate the given continuous view/transform
 */
PG_FUNCTION_INFO_V1(pipeline_activate);
Datum
pipeline_activate(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	bool result = set_cq_enabled(rv, true);

	PG_RETURN_BOOL(result);
}

/*
 * pipeline_deactivate
 *
 * Deactivate the given continuous view/transform
 */
PG_FUNCTION_INFO_V1(pipeline_deactivate);
Datum
pipeline_deactivate(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	bool result = set_cq_enabled(rv, false);

	PG_RETURN_BOOL(result);
}

/*
 * pipeline_flush
 */
PG_FUNCTION_INFO_V1(pipeline_flush);
Datum
pipeline_flush(PG_FUNCTION_ARGS)
{
	int i;
	ContQueryDatabaseMetadata *db_meta = GetMyContQueryDatabaseMetadata();
	uint64 start_generation = pg_atomic_read_u64(&db_meta->generation);
	microbatch_ack_t *ack = microbatch_ack_new(STREAM_INSERT_FLUSH);
	microbatch_t *mb = microbatch_new(FlushTuple, NULL, NULL);
	bool success;

	microbatch_ipc_init();

	microbatch_add_ack(mb, ack);

	for (i = 0; i < num_workers; i++)
		microbatch_send_to_worker(mb, i);

	microbatch_destroy(mb);

	microbatch_ack_increment_wtups(ack, num_workers);
	success = microbatch_ack_wait(ack, db_meta, start_generation);
	microbatch_ack_free(ack);

	PG_RETURN_BOOL(success);
}
