#include <stdio.h>
#include <string.h>

#include "postgres.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_encoding.h"
#include "events/decode.h"
#include "funcapi.h"
#include "parser/parse_node.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Cache for initialized decoders */
static HTAB *DecoderCache = NULL;

typedef struct DecoderCacheEntry
{
	StreamEventDecoder *decoder;
} DecoderCacheEntry;


static StreamEventDecoder *
check_decoder_cache(const char *encoding)
{
	DecoderCacheEntry *entry =
			(DecoderCacheEntry *) hash_search(DecoderCache, (void *) encoding, HASH_FIND, NULL);

	if (entry == NULL)
		return NULL;

	return entry->decoder;
}

static void
cache_decoder(const char *encoding, StreamEventDecoder *decoder)
{
	hash_search(DecoderCache, (void *) encoding, HASH_ENTER, NULL);
}

/*
 * get_schema
 *
 * Given an encoding OID, retrieve the corresponding attributes from pg_attribute
 */
static TupleDesc
get_schema(Oid encoding)
{
	HeapTuple	pg_attribute_tuple;
	Relation	pg_attribute_desc;
	SysScanDesc pg_attribute_scan;
	ScanKeyData skey[2];
	TupleDesc schema;
	List *lattrs = NIL;
	Form_pg_attribute *attrs;
	ListCell *lc;
	int i = 0;

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(encoding));

	ScanKeyInit(&skey[1],
				Anum_pg_attribute_attnum,
				BTGreaterStrategyNumber, F_INT2GT,
				Int16GetDatum(0));

	pg_attribute_desc = heap_open(AttributeRelationId, AccessShareLock);
	pg_attribute_scan = systable_beginscan(pg_attribute_desc,
										   AttributeRelidNumIndexId,
										   criticalRelcachesBuilt,
										   SnapshotNow,
										   2, skey);

	while (HeapTupleIsValid(pg_attribute_tuple = systable_getnext(pg_attribute_scan)))
	{
		Form_pg_attribute attp;
		attp = (Form_pg_attribute) GETSTRUCT(pg_attribute_tuple);
		lattrs = lappend(lattrs, attp);
	}

	systable_endscan(pg_attribute_scan);
	heap_close(pg_attribute_desc, AccessShareLock);

	attrs = palloc(list_length(lattrs) * sizeof(Form_pg_attribute));
	foreach(lc, lattrs)
	{
		attrs[i++] = (Form_pg_attribute) lfirst(lc);
	}

	schema = CreateTupleDesc(list_length(lattrs), false, attrs);

	pfree(lattrs);

	return schema;
}

/*
 * InitDecoderCache
 *
 * Creates the hashtable that is used as the encoding-to-decoder cache
 */
void
InitDecoderCache(void)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));

	/* Keyed by the name field of the pipeline_encoding catalog table */
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(DecoderCacheEntry);
	ctl.hash = string_hash;

	DecoderCache = hash_create("DecoderCache", 32, &ctl, HASH_ELEM);

	/* XXX TODO: we need to listen for invalidation events via CacheRegisterSyscacheCallback */
	MemoryContextSwitchTo(oldcontext);
}

/*
 * GetStreamEventDecoder
 *
 * Given an encoding, retreives the appropriate decoder
 */
StreamEventDecoder *
GetStreamEventDecoder(const char *encoding)
{
	NameData name;
	HeapTuple tup;
	Form_pipeline_encoding row;
	Datum rawarr;
	bool isNull;
	Datum *textargnames;
	Datum *argvals;
	Value *decodedbyname;
	FuncCandidateList clist;
	FunctionCallInfoData fcinfo;
	List *argnames = NIL;
	Datum *typedargs;
	ParseState *ps;
	StreamEventDecoder *decoder = check_decoder_cache(encoding);
	int nargnames;
	int nargvals;
	int i;

	if (decoder != NULL)
		return decoder;

	namestrcpy(&name, encoding);
	tup = SearchSysCache1(PIPELINEENCODINGNAME, NameGetDatum(&name));
	if (!HeapTupleIsValid(tup))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no decoder found for encoding \"%s\"", encoding)));
	}

	row = (Form_pipeline_encoding) GETSTRUCT(tup);
	rawarr = SysCacheGetAttr(PIPELINEENCODINGNAME, tup,
			Anum_pipeline_encoding_decodedbyargnames, &isNull);

	if (!isNull)
		deconstruct_array(DatumGetArrayTypeP(rawarr), TEXTOID, -1,
				false, 'i', &textargnames, NULL, &nargnames);

	rawarr = SysCacheGetAttr(PIPELINEENCODINGNAME, tup,
			Anum_pipeline_encoding_decodedbyargvalues, &isNull);

	if (!isNull)
		deconstruct_array(DatumGetArrayTypeP(rawarr), TEXTOID, -1,
				false, 'i', &argvals, NULL, &nargvals);

	/* we need to coerce the arguments into the specific types that the function expects */
	typedargs = palloc(nargvals * sizeof(Datum));
	ps = make_parsestate(NULL);
	for (i=0; i<nargvals; i++)
	{
		Value *v = (Value *) stringToNode(TextDatumGetCString(argvals[i]));
		Const *c = make_const(ps, v, 0);
		c = (Const *) coerce_to_common_type(ps, (Node *) c, TEXTOID, "decoder argument");
		typedargs[i] = c->constvalue;
	}

	for (i=0; i<nargnames; i++)
	{
		argnames = lappend(argnames, TextDatumGetCString(textargnames[i]));
	}

	decodedbyname = makeString(row->decodedby.data);
	clist = FuncnameGetCandidates(list_make1(decodedbyname),
			list_length(argnames) + 1, argnames, false, false);

	if (clist->next)
	{
		/* XXX: we need to actually find the right function based on arguments here */
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						errmsg("multiple decoder functions named \"%s\"", TextDatumGetCString(&row->decodedby))));
	}

	decoder = palloc(sizeof(StreamEventDecoder));
	/*
	 *  XXX TODO: it may not be the best idea to assume that the raw event is the first argument,
	 *  although we do need to be able to rely on some assumptions to avoid ambiguity
	 */
	decoder->schema = get_schema(row->oid);
	decoder->rawpos = 0;
	decoder->fcinfo_data.flinfo = palloc(sizeof(fcinfo.flinfo));
	decoder->fcinfo_data.flinfo->fn_mcxt = MemoryContextAllocZero(CurrentMemoryContext, ALLOCSET_SMALL_MAXSIZE);
	decoder->fcinfo_data.nargs = list_length(argnames) + 1;
	fmgr_info(clist->oid, decoder->fcinfo_data.flinfo);

	/* assign the arguments that will be passed on every call */
	for (i=1; i<decoder->fcinfo_data.nargs; i++)
	{
		if (i == decoder->rawpos)
			continue;
		/* XXX we're assuming that the raw arg is always 0--clean up positioning logic */
		decoder->fcinfo_data.arg[clist->argnumbers[i]] = typedargs[i - 1];
	}

	cache_decoder(encoding, decoder);

	ReleaseSysCache(tup);

	pfree(textargnames);
	pfree(argvals);
	pfree(decodedbyname);
	pfree(argnames);
	pfree(typedargs);
	pfree(ps);

	return decoder;
}

/*
 * DecodeStreamEvent
 *
 * Decodes an event into a physical tuple
 */
HeapTuple
DecodeStreamEvent(StreamEvent event, StreamEventDecoder *decoder)
{
	Datum result;
	Datum *fields;
	bool *isnull;
	HeapTuple decoded;
	int nfields;

	/* we can treat the raw bytes as text because texts are identical in structure to a byteas */
	decoder->fcinfo_data.arg[decoder->rawpos] = CStringGetTextDatum(event->raw);

	InitFunctionCallInfoData(decoder->fcinfo_data, decoder->fcinfo_data.flinfo,
			decoder->fcinfo_data.nargs, InvalidOid, NULL, NULL);

	result = FunctionCallInvoke(&decoder->fcinfo_data);

	deconstruct_array(DatumGetArrayTypeP(result), TEXTOID, -1,
			false, 'i', &fields, NULL, &nfields);

	isnull = palloc0(nfields * sizeof(bool));
	decoded = heap_form_tuple(decoder->schema, fields, isnull);

	return decoded;
}
