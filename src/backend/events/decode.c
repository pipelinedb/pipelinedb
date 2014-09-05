#include <stdio.h>
#include <string.h>

#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_encoding.h"
#include "events/decode.h"
#include "funcapi.h"
#include "parser/parse_node.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* Cache for initialized decoders */
static HTAB *DecoderCache = NULL;

typedef struct DecoderCacheEntry
{
	char key[NAMEDATALEN]; /* hash key --- MUST BE FIRST */
	StreamEventDecoder *decoder;
} DecoderCacheEntry;


static StreamEventDecoder *
check_decoder_cache(const char *encoding)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	DecoderCacheEntry *entry =
			(DecoderCacheEntry *) hash_search(DecoderCache, (void *) encoding, HASH_FIND, NULL);

	MemoryContextSwitchTo(oldcontext);

	if (entry == NULL)
		return NULL;

	return entry->decoder;
}

static void
cache_decoder(const char *encoding, StreamEventDecoder *decoder)
{
	DecoderCacheEntry *entry =
			(DecoderCacheEntry *) hash_search(DecoderCache, (void *) encoding, HASH_ENTER, NULL);
	entry->decoder = decoder;
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
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));

	/* Keyed by the name field of the pipeline_encoding catalog table */
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(DecoderCacheEntry);

	/* XXX TODO: we need to listen for invalidation events via CacheRegisterSyscacheCallback */
	DecoderCache = hash_create("DecoderCache", 32, &ctl, HASH_ELEM);
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
	HeapTuple	proctup;
	Form_pg_proc procform;
	MemoryContext oldcontext;
	int nargnames;
	int nargvals = 0;
	int i;

	if (decoder != NULL)
		return decoder;

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

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

	if (nargvals > 0)
	{
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
	}

	decodedbyname = makeString(NameStr(row->decodedby));
	clist = FuncnameGetCandidates(list_make1(decodedbyname),
			list_length(argnames) + 1, argnames, false, false);

	if (clist->next)
	{
		/* XXX: we need to actually find the right function based on arguments here */
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						errmsg("multiple decoder functions named \"%s\"", TextDatumGetCString(&row->decodedby))));
	}

	/* figure out the return type to to use when decoding events */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	decoder = palloc(sizeof(StreamEventDecoder));

	/*
	 *  XXX TODO: it may not be the best idea to assume that the raw event is the first argument,
	 *  although we do need to be able to rely on some assumptions to avoid ambiguity
	 */
	decoder->rawpos = 0;
	decoder->meta = NULL;
	decoder->name = pstrdup(encoding);
	decoder->rettype = procform->prorettype;
	decoder->tmp_ctxt = AllocSetContextCreate(CurrentMemoryContext,
													 "DecoderContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	decoder->fcinfo_data.flinfo = palloc(sizeof(fcinfo.flinfo));
	decoder->fcinfo_data.flinfo->fn_mcxt = AllocSetContextCreate(CurrentMemoryContext,
													 decoder->name,
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	decoder->fcinfo_data.nargs = list_length(argnames) + 1;
	fmgr_info(clist->oid, decoder->fcinfo_data.flinfo);

	/* assign the arguments that will be passed on every call */
	for (i=1; i<decoder->fcinfo_data.nargs; i++)
	{
		if (i == decoder->rawpos)
			continue;
		decoder->fcinfo_data.arg[clist->argnumbers[i]] = typedargs[i - 1];
	}

	cache_decoder(encoding, decoder);

	ReleaseSysCache(tup);
	ReleaseSysCache(proctup);

	MemoryContextSwitchTo(oldcontext);

	return decoder;
}

/*
 * ProjectStreamEvent
 *
 * Applies types to a decoded event's raw fields
 */
HeapTuple
ProjectStreamEvent(StreamEvent event, TupleDesc desc)
{
	return NULL;
}

/*
 * DecodeStreamEvent
 *
 * Decodes an event into a physical tuple
 */
HeapTuple
DecodeStreamEvent(StreamEvent event, StreamEventDecoder *decoder, TupleDesc desc)
{
	Datum result;
	Datum *fields;
	Datum rawarg;
	HeapTuple decoded;
	MemoryContext oldcontext;
	int nfields;
	char *evbytes;
	char **strs;
	int i;

	if (decoder->meta == NULL)
	{
		oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
		decoder->meta = TupleDescGetAttInMetadata(desc);
		MemoryContextSwitchTo(oldcontext);
	}

	oldcontext = MemoryContextSwitchTo(decoder->tmp_ctxt);
	evbytes = pnstrdup(event->raw, event->len);

	switch(decoder->rettype)
	{
		case JSONOID:
			rawarg = CStringGetDatum(evbytes);
			break;
		default:
			/* we can treat the raw bytes as text because texts are identical in structure to a byteas */
			rawarg = CStringGetTextDatum(evbytes);
	}

	decoder->fcinfo_data.arg[decoder->rawpos] = rawarg;
	InitFunctionCallInfoData(decoder->fcinfo_data, decoder->fcinfo_data.flinfo,
			decoder->fcinfo_data.nargs, InvalidOid, NULL, NULL);
	result = FunctionCallInvoke(&decoder->fcinfo_data);

	switch(decoder->rettype)
	{
		case TEXTARRAYOID:
			deconstruct_array(DatumGetArrayTypeP(result), TEXTOID, -1,
					false, 'i', &fields, NULL, &nfields);
			break;
		case JSONOID:
			{
				/* if we got this far, then the input bytes are valid JSON so we can use them directly here */
				int i;

				nfields = desc->natts;
				fields = palloc(nfields * sizeof(Datum));

				for (i=0; i<desc->natts; i++)
				{
					/* XXX TODO: handle nonexistent fields, json_object_field throws an error for these */
					char *rawstr;
					const char *key = NameStr(desc->attrs[i]->attname);
					Datum raw = DirectFunctionCall2(json_object_field, CStringGetTextDatum(evbytes),
							CStringGetTextDatum(key));

					rawstr = TextDatumGetCString(raw);

					if (rawstr[0] == '"')
					{
						/* trim off enclosing quotes for JSON strings */
						char trimmed[strlen(rawstr) - 1];

						memcpy(trimmed, &rawstr[1], strlen(rawstr) - 2);
						trimmed[strlen(rawstr) - 2] = '\0';
						raw = CStringGetTextDatum(rawstr);
					}

					fields[i] = raw;
				}
			}
			break;
//		case RECORDOID:
//			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
							errmsg("decode for encoding \"%s\" returned unknown type: %d",
									decoder->name, decoder->rettype)));
	}

	strs = palloc(nfields * sizeof(char *));
	for (i=0; i<nfields; i++)
	{
		strs[i] = TextDatumGetCString(fields[i]);
	}

	MemoryContextSwitchTo(oldcontext);

	decoded = BuildTupleFromCStrings(decoder->meta, strs);

	MemoryContextReset(decoder->tmp_ctxt);

	return decoded;
}
