#include <stdio.h>
#include <string.h>

#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_encoding.h"
#include "events/decode.h"
#include "funcapi.h"
#include "parser/parse_node.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static StreamEventDecoder *
check_decoder_cache(const char *encoding)
{
	return NULL;
}

static void
cache_decoder(const char *encoding, StreamEventDecoder *decoder)
{

}

/*
 * InitDecoderCache
 *
 * Creates the hashtable that is used as the encoding-to-decoder cache
 */
void
InitDecoderCache(void)
{

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
	StreamEventDecoder *decoder = check_decoder_cache(encoding);
	bool isNull;
	Datum *textargnames;
	Datum *argvals;
	Value *decodedbyname;
	FuncCandidateList clist;
	FunctionCallInfoData fcinfo;
	List *argnames = NIL;
	Datum *typedargs;
	ParseState *ps;
	int nargnames;
	int nargvals;
	int i;

	if (false && decoder != NULL)
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

	cache_decoder(encoding, decoder);

	ReleaseSysCache(tup);

	return NULL;
}

/*
 * DecodeStreamEvent
 *
 * Decodes an event into a physical tuple
 */
HeapTuple
DecodeStreamEvent(StreamEvent event, StreamEventDecoder *decoder)
{
	return NULL;
}

void decode_event(Relation stream, const char *raw, HeapTuple *tuple)
{
	TupleDesc	streamdesc = RelationGetDescr(stream);
	AttInMetadata *meta = TupleDescGetAttInMetadata(streamdesc);

	int i = 0;
	char *tok;
	char *str = pstrdup(raw);
	char *values[streamdesc->natts];

	while ((tok = strsep(&str, ",")) != NULL &&
			i < stream->rd_att->natts)
	{
		/* Consider empty fields NULL */
		values[i++] = strlen(tok) > 0 ? strdup(tok) : NULL;
	}
	free(str);

	*tuple = BuildTupleFromCStrings(meta, values);
}
