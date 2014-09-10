/*-----------------------------------------------------------------------------

 * decoder.h
 *		Interface for decoding raw event data
 *
 * This package implements decoding functionality for raw events
 *
 *
 * src/pipeline/decode.h
 *-----------------------------------------------------------------------------
*/
#include "access/htup.h"
#include "funcapi.h"
#include "pipeline/stream.h"
#include "utils/rel.h"

#define VALUES_ENCODING "__VALUES__"

typedef struct StreamEventDecoder
{
	char *name;
	int *fieldstoattrs;
	int rawpos;
	FunctionCallInfoData fcinfo_data;
	Oid rettype;
	MemoryContext tmp_ctxt;
	AttInMetadata *meta;
} StreamEventDecoder;


extern StreamEventDecoder *GetStreamEventDecoder(const char *channel);
extern void InitDecoderCache(void);
extern HeapTuple ProjectStreamEvent(StreamEvent event, TupleDesc desc);
extern HeapTuple DecodeStreamEvent(StreamEvent event, StreamEventDecoder *decoder, TupleDesc desc);
