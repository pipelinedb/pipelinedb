/*-----------------------------------------------------------------------------

 * decoder.h
 *		Interface for decoding raw event data
 *
 * This package implements decoding functionality for raw events
 *
 *
 * src/events/decode.h
 *-----------------------------------------------------------------------------
*/
#include "access/htup.h"
#include "events/stream.h"
#include "funcapi.h"
#include "utils/rel.h"


typedef struct StreamEventDecoder
{
	char *name;
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
