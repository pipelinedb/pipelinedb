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
#include "utils/rel.h"


typedef struct StreamEventDecoder
{
	char *name;
	int rawpos;
	FunctionCallInfoData fcinfo_data;
	TupleDesc schema;
	Oid rettype;
} StreamEventDecoder;


extern StreamEventDecoder *GetStreamEventDecoder(const char *channel);
extern void InitDecoderCache(void);
extern HeapTuple DecodeStreamEvent(StreamEvent event, StreamEventDecoder *decoder);
