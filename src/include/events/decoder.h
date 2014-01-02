/*-----------------------------------------------------------------------------

 * decoder.h
 *		Interface for decoding raw event data
 *
 * This package implements decoding functionality for raw events
 *
 *
 * src/events/decoder.h
 *-----------------------------------------------------------------------------
*/
#include "access/htup.h"
#include "utils/rel.h"


void decode_event(Relation stream, const char *raw, HeapTuple *tuple);
