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
#include "postgres.h"

#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_type.h"

#include "nodes/nodes.h"
#include "nodes/makefuncs.h"

void decode_event(Relation stream, const char *raw, HeapTuple *tuple);
