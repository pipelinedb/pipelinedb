#include <stdio.h>
#include <string.h>
#include "postgres.h"
#include "rewrite/rewriteHandler.h"
#include "events/decoder.h"
#include "parser/parser.h"
#include "utils/rel.h"
#include "storage/lock.h"
#include "access/heapam.h"
#include "utils/builtins.h"
#include "funcapi.h"


void decode_event(Relation stream, const char *raw, HeapTuple *tuple)
{
	TupleDesc	streamdesc = RelationGetDescr(stream);
	AttInMetadata *meta = TupleDescGetAttInMetadata(streamdesc);

	int i = 0;
	char *tok;
	char *str = strdup(raw);
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
