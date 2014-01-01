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


void decode_event(Relation stream, const char *raw, HeapTuple *tuple)
{
	TupleDesc	streamdesc = RelationGetDescr(stream);

	Datum record[streamdesc->natts];
	bool nulls[streamdesc->natts];

	int i = 0;
	char *tok;
	char *str = strdup(raw);

	MemSet(record, 0, sizeof(record));
	MemSet(nulls, false, sizeof(nulls));

	while ((tok = strsep(&str, ",")) != NULL &&
			i < stream->rd_att->natts)
	{
		/* Ignore empty fields */
		if (strlen(tok) > 0)
		{
			record[i] = CStringGetTextDatum(strdup(tok));
		} else {
			record[i] = Int32GetDatum(42);
		}
		i++;
	}
	free(str);

	*tuple = heap_form_tuple(streamdesc, record, nulls);
}
