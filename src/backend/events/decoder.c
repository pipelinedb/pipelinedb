#include <stdio.h>
#include <string.h>
#include "postgres.h"
#include "rewrite/rewriteHandler.h"
#include "events/decoder.h"
#include "parser/parser.h"
#include "utils/rel.h"
#include "storage/lock.h"
#include "access/heapam.h"


List *decode_event(RangeVar *stream, const char *raw)
{
	Relation streamrel = heap_openrv(stream, NoLock);
	InsertStmt *stmt;
	SelectStmt *select;
	Query *q;

	ResTarget *colname;
	A_Const *colvalue;
	List *values = NULL;
	List *columns = NULL;
	int i = 0;
	char *tok;
	char *str = strdup(raw);

	while ((tok = strsep(&str, ",")) != NULL &&
			i < streamrel->rd_att->natts)
	{
		/* Ignore empty fields */
		if (strlen(tok) > 0)
		{
			colname = makeNode(ResTarget);
			colname->name = streamrel->rd_att->attrs[i]->attname.data;
			columns = lcons(colname, columns);

			colvalue = makeNode(A_Const);
			colvalue->val.type = T_String;
			colvalue->val.val.str = strdup(tok);
			values = lcons(colvalue, values);
		}
		i++;
	}
	free(str);

	relation_close(streamrel, NoLock);

	stmt = makeNode(InsertStmt);
	stmt->relation = stream;
	stmt->cols = columns;

	select = makeNode(SelectStmt);
	select->valuesLists = lcons(values, NULL);
	stmt->selectStmt = (Node *)select;

	q = parse_analyze((Node *)stmt, "", NULL, 0);

	return QueryRewrite(q);
}
