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
	ColumnRef *crefs;
	List *values = NULL;
	List *columns = NULL;
	int i = 0;
	char *tok = strtok(strdup(raw), ",");

	while (tok != NULL)
	{
		if (i >= streamrel->rd_att->natts)
		{
			/* Ignore any extra fields in the tuple */
			break;
		}
		colname = makeNode(ResTarget);
		colname->name = streamrel->rd_att->attrs[i++]->attname.data;
		columns = lcons(colname, columns);

		colvalue = makeNode(A_Const);
		colvalue->val.type = T_String;
		colvalue->val.val.str = strdup(tok);
		values = lcons(colvalue, values);
		tok = strtok(NULL, ",");
	}

	relation_close(streamrel, NoLock);

	crefs = makeNode(ColumnRef);
	crefs->fields = columns;

	stmt = makeNode(InsertStmt);
	stmt->relation = stream;
	stmt->cols = lcons(crefs, NULL);

	select = makeNode(SelectStmt);
	select->valuesLists = lcons(values, NULL);
	stmt->selectStmt = (Node *)select;

	q = parse_analyze((Node *)stmt, "", NULL, 0);

	return QueryRewrite(q);
}
