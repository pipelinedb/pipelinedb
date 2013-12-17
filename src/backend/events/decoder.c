#include <stdio.h>
#include <string.h>
#include "postgres.h"
#include "rewrite/rewriteHandler.h"
#include "events/decoder.h"
#include "parser/parser.h"


const char *COLUMNS[3] = {
		"data", "value", "number"
};

List *decode_event(char *stream, const char *raw)
{
	char *tok = strtok(strdup(raw), ",");
	List *values = NULL;
	List *columns = NULL;
	int i = 0;
	while (tok != NULL)
	{
		ResTarget *col = makeNode(ResTarget);
		col->name = COLUMNS[i++];
		columns = lcons(col, columns);

		A_Const *v = makeNode(A_Const);
		v->val.type = T_String;
		v->val.val.str = strdup(tok);
		values = lcons(v, values);
		tok = strtok(NULL, ",");
	}

	ColumnRef *crefs;

	crefs = makeNode(ColumnRef);
	crefs->fields = columns;
	List *cols = lcons(crefs, NULL);

	InsertStmt *stmt = makeNode(InsertStmt);
	stmt->relation = makeRangeVar(NULL, stream, -1);
	stmt->cols = columns;

	ListCell *lc;
	ListCell *icols = list_head(stmt->cols);

	SelectStmt *select = makeNode(SelectStmt);
	select->valuesLists = lcons(values, NULL);
	stmt->selectStmt = (Node *)select;

	Query *q = parse_analyze((Node *)stmt, "", NULL, 0);

	return QueryRewrite(q);
}
