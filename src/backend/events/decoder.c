#include <stdio.h>
#include <string.h>
#include "postgres.h"

#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_type.h"

#include "nodes/nodes.h"
#include "nodes/makefuncs.h"



Query *decode()
{
	ResTarget *resData = makeNode(ResTarget);
	resData->name = "data";
	resData->location = -1;

	ResTarget *resValue = makeNode(ResTarget);
	resValue->name = "value";
	resValue->location = -1;

	List *fields = lcons(resData, NULL);
	fields = lcons(resValue, fields);

	ColumnRef *crefs = makeNode(ColumnRef);
	crefs->fields = fields;
	List *cols = lcons(crefs, NULL);

	A_Const *d = makeNode(A_Const);
	d->val.type = T_String;
	d->val.val.str = "data!";
	d->location = -1;

	A_Const *v = makeNode(A_Const);
	v->val.type = T_String;
	v->val.val.str = "value!";
	v->location = -1;

	List *values =lcons(d, NULL);
	values = lcons(v, values);

	InsertStmt *stmt = makeNode(InsertStmt);
	stmt->relation = makeRangeVar(NULL, "test", -1);
	stmt->cols = fields;

	ListCell *lc;
	ListCell *icols = list_head(stmt->cols);

	SelectStmt *select = makeNode(SelectStmt);
	select->valuesLists = lcons(values, NULL);
	stmt->selectStmt = select;

	Query *q = parse_analyze((Node *)stmt, "", NULL, 0);

	return q;
}

/*

	# Tuple tup = decode(decoder, raw)

	def decode(decoder, raw):
		conversions = get_schema(table)
		for each field, data in raw:
		  transform = conversions[field]
			tuple.add(field, transform(data))

		return tuple


 */
int
maidn(int argc, char *argv[])
{
	char *tuple = strdup("1,'derek'");
	char *token;

	token = strtok(tuple, ",");
	while (token)
	{
		printf("%s\n", token);
		token = strtok(NULL, ",");
	}
	// Split it
	// Transform each field to a Postgres type
	return 0;
}
