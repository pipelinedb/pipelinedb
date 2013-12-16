#include <stdio.h>
#include <string.h>
#include "postgres.h"
#include "rewrite/rewriteHandler.h"
#include "events/decoder.h"

/*

	# Tuple tup = decode(decoder, raw)

	def decode(decoder, raw):
		conversions = get_schema(table)
		for each field, data in raw:
		  transform = conversions[field]
			tuple.add(field, transform(data))

		return tuple

 */
List *decode_event(const char *raw)
{
	ResTarget *resId = makeNode(ResTarget);
	resId->name = "id";
	resId->location = -1;

	ResTarget *resData = makeNode(ResTarget);
	resData->name = "data";
	resData->location = -1;

	ResTarget *resValue = makeNode(ResTarget);
	resValue->name = "value";
	resValue->location = -1;

	List *fields = NULL;//lcons(resId, NULL);
	fields = lcons(resData, fields);
	fields = lcons(resValue, fields);

	ColumnRef *crefs = makeNode(ColumnRef);
	crefs->fields = fields;
	List *cols = lcons(crefs, NULL);

	A_Const *i = makeNode(A_Const);
	i->val.type = T_Integer;
	i->val.val.ival = 12321313;
	i->location = -1;

	A_Const *d = makeNode(A_Const);
	d->val.type = T_String;
	d->val.val.str = "data!";
	d->location = -1;

	A_Const *v = makeNode(A_Const);
	v->val.type = T_String;
	v->val.val.str = "value!";
	v->location = -1;

	List *values = NULL;//lcons(i, NULL);
	values = lcons(d, values);
	values = lcons(v, values);

	InsertStmt *stmt = makeNode(InsertStmt);
	stmt->relation = makeRangeVar(NULL, "test", -1);
	stmt->cols = fields;

	ListCell *lc;
	ListCell *icols = list_head(stmt->cols);

	SelectStmt *select = makeNode(SelectStmt);
	select->valuesLists = lcons(values, NULL);
	stmt->selectStmt = (Node *)select;

	Query *q = parse_analyze((Node *)stmt, "", NULL, 0);

	return QueryRewrite(q);
}
