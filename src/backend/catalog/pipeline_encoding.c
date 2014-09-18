/*-------------------------------------------------------------------------
 *
 * pipeline_encoding.c
 *	  routines to support manipulation of the pipeline_encoding relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_encoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_encoding.h"
#include "catalog/pipeline_encoding_fn.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CreateEncoding
 *
 * Creates an encoding that can be used to decode a raw event, and
 * adds it to the catalog
 */
void
CreateEncoding(CreateEncodingStmt *stmt)
{
	Relation	pipeline_encoding;
	HeapTuple	tup;
	bool nulls[Natts_pipeline_encoding];
	Datum values[Natts_pipeline_encoding];
	NameData name;
	NameData decodedby;
	ListCell *lc;
	Datum *argnames;
	Datum *argvalues;
	bool collides = true;
	Oid oid = InvalidOid;
	TupleDesc desc;
	CatalogIndexState indstate;
	Relation pgattr;
	int i;

	if (!stmt->name || !stmt->name->relname)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("encoding name is null")));

	for (i=0; i<Natts_pipeline_encoding; i++)
	{
		nulls[i] = true;
		values[i] = (Datum) NULL;
	}

	namestrcpy(&name, stmt->name->relname);
	values[Anum_pipeline_encoding_name - 1] = NameGetDatum(&name);
	nulls[Anum_pipeline_encoding_name - 1] = false;

	while (collides)
	{
		HeapTuple existing;
		oid = GetNewObjectId();
		existing = SearchSysCache1(PIPELINEENCODINGOID, ObjectIdGetDatum(oid));
		if (!HeapTupleIsValid(existing))
			collides = false;
	}

	/*
	 * We need to add the attributes to pg_attribute
	 */
	desc = BuildDescForRelation(stmt->coldefs);
	pgattr = heap_open(AttributeRelationId, RowExclusiveLock);
	indstate = CatalogOpenIndexes(pgattr);

	for (i=0; i<desc->natts; i++)
	{
		/* XXX: this is kind of a hack, as an encoding isn't really a relation, but it probably should be */
		desc->attrs[i]->attrelid = oid;

		InsertPgAttributeTuple(pgattr, desc->attrs[i], indstate);
	}

	CatalogCloseIndexes(indstate);
	heap_close(pgattr, RowExclusiveLock);

	values[Anum_pipeline_encoding_oid - 1] = oid;
	nulls[Anum_pipeline_encoding_oid - 1] = false;

	/*
	 * Here we save two parallel arrays of argument names and values
	 * that will be passed to the decode function on each invocation
	 */
	if (stmt->args)
	{
		int numargs = list_length(stmt->args);
		ArrayType *namearr;
		ArrayType *valarr;

		argnames = palloc(numargs * sizeof(Datum));
		argvalues = palloc(numargs * sizeof(Datum));
		i = 0;
		foreach(lc, stmt->args)
		{
			DefElem    *defel = (DefElem *) lfirst(lc);
			argnames[i] = CStringGetTextDatum(defel->defname);
			argvalues[i] = CStringGetTextDatum(nodeToString(defel->arg));
			i++;
		}

		/* {arg,names,go,here} */
		namearr = construct_array(argnames, numargs, TEXTOID, -1, false, 'i');
		values[Anum_pipeline_encoding_decodedbyargnames - 1] = PointerGetDatum(namearr);
		nulls[Anum_pipeline_encoding_decodedbyargnames - 1] = false;

		/* {"arg","values","go","here"} */
		valarr = construct_array(argvalues, numargs, TEXTOID, -1, false, 'i');
		values[Anum_pipeline_encoding_decodedbyargvalues - 1] = PointerGetDatum(valarr);
		nulls[Anum_pipeline_encoding_decodedbyargvalues - 1] = false;
	}

	/*
	 * Verify that a function with the given name actually exists. We don't match
	 * against argument types here because they aren't known until runtime, so this
	 * is just an initial sanity check.
	 */
	if (stmt->decodedby)
	{
		char *procname = stmt->decodedby->relname;
		List *names = list_make1(makeString(procname));
		FuncCandidateList clist = FuncnameGetCandidates(names, -1, NIL, false, false, false);
		if (clist == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("no function named \"%s\"", procname)));
		}

		namestrcpy(&decodedby, procname);
		values[Anum_pipeline_encoding_decodedby - 1] = NameGetDatum(&decodedby);
		nulls[Anum_pipeline_encoding_decodedby - 1] = false;
	}

	/* now actually add it to the catalog and update the index */
	pipeline_encoding = heap_open(PipelineEncodingRelationId, RowExclusiveLock);
	tup = heap_form_tuple(pipeline_encoding->rd_att, values, nulls);

	simple_heap_insert(pipeline_encoding, tup);
	CatalogUpdateIndexes(pipeline_encoding, tup);

	heap_freetuple(tup);
	heap_close(pipeline_encoding, RowExclusiveLock);
}
