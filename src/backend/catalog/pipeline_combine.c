/*-------------------------------------------------------------------------
 *
 * pipeline_combine.c
 *	  routines to support manipulation of the pipeline_combine relation
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_combine.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_combine_fn.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * lookup_single_func
 *
 * Lookup a function, and throw an error if there is not precisely one result
 */
static HeapTuple
lookup_single_func(List *name)
{
	/* Get list of possible candidates from namespace search */
	FuncCandidateList clist = FuncnameGetCandidates(name, -1, NIL, false, false, false);
	HeapTuple tup;

	/* there shouldn't be any ambiguity in resolving a combine function */
	if (!clist)
		elog(ERROR, "no function named %s found", NameListToString(name));
	else if (clist->next)
		elog(ERROR, "found multiple functions with name %s", NameListToString(name));

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for function %u", clist->oid);

	return tup;
}

static Oid
get_single_func_oid(List *name)
{
	HeapTuple t = lookup_single_func(name);
	Oid result;

	Assert(HeapTupleIsValid(t));

	result = HeapTupleGetOid(t);
	ReleaseSysCache(t);

	return result;
}

/*
 * GetCombineInfo
 *
 * Get all the pipeline_combine information associated with the given aggregate function
 */
void
GetCombineInfo(Oid aggfnoid, Oid *combinefn, Oid *transoutfn, Oid *combineinfn, Oid *statetype)
{
	HeapTuple	aggtup;
	HeapTuple combtup;
	Form_pg_aggregate aggform;
	Form_pipeline_combine combform;

	*combinefn = InvalidOid;
	*combineinfn = InvalidOid;
	*statetype = InvalidOid;

	aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	if (!HeapTupleIsValid(aggtup))
		return;

	aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);
	ReleaseSysCache(aggtup);

	combtup = SearchSysCache2(PIPELINECOMBINETRANSFNOID,
			ObjectIdGetDatum(aggform->aggfinalfn), ObjectIdGetDatum(aggform->aggtransfn));

	if (!HeapTupleIsValid(combtup))
		return;

	combform = (Form_pipeline_combine) GETSTRUCT(combtup);

	*combinefn = combform->combinefn;
	*transoutfn = combform->transoutfn;
	*combineinfn = combform->combineinfn;

	/* we only have a state type if we're actually storing state in tables */
	if (combform->aggfinalfn)
		*statetype = combform->transouttype;

	ReleaseSysCache(combtup);
}

/*
 * GetCombineStateType
 *
 * Given an aggregate function oid, determine the type of its transition state
 */
Oid
GetCombineStateType(Oid aggfnoid)
{
	Oid combinefn;
	Oid transoutfn;
	Oid combineinfn;
	Oid result;

	GetCombineInfo(aggfnoid, &combinefn, &transoutfn, &combineinfn, &result);

	return result;
}

/*
 * DefineCombiner
 *
 * Define a combiner that can be used to combine aggregate transition states for continuous views
 */
Oid
DefineCombiner(Oid aggoid, List *name, List *args, bool oldstyle, List *parameters)
{
	char *aggname;
	Oid namespace;
	AclResult	aclresult;
	ListCell *pl;
	List *combinename = NIL;
	List *transoutname = NIL;
	List *combineinname = NIL;
	HeapTuple	aggtup;
	HeapTuple combinetup;
	Form_pg_aggregate agg;
	Relation pipeline_combine;
	bool nulls[Natts_pipeline_combine];
	Datum values[Natts_pipeline_combine];
	Oid finalfn = InvalidOid;
	Oid transfn = InvalidOid;
	Oid transoutfn = InvalidOid;
	Oid combineinfn = InvalidOid;
	Oid combinefn = InvalidOid;
	Oid transouttype = InvalidOid;
	Oid result = InvalidOid;
	ObjectAddress referenced;
	ObjectAddress agg_objaddress;
	Oid combinetup_oid = InvalidOid;

	/* Convert list of names to a name and namespace */
	namespace = QualifiedNameGetCreationNamespace(name, &aggname);

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(namespace, GetUserId(), ACL_CREATE);
	if (namespace != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespace));

	foreach(pl, parameters)
	{
		DefElem *defel = (DefElem *) lfirst(pl);
		if (pg_strcasecmp(defel->defname, "combinefunc") == 0)
			combinename = defGetQualifiedName(defel);
		if (pg_strcasecmp(defel->defname, "combineinfunc") == 0)
			combineinname = defGetQualifiedName(defel);
		if (pg_strcasecmp(defel->defname, "transoutfunc") == 0)
			transoutname = defGetQualifiedName(defel);
	}

	CommandCounterIncrement();

	aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggoid));
	if (!HeapTupleIsValid(aggtup))
		elog(ERROR, "cache lookup failed for aggregate %u", aggoid);

	agg = (Form_pg_aggregate) GETSTRUCT(aggtup);

	/*
	 * If the trans type is internal, input and output functions are
	 * required to de/serialize the transition state to and from a type
	 * that can be stored in a table.
	 */
	if (agg->aggtranstype == INTERNALOID &&
			transoutname == NIL && combineinname == NIL)
		elog(ERROR, "combineinfunc and transoutfunc are required for internal transition type");

	finalfn = agg->aggfinalfn;
	transfn = agg->aggtransfn;
	transouttype = agg->aggtranstype;
	ReleaseSysCache(aggtup);

	combinefn = get_single_func_oid(combinename);

	if (combineinname)
		combineinfn = get_single_func_oid(combineinname);

	if (transoutname)
	{
		HeapTuple t = lookup_single_func(transoutname);
		Form_pg_proc transout = (Form_pg_proc) GETSTRUCT(t);

		if (ArrayGetNItems(ARR_NDIM(&transout->proargtypes), ARR_DIMS(&transout->proargtypes)) != 1)
			elog(ERROR, "transoutfunc must take exactly one argument");

		transouttype = transout->prorettype;
		transoutfn = HeapTupleGetOid(t);
		ReleaseSysCache(t);

		/*
		 * The transout function's input type must be the same
		 * as the transition function's return type.
		 */
		if (transout->proargtypes.values[0] != agg->aggtranstype)
			elog(ERROR, "transoutfunc %s must have input type %u", NameListToString(transoutname), agg->aggtranstype);
	}

	Assert(transouttype != INTERNALOID);

	combinetup = SearchSysCache2(PIPELINECOMBINETRANSFNOID,
			ObjectIdGetDatum(agg->aggfinalfn), ObjectIdGetDatum(agg->aggtransfn));

	if (HeapTupleIsValid(combinetup))
	{
		elog(ERROR, "combine function %u already exists for aggregate with final function %u and transition function %u",
				((Form_pipeline_combine) combinetup)->combinefn, agg->aggfinalfn, agg->aggtransfn);
	}

	/* we now have the information we need to create an entry in pipeline_combine */
	pipeline_combine = heap_open(PipelineCombineRelationId, RowExclusiveLock);

	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pipeline_combine_aggfinalfn - 1] = ObjectIdGetDatum(finalfn);
	values[Anum_pipeline_combine_transfn - 1] = ObjectIdGetDatum(transfn);
	values[Anum_pipeline_combine_transoutfn - 1] = ObjectIdGetDatum(transoutfn);
	values[Anum_pipeline_combine_combineinfn - 1] = ObjectIdGetDatum(combineinfn);
	values[Anum_pipeline_combine_combinefn - 1] = ObjectIdGetDatum(combinefn);
	values[Anum_pipeline_combine_transouttype - 1] = ObjectIdGetDatum(transouttype);

	combinetup = heap_form_tuple(pipeline_combine->rd_att, values, nulls);
	result = simple_heap_insert(pipeline_combine, combinetup);
	CatalogUpdateIndexes(pipeline_combine, combinetup);

	combinetup_oid = HeapTupleGetOid(combinetup);

	heap_freetuple(combinetup);
	heap_close(pipeline_combine, NoLock);

	/*
	 * Record a dependency between the combine function and its corresponding aggregate
	 */
	agg_objaddress.classId = ProcedureRelationId;
	agg_objaddress.objectId = aggoid;
	agg_objaddress.objectSubId = 0;

	referenced.classId = ProcedureRelationId;
	referenced.objectId = combinefn;
	referenced.objectSubId = 0;

	recordDependencyOn(&agg_objaddress, &referenced, DEPENDENCY_NORMAL);

	/*
	 * Do the same for the transout and combinein functions if they exist
	 */
	if (OidIsValid(combineinfn))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = combineinfn;
		referenced.objectSubId = 0;

		recordDependencyOn(&agg_objaddress, &referenced, DEPENDENCY_NORMAL);
	}

	if (OidIsValid(transoutfn))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = transoutfn;
		referenced.objectSubId = 0;

		recordDependencyOn(&agg_objaddress, &referenced, DEPENDENCY_NORMAL);
	}

	/*
	 * Record a dependency between pipeline_combine entry and the aggregate
	 */
	referenced.classId = PipelineCombineRelationId;
	referenced.objectId = combinetup_oid;
	referenced.objectSubId = 0;

	recordDependencyOn(&referenced, &agg_objaddress, DEPENDENCY_INTERNAL);

	return result;
}

/*
 * RemovePipelineCombineById
 */
void
RemovePipelineCombineById(Oid oid)
{
	Relation pipeline_combine;
	HeapTuple tuple;

	pipeline_combine = heap_open(PipelineCombineRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(PIPELINECOMBINEOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for pipeline_combine tuple with OID %u", oid);

	simple_heap_delete(pipeline_combine, &tuple->t_self);

	ReleaseSysCache(tuple);

	CommandCounterIncrement();

	heap_close(pipeline_combine, RowExclusiveLock);
}
