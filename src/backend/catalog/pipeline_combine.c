/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipeline_combine.c
 *	  routines to support manipulation of the pipeline_combine relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_combine.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pipeline_combine_fn.h"
#include "utils/syscache.h"

/*
 * GetCombineInfo
 *
 * Get all the pipeline_combine information associated with the given aggregate function
 */
void
GetCombineInfo(Oid aggfnoid, Oid *combinefn, Oid *combineinfn, Oid *statetype)
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
	*combineinfn = combform->combineinfn;

	/* we only have a state type if we're actually storing state in tables */
	if (combform->storestate)
		*statetype = combform->transouttype;

	ReleaseSysCache(combtup);
}

/*
 * GetCombineStateType
 *
 * Given an aggregate function oid, determine the type of its transition state
 */
Oid GetCombineStateType(Oid aggfnoid)
{
	Oid combinefn;
	Oid combineinfn;
	Oid result;

	GetCombineInfo(aggfnoid, &combinefn, &combineinfn, &result);

	return result;
}
