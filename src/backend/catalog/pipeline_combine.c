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
 * GetCombineStateType
 *
 * Given an aggregate function oid, determine the type of its transition state
 */
Oid GetCombineStateType(Oid aggfnoid)
{
	Oid result = InvalidOid;
	HeapTuple	aggtup;
	HeapTuple combtup;
	Form_pg_aggregate aggform;
	Form_pipeline_combine combform;

	aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	if (!HeapTupleIsValid(aggtup))
		return result;

	aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);
	ReleaseSysCache(aggtup);

	combtup = SearchSysCache2(PIPELINECOMBINETRANSFNOID,
			ObjectIdGetDatum(aggform->aggfinalfn), ObjectIdGetDatum(aggform->aggtransfn));

	if (!HeapTupleIsValid(combtup))
		return result;

	combform = (Form_pipeline_combine) GETSTRUCT(combtup);

	/* we only have a state type if we're actually storing state in tables */
	if (combform->storestate)
		result = combform->transouttype;

	ReleaseSysCache(combtup);

	return result;
}
