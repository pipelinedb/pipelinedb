/*-------------------------------------------------------------------------
 *
 * compat.c
 *  Consolidated functionality for signatures and symbols that break between
 *  major PG versions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "compat.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/pathnode.h"
#include "pipeline_query.h"
#include "postmaster/bgworker.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * CompatProcOidIsAgg
 */
bool
CompatProcOidIsAgg(Oid oid)
{
	HeapTuple ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	Form_pg_proc pform;
	bool is_agg = false;

	if (!HeapTupleIsValid(ftup))
		return false;

	pform = (Form_pg_proc) GETSTRUCT(ftup);
#if PG_VERSION_NUM < 110000
	is_agg = pform->proisagg;
#else
	is_agg = pform->prokind == PROKIND_AGGREGATE;
#endif

	ReleaseSysCache(ftup);

	return is_agg;
}

/*
 * CompatAnalyzeVacuumStmt
 */
void
CompatAnalyzeVacuumStmt(VacuumStmt *stmt)
{
//	if (stmt->relation && RangeVarIsContView(stmt->relation))
//		stmt->relation = RangeVarGetMatRelName(stmt->relation);
}

/*
 * CompatCalcNestLoopRequiredOuter
 */
Relids
CompatCalcNestLoopRequiredOuter(Path *outer, Path *inner)
{
//	return calc_nestloop_required_outer(outer, inner);
	return calc_nestloop_required_outer(outer->parent->relids,
			PATH_REQ_OUTER(outer), inner->parent->relids, PATH_REQ_OUTER(inner));
}

/*
 * CompatPrepareEState
 */
void
CompatPrepareEState(PlannedStmt *pstmt, EState *estate)
{
//	if (pstmt->nParamExec == 0)
//		return;
//
//	estate->es_param_exec_vals = (ParamExecData *)
//		palloc0(pstmt->nParamExec * sizeof(ParamExecData));
}

/*
 * CompatBuildTupleHashTable
 */
TupleHashTable
CompatBuildTupleHashTable(TupleDesc desc,
					int numCols, AttrNumber *keyColIdx,
#if PG_VERSION_NUM < 110000
					FmgrInfo *eqfuncs,
#else
					Oid *eqfuncs,
#endif
					FmgrInfo *hashfunctions,
					long nbuckets, Size additionalsize,
					MemoryContext tablecxt,
					MemoryContext tempcxt, bool use_variable_hash_iv)
{
#if PG_VERSION_NUM < 110000
	return BuildTupleHashTable(numCols, keyColIdx, eqfuncs, hashfunctions, nbuckets,
			additionalsize, tablecxt, tempcxt, use_variable_hash_iv);
#else
	{
		PlanState *parent = makeNode(PlanState);
		parent->state = CreateExecutorState();
		return BuildTupleHashTable(parent, desc, numCols, keyColIdx, eqfuncs, hashfunctions, nbuckets,
				additionalsize, tablecxt, tempcxt, use_variable_hash_iv);
	}
#endif

}

/*
 * CompatExecTuplesHashPrepare
 */
void
#if PG_VERSION_NUM < 110000
CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, FmgrInfo **eqFunctions, FmgrInfo **hashFunctions)
#else
CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, Oid **eqFunctions, FmgrInfo **hashFunctions)
#endif
{
	execTuplesHashPrepare(numCols, eqOperators, eqFunctions, hashFunctions);
}

/*
 * CompatGetAttName
 */
char *
CompatGetAttName(Oid relid, AttrNumber att)
{
#if PG_VERSION_NUM < 110000
	return get_relid_attribute_name(relid, att);
#else
	return get_attname(relid, att, false);
#endif
}
