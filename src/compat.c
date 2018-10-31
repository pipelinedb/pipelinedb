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
#if (PG_VERSION_NUM < 110000)
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
#if (PG_VERSION_NUM < 110000)
	if (stmt->relation && RangeVarIsContView(stmt->relation))
		stmt->relation = RangeVarGetMatRelName(stmt->relation);
#else
	{
		ListCell *lc;
		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
			if (vrel->relation && RangeVarIsContView(vrel->relation))
				vrel->relation = RangeVarGetMatRelName(vrel->relation);
		}
	}
#endif
}

/*
 * CompatCalcNestLoopRequiredOuter
 */
Relids
CompatCalcNestLoopRequiredOuter(Path *outer, Path *inner)
{
#if (PG_VERSION_NUM < 110000)
	return calc_nestloop_required_outer(outer, inner);
#else
	return calc_nestloop_required_outer(outer->parent->relids,
			PATH_REQ_OUTER(outer), inner->parent->relids, PATH_REQ_OUTER(inner));
#endif
}

/*
 * CompatPrepareEState
 */
void
CompatPrepareEState(PlannedStmt *pstmt, EState *estate)
{
	int len;

#if (PG_VERSION_NUM < 110000)
	len = pstmt->nParamExec;
#else
	len = list_length(pstmt->paramExecTypes);
#endif

	if (len == 0)
		return;

	estate->es_param_exec_vals = (ParamExecData *) palloc0(len * sizeof(ParamExecData));
}

/*
 * CompatBuildTupleHashTable
 */
TupleHashTable
CompatBuildTupleHashTable(TupleDesc desc,
					int numCols, AttrNumber *keyColIdx,
#if (PG_VERSION_NUM < 110000)
					FmgrInfo *eqfuncs,
#else
					Oid *eqfuncs,
#endif
					FmgrInfo *hashfunctions,
					long nbuckets, Size additionalsize,
					MemoryContext tablecxt,
					MemoryContext tempcxt, bool use_variable_hash_iv)
{
#if (PG_VERSION_NUM < 110000)
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
CompatExecTuplesHashPrepare(int numCols,
		Oid *eqOperators,
#if (PG_VERSION_NUM < 110000)
		FmgrInfo **eqFunctions,
#else
		Oid **eqFunctions,
#endif
		FmgrInfo **hashFunctions)
{
	execTuplesHashPrepare(numCols, eqOperators, eqFunctions, hashFunctions);
}

/*
 * CompatGetAttName
 */
char *
CompatGetAttName(Oid relid, AttrNumber att)
{
#if (PG_VERSION_NUM < 110000)
	return get_relid_attribute_name(relid, att);
#else
	return get_attname(relid, att, false);
#endif
}

/*
 * ComaptExecAssignResultTypeFromTL
 */
void
ComaptExecAssignResultTypeFromTL(PlanState *ps)
{
#if (PG_VERSION_NUM < 110000)
	ExecAssignResultTypeFromTL(ps);
#else
	ExecInitResultTupleSlotTL(ps->state, ps);
#endif
}
