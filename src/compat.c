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
 * CompatExecInitExtraTupleSlot
 */
TupleTableSlot *
CompatExecInitExtraTupleSlot(EState *estate)
{
//#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
//	return ExecInitExtraTupleSlot(estate);
//#else
//#endif
	return NULL;
}

/*
 * CompatDefineIndex
 */
ObjectAddress
CompatDefineIndex(Oid relationId,
			IndexStmt *stmt,
			Oid indexRelationId,
			bool is_alter_table,
			bool check_rights,
			bool check_not_in_use,
			bool skip_build,
			bool quiet)
{
//	return DefineIndex(relationId, stmt, indexRelationId, is_alter_table,
//			check_rights, check_not_in_use, skip_build, quiet);
	ObjectAddress o;
	return o;
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
 * CompatBackgroundWorkerInitializeConnectionByOid
 */
void
CompatBackgroundWorkerInitializeConnectionByOid(Oid db, Oid user)
{
//	BackgroundWorkerInitializeConnectionByOid(db, user);
}

/*
 * CompatInitializePostgres
 */
void
CompatInitializePostgres(const char *in_dbname, Oid dboid, const char *username,
			 Oid useroid, char *out_dbname)
{
//	InitPostgres(in_dbname, dboid, username, useroid, out_dbname);
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
 * CompatExecAssignResultTypeFromTL
 */
void
CompatExecAssignResultTypeFromTL(PlanState *ps)
{
//	ExecAssignResultTypeFromTL(ps);
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
