/*-------------------------------------------------------------------------
 *
 * compat.h
 *  Interface for functionality that breaks between major PG versions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/objectaddress.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"

#if PG_VERSION_NUM < 110000
#define PIPELINE_COMPAT_INDEX_ATTR_BITMAP_ALL INDEX_ATTR_BITMAP_ALL
#else
#define PIPELINE_COMPAT_INDEX_ATTR_BITMAP_ALL INDEX_ATTR_BITMAP_HOT
#endif

extern bool CompatProcOidIsAgg(Oid oid);
extern TupleTableSlot *CompatExecInitExtraTupleSlot(EState *estate);
extern ObjectAddress CompatDefineIndex(Oid relationId,
			IndexStmt *stmt,
			Oid indexRelationId,
			bool is_alter_table,
			bool check_rights,
			bool check_not_in_use,
			bool skip_build,
			bool quiet);

extern void CompatAnalyzeVacuumStmt(VacuumStmt *stmt);

extern void CompatBackgroundWorkerInitializeConnectionByOid(Oid db, Oid user);
extern void CompatInitializePostgres(const char *in_dbname, Oid dboid, const char *username,
			 Oid useroid, char *out_dbname);

extern Relids CompatCalcNestLoopRequiredOuter(Path *outer, Path *inner);
extern void CompatPrepareEState(PlannedStmt *pstmt, EState *estate);

extern void CompatExecAssignResultTypeFromTL(PlanState *ps);

#if PG_VERSION_NUM < 110000
extern void CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, FmgrInfo **eqFunctions, FmgrInfo **hashFunctions);
#else
extern void CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, Oid **eqFunctions, FmgrInfo **hashFunctions);
#endif

extern char *CompatGetAttName(Oid relid, AttrNumber att);
