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
extern void CompatAnalyzeVacuumStmt(VacuumStmt *stmt);

extern Relids CompatCalcNestLoopRequiredOuter(Path *outer, Path *inner);
extern void CompatPrepareEState(PlannedStmt *pstmt, EState *estate);

#if PG_VERSION_NUM < 110000
extern void CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, FmgrInfo **eqFunctions, FmgrInfo **hashFunctions);
#else
extern void CompatExecTuplesHashPrepare(int numCols, Oid *eqOperators, Oid **eqFunctions, FmgrInfo **hashFunctions);
#endif

extern char *CompatGetAttName(Oid relid, AttrNumber att);
