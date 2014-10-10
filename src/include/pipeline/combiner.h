/*-------------------------------------------------------------------------
 *
 * combiner.h
 *
 * Interface to the combiner process functionality
 *
 * src/include/pipeline/combiner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMBINER_H
#define COMBINER_H

#include "executor/execdesc.h"
#include "utils/plancache.h"
#include "utils/portal.h"
#include "utils/resowner.h"


typedef struct CombinerDesc
{
	char *name;
	int sock;
	int recvtimeoutms;
} CombinerDesc;

CombinerDesc *CreateCombinerDesc(QueryDesc *query);
void ContinuousQueryCombinerRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner);
PlannedStmt *GetCombinePlan(RangeVar *cvrel, Tuplestorestate *store, Query **query, TupleDesc *desc);
List *GetCombineColumns(Query *query);
void GetTuplesToCombineWith(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber merge_attr,
		List *group_clause, TupleHashTable merge_targets);
void SyncCombine(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, AttrNumber merge_attr, TupleHashTable merge_targets);
void Combine(Query *query, PlannedStmt *plan, TupleDesc cvdesc, Tuplestorestate *store);

#endif /* COMBINER_H */
