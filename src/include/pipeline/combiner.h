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
#include "utils/resowner.h"


typedef struct CombinerDesc
{
	char *name;
	int sock;
	int recvtimeoutms;
} CombinerDesc;

extern CombinerDesc *CreateCombinerDesc(QueryDesc *query);
extern void ContinuousQueryCombinerRun(CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner);
extern PlannedStmt *GetCombinePlan(char *cvname, Tuplestorestate *store, Query **query);
extern List *GetCombineColumns(Query *query);
extern void GetTuplesToCombineWith(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber merge_attr,
		List *group_clause, TupleHashTable merge_targets);
extern void SyncCombine(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, AttrNumber merge_attr, TupleHashTable merge_targets);
extern void Combine(char *cvname, TupleDesc cvdesc, Tuplestorestate *store);

#endif
