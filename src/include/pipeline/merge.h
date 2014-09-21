/*-------------------------------------------------------------------------
 *
 * merge.h
 *
 *	  Interface for incremental updates to continuous views
 *
 * src/include/pipeline/merge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MERGE_H
#define MERGE_H

#include "nodes/execnodes.h"
#include "utils/plancache.h"

extern CachedPlan *GetMergePlan(char *cvname, Tuplestorestate *store, CachedPlanSource **src);
extern void InitMergeMemory(void);
extern List *GetMergeColumns(Query *query);
extern void GetTuplesToMergeWith(char *cvname, TupleDesc desc,
		Tuplestorestate *incoming_merges, AttrNumber merge_attr,
		List *group_clause, TupleHashTable merge_targets);
extern void SyncMerge(char *cvname, Tuplestorestate *results,
		TupleTableSlot *slot, AttrNumber merge_attr, TupleHashTable merge_targets);
extern void Merge(char *cvname, Tuplestorestate *store);

#endif
