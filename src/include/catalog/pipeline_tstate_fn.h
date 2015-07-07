/*-------------------------------------------------------------------------
 *
 * pipeline_tstate_fn.h
 *	 prototypes for functions in catalog/pipeline_tstate.c
 *
 *
 * src/include/catalog/pipeline_tstate_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TSTATE_FN_H
#define PIPELINE_TSTATE_FN_H

#include "postgres.h"

#include "pipeline/bloom.h"
#include "catalog/pipeline_tstate.h"

extern void CreateTStateEntry(Oid id);
extern void RemoveTStateEntry(Oid id);
extern void ResetTStateEntry(Oid id);

extern void UpdateDistinctBloomFilter(Oid id, BloomFilter *distinct);
extern BloomFilter *GetDistinctBloomFilter(Oid id);

#endif
