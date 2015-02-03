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

extern void CreateTStateEntry(char *cvname);
extern void RemoveTStateEntry(char *cvname);
extern void ResetTStateEntry(char *cvname);

extern void UpdateDistinctBloomFilter(char *cvname, BloomFilter *distinct);
extern BloomFilter *GetDistinctBloomFilter(char *cvname);

#endif
