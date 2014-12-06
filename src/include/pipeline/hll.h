/*-------------------------------------------------------------------------
 *
 * hll.h
 *	  Interface for HyperLogLog support
 *
 *
 * src/include/pipeline/hll.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_HLL_H
#define PIPELINE_HLL_H

#include "c.h"

#define HLL_SPARSE = 's'
#define HLL_DENSE = 'd'

typedef struct HyperLogLog {
	/* dense or sparse? */
  char encoding;
  /* cached cardinality */
  uint8 card[8];
  /* substream registers */
  uint8_t registers[1];
} HyperLogLog;

uint64 MurmurHash64A(const void *key, Size keysize, uint32 seed);
uint64 HLLSize(HyperLogLog *hll);
bool HLLAdd(HyperLogLog *hll);
HyperLogLog *HLLCreate(int p, int k);


#endif
