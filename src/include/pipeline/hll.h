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

typedef struct HyperLogLog {
	/* Dense or sparse, dirty or clean? See above */
  char encoding;
  /*
   * Last computed cardinality, can be reused until new data is added.
   * That is, if the encoding is *_CLEAN.
   */
  long card;
  /* number of leading bits of hash values to use for determining register */
  uint8 p;
  /* number of bytes allocated for M */
  int mlen;
  /* substream registers */
  uint8 M[1];
} HyperLogLog;

uint64 MurmurHash64A(const void *key, Size keysize);
uint64 HLLSize(HyperLogLog *hll);
HyperLogLog *HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result);
HyperLogLog *HLLCreate(int p);

#endif
