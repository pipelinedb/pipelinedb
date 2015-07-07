/* Copyright (c) 2013-2015 PipelineDB */
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

#define HLL_MAX_SPARSE_BYTES 11000

#define HLL_SPARSE_DIRTY 's'
#define HLL_SPARSE_CLEAN 'S'
#define HLL_DENSE_DIRTY 'd'
#define HLL_DENSE_CLEAN 'D'

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
HyperLogLog *HLLCreateWithP(int p);
HyperLogLog *HLLCreate(void);
HyperLogLog *HLLCreateFromRaw(uint8 *M, int mlen, uint8 p, char encoding);
HyperLogLog *HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result);
uint64 HLLSize(HyperLogLog *hll);
HyperLogLog *HLLUnion(HyperLogLog *result, HyperLogLog *incoming);

#endif
