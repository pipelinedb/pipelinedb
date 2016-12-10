/*-------------------------------------------------------------------------
 *
 * hll.h
 *	  Interface for HyperLogLog support
 *
 * Portions Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * Portions Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/hll.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_HLL_H
#define PIPELINE_HLL_H

#include "c.h"
#include "postgres.h"

#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "utils/datum.h"

#define HLL_MAX_SPARSE_BYTES 11000
#define HLL_MAX_EXPLICIT_REGISTERS 2048 /* 2048 * 4 = 8192 bytes */

#define HLL_SPARSE_DIRTY 's'
#define HLL_SPARSE_CLEAN 'S'
#define HLL_DENSE_DIRTY 'd'
#define HLL_DENSE_CLEAN 'D'
#define HLL_EXPLICIT_DIRTY 'e'
#define HLL_EXPLICIT_CLEAN 'E'
#define HLL_UNPACKED 'u'

#define HLL_IS_SPARSE(hll) ((hll)->encoding == HLL_SPARSE_DIRTY || (hll)->encoding == HLL_SPARSE_CLEAN)
#define HLL_IS_EXPLICIT(hll) ((hll)->encoding == HLL_EXPLICIT_DIRTY || (hll)->encoding == HLL_EXPLICIT_CLEAN)
#define HLL_IS_DENSE(hll) ((hll)->encoding == HLL_DENSE_DIRTY || (hll)->encoding == HLL_DENSE_CLEAN)
#define HLL_IS_UNPACKED(hll) ((hll)->encoding == HLL_UNPACKED)

#define HLL_EXPLICIT_GET_NUM_REGISTERS(hll) ((hll)->mlen / 4)

#define HLLSize(hll) (sizeof(HyperLogLog) + (hll)->mlen)

typedef struct HyperLogLog
{
	uint32	vl_len_;
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

HyperLogLog *HLLCreateWithP(int p);
HyperLogLog *HLLCreate(void);
HyperLogLog *HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result);
HyperLogLog *HLLCopy(HyperLogLog *src);
uint64 HLLCardinality(HyperLogLog *hll);
HyperLogLog *HLLUnion(HyperLogLog *result, HyperLogLog *incoming);

HyperLogLog *HLLUnpack(HyperLogLog *initial);
HyperLogLog *HLLPack(HyperLogLog *hllu);
HyperLogLog *HLLUnionAdd(HyperLogLog *hllu, HyperLogLog *incoming);

#endif
