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
#include "postgres.h"

#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "utils/datum.h"

#define HLL_MAX_SPARSE_BYTES 11000

#define HLL_SPARSE_DIRTY 's'
#define HLL_SPARSE_CLEAN 'S'
#define HLL_DENSE_DIRTY 'd'
#define HLL_DENSE_CLEAN 'D'

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
HyperLogLog *HLLCreateFromRaw(uint8 *M, int mlen, uint8 p, char encoding);
HyperLogLog *HLLAdd(HyperLogLog *hll, void *elem, Size len, int *result);
HyperLogLog *HLLCopy(HyperLogLog *src);
uint64 HLLSize(HyperLogLog *hll);
HyperLogLog *HLLUnion(HyperLogLog *result, HyperLogLog *incoming);
HyperLogLog *HLLAddSlot(HyperLogLog *hll, TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, int *result);

#endif
