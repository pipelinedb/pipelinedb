/*-------------------------------------------------------------------------
 *
 * bloom.h
 *	  Interface for Bloom Filter support
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_BLOOM_H
#define PIPELINE_BLOOM_H

#include "c.h"

typedef struct BloomFilter
{
	uint32	vl_len_;
	uint32_t m;
	uint16_t k;
	uint32_t blen;
	uint64_t b[1];
} BloomFilter;

extern BloomFilter *BloomFilterCreateWithMAndK(uint32_t m, uint16_t k);
extern BloomFilter *BloomFilterCreateWithPAndN(float8 p, uint32_t n);
extern BloomFilter *BloomFilterCreate(void);
extern void BloomFilterDestroy(BloomFilter *bf);

extern BloomFilter *BloomFilterCopy(BloomFilter *bf);
extern void BloomFilterAdd(BloomFilter *bf, void *key, Size size);
extern bool BloomFilterContains(BloomFilter *bf, void *key, Size size);
extern BloomFilter *BloomFilterUnion(BloomFilter *result, BloomFilter *incoming);
extern BloomFilter *BloomFilterIntersection(BloomFilter *result, BloomFilter *incoming);
extern uint64_t BloomFilterCardinality(BloomFilter *bf);
extern float8 BloomFilterFillRatio(BloomFilter *bf);
extern Size BloomFilterSize(BloomFilter *bf);

#endif
