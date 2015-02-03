/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * bloom.h
 *	  Interface for Bloom Filter support
 *
 * src/include/pipeline/bloom.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_BLOOMFILTER_H
#define PIPELINE_BLOOMFILTER_H

#include "c.h"

typedef struct
{
	uint32	vl_len_;
	uint32_t m;
	uint16_t k;
	uint32_t blen;
	uint8_t b[1];
} BloomFilter;

extern BloomFilter *BloomFilterCreateWithMAndK(uint32_t m, uint16_t k);
extern BloomFilter *BloomFilterCreateWithPAndN(float8 p, uint32_t n);
extern BloomFilter *BloomFilterCreate(void);

extern BloomFilter *BloomFilterCopy(BloomFilter *bf);
extern void BloomFilterAdd(BloomFilter *bf, void *key, Size size);
extern bool BloomFilterContains(BloomFilter *bf, void *key, Size size);
extern BloomFilter *BloomFilterUnion(BloomFilter *result, BloomFilter *incoming);
extern BloomFilter *BloomFilterIntersection(BloomFilter *result, BloomFilter *incoming);
extern uint64_t BloomFilterCardinality(BloomFilter *bf);
extern Size BloomFilterSize(BloomFilter *bf);

#endif
