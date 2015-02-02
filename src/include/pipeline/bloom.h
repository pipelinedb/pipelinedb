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
	uint64_t m;
	uint16_t k;
	uint64_t num_bytes;
	char *bytea;
} BloomFilter;

extern BloomFilter *BloomFilterCreateWithPAndN(float8 p, int64_t n);
extern BloomFilter *BloomFilterCreate(void);

extern void BloomFilterAdd(BloomFilter *bf, void *key, Size size);
extern bool BloomFilterContains(BloomFilter *bf, void *key, Size size);
extern BloomFilter *BloomFilterUnion(BloomFilter *result, BloomFilter *incoming);
extern uint64_t BloomFilterCardinality(BloomFilter *bf);
extern Size BloomFilterSize(BloomFilter *bf);

#endif
