/*-------------------------------------------------------------------------
 *
 * bloom.c
 *	  Bloom Filter implementation.
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include "bloom.h"
#include "miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_P 0.02
#define DEFAULT_N (2 << 14) /* 16384 */
#define BUCKET_IDX(bf, i) (((idx) / 64) % (bf)->blen)
#define BIT_MASK(i) (1 << ((i) % 64))

/*
 * Murmur is faster than an SHA-based approach and provides as-good collision
 * resistance.  The combinatorial generation approach described in
 * https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
 * does prove to work in actual tests, and is obviously faster
 * than performing multiple iterations of Murmur.
 */
#define MURMUR_SEED 0x99496f1ddc863e6fL

/*
 * BloomFilterCreateWithMAndK
 */
BloomFilter *
BloomFilterCreateWithMAndK(uint32_t m, uint16_t k)
{
	BloomFilter *bf;
	uint32_t blen = ceil(m / 64.0); /* round m up to nearest uint64_t limit */

	bf = palloc0(sizeof(BloomFilter) + (sizeof(uint64_t) * blen));
	bf->m = m;
	bf->k = k;
	bf->blen = blen;

	SET_VARSIZE(bf, BloomFilterSize(bf));

	return bf;
}

/*
 * BloomFilterCreateWithPAndN
 */
BloomFilter *
BloomFilterCreateWithPAndN(float8 p, uint32_t n)
{
	/* Determine m and k from p and n */
	uint32_t m = -1 * ceil(n * log(p) / (pow(log(2), 2)));
	uint16_t k = round(log(2.0) * m / n);
	return BloomFilterCreateWithMAndK(m, k);
}

/*
 * BloomFilterCreate
 */
BloomFilter *
BloomFilterCreate(void)
{
	return BloomFilterCreateWithPAndN(DEFAULT_P, DEFAULT_N);
}

/*
 * BloomFilterDestroy
 */
void
BloomFilterDestroy(BloomFilter *bf)
{
	pfree(bf);
}

/*
 * BloomFilterCopy
 */
BloomFilter *
BloomFilterCopy(BloomFilter *bf)
{
	Size size = BloomFilterSize(bf);
	char *new = palloc(size);
	memcpy(new, (char *) bf, size);
	return (BloomFilter *) new;
}

/*
 * BloomFilterAdd
 */
void
BloomFilterAdd(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash[2];
	MurmurHash3_128(key, size, MURMUR_SEED, &hash);

	for (i = 0; i < bf->k; i++)
	{
		uint64_t h = hash[0] + (i * hash[1]);
		uint32_t idx = h % bf->m;
		bf->b[BUCKET_IDX(bf, idx)] |= BIT_MASK(idx);
	}
}

/*
 * BloomFilterContains
 */
bool
BloomFilterContains(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash[2];
	MurmurHash3_128(key, size, MURMUR_SEED, &hash);

	for (i = 0; i < bf->k; i++)
	{
		uint64_t h = hash[0] + (i * hash[1]);
		uint32_t idx = h % bf->m;
		if (!(bf->b[BUCKET_IDX(bf, idx)] & BIT_MASK(idx)))
			return false;
	}

	return true;
}

/*
 * BloomFilterUnion
 */
BloomFilter *
BloomFilterUnion(BloomFilter *result, BloomFilter *incoming)
{
	uint32_t i;

	Assert(result->m == incoming->m);
	Assert(result->k == incoming->k);

	for (i = 0; i < result->blen; i++)
		result->b[i] |= incoming->b[i];

	return result;
}

/*
 * BloomFilterIntersection
 */
BloomFilter *
BloomFilterIntersection(BloomFilter *result, BloomFilter *incoming)
{
	uint32_t i;

	Assert(result->m == incoming->m);
	Assert(result->k == incoming->k);

	for (i = 0; i < result->blen; i++)
		result->b[i] &= incoming->b[i];

	return result;
}

/*
 * BloomFilterSize
 */
Size
BloomFilterSize(BloomFilter *bf)
{
	return sizeof(BloomFilter) + (sizeof(uint64_t) * bf->blen);
}

/*
 * BloomFilterCardinality
 */
uint64_t
BloomFilterCardinality(BloomFilter *bf)
{
	uint32_t i;
	float8 x = 0;

	for (i = 0; i < bf->blen; i++)
		x += __builtin_popcount(bf->b[i]);

	/* From: http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter */
	return -1.0 * bf->m * log(1 - (x / bf->m)) / bf->k;
}

/*
 * BloomFilterFillRatio
 */
float8
BloomFilterFillRatio(BloomFilter *bf)
{
	uint32_t i;
	uint64_t x = 0;


	for (i = 0; i < bf->blen; i++)
		x += __builtin_popcount(bf->b[i]);

	return x / (bf->blen * 8.0);
}
