/*-------------------------------------------------------------------------
 *
 * bloom.c
 *	  Bloom Filter implementation.
 *
 * src/backend/pipeline/bloom.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "pipeline/bloom.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_P 0.03
#define DEFAULT_N (2 << 20)
#define NUM_SEEDS 128
#define BYTE_IDX(bf, i) (((idx) / 8) % (bf)->blen)
#define BIT_MASK(i) (1 << ((i) % 8))

/*
 * Murmur is faster than an SHA-based approach and provides as-good collision
 * resistance.  The combinatorial generation approach described in
 * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
 * does prove to work in actual tests, and is obviously faster
 * than performing multiple iterations of Murmur.
 */

BloomFilter *
BloomFilterCreateWithMAndK(uint32_t m, uint16_t k)
{
	BloomFilter *bf;
	uint32_t blen = ceil(m / 8.0); /* round m up to nearest byte limit */

	bf = palloc0(sizeof(BloomFilter) + blen);
	bf->m = m;
	bf->k = k;
	bf->blen = blen;

	return bf;
}

BloomFilter *
BloomFilterCreateWithPAndN(float8 p, uint32_t n)
{
	/* Determine m and k from p and n */
	uint32_t m = ceil(n * log(1 / p) / (pow(log(2), 2)));
	uint16_t k = round(log(2.0) * m / n);
	return BloomFilterCreateWithMAndK(m, k);
}

BloomFilter *
BloomFilterCreate(void)
{
	return BloomFilterCreateWithPAndN(DEFAULT_P, DEFAULT_N);
}

BloomFilter *
BloomFilterCopy(BloomFilter *bf)
{
	Size size = BloomFilterSize(bf);
	char *new = palloc(size);
	memcpy(new, (char *) bf, size);
	return (BloomFilter *) new;
}

void
BloomFilterAdd(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash = MurmurHash64A(key, size);
	uint32_t hash1 = hash >> 32;
	uint32_t hash2 = hash && 0xffffffff;

	for (i = 0; i < bf->k; i++)
	{
		uint32_t hash = hash1 + (i * hash2);
		uint32_t idx = hash % bf->m;
		bf->b[BYTE_IDX(bf, idx)] |= BIT_MASK(idx);
	}
}

bool
BloomFilterContains(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash = MurmurHash64A(key, size);
	uint32_t hash1 = hash >> 32;
	uint32_t hash2 = hash && 0xffffffff;

	for (i = 0; i < bf->k; i++)
	{
		uint64_t hash = hash1 + (i * hash2);
		uint64_t idx = hash % bf->m;
		if (!(bf->b[BYTE_IDX(bf, idx)] && BIT_MASK(idx)))
			return false;
	}

	return true;
}

BloomFilter *
BloomFilterUnion(BloomFilter *result, BloomFilter *incoming)
{
	int i;

	Assert(result->m == incoming->m);
	Assert(result->k == incoming->k);

	for (i = 0; i < result->blen; i++)
		result->b[i] |= incoming->b[i];

	return result;
}

BloomFilter *
BloomFilterIntersection(BloomFilter *result, BloomFilter *incoming)
{
	int i;

	Assert(result->m == incoming->m);
	Assert(result->k == incoming->k);

	for (i = 0; i < result->blen; i++)
		result->b[i] &= incoming->b[i];

	return result;
}

Size
BloomFilterSize(BloomFilter *bf)
{
	return sizeof(BloomFilter) + (sizeof(char) * bf->blen);
}

uint64_t
BloomFilterCardinality(BloomFilter *bf)
{
	uint32_t i;
	float8 x = 0;

	/* Bit counting algorithm from: http://www.inwap.com/pdp10/hbaker/hakmem/hacks.html#item167 */
	for (i = 0; i < bf->blen; i++)
		x += (bf->b[i] * 01001001001ULL & 042104210421ULL) % 017;

	/* From: http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter */
	return -1.0 * bf->m * log(1 - (x / bf->m)) / bf->k;
}
