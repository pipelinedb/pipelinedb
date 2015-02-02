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
#define BYTE_IDX(bf, i) (((idx) / 8) % (bf)->num_bytes)
#define BIT_MASK(i) (1 << ((i) % 8))

#define SEED1 0xa426a75efe21f2e5L
#define SEED2 0xbf3c00365ee5571dL

/*
 * Murmur is faster than an SHA-based approach and provides as-good collision
 * resistance.  The combinatorial generation approach described in
 * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
 * does prove to work in actual tests, and is obviously faster
 * than performing multiple iterations of murmur.
 */

BloomFilter *
BloomFilterCreateWithPAndN(float8 p, int64_t n)
{
	BloomFilter *bf = palloc(sizeof(BloomFilter));

	/* Determine m and k from p and n */
	bf->m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
	bf->k = round(log(2.0) * bf->m / n);

	/* Round m up to nearest byte limit */
	bf->num_bytes = ceil(bf->m / 8.0);
	bf->bytea = palloc0(sizeof(char) * bf->num_bytes);

	return bf;
}

BloomFilter *
BloomFilterCreate(void)
{
	return BloomFilterCreateWithPAndN(DEFAULT_P, DEFAULT_N);
}

void
BloomFilterAdd(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash1 = MurmurHash64AWithSeed(key, size, SEED1);
	uint64_t hash2 = MurmurHash64AWithSeed(key, size, SEED2);

	for (i = 0; i < bf->k; i++)
	{
		uint64_t hash = hash1 + (i * hash2);
		uint64_t idx = hash % bf->m;
		bf->bytea[BYTE_IDX(bf, idx)] |= BIT_MASK(idx);
	}
}

bool
BloomFilterContains(BloomFilter *bf, void *key, Size size)
{
	uint32_t i;
	uint64_t hash1 = MurmurHash64AWithSeed(key, size, SEED1);
	uint64_t hash2 = MurmurHash64AWithSeed(key, size, SEED2);

	for (i = 0; i < bf->k; i++)
	{
		uint64_t hash = hash1 + (i * hash2);
		uint64_t idx = hash % bf->m;
		if (!(bf->bytea[BYTE_IDX(bf, idx)] && BIT_MASK(idx)))
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

	for (i = 0; i < result->num_bytes; i++)
		result->bytea[i] |= incoming->bytea[i];

	return result;
}

Size
BloomFilterSize(BloomFilter *bf)
{
	return sizeof(BloomFilter) + (sizeof(char) * ceil(bf->m / 8.0));
}

uint64_t
BloomFilterCardinality(BloomFilter *bf)
{
	uint32_t i;
	float8 x = 0;

	/* Bit counting algorithm from: http://www.inwap.com/pdp10/hbaker/hakmem/hacks.html#item167 */
	for (i = 0; i < bf->num_bytes; i++)
		x += (bf->bytea[i] * 01001001001ULL & 042104210421ULL) % 017;

	/* From: http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter */
	return -1.0 * bf->m * log(1 - (x / bf->m)) / bf->k;
}
