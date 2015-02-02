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

/*
 * Copied from https://code.google.com/p/bloom/source/browse/trunk/bloom_filter.hpp#458
 */
static const uint64_t SEEDS[NUM_SEEDS] = { 0x8837ce5abc1b0f94L, 0x8d5c5f3de95d637bL,
		0xa426a75efe21f2e5L, 0x9f3ece5acc9c9a7bL, 0xa5d853a4594bee3dL,
		0x8ad7040aceaeccd7L, 0xa267de5724cad14cL, 0xae05ab19b511324fL,
		0xb7681a4d05883912L, 0xbe1b673159dc63beL, 0xbca31a9ea3cb4174L,
		0xbf902bf8b993c318L, 0xbf3c00365ee5571dL, 0x86269a6312433401L,
		0x991e156641fc5bd3L, 0x92c0d1302f6f85d3L, 0xb92b2acf138993f5L,
		0x95abb381d2542d1cL, 0x8b690c01582bc006L, 0xa594d080216b83b3L,
		0xb528b19dd8728267L, 0x92290d1cbae899ffL, 0x8f0b58cd789e21ffL,
		0x8e9898fe35830397L, 0x962ad38d372963e4L, 0x9ba0c161537afce8L,
		0x8d26894090cce3a3L, 0xb6f03e6216a2d394L, 0x8b056a8a344a97f5L,
		0x89523de50b7d095bL, 0xbfc5ef74fb32b6fcL, 0xad40e751a9578502L,
		0xa52ef2f006c9ffb5L, 0xab01fffc3ddb8bd1L, 0x8abe22973fdc2326L,
		0x99ba276f56155101L, 0x841d72a761101b62L, 0x95cf51e6096f58b0L,
		0x8a4a8947a00192bfL, 0xb1224c73919f788bL, 0x80713c7c93361a05L,
		0xa5e99eedd9e49ba9L, 0x9e1e1cdf25e0377eL, 0xaf36688a125f5d0cL,
		0x82b00564886880b4L, 0xb46cbebed733d17eL, 0xb0ddbf3e597f8f6bL,
		0xb51684508d31e45bL, 0xb8442a02f79dc8c2L, 0xbed4ae251086da66L,
		0x90544426a85f3213L, 0x8770cdfeecd29c5aL, 0xa019d57d0236b39eL,
		0xa8d0baeec582ff53L, 0x84d66334f9b5abb8L, 0xaea6aeafc9ffca0eL,
		0xa417c4e77b67a35eL, 0x93e2e1360d80eac1L, 0x9a2ebacdfda1dcf7L,
		0x82301a923061d20cL, 0xa6b7341de705132bL, 0x8b541459b250190bL,
		0x9648a9f118bc8422L, 0xa1cb1e207c141dfbL, 0xa3a79b35d15ba698L,
		0x943369792a9f096bL, 0xa576e1303fc02036L, 0xb47f72d3c5c8cd2cL,
		0x8133ede726e7b9a1L, 0x951340e282a1d79dL, 0x8a5d61eae452e534L,
		0x9627bd8350ff1abcL, 0x97aee3ea2332ed8cL, 0x98fcbd9dd435a0a7L,
		0xbef7d49dad9ee6ccL, 0xbec90b316e5873b9L, 0x8089a2cd0f9a43a8L,
		0xbabf16011bf7462dL, 0xa95bc00beaa30c05L, 0xb0e96d0dc1af81e4L,
		0x848b49dfb3341d62L, 0xae404a9097eba93aL, 0x9fab2e2f96740d6cL,
		0xa040d7513618cc1fL, 0x871ecb50774ebea7L, 0x9700092f7c87a77aL,
		0x88d2402ec6b9caebL, 0xb41038db603549f1L, 0x8dc6914cfe6f958bL,
		0xaef6f3d77da34db1L, 0x986e394dc5f55db5L, 0x9e75b852fb4b39a3L,
		0xb61abccacb253d7fL, 0xb793674475dd9e62L, 0xaef8b83150b4c709L,
		0x8c5b347a802ed48aL, 0x8aa7cc813aafb95fL, 0x80819b3d4609468aL,
		0x9d149ca1edfa9824L, 0xa75bd9145c4b5d4dL, 0x8a5ef75f78aa7af8L,
		0x9f075f312a46a7e2L, 0xa2538d171c561584L, 0xbc19d49bdd5a3943L,
		0x84a0f5f5b2e9ee6eL, 0xbf5522b24d849690L, 0xb95c361653a534eaL,
		0xa3d4d87fde28c0d3L, 0xb6266b2907fe00f0L, 0x93b39919a821ab00L,
		0x9205b50bc389fd49L, 0x8e14c63b4e6fadb5L, 0xb1547796433e25e9L,
		0xb999116960b12f2bL, 0xb6506cac1caf5611L, 0xa722dea50836deceL,
		0xb0316e52ed160666L, 0x832cf94be3ee9325L, 0xa18108efc61d8453L,
		0x9c46290881935eedL, 0x87723ef623aba2f1L, 0x89e1c777f1283948L,
		0xa4f6def28075f847L, 0xa37f0a6efa7ae87aL, 0x9db99bc1b61295b8L,
		0x9a33bd90b2f5a71eL, 0x8346fd396c919f2dL, 0xa68fdae97b4cc87aL
};

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
BloomFilterAdd(BloomFilter *bf, uint64_t key)
{
	uint32_t i;

	for (i = 0; i < bf->k; i++)
	{
		uint64_t seed = SEEDS[bf->k % NUM_SEEDS] + (bf->k / NUM_SEEDS);
		uint64_t hash = MurmurHash64AWithSeed(&key, sizeof(uint64_t), seed);
		uint64_t idx = hash % bf->m;
		bf->bytea[BYTE_IDX(bf, idx)] |= BIT_MASK(idx);
	}
}

bool
BloomFilterContains(BloomFilter *bf, uint64_t key)
{
	uint32_t i;

	for (i = 0; i < bf->k; i++)
	{
		uint64_t seed = SEEDS[bf->k % NUM_SEEDS] + (bf->k / NUM_SEEDS);
		uint64_t hash = MurmurHash64AWithSeed(&key, sizeof(uint64_t), seed);
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
