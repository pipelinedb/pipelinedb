#include <check.h>

#include "suites.h"
#include "pipeline/miscutils.h"
#include "pipeline/hll.h"

#define MURMUR_SEED 0xb3216312b7b5a93cL

static HyperLogLog *
add_elements(HyperLogLog *hll, long start, long end)
{
	int i;
	for (i=start; i<end; i++)
	{
		int result;
		char buf[10];

		sprintf(buf, "%010d", i);
		hll = HLLAdd(hll, buf, 10, &result);
	}

	return hll;
}

static void
assert_size_for_elements(uint64 expected, long start, long end)
{
	HyperLogLog *hll = HLLCreate();
	uint64 size;

	hll = add_elements(hll, start, end);
	size = HLLSize(hll);
	ck_assert_int_eq(expected, size);
}

START_TEST(test_murmurhash64a)
{
	/*
	 * Basic sanity checks for our hash function
	 */
	char *k = "some_key";
	uint32 ik;
	uint64 lk;
	float8 fk8;
	uint64 h = MurmurHash3_64(k, strlen(k) + 1, MURMUR_SEED);

	ck_assert_uint_eq(h, 5413312395049998591);

	k = "another key";
	h = MurmurHash3_64(k, strlen(k) + 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 1315362163169748446U);

	fk8 = 42.42;
	h = MurmurHash3_64(&fk8, 8, MURMUR_SEED);
	ck_assert_uint_eq(h, 758775358900077568U);

	ik = 42;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 13462425845129963738U);

	ik = 0;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 11930621527264735852U);

	ik = 1 << 31;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 4456853593563623847U);

	lk = 1UL << 63;
	h = MurmurHash3_64(&lk, 8, MURMUR_SEED);
	ck_assert_uint_eq(h, 15646792638355482678U);
}
END_TEST

START_TEST(test_murmurhash64a_varlen)
{
	/*
	 * Verify that the key length is used by the hash function
	 */
	char *k = "some_key";

	/* exclude the NULL byte */
	uint64 h = MurmurHash3_64(k, strlen(k), MURMUR_SEED);

	ck_assert_uint_eq(h, 4579875563451280355);

	/* truncate the last byte and the NULL byte */
	h = MurmurHash3_64(k, strlen(k) - 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 12571778299612594037U);

	/* include the NULL byte */
	h = MurmurHash3_64(k, strlen(k) + 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 5413312395049998591U);
}
END_TEST

START_TEST(test_sparse)
{
	/*
	 * Verify that our sparse HLL representation works
	 */
	HyperLogLog *hll = HLLCreate();
	uint64 size;
	int i;

	for (i=0; i<8000; i++)
		hll = add_elements(hll, i, i + 1);

	ck_assert(hll->encoding == HLL_SPARSE_DIRTY);

	size = HLLSize(hll);
	ck_assert(hll->encoding == HLL_SPARSE_CLEAN);
	ck_assert_int_eq(size, 8076);

	assert_size_for_elements(797, 204, 1003);
	assert_size_for_elements(1, 1, 2);
	assert_size_for_elements(5, 1000, 1005);
	assert_size_for_elements(398, 2000, 2400);
	assert_size_for_elements(4253, 4747, 8999);
}
END_TEST

START_TEST(test_dense)
{
	/*
	 * Verify that our dense HLL representation works
	 */
	HyperLogLog *hll = HLLCreate();
	uint64 size;

	hll = add_elements(hll, 0, 10000000);
	ck_assert(hll->encoding == HLL_DENSE_DIRTY);

	size = HLLSize(hll);
	ck_assert(hll->encoding == HLL_DENSE_CLEAN);
	ck_assert_int_eq(size, 9895714);

	assert_size_for_elements(8283, 4747, 12999);
	assert_size_for_elements(9, 10000000, 10000009);
	assert_size_for_elements(300807, 100223, 400000);
	assert_size_for_elements(301270, 435423, 735423);
	assert_size_for_elements(118974, 0, 120001);
}
END_TEST

START_TEST(test_sparse_to_dense)
{
	/*
	 * Verify that the sparse HLL representation is promoted
	 * to the dense representation when we add enough elements
	 */
	HyperLogLog *hll = HLLCreate();
	int elems = 1000;
	uint64 size;

	hll = add_elements(hll, 0, elems);

	ck_assert(hll->encoding == HLL_SPARSE_DIRTY);

	/* keep adding elements until we pass the sparse size threshold */
	for (;;)
	{
		int start = elems;
		int end = elems + 1;

		hll = add_elements(hll, start, end);
		if (hll->mlen > HLL_MAX_SPARSE_BYTES)
			break;

		elems = end;
	}

	ck_assert(hll->encoding == HLL_DENSE_DIRTY);

	size = HLLSize(hll);
	ck_assert_int_eq(size, 9033);
}
END_TEST

START_TEST(test_card_caching)
{
	/*
	 * Verify that the cached cardinality is invalidated when the cardinality changes
	 */
	HyperLogLog *hll = HLLCreate();
	uint64 size;

	hll = add_elements(hll, 0, 1000);

	ck_assert_int_eq(hll->encoding, HLL_SPARSE_DIRTY);

	size = HLLSize(hll);
	ck_assert_uint_eq(size, 998);
	ck_assert_int_eq(size, hll->card);

	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	/* nothing was added, so cached cardinality should remain clean */
	size = HLLSize(hll);
	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	/* add the same values again, cardinality shouldn't change so it should remain clean */
	hll = add_elements(hll, 0, 1000);
	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	/* now add new elements */
	hll = add_elements(hll, 1000, 1010);

	/* cardinality changed, cached value should have been invalidated */
	ck_assert_int_eq(hll->encoding, HLL_SPARSE_DIRTY);

	/* verify that it works with the dense representation as well */
	hll = add_elements(hll, 1010, 100000);

	ck_assert_int_eq(hll->encoding, HLL_DENSE_DIRTY);

	size = HLLSize(hll);
	ck_assert_int_eq(hll->encoding, HLL_DENSE_CLEAN);
	ck_assert_int_eq(size, 99527);
	ck_assert_int_eq(size, hll->card);
}
END_TEST

START_TEST(test_union)
{
	/*
	 * Verify that the cardinality of the union of multiple HLLs is correct
	 */
	HyperLogLog *hll1 = HLLCreate();
	HyperLogLog *hll2 = HLLCreate();
	uint64 size;

	hll1 = add_elements(hll1, 0, 1000);
	hll2 = add_elements(hll2, 1000, 2000);

	size = HLLSize(hll1);
	ck_assert_int_eq(size, 998);

	size = HLLSize(hll2);
	ck_assert_int_eq(size, 1002);

	hll1 = HLLUnion(hll1, hll2);

	/* these HLLs are disjoint, so the union should include both */
	size = HLLSize(hll1);
	ck_assert_int_eq(size, 1998);

	hll1 = add_elements(hll1, 500, 1500);

	hll1 = HLLUnion(hll1, hll2);
	size = HLLSize(hll1);

	/* no new elements, union cardinality shouldn't change */
	ck_assert_int_eq(size, 1998);

	hll1 = add_elements(hll1, 1400, 2500);
	hll1 = HLLUnion(hll1, hll2);

	size = HLLSize(hll1);
	ck_assert_int_eq(size, 2500);

	/* now add some new elements to the second HLL */
	hll2 = add_elements(hll2, 3000, 100000);
	ck_assert(hll2->encoding == HLL_DENSE_DIRTY);

	hll1 = HLLUnion(hll1, hll2);
	size = HLLSize(hll1);

	ck_assert_int_eq(size, 99003);
}
END_TEST

START_TEST(test_union_sparse_and_dense)
{
	/*
	 * Verify that the union of a sparse and dense HLL is correct
	 */
	HyperLogLog *dense = HLLCreate();
	HyperLogLog *sparse = HLLCreate();
	HyperLogLog *result;
	uint64 size;

	dense = add_elements(dense, 0, 100000);
	ck_assert(dense->encoding = HLL_DENSE_DIRTY);

	size = HLLSize(dense);
	ck_assert_int_eq(size, 99527);

	sparse = add_elements(sparse, 100000, 101000);
	ck_assert(sparse->encoding = HLL_SPARSE_DIRTY);

	size = HLLSize(sparse);
	ck_assert_int_eq(size, 1005);

	result = HLLUnion(dense, sparse);
	size = HLLSize(result);

	ck_assert_int_eq(size, 100486);
}
END_TEST

START_TEST(test_create_from_raw)
{
	/*
	 * Verify that we can create an HLL with raw input params
	 */
	HyperLogLog *hll = HLLCreate();
	HyperLogLog *copy;

	hll = add_elements(hll, 1, 10002);
	ck_assert_int_eq(10101, HLLSize(hll));

	ck_assert_int_eq(hll->encoding, HLL_DENSE_CLEAN);

	copy = HLLCreateFromRaw(hll->M, hll->mlen, hll->p, hll->encoding);

	ck_assert_int_eq(copy->encoding, HLL_DENSE_DIRTY);
	ck_assert_int_eq(10101, HLLSize(copy));

	hll = HLLCreate();
	hll = add_elements(hll, 0, 10);
	ck_assert_int_eq(10, HLLSize(hll));

	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	copy = HLLCreateFromRaw(hll->M, hll->mlen, hll->p, hll->encoding);

	ck_assert_int_eq(copy->encoding, HLL_SPARSE_DIRTY);
	ck_assert_int_eq(10, HLLSize(copy));
}
END_TEST

Suite *test_hll_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_hll");
	tc = tcase_create("test_hll");
	tcase_set_timeout(tc, 5);
	tcase_add_test(tc, test_murmurhash64a);
	tcase_add_test(tc, test_murmurhash64a_varlen);
	tcase_add_test(tc, test_sparse);
	tcase_add_test(tc, test_sparse_to_dense);
	tcase_add_test(tc, test_dense);
	tcase_add_test(tc, test_union);
	tcase_add_test(tc, test_union_sparse_and_dense);
	tcase_add_test(tc, test_card_caching);
	tcase_add_test(tc, test_create_from_raw);
	suite_add_tcase(s, tc);

	return s;
}
