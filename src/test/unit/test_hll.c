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
	size = HLLCardinality(hll);
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

	ck_assert_uint_eq(h, 12492017329703566690U);

	k = "another key";
	h = MurmurHash3_64(k, strlen(k) + 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 9755190654619219245U);

	fk8 = 42.42;
	h = MurmurHash3_64(&fk8, 8, MURMUR_SEED);
	ck_assert_uint_eq(h, 8641623225819217255U);

	ik = 42;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 7381808292743968275U);

	ik = 0;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 12830615003719739504U);

	ik = 1 << 31;
	h = MurmurHash3_64(&ik, 4, MURMUR_SEED);
	ck_assert_uint_eq(h, 9419030371136511660U);

	lk = 1UL << 63;
	h = MurmurHash3_64(&lk, 8, MURMUR_SEED);
	ck_assert_uint_eq(h, 12604183304546475368U);
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

	ck_assert_uint_eq(h, 16128310523266676924U);

	/* truncate the last byte and the NULL byte */
	h = MurmurHash3_64(k, strlen(k) - 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 8637276610791313299U);

	/* include the NULL byte */
	h = MurmurHash3_64(k, strlen(k) + 1, MURMUR_SEED);
	ck_assert_uint_eq(h, 12492017329703566690U);
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

	size = HLLCardinality(hll);
	ck_assert(hll->encoding == HLL_SPARSE_CLEAN);
	ck_assert_int_eq(size, 8017);

	assert_size_for_elements(803, 204, 1003);
	assert_size_for_elements(1, 1, 2);
	assert_size_for_elements(5, 1000, 1005);
	assert_size_for_elements(398, 2000, 2400);
	assert_size_for_elements(4235, 4747, 8999);
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

	size = HLLCardinality(hll);
	ck_assert(hll->encoding == HLL_DENSE_CLEAN);
	ck_assert_int_eq(size, 10096564);

	assert_size_for_elements(8233, 4747, 12999);
	assert_size_for_elements(9, 10000000, 10000009);
	assert_size_for_elements(298814, 100223, 400000);
	assert_size_for_elements(302357, 435423, 735423);
	assert_size_for_elements(118017, 0, 120001);
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

	size = HLLCardinality(hll);
	ck_assert_int_eq(size, 10813);
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

	size = HLLCardinality(hll);
	ck_assert_uint_eq(size, 999);
	ck_assert_int_eq(size, hll->card);

	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	/* nothing was added, so cached cardinality should remain clean */
	size = HLLCardinality(hll);
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

	size = HLLCardinality(hll);
	ck_assert_int_eq(hll->encoding, HLL_DENSE_CLEAN);
	ck_assert_int_eq(size, 98511);
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

	size = HLLCardinality(hll1);
	ck_assert_int_eq(size, 999);

	size = HLLCardinality(hll2);
	ck_assert_int_eq(size, 1002);

	hll1 = HLLUnion(hll1, hll2);

	/* these HLLs are disjoint, so the union should include both */
	size = HLLCardinality(hll1);
	ck_assert_int_eq(size, 2003);

	hll1 = add_elements(hll1, 500, 1500);

	hll1 = HLLUnion(hll1, hll2);
	size = HLLCardinality(hll1);

	/* no new elements, union cardinality shouldn't change */
	ck_assert_int_eq(size, 2003);

	hll1 = add_elements(hll1, 1400, 2500);
	hll1 = HLLUnion(hll1, hll2);

	size = HLLCardinality(hll1);
	ck_assert_int_eq(size, 2500);

	/* now add some new elements to the second HLL */
	hll2 = add_elements(hll2, 3000, 100000);
	ck_assert(hll2->encoding == HLL_DENSE_DIRTY);

	hll1 = HLLUnion(hll1, hll2);
	size = HLLCardinality(hll1);

	ck_assert_int_eq(size, 98067);
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

	size = HLLCardinality(dense);
	ck_assert_int_eq(size, 98511);

	sparse = add_elements(sparse, 100000, 101000);
	ck_assert(sparse->encoding = HLL_SPARSE_DIRTY);

	size = HLLCardinality(sparse);
	ck_assert_int_eq(size, 998);

	result = HLLUnion(dense, sparse);
	size = HLLCardinality(result);

	ck_assert_int_eq(size, 99340);
}
END_TEST

START_TEST(test_copy)
{
	/*
	 * Verify that we can create an HLL with raw input params
	 */
	HyperLogLog *hll = HLLCreate();
	HyperLogLog *copy;

	hll = add_elements(hll, 1, 11000);
	ck_assert_int_eq(10957, HLLCardinality(hll));

	ck_assert_int_eq(hll->encoding, HLL_DENSE_CLEAN);

	copy = HLLCopy(hll);

	ck_assert_int_eq(copy->encoding, HLL_DENSE_CLEAN);
	ck_assert_int_eq(10957, HLLCardinality(copy));

	hll = HLLCreate();
	hll = add_elements(hll, 0, 10);
	ck_assert_int_eq(10, HLLCardinality(hll));

	ck_assert_int_eq(hll->encoding, HLL_SPARSE_CLEAN);

	copy = HLLCopy(hll);

	ck_assert_int_eq(copy->encoding, HLL_SPARSE_CLEAN);
	ck_assert_int_eq(10, HLLCardinality(copy));
}
END_TEST

START_TEST(test_explicit)
{
	HyperLogLog *hll = HLLCreate();

//	ck_assert_int_eq(hll->encoding, HLL_EXPLICIT_CLEAN);
//	ck_assert(HLL_IS_EXPLICIT(hll));

	hll = add_elements(hll, 1, 10);
//	ck_assert_int_eq(hll->encoding, HLL_EXPLICIT_DIRTY);
//	ck_assert(HLL_IS_EXPLICIT(hll));

	/* there should be one repeated register */
//	ck_assert_int_eq(998, HLL_EXPLICIT_GET_NUM_REGISTERS(hll));
	ck_assert_int_eq(9, HLLCardinality(hll));

//	ck_assert_int_eq(hll->encoding, HLL_EXPLICIT_CLEAN);
}
END_TEST

Suite *
test_hll_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_hll");
	tc = tcase_create("test_hll");
	tcase_set_timeout(tc, 30);
//	tcase_add_test(tc, test_murmurhash64a);
//	tcase_add_test(tc, test_murmurhash64a_varlen);
//	tcase_add_test(tc, test_sparse);
//	tcase_add_test(tc, test_sparse_to_dense);
//	tcase_add_test(tc, test_dense);
//	tcase_add_test(tc, test_union);
//	tcase_add_test(tc, test_union_sparse_and_dense);
//	tcase_add_test(tc, test_card_caching);
//	tcase_add_test(tc, test_copy);
	tcase_add_test(tc, test_explicit);
	suite_add_tcase(s, tc);

	return s;
}
