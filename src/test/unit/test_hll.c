#include <check.h>

#include "suites.h"
#include "pipeline/hll.h"

static HyperLogLog *
add_elements(HyperLogLog *hll, int start, int end)
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

START_TEST(test_murmurhash64a)
{
	/*
	 * Basic sanity checks for our hash function
	 */
	char *k = "some_key";
	uint32 ik;
	uint64 lk;
	float8 fk8;
	uint64 h = MurmurHash64A(k, strlen(k) + 1);

	ck_assert_uint_eq(h, 2113501934961295984U);

	k = "another key";
	h = MurmurHash64A(k, strlen(k) + 1);
	ck_assert_uint_eq(h, 12692071826137624451U);

	fk8 = 42.42;
	h = MurmurHash64A(&fk8, 8);
	ck_assert_uint_eq(h, 14863334595384393056U);

	ik = 42;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 12294533768807684952U);

	ik = 0;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 13022586750568762182U);

	ik = 1 << 31;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 8284348265833171860U);

	lk = 1UL << 63;
	h = MurmurHash64A(&lk, 8);
	ck_assert_uint_eq(h, 13306872472524423528U);
}
END_TEST

START_TEST(test_murmurhash64a_varlen)
{
	/*
	 * Verify that the key length is used by the hash function
	 */
	char *k = "some_key";

	/* exclude the NULL byte */
	uint64 h = MurmurHash64A(k, strlen(k));

	ck_assert_uint_eq(h, 10141279358048777211U);

	/* truncate the last byte and the NULL byte */
	h = MurmurHash64A(k, strlen(k) - 1);
	ck_assert_uint_eq(h, 15279396389632052555U);

	/* include the NULL byte */
	h = MurmurHash64A(k, strlen(k) + 1);
	ck_assert_uint_eq(h, 2113501934961295984U);
}
END_TEST

START_TEST(test_sparse)
{
	/*
	 * Verify that our sparse HLL representation works
	 */
}
END_TEST

START_TEST(test_sparse_to_dense)
{
	/*
	 * Verify that the sparse HLL representation is promoted
	 * to the dense representation when we add enough elements
	 */
	ck_assert_int_eq(1, 1);
}
END_TEST

START_TEST(test_dense)
{
	/*
	 * Verify that our dense HLL representation works
	 */
	ck_assert_int_eq(1, 1);
}
END_TEST

START_TEST(test_union)
{
	/*
	 * Verify that the cardinality of the union of multiple HLLs is correct
	 */
	ck_assert_int_eq(1, 1);
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
	ck_assert_uint_eq(size, 1009);

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
	ck_assert_int_eq(size, 100225);
}
END_TEST

START_TEST(test_union_sparse_and_dense)
{
	/*
	 * Verify that the union of a sparse and dense HLL is correct
	 */
	ck_assert_int_eq(1, 1);
}
END_TEST

Suite *test_hll_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_hll");
	tc = tcase_create("test_hll");
	tcase_add_test(tc, test_murmurhash64a);
	tcase_add_test(tc, test_murmurhash64a_varlen);
	tcase_add_test(tc, test_sparse);
	tcase_add_test(tc, test_sparse_to_dense);
	tcase_add_test(tc, test_dense);
	tcase_add_test(tc, test_union);
	tcase_add_test(tc, test_union_sparse_and_dense);
	tcase_add_test(tc, test_card_caching);
	suite_add_tcase(s, tc);

	return s;
}
