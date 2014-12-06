#include <check.h>

#include "suites.h"
#include "pipeline/hll.h"

START_TEST(test_murmurhash64a)
{
	char *k = "some_key";
	uint32 ik;
	uint64 lk;
	float8 fk8;
	uint64 h = MurmurHash64A(k, strlen(k) + 1, 0);

	ck_assert_uint_eq(h, 14613800511125978524U);

	k = "another key";
	h = MurmurHash64A(k, strlen(k) + 1, 0);
	ck_assert_uint_eq(h, 14572063269992335582U);

	fk8 = 42.42;
	h = MurmurHash64A(&fk8, 8, 0);
	ck_assert_uint_eq(h, 749110323020550257U);

	ik = 42;
	h = MurmurHash64A(&ik, 4, 0);
	ck_assert_uint_eq(h, 1957777661270593380U);

	ik = 0;
	h = MurmurHash64A(&ik, 4, 0);
	ck_assert_uint_eq(h, 2224014147481998463U);

	ik = 1 << 31;
	h = MurmurHash64A(&ik, 4, 0);
	ck_assert_uint_eq(h, 488473488423332433U);

	lk = 1UL << 63;
	h = MurmurHash64A(&lk, 8, 0);
	ck_assert_uint_eq(h, 4903226226401862699U);
}
END_TEST

START_TEST(test_sparse)
{
	ck_assert_int_eq(1, 1);
}
END_TEST

START_TEST(test_dense)
{
	ck_assert_int_eq(1, 1);
}
END_TEST

START_TEST(test_low_cardinality)
{
	ck_assert_int_eq(1, 1);
}
END_TEST

START_TEST(test_union)
{
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
	tcase_add_test(tc, test_sparse);
	tcase_add_test(tc, test_dense);
	tcase_add_test(tc, test_low_cardinality);
	tcase_add_test(tc, test_union);
	suite_add_tcase(s, tc);

	return s;
}
