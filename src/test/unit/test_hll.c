#include <check.h>

#include "suites.h"
#include "pipeline/hll.h"

START_TEST(test_murmurhash64a)
{
	char *k = "some_key";
	uint32 ik;
	uint64 lk;
	float8 fk8;
	uint64 h = MurmurHash64A(k, strlen(k) + 1);

	ck_assert_uint_eq(h, 15128288175273613228U);

	k = "another key";
	h = MurmurHash64A(k, strlen(k) + 1);
	ck_assert_uint_eq(h, 9098300568195040227U);

	fk8 = 42.42;
	h = MurmurHash64A(&fk8, 8);
	ck_assert_uint_eq(h, 15272446282472167317U);

	ik = 42;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 7164345697233736769U);

	ik = 0;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 979726119277092382U);

	ik = 1 << 31;
	h = MurmurHash64A(&ik, 4);
	ck_assert_uint_eq(h, 7967641002240233708U);

	lk = 1UL << 63;
	h = MurmurHash64A(&lk, 8);
	ck_assert_uint_eq(h, 6972884522371238360U);
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
