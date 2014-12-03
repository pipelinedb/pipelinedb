#include <check.h>

#include "suites.h"
#include "pipeline/hll.h"


START_TEST(test_basic)
{
	ck_assert_int_eq(1, 1);
}
END_TEST

Suite *test_hll_suite(void)
{
	Suite *s;
	TCase *tc_core;

	s = suite_create("test_hll");
	tc_core = tcase_create("test_hll_core");
	tcase_add_test(tc_core, test_basic);
	suite_add_tcase(s, tc_core);

	return s;
}
