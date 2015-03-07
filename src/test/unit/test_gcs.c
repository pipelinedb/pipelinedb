#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

START_TEST(test_compression)
{
//	GCSReaderNext *gcs = GolombCodedSetCreate();
//	int num_keys = 25000;
//	int i;
//	List *vals;
//	ListCell *lc;
//
//	for (i = 0; i < num_keys; i++)
//	{
//		int key = rand();
//		GolombCodedSetAdd(gcs, &key, sizeof(int));
//	}
//
//	vals = list_copy(gcs->vals);
}
END_TEST

Suite *test_gcs_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_gcs");
	tc = tcase_create("test_gcs");
	tcase_add_test(tc, test_compression);
	suite_add_tcase(s, tc);

	return s;
}
