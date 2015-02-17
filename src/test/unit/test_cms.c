#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/cms.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

START_TEST(test_basic)
{
	CountMinSketch *cms = CountMinSketchCreate();
	int num_keys = 1000;
	int *keys = palloc(sizeof(int) * num_keys);
	int *counts = palloc(sizeof(int) * num_keys);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int key = rand();
		keys[i] = key;
		counts[i] = 0;
	}

	for (i = 0; i < 10 * num_keys; i++)
	{
		int idx = rand() % num_keys;
		int c = rand() % 10;
		counts[idx] += c;
		CountMinSketchAdd(cms, &keys[idx], sizeof(int), c);
	}

	for (i = 0; i < num_keys; i++)
	{
		int idx = rand() % num_keys;
		int c = CountMinSketchEstimateCount(cms, &keys[idx], sizeof(int));
	}

	for (i = 0; i < cms->d * cms->w; i++)
		elog(LOG, "count %d", cms->table[i]);
}
END_TEST

START_TEST(test_union)
{

}
END_TEST

START_TEST(test_false_positives)
{

}
END_TEST

Suite *test_cms_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_cms");
	tc = tcase_create("test_cms");
	tcase_add_test(tc, test_basic);
	tcase_add_test(tc, test_union);
	tcase_add_test(tc, test_false_positives);
	suite_add_tcase(s, tc);

	return s;
}
