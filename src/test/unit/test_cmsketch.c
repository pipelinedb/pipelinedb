#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/cmsketch.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define EPS 0.002
#define P 0.995

START_TEST(test_basic)
{
	CountMinSketch *cms = CountMinSketchCreateWithEpsAndP(EPS, P);
	int num_keys = 5000;
	int *keys = palloc0(sizeof(int) * num_keys);
	int *counts = palloc0(sizeof(int) * num_keys);
	int i;

	for (i = 0; i < num_keys; i++)
		keys[i] = rand();

	for (i = 0; i < num_keys; i++)
	{
		int c = rand() % 100;
		counts[i] += c;
		CountMinSketchAdd(cms, &keys[i], sizeof(int), c);
	}

	for (i = 0; i < num_keys; i++)
	{
		float diff = CountMinSketchEstimateFrequency(cms, &keys[i], sizeof(int)) - counts[i];
		ck_assert(diff / cms->count < EPS);
	}

}
END_TEST

START_TEST(test_union)
{
	CountMinSketch *cms1 = CountMinSketchCreateWithEpsAndP(EPS, P);
	CountMinSketch *cms2 = CountMinSketchCreateWithEpsAndP(EPS, P);
	CountMinSketch *cms;
	int num_keys = 5000;
	int *keys = palloc(sizeof(int) * num_keys * 2);
	int *counts = palloc0(sizeof(int) * num_keys * 2);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		keys[i] = rand();
		keys[num_keys + i] = rand();
	}

	for (i = 0; i < num_keys; i++)
	{
		counts[i] += rand() % 100;
		counts[num_keys + i] = rand() % 100;
		CountMinSketchAdd(cms1, &keys[i], sizeof(int), counts[i]);
		CountMinSketchAdd(cms2, &keys[num_keys + i], sizeof(int), counts[num_keys + i]);
	}

	cms = CountMinSketchMerge(cms1, cms2);

	for (i = 0; i < num_keys * 2; i++)
	{
		float diff = CountMinSketchEstimateFrequency(cms, &keys[i], sizeof(int)) - counts[i];
		ck_assert(diff / cms->count < EPS);
	}
}
END_TEST

START_TEST(test_false_positives)
{
	CountMinSketch *cms = CountMinSketchCreateWithEpsAndP(EPS, P);
	int range = 1 << 20;
	int num_keys = 1 << 16;
	int *counts = palloc0(sizeof(int) * range);
	int *keys = palloc(sizeof(int) * num_keys);
	int i;
	float num_errors = 0;

	for (i = 0; i < num_keys; i++)
		keys[i] = rand() % range;

	for (i = 0; i < num_keys; i++)
	{
		int c = rand() % 100;
		counts[keys[i]] += c;
		CountMinSketchAdd(cms, &keys[i], sizeof(int), c);
	}

	for (i = 0; i < range; i++)
	{
		float diff = 1.0 * abs(CountMinSketchEstimateFrequency(cms, &i, sizeof(int)) - counts[i]);
		if (diff / cms->count > EPS)
			num_errors++;
	}

	pfree(counts);
	pfree(keys);

	ck_assert(num_errors / range < (1 - P));
}
END_TEST

Suite *test_cmsketch_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_cmsketch");
	tc = tcase_create("test_cmsketch");
	tcase_add_test(tc, test_basic);
	tcase_add_test(tc, test_union);
	tcase_add_test(tc, test_false_positives);
	suite_add_tcase(s, tc);

	return s;
}
