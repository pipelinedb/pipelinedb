#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

static int
int_cmp(const void * a, const void * b)
{
	return *(int *) a - *(int *) b;
}

START_TEST(test_encoding)
{
	GolombCodedSet *gcs = GolombCodedSetCreate();
	int num_keys = 15000;
	int *keys = palloc(sizeof(int) * num_keys * 2);
	int i;
	int j;
	GCSReader *reader;
	ListCell *lc;
	int prev = -1;

	for (i = 0; i < num_keys; i++)
	{
		int key = rand();
		GolombCodedSetAdd(gcs, &key, sizeof(int));
	}

	i = 0;
	foreach(lc, gcs->vals)
	{
		keys[i] = lfirst_int(lc);
		i++;
	}
	qsort(keys, num_keys, sizeof(int), int_cmp);

	gcs = GolombCodedSetCompress(gcs);
	reader = GCSReaderCreate(gcs);

	ck_assert_ptr_eq(gcs->vals, NIL);

	j = 0;
	for (i = 0; i < num_keys; i++)
	{
		if (keys[i] == prev)
			continue;
		ck_assert_int_eq(GCSReaderNext(reader), keys[i]);
		prev = keys[i];
		j++;
	}

	ck_assert_int_eq(gcs->nvals, j);

	/*
	 * Check that encoding works fine when keys are split between the compressed
	 * format and vals List.
	 */
	for (i = 0; i < num_keys; i++)
	{
		int key = rand();
		GolombCodedSetAdd(gcs, &key, sizeof(int));
	}

	i = num_keys;
	foreach(lc, gcs->vals)
	{
		keys[i] = lfirst_int(lc);
		i++;
	}
	qsort(keys, 2 * num_keys, sizeof(int), int_cmp);

	gcs = GolombCodedSetCompress(gcs);
	reader = GCSReaderCreate(gcs);

	ck_assert_ptr_eq(gcs->vals, NIL);

	j = 0;
	for (i = 0; i < 2 * num_keys; i++)
	{
		if (keys[i] == prev)
			continue;
		ck_assert_int_eq(GCSReaderNext(reader), keys[i]);
		prev = keys[i];
		j++;
	}

	ck_assert_int_eq(gcs->nvals, j);
}
END_TEST

START_TEST(test_basic)
{
	GolombCodedSet *gcs = GolombCodedSetCreate();
	int num_keys = 2500;
	int *keys = palloc(sizeof(int) * num_keys);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int key = rand();
		keys[i] = key;
		GolombCodedSetAdd(gcs, &key, sizeof(int));
	}

	gcs = GolombCodedSetCompress(gcs);

	for (i = 0; i < num_keys; i++)
		ck_assert(GolombCodedSetContains(gcs, &keys[i], sizeof(int)));
}
END_TEST

START_TEST(test_union)
{
	GolombCodedSet *gcs1 = GolombCodedSetCreate();
	GolombCodedSet *gcs2 = GolombCodedSetCreate();
	int num_keys = 2500;
	int *keys = palloc(sizeof(int) * num_keys * 2);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int k1 = rand();
		int k2 = rand();
		keys[i] = k1;
		keys[num_keys + i] = k2;
		GolombCodedSetAdd(gcs1, &k1, sizeof(int));
		GolombCodedSetAdd(gcs2, &k2, sizeof(int));
	}

	gcs1 = GolombCodedSetUnion(gcs1, gcs2);

	for (i = 0; i < 2 * num_keys; i++)
		ck_assert(GolombCodedSetContains(gcs1, &keys[i], sizeof(int)));
}
END_TEST

START_TEST(test_intersection)
{
	GolombCodedSet *gcs1 = GolombCodedSetCreate();
	GolombCodedSet *gcs2 = GolombCodedSetCreate();
	int num_keys = 2500;
	int *keys = palloc(sizeof(int) * num_keys);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int k1 = rand();
		int k2 = i % 2 ? k1 : rand();
		keys[i] = k1;
		GolombCodedSetAdd(gcs1, &k1, sizeof(int));
		GolombCodedSetAdd(gcs2, &k2, sizeof(int));
	}

	gcs1 = GolombCodedSetIntersection(gcs1, gcs2);

	for (i = 0; i < num_keys; i++)
		if (i % 2)
			ck_assert(GolombCodedSetContains(gcs1, &keys[i], sizeof(int)));
		else
			ck_assert(!GolombCodedSetContains(gcs1, &keys[i], sizeof(int)));
}
END_TEST

Suite *test_gcs_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_gcs");
	tc = tcase_create("test_gcs");
	tcase_set_timeout(tc, 15);
	tcase_add_test(tc, test_encoding);
	tcase_add_test(tc, test_basic);
	tcase_add_test(tc, test_union);
	tcase_add_test(tc, test_intersection);
	suite_add_tcase(s, tc);

	return s;
}
