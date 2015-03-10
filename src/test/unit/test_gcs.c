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

	ck_assert_int_eq(gcs->nvals, num_keys);
	ck_assert_ptr_eq(gcs->vals, NIL);

	for (i = 0; i < num_keys; i++)
	{
		if (keys[i] == prev)
			continue;
		ck_assert_int_eq(GCSReaderNext(reader), keys[i]);
		prev = keys[i];
	}

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

	ck_assert_int_eq(gcs->nvals, 2 * num_keys);
	ck_assert_ptr_eq(gcs->vals, NIL);

	for (i = 0; i < 2 * num_keys; i++)
	{
		if (keys[i] == prev)
			continue;
		ck_assert_int_eq(GCSReaderNext(reader), keys[i]);
		prev = keys[i];
	}
}
END_TEST

Suite *test_gcs_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_gcs");
	tc = tcase_create("test_gcs");
	tcase_add_test(tc, test_encoding);
	suite_add_tcase(s, tc);

	return s;
}
