/*
 * test_fss.c
 *
 *  Created on: Aug 20, 2015
 *      Author: usmanm
 */
#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "postgres.h"
#include "catalog/pg_type.h"
#include "pipeline/fss.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define NUM_ITEMS 100000
#define K 10

static TypeCacheEntry *
get_int8_type()
{
	TypeCacheEntry *typ = palloc0(sizeof(TypeCacheEntry));
	typ->typbyval = true;
	typ->type_id = INT8OID;
	typ->typlen = sizeof(int64);

	return typ;
}

static void
assert_sorted(FSS *fss)
{
	int i;
	MonitoredElement *prev = NULL;
	bool saw_unset = false;

	for (i = 0; i < fss->m; i++)
	{
		MonitoredElement *elt = &fss->monitored_elements[i];

		if (!IS_SET(elt))
		{
			saw_unset = true;
			continue;
		}

		if (saw_unset)
			goto fail;

		if (!prev)
			continue;

		if (elt->frequency > prev->frequency)
			goto fail;

		if (elt->frequency == prev->frequency && elt->error < prev->error)
			goto fail;

		prev = elt;
	}

	return;

fail:
	FSSPrint(fss);
	ck_assert(false);
}

START_TEST(test_basic)
{
	TypeCacheEntry *typ = get_int8_type();
	FSS *fss = FSSCreate(K, typ);
	int i;
	int *counts = palloc0(sizeof(uint64_t) * 1000);
	Datum *values;
	uint64_t *freqs;
	int soft_errors;

	for (i = 0; i < NUM_ITEMS; i++)
	{
		int value = 10 * gaussian();
		value %= 500;
		FSSIncrement(fss, value, false);
		counts[value + 500]++;
		assert_sorted(fss);
	}

	values = FSSTopK(fss, K, NULL, NULL);
	freqs = FSSTopKCounts(fss, K, NULL);

	soft_errors = 0;
	for (i = 0; i < 10; i++)
	{
		int value = values[i];

		if (abs(freqs[i] - counts[value + 500]) > 2)
			soft_errors++;
	}

	ck_assert(soft_errors < 3);
}
END_TEST

START_TEST(test_merge)
{
	TypeCacheEntry *typ = get_int8_type();
	FSS *fss1 = FSSCreate(K, typ);
	FSS *fss2 = FSSCreate(K, typ);
	int i, j;
	Datum *values1, *values2;
	uint64_t *freqs1, *freqs2;
	int soft_errors;

	for (j = 0; j < 10; j++)
	{
		FSS *tmp = FSSCreate(K, typ);

		for (i = 0; i < NUM_ITEMS; i++)
		{
			int value = 10 * gaussian();
			value %= 500;
			FSSIncrement(fss1, value, false);
			FSSIncrement(tmp, value, false);
			assert_sorted(fss1);
			assert_sorted(tmp);
		}

		fss2 = FSSMerge(fss2, tmp);
		FSSDestroy(tmp);
	}

	values1 = FSSTopK(fss1, K, NULL, NULL);
	freqs1 = FSSTopKCounts(fss1, K, NULL);
	values2 = FSSTopK(fss2, K, NULL, NULL);
	freqs2 = FSSTopKCounts(fss2, K, NULL);
	soft_errors = 0;

	for (i = 0; i < 10; i++)
	{
		int value1 = values1[i];
		int value2 = values2[i];

		ck_assert(value1 == value2);

		if (abs(freqs1[i] - freqs2[i]) > 10)
			soft_errors++;
	}

	ck_assert(soft_errors < 3);
}
END_TEST

START_TEST(test_error)
{
	TypeCacheEntry *typ = get_int8_type();
	FSS *fss = FSSCreate(K, typ);
	int i;
	int *counts = palloc0(sizeof(uint64_t) * 1000);
	Datum *values;
	uint64_t *freqs;
	int min_freq = NUM_ITEMS / fss->m;

	for (i = 0; i <= min_freq; i++)
	{
		FSSIncrement(fss, 1, false);
		FSSIncrement(fss, 2, false);
		FSSIncrement(fss, 3, false);
		counts[1]++;
		counts[2]++;
		counts[3]++;
		assert_sorted(fss);
	}

	for (i = 0; i < NUM_ITEMS - (2 * min_freq); i++)
	{
		int value = (int) uniform() % 500 + 4;
		FSSIncrement(fss, value, false);
		counts[value]++;
		assert_sorted(fss);
	}

	values = FSSTopK(fss, K, NULL, NULL);
	freqs = FSSTopKCounts(fss, K, NULL);

	for (i = 0; i < 3; i++)
	{
		int value = values[i];
		ck_assert(value == 1 || value == 2 || value == 3);
		ck_assert(abs(freqs[i] - counts[value]) == 0);
	}
}
END_TEST

START_TEST(test_weighted)
{
	TypeCacheEntry *typ = get_int8_type();
	FSS *fss = FSSCreate(K, typ);
	int i;
	int *counts = palloc0(sizeof(uint64_t) * 1000);
	Datum *values;
	uint64_t *freqs;
	int min_freq = NUM_ITEMS / fss->m;

	for (i = 0; i <= min_freq; i++)
	{
		fss = FSSIncrementWeighted(fss, 1, false, 10);
		fss = FSSIncrementWeighted(fss, 2, false, 20);
		fss = FSSIncrementWeighted(fss, 3, false, 30);
		counts[1] += 10;
		counts[2] += 20;
		counts[3] += 30;
		assert_sorted(fss);
	}

	for (i = 0; i < NUM_ITEMS - (2 * min_freq); i++)
	{
		int value = (int) uniform() % 500 + 4;
		fss = FSSIncrementWeighted(fss, value, false, 1);
		counts[value]++;
		assert_sorted(fss);
	}

	values = FSSTopK(fss, K, NULL, NULL);
	freqs = FSSTopKCounts(fss, K, NULL);

	for (i = 0; i < 3; i++)
	{
		int value = values[i];
		ck_assert(value == 1 || value == 2 || value == 3);
		ck_assert(abs(freqs[i] - counts[value]) == 0);
	}
}
END_TEST

Suite *
test_fss_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_fss");
	tc = tcase_create("test_fss");
	tcase_set_timeout(tc, 3000);
	tcase_add_test(tc, test_basic);
	tcase_add_test(tc, test_merge);
	tcase_add_test(tc, test_error);
	tcase_add_test(tc, test_weighted);
	suite_add_tcase(s, tc);

	return s;
}
