#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/bloom.h"
#include "utils/elog.h"
#include "utils/palloc.h"

START_TEST(test_basic)
{
	BloomFilter *bf = BloomFilterCreate();
	int num_keys = 100000;
	int *keys = palloc(sizeof(int) * num_keys);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int key = rand();
		keys[i] = key;
		BloomFilterAdd(bf, key);
	}

	for (i = 0; i < num_keys; i++)
		ck_assert(BloomFilterContains(bf, keys[i]));
}
END_TEST

START_TEST(test_union)
{
	BloomFilter *bf1 = BloomFilterCreate();
	BloomFilter *bf2 = BloomFilterCreate();
	int num_keys = 100000;
	int *keys = palloc(sizeof(int) * num_keys * 2);
	int i;

	for (i = 0; i < num_keys; i++)
	{
		int k1 = rand();
		int k2 = rand();
		keys[i] = k1;
		keys[num_keys + i] = k2;
		BloomFilterAdd(bf1, k1);
		BloomFilterAdd(bf2, k2);
	}

	bf1 = BloomFilterUnion(bf1, bf2);

	for (i = 0; i < num_keys * 2; i++)
		ck_assert(BloomFilterContains(bf1, keys[i]));
}
END_TEST

Suite *test_bloom_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_bloom");
	tc = tcase_create("test_bloom");
	tcase_add_test(tc, test_basic);
	tcase_add_test(tc, test_union);
	suite_add_tcase(s, tc);

	return s;
}
