#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/tdigest.h"
#include "utils/elog.h"
#include "utils/palloc.h"

static int
float8_cmp(const void * a, const void * b)
{
	float8 diff = *(float8 *) a - *(float8 *) b;
	if (diff < 0)
		return -1;
	return diff > 0 ? 1 : 0;
}

static float8
cdf(float8 x, float8 *data, int n)
{
	int n1 = 0;
	int n2 = 0;
	int i;

	for (i = 0; i < n; i++)
	{
		float8 v = data[i];
		n1 += (v < x) ? 1 : 0;
		n2 += (v <= x) ? 1 : 0;
	}
	return (n1 + n2) / 2.0 / n;
}

static void
run_tdigest_test_on_distribution(float8 (*dist)(void))
{
	TDigest *t = TDigestCreate();
	float8 quantiles[] = {0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999};
	float8 x_values[7];
	float8 *data = (float8 *) palloc(sizeof(float8) * 100000);
	int i;
	int soft_errors = 0;

	for (i = 0; i < 100000; i++)
	{
		data[i] = dist();
		TDigestAdd(t, data[i], 1);
	}

	TDigestCompress(t);

	qsort(data, 100000, sizeof(float8), float8_cmp);

	for (i = 0; i < 7; i++)
	{
		float8 ix = 100000 * quantiles[i] - 0.5;
		int index = (int) floor(ix);
		float8 p = ix - index;
		x_values[i] = data[index] * (1 - p) + data[index + 1] * p;
	}

	for (i = 0; i < 7; i++)
	{
		float8 q = quantiles[i];
		float8 x = x_values[i];
		float8 estimate_q = TDigestCDF(t, x);
		float8 estimate_x = TDigestQuantile(t, q);
		float8 q_diff = fabs(q - estimate_q);
		ck_assert(q_diff < 0.005);

		if (fabs(cdf(estimate_x, data, 100000) - q) > 0.005)
			soft_errors++;
	}

	ck_assert_int_lt(soft_errors, 3);

	pfree(data);
}

static float8
uniform()
{
	return 1.0 * rand();
}

/*
 * From: http://phoxis.org/2013/05/04/generating-random-numbers-from-normal-distribution-in-c/
 */
static float8
gaussian()
{
	float8 U1, U2, W, mult;
	static float8 X1, X2;
	static int call = 0;
	float8 mu = 0;
	float8 sigma = 1;

	if (call == 1)
	{
		call = !call;
		return (mu + sigma * (float8) X2);
	}

	do
	{
		U1 = -1 + ((float8) rand () / RAND_MAX) * 2;
		U2 = -1 + ((float8) rand () / RAND_MAX) * 2;
		W = pow (U1, 2) + pow (U2, 2);
	}
	while (W >= 1 || W == 0);

	mult = sqrt ((-2 * log (W)) / W);
	X1 = U1 * mult;
	X2 = U2 * mult;

	call = !call;

	return rand() * (mu + sigma * (float8) X1);
}

START_TEST(test_tdigest)
{
	srand(time(NULL));

	run_tdigest_test_on_distribution(uniform);
	run_tdigest_test_on_distribution(gaussian);
}
END_TEST

START_TEST(test_tdigest_merge)
{
	TDigest *t0 = TDigestCreate();
	TDigest *t1 = TDigestCreate();
	TDigest *t2 = TDigestCreate();
	int i;
	float8 x;

	for (i = 0; i < 50000; i++)
	{
		x = rand();
		TDigestAdd(t0, x, 1);
		TDigestAdd(t1, x, 1);
	}

	for (i = 0; i < 50000; i++)
	{
		x = rand();
		TDigestAdd(t0, x, 1);
		TDigestAdd(t2, x, 1);
	}

	TDigestMerge(t1, t2);

	for (i = 0; i < 100; i++)
	{
		x = rand();
		ck_assert(fabs(TDigestCDF(t0, x) - TDigestCDF(t1, x)) < 0.01);
	}
}
END_TEST

Suite *test_tdigest_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_tdigest");
	tc = tcase_create("test_tdigest");
	tcase_add_test(tc, test_tdigest);
	tcase_add_test(tc, test_tdigest_merge);
	suite_add_tcase(s, tc);

	return s;
}
