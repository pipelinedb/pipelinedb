#include <check.h>
#include <math.h>
#include <time.h>

#include "suites.h"
#include "pipeline/tdigest.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEBUG 0

static int
int_cmp(const void * a, const void * b)
{
	return *(int *) a - *(int *) b;
}

static int
double_cmp(const void * a, const void * b)
{
	double diff = *(double *) a - *(double *) b;
	if (diff < 0)
		return -1;
	return diff > 0 ? 1 : 0;
}

static int
centroid_cmp(Centroid *a, Centroid *b)
{
	if (a->mean > b->mean)
		return 1;
	if (a->mean < b->mean)
		return -1;
	if (a->id > b->id)
		return 1;
	return 0;
}

static void
check_balance(AVLNode *node)
{
	int l, r;
	if (node->left == NULL)
		return;
	l = node->left->depth;
	r = node->right->depth;

	ck_assert_int_le(Max(l, r) - Min(l, r), 1);
	ck_assert_int_eq(node->left->count + node->right->count, node->count);
	ck_assert_int_eq(node->left->size + node->right->size, node->size);
	ck_assert_int_eq(Max(l, r) + 1, node->depth);
	ck_assert_int_eq(centroid_cmp(node->leaf, AVLNodeFirst(node->right)), 0);
	ck_assert_int_le(centroid_cmp(node->left->leaf, node->right->leaf), 0);
	check_balance(node->left);
	check_balance(node->right);
}

static void
print_centroid(Centroid *c)
{
	printf("Centroid: %f [%d]\n", c->mean, c->id);
}

static void
print_centroids(AVLNode *node)
{
	AVLNodeIterator *it = AVLNodeIteratorCreate(node, NULL);
	int i;

	for (i = 0; i < node->count; i++)
		print_centroid(AVLNodeNext(it));

	AVLNodeIteratorDestroy(it);
}

static void
print_inorder(AVLNode *node)
{
	printf("%*s==\n", node->depth, "");
	if (node->left)
		print_inorder(node->left);
	printf("%*sCentroid: %f [%d] (%d)\n", node->depth, "", node->leaf->mean, node->leaf->id, node->size);
	if (node->right)
		print_inorder(node->right);
	printf("%*s==\n", node->depth, "");
}

START_TEST(test_tree_adds)
{
	AVLNode *root = AVLNodeCreate(NULL, NULL, NULL);
	Centroid *c = CentroidCreate();
	CentroidAddSingle(c, 34);

	ck_assert_ptr_eq(AVLNodeFloor(root, c), NULL);
	ck_assert_ptr_eq(AVLNodeCeiling(root, c), NULL);
	ck_assert_int_eq(AVLNodeHeadCount(root, NULL), 0);
	ck_assert_int_eq(AVLNodeHeadSum(root, NULL), 0);

	c = CentroidCreate();
	CentroidAddSingle(c, 1);
	AVLNodeAdd(root, c);
	CentroidDestroy(c);
	c = CentroidCreate();
	CentroidAddSingle(c, 2);
	CentroidAddSingle(c, 3);
	CentroidAddSingle(c, 4);
	AVLNodeAdd(root, c);
	CentroidDestroy(c);

	ck_assert_int_eq(root->size, 2);
	ck_assert_int_eq(root->count, 4);
}
END_TEST

START_TEST(test_tree_balancing)
{
	AVLNode *root = AVLNodeCreate(NULL, NULL, NULL);
	int i;

	for (i = 0; i <= 100; i++)
	{
		Centroid *c = CentroidCreate();
		CentroidAddSingle(c, i);
		AVLNodeAdd(root, c);
		CentroidDestroy(c);
	}

	check_balance(root);
}
END_TEST

START_TEST(test_tree_iterator)
{
	AVLNode *root = AVLNodeCreate(NULL, NULL, NULL);
	int i;
	AVLNodeIterator *it;
	Centroid *c;

	for (i = 0; i <= 100; i++)
	{
		Centroid *c = CentroidCreate();
		CentroidAddSingle(c, i / 2);
		AVLNodeAdd(root, c);
		CentroidDestroy(c);
	}

	if (DEBUG)
		print_inorder(root);

	ck_assert_int_eq(AVLNodeFirst(root)->mean, 0);
	ck_assert_int_eq(AVLNodeLast(root)->mean, 50);

	it = AVLNodeIteratorCreate(root, NULL);

	for (i = 0; i <= 100; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, i / 2);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 34);
	it = AVLNodeIteratorCreate(root, c);
	CentroidDestroy(c);

	for (i = 68; i <= 100; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, i / 2);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 33);
	it = AVLNodeIteratorCreate(root, c);
	CentroidDestroy(c);

	for (i = 66; i <= 100; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, i / 2);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 34);
	c = CentroidCopy(AVLNodeCeiling(root, c));

	it = AVLNodeIteratorCreate(root, c);
	CentroidDestroy(c);

	for (i = 68; i <= 100; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, i / 2);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 34);
	c = CentroidCopy(AVLNodeFloor(root, c));

	it = AVLNodeIteratorCreate(root, c);
	CentroidDestroy(c);

	for (i = 67; i <= 100; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, i / 2);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);
}
END_TEST

START_TEST(test_tree_remove_and_sums)
{
	AVLNode *root = AVLNodeCreate(NULL, NULL, NULL);
	int i;
	Centroid *c;

	for (i = 0; i <= 100; i++)
	{
		Centroid *c = CentroidCreate();
		CentroidAddSingle(c, i / 2);
		AVLNodeAdd(root, c);
		CentroidDestroy(c);
	}

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 2);
	c = CentroidCopy(AVLNodeCeiling(root, c));
	AVLNodeRemove(root, c);

	check_balance(root);

	CentroidAddSingle(c, 3);
	AVLNodeAdd(root, c);
	CentroidDestroy(c);

	check_balance(root);

	ck_assert_int_eq(0, AVLNodeHeadCount(root, NULL));
	ck_assert_int_eq(0, AVLNodeHeadSum(root, NULL));

	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 0);

	ck_assert_int_eq(0, AVLNodeHeadCount(root, c));
	ck_assert_int_eq(0, AVLNodeHeadSum(root, c));

	CentroidDestroy(c);
	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 1);

	ck_assert_int_eq(2, AVLNodeHeadCount(root, c));
	ck_assert_int_eq(2, AVLNodeHeadSum(root, c));

	CentroidDestroy(c);
	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 2.1);

	ck_assert_int_eq(5, AVLNodeHeadCount(root, c));
	ck_assert_int_eq(5, AVLNodeHeadSum(root, c));

	CentroidDestroy(c);
	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 2.7);

	ck_assert_int_eq(6, AVLNodeHeadCount(root, c));
	ck_assert_int_eq(7, AVLNodeHeadSum(root, c));

	CentroidDestroy(c);
	c = CentroidCreateWithId(0);
	CentroidAddSingle(c, 200);

	ck_assert_int_eq(101, AVLNodeHeadCount(root, c));
	ck_assert_int_eq(102, AVLNodeHeadSum(root, c));

	CentroidDestroy(c);
}
END_TEST

START_TEST(test_tree_rand_balancing)
{
	AVLNode *root = AVLNodeCreate(NULL, NULL, NULL);
	int values[1001];
	int i;
	AVLNodeIterator *it;
	Centroid *c;

	for (i = 0; i <= 1000; i++)
	{
		Centroid *c = CentroidCreate();
		values[i] = rand();
		CentroidAddSingle(c, values[i]);
		AVLNodeAdd(root, c);
		CentroidDestroy(c);
		check_balance(root);
	}

	if (DEBUG)
		print_centroids(root);

	qsort(values, 1001, sizeof(int), int_cmp);

	it = AVLNodeIteratorCreate(root, NULL);

	for (i = 0; i <= 1000; i++)
	{
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, values[i]);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);

	for (i = 0; i <= 100; i++)
	{
		int r = rand() % (1001 - i);
		c = CentroidCreateWithId(0);
		CentroidAddSingle(c, values[r]);
		c = AVLNodeCeiling(root, c);
		AVLNodeRemove(root, c);
		CentroidDestroy(c);
		values[r] = -1;
	}

	if (DEBUG)
		print_centroids(root);

	check_balance(root);
	qsort(values, 1001, sizeof(int), int_cmp);

	it = AVLNodeIteratorCreate(root, NULL);

	for (i = 101; i <= 1000; i++)
	{
		ck_assert_int_ge(values[i], 0);
		c = AVLNodeNext(it);
		ck_assert_int_eq(c->mean, values[i]);
	}

	c = AVLNodeNext(it);
	ck_assert_ptr_eq(c, NULL);

	AVLNodeIteratorDestroy(it);
}
END_TEST

static double
cdf(double x, double *data, int n)
{
	int n1 = 0;
	int n2 = 0;
	int i;

	for (i = 0; i < n; i++)
	{
		double v = data[i];
		n1 += (v < x) ? 1 : 0;
		n2 += (v <= x) ? 1 : 0;
	}
	return (n1 + n2) / 2.0 / n;
}

static void
run_tdigest_test_on_distribution(double (*dist)(void))
{
	TDigest *t = TDigestCreate();
	double quantiles[] = {0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999};
	double x_values[7];
	double *data = (double *) palloc(sizeof(double) * 100000);
	int i;
	int soft_errors = 0;

	for (i = 0; i < 100000; i++)
	{
		data[i] = dist();
		TDigestAddSingle(t, data[i]);
	}

	TDigestCompress(t);
	qsort(data, 100000, sizeof(double), double_cmp);

	for (i = 0; i < 7; i++)
	{
		double ix = 100000 * quantiles[i] - 0.5;
		int index = (int) floor(ix);
		double p = ix - index;
		x_values[i] = data[index] * (1 - p) + data[index + 1] * p;
	}

	ck_assert_int_lt(t->summary->size, 10 * t->compression);

	for (i = 0; i < 7; i++)
	{
		double q = quantiles[i];
		double x = x_values[i];
		double estimate_q = TDigestCDF(t, x);
		double estimate_x = TDigestQuantile(t, q);
		double q_diff = fabs(q - estimate_q);

		ck_assert(q_diff < 0.005);

		if (fabs(cdf(estimate_x, data, 100000) - q) > 0.005)
			soft_errors++;
	}

	ck_assert_int_lt(soft_errors, 3);

	pfree(data);
}

static double
uniform()
{
	return (float) rand() / ((float) rand() / (float) RAND_MAX);
}

/*
 * From: http://phoxis.org/2013/05/04/generating-random-numbers-from-normal-distribution-in-c/
 */
static double
normal()
{
	double U1, U2, W, mult;
	static double X1, X2;
	static int call = 0;
	double mu = 0;
	double sigma = 1;

	if (call == 1)
	{
		call = !call;
		return (mu + sigma * (double) X2);
	}

	do
	{
		U1 = -1 + ((double) rand () / RAND_MAX) * 2;
		U2 = -1 + ((double) rand () / RAND_MAX) * 2;
		W = pow (U1, 2) + pow (U2, 2);
	}
	while (W >= 1 || W == 0);

	mult = sqrt ((-2 * log (W)) / W);
	X1 = U1 * mult;
	X2 = U2 * mult;

	call = !call;

	return (mu + sigma * (double) X1) * rand();
}

START_TEST(test_tdigest)
{
	srand(time(NULL));

	run_tdigest_test_on_distribution(uniform);
	run_tdigest_test_on_distribution(normal);
}
END_TEST

Suite *test_avltree_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("test_avltree");
	tc = tcase_create("test_avltree");
	tcase_set_timeout(tc, 10);
	tcase_add_test(tc, test_tree_adds);
	tcase_add_test(tc, test_tree_balancing);
	tcase_add_test(tc, test_tree_iterator);
	tcase_add_test(tc, test_tree_remove_and_sums);
	tcase_add_test(tc, test_tree_rand_balancing);
	tcase_add_test(tc, test_tdigest_uniform);
	suite_add_tcase(s, tc);

	return s;
}
