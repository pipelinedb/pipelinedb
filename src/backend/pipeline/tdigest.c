/*-------------------------------------------------------------------------
 *
 * tdigest.c
 *
 *	  t-digest implementation based on: https://github.com/tdunning/t-digest
 *
 * src/backend/pipeline/tdigest.h
 *
 */
#include <float.h>
#include <math.h>
#include <stdlib.h>

#include "pipeline/tdigest.h"
#include "nodes/pg_list.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

// TODO(usmanm): Add some ref-counting to Centroids to avoid mindless copying and destroying.

#define DEFAULT_TDIGEST_COMPRESSION 100
#define AVLNodeDepth(node) (Max((node)->left->depth, (node)->right->depth) + 1)

Centroid *
CentroidCreate(void)
{
	return CentroidCreateWithId(rand());
}

Centroid *
CentroidCreateWithId(int id)
{
	Centroid *c = (Centroid *) palloc(sizeof(Centroid));
	c->count = c->mean = 0;
	c->id = id;
	return c;
}

void
CentroidDestroy(Centroid *c)
{
	if (c == NULL)
		return;
	pfree(c);
}

void
CentroidAdd(Centroid *c, float8 x, int64 w)
{
	c->count += w;
	c->mean += w * (x - c->mean) / c->count;
}

void
CentroidAddSingle(Centroid *c, float8 x)
{
	CentroidAdd(c, x, 1);
}

Centroid *
CentroidCopy(Centroid *c)
{
	Centroid *cpy = (Centroid *) palloc(sizeof(Centroid));
	memcpy(cpy, c, sizeof(Centroid));
	return cpy;
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
	if (a->id < b->id)
		return -1;
	return 0;
}

static void
rotate(AVLNode *node, AVLNode *a, AVLNode *b, AVLNode *c, AVLNode *d)
{
	if (node->left != a)
		AVLNodeDestroy(node->left);
	if (node->right != d)
		AVLNodeDestroy(node->right);
	node->left = AVLNodeCreate(a, b, NULL);
	node->right = AVLNodeCreate(c, d, NULL);
	node->count = node->left->count + node->right->count;
	node->size = node->left->size + node->right->size;
	node->depth = AVLNodeDepth(node);
	CentroidDestroy(node->leaf);
	node->leaf = CentroidCopy(AVLNodeFirst(node->right));
}

static void
rebalance(AVLNode *node)
{
	int l = node->left->depth;
	int r = node->right->depth;
	if (l > r + 1)
	{
		if (node->left->left->depth > node->left->right->depth)
			rotate(node, node->left->left->left, node->left->left->right, node->left->right, node->right);
		else
			rotate(node, node->left->left, node->left->right->left, node->left->right->right, node->right);
	}
	else if (r > l + 1)
	{
		if (node->right->left->depth > node->right->right->depth)
			rotate(node, node->left, node->right->left->left, node->right->left->right, node->right->right);
		else
			rotate(node, node->left, node->right->left, node->right->right->left, node->right->right->right);
	}

	node->depth = AVLNodeDepth(node);
}

AVLNode *
AVLNodeCreate(AVLNode *left, AVLNode *right, Centroid *leaf)
{
	AVLNode *node = (AVLNode *) palloc(sizeof(AVLNode));

	if (left != NULL)
	{
		node->left = left;
		node->right = right;
		node->count = left->count + right->count;
		node->size = left->size + right->size;
		node->depth = AVLNodeDepth(node);
		node->leaf = CentroidCopy(AVLNodeFirst(right));
		rebalance(node);
	}
	else if (leaf != NULL)
	{
		Assert(left == right == NULL);
		node->count = leaf->count;
		node->depth = node->size = 1;
		node->left = node->right = NULL;
		node->leaf = CentroidCopy(leaf);
	}
	else
	{
		node->count = node->depth = node->size = 0;
		node->left = node->right = NULL;
		node->leaf = NULL;
	}

	return node;
}

void
AVLNodeDestroy(AVLNode *node)
{
	if (node == NULL)
		return;
	CentroidDestroy(node->leaf);
	pfree(node);
}

void
AVLNodeDestroyTree(AVLNode *root)
{
	if (root->left)
		AVLNodeDestroyTree(root->left);
	if (root->right)
		AVLNodeDestroyTree(root->right);
	AVLNodeDestroy(root);
}

void
AVLNodeAdd(AVLNode *node, Centroid *c)
{
	c = CentroidCopy(c);
	if (node->size == 0)
	{
		node->leaf = c;
		node->count = c->count;
		node->size = node->depth = 1;
		return;
	}

	if (node->size == 1)
	{
		int cmp = centroid_cmp(c, node->leaf);
		if (cmp < 0)
		{
			node->left = AVLNodeCreate(NULL, NULL, c);
			node->right = AVLNodeCreate(NULL, NULL, CentroidCopy(node->leaf));
		}
		else
		{
			node->left = AVLNodeCreate(NULL, NULL, node->leaf);
			node->right = AVLNodeCreate(NULL, NULL, c);
			node->leaf = CentroidCopy(c);
		}
	}
	else if (centroid_cmp(c, node->leaf) < 0)
		AVLNodeAdd(node->left, c);
	else
		AVLNodeAdd(node->right, c);

	node->count += c->count;
	node->size++;
	node->depth = AVLNodeDepth(node);
	rebalance(node);
}

void
AVLNodeRemove(AVLNode *node, Centroid *c)
{
	if (node->size == 0)
		elog(ERROR, "trying to remove centroid from empty tree");
	if (node->size == 1)
	{
		if (centroid_cmp(node->leaf, c) != 0)
			elog(ERROR, "trying to remove centroid that is not present");
		node->count = node->size = node->depth = 0;
		node->leaf = NULL;
	}
	else
	{
		int size;

		if (centroid_cmp(c, node->leaf) < 0)
		{
			size = node->left->size;
			AVLNodeRemove(node->left, c);

			if (size > 1)
			{
				node->count -= c->count;
				node->size--;
				rebalance(node);
			}
			else
			{
				AVLNode *right = node->right;
				AVLNodeDestroy(node->left);
				memcpy(node, right, sizeof(AVLNode));
				node->leaf = CentroidCopy(right->leaf);
				AVLNodeDestroy(right);
			}
		}
		else
		{
			size = node->right->size;
			AVLNodeRemove(node->right, c);

			if (size > 1)
			{
				CentroidDestroy(node->leaf);
				node->leaf = CentroidCopy(AVLNodeFirst(node->right));
				node->count -= c->count;
				node->size--;
				rebalance(node);
			}
			else
			{
				AVLNode *left = node->left;
				AVLNodeDestroy(node->right);
				memcpy(node, left, sizeof(AVLNode));
				node->leaf = CentroidCopy(left->leaf);
				AVLNodeDestroy(left);
			}
		}
	}
}

int
AVLNodeHeadSize(AVLNode *node, Centroid *c)
{
	if (c == NULL)
		c = AVLNodeFirst(node);
	if (node->size == 0)
		return 0;
	if (node->left == NULL)
		return centroid_cmp(node->leaf, c) < 0 ? node->size : 0;
	if (centroid_cmp(c, node->leaf) < 0)
		return AVLNodeHeadSize(node->left, c);
	return node->left->size + AVLNodeHeadSize(node->right, c);
}

int64
AVLNodeHeadCount(AVLNode *node, Centroid *c)
{
	if (c == NULL)
		c = AVLNodeFirst(node);
	if (node->size == 0)
		return 0;
	if (node->left == NULL)
		return centroid_cmp(node->leaf, c) < 0 ? node->count : 0;
	if (centroid_cmp(c, node->leaf) < 0)
		return AVLNodeHeadCount(node->left, c);
	return node->left->count + AVLNodeHeadCount(node->right, c);
}

Centroid *
AVLNodeFirst(AVLNode *node)
{
	if (node->left == NULL)
		return node->leaf;
	return AVLNodeFirst(node->left);
}

Centroid *
AVLNodeLast(AVLNode *node)
{
	if (node->right == NULL)
		return node->leaf;
	return AVLNodeLast(node->right);
}

Centroid *
AVLNodeFloor(AVLNode *node, Centroid *c)
{
	Centroid *floor;
	if (node->size == 0)
		return NULL;
	if (node->size == 1)
		return centroid_cmp(c, node->leaf) >= 0 ? node->leaf : NULL;
	if (centroid_cmp(c, node->leaf) < 0)
		return AVLNodeFloor(node->left, c);
	floor = AVLNodeFloor(node->right, c);
	if (floor != NULL)
		return floor;
	return AVLNodeLast(node->left);
}

Centroid *
AVLNodeCeiling(AVLNode *node, Centroid *c)
{
	Centroid *ceil;
	if (node->size == 0)
		return NULL;
	if (node->size == 1)
		return centroid_cmp(c, node->leaf) <= 0 ? node->leaf : NULL;
	if (centroid_cmp(c, node->leaf) >= 0)
		return AVLNodeCeiling(node->right, c);
	ceil = AVLNodeCeiling(node->left, c);
	if (ceil != NULL)
		return ceil;
	return AVLNodeFirst(node->right);
}

AVLNodeIterator *
AVLNodeIteratorCreate(AVLNode *node, Centroid *start)
{
	AVLNodeIterator *it = (AVLNodeIterator *) palloc(sizeof(AVLNodeIterator));
	it->initialized = false;
	it->stack = NIL;
	it->node = node;
	it->start = start ? CentroidCopy(start) : NULL;
	return it;
}

void
AVLNodeIteratorDestroy(AVLNodeIterator *it)
{
	list_free(it->stack);
	pfree(it);
}

static void
push(AVLNode *node, Centroid *c, AVLNodeIterator *it)
{
	while (node->left != NULL)
	{
		if (c == NULL || centroid_cmp(c, node->leaf) <= 0)
		{
			it->stack = list_concat(list_make1(node->right), it->stack);
			node = node->left;
		}
		else
			node = node->right;
	}

	if (c == NULL || centroid_cmp(c, node->leaf) <= 0)
		it->stack = list_concat(list_make1(node), it->stack);
}

Centroid *
AVLNodeNext(AVLNodeIterator *it)
{
	AVLNode *r = NULL;

	if (it->node->size == 0)
		return NULL;

	if (!it->initialized)
	{
		push(it->node, it->start, it);
		it->initialized = true;
	}

	while (list_length(it->stack) > 0)
	{
		r = (AVLNode *) linitial(it->stack);
		it->stack = list_delete(it->stack, r);
		if (r->left == NULL)
			break;
		push(r, it->start, it);
	}

	if (r != NULL)
		return r->leaf;

	return NULL;
}

TDigest *
TDigestCreate(void)
{
	return TDigestCreateWithCompression(DEFAULT_TDIGEST_COMPRESSION);
}

TDigest *
TDigestCreateWithCompression(int compression)
{
	TDigest *t = palloc(sizeof(TDigest));
	t->compression = compression;
	t->count = 0;
	t->summary = AVLNodeCreate(NULL, NULL, NULL);
	return t;
}

void
TDigestDestroy(TDigest *t)
{
	AVLNodeDestroyTree(t->summary);
	pfree(t);
}

void
TDigestAdd(TDigest *t, float8 x, int64 w)
{
	Centroid *c;
	Centroid *start;
	AVLNodeIterator *it;
	float8 min_dist;
	int last_neighbor;
	int i;
	Centroid *closest;
	int64 sum;
	int count;
	float n;
	float8 z;

	c = CentroidCreateWithId(0);
	CentroidAdd(c, x, w);

	start = AVLNodeFloor(t->summary, c);
	if (start == NULL)
		start = AVLNodeCeiling(t->summary, c);

	if (start == NULL)
	{
		c->id = rand();
		AVLNodeAdd(t->summary, c);
		t->count = c->count;
		CentroidDestroy(c);
		return;
	}

	CentroidDestroy(c);

	it = AVLNodeIteratorCreate(t->summary, start);
	min_dist = DBL_MAX;
	last_neighbor = 0;
	count = AVLNodeHeadSize(t->summary, start);
	i = count;

	while (true)
	{
		c = AVLNodeNext(it);
		if (c == NULL)
			break;

		z = fabs(c->mean - x);
		if (z < min_dist)
		{
			min_dist = z;
			start = c;
		}
		else if (z > min_dist)
		{
			last_neighbor = i;
			break;
		}
		i++;
	}

	AVLNodeIteratorDestroy(it);

	it = AVLNodeIteratorCreate(t->summary, start);
	closest = NULL;
	sum = AVLNodeHeadCount(t->summary, start);
	i = AVLNodeHeadSize(t->summary, start);
	n = 1;

	while (true)
	{
		float8 q, k;

		c = AVLNodeNext(it);
		if (i > last_neighbor)
			break;
		q = t->count == 1 ? 0.5 : (sum + (c->count - 1) / 2.0) / (t->count -1 );
		k = 4 * t->count * q * (1 - q) / t->compression;

		if (c->count + w <= k)
		{
			if (((float) rand() / (float) RAND_MAX) < 1 / n)
				closest = c;
			n++;
		}

		sum += c->count;
		i++;
	}

	AVLNodeIteratorDestroy(it);

	if (closest == NULL)
		c = CentroidCreate();
	else
	{
		c = CentroidCopy(c);
		AVLNodeRemove(t->summary, c);
	}

	CentroidAdd(c, x, w);
	AVLNodeAdd(t->summary, c);
	CentroidDestroy(c);

	t->count += w;

	if (t->summary->size > 20 * t->compression)
		TDigestCompress(t);
}

void
TDigestAddSingle(TDigest *t, float8 x)
{
	TDigestAdd(t, x, 1);
}

static void
shuffle(Centroid **array, int n)
{
	if (n > 1)
	{
		int i;
		for (i = 0; i < n - 1; i++)
		{
			int j = i + rand() / (RAND_MAX / (n - i) + 1);
			Centroid *ptr = array[j];
			array[j] = array[i];
			array[i] = ptr;
		}
	}
}

void
TDigestCompress(TDigest *t)
{
	TDigest *t_tmp = TDigestCreateWithCompression(t->compression);
	int size = t->summary->size;
	Centroid **centroids = palloc(sizeof(Centroid *) * size);
	Centroid *c;
	int i;
	AVLNodeIterator *it = AVLNodeIteratorCreate(t->summary, NULL);

	for (i = 0; i < size; i++)
		centroids[i] = AVLNodeNext(it);

	AVLNodeIteratorDestroy(it);

	shuffle(centroids, size);

	for (i = 0; i < size; i++)
	{
		c = centroids[i];
		TDigestAdd(t_tmp, c->mean, c->count);
	}

	AVLNodeDestroyTree(t->summary);
	t->summary = t_tmp->summary;
	pfree(centroids);
	pfree(t_tmp);
}

TDigest *
TDigestMerge(TDigest *t1, TDigest *t2)
{
	TDigest *t = TDigestCreateWithCompression(Max(t1->compression, t2->compression));
	int size = t1->summary->size + t2->summary->size;
	Centroid **centroids = palloc(sizeof(Centroid *) * size);
	Centroid *c;
	int i;
	int j = 0;
	AVLNodeIterator *it;

	it = AVLNodeIteratorCreate(t1->summary, NULL);

	for (i = 0; i < t1->summary->size; i++)
	{
		centroids[j] = AVLNodeNext(it);
		j++;
	}

	AVLNodeIteratorDestroy(it);

	it = AVLNodeIteratorCreate(t2->summary, NULL);

	for (i = 0; i < t2->summary->size; i++)
	{
		centroids[j] = AVLNodeNext(it);
		j++;
	}

	AVLNodeIteratorDestroy(it);

	shuffle(centroids, size);

	for (i = 0; i < size; i++)
	{
		c = centroids[i];
		TDigestAdd(t, c->mean, c->count);
	}

	pfree(centroids);

	return t;
}

static float8
interpolate(float8 x, float8 x0, float8 x1)
{
	return (x - x0) / (x1 - x0);
}

float8
TDigestCDF(TDigest *t, float8 x)
{
	AVLNode *summary = t->summary;
	float8 r = 0;
	AVLNodeIterator *it;
	Centroid *a, *b, *next;
	float8 left, right;

	if (summary->size == 0)
		return NAN;
	else if (summary->size == 1)
		return x < AVLNodeFirst(summary)->mean ? 0: 1;

	it = AVLNodeIteratorCreate(summary, NULL);
	a = AVLNodeNext(it);
	b = AVLNodeNext(it);
	left = (b->mean - a->mean) / 2;
	right = left;

	while ((next = AVLNodeNext(it)))
	{
		if (x < a->mean + right)
			return (r + a->count * interpolate(x, a->mean - left, a->mean + right)) / t->count;
		r += a->count;
		a = b;
		b = next;
		left = right;
		right = (b->mean - a->mean) / 2;
	}

	left = right;
	a = b;
	if (x < a->mean + right)
		return (r + a->count * interpolate(x, a->mean - left, a->mean + right)) / t->count;

	return 1;
}

float8
TDigestQuantile(TDigest *t, float8 q)
{
	AVLNode *summary = t->summary;
	AVLNodeIterator *it;
	Centroid *center, *leading, *next;
	float8 left, right, r;

	if (q < 0 || q > 1)
		elog(ERROR, "q should be in [0, 1], got %f", q);

	if (summary->size == 0)
		return NAN;
	else if (summary->size == 1)
		return AVLNodeFirst(summary)->mean;

	it = AVLNodeIteratorCreate(summary, NULL);
	center = AVLNodeNext(it);
	leading = AVLNodeNext(it);
	next = AVLNodeNext(it);
	right = (leading->mean - center->mean) / 2;

	if (next == NULL)
	{
		if (q > 0.75)
			return leading->mean + right * (4 * q - 3);
		else
			return center->mean + right * (4 * q - 1);
	}

	q *= t->count;
	left = right;
	r = center->count;

	do
	{
		if (r + center->count / 2 >= q)
			return center->mean - left * 2.0 * (q - r) / center->count;
		if (r + leading->count >= q)
			return center->mean + right * 2.0 * (center->count - (q - r)) / center->count;
		r += center->count;
		center = leading;
		leading = next;
		left = right;
		right = (leading->mean - center->mean) / 2;
	} while ((next = AVLNodeNext(it)));

	center = leading;
	left = right;

	if (r + center->count / 2 >= q)
		return center->mean - left * 2.0 * (q - r) / center->count;

	return center->mean + right * 2.0 * (center->count - (q - r)) / center->count;
}
