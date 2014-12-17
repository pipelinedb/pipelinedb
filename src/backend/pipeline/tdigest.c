/*-------------------------------------------------------------------------
 *
 * tdigest.c
 *
 *	  t-digest implementation based on: https://github.com/tdunning/t-digest
 *
 * src/backend/pipeline/tdigest.h
 *
 */
#include <math.h>
#include <stdlib.h>

#include "pipeline/tdigest.h"
#include "nodes/pg_list.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include "utils/memutils.h"

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
CentroidAdd(Centroid *c, double x, int w)
{
	c->count += w;
	c->mean += w * (x - c->mean) / c->count;
}

void
CentroidAddSingle(Centroid *c, double x)
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
AVLNodeHeadCount(AVLNode *node, Centroid *c)
{
	if (c == NULL)
		c = AVLNodeFirst(node);
	if (node->size == 0)
		return 0;
	if (node->left == NULL)
		return centroid_cmp(node->leaf, c) < 0 ? node->size : 0;
	if (centroid_cmp(c, node->leaf) < 0)
		return AVLNodeHeadCount(node->left, c);
	return node->left->size + AVLNodeHeadCount(node->right, c);
}

int
AVLNodeHeadSum(AVLNode *node, Centroid *c)
{
	if (c == NULL)
		c = AVLNodeFirst(node);
	if (node->size == 0)
		return 0;
	if (node->left == NULL)
		return centroid_cmp(node->leaf, c) < 0 ? node->count : 0;
	if (centroid_cmp(c, node->leaf) < 0)
		return AVLNodeHeadSum(node->left, c);
	return node->left->count + AVLNodeHeadSum(node->right, c);
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
