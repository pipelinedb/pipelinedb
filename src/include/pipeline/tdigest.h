/*-------------------------------------------------------------------------
 *
 * tdigest.h
 *	  Interface for t-digest support
 *
 *
 * src/include/pipeline/tdigest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TDIGEST_H
#define PIPELINE_TDIGEST_H

#include "c.h"
#include "nodes/pg_list.h"

typedef struct Centroid
{
	int64 count;
	float8 mean;
	int id;
} Centroid;

Centroid *CentroidCreate(void);
Centroid *CentroidCreateWithId(int id);
void CentroidDestroy(Centroid *c);
void CentroidAdd(Centroid *c, float8 x, int64 w);
void CentroidAddSingle(Centroid *c, float8 x);
Centroid *CentroidCopy(Centroid *c);

typedef struct AVLNode
{
	int64 count;
	int size;
	int depth;
	struct AVLNode *left;
	struct AVLNode *right;
	Centroid *leaf;
} AVLNode;

typedef struct AVLNodeIterator
{
	bool initialized;
	List *stack;
	AVLNode *node;
	Centroid *start;
} AVLNodeIterator;

AVLNode *AVLNodeCreate(AVLNode *left, AVLNode *right, Centroid *leaf);
void AVLNodeDestroy(AVLNode *node);
void AVLNodeDestroyTree(AVLNode *root);
void AVLNodeAdd(AVLNode *node, Centroid *c);
void AVLNodeRemove(AVLNode *node, Centroid *c);
int AVLNodeHeadSize(AVLNode *node, Centroid *c);
int64 AVLNodeHeadCount(AVLNode *node, Centroid *c);
Centroid *AVLNodeFirst(AVLNode *node);
Centroid *AVLNodeLast(AVLNode *node);
Centroid *AVLNodeFloor(AVLNode *node, Centroid *c);
Centroid *AVLNodeCeiling(AVLNode *node, Centroid *c);
AVLNodeIterator *AVLNodeIteratorCreate(AVLNode *node, Centroid *start);
void AVLNodeIteratorDestroy(AVLNodeIterator *it);
Centroid *AVLNodeNext(AVLNodeIterator *it);

typedef struct TDigest
{
	int64 count;
	float8 compression;
	AVLNode *summary;
} TDigest;

TDigest *TDigestCreate(void);
TDigest *TDigestCreateWithCompression(int compression);
void TDigestDestroy(TDigest *t);
void TDigestAdd(TDigest *t, float8 x, int64 w);
void TDigestAddSingle(TDigest *t, float8 x);
void TDigestCompress(TDigest *t);
TDigest *TDigestMerge(TDigest *t1, TDigest *t2);
float8 TDigestCDF(TDigest *t, float8 x);
float8 TDigestQuantile(TDigest *t, float8 q);

#endif
