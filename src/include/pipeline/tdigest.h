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
	int count;
	double mean;
	int id;
} Centroid;

Centroid *CentroidCreate(void);
Centroid *CentroidCreateWithId(int id);
void CentroidDestroy(Centroid *c);
void CentroidAdd(Centroid *c, double x, int w);
void CentroidAddSingle(Centroid *c, double x);
Centroid *CentroidCopy(Centroid *c);

typedef struct AVLNode
{
	int count;
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
void AVLNodeAdd(AVLNode *node, Centroid *c);
void AVLNodeRemove(AVLNode *node, Centroid *c);
int AVLNodeHeadCount(AVLNode *node, Centroid *c);
int AVLNodeHeadSum(AVLNode *node, Centroid *c);
Centroid *AVLNodeFirst(AVLNode *node);
Centroid *AVLNodeLast(AVLNode *node);
Centroid *AVLNodeFloor(AVLNode *node, Centroid *c);
Centroid *AVLNodeCeiling(AVLNode *node, Centroid *c);
AVLNodeIterator *AVLNodeIteratorCreate(AVLNode *node, Centroid *start);
void AVLNodeIteratorDestroy(AVLNodeIterator *it);
Centroid *AVLNodeNext(AVLNodeIterator *it);

typedef struct TDigest
{
	int count;
	double compression;
	AVLNode *summary;
} TDigest;

#endif
