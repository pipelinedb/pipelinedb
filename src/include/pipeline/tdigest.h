/* Copyright (c) 2013-2015 PipelineDB */
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
#ifndef TDIGEST_H
#define TDIGEST_H

#include "c.h"
#include "nodes/pg_list.h"

typedef struct Centroid
{
	uint64 weight;
	float8 mean;
} Centroid;

typedef struct TDigest {
	float8 compression;
	uint32 threshold;
	uint32 size;

	uint32 num_centroids;
	Centroid *centroids;

	uint64 total_weight;
	float8 min;
	float8 max;

	List *unmerged_centroids;
} TDigest;

extern TDigest *TDigestCreate(void);
extern TDigest *TDigestCreateWithCompression(int compression);
extern void TDigestDestroy(TDigest *t);
extern TDigest *TDigestCopy(TDigest *t);

extern void TDigestAdd(TDigest *t, float8 x, int64 w);
extern void TDigestCompress(TDigest *t);
extern void TDigestMerge(TDigest *t1, TDigest *t2);

extern float8 TDigestCDF(TDigest *t, float8 x);
extern float8 TDigestQuantile(TDigest *t, float8 q);

#endif
