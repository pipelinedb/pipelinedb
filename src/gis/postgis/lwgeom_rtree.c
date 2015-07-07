/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright (C) 2001-2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <assert.h>

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "liblwgeom.h"
#include "liblwgeom_internal.h"         /* For FP comparators. */
#include "lwgeom_cache.h"
#include "lwgeom_rtree.h"


/* Prototypes */
static void RTreeFree(RTREE_NODE* root);

/**
* Allocate a fresh clean RTREE_POLY_CACHE
*/
static RTREE_POLY_CACHE* 
RTreeCacheCreate()
{
	RTREE_POLY_CACHE* result;
	result = lwalloc(sizeof(RTREE_POLY_CACHE));
	memset(result, 0, sizeof(RTREE_POLY_CACHE));
	return result;
}

/**
* Recursively frees the child nodes, the interval and the line before
* freeing the root node.
*/
static void 
RTreeFree(RTREE_NODE* root)
{
	POSTGIS_DEBUGF(2, "RTreeFree called for %p", root);

	if (root->leftNode)
		RTreeFree(root->leftNode);
	if (root->rightNode)
		RTreeFree(root->rightNode);
	lwfree(root->interval);
	if (root->segment)
	{
		lwline_free(root->segment);
	}
	lwfree(root);
}

/**
* Free the cache object and all the sub-objects properly.
*/
static void 
RTreeCacheClear(RTREE_POLY_CACHE* cache)
{
	int g, r, i;
	POSTGIS_DEBUGF(2, "RTreeCacheClear called for %p", cache);
	i = 0;
	for (g = 0; g < cache->polyCount; g++)
	{
		for (r = 0; r < cache->ringCounts[g]; r++)
		{
			RTreeFree(cache->ringIndices[i]);
			i++;
		}
	}
	lwfree(cache->ringIndices);
	lwfree(cache->ringCounts);
	cache->ringIndices = 0;
	cache->ringCounts = 0;
	cache->polyCount = 0;
}


/**
 * Returns 1 if min < value <= max, 0 otherwise. 
*/
static uint32 
IntervalIsContained(RTREE_INTERVAL* interval, double value)
{
	return FP_CONTAINS_INCL(interval->min, value, interval->max) ? 1 : 0;
}

/**
* Creates an interval with the total extents of the two given intervals.
*/
static RTREE_INTERVAL* 
RTreeMergeIntervals(RTREE_INTERVAL *inter1, RTREE_INTERVAL *inter2)
{
	RTREE_INTERVAL *interval;

	POSTGIS_DEBUGF(2, "RTreeMergeIntervals called with %p, %p", inter1, inter2);

	interval = lwalloc(sizeof(RTREE_INTERVAL));
	interval->max = FP_MAX(inter1->max, inter2->max);
	interval->min = FP_MIN(inter1->min, inter2->min);

	POSTGIS_DEBUGF(3, "interval min = %8.3f, max = %8.3f", interval->min, interval->max);

	return interval;
}

/**
* Creates an interval given the min and max values, in arbitrary order.
*/
static RTREE_INTERVAL*
RTreeCreateInterval(double value1, double value2)
{
	RTREE_INTERVAL *interval;

	POSTGIS_DEBUGF(2, "RTreeCreateInterval called with %8.3f, %8.3f", value1, value2);

	interval = lwalloc(sizeof(RTREE_INTERVAL));
	interval->max = FP_MAX(value1, value2);
	interval->min = FP_MIN(value1, value2);

	POSTGIS_DEBUGF(3, "interval min = %8.3f, max = %8.3f", interval->min, interval->max);

	return interval;
}

/**
* Creates an interior node given the children.
*/
static RTREE_NODE* 
RTreeCreateInteriorNode(RTREE_NODE* left, RTREE_NODE* right)
{
	RTREE_NODE *parent;

	POSTGIS_DEBUGF(2, "RTreeCreateInteriorNode called for children %p, %p", left, right);

	parent = lwalloc(sizeof(RTREE_NODE));
	parent->leftNode = left;
	parent->rightNode = right;
	parent->interval = RTreeMergeIntervals(left->interval, right->interval);
	parent->segment = NULL;

	POSTGIS_DEBUGF(3, "RTreeCreateInteriorNode returning %p", parent);

	return parent;
}

/**
* Creates a leaf node given the pointer to the start point of the segment.
*/
static RTREE_NODE* 
RTreeCreateLeafNode(POINTARRAY* pa, int startPoint)
{
	RTREE_NODE *parent;
	LWLINE *line;
	double value1;
	double value2;
	POINT4D tmp;
	POINTARRAY *npa;

	POSTGIS_DEBUGF(2, "RTreeCreateLeafNode called for point %d of %p", startPoint, pa);

	if (pa->npoints < startPoint + 2)
	{
		lwerror("RTreeCreateLeafNode: npoints = %d, startPoint = %d", pa->npoints, startPoint);
	}

	/*
	 * The given point array will be part of a geometry that will be freed
	 * independently of the index.	Since we may want to cache the index,
	 * we must create independent arrays.
	 */
	npa = ptarray_construct_empty(0,0,2);

	getPoint4d_p(pa, startPoint, &tmp);
	value1 = tmp.y;
	ptarray_append_point(npa,&tmp,LW_TRUE);
	
	getPoint4d_p(pa, startPoint+1, &tmp);
	value2 = tmp.y;
	ptarray_append_point(npa,&tmp,LW_TRUE);

	line = lwline_construct(SRID_UNKNOWN, NULL, npa);
	
	parent = lwalloc(sizeof(RTREE_NODE));
	parent->interval = RTreeCreateInterval(value1, value2);
	parent->segment = line;
	parent->leftNode = NULL;
	parent->rightNode = NULL;

	POSTGIS_DEBUGF(3, "RTreeCreateLeafNode returning %p", parent);

	return parent;
}

/**
* Creates an rtree given a pointer to the point array.
* Must copy the point array.
*/
static RTREE_NODE* 
RTreeCreate(POINTARRAY* pointArray)
{
	RTREE_NODE* root;
	RTREE_NODE** nodes = lwalloc(pointArray->npoints * sizeof(RTREE_NODE*));
	int i, nodeCount;
	int childNodes, parentNodes;

	POSTGIS_DEBUGF(2, "RTreeCreate called with pointarray %p", pointArray);

	nodeCount = pointArray->npoints - 1;

	POSTGIS_DEBUGF(3, "Total leaf nodes: %d", nodeCount);

	/*
	 * Create a leaf node for every line segment.
	 */
	for (i = 0; i < nodeCount; i++)
	{
		nodes[i] = RTreeCreateLeafNode(pointArray, i);
	}

	/*
	 * Next we group nodes by pairs.  If there's an odd number of nodes,
	 * we bring the last node up a level as is.	 Continue until we have
	 * a single top node.
	 */
	childNodes = nodeCount;
	parentNodes = nodeCount / 2;
	while (parentNodes > 0)
	{
		POSTGIS_DEBUGF(3, "Merging %d children into %d parents.", childNodes, parentNodes);

		i = 0;
		while (i < parentNodes)
		{
			nodes[i] = RTreeCreateInteriorNode(nodes[i*2], nodes[i*2+1]);
			i++;
		}
		/*
		 * Check for an odd numbered final node.
		 */
		if (parentNodes * 2 < childNodes)
		{
			POSTGIS_DEBUGF(3, "Shuffling child %d to parent %d", childNodes - 1, i);

			nodes[i] = nodes[childNodes - 1];
			parentNodes++;
		}
		childNodes = parentNodes;
		parentNodes = parentNodes / 2;
	}

	root = nodes[0];
	lwfree(nodes);
	POSTGIS_DEBUGF(3, "RTreeCreate returning %p", root);

	return root;
}


/** 
* Merges two multilinestrings into a single multilinestring. 
*/
static LWMLINE* 
RTreeMergeMultiLines(LWMLINE *line1, LWMLINE *line2)
{
	LWGEOM **geoms;
	LWCOLLECTION *col;
	int i, j, ngeoms;

	POSTGIS_DEBUGF(2, "RTreeMergeMultiLines called on %p, %d, %d; %p, %d, %d", line1, line1->ngeoms, line1->type, line2, line2->ngeoms, line2->type);

	ngeoms = line1->ngeoms + line2->ngeoms;
	geoms = lwalloc(sizeof(LWGEOM *) * ngeoms);

	j = 0;
	for (i = 0; i < line1->ngeoms; i++, j++)
	{
		geoms[j] = lwgeom_clone((LWGEOM *)line1->geoms[i]);
	}
	for (i = 0; i < line2->ngeoms; i++, j++)
	{
		geoms[j] = lwgeom_clone((LWGEOM *)line2->geoms[i]);
	}
	col = lwcollection_construct(MULTILINETYPE, SRID_UNKNOWN, NULL, ngeoms, geoms);

	POSTGIS_DEBUGF(3, "RTreeMergeMultiLines returning %p, %d, %d", col, col->ngeoms, col->type);

	return (LWMLINE *)col;
}


/**
* Callback function sent into the GetGeomCache generic caching system. Given a
* LWGEOM* this function builds and stores an RTREE_POLY_CACHE into the provided
* GeomCache object.
*/
static int 
RTreeBuilder(const LWGEOM* lwgeom, GeomCache* cache)
{
	int i, p, r;
	LWMPOLY *mpoly;
	LWPOLY *poly;
	int nrings;
	RTreeGeomCache* rtree_cache = (RTreeGeomCache*)cache;
	RTREE_POLY_CACHE* currentCache;
	
	if ( ! cache )
		return LW_FAILURE;

	if ( rtree_cache->index )
	{
		lwerror("RTreeBuilder asked to build index where one already exists.");
		return LW_FAILURE;
	}
	
	if (lwgeom->type == MULTIPOLYGONTYPE)
	{
		POSTGIS_DEBUG(2, "RTreeBuilder MULTIPOLYGON");
		mpoly = (LWMPOLY *)lwgeom;
		nrings = 0;
		/*
		** Count the total number of rings.
		*/
		currentCache = RTreeCacheCreate();
		currentCache->polyCount = mpoly->ngeoms;
		currentCache->ringCounts = lwalloc(sizeof(int) * mpoly->ngeoms);
		for ( i = 0; i < mpoly->ngeoms; i++ )
		{
			currentCache->ringCounts[i] = mpoly->geoms[i]->nrings;
			nrings += mpoly->geoms[i]->nrings;
		}
		currentCache->ringIndices = lwalloc(sizeof(RTREE_NODE *) * nrings);
		/*
		** Load the array in geometry order, each outer ring followed by the inner rings
                ** associated with that outer ring
		*/
                i = 0;
		for ( p = 0; p < mpoly->ngeoms; p++ )
		{
			for ( r = 0; r < mpoly->geoms[p]->nrings; r++ )
			{
				currentCache->ringIndices[i] = RTreeCreate(mpoly->geoms[p]->rings[r]);
				i++;
			}
		}
		rtree_cache->index = currentCache;
	}
	else if ( lwgeom->type == POLYGONTYPE )
	{
		POSTGIS_DEBUG(2, "RTreeBuilder POLYGON");
		poly = (LWPOLY *)lwgeom;
		currentCache = RTreeCacheCreate();
		currentCache->polyCount = 1;
		currentCache->ringCounts = lwalloc(sizeof(int));
		currentCache->ringCounts[0] = poly->nrings;
		/*
		** Just load the rings on in order
		*/
		currentCache->ringIndices = lwalloc(sizeof(RTREE_NODE *) * poly->nrings);
		for ( i = 0; i < poly->nrings; i++ )
		{
			currentCache->ringIndices[i] = RTreeCreate(poly->rings[i]);
		}
		rtree_cache->index = currentCache;
	}
	else
	{
		/* Uh oh, shouldn't be here. */
		lwerror("RTreeBuilder got asked to build index on non-polygon");
		return LW_FAILURE;
	}
	return LW_SUCCESS;	
}

/**
* Callback function sent into the GetGeomCache generic caching system. On a 
* cache miss, this function clears the cached index object.
*/
static int
RTreeFreer(GeomCache* cache)
{
	RTreeGeomCache* rtree_cache = (RTreeGeomCache*)cache;
	
	if ( ! cache )
		return LW_FAILURE;
	
	if ( rtree_cache->index )
	{
		RTreeCacheClear(rtree_cache->index);
		lwfree(rtree_cache->index);
		rtree_cache->index = 0;
		rtree_cache->argnum = 0;
	}
	return LW_SUCCESS;
}

static GeomCache*
RTreeAllocator(void)
{
	RTreeGeomCache* cache = palloc(sizeof(RTreeGeomCache));
	memset(cache, 0, sizeof(RTreeGeomCache));
	return (GeomCache*)cache;
}

static GeomCacheMethods RTreeCacheMethods =
{
	RTREE_CACHE_ENTRY,
	RTreeBuilder,
	RTreeFreer,
	RTreeAllocator
};

RTREE_POLY_CACHE*
GetRtreeCache(FunctionCallInfoData* fcinfo, GSERIALIZED* g1)
{
	RTreeGeomCache* cache = (RTreeGeomCache*)GetGeomCache(fcinfo, &RTreeCacheMethods, g1, NULL);
	RTREE_POLY_CACHE* index = NULL;

	if ( cache )
		index = cache->index;

	return index;
}


/**
* Retrieves a collection of line segments given the root and crossing value.
* The collection is a multilinestring consisting of two point lines
* representing the segments of the ring that may be crossed by the
* horizontal projection line at the given y value.
*/
LWMLINE *RTreeFindLineSegments(RTREE_NODE *root, double value)
{
	LWMLINE *tmp, *result;
	LWGEOM **lwgeoms;

	POSTGIS_DEBUGF(2, "RTreeFindLineSegments called for tree %p and value %8.3f", root, value);

	result = NULL;

	if (!IntervalIsContained(root->interval, value))
	{
		POSTGIS_DEBUGF(3, "RTreeFindLineSegments %p: not contained.", root);

		return NULL;
	}

	/* If there is a segment defined for this node, include it. */
	if (root->segment)
	{
		POSTGIS_DEBUGF(3, "RTreeFindLineSegments %p: adding segment %p %d.", root, root->segment, root->segment->type);

		lwgeoms = lwalloc(sizeof(LWGEOM *));
		lwgeoms[0] = (LWGEOM *)root->segment;

		POSTGIS_DEBUGF(3, "Found geom %p, type %d, dim %d", root->segment, root->segment->type, FLAGS_GET_Z(root->segment->flags));

		result = (LWMLINE *)lwcollection_construct(MULTILINETYPE, SRID_UNKNOWN, NULL, 1, lwgeoms);
	}

	/* If there is a left child node, recursively include its results. */
	if (root->leftNode)
	{
		POSTGIS_DEBUGF(3, "RTreeFindLineSegments %p: recursing left.", root);

		tmp = RTreeFindLineSegments(root->leftNode, value);
		if (tmp)
		{
			POSTGIS_DEBUGF(3, "Found geom %p, type %d, dim %d", tmp, tmp->type, FLAGS_GET_Z(tmp->flags));

			if (result)
				result = RTreeMergeMultiLines(result, tmp);
			else
				result = tmp;
		}
	}

	/* Same for any right child. */
	if (root->rightNode)
	{
		POSTGIS_DEBUGF(3, "RTreeFindLineSegments %p: recursing right.", root);

		tmp = RTreeFindLineSegments(root->rightNode, value);
		if (tmp)
		{
			POSTGIS_DEBUGF(3, "Found geom %p, type %d, dim %d", tmp, tmp->type, FLAGS_GET_Z(tmp->flags));

			if (result)
				result = RTreeMergeMultiLines(result, tmp);
			else
				result = tmp;
		}
	}

	return result;
}



