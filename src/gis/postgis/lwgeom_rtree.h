#ifndef _LWGEOM_RTREE_H
#define _LWGEOM_RTREE_H 1

#include "liblwgeom.h"

/**
* Representation for the y-axis interval spanned by an edge.
*/
typedef struct
{
	double min;
	double max;
}
RTREE_INTERVAL;

/**
* The following struct and methods are used for a 1D RTree implementation,
* described at:
*  http://lin-ear-th-inking.blogspot.com/2007/06/packed-1-dimensional-r-tree.html
*/
typedef struct rtree_node
{
	RTREE_INTERVAL *interval;
	struct rtree_node *leftNode;
	struct rtree_node *rightNode;
	LWLINE *segment;
}
RTREE_NODE;

/**
* The tree structure used for fast P-i-P tests by point_in_multipolygon_rtree()
*/
typedef struct
{
	RTREE_NODE **ringIndices;
	int* ringCounts;
	int polyCount;
}
RTREE_POLY_CACHE;



typedef struct {
	int                         type;       // <GeomCache>
	GSERIALIZED*                geom1;      // 
	GSERIALIZED*                geom2;      // 
	size_t                      geom1_size; // 
	size_t                      geom2_size; // 
	int32                       argnum;     // </GeomCache>
	RTREE_POLY_CACHE*           index;
} RTreeGeomCache;


/**
* Retrieves a collection of line segments given the root and crossing value. 
*/
LWMLINE *RTreeFindLineSegments(RTREE_NODE *root, double value);


/**
* Checks for a cache hit against the provided geometry and returns
* a pre-built index structure (RTREE_POLY_CACHE) if one exists. Otherwise
* builds a new one and returns that.
*/
RTREE_POLY_CACHE* GetRtreeCache(FunctionCallInfoData* fcinfo, GSERIALIZED* g1);


#endif /* !defined _LWGEOM_RTREE_H */
