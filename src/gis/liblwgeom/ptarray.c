/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2012 Sandro Santilli <strk@keybit.net>
 * Copyright (C) 2001-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <string.h>

#include "liblwgeom_internal.h"

/*#define POSTGIS_DEBUG_LEVEL 4*/
#include "lwgeom_log.h"

int
ptarray_has_z(const POINTARRAY *pa)
{
	if ( ! pa ) return LW_FALSE;
	return FLAGS_GET_Z(pa->flags);
}

int
ptarray_has_m(const POINTARRAY *pa)
{
	if ( ! pa ) return LW_FALSE;
	return FLAGS_GET_M(pa->flags);
}

/*
 * Size of point represeneted in the POINTARRAY
 * 16 for 2d, 24 for 3d, 32 for 4d
 */
int inline
ptarray_point_size(const POINTARRAY *pa)
{
	LWDEBUGF(5, "ptarray_point_size: FLAGS_NDIMS(pa->flags)=%x",FLAGS_NDIMS(pa->flags));

	return sizeof(double)*FLAGS_NDIMS(pa->flags);
}

POINTARRAY*
ptarray_construct(char hasz, char hasm, uint32_t npoints)
{
	POINTARRAY *pa = ptarray_construct_empty(hasz, hasm, npoints);
	pa->npoints = npoints;
	return pa;
}

POINTARRAY*
ptarray_construct_empty(char hasz, char hasm, uint32_t maxpoints)
{
	POINTARRAY *pa = lwalloc(sizeof(POINTARRAY));
	pa->serialized_pointlist = NULL;
	
	/* Set our dimsionality info on the bitmap */
	pa->flags = gflags(hasz, hasm, 0);
	
	/* We will be allocating a bit of room */
	pa->npoints = 0;
	pa->maxpoints = maxpoints;
	
	/* Allocate the coordinate array */
	if ( maxpoints > 0 )
		pa->serialized_pointlist = lwalloc(maxpoints * ptarray_point_size(pa));
	else 
		pa->serialized_pointlist = NULL;

	return pa;
}

/*
* Add a point into a pointarray. Only adds as many dimensions as the 
* pointarray supports.
*/
int
ptarray_insert_point(POINTARRAY *pa, const POINT4D *p, int where)
{
	size_t point_size = ptarray_point_size(pa);
	LWDEBUGF(5,"pa = %p; p = %p; where = %d", pa, p, where);
	LWDEBUGF(5,"pa->npoints = %d; pa->maxpoints = %d", pa->npoints, pa->maxpoints);
	
	if ( FLAGS_GET_READONLY(pa->flags) ) 
	{
		lwerror("ptarray_insert_point: called on read-only point array");
		return LW_FAILURE;
	}
	
	/* Error on invalid offset value */
	if ( where > pa->npoints || where < 0)
	{
		lwerror("ptarray_insert_point: offset out of range (%d)", where);
		return LW_FAILURE;
	}
	
	/* If we have no storage, let's allocate some */
	if( pa->maxpoints == 0 || ! pa->serialized_pointlist ) 
	{
		pa->maxpoints = 32;
		pa->npoints = 0;
		pa->serialized_pointlist = lwalloc(ptarray_point_size(pa) * pa->maxpoints);
	}

	/* Error out if we have a bad situation */
	if ( pa->npoints > pa->maxpoints )
		lwerror("npoints (%d) is greated than maxpoints (%d)", pa->npoints, pa->maxpoints);
	
	/* Check if we have enough storage, add more if necessary */
	if( pa->npoints == pa->maxpoints )
	{
		pa->maxpoints *= 2;
		pa->serialized_pointlist = lwrealloc(pa->serialized_pointlist, ptarray_point_size(pa) * pa->maxpoints);
	}
	
	/* Make space to insert the new point */
	if( where < pa->npoints )
	{
		size_t copy_size = point_size * (pa->npoints - where);
		memmove(getPoint_internal(pa, where+1), getPoint_internal(pa, where), copy_size);
		LWDEBUGF(5,"copying %d bytes to start vertex %d from start vertex %d", copy_size, where+1, where);
	}
	
	/* We have one more point */
	++pa->npoints;
	
	/* Copy the new point into the gap */
	ptarray_set_point4d(pa, where, p);
	LWDEBUGF(5,"copying new point to start vertex %d", point_size, where);
	
	return LW_SUCCESS;
}

int
ptarray_append_point(POINTARRAY *pa, const POINT4D *pt, int repeated_points)
{

	/* Check for pathology */
	if( ! pa || ! pt ) 
	{
		lwerror("ptarray_append_point: null input");
		return LW_FAILURE;
	}

	/* Check for duplicate end point */
	if ( repeated_points == LW_FALSE && pa->npoints > 0 )
	{
		POINT4D tmp;
		getPoint4d_p(pa, pa->npoints-1, &tmp);
		LWDEBUGF(4,"checking for duplicate end point (pt = POINT(%g %g) pa->npoints-q = POINT(%g %g))",pt->x,pt->y,tmp.x,tmp.y);

		/* Return LW_SUCCESS and do nothing else if previous point in list is equal to this one */
		if ( (pt->x == tmp.x) && (pt->y == tmp.y) &&
		     (FLAGS_GET_Z(pa->flags) ? pt->z == tmp.z : 1) &&
		     (FLAGS_GET_M(pa->flags) ? pt->m == tmp.m : 1) )
		{
			return LW_SUCCESS;
		}
	}

	/* Append is just a special case of insert */
	return ptarray_insert_point(pa, pt, pa->npoints);
}

int
ptarray_append_ptarray(POINTARRAY *pa1, POINTARRAY *pa2, double gap_tolerance)
{
	unsigned int poff = 0;
	unsigned int npoints;
	unsigned int ncap;
	unsigned int ptsize;

	/* Check for pathology */
	if( ! pa1 || ! pa2 ) 
	{
		lwerror("ptarray_append_ptarray: null input");
		return LW_FAILURE;
	}

	npoints = pa2->npoints;
	
	if ( ! npoints ) return LW_SUCCESS; /* nothing more to do */

	if( FLAGS_GET_READONLY(pa1->flags) )
	{
		lwerror("ptarray_append_ptarray: target pointarray is read-only");
		return LW_FAILURE;
	}

	if( FLAGS_GET_ZM(pa1->flags) != FLAGS_GET_ZM(pa2->flags) )
	{
		lwerror("ptarray_append_ptarray: appending mixed dimensionality is not allowed");
		return LW_FAILURE;
	}

	ptsize = ptarray_point_size(pa1);

	/* Check for duplicate end point */
	if ( pa1->npoints )
	{
		POINT2D tmp1, tmp2;
		getPoint2d_p(pa1, pa1->npoints-1, &tmp1);
		getPoint2d_p(pa2, 0, &tmp2);

		/* If the end point and start point are the same, then don't copy start point */
		if (p2d_same(&tmp1, &tmp2)) {
			poff = 1;
			--npoints;
		}
		else if ( gap_tolerance == 0 || ( gap_tolerance > 0 &&
		           distance2d_pt_pt(&tmp1, &tmp2) > gap_tolerance ) ) 
		{
			lwerror("Second line start point too far from first line end point");
			return LW_FAILURE;
		} 
	}

	/* Check if we need extra space */
	ncap = pa1->npoints + npoints;
	if ( pa1->maxpoints < ncap )
	{
		pa1->maxpoints = ncap > pa1->maxpoints*2 ?
		                 ncap : pa1->maxpoints*2;
		pa1->serialized_pointlist = lwrealloc(pa1->serialized_pointlist, ptsize * pa1->maxpoints);
	}

	memcpy(getPoint_internal(pa1, pa1->npoints),
	       getPoint_internal(pa2, poff), ptsize * npoints);

	pa1->npoints = ncap;

	return LW_SUCCESS;
}

/*
* Add a point into a pointarray. Only adds as many dimensions as the 
* pointarray supports.
*/
int
ptarray_remove_point(POINTARRAY *pa, int where)
{
	size_t ptsize = ptarray_point_size(pa);

	/* Check for pathology */
	if( ! pa ) 
	{
		lwerror("ptarray_remove_point: null input");
		return LW_FAILURE;
	}
	
	/* Error on invalid offset value */
	if ( where >= pa->npoints || where < 0)
	{
		lwerror("ptarray_remove_point: offset out of range (%d)", where);
		return LW_FAILURE;
	}
	
	/* If the point is any but the last, we need to copy the data back one point */
	if( where < pa->npoints - 1 )
	{
		memmove(getPoint_internal(pa, where), getPoint_internal(pa, where+1), ptsize * (pa->npoints - where - 1));
	}
	
	/* We have one less point */
	pa->npoints--;
	
	return LW_SUCCESS;
}

/**
* Build a new #POINTARRAY, but on top of someone else's ordinate array. 
* Flag as read-only, so that ptarray_free() does not free the serialized_ptlist
*/
POINTARRAY* ptarray_construct_reference_data(char hasz, char hasm, uint32_t npoints, uint8_t *ptlist)
{
	POINTARRAY *pa = lwalloc(sizeof(POINTARRAY));
	LWDEBUGF(5, "hasz = %d, hasm = %d, npoints = %d, ptlist = %p", hasz, hasm, npoints, ptlist);
	pa->flags = gflags(hasz, hasm, 0);
	FLAGS_SET_READONLY(pa->flags, 1); /* We don't own this memory, so we can't alter or free it. */
	pa->npoints = npoints;
	pa->maxpoints = npoints;
	pa->serialized_pointlist = ptlist;
	return pa;
}


POINTARRAY*
ptarray_construct_copy_data(char hasz, char hasm, uint32_t npoints, const uint8_t *ptlist)
{
	POINTARRAY *pa = lwalloc(sizeof(POINTARRAY));

	pa->flags = gflags(hasz, hasm, 0);
	pa->npoints = npoints;
	pa->maxpoints = npoints;

	if ( npoints > 0 )
	{
		pa->serialized_pointlist = lwalloc(ptarray_point_size(pa) * npoints);
		memcpy(pa->serialized_pointlist, ptlist, ptarray_point_size(pa) * npoints);
	}
	else
	{
		pa->serialized_pointlist = NULL;
	}

	return pa;
}

void ptarray_free(POINTARRAY *pa)
{
	if(pa)
	{
		if(pa->serialized_pointlist && ( ! FLAGS_GET_READONLY(pa->flags) ) )
			lwfree(pa->serialized_pointlist);	
		lwfree(pa);
		LWDEBUG(5,"Freeing a PointArray");
	}
}


void
ptarray_reverse(POINTARRAY *pa)
{
	/* TODO change this to double array operations once point array is double aligned */
	POINT4D pbuf;
	uint32_t i;
	int ptsize = ptarray_point_size(pa);
	int last = pa->npoints-1;
	int mid = pa->npoints/2;

	for (i=0; i<mid; i++)
	{
		uint8_t *from, *to;
		from = getPoint_internal(pa, i);
		to = getPoint_internal(pa, (last-i));
		memcpy((uint8_t *)&pbuf, to, ptsize);
		memcpy(to, from, ptsize);
		memcpy(from, (uint8_t *)&pbuf, ptsize);
	}

}


/**
 * Reverse X and Y axis on a given POINTARRAY
 */
POINTARRAY*
ptarray_flip_coordinates(POINTARRAY *pa)
{
	int i;
	double d;
	POINT4D p;

	for (i=0 ; i < pa->npoints ; i++)
	{
		getPoint4d_p(pa, i, &p);
		d = p.y;
		p.y = p.x;
		p.x = d;
		ptarray_set_point4d(pa, i, &p);
	}

	return pa;
}

void
ptarray_swap_ordinates(POINTARRAY *pa, LWORD o1, LWORD o2)
{
	int i;
	double d, *dp1, *dp2;
	POINT4D p;

#if PARANOIA_LEVEL > 0
  assert(o1 < 4);
  assert(o2 < 4);
#endif

  dp1 = ((double*)&p)+(unsigned)o1;
  dp2 = ((double*)&p)+(unsigned)o2;
	for (i=0 ; i < pa->npoints ; i++)
	{
		getPoint4d_p(pa, i, &p);
		d = *dp2;
		*dp2 = *dp1;
		*dp1 = d;
		ptarray_set_point4d(pa, i, &p);
	}
}


/**
 * @brief Returns a modified #POINTARRAY so that no segment is
 * 		longer than the given distance (computed using 2d).
 *
 * Every input point is kept.
 * Z and M values for added points (if needed) are set to 0.
 */
POINTARRAY *
ptarray_segmentize2d(const POINTARRAY *ipa, double dist)
{
	double	segdist;
	POINT4D	p1, p2;
	POINT4D pbuf;
	POINTARRAY *opa;
	int ipoff=0; /* input point offset */
	int hasz = FLAGS_GET_Z(ipa->flags);
	int hasm = FLAGS_GET_M(ipa->flags);

	pbuf.x = pbuf.y = pbuf.z = pbuf.m = 0;

	/* Initial storage */
	opa = ptarray_construct_empty(hasz, hasm, ipa->npoints);
	
	/* Add first point */
	getPoint4d_p(ipa, ipoff, &p1);
	ptarray_append_point(opa, &p1, LW_FALSE);

	ipoff++;

	while (ipoff<ipa->npoints)
	{
		/*
		 * We use these pointers to avoid
		 * "strict-aliasing rules break" warning raised
		 * by gcc (3.3 and up).
		 *
		 * It looks that casting a variable address (also
		 * referred to as "type-punned pointer")
		 * breaks those "strict" rules.
		 *
		 */
		POINT4D *p1ptr=&p1, *p2ptr=&p2;

		getPoint4d_p(ipa, ipoff, &p2);

		segdist = distance2d_pt_pt((POINT2D *)p1ptr, (POINT2D *)p2ptr);

		if (segdist > dist) /* add an intermediate point */
		{
			pbuf.x = p1.x + (p2.x-p1.x)/segdist * dist;
			pbuf.y = p1.y + (p2.y-p1.y)/segdist * dist;
			if( hasz ) 
				pbuf.z = p1.z + (p2.z-p1.z)/segdist * dist;
			if( hasm )
				pbuf.m = p1.m + (p2.m-p1.m)/segdist * dist;
			ptarray_append_point(opa, &pbuf, LW_FALSE);
			p1 = pbuf;
		}
		else /* copy second point */
		{
			ptarray_append_point(opa, &p2, (ipa->npoints==2)?LW_TRUE:LW_FALSE);
			p1 = p2;
			ipoff++;
		}

		LW_ON_INTERRUPT(ptarray_free(opa); return NULL);
	}

	return opa;
}

char
ptarray_same(const POINTARRAY *pa1, const POINTARRAY *pa2)
{
	uint32_t i;
	size_t ptsize;

	if ( FLAGS_GET_ZM(pa1->flags) != FLAGS_GET_ZM(pa2->flags) ) return LW_FALSE;
	LWDEBUG(5,"dimensions are the same");
	
	if ( pa1->npoints != pa2->npoints ) return LW_FALSE;
	LWDEBUG(5,"npoints are the same");

	ptsize = ptarray_point_size(pa1);
	LWDEBUGF(5, "ptsize = %d", ptsize);

	for (i=0; i<pa1->npoints; i++)
	{
		if ( memcmp(getPoint_internal(pa1, i), getPoint_internal(pa2, i), ptsize) )
			return LW_FALSE;
		LWDEBUGF(5,"point #%d is the same",i);
	}

	return LW_TRUE;
}



/**
 * @brief Add a point in a pointarray.
 *
 * @param pa the source POINTARRAY
 * @param p the point to add
 * @param pdims number of ordinates in p (2..4)
 * @param where to insert the point. 0 prepends, pa->npoints appends
 *
 * @returns a newly constructed POINTARRAY using a newly allocated buffer
 *          for the actual points, or NULL on error.
 */
POINTARRAY *
ptarray_addPoint(const POINTARRAY *pa, uint8_t *p, size_t pdims, uint32_t where)
{
	POINTARRAY *ret;
	POINT4D pbuf;
	size_t ptsize = ptarray_point_size(pa);

	LWDEBUGF(3, "pa %x p %x size %d where %d",
	         pa, p, pdims, where);

	if ( pdims < 2 || pdims > 4 )
	{
		lwerror("ptarray_addPoint: point dimension out of range (%d)",
		        pdims);
		return NULL;
	}

	if ( where > pa->npoints )
	{
		lwerror("ptarray_addPoint: offset out of range (%d)",
		        where);
		return NULL;
	}

	LWDEBUG(3, "called with a %dD point");

	pbuf.x = pbuf.y = pbuf.z = pbuf.m = 0.0;
	memcpy((uint8_t *)&pbuf, p, pdims*sizeof(double));

	LWDEBUG(3, "initialized point buffer");

	ret = ptarray_construct(FLAGS_GET_Z(pa->flags),
	                        FLAGS_GET_M(pa->flags), pa->npoints+1);

	if ( where == -1 ) where = pa->npoints;

	if ( where )
	{
		memcpy(getPoint_internal(ret, 0), getPoint_internal(pa, 0), ptsize*where);
	}

	memcpy(getPoint_internal(ret, where), (uint8_t *)&pbuf, ptsize);

	if ( where+1 != ret->npoints )
	{
		memcpy(getPoint_internal(ret, where+1),
		       getPoint_internal(pa, where),
		       ptsize*(pa->npoints-where));
	}

	return ret;
}


/**
 * @brief Remove a point from a pointarray.
 * 	@param which -  is the offset (starting at 0)
 * @return #POINTARRAY is newly allocated
 */
POINTARRAY *
ptarray_removePoint(POINTARRAY *pa, uint32_t which)
{
	POINTARRAY *ret;
	size_t ptsize = ptarray_point_size(pa);

	LWDEBUGF(3, "pa %x which %d", pa, which);

#if PARANOIA_LEVEL > 0
	if ( which > pa->npoints-1 )
	{
		lwerror("ptarray_removePoint: offset (%d) out of range (%d..%d)",
		        which, 0, pa->npoints-1);
		return NULL;
	}

	if ( pa->npoints < 3 )
	{
		lwerror("ptarray_removePointe: can't remove a point from a 2-vertex POINTARRAY");
	}
#endif

	ret = ptarray_construct(FLAGS_GET_Z(pa->flags),
	                        FLAGS_GET_M(pa->flags), pa->npoints-1);

	/* copy initial part */
	if ( which )
	{
		memcpy(getPoint_internal(ret, 0), getPoint_internal(pa, 0), ptsize*which);
	}

	/* copy final part */
	if ( which < pa->npoints-1 )
	{
		memcpy(getPoint_internal(ret, which), getPoint_internal(pa, which+1),
		       ptsize*(pa->npoints-which-1));
	}

	return ret;
}


/**
 * @brief Merge two given POINTARRAY and returns a pointer
 * on the new aggregate one.
 * Warning: this function free the two inputs POINTARRAY
 * @return #POINTARRAY is newly allocated
 */
POINTARRAY *
ptarray_merge(POINTARRAY *pa1, POINTARRAY *pa2)
{
	POINTARRAY *pa;
	size_t ptsize = ptarray_point_size(pa1);

	if (FLAGS_GET_ZM(pa1->flags) != FLAGS_GET_ZM(pa2->flags))
		lwerror("ptarray_cat: Mixed dimension");

	pa = ptarray_construct( FLAGS_GET_Z(pa1->flags),
	                        FLAGS_GET_M(pa1->flags),
	                        pa1->npoints + pa2->npoints);

	memcpy(         getPoint_internal(pa, 0),
	                getPoint_internal(pa1, 0),
	                ptsize*(pa1->npoints));

	memcpy(         getPoint_internal(pa, pa1->npoints),
	                getPoint_internal(pa2, 0),
	                ptsize*(pa2->npoints));

	lwfree(pa1);
	lwfree(pa2);

	return pa;
}


/**
 * @brief Deep clone a pointarray (also clones serialized pointlist)
 */
POINTARRAY *
ptarray_clone_deep(const POINTARRAY *in)
{
	POINTARRAY *out = lwalloc(sizeof(POINTARRAY));
	size_t size;

	LWDEBUG(3, "ptarray_clone_deep called.");

	out->flags = in->flags;
	out->npoints = in->npoints;
	out->maxpoints = in->maxpoints;

	FLAGS_SET_READONLY(out->flags, 0);

	size = in->npoints * ptarray_point_size(in);
	out->serialized_pointlist = lwalloc(size);
	memcpy(out->serialized_pointlist, in->serialized_pointlist, size);

	return out;
}

/**
 * @brief Clone a POINTARRAY object. Serialized pointlist is not copied.
 */
POINTARRAY *
ptarray_clone(const POINTARRAY *in)
{
	POINTARRAY *out = lwalloc(sizeof(POINTARRAY));

	LWDEBUG(3, "ptarray_clone_deep called.");

	out->flags = in->flags;
	out->npoints = in->npoints;
	out->maxpoints = in->maxpoints;

	FLAGS_SET_READONLY(out->flags, 1);

	out->serialized_pointlist = in->serialized_pointlist;

	return out;
}

/**
* Check for ring closure using whatever dimensionality is declared on the 
* pointarray.
*/
int
ptarray_is_closed(const POINTARRAY *in)
{
	return 0 == memcmp(getPoint_internal(in, 0), getPoint_internal(in, in->npoints-1), ptarray_point_size(in));
}


int
ptarray_is_closed_2d(const POINTARRAY *in)
{
	return 0 == memcmp(getPoint_internal(in, 0), getPoint_internal(in, in->npoints-1), sizeof(POINT2D));
}

int
ptarray_is_closed_3d(const POINTARRAY *in)
{
	return 0 == memcmp(getPoint_internal(in, 0), getPoint_internal(in, in->npoints-1), sizeof(POINT3D));
}

int
ptarray_is_closed_z(const POINTARRAY *in)
{
	if ( FLAGS_GET_Z(in->flags) )
		return ptarray_is_closed_3d(in);
	else
		return ptarray_is_closed_2d(in);
}

/**
* Return 1 if the point is inside the POINTARRAY, -1 if it is outside,
* and 0 if it is on the boundary.
*/
int 
ptarray_contains_point(const POINTARRAY *pa, const POINT2D *pt)
{
	return ptarray_contains_point_partial(pa, pt, LW_TRUE, NULL);
}

int 
ptarray_contains_point_partial(const POINTARRAY *pa, const POINT2D *pt, int check_closed, int *winding_number)
{
	int wn = 0;
	int i;
	double side;
	const POINT2D *seg1;
	const POINT2D *seg2;
	double ymin, ymax;

	seg1 = getPoint2d_cp(pa, 0);
	seg2 = getPoint2d_cp(pa, pa->npoints-1);
	if ( check_closed && ! p2d_same(seg1, seg2) )
		lwerror("ptarray_contains_point called on unclosed ring");
	
	for ( i=1; i < pa->npoints; i++ )
	{
		seg2 = getPoint2d_cp(pa, i);
		
		/* Zero length segments are ignored. */
		if ( seg1->x == seg2->x && seg1->y == seg2->y )
		{
			seg1 = seg2;
			continue;
		}
			
		ymin = FP_MIN(seg1->y, seg2->y);
		ymax = FP_MAX(seg1->y, seg2->y);
		
		/* Only test segments in our vertical range */
		if ( pt->y > ymax || pt->y < ymin ) 
		{
			seg1 = seg2;
			continue;
		}

		side = lw_segment_side(seg1, seg2, pt);

		/* 
		* A point on the boundary of a ring is not contained. 
		* WAS: if (fabs(side) < 1e-12), see #852 
		*/
		if ( (side == 0) && lw_pt_in_seg(pt, seg1, seg2) )
		{
			return LW_BOUNDARY;
		}

		/*
		* If the point is to the left of the line, and it's rising,
		* then the line is to the right of the point and
		* circling counter-clockwise, so incremement.
		*/
		if ( (side < 0) && (seg1->y <= pt->y) && (pt->y < seg2->y) )
		{
			wn++;
		}
		
		/*
		* If the point is to the right of the line, and it's falling,
		* then the line is to the right of the point and circling
		* clockwise, so decrement.
		*/
		else if ( (side > 0) && (seg2->y <= pt->y) && (pt->y < seg1->y) )
		{
			wn--;
		}
		
		seg1 = seg2;
	}

	/* Sent out the winding number for calls that are building on this as a primitive */
	if ( winding_number )
		*winding_number = wn;

	/* Outside */
	if (wn == 0)
	{
		return LW_OUTSIDE;
	}
	
	/* Inside */
	return LW_INSIDE;
}

/**
* For POINTARRAYs representing CIRCULARSTRINGS. That is, linked triples
* with each triple being control points of a circular arc. Such
* POINTARRAYs have an odd number of vertices.
*
* Return 1 if the point is inside the POINTARRAY, -1 if it is outside,
* and 0 if it is on the boundary.
*/

int 
ptarrayarc_contains_point(const POINTARRAY *pa, const POINT2D *pt)
{
	return ptarrayarc_contains_point_partial(pa, pt, LW_TRUE /* Check closed*/, NULL);
}

int 
ptarrayarc_contains_point_partial(const POINTARRAY *pa, const POINT2D *pt, int check_closed, int *winding_number)
{
	int wn = 0;
	int i, side;
	const POINT2D *seg1;
	const POINT2D *seg2;
	const POINT2D *seg3;
	GBOX gbox;

	/* Check for not an arc ring (always have odd # of points) */
	if ( (pa->npoints % 2) == 0 )
	{
		lwerror("ptarrayarc_contains_point called with even number of points");
		return LW_OUTSIDE;
	}

	/* Check for not an arc ring (always have >= 3 points) */
	if ( pa->npoints < 3 )
	{
		lwerror("ptarrayarc_contains_point called too-short pointarray");
		return LW_OUTSIDE;
	}

	/* Check for unclosed case */
	seg1 = getPoint2d_cp(pa, 0);
	seg3 = getPoint2d_cp(pa, pa->npoints-1);
	if ( check_closed && ! p2d_same(seg1, seg3) )
	{
		lwerror("ptarrayarc_contains_point called on unclosed ring");
		return LW_OUTSIDE;
	} 
	/* OK, it's closed. Is it just one circle? */
	else if ( p2d_same(seg1, seg3) && pa->npoints == 3 )
	{
		double radius, d;
		POINT2D c;
		seg2 = getPoint2d_cp(pa, 1);
		
		/* Wait, it's just a point, so it can't contain anything */
		if ( lw_arc_is_pt(seg1, seg2, seg3) )
			return LW_OUTSIDE;
			
		/* See if the point is within the circle radius */
		radius = lw_arc_center(seg1, seg2, seg3, &c);
		d = distance2d_pt_pt(pt, &c);
		if ( FP_EQUALS(d, radius) )
			return LW_BOUNDARY; /* Boundary of circle */
		else if ( d < radius ) 
			return LW_INSIDE; /* Inside circle */
		else 
			return LW_OUTSIDE; /* Outside circle */
	} 
	else if ( p2d_same(seg1, pt) || p2d_same(seg3, pt) )
	{
		return LW_BOUNDARY; /* Boundary case */
	}

	/* Start on the ring */
	seg1 = getPoint2d_cp(pa, 0);
	for ( i=1; i < pa->npoints; i += 2 )
	{
		seg2 = getPoint2d_cp(pa, i);
		seg3 = getPoint2d_cp(pa, i+1);
		
		/* Catch an easy boundary case */
		if( p2d_same(seg3, pt) )
			return LW_BOUNDARY;
		
		/* Skip arcs that have no size */
		if ( lw_arc_is_pt(seg1, seg2, seg3) )
		{
			seg1 = seg3;
			continue;
		}
		
		/* Only test segments in our vertical range */
		lw_arc_calculate_gbox_cartesian_2d(seg1, seg2, seg3, &gbox);
		if ( pt->y > gbox.ymax || pt->y < gbox.ymin ) 
		{
			seg1 = seg3;
			continue;
		}

		/* Outside of horizontal range, and not between end points we also skip */
		if ( (pt->x > gbox.xmax || pt->x < gbox.xmin) && 
			 (pt->y > FP_MAX(seg1->y, seg3->y) || pt->y < FP_MIN(seg1->y, seg3->y)) ) 
		{
			seg1 = seg3;
			continue;
		}		
		
		side = lw_arc_side(seg1, seg2, seg3, pt);
		
		/* On the boundary */
		if ( (side == 0) && lw_pt_in_arc(pt, seg1, seg2, seg3) )
		{
			return LW_BOUNDARY;
		}
		
		/* Going "up"! Point to left of arc. */
		if ( side < 0 && (seg1->y <= pt->y) && (pt->y < seg3->y) )
		{
			wn++;
		}

		/* Going "down"! */
		if ( side > 0 && (seg2->y <= pt->y) && (pt->y < seg1->y) )
		{
			wn--;
		}
		
		/* Inside the arc! */
		if ( pt->x <= gbox.xmax && pt->x >= gbox.xmin ) 
		{
			POINT2D C;
			double radius = lw_arc_center(seg1, seg2, seg3, &C);
			double d = distance2d_pt_pt(pt, &C);

			/* On the boundary! */
			if ( d == radius )
				return LW_BOUNDARY;
			
			/* Within the arc! */
			if ( d  < radius )
			{
				/* Left side, increment winding number */
				if ( side < 0 )
					wn++;
				/* Right side, decrement winding number */
				if ( side > 0 ) 
					wn--;
			}
		}

		seg1 = seg3;
	}

	/* Sent out the winding number for calls that are building on this as a primitive */
	if ( winding_number )
		*winding_number = wn;

	/* Outside */
	if (wn == 0)
	{
		return LW_OUTSIDE;
	}
	
	/* Inside */
	return LW_INSIDE;
}

/**
* Returns the area in cartesian units. Area is negative if ring is oriented CCW, 
* positive if it is oriented CW and zero if the ring is degenerate or flat.
* http://en.wikipedia.org/wiki/Shoelace_formula
*/
double
ptarray_signed_area(const POINTARRAY *pa)
{
	const POINT2D *P1;
	const POINT2D *P2;
	const POINT2D *P3;
	double sum = 0.0;
	double x0, x, y1, y2;
	int i;
	
	if (! pa || pa->npoints < 3 )
		return 0.0;
		
	P1 = getPoint2d_cp(pa, 0);
	P2 = getPoint2d_cp(pa, 1);
	x0 = P1->x;
	for ( i = 1; i < pa->npoints - 1; i++ )
	{
		P3 = getPoint2d_cp(pa, i+1);
		x = P2->x - x0;
		y1 = P3->y;
		y2 = P1->y;
		sum += x * (y2-y1);
		
		/* Move forwards! */
		P1 = P2;
		P2 = P3;
	}
	return sum / 2.0;	
}

int
ptarray_isccw(const POINTARRAY *pa)
{
	double area = 0;
	area = ptarray_signed_area(pa);
	if ( area > 0 ) return LW_FALSE;
	else return LW_TRUE;
}

POINTARRAY*
ptarray_force_dims(const POINTARRAY *pa, int hasz, int hasm)
{
	/* TODO handle zero-length point arrays */
	int i;
	int in_hasz = FLAGS_GET_Z(pa->flags);
	int in_hasm = FLAGS_GET_M(pa->flags);
	POINT4D pt;
	POINTARRAY *pa_out = ptarray_construct_empty(hasz, hasm, pa->npoints);
	
	for( i = 0; i < pa->npoints; i++ )
	{
		getPoint4d_p(pa, i, &pt);
		if( hasz && ! in_hasz )
			pt.z = 0.0;
		if( hasm && ! in_hasm )
			pt.m = 0.0;
		ptarray_append_point(pa_out, &pt, LW_TRUE);
	} 

	return pa_out;
}

POINTARRAY *
ptarray_substring(POINTARRAY *ipa, double from, double to, double tolerance)
{
	POINTARRAY *dpa;
	POINT4D pt;
	POINT4D p1, p2;
	POINT4D *p1ptr=&p1; /* don't break strict-aliasing rule */
	POINT4D *p2ptr=&p2;
	int nsegs, i;
	double length, slength, tlength;
	int state = 0; /* 0=before, 1=inside */

	/*
	 * Create a dynamic pointarray with an initial capacity
	 * equal to full copy of input points
	 */
	dpa = ptarray_construct_empty(FLAGS_GET_Z(ipa->flags), FLAGS_GET_M(ipa->flags), ipa->npoints);

	/* Compute total line length */
	length = ptarray_length_2d(ipa);


	LWDEBUGF(3, "Total length: %g", length);


	/* Get 'from' and 'to' lengths */
	from = length*from;
	to = length*to;


	LWDEBUGF(3, "From/To: %g/%g", from, to);


	tlength = 0;
	getPoint4d_p(ipa, 0, &p1);
	nsegs = ipa->npoints - 1;
	for ( i = 0; i < nsegs; i++ )
	{
		double dseg;

		getPoint4d_p(ipa, i+1, &p2);


		LWDEBUGF(3 ,"Segment %d: (%g,%g,%g,%g)-(%g,%g,%g,%g)",
		         i, p1.x, p1.y, p1.z, p1.m, p2.x, p2.y, p2.z, p2.m);


		/* Find the length of this segment */
		slength = distance2d_pt_pt((POINT2D *)p1ptr, (POINT2D *)p2ptr);

		/*
		 * We are before requested start.
		 */
		if ( state == 0 ) /* before */
		{

			LWDEBUG(3, " Before start");

			if ( fabs ( from - ( tlength + slength ) ) <= tolerance )
			{

				LWDEBUG(3, "  Second point is our start");

				/*
				 * Second point is our start
				 */
				ptarray_append_point(dpa, &p2, LW_FALSE);
				state=1; /* we're inside now */
				goto END;
			}

			else if ( fabs(from - tlength) <= tolerance )
			{

				LWDEBUG(3, "  First point is our start");

				/*
				 * First point is our start
				 */
				ptarray_append_point(dpa, &p1, LW_FALSE);

				/*
				 * We're inside now, but will check
				 * 'to' point as well
				 */
				state=1;
			}

			/*
			 * Didn't reach the 'from' point,
			 * nothing to do
			 */
			else if ( from > tlength + slength ) goto END;

			else  /* tlength < from < tlength+slength */
			{

				LWDEBUG(3, "  Seg contains first point");

				/*
				 * Our start is between first and
				 * second point
				 */
				dseg = (from - tlength) / slength;

				interpolate_point4d(&p1, &p2, &pt, dseg);

				ptarray_append_point(dpa, &pt, LW_FALSE);

				/*
				 * We're inside now, but will check
				 * 'to' point as well
				 */
				state=1;
			}
		}

		if ( state == 1 ) /* inside */
		{

			LWDEBUG(3, " Inside");

			/*
			 * 'to' point is our second point.
			 */
			if ( fabs(to - ( tlength + slength ) ) <= tolerance )
			{

				LWDEBUG(3, " Second point is our end");

				ptarray_append_point(dpa, &p2, LW_FALSE);
				break; /* substring complete */
			}

			/*
			 * 'to' point is our first point.
			 * (should only happen if 'to' is 0)
			 */
			else if ( fabs(to - tlength) <= tolerance )
			{

				LWDEBUG(3, " First point is our end");

				ptarray_append_point(dpa, &p1, LW_FALSE);

				break; /* substring complete */
			}

			/*
			 * Didn't reach the 'end' point,
			 * just copy second point
			 */
			else if ( to > tlength + slength )
			{
				ptarray_append_point(dpa, &p2, LW_FALSE);
				goto END;
			}

			/*
			 * 'to' point falls on this segment
			 * Interpolate and break.
			 */
			else if ( to < tlength + slength )
			{

				LWDEBUG(3, " Seg contains our end");

				dseg = (to - tlength) / slength;
				interpolate_point4d(&p1, &p2, &pt, dseg);

				ptarray_append_point(dpa, &pt, LW_FALSE);

				break;
			}

			else
			{
				LWDEBUG(3, "Unhandled case");
			}
		}


END:

		tlength += slength;
		memcpy(&p1, &p2, sizeof(POINT4D));
	}

	LWDEBUGF(3, "Out of loop, ptarray has %d points", dpa->npoints);

	return dpa;
}

/*
 * Write into the *ret argument coordinates of the closes point on
 * the given segment to the reference input point.
 */
void
closest_point_on_segment(const POINT4D *p, const POINT4D *A, const POINT4D *B, POINT4D *ret)
{
	double r;

	if (  FP_EQUALS(A->x, B->x) && FP_EQUALS(A->y, B->y) )
	{
		*ret = *A;
		return;
	}

	/*
	 * We use comp.graphics.algorithms Frequently Asked Questions method
	 *
	 * (1)           AC dot AB
	 *           r = ----------
	 *                ||AB||^2
	 *	r has the following meaning:
	 *	r=0 P = A
	 *	r=1 P = B
	 *	r<0 P is on the backward extension of AB
	 *	r>1 P is on the forward extension of AB
	 *	0<r<1 P is interior to AB
	 *
	 */
	r = ( (p->x-A->x) * (B->x-A->x) + (p->y-A->y) * (B->y-A->y) )/( (B->x-A->x)*(B->x-A->x) +(B->y-A->y)*(B->y-A->y) );

	if (r<0)
	{
		*ret = *A;
		return;
	}
	if (r>1)
	{
		*ret = *B;
		return;
	}

	ret->x = A->x + ( (B->x - A->x) * r );
	ret->y = A->y + ( (B->y - A->y) * r );
	ret->z = A->z + ( (B->z - A->z) * r );
	ret->m = A->m + ( (B->m - A->m) * r );
}

/*
 * Given a point, returns the location of closest point on pointarray
 * and, optionally, it's actual distance from the point array.
 */
double
ptarray_locate_point(const POINTARRAY *pa, const POINT4D *p4d, double *mindistout, POINT4D *proj4d)
{
	double mindist=-1;
	double tlen, plen;
	int t, seg=-1;
	POINT4D	start4d, end4d, projtmp;
	POINT2D proj, p;
	const POINT2D *start = NULL, *end = NULL;

	/* Initialize our 2D copy of the input parameter */
	p.x = p4d->x;
	p.y = p4d->y;
	
	if ( ! proj4d ) proj4d = &projtmp;
	
	/* If the pointarray has only one point, the nearest point is */
	/* just that point */
	if ( pa->npoints == 1 )
	{
		getPoint4d_p(pa, 0, proj4d);
		*mindistout = distance2d_pt_pt(&p, start);
		return 0.0;
	}
	
	/* Loop through pointarray looking for nearest segment */
	start = getPoint2d_cp(pa, 0);
	for (t=1; t<pa->npoints; t++)
	{
		double dist;
		end = getPoint2d_cp(pa, t);
		dist = distance2d_pt_seg(&p, start, end);

		if (t==1 || dist < mindist )
		{
			mindist = dist;
			seg=t-1;
		}

		if ( mindist == 0 )
		{
			LWDEBUG(3, "Breaking on mindist=0");
			break;
		}

		start = end;
	}

	if ( mindistout ) *mindistout = mindist;

	LWDEBUGF(3, "Closest segment: %d", seg);
	LWDEBUGF(3, "mindist: %g", mindist);

	/*
	 * We need to project the
	 * point on the closest segment.
	 */
	getPoint4d_p(pa, seg, &start4d);
	getPoint4d_p(pa, seg+1, &end4d);
	closest_point_on_segment(p4d, &start4d, &end4d, proj4d);
	
	/* Copy 4D values into 2D holder */
	proj.x = proj4d->x;
	proj.y = proj4d->y;

	LWDEBUGF(3, "Closest segment:%d, npoints:%d", seg, pa->npoints);

	/* For robustness, force 1 when closest point == endpoint */
	if ( (seg >= (pa->npoints-2)) && p2d_same(&proj, end) )
	{
		return 1.0;
	}

	LWDEBUGF(3, "Closest point on segment: %g,%g", proj.x, proj.y);

	tlen = ptarray_length_2d(pa);

	LWDEBUGF(3, "tlen %g", tlen);

	/* Location of any point on a zero-length line is 0 */
	/* See http://trac.osgeo.org/postgis/ticket/1772#comment:2 */
	if ( tlen == 0 ) return 0;

	plen=0;
	start = getPoint2d_cp(pa, 0);
	for (t=0; t<seg; t++, start=end)
	{
		end = getPoint2d_cp(pa, t+1);
		plen += distance2d_pt_pt(start, end);

		LWDEBUGF(4, "Segment %d made plen %g", t, plen);
	}

	plen+=distance2d_pt_pt(&proj, start);

	LWDEBUGF(3, "plen %g, tlen %g", plen, tlen);

	return plen/tlen;
}

/**
 * @brief Longitude shift for a pointarray.
 *  	Y remains the same
 *  	X is converted:
 *	 		from -180..180 to 0..360
 *	 		from 0..360 to -180..180
 *  	X < 0 becomes X + 360
 *  	X > 180 becomes X - 360
 */
void
ptarray_longitude_shift(POINTARRAY *pa)
{
	int i;
	double x;

	for (i=0; i<pa->npoints; i++)
	{
		memcpy(&x, getPoint_internal(pa, i), sizeof(double));
		if ( x < 0 ) x+= 360;
		else if ( x > 180 ) x -= 360;
		memcpy(getPoint_internal(pa, i), &x, sizeof(double));
	}
}


/*
 * Returns a POINTARRAY with consecutive equal points
 * removed. Equality test on all dimensions of input.
 *
 * Always returns a newly allocated object.
 *
 */
POINTARRAY *
ptarray_remove_repeated_points(POINTARRAY *in)
{
	POINTARRAY* out;
	size_t ptsize;
	size_t ipn, opn;

	LWDEBUG(3, "ptarray_remove_repeated_points called.");

	/* Single or zero point arrays can't have duplicates */
	if ( in->npoints < 3 ) return ptarray_clone_deep(in);

	ptsize = ptarray_point_size(in);

	LWDEBUGF(3, "ptsize: %d", ptsize);

	/* Allocate enough space for all points */
	out = ptarray_construct(FLAGS_GET_Z(in->flags),
	                        FLAGS_GET_M(in->flags), in->npoints);

	/* Now fill up the actual points (NOTE: could be optimized) */

	opn=1;
	memcpy(getPoint_internal(out, 0), getPoint_internal(in, 0), ptsize);
	LWDEBUGF(3, " first point copied, out points: %d", opn);
	for (ipn=1; ipn<in->npoints; ++ipn)
	{
		if ( (ipn==in->npoints-1 && opn==1) || memcmp(getPoint_internal(in, ipn-1),
		        getPoint_internal(in, ipn), ptsize) )
		{
			/* The point is different from the previous,
			 * we add it to output */
			memcpy(getPoint_internal(out, opn++),
			       getPoint_internal(in, ipn), ptsize);
			LWDEBUGF(3, " Point %d differs from point %d. Out points: %d",
			         ipn, ipn-1, opn);
		}
	}

	LWDEBUGF(3, " in:%d out:%d", out->npoints, opn);
	out->npoints = opn;

	return out;
}

static void
ptarray_dp_findsplit(POINTARRAY *pts, int p1, int p2, int *split, double *dist)
{
	int k;
	const POINT2D *pk, *pa, *pb;
	double tmp, d;

	LWDEBUG(4, "function called");

	*split = p1;
	d = -1;

	if (p1 + 1 < p2)
	{

		pa = getPoint2d_cp(pts, p1);
		pb = getPoint2d_cp(pts, p2);

		LWDEBUGF(4, "P%d(%f,%f) to P%d(%f,%f)",
		         p1, pa->x, pa->y, p2, pb->x, pb->y);

		for (k=p1+1; k<p2; k++)
		{
			pk = getPoint2d_cp(pts, k);

			LWDEBUGF(4, "P%d(%f,%f)", k, pk->x, pk->y);

			/* distance computation */
			tmp = distance2d_sqr_pt_seg(pk, pa, pb);

			if (tmp > d)
			{
				d = tmp;	/* record the maximum */
				*split = k;

				LWDEBUGF(4, "P%d is farthest (%g)", k, d);
			}
		}
		*dist = d;

	} /* length---should be redone if can == 0 */
	else
	{
		LWDEBUG(3, "segment too short, no split/no dist");
		*dist = -1;
	}

}

POINTARRAY *
ptarray_simplify(POINTARRAY *inpts, double epsilon, unsigned int minpts)
{
	int *stack;			/* recursion stack */
	int sp=-1;			/* recursion stack pointer */
	int p1, split;
	double dist;
	POINTARRAY *outpts;
	POINT4D pt;

	double eps_sqr = epsilon * epsilon;

	/* Allocate recursion stack */
	stack = lwalloc(sizeof(int)*inpts->npoints);

	p1 = 0;
	stack[++sp] = inpts->npoints-1;

	LWDEBUGF(2, "Input has %d pts and %d dims", inpts->npoints,
	                                            FLAGS_NDIMS(inpts->flags));

	/* Allocate output POINTARRAY, and add first point. */
	outpts = ptarray_construct_empty(FLAGS_GET_Z(inpts->flags), FLAGS_GET_M(inpts->flags), inpts->npoints);
	getPoint4d_p(inpts, 0, &pt);
	ptarray_append_point(outpts, &pt, LW_FALSE);

	LWDEBUG(3, "Added P0 to simplified point array (size 1)");

	do
	{

		ptarray_dp_findsplit(inpts, p1, stack[sp], &split, &dist);

		LWDEBUGF(3, "Farthest point from P%d-P%d is P%d (dist. %g)", p1, stack[sp], split, dist);

		if (dist > eps_sqr || ( outpts->npoints+sp+1 < minpts && dist >= 0 ) )
		{
			LWDEBUGF(4, "Added P%d to stack (outpts:%d)", split, sp);
			stack[++sp] = split;
		}
		else
		{
			getPoint4d_p(inpts, stack[sp], &pt);
			LWDEBUGF(4, "npoints , minpoints %d %d", outpts->npoints, minpts);
			ptarray_append_point(outpts, &pt, LW_FALSE);
			
			LWDEBUGF(4, "Added P%d to simplified point array (size: %d)", stack[sp], outpts->npoints);

			p1 = stack[sp--];
		}

		LWDEBUGF(4, "stack pointer = %d", sp);
	}
	while (! (sp<0) );

	lwfree(stack);
	return outpts;
}

/**
* Find the 2d length of the given #POINTARRAY, using circular
* arc interpolation between each coordinate triple.
* Length(A1, A2, A3, A4, A5) = Length(A1, A2, A3)+Length(A3, A4, A5)
*/
double
ptarray_arc_length_2d(const POINTARRAY *pts)
{
	double dist = 0.0;
	int i;
	const POINT2D *a1;
	const POINT2D *a2;
	const POINT2D *a3;

	if ( pts->npoints % 2 != 1 )
        lwerror("arc point array with even number of points");
        
	a1 = getPoint2d_cp(pts, 0);
	
	for ( i=2; i < pts->npoints; i += 2 )
	{
    	a2 = getPoint2d_cp(pts, i-1);
		a3 = getPoint2d_cp(pts, i);
		dist += lw_arc_length(a1, a2, a3);
		a1 = a3;
	}
	return dist;
}

/**
* Find the 2d length of the given #POINTARRAY (even if it's 3d)
*/
double
ptarray_length_2d(const POINTARRAY *pts)
{
	double dist = 0.0;
	int i;
	const POINT2D *frm;
	const POINT2D *to;

	if ( pts->npoints < 2 ) return 0.0;

	frm = getPoint2d_cp(pts, 0);
	
	for ( i=1; i < pts->npoints; i++ )
	{
		to = getPoint2d_cp(pts, i);

		dist += sqrt( ((frm->x - to->x)*(frm->x - to->x))  +
		              ((frm->y - to->y)*(frm->y - to->y)) );
		
		frm = to;
	}
	return dist;
}

/**
* Find the 3d/2d length of the given #POINTARRAY
* (depending on its dimensionality)
*/
double
ptarray_length(const POINTARRAY *pts)
{
	double dist = 0.0;
	int i;
	POINT3DZ frm;
	POINT3DZ to;

	if ( pts->npoints < 2 ) return 0.0;

	/* compute 2d length if 3d is not available */
	if ( ! FLAGS_GET_Z(pts->flags) ) return ptarray_length_2d(pts);

	getPoint3dz_p(pts, 0, &frm);
	for ( i=1; i < pts->npoints; i++ )
	{
		getPoint3dz_p(pts, i, &to);
		dist += sqrt( ((frm.x - to.x)*(frm.x - to.x)) +
		              ((frm.y - to.y)*(frm.y - to.y)) +
		              ((frm.z - to.z)*(frm.z - to.z)) );
		frm = to;
	}
	return dist;
}


/*
 * Get a pointer to nth point of a POINTARRAY.
 *
 * Casting to returned pointer to POINT2D* should be safe,
 * as gserialized format always keeps the POINTARRAY pointer
 * aligned to double boundary.
 */
uint8_t *
getPoint_internal(const POINTARRAY *pa, int n)
{
	size_t size;
	uint8_t *ptr;

#if PARANOIA_LEVEL > 0
	if ( pa == NULL )
	{
		lwerror("getPoint got NULL pointarray");
		return NULL;
	}
	
	LWDEBUGF(5, "(n=%d, pa.npoints=%d, pa.maxpoints=%d)",n,pa->npoints,pa->maxpoints);

	if ( ( n < 0 ) || 
	     ( n > pa->npoints ) ||
	     ( n >= pa->maxpoints ) )
	{
		lwerror("getPoint_internal called outside of ptarray range (n=%d, pa.npoints=%d, pa.maxpoints=%d)",n,pa->npoints,pa->maxpoints);
		return NULL; /*error */
	}
#endif

	size = ptarray_point_size(pa);
	
	ptr = pa->serialized_pointlist + size * n;
	if ( FLAGS_NDIMS(pa->flags) == 2)
	{
		LWDEBUGF(5, "point = %g %g", *((double*)(ptr)), *((double*)(ptr+8)));
	}
	else if ( FLAGS_NDIMS(pa->flags) == 3)
	{
		LWDEBUGF(5, "point = %g %g %g", *((double*)(ptr)), *((double*)(ptr+8)), *((double*)(ptr+16)));
	}
	else if ( FLAGS_NDIMS(pa->flags) == 4)
	{
		LWDEBUGF(5, "point = %g %g %g %g", *((double*)(ptr)), *((double*)(ptr+8)), *((double*)(ptr+16)), *((double*)(ptr+24)));
	}

	return ptr;
}


/**
 * Affine transform a pointarray.
 */
void
ptarray_affine(POINTARRAY *pa, const AFFINE *a)
{
	int i;
	double x,y,z;
	POINT4D p4d;

	LWDEBUG(2, "lwgeom_affine_ptarray start");

	if ( FLAGS_GET_Z(pa->flags) )
	{
		LWDEBUG(3, " has z");

		for (i=0; i<pa->npoints; i++)
		{
			getPoint4d_p(pa, i, &p4d);
			x = p4d.x;
			y = p4d.y;
			z = p4d.z;
			p4d.x = a->afac * x + a->bfac * y + a->cfac * z + a->xoff;
			p4d.y = a->dfac * x + a->efac * y + a->ffac * z + a->yoff;
			p4d.z = a->gfac * x + a->hfac * y + a->ifac * z + a->zoff;
			ptarray_set_point4d(pa, i, &p4d);

			LWDEBUGF(3, " POINT %g %g %g => %g %g %g", x, y, x, p4d.x, p4d.y, p4d.z);
		}
	}
	else
	{
		LWDEBUG(3, " doesn't have z");

		for (i=0; i<pa->npoints; i++)
		{
			getPoint4d_p(pa, i, &p4d);
			x = p4d.x;
			y = p4d.y;
			p4d.x = a->afac * x + a->bfac * y + a->xoff;
			p4d.y = a->dfac * x + a->efac * y + a->yoff;
			ptarray_set_point4d(pa, i, &p4d);

			LWDEBUGF(3, " POINT %g %g %g => %g %g %g", x, y, x, p4d.x, p4d.y, p4d.z);
		}
	}

	LWDEBUG(3, "lwgeom_affine_ptarray end");

}

int
ptarray_startpoint(const POINTARRAY* pa, POINT4D* pt)
{
	return getPoint4d_p(pa, 0, pt);
}




/*
 * Stick an array of points to the given gridspec.
 * Return "gridded" points in *outpts and their number in *outptsn.
 *
 * Two consecutive points falling on the same grid cell are collapsed
 * into one single point.
 *
 */
POINTARRAY *
ptarray_grid(const POINTARRAY *pa, const gridspec *grid)
{
	POINT4D pt;
	int ipn; /* input point numbers */
	POINTARRAY *dpa;

	LWDEBUGF(2, "ptarray_grid called on %p", pa);

	dpa = ptarray_construct_empty(FLAGS_GET_Z(pa->flags),FLAGS_GET_M(pa->flags), pa->npoints);

	for (ipn=0; ipn<pa->npoints; ++ipn)
	{

		getPoint4d_p(pa, ipn, &pt);

		if ( grid->xsize )
			pt.x = rint((pt.x - grid->ipx)/grid->xsize) *
			         grid->xsize + grid->ipx;

		if ( grid->ysize )
			pt.y = rint((pt.y - grid->ipy)/grid->ysize) *
			         grid->ysize + grid->ipy;

		if ( FLAGS_GET_Z(pa->flags) && grid->zsize )
			pt.z = rint((pt.z - grid->ipz)/grid->zsize) *
			         grid->zsize + grid->ipz;

		if ( FLAGS_GET_M(pa->flags) && grid->msize )
			pt.m = rint((pt.m - grid->ipm)/grid->msize) *
			         grid->msize + grid->ipm;

		ptarray_append_point(dpa, &pt, LW_FALSE);

	}

	return dpa;
}

int 
ptarray_npoints_in_rect(const POINTARRAY *pa, const GBOX *gbox)
{
	const POINT2D *pt;
	int n = 0;
	int i;
	for ( i = 0; i < pa->npoints; i++ )
	{
		pt = getPoint2d_cp(pa, i);
		if ( gbox_contains_point2d(gbox, pt) )
			n++;
	}
	return n;
}


