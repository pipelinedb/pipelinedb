/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2001-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

/* basic LWCIRCSTRING functions */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"

void printLWCIRCSTRING(LWCIRCSTRING *curve);
void lwcircstring_reverse(LWCIRCSTRING *curve);
void lwcircstring_release(LWCIRCSTRING *lwcirc);
char lwcircstring_same(const LWCIRCSTRING *me, const LWCIRCSTRING *you);
LWCIRCSTRING *lwcircstring_from_lwpointarray(int srid, uint32_t npoints, LWPOINT **points);
LWCIRCSTRING *lwcircstring_from_lwmpoint(int srid, LWMPOINT *mpoint);
LWCIRCSTRING *lwcircstring_addpoint(LWCIRCSTRING *curve, LWPOINT *point, uint32_t where);
LWCIRCSTRING *lwcircstring_removepoint(LWCIRCSTRING *curve, uint32_t index);
void lwcircstring_setPoint4d(LWCIRCSTRING *curve, uint32_t index, POINT4D *newpoint);



/*
 * Construct a new LWCIRCSTRING.  points will *NOT* be copied
 * use SRID=SRID_UNKNOWN for unknown SRID (will have 8bit type's S = 0)
 */
LWCIRCSTRING *
lwcircstring_construct(int srid, GBOX *bbox, POINTARRAY *points)
{
	LWCIRCSTRING *result;

	/*
	        * The first arc requires three points.  Each additional
	        * arc requires two more points.  Thus the minimum point count
	        * is three, and the count must be odd.
	        */
	if (points->npoints % 2 != 1 || points->npoints < 3)
	{
		lwnotice("lwcircstring_construct: invalid point count %d", points->npoints);
	}

	result = (LWCIRCSTRING*) lwalloc(sizeof(LWCIRCSTRING));

	result->type = CIRCSTRINGTYPE;
	
	result->flags = points->flags;
	FLAGS_SET_BBOX(result->flags, bbox?1:0);

	result->srid = srid;
	result->points = points;
	result->bbox = bbox;

	return result;
}

LWCIRCSTRING *
lwcircstring_construct_empty(int srid, char hasz, char hasm)
{
	LWCIRCSTRING *result = lwalloc(sizeof(LWCIRCSTRING));
	result->type = CIRCSTRINGTYPE;
	result->flags = gflags(hasz,hasm,0);
	result->srid = srid;
	result->points = ptarray_construct_empty(hasz, hasm, 1);
	result->bbox = NULL;
	return result;
}

void
lwcircstring_release(LWCIRCSTRING *lwcirc)
{
	lwgeom_release(lwcircstring_as_lwgeom(lwcirc));
}


void lwcircstring_free(LWCIRCSTRING *curve)
{
	if ( ! curve ) return;
	
	if ( curve->bbox )
		lwfree(curve->bbox);
	if ( curve->points )
		ptarray_free(curve->points);
	lwfree(curve);
}



void printLWCIRCSTRING(LWCIRCSTRING *curve)
{
	lwnotice("LWCIRCSTRING {");
	lwnotice("    ndims = %i", (int)FLAGS_NDIMS(curve->flags));
	lwnotice("    srid = %i", (int)curve->srid);
	printPA(curve->points);
	lwnotice("}");
}

/* @brief Clone LWCIRCSTRING object. Serialized point lists are not copied.
 *
 * @see ptarray_clone 
 */
LWCIRCSTRING *
lwcircstring_clone(const LWCIRCSTRING *g)
{
	return (LWCIRCSTRING *)lwline_clone((LWLINE *)g);
}


void lwcircstring_reverse(LWCIRCSTRING *curve)
{
	ptarray_reverse(curve->points);
}

/* check coordinate equality */
char
lwcircstring_same(const LWCIRCSTRING *me, const LWCIRCSTRING *you)
{
	return ptarray_same(me->points, you->points);
}

/*
 * Construct a LWCIRCSTRING from an array of LWPOINTs
 * LWCIRCSTRING dimensions are large enough to host all input dimensions.
 */
LWCIRCSTRING *
lwcircstring_from_lwpointarray(int srid, uint32_t npoints, LWPOINT **points)
{
	int zmflag=0;
	uint32_t i;
	POINTARRAY *pa;
	uint8_t *newpoints, *ptr;
	size_t ptsize, size;

	/*
	 * Find output dimensions, check integrity
	 */
	for (i = 0; i < npoints; i++)
	{
		if (points[i]->type != POINTTYPE)
		{
			lwerror("lwcurve_from_lwpointarray: invalid input type: %s",
			        lwtype_name(points[i]->type));
			return NULL;
		}
		if (FLAGS_GET_Z(points[i]->flags)) zmflag |= 2;
		if (FLAGS_GET_M(points[i]->flags)) zmflag |= 1;
		if (zmflag == 3) break;
	}

	if (zmflag == 0) ptsize = 2 * sizeof(double);
	else if (zmflag == 3) ptsize = 4 * sizeof(double);
	else ptsize = 3 * sizeof(double);

	/*
	 * Allocate output points array
	 */
	size = ptsize * npoints;
	newpoints = lwalloc(size);
	memset(newpoints, 0, size);

	ptr = newpoints;
	for (i = 0; i < npoints; i++)
	{
		size = ptarray_point_size(points[i]->point);
		memcpy(ptr, getPoint_internal(points[i]->point, 0), size);
		ptr += ptsize;
	}
	pa = ptarray_construct_reference_data(zmflag&2, zmflag&1, npoints, newpoints);
	
	return lwcircstring_construct(srid, NULL, pa);
}

/*
 * Construct a LWCIRCSTRING from a LWMPOINT
 */
LWCIRCSTRING *
lwcircstring_from_lwmpoint(int srid, LWMPOINT *mpoint)
{
	uint32_t i;
	POINTARRAY *pa;
	char zmflag = FLAGS_GET_ZM(mpoint->flags);
	size_t ptsize, size;
	uint8_t *newpoints, *ptr;

	if (zmflag == 0) ptsize = 2 * sizeof(double);
	else if (zmflag == 3) ptsize = 4 * sizeof(double);
	else ptsize = 3 * sizeof(double);

	/* Allocate space for output points */
	size = ptsize * mpoint->ngeoms;
	newpoints = lwalloc(size);
	memset(newpoints, 0, size);

	ptr = newpoints;
	for (i = 0; i < mpoint->ngeoms; i++)
	{
		memcpy(ptr,
		       getPoint_internal(mpoint->geoms[i]->point, 0),
		       ptsize);
		ptr += ptsize;
	}

	pa = ptarray_construct_reference_data(zmflag&2, zmflag&1, mpoint->ngeoms, newpoints);
	
	LWDEBUGF(3, "lwcurve_from_lwmpoint: constructed pointarray for %d points, %d zmflag", mpoint->ngeoms, zmflag);

	return lwcircstring_construct(srid, NULL, pa);
}

LWCIRCSTRING *
lwcircstring_addpoint(LWCIRCSTRING *curve, LWPOINT *point, uint32_t where)
{
	POINTARRAY *newpa;
	LWCIRCSTRING *ret;

	newpa = ptarray_addPoint(curve->points,
	                         getPoint_internal(point->point, 0),
	                         FLAGS_NDIMS(point->flags), where);
	ret = lwcircstring_construct(curve->srid, NULL, newpa);

	return ret;
}

LWCIRCSTRING *
lwcircstring_removepoint(LWCIRCSTRING *curve, uint32_t index)
{
	POINTARRAY *newpa;
	LWCIRCSTRING *ret;

	newpa = ptarray_removePoint(curve->points, index);
	ret = lwcircstring_construct(curve->srid, NULL, newpa);

	return ret;
}

/*
 * Note: input will be changed, make sure you have permissions for this.
 * */
void
lwcircstring_setPoint4d(LWCIRCSTRING *curve, uint32_t index, POINT4D *newpoint)
{
	ptarray_set_point4d(curve->points, index, newpoint);
}

int
lwcircstring_is_closed(const LWCIRCSTRING *curve)
{
	if (FLAGS_GET_Z(curve->flags))
		return ptarray_is_closed_3d(curve->points);

	return ptarray_is_closed_2d(curve->points);
}

int lwcircstring_is_empty(const LWCIRCSTRING *circ)
{
	if ( !circ->points || circ->points->npoints < 1 )
		return LW_TRUE;
	return LW_FALSE;
}

double lwcircstring_length(const LWCIRCSTRING *circ)
{
	return lwcircstring_length_2d(circ);
}

double lwcircstring_length_2d(const LWCIRCSTRING *circ)
{
	if ( lwcircstring_is_empty(circ) )
		return 0.0;
	
	return ptarray_arc_length_2d(circ->points);
}

/*
 * Returns freshly allocated #LWPOINT that corresponds to the index where.
 * Returns NULL if the geometry is empty or the index invalid.
 */
LWPOINT* lwcircstring_get_lwpoint(LWCIRCSTRING *circ, int where) {
	POINT4D pt;
	LWPOINT *lwpoint;
	POINTARRAY *pa;

	if ( lwcircstring_is_empty(circ) || where < 0 || where >= circ->points->npoints )
		return NULL;

	pa = ptarray_construct_empty(FLAGS_GET_Z(circ->flags), FLAGS_GET_M(circ->flags), 1);
	pt = getPoint4d(circ->points, where);
	ptarray_append_point(pa, &pt, LW_TRUE);
	lwpoint = lwpoint_construct(circ->srid, NULL, pa);
	return lwpoint;
}

/*
* Snap to grid 
*/
LWCIRCSTRING* lwcircstring_grid(const LWCIRCSTRING *line, const gridspec *grid)
{
	LWCIRCSTRING *oline;
	POINTARRAY *opa;

	opa = ptarray_grid(line->points, grid);

	/* Skip line3d with less then 2 points */
	if ( opa->npoints < 2 ) return NULL;

	/* TODO: grid bounding box... */
	oline = lwcircstring_construct(line->srid, NULL, opa);

	return oline;
}

