/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * 
 * Copyright (C) 2010 - Oslandia
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

/* basic LWTRIANGLE manipulation */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"



/* construct a new LWTRIANGLE.
 * use SRID=SRID_UNKNOWN for unknown SRID (will have 8bit type's S = 0)
 */
LWTRIANGLE*
lwtriangle_construct(int srid, GBOX *bbox, POINTARRAY *points)
{
	LWTRIANGLE *result;

	result = (LWTRIANGLE*) lwalloc(sizeof(LWTRIANGLE));
	result->type = TRIANGLETYPE;

	result->flags = points->flags;
	FLAGS_SET_BBOX(result->flags, bbox?1:0);
	
	result->srid = srid;
	result->points = points;
	result->bbox = bbox;

	return result;
}

LWTRIANGLE*
lwtriangle_construct_empty(int srid, char hasz, char hasm)
{
	LWTRIANGLE *result = lwalloc(sizeof(LWTRIANGLE));
	result->type = TRIANGLETYPE;
	result->flags = gflags(hasz,hasm,0);
	result->srid = srid;
	result->points = ptarray_construct_empty(hasz, hasm, 1);
	result->bbox = NULL;
	return result;
}

void lwtriangle_free(LWTRIANGLE  *triangle)
{
	if ( ! triangle ) return;
	
	if (triangle->bbox)
		lwfree(triangle->bbox);
		
	if (triangle->points)
		ptarray_free(triangle->points);
		
	lwfree(triangle);
}

void printLWTRIANGLE(LWTRIANGLE *triangle)
{
	if (triangle->type != TRIANGLETYPE)
                lwerror("printLWTRIANGLE called with something else than a Triangle");

	lwnotice("LWTRIANGLE {");
	lwnotice("    ndims = %i", (int)FLAGS_NDIMS(triangle->flags));
	lwnotice("    SRID = %i", (int)triangle->srid);
	printPA(triangle->points);
	lwnotice("}");
}

/* @brief Clone LWTRIANGLE object. Serialized point lists are not copied.
 *
 * @see ptarray_clone 
 */
LWTRIANGLE *
lwtriangle_clone(const LWTRIANGLE *g)
{
	LWDEBUGF(2, "lwtriangle_clone called with %p", g);
	return (LWTRIANGLE *)lwline_clone((const LWLINE *)g);
}

void
lwtriangle_force_clockwise(LWTRIANGLE *triangle)
{
	if ( ptarray_isccw(triangle->points) )
		ptarray_reverse(triangle->points);
}

void
lwtriangle_reverse(LWTRIANGLE *triangle)
{
	if( lwtriangle_is_empty(triangle) ) return;
	ptarray_reverse(triangle->points);
}

void
lwtriangle_release(LWTRIANGLE *lwtriangle)
{
	lwgeom_release(lwtriangle_as_lwgeom(lwtriangle));
}

/* check coordinate equality  */
char
lwtriangle_same(const LWTRIANGLE *t1, const LWTRIANGLE *t2)
{
	char r = ptarray_same(t1->points, t2->points);
	LWDEBUGF(5, "returning %d", r);
	return r;
}

/*
 * Construct a triangle from a LWLINE being
 * the shell
 * Pointarray from intput geom are cloned.
 * Input line must have 4 points, and be closed.
 */
LWTRIANGLE *
lwtriangle_from_lwline(const LWLINE *shell)
{
	LWTRIANGLE *ret;
	POINTARRAY *pa;

	if ( shell->points->npoints != 4 )
		lwerror("lwtriangle_from_lwline: shell must have exactly 4 points");

	if (   (!FLAGS_GET_Z(shell->flags) && !ptarray_is_closed_2d(shell->points)) ||
	        (FLAGS_GET_Z(shell->flags) && !ptarray_is_closed_3d(shell->points)) )
		lwerror("lwtriangle_from_lwline: shell must be closed");

	pa = ptarray_clone_deep(shell->points);
	ret = lwtriangle_construct(shell->srid, NULL, pa);

	if (lwtriangle_is_repeated_points(ret))
		lwerror("lwtriangle_from_lwline: some points are repeated in triangle");

	return ret;
}

char
lwtriangle_is_repeated_points(LWTRIANGLE *triangle)
{
	char ret;
	POINTARRAY *pa;

	pa = ptarray_remove_repeated_points(triangle->points);
	ret = ptarray_same(pa, triangle->points);
	ptarray_free(pa);

	return ret;
}

int lwtriangle_is_empty(const LWTRIANGLE *triangle)
{
	if ( !triangle->points || triangle->points->npoints < 1 )
		return LW_TRUE;
	return LW_FALSE;
}

/**
 * Find the area of the outer ring 
 */
double
lwtriangle_area(const LWTRIANGLE *triangle)
{
	double area=0.0;
	int i;
	POINT2D p1;
	POINT2D p2;

	if (! triangle->points->npoints) return area; /* empty triangle */

	for (i=0; i < triangle->points->npoints-1; i++)
	{
		getPoint2d_p(triangle->points, i, &p1);
		getPoint2d_p(triangle->points, i+1, &p2);
		area += ( p1.x * p2.y ) - ( p1.y * p2.x );
	}

	area  /= 2.0;

	return fabs(area);
}


double
lwtriangle_perimeter(const LWTRIANGLE *triangle)
{
	if( triangle->points ) 
		return ptarray_length(triangle->points);
	else 
		return 0.0;
}

double
lwtriangle_perimeter_2d(const LWTRIANGLE *triangle)
{
	if( triangle->points ) 
		return ptarray_length_2d(triangle->points);
	else 
		return 0.0;
}
