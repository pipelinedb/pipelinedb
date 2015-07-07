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

/* basic LWCURVEPOLY manipulation */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"


LWCURVEPOLY *
lwcurvepoly_construct_empty(int srid, char hasz, char hasm)
{
	LWCURVEPOLY *ret;

	ret = lwalloc(sizeof(LWCURVEPOLY));
	ret->type = CURVEPOLYTYPE;
	ret->flags = gflags(hasz, hasm, 0);
	ret->srid = srid;
	ret->nrings = 0;
	ret->maxrings = 1; /* Allocate room for sub-members, just in case. */
	ret->rings = lwalloc(ret->maxrings * sizeof(LWGEOM*));
	ret->bbox = NULL;

	return ret;
}

LWCURVEPOLY *
lwcurvepoly_construct_from_lwpoly(LWPOLY *lwpoly)
{
	LWCURVEPOLY *ret;
	int i;
	ret = lwalloc(sizeof(LWCURVEPOLY));
	ret->type = CURVEPOLYTYPE;
	ret->flags = lwpoly->flags;
	ret->srid = lwpoly->srid;
	ret->nrings = lwpoly->nrings;
	ret->maxrings = lwpoly->nrings; /* Allocate room for sub-members, just in case. */
	ret->rings = lwalloc(ret->maxrings * sizeof(LWGEOM*));
	ret->bbox = lwpoly->bbox ? gbox_clone(lwpoly->bbox) : NULL;
	for ( i = 0; i < ret->nrings; i++ )
	{
		ret->rings[i] = lwline_as_lwgeom(lwline_construct(ret->srid, NULL, ptarray_clone_deep(lwpoly->rings[i])));
	}
	return ret;
}

int lwcurvepoly_add_ring(LWCURVEPOLY *poly, LWGEOM *ring)
{
	int i;
	
	/* Can't do anything with NULLs */
	if( ! poly || ! ring ) 
	{
		LWDEBUG(4,"NULL inputs!!! quitting");
		return LW_FAILURE;
	}

	/* Check that we're not working with garbage */
	if ( poly->rings == NULL && (poly->nrings || poly->maxrings) )
	{
		LWDEBUG(4,"mismatched nrings/maxrings");
		lwerror("Curvepolygon is in inconsistent state. Null memory but non-zero collection counts.");
	}

	/* Check that we're adding an allowed ring type */
	if ( ! ( ring->type == LINETYPE || ring->type == CIRCSTRINGTYPE || ring->type == COMPOUNDTYPE ) )
	{
		LWDEBUGF(4,"got incorrect ring type: %s",lwtype_name(ring->type));
		return LW_FAILURE;
	}

		
	/* In case this is a truly empty, make some initial space  */
	if ( poly->rings == NULL )
	{
		poly->maxrings = 2;
		poly->nrings = 0;
		poly->rings = lwalloc(poly->maxrings * sizeof(LWGEOM*));
	}

	/* Allocate more space if we need it */
	if ( poly->nrings == poly->maxrings )
	{
		poly->maxrings *= 2;
		poly->rings = lwrealloc(poly->rings, sizeof(LWGEOM*) * poly->maxrings);
	}

	/* Make sure we don't already have a reference to this geom */
	for ( i = 0; i < poly->nrings; i++ )
	{
		if ( poly->rings[i] == ring )
		{
			LWDEBUGF(4, "Found duplicate geometry in collection %p == %p", poly->rings[i], ring);
			return LW_SUCCESS;
		}
	}

	/* Add the ring and increment the ring count */
	poly->rings[poly->nrings] = (LWGEOM*)ring;
	poly->nrings++;
	return LW_SUCCESS;	
}

/**
 * This should be rewritten to make use of the curve itself.
 */
double
lwcurvepoly_area(const LWCURVEPOLY *curvepoly)
{
	double area = 0.0;
	LWPOLY *poly;
	if( lwgeom_is_empty((LWGEOM*)curvepoly) )
		return 0.0;
	poly = lwcurvepoly_segmentize(curvepoly, 32);
	area = lwpoly_area(poly);
	lwpoly_free(poly);
	return area;
}


double
lwcurvepoly_perimeter(const LWCURVEPOLY *poly)
{
	double result=0.0;
	int i;

	for (i=0; i<poly->nrings; i++)
		result += lwgeom_length(poly->rings[i]);

	return result;
}

double
lwcurvepoly_perimeter_2d(const LWCURVEPOLY *poly)
{
	double result=0.0;
	int i;

	for (i=0; i<poly->nrings; i++)
		result += lwgeom_length_2d(poly->rings[i]);

	return result;
}
