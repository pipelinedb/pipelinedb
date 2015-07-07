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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"



int
lwcompound_is_closed(const LWCOMPOUND *compound)
{
	size_t size;
	int npoints=0;

	if ( lwgeom_has_z((LWGEOM*)compound) )
	{
		size = sizeof(POINT3D);
	}
	else
	{
		size = sizeof(POINT2D);
	}

	if ( compound->geoms[compound->ngeoms - 1]->type == CIRCSTRINGTYPE )
	{
		npoints = ((LWCIRCSTRING *)compound->geoms[compound->ngeoms - 1])->points->npoints;
	}
	else if (compound->geoms[compound->ngeoms - 1]->type == LINETYPE)
	{
		npoints = ((LWLINE *)compound->geoms[compound->ngeoms - 1])->points->npoints;
	}

	if ( memcmp(getPoint_internal( (POINTARRAY *)compound->geoms[0]->data, 0),
	            getPoint_internal( (POINTARRAY *)compound->geoms[compound->ngeoms - 1]->data,
	                               npoints - 1),
	            size) ) 
	{
		return LW_FALSE;
	}

	return LW_TRUE;
}

double lwcompound_length(const LWCOMPOUND *comp)
{
	double length = 0.0;
	LWLINE *line;
	if ( lwgeom_is_empty((LWGEOM*)comp) )
		return 0.0;
	line = lwcompound_segmentize(comp, 32);
	length = lwline_length(line);
	lwline_free(line);
	return length;
}

double lwcompound_length_2d(const LWCOMPOUND *comp)
{
	double length = 0.0;
	LWLINE *line;
	if ( lwgeom_is_empty((LWGEOM*)comp) )
		return 0.0;
	line = lwcompound_segmentize(comp, 32);
	length = lwline_length_2d(line);
	lwline_free(line);
	return length;
}

int lwcompound_add_lwgeom(LWCOMPOUND *comp, LWGEOM *geom)
{
	LWCOLLECTION *col = (LWCOLLECTION*)comp;
	
	/* Empty things can't continuously join up with other things */
	if ( lwgeom_is_empty(geom) )
	{
		LWDEBUG(4, "Got an empty component for a compound curve!");
		return LW_FAILURE;
	}
	
	if( col->ngeoms > 0 )
	{
		POINT4D last, first;
		/* First point of the component we are adding */
		LWLINE *newline = (LWLINE*)geom;
		/* Last point of the previous component */
		LWLINE *prevline = (LWLINE*)(col->geoms[col->ngeoms-1]);

		getPoint4d_p(newline->points, 0, &first);
		getPoint4d_p(prevline->points, prevline->points->npoints-1, &last);
		
		if ( !(FP_EQUALS(first.x,last.x) && FP_EQUALS(first.y,last.y)) )
		{
			LWDEBUG(4, "Components don't join up end-to-end!");
			LWDEBUGF(4, "first pt (%g %g %g %g) last pt (%g %g %g %g)", first.x, first.y, first.z, first.m, last.x, last.y, last.z, last.m);			
			return LW_FAILURE;
		}
	}
	
	col = lwcollection_add_lwgeom(col, geom);
	return LW_SUCCESS;
}

LWCOMPOUND *
lwcompound_construct_empty(int srid, char hasz, char hasm)
{
        LWCOMPOUND *ret = (LWCOMPOUND*)lwcollection_construct_empty(COMPOUNDTYPE, srid, hasz, hasm);
        return ret;
}

int lwgeom_contains_point(const LWGEOM *geom, const POINT2D *pt)
{
	switch( geom->type )
	{
		case LINETYPE:
			return ptarray_contains_point(((LWLINE*)geom)->points, pt);
		case CIRCSTRINGTYPE:
			return ptarrayarc_contains_point(((LWCIRCSTRING*)geom)->points, pt);
		case COMPOUNDTYPE:
			return lwcompound_contains_point((LWCOMPOUND*)geom, pt);
	}
	lwerror("lwgeom_contains_point failed");
	return LW_FAILURE;
}

int 
lwcompound_contains_point(const LWCOMPOUND *comp, const POINT2D *pt)
{
	int i;
	LWLINE *lwline;
	LWCIRCSTRING *lwcirc;
	int wn = 0;
	int winding_number = 0;
	int result;

	for ( i = 0; i < comp->ngeoms; i++ )
	{
		LWGEOM *lwgeom = comp->geoms[i];
		if ( lwgeom->type == LINETYPE )
		{
			lwline = lwgeom_as_lwline(lwgeom);
			if ( comp->ngeoms == 1 )
			{
				return ptarray_contains_point(lwline->points, pt); 
			}
			else
			{
				/* Don't check closure while doing p-i-p test */
				result = ptarray_contains_point_partial(lwline->points, pt, LW_FALSE, &winding_number);
			}
		}
		else
		{
			lwcirc = lwgeom_as_lwcircstring(lwgeom);
			if ( ! lwcirc ) {
				lwerror("Unexpected component of type %s in compound curve", lwtype_name(lwgeom->type));
				return 0;
			}
			if ( comp->ngeoms == 1 )
			{
				return ptarrayarc_contains_point(lwcirc->points, pt); 				
			}
			else
			{
				/* Don't check closure while doing p-i-p test */
				result = ptarrayarc_contains_point_partial(lwcirc->points, pt, LW_FALSE, &winding_number);
			}
		}

		/* Propogate boundary condition */
		if ( result == LW_BOUNDARY ) 
			return LW_BOUNDARY;

		wn += winding_number;
	}

	/* Outside */
	if (wn == 0)
		return LW_OUTSIDE;
	
	/* Inside */
	return LW_INSIDE;
}	

LWCOMPOUND *
lwcompound_construct_from_lwline(const LWLINE *lwline)
{
  LWCOMPOUND* ogeom = lwcompound_construct_empty(lwline->srid, FLAGS_GET_Z(lwline->flags), FLAGS_GET_M(lwline->flags));
  lwcompound_add_lwgeom(ogeom, lwgeom_clone((LWGEOM*)lwline));
	/* ogeom->bbox = lwline->bbox; */
  return ogeom;
}
