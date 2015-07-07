/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"

void
lwmline_release(LWMLINE *lwmline)
{
	lwgeom_release(lwmline_as_lwgeom(lwmline));
}

LWMLINE *
lwmline_construct_empty(int srid, char hasz, char hasm)
{
	LWMLINE *ret = (LWMLINE*)lwcollection_construct_empty(MULTILINETYPE, srid, hasz, hasm);
	return ret;
}



LWMLINE* lwmline_add_lwline(LWMLINE *mobj, const LWLINE *obj)
{
	return (LWMLINE*)lwcollection_add_lwgeom((LWCOLLECTION*)mobj, (LWGEOM*)obj);
}

/**
* Re-write the measure ordinate (or add one, if it isn't already there) interpolating
* the measure between the supplied start and end values.
*/
LWMLINE*
lwmline_measured_from_lwmline(const LWMLINE *lwmline, double m_start, double m_end)
{
	int i = 0;
	int hasm = 0, hasz = 0;
	double length = 0.0, length_so_far = 0.0;
	double m_range = m_end - m_start;
	LWGEOM **geoms = NULL;

	if ( lwmline->type != MULTILINETYPE )
	{
		lwerror("lwmline_measured_from_lmwline: only multiline types supported");
		return NULL;
	}

	hasz = FLAGS_GET_Z(lwmline->flags);
	hasm = 1;

	/* Calculate the total length of the mline */
	for ( i = 0; i < lwmline->ngeoms; i++ )
	{
		LWLINE *lwline = (LWLINE*)lwmline->geoms[i];
		if ( lwline->points && lwline->points->npoints > 1 )
		{
			length += ptarray_length_2d(lwline->points);
		}
	}

	if ( lwgeom_is_empty((LWGEOM*)lwmline) )
	{
		return (LWMLINE*)lwcollection_construct_empty(MULTILINETYPE, lwmline->srid, hasz, hasm);
	}

	geoms = lwalloc(sizeof(LWGEOM*) * lwmline->ngeoms);

	for ( i = 0; i < lwmline->ngeoms; i++ )
	{
		double sub_m_start, sub_m_end;
		double sub_length = 0.0;
		LWLINE *lwline = (LWLINE*)lwmline->geoms[i];

		if ( lwline->points && lwline->points->npoints > 1 )
		{
			sub_length = ptarray_length_2d(lwline->points);
		}

		sub_m_start = (m_start + m_range * length_so_far / length);
		sub_m_end = (m_start + m_range * (length_so_far + sub_length) / length);

		geoms[i] = (LWGEOM*)lwline_measured_from_lwline(lwline, sub_m_start, sub_m_end);

		length_so_far += sub_length;
	}

	return (LWMLINE*)lwcollection_construct(lwmline->type, lwmline->srid, NULL, lwmline->ngeoms, geoms);
}

void lwmline_free(LWMLINE *mline)
{
	int i;
	if ( ! mline ) return;
	
	if ( mline->bbox )
		lwfree(mline->bbox);

	for ( i = 0; i < mline->ngeoms; i++ )
		if ( mline->geoms && mline->geoms[i] )
			lwline_free(mline->geoms[i]);

	if ( mline->geoms )
		lwfree(mline->geoms);

	lwfree(mline);
}
