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

/* basic LWLINE functions */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"



/*
 * Construct a new LWLINE.  points will *NOT* be copied
 * use SRID=SRID_UNKNOWN for unknown SRID (will have 8bit type's S = 0)
 */
LWLINE *
lwline_construct(int srid, GBOX *bbox, POINTARRAY *points)
{
	LWLINE *result;
	result = (LWLINE*) lwalloc(sizeof(LWLINE));

	LWDEBUG(2, "lwline_construct called.");

	result->type = LINETYPE;
	
	result->flags = points->flags;
	FLAGS_SET_BBOX(result->flags, bbox?1:0);

	LWDEBUGF(3, "lwline_construct type=%d", result->type);

	result->srid = srid;
	result->points = points;
	result->bbox = bbox;

	return result;
}

LWLINE *
lwline_construct_empty(int srid, char hasz, char hasm)
{
	LWLINE *result = lwalloc(sizeof(LWLINE));
	result->type = LINETYPE;
	result->flags = gflags(hasz,hasm,0);
	result->srid = srid;
	result->points = ptarray_construct_empty(hasz, hasm, 1);
	result->bbox = NULL;
	return result;
}


void lwline_free (LWLINE  *line)
{
	if ( ! line ) return;
	
	if ( line->bbox )
		lwfree(line->bbox);
	if ( line->points )
		ptarray_free(line->points);
	lwfree(line);
}


void printLWLINE(LWLINE *line)
{
	lwnotice("LWLINE {");
	lwnotice("    ndims = %i", (int)FLAGS_NDIMS(line->flags));
	lwnotice("    srid = %i", (int)line->srid);
	printPA(line->points);
	lwnotice("}");
}

/* @brief Clone LWLINE object. Serialized point lists are not copied.
 *
 * @see ptarray_clone 
 */
LWLINE *
lwline_clone(const LWLINE *g)
{
	LWLINE *ret = lwalloc(sizeof(LWLINE));

	LWDEBUGF(2, "lwline_clone called with %p", g);

	memcpy(ret, g, sizeof(LWLINE));

	ret->points = ptarray_clone(g->points);

	if ( g->bbox ) ret->bbox = gbox_copy(g->bbox);
	return ret;
}

/* Deep clone LWLINE object. POINTARRAY *is* copied. */
LWLINE *
lwline_clone_deep(const LWLINE *g)
{
	LWLINE *ret = lwalloc(sizeof(LWLINE));

	LWDEBUGF(2, "lwline_clone_deep called with %p", g);
	memcpy(ret, g, sizeof(LWLINE));

	if ( g->bbox ) ret->bbox = gbox_copy(g->bbox);
	if ( g->points ) ret->points = ptarray_clone_deep(g->points);
	FLAGS_SET_READONLY(ret->flags,0);

	return ret;
}


void
lwline_release(LWLINE *lwline)
{
	lwgeom_release(lwline_as_lwgeom(lwline));
}

void
lwline_reverse(LWLINE *line)
{
	if ( lwline_is_empty(line) ) return;
	ptarray_reverse(line->points);
}

LWLINE *
lwline_segmentize2d(LWLINE *line, double dist)
{
	POINTARRAY *segmentized = ptarray_segmentize2d(line->points, dist);
	if ( ! segmentized ) return NULL;
	return lwline_construct(line->srid, NULL, segmentized);
}

/* check coordinate equality  */
char
lwline_same(const LWLINE *l1, const LWLINE *l2)
{
	return ptarray_same(l1->points, l2->points);
}

/*
 * Construct a LWLINE from an array of point and line geometries
 * LWLINE dimensions are large enough to host all input dimensions.
 */
LWLINE *
lwline_from_lwgeom_array(int srid, uint32_t ngeoms, LWGEOM **geoms)
{
 	int i;
	int hasz = LW_FALSE;
	int hasm = LW_FALSE;
	POINTARRAY *pa;
	LWLINE *line;
	POINT4D pt;

	/*
	 * Find output dimensions, check integrity
	 */
	for (i=0; i<ngeoms; i++)
	{
		if ( FLAGS_GET_Z(geoms[i]->flags) ) hasz = LW_TRUE;
		if ( FLAGS_GET_M(geoms[i]->flags) ) hasm = LW_TRUE;
		if ( hasz && hasm ) break; /* Nothing more to learn! */
	}

	/* ngeoms should be a guess about how many points we have in input */
	pa = ptarray_construct_empty(hasz, hasm, ngeoms);
	
	for ( i=0; i < ngeoms; i++ )
	{
		LWGEOM *g = geoms[i];

		if ( lwgeom_is_empty(g) ) continue;

		if ( g->type == POINTTYPE )
		{
			lwpoint_getPoint4d_p((LWPOINT*)g, &pt);
			ptarray_append_point(pa, &pt, LW_TRUE);
		}
		else if ( g->type == LINETYPE )
		{
			ptarray_append_ptarray(pa, ((LWLINE*)g)->points, -1);
		}
		else
		{
			ptarray_free(pa);
			lwerror("lwline_from_ptarray: invalid input type: %s", lwtype_name(g->type));
			return NULL;
		}
	}

	if ( pa->npoints > 0 )
		line = lwline_construct(srid, NULL, pa);
	else  {
		/* Is this really any different from the above ? */
		ptarray_free(pa);
		line = lwline_construct_empty(srid, hasz, hasm);
	}
	
	return line;
}

/*
 * Construct a LWLINE from an array of LWPOINTs
 * LWLINE dimensions are large enough to host all input dimensions.
 */
LWLINE *
lwline_from_ptarray(int srid, uint32_t npoints, LWPOINT **points)
{
 	int i;
	int hasz = LW_FALSE;
	int hasm = LW_FALSE;
	POINTARRAY *pa;
	LWLINE *line;
	POINT4D pt;

	/*
	 * Find output dimensions, check integrity
	 */
	for (i=0; i<npoints; i++)
	{
		if ( points[i]->type != POINTTYPE )
		{
			lwerror("lwline_from_ptarray: invalid input type: %s", lwtype_name(points[i]->type));
			return NULL;
		}
		if ( FLAGS_GET_Z(points[i]->flags) ) hasz = LW_TRUE;
		if ( FLAGS_GET_M(points[i]->flags) ) hasm = LW_TRUE;
		if ( hasz && hasm ) break; /* Nothing more to learn! */
	}

	pa = ptarray_construct_empty(hasz, hasm, npoints);
	
	for ( i=0; i < npoints; i++ )
	{
		if ( ! lwpoint_is_empty(points[i]) )
		{
			lwpoint_getPoint4d_p(points[i], &pt);
			ptarray_append_point(pa, &pt, LW_TRUE);
		}
	}

	if ( pa->npoints > 0 )
		line = lwline_construct(srid, NULL, pa);
	else 
		line = lwline_construct_empty(srid, hasz, hasm);
	
	return line;
}

/*
 * Construct a LWLINE from a LWMPOINT
 */
LWLINE *
lwline_from_lwmpoint(int srid, const LWMPOINT *mpoint)
{
	uint32_t i;
	POINTARRAY *pa = NULL;
	LWGEOM *lwgeom = (LWGEOM*)mpoint;
	POINT4D pt;

	char hasz = lwgeom_has_z(lwgeom);
	char hasm = lwgeom_has_m(lwgeom);
	uint32_t npoints = mpoint->ngeoms;

	if ( lwgeom_is_empty(lwgeom) ) 
	{
		return lwline_construct_empty(srid, hasz, hasm);
	}

	pa = ptarray_construct(hasz, hasm, npoints);

	for (i=0; i < npoints; i++)
	{
		getPoint4d_p(mpoint->geoms[i]->point, 0, &pt);
		ptarray_set_point4d(pa, i, &pt);
	}
	
	LWDEBUGF(3, "lwline_from_lwmpoint: constructed pointarray for %d points", mpoint->ngeoms);

	return lwline_construct(srid, NULL, pa);
}

/**
* Returns freshly allocated #LWPOINT that corresponds to the index where.
* Returns NULL if the geometry is empty or the index invalid.
*/
LWPOINT*
lwline_get_lwpoint(LWLINE *line, int where)
{
	POINT4D pt;
	LWPOINT *lwpoint;
	POINTARRAY *pa;

	if ( lwline_is_empty(line) || where < 0 || where >= line->points->npoints )
		return NULL;

	pa = ptarray_construct_empty(FLAGS_GET_Z(line->flags), FLAGS_GET_M(line->flags), 1);
	pt = getPoint4d(line->points, where);
	ptarray_append_point(pa, &pt, LW_TRUE);
	lwpoint = lwpoint_construct(line->srid, NULL, pa);
	return lwpoint;
}


int
lwline_add_lwpoint(LWLINE *line, LWPOINT *point, int where)
{
	POINT4D pt;	
	getPoint4d_p(point->point, 0, &pt);

	if ( ptarray_insert_point(line->points, &pt, where) != LW_SUCCESS )
		return LW_FAILURE;

	/* Update the bounding box */
	lwgeom_drop_bbox(lwline_as_lwgeom(line));
	lwgeom_add_bbox(lwline_as_lwgeom(line));
	
	return LW_SUCCESS;
}



LWLINE *
lwline_removepoint(LWLINE *line, uint32_t index)
{
	POINTARRAY *newpa;
	LWLINE *ret;

	newpa = ptarray_removePoint(line->points, index);

	ret = lwline_construct(line->srid, NULL, newpa);
	lwgeom_add_bbox((LWGEOM *) ret);

	return ret;
}

/*
 * Note: input will be changed, make sure you have permissions for this.
 */
void
lwline_setPoint4d(LWLINE *line, uint32_t index, POINT4D *newpoint)
{
	ptarray_set_point4d(line->points, index, newpoint);
	/* Update the box, if there is one to update */
	if ( line->bbox )
	{
		lwgeom_drop_bbox((LWGEOM*)line);
		lwgeom_add_bbox((LWGEOM*)line);
	}
}

/**
* Re-write the measure ordinate (or add one, if it isn't already there) interpolating
* the measure between the supplied start and end values.
*/
LWLINE*
lwline_measured_from_lwline(const LWLINE *lwline, double m_start, double m_end)
{
	int i = 0;
	int hasm = 0, hasz = 0;
	int npoints = 0;
	double length = 0.0;
	double length_so_far = 0.0;
	double m_range = m_end - m_start;
	double m;
	POINTARRAY *pa = NULL;
	POINT3DZ p1, p2;

	if ( lwline->type != LINETYPE )
	{
		lwerror("lwline_construct_from_lwline: only line types supported");
		return NULL;
	}

	hasz = FLAGS_GET_Z(lwline->flags);
	hasm = 1;

	/* Null points or npoints == 0 will result in empty return geometry */
	if ( lwline->points )
	{
		npoints = lwline->points->npoints;
		length = ptarray_length_2d(lwline->points);
		getPoint3dz_p(lwline->points, 0, &p1);
	}

	pa = ptarray_construct(hasz, hasm, npoints);

	for ( i = 0; i < npoints; i++ )
	{
		POINT4D q;
		POINT2D a, b;
		getPoint3dz_p(lwline->points, i, &p2);
		a.x = p1.x;
		a.y = p1.y;
		b.x = p2.x;
		b.y = p2.y;
		length_so_far += distance2d_pt_pt(&a, &b);
		if ( length > 0.0 )
			m = m_start + m_range * length_so_far / length;
		else
			m = 0.0;
		q.x = p2.x;
		q.y = p2.y;
		q.z = p2.z;
		q.m = m;
		ptarray_set_point4d(pa, i, &q);
		p1 = p2;
	}

	return lwline_construct(lwline->srid, NULL, pa);
}

LWGEOM*
lwline_remove_repeated_points(LWLINE *lwline)
{
	POINTARRAY* npts = ptarray_remove_repeated_points(lwline->points);

	LWDEBUGF(3, "lwline_remove_repeated_points: npts %p", npts);

	return (LWGEOM*)lwline_construct(lwline->srid,
	                                 lwline->bbox ? gbox_copy(lwline->bbox) : 0,
	                                 npts);
}

int
lwline_is_closed(const LWLINE *line)
{
	if (FLAGS_GET_Z(line->flags))
		return ptarray_is_closed_3d(line->points);

	return ptarray_is_closed_2d(line->points);
}


LWLINE*
lwline_force_dims(const LWLINE *line, int hasz, int hasm)
{
	POINTARRAY *pdims = NULL;
	LWLINE *lineout;
	
	/* Return 2D empty */
	if( lwline_is_empty(line) )
	{
		lineout = lwline_construct_empty(line->srid, hasz, hasm);
	}
	else
	{	
		pdims = ptarray_force_dims(line->points, hasz, hasm);
		lineout = lwline_construct(line->srid, NULL, pdims);
	}
	lineout->type = line->type;
	return lineout;
}

int lwline_is_empty(const LWLINE *line)
{
	if ( !line->points || line->points->npoints < 1 )
		return LW_TRUE;
	return LW_FALSE;
}


int lwline_count_vertices(LWLINE *line)
{
	assert(line);
	if ( ! line->points )
		return 0;
	return line->points->npoints;
}

LWLINE* lwline_simplify(const LWLINE *iline, double dist)
{
	LWLINE *oline;

	LWDEBUG(2, "function called");

	/* Skip empty case */
	if( lwline_is_empty(iline) )
		return lwline_clone(iline);
		
	static const int minvertices = 0; /* TODO: allow setting this */
	oline = lwline_construct(iline->srid, NULL, ptarray_simplify(iline->points, dist, minvertices));
	oline->type = iline->type;
	return oline;
}

double lwline_length(const LWLINE *line)
{
	if ( lwline_is_empty(line) )
		return 0.0;
	return ptarray_length(line->points);
}

double lwline_length_2d(const LWLINE *line)
{
	if ( lwline_is_empty(line) )
		return 0.0;
	return ptarray_length_2d(line->points);
}



LWLINE* lwline_grid(const LWLINE *line, const gridspec *grid)
{
	LWLINE *oline;
	POINTARRAY *opa;

	opa = ptarray_grid(line->points, grid);

	/* Skip line3d with less then 2 points */
	if ( opa->npoints < 2 ) return NULL;

	/* TODO: grid bounding box... */
	oline = lwline_construct(line->srid, NULL, opa);

	return oline;
}

