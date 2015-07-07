/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2011-2015 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * Split polygon by line, line by (multi)line or (multi)polygon boundary,
 * line by point.
 * Returns at most components as a collection.
 * First element of the collection is always the part which
 * remains after the cut, while the second element is the
 * part which has been cut out. We arbitrarely take the part
 * on the *right* of cut lines as the part which has been cut out.
 * For a line cut by a point the part which remains is the one
 * from start of the line to the cut point.
 *
 *
 * Author: Sandro Santilli <strk@keybit.net>
 *
 * Work done for Faunalia (http://www.faunalia.it) with fundings
 * from Regione Toscana - Sistema Informativo per il Governo
 * del Territorio e dell'Ambiente (RT-SIGTA).
 *
 * Thanks to the PostGIS community for sharing poly/line ideas [1]
 *
 * [1] http://trac.osgeo.org/postgis/wiki/UsersWikiSplitPolygonWithLineString
 *
 *
 **********************************************************************/

#include "lwgeom_geos.h"
#include "liblwgeom_internal.h"

#include <string.h>
#include <assert.h>

static LWGEOM* lwline_split_by_line(const LWLINE* lwgeom_in, const LWGEOM* blade_in);
static LWGEOM* lwline_split_by_point(const LWLINE* lwgeom_in, const LWPOINT* blade_in);
static LWGEOM* lwline_split_by_mpoint(const LWLINE* lwgeom_in, const LWMPOINT* blade_in);
static LWGEOM* lwline_split(const LWLINE* lwgeom_in, const LWGEOM* blade_in);
static LWGEOM* lwpoly_split_by_line(const LWPOLY* lwgeom_in, const LWLINE* blade_in);
static LWGEOM* lwcollection_split(const LWCOLLECTION* lwcoll_in, const LWGEOM* blade_in);
static LWGEOM* lwpoly_split(const LWPOLY* lwpoly_in, const LWGEOM* blade_in);

/* Initializes and uses GEOS internally */
static LWGEOM*
lwline_split_by_line(const LWLINE* lwline_in, const LWGEOM* blade_in)
{
	LWGEOM** components;
	LWGEOM* diff;
	LWCOLLECTION* out;
	GEOSGeometry* gdiff; /* difference */
	GEOSGeometry* g1;
	GEOSGeometry* g2;
	int ret;

	/* ASSERT blade_in is LINE or MULTILINE */
	assert (blade_in->type == LINETYPE ||
	        blade_in->type == MULTILINETYPE ||
	        blade_in->type == POLYGONTYPE ||
	        blade_in->type == MULTIPOLYGONTYPE );

	/* Possible outcomes:
	 *
	 *  1. The lines do not cross or overlap
	 *      -> Return a collection with single element
	 *  2. The lines cross
	 *      -> Return a collection of all elements resulting from the split
	 */

	initGEOS(lwgeom_geos_error, lwgeom_geos_error);

	g1 = LWGEOM2GEOS((LWGEOM*)lwline_in, 0);
	if ( ! g1 )
	{
		lwerror("LWGEOM2GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}
	g2 = LWGEOM2GEOS(blade_in, 0);
	if ( ! g2 )
	{
		GEOSGeom_destroy(g1);
		lwerror("LWGEOM2GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	/* If blade is a polygon, pick its boundary */
	if ( blade_in->type == POLYGONTYPE || blade_in->type == MULTIPOLYGONTYPE )
	{
		gdiff = GEOSBoundary(g2);
		GEOSGeom_destroy(g2);
		if ( ! gdiff )
		{
			GEOSGeom_destroy(g1);
			lwerror("GEOSBoundary: %s", lwgeom_geos_errmsg);
			return NULL;
		}
		g2 = gdiff; gdiff = NULL;
	}

	/* If interior intersecton is linear we can't split */
	ret = GEOSRelatePattern(g1, g2, "1********");
	if ( 2 == ret )
	{
		lwerror("GEOSRelatePattern: %s", lwgeom_geos_errmsg);
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		return NULL;
	}
	if ( ret )
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		lwerror("Splitter line has linear intersection with input");
		return NULL;
	}


	gdiff = GEOSDifference(g1,g2);
	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);
	if (gdiff == NULL)
	{
		lwerror("GEOSDifference: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	diff = GEOS2LWGEOM(gdiff, FLAGS_GET_Z(lwline_in->flags));
	GEOSGeom_destroy(gdiff);
	if (NULL == diff)
	{
		lwerror("GEOS2LWGEOM: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	out = lwgeom_as_lwcollection(diff);
	if ( ! out )
	{
		components = lwalloc(sizeof(LWGEOM*)*1);
		components[0] = diff;
		out = lwcollection_construct(COLLECTIONTYPE, lwline_in->srid,
		                             NULL, 1, components);
	}
	else
	{
	  /* Set SRID */
		lwgeom_set_srid((LWGEOM*)out, lwline_in->srid);
	  /* Force collection type */
	  out->type = COLLECTIONTYPE;
	}


	return (LWGEOM*)out;
}

static LWGEOM*
lwline_split_by_point(const LWLINE* lwline_in, const LWPOINT* blade_in)
{
	LWMLINE* out;

	out = lwmline_construct_empty(lwline_in->srid,
		FLAGS_GET_Z(lwline_in->flags),
		FLAGS_GET_M(lwline_in->flags));
	if ( lwline_split_by_point_to(lwline_in, blade_in, out) < 2 )
	{
		lwmline_add_lwline(out, lwline_clone(lwline_in));
	}

	/* Turn multiline into collection */
	out->type = COLLECTIONTYPE;

	return (LWGEOM*)out;
}

static LWGEOM*
lwline_split_by_mpoint(const LWLINE* lwline_in, const LWMPOINT* mp)
{
  LWMLINE* out;
  int i, j;

  out = lwmline_construct_empty(lwline_in->srid,
          FLAGS_GET_Z(lwline_in->flags),
          FLAGS_GET_M(lwline_in->flags));
  lwmline_add_lwline(out, lwline_clone(lwline_in));

  for (i=0; i<mp->ngeoms; ++i)
  {
    for (j=0; j<out->ngeoms; ++j)
    {
      lwline_in = out->geoms[j];
      LWPOINT *blade_in = mp->geoms[i];
      int ret = lwline_split_by_point_to(lwline_in, blade_in, out);
      if ( 2 == ret )
      {
        /* the point splits this line,
         * 2 splits were added to collection.
         * We'll move the latest added into
         * the slot of the current one.
         */
        lwline_free(out->geoms[j]);
        out->geoms[j] = out->geoms[--out->ngeoms];
      }
    }
  }

  /* Turn multiline into collection */
  out->type = COLLECTIONTYPE;

  return (LWGEOM*)out;
}

int
lwline_split_by_point_to(const LWLINE* lwline_in, const LWPOINT* blade_in,
                         LWMLINE* v)
{
	double loc, dist;
	POINT4D pt, pt_projected;
	POINTARRAY* pa1;
	POINTARRAY* pa2;
	double vstol; /* vertex snap tolerance */

	/* Possible outcomes:
	 *
	 *  1. The point is not on the line or on the boundary
	 *      -> Leave collection untouched, return 0
	 *  2. The point is on the boundary
	 *      -> Leave collection untouched, return 1
	 *  3. The point is in the line
	 *      -> Push 2 elements on the collection:
	 *         o start_point - cut_point
	 *         o cut_point - last_point
	 *      -> Return 2
	 */

	getPoint4d_p(blade_in->point, 0, &pt);
	loc = ptarray_locate_point(lwline_in->points, &pt, &dist, &pt_projected);

	/* lwnotice("Location: %g -- Distance: %g", loc, dist); */

	if ( dist > 0 )   /* TODO: accept a tolerance ? */
	{
		/* No intersection */
		return 0;
	}

	if ( loc == 0 || loc == 1 )
	{
		/* Intersection is on the boundary */
		return 1;
	}

	/* There is a real intersection, let's get two substrings */

	/* Compute vertex snap tolerance based on line length
	 * TODO: take as parameter ? */
	vstol = ptarray_length_2d(lwline_in->points) / 1e14;

	pa1 = ptarray_substring(lwline_in->points, 0, loc, vstol);
	pa2 = ptarray_substring(lwline_in->points, loc, 1, vstol);

	/* NOTE: I've seen empty pointarrays with loc != 0 and loc != 1 */
	if ( pa1->npoints == 0 || pa2->npoints == 0 ) {
		ptarray_free(pa1);
		ptarray_free(pa2);
		/* Intersection is on the boundary */
		return 1;
	}

	lwmline_add_lwline(v, lwline_construct(SRID_UNKNOWN, NULL, pa1));
	lwmline_add_lwline(v, lwline_construct(SRID_UNKNOWN, NULL, pa2));
	return 2;
}

static LWGEOM*
lwline_split(const LWLINE* lwline_in, const LWGEOM* blade_in)
{
	switch (blade_in->type)
	{
	case POINTTYPE:
		return lwline_split_by_point(lwline_in, (LWPOINT*)blade_in);
	case MULTIPOINTTYPE:
		return lwline_split_by_mpoint(lwline_in, (LWMPOINT*)blade_in);

	case LINETYPE:
	case MULTILINETYPE:
	case POLYGONTYPE:
	case MULTIPOLYGONTYPE:
		return lwline_split_by_line(lwline_in, blade_in);

	default:
		lwerror("Splitting a Line by a %s is unsupported",
		        lwtype_name(blade_in->type));
		return NULL;
	}
	return NULL;
}

/* Initializes and uses GEOS internally */
static LWGEOM*
lwpoly_split_by_line(const LWPOLY* lwpoly_in, const LWLINE* blade_in)
{
	LWCOLLECTION* out;
	GEOSGeometry* g1;
	GEOSGeometry* g2;
	GEOSGeometry* g1_bounds;
	GEOSGeometry* polygons;
	const GEOSGeometry *vgeoms[1];
	int i,n;
	int hasZ = FLAGS_GET_Z(lwpoly_in->flags);


	/* Possible outcomes:
	 *
	 *  1. The line does not split the polygon
	 *      -> Return a collection with single element
	 *  2. The line does split the polygon
	 *      -> Return a collection of all elements resulting from the split
	 */

	initGEOS(lwgeom_geos_error, lwgeom_geos_error);

	g1 = LWGEOM2GEOS((LWGEOM*)lwpoly_in, 0);
	if ( NULL == g1 )
	{
		lwerror("LWGEOM2GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}
	g1_bounds = GEOSBoundary(g1);
	if ( NULL == g1_bounds )
	{
		GEOSGeom_destroy(g1);
		lwerror("GEOSBoundary: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = LWGEOM2GEOS((LWGEOM*)blade_in, 0);
	if ( NULL == g2 )
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g1_bounds);
		lwerror("LWGEOM2GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	vgeoms[0] = GEOSUnion(g1_bounds, g2);
	if ( NULL == vgeoms[0] )
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g1_bounds);
		lwerror("GEOSUnion: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	/* debugging..
		lwnotice("Bounds poly: %s",
		               lwgeom_to_ewkt(GEOS2LWGEOM(g1_bounds, hasZ)));
		lwnotice("Line: %s",
		               lwgeom_to_ewkt(GEOS2LWGEOM(g2, hasZ)));

		lwnotice("Noded bounds: %s",
		               lwgeom_to_ewkt(GEOS2LWGEOM(vgeoms[0], hasZ)));
	*/

	polygons = GEOSPolygonize(vgeoms, 1);
	if ( NULL == polygons )
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g1_bounds);
		GEOSGeom_destroy((GEOSGeometry*)vgeoms[0]);
		lwerror("GEOSPolygonize: %s", lwgeom_geos_errmsg);
		return NULL;
	}

#if PARANOIA_LEVEL > 0
	if ( GEOSGeometryTypeId(polygons) != COLLECTIONTYPE )
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g1_bounds);
		GEOSGeom_destroy((GEOSGeometry*)vgeoms[0]);
		GEOSGeom_destroy(polygons);
		lwerror("Unexpected return from GEOSpolygonize");
		return 0;
	}
#endif

	/* We should now have all polygons, just skip
	 * the ones which are in holes of the original
	 * geometries and return the rest in a collection
	 */
	n = GEOSGetNumGeometries(polygons);
	out = lwcollection_construct_empty(COLLECTIONTYPE, lwpoly_in->srid,
				     hasZ, 0);
	/* Allocate space for all polys */
	out->geoms = lwrealloc(out->geoms, sizeof(LWGEOM*)*n);
	assert(0 == out->ngeoms);
	for (i=0; i<n; ++i)
	{
		GEOSGeometry* pos; /* point on surface */
		const GEOSGeometry* p = GEOSGetGeometryN(polygons, i);
		int contains;

		pos = GEOSPointOnSurface(p);
		if ( ! pos )
		{
			GEOSGeom_destroy(g1);
			GEOSGeom_destroy(g2);
			GEOSGeom_destroy(g1_bounds);
			GEOSGeom_destroy((GEOSGeometry*)vgeoms[0]);
			GEOSGeom_destroy(polygons);
			lwerror("GEOSPointOnSurface: %s", lwgeom_geos_errmsg);
			return NULL;
		}

		contains = GEOSContains(g1, pos);
		if ( 2 == contains )
		{
			GEOSGeom_destroy(g1);
			GEOSGeom_destroy(g2);
			GEOSGeom_destroy(g1_bounds);
			GEOSGeom_destroy((GEOSGeometry*)vgeoms[0]);
			GEOSGeom_destroy(polygons);
			GEOSGeom_destroy(pos);
			lwerror("GEOSContains: %s", lwgeom_geos_errmsg);
			return NULL;
		}

		GEOSGeom_destroy(pos);

		if ( 0 == contains )
		{
			/* Original geometry doesn't contain
			 * a point in this ring, must be an hole
			 */
			continue;
		}

		out->geoms[out->ngeoms++] = GEOS2LWGEOM(p, hasZ);
	}

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);
	GEOSGeom_destroy(g1_bounds);
	GEOSGeom_destroy((GEOSGeometry*)vgeoms[0]);
	GEOSGeom_destroy(polygons);

	return (LWGEOM*)out;
}

static LWGEOM*
lwcollection_split(const LWCOLLECTION* lwcoll_in, const LWGEOM* blade_in)
{
	LWGEOM** split_vector=NULL;
	LWCOLLECTION* out;
	size_t split_vector_capacity;
	size_t split_vector_size=0;
	size_t i,j;

	split_vector_capacity=8;
	split_vector = lwalloc(split_vector_capacity * sizeof(LWGEOM*));
	if ( ! split_vector )
	{
		lwerror("Out of virtual memory");
		return NULL;
	}

	for (i=0; i<lwcoll_in->ngeoms; ++i)
	{
		LWCOLLECTION* col;
		LWGEOM* split = lwgeom_split(lwcoll_in->geoms[i], blade_in);
		/* an exception should prevent this from ever returning NULL */
		if ( ! split ) return NULL;

		col = lwgeom_as_lwcollection(split);
		/* Output, if any, will always be a collection */
		assert(col);

		/* Reallocate split_vector if needed */
		if ( split_vector_size + col->ngeoms > split_vector_capacity )
		{
			/* NOTE: we could be smarter on reallocations here */
			split_vector_capacity += col->ngeoms;
			split_vector = lwrealloc(split_vector,
			                         split_vector_capacity * sizeof(LWGEOM*));
			if ( ! split_vector )
			{
				lwerror("Out of virtual memory");
				return NULL;
			}
		}

		for (j=0; j<col->ngeoms; ++j)
		{
			col->geoms[j]->srid = SRID_UNKNOWN; /* strip srid */
			split_vector[split_vector_size++] = col->geoms[j];
		}
		lwfree(col->geoms);
		lwfree(col);
	}

	/* Now split_vector has split_vector_size geometries */
	out = lwcollection_construct(COLLECTIONTYPE, lwcoll_in->srid,
	                             NULL, split_vector_size, split_vector);

	return (LWGEOM*)out;
}

static LWGEOM*
lwpoly_split(const LWPOLY* lwpoly_in, const LWGEOM* blade_in)
{
	switch (blade_in->type)
	{
	case LINETYPE:
		return lwpoly_split_by_line(lwpoly_in, (LWLINE*)blade_in);
	default:
		lwerror("Splitting a Polygon by a %s is unsupported",
		        lwtype_name(blade_in->type));
		return NULL;
	}
	return NULL;
}

/* exported */
LWGEOM*
lwgeom_split(const LWGEOM* lwgeom_in, const LWGEOM* blade_in)
{
	switch (lwgeom_in->type)
	{
	case LINETYPE:
		return lwline_split((const LWLINE*)lwgeom_in, blade_in);

	case POLYGONTYPE:
		return lwpoly_split((const LWPOLY*)lwgeom_in, blade_in);

	case MULTIPOLYGONTYPE:
	case MULTILINETYPE:
	case COLLECTIONTYPE:
		return lwcollection_split((const LWCOLLECTION*)lwgeom_in, blade_in);

	default:
		lwerror("Splitting of %s geometries is unsupported",
		        lwtype_name(lwgeom_in->type));
		return NULL;
	}

}

