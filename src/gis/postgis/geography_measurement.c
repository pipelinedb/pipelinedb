/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"

#include "../postgis_config.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "liblwgeom.h"         /* For standard geometry types. */
#include "liblwgeom_internal.h"         /* For FP comparators. */
#include "lwgeom_pg.h"       /* For debugging macros. */
#include "geography.h"	     /* For utility functions. */
#include "geography_measurement_trees.h" /* For circ_tree caching */
#include "lwgeom_transform.h" /* For SRID functions */

#ifdef USE_PRE22GEODESIC
/* round to 100 nm precision */
#define INVMINDIST 1.0e9
#else
/* round to 10 nm precision */
#define INVMINDIST 1.0e8
#endif

Datum geography_distance(PG_FUNCTION_ARGS);
Datum geography_distance_uncached(PG_FUNCTION_ARGS);
Datum geography_distance_tree(PG_FUNCTION_ARGS);
Datum geography_dwithin(PG_FUNCTION_ARGS);
Datum geography_dwithin_uncached(PG_FUNCTION_ARGS);
Datum geography_area(PG_FUNCTION_ARGS);
Datum geography_length(PG_FUNCTION_ARGS);
Datum geography_expand(PG_FUNCTION_ARGS);
Datum geography_point_outside(PG_FUNCTION_ARGS);
Datum geography_covers(PG_FUNCTION_ARGS);
Datum geography_bestsrid(PG_FUNCTION_ARGS);
Datum geography_perimeter(PG_FUNCTION_ARGS);
Datum geography_project(PG_FUNCTION_ARGS);
Datum geography_azimuth(PG_FUNCTION_ARGS);
Datum geography_segmentize(PG_FUNCTION_ARGS);

/*
** geography_distance_uncached(GSERIALIZED *g1, GSERIALIZED *g2, double tolerance, boolean use_spheroid)
** returns double distance in meters
*/
PG_FUNCTION_INFO_V1(geography_distance_uncached);
Datum geography_distance_uncached(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom1 = NULL;
	LWGEOM *lwgeom2 = NULL;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double distance;
	/* double tolerance; */
	bool use_spheroid;
	SPHEROID s;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	/* Read our tolerance value. */
	/* tolerance = PG_GETARG_FLOAT8(2); */
    
	/* Read our calculation type. */
	use_spheroid = PG_GETARG_BOOL(3);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);
	
	/* Set to sphere if requested */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	lwgeom1 = lwgeom_from_gserialized(g1);
	lwgeom2 = lwgeom_from_gserialized(g2);

	/* Return NULL on empty arguments. */
	if ( lwgeom_is_empty(lwgeom1) || lwgeom_is_empty(lwgeom2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_NULL();
	}

	/* Make sure we have boxes attached */
	lwgeom_add_bbox_deep(lwgeom1, NULL);
	lwgeom_add_bbox_deep(lwgeom2, NULL);
	
	distance = lwgeom_distance_spheroid(lwgeom1, lwgeom2, &s, FP_TOLERANCE);

	/* Clean up */
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	/* Something went wrong, negative return... should already be eloged, return NULL */
	if ( distance < 0.0 )
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_FLOAT8(distance);
}


/*
** geography_distance(GSERIALIZED *g1, GSERIALIZED *g2, double tolerance, boolean use_spheroid)
** returns double distance in meters
*/
PG_FUNCTION_INFO_V1(geography_distance);
Datum geography_distance(PG_FUNCTION_ARGS)
{
	GSERIALIZED* g1 = NULL;
	GSERIALIZED* g2 = NULL;
	double distance;
	double tolerance;
	bool use_spheroid;
	SPHEROID s;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	/* Read our tolerance value. */
	tolerance = PG_GETARG_FLOAT8(2);

	/* Read our calculation type. */
	use_spheroid = PG_GETARG_BOOL(3);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);
	
	/* Set to sphere if requested */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	/* Return NULL on empty arguments. */
	if ( gserialized_is_empty(g1) || gserialized_is_empty(g2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_NULL();
	}
	
	/* Do the brute force calculation if the cached calculation doesn't tick over */
	if ( LW_FAILURE == geography_distance_cache(fcinfo, g1, g2, &s, &distance) )
	{
		LWGEOM* lwgeom1 = lwgeom_from_gserialized(g1);
		LWGEOM* lwgeom2 = lwgeom_from_gserialized(g2);
		distance = lwgeom_distance_spheroid(lwgeom1, lwgeom2, &s, tolerance);
		lwgeom_free(lwgeom1);
		lwgeom_free(lwgeom2);
	}

	/* Clean up */
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	/* Knock off any funny business at the nanometer level, ticket #2168 */
	distance = round(distance * INVMINDIST) / INVMINDIST;

	/* Something went wrong, negative return... should already be eloged, return NULL */
	if ( distance < 0.0 )
	{
		elog(ERROR, "distance returned negative!");
		PG_RETURN_NULL();
	}

	PG_RETURN_FLOAT8(distance);
}


/*
** geography_dwithin(GSERIALIZED *g1, GSERIALIZED *g2, double tolerance, boolean use_spheroid)
** returns double distance in meters
*/
PG_FUNCTION_INFO_V1(geography_dwithin);
Datum geography_dwithin(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double tolerance;
	double distance;
	bool use_spheroid;
	SPHEROID s;
	int dwithin = LW_FALSE;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	/* Read our tolerance value. */
	tolerance = PG_GETARG_FLOAT8(2);

	/* Read our calculation type. */
	use_spheroid = PG_GETARG_BOOL(3);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);

	/* Set to sphere if requested */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	/* Return FALSE on empty arguments. */
	if ( gserialized_is_empty(g1) || gserialized_is_empty(g2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_BOOL(FALSE);
	}

	/* Do the brute force calculation if the cached calculation doesn't tick over */
	if ( LW_FAILURE == geography_dwithin_cache(fcinfo, g1, g2, &s, tolerance, &dwithin) )
	{
		LWGEOM* lwgeom1 = lwgeom_from_gserialized(g1);
		LWGEOM* lwgeom2 = lwgeom_from_gserialized(g2);
		distance = lwgeom_distance_spheroid(lwgeom1, lwgeom2, &s, tolerance);
		/* Something went wrong... */
		if ( distance < 0.0 )
			elog(ERROR, "lwgeom_distance_spheroid returned negative!");
		dwithin = (distance <= tolerance);
		lwgeom_free(lwgeom1);
		lwgeom_free(lwgeom2);
	}

	/* Clean up */
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	PG_RETURN_BOOL(dwithin);
}


/*
** geography_distance_tree(GSERIALIZED *g1, GSERIALIZED *g2, double tolerance, boolean use_spheroid)
** returns double distance in meters
*/
PG_FUNCTION_INFO_V1(geography_distance_tree);
Datum geography_distance_tree(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double tolerance;
	double distance;
	bool use_spheroid;
	SPHEROID s;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	/* Return FALSE on empty arguments. */
	if ( gserialized_is_empty(g1) || gserialized_is_empty(g2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_FLOAT8(0.0);
	}

	/* Read our tolerance value. */
	tolerance = PG_GETARG_FLOAT8(2);

	/* Read our calculation type. */
	use_spheroid = PG_GETARG_BOOL(3);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);

	/* Set to sphere if requested */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	if  ( geography_tree_distance(g1, g2, &s, tolerance, &distance) == LW_FAILURE )
	{
		elog(ERROR, "geography_distance_tree failed!");
		PG_RETURN_NULL();
	}
	
	PG_RETURN_FLOAT8(distance);
}



/*
** geography_dwithin_uncached(GSERIALIZED *g1, GSERIALIZED *g2, double tolerance, boolean use_spheroid)
** returns double distance in meters
*/
PG_FUNCTION_INFO_V1(geography_dwithin_uncached);
Datum geography_dwithin_uncached(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom1 = NULL;
	LWGEOM *lwgeom2 = NULL;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double tolerance;
	double distance;
	bool use_spheroid;
	SPHEROID s;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	/* Read our tolerance value. */
	tolerance = PG_GETARG_FLOAT8(2);

	/* Read our calculation type. */
	use_spheroid = PG_GETARG_BOOL(3);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);

	/* Set to sphere if requested */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	lwgeom1 = lwgeom_from_gserialized(g1);
	lwgeom2 = lwgeom_from_gserialized(g2);

	/* Return FALSE on empty arguments. */
	if ( lwgeom_is_empty(lwgeom1) || lwgeom_is_empty(lwgeom2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	distance = lwgeom_distance_spheroid(lwgeom1, lwgeom2, &s, tolerance);

	/* Clean up */
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	/* Something went wrong... should already be eloged, return FALSE */
	if ( distance < 0.0 )
	{
		elog(ERROR, "lwgeom_distance_spheroid returned negative!");
		PG_RETURN_BOOL(FALSE);
	}

	PG_RETURN_BOOL(distance <= tolerance);
}


/*
** geography_expand(GSERIALIZED *g) returns *GSERIALIZED
**
** warning, this tricky little function does not expand the
** geometry at all, just re-writes bounding box value to be
** a bit bigger. only useful when passing the result along to
** an index operator (&&)
*/
PG_FUNCTION_INFO_V1(geography_expand);
Datum geography_expand(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g = NULL;
	GSERIALIZED *g_out = NULL;
	double distance;

	/* Get a wholly-owned pointer to the geography */
	g = PG_GETARG_GSERIALIZED_P_COPY(0);

	/* Read our distance value and normalize to unit-sphere. */
	distance = PG_GETARG_FLOAT8(1) / WGS84_RADIUS;

	/* Try the expansion */
	g_out = gserialized_expand(g, distance);

	/* If the expansion fails, the return our input */
	if ( g_out == NULL )
	{
		PG_RETURN_POINTER(g);
	}
	
	if ( g_out != g )
	{
		pfree(g);
	}

	PG_RETURN_POINTER(g_out);
}

/*
** geography_area(GSERIALIZED *g)
** returns double area in meters square
*/
PG_FUNCTION_INFO_V1(geography_area);
Datum geography_area(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	GBOX gbox;
	double area;
	bool use_spheroid = LW_TRUE;
	SPHEROID s;

	/* Get our geometry object loaded into memory. */
	g = PG_GETARG_GSERIALIZED_P(0);

	/* Read our calculation type */
	use_spheroid = PG_GETARG_BOOL(1);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g), &s);

	lwgeom = lwgeom_from_gserialized(g);

	/* EMPTY things have no area */
	if ( lwgeom_is_empty(lwgeom) )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_FLOAT8(0.0);
	}
	
	if ( lwgeom->bbox )
		gbox = *(lwgeom->bbox);
	else
		lwgeom_calculate_gbox_geodetic(lwgeom, &gbox);

#ifdef USE_PRE22GEODESIC
	/* Test for cases that are currently not handled by spheroid code */
	if ( use_spheroid )
	{
		/* We can't circle the poles right now */
		if ( FP_GTEQ(gbox.zmax,1.0) || FP_LTEQ(gbox.zmin,-1.0) )
			use_spheroid = LW_FALSE;
		/* We can't cross the equator right now */
		if ( gbox.zmax > 0.0 && gbox.zmin < 0.0 )
			use_spheroid = LW_FALSE;
	}
#endif /* ifdef USE_PRE22GEODESIC */

	/* User requests spherical calculation, turn our spheroid into a sphere */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	/* Calculate the area */
	if ( use_spheroid )
		area = lwgeom_area_spheroid(lwgeom, &s);
	else
		area = lwgeom_area_sphere(lwgeom, &s);

	/* Clean up */
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(g, 0);

	/* Something went wrong... */
	if ( area < 0.0 )
	{
		elog(ERROR, "lwgeom_area_spher(oid) returned area < 0.0");
		PG_RETURN_NULL();
	}

	PG_RETURN_FLOAT8(area);
}

/*
** geography_perimeter(GSERIALIZED *g)
** returns double perimeter in meters for area features
*/
PG_FUNCTION_INFO_V1(geography_perimeter);
Datum geography_perimeter(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	double length;
	bool use_spheroid = LW_TRUE;
	SPHEROID s;
    int type;

	/* Get our geometry object loaded into memory. */
	g = PG_GETARG_GSERIALIZED_P(0);
	
	/* Only return for area features. */
    type = gserialized_get_type(g);
    if ( ! (type == POLYGONTYPE || type == MULTIPOLYGONTYPE || type == COLLECTIONTYPE) )
    {
		PG_RETURN_FLOAT8(0.0);
    }
	
	lwgeom = lwgeom_from_gserialized(g);

	/* EMPTY things have no perimeter */
	if ( lwgeom_is_empty(lwgeom) )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_FLOAT8(0.0);
	}

	/* Read our calculation type */
	use_spheroid = PG_GETARG_BOOL(1);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g), &s);

	/* User requests spherical calculation, turn our spheroid into a sphere */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	/* Calculate the length */
	length = lwgeom_length_spheroid(lwgeom, &s);

	/* Something went wrong... */
	if ( length < 0.0 )
	{
		elog(ERROR, "lwgeom_length_spheroid returned length < 0.0");
		PG_RETURN_NULL();
	}

	/* Clean up, but not all the way to the point arrays */
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(g, 0);
	PG_RETURN_FLOAT8(length);
}

/*
** geography_length(GSERIALIZED *g)
** returns double length in meters
*/
PG_FUNCTION_INFO_V1(geography_length);
Datum geography_length(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	double length;
	bool use_spheroid = LW_TRUE;
	SPHEROID s;

	/* Get our geometry object loaded into memory. */
	g = PG_GETARG_GSERIALIZED_P(0);
	lwgeom = lwgeom_from_gserialized(g);

	/* EMPTY things have no length */
	if ( lwgeom_is_empty(lwgeom) || lwgeom->type == POLYGONTYPE || lwgeom->type == MULTIPOLYGONTYPE )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_FLOAT8(0.0);
	}

	/* Read our calculation type */
	use_spheroid = PG_GETARG_BOOL(1);

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g), &s);

	/* User requests spherical calculation, turn our spheroid into a sphere */
	if ( ! use_spheroid )
		s.a = s.b = s.radius;

	/* Calculate the length */
	length = lwgeom_length_spheroid(lwgeom, &s);

	/* Something went wrong... */
	if ( length < 0.0 )
	{
		elog(ERROR, "lwgeom_length_spheroid returned length < 0.0");
		PG_RETURN_NULL();
	}

	/* Clean up */
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(g, 0);
	PG_RETURN_FLOAT8(length);
}

/*
** geography_point_outside(GSERIALIZED *g)
** returns point outside the object
*/
PG_FUNCTION_INFO_V1(geography_point_outside);
Datum geography_point_outside(PG_FUNCTION_ARGS)
{
	GBOX gbox;
	GSERIALIZED *g = NULL;
	GSERIALIZED *g_out = NULL;
	size_t g_out_size;
	LWPOINT *lwpoint = NULL;
	POINT2D pt;

	/* Get our geometry object loaded into memory. */
	g = PG_GETARG_GSERIALIZED_P(0);
	
	/* We need the bounding box to get an outside point for area algorithm */
	if ( gserialized_get_gbox_p(g, &gbox) == LW_FAILURE )
	{
		POSTGIS_DEBUG(4,"gserialized_get_gbox_p returned LW_FAILURE");
		elog(ERROR, "Error in gserialized_get_gbox_p calculation.");
		PG_RETURN_NULL();
	}
	POSTGIS_DEBUGF(4, "got gbox %s", gbox_to_string(&gbox));

	/* Get an exterior point, based on this gbox */
	gbox_pt_outside(&gbox, &pt);

	lwpoint = lwpoint_make2d(4326, pt.x, pt.y);

	g_out = gserialized_from_lwgeom((LWGEOM*)lwpoint, 1, &g_out_size);
	SET_VARSIZE(g_out, g_out_size);

	PG_FREE_IF_COPY(g, 0);
	PG_RETURN_POINTER(g_out);

}

/*
** geography_covers(GSERIALIZED *g, GSERIALIZED *g) returns boolean
** Only works for (multi)points and (multi)polygons currently.
** Attempts a simple point-in-polygon test on the polygon and point.
** Current algorithm does not distinguish between points on edge
** and points within.
*/
PG_FUNCTION_INFO_V1(geography_covers);
Datum geography_covers(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom1 = NULL;
	LWGEOM *lwgeom2 = NULL;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	int type1, type2;
	int result = LW_FALSE;

	/* Get our geometry objects loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);

	type1 = gserialized_get_type(g1);
	type2 = gserialized_get_type(g2);

	/* Right now we only handle points and polygons */
	if ( ! ( (type1 == POLYGONTYPE || type1 == MULTIPOLYGONTYPE || type1 == COLLECTIONTYPE) &&
	         (type2 == POINTTYPE || type2 == MULTIPOINTTYPE || type2 == COLLECTIONTYPE) ) )
	{
		elog(ERROR, "geography_covers: only POLYGON and POINT types are currently supported");
		PG_RETURN_NULL();
	}

	/* Construct our working geometries */
	lwgeom1 = lwgeom_from_gserialized(g1);
	lwgeom2 = lwgeom_from_gserialized(g2);

	/* EMPTY never intersects with another geometry */
	if ( lwgeom_is_empty(lwgeom1) || lwgeom_is_empty(lwgeom2) )
	{
		lwgeom_free(lwgeom1);
		lwgeom_free(lwgeom2);
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_BOOL(false);
	}

	/* Calculate answer */
	result = lwgeom_covers_lwgeom_sphere(lwgeom1, lwgeom2);

	/* Clean up */
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	PG_RETURN_BOOL(result);
}

/*
** geography_bestsrid(GSERIALIZED *g, GSERIALIZED *g) returns int
** Utility function. Returns negative SRID numbers that match to the
** numbers handled in code by the transform(lwgeom, srid) function.
** UTM, polar stereographic and mercator as fallback. To be used
** in wrapping existing geometry functions in SQL to provide access
** to them in the geography module.
*/
PG_FUNCTION_INFO_V1(geography_bestsrid);
Datum geography_bestsrid(PG_FUNCTION_ARGS)
{
	GBOX gbox, gbox1, gbox2;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	int empty1 = LW_FALSE;
	int empty2 = LW_FALSE;
	double xwidth, ywidth;
	POINT2D center;

	Datum d1 = PG_GETARG_DATUM(0);
	Datum d2 = PG_GETARG_DATUM(1);

	/* Get our geometry objects loaded into memory. */
	g1 = (GSERIALIZED*)PG_DETOAST_DATUM(d1);
	/* Synchronize our box types */
	gbox1.flags = g1->flags;
	/* Calculate if the geometry is empty. */
	empty1 = gserialized_is_empty(g1);
	/* Calculate a geocentric bounds for the objects */
	if ( ! empty1 && gserialized_get_gbox_p(g1, &gbox1) == LW_FAILURE )
		elog(ERROR, "Error in geography_bestsrid calling gserialized_get_gbox_p(g1, &gbox1)");
	
	POSTGIS_DEBUGF(4, "calculated gbox = %s", gbox_to_string(&gbox1));

	/* If we have a unique second argument, fill in all the necessary variables. */
	if ( d1 != d2 )
	{
		g2 = (GSERIALIZED*)PG_DETOAST_DATUM(d2);
		gbox2.flags = g2->flags;
		empty2 = gserialized_is_empty(g2);
		if ( ! empty2 && gserialized_get_gbox_p(g2, &gbox2) == LW_FAILURE )
			elog(ERROR, "Error in geography_bestsrid calling gserialized_get_gbox_p(g2, &gbox2)");
	}
	/*
	** If no unique second argument, copying the box for the first
	** argument will give us the right answer for all subsequent tests.
	*/
	else
	{
		gbox = gbox2 = gbox1;
	}

	/* Both empty? We don't have an answer. */
	if ( empty1 && empty2 )
		PG_RETURN_NULL();

	/* One empty? We can use the other argument values as infill. Otherwise merge the boxen */
	if ( empty1 )
		gbox = gbox2;
	else if ( empty2 )
		gbox = gbox1;
	else
		gbox_union(&gbox1, &gbox2, &gbox);

	gbox_centroid(&gbox, &center);

	/* Width and height in degrees */
	xwidth = 180.0 * gbox_angular_width(&gbox)  / M_PI;
	ywidth = 180.0 * gbox_angular_height(&gbox) / M_PI;

	POSTGIS_DEBUGF(2, "xwidth %g", xwidth);
	POSTGIS_DEBUGF(2, "ywidth %g", ywidth);
	POSTGIS_DEBUGF(2, "center POINT(%g %g)", center.x, center.y);

	/* Are these data arctic? Lambert Azimuthal Equal Area North. */
	if ( center.y > 70.0 && ywidth < 45.0 )
	{
		PG_RETURN_INT32(SRID_NORTH_LAMBERT);
	}

	/* Are these data antarctic? Lambert Azimuthal Equal Area South. */
	if ( center.y < -70.0 && ywidth < 45.0 )
	{
		PG_RETURN_INT32(SRID_SOUTH_LAMBERT); 
	}

	/*
	** Can we fit these data into one UTM zone?
	** We will assume we can push things as
	** far as a half zone past a zone boundary.
	** Note we have no handling for the date line in here.
	*/
	if ( xwidth < 6.0 )
	{
		int zone = floor((center.x + 180.0) / 6.0);
		
		if ( zone > 59 ) zone = 59;

		/* Are these data below the equator? UTM South. */
		if ( center.y < 0.0 )
		{
			PG_RETURN_INT32( SRID_SOUTH_UTM_START + zone );
		}
		/* Are these data above the equator? UTM North. */
		else
		{
			PG_RETURN_INT32( SRID_NORTH_UTM_START + zone );
		}
	}

	/*
	** Can we fit into a custom LAEA area? (30 degrees high, variable width) 
	** We will allow overlap into adjoining areas, but use a slightly narrower test (25) to try
	** and minimize the worst case.
	** Again, we are hoping the dateline doesn't trip us up much
	*/
	if ( ywidth < 25.0 )
	{
		int xzone = -1;
		int yzone = 3 + floor(center.y / 30.0); /* (range of 0-5) */
		
		/* Equatorial band, 12 zones, 30 degrees wide */
		if ( (yzone == 2 || yzone == 3) && xwidth < 30.0 )
		{
			xzone = 6 + floor(center.x / 30.0);
		}
		/* Temperate band, 8 zones, 45 degrees wide */
		else if ( (yzone == 1 || yzone == 4) && xwidth < 45.0 )
		{
			xzone = 4 + floor(center.x / 45.0);
		}
		/* Arctic band, 4 zones, 90 degrees wide */
		else if ( (yzone == 0 || yzone == 5) && xwidth < 90.0 )
		{
			xzone = 2 + floor(center.x / 90.0);
		}
		
		/* Did we fit into an appropriate xzone? */
		if ( xzone != -1 )
		{
			PG_RETURN_INT32(SRID_LAEA_START + 20 * yzone + xzone);
		}
	}

	/*
	** Running out of options... fall-back to Mercator
	** and hope for the best.
	*/
	PG_RETURN_INT32(SRID_WORLD_MERCATOR);

}

/*
** geography_project(GSERIALIZED *g, distance, azimuth)
** returns point of projection given start point, 
** azimuth in radians (bearing) and distance in meters
*/
PG_FUNCTION_INFO_V1(geography_project);
Datum geography_project(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	LWPOINT *lwp_projected;
	GSERIALIZED *g = NULL;
	GSERIALIZED *g_out = NULL;
	double azimuth, distance;
	SPHEROID s;
	uint32_t type;

	/* Return NULL on NULL distance or geography */
	if ( PG_ARGISNULL(0) || PG_ARGISNULL(1) )
		PG_RETURN_NULL();
	
	/* Get our geometry object loaded into memory. */
	g = PG_GETARG_GSERIALIZED_P(0);
	
	/* Only return for points. */
    type = gserialized_get_type(g);
    if ( type != POINTTYPE )
    {
		elog(ERROR, "ST_Project(geography) is only valid for point inputs");
		PG_RETURN_NULL();
    }

	distance = PG_GETARG_FLOAT8(1); /* Distance in Meters */
	lwgeom = lwgeom_from_gserialized(g);

	/* EMPTY things cannot be projected from */
	if ( lwgeom_is_empty(lwgeom) )
	{
		lwgeom_free(lwgeom);
		elog(ERROR, "ST_Project(geography) cannot project from an empty start point");
		PG_RETURN_NULL();
	}
	
	if ( PG_ARGISNULL(2) )
		azimuth = 0.0;
	else
		azimuth = PG_GETARG_FLOAT8(2); /* Azimuth in Radians */

	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g), &s);

	/* Handle the zero distance case */
	if( FP_EQUALS(distance, 0.0) )
	{
		PG_RETURN_POINTER(g);
	}

	/* Calculate the length */
	lwp_projected = lwgeom_project_spheroid(lwgeom_as_lwpoint(lwgeom), &s, distance, azimuth);

	/* Something went wrong... */
	if ( lwp_projected == NULL )
	{
		elog(ERROR, "lwgeom_project_spheroid returned null");
		PG_RETURN_NULL();
	}

	/* Clean up, but not all the way to the point arrays */
	lwgeom_free(lwgeom);
	g_out = geography_serialize(lwpoint_as_lwgeom(lwp_projected));
	lwpoint_free(lwp_projected);

	PG_FREE_IF_COPY(g, 0);
	PG_RETURN_POINTER(g_out);
}


/*
** geography_azimuth(GSERIALIZED *g1, GSERIALIZED *g2)
** returns direction between points (north = 0)
** azimuth (bearing) and distance
*/
PG_FUNCTION_INFO_V1(geography_azimuth);
Datum geography_azimuth(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom1 = NULL;
	LWGEOM *lwgeom2 = NULL;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double azimuth;
	SPHEROID s;
	uint32_t type1, type2;

	/* Get our geometry object loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
	g2 = PG_GETARG_GSERIALIZED_P(1);
	
	/* Only return for points. */
    type1 = gserialized_get_type(g1);
    type2 = gserialized_get_type(g2);
    if ( type1 != POINTTYPE || type2 != POINTTYPE )
    {
		elog(ERROR, "ST_Azimuth(geography, geography) is only valid for point inputs");
		PG_RETURN_NULL();
    }
	
	lwgeom1 = lwgeom_from_gserialized(g1);
	lwgeom2 = lwgeom_from_gserialized(g2);

	/* EMPTY things cannot be used */
	if ( lwgeom_is_empty(lwgeom1) || lwgeom_is_empty(lwgeom2) )
	{
		lwgeom_free(lwgeom1);
		lwgeom_free(lwgeom2);
		elog(ERROR, "ST_Azimuth(geography, geography) cannot work with empty points");
		PG_RETURN_NULL();
	}
	
	/* Initialize spheroid */
	spheroid_init_from_srid(fcinfo, gserialized_get_srid(g1), &s);

	/* Calculate the direction */
	azimuth = lwgeom_azumith_spheroid(lwgeom_as_lwpoint(lwgeom1), lwgeom_as_lwpoint(lwgeom2), &s);

	/* Clean up */
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	/* Return NULL for unknown (same point) azimuth */
	if( isnan(azimuth) )
	{
		PG_RETURN_NULL();		
	}

	PG_RETURN_FLOAT8(azimuth);
}



/*
** geography_segmentize(GSERIALIZED *g1, double max_seg_length)
** returns densified geometry with no segment longer than max
*/
PG_FUNCTION_INFO_V1(geography_segmentize);
Datum geography_segmentize(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom1 = NULL;
	LWGEOM *lwgeom2 = NULL;
	GSERIALIZED *g1 = NULL;
	GSERIALIZED *g2 = NULL;
	double max_seg_length;
	uint32_t type1;

	/* Get our geometry object loaded into memory. */
	g1 = PG_GETARG_GSERIALIZED_P(0);
    type1 = gserialized_get_type(g1);

	/* Convert max_seg_length from metric units to radians */
	max_seg_length = PG_GETARG_FLOAT8(1) / WGS84_RADIUS;

	/* We can't densify points or points, reflect them back */
    if ( type1 == POINTTYPE || type1 == MULTIPOINTTYPE || gserialized_is_empty(g1) )
		PG_RETURN_POINTER(g1);

	/* Deserialize */
	lwgeom1 = lwgeom_from_gserialized(g1);

	/* Calculate the densified geometry */
	lwgeom2 = lwgeom_segmentize_sphere(lwgeom1, max_seg_length);
	g2 = geography_serialize(lwgeom2);
	
	/* Clean up */
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);
	PG_FREE_IF_COPY(g1, 0);

	PG_RETURN_POINTER(g2);
}


