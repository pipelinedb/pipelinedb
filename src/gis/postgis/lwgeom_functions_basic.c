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

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/array.h"
#include "utils/geo_decls.h"

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

Datum LWGEOM_mem_size(PG_FUNCTION_ARGS);
Datum LWGEOM_summary(PG_FUNCTION_ARGS);
Datum LWGEOM_npoints(PG_FUNCTION_ARGS);
Datum LWGEOM_nrings(PG_FUNCTION_ARGS);
Datum LWGEOM_area_polygon(PG_FUNCTION_ARGS);
Datum postgis_uses_stats(PG_FUNCTION_ARGS);
Datum postgis_autocache_bbox(PG_FUNCTION_ARGS);
Datum postgis_scripts_released(PG_FUNCTION_ARGS);
Datum postgis_version(PG_FUNCTION_ARGS);
Datum postgis_lib_version(PG_FUNCTION_ARGS);
Datum postgis_svn_version(PG_FUNCTION_ARGS);
Datum postgis_libxml_version(PG_FUNCTION_ARGS);
Datum postgis_lib_build_date(PG_FUNCTION_ARGS);
Datum LWGEOM_length2d_linestring(PG_FUNCTION_ARGS);
Datum LWGEOM_length_linestring(PG_FUNCTION_ARGS);
Datum LWGEOM_perimeter2d_poly(PG_FUNCTION_ARGS);
Datum LWGEOM_perimeter_poly(PG_FUNCTION_ARGS);

Datum LWGEOM_maxdistance2d_linestring(PG_FUNCTION_ARGS);
Datum LWGEOM_mindistance2d(PG_FUNCTION_ARGS);
Datum LWGEOM_closestpoint(PG_FUNCTION_ARGS);
Datum LWGEOM_shortestline2d(PG_FUNCTION_ARGS);
Datum LWGEOM_longestline2d(PG_FUNCTION_ARGS);
Datum LWGEOM_dwithin(PG_FUNCTION_ARGS);
Datum LWGEOM_dfullywithin(PG_FUNCTION_ARGS);

Datum LWGEOM_maxdistance3d(PG_FUNCTION_ARGS);
Datum LWGEOM_mindistance3d(PG_FUNCTION_ARGS);
Datum LWGEOM_closestpoint3d(PG_FUNCTION_ARGS);
Datum LWGEOM_shortestline3d(PG_FUNCTION_ARGS);
Datum LWGEOM_longestline3d(PG_FUNCTION_ARGS);
Datum LWGEOM_dwithin3d(PG_FUNCTION_ARGS);
Datum LWGEOM_dfullywithin3d(PG_FUNCTION_ARGS);

Datum LWGEOM_inside_circle_point(PG_FUNCTION_ARGS);
Datum LWGEOM_collect(PG_FUNCTION_ARGS);
Datum LWGEOM_collect_garray(PG_FUNCTION_ARGS);
Datum LWGEOM_expand(PG_FUNCTION_ARGS);
Datum LWGEOM_to_BOX(PG_FUNCTION_ARGS);
Datum LWGEOM_envelope(PG_FUNCTION_ARGS);
Datum LWGEOM_isempty(PG_FUNCTION_ARGS);
Datum LWGEOM_segmentize2d(PG_FUNCTION_ARGS);
Datum LWGEOM_reverse(PG_FUNCTION_ARGS);
Datum LWGEOM_force_clockwise_poly(PG_FUNCTION_ARGS);
Datum LWGEOM_force_sfs(PG_FUNCTION_ARGS);
Datum LWGEOM_noop(PG_FUNCTION_ARGS);
Datum LWGEOM_zmflag(PG_FUNCTION_ARGS);
Datum LWGEOM_hasz(PG_FUNCTION_ARGS);
Datum LWGEOM_hasm(PG_FUNCTION_ARGS);
Datum LWGEOM_ndims(PG_FUNCTION_ARGS);
Datum LWGEOM_makepoint(PG_FUNCTION_ARGS);
Datum LWGEOM_makepoint3dm(PG_FUNCTION_ARGS);
Datum LWGEOM_makeline_garray(PG_FUNCTION_ARGS);
Datum LWGEOM_makeline(PG_FUNCTION_ARGS);
Datum LWGEOM_makepoly(PG_FUNCTION_ARGS);
Datum LWGEOM_line_from_mpoint(PG_FUNCTION_ARGS);
Datum LWGEOM_addpoint(PG_FUNCTION_ARGS);
Datum LWGEOM_removepoint(PG_FUNCTION_ARGS);
Datum LWGEOM_setpoint_linestring(PG_FUNCTION_ARGS);
Datum LWGEOM_asEWKT(PG_FUNCTION_ARGS);
Datum LWGEOM_hasBBOX(PG_FUNCTION_ARGS);
Datum LWGEOM_azimuth(PG_FUNCTION_ARGS);
Datum LWGEOM_affine(PG_FUNCTION_ARGS);
Datum LWGEOM_longitude_shift(PG_FUNCTION_ARGS);
Datum optimistic_overlap(PG_FUNCTION_ARGS);
Datum ST_GeoHash(PG_FUNCTION_ARGS);
Datum ST_MakeEnvelope(PG_FUNCTION_ARGS);
Datum ST_CollectionExtract(PG_FUNCTION_ARGS);
Datum ST_CollectionHomogenize(PG_FUNCTION_ARGS);
Datum ST_IsCollection(PG_FUNCTION_ARGS);


/*------------------------------------------------------------------*/

/** find the size of geometry */
PG_FUNCTION_INFO_V1(LWGEOM_mem_size);
Datum LWGEOM_mem_size(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	size_t size = VARSIZE(geom);
	PG_FREE_IF_COPY(geom,0);
	PG_RETURN_INT32(size);
}

/** get summary info on a GEOMETRY */
PG_FUNCTION_INFO_V1(LWGEOM_summary);
Datum LWGEOM_summary(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	char *result;
	text *mytext;
	LWGEOM *lwgeom;

	lwgeom = lwgeom_from_gserialized(geom);
	result = lwgeom_summary(lwgeom, 0);
	lwgeom_free(lwgeom);

	/* create a text obj to return */
	mytext = cstring2text(result);
	pfree(result);
	
	PG_FREE_IF_COPY(geom,0);
	PG_RETURN_TEXT_P(mytext);
}

PG_FUNCTION_INFO_V1(postgis_version);
Datum postgis_version(PG_FUNCTION_ARGS)
{
	char *ver = POSTGIS_VERSION;
	text *result = cstring2text(ver);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(postgis_lib_version);
Datum postgis_lib_version(PG_FUNCTION_ARGS)
{
	char *ver = POSTGIS_LIB_VERSION;
	text *result = cstring2text(ver);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(postgis_svn_version);
Datum postgis_svn_version(PG_FUNCTION_ARGS)
{
	static int rev = POSTGIS_SVN_REVISION;
	char ver[32];
	if ( rev > 0 )
	{
		snprintf(ver, 32, "%d", rev);	
		PG_RETURN_TEXT_P(cstring2text(ver));
	}
	else
		PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(postgis_lib_build_date);
Datum postgis_lib_build_date(PG_FUNCTION_ARGS)
{
	char *ver = POSTGIS_BUILD_DATE;
	text *result = cstring2text(ver);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(postgis_scripts_released);
Datum postgis_scripts_released(PG_FUNCTION_ARGS)
{
	char ver[64];
	text *result;

	snprintf(ver, 64, "%s r%d", POSTGIS_LIB_VERSION, POSTGIS_SVN_REVISION);
	ver[63] = '\0';

	result = cstring2text(ver);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(postgis_uses_stats);
Datum postgis_uses_stats(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(TRUE);
}

PG_FUNCTION_INFO_V1(postgis_autocache_bbox);
Datum postgis_autocache_bbox(PG_FUNCTION_ARGS)
{
#ifdef POSTGIS_AUTOCACHE_BBOX
	PG_RETURN_BOOL(TRUE);
#else
	PG_RETURN_BOOL(FALSE);
#endif
}


PG_FUNCTION_INFO_V1(postgis_libxml_version);
Datum postgis_libxml_version(PG_FUNCTION_ARGS)
{
	char *ver = POSTGIS_LIBXML2_VERSION;
	text *result = cstring2text(ver);
	PG_RETURN_TEXT_P(result);
}

/** number of points in an object */
PG_FUNCTION_INFO_V1(LWGEOM_npoints);
Datum LWGEOM_npoints(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	int npoints = 0;

	npoints = lwgeom_count_vertices(lwgeom);
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_INT32(npoints);
}

/** number of rings in an object */
PG_FUNCTION_INFO_V1(LWGEOM_nrings);
Datum LWGEOM_nrings(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	int nrings = 0;

	nrings = lwgeom_count_rings(lwgeom);
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_INT32(nrings);
}

/**
 * @brief Calculate the area of all the subobj in a polygon
 * 		area(point) = 0
 * 		area (line) = 0
 * 		area(polygon) = find its 2d area
 */
PG_FUNCTION_INFO_V1(LWGEOM_area_polygon);
Datum LWGEOM_area_polygon(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double area = 0.0;

	POSTGIS_DEBUG(2, "in LWGEOM_area_polygon");

	area = lwgeom_area(lwgeom);

	lwgeom_free(lwgeom);	
	PG_FREE_IF_COPY(geom, 0);
	
	PG_RETURN_FLOAT8(area);
}

/**
 * @brief find the "length of a geometry"
 *  	length2d(point) = 0
 *  	length2d(line) = length of line
 *  	length2d(polygon) = 0  -- could make sense to return sum(ring perimeter)
 *  	uses euclidian 2d length (even if input is 3d)
 */
PG_FUNCTION_INFO_V1(LWGEOM_length2d_linestring);
Datum LWGEOM_length2d_linestring(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double dist = lwgeom_length_2d(lwgeom);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_FLOAT8(dist);
}

/**
 * @brief find the "length of a geometry"
 *  	length(point) = 0
 *  	length(line) = length of line
 *  	length(polygon) = 0  -- could make sense to return sum(ring perimeter)
 *  	uses euclidian 3d/2d length depending on input dimensions.
 */
PG_FUNCTION_INFO_V1(LWGEOM_length_linestring);
Datum LWGEOM_length_linestring(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double dist = lwgeom_length(lwgeom);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_FLOAT8(dist);
}

/**
 *  @brief find the "perimeter of a geometry"
 *  	perimeter(point) = 0
 *  	perimeter(line) = 0
 *  	perimeter(polygon) = sum of ring perimeters
 *  	uses euclidian 3d/2d computation depending on input dimension.
 */
PG_FUNCTION_INFO_V1(LWGEOM_perimeter_poly);
Datum LWGEOM_perimeter_poly(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double perimeter = 0.0;
	
	perimeter = lwgeom_perimeter(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_FLOAT8(perimeter);
}

/**
 *  @brief find the "perimeter of a geometry"
 *  	perimeter(point) = 0
 *  	perimeter(line) = 0
 *  	perimeter(polygon) = sum of ring perimeters
 *  	uses euclidian 2d computation even if input is 3d
 */
PG_FUNCTION_INFO_V1(LWGEOM_perimeter2d_poly);
Datum LWGEOM_perimeter2d_poly(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double perimeter = 0.0;
	
	perimeter = lwgeom_perimeter_2d(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_FLOAT8(perimeter);
}


/* transform input geometry to 2d if not 2d already */
PG_FUNCTION_INFO_V1(LWGEOM_force_2d);
Datum LWGEOM_force_2d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_geom_in = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pg_geom_out;
	LWGEOM *lwg_in, *lwg_out;

	/* already 2d */
	if ( gserialized_ndims(pg_geom_in) == 2 ) PG_RETURN_POINTER(pg_geom_in);

	lwg_in = lwgeom_from_gserialized(pg_geom_in);
	lwg_out = lwgeom_force_2d(lwg_in);
	pg_geom_out = geometry_serialize(lwg_out);
	lwgeom_free(lwg_out);
	lwgeom_free(lwg_in);

	PG_FREE_IF_COPY(pg_geom_in, 0);
	PG_RETURN_POINTER(pg_geom_out);
}

/* transform input geometry to 3dz if not 3dz already */
PG_FUNCTION_INFO_V1(LWGEOM_force_3dz);
Datum LWGEOM_force_3dz(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_geom_in = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pg_geom_out;
	LWGEOM *lwg_in, *lwg_out;

	/* already 3d */
	if ( gserialized_ndims(pg_geom_in) == 3 && gserialized_has_z(pg_geom_in) ) 
		PG_RETURN_POINTER(pg_geom_in);

	lwg_in = lwgeom_from_gserialized(pg_geom_in);
	lwg_out = lwgeom_force_3dz(lwg_in);
	pg_geom_out = geometry_serialize(lwg_out);
	lwgeom_free(lwg_out);
	lwgeom_free(lwg_in);

	PG_FREE_IF_COPY(pg_geom_in, 0);
	PG_RETURN_POINTER(pg_geom_out);
}

/** transform input geometry to 3dm if not 3dm already */
PG_FUNCTION_INFO_V1(LWGEOM_force_3dm);
Datum LWGEOM_force_3dm(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_geom_in = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pg_geom_out;
	LWGEOM *lwg_in, *lwg_out;

	/* already 3d */
	if ( gserialized_ndims(pg_geom_in) == 3 && gserialized_has_m(pg_geom_in) ) 
		PG_RETURN_POINTER(pg_geom_in);

	lwg_in = lwgeom_from_gserialized(pg_geom_in);
	lwg_out = lwgeom_force_3dm(lwg_in);
	pg_geom_out = geometry_serialize(lwg_out);
	lwgeom_free(lwg_out);
	lwgeom_free(lwg_in);

	PG_FREE_IF_COPY(pg_geom_in, 0);
	PG_RETURN_POINTER(pg_geom_out);
}

/* transform input geometry to 4d if not 4d already */
PG_FUNCTION_INFO_V1(LWGEOM_force_4d);
Datum LWGEOM_force_4d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_geom_in = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pg_geom_out;
	LWGEOM *lwg_in, *lwg_out;

	/* already 4d */
	if ( gserialized_ndims(pg_geom_in) == 4 ) 
		PG_RETURN_POINTER(pg_geom_in);

	lwg_in = lwgeom_from_gserialized(pg_geom_in);
	lwg_out = lwgeom_force_4d(lwg_in);
	pg_geom_out = geometry_serialize(lwg_out);
	lwgeom_free(lwg_out);
	lwgeom_free(lwg_in);

	PG_FREE_IF_COPY(pg_geom_in, 0);
	PG_RETURN_POINTER(pg_geom_out);
}

/** transform input geometry to a collection type */
PG_FUNCTION_INFO_V1(LWGEOM_force_collection);
Datum LWGEOM_force_collection(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *result;
	LWGEOM **lwgeoms;
	LWGEOM *lwgeom;
	int srid;
	GBOX *bbox;

	POSTGIS_DEBUG(2, "LWGEOM_force_collection called");

	/*
	 * This funx is a no-op only if a bbox cache is already present
	 * in input. If bbox cache is not there we'll need to handle
	 * automatic bbox addition FOR_COMPLEX_GEOMS.
	 */
	if ( gserialized_get_type(geom) == COLLECTIONTYPE &&
	        gserialized_has_bbox(geom) )
	{
		PG_RETURN_POINTER(geom);
	}

	/* deserialize into lwgeoms[0] */
	lwgeom = lwgeom_from_gserialized(geom);

	/* alread a multi*, just make it a collection */
	if ( lwgeom_is_collection(lwgeom) )
	{
		lwgeom->type = COLLECTIONTYPE;
	}

	/* single geom, make it a collection */
	else
	{
		srid = lwgeom->srid;
		/* We transfer bbox ownership from input to output */
		bbox = lwgeom->bbox;
		lwgeom->srid = SRID_UNKNOWN;
		lwgeom->bbox = NULL;
		lwgeoms = palloc(sizeof(LWGEOM*));
		lwgeoms[0] = lwgeom;
		lwgeom = (LWGEOM *)lwcollection_construct(COLLECTIONTYPE,
		         srid, bbox, 1,
		         lwgeoms);
	}

	result = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_POINTER(result);
}

/** transform input geometry to a multi* type */
PG_FUNCTION_INFO_V1(LWGEOM_force_multi);
Datum LWGEOM_force_multi(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *result;
	LWGEOM *lwgeom;
	LWGEOM *ogeom;

	POSTGIS_DEBUG(2, "LWGEOM_force_multi called");

	/*
	** This funx is a no-op only if a bbox cache is already present
	** in input. If bbox cache is not there we'll need to handle
	** automatic bbox addition FOR_COMPLEX_GEOMS.
	*/
	if ( gserialized_has_bbox(geom) ) {
		switch (gserialized_get_type(geom)) 
		{
			case MULTIPOINTTYPE:
			case MULTILINETYPE:
			case MULTIPOLYGONTYPE:
			case COLLECTIONTYPE:
			case MULTICURVETYPE:
			case MULTISURFACETYPE:
			case TINTYPE:
				PG_RETURN_POINTER(geom);
			default:
				break;
		}
	}

	/* deserialize into lwgeoms[0] */
	lwgeom = lwgeom_from_gserialized(geom);
	ogeom = lwgeom_as_multi(lwgeom);

	result = geometry_serialize(ogeom);

	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(result);
}

/** transform input geometry to a curved type */
PG_FUNCTION_INFO_V1(LWGEOM_force_curve);
Datum LWGEOM_force_curve(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *result;
	LWGEOM *lwgeom;
	LWGEOM *ogeom;

	POSTGIS_DEBUG(2, "LWGEOM_force_curve called");

  /* TODO: early out if input is already a curve */

	lwgeom = lwgeom_from_gserialized(geom);
	ogeom = lwgeom_as_curve(lwgeom);

	result = geometry_serialize(ogeom);

	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(result);
}

/** transform input geometry to a SFS 1.1 geometry type compliant */
PG_FUNCTION_INFO_V1(LWGEOM_force_sfs);
Datum LWGEOM_force_sfs(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *result;
	LWGEOM *lwgeom;
	LWGEOM *ogeom;
	text * ver;
	int version = 110; /* default version is SFS 1.1 */

	POSTGIS_DEBUG(2, "LWGEOM_force_sfs called");

        /* If user specified version, respect it */
        if ( (PG_NARGS()>1) && (!PG_ARGISNULL(1)) )
        {
                ver = PG_GETARG_TEXT_P(1);

                if  ( ! strncmp(VARDATA(ver), "1.2", 3))
                {
                        version = 120;
                }
        }

	lwgeom = lwgeom_from_gserialized(geom);
	ogeom = lwgeom_force_sfs(lwgeom, version);

	result = geometry_serialize(ogeom);

	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(result);
}

/**
Returns the point in first input geometry that is closest to the second input geometry in 2d
*/

PG_FUNCTION_INFO_V1(LWGEOM_closestpoint);
Datum LWGEOM_closestpoint(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *point;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	point = lwgeom_closest_point(lwgeom1, lwgeom2);

	if (lwgeom_is_empty(point))
		PG_RETURN_NULL();
	
	result = geometry_serialize(point);
	lwgeom_free(point);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}

/**
Returns the shortest 2d line between two geometries
*/
PG_FUNCTION_INFO_V1(LWGEOM_shortestline2d);
Datum LWGEOM_shortestline2d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *theline;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	theline = lwgeom_closest_line(lwgeom1, lwgeom2);
	
	if (lwgeom_is_empty(theline))
		PG_RETURN_NULL();	

	result = geometry_serialize(theline);
	lwgeom_free(theline);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}

/**
Returns the longest 2d line between two geometries
*/
PG_FUNCTION_INFO_V1(LWGEOM_longestline2d);
Datum LWGEOM_longestline2d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *theline;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	theline = lwgeom_furthest_line(lwgeom1, lwgeom2);
	
	if (lwgeom_is_empty(theline))
		PG_RETURN_NULL();

	result = geometry_serialize(theline);
	lwgeom_free(theline);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}
/**
 Minimum 2d distance between objects in geom1 and geom2.
 */
PG_FUNCTION_INFO_V1(LWGEOM_mindistance2d);
Datum LWGEOM_mindistance2d(PG_FUNCTION_ARGS)
{
	double mindist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	mindist = lwgeom_mindistance2d(lwgeom1, lwgeom2);

	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (mindist<FLT_MAX)
		PG_RETURN_FLOAT8(mindist);
	
	PG_RETURN_NULL();
}

/**
Returns boolean describing if
mininimum 2d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(LWGEOM_dwithin);
Datum LWGEOM_dwithin(PG_FUNCTION_ARGS)
{
	double mindist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	double tolerance = PG_GETARG_FLOAT8(2);	
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if ( tolerance < 0 )
	{
		elog(ERROR,"Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	mindist = lwgeom_mindistance2d_tolerance(lwgeom1,lwgeom2,tolerance);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	/*empty geometries cases should be right handled since return from underlying
	 functions should be FLT_MAX which causes false as answer*/
	PG_RETURN_BOOL(tolerance >= mindist);
}

/**
Returns boolean describing if
maximum 2d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(LWGEOM_dfullywithin);
Datum LWGEOM_dfullywithin(PG_FUNCTION_ARGS)
{
	double maxdist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	double tolerance = PG_GETARG_FLOAT8(2);	
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if ( tolerance < 0 )
	{
		elog(ERROR,"Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}
	
	maxdist = lwgeom_maxdistance2d_tolerance(lwgeom1, lwgeom2, tolerance);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*If function is feed with empty geometries we should return false*/
	if (maxdist>-1)
		PG_RETURN_BOOL(tolerance >= maxdist);
	
	PG_RETURN_BOOL(LW_FALSE);
}

/**
 Maximum 2d distance between objects in geom1 and geom2.
 */
PG_FUNCTION_INFO_V1(LWGEOM_maxdistance2d_linestring);
Datum LWGEOM_maxdistance2d_linestring(PG_FUNCTION_ARGS)
{
	double maxdist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	maxdist = lwgeom_maxdistance2d(lwgeom1, lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (maxdist>-1)
		PG_RETURN_FLOAT8(maxdist);
	
	PG_RETURN_NULL();
}

/**
Returns the point in first input geometry that is closest to the second input geometry in 3D
*/

PG_FUNCTION_INFO_V1(LWGEOM_closestpoint3d);
Datum LWGEOM_closestpoint3d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *point;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	point = lwgeom_closest_point_3d(lwgeom1, lwgeom2);
	// point = lw_dist3d_distancepoint(lwgeom1, lwgeom2, lwgeom1->srid, DIST_MIN);

	if (lwgeom_is_empty(point))
		PG_RETURN_NULL();	

	result = geometry_serialize(point);
	
	lwgeom_free(point);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}

/**
Returns the shortest line between two geometries in 3D
*/
PG_FUNCTION_INFO_V1(LWGEOM_shortestline3d);
Datum LWGEOM_shortestline3d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *theline;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	theline = lwgeom_closest_line_3d(lwgeom1, lwgeom2);
	// theline = lw_dist3d_distanceline(lwgeom1, lwgeom2, lwgeom1->srid, DIST_MIN);
	
	if (lwgeom_is_empty(theline))
		PG_RETURN_NULL();

	result = geometry_serialize(theline);
	
	lwgeom_free(theline);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}

/**
Returns the longest line between two geometries in 3D
*/
PG_FUNCTION_INFO_V1(LWGEOM_longestline3d);
Datum LWGEOM_longestline3d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *theline;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	theline = lwgeom_furthest_line_3d(lwgeom1, lwgeom2);
	// theline = lw_dist3d_distanceline(lwgeom1, lwgeom2, lwgeom1->srid, DIST_MAX);
	
	if (lwgeom_is_empty(theline))
		PG_RETURN_NULL();
	
	result = geometry_serialize(theline);
	
	lwgeom_free(theline);
	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	PG_RETURN_POINTER(result);
}
/**
 Minimum 2d distance between objects in geom1 and geom2 in 3D
 */
PG_FUNCTION_INFO_V1(LWGEOM_mindistance3d);
Datum LWGEOM_mindistance3d(PG_FUNCTION_ARGS)
{
	double mindist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	mindist = lwgeom_mindistance3d(lwgeom1, lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (mindist<FLT_MAX)
		PG_RETURN_FLOAT8(mindist);

	PG_RETURN_NULL();
}

/**
Returns boolean describing if
mininimum 3d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(LWGEOM_dwithin3d);
Datum LWGEOM_dwithin3d(PG_FUNCTION_ARGS)
{
	double mindist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	double tolerance = PG_GETARG_FLOAT8(2);	
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if ( tolerance < 0 )
	{
		elog(ERROR,"Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	mindist = lwgeom_mindistance3d_tolerance(lwgeom1,lwgeom2,tolerance);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*empty geometries cases should be right handled since return from underlying
	 functions should be FLT_MAX which causes false as answer*/
	PG_RETURN_BOOL(tolerance >= mindist);
}

/**
Returns boolean describing if
maximum 3d distance between objects in
geom1 and geom2 is shorter than tolerance
*/
PG_FUNCTION_INFO_V1(LWGEOM_dfullywithin3d);
Datum LWGEOM_dfullywithin3d(PG_FUNCTION_ARGS)
{
	double maxdist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	double tolerance = PG_GETARG_FLOAT8(2);	
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if ( tolerance < 0 )
	{
		elog(ERROR,"Tolerance cannot be less than zero\n");
		PG_RETURN_NULL();
	}

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}
	maxdist = lwgeom_maxdistance3d_tolerance(lwgeom1, lwgeom2, tolerance);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*If function is feed with empty geometries we should return false*/
	if (maxdist>-1)
		PG_RETURN_BOOL(tolerance >= maxdist);

	PG_RETURN_BOOL(LW_FALSE);
}

/**
 Maximum 3d distance between objects in geom1 and geom2.
 */
PG_FUNCTION_INFO_V1(LWGEOM_maxdistance3d);
Datum LWGEOM_maxdistance3d(PG_FUNCTION_ARGS)
{
	double maxdist;
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

	if (lwgeom1->srid != lwgeom2->srid)
	{
		elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	maxdist = lwgeom_maxdistance3d(lwgeom1, lwgeom2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);
	
	/*if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (maxdist>-1)
		PG_RETURN_FLOAT8(maxdist);

	PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(LWGEOM_longitude_shift);
Datum LWGEOM_longitude_shift(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	GSERIALIZED *ret;

	POSTGIS_DEBUG(2, "LWGEOM_longitude_shift called.");

	geom = PG_GETARG_GSERIALIZED_P_COPY(0);
	lwgeom = lwgeom_from_gserialized(geom);

	/* Drop bbox, will be recomputed */
	lwgeom_drop_bbox(lwgeom);

	/* Modify geometry */
	lwgeom_longitude_shift(lwgeom);

	/* Construct GSERIALIZED */
	ret = geometry_serialize(lwgeom);

	/* Release deserialized geometry */
	lwgeom_free(lwgeom);

	/* Release detoasted geometry */
	pfree(geom);

	PG_RETURN_POINTER(ret);
}

PG_FUNCTION_INFO_V1(LWGEOM_inside_circle_point);
Datum LWGEOM_inside_circle_point(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	double cx = PG_GETARG_FLOAT8(1);
	double cy = PG_GETARG_FLOAT8(2);
	double rr = PG_GETARG_FLOAT8(3);
	LWPOINT *lwpoint;
	LWGEOM *lwgeom;
	int inside;

	geom = PG_GETARG_GSERIALIZED_P(0);
	lwgeom = lwgeom_from_gserialized(geom);
	lwpoint = lwgeom_as_lwpoint(lwgeom);
	if ( lwpoint == NULL || lwgeom_is_empty(lwgeom) )
	{
		PG_FREE_IF_COPY(geom, 0);
		PG_RETURN_NULL(); /* not a point */
	}

	inside = lwpoint_inside_circle(lwpoint, cx, cy, rr);
	lwpoint_free(lwpoint);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_BOOL(inside);
}

/**
 *  @brief collect( geom, geom ) returns a geometry which contains
 *  		all the sub_objects from both of the argument geometries
 *  @return geometry is the simplest possible, based on the types
 *  	of the collected objects
 *  	ie. if all are of either X or multiX, then a multiX is returned.
 */
PG_FUNCTION_INFO_V1(LWGEOM_collect);
Datum LWGEOM_collect(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gser1, *gser2, *result;
	LWGEOM *lwgeoms[2], *outlwg;
    uint32 type1, type2;
    uint8_t outtype;
	int srid;

	POSTGIS_DEBUG(2, "LWGEOM_collect called.");

	/* return null if both geoms are null */
	if ( PG_ARGISNULL(0) && PG_ARGISNULL(1) )
		PG_RETURN_NULL();

	/* Return the second geom if the first geom is null */
	if (PG_ARGISNULL(0))
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));

	/* Return the first geom if the second geom is null */
	if (PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	gser1 = PG_GETARG_GSERIALIZED_P(0);
	gser2 = PG_GETARG_GSERIALIZED_P(1);

	POSTGIS_DEBUGF(3, "LWGEOM_collect(%s, %s): call", lwtype_name(gserialized_get_type(gser1)), lwtype_name(gserialized_get_type(gser2)));

	if ( FLAGS_GET_ZM(gser1->flags) != FLAGS_GET_ZM(gser2->flags) )
	{
		elog(ERROR,"Cannot ST_Collect geometries with differing dimensionality.");
		PG_RETURN_NULL();
	}

	srid = gserialized_get_srid(gser1);
	error_if_srid_mismatch(srid, gserialized_get_srid(gser2));

	lwgeoms[0] = lwgeom_from_gserialized(gser1);
	lwgeoms[1] = lwgeom_from_gserialized(gser2);

	type1 = lwgeoms[0]->type;
	type2 = lwgeoms[1]->type;
	
	if ( (type1 == type2) && (!lwgeom_is_collection(lwgeoms[0])) ) 
		outtype = lwtype_get_collectiontype(type1);
	else 
		outtype = COLLECTIONTYPE;

	POSTGIS_DEBUGF(3, " outtype = %d", outtype);

	/* Drop input geometries bbox and SRID */
	lwgeom_drop_bbox(lwgeoms[0]);
	lwgeom_drop_srid(lwgeoms[0]);
	lwgeom_drop_bbox(lwgeoms[1]);
	lwgeom_drop_srid(lwgeoms[1]);

	outlwg = (LWGEOM *)lwcollection_construct(outtype, srid, NULL, 2, lwgeoms);
	result = geometry_serialize(outlwg);

	lwgeom_free(lwgeoms[0]);
	lwgeom_free(lwgeoms[1]);

	PG_FREE_IF_COPY(gser1, 0);
	PG_FREE_IF_COPY(gser2, 1);

	PG_RETURN_POINTER(result);
}


/**
 * @brief collect_garray ( GEOMETRY[] ) returns a geometry which contains
 * 		all the sub_objects from all of the geometries in given array.
 *
 * @return geometry is the simplest possible, based on the types
 * 		of the collected objects
 * 		ie. if all are of either X or multiX, then a multiX is returned
 * 		bboxonly types are treated as null geometries (no sub_objects)
 */
PG_FUNCTION_INFO_V1(LWGEOM_collect_garray);
Datum LWGEOM_collect_garray(PG_FUNCTION_ARGS)
{
	ArrayType *array;
	int nelems;
	/*GSERIALIZED **geoms; */
	GSERIALIZED *result = NULL;
	LWGEOM **lwgeoms, *outlwg;
	uint32 outtype;
	int count;
	int srid = SRID_UNKNOWN;
	GBOX *box = NULL;

	ArrayIterator iterator;
	Datum value;
	bool isnull;

	POSTGIS_DEBUG(2, "LWGEOM_collect_garray called.");

	if ( PG_ARGISNULL(0) )
		PG_RETURN_NULL();

	/* Get actual ArrayType */
	array = PG_GETARG_ARRAYTYPE_P(0);
	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	POSTGIS_DEBUGF(3, " array is %d-bytes in size, %ld w/out header",
	               ARR_SIZE(array), ARR_SIZE(array)-ARR_OVERHEAD_NONULLS(ARR_NDIM(array)));

	POSTGIS_DEBUGF(3, "LWGEOM_collect_garray: array has %d elements", nelems);

	/* Return null on 0-elements input array */
	if ( nelems == 0 )
		PG_RETURN_NULL();

	/*
	 * Deserialize all geometries in array into the lwgeoms pointers
	 * array. Check input types to form output type.
	 */
	lwgeoms = palloc(sizeof(LWGEOM*) * nelems);
	count = 0;
	outtype = 0;

#if POSTGIS_PGSQL_VERSION >= 95	
	iterator = array_create_iterator(array, 0, NULL);
#else
	iterator = array_create_iterator(array, 0);
#endif
	
	while( array_iterate(iterator, &value, &isnull) )
	{
		GSERIALIZED *geom;
		uint8_t intype;

		/* Don't do anything for NULL values */
		if ( isnull )
			continue;
		
		geom = (GSERIALIZED *)DatumGetPointer(value);
		intype = gserialized_get_type(geom);

		lwgeoms[count] = lwgeom_from_gserialized(geom);

		POSTGIS_DEBUGF(3, "%s: geom %d deserialized", __func__, count);

		if ( ! count )
		{
			/* Get first geometry SRID */
			srid = lwgeoms[count]->srid;

			/* COMPUTE_BBOX WHEN_SIMPLE */
			if ( lwgeoms[count]->bbox )
			{
				box = gbox_copy(lwgeoms[count]->bbox);
			}
		}
		else
		{
			/* Check SRID homogeneity */
			if ( lwgeoms[count]->srid != srid )
			{
				elog(ERROR, "Operation on mixed SRID geometries");
				PG_RETURN_NULL();
			}

			/* COMPUTE_BBOX WHEN_SIMPLE */
			if ( box )
			{
				if ( lwgeoms[count]->bbox )
				{
					gbox_merge(lwgeoms[count]->bbox, box);
				}
				else
				{
					pfree(box);
					box = NULL;
				}
			}
		}

		lwgeom_drop_srid(lwgeoms[count]);
		lwgeom_drop_bbox(lwgeoms[count]);

		/* Output type not initialized */
		if ( ! outtype )
		{
			/* Input is single, make multi */
			if ( ! lwtype_is_collection(intype) ) 
				outtype = lwtype_get_collectiontype(intype);
			/* Input is multi, make collection */
			else 
				outtype = COLLECTIONTYPE;
		}

		/* Input type not compatible with output */
		/* make output type a collection */
		else if ( outtype != COLLECTIONTYPE && intype != outtype-3 )
		{
			outtype = COLLECTIONTYPE;
		}

		count++;

	}
	array_free_iterator(iterator);
	

	POSTGIS_DEBUGF(3, "LWGEOM_collect_garray: outtype = %d", outtype);

	/* If we have been passed a complete set of NULLs then return NULL */
	if (!outtype)
	{
		PG_RETURN_NULL();
	}
	else
	{
		outlwg = (LWGEOM *)lwcollection_construct(
		             outtype, srid,
		             box, count, lwgeoms);

		result = geometry_serialize(outlwg);

		PG_RETURN_POINTER(result);
	}
}

/**
 * LineFromMultiPoint ( GEOMETRY ) returns a LINE formed by
 * 		all the points in the in given multipoint.
 */
PG_FUNCTION_INFO_V1(LWGEOM_line_from_mpoint);
Datum LWGEOM_line_from_mpoint(PG_FUNCTION_ARGS)
{
	GSERIALIZED *ingeom, *result;
	LWLINE *lwline;
	LWMPOINT *mpoint;

	POSTGIS_DEBUG(2, "LWGEOM_line_from_mpoint called");

	/* Get input GSERIALIZED and deserialize it */
	ingeom = PG_GETARG_GSERIALIZED_P(0);

	if ( gserialized_get_type(ingeom) != MULTIPOINTTYPE )
	{
		elog(ERROR, "makeline: input must be a multipoint");
		PG_RETURN_NULL(); /* input is not a multipoint */
	}

	mpoint = lwgeom_as_lwmpoint(lwgeom_from_gserialized(ingeom));
	lwline = lwline_from_lwmpoint(mpoint->srid, mpoint);
	if ( ! lwline )
	{
		PG_FREE_IF_COPY(ingeom, 0);
		elog(ERROR, "makeline: lwline_from_lwmpoint returned NULL");
		PG_RETURN_NULL();
	}

	result = geometry_serialize(lwline_as_lwgeom(lwline));

	PG_FREE_IF_COPY(ingeom, 0);
	lwline_free(lwline);

	PG_RETURN_POINTER(result);
}

/**
 * @brief makeline_garray ( GEOMETRY[] ) returns a LINE formed by
 * 		all the point geometries in given array.
 * 		array elements that are NOT points are discarded..
 */
PG_FUNCTION_INFO_V1(LWGEOM_makeline_garray);
Datum LWGEOM_makeline_garray(PG_FUNCTION_ARGS)
{
	ArrayType *array;
	int nelems;
	GSERIALIZED *result = NULL;
	LWGEOM **geoms;
	LWGEOM *outlwg;
	uint32 ngeoms;
	int srid = SRID_UNKNOWN;
	
	ArrayIterator iterator;
	Datum value;
	bool isnull;

	POSTGIS_DEBUGF(2, "%s called", __func__);

	/* Return null on null input */
	if ( PG_ARGISNULL(0) )
		PG_RETURN_NULL();

	/* Get actual ArrayType */
	array = PG_GETARG_ARRAYTYPE_P(0);

	/* Get number of geometries in array */
	nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	POSTGIS_DEBUGF(3, "%s: array has %d elements", __func__, nelems);

	/* Return null on 0-elements input array */
	if ( nelems == 0 )
		PG_RETURN_NULL();


	/*
	 * Deserialize all point geometries in array into the
	 * geoms pointers array.
	 * Count actual number of points.
	 */

	/* possibly more then required */
	geoms = palloc(sizeof(LWGEOM *) * nelems);
	ngeoms = 0;

#if POSTGIS_PGSQL_VERSION >= 95	
	iterator = array_create_iterator(array, 0, NULL);
#else
	iterator = array_create_iterator(array, 0);
#endif

	while( array_iterate(iterator, &value, &isnull) )
	{
		GSERIALIZED *geom;

		if ( isnull )
			continue;
		
		geom = (GSERIALIZED *)DatumGetPointer(value);

		if ( gserialized_get_type(geom) != POINTTYPE && 
		     gserialized_get_type(geom) != LINETYPE ) 
		{
			continue;
		}

		geoms[ngeoms++] = lwgeom_from_gserialized(geom);

		/* Check SRID homogeneity */
		if ( ngeoms == 1 )
		{
			/* Get first geometry SRID */
			srid = geoms[ngeoms-1]->srid;
			/* TODO: also get ZMflags */
		}
		else
		{
			if ( geoms[ngeoms-1]->srid != srid )
			{
				elog(ERROR, "Operation on mixed SRID geometries");
				PG_RETURN_NULL();
			}
		}

		POSTGIS_DEBUGF(3, "%s: element %d deserialized", __func__, ngeoms);
	}
	array_free_iterator(iterator);

	/* Return null on 0-points input array */
	if ( ngeoms == 0 )
	{
		/* TODO: should we return LINESTRING EMPTY here ? */
		elog(NOTICE, "No points or linestrings in input array");
		PG_RETURN_NULL();
	}

	POSTGIS_DEBUGF(3, "LWGEOM_makeline_garray: elements: %d", ngeoms);

	outlwg = (LWGEOM *)lwline_from_lwgeom_array(srid, ngeoms, geoms);

	result = geometry_serialize(outlwg);

	PG_RETURN_POINTER(result);
}

/**
 * makeline ( GEOMETRY, GEOMETRY ) returns a LINESTRIN segment
 * formed by the given point geometries.
 */
PG_FUNCTION_INFO_V1(LWGEOM_makeline);
Datum LWGEOM_makeline(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwg1, *pglwg2;
	GSERIALIZED *result=NULL;
	LWGEOM *lwgeoms[2];
	LWLINE *outline;

	POSTGIS_DEBUG(2, "LWGEOM_makeline called.");

	/* Get input datum */
	pglwg1 = PG_GETARG_GSERIALIZED_P(0);
	pglwg2 = PG_GETARG_GSERIALIZED_P(1);

	if ( (gserialized_get_type(pglwg1) != POINTTYPE && gserialized_get_type(pglwg1) != LINETYPE) ||
	     (gserialized_get_type(pglwg2) != POINTTYPE && gserialized_get_type(pglwg2) != LINETYPE) )
	{
		elog(ERROR, "Input geometries must be points or lines");
		PG_RETURN_NULL();
	}

	error_if_srid_mismatch(gserialized_get_srid(pglwg1), gserialized_get_srid(pglwg2));

	lwgeoms[0] = lwgeom_from_gserialized(pglwg1);
	lwgeoms[1] = lwgeom_from_gserialized(pglwg2);

	outline = lwline_from_lwgeom_array(lwgeoms[0]->srid, 2, lwgeoms);

	result = geometry_serialize((LWGEOM *)outline);

	PG_FREE_IF_COPY(pglwg1, 0);
	PG_FREE_IF_COPY(pglwg2, 1);
	lwgeom_free(lwgeoms[0]);
	lwgeom_free(lwgeoms[1]);

	PG_RETURN_POINTER(result);
}

/**
 * makepoly( GEOMETRY, GEOMETRY[] ) returns a POLYGON
 * 		formed by the given shell and holes geometries.
 */
PG_FUNCTION_INFO_V1(LWGEOM_makepoly);
Datum LWGEOM_makepoly(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwg1;
	ArrayType *array=NULL;
	GSERIALIZED *result=NULL;
	const LWLINE *shell=NULL;
	const LWLINE **holes=NULL;
	LWPOLY *outpoly;
	uint32 nholes=0;
	uint32 i;
	size_t offset=0;

	POSTGIS_DEBUG(2, "LWGEOM_makepoly called.");

	/* Get input shell */
	pglwg1 = PG_GETARG_GSERIALIZED_P(0);
	if ( gserialized_get_type(pglwg1) != LINETYPE )
	{
		lwerror("Shell is not a line");
	}
	shell = lwgeom_as_lwline(lwgeom_from_gserialized(pglwg1));

	/* Get input holes if any */
	if ( PG_NARGS() > 1 )
	{
		array = PG_GETARG_ARRAYTYPE_P(1);
		nholes = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		holes = lwalloc(sizeof(LWLINE *)*nholes);
		for (i=0; i<nholes; i++)
		{
			GSERIALIZED *g = (GSERIALIZED *)(ARR_DATA_PTR(array)+offset);
			LWLINE *hole;
			offset += INTALIGN(VARSIZE(g));
			if ( gserialized_get_type(g) != LINETYPE )
			{
				lwerror("Hole %d is not a line", i);
			}
			hole = lwgeom_as_lwline(lwgeom_from_gserialized(g));
			holes[i] = hole;
		}
	}

	outpoly = lwpoly_from_lwlines(shell, nholes, holes);
	POSTGIS_DEBUGF(3, "%s", lwgeom_summary((LWGEOM*)outpoly, 0));
	result = geometry_serialize((LWGEOM *)outpoly);

	lwline_free((LWLINE*)shell);
	PG_FREE_IF_COPY(pglwg1, 0);

	for (i=0; i<nholes; i++) 
	{
		lwline_free((LWLINE*)holes[i]);
	}

	PG_RETURN_POINTER(result);
}

/**
 *  makes a polygon of the expanded features bvol - 1st point = LL 3rd=UR
 *  2d only. (3d might be worth adding).
 *  create new geometry of type polygon, 1 ring, 5 points
 */
PG_FUNCTION_INFO_V1(LWGEOM_expand);
Datum LWGEOM_expand(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double d = PG_GETARG_FLOAT8(1);
	POINT4D pt;
	POINTARRAY *pa;
	POINTARRAY **ppa;
	LWPOLY *poly;
	GSERIALIZED *result;
	GBOX gbox;

	POSTGIS_DEBUG(2, "LWGEOM_expand called.");

	/* Can't expand an empty */
	if ( lwgeom_is_empty(lwgeom) )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_POINTER(geom);
	}

	/* Can't expand something with no gbox! */
	if ( LW_FAILURE == lwgeom_calculate_gbox(lwgeom, &gbox) )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_POINTER(geom);
	}

	gbox_expand(&gbox, d);

	pa = ptarray_construct_empty(lwgeom_has_z(lwgeom), lwgeom_has_m(lwgeom), 5);
	
	/* Assign coordinates to POINT2D array */
	pt.x = gbox.xmin;
	pt.y = gbox.ymin;
	pt.z = gbox.zmin;
	pt.m = gbox.mmin;
	ptarray_append_point(pa, &pt, LW_TRUE);
	pt.x = gbox.xmin;
	pt.y = gbox.ymax;
	pt.z = gbox.zmin;
	pt.m = gbox.mmin;
	ptarray_append_point(pa, &pt, LW_TRUE);
	pt.x = gbox.xmax;
	pt.y = gbox.ymax;
	pt.z = gbox.zmax;
	pt.m = gbox.mmax;
	ptarray_append_point(pa, &pt, LW_TRUE);
	pt.x = gbox.xmax;
	pt.y = gbox.ymin;
	pt.z = gbox.zmax;
	pt.m = gbox.mmax;
	ptarray_append_point(pa, &pt, LW_TRUE);
	pt.x = gbox.xmin;
	pt.y = gbox.ymin;
	pt.z = gbox.zmin;
	pt.m = gbox.mmin;
	ptarray_append_point(pa, &pt, LW_TRUE);

	/* Construct point array */
	ppa = lwalloc(sizeof(POINTARRAY*));
	ppa[0] = pa;

	/* Construct polygon  */
	poly = lwpoly_construct(lwgeom->srid, NULL, 1, ppa);
	lwgeom_add_bbox(lwpoly_as_lwgeom(poly));

	/* Construct GSERIALIZED  */
	result = geometry_serialize(lwpoly_as_lwgeom(poly));

	lwgeom_free(lwpoly_as_lwgeom(poly));
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(result);
}

/** Convert geometry to BOX (internal postgres type) */
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX);
Datum LWGEOM_to_BOX(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_lwgeom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(pg_lwgeom);
	GBOX gbox;
	int result;
	BOX *out = NULL;
	
	/* Zero out flags */
	gbox_init(&gbox);

	/* Calculate the GBOX of the geometry */
	result = lwgeom_calculate_gbox(lwgeom, &gbox);

	/* Clean up memory */
	lwfree(lwgeom);
	PG_FREE_IF_COPY(pg_lwgeom, 0);
	
	/* Null on failure */
	if ( ! result )
		PG_RETURN_NULL();
	
    out = lwalloc(sizeof(BOX));
	out->low.x = gbox.xmin;
	out->low.y = gbox.ymin;
	out->high.x = gbox.xmax;
	out->high.y = gbox.ymax;
	PG_RETURN_POINTER(out);
}

/**
 *  makes a polygon of the features bvol - 1st point = LL 3rd=UR
 *  2d only. (3d might be worth adding).
 *  create new geometry of type polygon, 1 ring, 5 points
 */
PG_FUNCTION_INFO_V1(LWGEOM_envelope);
Datum LWGEOM_envelope(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	int srid = lwgeom->srid;
	POINT4D pt;
	GBOX box;
	POINTARRAY *pa;
	GSERIALIZED *result;


	if ( lwgeom_is_empty(lwgeom) )
	{
		/* must be the EMPTY geometry */
		PG_RETURN_POINTER(geom);
	}
	
	if ( lwgeom_calculate_gbox(lwgeom, &box) == LW_FAILURE )
	{
		/* must be the EMPTY geometry */
		PG_RETURN_POINTER(geom);
	}
	
	/*
	 * Alter envelope type so that a valid geometry is always
	 * returned depending upon the size of the geometry. The
	 * code makes the following assumptions:
	 *     - If the bounding box is a single point then return a
	 *     POINT geometry
	 *     - If the bounding box represents either a horizontal or
	 *     vertical line, return a LINESTRING geometry
	 *     - Otherwise return a POLYGON
	 */

	if ( (box.xmin == box.xmax) && (box.ymin == box.ymax) )
	{
		/* Construct and serialize point */
		LWPOINT *point = lwpoint_make2d(srid, box.xmin, box.ymin);
		result = geometry_serialize(lwpoint_as_lwgeom(point));
		lwpoint_free(point);
	}
	else if ( (box.xmin == box.xmax) || (box.ymin == box.ymax) )
	{
		LWLINE *line;
		/* Construct point array */
		pa = ptarray_construct_empty(0, 0, 2);

		/* Assign coordinates to POINT2D array */
		pt.x = box.xmin;
		pt.y = box.ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box.xmax;
		pt.y = box.ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);

		/* Construct and serialize linestring */
		line = lwline_construct(srid, NULL, pa);
		result = geometry_serialize(lwline_as_lwgeom(line));
		lwline_free(line);
	}
	else
	{
		LWPOLY *poly;
		POINTARRAY **ppa = lwalloc(sizeof(POINTARRAY*));
		pa = ptarray_construct_empty(0, 0, 5);
		ppa[0] = pa;

		/* Assign coordinates to POINT2D array */
		pt.x = box.xmin;
		pt.y = box.ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box.xmin;
		pt.y = box.ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box.xmax;
		pt.y = box.ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box.xmax;
		pt.y = box.ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box.xmin;
		pt.y = box.ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);

		/* Construct polygon  */
		poly = lwpoly_construct(srid, NULL, 1, ppa);
		result = geometry_serialize(lwpoly_as_lwgeom(poly));
		lwpoly_free(poly);
	}

	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(LWGEOM_isempty);
Datum LWGEOM_isempty(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	bool empty = lwgeom_is_empty(lwgeom);

	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_BOOL(empty);
}


/**
 *  @brief Returns a modified geometry so that no segment is
 *  	longer then the given distance (computed using 2d).
 *  	Every input point is kept.
 *  	Z and M values for added points (if needed) are set to 0.
 */
PG_FUNCTION_INFO_V1(LWGEOM_segmentize2d);
Datum LWGEOM_segmentize2d(PG_FUNCTION_ARGS)
{
	GSERIALIZED *outgeom, *ingeom;
	double dist;
	LWGEOM *inlwgeom, *outlwgeom;
	int type;

	POSTGIS_DEBUG(2, "LWGEOM_segmentize2d called");

	ingeom = PG_GETARG_GSERIALIZED_P(0);
	dist = PG_GETARG_FLOAT8(1);
	type = gserialized_get_type(ingeom); 

	/* Avoid types we cannot segmentize. */
	if ( (type == POINTTYPE) || 
	     (type == MULTIPOINTTYPE) || 
	     (type == TRIANGLETYPE) || 
	     (type == TINTYPE) || 
	     (type == POLYHEDRALSURFACETYPE) )
	{
		PG_RETURN_POINTER(ingeom);
	}

	if ( dist <= 0 ) {
		/* Protect from knowingly infinite loops, see #1799 */
		/* Note that we'll end out of memory anyway for other small distances */
		elog(ERROR, "ST_Segmentize: invalid max_distance %g (must be >= 0)", dist);
		PG_RETURN_NULL();
	}

	LWGEOM_INIT();

	inlwgeom = lwgeom_from_gserialized(ingeom);
	if ( lwgeom_is_empty(inlwgeom) )
	{
		/* Should only happen on interruption */
		lwgeom_free(inlwgeom);
		PG_RETURN_POINTER(ingeom);
	}
	
	outlwgeom = lwgeom_segmentize2d(inlwgeom, dist);
	if ( ! outlwgeom ) {
		/* Should only happen on interruption */
		PG_FREE_IF_COPY(ingeom, 0);
		PG_RETURN_NULL();
	}

	/* Copy input bounding box if any */
	if ( inlwgeom->bbox )
		outlwgeom->bbox = gbox_copy(inlwgeom->bbox);

	outgeom = geometry_serialize(outlwgeom);

	//lwgeom_free(outlwgeom); /* TODO fix lwgeom_clone / ptarray_clone_deep for consistent semantics */
	lwgeom_free(inlwgeom);
	
	PG_FREE_IF_COPY(ingeom, 0);

	PG_RETURN_POINTER(outgeom);
}

/** Reverse vertex order of geometry */
PG_FUNCTION_INFO_V1(LWGEOM_reverse);
Datum LWGEOM_reverse(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWGEOM *lwgeom;

	POSTGIS_DEBUG(2, "LWGEOM_reverse called");

	geom = PG_GETARG_GSERIALIZED_P_COPY(0);

	lwgeom = lwgeom_from_gserialized(geom);
	lwgeom_reverse(lwgeom);

	geom = geometry_serialize(lwgeom);

	PG_RETURN_POINTER(geom);
}

/** Force polygons of the collection to obey Right-Hand-Rule */
PG_FUNCTION_INFO_V1(LWGEOM_force_clockwise_poly);
Datum LWGEOM_force_clockwise_poly(PG_FUNCTION_ARGS)
{
	GSERIALIZED *ingeom, *outgeom;
	LWGEOM *lwgeom;

	POSTGIS_DEBUG(2, "LWGEOM_force_clockwise_poly called");

	ingeom = PG_GETARG_GSERIALIZED_P_COPY(0);

	lwgeom = lwgeom_from_gserialized(ingeom);
	lwgeom_force_clockwise(lwgeom);

	outgeom = geometry_serialize(lwgeom);

	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(ingeom, 0);
	PG_RETURN_POINTER(outgeom);
}

/** Test deserialize/serialize operations */
PG_FUNCTION_INFO_V1(LWGEOM_noop);
Datum LWGEOM_noop(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in, *out;
	LWGEOM *lwgeom;

	POSTGIS_DEBUG(2, "LWGEOM_noop called");

	in = PG_GETARG_GSERIALIZED_P(0);

	lwgeom = lwgeom_from_gserialized(in);

	POSTGIS_DEBUGF(3, "Deserialized: %s", lwgeom_summary(lwgeom, 0));

	out = geometry_serialize(lwgeom);

	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_POINTER(out);
}

/**
 *  @return:
 *   0==2d
 *   1==3dm
 *   2==3dz
 *   3==4d
 */
PG_FUNCTION_INFO_V1(LWGEOM_zmflag);
Datum LWGEOM_zmflag(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in;
	int ret = 0;

	in = PG_GETARG_GSERIALIZED_P(0);
	if ( gserialized_has_z(in) ) ret += 2;
	if ( gserialized_has_m(in) ) ret += 1;
	PG_FREE_IF_COPY(in, 0);
	PG_RETURN_INT16(ret);
}

PG_FUNCTION_INFO_V1(LWGEOM_hasz);
Datum LWGEOM_hasz(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in = PG_GETARG_GSERIALIZED_P(0);
	PG_RETURN_BOOL(gserialized_has_z(in));
}

PG_FUNCTION_INFO_V1(LWGEOM_hasm);
Datum LWGEOM_hasm(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in = PG_GETARG_GSERIALIZED_P(0);
	PG_RETURN_BOOL(gserialized_has_m(in));
}


PG_FUNCTION_INFO_V1(LWGEOM_hasBBOX);
Datum LWGEOM_hasBBOX(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in = PG_GETARG_GSERIALIZED_P(0);
	char res = gserialized_has_bbox(in);
	PG_FREE_IF_COPY(in, 0);
	PG_RETURN_BOOL(res);
}

/** Return: 2,3 or 4 */
PG_FUNCTION_INFO_V1(LWGEOM_ndims);
Datum LWGEOM_ndims(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in;
	int ret;

	in = PG_GETARG_GSERIALIZED_P(0);
	ret = (gserialized_ndims(in));
	PG_FREE_IF_COPY(in, 0);
	PG_RETURN_INT16(ret);
}

/** lwgeom_same(lwgeom1, lwgeom2) */
PG_FUNCTION_INFO_V1(LWGEOM_same);
Datum LWGEOM_same(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *g2 = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwg1, *lwg2;
	bool result;

	if ( gserialized_get_type(g1) != gserialized_get_type(g2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_BOOL(FALSE); /* different types */
	}

	if ( gserialized_get_zm(g1) != gserialized_get_zm(g2) )
	{
		PG_FREE_IF_COPY(g1, 0);
		PG_FREE_IF_COPY(g2, 1);
		PG_RETURN_BOOL(FALSE); /* different dimensions */
	}

	/* ok, deserialize. */
	lwg1 = lwgeom_from_gserialized(g1);
	lwg2 = lwgeom_from_gserialized(g2);

	/* invoke appropriate function */
	result = lwgeom_same(lwg1, lwg2);

	/* Relase memory */
	lwgeom_free(lwg1);
	lwgeom_free(lwg2);
	PG_FREE_IF_COPY(g1, 0);
	PG_FREE_IF_COPY(g2, 1);

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(ST_MakeEnvelope);
Datum ST_MakeEnvelope(PG_FUNCTION_ARGS)
{
	LWPOLY *poly;
	GSERIALIZED *result;
	POINTARRAY **pa;
	POINT4D p;
	double x1, y1, x2, y2;
	int srid = SRID_UNKNOWN;

	POSTGIS_DEBUG(2, "ST_MakeEnvelope called");

	x1 = PG_GETARG_FLOAT8(0);
	y1 = PG_GETARG_FLOAT8(1);
	x2 = PG_GETARG_FLOAT8(2);
	y2 = PG_GETARG_FLOAT8(3);
	if ( PG_NARGS() > 4 ) {
		srid = PG_GETARG_INT32(4);
	}

	pa = (POINTARRAY**)palloc(sizeof(POINTARRAY**));
	pa[0] = ptarray_construct_empty(0, 0, 5);

	/* 1st point */
	p.x = x1;
	p.y = y1;
	ptarray_append_point(pa[0], &p, LW_TRUE);

	/* 2nd point */
	p.x = x1;
	p.y = y2;
	ptarray_append_point(pa[0], &p, LW_TRUE);

	/* 3rd point */
	p.x = x2;
	p.y = y2;
	ptarray_append_point(pa[0], &p, LW_TRUE);

	/* 4th point */
	p.x = x2;
	p.y = y1;
	ptarray_append_point(pa[0], &p, LW_TRUE);

	/* 5th point */
	p.x = x1;
	p.y = y1;
	ptarray_append_point(pa[0], &p, LW_TRUE);

	poly = lwpoly_construct(srid, NULL, 1, pa);
	lwgeom_add_bbox(lwpoly_as_lwgeom(poly));

	result = geometry_serialize(lwpoly_as_lwgeom(poly));
	lwpoly_free(poly);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(ST_IsCollection);
Datum ST_IsCollection(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	int type;
	size_t size;

	/* Pull only a small amount of the tuple, enough to get the type. */
	/* header + srid/flags + bbox? + type number */
	size = VARHDRSZ + 8 + 32 + 4;  

	geom = PG_GETARG_GSERIALIZED_P_SLICE(0, 0, size);

	type = gserialized_get_type(geom);
	PG_RETURN_BOOL(lwtype_is_collection(type));
}

PG_FUNCTION_INFO_V1(LWGEOM_makepoint);
Datum LWGEOM_makepoint(PG_FUNCTION_ARGS)
{
	double x,y,z,m;
	LWPOINT *point;
	GSERIALIZED *result;

	POSTGIS_DEBUG(2, "LWGEOM_makepoint called");

	x = PG_GETARG_FLOAT8(0);
	y = PG_GETARG_FLOAT8(1);

	if ( PG_NARGS() == 2 ) point = lwpoint_make2d(SRID_UNKNOWN, x, y);
	else if ( PG_NARGS() == 3 )
	{
		z = PG_GETARG_FLOAT8(2);
		point = lwpoint_make3dz(SRID_UNKNOWN, x, y, z);
	}
	else if ( PG_NARGS() == 4 )
	{
		z = PG_GETARG_FLOAT8(2);
		m = PG_GETARG_FLOAT8(3);
		point = lwpoint_make4d(SRID_UNKNOWN, x, y, z, m);
	}
	else
	{
		elog(ERROR, "LWGEOM_makepoint: unsupported number of args: %d",
		     PG_NARGS());
		PG_RETURN_NULL();
	}

	result = geometry_serialize((LWGEOM *)point);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(LWGEOM_makepoint3dm);
Datum LWGEOM_makepoint3dm(PG_FUNCTION_ARGS)
{
	double x,y,m;
	LWPOINT *point;
	GSERIALIZED *result;

	POSTGIS_DEBUG(2, "LWGEOM_makepoint3dm called.");

	x = PG_GETARG_FLOAT8(0);
	y = PG_GETARG_FLOAT8(1);
	m = PG_GETARG_FLOAT8(2);

	point = lwpoint_make3dm(SRID_UNKNOWN, x, y, m);
	result = geometry_serialize((LWGEOM *)point);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(LWGEOM_addpoint);
Datum LWGEOM_addpoint(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwg1, *pglwg2, *result;
	LWPOINT *point;
	LWLINE *line, *linecopy;
	int where = -1;

	POSTGIS_DEBUG(2, "LWGEOM_addpoint called.");

	pglwg1 = PG_GETARG_GSERIALIZED_P(0);
	pglwg2 = PG_GETARG_GSERIALIZED_P(1);

	if ( PG_NARGS() > 2 )
	{
		where = PG_GETARG_INT32(2);
	}

	if ( gserialized_get_type(pglwg1) != LINETYPE )
	{
		elog(ERROR, "First argument must be a LINESTRING");
		PG_RETURN_NULL();
	}

	if ( gserialized_get_type(pglwg2) != POINTTYPE )
	{
		elog(ERROR, "Second argument must be a POINT");
		PG_RETURN_NULL();
	}

	line = lwgeom_as_lwline(lwgeom_from_gserialized(pglwg1));

	if ( where == -1 ) where = line->points->npoints;
	else if ( where < 0 || where > line->points->npoints )
	{
		elog(ERROR, "Invalid offset");
		PG_RETURN_NULL();
	}

	point = lwgeom_as_lwpoint(lwgeom_from_gserialized(pglwg2));
	linecopy = lwgeom_as_lwline(lwgeom_clone_deep(lwline_as_lwgeom(line)));
	lwline_free(line);
	
	if ( lwline_add_lwpoint(linecopy, point, where) == LW_FAILURE )
	{
		elog(ERROR, "Point insert failed");
		PG_RETURN_NULL();
	}

	result = geometry_serialize(lwline_as_lwgeom(linecopy));

	/* Release memory */
	PG_FREE_IF_COPY(pglwg1, 0);
	PG_FREE_IF_COPY(pglwg2, 1);
	lwpoint_free(point);

	PG_RETURN_POINTER(result);

}

PG_FUNCTION_INFO_V1(LWGEOM_removepoint);
Datum LWGEOM_removepoint(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwg1, *result;
	LWLINE *line, *outline;
	uint32 which;

	POSTGIS_DEBUG(2, "LWGEOM_removepoint called.");

	pglwg1 = PG_GETARG_GSERIALIZED_P(0);
	which = PG_GETARG_INT32(1);

	if ( gserialized_get_type(pglwg1) != LINETYPE )
	{
		elog(ERROR, "First argument must be a LINESTRING");
		PG_RETURN_NULL();
	}

	line = lwgeom_as_lwline(lwgeom_from_gserialized(pglwg1));

	if ( which > line->points->npoints-1 )
	{
		elog(ERROR, "Point index out of range (%d..%d)", 0, line->points->npoints-1);
		PG_RETURN_NULL();
	}

	if ( line->points->npoints < 3 )
	{
		elog(ERROR, "Can't remove points from a single segment line");
		PG_RETURN_NULL();
	}

	outline = lwline_removepoint(line, which);
	/* Release memory */
	lwline_free(line);

	result = geometry_serialize((LWGEOM *)outline);
	lwline_free(outline);

	PG_FREE_IF_COPY(pglwg1, 0);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(LWGEOM_setpoint_linestring);
Datum LWGEOM_setpoint_linestring(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwg1, *pglwg2, *result;
	LWGEOM *lwg;
	LWLINE *line;
	LWPOINT *lwpoint;
	POINT4D newpoint;
	uint32 which;

	POSTGIS_DEBUG(2, "LWGEOM_setpoint_linestring called.");

	/* we copy input as we're going to modify it */
	pglwg1 = PG_GETARG_GSERIALIZED_P_COPY(0);

	which = PG_GETARG_INT32(1);
	pglwg2 = PG_GETARG_GSERIALIZED_P(2);


	/* Extract a POINT4D from the point */
	lwg = lwgeom_from_gserialized(pglwg2);
	lwpoint = lwgeom_as_lwpoint(lwg);
	if ( ! lwpoint )
	{
		elog(ERROR, "Third argument must be a POINT");
		PG_RETURN_NULL();
	}
	getPoint4d_p(lwpoint->point, 0, &newpoint);
	lwpoint_free(lwpoint);
	PG_FREE_IF_COPY(pglwg2, 2);

	lwg = lwgeom_from_gserialized(pglwg1);
	line = lwgeom_as_lwline(lwg);
	if ( ! line )
	{
		elog(ERROR, "First argument must be a LINESTRING");
		PG_RETURN_NULL();
	}
	if ( which > line->points->npoints-1 )
	{
		elog(ERROR, "Point index out of range (%d..%d)", 0, line->points->npoints-1);
		PG_RETURN_NULL();
	}

	/*
	 * This will change pointarray of the serialized pglwg1,
	 */
	lwline_setPoint4d(line, which, &newpoint);
	result = geometry_serialize((LWGEOM *)line);

	/* Release memory */
	lwline_free(line);
	pfree(pglwg1); /* we forced copy, POINARRAY is released now */

	PG_RETURN_POINTER(result);
}

/* convert LWGEOM to ewkt (in TEXT format) */
PG_FUNCTION_INFO_V1(LWGEOM_asEWKT);
Datum LWGEOM_asEWKT(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	char *wkt;
	size_t wkt_size;
	text *result;

	POSTGIS_DEBUG(2, "LWGEOM_asEWKT called.");

	geom = PG_GETARG_GSERIALIZED_P(0);
	lwgeom = lwgeom_from_gserialized(geom);

	/* Write to WKT and free the geometry */
	wkt = lwgeom_to_wkt(lwgeom, WKT_EXTENDED, DBL_DIG, &wkt_size);
	lwgeom_free(lwgeom);

	/* Write to text and free the WKT */
	result = cstring2text(wkt);
	pfree(wkt);

	/* Return the text */
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_TEXT_P(result);
}

/**
 * Compute the azimuth of segment defined by the two
 * given Point geometries.
 * @return NULL on exception (same point).
 * 		Return radians otherwise.
 */
PG_FUNCTION_INFO_V1(LWGEOM_azimuth);
Datum LWGEOM_azimuth(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWPOINT *lwpoint;
	POINT2D p1, p2;
	double result;
	int srid;

	/* Extract first point */
	geom = PG_GETARG_GSERIALIZED_P(0);
	lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(geom));
	if ( ! lwpoint )
	{
		PG_FREE_IF_COPY(geom, 0);
		lwerror("Argument must be POINT geometries");
		PG_RETURN_NULL();
	}
	srid = lwpoint->srid;
	if ( ! getPoint2d_p(lwpoint->point, 0, &p1) )
	{
		PG_FREE_IF_COPY(geom, 0);
		lwerror("Error extracting point");
		PG_RETURN_NULL();
	}
	lwpoint_free(lwpoint);
	PG_FREE_IF_COPY(geom, 0);

	/* Extract second point */
	geom = PG_GETARG_GSERIALIZED_P(1);
	lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(geom));
	if ( ! lwpoint )
	{
		PG_FREE_IF_COPY(geom, 1);
		lwerror("Argument must be POINT geometries");
		PG_RETURN_NULL();
	}
	if ( lwpoint->srid != srid )
	{
		PG_FREE_IF_COPY(geom, 1);
		lwerror("Operation on mixed SRID geometries");
		PG_RETURN_NULL();
	}
	if ( ! getPoint2d_p(lwpoint->point, 0, &p2) )
	{
		PG_FREE_IF_COPY(geom, 1);
		lwerror("Error extracting point");
		PG_RETURN_NULL();
	}
	lwpoint_free(lwpoint);
	PG_FREE_IF_COPY(geom, 1);

	/* Standard return value for equality case */
	if ( (p1.x == p2.x) && (p1.y == p2.y) )
	{
		PG_RETURN_NULL();
	}

	/* Compute azimuth */
	if ( ! azimuth_pt_pt(&p1, &p2, &result) )
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_FLOAT8(result);
}

/*
 * optimistic_overlap(Polygon P1, Multipolygon MP2, double dist)
 * returns true if P1 overlaps MP2
 *   method: bbox check -
 *   is separation < dist?  no - return false (quick)
 *                          yes  - return distance(P1,MP2) < dist
 */
PG_FUNCTION_INFO_V1(optimistic_overlap);
Datum optimistic_overlap(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pg_geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pg_geom2 = PG_GETARG_GSERIALIZED_P(1);
	double dist = PG_GETARG_FLOAT8(2);
	GBOX g1_bvol;
	double calc_dist;

	LWGEOM *geom1 = lwgeom_from_gserialized(pg_geom1);
	LWGEOM *geom2 = lwgeom_from_gserialized(pg_geom2);

	if (geom1->srid != geom2->srid)
	{
		elog(ERROR,"optimistic_overlap:Operation on two GEOMETRIES with different SRIDs\\n");
		PG_RETURN_NULL();
	}

	if (geom1->type != POLYGONTYPE)
	{
		elog(ERROR,"optimistic_overlap: first arg isnt a polygon\n");
		PG_RETURN_NULL();
	}

	if (geom2->type != POLYGONTYPE &&  geom2->type != MULTIPOLYGONTYPE)
	{
		elog(ERROR,"optimistic_overlap: 2nd arg isnt a [multi-]polygon\n");
		PG_RETURN_NULL();
	}

	/*bbox check */
	gserialized_get_gbox_p(pg_geom1, &g1_bvol );

	g1_bvol.xmin = g1_bvol.xmin - dist;
	g1_bvol.ymin = g1_bvol.ymin - dist;
	g1_bvol.xmax = g1_bvol.xmax + dist;
	g1_bvol.ymax = g1_bvol.ymax + dist;

	if ( (g1_bvol.xmin > geom2->bbox->xmax) ||
	     (g1_bvol.xmax < geom2->bbox->xmin) ||
	     (g1_bvol.ymin > geom2->bbox->ymax) ||
	     (g1_bvol.ymax < geom2->bbox->ymin) )
	{
		PG_RETURN_BOOL(FALSE);  /*bbox not overlap */
	}

	/*
	 * compute distances
	 * should be a fast calc if they actually do intersect
	 */
	calc_dist = DatumGetFloat8 ( DirectFunctionCall2(LWGEOM_mindistance2d,   PointerGetDatum( pg_geom1 ),       PointerGetDatum( pg_geom2 )));

	PG_RETURN_BOOL(calc_dist < dist);
}


/*affine transform geometry */
PG_FUNCTION_INFO_V1(LWGEOM_affine);
Datum LWGEOM_affine(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P_COPY(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	GSERIALIZED *ret;
	AFFINE affine;

	affine.afac =  PG_GETARG_FLOAT8(1);
	affine.bfac =  PG_GETARG_FLOAT8(2);
	affine.cfac =  PG_GETARG_FLOAT8(3);
	affine.dfac =  PG_GETARG_FLOAT8(4);
	affine.efac =  PG_GETARG_FLOAT8(5);
	affine.ffac =  PG_GETARG_FLOAT8(6);
	affine.gfac =  PG_GETARG_FLOAT8(7);
	affine.hfac =  PG_GETARG_FLOAT8(8);
	affine.ifac =  PG_GETARG_FLOAT8(9);
	affine.xoff =  PG_GETARG_FLOAT8(10);
	affine.yoff =  PG_GETARG_FLOAT8(11);
	affine.zoff =  PG_GETARG_FLOAT8(12);

	POSTGIS_DEBUG(2, "LWGEOM_affine called.");

	lwgeom_affine(lwgeom, &affine);

	/* COMPUTE_BBOX TAINTING */
	lwgeom_drop_bbox(lwgeom);
	lwgeom_add_bbox(lwgeom);
	ret = geometry_serialize(lwgeom);

	/* Release memory */
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);

	PG_RETURN_POINTER(ret);
}

PG_FUNCTION_INFO_V1(ST_GeoHash);
Datum ST_GeoHash(PG_FUNCTION_ARGS)
{

	GSERIALIZED *geom = NULL;
	int precision = 0;
	char *geohash = NULL;
	text *result = NULL;

	if ( PG_ARGISNULL(0) )
	{
		PG_RETURN_NULL();
	}

	geom = PG_GETARG_GSERIALIZED_P(0);

	if ( ! PG_ARGISNULL(1) )
	{
		precision = PG_GETARG_INT32(1);
	}

	geohash = lwgeom_geohash((LWGEOM*)(lwgeom_from_gserialized(geom)), precision);

	if ( ! geohash )
		PG_RETURN_NULL();

	result = cstring2text(geohash);
	pfree(geohash);
	
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(ST_CollectionExtract);
Datum ST_CollectionExtract(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *output;
	LWGEOM *lwgeom = lwgeom_from_gserialized(input);
	LWGEOM *lwcol = NULL;
	int type = PG_GETARG_INT32(1);
	int lwgeom_type = lwgeom->type;

	/* Ensure the right type was input */
	if ( ! ( type == POINTTYPE || type == LINETYPE || type == POLYGONTYPE ) )
	{
		lwgeom_free(lwgeom);
		elog(ERROR, "ST_CollectionExtract: only point, linestring and polygon may be extracted");
		PG_RETURN_NULL();
	}

	/* Mirror non-collections right back */
	if ( ! lwgeom_is_collection(lwgeom) )
	{
		/* Non-collections of the matching type go back */
		if(lwgeom_type == type)
		{
			lwgeom_free(lwgeom);
			PG_RETURN_POINTER(input);
		}
		/* Others go back as EMPTY */
		else
		{
			lwcol = lwgeom_construct_empty(type, lwgeom->srid, FLAGS_GET_Z(lwgeom->flags), FLAGS_GET_M(lwgeom->flags));
		}
	}
	else
	{
		lwcol = lwcollection_as_lwgeom(lwcollection_extract((LWCOLLECTION*)lwgeom, type));
	}

#if 0
	if (lwgeom_is_empty(lwcollection_as_lwgeom(lwcol)))
	{
		lwgeom_free(lwgeom);
		PG_RETURN_NULL();
	}
#endif
	output = geometry_serialize((LWGEOM*)lwcol);
	lwgeom_free(lwgeom);
	lwgeom_free(lwcol);

	PG_RETURN_POINTER(output);
}

PG_FUNCTION_INFO_V1(ST_CollectionHomogenize);
Datum ST_CollectionHomogenize(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *output;
	LWGEOM *lwgeom = lwgeom_from_gserialized(input);
	LWGEOM *lwoutput = NULL;

	lwoutput = lwgeom_homogenize(lwgeom);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(input, 0);

	if ( ! lwoutput )
		PG_RETURN_NULL();

	output = geometry_serialize(lwoutput);
	lwgeom_free(lwoutput);

	PG_RETURN_POINTER(output);
}

Datum ST_RemoveRepeatedPoints(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_RemoveRepeatedPoints);
Datum ST_RemoveRepeatedPoints(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input = PG_GETARG_GSERIALIZED_P_COPY(0);
	GSERIALIZED *output;
	LWGEOM *lwgeom_in = lwgeom_from_gserialized(input);
	LWGEOM *lwgeom_out;

	/* lwnotice("ST_RemoveRepeatedPoints got %p", lwgeom_in); */

	lwgeom_out = lwgeom_remove_repeated_points(lwgeom_in);
	output = geometry_serialize(lwgeom_out);

	lwgeom_free(lwgeom_in);
	PG_FREE_IF_COPY(input, 0);

	PG_RETURN_POINTER(output);
}

Datum ST_FlipCoordinates(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_FlipCoordinates);
Datum ST_FlipCoordinates(PG_FUNCTION_ARGS)
{
	GSERIALIZED *in = PG_GETARG_GSERIALIZED_P_COPY(0);
	GSERIALIZED *out;
	LWGEOM *lwgeom = lwgeom_from_gserialized(in);

	lwgeom_swap_ordinates(lwgeom, LWORD_X, LWORD_Y);
	out = geometry_serialize(lwgeom);

	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(in, 0);

	PG_RETURN_POINTER(out);
}

static LWORD ordname2ordval(char n)
{
  if ( n == 'x' || n == 'X' ) return LWORD_X;
  if ( n == 'y' || n == 'y' ) return LWORD_Y;
  if ( n == 'z' || n == 'Z' ) return LWORD_Z;
  if ( n == 'm' || n == 'M' ) return LWORD_M;
  lwerror("Invalid ordinate name '%c'. Expected x,y,z or m", n);
  return (LWORD)-1;
}

Datum ST_SwapOrdinates(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_SwapOrdinates);
Datum ST_SwapOrdinates(PG_FUNCTION_ARGS)
{
  GSERIALIZED *in;
  GSERIALIZED *out;
  LWGEOM *lwgeom;
  const char *ospec;
  LWORD o1, o2;

  ospec = PG_GETARG_CSTRING(1);
  if ( strlen(ospec) != 2 )
  {
    lwerror("Invalid ordinate specification. "
            "Need two letters from the set (x,y,z,m). "
            "Got '%s'", ospec);
    PG_RETURN_NULL();
  }
  o1 = ordname2ordval( ospec[0] );
  o2 = ordname2ordval( ospec[1] );

  in = PG_GETARG_GSERIALIZED_P_COPY(0);

  /* Check presence of given ordinates */
  if ( ( o1 == LWORD_M || o2 == LWORD_M ) && ! gserialized_has_m(in) )
  {
    lwerror("Geometry does not have an M ordinate");
    PG_RETURN_NULL();
  }
  if ( ( o1 == LWORD_Z || o2 == LWORD_Z ) && ! gserialized_has_z(in) )
  {
    lwerror("Geometry does not have a Z ordinate");
    PG_RETURN_NULL();
  }

  /* Nothing to do if swapping the same ordinate, pity for the copy... */
  if ( o1 == o2 ) PG_RETURN_POINTER(in);

  lwgeom = lwgeom_from_gserialized(in);
  lwgeom_swap_ordinates(lwgeom, o1, o2);
  out = geometry_serialize(lwgeom);
  lwgeom_free(lwgeom);
  PG_FREE_IF_COPY(in, 0);
  PG_RETURN_POINTER(out);
}
