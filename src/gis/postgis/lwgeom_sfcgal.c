/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Wrapper around SFCGAL for 3D functions
 *
 * Copyright 2012-2013 Oslandia <infos@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "../liblwgeom/liblwgeom.h"

#include "lwgeom_pg.h"
#include "lwgeom_sfcgal.h"


Datum postgis_sfcgal_version(PG_FUNCTION_ARGS);

Datum sfcgal_from_ewkt(PG_FUNCTION_ARGS);
Datum sfcgal_distance(PG_FUNCTION_ARGS);
Datum sfcgal_distance3D(PG_FUNCTION_ARGS);
Datum sfcgal_area(PG_FUNCTION_ARGS);
Datum sfcgal_area3D(PG_FUNCTION_ARGS);
Datum sfcgal_intersects(PG_FUNCTION_ARGS);
Datum sfcgal_intersects3D(PG_FUNCTION_ARGS);
Datum sfcgal_intersection(PG_FUNCTION_ARGS);
Datum sfcgal_intersection3D(PG_FUNCTION_ARGS);
Datum sfcgal_extrude(PG_FUNCTION_ARGS);
Datum sfcgal_straight_skeleton(PG_FUNCTION_ARGS);
Datum sfcgal_is_planar(PG_FUNCTION_ARGS);
Datum sfcgal_orientation(PG_FUNCTION_ARGS);
Datum sfcgal_force_lhr(PG_FUNCTION_ARGS);
Datum sfcgal_triangulate(PG_FUNCTION_ARGS);
Datum sfcgal_tesselate(PG_FUNCTION_ARGS);
Datum sfcgal_minkowski_sum(PG_FUNCTION_ARGS);


GSERIALIZED *geometry_serialize(LWGEOM *lwgeom);
char* text2cstring(const text *textptr);

static int __sfcgal_init = 0;

void sfcgal_postgis_init(void)
{
    if ( ! __sfcgal_init ) {
	sfcgal_init();
	sfcgal_set_error_handlers((sfcgal_error_handler_t) lwnotice, (sfcgal_error_handler_t) lwerror);
	sfcgal_set_alloc_handlers(lwalloc, lwfree);
	__sfcgal_init = 1;
    }
}


/* Conversion from GSERIALIZED* to SFCGAL::Geometry */
sfcgal_geometry_t* POSTGIS2SFCGALGeometry(GSERIALIZED *pglwgeom)
{
	sfcgal_geometry_t* g;
	LWGEOM *lwgeom = lwgeom_from_gserialized(pglwgeom);

	if (! lwgeom)
	{
		lwerror("POSTGIS2SFCGALGeometry: Unable to deserialize input");
	}
	g = LWGEOM2SFCGAL(lwgeom);
	lwgeom_free(lwgeom);

	return g;
}


/* Conversion from GSERIALIZED* to SFCGAL::PreparedGeometry */
sfcgal_prepared_geometry_t* POSTGIS2SFCGALPreparedGeometry(GSERIALIZED *pglwgeom)
{
	sfcgal_geometry_t* g;
	LWGEOM *lwgeom = lwgeom_from_gserialized(pglwgeom);

	if (!lwgeom)
	{
		lwerror("POSTGIS2SFCGALPreparedGeometry: Unable to deserialize input");
	}
	g = LWGEOM2SFCGAL(lwgeom);

	lwgeom_free(lwgeom);

	return sfcgal_prepared_geometry_create_from_geometry(g, gserialized_get_srid(pglwgeom));
}


/* Conversion from SFCGAL::Geometry to GSERIALIZED */
GSERIALIZED* SFCGALGeometry2POSTGIS(const sfcgal_geometry_t* geom, int force3D, int SRID)
{
	GSERIALIZED *result;
	LWGEOM* lwgeom = SFCGAL2LWGEOM(geom, force3D, SRID);

	if (lwgeom_needs_bbox(lwgeom) == LW_TRUE)
		lwgeom_add_bbox(lwgeom);

	result = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	return result;
}


/* Conversion from SFCGAL::PreparedGeometry to GSERIALIZED */
GSERIALIZED* SFCGALPreparedGeometry2POSTGIS(const sfcgal_prepared_geometry_t* geom, int force3D)
{
    return SFCGALGeometry2POSTGIS(sfcgal_prepared_geometry_geometry(geom),
		force3D, sfcgal_prepared_geometry_srid(geom));
}


/* Conversion from EWKT to GSERIALIZED */
PG_FUNCTION_INFO_V1(sfcgal_from_ewkt);
Datum sfcgal_from_ewkt(PG_FUNCTION_ARGS)
{
	GSERIALIZED* result;
	sfcgal_prepared_geometry_t* g;
	text *wkttext = PG_GETARG_TEXT_P(0);
	char *cstring = text2cstring(wkttext);

	sfcgal_postgis_init();

	g = sfcgal_io_read_ewkt( cstring, strlen(cstring) );

	result = SFCGALPreparedGeometry2POSTGIS( g, 0 );
	sfcgal_prepared_geometry_delete( g );
	PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(sfcgal_area);
Datum sfcgal_area(PG_FUNCTION_ARGS)
    {
	GSERIALIZED *input;
	sfcgal_geometry_t *geom;
	double result;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	geom = POSTGIS2SFCGALGeometry(input);

	result = sfcgal_geometry_area(geom);
	sfcgal_geometry_delete(geom);

	PG_FREE_IF_COPY(input, 0);

	PG_RETURN_FLOAT8(result);
}


PG_FUNCTION_INFO_V1(sfcgal_area3D);
Datum sfcgal_area3D(PG_FUNCTION_ARGS)
    {
	GSERIALIZED *input;
	sfcgal_geometry_t *geom;
	double result;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	geom = POSTGIS2SFCGALGeometry(input);

	result = sfcgal_geometry_area_3d(geom);
	sfcgal_geometry_delete(geom);

	PG_FREE_IF_COPY(input, 0);

	PG_RETURN_FLOAT8(result);
}


PG_FUNCTION_INFO_V1(sfcgal_is_planar);
Datum sfcgal_is_planar(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input;
	sfcgal_geometry_t *geom;
	int result;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	geom = POSTGIS2SFCGALGeometry(input);

	result = sfcgal_geometry_is_planar(geom);
	sfcgal_geometry_delete(geom);

	PG_FREE_IF_COPY(input, 0);

	PG_RETURN_BOOL(result);
}


PG_FUNCTION_INFO_V1(sfcgal_orientation);
Datum sfcgal_orientation(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input;
	sfcgal_geometry_t *geom;
	int result;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	geom = POSTGIS2SFCGALGeometry(input);

	result = sfcgal_geometry_orientation(geom);
	sfcgal_geometry_delete(geom);

	PG_FREE_IF_COPY(input, 0);

	PG_RETURN_INT32(result);
}


PG_FUNCTION_INFO_V1(sfcgal_intersects);
Datum sfcgal_intersects(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1;
	sfcgal_geometry_t *geom0, *geom1;
	int result;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_intersects(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	PG_RETURN_BOOL(result);
}


PG_FUNCTION_INFO_V1(sfcgal_intersects3D);
Datum sfcgal_intersects3D(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1;
	sfcgal_geometry_t *geom0, *geom1;
	int result;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_intersects_3d(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	PG_RETURN_BOOL(result);
}


PG_FUNCTION_INFO_V1(sfcgal_distance);
Datum sfcgal_distance(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1;
	sfcgal_geometry_t *geom0, *geom1;
	double result;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_distance(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	PG_RETURN_FLOAT8(result);
}


PG_FUNCTION_INFO_V1(sfcgal_distance3D);
Datum sfcgal_distance3D(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1;
	sfcgal_geometry_t *geom0, *geom1;
	double result;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_distance_3d(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	PG_RETURN_FLOAT8(result);
}


PG_FUNCTION_INFO_V1(sfcgal_tesselate);	
Datum sfcgal_tesselate(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input, *output;
	sfcgal_geometry_t *geom;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input);
	geom = POSTGIS2SFCGALGeometry(input);
	PG_FREE_IF_COPY(input, 0);

	result = sfcgal_geometry_tesselate(geom);
	sfcgal_geometry_delete(geom);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_triangulate);	
Datum sfcgal_triangulate(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input, *output;
	sfcgal_geometry_t *geom;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input);
	geom = POSTGIS2SFCGALGeometry(input);
	PG_FREE_IF_COPY(input, 0);

	result = sfcgal_geometry_triangulate_2dz(geom);
	sfcgal_geometry_delete(geom);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_force_lhr);	
Datum sfcgal_force_lhr(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input, *output;
	sfcgal_geometry_t *geom;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input);
	geom = POSTGIS2SFCGALGeometry(input);
	PG_FREE_IF_COPY(input, 0);

	result = sfcgal_geometry_force_lhr(geom);
	sfcgal_geometry_delete(geom);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_straight_skeleton);	
Datum sfcgal_straight_skeleton(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input, *output;
	sfcgal_geometry_t *geom;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input);
	geom = POSTGIS2SFCGALGeometry(input);
	PG_FREE_IF_COPY(input, 0);

	result = sfcgal_geometry_straight_skeleton(geom);
	sfcgal_geometry_delete(geom);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_intersection);	
Datum sfcgal_intersection(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1, *output;
	sfcgal_geometry_t *geom0, *geom1;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_intersection(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_intersection3D);	
Datum sfcgal_intersection3D(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1, *output;
	sfcgal_geometry_t *geom0, *geom1;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_intersection_3d(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_minkowski_sum);	
Datum sfcgal_minkowski_sum(PG_FUNCTION_ARGS)
{
	GSERIALIZED *input0, *input1, *output;
	sfcgal_geometry_t *geom0, *geom1;
	sfcgal_geometry_t *result;
	srid_t srid;

	sfcgal_postgis_init();

	input0 = PG_GETARG_GSERIALIZED_P(0);
	srid = gserialized_get_srid(input0);
	input1 = PG_GETARG_GSERIALIZED_P(1);
	geom0 = POSTGIS2SFCGALGeometry(input0);
	PG_FREE_IF_COPY(input0, 0);
	geom1 = POSTGIS2SFCGALGeometry(input1);
	PG_FREE_IF_COPY(input1, 1);

	result = sfcgal_geometry_minkowski_sum(geom0, geom1);
	sfcgal_geometry_delete(geom0);
	sfcgal_geometry_delete(geom1);

	output = SFCGALGeometry2POSTGIS(result, 0, srid);
	sfcgal_geometry_delete(result);

	PG_RETURN_POINTER(output);
}


PG_FUNCTION_INFO_V1(sfcgal_extrude);
Datum sfcgal_extrude(PG_FUNCTION_ARGS)
{
    GSERIALIZED *input, *output;
    sfcgal_geometry_t *geom;
    sfcgal_geometry_t *result;
    double dx, dy, dz;
    srid_t srid;

    sfcgal_postgis_init();
    
    input = PG_GETARG_GSERIALIZED_P(0);
    srid = gserialized_get_srid(input);

    geom = POSTGIS2SFCGALGeometry(input);
    PG_FREE_IF_COPY(input, 0);
    
    dx = PG_GETARG_FLOAT8(1);
    dy = PG_GETARG_FLOAT8(2);
    dz = PG_GETARG_FLOAT8(3);
    
    result = sfcgal_geometry_extrude(geom, dx, dy, dz);
    sfcgal_geometry_delete(geom);
    
    output = SFCGALGeometry2POSTGIS(result, 0, srid);
    sfcgal_geometry_delete(result);
    
    PG_RETURN_POINTER(output);
}

PG_FUNCTION_INFO_V1(postgis_sfcgal_version);
Datum postgis_sfcgal_version(PG_FUNCTION_ARGS)
{
        const char *ver = lwgeom_sfcgal_version();
        text *result = cstring2text(ver);
        PG_RETURN_POINTER(result);
}

