/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2012 J Smith <dark.panda@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <assert.h>

#include "postgres.h"

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "liblwgeom.h"
#include "liblwgeom_internal.h"/* for decode_geohash_bbox */

Datum box2d_from_geohash(PG_FUNCTION_ARGS);
Datum point_from_geohash(PG_FUNCTION_ARGS);

static void geohash_lwerror(char *msg, int error_code)
{
	POSTGIS_DEBUGF(3, "ST_Box2dFromGeoHash ERROR %i", error_code);
	lwerror("%s", msg);
}

#include "lwgeom_export.h"

static GBOX*
parse_geohash(char *geohash, int precision)
{
	GBOX *box = NULL;
	double lat[2], lon[2];

	POSTGIS_DEBUG(2, "parse_geohash called.");

	if (NULL == geohash)
	{
		geohash_lwerror("invalid GeoHash representation", 2);
	}

	decode_geohash_bbox(geohash, lat, lon, precision);

	POSTGIS_DEBUGF(2, "ST_Box2dFromGeoHash sw: %.20f, %.20f", lon[0], lat[0]);
	POSTGIS_DEBUGF(2, "ST_Box2dFromGeoHash ne: %.20f, %.20f", lon[1], lat[1]);

	box = gbox_new(gflags(0, 0, 1));

	box->xmin = lon[0];
	box->ymin = lat[0];

	box->xmax = lon[1];
	box->ymax = lat[1];

	POSTGIS_DEBUG(2, "parse_geohash finished.");
	return box;
}

PG_FUNCTION_INFO_V1(box2d_from_geohash);
Datum box2d_from_geohash(PG_FUNCTION_ARGS)
{
	GBOX *box = NULL;
	text *geohash_input = NULL;
	char *geohash = NULL;
	int precision = -1;

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}

	if (!PG_ARGISNULL(1))
	{
		precision = PG_GETARG_INT32(1);
	}

	geohash_input = PG_GETARG_TEXT_P(0);
	geohash = text2cstring(geohash_input);

	box = parse_geohash(geohash, precision);

	PG_RETURN_POINTER(box);
}

PG_FUNCTION_INFO_V1(point_from_geohash);
Datum point_from_geohash(PG_FUNCTION_ARGS)
{
	GBOX *box = NULL;
	LWPOINT *point = NULL;
	GSERIALIZED *result = NULL;
	text *geohash_input = NULL;
	char *geohash = NULL;
	double lon, lat;
	int precision = -1;

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}

	if (!PG_ARGISNULL(1))
	{
		precision = PG_GETARG_INT32(1);
	}

	geohash_input = PG_GETARG_TEXT_P(0);
	geohash = text2cstring(geohash_input);

	box = parse_geohash(geohash, precision);

	lon = box->xmin + (box->xmax - box->xmin) / 2;
	lat = box->ymin + (box->ymax - box->ymin) / 2;

	point = lwpoint_make2d(SRID_UNKNOWN, lon, lat);
	result = geometry_serialize((LWGEOM *) point);

	lwfree(box);

	PG_RETURN_POINTER(result);
}
