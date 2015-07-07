/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2011 Kashif Rasul <kashif.rasul@gmail.com>
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
#include "lwgeom_export.h"

#ifdef HAVE_LIBJSON
# ifdef HAVE_LIBJSON_C
#  include <json-c/json.h>
# else
#  include <json/json.h>
# endif
#endif

Datum geom_from_geojson(PG_FUNCTION_ARGS);
Datum postgis_libjson_version(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(postgis_libjson_version);
Datum postgis_libjson_version(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBJSON
	PG_RETURN_NULL();
#else /* HAVE_LIBJSON  */
# ifdef JSON_C_VERSION
  const char *ver = json_c_version(); 
# else
	const char *ver = "UNKNOWN";
# endif
	text *result = cstring2text(ver);
	PG_RETURN_POINTER(result);
#endif
}

PG_FUNCTION_INFO_V1(geom_from_geojson);
Datum geom_from_geojson(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBJSON
	elog(ERROR, "You need JSON-C for ST_GeomFromGeoJSON");
	PG_RETURN_NULL();
#else /* HAVE_LIBJSON  */

	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	text *geojson_input;
	char *geojson;
  char *srs = NULL;

	/* Get the geojson stream */
	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	geojson_input = PG_GETARG_TEXT_P(0);
	geojson = text2cstring(geojson_input);

  lwgeom = lwgeom_from_geojson(geojson, &srs);
  if ( ! lwgeom ) {
    /* Shouldn't get here */
	  elog(ERROR, "lwgeom_from_geojson returned NULL");
    PG_RETURN_NULL();
  }

  if ( srs ) {
    lwgeom_set_srid(lwgeom, getSRIDbySRS(srs));
    lwfree(srs);
  }

	geom = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	PG_RETURN_POINTER(geom);
#endif
}

