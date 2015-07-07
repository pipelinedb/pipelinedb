/**********************************************************************
*
* PostGIS - Spatial Types for PostgreSQL
* http://postgis.net
*
* Copyright 2014 Kashif Rasul <kashif.rasul@gmail.com> and
*                Shoaib Burq <saburq@gmail.com>
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

Datum line_from_encoded_polyline(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(line_from_encoded_polyline);
Datum line_from_encoded_polyline(PG_FUNCTION_ARGS)
{
  GSERIALIZED *geom;
  LWGEOM *lwgeom;
  text *encodedpolyline_input;
  char *encodedpolyline;
  int precision = 5;

  if (PG_ARGISNULL(0)) PG_RETURN_NULL();

  encodedpolyline_input = PG_GETARG_TEXT_P(0);
  encodedpolyline = text2cstring(encodedpolyline_input);

  if (PG_NARGS() >2 && !PG_ARGISNULL(2))
  {
    precision = PG_GETARG_INT32(2);
    if ( precision < 0 ) precision = 5;
  }

  lwgeom = lwgeom_from_encoded_polyline(encodedpolyline, precision);
  if ( ! lwgeom ) {
    /* Shouldn't get here */
    elog(ERROR, "lwgeom_from_encoded_polyline returned NULL");
    PG_RETURN_NULL();
  }
  lwgeom_set_srid(lwgeom, 4326);

  geom = geometry_serialize(lwgeom);
  lwgeom_free(lwgeom);
  PG_RETURN_POINTER(geom);
}
