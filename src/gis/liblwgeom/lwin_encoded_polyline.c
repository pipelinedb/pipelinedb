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
#include <string.h>
#include "liblwgeom.h"
#include "../postgis_config.h"

LWGEOM*
lwgeom_from_encoded_polyline(const char *encodedpolyline, int precision)
{
  LWGEOM *geom = NULL;
  POINTARRAY *pa = NULL;
  int length = strlen(encodedpolyline);
  int idx = 0;
	double scale = pow(10,precision);

  float latitude = 0.0f;
  float longitude = 0.0f;

  pa = ptarray_construct_empty(LW_FALSE, LW_FALSE, 1);

  while (idx < length) {
    POINT4D pt;
    char byte = 0;

    int res = 0;
    char shift = 0;
    do {
      byte = encodedpolyline[idx++] - 63;
      res |= (byte & 0x1F) << shift;
      shift += 5;
    } while (byte >= 0x20);
    float deltaLat = ((res & 1) ? ~(res >> 1) : (res >> 1));
    latitude += deltaLat;

    shift = 0;
    res = 0;
    do {
      byte = encodedpolyline[idx++] - 63;
      res |= (byte & 0x1F) << shift;
      shift += 5;
    } while (byte >= 0x20);
    float deltaLon = ((res & 1) ? ~(res >> 1) : (res >> 1));
    longitude += deltaLon;

    pt.x = longitude/scale;
    pt.y = latitude/scale;
	pt.m = pt.z = 0.0;
    ptarray_append_point(pa, &pt, LW_FALSE);
  }

  geom = (LWGEOM *)lwline_construct(4326, NULL, pa);
  lwgeom_add_bbox(geom);

  return geom;
}
