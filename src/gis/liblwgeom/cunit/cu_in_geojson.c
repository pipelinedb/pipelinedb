/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2013 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"

#include "liblwgeom_internal.h"
#include "cu_tester.h"

static void do_geojson_test(const char * exp, char * in, char * exp_srs, int precision, int has_bbox)
{
	LWGEOM *g;
	char * h = NULL;
  char * srs = NULL;
  size_t size;

	g = lwgeom_from_geojson(in, &srs);
  if ( ! g ) {
		fprintf(stderr, "\nIn:   %s\nExp:  %s\nObt: %s\n", in, exp, cu_error_msg);
	  CU_ASSERT(g != NULL);
    return;
  }

	h = lwgeom_to_wkt(g, WKT_EXTENDED, 15, &size);

	if (strcmp(h, exp)) {
		fprintf(stderr, "\nIn:   %s\nExp:  %s\nObt: %s\n", in, exp, h);
	  CU_ASSERT_STRING_EQUAL(h, exp);
  }

  if ( exp_srs ) {
    if ( ! srs ) {
      fprintf(stderr, "\nIn:   %s\nExp:  %s\nObt: (null)\n", in, exp_srs);
	    CU_ASSERT_EQUAL(srs, exp_srs);
    }
    else if (strcmp(srs, exp_srs)) {
      fprintf(stderr, "\nIn:   %s\nExp:  %s\nObt: %s\n", in, exp_srs, srs);
	    CU_ASSERT_STRING_EQUAL(srs, exp_srs);
    }
  } else if ( srs ) {
    fprintf(stderr, "\nIn:   %s\nExp:  (null)\nObt: %s\n", in, srs);
	  CU_ASSERT_EQUAL(srs, exp_srs);
  }

	lwgeom_free(g);
	if ( h ) lwfree(h);
  if ( srs ) lwfree(srs);
}

static void in_geojson_test_srid(void)
{
	/* Linestring */
	do_geojson_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "{\"type\":\"LineString\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[[0,1],[2,3],[4,5]]}",
	    "EPSG:4326", 0, 0);

	/* Polygon */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "{\"type\":\"Polygon\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]]]}",
	    "EPSG:4326", 0, 0);

	/* Polygon - with internal ring */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "{\"type\":\"Polygon\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]],[[6,7],[8,9],[10,11],[6,7]]]}",
	    "EPSG:4326", 0, 0);

	/* Multiline */
	do_geojson_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "{\"type\":\"MultiLineString\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[[[0,1],[2,3],[4,5]],[[6,7],[8,9],[10,11]]]}",
	    "EPSG:4326", 0, 0);

	/* MultiPolygon */
	do_geojson_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "{\"type\":\"MultiPolygon\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"coordinates\":[[[[0,1],[2,3],[4,5],[0,1]]],[[[6,7],[8,9],[10,11],[6,7]]]]}",
	    "EPSG:4326", 0, 0);

	/* Empty GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "{\"type\":\"GeometryCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"geometries\":[]}",
	    "EPSG:4326", 0, 0);
}

static void in_geojson_test_bbox(void)
{
	/* Linestring */
	do_geojson_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "{\"type\":\"LineString\",\"bbox\":[0,1,4,5],\"coordinates\":[[0,1],[2,3],[4,5]]}",
	    NULL, 0, 1);

	/* Polygon */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "{\"type\":\"Polygon\",\"bbox\":[0,1,4,5],\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]]]}",
	    NULL, 0, 1);

	/* Polygon - with internal ring */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "{\"type\":\"Polygon\",\"bbox\":[0,1,4,5],\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]],[[6,7],[8,9],[10,11],[6,7]]]}",
	    NULL, 0, 1);

	/* Multiline */
	do_geojson_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "{\"type\":\"MultiLineString\",\"bbox\":[0,1,10,11],\"coordinates\":[[[0,1],[2,3],[4,5]],[[6,7],[8,9],[10,11]]]}",
	    NULL, 0, 1);

	/* MultiPolygon */
	do_geojson_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "{\"type\":\"MultiPolygon\",\"bbox\":[0,1,10,11],\"coordinates\":[[[[0,1],[2,3],[4,5],[0,1]]],[[[6,7],[8,9],[10,11],[6,7]]]]}",
	    NULL, 0, 1);

	/* GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION(LINESTRING(0 1,-1 3),LINESTRING(2 3,4 5))",
	    "{\"type\":\"GeometryCollection\",\"bbox\":[-1,1,4,5],\"geometries\":[{\"type\":\"LineString\",\"coordinates\":[[0,1],[-1,3]]},{\"type\":\"LineString\",\"coordinates\":[[2,3],[4,5]]}]}",
	    NULL, 0, 1);

	/* Empty GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "{\"type\":\"GeometryCollection\",\"geometries\":[]}",
	    NULL, 0, 1);
}

static void in_geojson_test_geoms(void)
{
	/* Linestring */
	do_geojson_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "{\"type\":\"LineString\",\"coordinates\":[[0,1],[2,3],[4,5]]}",
	    NULL, 0, 0);

	/* Polygon */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "{\"type\":\"Polygon\",\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]]]}",
	    NULL, 0, 0);

	/* Polygon - with internal ring */
	do_geojson_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "{\"type\":\"Polygon\",\"coordinates\":[[[0,1],[2,3],[4,5],[0,1]],[[6,7],[8,9],[10,11],[6,7]]]}",
	    NULL, 0, 0);

	/* Multiline */
	do_geojson_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "{\"type\":\"MultiLineString\",\"coordinates\":[[[0,1],[2,3],[4,5]],[[6,7],[8,9],[10,11]]]}",
	    NULL, 0, 0);

	/* MultiPolygon */
	do_geojson_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[0,1],[2,3],[4,5],[0,1]]],[[[6,7],[8,9],[10,11],[6,7]]]]}",
	    NULL, 0, 0);

  /* MultiPolygon with internal rings */
  /* See http://trac.osgeo.org/postgis/ticket/2216 */
  do_geojson_test(
      "MULTIPOLYGON(((4 0,0 -4,-4 0,0 4,4 0),(2 0,0 2,-2 0,0 -2,2 0)),((24 0,20 -4,16 0,20 4,24 0),(22 0,20 2,18 0,20 -2,22 0)),((44 0,40 -4,36 0,40 4,44 0),(42 0,40 2,38 0,40 -2,42 0)))",
      "{'type':'MultiPolygon','coordinates':[[[[4,0],[0,-4],[-4,0],[0,4],[4,0]],[[2,0],[0,2],[-2,0],[0,-2],[2,0]]],[[[24,0],[20,-4],[16,0],[20,4],[24,0]],[[22,0],[20,2],[18,0],[20,-2],[22,0]]],[[[44,0],[40,-4],[36,0],[40,4],[44,0]],[[42,0],[40,2],[38,0],[40,-2],[42,0]]]]}",
      NULL, 0, 0);

	/* GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[0,1]},{\"type\":\"LineString\",\"coordinates\":[[2,3],[4,5]]}]}",
	    NULL, 0, 0);

	/* Empty GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "{\"type\":\"GeometryCollection\",\"geometries\":[]}",
	    NULL, 0, 0);

}

/*
** Used by test harness to register the tests in this file.
*/
void in_geojson_suite_setup(void);
void in_geojson_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("geojson_input", NULL, NULL);
	PG_ADD_TEST(suite, in_geojson_test_srid);
	PG_ADD_TEST(suite, in_geojson_test_bbox);
	PG_ADD_TEST(suite, in_geojson_test_geoms);
}
