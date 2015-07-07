/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 Olivier Courtin <olivier.courtin@oslandia.com>
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

static void do_geojson_test(char * in, char * out, char * srs, int precision, int has_bbox)
{
	LWGEOM *g;
	char * h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_geojson(g, srs, precision, has_bbox);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}


static void do_geojson_unsupported(char * in, char * out)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_geojson(g, NULL, 0, 0);

	if (strcmp(cu_error_msg, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n",
		        in, cu_error_msg, out);

	CU_ASSERT_STRING_EQUAL(out, cu_error_msg);
	cu_error_msg_reset();

	lwfree(h);
	lwgeom_free(g);
}


static void out_geojson_test_precision(void)
{
	/* 0 precision, i.e a round */
	do_geojson_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "{\"type\":\"Point\",\"coordinates\":[1,1]}",
	    NULL, 0, 0);

	/* 3 digits precision */
	do_geojson_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "{\"type\":\"Point\",\"coordinates\":[1.111,1.111]}",
	    NULL, 3, 0);

	/* 9 digits precision */
	do_geojson_test(
	    "POINT(1.2345678901234 1.2345678901234)",
	    "{\"type\":\"Point\",\"coordinates\":[1.23456789,1.23456789]}",
	    NULL, 9, 0);

	/* huge data */
	do_geojson_test(
	    "POINT(1E300 -1E300)",
	    "{\"type\":\"Point\",\"coordinates\":[1e+300,-1e+300]}",
	    NULL, 0, 0);

	/* huge precision, see http://trac.osgeo.org/postgis/ticket/2052 */
	do_geojson_test(
	    "POINT(1 2)",
	    "{\"type\":\"Point\",\"coordinates\":[1,2]}",
	    NULL, 100, 0);

	/* double precision, see http://trac.osgeo.org/postgis/ticket/2051 */
	do_geojson_test(
	    "POINT(59.99 -59.99)",
	    "{\"type\":\"Point\",\"coordinates\":[59.99,-59.99]}",
	    NULL, 15, 0);

	/* small numbers */
  /* NOTE: precision of 300 will be converted to max precision (15) 
   *       and being there no significant digit within that range
   *       only zeroes will be returned
   * See http://trac.osgeo.org/postgis/ticket/2051#comment:11
   */
	do_geojson_test(
	    "POINT(1E-300 -2E-200)",
	    "{\"type\":\"Point\",\"coordinates\":[0,-0]}",
	    NULL, 300, 0);

}


static void out_geojson_test_dims(void)
{
	/* 3D */
	do_geojson_test(
	    "POINT(0 1 2)",
	    "{\"type\":\"Point\",\"coordinates\":[0,1,2]}",
	    NULL, 0, 0);

	/* 3DM */
	do_geojson_test(
	    "POINTM(0 1 2)",
	    "{\"type\":\"Point\",\"coordinates\":[0,1]}",
	    NULL, 0, 0);

	/* 4D */
	do_geojson_test(
	    "POINT(0 1 2 3)",
	    "{\"type\":\"Point\",\"coordinates\":[0,1,2]}",
	    NULL, 0, 0);
}


static void out_geojson_test_srid(void)
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

	/* GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "{\"type\":\"GeometryCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"geometries\":[{\"type\":\"Point\",\"coordinates\":[0,1]},{\"type\":\"LineString\",\"coordinates\":[[2,3],[4,5]]}]}",
	    "EPSG:4326", 0, 0);

	/* Empty GeometryCollection */
	do_geojson_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "{\"type\":\"GeometryCollection\",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}},\"geometries\":[]}",
	    "EPSG:4326", 0, 0);
}

static void out_geojson_test_bbox(void)
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

static void out_geojson_test_geoms(void)
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

	/* Nested GeometryCollection */
	do_geojson_unsupported(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "GeoJson: geometry not supported.");

	/* CircularString */
	do_geojson_unsupported(
	    "CIRCULARSTRING(-2 0,0 2,2 0,0 2,2 4)",
	    "lwgeom_to_geojson: 'CircularString' geometry type not supported");

	/* CompoundCurve */
	do_geojson_unsupported(
	    "COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))",
	    "lwgeom_to_geojson: 'CompoundCurve' geometry type not supported");

	/* CurvePolygon */
	do_geojson_unsupported(
	    "CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))",
	    "lwgeom_to_geojson: 'CurvePolygon' geometry type not supported");

	/* MultiCurve */
	do_geojson_unsupported(
	    "MULTICURVE((5 5,3 5,3 3,0 3),CIRCULARSTRING(0 0,2 1,2 2))",
	    "lwgeom_to_geojson: 'MultiCurve' geometry type not supported");

	/* MultiSurface */
	do_geojson_unsupported(
	    "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
	    "lwgeom_to_geojson: 'MultiSurface' geometry type not supported");
}

/*
** Used by test harness to register the tests in this file.
*/
void out_geojson_suite_setup(void);
void out_geojson_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("geojson_output", NULL, NULL);
	PG_ADD_TEST(suite, out_geojson_test_precision);
	PG_ADD_TEST(suite, out_geojson_test_dims);
	PG_ADD_TEST(suite, out_geojson_test_srid);
	PG_ADD_TEST(suite, out_geojson_test_bbox);
	PG_ADD_TEST(suite, out_geojson_test_geoms);
}
