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

static void do_kml_test(char * in, char * out, int precision)
{
	LWGEOM *g;
	char * h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_kml2(g, precision, "");

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}


static void do_kml_unsupported(char * in, char * out)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_kml2(g, 0, "");

	if (strcmp(cu_error_msg, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n",
		        in, cu_error_msg, out);

	CU_ASSERT_STRING_EQUAL(out, cu_error_msg);
	cu_error_msg_reset();

	lwfree(h);
	lwgeom_free(g);
}


static void do_kml_test_prefix(char * in, char * out, int precision, const char *prefix)
{
	LWGEOM *g;
	char * h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_kml2(g, precision, prefix);

	if (strcmp(h, out))
		fprintf(stderr, "\nPrefix: %s\nIn:   %s\nOut:  %s\nTheo: %s\n",
		        prefix, in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}


static void out_kml_test_precision(void)
{
	/* 0 precision, i.e a round */
	do_kml_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<Point><coordinates>1,1</coordinates></Point>",
	    0);

	/* 3 digits precision */
	do_kml_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<Point><coordinates>1.111,1.111</coordinates></Point>",
	    3);

	/* 9 digits precision */
	do_kml_test(
	    "POINT(1.2345678901234 1.2345678901234)",
	    "<Point><coordinates>1.23456789,1.23456789</coordinates></Point>",
	    8);

	/* huge data */
	do_kml_test(
	    "POINT(1E300 -1E300)",
	    "<Point><coordinates>1e+300,-1e+300</coordinates></Point>",
	    0);
}


static void out_kml_test_dims(void)
{
	/* 3D */
	do_kml_test(
	    "POINT(0 1 2)",
	    "<Point><coordinates>0,1,2</coordinates></Point>",
	    0);

	/* 3DM */
	do_kml_test(
	    "POINTM(0 1 2)",
	    "<Point><coordinates>0,1</coordinates></Point>",
	    0);

	/* 4D */
	do_kml_test(
	    "POINT(0 1 2 3)",
	    "<Point><coordinates>0,1,2</coordinates></Point>",
	    0);
}


static void out_kml_test_geoms(void)
{
	/* Linestring */
	do_kml_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<LineString><coordinates>0,1 2,3 4,5</coordinates></LineString>",
	    0);

	/* Polygon */
	do_kml_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs></Polygon>",
	    0);

	/* Polygon - with internal ring */
	do_kml_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs><innerBoundaryIs><LinearRing><coordinates>6,7 8,9 10,11 6,7</coordinates></LinearRing></innerBoundaryIs></Polygon>",
	    0);

	/* MultiPoint */
	do_kml_test(
	    "MULTIPOINT(0 1,2 3)",
	    "<MultiGeometry><Point><coordinates>0,1</coordinates></Point><Point><coordinates>2,3</coordinates></Point></MultiGeometry>",
	    0);

	/* MultiLine */
	do_kml_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<MultiGeometry><LineString><coordinates>0,1 2,3 4,5</coordinates></LineString><LineString><coordinates>6,7 8,9 10,11</coordinates></LineString></MultiGeometry>",
	    0);

	/* MultiPolygon */
	do_kml_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<MultiGeometry><Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs></Polygon><Polygon><outerBoundaryIs><LinearRing><coordinates>6,7 8,9 10,11 6,7</coordinates></LinearRing></outerBoundaryIs></Polygon></MultiGeometry>",
	    0);

	/* GeometryCollection */
	do_kml_unsupported(
	    "GEOMETRYCOLLECTION(POINT(0 1))",
	    "lwgeom_to_kml2: 'GeometryCollection' geometry type not supported");

	/* CircularString */
	do_kml_unsupported(
	    "CIRCULARSTRING(-2 0,0 2,2 0,0 2,2 4)",
	    "lwgeom_to_kml2: 'CircularString' geometry type not supported");

	/* CompoundCurve */
	do_kml_unsupported(
	    "COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))",
	    "lwgeom_to_kml2: 'CompoundCurve' geometry type not supported");

	/* CurvePolygon */
	do_kml_unsupported(
	    "CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))",
	    "lwgeom_to_kml2: 'CurvePolygon' geometry type not supported");

	/* MultiCurve */
	do_kml_unsupported(
	    "MULTICURVE((5 5,3 5,3 3,0 3),CIRCULARSTRING(0 0,2 1,2 2))",
	    "lwgeom_to_kml2: 'MultiCurve' geometry type not supported");

	/* MultiSurface */
	do_kml_unsupported(
	    "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
	    "lwgeom_to_kml2: 'MultiSurface' geometry type not supported");
}

static void out_kml_test_prefix(void)
{
	/* Linestring */
	do_kml_test_prefix(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<kml:LineString><kml:coordinates>0,1 2,3 4,5</kml:coordinates></kml:LineString>",
	    0, "kml:");

	/* Polygon */
	do_kml_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>0,1 2,3 4,5 0,1</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>",
	    0, "kml:");

	/* Polygon - with internal ring */
	do_kml_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>0,1 2,3 4,5 0,1</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>6,7 8,9 10,11 6,7</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>",
	    0, "kml:");

	/* MultiPoint */
	do_kml_test_prefix(
	    "MULTIPOINT(0 1,2 3)",
	    "<kml:MultiGeometry><kml:Point><kml:coordinates>0,1</kml:coordinates></kml:Point><kml:Point><kml:coordinates>2,3</kml:coordinates></kml:Point></kml:MultiGeometry>",
	    0, "kml:");

	/* MultiLine */
	do_kml_test_prefix(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<kml:MultiGeometry><kml:LineString><kml:coordinates>0,1 2,3 4,5</kml:coordinates></kml:LineString><kml:LineString><kml:coordinates>6,7 8,9 10,11</kml:coordinates></kml:LineString></kml:MultiGeometry>",
	    0, "kml:");

	/* MultiPolygon */
	do_kml_test_prefix(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<kml:MultiGeometry><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>0,1 2,3 4,5 0,1</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>6,7 8,9 10,11 6,7</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry>",
	    0, "kml:");

}
/*
** Used by test harness to register the tests in this file.
*/
void out_kml_suite_setup(void);
void out_kml_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("kml_output", NULL, NULL);
	PG_ADD_TEST(suite, out_kml_test_precision);
	PG_ADD_TEST(suite, out_kml_test_dims);
	PG_ADD_TEST(suite, out_kml_test_geoms);
	PG_ADD_TEST(suite, out_kml_test_prefix);
}
