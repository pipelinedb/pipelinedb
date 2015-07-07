/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
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

static void do_gml2_test(char * in, char * out, char * srs, int precision)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml2(g, srs, precision, "gml:");

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml2_test_prefix(char * in, char * out, char * srs, int precision, const char *prefix)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml2(g, srs, precision, prefix);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml3_test_opts(char * in, char * out, char * srs, int precision, int opts)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml3(g, srs, precision, opts, "gml:", NULL);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml3_test(char * in, char * out, char * srs, int precision, int is_geodetic)
{
	LWGEOM *g;
	char *h;
	int opts = LW_GML_IS_DIMS;
	if ( is_geodetic ) opts |= LW_GML_IS_DEGREE;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml3(g, srs, precision, opts, "gml:", NULL);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml3_test_prefix(char * in, char * out, char * srs, int precision, int is_geodetic, const char *prefix)
{
	LWGEOM *g;
	char *h;
	int opts = LW_GML_IS_DIMS;

	if ( is_geodetic ) opts |= LW_GML_IS_DEGREE;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml3(g, srs, precision, opts, prefix, NULL);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml3_test_nodims(char * in, char * out, char * srs, int precision, int is_geodetic, int is_dims, const char *prefix)
{
	LWGEOM *g;
	char *h;
	int opts = 0;

	if ( is_geodetic ) opts |= LW_GML_IS_DEGREE;
	if ( is_dims ) opts |= LW_GML_IS_DIMS;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml3(g, srs, precision, opts, prefix, NULL);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	lwfree(h);
}

static void do_gml2_unsupported(char * in, char * out)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_to_gml2(g, NULL, 0, "");

	if (strcmp(cu_error_msg, out))
		fprintf(stderr, "\nGML 2 - In:   %s\nOut:  %s\nTheo: %s\n",
		        in, cu_error_msg, out);
	CU_ASSERT_STRING_EQUAL(out, cu_error_msg);
	cu_error_msg_reset();

	lwfree(h);
	lwgeom_free(g);
}

static void do_gml2_extent_test(char * in, char * out, char * srs,
                                   double precision, char * prefix)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_extent_to_gml2(g, srs, precision, prefix);
	if ( ! h ) h = strdup(cu_error_msg);

	if (strcmp(h, out))
		fprintf(stderr, "\nEXT GML 2 - In:   %s\nObt: %s\nExp: %s\n",
		        in, h, out);
	CU_ASSERT_STRING_EQUAL(out, h);
	cu_error_msg_reset();

	lwfree(h);
	lwgeom_free(g);
}

static void do_gml3_extent_test(char * in, char * out, char * srs,
                                double precision, int opts, char* prefix)
{
	LWGEOM *g;
	char *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_extent_to_gml3(g, srs, precision, opts, prefix);
	if ( ! h ) h = strdup(cu_error_msg);

	if (strcmp(h, out))
		fprintf(stderr, "\nEXT GML 3 - In:   %s\nObt: %s\nExp: %s\n",
		        in, h, out);
	CU_ASSERT_STRING_EQUAL(out, h);
	cu_error_msg_reset();

	lwfree(h);
	lwgeom_free(g);
}

static void out_gml_test_precision(void)
{
	/* GML2 - 0 precision, i.e a round */
	do_gml2_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<gml:Point><gml:coordinates>1,1</gml:coordinates></gml:Point>",
	    NULL, 0);

	/* GML3 - 0 precision, i.e a round */
	do_gml3_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<gml:Point><gml:pos srsDimension=\"2\">1 1</gml:pos></gml:Point>",
	    NULL, 0, 0);


	/* GML2 - 3 digits precision */
	do_gml2_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<gml:Point><gml:coordinates>1.111,1.111</gml:coordinates></gml:Point>",
	    NULL, 3);

	/* GML3 - 3 digits precision */
	do_gml3_test(
	    "POINT(1.1111111111111 1.1111111111111)",
	    "<gml:Point><gml:pos srsDimension=\"2\">1.111 1.111</gml:pos></gml:Point>",
	    NULL, 3, 0);


	/* GML2 - 9 digits precision */
	do_gml2_test(
	    "POINT(1.2345678901234 1.2345678901234)",
	    "<gml:Point><gml:coordinates>1.23456789,1.23456789</gml:coordinates></gml:Point>",
	    NULL, 9);

	/* GML3 - 9 digits precision */
	do_gml3_test(
	    "POINT(1.2345678901234 1.2345678901234)",
	    "<gml:Point><gml:pos srsDimension=\"2\">1.23456789 1.23456789</gml:pos></gml:Point>",
	    NULL, 9, 0);


	/* GML2 - huge data */
	do_gml2_test(
	    "POINT(1E300 -1E300)",
	    "<gml:Point><gml:coordinates>1e+300,-1e+300</gml:coordinates></gml:Point>",
	    NULL, 0);

	/* GML3 - huge data */
	do_gml3_test(
	    "POINT(1E300 -1E300)",
	    "<gml:Point><gml:pos srsDimension=\"2\">1e+300 -1e+300</gml:pos></gml:Point>",
	    NULL, 0, 0);
}

static void out_gml_test_srid(void)
{
	/* GML2 - Point with SRID */
	do_gml2_test(
	    "POINT(0 1)",
	    "<gml:Point srsName=\"EPSG:4326\"><gml:coordinates>0,1</gml:coordinates></gml:Point>",
	    "EPSG:4326", 0);

	/* GML3 - Point with SRID */
	do_gml3_test(
	    "POINT(0 1)",
	    "<gml:Point srsName=\"EPSG:4326\"><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point>",
	    "EPSG:4326", 0, 0);


	/* GML2 - Linestring with SRID */
	do_gml2_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<gml:LineString srsName=\"EPSG:4326\"><gml:coordinates>0,1 2,3 4,5</gml:coordinates></gml:LineString>",
	    "EPSG:4326", 0);

	/* GML3 - Linestring with SRID */
	do_gml3_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<gml:Curve srsName=\"EPSG:4326\"><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">0 1 2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>",
	    "EPSG:4326", 0, 0);

	/* GML3 - Linestring with SRID and short tag*/
	do_gml3_test_opts(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<gml:LineString srsName=\"EPSG:4326\"><gml:posList>0 1 2 3 4 5</gml:posList></gml:LineString>",
	    "EPSG:4326", 0, LW_GML_SHORTLINE);


	/* GML2 Polygon with SRID */
	do_gml2_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<gml:Polygon srsName=\"EPSG:4326\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,1 2,3 4,5 0,1</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>",
	    "EPSG:4326", 0);

	/* GML3 Polygon with SRID */
	do_gml3_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<gml:Polygon srsName=\"EPSG:4326\"><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>",
	    "EPSG:4326", 0, 0);


	/* GML2 MultiPoint with SRID */
	do_gml2_test(
	    "MULTIPOINT(0 1,2 3)",
	    "<gml:MultiPoint srsName=\"EPSG:4326\"><gml:pointMember><gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:coordinates>2,3</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>",
	    "EPSG:4326", 0);

	/* GML3 MultiPoint with SRID */
	do_gml3_test(
	    "MULTIPOINT(0 1,2 3)",
	    "<gml:MultiPoint srsName=\"EPSG:4326\"><gml:pointMember><gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos srsDimension=\"2\">2 3</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint>",
	    "EPSG:4326", 0, 0);


	/* GML2 Multiline with SRID */
	do_gml2_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<gml:MultiLineString srsName=\"EPSG:4326\"><gml:lineStringMember><gml:LineString><gml:coordinates>0,1 2,3 4,5</gml:coordinates></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString><gml:coordinates>6,7 8,9 10,11</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>",
	    "EPSG:4326", 0);


	/* GML3 Multiline with SRID */
	do_gml3_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<gml:MultiCurve srsName=\"EPSG:4326\"><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">0 1 2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">6 7 8 9 10 11</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>",
	    "EPSG:4326", 0, 0);

	/* GML3 Multiline with SRID and LineString tag */
	do_gml3_test_opts(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<gml:MultiCurve srsName=\"EPSG:4326\"><gml:curveMember><gml:LineString><gml:posList>0 1 2 3 4 5</gml:posList></gml:LineString></gml:curveMember><gml:curveMember><gml:LineString><gml:posList>6 7 8 9 10 11</gml:posList></gml:LineString></gml:curveMember></gml:MultiCurve>",
	    "EPSG:4326", 0, LW_GML_SHORTLINE);


	/* GML2 MultiPolygon with SRID */
	do_gml2_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:MultiPolygon srsName=\"EPSG:4326\"><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,1 2,3 4,5 0,1</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>6,7 8,9 10,11 6,7</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>",
	    "EPSG:4326", 0);

	/* GML3 MultiPolygon with SRID */
	do_gml3_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:MultiSurface srsName=\"EPSG:4326\"><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>",
	    "EPSG:4326", 0, 0);

	/* GML3 PolyhedralSurface with SRID */
	do_gml3_test(
	    "POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:PolyhedralSurface srsName=\"EPSG:4326\"><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>",
	    "EPSG:4326", 0, 0);

	/* GML3 Tin with SRID */
	do_gml3_test(
	    "TIN(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:Tin srsName=\"EPSG:4326\"><gml:trianglePatches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:trianglePatches></gml:Tin>",
	    "EPSG:4326", 0, 0);


	/* GML2 GeometryCollection with SRID */
	do_gml2_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<gml:MultiGeometry srsName=\"EPSG:4326\"><gml:geometryMember><gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point></gml:geometryMember><gml:geometryMember><gml:LineString><gml:coordinates>2,3 4,5</gml:coordinates></gml:LineString></gml:geometryMember></gml:MultiGeometry>",
	    "EPSG:4326", 0);

	/* GML3 GeometryCollection with SRID */
	do_gml3_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<gml:MultiGeometry srsName=\"EPSG:4326\"><gml:geometryMember><gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember></gml:MultiGeometry>",
	    "EPSG:4326", 0, 0);
}


static void out_gml_test_geodetic(void)
{
	/* GML3 - Geodetic Point */
	do_gml3_test(
	    "POINT(0 1)",
	    "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::4326\"><gml:pos srsDimension=\"2\">1 0</gml:pos></gml:Point>",
	    "urn:ogc:def:crs:EPSG::4326", 0, 1);

	/* GML3 - 3D Geodetic Point */
	do_gml3_test(
	    "POINT(0 1 2)",
	    "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::4326\"><gml:pos srsDimension=\"3\">1 0 2</gml:pos></gml:Point>",
	    "urn:ogc:def:crs:EPSG::4326", 0, 1);
}


static void out_gml_test_dims(void)
{
	/* GML2 - 3D */
	do_gml2_test(
	    "POINT(0 1 2)",
	    "<gml:Point><gml:coordinates>0,1,2</gml:coordinates></gml:Point>",
	    NULL, 0);

	/* GML3 - 3D */
	do_gml3_test(
	    "POINT(0 1 2)",
	    "<gml:Point><gml:pos srsDimension=\"3\">0 1 2</gml:pos></gml:Point>",
	    NULL, 0, 0);


	/* GML2 - 3DM */
	do_gml2_test(
	    "POINTM(0 1 2)",
	    "<gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point>",
	    NULL, 0);

	/* GML3 - 3DM */
	do_gml3_test(
	    "POINTM(0 1 2)",
	    "<gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point>",
	    NULL, 0, 0);


	/* GML2 - 4D */
	do_gml2_test(
	    "POINT(0 1 2 3)",
	    "<gml:Point><gml:coordinates>0,1,2</gml:coordinates></gml:Point>",
	    NULL, 0);

	/* GML3 - 4D */
	do_gml3_test(
	    "POINT(0 1 2 3)",
	    "<gml:Point><gml:pos srsDimension=\"3\">0 1 2</gml:pos></gml:Point>",
	    NULL, 0, 0);
}


static void out_gml_test_geoms(void)
{
	/* GML2 - Linestring */
	do_gml2_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<gml:LineString><gml:coordinates>0,1 2,3 4,5</gml:coordinates></gml:LineString>",
	    NULL, 0);

	/* GML3 - Linestring */
	do_gml3_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">0 1 2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>",
	    NULL, 0, 0);


	/* GML2 Polygon */
	do_gml2_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,1 2,3 4,5 0,1</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>",
	    NULL, 0);

	/* GML3 Polygon */
	do_gml3_test(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>",
	    NULL, 0, 0);

	/* GML2 Polygon - with internal ring */
	do_gml2_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,1 2,3 4,5 0,1</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>6,7 8,9 10,11 6,7</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>",
	    NULL, 0);

	/* GML3 Polygon - with internal ring */
	do_gml3_test(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</gml:posList></gml:LinearRing></gml:interior></gml:Polygon>",
	    NULL, 0, 0);


	/* GML3 Triangle */
	do_gml3_test(
	    "TRIANGLE((0 1,2 3,4 5,0 1))",
	    "<gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle>",
	    NULL, 0, 0);


	/* GML2 MultiPoint */
	do_gml2_test(
	    "MULTIPOINT(0 1,2 3)",
	    "<gml:MultiPoint><gml:pointMember><gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:coordinates>2,3</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>",
	    NULL, 0);

	/* GML3 MultiPoint */
	do_gml3_test(
	    "MULTIPOINT(0 1,2 3)",
	    "<gml:MultiPoint><gml:pointMember><gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos srsDimension=\"2\">2 3</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint>",
	    NULL, 0, 0);


	/* GML2 Multiline */
	do_gml2_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<gml:MultiLineString><gml:lineStringMember><gml:LineString><gml:coordinates>0,1 2,3 4,5</gml:coordinates></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString><gml:coordinates>6,7 8,9 10,11</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>",
	    NULL, 0);

	/* GML3 Multiline */
	do_gml3_test(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">0 1 2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">6 7 8 9 10 11</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>",
	    NULL, 0, 0);


	/* GML2 MultiPolygon */
	do_gml2_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:MultiPolygon><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,1 2,3 4,5 0,1</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>6,7 8,9 10,11 6,7</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>",
	    NULL, 0);

	/* GML3 MultiPolygon */
	do_gml3_test(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>",
	    NULL, 0, 0);


	/* GML2 - GeometryCollection */
	do_gml2_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point></gml:geometryMember><gml:geometryMember><gml:LineString><gml:coordinates>2,3 4,5</gml:coordinates></gml:LineString></gml:geometryMember></gml:MultiGeometry>",
	    NULL, 0);

	/* GML3 - GeometryCollection */
	do_gml3_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember></gml:MultiGeometry>",
	    NULL, 0, 0);


	/* GML2 - Nested GeometryCollection */
	do_gml2_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:coordinates>0,1</gml:coordinates></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:LineString><gml:coordinates>2,3 4,5</gml:coordinates></gml:LineString></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>",
	    NULL, 0);

	/* GML3 - Nested GeometryCollection */
	do_gml3_test(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension=\"2\">0 1</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">2 3 4 5</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>",
	    NULL, 0, 0);

	/* GML2 - CircularString */
	do_gml2_unsupported(
	    "CIRCULARSTRING(-2 0,0 2,2 0,0 2,2 4)",
	    "lwgeom_to_gml2: 'CircularString' geometry type not supported");
	/* GML3 - CircularString */
    do_gml3_test(
                "CIRCULARSTRING(-2 0,0 2,2 0,0 2,2 4)",
                "<gml:Curve><gml:segments><gml:ArcString><gml:posList srsDimension=\"2\">-2 0 0 2 2 0 0 2 2 4</gml:posList></gml:ArcString></gml:segments></gml:Curve>",
                NULL, 0, 0 );

	/* GML2 - CompoundCurve */
	do_gml2_unsupported(
	    "COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))",
	    "lwgeom_to_gml2: 'CompoundCurve' geometry type not supported");
	/* GML3 - CompoundCurve */

    do_gml3_test(
       "COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))",
                "<gml:Curve><gml:segments><gml:ArcString><gml:posList srsDimension=\"2\">0 0 1 1 1 0</gml:posList></gml:ArcString><gml:LineStringSegment><gml:posList srsDimension=\"2\">1 0 0 1</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>",
                NULL, 0, 0 );

	/* GML2 - CurvePolygon */
	do_gml2_unsupported(
	    "CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))",
	    "lwgeom_to_gml2: 'CurvePolygon' geometry type not supported");

	/* GML3 - CurvePolygon */
    do_gml3_test(
        "CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))",
                "<gml:Polygon><gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:ArcString><gml:posList srsDimension=\"2\">-2 0 -1 -1 0 0 1 -1 2 0 0 2 -2 0</gml:posList></gml:ArcString></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension=\"2\">-1 0 0 0.5 1 0 0 1 -1 0</gml:posList></gml:LinearRing></gml:interior></gml:Polygon>",
                NULL, 1, 0 );
	do_gml3_test(
		"CURVEPOLYGON(COMPOUNDCURVE((763650.600000001 189057.100000001,7636.35 189045.199999999, 763650.548999999 189057.844000001,763650.600000001 189057.100000001)))",
				 "<gml:Polygon><gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">763650.6 189057.1 7636.35 189045.2 763650.549 189057.844 763650.6 189057.1</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior></gml:Polygon>",
				NULL, 7, 0 );

	/* GML2 - MultiCurve */
	do_gml2_unsupported(
	    "MULTICURVE((5 5,3 5,3 3,0 3),CIRCULARSTRING(0 0,2 1,2 2))",
	    "lwgeom_to_gml2: 'MultiCurve' geometry type not supported");

	/* GML3 - MultiCurve */
    do_gml3_test(
         "MULTICURVE((5 5,3 5,3 3,0 3),CIRCULARSTRING(0 0,2 1,2 2))",
                "<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension=\"2\">5 5 3 5 3 3 0 3</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:ArcString><gml:posList srsDimension=\"2\">0 0 2 1 2 2</gml:posList></gml:ArcString></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>",
                NULL, 0, 0 );

	/* GML2 - MultiSurface */
	do_gml2_unsupported(
	    "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
	    "lwgeom_to_gml2: 'MultiSurface' geometry type not supported");

	/* GML3 - MultiSurface */
    do_gml3_test(
                "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
                "<gml:MultiSurface><gml:Polygon><gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:ArcString><gml:posList srsDimension=\"2\">-2 0 -1 -1 0 0 1 -1 2 0 0 2 -2 0</gml:posList></gml:ArcString></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension=\"2\">-1 0 0 0.5 1 0 0 1 -1 0</gml:posList></gml:LinearRing></gml:interior></gml:Polygon><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">7 8 10 10 6 14 4 11 7 8</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:MultiSurface>",
                NULL, 1, 0 );

	/* GML2 - PolyhedralSurface */
	do_gml2_unsupported(
	    "POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)))",
	    "Cannot convert PolyhedralSurface to GML2. Try ST_AsGML(3, <geometry>) to generate GML3.");

	/* GML2 - Tin */
	do_gml2_unsupported(
	    "TIN(((0 1,2 3,4 5,0 1)))",
	    "Cannot convert Tin to GML2. Try ST_AsGML(3, <geometry>) to generate GML3.");
}

static void out_gml_test_geoms_prefix(void)
{
	/* GML2 - Linestring */
	do_gml2_test_prefix(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<custom:LineString><custom:coordinates>0,1 2,3 4,5</custom:coordinates></custom:LineString>",
	    NULL, 0, "custom:");

	/* GML3 - Linestring */
	do_gml3_test_prefix(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<custom:Curve><custom:segments><custom:LineStringSegment><custom:posList srsDimension=\"2\">0 1 2 3 4 5</custom:posList></custom:LineStringSegment></custom:segments></custom:Curve>",
	    NULL, 0, 0, "custom:");


	/* GML2 Polygon */
	do_gml2_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<custom:Polygon><custom:outerBoundaryIs><custom:LinearRing><custom:coordinates>0,1 2,3 4,5 0,1</custom:coordinates></custom:LinearRing></custom:outerBoundaryIs></custom:Polygon>",
	    NULL, 0, "custom:");

	/* GML3 Polygon */
	do_gml3_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<custom:Polygon><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior></custom:Polygon>",
	    NULL, 0, 0, "custom:");


	/* GML2 Polygon - with internal ring */
	do_gml2_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<custom:Polygon><custom:outerBoundaryIs><custom:LinearRing><custom:coordinates>0,1 2,3 4,5 0,1</custom:coordinates></custom:LinearRing></custom:outerBoundaryIs><custom:innerBoundaryIs><custom:LinearRing><custom:coordinates>6,7 8,9 10,11 6,7</custom:coordinates></custom:LinearRing></custom:innerBoundaryIs></custom:Polygon>",
	    NULL, 0, "custom:");

	/* GML3 Polygon - with internal ring */
	do_gml3_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<custom:Polygon><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior><custom:interior><custom:LinearRing><custom:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</custom:posList></custom:LinearRing></custom:interior></custom:Polygon>",
	    NULL, 0, 0, "custom:");

	/* GML3 Triangle */
	do_gml3_test_prefix(
	    "TRIANGLE((0 1,2 3,4 5,0 1))",
	    "<custom:Triangle><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior></custom:Triangle>",
	    NULL, 0, 0, "custom:");


	/* GML2 MultiPoint */
	do_gml2_test_prefix(
	    "MULTIPOINT(0 1,2 3)",
	    "<custom:MultiPoint><custom:pointMember><custom:Point><custom:coordinates>0,1</custom:coordinates></custom:Point></custom:pointMember><custom:pointMember><custom:Point><custom:coordinates>2,3</custom:coordinates></custom:Point></custom:pointMember></custom:MultiPoint>",
	    NULL, 0, "custom:");

	/* GML3 MultiPoint */
	do_gml3_test_prefix(
	    "MULTIPOINT(0 1,2 3)",
	    "<custom:MultiPoint><custom:pointMember><custom:Point><custom:pos srsDimension=\"2\">0 1</custom:pos></custom:Point></custom:pointMember><custom:pointMember><custom:Point><custom:pos srsDimension=\"2\">2 3</custom:pos></custom:Point></custom:pointMember></custom:MultiPoint>",
	    NULL, 0, 0, "custom:");


	/* GML2 Multiline */
	do_gml2_test_prefix(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<custom:MultiLineString><custom:lineStringMember><custom:LineString><custom:coordinates>0,1 2,3 4,5</custom:coordinates></custom:LineString></custom:lineStringMember><custom:lineStringMember><custom:LineString><custom:coordinates>6,7 8,9 10,11</custom:coordinates></custom:LineString></custom:lineStringMember></custom:MultiLineString>",
	    NULL, 0, "custom:");

	/* GML3 Multiline */
	do_gml3_test_prefix(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<custom:MultiCurve><custom:curveMember><custom:Curve><custom:segments><custom:LineStringSegment><custom:posList srsDimension=\"2\">0 1 2 3 4 5</custom:posList></custom:LineStringSegment></custom:segments></custom:Curve></custom:curveMember><custom:curveMember><custom:Curve><custom:segments><custom:LineStringSegment><custom:posList srsDimension=\"2\">6 7 8 9 10 11</custom:posList></custom:LineStringSegment></custom:segments></custom:Curve></custom:curveMember></custom:MultiCurve>",
	    NULL, 0, 0, "custom:");


	/* GML2 MultiPolygon */
	do_gml2_test_prefix(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<custom:MultiPolygon><custom:polygonMember><custom:Polygon><custom:outerBoundaryIs><custom:LinearRing><custom:coordinates>0,1 2,3 4,5 0,1</custom:coordinates></custom:LinearRing></custom:outerBoundaryIs></custom:Polygon></custom:polygonMember><custom:polygonMember><custom:Polygon><custom:outerBoundaryIs><custom:LinearRing><custom:coordinates>6,7 8,9 10,11 6,7</custom:coordinates></custom:LinearRing></custom:outerBoundaryIs></custom:Polygon></custom:polygonMember></custom:MultiPolygon>",
	    NULL, 0, "custom:");

	/* GML3 MultiPolygon */
	do_gml3_test_prefix(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<custom:MultiSurface><custom:surfaceMember><custom:Polygon><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior></custom:Polygon></custom:surfaceMember><custom:surfaceMember><custom:Polygon><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</custom:posList></custom:LinearRing></custom:exterior></custom:Polygon></custom:surfaceMember></custom:MultiSurface>",
	    NULL, 0, 0, "custom:");

	/* GML3 PolyhedralSurface */
	do_gml3_test_prefix(
	    "POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<custom:PolyhedralSurface><custom:polygonPatches><custom:PolygonPatch><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior></custom:PolygonPatch><custom:PolygonPatch><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</custom:posList></custom:LinearRing></custom:exterior></custom:PolygonPatch></custom:polygonPatches></custom:PolyhedralSurface>",
	    NULL, 0, 0, "custom:");

	/* GML3 Tin */
	do_gml3_test_prefix(
	    "TIN(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<custom:Tin><custom:trianglePatches><custom:Triangle><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">0 1 2 3 4 5 0 1</custom:posList></custom:LinearRing></custom:exterior></custom:Triangle><custom:Triangle><custom:exterior><custom:LinearRing><custom:posList srsDimension=\"2\">6 7 8 9 10 11 6 7</custom:posList></custom:LinearRing></custom:exterior></custom:Triangle></custom:trianglePatches></custom:Tin>",
	    NULL, 0, 0, "custom:");

	/* GML2 - GeometryCollection */
	do_gml2_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<custom:MultiGeometry><custom:geometryMember><custom:Point><custom:coordinates>0,1</custom:coordinates></custom:Point></custom:geometryMember><custom:geometryMember><custom:LineString><custom:coordinates>2,3 4,5</custom:coordinates></custom:LineString></custom:geometryMember></custom:MultiGeometry>",
	    NULL, 0, "custom:");

	/* GML3 - GeometryCollection */
	do_gml3_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<custom:MultiGeometry><custom:geometryMember><custom:Point><custom:pos srsDimension=\"2\">0 1</custom:pos></custom:Point></custom:geometryMember><custom:geometryMember><custom:Curve><custom:segments><custom:LineStringSegment><custom:posList srsDimension=\"2\">2 3 4 5</custom:posList></custom:LineStringSegment></custom:segments></custom:Curve></custom:geometryMember></custom:MultiGeometry>",
	    NULL, 0, 0, "custom:");

	/* GML2 - Nested GeometryCollection */
	do_gml2_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<custom:MultiGeometry><custom:geometryMember><custom:Point><custom:coordinates>0,1</custom:coordinates></custom:Point></custom:geometryMember><custom:geometryMember><custom:MultiGeometry><custom:geometryMember><custom:LineString><custom:coordinates>2,3 4,5</custom:coordinates></custom:LineString></custom:geometryMember></custom:MultiGeometry></custom:geometryMember></custom:MultiGeometry>",
	    NULL, 0, "custom:");

	/* GML3 - Nested GeometryCollection */
	do_gml3_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<custom:MultiGeometry><custom:geometryMember><custom:Point><custom:pos srsDimension=\"2\">0 1</custom:pos></custom:Point></custom:geometryMember><custom:geometryMember><custom:MultiGeometry><custom:geometryMember><custom:Curve><custom:segments><custom:LineStringSegment><custom:posList srsDimension=\"2\">2 3 4 5</custom:posList></custom:LineStringSegment></custom:segments></custom:Curve></custom:geometryMember></custom:MultiGeometry></custom:geometryMember></custom:MultiGeometry>",
	    NULL, 0, 0, "custom:");

	/*------------- empty prefixes below ------------------------ */

	/* GML2 - Linestring */
	do_gml2_test_prefix(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<LineString><coordinates>0,1 2,3 4,5</coordinates></LineString>",
	    NULL, 0, "");

	/* GML3 - Linestring */
	do_gml3_test_prefix(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<Curve><segments><LineStringSegment><posList srsDimension=\"2\">0 1 2 3 4 5</posList></LineStringSegment></segments></Curve>",
	    NULL, 0, 0, "");


	/* GML2 Polygon */
	do_gml2_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs></Polygon>",
	    NULL, 0, "");

	/* GML3 Polygon */
	do_gml3_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<Polygon><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Polygon>",
	    NULL, 0, 0, "");


	/* GML2 Polygon - with internal ring */
	do_gml2_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs><innerBoundaryIs><LinearRing><coordinates>6,7 8,9 10,11 6,7</coordinates></LinearRing></innerBoundaryIs></Polygon>",
	    NULL, 0, "");

	/* GML3 Polygon - with internal ring */
	do_gml3_test_prefix(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<Polygon><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior><interior><LinearRing><posList srsDimension=\"2\">6 7 8 9 10 11 6 7</posList></LinearRing></interior></Polygon>",
	    NULL, 0, 0, "");

	/* GML3 Triangle */
	do_gml3_test_prefix(
	    "TRIANGLE((0 1,2 3,4 5,0 1))",
	    "<Triangle><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Triangle>",
	    NULL, 0, 0, "");


	/* GML2 MultiPoint */
	do_gml2_test_prefix(
	    "MULTIPOINT(0 1,2 3)",
	    "<MultiPoint><pointMember><Point><coordinates>0,1</coordinates></Point></pointMember><pointMember><Point><coordinates>2,3</coordinates></Point></pointMember></MultiPoint>",
	    NULL, 0, "");

	/* GML3 MultiPoint */
	do_gml3_test_prefix(
	    "MULTIPOINT(0 1,2 3)",
	    "<MultiPoint><pointMember><Point><pos srsDimension=\"2\">0 1</pos></Point></pointMember><pointMember><Point><pos srsDimension=\"2\">2 3</pos></Point></pointMember></MultiPoint>",
	    NULL, 0, 0, "");


	/* GML2 Multiline */
	do_gml2_test_prefix(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<MultiLineString><lineStringMember><LineString><coordinates>0,1 2,3 4,5</coordinates></LineString></lineStringMember><lineStringMember><LineString><coordinates>6,7 8,9 10,11</coordinates></LineString></lineStringMember></MultiLineString>",
	    NULL, 0, "");

	/* GML3 Multiline */
	do_gml3_test_prefix(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<MultiCurve><curveMember><Curve><segments><LineStringSegment><posList srsDimension=\"2\">0 1 2 3 4 5</posList></LineStringSegment></segments></Curve></curveMember><curveMember><Curve><segments><LineStringSegment><posList srsDimension=\"2\">6 7 8 9 10 11</posList></LineStringSegment></segments></Curve></curveMember></MultiCurve>",
	    NULL, 0, 0, "");


	/* GML2 MultiPolygon */
	do_gml2_test_prefix(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<MultiPolygon><polygonMember><Polygon><outerBoundaryIs><LinearRing><coordinates>0,1 2,3 4,5 0,1</coordinates></LinearRing></outerBoundaryIs></Polygon></polygonMember><polygonMember><Polygon><outerBoundaryIs><LinearRing><coordinates>6,7 8,9 10,11 6,7</coordinates></LinearRing></outerBoundaryIs></Polygon></polygonMember></MultiPolygon>",
	    NULL, 0, "");

	/* GML3 MultiPolygon */
	do_gml3_test_prefix(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<MultiSurface><surfaceMember><Polygon><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Polygon></surfaceMember><surfaceMember><Polygon><exterior><LinearRing><posList srsDimension=\"2\">6 7 8 9 10 11 6 7</posList></LinearRing></exterior></Polygon></surfaceMember></MultiSurface>",
	    NULL, 0, 0, "");

	/* GML3 PolyhedralSurface */
	do_gml3_test_prefix(
	    "POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<PolyhedralSurface><polygonPatches><PolygonPatch><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior></PolygonPatch><PolygonPatch><exterior><LinearRing><posList srsDimension=\"2\">6 7 8 9 10 11 6 7</posList></LinearRing></exterior></PolygonPatch></polygonPatches></PolyhedralSurface>",
	    NULL, 0, 0, "");

	/* GML3 PolyhedralSurface */
	do_gml3_test_prefix(
	    "TIN(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<Tin><trianglePatches><Triangle><exterior><LinearRing><posList srsDimension=\"2\">0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Triangle><Triangle><exterior><LinearRing><posList srsDimension=\"2\">6 7 8 9 10 11 6 7</posList></LinearRing></exterior></Triangle></trianglePatches></Tin>",
	    NULL, 0, 0, "");

	/* GML2 - GeometryCollection */
	do_gml2_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<MultiGeometry><geometryMember><Point><coordinates>0,1</coordinates></Point></geometryMember><geometryMember><LineString><coordinates>2,3 4,5</coordinates></LineString></geometryMember></MultiGeometry>",
	    NULL, 0, "");

	/* GML3 - GeometryCollection */
	do_gml3_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<MultiGeometry><geometryMember><Point><pos srsDimension=\"2\">0 1</pos></Point></geometryMember><geometryMember><Curve><segments><LineStringSegment><posList srsDimension=\"2\">2 3 4 5</posList></LineStringSegment></segments></Curve></geometryMember></MultiGeometry>",
	    NULL, 0, 0, "");

	/* GML2 - Nested GeometryCollection */
	do_gml2_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<MultiGeometry><geometryMember><Point><coordinates>0,1</coordinates></Point></geometryMember><geometryMember><MultiGeometry><geometryMember><LineString><coordinates>2,3 4,5</coordinates></LineString></geometryMember></MultiGeometry></geometryMember></MultiGeometry>",
	    NULL, 0, "");

	/* GML3 - Nested GeometryCollection */
	do_gml3_test_prefix(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<MultiGeometry><geometryMember><Point><pos srsDimension=\"2\">0 1</pos></Point></geometryMember><geometryMember><MultiGeometry><geometryMember><Curve><segments><LineStringSegment><posList srsDimension=\"2\">2 3 4 5</posList></LineStringSegment></segments></Curve></geometryMember></MultiGeometry></geometryMember></MultiGeometry>",
	    NULL, 0, 0, "");



}


static void out_gml_test_geoms_nodims(void)
{
	/* GML3 - Linestring */
	do_gml3_test_nodims(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<Curve><segments><LineStringSegment><posList>0 1 2 3 4 5</posList></LineStringSegment></segments></Curve>",
	    NULL, 0, 0, 0, "");


	/* GML3 Polygon */
	do_gml3_test_nodims(
	    "POLYGON((0 1,2 3,4 5,0 1))",
	    "<Polygon><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Polygon>",
	    NULL, 0, 0, 0, "");


	/* GML3 Polygon - with internal ring */
	do_gml3_test_nodims(
	    "POLYGON((0 1,2 3,4 5,0 1),(6 7,8 9,10 11,6 7))",
	    "<Polygon><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior><interior><LinearRing><posList>6 7 8 9 10 11 6 7</posList></LinearRing></interior></Polygon>",
	    NULL, 0, 0, 0, "");

	/* GML3 Triangle */
	do_gml3_test_nodims(
	    "TRIANGLE((0 1,2 3,4 5,0 1))",
	    "<Triangle><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Triangle>",
	    NULL, 0, 0, 0, "");


	/* GML3 MultiPoint */
	do_gml3_test_nodims(
	    "MULTIPOINT(0 1,2 3)",
	    "<MultiPoint><pointMember><Point><pos>0 1</pos></Point></pointMember><pointMember><Point><pos>2 3</pos></Point></pointMember></MultiPoint>",
	    NULL, 0, 0, 0, "");


	/* GML3 Multiline */
	do_gml3_test_nodims(
	    "MULTILINESTRING((0 1,2 3,4 5),(6 7,8 9,10 11))",
	    "<MultiCurve><curveMember><Curve><segments><LineStringSegment><posList>0 1 2 3 4 5</posList></LineStringSegment></segments></Curve></curveMember><curveMember><Curve><segments><LineStringSegment><posList>6 7 8 9 10 11</posList></LineStringSegment></segments></Curve></curveMember></MultiCurve>",
	    NULL, 0, 0, 0, "");


	/* GML3 MultiPolygon */
	do_gml3_test_nodims(
	    "MULTIPOLYGON(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<MultiSurface><surfaceMember><Polygon><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Polygon></surfaceMember><surfaceMember><Polygon><exterior><LinearRing><posList>6 7 8 9 10 11 6 7</posList></LinearRing></exterior></Polygon></surfaceMember></MultiSurface>",
	    NULL, 0, 0, 0, "");

	/* GML3 PolyhedralSurface */
	do_gml3_test_nodims(
	    "POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<PolyhedralSurface><polygonPatches><PolygonPatch><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior></PolygonPatch><PolygonPatch><exterior><LinearRing><posList>6 7 8 9 10 11 6 7</posList></LinearRing></exterior></PolygonPatch></polygonPatches></PolyhedralSurface>",
	    NULL, 0, 0, 0, "");

	/* GML3 Tin */
	do_gml3_test_nodims(
	    "TIN(((0 1,2 3,4 5,0 1)),((6 7,8 9,10 11,6 7)))",
	    "<Tin><trianglePatches><Triangle><exterior><LinearRing><posList>0 1 2 3 4 5 0 1</posList></LinearRing></exterior></Triangle><Triangle><exterior><LinearRing><posList>6 7 8 9 10 11 6 7</posList></LinearRing></exterior></Triangle></trianglePatches></Tin>",
	    NULL, 0, 0, 0, "");

	/* GML3 - GeometryCollection */
	do_gml3_test_nodims(
	    "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(2 3,4 5))",
	    "<MultiGeometry><geometryMember><Point><pos>0 1</pos></Point></geometryMember><geometryMember><Curve><segments><LineStringSegment><posList>2 3 4 5</posList></LineStringSegment></segments></Curve></geometryMember></MultiGeometry>",
	    NULL, 0, 0, 0, "");

	/* GML3 - Nested GeometryCollection */
	do_gml3_test_nodims(
	    "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(LINESTRING(2 3,4 5)))",
	    "<MultiGeometry><geometryMember><Point><pos>0 1</pos></Point></geometryMember><geometryMember><MultiGeometry><geometryMember><Curve><segments><LineStringSegment><posList>2 3 4 5</posList></LineStringSegment></segments></Curve></geometryMember></MultiGeometry></geometryMember></MultiGeometry>",
	    NULL, 0, 0, 0, "");
}

static void out_gml2_extent(void)
{
	/* GML2: Point */
	do_gml2_extent_test(
	    "POINT(-15 60)",
	    "<Box><coordinates>-15,60 -15,60</coordinates></Box>",
	    NULL, 15, "");
	do_gml2_extent_test(
	    "POINT(-15 60)",
	    "<gml:Box><gml:coordinates>-15,60 -15,60</gml:coordinates></gml:Box>",
	    NULL, 15, "gml:");
	do_gml2_extent_test(
	    "POINT(-15 60)",
	    "<Box srsName=\"urn:ogc:def:crs:EPSG::4326\"><coordinates>-15,60 -15,60</coordinates></Box>",
	    "urn:ogc:def:crs:EPSG::4326", 15, "");

	/* GML2: Multipoint */
	do_gml2_extent_test(
	    "MULTIPOINT(2 3, -5 -6)",
	    "<Box><coordinates>-5,-6 2,3</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: Linestring */
	do_gml2_extent_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<Box><coordinates>0,1 4,5</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: MultiLinestring */
	do_gml2_extent_test(
	    "MULTILINESTRING((0 1,2 3),(4 5, 10 6))",
	    "<Box><coordinates>0,1 10,6</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: Polygon */
	do_gml2_extent_test(
	    "POLYGON((1 7,7 14, 14 7, 1 7))",
	    "<Box><coordinates>1,7 14,14</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: MultiPolygon */
	do_gml2_extent_test(
	    "MULTIPOLYGON(((1 7,7 14, 14 7, 1 7)),((-4 -6, -15 3, 0 0, -4 -6)))",
	    "<Box><coordinates>-15,-6 14,14</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: MultiSurface */
	do_gml2_extent_test(
	    "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
	    "<Box><coordinates>-2,-1 10,14</coordinates></Box>",
	    NULL, 15, "");

	/* GML2: empty */
	do_gml2_extent_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "<Box/>",
	    NULL, 15, "");

	/* GML2: empty with srsName */
	do_gml2_extent_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "<Box srsName=\"urn:ogc:def:crs:EPSG::4326\"/>",
	    "urn:ogc:def:crs:EPSG::4326", 15, "");

}

static void out_gml3_extent(void)
{
	/* GML3: Point */
	do_gml3_extent_test(
	    "POINT(-15 60)",
	    "<Envelope><lowerCorner>-15 60</lowerCorner><upperCorner>-15 60</upperCorner></Envelope>",
	    NULL, 15, 0, "");
	do_gml3_extent_test(
	    "POINT(-15 60)",
	    "<gml:Envelope><gml:lowerCorner>-15 60</gml:lowerCorner><gml:upperCorner>-15 60</gml:upperCorner></gml:Envelope>",
	    NULL, 15, 0, "gml:");
	do_gml3_extent_test(
	    "POINT(-15 60)",
	    "<Envelope srsName=\"urn:ogc:def:crs:EPSG::4326\"><lowerCorner>-15 60</lowerCorner><upperCorner>-15 60</upperCorner></Envelope>",
	    "urn:ogc:def:crs:EPSG::4326", 15, 0, "");

	/* GML3: Multipoint */
	do_gml3_extent_test(
	    "MULTIPOINT(2 3, -5 -6)",
	    "<Envelope><lowerCorner>-5 -6</lowerCorner><upperCorner>2 3</upperCorner></Envelope>",
	    NULL, 15, 0, "");

	/* GML3: Linestring */
	do_gml3_extent_test(
	    "LINESTRING(0 1,2 3,4 5)",
	    "<Envelope><lowerCorner>0 1</lowerCorner><upperCorner>4 5</upperCorner></Envelope>",
	    NULL, 15, 0, "");

	/* GML3: MultiLinestring */
	do_gml3_extent_test(
	    "MULTILINESTRING((0 1,2 3),(4 5, 10 6))",
	    "<Envelope><lowerCorner>0 1</lowerCorner><upperCorner>10 6</upperCorner></Envelope>",
	    NULL, 15, 0, "");
	do_gml3_extent_test(
	    "MULTILINESTRING((0 1,2 3),(4 5, 10 6))",
	    "<Envelope><lowerCorner>1 0</lowerCorner><upperCorner>6 10</upperCorner></Envelope>",
	    NULL, 15, LW_GML_IS_DEGREE, "");
	do_gml3_extent_test(
	    "MULTILINESTRING((0 1,2 3),(4 5, 10 6))",
	    "<Envelope srsDimension=\"2\"><lowerCorner>1 0</lowerCorner><upperCorner>6 10</upperCorner></Envelope>",
	    NULL, 15, LW_GML_IS_DEGREE|LW_GML_IS_DIMS, "");
	do_gml3_extent_test(
	    "MULTILINESTRING((0 1 10,2 3 30),(4 5 50, 10 6 -70))",
	    "<Envelope srsDimension=\"3\"><lowerCorner>1 0 -70</lowerCorner><upperCorner>6 10 50</upperCorner></Envelope>",
	    NULL, 15, LW_GML_IS_DEGREE|LW_GML_IS_DIMS, "");

	/* GML3: Polygon */
	do_gml3_extent_test(
	    "POLYGON((1 7,7 14, 14 7, 1 7))",
	    "<Envelope><lowerCorner>1 7</lowerCorner><upperCorner>14 14</upperCorner></Envelope>",
	    NULL, 15, 0, "");

	/* GML3: MultiPolygon */
	do_gml3_extent_test(
	    "MULTIPOLYGON(((1 7,7 14, 14 7, 1 7)),((-4 -6, -15 3, 0 0, -4 -6)))",
	    "<Envelope><lowerCorner>-15 -6</lowerCorner><upperCorner>14 14</upperCorner></Envelope>",
	    NULL, 15, 0, "");

	/* GML3: MultiSurface */
	do_gml3_extent_test(
	    "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))",
	    "<Envelope><lowerCorner>-2 -1</lowerCorner><upperCorner>10 14</upperCorner></Envelope>",
	    NULL, 15, 0, "");

	/* GML3: empty */
	do_gml3_extent_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "<Envelope/>",
	    NULL, 15, 0, "");

	/* GML3: empty with srsName */
	do_gml3_extent_test(
	    "GEOMETRYCOLLECTION EMPTY",
	    "<Envelope srsName=\"urn:ogc:def:crs:EPSG::4326\"/>",
	    "urn:ogc:def:crs:EPSG::4326", 15, 0, "");

}

/*
** Used by test harness to register the tests in this file.
*/
void out_gml_suite_setup(void);
void out_gml_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("gml_output", NULL, NULL);
	PG_ADD_TEST(suite, out_gml_test_precision);
	PG_ADD_TEST(suite, out_gml_test_srid);
	PG_ADD_TEST(suite, out_gml_test_dims);
	PG_ADD_TEST(suite, out_gml_test_geodetic);
	PG_ADD_TEST(suite, out_gml_test_geoms);
	PG_ADD_TEST(suite, out_gml_test_geoms_prefix);
	PG_ADD_TEST(suite, out_gml_test_geoms_nodims);
	PG_ADD_TEST(suite, out_gml2_extent);
	PG_ADD_TEST(suite, out_gml3_extent);
}
