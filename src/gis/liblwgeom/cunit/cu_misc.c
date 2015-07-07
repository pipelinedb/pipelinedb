/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2008 Paul Ramsey
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


static void test_misc_force_2d(void)
{
	LWGEOM *geom;
	LWGEOM *geom2d;
	char *wkt_out;

	geom = lwgeom_from_wkt("CIRCULARSTRINGM(-5 0 4,0 5 3,5 0 2,10 -5 1,15 0 0)", LW_PARSER_CHECK_NONE);
	geom2d = lwgeom_force_2d(geom);
	wkt_out = lwgeom_to_ewkt(geom2d);
	CU_ASSERT_STRING_EQUAL("CIRCULARSTRING(-5 0,0 5,5 0,10 -5,15 0)",wkt_out);
	lwgeom_free(geom);
	lwgeom_free(geom2d);
	lwfree(wkt_out);

	geom = lwgeom_from_wkt("GEOMETRYCOLLECTION(POINT(0 0 0),LINESTRING(1 1 1,2 2 2),POLYGON((0 0 1,0 1 1,1 1 1,1 0 1,0 0 1)),CURVEPOLYGON(CIRCULARSTRING(0 0 0,1 1 1,2 2 2,1 1 1,0 0 0)))", LW_PARSER_CHECK_NONE);
	geom2d = lwgeom_force_2d(geom);
	wkt_out = lwgeom_to_ewkt(geom2d);
	CU_ASSERT_STRING_EQUAL("GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2),POLYGON((0 0,0 1,1 1,1 0,0 0)),CURVEPOLYGON(CIRCULARSTRING(0 0,1 1,2 2,1 1,0 0)))",wkt_out);
	lwgeom_free(geom);
	lwgeom_free(geom2d);
	lwfree(wkt_out);
}

static void test_misc_simplify(void)
{
	LWGEOM *geom;
	LWGEOM *geom2d;
	char *wkt_out;

	geom = lwgeom_from_wkt("LINESTRING(0 0,0 10,0 51,50 20,30 20,7 32)", LW_PARSER_CHECK_NONE);
	geom2d = lwgeom_simplify(geom,2);
	wkt_out = lwgeom_to_ewkt(geom2d);
	CU_ASSERT_STRING_EQUAL("LINESTRING(0 0,0 51,50 20,30 20,7 32)",wkt_out);
	lwgeom_free(geom);
	lwgeom_free(geom2d);
	lwfree(wkt_out);

	geom = lwgeom_from_wkt("MULTILINESTRING((0 0,0 10,0 51,50 20,30 20,7 32))", LW_PARSER_CHECK_NONE);
	geom2d = lwgeom_simplify(geom,2);
	wkt_out = lwgeom_to_ewkt(geom2d);
	CU_ASSERT_STRING_EQUAL("MULTILINESTRING((0 0,0 51,50 20,30 20,7 32))",wkt_out);
	lwgeom_free(geom);
	lwgeom_free(geom2d);
	lwfree(wkt_out);
}

static void test_misc_count_vertices(void)
{
	LWGEOM *geom;
	int count;

	geom = lwgeom_from_wkt("GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1),POLYGON((0 0,0 1,1 0,0 0)),CIRCULARSTRING(0 0,0 1,1 1),CURVEPOLYGON(CIRCULARSTRING(0 0,0 1,1 1)))", LW_PARSER_CHECK_NONE);
	count = lwgeom_count_vertices(geom);
	CU_ASSERT_EQUAL(count,13);
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("GEOMETRYCOLLECTION(CIRCULARSTRING(0 0,0 1,1 1),POINT(0 0),CURVEPOLYGON(CIRCULARSTRING(0 0,0 1,1 1,1 0,0 0)))", LW_PARSER_CHECK_NONE);
	count = lwgeom_count_vertices(geom);
	CU_ASSERT_EQUAL(count,9);
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("CURVEPOLYGON((0 0,1 0,0 1,0 0),CIRCULARSTRING(0 0,1 0,1 1,1 0,0 0))", LW_PARSER_CHECK_NONE);
	count = lwgeom_count_vertices(geom);
	CU_ASSERT_EQUAL(count,9);
	lwgeom_free(geom);


	geom = lwgeom_from_wkt("POLYGON((0 0,1 0,0 1,0 0))", LW_PARSER_CHECK_NONE);
	count = lwgeom_count_vertices(geom);
	CU_ASSERT_EQUAL(count,4);
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("CURVEPOLYGON((0 0,1 0,0 1,0 0),CIRCULARSTRING(0 0,1 0,1 1,1 0,0 0))", LW_PARSER_CHECK_NONE);
	count = lwgeom_count_vertices(geom);
	CU_ASSERT_EQUAL(count,9);
	lwgeom_free(geom);	
}

static void test_misc_area(void)
{
	LWGEOM *geom;
	double area;

	geom = lwgeom_from_wkt("LINESTRING EMPTY", LW_PARSER_CHECK_ALL);
	area = lwgeom_area(geom);
	CU_ASSERT_DOUBLE_EQUAL(area, 0.0, 0.0001);	
	lwgeom_free(geom);
}

static void test_misc_wkb(void)
{
	static char *wkb = "010A0000000200000001080000000700000000000000000000C00000000000000000000000000000F0BF000000000000F0BF00000000000000000000000000000000000000000000F03F000000000000F0BF000000000000004000000000000000000000000000000000000000000000004000000000000000C00000000000000000010200000005000000000000000000F0BF00000000000000000000000000000000000000000000E03F000000000000F03F00000000000000000000000000000000000000000000F03F000000000000F0BF0000000000000000";
	LWGEOM *geom = lwgeom_from_hexwkb(wkb, LW_PARSER_CHECK_ALL);
	char *str = lwgeom_to_wkt(geom, WKB_ISO, 8, 0);
	CU_ASSERT_STRING_EQUAL(str, "CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))");
	lwfree(str);
	lwgeom_free(geom);
		
}


static void test_grid(void)
{
	gridspec grid;
	static char *wkt = "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))";
	LWGEOM *geom = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_ALL);
	LWGEOM *geomgrid;
	char *str;
	
	grid.ipx = grid.ipy = 0;
	grid.xsize = grid.ysize = 20;

	geomgrid = lwgeom_grid(geom, &grid);
	str = lwgeom_to_ewkt(geomgrid);
	CU_ASSERT_STRING_EQUAL(str, "MULTIPOLYGON EMPTY");
	lwfree(str);
	lwgeom_free(geom);
	lwgeom_free(geomgrid);		
}

static void test_rect_count(void)
{
	GBOX box;
	int n;
	static char *wkt = "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))";
	LWGEOM *geom = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_ALL);

	box.xmin = -5;  box.ymin = -5;
	box.xmax =  5;  box.ymax =  5;
	n = lwgeom_npoints_in_rect(geom, &box);
	CU_ASSERT_EQUAL(2, n);

	box.xmin = -5;  box.ymin = -5;
	box.xmax = 15;  box.ymax = 15; 
	n = lwgeom_npoints_in_rect(geom, &box);
	CU_ASSERT_EQUAL(5, n);
}


/*
** Used by the test harness to register the tests in this file.
*/
void misc_suite_setup(void);
void misc_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("miscellaneous", NULL, NULL);
	PG_ADD_TEST(suite, test_misc_force_2d);
	PG_ADD_TEST(suite, test_misc_simplify);
	PG_ADD_TEST(suite, test_misc_count_vertices);
	PG_ADD_TEST(suite, test_misc_area);
	PG_ADD_TEST(suite, test_misc_wkb);
	PG_ADD_TEST(suite, test_grid);
	PG_ADD_TEST(suite, test_rect_count);
}
