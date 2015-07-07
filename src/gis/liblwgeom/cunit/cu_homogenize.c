/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2009 Olivier Courtin <olivier.courtin@oslandia.com>
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

static void do_geom_test(char * in, char * out)
{
	LWGEOM *g, *h;
	char *tmp;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_homogenize(g);
	tmp = lwgeom_to_ewkt(h);
	if (strcmp(tmp, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nExp:  %s\n",
		        in, tmp, out);
	CU_ASSERT_STRING_EQUAL(tmp, out);
	lwfree(tmp);
	lwgeom_free(g);
	/* See http://trac.osgeo.org/postgis/ticket/1104 */
	lwgeom_free(h);
}


static void test_coll_point(void)
{
	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2))",
	             "POINT(1 2)");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4))",
	             "MULTIPOINT(1 2,3 4)");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4),POINT(5 6))",
	             "MULTIPOINT(1 2,3 4,5 6)");

	do_geom_test("GEOMETRYCOLLECTION(MULTIPOINT(1 2,3 4),POINT(5 6))",
	             "MULTIPOINT(1 2,3 4,5 6)");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),MULTIPOINT(3 4,5 6))",
	             "MULTIPOINT(1 2,3 4,5 6)");

	do_geom_test("GEOMETRYCOLLECTION(MULTIPOINT(1 2,3 4),MULTIPOINT(5 6,7 8))",
	             "MULTIPOINT(1 2,3 4,5 6,7 8)");
}


static void test_coll_line(void)
{
	do_geom_test("GEOMETRYCOLLECTION(LINESTRING(1 2,3 4))",
	             "LINESTRING(1 2,3 4)");

	do_geom_test("GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),LINESTRING(5 6,7 8))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8))");

	do_geom_test("GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),LINESTRING(5 6,7 8),LINESTRING(9 10,11 12))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8),(9 10,11 12))");

	do_geom_test("GEOMETRYCOLLECTION(MULTILINESTRING((1 2,3 4),(5 6,7 8)),LINESTRING(9 10,11 12))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8),(9 10,11 12))");

	do_geom_test("GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),MULTILINESTRING((5 6,7 8),(9 10,11 12)))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8),(9 10,11 12))");

	do_geom_test("GEOMETRYCOLLECTION(MULTILINESTRING((1 2,3 4),(5 6,7 8)),MULTILINESTRING((9 10,11 12),(13 14,15 16)))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8),(9 10,11 12),(13 14,15 16))");
}


static void test_coll_poly(void)
{
	do_geom_test("GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)))",
	             "POLYGON((1 2,3 4,5 6,1 2))");

	do_geom_test("GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))");

	do_geom_test("GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)),POLYGON((13 14,15 16,17 18,13 14)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))");

	do_geom_test("GEOMETRYCOLLECTION(MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8))),POLYGON((13 14,15 16,17 18,13 14)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))");

	do_geom_test("GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),MULTIPOLYGON(((7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14))))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))");

	do_geom_test("GEOMETRYCOLLECTION(MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8))),MULTIPOLYGON(((13 14,15 16,17 18,13 14)),((19 20,21 22,23 24,19 20))))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)),((19 20,21 22,23 24,19 20)))");
}


static void test_coll_coll(void)
{
	/* Two different types together must produce a Collection as output */
	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
	             "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");

	do_geom_test("GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),POLYGON((5 6,7 8,9 10,5 6)))",
	             "GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),POLYGON((5 6,7 8,9 10,5 6)))");


	/* Ability to produce a single MULTI with same type */
	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6),POINT(7 8))",
	             "GEOMETRYCOLLECTION(MULTIPOINT(1 2,7 8),LINESTRING(3 4,5 6))");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6),MULTIPOINT(7 8,9 10))",
	             "GEOMETRYCOLLECTION(MULTIPOINT(1 2,7 8,9 10),LINESTRING(3 4,5 6))");


	/* Recursive Collection handle */
	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2))))",
	             "POINT(1 2)");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION(LINESTRING(3 4,5 6)))",
	             "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");


	/* EMPTY Collection */
	do_geom_test("GEOMETRYCOLLECTION EMPTY",
	             "GEOMETRYCOLLECTION EMPTY");


	/* Recursive EMPTY Collection */
	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)",
	             "GEOMETRYCOLLECTION EMPTY");

	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY))",
	             "GEOMETRYCOLLECTION EMPTY");
}

static void test_coll_curve(void)
{
	/* Two different types together must produce a Collection as output */
	do_geom_test("GEOMETRYCOLLECTION(CIRCULARSTRING(0 0,1 1,2 2))",
	             "CIRCULARSTRING(0 0,1 1,2 2)");

	do_geom_test("GEOMETRYCOLLECTION(CIRCULARSTRING(0 0,1 1,2 2),CIRCULARSTRING(0 0,1 1,2 2))",
	             "MULTICURVE(CIRCULARSTRING(0 0,1 1,2 2),CIRCULARSTRING(0 0,1 1,2 2))");
}

static void test_geom(void)
{
	/* Already simple geometry */
	do_geom_test("POINT(1 2)",
	             "POINT(1 2)");

	do_geom_test("LINESTRING(1 2,3 4)",
	             "LINESTRING(1 2,3 4)");

	do_geom_test("POLYGON((1 2,3 4,5 6,1 2))",
	             "POLYGON((1 2,3 4,5 6,1 2))");

	do_geom_test("POLYGON((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8))",
	             "POLYGON((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8))");


	/* Empty geometry */
	do_geom_test("GEOMETRYCOLLECTION EMPTY",
	             "GEOMETRYCOLLECTION EMPTY");


	/* A MULTI with a single geometry inside */
	do_geom_test("MULTIPOINT(1 2)",
	             "POINT(1 2)");

	do_geom_test("MULTILINESTRING((1 2,3 4))",
	             "LINESTRING(1 2,3 4)");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2)))",
	             "POLYGON((1 2,3 4,5 6,1 2))");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8)))",
	             "POLYGON((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8))");


	/* A real MULTI */
	do_geom_test("MULTIPOINT(1 2,3 4)",
	             "MULTIPOINT(1 2,3 4)");

	do_geom_test("MULTILINESTRING((1 2,3 4),(5 6,7 8))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8))");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))");


	/* A Collection */
	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
	             "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");


	/* SRID */
	do_geom_test("SRID=4326;GEOMETRYCOLLECTION EMPTY",
	             "SRID=4326;GEOMETRYCOLLECTION EMPTY");

	/* See http://trac.osgeo.org/postgis/ticket/2129 */
	do_geom_test("SRID=4326;GEOMETRYCOLLECTION(MULTIPOINT(0 0))",
	             "SRID=4326;POINT(0 0)");

	/* See http://trac.osgeo.org/postgis/ticket/2129 */
	do_geom_test("SRID=4326;GEOMETRYCOLLECTION(MULTIPOINT(0 0, 1 2))",
	             "SRID=4326;MULTIPOINT(0 0,1 2)");

	/* See http://trac.osgeo.org/postgis/ticket/2129 */
	do_geom_test("SRID=4326;GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,10 0))",
	             "SRID=4326;GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,10 0))");

	do_geom_test("SRID=4326;POINT(1 2)",
	             "SRID=4326;POINT(1 2)");

	do_geom_test("SRID=4326;MULTIPOINT(1 2)",
	             "SRID=4326;POINT(1 2)");

	do_geom_test("SRID=4326;MULTIPOINT(1 2,3 4)",
	             "SRID=4326;MULTIPOINT(1 2,3 4)");

	do_geom_test("SRID=4326;MULTILINESTRING((1 2,3 4))",
	             "SRID=4326;LINESTRING(1 2,3 4)");

	do_geom_test("SRID=4326;MULTILINESTRING((1 2,3 4),(5 6,7 8))",
	             "SRID=4326;MULTILINESTRING((1 2,3 4),(5 6,7 8))");

	/* 3D and 4D */
	do_geom_test("POINT(1 2 3)",
	             "POINT(1 2 3)");

	do_geom_test("POINTM(1 2 3)",
	             "POINTM(1 2 3)");

	do_geom_test("POINT(1 2 3 4)",
	             "POINT(1 2 3 4)");
}

/*
** Used by test harness to register the tests in this file.
*/
void homogenize_suite_setup(void);
void homogenize_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("homogenize", NULL, NULL);
	PG_ADD_TEST(suite, test_coll_point);
	PG_ADD_TEST(suite, test_coll_line);
	PG_ADD_TEST(suite, test_coll_poly);
	PG_ADD_TEST(suite, test_coll_coll);
	PG_ADD_TEST(suite, test_geom);
	PG_ADD_TEST(suite, test_coll_curve);
}
