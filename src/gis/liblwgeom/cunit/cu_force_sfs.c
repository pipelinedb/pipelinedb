/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2013 Olivier Courtin <olivier.courtin@oslandia.com>
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
	h = lwgeom_force_sfs(g, 110);
	tmp = lwgeom_to_ewkt(h);
	if (strcmp(tmp, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nExp:  %s\n",
		        in, tmp, out);
	CU_ASSERT_STRING_EQUAL(tmp, out);
	lwfree(tmp);
	lwgeom_free(h);
}


static void do_type_test(char * in, int type)
{
	LWGEOM *g, *h;

	g = lwgeom_from_wkt(in, LW_PARSER_CHECK_NONE);
	h = lwgeom_force_sfs(g, 110);
	if(h->type != type)
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nExp:  %s\n", 
			in, lwtype_name(h->type), lwtype_name(type));
	CU_ASSERT_EQUAL(h->type, type);
	lwgeom_free(h);
	lwgeom_free(g);
}


static void test_sqlmm(void)
{
	do_type_test("CIRCULARSTRING(-1 0,0 1,0 -1)",
		     LINETYPE);

        do_type_test("COMPOUNDCURVE(CIRCULARSTRING(-1 0,0 1,0 -1),(0 -1,-1 -1))",
		     LINETYPE);

        do_type_test("COMPOUNDCURVE((-3 -3,-1 0),CIRCULARSTRING(-1 0,0 1,0 -1),(0 -1,0 -1.5,0 -2),CIRCULARSTRING(0 -2,-1 -3,1 -3),(1 -3,5 5))",
		     LINETYPE);

        do_type_test("COMPOUNDCURVE(CIRCULARSTRING(-1 0,0 1,0 -1),CIRCULARSTRING(0 -1,-1 -2,1 -2))",
		     LINETYPE);

	do_type_test("CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING (0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2),(0 1 2, 0 0 2)))",
		     POLYGONTYPE);

	do_type_test("CURVEPOLYGON (COMPOUNDCURVE (CIRCULARSTRING (0 0 2 5,1 1 2 6,1 0 2 5), (1 0 2 3,0 1 2 2), (0 1 2 2,30 1 2 2), CIRCULARSTRING (30 1 2 2,12 1 2 6,1 10 2 5, 1 10 3 5, 0 0 2 5)))",
		     POLYGONTYPE);	

	do_type_test("MULTISURFACE (CURVEPOLYGON (CIRCULARSTRING (-2 0, -1 -1, 0 0, 1 -1, 2 0, 0 2, -2 0), (-1 0, 0 0.5, 1 0, 0 1, -1 0)), ((7 8, 10 10, 6 14, 4 11, 7 8)))",
		     MULTIPOLYGONTYPE);

}

static void test_sfs_12(void)
{
	do_geom_test("TRIANGLE((1 2,3 4,5 6,1 2))",
	             "POLYGON((1 2,3 4,5 6,1 2))");

	do_geom_test("GEOMETRYCOLLECTION(TRIANGLE((1 2,3 4,5 6,1 2)))",
	             "GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)))");

	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(TRIANGLE((1 2,3 4,5 6,1 2))))",
	             "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2))))");


	do_geom_test("TIN(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))",
	             "GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))");

	do_geom_test("GEOMETRYCOLLECTION(TIN(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8))))",
	             "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8))))");

	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(TIN(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))))",
	             "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))))");


	do_geom_test("POLYHEDRALSURFACE(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))",
	             "GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))");

	do_geom_test("GEOMETRYCOLLECTION(POLYHEDRALSURFACE(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8))))",
	             "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8))))");

	do_geom_test("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))))",
	             "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((1 2,3 4,5 6,1 2)),POLYGON((7 8,9 10,11 12,7 8)))))");

}

static void test_sfs_11(void)
{
	do_geom_test("POINT(1 2)",
	             "POINT(1 2)");

	do_geom_test("LINESTRING(1 2,3 4)",
	             "LINESTRING(1 2,3 4)");

	do_geom_test("POLYGON((1 2,3 4,5 6,1 2))",
	             "POLYGON((1 2,3 4,5 6,1 2))");

	do_geom_test("POLYGON((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8))",
	             "POLYGON((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8))");

	do_geom_test("MULTIPOINT(1 2,3 4)",
	             "MULTIPOINT(1 2,3 4)");

	do_geom_test("MULTILINESTRING((1 2,3 4),(5 6,7 8))",
	             "MULTILINESTRING((1 2,3 4),(5 6,7 8))");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))");

	do_geom_test("MULTIPOLYGON(((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))",
	             "MULTIPOLYGON(((1 2,3 4,5 6,1 2),(7 8,9 10,11 12,7 8)),((13 14,15 16,17 18,13 14)))");

	do_geom_test("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
	             "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))");

	do_geom_test("GEOMETRYCOLLECTION EMPTY",
	             "GEOMETRYCOLLECTION EMPTY");

	/* SRID */
	do_geom_test("SRID=4326;GEOMETRYCOLLECTION EMPTY",
	             "SRID=4326;GEOMETRYCOLLECTION EMPTY");

	do_geom_test("SRID=4326;POINT(1 2)",
	             "SRID=4326;POINT(1 2)");


	/* 3D and 4D */
        /* SFS 1.2 is only 2D but we choose here to keep 3D and 4D,
           and let the user use force_2d if he want/need it */
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
void force_sfs_suite_setup(void);
void force_sfs_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("force_sfs", NULL, NULL);
	PG_ADD_TEST(suite, test_sfs_11);
	PG_ADD_TEST(suite, test_sfs_12);
	PG_ADD_TEST(suite, test_sqlmm);
}
