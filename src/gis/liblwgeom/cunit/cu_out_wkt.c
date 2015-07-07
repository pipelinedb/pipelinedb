/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 Paul Ramsey <pramsey@cleverelephant.ca>
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

/*
** Global variable to hold WKT strings
*/
char *s = NULL; 

/*
** The suite initialization function.
** Create any re-used objects.
*/
static int init_wkt_out_suite(void)
{
	s = NULL;
	return 0;
}

/*
** The suite cleanup function.
** Frees any global objects.
*/
static int clean_wkt_out_suite(void)
{
	if ( s ) free(s);
	s = NULL;
	return 0;
}

static char* cu_wkt(char *wkt, uint8_t variant)
{
	LWGEOM *g = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	if ( s ) free(s);
	if ( ! g ) 
	{
		printf("error converting '%s' to lwgeom\n", wkt);
		exit(0);
	}
	s = lwgeom_to_wkt(g, variant, 8, NULL);
	lwgeom_free(g);
	return s;
}

static void test_wkt_out_point(void)
{
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(0.1111 0.1111 0.1111 0)",WKT_ISO), "POINT ZM (0.1111 0.1111 0.1111 0)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(0 0 0 0)",WKT_EXTENDED), "POINT(0 0 0 0)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(0 0 0 0)",WKT_SFSQL), "POINT(0 0)");

	CU_ASSERT_STRING_EQUAL(cu_wkt("POINTM(0 0 0)",WKT_ISO), "POINT M (0 0 0)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINTM(0 0 0)",WKT_EXTENDED), "POINTM(0 0 0)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINTM(0 0 0)",WKT_SFSQL), "POINT(0 0)");

	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100 100)",WKT_ISO), "POINT(100 100)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100 100)",WKT_EXTENDED), "POINT(100 100)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100 100)",WKT_SFSQL), "POINT(100 100)");

	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100.1 100 12 12)",WKT_ISO), "POINT ZM (100.1 100 12 12)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100.1 100 12 12)",WKT_EXTENDED), "POINT(100.1 100 12 12)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("POINT(100.1 100 12 12)",WKT_SFSQL), "POINT(100.1 100)");

	CU_ASSERT_STRING_EQUAL(cu_wkt("SRID=100;POINT(100.1 100 12 12)",WKT_SFSQL), "POINT(100.1 100)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("SRID=100;POINT(100.1 100 12 12)",WKT_EXTENDED), "SRID=100;POINT(100.1 100 12 12)");
//	printf("%s\n",cu_wkt("SRID=100;POINT(100.1 100 12 12)",WKT_EXTENDED));

}

static void test_wkt_out_linestring(void)
{
	CU_ASSERT_STRING_EQUAL(cu_wkt("LINESTRING(1 2 3 4,5 6 7 8)",WKT_ISO), "LINESTRING ZM (1 2 3 4,5 6 7 8)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("LINESTRING(1 2 3,5 6 7)",WKT_ISO), "LINESTRING Z (1 2 3,5 6 7)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("LINESTRINGM(1 2 3,5 6 7)",WKT_ISO), "LINESTRING M (1 2 3,5 6 7)");
}

static void test_wkt_out_polygon(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("POLYGON((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2))",WKT_ISO),
	    "POLYGON Z ((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("POLYGON((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2))",WKT_EXTENDED),
	    "POLYGON((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2))"
	);
}
static void test_wkt_out_multipoint(void)
{
	CU_ASSERT_STRING_EQUAL(cu_wkt("MULTIPOINT(1 2 3 4,5 6 7 8)",WKT_ISO), "MULTIPOINT ZM (1 2 3 4,5 6 7 8)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("MULTIPOINT(1 2 3,5 6 7)",WKT_ISO), "MULTIPOINT Z (1 2 3,5 6 7)");
	CU_ASSERT_STRING_EQUAL(cu_wkt("MULTIPOINTM(1 2 3,5 6 7)",WKT_ISO), "MULTIPOINT M (1 2 3,5 6 7)");

}

static void test_wkt_out_multilinestring(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTILINESTRING((1 2 3 4,5 6 7 8))",WKT_ISO),
	    "MULTILINESTRING ZM ((1 2 3 4,5 6 7 8))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTILINESTRING((1 2 3,5 6 7))",WKT_ISO),
	    "MULTILINESTRING Z ((1 2 3,5 6 7))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTILINESTRINGM((1 2 3,5 6 7))",WKT_ISO),
	    "MULTILINESTRING M ((1 2 3,5 6 7))"
	);
}

static void test_wkt_out_multipolygon(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTIPOLYGON(((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2)))",WKT_ISO),
	    "MULTIPOLYGON Z (((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2)))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTIPOLYGON(((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2)))",WKT_EXTENDED),
	    "MULTIPOLYGON(((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2)))"
	);
}

static void test_wkt_out_collection(void)
{
	//printf("%s\n",cu_wkt("GEOMETRYCOLLECTION(MULTIPOLYGON(((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2))),MULTIPOINT(.5 .5 .5,1 1 1),CURVEPOLYGON((.8 .8 .8,.8 .8 .8,.8 .8 .8)))",WKT_ISO));
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("GEOMETRYCOLLECTION(POLYGON((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2)),POINT(.5 .5 .5),CIRCULARSTRING(.8 .8 .8,.8 .8 .8,.8 .8 .8))",WKT_ISO),
	    "GEOMETRYCOLLECTION Z (POLYGON Z ((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2)),POINT Z (0.5 0.5 0.5),CIRCULARSTRING Z (0.8 0.8 0.8,0.8 0.8 0.8,0.8 0.8 0.8))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("GEOMETRYCOLLECTION(MULTIPOLYGON(((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2))),MULTIPOINT(.5 .5 .5,1 1 1),CURVEPOLYGON((.8 .8 .8,.8 .8 .8,.8 .8 .8)))",WKT_ISO),
	    "GEOMETRYCOLLECTION Z (MULTIPOLYGON Z (((100 100 2,100 200 2,200 200 2,200 100 2,100 100 2))),MULTIPOINT Z (0.5 0.5 0.5,1 1 1),CURVEPOLYGON Z ((0.8 0.8 0.8,0.8 0.8 0.8,0.8 0.8 0.8)))"
	);

        /* See http://trac.osgeo.org/postgis/ticket/724 */
	CU_ASSERT_STRING_EQUAL(
            cu_wkt("GEOMETRYCOLLECTIONM(MULTIPOINTM(0 0 0), POINTM(1 1 1))", WKT_EXTENDED),
            "GEOMETRYCOLLECTIONM(MULTIPOINTM(0 0 0),POINTM(1 1 1))"
	);
}

static void test_wkt_out_circularstring(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("CIRCULARSTRING(1 2 3 4,4 5 6 7,7 8 9 0)",WKT_ISO),
	    "CIRCULARSTRING ZM (1 2 3 4,4 5 6 7,7 8 9 0)"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("CIRCULARSTRING(1 2 3 4,4 5 6 7,7 8 9 0)",WKT_EXTENDED),
	    "CIRCULARSTRING(1 2 3 4,4 5 6 7,7 8 9 0)"
	);
	//printf("%s\n",cu_wkt("GEOMETRYCOLLECTION(MULTIPOLYGON(((100 100 2, 100 200 2, 200 200 2, 200 100 2, 100 100 2))),MULTIPOINT(.5 .5 .5,1 1 1),CURVEPOLYGON((.8 .8 .8,.8 .8 .8,.8 .8 .8)))",WKT_ISO));
}

static void test_wkt_out_compoundcurve(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("COMPOUNDCURVE((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING(7 8 9 0,4 3 2 1,1 2 3 4,4 5 6 7,7 8 9 0))",WKT_ISO),
	    "COMPOUNDCURVE ZM ((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING ZM (7 8 9 0,4 3 2 1,1 2 3 4,4 5 6 7,7 8 9 0))"
	);
}

static void test_wkt_out_curvpolygon(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("CURVEPOLYGON((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING(7 8 9 0,1 2 1 1,1 2 3 4,4 5 6 7,7 8 9 0))",WKT_ISO),
	    "CURVEPOLYGON ZM ((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING ZM (7 8 9 0,1 2 1 1,1 2 3 4,4 5 6 7,7 8 9 0))"
	);
}

static void test_wkt_out_multicurve(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTICURVE((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING(1 2 3 4,4 5 6 7,7 8 9 0))",WKT_ISO),
	    "MULTICURVE ZM ((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING ZM (1 2 3 4,4 5 6 7,7 8 9 0))"
	);
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTICURVE(COMPOUNDCURVE((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING(7 8 9 0,8 9 0 0,1 2 3 4,4 5 6 7,7 8 9 0)),(1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING(1 2 3 4,4 5 6 7,7 8 9 0))",WKT_ISO),
	    "MULTICURVE ZM (COMPOUNDCURVE ZM ((1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING ZM (7 8 9 0,8 9 0 0,1 2 3 4,4 5 6 7,7 8 9 0)),(1 2 3 4,4 5 6 7,7 8 9 0),CIRCULARSTRING ZM (1 2 3 4,4 5 6 7,7 8 9 0))"
	);
}

static void test_wkt_out_multisurface(void)
{
	CU_ASSERT_STRING_EQUAL(
	    cu_wkt("MULTISURFACE(((1 2 3 4,4 5 6 7,7 8 9 0)),CURVEPOLYGON((1 2 3 4,4 5 6 7,7 8 9 0)))",WKT_ISO),
	    "MULTISURFACE ZM (((1 2 3 4,4 5 6 7,7 8 9 0)),CURVEPOLYGON ZM ((1 2 3 4,4 5 6 7,7 8 9 0)))"
	);

}

/*
** Used by test harness to register the tests in this file.
*/
void wkt_out_suite_setup(void);
void wkt_out_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("wkt_output", init_wkt_out_suite, clean_wkt_out_suite);
	PG_ADD_TEST(suite, test_wkt_out_point);
	PG_ADD_TEST(suite, test_wkt_out_linestring);
	PG_ADD_TEST(suite, test_wkt_out_polygon);
	PG_ADD_TEST(suite, test_wkt_out_multipoint);
	PG_ADD_TEST(suite, test_wkt_out_multilinestring);
	PG_ADD_TEST(suite, test_wkt_out_multipolygon);
	PG_ADD_TEST(suite, test_wkt_out_collection);
	PG_ADD_TEST(suite, test_wkt_out_circularstring);
	PG_ADD_TEST(suite, test_wkt_out_compoundcurve);
	PG_ADD_TEST(suite, test_wkt_out_curvpolygon);
	PG_ADD_TEST(suite, test_wkt_out_multicurve);
	PG_ADD_TEST(suite, test_wkt_out_multisurface);
}
