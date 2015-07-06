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
** Globals used by tests
*/
char *s;
char *r;

/*
** The suite initialization function.
** Create any re-used objects.
*/
static int init_wkt_in_suite(void)
{
	return 0;
}

/*
** The suite cleanup function.
** Frees any global objects.
*/
static int clean_wkt_in_suite(void)
{
	return 0;
}

/*
* Return a char* that results from taking the input
* WKT, creating an LWGEOM, then writing it back out
* as an output WKT using the supplied variant.
* If there is an error, return that.
*/
static char* cu_wkt_in(char *wkt, uint8_t variant)
{
	LWGEOM_PARSER_RESULT p;
	int rv = 0;
	char *s = 0;

	rv = lwgeom_parse_wkt(&p, wkt, 0);
	if( p.errcode ) {
	  CU_ASSERT_EQUAL( rv, LW_FAILURE );
		return strdup(p.message);
	}
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	s = lwgeom_to_wkt(p.geom, variant, 8, NULL);
	lwgeom_parser_result_free(&p);
	return s;
}


static void test_wkt_in_point(void)
{
	s = "POINT(1e700 0)";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_TEST ( ! strcmp(r, "POINT(inf 0)") || ! strcmp(r, "POINT(1.#INF 0)") || ! strcmp(r, "POINT(Infinity 0)") );
	lwfree(r);

	s = "POINT(0 0)";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "POINT EMPTY";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "POINT M EMPTY";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	//printf("\nIN:  %s\nOUT: %s\n",s,r);
}

static void test_wkt_in_linestring(void)
{
	s = "LINESTRING EMPTY";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING(0 0,1 1)";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING(0 0 0,1 1 1)";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING M (0 0 0,1 1 1)";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING ZM (0 0 0 1,1 1 1 1,2 2 2 2,0.141231 4 5 4)";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRINGM(0 0 0,1 1 1)";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING ZM EMPTY";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "LINESTRING Z (0 0 0 1, 0 1 0 1)";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,"can not mix dimensionality in a geometry");
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

}

static void test_wkt_in_polygon(void)
{
	s = "POLYGON((0 0,0 1,1 1,0 0))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "POLYGON Z ((0 0,0 10,10 10,10 0,0 0),(1 1 1,1 2 1,2 2 1,2 1 1,1 1 1))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,"can not mix dimensionality in a geometry");
	lwfree(r);

	s = "POLYGON Z ((0 0,0 10,10 10,10 0,0 0),(1 1,1 2,2 2,2 1,1 1))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,"can not mix dimensionality in a geometry");
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_multipoint(void)
{
	/**I'm remarking this out since it fails on windows because windows returns
	 MULTIPOINT(-1 -2 -3,5.4 6.6 7.77,-5.4 -6.6 -7.77,1000000 1e-006 -1000000,-1.3e-006 -1.4e-005 0) **/
	/** @todo TODO: Paul put back in if you care after you do replace mumbo jumbo to replace the extra 0s in Windows
	*/
	// s = "MULTIPOINT(-1 -2 -3,5.4 6.6 7.77,-5.4 -6.6 -7.77,1000000 1e-06 -1000000,-1.3e-06 -1.4e-05 0)";
	// r = cu_wkt_in(s, WKT_EXTENDED);
	// CU_ASSERT_STRING_EQUAL(r,s);
	// printf("\nIN:  %s\nOUT: %s\n",s,r);
	// lwfree(r);

	s = "MULTIPOINT(0 0)";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "MULTIPOINT(0 0,1 1)";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_multilinestring(void)
{
	s = "MULTILINESTRING((0 0,1 1),(1 1,2 2),(3 3,3 3,3 3,2 2,2 1))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

}

static void test_wkt_in_multipolygon(void)
{
	s = "MULTIPOLYGON(((0 0,0 1,1 1,0 0)))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "MULTIPOLYGON(((0 0,0 10,10 10,0 0),(1 1,1 2,2 2,1 1)),((-10 -10,-10 -5,-5 -5,-10 -10)))";
	r = cu_wkt_in(s, WKT_SFSQL);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "SRID=4;MULTIPOLYGON(((0 0,0 1,1 1,0 0)))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

}

static void test_wkt_in_collection(void)
{
	s = "SRID=5;GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 0,0 0),CIRCULARSTRING(0 0,0 1,1 1,0 1,2 2))";
	r = cu_wkt_in(s, WKT_EXTENDED);
 	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "GEOMETRYCOLLECTION(POINT(0 0),POINT EMPTY,LINESTRING(1 0,0 0),POLYGON EMPTY,CIRCULARSTRING(0 0,0 1,1 1,0 1,2 2))";
	r = cu_wkt_in(s, WKT_SFSQL);
 	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "GEOMETRYCOLLECTION Z (POINT Z (0 0 0))";
	r = cu_wkt_in(s, WKT_ISO);
 	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	s = "GEOMETRYCOLLECTION M (MULTILINESTRING M ((0 0 5,2 0 5),(1 1 5,2 2 5)))";
	r = cu_wkt_in(s, WKT_ISO);
 	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);

	/* See http://trac.osgeo.org/postgis/ticket/1455#comment:3 */
	s = "GEOMETRYCOLLECTION Z (MULTILINESTRING Z ((0 0 5,2 0 5),(1 1 5,2 2 5)))";
	r = cu_wkt_in(s, WKT_ISO);
 	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);
}

static void test_wkt_in_circularstring(void)
{
	s = "CIRCULARSTRING(0 0,0 1,1 1,0 1,2 2)";
	r = cu_wkt_in(s, WKT_SFSQL);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	CU_ASSERT_STRING_EQUAL(r,s);
	lwfree(r);
}

static void test_wkt_in_compoundcurve(void)
{
	s = "SRID=4326;COMPOUNDCURVEM(CIRCULARSTRINGM(0 0 2,1 1 2,1 0 2),(1 0 2,0 1 2))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "COMPOUNDCURVE Z (CIRCULARSTRING Z (0 0 0,0 1 0,1 1 0,0 0 0,2 2 0),(2 2 0,0 0 1,1 1 1,2 2 1))";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_curvpolygon(void)
{
	s = "CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(0 0,0 1,1 1,2 2,0 0),(0 0,1 1,2 2)),CIRCULARSTRING(0 0,0 1,1 1,0 0,2 2),(0 0,1 1,2 1))";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_multicurve(void)
{
	s = "SRID=4326;MULTICURVE(COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1)))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_multisurface(void)
{
	s = "SRID=4326;MULTICURVE(COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1)))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_tin(void)
{
	s = "TIN(((0 1 2,3 4 5,6 7 8,0 1 2)),((0 1 2,3 4 5,6 7 8,9 10 11,0 1 2)))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,"triangle must have exactly 4 points");
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);
}

static void test_wkt_in_polyhedralsurface(void)
{
	s = "POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))";
	r = cu_wkt_in(s, WKT_EXTENDED);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "POLYHEDRALSURFACE Z (((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,s);
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

	s = "POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7,0 1 2)))";
	r = cu_wkt_in(s, WKT_ISO);
	CU_ASSERT_STRING_EQUAL(r,"can not mix dimensionality in a geometry");
	//printf("\nIN:  %s\nOUT: %s\n",s,r);
	lwfree(r);

}

static void test_wkt_in_errlocation(void)
{
	LWGEOM_PARSER_RESULT p;
	int rv = 0;
	char *wkt = 0;

	wkt = "LINESTRING((0 0 0,1 1)";
	lwgeom_parser_result_init(&p);
	rv = lwgeom_parse_wkt(&p, wkt, LW_PARSER_CHECK_ALL);
	CU_ASSERT_EQUAL( rv, LW_FAILURE );
	CU_ASSERT((12 - p.errlocation) < 1.5);

//	printf("errlocation %d\n", p.errlocation);
//	printf("message %s\n", p.message);

	lwgeom_parser_result_free(&p);

}

/*
** Used by test harness to register the tests in this file.
*/
void wkt_in_suite_setup(void);
void wkt_in_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("wkt_input", init_wkt_in_suite, clean_wkt_in_suite);
	PG_ADD_TEST(suite, test_wkt_in_point);
	PG_ADD_TEST(suite, test_wkt_in_linestring);
	PG_ADD_TEST(suite, test_wkt_in_polygon);
	PG_ADD_TEST(suite, test_wkt_in_multipoint);
	PG_ADD_TEST(suite, test_wkt_in_multilinestring);
	PG_ADD_TEST(suite, test_wkt_in_multipolygon);
	PG_ADD_TEST(suite, test_wkt_in_collection);
	PG_ADD_TEST(suite, test_wkt_in_circularstring);
	PG_ADD_TEST(suite, test_wkt_in_compoundcurve);
	PG_ADD_TEST(suite, test_wkt_in_curvpolygon);
	PG_ADD_TEST(suite, test_wkt_in_multicurve);
	PG_ADD_TEST(suite, test_wkt_in_multisurface);
	PG_ADD_TEST(suite, test_wkt_in_tin);
	PG_ADD_TEST(suite, test_wkt_in_polyhedralsurface);
	PG_ADD_TEST(suite, test_wkt_in_errlocation);
}
