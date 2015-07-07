/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
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
#include "measures.h"
#include "lwtree.h"

static LWGEOM* lwgeom_from_text(const char *str)
{
	LWGEOM_PARSER_RESULT r;
	if( LW_FAILURE == lwgeom_parse_wkt(&r, (char*)str, LW_PARSER_CHECK_NONE) )
		return NULL;
	return r.geom;
}

#define DIST2DTEST(str1, str2, res) do_test_mindistance2d_tolerance(str1, str2, res, __LINE__)

static void do_test_mindistance2d_tolerance(char *in1, char *in2, double expected_res, int line)
{
	LWGEOM *lw1;
	LWGEOM *lw2;
	double distance;
	char *msg1 = "test_mindistance2d_tolerance failed (got %g expected %g) at line %d\n";
	char *msg2 = "\n\ndo_test_mindistance2d_tolerance: NULL lwgeom generated from WKT\n  %s\n\n";

	lw1 = lwgeom_from_wkt(in1, LW_PARSER_CHECK_NONE);
	lw2 = lwgeom_from_wkt(in2, LW_PARSER_CHECK_NONE);

	if ( ! lw1 )
	{
		printf(msg2, in1);
		exit(1);
	}
	if ( ! lw2 )
	{
		printf(msg2, in2);
		exit(1);
	}

	distance = lwgeom_mindistance2d_tolerance(lw1, lw2, 0.0);
	lwgeom_free(lw1);
	lwgeom_free(lw2);

	if ( fabs(distance - expected_res) > 0.00001 )
	{
		printf(msg1, distance, expected_res, line);
		CU_FAIL();
	}
	else
	{
		CU_PASS();
	}

}

static void test_mindistance2d_tolerance(void)
{
	/*
	** Simple case.
	*/
	DIST2DTEST("POINT(0 0)", "MULTIPOINT(0 1.5,0 2,0 2.5)", 1.5);

	/*
	** Point vs Geometry Collection.
	*/
	DIST2DTEST("POINT(0 0)", "GEOMETRYCOLLECTION(POINT(3 4))", 5.0);

	/*
	** Point vs Geometry Collection Collection.
	*/
	DIST2DTEST("POINT(0 0)", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(3 4)))", 5.0);

	/*
	** Point vs Geometry Collection Collection Collection.
	*/
	DIST2DTEST("POINT(0 0)", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(3 4))))", 5.0);

	/*
	** Point vs Geometry Collection Collection Collection Multipoint.
	*/
	DIST2DTEST("POINT(0 0)", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT(3 4))))", 5.0);

	/*
	** Geometry Collection vs Geometry Collection
	*/
	DIST2DTEST("GEOMETRYCOLLECTION(POINT(0 0))", "GEOMETRYCOLLECTION(POINT(3 4))", 5.0);

	/*
	** Geometry Collection Collection vs Geometry Collection Collection
	*/
	DIST2DTEST("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)))", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(3 4)))", 5.0);

	/*
	** Geometry Collection Collection Multipoint vs Geometry Collection Collection Multipoint
	*/
	DIST2DTEST("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT(0 0)))", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT(3 4)))", 5.0);

	/*
	** Linestring vs its start point 
	*/
	DIST2DTEST("LINESTRING(-2 0, -0.2 0)", "POINT(-2 0)", 0);

	/*
	** Linestring vs its end point 
	*/
	DIST2DTEST("LINESTRING(-0.2 0, -2 0)", "POINT(-2 0)", 0);

	/*
	** Linestring vs its start point (tricky number, see #1459)
	*/
	DIST2DTEST("LINESTRING(-1e-8 0, -0.2 0)", "POINT(-1e-8 0)", 0);

	/*
	** Linestring vs its end point (tricky number, see #1459)
	*/
	DIST2DTEST("LINESTRING(-0.2 0, -1e-8 0)", "POINT(-1e-8 0)", 0);

	/*
	* Circular string and point 
	*/
	DIST2DTEST("CIRCULARSTRING(-1 0, 0 1, 1 0)", "POINT(0 0)", 1);
	DIST2DTEST("CIRCULARSTRING(-3 0, -2 0, -1 0, 0 1, 1 0)", "POINT(0 0)", 1);

	/*
	* Circular string and Circular string 
	*/
	DIST2DTEST("CIRCULARSTRING(-1 0, 0 1, 1 0)", "CIRCULARSTRING(0 0, 1 -1, 2 0)", 1);

	/*
	* CurvePolygon and Point
	*/
	static char *cs1 = "CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(1 6, 6 1, 9 7),(9 7, 3 13, 1 6)),COMPOUNDCURVE((3 6, 5 4, 7 4, 7 6),CIRCULARSTRING(7 6,5 8,3 6)))";
	DIST2DTEST(cs1, "POINT(3 14)", 1);
	DIST2DTEST(cs1, "POINT(3 8)", 0);
	DIST2DTEST(cs1, "POINT(6 5)", 1);
	DIST2DTEST(cs1, "POINT(6 4)", 0);

	/*
	* CurvePolygon and Linestring
	*/
	DIST2DTEST(cs1, "LINESTRING(0 0, 50 0)", 0.917484);
	DIST2DTEST(cs1, "LINESTRING(6 0, 10 7)", 0);
	DIST2DTEST(cs1, "LINESTRING(4 4, 4 8)", 0);
	DIST2DTEST(cs1, "LINESTRING(4 7, 5 6, 6 7)", 0.585786);
	DIST2DTEST(cs1, "LINESTRING(10 0, 10 2, 10 0)", 1.52913);

	/*
	* CurvePolygon and Polygon
	*/
	DIST2DTEST(cs1, "POLYGON((10 4, 10 8, 13 8, 13 4, 10 4))", 0.58415);
	DIST2DTEST(cs1, "POLYGON((9 4, 9 8, 12 8, 12 4, 9 4))", 0);
	DIST2DTEST(cs1, "POLYGON((1 4, 1 8, 4 8, 4 4, 1 4))", 0);

	/*
	* CurvePolygon and CurvePolygon
	*/
	DIST2DTEST(cs1, "CURVEPOLYGON(CIRCULARSTRING(-1 4, 0 5, 1 4, 0 3, -1 4))", 0.0475666);
	DIST2DTEST(cs1, "CURVEPOLYGON(CIRCULARSTRING(1 4, 2 5, 3 4, 2 3, 1 4))", 0.0);

	/*
	* MultiSurface and CurvePolygon 
	*/
	static char *cs2 = "MULTISURFACE(POLYGON((0 0,0 4,4 4,4 0,0 0)),CURVEPOLYGON(CIRCULARSTRING(8 2,10 4,12 2,10 0,8 2)))";
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(5 2,6 3,7 2,6 1,5 2))", 1);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(4 2,5 3,6 2,5 1,4 2))", 0);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(5 3,6 2,5 1,4 2,5 3))", 0);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(4.5 3,5.5 2,4.5 1,3.5 2,4.5 3))", 0);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(5.5 3,6.5 2,5.5 1,4.5 2,5.5 3))", 0.5);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(10 3,11 2,10 1,9 2,10 3))", 0);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(2 3,3 2,2 1,1 2,2 3))", 0);
	DIST2DTEST(cs2, "CURVEPOLYGON(CIRCULARSTRING(5 7,6 8,7 7,6 6,5 7))", 2.60555);

	/*
	* MultiCurve and Linestring
	*/
	DIST2DTEST("LINESTRING(0.5 1,0.5 3)", "MULTICURVE(CIRCULARSTRING(2 3,3 2,2 1,1 2,2 3),(0 0, 0 5))", 0.5);

}

static void test_rect_tree_contains_point(void)
{
	LWPOLY *poly;
	POINT2D p;
	RECT_NODE* tree;
	int result;
	int boundary = 0;

	/* square */
	poly = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", LW_PARSER_CHECK_NONE);
	tree = rect_tree_new(poly->rings[0]);

	/* inside square */
	boundary = 0;
	p.x = 0.5;
	p.y = 0.5;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_NOT_EQUAL(result, 0);

	/* outside square */
	boundary = 0;
	p.x = 1.5;
	p.y = 0.5;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(result, 0);

	rect_tree_free(tree);
	lwpoly_free(poly);

	/* ziggy zaggy horizontal saw tooth polygon */
	poly = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 1 3, 2 0, 3 3, 4 0, 4 5, 0 5, 0 0))", LW_PARSER_CHECK_NONE);
	tree = rect_tree_new(poly->rings[0]);

	/* not in, left side */
	boundary = 0;
	p.x = -0.5;
	p.y = 0.5;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(result, 0);

	/* not in, right side */
	boundary = 0;
	p.x = 3.0;
	p.y = 1.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(result, 0);

	/* inside */
	boundary = 0;
	p.x = 2.0;
	p.y = 1.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_NOT_EQUAL(result, 0);

	/* on left border */
	boundary = 0;
	p.x = 0.0;
	p.y = 1.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on right border */
	boundary = 0;
	p.x = 4.0;
	p.y = 0.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on tooth concave */
	boundary = 0;
	p.x = 3.0;
	p.y = 3.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on tooth convex */
	boundary = 0;
	p.x = 2.0;
	p.y = 0.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	rect_tree_free(tree);
	lwpoly_free(poly);

	/* ziggy zaggy vertical saw tooth polygon */
	poly = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	tree = rect_tree_new(poly->rings[0]);

	/* not in, left side */
	boundary = 0;
	p.x = -0.5;
	p.y = 3.5;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(result, 0);

	/* not in, right side */
	boundary = 0;
	p.x = 6.0;
	p.y = 2.2;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(result, 0);

	/* inside */
	boundary = 0;
	p.x = 3.0;
	p.y = 2.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_NOT_EQUAL(result, 0);

	/* on bottom border */
	boundary = 0;
	p.x = 1.0;
	p.y = 0.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on top border */
	boundary = 0;
	p.x = 3.0;
	p.y = 6.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on tooth concave */
	boundary = 0;
	p.x = 3.0;
	p.y = 1.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on tooth convex */
	boundary = 0;
	p.x = 0.0;
	p.y = 2.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	/* on tooth convex */
	boundary = 0;
	p.x = 0.0;
	p.y = 6.0;
	result = rect_tree_contains_point(tree, &p, &boundary);
	CU_ASSERT_EQUAL(boundary, 1);

	rect_tree_free(tree);
	lwpoly_free(poly);

}

static void test_rect_tree_intersects_tree(void)
{
	LWPOLY *poly1, *poly2;
	RECT_NODE *tree1, *tree2;
	int result;

	/* total overlap, A == B */
	poly1 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	poly2 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	tree1 = rect_tree_new(poly1->rings[0]);
	tree2 = rect_tree_new(poly2->rings[0]);
	result = rect_tree_intersects_tree(tree1, tree2);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwpoly_free(poly1);
	lwpoly_free(poly2);
	rect_tree_free(tree1);
	rect_tree_free(tree2);

	/* hiding between the tines of the comb */
	poly1 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	poly2 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0.3 0.7, 0.3 0.8, 0.4 0.8, 0.4 0.7, 0.3 0.7))", LW_PARSER_CHECK_NONE);
	tree1 = rect_tree_new(poly1->rings[0]);
	tree2 = rect_tree_new(poly2->rings[0]);
	result = rect_tree_intersects_tree(tree1, tree2);
	CU_ASSERT_EQUAL(result, LW_FALSE);
	lwpoly_free(poly1);
	lwpoly_free(poly2);
	rect_tree_free(tree1);
	rect_tree_free(tree2);

	/* between the tines, but with a corner overlapping */
	poly1 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	poly2 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0.3 0.7, 0.3 0.8, 0.4 0.8, 1.3 0.3, 0.3 0.7))", LW_PARSER_CHECK_NONE);
	tree1 = rect_tree_new(poly1->rings[0]);
	tree2 = rect_tree_new(poly2->rings[0]);
	result = rect_tree_intersects_tree(tree1, tree2);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwpoly_free(poly1);
	lwpoly_free(poly2);
	rect_tree_free(tree1);
	rect_tree_free(tree2);

	/* Just touching the top left corner of the comb */
	poly1 = (LWPOLY*)lwgeom_from_wkt("POLYGON((0 0, 3 1, 0 2, 3 3, 0 4, 3 5, 0 6, 5 6, 5 0, 0 0))", LW_PARSER_CHECK_NONE);
	poly2 = (LWPOLY*)lwgeom_from_wkt("POLYGON((-1 5, 0 5, 0 7, -1 7, -1 5))", LW_PARSER_CHECK_NONE);
	tree1 = rect_tree_new(poly1->rings[0]);
	tree2 = rect_tree_new(poly2->rings[0]);
	result = rect_tree_intersects_tree(tree1, tree2);
	CU_ASSERT_EQUAL(result, LW_TRUE);
	lwpoly_free(poly1);
	lwpoly_free(poly2);
	rect_tree_free(tree1);
	rect_tree_free(tree2);


}

static void
test_lwgeom_segmentize2d(void)
{
	LWGEOM *linein = lwgeom_from_wkt("LINESTRING(0 0,10 0)", LW_PARSER_CHECK_NONE);
	LWGEOM *lineout = lwgeom_segmentize2d(linein, 5);
	char *strout = lwgeom_to_ewkt(lineout);
	CU_ASSERT_STRING_EQUAL(strout, "LINESTRING(0 0,5 0,10 0)");
	lwfree(strout);
	lwgeom_free(linein);
	lwgeom_free(lineout);

	/* test interruption */

	linein = lwgeom_from_wkt("LINESTRING(0 0,10 0)", LW_PARSER_CHECK_NONE);
	lwgeom_request_interrupt();
	lineout = lwgeom_segmentize2d(linein, 1e-100);
	CU_ASSERT_EQUAL(lineout, NULL);
	lwgeom_free(linein);

	linein = lwgeom_from_wkt("MULTILINESTRING((0 0,10 0),(20 0, 30 0))", LW_PARSER_CHECK_NONE);
	lwgeom_request_interrupt();
	lineout = lwgeom_segmentize2d(linein, 1e-100);
	CU_ASSERT_EQUAL(lineout, NULL);
	lwgeom_free(linein);

	linein = lwgeom_from_wkt(
"MULTIPOLYGON(((0 0,20 0,20 20,0 20,0 0),(2 2,2 4,4 4,4 2,2 2),(6 6,6 8,8 8,8 6,6 6)),((40 0,40 20,60 20,60 0,40 0),(42 2,42 4,44 4,44 2,42 2)))"
    , LW_PARSER_CHECK_NONE);
	lwgeom_request_interrupt();
	lineout = lwgeom_segmentize2d(linein, 1e-100);
	CU_ASSERT_EQUAL(lineout, NULL);
	lwgeom_free(linein);

	linein = lwgeom_from_wkt(
"GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,20 0,20 20,0 20,0 0),(2 2,2 4,4 4,4 2,2 2),(6 6,6 8,8 8,8 6,6 6)),((40 0,40 20,60 20,60 0,40 0),(42 2,42 4,44 4,44 2,42 2))),MULTILINESTRING((0 0,10 0),(20 0, 30 0)),MULTIPOINT(0 0, 3 4))"
    , LW_PARSER_CHECK_NONE);
	CU_ASSERT_FATAL(linein != NULL);
	lwgeom_request_interrupt();
	lineout = lwgeom_segmentize2d(linein, 1e-100);
	CU_ASSERT_EQUAL(lineout, NULL);
	lwgeom_free(linein);

	linein = lwgeom_from_wkt("LINESTRING(20 0, 30 0)", LW_PARSER_CHECK_NONE);
	/* NOT INTERRUPTED */
	lineout = lwgeom_segmentize2d(linein, 5);
	strout = lwgeom_to_ewkt(lineout);
	CU_ASSERT_STRING_EQUAL(strout, "LINESTRING(20 0,25 0,30 0)");
	lwfree(strout);
	lwgeom_free(linein);
	lwgeom_free(lineout);
}

static void
test_lwgeom_locate_along(void)
{
	LWGEOM *geom = NULL;
	LWGEOM *out = NULL;
	double measure = 105.0;
	char *str;
	
	/* ST_Locatealong(ST_GeomFromText('MULTILINESTRING M ((1 2 3, 5 4 5), (50 50 1, 60 60 200))'), 105) */
	geom = lwgeom_from_wkt("MULTILINESTRING M ((1 2 3, 5 4 5), (50 50 1, 60 60 200))", LW_PARSER_CHECK_NONE);
	out = lwgeom_locate_along(geom, measure, 0.0);
	str = lwgeom_to_wkt(out, WKT_ISO, 8, NULL);
	lwgeom_free(geom);
	lwgeom_free(out);
	CU_ASSERT_STRING_EQUAL("MULTIPOINT M (55.226131 55.226131 105)", str);
	lwfree(str);
	
	/* ST_Locatealong(ST_GeomFromText('MULTILINESTRING M ((1 2 3, 5 4 5), (50 50 1, 60 60 200))'), 105) */
	geom = lwgeom_from_wkt("MULTILINESTRING M ((1 2 3, 3 4 2, 9 4 3), (1 2 3, 5 4 5), (50 50 1, 60 60 200))", LW_PARSER_CHECK_NONE);
	out = lwgeom_locate_along(geom, measure, 0.0);
	str = lwgeom_to_wkt(out, WKT_ISO, 8, NULL);
	lwgeom_free(geom);
	lwgeom_free(out);
	CU_ASSERT_STRING_EQUAL("MULTIPOINT M (55.226131 55.226131 105)", str);
	lwfree(str);
}

static void
test_lw_dist2d_pt_arc(void)
{
	/* int lw_dist2d_pt_arc(const POINT2D* P, const POINT2D* A1, const POINT2D* A2, const POINT2D* A3, DISTPTS* dl) */
	DISTPTS dl;
	POINT2D P, A1, A2, A3;
	int rv;

	
	/* Point within unit semicircle, 0.5 units from arc */
	A1.x = -1; A1.y = 0;
	A2.x = 0 ; A2.y = 1;
	A3.x = 1 ; A3.y = 0;
	P.x  = 0 ; P.y  = 0.5;	

	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Point outside unit semicircle, 0.5 units from arc */
	P.x  = 0 ; P.y  = 1.5;	
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Point outside unit semicircle, sqrt(2) units from arc end point*/
	P.x  = 0 ; P.y  = -1;	
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0), 0.000001);

	/* Point outside unit semicircle, sqrt(2)-1 units from arc end point*/
	P.x  = 1 ; P.y  = 1;	
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0)-1, 0.000001);

	/* Point on unit semicircle midpoint */
	P.x  = 0 ; P.y  = 1;	
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0, 0.000001);

	/* Point on unit semicircle endpoint */
	P.x  = 1 ; P.y  = 0;	
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0, 0.000001);

	/* Point inside closed circle */
	P.x  = 0 ; P.y  = 0.5;
	A2.x = 1; A2.y = 0;
	A3 = A1;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_arc(&P, &A1, &A2, &A3, &dl);
	//printf("distance %g\n", dl.distance);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);	
}

static void
test_lw_dist2d_seg_arc(void)
{
	/* int lw_dist2d_seg_arc(const POINT2D *A1, const POINT2D *A2, const POINT2D *B1, const POINT2D *B2, const POINT2D *B3, DISTPTS *dl) */

	DISTPTS dl;
	POINT2D A1, A2, B1, B2, B3;
	int rv;
	
	/* Unit semicircle */
	B1.x = -1; B1.y = 0;
	B2.x = 0 ; B2.y = 1;
	B3.x = 1 ; B3.y = 0;

	/* Edge above the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -2; A1.y = 2;
	A2.x = 2 ; A2.y = 2;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Edge to the right of the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = 2; A1.y = -2;
	A2.x = 2; A2.y = 2;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Edge to the left of the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -2; A1.y = -2;
	A2.x = -2; A2.y = 2;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Edge within the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = 0; A1.y = 0;
	A2.x = 0; A2.y = 0.5;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Edge grazing the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -2; A1.y = 1;
	A2.x =  2; A2.y = 1;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0., 0.000001);

	/* Line grazing the unit semicircle, but edge not */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = 1; A1.y = 1;
	A2.x = 2; A2.y = 1;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0)-1, 0.000001);

	/* Edge intersecting the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = 0; A1.y = 0;
	A2.x = 2; A2.y = 2;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0, 0.000001);

	/* Line intersecting the unit semicircle, but edge not */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -1; A1.y = 1;
	A2.x = -2; A2.y = 2;
	rv = lw_dist2d_seg_arc(&A1, &A2, &B1, &B2, &B3, &dl);
	//printf("distance %g\n", dl.distance);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0)-1, 0.000001);
}

static void
test_lw_dist2d_arc_arc(void)
{
	/* lw_dist2d_arc_arc(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3, 
	                     const POINT2D *B1, const POINT2D *B2, const POINT2D *B3,
	                     DISTPTS *dl) */
	DISTPTS dl;
	POINT2D A1, A2, A3, B1, B2, B3;
	int rv;
	
	/* Unit semicircle at 0,0 */
	B1.x = -1; B1.y = 0;
	B2.x = 0 ; B2.y = 1;
	B3.x = 1 ; B3.y = 0;

	/* Arc above the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -1; A1.y = 3;
	A2.x = 0 ; A2.y = 2;
	A3.x = 1 ; A3.y = 3;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Arc grazes the unit semicircle */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -1; A1.y = 2;
	A2.x = 0 ; A2.y = 1;
	A3.x = 1 ; A3.y = 2;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0, 0.000001);

	/* Circles intersect, but arcs do not */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -1; A1.y =  1;
	A2.x =  0; A2.y =  2;
	A3.x =  1; A3.y =  1;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2)-1, 0.000001);

	/* Circles and arcs intersect */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -1; A1.y =  1;
	A2.x =  0; A2.y =  0;
	A3.x =  1; A3.y =  1;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0, 0.000001);

	/* inscribed and closest on arcs */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -0.5; A1.y = 0.0;
	A2.x =  0.0; A2.y = 0.5;
	A3.x =  0.5; A3.y = 0.0;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	//printf("distance %g\n", dl.distance);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* inscribed and closest not on arcs */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	A1.x = -0.5; A1.y =  0.0;
	A2.x =  0.0; A2.y = -0.5;
	A3.x =  0.5; A3.y =  0.0;
	rv = lw_dist2d_arc_arc(&A1, &A2, &A3, &B1, &B2, &B3, &dl);
	//printf("distance %g\n", dl.distance);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);
}

static void
test_lw_arc_length(void)
{
/* double lw_arc_length(const POINT2D *A1, const POINT2D *A2, const POINT2D *A3) */

	POINT2D A1, A2, A3;
	double d;
	
	/* Unit semicircle at 0,0 */
	A1.x = -1; A1.y = 0;
	A2.x = 0 ; A2.y = 1;
	A3.x = 1 ; A3.y = 0;

	/* Arc above the unit semicircle */
	d = lw_arc_length(&A1, &A2, &A3);
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI, 0.000001);
	d = lw_arc_length(&A3, &A2, &A1);
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI, 0.000001);

	/* Unit semicircle at 0,0 */
	A1.x = 0; A1.y = 1;
	A2.x = 1; A2.y = 0;
	A3.x = 0; A3.y = -1;

	/* Arc to right of the unit semicircle */
	d = lw_arc_length(&A1, &A2, &A3);
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI, 0.000001);
	d = lw_arc_length(&A3, &A2, &A1);
	CU_ASSERT_DOUBLE_EQUAL(d, M_PI, 0.000001);

	/* Unit 3/4 circle at 0,0 */
	A1.x = -1; A1.y = 0;
	A2.x = 1; A2.y = 0;
	A3.x = 0; A3.y = -1;

	/* Arc to right of the unit semicircle */
	d = lw_arc_length(&A1, &A2, &A3);
	CU_ASSERT_DOUBLE_EQUAL(d, 3*M_PI_2, 0.000001);
	d = lw_arc_length(&A3, &A2, &A1);
	CU_ASSERT_DOUBLE_EQUAL(d, 3*M_PI_2, 0.000001);
}

static void
test_lw_dist2d_pt_ptarrayarc(void)
{
	/* lw_dist2d_pt_ptarrayarc(const POINT2D *p, const POINTARRAY *pa, DISTPTS *dl) */
	DISTPTS dl;
	int rv;
	LWLINE *lwline;
	POINT2D P;

	/* Unit semi-circle above X axis */
	lwline = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-1 0, 0 1, 1 0)"));
	
	/* Point at origin */
	P.x = P.y = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Point above arc on Y axis */
	P.y = 2;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Point 45 degrees off arc, 2 radii from center */
	P.y = P.x = 2 * cos(M_PI_4);
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Four unit semi-circles surrounding the 2x2 box around origin */
	lwline_free(lwline);
	lwline = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-1 -1, -2 0, -1 1, 0 2, 1 1, 2 0, 1 -1, 0 -2, -1 -1)"));

	/* Point at origin */
	P.x = P.y = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0), 0.000001);

	/* Point on box edge */
	P.x = -1; P.y = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Point within a semicircle lobe */
	P.x = -1.5; P.y = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Point outside a semicircle lobe */
	P.x = -2.5; P.y = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Point outside a semicircle lobe */
	P.y = -2.5; P.x = 0;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	/* Point outside a semicircle lobe */
	P.y = 2; P.x = 1;
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_pt_ptarrayarc(&P, lwline->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, sqrt(2.0)-1.0, 0.000001);

	/* Clean up */
	lwline_free(lwline);
}

static void
test_lw_dist2d_ptarray_ptarrayarc(void)
{
	/* int lw_dist2d_ptarray_ptarrayarc(const POINTARRAY *pa, const POINTARRAY *pb, DISTPTS *dl) */
	DISTPTS dl;
	int rv;
	LWLINE *lwline1;
	LWLINE *lwline2;

	/* Unit semi-circle above X axis */
	lwline1 = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-1 0, 0 1, 1 0)"));
	
	/* Line above top of semi-circle */
	lwline2 = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-2 2, -1 2, 1 2, 2 2)"));
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_ptarray_ptarrayarc(lwline2->points, lwline1->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Reversed arguments, should fail */
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	cu_error_msg_reset();
	rv = lw_dist2d_ptarray_ptarrayarc(lwline1->points, lwline2->points, &dl);
	//printf("%s\n", cu_error_msg);
	CU_ASSERT_EQUAL( rv, LW_FAILURE );
	CU_ASSERT_STRING_EQUAL("lw_dist2d_ptarray_ptarrayarc called with non-arc input", cu_error_msg);

	lwline_free(lwline2);
	
	/* Line along side of semi-circle */
	lwline2 = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-2 -3, -2 -2, -2 2, -2 3)"));
	lw_dist2d_distpts_init(&dl, DIST_MIN);
	rv = lw_dist2d_ptarray_ptarrayarc(lwline2->points, lwline1->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 1, 0.000001);

	/* Four unit semi-circles surrounding the 2x2 box around origin */
	lwline_free(lwline1);
	lwline_free(lwline2);
	lwline1 = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-1 -1, -2 0, -1 1, 0 2, 1 1, 2 0, 1 -1, 0 -2, -1 -1)"));
	lwline2 = lwgeom_as_lwline(lwgeom_from_text("LINESTRING(-2.5 -3, -2.5 -2, -2.5 2, -2.5 3)"));
	rv = lw_dist2d_ptarray_ptarrayarc(lwline2->points, lwline1->points, &dl);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_DOUBLE_EQUAL(dl.distance, 0.5, 0.000001);

	lwline_free(lwline2);
	lwline_free(lwline1);
}

/*
** Used by test harness to register the tests in this file.
*/
void measures_suite_setup(void);
void measures_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("measures", NULL, NULL);
	PG_ADD_TEST(suite, test_mindistance2d_tolerance);
	PG_ADD_TEST(suite, test_rect_tree_contains_point);
	PG_ADD_TEST(suite, test_rect_tree_intersects_tree);
	PG_ADD_TEST(suite, test_lwgeom_segmentize2d);
	PG_ADD_TEST(suite, test_lwgeom_locate_along);
	PG_ADD_TEST(suite, test_lw_dist2d_pt_arc);
	PG_ADD_TEST(suite, test_lw_dist2d_seg_arc);
	PG_ADD_TEST(suite, test_lw_dist2d_arc_arc);
	PG_ADD_TEST(suite, test_lw_arc_length);
	PG_ADD_TEST(suite, test_lw_dist2d_pt_ptarrayarc);
	PG_ADD_TEST(suite, test_lw_dist2d_ptarray_ptarrayarc);
}
