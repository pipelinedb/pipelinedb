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

/*
** Global variables used by tests below
*/

/* Two-point objects */
POINTARRAY *pa21 = NULL;
POINTARRAY *pa22 = NULL;
LWLINE *l21 = NULL;
LWLINE *l22 = NULL;
/* Parsing support */
LWGEOM_PARSER_RESULT parse_result;


/*
** The suite initialization function.
** Create any re-used objects.
*/
static int init_cg_suite(void)
{
	pa21 = ptarray_construct(0, 0, 2);
	pa22 = ptarray_construct(0, 0, 2);
	l21 = lwline_construct(SRID_UNKNOWN, NULL, pa21);
	l22 = lwline_construct(SRID_UNKNOWN, NULL, pa22);
	return 0;

}

/*
** The suite cleanup function.
** Frees any global objects.
*/
static int clean_cg_suite(void)
{
	if ( l21 ) lwline_free(l21);
	if ( l22 ) lwline_free(l22);
	return 0;
}

/*
** Test left/right side.
*/
static void test_lw_segment_side(void)
{
	int rv = 0;
	POINT2D p1, p2, q;

	/* Vertical line at x=0 */
	p1.x = 0.0;
	p1.y = 0.0;
	p2.x = 0.0;
	p2.y = 1.0;

	/* On the left */
	q.x = -2.0;
	q.y = 1.5;
	rv = lw_segment_side(&p1, &p2, &q);
	//printf("left %g\n",rv);
	CU_ASSERT(rv < 0);

	/* On the right */
	q.x = 2.0;
	rv = lw_segment_side(&p1, &p2, &q);
	//printf("right %g\n",rv);
	CU_ASSERT(rv > 0);

	/* On the line */
	q.x = 0.0;
	rv = lw_segment_side(&p1, &p2, &q);
	//printf("on line %g\n",rv);
	CU_ASSERT_EQUAL(rv, 0);

}

/*
** Test crossings side.
*/
static void test_lw_segment_intersects(void)
{

#define setpoint(p, x1, y1) {(p).x = (x1); (p).y = (y1);}

	POINT2D p1, p2, q1, q2;

	/* P: Vertical line at x=0 */
	setpoint(p1, 0.0, 0.0);
	p1.x = 0.0;
	p1.y = 0.0;
	p2.x = 0.0;
	p2.y = 1.0;

	/* Q: Horizontal line crossing left to right */
	q1.x = -0.5;
	q1.y = 0.5;
	q2.x = 0.5;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_RIGHT );

	/* Q: Horizontal line crossing right to left */
	q1.x = 0.5;
	q1.y = 0.5;
	q2.x = -0.5;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_LEFT );

	/* Q: Horizontal line not crossing right to left */
	q1.x = 0.5;
	q1.y = 1.5;
	q2.x = -0.5;
	q2.y = 1.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal line crossing at second vertex right to left */
	q1.x = 0.5;
	q1.y = 1.0;
	q2.x = -0.5;
	q2.y = 1.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal line crossing at first vertex right to left */
	q1.x = 0.5;
	q1.y = 0.0;
	q2.x = -0.5;
	q2.y = 0.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_LEFT );

	/* Q: Diagonal line with large range crossing at first vertex right to left */
	q1.x = 0.5;
	q1.y = 10.0;
	q2.x = -0.5;
	q2.y = -10.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_LEFT );

	/* Q: Diagonal line with large range crossing at second vertex right to left */
	q1.x = 0.5;
	q1.y = 11.0;
	q2.x = -0.5;
	q2.y = -9.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal touching from left at second vertex*/
	q1.x = -0.5;
	q1.y = 0.5;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal touching from right at first vertex */
	q1.x = 0.0;
	q1.y = 0.5;
	q2.x = 0.5;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_RIGHT );

	/* Q: Horizontal touching from left and far below on second vertex */
	q1.x = -0.5;
	q1.y = -10.5;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal touching from right and far above on second vertex */
	q1.x = 0.5;
	q1.y = 10.5;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Co-linear from top */
	q1.x = 0.0;
	q1.y = 10.0;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_COLINEAR );

	/* Q: Co-linear from bottom */
	q1.x = 0.0;
	q1.y = -10.0;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_COLINEAR );

	/* Q: Co-linear contained */
	q1.x = 0.0;
	q1.y = 0.4;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_COLINEAR );

	/* Q: Horizontal touching at end point from left */
	q1.x = -0.5;
	q1.y = 1.0;
	q2.x = 0.0;
	q2.y = 1.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_NO_INTERSECTION );

	/* Q: Horizontal touching at end point from right */
	q1.x = 0.0;
	q1.y = 1.0;
	q2.x = 0.0;
	q2.y = 0.5;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_COLINEAR );

	/* Q: Horizontal touching at start point from left */
	q1.x = 0.0;
	q1.y = 0.0;
	q2.x = -0.5;
	q2.y = 0.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_LEFT );

	/* Q: Horizontal touching at start point from right */
	q1.x = 0.0;
	q1.y = 0.0;
	q2.x = 0.5;
	q2.y = 0.0;
	CU_ASSERT( lw_segment_intersects(&p1, &p2, &q1, &q2) == SEG_CROSS_RIGHT );

}

static void test_lwline_crossing_short_lines(void)
{

	POINT4D p;

	/*
	** Simple test, two two-point lines
	*/

	/* Vertical line from 0,0 to 1,1 */
	p.x = 0.0;
	p.y = 0.0;
	ptarray_set_point4d(pa21, 0, &p);
	p.y = 1.0;
	ptarray_set_point4d(pa21, 1, &p);

	/* Horizontal, crossing mid-segment */
	p.x = -0.5;
	p.y = 0.5;
	ptarray_set_point4d(pa22, 0, &p);
	p.x = 0.5;
	ptarray_set_point4d(pa22, 1, &p);

	CU_ASSERT( lwline_crossing_direction(l21, l22) == LINE_CROSS_RIGHT );

	/* Horizontal, crossing at top end vertex (end crossings don't count) */
	p.x = -0.5;
	p.y = 1.0;
	ptarray_set_point4d(pa22, 0, &p);
	p.x = 0.5;
	ptarray_set_point4d(pa22, 1, &p);

	CU_ASSERT( lwline_crossing_direction(l21, l22) == LINE_NO_CROSS );

	/* Horizontal, crossing at bottom end vertex */
	p.x = -0.5;
	p.y = 0.0;
	ptarray_set_point4d(pa22, 0, &p);
	p.x = 0.5;
	ptarray_set_point4d(pa22, 1, &p);

	CU_ASSERT( lwline_crossing_direction(l21, l22) == LINE_CROSS_RIGHT );

	/* Horizontal, no crossing */
	p.x = -0.5;
	p.y = 2.0;
	ptarray_set_point4d(pa22, 0, &p);
	p.x = 0.5;
	ptarray_set_point4d(pa22, 1, &p);

	CU_ASSERT( lwline_crossing_direction(l21, l22) == LINE_NO_CROSS );

	/* Vertical, no crossing */
	p.x = -0.5;
	p.y = 0.0;
	ptarray_set_point4d(pa22, 0, &p);
	p.y = 1.0;
	ptarray_set_point4d(pa22, 1, &p);

	CU_ASSERT( lwline_crossing_direction(l21, l22) == LINE_NO_CROSS );

}

static void test_lwline_crossing_long_lines(void)
{
	LWLINE *l51;
	LWLINE *l52;
	/*
	** More complex test, longer lines and multiple crossings
	*/
	/* Vertical line with vertices at y integers */
	l51 = (LWLINE*)lwgeom_from_wkt("LINESTRING(0 0, 0 1, 0 2, 0 3, 0 4)", LW_PARSER_CHECK_NONE);

	/* Two crossings at segment midpoints */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, -1 1.5, 1 3, 1 4, 1 5)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_MULTICROSS_END_SAME_FIRST_LEFT );
	lwline_free(l52);

	/* One crossing at interior vertex */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 0 1, -1 1, -1 2, -1 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_CROSS_LEFT );
	lwline_free(l52);

	/* Two crossings at interior vertices */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 0 1, -1 1, 0 3, 1 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_MULTICROSS_END_SAME_FIRST_LEFT );
	lwline_free(l52);

	/* Two crossings, one at the first vertex on at interior vertex */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 0, 0 0, -1 1, 0 3, 1 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_MULTICROSS_END_SAME_FIRST_LEFT );
	lwline_free(l52);

	/* Two crossings, one at the first vertex on the next interior vertex */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 0, 0 0, -1 1, 0 1, 1 2)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_MULTICROSS_END_SAME_FIRST_LEFT );
	lwline_free(l52);

	/* Three crossings, two at midpoints, one at vertex */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(0.5 1, -1 0.5, 1 2, -1 2, -1 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_MULTICROSS_END_LEFT );
	lwline_free(l52);

	/* One mid-point co-linear crossing */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 0 1.5, 0 2.5, -1 3, -1 4)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_CROSS_LEFT );
	lwline_free(l52);

	/* One on-vertices co-linear crossing */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 0 1, 0 2, -1 4, -1 4)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_CROSS_LEFT );
	lwline_free(l52);

	/* No crossing, but end on a co-linearity. */
	l52 = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1, 1 2, 1 3, 0 3, 0 4)", LW_PARSER_CHECK_NONE);
	CU_ASSERT( lwline_crossing_direction(l51, l52) == LINE_NO_CROSS );
	lwline_free(l52);

	lwline_free(l51);

}


static void test_lwline_crossing_bugs(void)
{
	LWLINE *l1;
	LWLINE *l2;

	l1 = (LWLINE*)lwgeom_from_wkt("LINESTRING(2.99 90.16,71 74,20 140,171 154)", LW_PARSER_CHECK_NONE);
	l2 = (LWLINE*)lwgeom_from_wkt("LINESTRING(25 169,89 114,40 70,86 43)", LW_PARSER_CHECK_NONE);

	CU_ASSERT( lwline_crossing_direction(l1, l2) == LINE_MULTICROSS_END_RIGHT );
	lwline_free(l1);
	lwline_free(l2);

}

static void test_lwpoint_set_ordinate(void)
{
	POINT4D p;

	p.x = 0.0;
	p.y = 0.0;
	p.z = 0.0;
	p.m = 0.0;

	lwpoint_set_ordinate(&p, 'X', 1.5);
	CU_ASSERT_EQUAL( p.x, 1.5 );

	lwpoint_set_ordinate(&p, 'M', 2.5);
	CU_ASSERT_EQUAL( p.m, 2.5 );

	lwpoint_set_ordinate(&p, 'Z', 3.5);
	CU_ASSERT_EQUAL( p.z, 3.5 );

}

static void test_lwpoint_get_ordinate(void)
{
	POINT4D p;

	p.x = 10.0;
	p.y = 20.0;
	p.z = 30.0;
	p.m = 40.0;

	CU_ASSERT_EQUAL( lwpoint_get_ordinate(&p, 'X'), 10.0 );
	CU_ASSERT_EQUAL( lwpoint_get_ordinate(&p, 'Y'), 20.0 );
	CU_ASSERT_EQUAL( lwpoint_get_ordinate(&p, 'Z'), 30.0 );
	CU_ASSERT_EQUAL( lwpoint_get_ordinate(&p, 'M'), 40.0 );

}

static void test_point_interpolate(void)
{
	POINT4D p, q, r;
	int rv = 0;

	p.x = 10.0;
	p.y = 20.0;
	p.z = 30.0;
	p.m = 40.0;

	q.x = 20.0;
	q.y = 30.0;
	q.z = 40.0;
	q.m = 50.0;

	rv = point_interpolate(&p, &q, &r, 1, 1, 'Z', 35.0);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_EQUAL( r.x, 15.0);

	rv = point_interpolate(&p, &q, &r, 1, 1, 'M', 41.0);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_EQUAL( r.y, 21.0);

	rv = point_interpolate(&p, &q, &r, 1, 1, 'M', 50.0);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_EQUAL( r.y, 30.0);

	rv = point_interpolate(&p, &q, &r, 1, 1, 'M', 40.0);
	CU_ASSERT_EQUAL( rv, LW_SUCCESS );
	CU_ASSERT_EQUAL( r.y, 20.0);

}

static void test_lwline_clip(void)
{
	LWCOLLECTION *c;
	LWLINE *line = NULL;
	LWLINE *l51 = NULL;
	char *ewkt;

	/* Vertical line with vertices at y integers */
	l51 = (LWLINE*)lwgeom_from_wkt("LINESTRING(0 0, 0 1, 0 2, 0 3, 0 4)", LW_PARSER_CHECK_NONE);

	/* Clip in the middle, mid-range. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', 1.5, 2.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 1.5,0 2,0 2.5))");
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip off the top. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', 3.5, 5.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 3.5,0 4))");
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip off the bottom. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', -1.5, 2.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 0,0 1,0 2,0 2.5))" );
	lwfree(ewkt);
	lwcollection_free(c);

	/* Range holds entire object. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', -1.5, 5.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 0,0 1,0 2,0 3,0 4))" );
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip on vertices. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', 1.0, 2.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 1,0 2))" );
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip on vertices off the bottom. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', -1.0, 2.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 0,0 1,0 2))" );
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip on top. */
	c = lwline_clip_to_ordinate_range(l51, 'Y', -1.0, 0.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "GEOMETRYCOLLECTION(POINT(0 0))" );
	lwfree(ewkt);
	lwcollection_free(c);

	/* ST_LocateBetweenElevations(ST_GeomFromEWKT('LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)'), 1, 2)) */
	line = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)", LW_PARSER_CHECK_NONE);
	c = lwline_clip_to_ordinate_range(line, 'Z', 1.0, 2.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((2 2 2,1 1 1))" );
	lwfree(ewkt);
	lwcollection_free(c);
	lwline_free(line);

	/* ST_LocateBetweenElevations('LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)', 1, 2)) */
	line = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)", LW_PARSER_CHECK_NONE);
	c = lwline_clip_to_ordinate_range(line, 'Z', 1.0, 2.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("a = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((2 2 2,1 1 1))" );
	lwfree(ewkt);
	lwcollection_free(c);
	lwline_free(line);

	/* ST_LocateBetweenElevations('LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)', 1, 1)) */
	line = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 2 3, 4 5 6, 6 6 6, 1 1 1)", LW_PARSER_CHECK_NONE);
	c = lwline_clip_to_ordinate_range(line, 'Z', 1.0, 1.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("b = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "GEOMETRYCOLLECTION(POINT(1 1 1))" );
	lwfree(ewkt);
	lwcollection_free(c);
	lwline_free(line);

	/* ST_LocateBetweenElevations('LINESTRING(1 1 1, 1 2 2)', 1,1) */
	line = (LWLINE*)lwgeom_from_wkt("LINESTRING(1 1 1, 1 2 2)", LW_PARSER_CHECK_NONE);
	c = lwline_clip_to_ordinate_range(line, 'Z', 1.0, 1.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "GEOMETRYCOLLECTION(POINT(1 1 1))" );
	lwfree(ewkt);
	lwcollection_free(c);
	lwline_free(line);

	lwline_free(l51);

}

static void test_lwmline_clip(void)
{
	LWCOLLECTION *c;
	char *ewkt;
	LWMLINE *mline = NULL;
	LWLINE *line = NULL;

	/*
	** Set up the input line. Trivial one-member case.
	*/
	mline = (LWMLINE*)lwgeom_from_wkt("MULTILINESTRING((0 0,0 1,0 2,0 3,0 4))", LW_PARSER_CHECK_NONE);

	/* Clip in the middle, mid-range. */
	c = lwmline_clip_to_ordinate_range(mline, 'Y', 1.5, 2.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0 1.5,0 2,0 2.5))");
	lwfree(ewkt);
	lwcollection_free(c);

	lwmline_free(mline);

	/*
	** Set up the input line. Two-member case.
	*/
	mline = (LWMLINE*)lwgeom_from_wkt("MULTILINESTRING((1 0,1 1,1 2,1 3,1 4), (0 0,0 1,0 2,0 3,0 4))", LW_PARSER_CHECK_NONE);

	/* Clip off the top. */
	c = lwmline_clip_to_ordinate_range(mline, 'Y', 3.5, 5.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((1 3.5,1 4),(0 3.5,0 4))");
	lwfree(ewkt);
	lwcollection_free(c);

	lwmline_free(mline);

	/*
	** Set up staggered input line to create multi-type output.
	*/
	mline = (LWMLINE*)lwgeom_from_wkt("MULTILINESTRING((1 0,1 -1,1 -2,1 -3,1 -4), (0 0,0 1,0 2,0 3,0 4))", LW_PARSER_CHECK_NONE);

	/* Clip from 0 upwards.. */
	c = lwmline_clip_to_ordinate_range(mline, 'Y', 0.0, 2.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "GEOMETRYCOLLECTION(POINT(1 0),LINESTRING(0 0,0 1,0 2,0 2.5))");
	lwfree(ewkt);
	lwcollection_free(c);

	lwmline_free(mline);

	/*
	** Set up input line from MAC
	*/
	line = (LWLINE*)lwgeom_from_wkt("LINESTRING(0 0 0 0,1 1 1 1,2 2 2 2,3 3 3 3,4 4 4 4,3 3 3 5,2 2 2 6,1 1 1 7,0 0 0 8)", LW_PARSER_CHECK_NONE);

	/* Clip from 3 to 3.5 */
	c = lwline_clip_to_ordinate_range(line, 'Z', 3.0, 3.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((3 3 3 3,3.5 3.5 3.5 3.5),(3.5 3.5 3.5 4.5,3 3 3 5))");
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip from 2 to 3.5 */
	c = lwline_clip_to_ordinate_range(line, 'Z', 2.0, 3.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((2 2 2 2,3 3 3 3,3.5 3.5 3.5 3.5),(3.5 3.5 3.5 4.5,3 3 3 5,2 2 2 6))");
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip from 3 to 4 */
	c = lwline_clip_to_ordinate_range(line, 'Z', 3.0, 4.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((3 3 3 3,4 4 4 4,3 3 3 5))");
	lwfree(ewkt);
	lwcollection_free(c);

	/* Clip from 2 to 3 */
	c = lwline_clip_to_ordinate_range(line, 'Z', 2.0, 3.0);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((2 2 2 2,3 3 3 3),(3 3 3 5,2 2 2 6))");
	lwfree(ewkt);
	lwcollection_free(c);


	lwline_free(line);

}



static void test_lwline_clip_big(void)
{
	POINTARRAY *pa = ptarray_construct(1, 0, 3);
	LWLINE *line = lwline_construct(SRID_UNKNOWN, NULL, pa);
	LWCOLLECTION *c;
	char *ewkt;
	POINT4D p;

	p.x = 0.0;
	p.y = 0.0;
	p.z = 0.0;
	ptarray_set_point4d(pa, 0, &p);

	p.x = 1.0;
	p.y = 1.0;
	p.z = 1.0;
	ptarray_set_point4d(pa, 1, &p);

	p.x = 2.0;
	p.y = 2.0;
	p.z = 2.0;
	ptarray_set_point4d(pa, 2, &p);

	c = lwline_clip_to_ordinate_range(line, 'Z', 0.5, 1.5);
	ewkt = lwgeom_to_ewkt((LWGEOM*)c);
	//printf("c = %s\n", ewkt);
	CU_ASSERT_STRING_EQUAL(ewkt, "MULTILINESTRING((0.5 0.5 0.5,1 1 1,1.5 1.5 1.5))" );

	lwfree(ewkt);
	lwcollection_free(c);
	lwline_free(line);
}

static void test_geohash_precision(void)
{
	GBOX bbox;
	GBOX bounds;
	int precision = 0;
	gbox_init(&bbox);
	gbox_init(&bounds);

	bbox.xmin = 23.0;
	bbox.xmax = 23.0;
	bbox.ymin = 25.2;
	bbox.ymax = 25.2;
	precision = lwgeom_geohash_precision(bbox, &bounds);
	//printf("\nprecision %d\n",precision);
	CU_ASSERT_EQUAL(precision, 20);

	bbox.xmin = 23.0;
	bbox.ymin = 23.0;
	bbox.xmax = 23.1;
	bbox.ymax = 23.1;
	precision = lwgeom_geohash_precision(bbox, &bounds);
	//printf("precision %d\n",precision);
	CU_ASSERT_EQUAL(precision, 3);

	bbox.xmin = 23.0;
	bbox.ymin = 23.0;
	bbox.xmax = 23.0001;
	bbox.ymax = 23.0001;
	precision = lwgeom_geohash_precision(bbox, &bounds);
	//printf("precision %d\n",precision);
	CU_ASSERT_EQUAL(precision, 7);

}

static void test_geohash_point(void)
{
	char *geohash;

	geohash = geohash_point(0, 0, 16);
	//printf("\ngeohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "s000000000000000");
	lwfree(geohash);

	geohash = geohash_point(90, 0, 16);
	//printf("\ngeohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "w000000000000000");
	lwfree(geohash);

	geohash = geohash_point(20.012345, -20.012345, 15);
	//printf("\ngeohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "kkqnpkue9ktbpe5");
	lwfree(geohash);

}

static void test_geohash(void)
{
	LWPOINT *lwpoint = NULL;
	LWLINE *lwline = NULL;
	LWMLINE *lwmline = NULL;
	char *geohash = NULL;

	lwpoint = (LWPOINT*)lwgeom_from_wkt("POINT(23.0 25.2)", LW_PARSER_CHECK_NONE);
	geohash = lwgeom_geohash((LWGEOM*)lwpoint,0);
	//printf("\ngeohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "ss2r77s0du7p2ewb8hmx");
	lwpoint_free(lwpoint);
	lwfree(geohash);

	lwpoint = (LWPOINT*)lwgeom_from_wkt("POINT(23.0 25.2 2.0)", LW_PARSER_CHECK_NONE);
	geohash = lwgeom_geohash((LWGEOM*)lwpoint,0);
	//printf("geohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "ss2r77s0du7p2ewb8hmx");
	lwpoint_free(lwpoint);
	lwfree(geohash);

	lwline = (LWLINE*)lwgeom_from_wkt("LINESTRING(23.0 23.0,23.1 23.1)", LW_PARSER_CHECK_NONE);
	geohash = lwgeom_geohash((LWGEOM*)lwline,0);
	//printf("geohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "ss0");
	lwline_free(lwline);
	lwfree(geohash);

	lwline = (LWLINE*)lwgeom_from_wkt("LINESTRING(23.0 23.0,23.001 23.001)", LW_PARSER_CHECK_NONE);
	geohash = lwgeom_geohash((LWGEOM*)lwline,0);
	//printf("geohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "ss06g7");
	lwline_free(lwline);
	lwfree(geohash);

	lwmline = (LWMLINE*)lwgeom_from_wkt("MULTILINESTRING((23.0 23.0,23.1 23.1),(23.0 23.0,23.1 23.1))", LW_PARSER_CHECK_NONE);
	geohash = lwgeom_geohash((LWGEOM*)lwmline,0);
	//printf("geohash %s\n",geohash);
	CU_ASSERT_STRING_EQUAL(geohash, "ss0");
	lwmline_free(lwmline);
	lwfree(geohash);
}

static void test_isclosed(void)
{
	LWGEOM *geom;

	/* LINESTRING */

	/* Not Closed on 2D */
	geom = lwgeom_from_wkt("LINESTRING(1 2,3 4)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwline_is_closed((LWLINE *) geom));
	lwgeom_free(geom);

	/* Closed on 2D */
	geom = lwgeom_from_wkt("LINESTRING(1 2,3 4,1 2)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwline_is_closed((LWLINE *) geom));
	lwgeom_free(geom);

	/* Not closed on 3D */
	geom = lwgeom_from_wkt("LINESTRING(1 2 3,4 5 6)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwline_is_closed((LWLINE *) geom));
	lwgeom_free(geom);

	/* Closed on 3D */
	geom = lwgeom_from_wkt("LINESTRING(1 2 3,4 5 6,1 2 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwline_is_closed((LWLINE *) geom));
	lwgeom_free(geom);

	/* Closed on 4D, even if M is not the same */
	geom = lwgeom_from_wkt("LINESTRING(1 2 3 4,5 6 7 8,1 2 3 0)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwline_is_closed((LWLINE *) geom));
	lwgeom_free(geom);


	/* CIRCULARSTRING */

	/* Not Closed on 2D */
	geom = lwgeom_from_wkt("CIRCULARSTRING(1 2,3 4,5 6)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcircstring_is_closed((LWCIRCSTRING *) geom));
	lwgeom_free(geom);

	/* Closed on 2D */
	geom = lwgeom_from_wkt("CIRCULARSTRING(1 2,3 4,1 2)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcircstring_is_closed((LWCIRCSTRING *) geom));
	lwgeom_free(geom);

	/* Not closed on 3D */
	geom = lwgeom_from_wkt("CIRCULARSTRING(1 2 3,4 5 6,7 8 9)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcircstring_is_closed((LWCIRCSTRING *) geom));
	lwgeom_free(geom);

	/* Closed on 3D */
	geom = lwgeom_from_wkt("CIRCULARSTRING(1 2 3,4 5 6,1 2 3)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcircstring_is_closed((LWCIRCSTRING *) geom));
	lwgeom_free(geom);

	/* Closed on 4D, even if M is not the same */
	geom = lwgeom_from_wkt("CIRCULARSTRING(1 2 3 4,5 6 7 8,1 2 3 0)", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcircstring_is_closed((LWCIRCSTRING *) geom));
	lwgeom_free(geom);


	/* COMPOUNDCURVE */

	/* Not Closed on 2D */
	geom = lwgeom_from_wkt("COMPOUNDCURVE(CIRCULARSTRING(1 2,3 4,1 2),(1 2,7 8,5 6))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("COMPOUNDCURVE((1 2,3 4,1 2),CIRCULARSTRING(1 2,7 8,5 6))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	/* Closed on 2D */
	geom = lwgeom_from_wkt("COMPOUNDCURVE(CIRCULARSTRING(1 2,3 4,5 6), (5 6,7 8,1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("COMPOUNDCURVE((1 2,3 4,5 6),CIRCULARSTRING(5 6,7 8,1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	/* Not Closed on 3D */
	geom = lwgeom_from_wkt("COMPOUNDCURVE(CIRCULARSTRING(1 2 3,4 5 6,1 2 3),(1 2 3,7 8 9,10 11 12))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("COMPOUNDCURVE((1 2 3,4 5 6,1 2 3),CIRCULARSTRING(1 2 3,7 8 9,10 11 12))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(!lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	/* Closed on 3D */
	geom = lwgeom_from_wkt("COMPOUNDCURVE(CIRCULARSTRING(1 2 3,4 5 6,7 8 9),(7 8 9,10 11 12,1 2 3))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	geom = lwgeom_from_wkt("COMPOUNDCURVE((1 2 3,4 5 6,7 8 9),CIRCULARSTRING(7 8 9,10 11 12,1 2 3))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);

	/* Closed on 4D, even if M is not the same */
	geom = lwgeom_from_wkt("COMPOUNDCURVE((1 2 3 4,5 6 7 8,9 10 11 12),CIRCULARSTRING(9 10 11 12,13 14 15 16,1 2 3 0))", LW_PARSER_CHECK_NONE);
	CU_ASSERT(lwcompound_is_closed((LWCOMPOUND *) geom));
	lwgeom_free(geom);
}


static void test_geohash_point_as_int(void)
{
	unsigned int gh;
	POINT2D p;
	
	p.x = 50; p.y = 35;
	gh = geohash_point_as_int(&p);
	CU_ASSERT_EQUAL(gh, (unsigned int)3440103613);
	p.x = 140; p.y = 45;
	gh = geohash_point_as_int(&p);
	CU_ASSERT_EQUAL(gh, (unsigned int)3982480893);
	p.x = 140; p.y = 55;
	gh = geohash_point_as_int(&p);
	CU_ASSERT_EQUAL(gh, (unsigned int)4166944232);	
}

static void test_lwgeom_simplify(void)
{
		LWGEOM *l;
		char *ewkt;

		/* Not simplifiable */
		l = lwgeom_simplify(lwgeom_from_wkt("LINESTRING(0 0, 50 1.00001, 100 0)", LW_PARSER_CHECK_NONE), 1.0);
		ewkt = lwgeom_to_ewkt(l);
		CU_ASSERT_STRING_EQUAL(ewkt, "LINESTRING(0 0,50 1.00001,100 0)");
		lwgeom_free(l);
		lwfree(ewkt);

		/* Simplifiable */
		l = lwgeom_simplify(lwgeom_from_wkt("LINESTRING(0 0,50 0.99999,100 0)", LW_PARSER_CHECK_NONE), 1.0);
		ewkt = lwgeom_to_ewkt(l);
		CU_ASSERT_STRING_EQUAL(ewkt, "LINESTRING(0 0,100 0)");
		lwgeom_free(l);
		lwfree(ewkt);
}

/*
** Used by test harness to register the tests in this file.
*/
void algorithms_suite_setup(void);
void algorithms_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("computational_geometry", init_cg_suite, clean_cg_suite);
	PG_ADD_TEST(suite,test_lw_segment_side);
	PG_ADD_TEST(suite,test_lw_segment_intersects);
	PG_ADD_TEST(suite,test_lwline_crossing_short_lines);
	PG_ADD_TEST(suite,test_lwline_crossing_long_lines);
	PG_ADD_TEST(suite,test_lwline_crossing_bugs);
	PG_ADD_TEST(suite,test_lwpoint_set_ordinate);
	PG_ADD_TEST(suite,test_lwpoint_get_ordinate);
	PG_ADD_TEST(suite,test_point_interpolate);
	PG_ADD_TEST(suite,test_lwline_clip);
	PG_ADD_TEST(suite,test_lwline_clip_big);
	PG_ADD_TEST(suite,test_lwmline_clip);
	PG_ADD_TEST(suite,test_geohash_point);
	PG_ADD_TEST(suite,test_geohash_precision);
	PG_ADD_TEST(suite,test_geohash);
	PG_ADD_TEST(suite,test_geohash_point_as_int);
	PG_ADD_TEST(suite,test_isclosed);
	PG_ADD_TEST(suite,test_lwgeom_simplify);
}
