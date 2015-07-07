/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2012 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "CUnit/Basic.h"
#include "cu_tester.h"

#include "liblwgeom.h"
#include "liblwgeom_internal.h"

/*
 * TODO: change lwgeom_same to lwgeom_equals
 * (requires porting predicates to liblwgeom)
 */
#define check_geom_equal(gobt, gexp) do { \
	char *obt, *exp; \
	LWGEOM *ngobt, *ngexp; \
	ngobt = lwgeom_normalize(gobt); \
	ngexp = lwgeom_normalize(gexp); \
	if ( ! lwgeom_same((ngobt), (ngexp)) ) { \
 		obt = lwgeom_to_wkt((ngobt), WKT_ISO, 8, NULL); \
 		exp = lwgeom_to_wkt((ngexp), WKT_ISO, 8, NULL); \
		printf(" Failure at %s:%d\n", __FILE__, __LINE__); \
		printf(" Exp: %s\n", exp); \
		printf(" Obt: %s\n", obt); \
		free(obt); free(exp); \
    lwgeom_free(ngobt); lwgeom_free(ngexp); \
		CU_ASSERT(0); \
	} else { \
    lwgeom_free(ngobt); lwgeom_free(ngexp); \
    CU_ASSERT(1); \
  } \
} while (0)

/*
             +-----+
             |     |
       +-----+-----+
       |     |
       +-----+
*/
static void buildarea1(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 20, 10 20, 10 10))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT( gin != NULL );

	gexp = lwgeom_from_wkt(
"MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT( gexp != NULL );

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);
}

/*
       +-----+-----+
       |     |     |
       +-----+-----+
*/
static void buildarea2(void)
{
	LWGEOM *gin, *gout, *gexp;

	/* because i don't trust that much prior tests...  ;) */
	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 0, 10 0, 10 10))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"POLYGON((0 0,0 10,10 10,20 10,20 0,10 0,0 0))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +-----------+
       |  +-----+  |
       |  |     |  |
       |  +-----+  |
       +-----------+
*/
static void buildarea3(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"POLYGON((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +-----------+
       |  +-----+  |
       |  | +-+ |  |
       |  | | | |  |
       |  | +-+ |  |
       |  +-----+  |
       +-----------+
*/
static void buildarea4(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"MULTIPOLYGON(((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2)),((8 8,8 12,12 12,12 8,8 8)))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +-----------+
       |  +-----+  | This time the innermost ring has 
       |  | +-+ |  | more points than the other (outer) two.
       |  | | | |  | 
       |  | +-+ |  |
       |  +-----+  |
       +-----------+
*/
static void buildarea4b(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2), (8 8, 8 9, 8 10, 8 11, 8 12, 9 12, 10 12, 11 12, 12 12, 12 11, 12 10, 12 9, 12 8, 11 8, 10 8, 9 8, 8 8))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"MULTIPOLYGON(((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2)),((8 8,8 9,8 10,8 11,8 12,9 12,10 12,11 12,12 12,12 11,12 10,12 9,12 8,11 8,10 8,9 8,8 8)))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +---------------+   
       |  +---------+  |   
       |  | +--+--+ |  |   
       |  | |  |  | |  |   
       |  | +--+--+ |  |   
       |  +---------+  |   
       +---------------+   
*/
static void buildarea5(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8),(10 8, 10 12))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"MULTIPOLYGON(((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2)),((8 8,8 12,12 12,12 8,8 8)))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +---------------+
       |  +----+----+  |
       |  |    |    |  |
       |  |    |    |  |
       |  |    |    |  |
       |  +----+----+  |
       +---------------+
*/
static void buildarea6(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(10 2, 10 18))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"POLYGON((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}

/*
       +--------------------+  +-------+
       |  +-----+   +----+  |  | +---+ |
       |  | +-+ |   |    |  |  | |   | |
       |  | | | |   +----+  |  | +---+ |
       |  | +-+ |   |    |  |  |       |
       |  | | | |   |    |  |  |       |
       |  | +-+ |   |    |  |  |       |
       |  +-----+   +----+  |  |       |
       +--------------------+  +-------+
*/
static void buildarea7(void)
{
	LWGEOM *gin, *gout, *gexp;

	cu_error_msg_reset();

	gin = lwgeom_from_wkt(
"MULTILINESTRING( (0 0, 70 0, 70 70, 0 70, 0 0), (10 10, 10 60, 40 60, 40 10, 10 10), (20 20, 20 30, 30 30, 30 20, 20 20), (20 30, 30 30, 30 50, 20 50, 20 30), (50 20, 60 20, 60 40, 50 40, 50 20), (50 40, 60 40, 60 60, 50 60, 50 40), (80 0, 110 0, 110 70, 80 70, 80 0), (90 60, 100 60, 100 50, 90 50, 90 60))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gin != NULL);

	gexp = lwgeom_from_wkt(
"MULTIPOLYGON(((80 0,80 70,110 70,110 0,80 0),(90 60,90 50,100 50,100 60,90 60)),((20 20,20 30,20 50,30 50,30 30,30 20,20 20)),((0 0,0 70,70 70,70 0,0 0),(10 10,40 10,40 60,10 60,10 10),(50 20,60 20,60 40,60 60,50 60,50 40,50 20)))",
		LW_PARSER_CHECK_NONE);
	CU_ASSERT(gexp != NULL);

	gout = lwgeom_buildarea(gin);
	CU_ASSERT(gout != NULL);

	check_geom_equal(gout, gexp);

	lwgeom_free(gout);
	lwgeom_free(gexp);
	lwgeom_free(gin);

}


void buildarea_suite_setup(void);
void buildarea_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("buildarea", NULL, NULL);
	PG_ADD_TEST(suite,buildarea1);
	PG_ADD_TEST(suite,buildarea2);
	PG_ADD_TEST(suite,buildarea3);
	PG_ADD_TEST(suite,buildarea4);
	PG_ADD_TEST(suite,buildarea4b);
	PG_ADD_TEST(suite,buildarea5);
	PG_ADD_TEST(suite,buildarea6);
	PG_ADD_TEST(suite,buildarea7);
}
