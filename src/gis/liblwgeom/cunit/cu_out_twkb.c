/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2014 Nicklas Avén
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
** Global variable to hold hex TWKB strings
*/
char *s;
char *w;

/*
** The suite initialization function.
** Create any re-used objects.
*/
static int init_twkb_out_suite(void)
{
	s = NULL;
	w = NULL;
	return 0;
}

/*
** The suite cleanup function.
** Frees any global objects.
*/
static int clean_twkb_out_suite(void)
{
	if (s) free(s);
	if (w) free(w);
	s = NULL;
	w = NULL;
	return 0;
}


/*
** Creating an input TWKB from a wkt string
*/
static void cu_twkb(char *wkt, int8_t prec_xy, int8_t prec_z, int8_t prec_m, uint8_t variant)
{
	LWGEOM *g = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	size_t twkb_size;
	uint8_t *twkb;
	if ( ! g )  lwnotice("input wkt is invalid: %s", wkt);
	twkb = lwgeom_to_twkb(g, variant, prec_xy,  prec_z, prec_m, &twkb_size);
	lwgeom_free(g);
	if ( s ) free(s);
	s = hexbytes_from_bytes(twkb, twkb_size);
	free(twkb);
}


/*
** Creating an input TWKB from a wkt string
*/
static void cu_twkb_idlist(char *wkt, int64_t *idlist, int8_t prec_xy, int8_t prec_z, int8_t prec_m, uint8_t variant)
{
	LWGEOM *g = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	LWGEOM *g_b;
	size_t twkb_size;
	uint8_t *twkb;
	if ( ! g )  lwnotice("input wkt is invalid: %s", wkt);
	twkb = lwgeom_to_twkb_with_idlist(g, idlist, variant, prec_xy,  prec_z, prec_m, &twkb_size);
	lwgeom_free(g);
	if ( s ) free(s);
	if ( w ) free(w);
	s = hexbytes_from_bytes(twkb, twkb_size);
	g_b = lwgeom_from_twkb(twkb, twkb_size, LW_PARSER_CHECK_NONE);
	w = lwgeom_to_ewkt(g_b);
	lwgeom_free(g_b);
	free(twkb);
}



static void test_twkb_out_point(void)
{

	cu_twkb("POINT EMPTY", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"0110");

	cu_twkb("POINT(0 0)", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"01000000");

	cu_twkb("POINT(0 0 0 0)", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"01080300000000");

	/* Point with bounding box */
	cu_twkb("POINT(0 0)", 0, 0, 0, TWKB_BBOX);
	CU_ASSERT_STRING_EQUAL(s,"0101000000000000");
	// printf("TWKB: %s\n",s);

	/* Adding a size paramters to X/Y */
	cu_twkb("POINT(0 0)", 0, 0, 0, TWKB_SIZE);
	CU_ASSERT_STRING_EQUAL(s,"0102020000");

	/* Adding a size paramters to X/Y/M */
	cu_twkb("POINTM(0 0 0)", 0, 0, 0, TWKB_SIZE);
	CU_ASSERT_STRING_EQUAL(s,"010A0203000000");

	/* Adding a size paramters to X/Y/Z/M */
	cu_twkb("POINT(0 0 0 0)", 0, 0, 0, TWKB_SIZE);
	CU_ASSERT_STRING_EQUAL(s,"010A030400000000");

	/* Since the third dimension is Z it shall get a precision of 1 decimal (third argument) */
	cu_twkb("POINTZ(1 1 1)", 0,1,2, 0);
	CU_ASSERT_STRING_EQUAL(s,"010845020214");

	/* Since the third dimension is M it shall get a precision of 2 decimals (fourth argument) */
	cu_twkb("POINTM(1 1 1)", 0,1,2, 0);
	// printf("\n%s\n", s);
	CU_ASSERT_STRING_EQUAL(s,"0108460202C801");
}

static void test_twkb_out_linestring(void)
{

	cu_twkb("LINESTRING(0 0,1 1)", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"02000200000202");
	// printf("TWKB: %s\n",s);

	cu_twkb("LINESTRING(0 0 1,1 1 2,2 2 3)", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"02080103000002020202020202");
	// printf("TWKB: %s\n",s);

	/* Line with bounding box */
	cu_twkb("LINESTRING(0 0,1 1,2 2)", 0, 0, 0, TWKB_BBOX);
	CU_ASSERT_STRING_EQUAL(s,"02010004000403000002020202");
	// printf("TWKB: %s\n",s);

	cu_twkb("LINESTRING EMPTY", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"0210");
	// printf("TWKB: %s\n",s);
}

static void test_twkb_out_polygon(void)
{
	cu_twkb("SRID=4;POLYGON((0 0 0, 0 1 0,1 1 0,1 0 0, 0 0 0))", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"0308010105000000000200020000000100010000");
	// printf("TWKB: %s\n",s);

	cu_twkb("SRID=14;POLYGON((0 0 0 1, 0 1 0 2,1 1 0 3,1 0 0 4, 0 0 0 5))", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"03080301050000000200020002020000020001000201000002");
	// printf("TWKB: %s\n",s);

	cu_twkb("POLYGON EMPTY", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"0310");
	// printf("TWKB: %s\n",s);
}

static void test_twkb_out_multipoint(void)
{
	cu_twkb("MULTIPOINT(0 0 0, 0 1 0,1 1 0,1 0 0, 0 0 0)", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"04080105000000000200020000000100010000");

	cu_twkb("MULTIPOINT(0 0 0, 0.26794919243112270647255365849413 1 3)",7 ,7 , 0, 0);
	//printf("WKB: %s",s);
	CU_ASSERT_STRING_EQUAL(s,"E4081D02000000888BC70280DAC409808ECE1C");
//	printf("TWKB: %s\n",s);
}

static void test_twkb_out_multilinestring(void) {}

static void test_twkb_out_multipolygon(void)
{
	cu_twkb("MULTIPOLYGON(((0 0 0, 0 1 0,1 1 0,1 0 0, 0 0 0)),((-1 -1 0,-1 2 0,2 2 0,2 -1 0,-1 -1 0),(0 0 0, 0 1 0,1 1 0,1 0 0, 0 0 0)))", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"060801020105000000000200020000000100010000020501010000060006000000050005000005020200000200020000000100010000");
}

static void test_twkb_out_collection(void)
{
	cu_twkb("GEOMETRYCOLLECTION(LINESTRING(1 1, 2 2), LINESTRING(3 3, 4 4), LINESTRING(5 5, 6 6))", 0, 0, 0, 0);
	// printf("TWKB: %s\n",s);
	CU_ASSERT_STRING_EQUAL(s,"07000302000202020202020002060602020200020A0A0202");

	cu_twkb("GEOMETRYCOLLECTION(POLYGON((0 0 0, 0 1 0,1 1 0,1 0 0, 0 0 0)),POINT(1 1 1))", 0, 0, 0, 0);
	// printf("TWKB: %s\n",s);
	CU_ASSERT_STRING_EQUAL(s,"070801020308010105000000000200020000000100010000010801020202");

	cu_twkb("GEOMETRYCOLLECTION EMPTY", 0, 0, 0, 0);
	CU_ASSERT_STRING_EQUAL(s,"0710");
}

static void test_twkb_out_idlist(void)
{
	int64_t idlist[2];

	idlist[0] = 2;
	idlist[1] = 4;
	cu_twkb_idlist("MULTIPOINT(1 1, 0 0)",idlist, 0, 0, 0, 0);
	// printf("TWKB: %s\n",s);   
	// printf("WKT: %s\n",w);   
	CU_ASSERT_STRING_EQUAL(s,"040402040802020101");		
	CU_ASSERT_STRING_EQUAL(w,"MULTIPOINT(1 1,0 0)");		

	/* 
	04 06 multipoint, size/idlist
	07 size 7 bytes
	02 two geometries
	0408 idlist (2, 4)
	0202 first point @ 1,1
	0101 second point offset -1,-1 
	*/
	idlist[0] = 2;
	idlist[1] = 4;
	cu_twkb_idlist("MULTIPOINT(1 1, 0 0)",idlist, 0, 0, 0, TWKB_SIZE);
	// printf("TWKB: %s\n",s);
	// printf("WKT: %s\n",w);   
	CU_ASSERT_STRING_EQUAL(s,"04060702040802020101");		
	CU_ASSERT_STRING_EQUAL(w,"MULTIPOINT(1 1,0 0)");		

	/*
	04 07 multipoint, bbox/size/idlist
	0B size 11 bytes
	00020002 bbox x(0,1), y(0,1)
	02 two geometries
	0408 idlist (2,4)
	0202 first point @ 1,1
	0101 seconds point offset -1,-1
	*/
	idlist[0] = 2;
	idlist[1] = 4;
	cu_twkb_idlist("MULTIPOINT(1 1, 0 0)",idlist, 0, 0, 0, TWKB_SIZE | TWKB_BBOX);
	// printf("TWKB: %s\n",s);
	// printf("WKT: %s\n",w);   
	CU_ASSERT_STRING_EQUAL(s,"04070B0002000202040802020101");		
	CU_ASSERT_STRING_EQUAL(w,"MULTIPOINT(1 1,0 0)");		

	/*
	0704 geometrycollection, idlist
	02 two geometries
	0408 idlist (2,4)
	01000202 first point (type, meta, x, y)
	01000000 second point (type, meta, x, y)
	*/		
	idlist[0] = 2;
	idlist[1] = 4;
	cu_twkb_idlist("GEOMETRYCOLLECTION(POINT(1 1),POINT(0 0))",idlist, 0, 0, 0, 0);
	// printf("TWKB: %s\n",s);
	CU_ASSERT_STRING_EQUAL(s,"07040204080100020201000000");
	CU_ASSERT_STRING_EQUAL(w,"GEOMETRYCOLLECTION(POINT(1 1),POINT(0 0))");		

	/*
	0706 geometrycollection, size/idlist
	0D size, 13 bytes
	02 two geometries
	0408 idlist (2,4)
	0102020202 first point (type, meta, size, x, y)
	0102020000 second point (type, meta, size, x, y)
	*/
	idlist[0] = 2;
	idlist[1] = 4;
	cu_twkb_idlist("GEOMETRYCOLLECTION(POINT(1 1),POINT(0 0))",idlist, 0, 0, 0, TWKB_SIZE);
	// printf("TWKB: %s\n",s);
	CU_ASSERT_STRING_EQUAL(s,"07060D02040801020202020102020000");
	CU_ASSERT_STRING_EQUAL(w,"GEOMETRYCOLLECTION(POINT(1 1),POINT(0 0))");		

}


/*
** Used by test harness to register the tests in this file.
*/
void twkb_out_suite_setup(void);
void twkb_out_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("twkb_output", init_twkb_out_suite, clean_twkb_out_suite);
	PG_ADD_TEST(suite, test_twkb_out_point);
	PG_ADD_TEST(suite, test_twkb_out_linestring);
	PG_ADD_TEST(suite, test_twkb_out_polygon);
	PG_ADD_TEST(suite, test_twkb_out_multipoint);
	PG_ADD_TEST(suite, test_twkb_out_multilinestring);
	PG_ADD_TEST(suite, test_twkb_out_multipolygon);
	PG_ADD_TEST(suite, test_twkb_out_collection);
	PG_ADD_TEST(suite, test_twkb_out_idlist);
}
