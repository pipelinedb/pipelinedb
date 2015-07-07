/**********************************************************************
*
* PostGIS - Spatial Types for PostgreSQL
* http://postgis.net
*
* Copyright 2014 Kashif Rasul <kashif.rasul@gmail.com> and
*                Shoaib Burq <saburq@gmail.com>
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

static void do_encoded_polyline_test(char * in, int precision, char * out)
{
	LWGEOM *g;
	char * h;
	size_t size;

	g = lwgeom_from_encoded_polyline(in, precision);
	h = lwgeom_to_wkt(g, WKT_EXTENDED, 15, &size);

	if (strcmp(h, out))
		fprintf(stderr, "\nIn:   %s\nOut:  %s\nTheo: %s\n", in, h, out);

	CU_ASSERT_STRING_EQUAL(h, out);

	lwgeom_free(g);
	if ( h ) lwfree(h);
}

static void in_encoded_polyline_test_geoms(void)
{
	do_encoded_polyline_test(
	    "_p~iF~ps|U_ulLnnqC_mqNvxq`@",
	    5,
			"SRID=4326;LINESTRING(-120.2 38.5,-120.95 40.7,-126.453 43.252)");
}

static void in_encoded_polyline_test_precision(void)
{
	do_encoded_polyline_test(
			"o}~~|AdshNoSsBgd@eGoBlm@wKhj@~@?",
	    6,
	    "SRID=4326;LINESTRING(-0.250691 49.283048,-0.250633 49.283376,-0.250502 49.283972,-0.251245 49.284028,-0.251938 49.284232,-0.251938 49.2842)");
}

/*
** Used by test harness to register the tests in this file.
*/
void in_encoded_polyline_suite_setup(void);
void in_encoded_polyline_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("encoded_polyline_input", NULL, NULL);
	PG_ADD_TEST(suite, in_encoded_polyline_test_geoms);
	PG_ADD_TEST(suite, in_encoded_polyline_test_precision);
}
