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

static void test_lwgeom_delaunay_triangulation(void)
{
#if POSTGIS_GEOS_VERSION >= 34
	LWGEOM *in, *tmp, *out;
	char *wkt, *exp_wkt;

	/* Because i don't trust that much prior tests...  ;) */
	cu_error_msg_reset();

	in = lwgeom_from_wkt("MULTIPOINT(10 0, 20 0, 5 5)", LW_PARSER_CHECK_NONE);

	tmp = lwgeom_delaunay_triangulation(in, 0, 0);
	lwgeom_free(in);
	out = lwgeom_normalize(tmp); 
	lwgeom_free(tmp);

        wkt = lwgeom_to_ewkt(out);
	lwgeom_free(out);

	exp_wkt = "GEOMETRYCOLLECTION(POLYGON((5 5,20 0,10 0,5 5)))";
        if ( strcmp(wkt, exp_wkt) )
	{
                fprintf(stderr, "\nExp:  %s\nObt:  %s\n", exp_wkt, wkt);
	}
	CU_ASSERT_STRING_EQUAL(wkt, exp_wkt);
	lwfree(wkt);

#endif /* POSTGIS_GEOS_VERSION >= 33 */
}


/*
** Used by test harness to register the tests in this file.
*/
void triangulate_suite_setup(void);
void triangulate_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("triangulate", NULL, NULL);
	PG_ADD_TEST(suite, test_lwgeom_delaunay_triangulation);
}
