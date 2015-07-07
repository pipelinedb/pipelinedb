/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2011 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "CUnit/Basic.h"
#include "cu_tester.h"

#include "liblwgeom.h"
#include "liblwgeom_internal.h"

static void test_lwgeom_node(void)
{
#if POSTGIS_GEOS_VERSION >= 33
	LWGEOM *in, *out;
	const char *wkt;
	char *tmp;

	/* Because i don't trust that much prior tests...  ;) */
	cu_error_msg_reset();

	wkt = "LINESTRING(0 0,5 5, 10 0)";
	in = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	out = lwgeom_node(in);
	/* printf("%s\n", lwgeom_to_ewkt(out)); */
	CU_ASSERT(lwgeom_same(in, out));
	lwgeom_free(out); lwgeom_free(in);

	wkt = "MULTILINESTRING((0 0,0 5),(10 0, -10 5))";
	in = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	out = lwgeom_node(in);
	tmp = lwgeom_to_ewkt(out);
	CU_ASSERT_STRING_EQUAL("MULTILINESTRING((0 2.5,-10 5),(0 0,0 2.5),(0 2.5,0 5),(10 0,0 2.5))", tmp)
	lwfree(tmp); lwgeom_free(out); lwgeom_free(in);

	wkt = "MULTILINESTRING((0 0,5 5,10 0, 11 0, 20 0),(10 0, 12 0, 22 0))";
	in = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	out = lwgeom_node(in);
	tmp = lwgeom_to_ewkt(out);
	/* printf("%s\n", tmp); */
	CU_ASSERT_STRING_EQUAL("MULTILINESTRING((0 0,5 5,10 0),(10 0,11 0,12 0,20 0),(20 0,22 0))", tmp);
	lwfree(tmp); lwgeom_free(out); lwgeom_free(in);

	wkt = "MULTILINESTRING((0 0,5 5,10 0, 11 0, 20 0),(22 0, 12 0, 10 0),(0 5, 5 0))";
	in = lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE);
	out = lwgeom_node(in);
	tmp = lwgeom_to_ewkt(out);
	/* printf("%s\n", tmp); */
	CU_ASSERT_STRING_EQUAL(
"MULTILINESTRING((0 0,2.5 2.5),(0 5,2.5 2.5),(22 0,20 0),(20 0,12 0,11 0,10 0),(10 0,5 5,2.5 2.5),(2.5 2.5,5 0))",
		tmp);
	lwfree(tmp); lwgeom_free(out); lwgeom_free(in);
#endif /* POSTGIS_GEOS_VERSION >= 33 */
}

/*
** Used by test harness to register the tests in this file.
*/
void node_suite_setup(void);
void node_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("noding", NULL, NULL);
	PG_ADD_TEST(suite, test_lwgeom_node);
}
