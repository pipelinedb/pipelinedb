/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010-2012 Olivier Courtin <olivier.courtin@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "cu_surface.h"

void triangle_parse(void)
{
	LWGEOM *geom;
	GSERIALIZED *g;
	char *tmp;

	cu_error_msg_reset();	/* Because i don't trust that much prior tests...  ;) */

	/* 2 dims */
	geom = lwgeom_from_wkt("TRIANGLE((0 1,2 3,4 5,0 1))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TRIANGLETYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TRIANGLE((0 1,2 3,4 5,0 1))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);
	/* 3DM */
	geom = lwgeom_from_wkt("TRIANGLEM((0 1 2,3 4 5,6 7 8,0 1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TRIANGLETYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TRIANGLEM((0 1 2,3 4 5,6 7 8,0 1 2))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* ERROR: a missing Z values */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7,0 1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("can not mix dimensionality in a geometry", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed rings */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7 8,0 0 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7 8,0 1 3))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim, with a 4D geom */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2 3,4 5 6 7,8 9 10 11,0 1 3 3))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: only 3 points in a face */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,0 1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("triangle must have exactly 4 points", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: more than 4 points in a face */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7 8,9 10 11,0 1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("triangle must have exactly 4 points", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: no interior rings allowed */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7 8,0 1 2),(9 10 11,12 13 14,15 16 17,9 10 11)", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("parse error - invalid geometry", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* EMPTY face */
	geom = lwgeom_from_wkt("TRIANGLE EMPTY", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TRIANGLETYPE);
	tmp = lwgeom_to_wkt(geom, LW_PARSER_CHECK_NONE, 0, 0);
	CU_ASSERT_STRING_EQUAL("TRIANGLE EMPTY", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* explicit SRID */
	geom = lwgeom_from_wkt("SRID=4326;TRIANGLE((0 1 2,3 4 5,6 7 8,0 1 2))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TRIANGLETYPE);
	CU_ASSERT_EQUAL(geom->srid, 4326);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("SRID=4326;TRIANGLE((0 1 2,3 4 5,6 7 8,0 1 2))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* geography support */
	geom = lwgeom_from_wkt("TRIANGLE((0 1 2,3 4 5,6 7 8,0 1 2))", LW_PARSER_CHECK_NONE);
	g = gserialized_from_lwgeom(geom, 1, 0);
	CU_ASSERT_EQUAL(gserialized_get_type(g), TRIANGLETYPE);
	lwgeom_free(geom);
	lwfree(g);
}


void tin_parse(void)
{
	LWGEOM *geom;
	GSERIALIZED *g;
	char *tmp;

	cu_error_msg_reset();	/* Because i don't trust that much prior tests...  ;) */

	/* empty */
	geom = lwgeom_from_wkt("TIN EMPTY", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TIN EMPTY", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* 2 dims */
	geom = lwgeom_from_wkt("TIN(((0 1,2 3,4 5,0 1)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TIN(((0 1,2 3,4 5,0 1)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* 3DM */
	geom = lwgeom_from_wkt("TINM(((0 1 2,3 4 5,6 7 8,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TINM(((0 1 2,3 4 5,6 7 8,0 1 2)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* ERROR: a missing Z values */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("can not mix dimensionality in a geometry", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed rings */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7 8,0 0 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7 8,0 1 3)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim, with a 4D geom */
	geom = lwgeom_from_wkt("TIN(((0 1 2 3,4 5 6 7,8 9 10 11,0 1 3 3)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: only 3 points in a face */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("triangle must have exactly 4 points", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: more than 3 points in a face */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7 8,9 10 11,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("triangle must have exactly 4 points", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: use ring for triangle */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7 8,0 1 2),(9 10 11,12 13 14,15 16 17,9 10 11)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("parse error - invalid geometry", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* EMPTY face */
	geom = lwgeom_from_wkt("TIN EMPTY", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TIN EMPTY", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* A simple tetrahedron */
	geom = lwgeom_from_wkt("TIN(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	CU_ASSERT_EQUAL(geom->srid, SRID_UNKNOWN);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TIN(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* A 4D tetrahedron */
	geom = lwgeom_from_wkt("TIN(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	CU_ASSERT_EQUAL(FLAGS_GET_M(geom->flags), 1);
	CU_ASSERT_EQUAL(geom->srid, SRID_UNKNOWN);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("TIN(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* explicit SRID */
	geom = lwgeom_from_wkt("SRID=4326;TIN(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, TINTYPE);
	CU_ASSERT_EQUAL(geom->srid, 4326);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("SRID=4326;TIN(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* geography support */
	geom = lwgeom_from_wkt("TIN(((0 1 2,3 4 5,6 7 8,0 1 2)))", LW_PARSER_CHECK_NONE);
	g = gserialized_from_lwgeom(geom, 1, 0);
	CU_ASSERT_EQUAL(gserialized_get_type(g), TINTYPE);
	lwgeom_free(geom);
	lwfree(g);
}


void polyhedralsurface_parse(void)
{
	LWGEOM *geom;
	GSERIALIZED *g;
	char *tmp;

	cu_error_msg_reset();	/* Because i don't trust that much prior tests...  ;) */

	/* 2 dims */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F00000001000000010300000001000000040000000000000000000000000000000000F03F00000000000000400000000000000840000000000000104000000000000014400000000000000000000000000000F03F", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACE(((0 1,2 3,4 5,0 1)))", tmp);
	lwfree(tmp);
	lwgeom_free(geom);
	
	/* 3DM */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACEM(((0 1 2,3 4 5,6 7 8,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACEM(((0 1 2,3 4 5,6 7 8,0 1 2)))", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F00004001000000010300004001000000040000000000000000000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000000000000000000000F03F0000000000000040", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* ERROR: a missing Z values */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7,0 1 2)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_STRING_EQUAL("can not mix dimensionality in a geometry", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* 1 face with 1 interior ring */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7 8,0 1 2),(9 10 11,12 13 14,15 16 17,9 10 11)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7 8,0 1 2),(9 10 11,12 13 14,15 16 17,9 10 11)))", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F00008001000000010300008002000000040000000000000000000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C4000000000000020400000000000000000000000000000F03F00000000000000400400000000000000000022400000000000002440000000000000264000000000000028400000000000002A400000000000002C400000000000002E4000000000000030400000000000003140000000000000224000000000000024400000000000002640", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* ERROR: non closed rings */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7 8,0 0 2)))", LW_PARSER_CHECK_ALL);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7 8,0 1 3)))", LW_PARSER_CHECK_ALL);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: non closed face in Z dim, with a 4D geom */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2 3,4 5 6 7,8 9 10 11,0 1 3 3)))", LW_PARSER_CHECK_ALL);
	CU_ASSERT_STRING_EQUAL("geometry contains non-closed rings", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* ERROR: only 3 points in a face */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,0 1 2)))", LW_PARSER_CHECK_ALL);
	CU_ASSERT_STRING_EQUAL("geometry requires more points", cu_error_msg);
	cu_error_msg_reset();
	lwgeom_free(geom);

	/* EMPTY face */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE EMPTY", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	tmp = (char *)lwgeom_to_wkb(geom, WKB_HEX | WKB_ISO | WKB_NDR, 0);
	CU_ASSERT_STRING_EQUAL("010F00000000000000", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACE EMPTY", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* A simple tetrahedron */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	CU_ASSERT_EQUAL(geom->srid, SRID_UNKNOWN);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F000080040000000103000080010000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000010300008001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000000001030000800100000004000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000F03F00000000000000000000000000000000000000000000000001030000800100000004000000000000000000F03F000000000000000000000000000000000000000000000000000000000000F03F000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000", tmp);
	lwfree(tmp);
	lwgeom_free(geom);

	/* A 4D tetrahedron */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	CU_ASSERT_EQUAL(FLAGS_GET_M(geom->flags), 1);
	CU_ASSERT_EQUAL(geom->srid, SRID_UNKNOWN);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("POLYHEDRALSURFACE(((0 0 0 0,0 0 1 0,0 1 0 2,0 0 0 0)),((0 0 0 0,0 1 0 0,1 0 0 4,0 0 0 0)),((0 0 0 0,1 0 0 0,0 0 1 6,0 0 0 0)),((1 0 0 0,0 1 0 0,0 0 1 0,1 0 0 0)))", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F0000C00400000001030000C00100000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F00000000000000000000000000000000000000000000F03F00000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000001030000C0010000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F00000000000000000000000000000000000000000000F03F000000000000000000000000000000000000000000001040000000000000000000000000000000000000000000000000000000000000000001030000C001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000001840000000000000000000000000000000000000000000000000000000000000000001030000C00100000004000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000000000000000000000000000000000000000", tmp);
	lwfree(tmp);
	lwgeom_free(geom);


	/* explicit SRID */
	geom = lwgeom_from_wkt("SRID=4326;POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(geom->type, POLYHEDRALSURFACETYPE);
	CU_ASSERT_EQUAL(geom->srid, 4326);
	tmp = lwgeom_to_ewkt(geom);
	CU_ASSERT_STRING_EQUAL("SRID=4326;POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", tmp);
	lwfree(tmp);
	tmp = lwgeom_to_hexwkb(geom, WKB_NDR | WKB_EXTENDED, 0);
	CU_ASSERT_STRING_EQUAL("010F0000A0E6100000040000000103000080010000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000010300008001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000000001030000800100000004000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000000000000000000000000000000000000000000000000000F03F00000000000000000000000000000000000000000000000001030000800100000004000000000000000000F03F000000000000000000000000000000000000000000000000000000000000F03F000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000000000000000000000", tmp);
	lwfree(tmp);
	lwgeom_free(geom);


	/* geography support */
	geom = lwgeom_from_wkt("POLYHEDRALSURFACE(((0 1 2,3 4 5,6 7 8,0 1 2)))", LW_PARSER_CHECK_NONE);
	g = gserialized_from_lwgeom(geom, 1, 0);
	CU_ASSERT_EQUAL(gserialized_get_type(g), POLYHEDRALSURFACETYPE);
	lwgeom_free(geom);
	lwfree(g);
}


static void
check_dimension(char *ewkt, int dim)
{
	LWGEOM *geom;

	geom = lwgeom_from_wkt(ewkt, LW_PARSER_CHECK_NONE);
	CU_ASSERT_EQUAL(strlen(cu_error_msg), 0);
	CU_ASSERT_EQUAL(lwgeom_dimensionality(geom), dim);
	lwgeom_free(geom);
}

void
surface_dimension(void)
{
	/* 2D */
	check_dimension("POLYHEDRALSURFACE(((0 0,0 1,1 1,0 0)))", 2);
	check_dimension("TIN(((0 0,0 1,1 1,0 0)))", 2);

	/* 3D single face */
	check_dimension("POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)))", 2);
	check_dimension("TIN(((0 0 0,0 0 1,0 1 0,0 0 0)))", 2);

	/* Tetrahedron */
	check_dimension("POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", 3);
	check_dimension("TIN(((0 0 0,0 0 1,0 1 0,0 0 0)),((0 0 0,0 1 0,1 0 0,0 0 0)),((0 0 0,1 0 0,0 0 1,0 0 0)),((1 0 0,0 1 0,0 0 1,1 0 0)))", 3);
}


/*
** Used by test harness to register the tests in this file.
*/
void surface_suite_setup(void);
void surface_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("surface", NULL,  NULL);
	PG_ADD_TEST(suite, triangle_parse);
	PG_ADD_TEST(suite, tin_parse);
	PG_ADD_TEST(suite, polyhedralsurface_parse);
	PG_ADD_TEST(suite, surface_dimension);
}
