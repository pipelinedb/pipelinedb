/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "access/hash.h"

#include "../postgis_config.h"

#include "liblwgeom.h"         /* For standard geometry types. */
#include "liblwgeom_internal.h"         /* For FP comparators. */
#include "lwgeom_pg.h"       /* For debugging macros. */
#include "gserialized_gist.h"
#include "geography.h"	     /* For utility functions. */

Datum geography_lt(PG_FUNCTION_ARGS);
Datum geography_le(PG_FUNCTION_ARGS);
Datum geography_eq(PG_FUNCTION_ARGS);
Datum geography_ge(PG_FUNCTION_ARGS);
Datum geography_gt(PG_FUNCTION_ARGS);
Datum geography_cmp(PG_FUNCTION_ARGS);


/*
** Utility function to return the center point of a
** geocentric bounding box. We don't divide by two
** because we're only using the values for comparison.
*/
static void geography_gidx_center(GIDX *gidx, POINT3D *p)
{
	p->x = GIDX_GET_MIN(gidx, 0) + GIDX_GET_MAX(gidx, 0);
	p->y = GIDX_GET_MIN(gidx, 1) + GIDX_GET_MAX(gidx, 1);
	p->z = GIDX_GET_MIN(gidx, 2) + GIDX_GET_MAX(gidx, 2);
}

/*
** BTree support function. Based on two geographies return true if
** they are "less than" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_lt);
Datum geography_lt(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	        ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if ( p1.x < p2.x || p1.y < p2.y || p1.z < p2.z )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);
}

/*
** BTree support function. Based on two geographies return true if
** they are "less than or equal" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_le);
Datum geography_le(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	        ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if ( p1.x <= p2.x || p1.y <= p2.y || p1.z <= p2.z )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);
}

/*
** BTree support function. Based on two geographies return true if
** they are "greater than" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_gt);
Datum geography_gt(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	        ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if ( p1.x > p2.x && p1.y > p2.y && p1.z > p2.z )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);
}

/*
** BTree support function. Based on two geographies return true if
** they are "greater than or equal" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_ge);
Datum geography_ge(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	        ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if ( p1.x >= p2.x && p1.y >= p2.y && p1.z >= p2.z )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);
}


#if 0
/*
** Calculate a hash code based on the geometry data alone
*/
static uint32 geography_hash(GSERIALIZED *g)
{
	return DatumGetUInt32(hash_any((void*)g, VARSIZE(g)));
}
/*
** BTree support function. Based on two geographies return true if
** they are "equal" and false otherwise. This version uses a hash
** function to try and shoot for a more exact equality test.
*/
PG_FUNCTION_INFO_V1(geography_eq);
Datum geography_eq(PG_FUNCTION_ARGS)
{
	/* Perfect equals test based on hash */
	GSERIALIZED *g1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *g2 = PG_GETARG_GSERIALIZED_P(1);

	uint32 h1 = geography_hash(g1);
	uint32 h2 = geography_hash(g2);

	PG_FREE_IF_COPY(g1,0);
	PG_FREE_IF_COPY(g2,0);

	if ( h1 == h2 )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);
}
#endif

/*
** BTree support function. Based on two geographies return true if
** they are "equal" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_eq);
Datum geography_eq(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	     ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if ( FP_EQUALS(p1.x, p2.x) && FP_EQUALS(p1.y, p2.y) && FP_EQUALS(p1.z, p2.z) )
		PG_RETURN_BOOL(TRUE);

	PG_RETURN_BOOL(FALSE);

}

/*
** BTree support function. Based on two geographies return true if
** they are "equal" and false otherwise.
*/
PG_FUNCTION_INFO_V1(geography_cmp);
Datum geography_cmp(PG_FUNCTION_ARGS)
{
	/* Put aside some stack memory and use it for GIDX pointers. */
	char gboxmem1[GIDX_MAX_SIZE];
	char gboxmem2[GIDX_MAX_SIZE];
	GIDX *gbox1 = (GIDX*)gboxmem1;
	GIDX *gbox2 = (GIDX*)gboxmem2;
	POINT3D p1, p2;

	/* Must be able to build box for each argument (ie, not empty geometry) */
	if ( ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(0), gbox1) ||
	        ! gserialized_datum_get_gidx_p(PG_GETARG_DATUM(1), gbox2) )
	{
		PG_RETURN_BOOL(FALSE);
	}

	geography_gidx_center(gbox1, &p1);
	geography_gidx_center(gbox2, &p2);

	if  ( ! FP_EQUALS(p1.x, p2.x) )
	{
		if  (p1.x < p2.x)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	if  ( ! FP_EQUALS(p1.y, p2.y) )
	{
		if  (p1.y < p2.y)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	if  ( ! FP_EQUALS(p1.z, p2.z) )
	{
		if  (p1.z < p2.z)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	PG_RETURN_INT32(0);
}

