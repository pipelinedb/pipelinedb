/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2010 Olivier Courtin <olivier.courtin@oslandia.com>
 * Copyright (C) 2010 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 * Copyright (C) 2009-2011 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * Comparision function for use in Binary Tree searches
 * (ORDER BY, GROUP BY, DISTINCT)
 *
 ***********************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "utils/geo_decls.h"

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

Datum lwgeom_lt(PG_FUNCTION_ARGS);
Datum lwgeom_le(PG_FUNCTION_ARGS);
Datum lwgeom_eq(PG_FUNCTION_ARGS);
Datum lwgeom_ge(PG_FUNCTION_ARGS);
Datum lwgeom_gt(PG_FUNCTION_ARGS);
Datum lwgeom_cmp(PG_FUNCTION_ARGS);


#define BTREE_SRID_MISMATCH_SEVERITY ERROR

PG_FUNCTION_INFO_V1(lwgeom_lt);
Datum lwgeom_lt(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;

	POSTGIS_DEBUG(2, "lwgeom_lt called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	POSTGIS_DEBUG(3, "lwgeom_lt passed getSRID test");

	gserialized_get_gbox_p(geom1, &box1);
	gserialized_get_gbox_p(geom2, &box2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	POSTGIS_DEBUG(3, "lwgeom_lt getbox2d_p passed");

	if  ( ! FPeq(box1.xmin , box2.xmin) )
	{
		if  (box1.xmin < box2.xmin)
			PG_RETURN_BOOL(TRUE);
	}

	if  ( ! FPeq(box1.ymin , box2.ymin) )
	{
		if  (box1.ymin < box2.ymin)
			PG_RETURN_BOOL(TRUE);
	}

	if  ( ! FPeq(box1.xmax , box2.xmax) )
	{
		if  (box1.xmax < box2.xmax)
			PG_RETURN_BOOL(TRUE);
	}

	if  ( ! FPeq(box1.ymax , box2.ymax) )
	{
		if  (box1.ymax < box2.ymax)
			PG_RETURN_BOOL(TRUE);
	}

	PG_RETURN_BOOL(FALSE);
}

PG_FUNCTION_INFO_V1(lwgeom_le);
Datum lwgeom_le(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;

	POSTGIS_DEBUG(2, "lwgeom_le called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	gserialized_get_gbox_p(geom1, &box1);
	gserialized_get_gbox_p(geom2, &box2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	if  ( ! FPeq(box1.xmin , box2.xmin) )
	{
		if  (box1.xmin < box2.xmin)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.ymin , box2.ymin) )
	{
		if  (box1.ymin < box2.ymin)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.xmax , box2.xmax) )
	{
		if  (box1.xmax < box2.xmax)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.ymax , box2.ymax) )
	{
		if  (box1.ymax < box2.ymax)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	PG_RETURN_BOOL(TRUE);
}

PG_FUNCTION_INFO_V1(lwgeom_eq);
Datum lwgeom_eq(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;
  bool empty1, empty2;
	bool result;

	POSTGIS_DEBUG(2, "lwgeom_eq called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	gbox_init(&box1);
	gbox_init(&box2);
	
	empty1 = ( gserialized_get_gbox_p(geom1, &box1) == LW_FAILURE );
	empty2 = ( gserialized_get_gbox_p(geom2, &box2) == LW_FAILURE );
	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	if  ( empty1 != empty2 ) 
	{
    result = FALSE;
	}
  else if  ( ! (FPeq(box1.xmin, box2.xmin) && FPeq(box1.ymin, box2.ymin) &&
	         FPeq(box1.xmax, box2.xmax) && FPeq(box1.ymax, box2.ymax)) )
	{
		result = FALSE;
	}
	else
	{
		result = TRUE;
	}

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(lwgeom_ge);
Datum lwgeom_ge(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;

	POSTGIS_DEBUG(2, "lwgeom_ge called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	gserialized_get_gbox_p(geom1, &box1);
	gserialized_get_gbox_p(geom2, &box2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	if  ( ! FPeq(box1.xmin , box2.xmin) )
	{
		if  (box1.xmin > box2.xmin)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.ymin , box2.ymin) )
	{
		if  (box1.ymin > box2.ymin)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.xmax , box2.xmax) )
	{
		if  (box1.xmax > box2.xmax)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	if  ( ! FPeq(box1.ymax , box2.ymax) )
	{
		if  (box1.ymax > box2.ymax)
		{
			PG_RETURN_BOOL(TRUE);
		}
		PG_RETURN_BOOL(FALSE);
	}

	PG_RETURN_BOOL(TRUE);
}

PG_FUNCTION_INFO_V1(lwgeom_gt);
Datum lwgeom_gt(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;

	POSTGIS_DEBUG(2, "lwgeom_gt called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	gserialized_get_gbox_p(geom1, &box1);
	gserialized_get_gbox_p(geom2, &box2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	if  ( ! FPeq(box1.xmin , box2.xmin) )
	{
		if  (box1.xmin > box2.xmin)
		{
			PG_RETURN_BOOL(TRUE);
		}
	}

	if  ( ! FPeq(box1.ymin , box2.ymin) )
	{
		if  (box1.ymin > box2.ymin)
		{
			PG_RETURN_BOOL(TRUE);
		}
	}

	if  ( ! FPeq(box1.xmax , box2.xmax) )
	{
		if  (box1.xmax > box2.xmax)
		{
			PG_RETURN_BOOL(TRUE);
		}
	}

	if  ( ! FPeq(box1.ymax , box2.ymax) )
	{
		if  (box1.ymax > box2.ymax)
		{
			PG_RETURN_BOOL(TRUE);
		}
	}

	PG_RETURN_BOOL(FALSE);
}

PG_FUNCTION_INFO_V1(lwgeom_cmp);
Datum lwgeom_cmp(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	GBOX box1;
	GBOX box2;

	POSTGIS_DEBUG(2, "lwgeom_cmp called");

	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(BTREE_SRID_MISMATCH_SEVERITY,
		     "Operation on two GEOMETRIES with different SRIDs\n");
		PG_FREE_IF_COPY(geom1, 0);
		PG_FREE_IF_COPY(geom2, 1);
		PG_RETURN_NULL();
	}

	gserialized_get_gbox_p(geom1, &box1);
	gserialized_get_gbox_p(geom2, &box2);

	PG_FREE_IF_COPY(geom1, 0);
	PG_FREE_IF_COPY(geom2, 1);

	if  ( ! FPeq(box1.xmin , box2.xmin) )
	{
		if  (box1.xmin < box2.xmin)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	if  ( ! FPeq(box1.ymin , box2.ymin) )
	{
		if  (box1.ymin < box2.ymin)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	if  ( ! FPeq(box1.xmax , box2.xmax) )
	{
		if  (box1.xmax < box2.xmax)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	if  ( ! FPeq(box1.ymax , box2.ymax) )
	{
		if  (box1.ymax < box2.ymax)
		{
			PG_RETURN_INT32(-1);
		}
		PG_RETURN_INT32(1);
	}

	PG_RETURN_INT32(0);
}

