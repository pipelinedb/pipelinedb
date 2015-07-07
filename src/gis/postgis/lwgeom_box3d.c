/**********************************************************************
 *
 * BOX3D IO and conversions
 *
 * Portions Copyright 2013-2015 PipelineDB
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/geo_decls.h"

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "liblwgeom.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define SHOW_DIGS_DOUBLE 15
#define MAX_DIGS_DOUBLE (SHOW_DIGS_DOUBLE + 6 + 1 + 3 +1)

/* forward defs */
Datum BOX3D_in(PG_FUNCTION_ARGS);
Datum BOX3D_out(PG_FUNCTION_ARGS);
Datum LWGEOM_to_BOX3D(PG_FUNCTION_ARGS);
Datum BOX3D_to_LWGEOM(PG_FUNCTION_ARGS);
Datum BOX3D_expand(PG_FUNCTION_ARGS);
Datum BOX3D_to_BOX2D(PG_FUNCTION_ARGS);
Datum BOX3D_to_BOX(PG_FUNCTION_ARGS);
Datum BOX3D_xmin(PG_FUNCTION_ARGS);
Datum BOX3D_ymin(PG_FUNCTION_ARGS);
Datum BOX3D_zmin(PG_FUNCTION_ARGS);
Datum BOX3D_xmax(PG_FUNCTION_ARGS);
Datum BOX3D_ymax(PG_FUNCTION_ARGS);
Datum BOX3D_zmax(PG_FUNCTION_ARGS);
Datum BOX3D_combine(PG_FUNCTION_ARGS);

/**
 *  BOX3D_in - takes a string rep of BOX3D and returns internal rep
 *
 *  example:
 *     "BOX3D(x1 y1 z1,x2 y2 z2)"
 * or  "BOX3D(x1 y1,x2 y2)"   z1 and z2 = 0.0
 *
 *
 */

PG_FUNCTION_INFO_V1(BOX3D_in);
Datum BOX3D_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	int nitems;
	BOX3D *box = (BOX3D *) palloc(sizeof(BOX3D));
	box->zmin = 0;
	box->zmax = 0;


	/*printf( "box3d_in gets '%s'\n",str); */

	if (strstr(str,"BOX3D(") !=  str )
	{
		pfree(box);
		elog(ERROR,"BOX3D parser - doesnt start with BOX3D(");
		PG_RETURN_NULL();
	}

	nitems = sscanf(str,"BOX3D(%le %le %le ,%le %le %le)",
	                &box->xmin, &box->ymin, &box->zmin,
	                &box->xmax, &box->ymax, &box->zmax);
	if (nitems != 6 )
	{
		nitems = sscanf(str,"BOX3D(%le %le ,%le %le)",
		                &box->xmin, &box->ymin, &box->xmax, &box->ymax);
		if (nitems != 4)
		{
			pfree(box);
			elog(ERROR,"BOX3D parser - couldnt parse.  It should look like: BOX3D(xmin ymin zmin,xmax ymax zmax) or BOX3D(xmin ymin,xmax ymax)");
			PG_RETURN_NULL();
		}
	}

	if (box->xmin > box->xmax)
	{
		float tmp = box->xmin;
		box->xmin = box->xmax;
		box->xmax = tmp;
	}
	if (box->ymin > box->ymax)
	{
		float tmp = box->ymin;
		box->ymin = box->ymax;
		box->ymax = tmp;
	}
	if (box->zmin > box->zmax)
	{
		float tmp = box->zmin;
		box->zmin = box->zmax;
		box->zmax = tmp;
	}
	box->srid = SRID_UNKNOWN;
	PG_RETURN_POINTER(box);
}


/**
 *  Takes an internal rep of a BOX3D and returns a string rep.
 *
 *  example:
 *     "BOX3D(xmin ymin zmin, xmin ymin zmin)"
 */
PG_FUNCTION_INFO_V1(BOX3D_out);
Datum BOX3D_out(PG_FUNCTION_ARGS)
{
	BOX3D  *bbox = (BOX3D *) PG_GETARG_POINTER(0);
	int size;
	char *result;

	if (bbox == NULL)
	{
		result = palloc(5);
		strcat(result,"NULL");
		PG_RETURN_CSTRING(result);
	}


	/*double digits+ "BOX3D"+ "()" + commas +null */
	size = MAX_DIGS_DOUBLE*6+5+2+4+5+1;

	result = (char *) palloc(size);

	sprintf(result, "BOX3D(%.15g %.15g %.15g,%.15g %.15g %.15g)",
	        bbox->xmin, bbox->ymin, bbox->zmin,
	        bbox->xmax,bbox->ymax,bbox->zmax);

	PG_RETURN_CSTRING(result);
}


PG_FUNCTION_INFO_V1(BOX3D_to_BOX2D);
Datum BOX3D_to_BOX2D(PG_FUNCTION_ARGS)
{
	BOX3D *in = (BOX3D *)PG_GETARG_POINTER(0);
	GBOX *out = box3d_to_gbox(in);
	PG_RETURN_POINTER(out);
}

static void
box3d_to_box_p(BOX3D *box, BOX *out)
{
#if PARANOIA_LEVEL > 0
	if (box == NULL) return;
#endif

	out->low.x = box->xmin;
	out->low.y = box->ymin;

	out->high.x = box->xmax;
	out->high.y = box->ymax;
}

PG_FUNCTION_INFO_V1(BOX3D_to_BOX);
Datum BOX3D_to_BOX(PG_FUNCTION_ARGS)
{
	BOX3D *in = (BOX3D *)PG_GETARG_POINTER(0);
	BOX *box = palloc(sizeof(BOX));

	box3d_to_box_p(in, box);
	PG_RETURN_POINTER(box);
}


PG_FUNCTION_INFO_V1(BOX3D_to_LWGEOM);
Datum BOX3D_to_LWGEOM(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	POINTARRAY *pa;
	GSERIALIZED *result;
	POINT4D pt;


	/**
	 * Alter BOX3D cast so that a valid geometry is always
	 * returned depending upon the size of the BOX3D. The
	 * code makes the following assumptions:
	 *     - If the BOX3D is a single point then return a
	 *     POINT geometry
	 *     - If the BOX3D represents either a horizontal or
	 *     vertical line, return a LINESTRING geometry
	 *     - Otherwise return a POLYGON
	 */

	pa = ptarray_construct_empty(0, 0, 5);

	if ( (box->xmin == box->xmax) && (box->ymin == box->ymax) )
	{
		LWPOINT *lwpt = lwpoint_construct(SRID_UNKNOWN, NULL, pa);

		pt.x = box->xmin;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);

		result = geometry_serialize(lwpoint_as_lwgeom(lwpt));
	}
	else if (box->xmin == box->xmax ||
	         box->ymin == box->ymax)
	{
		LWLINE *lwline = lwline_construct(SRID_UNKNOWN, NULL, pa);

		pt.x = box->xmin;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmax;
		pt.y = box->ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);

		result = geometry_serialize(lwline_as_lwgeom(lwline));
	}
	else
	{
		LWPOLY *lwpoly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, &pa);

		pt.x = box->xmin;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmin;
		pt.y = box->ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmax;
		pt.y = box->ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmax;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmin;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);

		result = geometry_serialize(lwpoly_as_lwgeom(lwpoly));
		
	}

	gserialized_set_srid(result, box->srid);
	PG_RETURN_POINTER(result);
}

/** Expand given box of 'd' units in all directions */
void
expand_box3d(BOX3D *box, double d)
{
	box->xmin -= d;
	box->ymin -= d;
	box->zmin -= d;

	box->xmax += d;
	box->ymax += d;
	box->zmax += d;
}

PG_FUNCTION_INFO_V1(BOX3D_expand);
Datum BOX3D_expand(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	double d = PG_GETARG_FLOAT8(1);
	BOX3D *result = (BOX3D *)palloc(sizeof(BOX3D));

	memcpy(result, box, sizeof(BOX3D));
	expand_box3d(result, d);

	PG_RETURN_POINTER(result);
}

/**
 * convert a GSERIALIZED to BOX3D
 *
 * NOTE: the bounding box is *always* recomputed as the cache
 * is a box2d, not a box3d...
 *
 */
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX3D);
Datum LWGEOM_to_BOX3D(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	GBOX gbox;
	BOX3D *result;
	int rv = lwgeom_calculate_gbox(lwgeom, &gbox);

	if ( rv == LW_FAILURE )
		PG_RETURN_NULL();
		
	result = box3d_from_gbox(&gbox);
	result->srid = lwgeom->srid;

	lwgeom_free(lwgeom);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX3D_xmin);
Datum BOX3D_xmin(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Min(box->xmin, box->xmax));
}

PG_FUNCTION_INFO_V1(BOX3D_ymin);
Datum BOX3D_ymin(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Min(box->ymin, box->ymax));
}

PG_FUNCTION_INFO_V1(BOX3D_zmin);
Datum BOX3D_zmin(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Min(box->zmin, box->zmax));
}

PG_FUNCTION_INFO_V1(BOX3D_xmax);
Datum BOX3D_xmax(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Max(box->xmin, box->xmax));
}

PG_FUNCTION_INFO_V1(BOX3D_ymax);
Datum BOX3D_ymax(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Max(box->ymin, box->ymax));
}

PG_FUNCTION_INFO_V1(BOX3D_zmax);
Datum BOX3D_zmax(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
	PG_RETURN_FLOAT8(Max(box->zmin, box->zmax));
}

PG_FUNCTION_INFO_V1(BOX3D_combine_3d);
Datum BOX3D_combine_3d(PG_FUNCTION_ARGS)
{
	BOX3D *state;
	BOX3D *incoming = (BOX3D *) PG_GETARG_POINTER(1);

	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		PG_RETURN_POINTER(incoming);
	}

	state = (BOX3D *) PG_GETARG_POINTER(0);

	state->xmax = Max(state->xmax, incoming->xmax);
	state->ymax = Max(state->ymax, incoming->ymax);
	state->zmax = Max(state->zmax, incoming->zmax);
	state->xmin = Min(state->xmin, incoming->xmin);
	state->ymin = Min(state->ymin, incoming->ymin);
	state->zmin = Min(state->zmin, incoming->zmin);

	PG_RETURN_POINTER(state);
}

/**
* Used in the ST_Extent and ST_Extent3D aggregates, does not read the 
* serialized cached bounding box (since that is floating point)
* but calculates the box in full from the underlying geometry.
*/
PG_FUNCTION_INFO_V1(BOX3D_combine);
Datum BOX3D_combine(PG_FUNCTION_ARGS)
{
	BOX3D *box = (BOX3D*)PG_GETARG_POINTER(0);
	GSERIALIZED *geom = PG_ARGISNULL(1) ? NULL : (GSERIALIZED*)PG_DETOAST_DATUM(PG_GETARG_POINTER(1));
	LWGEOM *lwgeom = NULL;
	BOX3D *result = NULL;
	GBOX gbox;
	int32_t srid;
	int rv;

	/* Can't do anything with null inputs */
	if  ( (box == NULL) && (geom == NULL) )
		PG_RETURN_NULL();

	/* Null geometry but non-null box, return the box */
	if (geom == NULL)
	{
		result = palloc(sizeof(BOX3D));
		memcpy(result, box, sizeof(BOX3D));
		PG_RETURN_POINTER(result);
	}

	/* Deserialize geometry and *calculate* the box */
	/* We can't use the cached box because it's float, we *must* calculate */
	lwgeom = lwgeom_from_gserialized(geom);
	srid = lwgeom->srid;
	rv = lwgeom_calculate_gbox(lwgeom, &gbox);
	lwgeom_free(lwgeom);

	/* If we couldn't calculate the box, return what we know */
	if ( rv == LW_FAILURE )
	{
		PG_FREE_IF_COPY(geom, 1);
		/* No geom box, no input box, so null return */
		if ( box == NULL )
			PG_RETURN_NULL();
		result = palloc(sizeof(BOX3D));
		memcpy(result, box, sizeof(BOX3D));
		PG_RETURN_POINTER(result);
	}

	/* Null box and non-null geometry, just return the geometry box */
	if ( box == NULL )
	{
		PG_FREE_IF_COPY(geom, 1);
		result = box3d_from_gbox(&gbox);
		result->srid = srid;
		PG_RETURN_POINTER(result);
	}

	result = palloc(sizeof(BOX3D));
	result->xmax = Max(box->xmax, gbox.xmax);
	result->ymax = Max(box->ymax, gbox.ymax);
	result->zmax = Max(box->zmax, gbox.zmax);
	result->xmin = Min(box->xmin, gbox.xmin);
	result->ymin = Min(box->ymin, gbox.ymin);
	result->zmin = Min(box->zmin, gbox.zmin);
	result->srid = srid;

	PG_FREE_IF_COPY(geom, 1);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX3D_construct);
Datum BOX3D_construct(PG_FUNCTION_ARGS)
{
	GSERIALIZED *min = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *max = PG_GETARG_GSERIALIZED_P(1);
	BOX3D *result = palloc(sizeof(BOX3D));
	LWGEOM *minpoint, *maxpoint;
	POINT3DZ minp, maxp;

	minpoint = lwgeom_from_gserialized(min);
	maxpoint = lwgeom_from_gserialized(max);

	if ( minpoint->type != POINTTYPE ||
	     maxpoint->type != POINTTYPE )
	{
		elog(ERROR, "BOX3D_construct: args must be points");
		PG_RETURN_NULL();
	}

	error_if_srid_mismatch(minpoint->srid, maxpoint->srid);

	getPoint3dz_p(((LWPOINT *)minpoint)->point, 0, &minp);
	getPoint3dz_p(((LWPOINT *)maxpoint)->point, 0, &maxp);

	result->xmax = maxp.x;
	result->ymax = maxp.y;
	result->zmax = maxp.z;

	result->xmin = minp.x;
	result->ymin = minp.y;
	result->zmin = minp.z;

	result->srid = minpoint->srid;

	PG_RETURN_POINTER(result);
}
