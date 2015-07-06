/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2009 Refractions Research Inc.
 * Copyright 2009 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "access/gist.h"
#include "access/itup.h"
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


/* forward defs */
Datum BOX2D_in(PG_FUNCTION_ARGS);
Datum BOX2D_out(PG_FUNCTION_ARGS);
Datum LWGEOM_to_BOX2D(PG_FUNCTION_ARGS);
Datum LWGEOM_to_BOX2DF(PG_FUNCTION_ARGS);
Datum BOX2D_expand(PG_FUNCTION_ARGS);
Datum BOX2D_to_BOX3D(PG_FUNCTION_ARGS);
Datum BOX2D_combine(PG_FUNCTION_ARGS);
Datum BOX2D_to_LWGEOM(PG_FUNCTION_ARGS);
Datum BOX2D_construct(PG_FUNCTION_ARGS);

/* parser - "BOX(xmin ymin,xmax ymax)" */
PG_FUNCTION_INFO_V1(BOX2D_in);
Datum BOX2D_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	int nitems;
	double tmp;
	GBOX box;
	int i;
	
	gbox_init(&box);

	for(i = 0; str[i]; i++) {
	  str[i] = tolower(str[i]);
	}
	
	nitems = sscanf(str,"box(%lf %lf,%lf %lf)", &box.xmin, &box.ymin, &box.xmax, &box.ymax);
	if (nitems != 4)
	{
		elog(ERROR,"box2d parser - couldnt parse.  It should look like: BOX(xmin ymin,xmax ymax)");
		PG_RETURN_NULL();
	}

	if (box.xmin > box.xmax)
	{
		tmp = box.xmin;
		box.xmin = box.xmax;
		box.xmax = tmp;
	}
	if (box.ymin > box.ymax)
	{
		tmp = box.ymin;
		box.ymin = box.ymax;
		box.ymax = tmp;
	}
	PG_RETURN_POINTER(gbox_copy(&box));
}

/*writer  "BOX(xmin ymin,xmax ymax)" */
PG_FUNCTION_INFO_V1(BOX2D_out);
Datum BOX2D_out(PG_FUNCTION_ARGS)
{
	GBOX *box = (GBOX *) PG_GETARG_POINTER(0);
	char tmp[500]; /* big enough */
	char *result;
	int size;

	size  = sprintf(tmp,"BOX(%.15g %.15g,%.15g %.15g)",
	                box->xmin, box->ymin, box->xmax, box->ymax);

	result= palloc(size+1); /* +1= null term */
	memcpy(result,tmp,size+1);
	result[size] = '\0';

	PG_RETURN_CSTRING(result);
}


/*convert a GSERIALIZED to BOX2D */
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX2D);
Datum LWGEOM_to_BOX2D(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	GBOX gbox;

	/* Cannot box empty! */
	if ( lwgeom_is_empty(lwgeom) )
		PG_RETURN_NULL(); 

	/* Cannot calculate box? */
	if ( lwgeom_calculate_gbox(lwgeom, &gbox) == LW_FAILURE )
		PG_RETURN_NULL();
		
	/* Strip out higher dimensions */
	FLAGS_SET_Z(gbox.flags, 0);
	FLAGS_SET_M(gbox.flags, 0);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_POINTER(gbox_copy(&gbox));
}


/*convert a GSERIALIZED to BOX2D */
PG_FUNCTION_INFO_V1(LWGEOM_to_BOX2DF);
Datum LWGEOM_to_BOX2DF(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GBOX gbox;

	if ( gserialized_get_gbox_p(geom, &gbox) == LW_FAILURE )
		PG_RETURN_NULL();

	/* Strip out higher dimensions */
	FLAGS_SET_Z(gbox.flags, 0);
	FLAGS_SET_M(gbox.flags, 0);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_POINTER(gbox_copy(&gbox));
}


/*----------------------------------------------------------
 *	Relational operators for BOXes.
 *		<, >, <=, >=, and == are based on box area.
 *---------------------------------------------------------*/

/*
 * box_same - are two boxes identical?
 */
PG_FUNCTION_INFO_V1(BOX2D_same);
Datum BOX2D_same(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPeq(box1->xmax, box2->xmax) &&
	               FPeq(box1->xmin, box2->xmin) &&
	               FPeq(box1->ymax, box2->ymax) &&
	               FPeq(box1->ymin, box2->ymin));
}

/*
 * box_overlap - does box1 overlap box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_overlap);
Datum BOX2D_overlap(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);
	bool       result;


	result = ((FPge(box1->xmax, box2->xmax) &&
	           FPle(box1->xmin, box2->xmax)) ||
	          (FPge(box2->xmax, box1->xmax) &&
	           FPle(box2->xmin, box1->xmax)))
	         &&
	         ((FPge(box1->ymax, box2->ymax) &&
	           FPle(box1->ymin, box2->ymax)) ||
	          (FPge(box2->ymax, box1->ymax) &&
	           FPle(box2->ymin, box1->ymax)));

	PG_RETURN_BOOL(result);
}


/*
 * box_overleft - is the right edge of box1 to the left of
 *                the right edge of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_overleft);
Datum BOX2D_overleft(PG_FUNCTION_ARGS)
{
	GBOX *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPle(box1->xmax, box2->xmax));
}

/*
 * box_left - is box1 strictly left of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_left);
Datum BOX2D_left(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPlt(box1->xmax, box2->xmin));
}

/*
 * box_right - is box1 strictly right of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_right);
Datum BOX2D_right(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPgt(box1->xmin, box2->xmax));
}

/*
 * box_overright - is the left edge of box1 to the right of
 *                 the left edge of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_overright);
Datum BOX2D_overright(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPge(box1->xmin, box2->xmin));
}

/*
 * box_overbelow - is the bottom edge of box1 below
 *                 the bottom edge of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_overbelow);
Datum BOX2D_overbelow(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPle(box1->ymax, box2->ymax));
}

/*
 * box_below - is box1 strictly below box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_below);
Datum BOX2D_below(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPlt(box1->ymax, box2->ymin));
}

/*
 * box_above - is box1 strictly above box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_above);
Datum BOX2D_above(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPgt(box1->ymin, box2->ymax));
}

/*
 * box_overabove - the top edge of box1 above
 *                 the top edge of box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_overabove);
Datum BOX2D_overabove(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPge(box1->ymin, box2->ymin));
}

/*
 * box_contained - is box1 contained by box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_contained);
Datum BOX2D_contained(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 =(GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPle(box1->xmax, box2->xmax) &&
	               FPge(box1->xmin, box2->xmin) &&
	               FPle(box1->ymax, box2->ymax) &&
	               FPge(box1->ymin, box2->ymin));
}

/*
 * box_contain - does box1 contain box2?
 */
PG_FUNCTION_INFO_V1(BOX2D_contain);
Datum BOX2D_contain(PG_FUNCTION_ARGS)
{
	GBOX		   *box1 = (GBOX *) PG_GETARG_POINTER(0);
	GBOX		   *box2 = (GBOX *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(FPge(box1->xmax, box2->xmax) &&
	               FPle(box1->xmin, box2->xmin) &&
	               FPge(box1->ymax, box2->ymax) &&
	               FPle(box1->ymin, box2->ymin));

}

PG_FUNCTION_INFO_V1(BOX2D_intersects);
Datum BOX2D_intersects(PG_FUNCTION_ARGS)
{
	GBOX *a = (GBOX *) PG_GETARG_POINTER(0);
	GBOX *b = (GBOX *) PG_GETARG_POINTER(1);
	GBOX *n;


	n = (GBOX *) palloc(sizeof(GBOX));

	n->xmax = Min(a->xmax, b->xmax);
	n->ymax = Min(a->ymax, b->ymax);
	n->xmin = Max(a->xmin, b->xmin);
	n->ymin = Max(a->ymin, b->ymin);


	if (n->xmax < n->xmin || n->ymax < n->ymin)
	{
		pfree(n);
		/* Indicate "no intersection" by returning NULL pointer */
		n = NULL;
	}

	PG_RETURN_POINTER(n);
}


/*
 * union of two BOX2Ds
 */
PG_FUNCTION_INFO_V1(BOX2D_union);
Datum BOX2D_union(PG_FUNCTION_ARGS)
{
	GBOX *a = (GBOX*) PG_GETARG_POINTER(0);
	GBOX *b = (GBOX*) PG_GETARG_POINTER(1);
	GBOX *n;

	n = (GBOX *) lwalloc(sizeof(GBOX));
	if ( ! gbox_union(a,b,n) ) PG_RETURN_NULL();
	PG_RETURN_POINTER(n);
}


PG_FUNCTION_INFO_V1(BOX2D_expand);
Datum BOX2D_expand(PG_FUNCTION_ARGS)
{
	GBOX *box = (GBOX *)PG_GETARG_POINTER(0);
	double d = PG_GETARG_FLOAT8(1);
	GBOX *result = (GBOX *)palloc(sizeof(GBOX));

	memcpy(result, box, sizeof(GBOX));
    gbox_expand(result, d);
    
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX2D_to_BOX3D);
Datum BOX2D_to_BOX3D(PG_FUNCTION_ARGS)
{
	GBOX *box = (GBOX *)PG_GETARG_POINTER(0);
	BOX3D *result = box3d_from_gbox(box);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX2D_combine);
Datum BOX2D_combine(PG_FUNCTION_ARGS)
{
	Pointer box2d_ptr = PG_GETARG_POINTER(0);
	Pointer geom_ptr = PG_GETARG_POINTER(1);
	GBOX *a,*b;
	GSERIALIZED *lwgeom;
	GBOX box, *result;

	if  ( (box2d_ptr == NULL) && (geom_ptr == NULL) )
	{
		PG_RETURN_NULL(); /* combine_box2d(null,null) => null */
	}

	result = (GBOX *)palloc(sizeof(GBOX));

	if (box2d_ptr == NULL)
	{
		lwgeom = PG_GETARG_GSERIALIZED_P(1);
		/* empty geom would make getbox2d_p return NULL */
		if ( ! gserialized_get_gbox_p(lwgeom, &box) ) PG_RETURN_NULL();
		memcpy(result, &box, sizeof(GBOX));
		PG_RETURN_POINTER(result);
	}

	/* combine_bbox(BOX3D, null) => BOX3D */
	if (geom_ptr == NULL)
	{
		memcpy(result, (char *)PG_GETARG_DATUM(0), sizeof(GBOX));
		PG_RETURN_POINTER(result);
	}

	/*combine_bbox(BOX3D, geometry) => union(BOX3D, geometry->bvol) */

	lwgeom = PG_GETARG_GSERIALIZED_P(1);
	if ( ! gserialized_get_gbox_p(lwgeom, &box) )
	{
		/* must be the empty geom */
		memcpy(result, (char *)PG_GETARG_DATUM(0), sizeof(GBOX));
		PG_RETURN_POINTER(result);
	}

	a = (GBOX *)PG_GETARG_DATUM(0);
	b = &box;

	result->xmax = Max(a->xmax, b->xmax);
	result->ymax = Max(a->ymax, b->ymax);
	result->xmin = Min(a->xmin, b->xmin);
	result->ymin = Min(a->ymin, b->ymin);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX2D_to_LWGEOM);
Datum BOX2D_to_LWGEOM(PG_FUNCTION_ARGS)
{
	GBOX *box = (GBOX *)PG_GETARG_POINTER(0);
	POINTARRAY *pa = ptarray_construct_empty(0, 0, 5);
	POINT4D pt;
	GSERIALIZED *result;


	/*
	 * Alter BOX2D cast so that a valid geometry is always
	 * returned depending upon the size of the BOX2D. The
	 * code makes the following assumptions:
	 *     - If the BOX2D is a single point then return a
	 *     POINT geometry
	 *     - If the BOX2D represents either a horizontal or
	 *     vertical line, return a LINESTRING geometry
	 *     - Otherwise return a POLYGON
	 */

	if ( (box->xmin == box->xmax) && (box->ymin == box->ymax) )
	{
		/* Construct and serialize point */
		LWPOINT *point = lwpoint_make2d(SRID_UNKNOWN, box->xmin, box->ymin);
		result = geometry_serialize(lwpoint_as_lwgeom(point));
		lwpoint_free(point);
	}
	else if ( (box->xmin == box->xmax) || (box->ymin == box->ymax) )
	{
		LWLINE *line;

		/* Assign coordinates to point array */
		pt.x = box->xmin;
		pt.y = box->ymin;
		ptarray_append_point(pa, &pt, LW_TRUE);
		pt.x = box->xmax;
		pt.y = box->ymax;
		ptarray_append_point(pa, &pt, LW_TRUE);

		/* Construct and serialize linestring */
		line = lwline_construct(SRID_UNKNOWN, NULL, pa);
		result = geometry_serialize(lwline_as_lwgeom(line));
		lwline_free(line);
	}
	else
	{
		LWPOLY *poly;
		POINTARRAY **ppa = lwalloc(sizeof(POINTARRAY*));

		/* Assign coordinates to point array */
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

		/* Construct polygon */
		ppa[0] = pa;
		poly = lwpoly_construct(SRID_UNKNOWN, NULL, 1, ppa);
		result = geometry_serialize(lwpoly_as_lwgeom(poly));
		lwpoly_free(poly);
	}

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(BOX2D_construct);
Datum BOX2D_construct(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pgmin = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *pgmax = PG_GETARG_GSERIALIZED_P(1);
	GBOX *result;
	LWPOINT *minpoint, *maxpoint;
	double min, max, tmp;

	minpoint = (LWPOINT*)lwgeom_from_gserialized(pgmin);
	maxpoint = (LWPOINT*)lwgeom_from_gserialized(pgmax);

	if ( (minpoint->type != POINTTYPE) || (maxpoint->type != POINTTYPE) )
	{
		elog(ERROR, "GBOX_construct: arguments must be points");
		PG_RETURN_NULL();
	}

	error_if_srid_mismatch(minpoint->srid, maxpoint->srid);

	result = gbox_new(gflags(0, 0, 0));

	/* Process X min/max */
	min = lwpoint_get_x(minpoint);
	max = lwpoint_get_x(maxpoint);
	if ( min > max ) 
	{
		tmp = min;
		min = max;
		max = tmp;
	}
	result->xmin = min;
	result->xmax = max;

	/* Process Y min/max */
	min = lwpoint_get_y(minpoint);
	max = lwpoint_get_y(maxpoint);
	if ( min > max ) 
	{
		tmp = min;
		min = max;
		max = tmp;
	}
	result->ymin = min;
	result->ymax = max;

	PG_RETURN_POINTER(result);
}

