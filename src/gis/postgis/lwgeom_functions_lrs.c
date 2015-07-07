/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2001-2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <math.h>

#include "postgres.h"
#include "fmgr.h"

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

/*
* Add a measure dimension to a line, interpolating linearly from the
* start value to the end value.
* ST_AddMeasure(Geometry, StartMeasure, EndMeasure) returns Geometry
*/
Datum ST_AddMeasure(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_AddMeasure);
Datum ST_AddMeasure(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gin = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *gout;
	double start_measure = PG_GETARG_FLOAT8(1);
	double end_measure = PG_GETARG_FLOAT8(2);
	LWGEOM *lwin, *lwout;
	int type = gserialized_get_type(gin);

	/* Raise an error if input is not a linestring or multilinestring */
	if ( type != LINETYPE && type != MULTILINETYPE )
	{
		lwerror("Only LINESTRING and MULTILINESTRING are supported");
		PG_RETURN_NULL();
	}

	lwin = lwgeom_from_gserialized(gin);
	if ( type == LINETYPE )
		lwout = (LWGEOM*)lwline_measured_from_lwline((LWLINE*)lwin, start_measure, end_measure);
	else
		lwout = (LWGEOM*)lwmline_measured_from_lwmline((LWMLINE*)lwin, start_measure, end_measure);

	lwgeom_free(lwin);

	if ( lwout == NULL )
		PG_RETURN_NULL();

	gout = geometry_serialize(lwout);
	lwgeom_free(lwout);

	PG_RETURN_POINTER(gout);
}


/*
* Locate a point along a feature based on a measure value.
* ST_LocateAlong(Geometry, Measure, [Offset]) returns Geometry
*/
Datum ST_LocateAlong(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_LocateAlong);
Datum ST_LocateAlong(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gin = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *gout;
	LWGEOM *lwin = NULL, *lwout = NULL;
	double measure = PG_GETARG_FLOAT8(1);
	double offset = PG_GETARG_FLOAT8(2);;

	lwin = lwgeom_from_gserialized(gin);
	lwout = lwgeom_locate_along(lwin, measure, offset);
	lwgeom_free(lwin);
	PG_FREE_IF_COPY(gin, 0);

	if ( ! lwout )
		PG_RETURN_NULL();

	gout = geometry_serialize(lwout);
	lwgeom_free(lwout);

	PG_RETURN_POINTER(gout);
}


/*
* Locate the portion of a line between the specified measures
*/
Datum ST_LocateBetween(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_LocateBetween);
Datum ST_LocateBetween(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom_in = PG_GETARG_GSERIALIZED_P(0);
	double from = PG_GETARG_FLOAT8(1);
	double to = PG_GETARG_FLOAT8(2);
	double offset = PG_GETARG_FLOAT8(3);
	LWCOLLECTION *geom_out = NULL;
	LWGEOM *line_in = NULL;
	static char ordinate = 'M'; /* M */

	if ( ! gserialized_has_m(geom_in) )
	{
		elog(ERROR,"This function only accepts geometries that have an M dimension.");
		PG_RETURN_NULL();
	}

	/* This should be a call to ST_LocateAlong! */
	if ( to == from )
	{
		PG_RETURN_DATUM(DirectFunctionCall3(ST_LocateAlong, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1), PG_GETARG_DATUM(3)));
	}

	line_in = lwgeom_from_gserialized(geom_in);
	geom_out = lwgeom_clip_to_ordinate_range(line_in,  ordinate, from, to, offset);	
	lwgeom_free(line_in);
	PG_FREE_IF_COPY(geom_in, 0);

	if ( ! geom_out )
	{
		elog(ERROR,"lwline_clip_to_ordinate_range returned null");
		PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(geometry_serialize((LWGEOM*)geom_out));
}

/*
* Locate the portion of a line between the specified elevations
*/
Datum ST_LocateBetweenElevations(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_LocateBetweenElevations);
Datum ST_LocateBetweenElevations(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom_in = PG_GETARG_GSERIALIZED_P(0);
	double from = PG_GETARG_FLOAT8(1);
	double to = PG_GETARG_FLOAT8(2);
	LWCOLLECTION *geom_out = NULL;
	LWGEOM *line_in = NULL;
	static char ordinate = 'Z'; /* Z */
	static double offset = 0.0;

	if ( ! gserialized_has_z(geom_in) )
	{
		elog(ERROR,"This function only accepts LINESTRING or MULTILINESTRING with Z dimensions.");
		PG_RETURN_NULL();
	}

	line_in = lwgeom_from_gserialized(geom_in);
	geom_out = lwgeom_clip_to_ordinate_range(line_in,  ordinate, from, to, offset);	
	lwgeom_free(line_in);
	PG_FREE_IF_COPY(geom_in, 0);

	if ( ! geom_out )
	{
		elog(ERROR,"lwline_clip_to_ordinate_range returned null");
		PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(geometry_serialize((LWGEOM*)geom_out));
}


Datum ST_InterpolatePoint(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_InterpolatePoint);
Datum ST_InterpolatePoint(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gser_line = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *gser_point = PG_GETARG_GSERIALIZED_P(1);
	LWGEOM *lwline;
	LWPOINT *lwpoint;

	if ( gserialized_get_type(gser_line) != LINETYPE )
	{
		elog(ERROR,"ST_InterpolatePoint: 1st argument isn't a line");
		PG_RETURN_NULL();
	}
	if ( gserialized_get_type(gser_point) != POINTTYPE )
	{
		elog(ERROR,"ST_InterpolatePoint: 2st argument isn't a point");
		PG_RETURN_NULL();
	}
	if ( gserialized_get_srid(gser_line) != gserialized_get_srid(gser_point) )
	{
		elog(ERROR, "Operation on two geometries with different SRIDs");
		PG_RETURN_NULL();
	}
	if ( ! gserialized_has_m(gser_line) )
	{
		elog(ERROR,"ST_InterpolatePoint only accepts geometries that have an M dimension");
		PG_RETURN_NULL();
	}
	
	lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(gser_point));
	lwline = lwgeom_from_gserialized(gser_line);
	
	PG_RETURN_FLOAT8(lwgeom_interpolate_point(lwline, lwpoint));
}


Datum LWGEOM_line_locate_point(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(LWGEOM_line_locate_point);
Datum LWGEOM_line_locate_point(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	LWLINE *lwline;
	LWPOINT *lwpoint;
	POINTARRAY *pa;
	POINT4D p, p_proj;
	double ret;

	if ( gserialized_get_type(geom1) != LINETYPE )
	{
		elog(ERROR,"line_locate_point: 1st arg isnt a line");
		PG_RETURN_NULL();
	}
	if ( gserialized_get_type(geom2) != POINTTYPE )
	{
		elog(ERROR,"line_locate_point: 2st arg isnt a point");
		PG_RETURN_NULL();
	}
	if ( gserialized_get_srid(geom1) != gserialized_get_srid(geom2) )
	{
		elog(ERROR, "Operation on two geometries with different SRIDs");
		PG_RETURN_NULL();
	}

	lwline = lwgeom_as_lwline(lwgeom_from_gserialized(geom1));
	lwpoint = lwgeom_as_lwpoint(lwgeom_from_gserialized(geom2));

	pa = lwline->points;
	lwpoint_getPoint4d_p(lwpoint, &p);

	ret = ptarray_locate_point(pa, &p, NULL, &p_proj);

	PG_RETURN_FLOAT8(ret);
}


/***********************************************************************
* LEGACY SUPPORT FOR locate_between_measures and locate_along_measure
* Deprecated at PostGIS 2.0. To be removed.
*/


typedef struct
{
	POINTARRAY **ptarrays;
	uint32 nptarrays;
}
POINTARRAYSET;

static POINTARRAYSET ptarray_locate_between_m(
    POINTARRAY *ipa, double m0, double m1);

static LWGEOM *lwcollection_locate_between_m(
    LWCOLLECTION *lwcoll, double m0, double m1);

static LWGEOM *lwgeom_locate_between_m(
    LWGEOM *lwin, double m0, double m1);

static LWGEOM *lwline_locate_between_m(
    LWLINE *lwline_in, double m0, double m1);

static LWGEOM *lwpoint_locate_between_m(
    LWPOINT *lwpoint, double m0, double m1);

static int clip_seg_by_m_range(
    POINT4D *p1, POINT4D *p2, double m0, double m1);


/*
 * Clip a segment by a range of measures.
 * Z and M values are interpolated in case of clipping.
 *
 * Returns a bitfield, flags being:
 *    0x0001 : segment intersects the range
 *    0x0010 : first point is modified
 *    0x0100 : second point is modified
 *
 * Values:
 *     - 0 segment fully outside the range, no modifications
 *     - 1 segment fully inside the range, no modifications
 *     - 7 segment crosses the range, both points modified.
 *     - 3 first point out, second in, first point modified
 *     - 5 first point in, second out, second point modified
 */
static int
clip_seg_by_m_range(POINT4D *p1, POINT4D *p2, double m0, double m1)
{
	double dM0, dM1, dX, dY, dZ;
	POINT4D *tmp;
	int swapped=0;
	int ret=0;

	POSTGIS_DEBUGF(3, "m0: %g m1: %g", m0, m1);

	/* Handle corner case of m values being the same */
	if ( p1->m == p2->m )
	{
		/* out of range, no clipping */
		if ( p1->m < m0 || p1->m > m1 )
			return 0;

		/* inside range, no clipping */
		return 1;
	}

	/*
	 * Order points so that p1 has the smaller M
	 */
	if ( p1->m > p2->m )
	{
		tmp=p2;
		p2=p1;
		p1=tmp;
		swapped=1;
	}

	/*
	 * The M range is not intersected, segment
	 * fully out of range, no clipping.
	 */
	if ( p2->m < m0 || p1->m > m1 )
		return 0;

	/*
	 * The segment is fully inside the range,
	 * no clipping.
	 */
	if ( p1->m >= m0 && p2->m <= m1 )
		return 1;

	/*
	 * Segment intersects range, lets compute
	 * the proportional location of the two
	 * measures wrt p1/p2 m range.
	 *
	 * if p1 and p2 have the same measure
	 * this should never be reached (either
	 * both inside or both outside)
	 *
	 */
	dM0=(m0-p1->m)/(p2->m-p1->m); /* delta-M0 */
	dM1=(m1-p2->m)/(p2->m-p1->m); /* delta-M1 */
	dX=p2->x-p1->x;
	dY=p2->y-p1->y;
	dZ=p2->z-p1->z;

	POSTGIS_DEBUGF(3, "dM0:%g dM1:%g", dM0, dM1);
	POSTGIS_DEBUGF(3, "dX:%g dY:%g dZ:%g", dX, dY, dZ);
	POSTGIS_DEBUGF(3, "swapped: %d", swapped);

	/*
	 * First point out of range, project
	 * it on the range
	 */
	if ( p1->m < m0 )
	{
		/*
		 * To prevent rounding errors, then if m0==m1 and p2 lies within the range, copy
		 * p1 as a direct copy of p2
		 */
		if (m0 == m1 && p2->m <= m1)
		{
			memcpy(p1, p2, sizeof(POINT4D));

			POSTGIS_DEBUG(3, "Projected p1 on range (as copy of p2)");
		}
		else
		{
			/* Otherwise interpolate coordinates */
			p1->x += (dX*dM0);
			p1->y += (dY*dM0);
			p1->z += (dZ*dM0);
			p1->m = m0;

			POSTGIS_DEBUG(3, "Projected p1 on range");
		}

		if ( swapped ) ret |= 0x0100;
		else ret |= 0x0010;
	}

	/*
	 * Second point out of range, project
	 * it on the range
	 */
	if ( p2->m > m1 )
	{
		/*
		 * To prevent rounding errors, then if m0==m1 and p1 lies within the range, copy
		 * p2 as a direct copy of p1
		 */
		if (m0 == m1 && p1->m >= m0)
		{
			memcpy(p2, p1, sizeof(POINT4D));

			POSTGIS_DEBUG(3, "Projected p2 on range (as copy of p1)");
		}
		else
		{
			/* Otherwise interpolate coordinates */
			p2->x += (dX*dM1);
			p2->y += (dY*dM1);
			p2->z += (dZ*dM1);
			p2->m = m1;

			POSTGIS_DEBUG(3, "Projected p2 on range");
		}

		if ( swapped ) ret |= 0x0010;
		else ret |= 0x0100;
	}

	/* Clipping occurred */
	return ret;

}

static POINTARRAYSET
ptarray_locate_between_m(POINTARRAY *ipa, double m0, double m1)
{
	POINTARRAYSET ret;
	POINTARRAY *dpa=NULL;
	int i;

	ret.nptarrays=0;

	/*
	 * We allocate space for as many pointarray as
	 * segments in the input POINTARRAY, as worst
	 * case is for each segment to cross the M range
	 * window.
	 * TODO: rework this to reduce used memory
	 */
	ret.ptarrays=lwalloc(sizeof(POINTARRAY *)*ipa->npoints-1);

	POSTGIS_DEBUGF(2, "ptarray_locate...: called for pointarray %x, m0:%g, m1:%g",
	         ipa, m0, m1);


	for (i=1; i<ipa->npoints; i++)
	{
		POINT4D p1, p2;
		int clipval;

		getPoint4d_p(ipa, i-1, &p1);
		getPoint4d_p(ipa, i, &p2);

		POSTGIS_DEBUGF(3, " segment %d-%d [ %g %g %g %g -  %g %g %g %g ]",
		         i-1, i,
		         p1.x, p1.y, p1.z, p1.m,
		         p2.x, p2.y, p2.z, p2.m);

		clipval = clip_seg_by_m_range(&p1, &p2, m0, m1);

		/* segment completely outside, nothing to do */
		if (! clipval ) continue;

		POSTGIS_DEBUGF(3, " clipped to: [ %g %g %g %g - %g %g %g %g ]   clipval: %x", p1.x, p1.y, p1.z, p1.m,
		         p2.x, p2.y, p2.z, p2.m, clipval);

		/* If no points have been accumulated so far, then if clipval != 0 the first point must be the
		   start of the intersection */
		if (dpa == NULL)
		{
			POSTGIS_DEBUGF(3, " 1 creating new POINTARRAY with first point %g,%g,%g,%g", p1.x, p1.y, p1.z, p1.m);

			dpa = ptarray_construct_empty(FLAGS_GET_Z(ipa->flags), FLAGS_GET_M(ipa->flags), ipa->npoints-i);
			ptarray_append_point(dpa, &p1, LW_TRUE);
		}

		/* Otherwise always add the next point, avoiding duplicates */
		if (dpa)
			ptarray_append_point(dpa, &p2, LW_FALSE);

		/*
		 * second point has been clipped
		 */
		if ( clipval & 0x0100 || i == ipa->npoints-1 )
		{
			POSTGIS_DEBUGF(3, " closing pointarray %x with %d points", dpa, dpa->npoints);

			ret.ptarrays[ret.nptarrays++] = dpa;
			dpa = NULL;
		}
	}

	/*
	 * if dpa!=NULL it means we didn't close it yet.
	 * this should never happen.
	 */
	if ( dpa != NULL ) lwerror("Something wrong with algorithm");

	return ret;
}

/*
 * Point is assumed to have an M value.
 * Return NULL if point is not in the given range (inclusive)
 * Return an LWPOINT *copy* otherwise.
 */
static LWGEOM *
lwpoint_locate_between_m(LWPOINT *lwpoint, double m0, double m1)
{
	POINT3DM p3dm;

	POSTGIS_DEBUGF(2, "lwpoint_locate_between called for lwpoint %x", lwpoint);

	lwpoint_getPoint3dm_p(lwpoint, &p3dm);
	if ( p3dm.m >= m0 && p3dm.m <= m1)
	{
		POSTGIS_DEBUG(3, " lwpoint... returning a clone of input");

		return (LWGEOM *)lwpoint_clone(lwpoint);
	}
	else
	{
		POSTGIS_DEBUG(3, " lwpoint... returning a clone of input");

		return NULL;
	}
}

/*
 * Line is assumed to have an M value.
 *
 * Return NULL if no parts of the line are in the given range (inclusive)
 *
 * Return an LWCOLLECTION with LWLINES and LWPOINT being consecutive
 * and isolated points on the line falling in the range.
 *
 * X,Y and Z (if present) ordinates are interpolated.
 *
 */
static LWGEOM *
lwline_locate_between_m(LWLINE *lwline_in, double m0, double m1)
{
	POINTARRAY *ipa=lwline_in->points;
	int i;
	LWGEOM **geoms;
	int ngeoms;
	int outtype;
	int typeflag=0; /* see flags below */
	const int pointflag=0x01;
	const int lineflag=0x10;
	POINTARRAYSET paset=ptarray_locate_between_m(ipa, m0, m1);

	POSTGIS_DEBUGF(2, "lwline_locate_between called for lwline %x", lwline_in);

	POSTGIS_DEBUGF(3, " ptarray_locate... returned %d pointarrays",
	         paset.nptarrays);

	if ( paset.nptarrays == 0 )
	{
		return NULL;
	}

	ngeoms=paset.nptarrays;
	/* TODO: rework this to reduce used memory */
	geoms=lwalloc(sizeof(LWGEOM *)*ngeoms);
	for (i=0; i<ngeoms; i++)
	{
		LWPOINT *lwpoint;
		LWLINE *lwline;

		POINTARRAY *pa=paset.ptarrays[i];

		/* This is a point */
		if ( pa->npoints == 1 )
		{
			lwpoint = lwpoint_construct(lwline_in->srid, NULL, pa);
			geoms[i]=(LWGEOM *)lwpoint;
			typeflag|=pointflag;
		}

		/* This is a line */
		else if ( pa->npoints > 1 )
		{
			lwline = lwline_construct(lwline_in->srid, NULL, pa);
			geoms[i]=(LWGEOM *)lwline;
			typeflag|=lineflag;
		}

		/* This is a bug */
		else
		{
			lwerror("ptarray_locate_between_m returned a POINARRAY set containing POINTARRAY with 0 points");
		}

	}

	if ( ngeoms == 1 )
	{
		return geoms[0];
	}
	else
	{
		/* Choose best type */
		if ( typeflag == 1 ) outtype=MULTIPOINTTYPE;
		else if ( typeflag == 2 ) outtype=MULTILINETYPE;
		else outtype = COLLECTIONTYPE;

		return (LWGEOM *)lwcollection_construct(outtype,
		                                        lwline_in->srid, NULL, ngeoms, geoms);
	}
}

/*
 * Return a fully new allocated LWCOLLECTION
 * always tagged as COLLECTIONTYPE.
 */
static LWGEOM *
lwcollection_locate_between_m(LWCOLLECTION *lwcoll, double m0, double m1)
{
	int i;
	int ngeoms=0;
	LWGEOM **geoms;

	POSTGIS_DEBUGF(2, "lwcollection_locate_between_m called for lwcoll %x", lwcoll);

	geoms=lwalloc(sizeof(LWGEOM *)*lwcoll->ngeoms);
	for (i=0; i<lwcoll->ngeoms; i++)
	{
		LWGEOM *sub=lwgeom_locate_between_m(lwcoll->geoms[i],
		                                    m0, m1);
		if ( sub != NULL )
			geoms[ngeoms++] = sub;
	}

	if ( ngeoms == 0 ) return NULL;

	return (LWGEOM *)lwcollection_construct(COLLECTIONTYPE,
	                                        lwcoll->srid, NULL, ngeoms, geoms);
}

/*
 * Return a fully allocated LWGEOM containing elements
 * intersected/interpolated with the given M range.
 * Return NULL if none of the elements fall in the range.
 *
 * m0 is assumed to be less-or-equal to m1.
 * input LWGEOM is assumed to contain an M value.
 *
 */
static LWGEOM *
lwgeom_locate_between_m(LWGEOM *lwin, double m0, double m1)
{
	POSTGIS_DEBUGF(2, "lwgeom_locate_between called for lwgeom %x", lwin);

	switch (lwin->type)
	{
	case POINTTYPE:
		return lwpoint_locate_between_m(
		           (LWPOINT *)lwin, m0, m1);
	case LINETYPE:
		return lwline_locate_between_m(
		           (LWLINE *)lwin, m0, m1);

	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case COLLECTIONTYPE:
		return lwcollection_locate_between_m(
		           (LWCOLLECTION *)lwin, m0, m1);

		/* Polygon types are not supported */
	case POLYGONTYPE:
	case MULTIPOLYGONTYPE:
		lwerror("Areal geometries are not supported by locate_between_measures");
		return NULL;
	}

	lwerror("Unkonwn geometry type (%s:%d)", __FILE__, __LINE__);
	return NULL;
}

/*
 * Return a derived geometry collection value with elements that match
 * the specified range of measures inclusively.
 *
 * Implements SQL/MM ST_LocateBetween(measure, measure) method.
 * See ISO/IEC CD 13249-3:200x(E)
 *
 */
Datum LWGEOM_locate_between_m(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(LWGEOM_locate_between_m);
Datum LWGEOM_locate_between_m(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gin = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *gout;
	double start_measure = PG_GETARG_FLOAT8(1);
	double end_measure = PG_GETARG_FLOAT8(2);
	LWGEOM *lwin, *lwout;
	int hasz = gserialized_has_z(gin);
	int hasm = gserialized_has_m(gin);
	int type;

	elog(WARNING,"ST_Locate_Between_Measures and ST_Locate_Along_Measure were deprecated in 2.2.0. Please use ST_LocateAlong and ST_LocateBetween");

	if ( end_measure < start_measure )
	{
		lwerror("locate_between_m: 2nd arg must be bigger then 1st arg");
		PG_RETURN_NULL();
	}
	
	/*
	 * Return error if input doesn't have a measure
	 */
	if ( ! hasm )
	{
		lwerror("Geometry argument does not have an 'M' ordinate");
		PG_RETURN_NULL();
	}

	/*
	 * Raise an error if input is a polygon, a multipolygon
	 * or a collection
	 */
	type = gserialized_get_type(gin);

	if ( type == POLYGONTYPE || type == MULTIPOLYGONTYPE || type == COLLECTIONTYPE )
	{
		lwerror("Areal or Collection types are not supported");
		PG_RETURN_NULL();
	}

	lwin = lwgeom_from_gserialized(gin);

	lwout = lwgeom_locate_between_m(lwin,
	                                start_measure, end_measure);

	lwgeom_free(lwin);

	if ( lwout == NULL )
	{
		lwout = (LWGEOM *)lwcollection_construct_empty(COLLECTIONTYPE, 
		            gserialized_get_srid(gin), hasz, hasm);
	}

	gout = geometry_serialize(lwout);
	lwgeom_free(lwout);

	PG_RETURN_POINTER(gout);
}

