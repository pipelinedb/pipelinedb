/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2011-2014 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "lwgeom_geos.h"
#include "liblwgeom.h"
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"

#include <stdlib.h>

LWTIN *lwtin_from_geos(const GEOSGeometry *geom, int want3d);

#undef LWGEOM_PROFILE_BUILDAREA

#define LWGEOM_GEOS_ERRMSG_MAXSIZE 256
char lwgeom_geos_errmsg[LWGEOM_GEOS_ERRMSG_MAXSIZE];

extern void
lwgeom_geos_error(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	/* Call the supplied function */
	if ( LWGEOM_GEOS_ERRMSG_MAXSIZE-1 < vsnprintf(lwgeom_geos_errmsg, LWGEOM_GEOS_ERRMSG_MAXSIZE-1, fmt, ap) )
	{
		lwgeom_geos_errmsg[LWGEOM_GEOS_ERRMSG_MAXSIZE-1] = '\0';
	}

	va_end(ap);
}


/*
**  GEOS <==> PostGIS conversion functions
**
** Default conversion creates a GEOS point array, then iterates through the
** PostGIS points, setting each value in the GEOS array one at a time.
**
*/

/* Return a POINTARRAY from a GEOSCoordSeq */
POINTARRAY *
ptarray_from_GEOSCoordSeq(const GEOSCoordSequence *cs, char want3d)
{
	uint32_t dims=2;
	uint32_t size, i;
	POINTARRAY *pa;
	POINT4D point;

	LWDEBUG(2, "ptarray_fromGEOSCoordSeq called");

	if ( ! GEOSCoordSeq_getSize(cs, &size) )
		lwerror("Exception thrown");

	LWDEBUGF(4, " GEOSCoordSeq size: %d", size);

	if ( want3d )
	{
		if ( ! GEOSCoordSeq_getDimensions(cs, &dims) )
			lwerror("Exception thrown");

		LWDEBUGF(4, " GEOSCoordSeq dimensions: %d", dims);

		/* forget higher dimensions (if any) */
		if ( dims > 3 ) dims = 3;
	}

	LWDEBUGF(4, " output dimensions: %d", dims);

	pa = ptarray_construct((dims==3), 0, size);

	for (i=0; i<size; i++)
	{
		GEOSCoordSeq_getX(cs, i, &(point.x));
		GEOSCoordSeq_getY(cs, i, &(point.y));
		if ( dims >= 3 ) GEOSCoordSeq_getZ(cs, i, &(point.z));
		ptarray_set_point4d(pa,i,&point);
	}

	return pa;
}

/* Return an LWGEOM from a Geometry */
LWGEOM *
GEOS2LWGEOM(const GEOSGeometry *geom, char want3d)
{
	int type = GEOSGeomTypeId(geom) ;
	int hasZ;
	int SRID = GEOSGetSRID(geom);

	/* GEOS's 0 is equivalent to our unknown as for SRID values */
	if ( SRID == 0 ) SRID = SRID_UNKNOWN;

	if ( want3d )
	{
		hasZ = GEOSHasZ(geom);
		if ( ! hasZ )
		{
			LWDEBUG(3, "Geometry has no Z, won't provide one");

			want3d = 0;
		}
	}

/*
	if ( GEOSisEmpty(geom) )
	{
		return (LWGEOM*)lwcollection_construct_empty(COLLECTIONTYPE, SRID, want3d, 0);
	}
*/

	switch (type)
	{
		const GEOSCoordSequence *cs;
		POINTARRAY *pa, **ppaa;
		const GEOSGeometry *g;
		LWGEOM **geoms;
		uint32_t i, ngeoms;

	case GEOS_POINT:
		LWDEBUG(4, "lwgeom_from_geometry: it's a Point");
		cs = GEOSGeom_getCoordSeq(geom);
		if ( GEOSisEmpty(geom) )
		  return (LWGEOM*)lwpoint_construct_empty(SRID, want3d, 0);
		pa = ptarray_from_GEOSCoordSeq(cs, want3d);
		return (LWGEOM *)lwpoint_construct(SRID, NULL, pa);

	case GEOS_LINESTRING:
	case GEOS_LINEARRING:
		LWDEBUG(4, "lwgeom_from_geometry: it's a LineString or LinearRing");
		if ( GEOSisEmpty(geom) )
		  return (LWGEOM*)lwline_construct_empty(SRID, want3d, 0);

		cs = GEOSGeom_getCoordSeq(geom);
		pa = ptarray_from_GEOSCoordSeq(cs, want3d);
		return (LWGEOM *)lwline_construct(SRID, NULL, pa);

	case GEOS_POLYGON:
		LWDEBUG(4, "lwgeom_from_geometry: it's a Polygon");
		if ( GEOSisEmpty(geom) )
		  return (LWGEOM*)lwpoly_construct_empty(SRID, want3d, 0);
		ngeoms = GEOSGetNumInteriorRings(geom);
		ppaa = lwalloc(sizeof(POINTARRAY *)*(ngeoms+1));
		g = GEOSGetExteriorRing(geom);
		cs = GEOSGeom_getCoordSeq(g);
		ppaa[0] = ptarray_from_GEOSCoordSeq(cs, want3d);
		for (i=0; i<ngeoms; i++)
		{
			g = GEOSGetInteriorRingN(geom, i);
			cs = GEOSGeom_getCoordSeq(g);
			ppaa[i+1] = ptarray_from_GEOSCoordSeq(cs,
			                                      want3d);
		}
		return (LWGEOM *)lwpoly_construct(SRID, NULL,
		                                  ngeoms+1, ppaa);

	case GEOS_MULTIPOINT:
	case GEOS_MULTILINESTRING:
	case GEOS_MULTIPOLYGON:
	case GEOS_GEOMETRYCOLLECTION:
		LWDEBUG(4, "lwgeom_from_geometry: it's a Collection or Multi");

		ngeoms = GEOSGetNumGeometries(geom);
		geoms = NULL;
		if ( ngeoms )
		{
			geoms = lwalloc(sizeof(LWGEOM *)*ngeoms);
			for (i=0; i<ngeoms; i++)
			{
				g = GEOSGetGeometryN(geom, i);
				geoms[i] = GEOS2LWGEOM(g, want3d);
			}
		}
		return (LWGEOM *)lwcollection_construct(type,
		                                        SRID, NULL, ngeoms, geoms);

	default:
		lwerror("GEOS2LWGEOM: unknown geometry type: %d", type);
		return NULL;

	}

}



GEOSCoordSeq ptarray_to_GEOSCoordSeq(const POINTARRAY *);


GEOSCoordSeq
ptarray_to_GEOSCoordSeq(const POINTARRAY *pa)
{
	uint32_t dims = 2;
	uint32_t i;
	const POINT3DZ *p3d;
	const POINT2D *p2d;
	GEOSCoordSeq sq;

	if ( FLAGS_GET_Z(pa->flags) ) 
		dims = 3;

	if ( ! (sq = GEOSCoordSeq_create(pa->npoints, dims)) ) 
		lwerror("Error creating GEOS Coordinate Sequence");

	for ( i=0; i < pa->npoints; i++ )
	{
		if ( dims == 3 )
		{
			p3d = getPoint3dz_cp(pa, i);
			p2d = (const POINT2D *)p3d;
			LWDEBUGF(4, "Point: %g,%g,%g", p3d->x, p3d->y, p3d->z);
		}
		else
		{
			p2d = getPoint2d_cp(pa, i);
			LWDEBUGF(4, "Point: %g,%g", p2d->x, p2d->y);
		}

#if POSTGIS_GEOS_VERSION < 33
		/* Make sure we don't pass any infinite values down into GEOS */
		/* GEOS 3.3+ is supposed to  handle this stuff OK */
		if ( isinf(p2d->x) || isinf(p2d->y) || (dims == 3 && isinf(p3d->z)) )
			lwerror("Infinite coordinate value found in geometry.");
		if ( isnan(p2d->x) || isnan(p2d->y) || (dims == 3 && isnan(p3d->z)) )
			lwerror("NaN coordinate value found in geometry.");
#endif

		GEOSCoordSeq_setX(sq, i, p2d->x);
		GEOSCoordSeq_setY(sq, i, p2d->y);
		
		if ( dims == 3 ) 
			GEOSCoordSeq_setZ(sq, i, p3d->z);
	}
	return sq;
}

static GEOSGeometry *
ptarray_to_GEOSLinearRing(const POINTARRAY *pa, int autofix)
{
	GEOSCoordSeq sq;
	GEOSGeom g;
  POINTARRAY *npa = 0;

  if ( autofix )
  {
    /* check ring for being closed and fix if not */
    if ( ! ptarray_is_closed_2d(pa) ) {
      npa = ptarray_addPoint(pa, getPoint_internal(pa, 0),
                             FLAGS_NDIMS(pa->flags), pa->npoints);
      pa = npa;
    }
    /* TODO: check ring for having at least 4 vertices */
#if 0
    while ( pa->npoints < 4 ) {
      npa = ptarray_addPoint(npa, getPoint_internal(pa, 0),
                             FLAGS_NDIMS(pa->flags), pa->npoints);
    }
#endif
  }

  sq = ptarray_to_GEOSCoordSeq(pa);
  if ( npa ) ptarray_free(npa);
	g = GEOSGeom_createLinearRing(sq);
  return g;
}

GEOSGeometry *
LWGEOM2GEOS(const LWGEOM *lwgeom, int autofix)
{
	GEOSCoordSeq sq;
	GEOSGeom g, shell;
	GEOSGeom *geoms = NULL;
	/*
	LWGEOM *tmp;
	*/
	uint32_t ngeoms, i;
	int geostype;
#if LWDEBUG_LEVEL >= 4
	char *wkt;
#endif

	LWDEBUGF(4, "LWGEOM2GEOS got a %s", lwtype_name(lwgeom->type));

	if (lwgeom_has_arc(lwgeom))
	{
		LWDEBUG(3, "LWGEOM2GEOS: arced geometry found.");

		lwerror("Exception in LWGEOM2GEOS: curved geometry not supported.");
		return NULL;
	}
	
	switch (lwgeom->type)
	{
		LWPOINT *lwp = NULL;
		LWPOLY *lwpoly = NULL;
		LWLINE *lwl = NULL;
		LWCOLLECTION *lwc = NULL;
#if POSTGIS_GEOS_VERSION < 33
		POINTARRAY *pa = NULL;
#endif
		
	case POINTTYPE:
		lwp = (LWPOINT *)lwgeom;
		
		if ( lwgeom_is_empty(lwgeom) )
		{
#if POSTGIS_GEOS_VERSION < 33
			pa = ptarray_construct_empty(lwgeom_has_z(lwgeom), lwgeom_has_m(lwgeom), 2);
			sq = ptarray_to_GEOSCoordSeq(pa);
			shell = GEOSGeom_createLinearRing(sq);
			g = GEOSGeom_createPolygon(shell, NULL, 0);
#else
			g = GEOSGeom_createEmptyPolygon();
#endif
		}
		else
		{
			sq = ptarray_to_GEOSCoordSeq(lwp->point);
			g = GEOSGeom_createPoint(sq);
		}
		if ( ! g )
		{
			/* lwnotice("Exception in LWGEOM2GEOS"); */
			return NULL;
		}
		break;
	case LINETYPE:
		lwl = (LWLINE *)lwgeom;
		/* TODO: if (autofix) */
		if ( lwl->points->npoints == 1 ) {
			/* Duplicate point, to make geos-friendly */
			lwl->points = ptarray_addPoint(lwl->points,
		                           getPoint_internal(lwl->points, 0),
		                           FLAGS_NDIMS(lwl->points->flags),
		                           lwl->points->npoints);
		}
		sq = ptarray_to_GEOSCoordSeq(lwl->points);
		g = GEOSGeom_createLineString(sq);
		if ( ! g )
		{
			/* lwnotice("Exception in LWGEOM2GEOS"); */
			return NULL;
		}
		break;

	case POLYGONTYPE:
		lwpoly = (LWPOLY *)lwgeom;
		if ( lwgeom_is_empty(lwgeom) )
		{
#if POSTGIS_GEOS_VERSION < 33
			POINTARRAY *pa = ptarray_construct_empty(lwgeom_has_z(lwgeom), lwgeom_has_m(lwgeom), 2);
			sq = ptarray_to_GEOSCoordSeq(pa);
			shell = GEOSGeom_createLinearRing(sq);
			g = GEOSGeom_createPolygon(shell, NULL, 0);
#else
			g = GEOSGeom_createEmptyPolygon();
#endif
		}
		else
		{
			shell = ptarray_to_GEOSLinearRing(lwpoly->rings[0], autofix);
			if ( ! shell ) return NULL;
			/*lwerror("LWGEOM2GEOS: exception during polygon shell conversion"); */
			ngeoms = lwpoly->nrings-1;
			if ( ngeoms > 0 )
				geoms = malloc(sizeof(GEOSGeom)*ngeoms);

			for (i=1; i<lwpoly->nrings; ++i)
			{
				geoms[i-1] = ptarray_to_GEOSLinearRing(lwpoly->rings[i], autofix);
				if ( ! geoms[i-1] )
				{
					--i;
					while (i) GEOSGeom_destroy(geoms[--i]);
					free(geoms);
					GEOSGeom_destroy(shell);
					return NULL;
				}
				/*lwerror("LWGEOM2GEOS: exception during polygon hole conversion"); */
			}
			g = GEOSGeom_createPolygon(shell, geoms, ngeoms);
			if (geoms) free(geoms);
		}
		if ( ! g ) return NULL;
		break;
	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTIPOLYGONTYPE:
	case COLLECTIONTYPE:
		if ( lwgeom->type == MULTIPOINTTYPE )
			geostype = GEOS_MULTIPOINT;
		else if ( lwgeom->type == MULTILINETYPE )
			geostype = GEOS_MULTILINESTRING;
		else if ( lwgeom->type == MULTIPOLYGONTYPE )
			geostype = GEOS_MULTIPOLYGON;
		else
			geostype = GEOS_GEOMETRYCOLLECTION;

		lwc = (LWCOLLECTION *)lwgeom;

		ngeoms = lwc->ngeoms;
		if ( ngeoms > 0 )
			geoms = malloc(sizeof(GEOSGeom)*ngeoms);

		for (i=0; i<ngeoms; ++i)
		{
			GEOSGeometry* g = LWGEOM2GEOS(lwc->geoms[i], 0);
			if ( ! g )
			{
				while (i) GEOSGeom_destroy(geoms[--i]);
				free(geoms);
				return NULL;
			}
			geoms[i] = g;
		}
		g = GEOSGeom_createCollection(geostype, geoms, ngeoms);
		if ( geoms ) free(geoms);
		if ( ! g ) return NULL;
		break;

	default:
		lwerror("Unknown geometry type: %d - %s", lwgeom->type, lwtype_name(lwgeom->type));
		return NULL;
	}

	GEOSSetSRID(g, lwgeom->srid);

#if LWDEBUG_LEVEL >= 4
	wkt = GEOSGeomToWKT(g);
	LWDEBUGF(4, "LWGEOM2GEOS: GEOSGeom: %s", wkt);
	free(wkt);
#endif

	return g;
}

const char*
lwgeom_geos_version()
{
	const char *ver = GEOSversion();
	return ver;
}

LWGEOM *
lwgeom_normalize(const LWGEOM *geom1)
{
	LWGEOM *result ;
	GEOSGeometry *g1;
	int is3d ;
	int srid ;

	srid = (int)(geom1->srid);
	is3d = FLAGS_GET_Z(geom1->flags);

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = LWGEOM2GEOS(geom1, 0);
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL ;
	}

	if ( -1 == GEOSNormalize(g1) )
	{
	  lwerror("Error in GEOSNormalize: %s", lwgeom_geos_errmsg);
		return NULL; /* never get here */
	}

	GEOSSetSRID(g1, srid); /* needed ? */
	result = GEOS2LWGEOM(g1, is3d);
	GEOSGeom_destroy(g1);

	if (result == NULL)
	{
	  lwerror("Error performing intersection: GEOS2LWGEOM: %s",
	                lwgeom_geos_errmsg);
		return NULL ; /* never get here */
	}

	return result ;
}

LWGEOM *
lwgeom_intersection(const LWGEOM *geom1, const LWGEOM *geom2)
{
	LWGEOM *result ;
	GEOSGeometry *g1, *g2, *g3 ;
	int is3d ;
	int srid ;

	/* A.Intersection(Empty) == Empty */
	if ( lwgeom_is_empty(geom2) )
		return lwgeom_clone(geom2);

	/* Empty.Intersection(A) == Empty */
	if ( lwgeom_is_empty(geom1) )
		return lwgeom_clone(geom1);

	/* ensure srids are identical */
	srid = (int)(geom1->srid);
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	LWDEBUG(3, "intersection() START");

	g1 = LWGEOM2GEOS(geom1, 0);
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL ;
	}

	g2 = LWGEOM2GEOS(geom2, 0);
	if ( 0 == g2 )   /* exception thrown at construction */
	{
		lwerror("Second argument geometry could not be converted to GEOS.");
		GEOSGeom_destroy(g1);
		return NULL ;
	}

	LWDEBUG(3, " constructed geometrys - calling geos");
	LWDEBUGF(3, " g1 = %s", GEOSGeomToWKT(g1));
	LWDEBUGF(3, " g2 = %s", GEOSGeomToWKT(g2));
	/*LWDEBUGF(3, "g2 is valid = %i",GEOSisvalid(g2)); */
	/*LWDEBUGF(3, "g1 is valid = %i",GEOSisvalid(g1)); */

	g3 = GEOSIntersection(g1,g2);

	LWDEBUG(3, " intersection finished");

	if (g3 == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
	        lwerror("Error performing intersection: %s",
	                lwgeom_geos_errmsg);
		return NULL; /* never get here */
	}

	LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3) ) ;

	GEOSSetSRID(g3, srid);

	result = GEOS2LWGEOM(g3, is3d);

	if (result == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g3);
	        lwerror("Error performing intersection: GEOS2LWGEOM: %s",
	                lwgeom_geos_errmsg);
		return NULL ; /* never get here */
	}

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);
	GEOSGeom_destroy(g3);

	return result ;
}

LWGEOM *
lwgeom_difference(const LWGEOM *geom1, const LWGEOM *geom2)
{
	GEOSGeometry *g1, *g2, *g3;
	LWGEOM *result;
	int is3d;
	int srid;

	/* A.Difference(Empty) == A */
	if ( lwgeom_is_empty(geom2) )
		return lwgeom_clone(geom1);

	/* Empty.Intersection(A) == Empty */
	if ( lwgeom_is_empty(geom1) )
		return lwgeom_clone(geom1);

	/* ensure srids are identical */
	srid = (int)(geom1->srid);
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = LWGEOM2GEOS(geom1, 0);
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = LWGEOM2GEOS(geom2, 0);
	if ( 0 == g2 )   /* exception thrown at construction */
	{
		GEOSGeom_destroy(g1);
		lwerror("Second argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g3 = GEOSDifference(g1,g2);

	if (g3 == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		lwerror("GEOSDifference: %s", lwgeom_geos_errmsg);
		return NULL ; /* never get here */
	}

	LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3) ) ;

	GEOSSetSRID(g3, srid);

	result = GEOS2LWGEOM(g3, is3d);

	if (result == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g3);
	        lwerror("Error performing difference: GEOS2LWGEOM: %s",
	                lwgeom_geos_errmsg);
		return NULL; /* never get here */
	}

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);
	GEOSGeom_destroy(g3);

	/* compressType(result); */

	return result;
}

LWGEOM *
lwgeom_symdifference(const LWGEOM* geom1, const LWGEOM* geom2)
{
	GEOSGeometry *g1, *g2, *g3;
	LWGEOM *result;
	int is3d;
	int srid;

	/* A.SymDifference(Empty) == A */
	if ( lwgeom_is_empty(geom2) )
		return lwgeom_clone(geom1);

	/* Empty.DymDifference(B) == B */
	if ( lwgeom_is_empty(geom1) )
		return lwgeom_clone(geom2);

	/* ensure srids are identical */
	srid = (int)(geom1->srid);
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = LWGEOM2GEOS(geom1, 0);

	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = LWGEOM2GEOS(geom2, 0);

	if ( 0 == g2 )   /* exception thrown at construction */
	{
		lwerror("Second argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		GEOSGeom_destroy(g1);
		return NULL;
	}

	g3 = GEOSSymDifference(g1,g2);

	if (g3 == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		lwerror("GEOSSymDifference: %s", lwgeom_geos_errmsg);
		return NULL; /*never get here */
	}

	LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3));

	GEOSSetSRID(g3, srid);

	result = GEOS2LWGEOM(g3, is3d);

	if (result == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		GEOSGeom_destroy(g3);
		lwerror("GEOS symdifference() threw an error (result postgis geometry formation)!");
		return NULL ; /*never get here */
	}

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);
	GEOSGeom_destroy(g3);

	return result;
}

LWGEOM*
lwgeom_union(const LWGEOM *geom1, const LWGEOM *geom2)
{
	int is3d;
	int srid;
	GEOSGeometry *g1, *g2, *g3;
	LWGEOM *result;

	LWDEBUG(2, "in geomunion");

	/* A.Union(empty) == A */
	if ( lwgeom_is_empty(geom1) )
		return lwgeom_clone(geom2);

	/* B.Union(empty) == B */
	if ( lwgeom_is_empty(geom2) )
		return lwgeom_clone(geom1);


	/* ensure srids are identical */
	srid = (int)(geom1->srid);
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = LWGEOM2GEOS(geom1, 0);

	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = LWGEOM2GEOS(geom2, 0);

	if ( 0 == g2 )   /* exception thrown at construction */
	{
		GEOSGeom_destroy(g1);
		lwerror("Second argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	LWDEBUGF(3, "g1=%s", GEOSGeomToWKT(g1));
	LWDEBUGF(3, "g2=%s", GEOSGeomToWKT(g2));

	g3 = GEOSUnion(g1,g2);

	LWDEBUGF(3, "g3=%s", GEOSGeomToWKT(g3));

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);

	if (g3 == NULL)
	{
		lwerror("GEOSUnion: %s", lwgeom_geos_errmsg);
		return NULL; /* never get here */
	}


	GEOSSetSRID(g3, srid);

	result = GEOS2LWGEOM(g3, is3d);

	GEOSGeom_destroy(g3);

	if (result == NULL)
	{
	        lwerror("Error performing union: GEOS2LWGEOM: %s",
	                lwgeom_geos_errmsg);
		return NULL; /*never get here */
	}

	return result;
}

LWGEOM *
lwgeom_clip_by_rect(const LWGEOM *geom1, double x0, double y0, double x1, double y1)
{
#if POSTGIS_GEOS_VERSION < 35
	lwerror("The GEOS version this postgis binary "
	        "was compiled against (%d) doesn't support "
	        "'GEOSClipByRect' function (3.3.5+ required)",
	        POSTGIS_GEOS_VERSION);
	return NULL;
#else /* POSTGIS_GEOS_VERSION >= 35 */
	LWGEOM *result ;
	GEOSGeometry *g1, *g3 ;
	int is3d ;

	/* A.Intersection(Empty) == Empty */
	if ( lwgeom_is_empty(geom1) )
		return lwgeom_clone(geom1);

	is3d = FLAGS_GET_Z(geom1->flags);

	initGEOS(lwnotice, lwgeom_geos_error);

	LWDEBUG(3, "clip_by_rect() START");

	g1 = LWGEOM2GEOS(geom1, 1); /* auto-fix structure */
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL ;
	}

	LWDEBUG(3, " constructed geometrys - calling geos");
	LWDEBUGF(3, " g1 = %s", GEOSGeomToWKT(g1));
	/*LWDEBUGF(3, "g1 is valid = %i",GEOSisvalid(g1)); */

	g3 = GEOSClipByRect(g1,x0,y0,x1,y1);
	GEOSGeom_destroy(g1);

	LWDEBUG(3, " clip_by_rect finished");

	if (g3 == NULL)
	{
	  lwerror("Error performing rectangular clipping: %s",
	          lwgeom_geos_errmsg);
		return NULL; /* never get here */
	}

	LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3) ) ;

	result = GEOS2LWGEOM(g3, is3d);
	GEOSGeom_destroy(g3);

	if (result == NULL)
	{
    lwerror("Error performing intersection: GEOS2LWGEOM: %s",
            lwgeom_geos_errmsg);
		return NULL ; /* never get here */
	}

	result->srid = geom1->srid;

	return result ;
#endif /* POSTGIS_GEOS_VERSION >= 35 */
}


/* ------------ BuildArea stuff ---------------------------------------------------------------------{ */

typedef struct Face_t {
  const GEOSGeometry* geom;
  GEOSGeometry* env;
  double envarea;
  struct Face_t* parent; /* if this face is an hole of another one, or NULL */
} Face;

static Face* newFace(const GEOSGeometry* g);
static void delFace(Face* f);
static unsigned int countParens(const Face* f);
static void findFaceHoles(Face** faces, int nfaces);

static Face*
newFace(const GEOSGeometry* g)
{
  Face* f = lwalloc(sizeof(Face));
  f->geom = g;
  f->env = GEOSEnvelope(f->geom);
  GEOSArea(f->env, &f->envarea);
  f->parent = NULL;
  /* lwnotice("Built Face with area %g and %d holes", f->envarea, GEOSGetNumInteriorRings(f->geom)); */
  return f;
}

static unsigned int
countParens(const Face* f)
{
  unsigned int pcount = 0;
  while ( f->parent ) {
    ++pcount;
    f = f->parent;
  }
  return pcount;
}

/* Destroy the face and release memory associated with it */
static void
delFace(Face* f)
{
  GEOSGeom_destroy(f->env);
  lwfree(f);
}


static int
compare_by_envarea(const void* g1, const void* g2)
{
  Face* f1 = *(Face**)g1;
  Face* f2 = *(Face**)g2;
  double n1 = f1->envarea;
  double n2 = f2->envarea;

  if ( n1 < n2 ) return 1;
  if ( n1 > n2 ) return -1;
  return 0;
}

/* Find holes of each face */
static void
findFaceHoles(Face** faces, int nfaces)
{
  int i, j, h;

  /* We sort by envelope area so that we know holes are only
   * after their shells */
  qsort(faces, nfaces, sizeof(Face*), compare_by_envarea);
  for (i=0; i<nfaces; ++i) {
    Face* f = faces[i];
    int nholes = GEOSGetNumInteriorRings(f->geom);
    LWDEBUGF(2, "Scanning face %d with env area %g and %d holes", i, f->envarea, nholes);
    for (h=0; h<nholes; ++h) {
      const GEOSGeometry *hole = GEOSGetInteriorRingN(f->geom, h);
      LWDEBUGF(2, "Looking for hole %d/%d of face %d among %d other faces", h+1, nholes, i, nfaces-i-1);
      for (j=i+1; j<nfaces; ++j) {
		const GEOSGeometry *f2er;
        Face* f2 = faces[j];
        if ( f2->parent ) continue; /* hole already assigned */
        f2er = GEOSGetExteriorRing(f2->geom); 
        /* TODO: can be optimized as the ring would have the
         *       same vertices, possibly in different order.
         *       maybe comparing number of points could already be
         *       useful.
         */
        if ( GEOSEquals(f2er, hole) ) {
          LWDEBUGF(2, "Hole %d/%d of face %d is face %d", h+1, nholes, i, j);
          f2->parent = f;
          break;
        }
      }
    }
  }
}

static GEOSGeometry*
collectFacesWithEvenAncestors(Face** faces, int nfaces)
{
  GEOSGeometry **geoms = lwalloc(sizeof(GEOSGeometry*)*nfaces);
  GEOSGeometry *ret;
  unsigned int ngeoms = 0;
  int i;

  for (i=0; i<nfaces; ++i) {
    Face *f = faces[i];
    if ( countParens(f) % 2 ) continue; /* we skip odd parents geoms */
    geoms[ngeoms++] = GEOSGeom_clone(f->geom);
  }

  ret = GEOSGeom_createCollection(GEOS_MULTIPOLYGON, geoms, ngeoms);
  lwfree(geoms);
  return ret;
}

GEOSGeometry*
LWGEOM_GEOS_buildArea(const GEOSGeometry* geom_in)
{
  GEOSGeometry *tmp;
  GEOSGeometry *geos_result, *shp;
  GEOSGeometry const *vgeoms[1];
  uint32_t i, ngeoms;
  int srid = GEOSGetSRID(geom_in);
  Face ** geoms;

  vgeoms[0] = geom_in;
#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Polygonizing");
#endif
  geos_result = GEOSPolygonize(vgeoms, 1);

  LWDEBUGF(3, "GEOSpolygonize returned @ %p", geos_result);

  /* Null return from GEOSpolygonize (an exception) */
  if ( ! geos_result ) return 0;

  /*
   * We should now have a collection
   */
#if PARANOIA_LEVEL > 0
  if ( GEOSGeometryTypeId(geos_result) != COLLECTIONTYPE )
  {
    GEOSGeom_destroy(geos_result);
    lwerror("Unexpected return from GEOSpolygonize");
    return 0;
  }
#endif

  ngeoms = GEOSGetNumGeometries(geos_result);
#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Num geometries from polygonizer: %d", ngeoms);
#endif


  LWDEBUGF(3, "GEOSpolygonize: ngeoms in polygonize output: %d", ngeoms);
  LWDEBUGF(3, "GEOSpolygonize: polygonized:%s",
              lwgeom_to_ewkt(GEOS2LWGEOM(geos_result, 0)));

  /*
   * No geometries in collection, early out
   */
  if ( ngeoms == 0 )
  {
    GEOSSetSRID(geos_result, srid);
    return geos_result;
  }

  /*
   * Return first geometry if we only have one in collection,
   * to avoid the unnecessary Geometry clone below.
   */
  if ( ngeoms == 1 )
  {
    tmp = (GEOSGeometry *)GEOSGetGeometryN(geos_result, 0);
    if ( ! tmp )
    {
      GEOSGeom_destroy(geos_result);
      return 0; /* exception */
    }
    shp = GEOSGeom_clone(tmp);
    GEOSGeom_destroy(geos_result); /* only safe after the clone above */
    GEOSSetSRID(shp, srid);
    return shp;
  }

  LWDEBUGF(2, "Polygonize returned %d geoms", ngeoms);

  /*
   * Polygonizer returns a polygon for each face in the built topology.
   *
   * This means that for any face with holes we'll have other faces
   * representing each hole. We can imagine a parent-child relationship
   * between these faces.
   *
   * In order to maximize the number of visible rings in output we
   * only use those faces which have an even number of parents.
   *
   * Example:
   *
   *   +---------------+
   *   |     L0        |  L0 has no parents 
   *   |  +---------+  |
   *   |  |   L1    |  |  L1 is an hole of L0
   *   |  |  +---+  |  |
   *   |  |  |L2 |  |  |  L2 is an hole of L1 (which is an hole of L0)
   *   |  |  |   |  |  |
   *   |  |  +---+  |  |
   *   |  +---------+  |
   *   |               |
   *   +---------------+
   * 
   * See http://trac.osgeo.org/postgis/ticket/1806
   *
   */

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Preparing face structures");
#endif

  /* Prepare face structures for later analysis */
  geoms = lwalloc(sizeof(Face**)*ngeoms);
  for (i=0; i<ngeoms; ++i)
    geoms[i] = newFace(GEOSGetGeometryN(geos_result, i));

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Finding face holes");
#endif

  /* Find faces representing other faces holes */
  findFaceHoles(geoms, ngeoms);

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Colletting even ancestor faces");
#endif

  /* Build a MultiPolygon composed only by faces with an
   * even number of ancestors */
  tmp = collectFacesWithEvenAncestors(geoms, ngeoms);

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Cleaning up");
#endif

  /* Cleanup face structures */
  for (i=0; i<ngeoms; ++i) delFace(geoms[i]);
  lwfree(geoms);

  /* Faces referenced memory owned by geos_result.
   * It is safe to destroy geos_result after deleting them. */
  GEOSGeom_destroy(geos_result);

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Self-unioning");
#endif

  /* Run a single overlay operation to dissolve shared edges */
  shp = GEOSUnionCascaded(tmp);
  if ( ! shp )
  {
    GEOSGeom_destroy(tmp);
    return 0; /* exception */
  }

#ifdef LWGEOM_PROFILE_BUILDAREA
  lwnotice("Final cleanup");
#endif

  GEOSGeom_destroy(tmp);

  GEOSSetSRID(shp, srid);

  return shp;
}

LWGEOM*
lwgeom_buildarea(const LWGEOM *geom)
{
	GEOSGeometry* geos_in;
	GEOSGeometry* geos_out;
	LWGEOM* geom_out;
	int SRID = (int)(geom->srid);
	int is3d = FLAGS_GET_Z(geom->flags);

	/* Can't build an area from an empty! */
	if ( lwgeom_is_empty(geom) )
	{
		return (LWGEOM*)lwpoly_construct_empty(SRID, is3d, 0);
	}

	LWDEBUG(3, "buildarea called");

	LWDEBUGF(3, "ST_BuildArea got geom @ %p", geom);

	initGEOS(lwnotice, lwgeom_geos_error);

	geos_in = LWGEOM2GEOS(geom, 0);
	
	if ( 0 == geos_in )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}
	geos_out = LWGEOM_GEOS_buildArea(geos_in);
	GEOSGeom_destroy(geos_in);

	if ( ! geos_out ) /* exception thrown.. */
	{
		lwerror("LWGEOM_GEOS_buildArea: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	/* If no geometries are in result collection, return NULL */
	if ( GEOSGetNumGeometries(geos_out) == 0 )
	{
		GEOSGeom_destroy(geos_out);
		return NULL;
	}

	geom_out = GEOS2LWGEOM(geos_out, is3d);
	GEOSGeom_destroy(geos_out);

#if PARANOIA_LEVEL > 0
	if ( geom_out == NULL )
	{
		lwerror("serialization error");
		return NULL;
	}

#endif

	return geom_out;
}

/* ------------ end of BuildArea stuff ---------------------------------------------------------------------} */

LWGEOM*
lwgeom_geos_noop(const LWGEOM* geom_in)
{
	GEOSGeometry *geosgeom;
	LWGEOM* geom_out;

	int is3d = FLAGS_GET_Z(geom_in->flags);

	initGEOS(lwnotice, lwgeom_geos_error);
	geosgeom = LWGEOM2GEOS(geom_in, 0);
	if ( ! geosgeom ) {
		lwerror("Geometry could not be converted to GEOS: %s",
			lwgeom_geos_errmsg);
		return NULL;
	}
	geom_out = GEOS2LWGEOM(geosgeom, is3d);
	GEOSGeom_destroy(geosgeom);
	if ( ! geom_out ) {
		lwerror("GEOS Geometry could not be converted to LWGEOM: %s",
			lwgeom_geos_errmsg);
	}
	return geom_out;
	
}

LWGEOM*
lwgeom_snap(const LWGEOM* geom1, const LWGEOM* geom2, double tolerance)
{
#if POSTGIS_GEOS_VERSION < 33
	lwerror("The GEOS version this lwgeom library "
	        "was compiled against (%d) doesn't support "
	        "'Snap' function (3.3.0+ required)",
	        POSTGIS_GEOS_VERSION);
	return NULL;
#else /* POSTGIS_GEOS_VERSION >= 33 */

	int srid, is3d;
	GEOSGeometry *g1, *g2, *g3;
	LWGEOM* out;

	srid = geom1->srid;
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = (GEOSGeometry *)LWGEOM2GEOS(geom1, 0);
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = (GEOSGeometry *)LWGEOM2GEOS(geom2, 0);
	if ( 0 == g2 )   /* exception thrown at construction */
	{
		lwerror("Second argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		GEOSGeom_destroy(g1);
		return NULL;
	}

	g3 = GEOSSnap(g1, g2, tolerance);
	if (g3 == NULL)
	{
		GEOSGeom_destroy(g1);
		GEOSGeom_destroy(g2);
		lwerror("GEOSSnap: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);

	GEOSSetSRID(g3, srid);
	out = GEOS2LWGEOM(g3, is3d);
	if (out == NULL)
	{
		GEOSGeom_destroy(g3);
		lwerror("GEOSSnap() threw an error (result LWGEOM geometry formation)!");
		return NULL;
	}
	GEOSGeom_destroy(g3);

	return out;

#endif /* POSTGIS_GEOS_VERSION >= 33 */
}

LWGEOM*
lwgeom_sharedpaths(const LWGEOM* geom1, const LWGEOM* geom2)
{
#if POSTGIS_GEOS_VERSION < 33
	lwerror("The GEOS version this postgis binary "
	        "was compiled against (%d) doesn't support "
	        "'SharedPaths' function (3.3.0+ required)",
	        POSTGIS_GEOS_VERSION);
	return NULL;
#else /* POSTGIS_GEOS_VERSION >= 33 */
	GEOSGeometry *g1, *g2, *g3;
	LWGEOM *out;
	int is3d, srid;

	srid = geom1->srid;
	error_if_srid_mismatch(srid, (int)(geom2->srid));

	is3d = (FLAGS_GET_Z(geom1->flags) || FLAGS_GET_Z(geom2->flags)) ;

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = (GEOSGeometry *)LWGEOM2GEOS(geom1, 0);
	if ( 0 == g1 )   /* exception thrown at construction */
	{
		lwerror("First argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	g2 = (GEOSGeometry *)LWGEOM2GEOS(geom2, 0);
	if ( 0 == g2 )   /* exception thrown at construction */
	{
		lwerror("Second argument geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		GEOSGeom_destroy(g1);
		return NULL;
	}

	g3 = GEOSSharedPaths(g1,g2);

	GEOSGeom_destroy(g1);
	GEOSGeom_destroy(g2);

	if (g3 == NULL)
	{
		lwerror("GEOSSharedPaths: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	GEOSSetSRID(g3, srid);
	out = GEOS2LWGEOM(g3, is3d);
	GEOSGeom_destroy(g3);

	if (out == NULL)
	{
		lwerror("GEOS2LWGEOM threw an error");
		return NULL;
	}

	return out;
#endif /* POSTGIS_GEOS_VERSION >= 33 */
}

LWGEOM*
lwgeom_offsetcurve(const LWLINE *lwline, double size, int quadsegs, int joinStyle, double mitreLimit)
{
#if POSTGIS_GEOS_VERSION < 32
	lwerror("lwgeom_offsetcurve: GEOS 3.2 or higher required");
#else
	GEOSGeometry *g1, *g3;
	LWGEOM *lwgeom_result;
	LWGEOM *lwgeom_in = lwline_as_lwgeom(lwline);

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = (GEOSGeometry *)LWGEOM2GEOS(lwgeom_in, 0);
	if ( ! g1 ) 
	{
		lwerror("lwgeom_offsetcurve: Geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

#if POSTGIS_GEOS_VERSION < 33
	/* Size is always positive for GEOSSingleSidedBuffer, and a flag determines left/right */
	g3 = GEOSSingleSidedBuffer(g1, size < 0 ? -size : size,
	                           quadsegs, joinStyle, mitreLimit,
	                           size < 0 ? 0 : 1);
#else
	g3 = GEOSOffsetCurve(g1, size, quadsegs, joinStyle, mitreLimit);
#endif
	/* Don't need input geometry anymore */
	GEOSGeom_destroy(g1);

	if (g3 == NULL)
	{
		lwerror("GEOSOffsetCurve: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3));

	GEOSSetSRID(g3, lwgeom_get_srid(lwgeom_in));
	lwgeom_result = GEOS2LWGEOM(g3, lwgeom_has_z(lwgeom_in));
	GEOSGeom_destroy(g3);

	if (lwgeom_result == NULL)
	{
		lwerror("lwgeom_offsetcurve: GEOS2LWGEOM returned null");
		return NULL;
	}

	return lwgeom_result;
	
#endif /* POSTGIS_GEOS_VERSION < 32 */
}

LWTIN *lwtin_from_geos(const GEOSGeometry *geom, int want3d) {
	int type = GEOSGeomTypeId(geom);
	int hasZ;
	int SRID = GEOSGetSRID(geom);

	/* GEOS's 0 is equivalent to our unknown as for SRID values */
	if ( SRID == 0 ) SRID = SRID_UNKNOWN;

	if ( want3d ) {
		hasZ = GEOSHasZ(geom);
		if ( ! hasZ ) {
			LWDEBUG(3, "Geometry has no Z, won't provide one");
			want3d = 0;
		}
	}

	switch (type) {
		LWTRIANGLE **geoms;
		uint32_t i, ngeoms;
	case GEOS_GEOMETRYCOLLECTION:
		LWDEBUG(4, "lwgeom_from_geometry: it's a Collection or Multi");

		ngeoms = GEOSGetNumGeometries(geom);
		geoms = NULL;
		if ( ngeoms ) {
			geoms = lwalloc(ngeoms * sizeof *geoms);
			if (!geoms) {
				lwerror("lwtin_from_geos: can't allocate geoms");
				return NULL;
			}
			for (i=0; i<ngeoms; i++) {
				const GEOSGeometry *poly, *ring;
				const GEOSCoordSequence *cs;
				POINTARRAY *pa;

				poly = GEOSGetGeometryN(geom, i);
				ring = GEOSGetExteriorRing(poly);
				cs = GEOSGeom_getCoordSeq(ring);
				pa = ptarray_from_GEOSCoordSeq(cs, want3d);

				geoms[i] = lwtriangle_construct(SRID, NULL, pa);
			}
		}
		return (LWTIN *)lwcollection_construct(TINTYPE, SRID, NULL, ngeoms, (LWGEOM **)geoms);
	case GEOS_POLYGON:
	case GEOS_MULTIPOINT:
	case GEOS_MULTILINESTRING:
	case GEOS_MULTIPOLYGON:
	case GEOS_LINESTRING:
	case GEOS_LINEARRING:
	case GEOS_POINT:
		lwerror("lwtin_from_geos: invalid geometry type for tin: %d", type);
		break;

	default:
		lwerror("GEOS2LWGEOM: unknown geometry type: %d", type);
		return NULL;
	}

	/* shouldn't get here */
	return NULL;
}
/*
 * output = 1 for edges, 2 for TIN, 0 for polygons
 */
LWGEOM* lwgeom_delaunay_triangulation(const LWGEOM *lwgeom_in, double tolerance, int output) {
#if POSTGIS_GEOS_VERSION < 34
	lwerror("lwgeom_delaunay_triangulation: GEOS 3.4 or higher required");
	return NULL;
#else
	GEOSGeometry *g1, *g3;
	LWGEOM *lwgeom_result;

	if (output < 0 || output > 2) {
		lwerror("lwgeom_delaunay_triangulation: invalid output type specified %d", output);
		return NULL;
	}

	initGEOS(lwnotice, lwgeom_geos_error);

	g1 = (GEOSGeometry *)LWGEOM2GEOS(lwgeom_in, 0);
	if ( ! g1 ) 
	{
		lwerror("lwgeom_delaunay_triangulation: Geometry could not be converted to GEOS: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	/* if output != 1 we want polys */
	g3 = GEOSDelaunayTriangulation(g1, tolerance, output == 1);

	/* Don't need input geometry anymore */
	GEOSGeom_destroy(g1);

	if (g3 == NULL)
	{
		lwerror("GEOSDelaunayTriangulation: %s", lwgeom_geos_errmsg);
		return NULL;
	}

	/* LWDEBUGF(3, "result: %s", GEOSGeomToWKT(g3)); */

	GEOSSetSRID(g3, lwgeom_get_srid(lwgeom_in));

	if (output == 2) {
		lwgeom_result = (LWGEOM *)lwtin_from_geos(g3, lwgeom_has_z(lwgeom_in));
	} else {
		lwgeom_result = GEOS2LWGEOM(g3, lwgeom_has_z(lwgeom_in));
	}

	GEOSGeom_destroy(g3);

	if (lwgeom_result == NULL) {
		if (output != 2) {
			lwerror("lwgeom_delaunay_triangulation: GEOS2LWGEOM returned null");
		} else {
			lwerror("lwgeom_delaunay_triangulation: lwtin_from_geos returned null");
		}
		return NULL;
	}

	return lwgeom_result;
	
#endif /* POSTGIS_GEOS_VERSION < 34 */
}
