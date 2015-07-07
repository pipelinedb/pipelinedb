/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"


#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "access/gist.h"
#include "access/itup.h"

#include "fmgr.h"
#include "utils/elog.h"

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

#define SHOW_DIGS_DOUBLE 15
#define MAX_DIGS_DOUBLE (SHOW_DIGS_DOUBLE + 6 + 1 + 3 +1)

/*
 * distance from -126 49  to -126 49.011096139863
 * in  'SPHEROID["GRS_1980",6378137,298.257222101]'
 * is 1234.000
 */


/* PG-exposed  */
Datum ellipsoid_in(PG_FUNCTION_ARGS);
Datum ellipsoid_out(PG_FUNCTION_ARGS);
Datum LWGEOM_length2d_ellipsoid(PG_FUNCTION_ARGS);
Datum LWGEOM_length_ellipsoid_linestring(PG_FUNCTION_ARGS);
Datum LWGEOM_distance_ellipsoid(PG_FUNCTION_ARGS);
Datum LWGEOM_distance_sphere(PG_FUNCTION_ARGS);
Datum geometry_distance_spheroid(PG_FUNCTION_ARGS);

/* internal */
double distance_sphere_method(double lat1, double long1,double lat2,double long2, SPHEROID *sphere);
double distance_ellipse_calculation(double lat1, double long1, double lat2, double long2, SPHEROID *sphere);
double	distance_ellipse(double lat1, double long1, double lat2, double long2, SPHEROID *sphere);
double deltaLongitude(double azimuth, double sigma, double tsm,SPHEROID *sphere);
double mu2(double azimuth,SPHEROID *sphere);
double bigA(double u2);
double bigB(double u2);


/*
 * Use the WKT definition of an ellipsoid
 * ie. SPHEROID["name",A,rf] or SPHEROID("name",A,rf)
 *	  SPHEROID["GRS_1980",6378137,298.257222101]
 * wkt says you can use "(" or "["
 */
PG_FUNCTION_INFO_V1(ellipsoid_in);
Datum ellipsoid_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	SPHEROID *sphere = (SPHEROID *) palloc(sizeof(SPHEROID));
	int nitems;
	double rf;

	memset(sphere,0, sizeof(SPHEROID));

	if (strstr(str,"SPHEROID") !=  str )
	{
		elog(ERROR,"SPHEROID parser - doesnt start with SPHEROID");
		pfree(sphere);
		PG_RETURN_NULL();
	}

	nitems = sscanf(str,"SPHEROID[\"%19[^\"]\",%lf,%lf]",
	                sphere->name, &sphere->a, &rf);

	if ( nitems==0)
		nitems = sscanf(str,"SPHEROID(\"%19[^\"]\",%lf,%lf)",
		                sphere->name, &sphere->a, &rf);

	if (nitems != 3)
	{
		elog(ERROR,"SPHEROID parser - couldnt parse the spheroid");
		pfree(sphere);
		PG_RETURN_NULL();
	}

	sphere->f = 1.0/rf;
	sphere->b = sphere->a - (1.0/rf)*sphere->a;
	sphere->e_sq = ((sphere->a*sphere->a) - (sphere->b*sphere->b)) /
	               (sphere->a*sphere->a);
	sphere->e = sqrt(sphere->e_sq);

	PG_RETURN_POINTER(sphere);

}

PG_FUNCTION_INFO_V1(ellipsoid_out);
Datum ellipsoid_out(PG_FUNCTION_ARGS)
{
	SPHEROID *sphere = (SPHEROID *) PG_GETARG_POINTER(0);
	char *result;

	result = palloc(MAX_DIGS_DOUBLE + MAX_DIGS_DOUBLE + 20 + 9 + 2);

	sprintf(result,"SPHEROID(\"%s\",%.15g,%.15g)",
	        sphere->name, sphere->a, 1.0/sphere->f);

	PG_RETURN_CSTRING(result);
}

/*
 * support function for distance calc
 * code is taken from David Skea
 * Geographic Data BC, Province of British Columbia, Canada.
 * Thanks to GDBC and David Skea for allowing this to be
 * put in PostGIS.
 */
double
deltaLongitude(double azimuth, double sigma, double tsm,SPHEROID *sphere)
{
	/* compute the expansion C */
	double das,C;
	double ctsm,DL;

	das = cos(azimuth)*cos(azimuth);
	C = sphere->f/16.0 * das * (4.0 + sphere->f * (4.0 - 3.0 * das));

	/* compute the difference in longitude */
	ctsm = cos(tsm);
	DL = ctsm + C * cos(sigma) * (-1.0 + 2.0 * ctsm*ctsm);
	DL = sigma + C * sin(sigma) * DL;
	return (1.0 - C) * sphere->f * sin(azimuth) * DL;
}


/*
 * support function for distance calc
 * code is taken from David Skea
 * Geographic Data BC, Province of British Columbia, Canada.
 *  Thanks to GDBC and David Skea for allowing this to be
 *  put in PostGIS.
 */
double
mu2(double azimuth,SPHEROID *sphere)
{
	double    e2;

	e2 = sqrt(sphere->a*sphere->a-sphere->b*sphere->b)/sphere->b;
	return cos(azimuth)*cos(azimuth) * e2*e2;
}


/*
 * Support function for distance calc
 * code is taken from David Skea
 * Geographic Data BC, Province of British Columbia, Canada.
 *  Thanks to GDBC and David Skea for allowing this to be
 *  put in PostGIS.
 */
double
bigA(double u2)
{
	return 1.0 + u2/256.0 * (64.0 + u2 * (-12.0 + 5.0 * u2));
}


/*
 * Support function for distance calc
 * code is taken from David Skea
 * Geographic Data BC, Province of British Columbia, Canada.
 *  Thanks to GDBC and David Skea for allowing this to be
 *  put in PostGIS.
 */
double
bigB(double u2)
{
	return u2/512.0 * (128.0 + u2 * (-64.0 + 37.0 * u2));
}



double
distance_ellipse(double lat1, double long1,
                 double lat2, double long2, SPHEROID *sphere)
{
	double result = 0;
#if POSTGIS_DEBUG_LEVEL >= 4
	double result2 = 0;
#endif

	if ( (lat1==lat2) && (long1 == long2) )
	{
		return 0.0; /* same point, therefore zero distance */
	}

	result = distance_ellipse_calculation(lat1,long1,lat2,long2,sphere);

#if POSTGIS_DEBUG_LEVEL >= 4
	result2 =  distance_sphere_method(lat1, long1,lat2,long2, sphere);

	POSTGIS_DEBUGF(4, "delta = %lf, skae says: %.15lf,2 circle says: %.15lf",
	         (result2-result),result,result2);
	POSTGIS_DEBUGF(4, "2 circle says: %.15lf",result2);
#endif

	if (result != result)  /* NaN check
			                        * (x==x for all x except NaN by IEEE definition)
			                        */
	{
		result =  distance_sphere_method(lat1, long1,
		                                 lat2,long2, sphere);
	}

	return result;
}

/*
 * Given 2 lat/longs and ellipse, find the distance
 * note original r = 1st, s=2nd location
 */
double
distance_ellipse_calculation(double lat1, double long1,
                             double lat2, double long2, SPHEROID *sphere)
{
	/*
	 * Code is taken from David Skea
	 * Geographic Data BC, Province of British Columbia, Canada.
	 *  Thanks to GDBC and David Skea for allowing this to be
	 *  put in PostGIS.
	 */

	double L1,L2,sinU1,sinU2,cosU1,cosU2;
	double dl,dl1,dl2,dl3,cosdl1,sindl1;
	double cosSigma,sigma,azimuthEQ,tsm;
	double u2,A,B;
	double dsigma;

	double TEMP;

	int iterations;


	L1 = atan((1.0 - sphere->f ) * tan( lat1) );
	L2 = atan((1.0 - sphere->f ) * tan( lat2) );
	sinU1 = sin(L1);
	sinU2 = sin(L2);
	cosU1 = cos(L1);
	cosU2 = cos(L2);

	dl = long2- long1;
	dl1 = dl;
	cosdl1 = cos(dl);
	sindl1 = sin(dl);
	iterations = 0;
	do
	{
		cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosdl1;
		sigma = acos(cosSigma);
		azimuthEQ = asin((cosU1 * cosU2 * sindl1)/sin(sigma));

		/*
		 * Patch from Patrica Tozer to handle minor
		 * mathematical stability problem
		 */
		TEMP = cosSigma - (2.0 * sinU1 * sinU2)/
		       (cos(azimuthEQ)*cos(azimuthEQ));
		if (TEMP > 1)
		{
			TEMP = 1;
		}
		else if (TEMP < -1)
		{
			TEMP = -1;
		}
		tsm = acos(TEMP);


		/* (old code?)
		tsm = acos(cosSigma - (2.0 * sinU1 * sinU2)/(cos(azimuthEQ)*cos(azimuthEQ)));
		*/

		dl2 = deltaLongitude(azimuthEQ, sigma, tsm,sphere);
		dl3 = dl1 - (dl + dl2);
		dl1 = dl + dl2;
		cosdl1 = cos(dl1);
		sindl1 = sin(dl1);
		iterations++;
	}
	while ( (iterations<999) && (fabs(dl3) > 1.0e-032));

	/* compute expansions A and B */
	u2 = mu2(azimuthEQ,sphere);
	A = bigA(u2);
	B = bigB(u2);

	/* compute length of geodesic */
	dsigma = B * sin(sigma) * (cos(tsm) +
	                           (B*cosSigma*(-1.0 + 2.0 * (cos(tsm)*cos(tsm))))/4.0);
	return sphere->b * (A * (sigma - dsigma));
}


/*
 * Find the "length of a geometry"
 * length2d_spheroid(point, sphere) = 0
 * length2d_spheroid(line, sphere) = length of line
 * length2d_spheroid(polygon, sphere) = 0
 *	-- could make sense to return sum(ring perimeter)
 * uses ellipsoidal math to find the distance
 * x's are longitude, and y's are latitude - both in decimal degrees
 */
PG_FUNCTION_INFO_V1(LWGEOM_length2d_ellipsoid);
Datum LWGEOM_length2d_ellipsoid(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	SPHEROID *sphere = (SPHEROID *) PG_GETARG_POINTER(1);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	double dist = lwgeom_length_spheroid(lwgeom, sphere);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_FLOAT8(dist);
}


/*
 * Find the "length of a geometry"
 *
 * length2d_spheroid(point, sphere) = 0
 * length2d_spheroid(line, sphere) = length of line
 * length2d_spheroid(polygon, sphere) = 0
 *	-- could make sense to return sum(ring perimeter)
 * uses ellipsoidal math to find the distance
 * x's are longitude, and y's are latitude - both in decimal degrees
 */
PG_FUNCTION_INFO_V1(LWGEOM_length_ellipsoid_linestring);
Datum LWGEOM_length_ellipsoid_linestring(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);
	SPHEROID *sphere = (SPHEROID *) PG_GETARG_POINTER(1);
	double length = 0.0;

	/* EMPTY things have no length */
	if ( lwgeom_is_empty(lwgeom) )
	{
		lwgeom_free(lwgeom);
		PG_RETURN_FLOAT8(0.0);
	}

	length = lwgeom_length_spheroid(lwgeom, sphere);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 0);

	/* Something went wrong... */
	if ( length < 0.0 )
	{
		elog(ERROR, "lwgeom_length_spheroid returned length < 0.0");
		PG_RETURN_NULL();
	}

	/* Clean up */
	PG_RETURN_FLOAT8(length);
}

/*
 *  For some lat/long points, the above method doesnt calculate the distance very well.
 *  Typically this is for two lat/long points that are very very close together (<10cm).
 *  This gets worse closer to the equator.
 *
 *   This method works very well for very close together points, not so well if they're
 *   far away (>1km).
 *
 *  METHOD:
 *    We create two circles (with Radius R and Radius S) and use these to calculate the distance.
 *
 *    The first (R) is basically a (north-south) line of longitude.
 *    Its radius is approximated by looking at the ellipse. Near the equator R = 'a' (earth's major axis)
 *    near the pole R = 'b' (earth's minor axis).
 *
 *    The second (S) is basically a (east-west) line of lattitude.
 *    Its radius runs from 'a' (major axis) at the equator, and near 0 at the poles.
 *
 *
 *                North pole
 *                *
 *               *
 *              *\--S--
 *             *  R   +
 *            *    \  +
 *           *     A\ +
 *          * ------ \         Equator/centre of earth
 *           *
 *            *
 *             *
 *              *
 *               *
 *                *
 *                South pole
 *  (side view of earth)
 *
 *   Angle A is lat1
 *   R is the distance from the centre of the earth to the lat1/long1 point on the surface
 *   of the Earth.
 *   S is the circle-of-lattitude.  Its calculated from the right triangle defined by
 *      the angle (90-A), and the hypothenus R.
 *
 *
 *
 *   Once R and S have been calculated, the actual distance between the two points can be
 *   calculated.
 *
 *   We dissolve the vector from lat1,long1 to lat2,long2 into its X and Y components (called DeltaX,DeltaY).
 *   The actual distance that these angle-based measurements represent is taken from the two
 *   circles we just calculated; R (for deltaY) and S (for deltaX).
 *
 *    (if deltaX is 1 degrees, then that distance represents 1/360 of a circle of radius S.)
 *
 *
 *  Parts taken from PROJ4 - geodetic_to_geocentric() (for calculating Rn)
 *
 *  remember that lat1/long1/lat2/long2 are comming in a *RADIANS* not degrees.
 *
 * By Patricia Tozer and Dave Blasby
 *
 *  This is also called the "curvature method".
 */

double distance_sphere_method(double lat1, double long1,double lat2,double long2, SPHEROID *sphere)
{
	double R,S,X,Y,deltaX,deltaY;

	double 	distance 	= 0.0;
	double 	sin_lat 	= sin(lat1);
	double 	sin2_lat 	= sin_lat * sin_lat;

	double  Geocent_a 	= sphere->a;
	double  Geocent_e2 	= sphere->e_sq;

	R  	= Geocent_a / (sqrt(1.0e0 - Geocent_e2 * sin2_lat));
	/* 90 - lat1, but in radians */
	S 	= R * sin(M_PI_2 - lat1) ;

	deltaX = long2 - long1;  /* in rads */
	deltaY = lat2 - lat1;    /* in rads */

	/* think: a % of 2*pi*S */
	X = deltaX/(2.0*M_PI) * 2 * M_PI * S;
	Y = deltaY/(2.0*M_PI) * 2 * M_PI * R;

	distance = sqrt((X * X + Y * Y));

	return distance;
}

/*
 * distance (geometry,geometry, sphere)
 */
PG_FUNCTION_INFO_V1(geometry_distance_spheroid);
Datum geometry_distance_spheroid(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
	SPHEROID *sphere = (SPHEROID *)PG_GETARG_POINTER(2);
	int type1 = gserialized_get_type(geom1);
	int type2 = gserialized_get_type(geom2);
	bool use_spheroid = PG_GETARG_BOOL(3);
	LWGEOM *lwgeom1, *lwgeom2;
	double distance;

	/* Calculate some other parameters on the spheroid */
	spheroid_init(sphere, sphere->a, sphere->b);

	/* Catch sphere special case and re-jig spheroid appropriately */
	if ( ! use_spheroid )
	{
		sphere->a = sphere->b = sphere->radius;
	}

	if ( ! ( type1 == POINTTYPE || type1 == LINETYPE || type1 == POLYGONTYPE ||
	         type1 == MULTIPOINTTYPE || type1 == MULTILINETYPE || type1 == MULTIPOLYGONTYPE ))
	{
		elog(ERROR, "geometry_distance_spheroid: Only point/line/polygon supported.\n");
		PG_RETURN_NULL();
	}

	if ( ! ( type2 == POINTTYPE || type2 == LINETYPE || type2 == POLYGONTYPE ||
	         type2 == MULTIPOINTTYPE || type2 == MULTILINETYPE || type2 == MULTIPOLYGONTYPE ))
	{
		elog(ERROR, "geometry_distance_spheroid: Only point/line/polygon supported.\n");
		PG_RETURN_NULL();
	}


	if (gserialized_get_srid(geom1) != gserialized_get_srid(geom2))
	{
		elog(ERROR, "geometry_distance_spheroid: Operation on two GEOMETRIES with different SRIDs\n");
		PG_RETURN_NULL();
	}

	/* Get #LWGEOM structures */
	lwgeom1 = lwgeom_from_gserialized(geom1);
	lwgeom2 = lwgeom_from_gserialized(geom2);
	
	/* We are going to be calculating geodetic distances */
	lwgeom_set_geodetic(lwgeom1, LW_TRUE);
	lwgeom_set_geodetic(lwgeom2, LW_TRUE);

	distance = lwgeom_distance_spheroid(lwgeom1, lwgeom2, sphere, 0.0);

	PG_RETURN_FLOAT8(distance);

}

PG_FUNCTION_INFO_V1(LWGEOM_distance_ellipsoid);
Datum LWGEOM_distance_ellipsoid(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall4(geometry_distance_spheroid,
	                                    PG_GETARG_DATUM(0), PG_GETARG_DATUM(1), PG_GETARG_DATUM(2), BoolGetDatum(TRUE)));
}

PG_FUNCTION_INFO_V1(LWGEOM_distance_sphere);
Datum LWGEOM_distance_sphere(PG_FUNCTION_ARGS)
{
	SPHEROID s;

	/* Init to WGS84 */
	spheroid_init(&s, 6378137.0, 6356752.314245179498);
	s.a = s.b = s.radius;

	PG_RETURN_DATUM(DirectFunctionCall4(geometry_distance_spheroid,
	                                    PG_GETARG_DATUM(0), PG_GETARG_DATUM(1), PointerGetDatum(&s), BoolGetDatum(FALSE)));
}

