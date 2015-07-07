/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright (C) 2009 David Skea <David.Skea@gov.bc.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "liblwgeom_internal.h"
#include "lwgeodetic.h"
#include "lwgeom_log.h"

/* GeographicLib */
#include "geodesic.h"

/**
* Initialize spheroid object based on major and minor axis
*/
void spheroid_init(SPHEROID *s, double a, double b)
{
	s->a = a;
	s->b = b;
	s->f = (a - b) / a;
	s->e_sq = (a*a - b*b)/(a*a);
	s->radius = (2.0 * a + b ) / 3.0;
}

#ifdef USE_PRE22GEODESIC
static double spheroid_mu2(double alpha, const SPHEROID *s)
{
	double b2 = POW2(s->b);
	return POW2(cos(alpha)) * (POW2(s->a) - b2) / b2;
}

static double spheroid_big_a(double u2)
{
	return 1.0 + (u2 / 16384.0) * (4096.0 + u2 * (-768.0 + u2 * (320.0 - 175.0 * u2)));
}

static double spheroid_big_b(double u2)
{
	return (u2 / 1024.0) * (256.0 + u2 * (-128.0 + u2 * (74.0 - 47.0 * u2)));
}
#endif /* def USE_PRE22GEODESIC */


#ifndef USE_PRE22GEODESIC

/**
* Computes the shortest distance along the surface of the spheroid
* between two points, using the inverse geodesic problem from
* GeographicLib (Karney 2013).
*
* @param a - location of first point
* @param b - location of second point
* @param s - spheroid to calculate on
* @return spheroidal distance between a and b in spheroid units
*/
double spheroid_distance(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, const SPHEROID *spheroid)
{
	struct geod_geodesic gd;
	geod_init(&gd, spheroid->a, spheroid->f);
	double lat1 = a->lat * 180.0 / M_PI;
	double lon1 = a->lon * 180.0 / M_PI;
	double lat2 = b->lat * 180.0 / M_PI;
	double lon2 = b->lon * 180.0 / M_PI;
	double s12; /* return distance */
	geod_inverse(&gd, lat1, lon1, lat2, lon2, &s12, 0, 0);
	return s12;
}

/**
* Computes the forward azimuth of the geodesic joining two points on
* the spheroid, using the inverse geodesic problem (Karney 2013).
*
* @param r - location of first point
* @param s - location of second point
* @return azimuth of line joining r to s (but not reverse)
*/
double spheroid_direction(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, const SPHEROID *spheroid)
{
	struct geod_geodesic gd;
	geod_init(&gd, spheroid->a, spheroid->f);
	double lat1 = a->lat * 180.0 / M_PI;
	double lon1 = a->lon * 180.0 / M_PI;
	double lat2 = b->lat * 180.0 / M_PI;
	double lon2 = b->lon * 180.0 / M_PI;
	double azi1; /* return azimuth */
	geod_inverse(&gd, lat1, lon1, lat2, lon2, 0, &azi1, 0);
	return azi1 * M_PI / 180.0;
}

/**
* Given a location, an azimuth and a distance, computes the location of
* the projected point. Using the direct geodesic problem from
* GeographicLib (Karney 2013).
*
* @param r - location of first point
* @param distance - distance in meters
* @param azimuth - azimuth in radians
* @return g - location of projected point
*/
int spheroid_project(const GEOGRAPHIC_POINT *r, const SPHEROID *spheroid, double distance, double azimuth, GEOGRAPHIC_POINT *g)
{
	struct geod_geodesic gd;
	geod_init(&gd, spheroid->a, spheroid->f);
	double lat1 = r->lat * 180.0 / M_PI;
	double lon1 = r->lon * 180.0 / M_PI;
	double lat2, lon2; /* return projected position */
	geod_direct(&gd, lat1, lon1, azimuth * 180.0 / M_PI, distance, &lat2, &lon2, 0);
	g->lat = lat2 * M_PI / 180.0;
	g->lon = lon2 * M_PI / 180.0;
	return LW_SUCCESS;
}


static double ptarray_area_spheroid(const POINTARRAY *pa, const SPHEROID *spheroid)
{
	/* Return zero on non-sensical inputs */
	if ( ! pa || pa->npoints < 4 )
		return 0.0;

	struct geod_geodesic gd;
	geod_init(&gd, spheroid->a, spheroid->f);
	struct geod_polygon poly;
	geod_polygon_init(&poly, 0);
	int i;
	double area; /* returned polygon area */
	POINT2D p; /* long/lat units are degrees */

	/* Pass points from point array; don't close the linearring */
	for ( i = 0; i < pa->npoints - 1; i++ )
	{
		getPoint2d_p(pa, i, &p);
		geod_polygon_addpoint(&gd, &poly, p.y, p.x);
		LWDEBUGF(4, "geod_polygon_addpoint %d: %.12g %.12g", i, p.y, p.x);
	}
	i = geod_polygon_compute(&gd, &poly, 0, 1, &area, 0);
	if ( i != pa->npoints - 1 )
	{
		lwerror("ptarray_area_spheroid: different number of points %d vs %d",
				i, pa->npoints - 1);
	}
	LWDEBUGF(4, "geod_polygon_compute area: %.12g", area);
	return fabs(area);
}

/* Above use GeographicLib */
#else /* ndef USE_PRE22GEODESIC */
/* Below use pre-version 2.2 geodesic functions */

/**
* Computes the shortest distance along the surface of the spheroid
* between two points. Based on Vincenty's formula for the geodetic
* inverse problem as described in "Geocentric Datum of Australia
* Technical Manual", Chapter 4. Tested against:
* http://mascot.gdbc.gov.bc.ca/mascot/util1a.html
* and
* http://www.ga.gov.au/nmd/geodesy/datums/vincenty_inverse.jsp
*
* @param a - location of first point.
* @param b - location of second point.
* @param s - spheroid to calculate on
* @return spheroidal distance between a and b in spheroid units.
*/
double spheroid_distance(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, const SPHEROID *spheroid)
{
	double lambda = (b->lon - a->lon);
	double f = spheroid->f;
	double omf = 1 - spheroid->f;
	double u1, u2;
	double cos_u1, cos_u2;
	double sin_u1, sin_u2;
	double big_a, big_b, delta_sigma;
	double alpha, sin_alpha, cos_alphasq, c;
	double sigma, sin_sigma, cos_sigma, cos2_sigma_m, sqrsin_sigma, last_lambda, omega;
	double cos_lambda, sin_lambda;
	double distance;
	int i = 0;

	/* Same point => zero distance */
	if ( geographic_point_equals(a, b) )
	{
		return 0.0;
	}

	u1 = atan(omf * tan(a->lat));
	cos_u1 = cos(u1);
	sin_u1 = sin(u1);
	u2 = atan(omf * tan(b->lat));
	cos_u2 = cos(u2);
	sin_u2 = sin(u2);

	omega = lambda;
	do
	{
		cos_lambda = cos(lambda);
		sin_lambda = sin(lambda);
		sqrsin_sigma = POW2(cos_u2 * sin_lambda) +
		               POW2((cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda));
		sin_sigma = sqrt(sqrsin_sigma);
		cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
		sigma = atan2(sin_sigma, cos_sigma);
		sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin(sigma);

		/* Numerical stability issue, ensure asin is not NaN */
		if ( sin_alpha > 1.0 )
			alpha = M_PI_2;
		else if ( sin_alpha < -1.0 )
			alpha = -1.0 * M_PI_2;
		else
			alpha = asin(sin_alpha);

		cos_alphasq = POW2(cos(alpha));
		cos2_sigma_m = cos(sigma) - (2.0 * sin_u1 * sin_u2 / cos_alphasq);

		/* Numerical stability issue, cos2 is in range */
		if ( cos2_sigma_m > 1.0 )
			cos2_sigma_m = 1.0;
		if ( cos2_sigma_m < -1.0 )
			cos2_sigma_m = -1.0;

		c = (f / 16.0) * cos_alphasq * (4.0 + f * (4.0 - 3.0 * cos_alphasq));
		last_lambda = lambda;
		lambda = omega + (1.0 - c) * f * sin(alpha) * (sigma + c * sin(sigma) *
		         (cos2_sigma_m + c * cos(sigma) * (-1.0 + 2.0 * POW2(cos2_sigma_m))));
		i++;
	}
	while ( (i < 999) && (lambda != 0.0) && (fabs((last_lambda - lambda)/lambda) > 1.0e-9) );

	u2 = spheroid_mu2(alpha, spheroid);
	big_a = spheroid_big_a(u2);
	big_b = spheroid_big_b(u2);
	delta_sigma = big_b * sin_sigma * (cos2_sigma_m + (big_b / 4.0) * (cos_sigma * (-1.0 + 2.0 * POW2(cos2_sigma_m)) -
	                                   (big_b / 6.0) * cos2_sigma_m * (-3.0 + 4.0 * sqrsin_sigma) * (-3.0 + 4.0 * POW2(cos2_sigma_m))));

	distance = spheroid->b * big_a * (sigma - delta_sigma);

	/* Algorithm failure, distance == NaN, fallback to sphere */
	if ( distance != distance )
	{
		lwerror("spheroid_distance returned NaN: (%.20g %.20g) (%.20g %.20g) a = %.20g b = %.20g",a->lat, a->lon, b->lat, b->lon, spheroid->a, spheroid->b);
		return spheroid->radius * sphere_distance(a, b);
	}

	return distance;
}

/**
* Computes the direction of the geodesic joining two points on
* the spheroid. Based on Vincenty's formula for the geodetic
* inverse problem as described in "Geocentric Datum of Australia
* Technical Manual", Chapter 4. Tested against:
* http://mascot.gdbc.gov.bc.ca/mascot/util1a.html
* and
* http://www.ga.gov.au/nmd/geodesy/datums/vincenty_inverse.jsp
*
* @param r - location of first point
* @param s - location of second point
* @return azimuth of line joining r and s
*/
double spheroid_direction(const GEOGRAPHIC_POINT *r, const GEOGRAPHIC_POINT *s, const SPHEROID *spheroid)
{
	int i = 0;
	double lambda = s->lon - r->lon;
	double omf = 1 - spheroid->f;
	double u1 = atan(omf * tan(r->lat));
	double cos_u1 = cos(u1);
	double sin_u1 = sin(u1);
	double u2 = atan(omf * tan(s->lat));
	double cos_u2 = cos(u2);
	double sin_u2 = sin(u2);

	double omega = lambda;
	double alpha, sigma, sin_sigma, cos_sigma, cos2_sigma_m, sqr_sin_sigma, last_lambda;
	double sin_alpha, cos_alphasq, C, alphaFD;
	do
	{
		sqr_sin_sigma = POW2(cos_u2 * sin(lambda)) +
		                POW2((cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos(lambda)));
		sin_sigma = sqrt(sqr_sin_sigma);
		cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos(lambda);
		sigma = atan2(sin_sigma, cos_sigma);
		sin_alpha = cos_u1 * cos_u2 * sin(lambda) / sin(sigma);

		/* Numerical stability issue, ensure asin is not NaN */
		if ( sin_alpha > 1.0 )
			alpha = M_PI_2;
		else if ( sin_alpha < -1.0 )
			alpha = -1.0 * M_PI_2;
		else
			alpha = asin(sin_alpha);

		cos_alphasq = POW2(cos(alpha));
		cos2_sigma_m = cos(sigma) - (2.0 * sin_u1 * sin_u2 / cos_alphasq);

		/* Numerical stability issue, cos2 is in range */
		if ( cos2_sigma_m > 1.0 )
			cos2_sigma_m = 1.0;
		if ( cos2_sigma_m < -1.0 )
			cos2_sigma_m = -1.0;

		C = (spheroid->f / 16.0) * cos_alphasq * (4.0 + spheroid->f * (4.0 - 3.0 * cos_alphasq));
		last_lambda = lambda;
		lambda = omega + (1.0 - C) * spheroid->f * sin(alpha) * (sigma + C * sin(sigma) *
		         (cos2_sigma_m + C * cos(sigma) * (-1.0 + 2.0 * POW2(cos2_sigma_m))));
		i++;
	}
	while ( (i < 999) && (lambda != 0) && (fabs((last_lambda - lambda) / lambda) > 1.0e-9) );

	alphaFD = atan2((cos_u2 * sin(lambda)),
	                (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos(lambda)));
	if (alphaFD < 0.0)
	{
		alphaFD = alphaFD + 2.0 * M_PI;
	}
	if (alphaFD > 2.0 * M_PI)
	{
		alphaFD = alphaFD - 2.0 * M_PI;
	}
	return alphaFD;
}


/**
* Given a location, an azimuth and a distance, computes the
* location of the projected point. Based on Vincenty's formula
* for the geodetic direct problem as described in "Geocentric
* Datum of Australia Technical Manual", Chapter 4. Tested against:
* http://mascot.gdbc.gov.bc.ca/mascot/util1b.html
* and
* http://www.ga.gov.au/nmd/geodesy/datums/vincenty_direct.jsp
*
* @param r - location of first point.
* @param distance - distance in meters.
* @param azimuth - azimuth in radians.
* @return s - location of projected point.
*/
int spheroid_project(const GEOGRAPHIC_POINT *r, const SPHEROID *spheroid, double distance, double azimuth, GEOGRAPHIC_POINT *g)
{
	double omf = 1 - spheroid->f;
	double tan_u1 = omf * tan(r->lat);
	double u1 = atan(tan_u1);
	double sigma, last_sigma, delta_sigma, two_sigma_m;
	double sigma1, sin_alpha, alpha, cos_alphasq;
	double u2, A, B;
	double lat2, lambda, lambda2, C, omega;
	int i = 0;

	if (azimuth < 0.0)
	{
		azimuth = azimuth + M_PI * 2.0;
	}
	if (azimuth > (M_PI * 2.0))
	{
		azimuth = azimuth - M_PI * 2.0;
	}

	sigma1 = atan2(tan_u1, cos(azimuth));
	sin_alpha = cos(u1) * sin(azimuth);
	alpha = asin(sin_alpha);
	cos_alphasq = 1.0 - POW2(sin_alpha);

	u2 = spheroid_mu2(alpha, spheroid);
	A = spheroid_big_a(u2);
	B = spheroid_big_b(u2);

	sigma = (distance / (spheroid->b * A));
	do
	{
		two_sigma_m = 2.0 * sigma1 + sigma;
		delta_sigma = B * sin(sigma) * (cos(two_sigma_m) + (B / 4.0) * (cos(sigma) * (-1.0 + 2.0 * POW2(cos(two_sigma_m)) - (B / 6.0) * cos(two_sigma_m) * (-3.0 + 4.0 * POW2(sin(sigma))) * (-3.0 + 4.0 * POW2(cos(two_sigma_m))))));
		last_sigma = sigma;
		sigma = (distance / (spheroid->b * A)) + delta_sigma;
		i++;
	}
	while (i < 999 && fabs((last_sigma - sigma) / sigma) > 1.0e-9);

	lat2 = atan2((sin(u1) * cos(sigma) + cos(u1) * sin(sigma) *
	              cos(azimuth)), (omf * sqrt(POW2(sin_alpha) +
	                                         POW2(sin(u1) * sin(sigma) - cos(u1) * cos(sigma) *
	                                              cos(azimuth)))));
	lambda = atan2((sin(sigma) * sin(azimuth)), (cos(u1) * cos(sigma) -
	               sin(u1) * sin(sigma) * cos(azimuth)));
	C = (spheroid->f / 16.0) * cos_alphasq * (4.0 + spheroid->f * (4.0 - 3.0 * cos_alphasq));
	omega = lambda - (1.0 - C) * spheroid->f * sin_alpha * (sigma + C * sin(sigma) *
	        (cos(two_sigma_m) + C * cos(sigma) * (-1.0 + 2.0 * POW2(cos(two_sigma_m)))));
	lambda2 = r->lon + omega;
	g->lat = lat2;
	g->lon = lambda2;
	return LW_SUCCESS;
}


static inline double spheroid_prime_vertical_radius_of_curvature(double latitude, const SPHEROID *spheroid)
{
	return spheroid->a / (sqrt(1.0 - spheroid->e_sq * POW2(sin(latitude))));
}

static inline double spheroid_parallel_arc_length(double latitude, double deltaLongitude, const SPHEROID *spheroid)
{
	return spheroid_prime_vertical_radius_of_curvature(latitude, spheroid)
	       * cos(latitude)
	       * deltaLongitude;
}


/**
* Computes the area on the spheroid of a box bounded by meridians and
* parallels. The box is defined by two points, the South West corner
* and the North East corner. Formula based on Bagratuni 1967.
*
* @param southWestCorner - lower left corner of bounding box.
* @param northEastCorner - upper right corner of bounding box.
* @return area in square meters.
*/
static double spheroid_boundingbox_area(const GEOGRAPHIC_POINT *southWestCorner, const GEOGRAPHIC_POINT *northEastCorner, const SPHEROID *spheroid)
{
	double z0 = (northEastCorner->lon - southWestCorner->lon) * POW2(spheroid->b) / 2.0;
	double e = sqrt(spheroid->e_sq);
	double sinPhi1 = sin(southWestCorner->lat);
	double sinPhi2 = sin(northEastCorner->lat);
	double t1p1 = sinPhi1 / (1.0 - spheroid->e_sq * sinPhi1 * sinPhi1);
	double t1p2 = sinPhi2 / (1.0 - spheroid->e_sq * sinPhi2 * sinPhi2);
	double oneOver2e = 1.0 / (2.0 * e);
	double t2p1 = oneOver2e * log((1.0 + e * sinPhi1) / (1.0 - e * sinPhi1));
	double t2p2 = oneOver2e * log((1.0 + e * sinPhi2) / (1.0 - e * sinPhi2));
	return z0 * (t1p2 + t2p2) - z0 * (t1p1 + t2p1);
}

/**
* This function doesn't work for edges crossing the dateline or in the southern
* hemisphere. Points are pre-conditioned in ptarray_area_spheroid.
*/
static double spheroid_striparea(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, double latitude_min, const SPHEROID *spheroid)
{
	GEOGRAPHIC_POINT A, B, mL, nR;
	double deltaLng, baseArea, topArea;
	double bE, tE, ratio, sign;

	A = *a;
	B = *b;

	mL.lat = latitude_min;
	mL.lon = FP_MIN(A.lon, B.lon);
	nR.lat = FP_MIN(A.lat, B.lat);
	nR.lon = FP_MAX(A.lon, B.lon);
	LWDEBUGF(4, "mL (%.12g %.12g)", mL.lat, mL.lon);
	LWDEBUGF(4, "nR (%.12g %.12g)", nR.lat, nR.lon);
	baseArea = spheroid_boundingbox_area(&mL, &nR, spheroid);
	LWDEBUGF(4, "baseArea %.12g", baseArea);

	mL.lat = FP_MIN(A.lat, B.lat);
	mL.lon = FP_MIN(A.lon, B.lon);
	nR.lat = FP_MAX(A.lat, B.lat);
	nR.lon = FP_MAX(A.lon, B.lon);
	LWDEBUGF(4, "mL (%.12g %.12g)", mL.lat, mL.lon);
	LWDEBUGF(4, "nR (%.12g %.12g)", nR.lat, nR.lon);
	topArea = spheroid_boundingbox_area(&mL, &nR, spheroid);
	LWDEBUGF(4, "topArea %.12g", topArea);

	deltaLng = B.lon - A.lon;
	LWDEBUGF(4, "deltaLng %.12g", deltaLng);
	bE = spheroid_parallel_arc_length(A.lat, deltaLng, spheroid);
	tE = spheroid_parallel_arc_length(B.lat, deltaLng, spheroid);
	LWDEBUGF(4, "bE %.12g", bE);
	LWDEBUGF(4, "tE %.12g", tE);

	ratio = (bE + tE)/tE;
	sign = signum(B.lon - A.lon);
	return (baseArea + topArea / ratio) * sign;
}

static double ptarray_area_spheroid(const POINTARRAY *pa, const SPHEROID *spheroid)
{
	GEOGRAPHIC_POINT a, b;
	POINT2D p;
	int i;
	double area = 0.0;
	GBOX gbox2d;
	int in_south = LW_FALSE;
	double delta_lon_tolerance;
	double latitude_min;

	gbox2d.flags = gflags(0, 0, 0);

	/* Return zero on non-sensical inputs */
	if ( ! pa || pa->npoints < 4 )
		return 0.0;

	/* Get the raw min/max values for the latitudes */
	ptarray_calculate_gbox_cartesian(pa, &gbox2d);

	if ( signum(gbox2d.ymin) != signum(gbox2d.ymax) )
		lwerror("ptarray_area_spheroid: cannot handle ptarray that crosses equator");

	/* Geodetic bbox < 0.0 implies geometry is entirely in southern hemisphere */
	if ( gbox2d.ymax < 0.0 )
		in_south = LW_TRUE;

	LWDEBUGF(4, "gbox2d.ymax %.12g", gbox2d.ymax);

	/* Tolerance for strip area calculation */
	if ( in_south )
	{
		delta_lon_tolerance = (90.0 / (fabs(gbox2d.ymin) / 8.0) - 2.0) / 10000.0;
		latitude_min = deg2rad(fabs(gbox2d.ymax));
	}
	else
	{
		delta_lon_tolerance = (90.0 / (fabs(gbox2d.ymax) / 8.0) - 2.0) / 10000.0;
		latitude_min = deg2rad(gbox2d.ymin);
	}

	/* Initialize first point */
	getPoint2d_p(pa, 0, &p);
	geographic_point_init(p.x, p.y, &a);

	for ( i = 1; i < pa->npoints; i++ )
	{
		GEOGRAPHIC_POINT a1, b1;
		double strip_area = 0.0;
		double delta_lon = 0.0;
		LWDEBUGF(4, "edge #%d", i);

		getPoint2d_p(pa, i, &p);
		geographic_point_init(p.x, p.y, &b);

		a1 = a;
		b1 = b;

		/* Flip into north if in south */
		if ( in_south )
		{
			a1.lat = -1.0 * a1.lat;
			b1.lat = -1.0 * b1.lat;
		}

		LWDEBUGF(4, "in_south %d", in_south);

		LWDEBUGF(4, "crosses_dateline(a, b) %d", crosses_dateline(&a, &b) );

		if ( crosses_dateline(&a, &b) )
		{
			double shift;

			if ( a1.lon > 0.0 )
				shift = (M_PI - a1.lon) + 0.088; /* About 5deg more */
			else
				shift = (M_PI - b1.lon) + 0.088; /* About 5deg more */

			LWDEBUGF(4, "shift: %.8g", shift);
			LWDEBUGF(4, "before shift a1(%.8g %.8g) b1(%.8g %.8g)", a1.lat, a1.lon, b1.lat, b1.lon);
			point_shift(&a1, shift);
			point_shift(&b1, shift);
			LWDEBUGF(4, "after shift a1(%.8g %.8g) b1(%.8g %.8g)", a1.lat, a1.lon, b1.lat, b1.lon);

		}


		delta_lon = fabs(b1.lon - a1.lon);

		LWDEBUGF(4, "a1(%.18g %.18g) b1(%.18g %.18g)", a1.lat, a1.lon, b1.lat, b1.lon);
		LWDEBUGF(4, "delta_lon %.18g", delta_lon);
		LWDEBUGF(4, "delta_lon_tolerance %.18g", delta_lon_tolerance);

		if ( delta_lon > 0.0 )
		{
			if ( delta_lon < delta_lon_tolerance )
			{
				strip_area = spheroid_striparea(&a1, &b1, latitude_min, spheroid);
				LWDEBUGF(4, "strip_area %.12g", strip_area);
				area += strip_area;
			}
			else
			{
				GEOGRAPHIC_POINT p, q;
				double step = floor(delta_lon / delta_lon_tolerance);
				double distance = spheroid_distance(&a1, &b1, spheroid);
				double pDistance = 0.0;
				int j = 0;
				LWDEBUGF(4, "step %.18g", step);
				LWDEBUGF(4, "distance %.18g", distance);
				step = distance / step;
				LWDEBUGF(4, "step %.18g", step);
				p = a1;
				while (pDistance < (distance - step * 1.01))
				{
					double azimuth = spheroid_direction(&p, &b1, spheroid);
					j++;
					LWDEBUGF(4, "  iteration %d", j);
					LWDEBUGF(4, "  azimuth %.12g", azimuth);
					pDistance = pDistance + step;
					LWDEBUGF(4, "  pDistance %.12g", pDistance);
					spheroid_project(&p, spheroid, step, azimuth, &q);
					strip_area = spheroid_striparea(&p, &q, latitude_min, spheroid);
					LWDEBUGF(4, "  strip_area %.12g", strip_area);
					area += strip_area;
					LWDEBUGF(4, "  area %.12g", area);
					p.lat = q.lat;
					p.lon = q.lon;
				}
				strip_area = spheroid_striparea(&p, &b1, latitude_min, spheroid);
				area += strip_area;
			}
		}

		/* B gets incremented in the next loop, so we save the value here */
		a = b;
	}
	return fabs(area);
}
#endif /* else USE_PRE22GEODESIC */

/**
* Calculate the area of an LWGEOM. Anything except POLYGON, MULTIPOLYGON
* and GEOMETRYCOLLECTION return zero immediately. Multi's recurse, polygons
* calculate external ring area and subtract internal ring area. A GBOX is
* required to check relationship to equator an outside point.
* WARNING: Does NOT WORK for polygons over equator or pole.
*/
double lwgeom_area_spheroid(const LWGEOM *lwgeom, const SPHEROID *spheroid)
{
	int type;

	assert(lwgeom);

	/* No area in nothing */
	if ( lwgeom_is_empty(lwgeom) )
		return 0.0;

	/* Read the geometry type number */
	type = lwgeom->type;

	/* Anything but polygons and collections returns zero */
	if ( ! ( type == POLYGONTYPE || type == MULTIPOLYGONTYPE || type == COLLECTIONTYPE ) )
		return 0.0;

	/* Actually calculate area */
	if ( type == POLYGONTYPE )
	{
		LWPOLY *poly = (LWPOLY*)lwgeom;
		int i;
		double area = 0.0;

		/* Just in case there's no rings */
		if ( poly->nrings < 1 )
			return 0.0;

		/* First, the area of the outer ring */
		area += ptarray_area_spheroid(poly->rings[0], spheroid);

		/* Subtract areas of inner rings */
		for ( i = 1; i < poly->nrings; i++ )
		{
			area -=  ptarray_area_spheroid(poly->rings[i], spheroid);
		}
		return area;
	}

	/* Recurse into sub-geometries to get area */
	if ( type == MULTIPOLYGONTYPE || type == COLLECTIONTYPE )
	{
		LWCOLLECTION *col = (LWCOLLECTION*)lwgeom;
		int i;
		double area = 0.0;

		for ( i = 0; i < col->ngeoms; i++ )
		{
			area += lwgeom_area_spheroid(col->geoms[i], spheroid);
		}
		return area;
	}

	/* Shouldn't get here. */
	return 0.0;
}



