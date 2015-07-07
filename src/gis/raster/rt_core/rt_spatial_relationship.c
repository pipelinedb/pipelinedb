/*
 *
 * WKTRaster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2011-2013 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 * Copyright (C) 2010-2011 Jorge Arevalo <jorge.arevalo@deimos-space.com>
 * Copyright (C) 2010-2011 David Zwarg <dzwarg@azavea.com>
 * Copyright (C) 2009-2011 Pierre Racine <pierre.racine@sbf.ulaval.ca>
 * Copyright (C) 2009-2011 Mateusz Loskot <mateusz@loskot.net>
 * Copyright (C) 2008-2009 Sandro Santilli <strk@keybit.net>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */

#include "librtcore.h"
#include "librtcore_internal.h"

/*
 * Return ES_ERROR if error occurred in function.
 * Paramter aligned returns non-zero if two rasters are aligned
 *
 * @param rast1 : the first raster for alignment test
 * @param rast2 : the second raster for alignment test
 * @param *aligned : non-zero value if the two rasters are aligned
 * @param *reason : reason why rasters are not aligned
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_same_alignment(
	rt_raster rast1,
	rt_raster rast2,
	int *aligned, char **reason
) {
	double xr;
	double yr;
	double xw;
	double yw;
	int err = 0;

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != aligned);

	err = 0;
	/* same srid */
	if (rt_raster_get_srid(rast1) != rt_raster_get_srid(rast2)) {
		if (reason != NULL) *reason = "The rasters have different SRIDs";
		err = 1;
	}
	/* scales must match */
	else if (FLT_NEQ(fabs(rast1->scaleX), fabs(rast2->scaleX))) {
		if (reason != NULL) *reason = "The rasters have different scales on the X axis";
		err = 1;
	}
	else if (FLT_NEQ(fabs(rast1->scaleY), fabs(rast2->scaleY))) {
		if (reason != NULL) *reason = "The rasters have different scales on the Y axis";
		err = 1;
	}
	/* skews must match */
	else if (FLT_NEQ(rast1->skewX, rast2->skewX)) {
		if (reason != NULL) *reason = "The rasters have different skews on the X axis";
		err = 1;
	}
	else if (FLT_NEQ(rast1->skewY, rast2->skewY)) {
		if (reason != NULL) *reason = "The rasters have different skews on the Y axis";
		err = 1;
	}

	if (err) {
		*aligned = 0;
		return ES_NONE;
	}

	/* raster coordinates in context of second raster of first raster's upper-left corner */
	if (rt_raster_geopoint_to_cell(
			rast2,
			rast1->ipX, rast1->ipY,
			&xr, &yr,
			NULL
	) != ES_NONE) {
		rterror("rt_raster_same_alignment: Could not get raster coordinates of second raster from first raster's spatial coordinates");
		*aligned = 0;
		return ES_ERROR;
	}

	/* spatial coordinates of raster coordinates from above */
	if (rt_raster_cell_to_geopoint(
		rast2,
		xr, yr,
		&xw, &yw,
		NULL
	) != ES_NONE) {
		rterror("rt_raster_same_alignment: Could not get spatial coordinates of second raster from raster coordinates");
		*aligned = 0;
		return ES_ERROR;
	}

	RASTER_DEBUGF(4, "rast1(ipX, ipxY) = (%f, %f)", rast1->ipX, rast1->ipY);
	RASTER_DEBUGF(4, "rast2(xr, yr) = (%f, %f)", xr, yr);
	RASTER_DEBUGF(4, "rast2(xw, yw) = (%f, %f)", xw, yw);

	/* spatial coordinates are identical to that of first raster's upper-left corner */
	if (FLT_EQ(xw, rast1->ipX) && FLT_EQ(yw, rast1->ipY)) {
		if (reason != NULL) *reason = "The rasters are aligned";
		*aligned = 1;
		return ES_NONE;
	}

	/* no alignment */
	if (reason != NULL) *reason = "The rasters (pixel corner coordinates) are not aligned";

	*aligned = 0;
	return ES_NONE;
}

/******************************************************************************
* GEOS-based spatial relationship tests
******************************************************************************/

static
rt_errorstate rt_raster_geos_spatial_relationship(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	rt_geos_spatial_test testtype,
	int *testresult
) {
	LWMPOLY *surface1 = NULL;
	LWMPOLY *surface2 = NULL;
	GEOSGeometry *geom1 = NULL;
	GEOSGeometry *geom2 = NULL;
	int rtn = 0;
	int flag = 0;

	RASTER_DEBUG(3, "Starting");

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != testresult);

	if (nband1 < 0 && nband2 < 0) {
		nband1 = -1;
		nband2 = -1;
	}
	else {
		assert(nband1 >= 0 && nband1 < rt_raster_get_num_bands(rast1));
		assert(nband2 >= 0 && nband2 < rt_raster_get_num_bands(rast2));
	}

	/* initialize to zero, false result of spatial relationship test */
	*testresult = 0;

	/* same srid */
	if (rt_raster_get_srid(rast1) != rt_raster_get_srid(rast2)) {
		rterror("rt_raster_geos_spatial_relationship: The two rasters provided have different SRIDs");
		return ES_ERROR;
	}

	initGEOS(lwnotice, lwgeom_geos_error);

	/* get LWMPOLY of each band */
	if (rt_raster_surface(rast1, nband1, &surface1) != ES_NONE) {
		rterror("rt_raster_geos_spatial_relationship: Could not get surface of the specified band from the first raster");
		return ES_ERROR;
	}
	if (rt_raster_surface(rast2, nband2, &surface2) != ES_NONE) {
		rterror("rt_raster_geos_spatial_relationship: Could not get surface of the specified band from the second raster");
		lwmpoly_free(surface1);
		return ES_ERROR;
	}

	/* either surface is NULL, spatial relationship test is false */
	if (surface1 == NULL || surface2 == NULL) {
		if (surface1 != NULL) lwmpoly_free(surface1);
		if (surface2 != NULL) lwmpoly_free(surface2);
		return ES_NONE;
	}

	/* convert LWMPOLY to GEOSGeometry */
	geom1 = LWGEOM2GEOS(lwmpoly_as_lwgeom(surface1), 0);
	lwmpoly_free(surface1);
	if (geom1 == NULL) {
		rterror("rt_raster_geos_spatial_relationship: Could not convert surface of the specified band from the first raster to a GEOSGeometry");
		lwmpoly_free(surface2);
		return ES_ERROR;
	}

	geom2 = LWGEOM2GEOS(lwmpoly_as_lwgeom(surface2), 0);
	lwmpoly_free(surface2);
	if (geom2 == NULL) {
		rterror("rt_raster_geos_spatial_relationship: Could not convert surface of the specified band from the second raster to a GEOSGeometry");
		return ES_ERROR;
	}

	flag = 0;
	switch (testtype) {
		case GSR_OVERLAPS:
			rtn = GEOSOverlaps(geom1, geom2);
			break;
		case GSR_TOUCHES:
			rtn = GEOSTouches(geom1, geom2);
			break;
		case GSR_CONTAINS:
			rtn = GEOSContains(geom1, geom2);
			break;
		case GSR_CONTAINSPROPERLY:
			rtn = GEOSRelatePattern(geom1, geom2, "T**FF*FF*");
			break;
		case GSR_COVERS:
			rtn = GEOSRelatePattern(geom1, geom2, "******FF*");
			break;
		case GSR_COVEREDBY:
			rtn = GEOSRelatePattern(geom1, geom2, "**F**F***");
			break;
		default:
			rterror("rt_raster_geos_spatial_relationship: Unknown or unsupported GEOS spatial relationship test");
			flag = -1;
			break;
	}
	GEOSGeom_destroy(geom1);
	GEOSGeom_destroy(geom2);

	/* something happened in the spatial relationship test */
	if (rtn == 2) {
		rterror("rt_raster_geos_spatial_relationship: Could not run the appropriate GEOS spatial relationship test");
		flag = ES_ERROR;
	}
	/* spatial relationship test ran fine */
	else if (flag >= 0) {
		if (rtn != 0)
			*testresult = 1;
		flag = ES_NONE;
	}
	/* flag < 0 for when testtype is unknown */
	else
		flag = ES_ERROR;

	return flag;
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter overlaps returns non-zero if two rasters overlap
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param overlaps : non-zero value if the two rasters' bands overlaps
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_overlaps(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *overlaps
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_OVERLAPS,
		overlaps
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter touches returns non-zero if two rasters touch
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param touches : non-zero value if the two rasters' bands touch
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_touches(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *touches
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_TOUCHES,
		touches
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter contains returns non-zero if rast1 contains rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param contains : non-zero value if rast1 contains rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_contains(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *contains
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_CONTAINS,
		contains
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter contains returns non-zero if rast1 contains properly rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param contains : non-zero value if rast1 contains properly rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_contains_properly(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *contains
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_CONTAINSPROPERLY,
		contains
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter covers returns non-zero if rast1 covers rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param covers : non-zero value if rast1 covers rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_covers(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *covers
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_COVERS,
		covers
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter coveredby returns non-zero if rast1 is covered by rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param coveredby : non-zero value if rast1 is covered by rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_coveredby(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *coveredby
) {
	RASTER_DEBUG(3, "Starting");

	return rt_raster_geos_spatial_relationship(
		rast1, nband1,
		rast2, nband2,
		GSR_COVEREDBY,
		coveredby
	);
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter dwithin returns non-zero if rast1 is within the specified
 *   distance of rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param dwithin : non-zero value if rast1 is within the specified distance
 *   of rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_within_distance(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	double distance,
	int *dwithin
) {
	LWMPOLY *surface = NULL;
	LWGEOM *surface1 = NULL;
	LWGEOM *surface2 = NULL;
	double mindist = 0;

	RASTER_DEBUG(3, "Starting");

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != dwithin);

	if (nband1 < 0 && nband2 < 0) {
		nband1 = -1;
		nband2 = -1;
	}
	else {
		assert(nband1 >= 0 && nband1 < rt_raster_get_num_bands(rast1));
		assert(nband2 >= 0 && nband2 < rt_raster_get_num_bands(rast2));
	}

	/* initialize to zero, false result */
	*dwithin = 0;

	/* same srid */
	if (rt_raster_get_srid(rast1) != rt_raster_get_srid(rast2)) {
		rterror("rt_raster_distance_within: The two rasters provided have different SRIDs");
		return ES_ERROR;
	}

	/* distance cannot be less than zero */
	if (distance < 0) {
		rterror("rt_raster_distance_within: Distance cannot be less than zero");
		return ES_ERROR;
	}

	/* get LWMPOLY of each band */
	if (rt_raster_surface(rast1, nband1, &surface) != ES_NONE) {
		rterror("rt_raster_distance_within: Could not get surface of the specified band from the first raster");
		return ES_ERROR;
	}
	surface1 = lwmpoly_as_lwgeom(surface);

	if (rt_raster_surface(rast2, nband2, &surface) != ES_NONE) {
		rterror("rt_raster_distance_within: Could not get surface of the specified band from the second raster");
		lwgeom_free(surface1);
		return ES_ERROR;
	}
	surface2 = lwmpoly_as_lwgeom(surface);

	/* either surface is NULL, test is false */
	if (surface1 == NULL || surface2 == NULL) {
		if (surface1 != NULL) lwgeom_free(surface1);
		if (surface2 != NULL) lwgeom_free(surface2);
		return ES_NONE;
	}

	/* get the min distance between the two surfaces */
	mindist = lwgeom_mindistance2d_tolerance(surface1, surface2, distance);

	lwgeom_free(surface1);
	lwgeom_free(surface2);

	/* if distance >= mindist, true */
	if (FLT_EQ(mindist, distance) || distance > mindist)
		*dwithin = 1;

	RASTER_DEBUGF(3, "(mindist, distance) = (%f, %f, %d)", mindist, distance, *dwithin);

	return ES_NONE;
}

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter dfwithin returns non-zero if rast1 is fully within the specified
 *   distance of rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param dfwithin : non-zero value if rast1 is fully within the specified
 *   distance of rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_fully_within_distance(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	double distance,
	int *dfwithin
) {
	LWMPOLY *surface = NULL;
	LWGEOM *surface1 = NULL;
	LWGEOM *surface2 = NULL;
	double maxdist = 0;

	RASTER_DEBUG(3, "Starting");

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != dfwithin);

	if (nband1 < 0 && nband2 < 0) {
		nband1 = -1;
		nband2 = -1;
	}
	else {
		assert(nband1 >= 0 && nband1 < rt_raster_get_num_bands(rast1));
		assert(nband2 >= 0 && nband2 < rt_raster_get_num_bands(rast2));
	}

	/* initialize to zero, false result */
	*dfwithin = 0;

	/* same srid */
	if (rt_raster_get_srid(rast1) != rt_raster_get_srid(rast2)) {
		rterror("rt_raster_fully_within_distance: The two rasters provided have different SRIDs");
		return ES_ERROR;
	}

	/* distance cannot be less than zero */
	if (distance < 0) {
		rterror("rt_raster_fully_within_distance: Distance cannot be less than zero");
		return ES_ERROR;
	}

	/* get LWMPOLY of each band */
	if (rt_raster_surface(rast1, nband1, &surface) != ES_NONE) {
		rterror("rt_raster_fully_within_distance: Could not get surface of the specified band from the first raster");
		return ES_ERROR;
	}
	surface1 = lwmpoly_as_lwgeom(surface);

	if (rt_raster_surface(rast2, nband2, &surface) != ES_NONE) {
		rterror("rt_raster_fully_within_distance: Could not get surface of the specified band from the second raster");
		lwgeom_free(surface1);
		return ES_ERROR;
	}
	surface2 = lwmpoly_as_lwgeom(surface);

	/* either surface is NULL, test is false */
	if (surface1 == NULL || surface2 == NULL) {
		if (surface1 != NULL) lwgeom_free(surface1);
		if (surface2 != NULL) lwgeom_free(surface2);
		return ES_NONE;
	}

	/* get the maximum distance between the two surfaces */
	maxdist = lwgeom_maxdistance2d_tolerance(surface1, surface2, distance);

	lwgeom_free(surface1);
	lwgeom_free(surface2);

	/* if distance >= maxdist, true */
	if (FLT_EQ(maxdist, distance) || distance > maxdist)
		*dfwithin = 1;

	RASTER_DEBUGF(3, "(maxdist, distance, dfwithin) = (%f, %f, %d)", maxdist, distance, *dfwithin);

	return ES_NONE;
}

/******************************************************************************
* rt_raster_intersects()
******************************************************************************/

static
int rt_raster_intersects_algorithm(
	rt_raster rast1, rt_raster rast2,
	rt_band band1, rt_band band2,
	int hasnodata1, int hasnodata2,
	double nodata1, double nodata2
) {
	int i;
	int byHeight = 1;
	uint32_t dimValue;

	uint32_t row;
	uint32_t rowoffset;
	uint32_t col;
	uint32_t coloffset;

	enum line_points {X1, Y1, X2, Y2};
	enum point {pX, pY};
	double line1[4] = {0.};
	double line2[4] = {0.};
	double P[2] = {0.};
	double Qw[2] = {0.};
	double Qr[2] = {0.};
	double gt1[6] = {0.};
	double gt2[6] = {0.};
	double igt1[6] = {0};
	double igt2[6] = {0};
	double d;
	double val1;
	int noval1;
	int isnodata1;
	double val2;
	int noval2;
	int isnodata2;
	uint32_t adjacent[8] = {0};

	double xscale;
	double yscale;

	uint16_t width1;
	uint16_t height1;
	uint16_t width2;
	uint16_t height2;

	width1 = rt_raster_get_width(rast1);
	height1 = rt_raster_get_height(rast1);
	width2 = rt_raster_get_width(rast2);
	height2 = rt_raster_get_height(rast2);

	/* sampling scale */
	xscale = fmin(rt_raster_get_x_scale(rast1), rt_raster_get_x_scale(rast2)) / 10.;
	yscale = fmin(rt_raster_get_y_scale(rast1), rt_raster_get_y_scale(rast2)) / 10.;

	/* see if skew made rast2's rows are parallel to rast1's cols */
	rt_raster_cell_to_geopoint(
		rast1,
		0, 0,
		&(line1[X1]), &(line1[Y1]),
		gt1
	);

	rt_raster_cell_to_geopoint(
		rast1,
		0, height1,
		&(line1[X2]), &(line1[Y2]),
		gt1
	);

	rt_raster_cell_to_geopoint(
		rast2,
		0, 0,
		&(line2[X1]), &(line2[Y1]),
		gt2
	);

	rt_raster_cell_to_geopoint(
		rast2,
		width2, 0,
		&(line2[X2]), &(line2[Y2]),
		gt2
	);

	/* parallel vertically */
	if (FLT_EQ(line1[X2] - line1[X1], 0.) && FLT_EQ(line2[X2] - line2[X1], 0.))
		byHeight = 0;
	/* parallel */
	else if (FLT_EQ(((line1[Y2] - line1[Y1]) / (line1[X2] - line1[X1])), ((line2[Y2] - line2[Y1]) / (line2[X2] - line2[X1]))))
		byHeight = 0;

	if (byHeight)
		dimValue = height2;
	else
		dimValue = width2;
	RASTER_DEBUGF(4, "byHeight: %d, dimValue: %d", byHeight, dimValue);

	/* 3 x 3 search */
	for (coloffset = 0; coloffset < 3; coloffset++) {
		for (rowoffset = 0; rowoffset < 3; rowoffset++) {
			/* smaller raster */
			for (col = coloffset; col <= width1; col += 3) {

				rt_raster_cell_to_geopoint(
					rast1,
					col, 0,
					&(line1[X1]), &(line1[Y1]),
					gt1
				);

				rt_raster_cell_to_geopoint(
					rast1,
					col, height1,
					&(line1[X2]), &(line1[Y2]),
					gt1
				);

				/* larger raster */
				for (row = rowoffset; row <= dimValue; row += 3) {

					if (byHeight) {
						rt_raster_cell_to_geopoint(
							rast2,
							0, row,
							&(line2[X1]), &(line2[Y1]),
							gt2
						);

						rt_raster_cell_to_geopoint(
							rast2,
							width2, row,
							&(line2[X2]), &(line2[Y2]),
							gt2
						);
					}
					else {
						rt_raster_cell_to_geopoint(
							rast2,
							row, 0,
							&(line2[X1]), &(line2[Y1]),
							gt2
						);

						rt_raster_cell_to_geopoint(
							rast2,
							row, height2,
							&(line2[X2]), &(line2[Y2]),
							gt2
						);
					}

					RASTER_DEBUGF(4, "(col, row) = (%d, %d)", col, row);
					RASTER_DEBUGF(4, "line1(x1, y1, x2, y2) = (%f, %f, %f, %f)",
						line1[X1], line1[Y1], line1[X2], line1[Y2]);
					RASTER_DEBUGF(4, "line2(x1, y1, x2, y2) = (%f, %f, %f, %f)",
						line2[X1], line2[Y1], line2[X2], line2[Y2]);

					/* intersection */
					/* http://en.wikipedia.org/wiki/Line-line_intersection */
					d = ((line1[X1] - line1[X2]) * (line2[Y1] - line2[Y2])) - ((line1[Y1] - line1[Y2]) * (line2[X1] - line2[X2]));
					/* no intersection */
					if (FLT_EQ(d, 0.)) {
						continue;
					}

					P[pX] = (((line1[X1] * line1[Y2]) - (line1[Y1] * line1[X2])) * (line2[X1] - line2[X2])) -
						((line1[X1] - line1[X2]) * ((line2[X1] * line2[Y2]) - (line2[Y1] * line2[X2])));
					P[pX] = P[pX] / d;

					P[pY] = (((line1[X1] * line1[Y2]) - (line1[Y1] * line1[X2])) * (line2[Y1] - line2[Y2])) -
						((line1[Y1] - line1[Y2]) * ((line2[X1] * line2[Y2]) - (line2[Y1] * line2[X2])));
					P[pY] = P[pY] / d;

					RASTER_DEBUGF(4, "P(x, y) = (%f, %f)", P[pX], P[pY]);

					/* intersection within bounds */
					if ((
							(FLT_EQ(P[pX], line1[X1]) || FLT_EQ(P[pX], line1[X2])) ||
								(P[pX] > fmin(line1[X1], line1[X2]) && (P[pX] < fmax(line1[X1], line1[X2])))
						) && (
							(FLT_EQ(P[pY], line1[Y1]) || FLT_EQ(P[pY], line1[Y2])) ||
								(P[pY] > fmin(line1[Y1], line1[Y2]) && (P[pY] < fmax(line1[Y1], line1[Y2])))
						) && (
							(FLT_EQ(P[pX], line2[X1]) || FLT_EQ(P[pX], line2[X2])) ||
								(P[pX] > fmin(line2[X1], line2[X2]) && (P[pX] < fmax(line2[X1], line2[X2])))
						) && (
							(FLT_EQ(P[pY], line2[Y1]) || FLT_EQ(P[pY], line2[Y2])) ||
								(P[pY] > fmin(line2[Y1], line2[Y2]) && (P[pY] < fmax(line2[Y1], line2[Y2])))
					)) {
						RASTER_DEBUG(4, "within bounds");

						for (i = 0; i < 8; i++) adjacent[i] = 0;

						/* test points around intersection */
						for (i = 0; i < 8; i++) {
							switch (i) {
								case 7:
									Qw[pX] = P[pX] - xscale;
									Qw[pY] = P[pY] + yscale;
									break;
								/* 270 degrees = 09:00 */
								case 6:
									Qw[pX] = P[pX] - xscale;
									Qw[pY] = P[pY];
									break;
								case 5:
									Qw[pX] = P[pX] - xscale;
									Qw[pY] = P[pY] - yscale;
									break;
								/* 180 degrees = 06:00 */
								case 4:
									Qw[pX] = P[pX];
									Qw[pY] = P[pY] - yscale;
									break;
								case 3:
									Qw[pX] = P[pX] + xscale;
									Qw[pY] = P[pY] - yscale;
									break;
								/* 90 degrees = 03:00 */
								case 2:
									Qw[pX] = P[pX] + xscale;
									Qw[pY] = P[pY];
									break;
								/* 45 degrees */
								case 1:
									Qw[pX] = P[pX] + xscale;
									Qw[pY] = P[pY] + yscale;
									break;
								/* 0 degrees = 00:00 */
								case 0:
									Qw[pX] = P[pX];
									Qw[pY] = P[pY] + yscale;
									break;
							}

							/* unable to convert point to cell */
							noval1 = 0;
							if (rt_raster_geopoint_to_cell(
								rast1,
								Qw[pX], Qw[pY],
								&(Qr[pX]), &(Qr[pY]),
								igt1
							) != ES_NONE) {
								noval1 = 1;
							}
							/* cell is outside bounds of grid */
							else if (
								(Qr[pX] < 0 || Qr[pX] > width1 || FLT_EQ(Qr[pX], width1)) ||
								(Qr[pY] < 0 || Qr[pY] > height1 || FLT_EQ(Qr[pY], height1))
							) {
								noval1 = 1;
							}
							else if (hasnodata1 == FALSE)
								val1 = 1;
							/* unable to get value at cell */
							else if (rt_band_get_pixel(band1, Qr[pX], Qr[pY], &val1, &isnodata1) != ES_NONE)
								noval1 = 1;

							/* unable to convert point to cell */
							noval2 = 0;
							if (rt_raster_geopoint_to_cell(
								rast2,
								Qw[pX], Qw[pY],
								&(Qr[pX]), &(Qr[pY]),
								igt2
							) != ES_NONE) {
								noval2 = 1;
							}
							/* cell is outside bounds of grid */
							else if (
								(Qr[pX] < 0 || Qr[pX] > width2 || FLT_EQ(Qr[pX], width2)) ||
								(Qr[pY] < 0 || Qr[pY] > height2 || FLT_EQ(Qr[pY], height2))
							) {
								noval2 = 1;
							}
							else if (hasnodata2 == FALSE)
								val2 = 1;
							/* unable to get value at cell */
							else if (rt_band_get_pixel(band2, Qr[pX], Qr[pY], &val2, &isnodata2) != ES_NONE)
								noval2 = 1;

							if (!noval1) {
								RASTER_DEBUGF(4, "val1 = %f", val1);
							}
							if (!noval2) {
								RASTER_DEBUGF(4, "val2 = %f", val2);
							}

							/* pixels touch */
							if (!noval1 && (
								(hasnodata1 == FALSE) || !isnodata1
							)) {
								adjacent[i]++;
							}
							if (!noval2 && (
								(hasnodata2 == FALSE) || !isnodata2
							)) {
								adjacent[i] += 3;
							}

							/* two pixel values not present */
							if (noval1 || noval2) {
								RASTER_DEBUGF(4, "noval1 = %d, noval2 = %d", noval1, noval2);
								continue;
							}

							/* pixels valid, so intersect */
							if (
								((hasnodata1 == FALSE) || !isnodata1) &&
								((hasnodata2 == FALSE) || !isnodata2)
							) {
								RASTER_DEBUG(3, "The two rasters do intersect");

								return 1;
							}
						}

						/* pixels touch */
						for (i = 0; i < 4; i++) {
							RASTER_DEBUGF(4, "adjacent[%d] = %d, adjacent[%d] = %d"
								, i, adjacent[i], i + 4, adjacent[i + 4]);
							if (adjacent[i] == 0) continue;

							if (adjacent[i] + adjacent[i + 4] == 4) {
								RASTER_DEBUG(3, "The two rasters touch");

								return 1;
							}
						}
					}
					else {
						RASTER_DEBUG(4, "outside of bounds");
					}
				}
			}
		}
	}

	return 0;
}

/**
 * Return zero if error occurred in function.
 * Parameter intersects returns non-zero if two rasters intersect
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param intersects : non-zero value if the two rasters' bands intersects
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_intersects(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *intersects
) {
	int i;
	int j;
	int within = 0;

	LWGEOM *hull[2] = {NULL};
	GEOSGeometry *ghull[2] = {NULL};

	uint16_t width1;
	uint16_t height1;
	uint16_t width2;
	uint16_t height2;
	double area1;
	double area2;
	double pixarea1;
	double pixarea2;
	rt_raster rastS = NULL;
	rt_raster rastL = NULL;
	uint16_t *widthS = NULL;
	uint16_t *heightS = NULL;
	uint16_t *widthL = NULL;
	uint16_t *heightL = NULL;
	int nbandS;
	int nbandL;
	rt_band bandS = NULL;
	rt_band bandL = NULL;
	int hasnodataS = FALSE;
	int hasnodataL = FALSE;
	double nodataS = 0;
	double nodataL = 0;
	int isnodataS = 0;
	int isnodataL = 0;
	double gtS[6] = {0};
	double igtL[6] = {0};

	uint32_t row;
	uint32_t rowoffset;
	uint32_t col;
	uint32_t coloffset;

	enum line_points {X1, Y1, X2, Y2};
	enum point {pX, pY};
	double lineS[4];
	double Qr[2];
	double valS;
	double valL;

	RASTER_DEBUG(3, "Starting");

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != intersects);

	if (nband1 < 0 && nband2 < 0) {
		nband1 = -1;
		nband2 = -1;
	}
	else {
		assert(nband1 >= 0 && nband1 < rt_raster_get_num_bands(rast1));
		assert(nband2 >= 0 && nband2 < rt_raster_get_num_bands(rast2));
	}

	/* same srid */
	if (rt_raster_get_srid(rast1) != rt_raster_get_srid(rast2)) {
		rterror("rt_raster_intersects: The two rasters provided have different SRIDs");
		*intersects = 0;
		return ES_ERROR;
	}

	/* raster extents need to intersect */
	do {
		int rtn;

		initGEOS(lwnotice, lwgeom_geos_error);

		rtn = 1;
		for (i = 0; i < 2; i++) {
			if ((rt_raster_get_convex_hull(i < 1 ? rast1 : rast2, &(hull[i])) != ES_NONE) || NULL == hull[i]) {
				for (j = 0; j < i; j++) {
					GEOSGeom_destroy(ghull[j]);
					lwgeom_free(hull[j]);
				}
				rtn = 0;
				break;
			}
			ghull[i] = (GEOSGeometry *) LWGEOM2GEOS(hull[i], 0);
			if (NULL == ghull[i]) {
				for (j = 0; j < i; j++) {
					GEOSGeom_destroy(ghull[j]);
					lwgeom_free(hull[j]);
				}
				lwgeom_free(hull[i]);
				rtn = 0;
				break;
			}
		}
		if (!rtn) break;

		/* test to see if raster within the other */
		within = 0;
		if (GEOSWithin(ghull[0], ghull[1]) == 1)
			within = -1;
		else if (GEOSWithin(ghull[1], ghull[0]) == 1)
			within = 1;

		if (within != 0)
			rtn = 1;
		else
			rtn = GEOSIntersects(ghull[0], ghull[1]);

		for (i = 0; i < 2; i++) {
			GEOSGeom_destroy(ghull[i]);
			lwgeom_free(hull[i]);
		}

		if (rtn != 2) {
			RASTER_DEBUGF(4, "convex hulls of rasters do %sintersect", rtn != 1 ? "NOT " : "");
			if (rtn != 1) {
				*intersects = 0;
				return ES_NONE;
			}
			/* band isn't specified */
			else if (nband1 < 0) {
				*intersects = 1;
				return ES_NONE;
			}
		}
		else {
			RASTER_DEBUG(4, "GEOSIntersects() returned a 2!!!!");
		}
	}
	while (0);

	/* smaller raster by area or width */
	width1 = rt_raster_get_width(rast1);
	height1 = rt_raster_get_height(rast1);
	width2 = rt_raster_get_width(rast2);
	height2 = rt_raster_get_height(rast2);
	pixarea1 = fabs(rt_raster_get_x_scale(rast1) * rt_raster_get_y_scale(rast1));
	pixarea2 = fabs(rt_raster_get_x_scale(rast2) * rt_raster_get_y_scale(rast2));
	area1 = fabs(width1 * height1 * pixarea1);
	area2 = fabs(width2 * height2 * pixarea2);
	RASTER_DEBUGF(4, "pixarea1, pixarea2, area1, area2 = %f, %f, %f, %f",
		pixarea1, pixarea2, area1, area2);
	if (
		(within <= 0) ||
		(area1 < area2) ||
		FLT_EQ(area1, area2) ||
		(area1 < pixarea2) || /* area of rast1 smaller than pixel area of rast2 */
		FLT_EQ(area1, pixarea2)
	) {
		rastS = rast1;
		nbandS = nband1;
		widthS = &width1;
		heightS = &height1;

		rastL = rast2;
		nbandL = nband2;
		widthL = &width2;
		heightL = &height2;
	}
	else {
		rastS = rast2;
		nbandS = nband2;
		widthS = &width2;
		heightS = &height2;

		rastL = rast1;
		nbandL = nband1;
		widthL = &width1;
		heightL = &height1;
	}

	/* no band to use, set band to zero */
	if (nband1 < 0) {
		nbandS = 0;
		nbandL = 0;
	}

	RASTER_DEBUGF(4, "rast1 @ %p", rast1);
	RASTER_DEBUGF(4, "rast2 @ %p", rast2);
	RASTER_DEBUGF(4, "rastS @ %p", rastS);
	RASTER_DEBUGF(4, "rastL @ %p", rastL);

	/* load band of smaller raster */
	bandS = rt_raster_get_band(rastS, nbandS);
	if (NULL == bandS) {
		rterror("rt_raster_intersects: Could not get band %d of the first raster", nbandS);
		*intersects = 0;
		return ES_ERROR;
	}

	hasnodataS = rt_band_get_hasnodata_flag(bandS);
	if (hasnodataS != FALSE)
		rt_band_get_nodata(bandS, &nodataS);

	/* load band of larger raster */
	bandL = rt_raster_get_band(rastL, nbandL);
	if (NULL == bandL) {
		rterror("rt_raster_intersects: Could not get band %d of the first raster", nbandL);
		*intersects = 0;
		return ES_ERROR;
	}

	hasnodataL = rt_band_get_hasnodata_flag(bandL);
	if (hasnodataL != FALSE)
		rt_band_get_nodata(bandL, &nodataL);

	/* no band to use, ignore nodata */
	if (nband1 < 0) {
		hasnodataS = FALSE;
		hasnodataL = FALSE;
	}

	/* hasnodata(S|L) = TRUE and one of the two bands is isnodata */
	if (
		(hasnodataS && rt_band_get_isnodata_flag(bandS)) ||
		(hasnodataL && rt_band_get_isnodata_flag(bandL))
	) {
		RASTER_DEBUG(3, "One of the two raster bands is NODATA. The two rasters do not intersect");
		*intersects = 0;
		return ES_NONE;
	}

	/* special case where a raster can fit inside another raster's pixel */
	if (within != 0 && ((pixarea1 > area2) || (pixarea2 > area1))) {
		RASTER_DEBUG(4, "Using special case of raster fitting into another raster's pixel");
		/* 3 x 3 search */
		for (coloffset = 0; coloffset < 3; coloffset++) {
			for (rowoffset = 0; rowoffset < 3; rowoffset++) {
				for (col = coloffset; col < *widthS; col += 3) {
					for (row = rowoffset; row < *heightS; row += 3) {
						if (hasnodataS == FALSE)
							valS = 1;
						else if (rt_band_get_pixel(bandS, col, row, &valS, &isnodataS) != ES_NONE)
							continue;

						if ((hasnodataS == FALSE) || !isnodataS) {
							rt_raster_cell_to_geopoint(
								rastS,
								col, row,
								&(lineS[X1]), &(lineS[Y1]),
								gtS
							);

							if (rt_raster_geopoint_to_cell(
								rastL,
								lineS[X1], lineS[Y1],
								&(Qr[pX]), &(Qr[pY]),
								igtL
							) != ES_NONE) {
								continue;
							}

							if (
								(Qr[pX] < 0 || Qr[pX] > *widthL || FLT_EQ(Qr[pX], *widthL)) ||
								(Qr[pY] < 0 || Qr[pY] > *heightL || FLT_EQ(Qr[pY], *heightL))
							) {
								continue;
							}

							if (hasnodataS == FALSE)
								valL = 1;
							else if (rt_band_get_pixel(bandL, Qr[pX], Qr[pY], &valL, &isnodataL) != ES_NONE)
								continue;

							if ((hasnodataL == FALSE) || !isnodataL) {
								RASTER_DEBUG(3, "The two rasters do intersect");
								*intersects = 1;
								return ES_NONE;
							}
						}
					}
				}
			}
		}
		RASTER_DEBUG(4, "Smaller raster not in the other raster's pixel. Continuing");
	}

	RASTER_DEBUG(4, "Testing smaller raster vs larger raster");
	*intersects = rt_raster_intersects_algorithm(
		rastS, rastL,
		bandS, bandL,
		hasnodataS, hasnodataL,
		nodataS, nodataL
	);

	if (*intersects) return ES_NONE;

	RASTER_DEBUG(4, "Testing larger raster vs smaller raster");
	*intersects = rt_raster_intersects_algorithm(
		rastL, rastS,
		bandL, bandS,
		hasnodataL, hasnodataS,
		nodataL, nodataS
	);

	if (*intersects) return ES_NONE;

	RASTER_DEBUG(3, "The two rasters do not intersect");

	*intersects = 0;
	return ES_NONE;
}

