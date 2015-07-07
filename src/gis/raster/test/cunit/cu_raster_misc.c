/*
 * PostGIS Raster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2012 Regents of the University of California
 *   <bkpark@ucdavis.edu>
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

#include "CUnit/Basic.h"
#include "cu_tester.h"

static void test_raster_cell_to_geopoint() {
	rt_raster raster;
	int rtn;
	double xw, yw;
	double gt[6] = {-128.604911499087763, 0.002424431085498, 0, 53.626968388905752, 0, -0.002424431085498};

	raster = rt_raster_new(1, 1);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	rt_raster_set_srid(raster, 4326);
	rt_raster_set_geotransform_matrix(raster, gt);

	rtn = rt_raster_cell_to_geopoint(raster, 0, 0, &xw, &yw, NULL);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(xw, gt[0], DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(yw, gt[3], DBL_EPSILON);

	cu_free_raster(raster);
}

static void test_raster_geopoint_to_cell() {
	rt_raster raster;
	int rtn;
	double xr, yr;
	double gt[6] = {-128.604911499087763, 0.002424431085498, 0, 53.626968388905752, 0, -0.002424431085498};

	raster = rt_raster_new(1, 1);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	rt_raster_set_srid(raster, 4326);
	rt_raster_set_geotransform_matrix(raster, gt);

	rtn = rt_raster_geopoint_to_cell(raster, gt[0], gt[3], &xr, &yr, NULL);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(xr, 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(yr, 0, DBL_EPSILON);

	cu_free_raster(raster);
}

static void test_raster_from_two_rasters() {
	rt_raster rast1;
	rt_raster rast2;
	rt_raster rast = NULL;
	int err;
	double offset[4] = {0.};

	rast1 = rt_raster_new(4, 4);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -2, -2);

	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_FIRST,
		&rast,
		offset
	);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(rast != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_width(rast), 4);
	CU_ASSERT_EQUAL(rt_raster_get_height(rast), 4);
	CU_ASSERT_DOUBLE_EQUAL(offset[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[1], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[2], 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[3], 2, DBL_EPSILON);
	cu_free_raster(rast);

	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_SECOND,
		&rast,
		offset
	);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(rast != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_width(rast), 2);
	CU_ASSERT_EQUAL(rt_raster_get_height(rast), 2);
	CU_ASSERT_DOUBLE_EQUAL(offset[0], -2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[1], -2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[2], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[3], 0, DBL_EPSILON);
	cu_free_raster(rast);

	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_INTERSECTION,
		&rast,
		offset
	);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(rast != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_width(rast), 2);
	CU_ASSERT_EQUAL(rt_raster_get_height(rast), 2);
	CU_ASSERT_DOUBLE_EQUAL(offset[0], -2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[1], -2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[2], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[3], 0, DBL_EPSILON);
	cu_free_raster(rast);

	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_UNION,
		&rast,
		offset
	);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(rast != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_width(rast), 4);
	CU_ASSERT_EQUAL(rt_raster_get_height(rast), 4);
	CU_ASSERT_DOUBLE_EQUAL(offset[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[1], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[2], 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(offset[3], 2, DBL_EPSILON);
	cu_free_raster(rast);

	rt_raster_set_scale(rast2, 1, 0.1);
	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_UNION,
		&rast,
		offset
	);
	CU_ASSERT_NOT_EQUAL(err, ES_NONE);
	rt_raster_set_scale(rast2, 1, 1);

	rt_raster_set_srid(rast2, 9999);
	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_UNION,
		&rast,
		offset
	);
	CU_ASSERT_NOT_EQUAL(err, ES_NONE);
	rt_raster_set_srid(rast2, 0);

	rt_raster_set_skews(rast2, -1, 1);
	err = rt_raster_from_two_rasters(
		rast1, rast2,
		ET_UNION,
		&rast,
		offset
	);
	CU_ASSERT_NOT_EQUAL(err, ES_NONE);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_compute_skewed_raster() {
	rt_envelope extent;
	rt_raster rast;
	double skew[2] = {0.25, 0.25};
	double scale[2] = {1, -1};

	extent.MinX = 0;
	extent.MaxY = 0;
	extent.MaxX = 2;
	extent.MinY = -2;
	extent.UpperLeftX = extent.MinX;
	extent.UpperLeftY = extent.MaxY;

	rast = rt_raster_compute_skewed_raster(
		extent,
		skew,
		scale,
		0
	);

	CU_ASSERT(rast != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_width(rast), 2);
	CU_ASSERT_EQUAL(rt_raster_get_height(rast), 3);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rast), -0.5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rast), 0, DBL_EPSILON);

	cu_free_raster(rast);
}

/* register tests */
void raster_misc_suite_setup(void);
void raster_misc_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("raster_misc", NULL, NULL);
	PG_ADD_TEST(suite, test_raster_cell_to_geopoint);
	PG_ADD_TEST(suite, test_raster_geopoint_to_cell);
	PG_ADD_TEST(suite, test_raster_from_two_rasters);
	PG_ADD_TEST(suite, test_raster_compute_skewed_raster);
}

