/*
 * PostGIS Raster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2013 Regents of the University of California
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

static void test_rgb_to_hsv() {
	double rgb[3] = {0, 0, 0};
	double hsv[3] = {0, 0, 0};

	rt_util_rgb_to_hsv(rgb, hsv);
	CU_ASSERT_DOUBLE_EQUAL(hsv[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[1], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[2], 0, DBL_EPSILON);

	rgb[0] = 0;
	rgb[1] = 0;
	rgb[2] = 1;
	rt_util_rgb_to_hsv(rgb, hsv);
	CU_ASSERT_DOUBLE_EQUAL(hsv[0], 2/3., DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[1], 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[2], 1, DBL_EPSILON);

	rgb[0] = 0;
	rgb[1] = 0.25;
	rgb[2] = 0.5;
	rt_util_rgb_to_hsv(rgb, hsv);
	CU_ASSERT_DOUBLE_EQUAL(hsv[0], 7/12., DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[1], 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[2], 0.5, DBL_EPSILON);

	rgb[0] = 0.5;
	rgb[1] = 1;
	rgb[2] = 0.5;
	rt_util_rgb_to_hsv(rgb, hsv);
	CU_ASSERT_DOUBLE_EQUAL(hsv[0], 1/3., DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[1], 0.5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[2], 1, DBL_EPSILON);

	rgb[0] = 0.2;
	rgb[1] = 0.4;
	rgb[2] = 0.4;
	rt_util_rgb_to_hsv(rgb, hsv);
	CU_ASSERT_DOUBLE_EQUAL(hsv[0], 0.5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[1], 0.5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(hsv[2], 0.4, DBL_EPSILON);
}

static void test_hsv_to_rgb() {
	double hsv[3] = {0, 0, 0};
	double rgb[3] = {0, 0, 0};

	rt_util_hsv_to_rgb(hsv, rgb);
	CU_ASSERT_DOUBLE_EQUAL(rgb[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[1], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[2], 0, DBL_EPSILON);

	hsv[0] = 2/3.;
	hsv[1] = 1;
	hsv[2] = 1;
	rt_util_hsv_to_rgb(hsv, rgb);
	CU_ASSERT_DOUBLE_EQUAL(rgb[0], 0., DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[1], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[2], 1, DBL_EPSILON);

	hsv[0] = 7/12.;
	hsv[1] = 1;
	hsv[2] = 0.5;
	rt_util_hsv_to_rgb(hsv, rgb);
	CU_ASSERT_DOUBLE_EQUAL(rgb[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[1], 0.25, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[2], 0.5, DBL_EPSILON);

	hsv[0] = 1/3.;
	hsv[1] = 0.5;
	hsv[2] = 1;
	rt_util_hsv_to_rgb(hsv, rgb);
	CU_ASSERT_DOUBLE_EQUAL(rgb[0], 0.5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[1], 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[2], 0.5, DBL_EPSILON);

	hsv[0] = 0.5;
	hsv[1] = 0.5;
	hsv[2] = 0.4;
	rt_util_hsv_to_rgb(hsv, rgb);
	CU_ASSERT_DOUBLE_EQUAL(rgb[0], 0.2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[1], 0.4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rgb[2], 0.4, DBL_EPSILON);
}

static void test_util_gdal_open() {
	extern char *gdal_enabled_drivers;

	GDALDatasetH ds;

	char *disable_all = GDAL_DISABLE_ALL;
	char *enabled = "GTiff JPEG PNG";
	char *enabled_vsi = "GTiff JPEG PNG VSICURL";

	rt_util_gdal_register_all(1);

	/* all drivers disabled */
	gdal_enabled_drivers = disable_all;
	ds = rt_util_gdal_open("/tmp/foo", GA_ReadOnly, 0);
	CU_ASSERT(ds == NULL);

	/* can't test VSICURL if HTTP driver not found */
	if (!rt_util_gdal_driver_registered("HTTP"))
		return;

	/* enabled drivers, no VSICURL */
	gdal_enabled_drivers = enabled;
	ds = rt_util_gdal_open("/vsicurl/http://download.osgeo.org/gdal/data/gtiff/small_world.tif", GA_ReadOnly, 0);
	CU_ASSERT(ds == NULL);

	/* enabled drivers with VSICURL */
	/* disabled as we don't want network access as a requirement 
	gdal_enabled_drivers = enabled_vsi;
	ds = rt_util_gdal_open("/vsicurl/http://download.osgeo.org/gdal/data/gtiff/small_world.tif", GA_ReadOnly, 0);
	CU_ASSERT(ds != NULL);
	GDALClose(ds);
	*/
}

/* register tests */
void misc_suite_setup(void);
void misc_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("misc", NULL, NULL);
	PG_ADD_TEST(suite, test_rgb_to_hsv);
	PG_ADD_TEST(suite, test_hsv_to_rgb);
	PG_ADD_TEST(suite, test_util_gdal_open);
}

