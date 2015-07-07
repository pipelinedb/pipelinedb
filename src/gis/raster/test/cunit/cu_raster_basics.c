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

static void test_raster_new() {
	rt_raster raster = NULL;

	raster = rt_raster_new(0, 0);
	CU_ASSERT(raster != NULL);
	cu_free_raster(raster);

	raster = rt_raster_new(1, 1);
	CU_ASSERT(raster != NULL);
	cu_free_raster(raster);

	raster = rt_raster_new(10, 10);
	CU_ASSERT(raster != NULL);
	cu_free_raster(raster);
}

static void test_raster_empty() {
	rt_raster raster = NULL;

	/* check that raster is empty */
	raster = rt_raster_new(0, 0);
	CU_ASSERT(raster != NULL);
	CU_ASSERT(rt_raster_is_empty(raster));
	cu_free_raster(raster);

	/* create raster */
	raster = rt_raster_new(1, 1);
	CU_ASSERT(raster != NULL);

	/* check that raster is not empty */
	CU_ASSERT(!rt_raster_is_empty(raster));

	cu_free_raster(raster);
}

static void test_raster_metadata() {
	rt_raster raster = NULL;

	/* create raster */
	raster = rt_raster_new(5, 5);
	CU_ASSERT(raster != NULL);

	/* # of bands */
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 0);

	/* has bands */
	CU_ASSERT(!rt_raster_has_band(raster, 1));

	/* upper-left corner */
	rt_raster_set_offsets(raster, 30, -70);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 30, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), -70, DBL_EPSILON);

	/* scale */
	rt_raster_set_scale(raster, 10, -10);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 10, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), -10, DBL_EPSILON);

	/* skew */
	rt_raster_set_skews(raster, 0.0001, -0.05);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 0.0001, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), -0.05, DBL_EPSILON);

	/* srid */
	rt_raster_set_srid(raster, 4326);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 4326);
	rt_raster_set_srid(raster, 4269);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 4269);

	cu_free_raster(raster);
}

static void test_raster_clone() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band;

	int maxX = 5;
	int maxY = 5;
	double gt[6];

	rast1 = rt_raster_new(maxX, maxY);
	CU_ASSERT(rast1 != NULL);

	rt_raster_set_offsets(rast1, 0, 0);
	rt_raster_set_scale(rast1, 1, -1);
	rt_raster_set_srid(rast1, 4326);

	band = cu_add_band(rast1, PT_32BUI, 1, 6);
	CU_ASSERT(band != NULL);

	/* clone without bands */
	rast2 = rt_raster_clone(rast1, 0);
	CU_ASSERT(rast2 != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rast2), 0);

	rt_raster_get_geotransform_matrix(rast2, gt);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rast2), 4326);
	CU_ASSERT_DOUBLE_EQUAL(gt[0], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(gt[1], 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(gt[2], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(gt[3], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(gt[4], 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(gt[5], -1, DBL_EPSILON);

	cu_free_raster(rast2);

	/* clone with bands */
	rast2 = rt_raster_clone(rast1, 1);
	CU_ASSERT(rast2 != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rast2), 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_from_band() {
	uint32_t bandNums[] = {1,3};
	int lenBandNums = 2;
	rt_raster raster;
	rt_raster rast;
	rt_band band;
	uint32_t xmax = 100;
	uint32_t ymax = 100;
	uint32_t x;

	raster = rt_raster_new(xmax, ymax);
	CU_ASSERT(raster != NULL);

	for (x = 0; x < 5; x++) {
		band = cu_add_band(raster, PT_32BUI, 0, 0);
		CU_ASSERT(band != NULL);
		rt_band_set_nodata(band, 0, NULL);
	}

	rast = rt_raster_from_band(raster, bandNums, lenBandNums);
	CU_ASSERT(rast != NULL);

	CU_ASSERT(!rt_raster_is_empty(rast));
	CU_ASSERT(rt_raster_has_band(rast, 1));

	cu_free_raster(rast);
	cu_free_raster(raster);
}

static void test_raster_replace_band() {
	rt_raster raster;
	rt_band band;
	rt_band rband;
	void* mem;
	size_t datasize;
	uint16_t width;
	uint16_t height;
	double nodata;

	raster = rt_raster_new(10, 10);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	band = cu_add_band(raster, PT_8BUI, 0, 0);
	CU_ASSERT(band != NULL);
	band = cu_add_band(raster, PT_8BUI, 1, 255);
	CU_ASSERT(band != NULL);

	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);

	datasize = rt_pixtype_size(PT_8BUI) * width * height;
	mem = rtalloc(datasize);
	band = rt_band_new_inline(width, height, PT_8BUI, 1, 1, mem);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);

	rband = rt_raster_replace_band(raster, band, 0);
	CU_ASSERT(rband != NULL);
	rt_band_get_nodata(rt_raster_get_band(raster, 0), &nodata);
	CU_ASSERT_DOUBLE_EQUAL(nodata, 1, DBL_EPSILON);

	rt_band_destroy(rband);
	cu_free_raster(raster);
}

/* register tests */
void raster_basics_suite_setup(void);
void raster_basics_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("raster_basics", NULL, NULL);
	PG_ADD_TEST(suite, test_raster_new);
	PG_ADD_TEST(suite, test_raster_empty);
	PG_ADD_TEST(suite, test_raster_metadata);
	PG_ADD_TEST(suite, test_raster_clone);
	PG_ADD_TEST(suite, test_raster_from_band);
	PG_ADD_TEST(suite, test_raster_replace_band);
}

