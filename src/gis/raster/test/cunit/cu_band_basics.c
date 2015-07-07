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

static void test_band_metadata() {
	rt_raster rast = NULL;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int temp = 0;
	double val = 0;
	char *path = "../regress/loader/testraster.tif";
	uint8_t extband = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(sizeof(uint8_t) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, sizeof(uint8_t) * width * height);

	band = rt_band_new_inline(
		width, height,
		PT_8BUI,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);

	/* isoffline */
	CU_ASSERT(!rt_band_is_offline(band));

	/* data */
	CU_ASSERT(rt_band_get_data(band) != NULL);

	/* ownsdata */
	CU_ASSERT(!rt_band_get_ownsdata_flag(band));
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	/* dimensions */
	CU_ASSERT_EQUAL(rt_band_get_width(band), width);
	CU_ASSERT_EQUAL(rt_band_get_height(band), height);

	/* pixtype */
	CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_8BUI);

	/* hasnodata */
	CU_ASSERT(!rt_band_get_hasnodata_flag(band));
	rt_band_set_hasnodata_flag(band, 1);
	CU_ASSERT(rt_band_get_hasnodata_flag(band));

	/* nodataval */
	CU_ASSERT_EQUAL(rt_band_set_nodata(band, 0, &temp), ES_NONE);
	CU_ASSERT(!temp);
	CU_ASSERT_EQUAL(rt_band_get_nodata(band, &val), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	/* clamped nodataval */
	CU_ASSERT_EQUAL(rt_band_set_nodata(band, -1, &temp), ES_NONE);
	CU_ASSERT(temp);
	CU_ASSERT_EQUAL(rt_band_get_nodata(band, &val), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	/* isnodata */
	CU_ASSERT(!rt_band_get_isnodata_flag(band));
	rt_band_check_is_nodata(band);
	CU_ASSERT(rt_band_get_isnodata_flag(band));

	rt_band_destroy(band);
	band = NULL;
	data = NULL;
	rast = NULL;

	/* offline band */
	width = 10;
	height = 10;
	band = rt_band_new_offline(
		width, height,
		PT_8BUI,
		0, 0,
		2, path
	);
	CU_ASSERT(band != NULL);

	rast = rt_raster_new(width, height);
	CU_ASSERT(rast != NULL);
	rt_raster_set_offsets(rast, 80, 80);
	CU_ASSERT_NOT_EQUAL(rt_raster_add_band(rast, band, 0), -1);

	/* isoffline */
	CU_ASSERT(rt_band_is_offline(band));

	/* ext path */
	CU_ASSERT_STRING_EQUAL(rt_band_get_ext_path(band), path);

	/* ext band number */
	CU_ASSERT_EQUAL(rt_band_get_ext_band_num(band, &extband), ES_NONE);
	CU_ASSERT_EQUAL(extband, 2);

	/* band data */
	CU_ASSERT_EQUAL(rt_band_load_offline_data(band), ES_NONE);
	CU_ASSERT(rt_band_get_data(band) != NULL);

	for (x = 0; x < width; x++) {
		for (y = 0; y < height; y++) {
			CU_ASSERT_EQUAL(rt_band_get_pixel(band, x, y, &val, NULL), ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, 1.);
		}
	}

	/* test rt_band_check_is_nodata */
	rtdealloc(band->data.offline.mem);
	band->data.offline.mem = NULL;
	CU_ASSERT_EQUAL(rt_band_check_is_nodata(band), FALSE);

	cu_free_raster(rast);
}

static void test_band_pixtype_1BB() {
	rt_pixtype pixtype = PT_1BB;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);

	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);
	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_nodata(band, 3, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 3, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	for (x = 0; x < rt_band_get_width(band); ++x) {
		for ( y = 0; y < rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_2BUI() {
	rt_pixtype pixtype = PT_2BUI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);

	err = rt_band_set_nodata(band, 3, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 3, DBL_EPSILON);

	err = rt_band_set_nodata(band, 4, &clamped); /* invalid: out of range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_nodata(band, 5, &clamped); /* invalid: out of range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 4, &clamped); /* out of range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 5, &clamped); /* out of range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 2, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 3, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 3, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_4BUI() {
	rt_pixtype pixtype = PT_4BUI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);

	err = rt_band_set_nodata(band, 4, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 4, DBL_EPSILON);

	err = rt_band_set_nodata(band, 8, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 8, DBL_EPSILON);

	err = rt_band_set_nodata(band, 15, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 15, DBL_EPSILON);

	err = rt_band_set_nodata(band, 16, &clamped);  /* out of value range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_nodata(band, 17, &clamped);  /* out of value range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 35, &clamped); /* out of value range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 3, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 3, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 7, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 7, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 15, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 15, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_8BUI() {
	rt_pixtype pixtype = PT_8BUI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);

	err = rt_band_set_nodata(band, 4, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 4, DBL_EPSILON);

	err = rt_band_set_nodata(band, 8, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 8, DBL_EPSILON);

	err = rt_band_set_nodata(band, 15, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 15, DBL_EPSILON);

	err = rt_band_set_nodata(band, 31, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

	err = rt_band_set_nodata(band, 255, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

	err = rt_band_set_nodata(band, 256, &clamped); /* out of value range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	err = rt_band_set_pixel(band, 0, 0, 256, &clamped); /* out of value range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 31, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 255, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 1, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_8BSI() {
	rt_pixtype pixtype = PT_8BSI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);

	err = rt_band_set_nodata(band, 4, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 4, DBL_EPSILON);

	err = rt_band_set_nodata(band, 8, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 8, DBL_EPSILON);

	err = rt_band_set_nodata(band, 15, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 15, DBL_EPSILON);

	err = rt_band_set_nodata(band, 31, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

	err = rt_band_set_nodata(band, -127, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, -127, DBL_EPSILON);

	err = rt_band_set_nodata(band, 127, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 127, DBL_EPSILON);

	/* out of range (-127..127) */
	err = rt_band_set_nodata(band, -129, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-127..127) */
	err = rt_band_set_nodata(band, 129, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-127..127) */
	err = rt_band_set_pixel(band, 0, 0, -129, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-127..127) */
	err = rt_band_set_pixel(band, 0, 0, 129, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 31, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 1, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, -127, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, -127, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 127, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 127, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_16BUI() {
	rt_pixtype pixtype = PT_16BUI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 31, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

	err = rt_band_set_nodata(band, 255, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65535, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65536, &clamped); /* out of range */
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of value range */
	err = rt_band_set_pixel(band, 0, 0, 65536, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of dimensions range */
	err = rt_band_set_pixel(band, rt_band_get_width(band), 0, 0, &clamped);
	CU_ASSERT((err != ES_NONE));

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 255, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 65535, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_16BSI() {
	rt_pixtype pixtype = PT_16BSI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 31, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 31, DBL_EPSILON);

	err = rt_band_set_nodata(band, 255, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

	err = rt_band_set_nodata(band, -32767, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, -32767, DBL_EPSILON);

	err = rt_band_set_nodata(band, 32767, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 32767, DBL_EPSILON);

	/* out of range (-32767..32767) */
	err = rt_band_set_nodata(band, -32769, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-32767..32767) */
	err = rt_band_set_nodata(band, 32769, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-32767..32767) */
	err = rt_band_set_pixel(band, 0, 0, -32769, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of range (-32767..32767) */
	err = rt_band_set_pixel(band, 0, 0, 32769, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of dimensions range */
	err = rt_band_set_pixel(band, rt_band_get_width(band), 0, 0, NULL);
	CU_ASSERT((err != ES_NONE));

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 255, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, -32767, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, -32767, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 32767, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 32767, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_32BUI() {
	rt_pixtype pixtype = PT_32BUI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65535, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);

	err = rt_band_set_nodata(band, 4294967295UL, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 4294967295UL, DBL_EPSILON);

	/* out of range */
	err = rt_band_set_nodata(band, 4294967296ULL, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of value range */
	err = rt_band_set_pixel(band, 0, 0, 4294967296ULL, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of dimensions range */
	err = rt_band_set_pixel(band, rt_band_get_width(band), 0, 4294967296ULL, NULL);
	CU_ASSERT((err != ES_NONE));

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 65535, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 4294967295UL, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 4294967295UL, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_32BSI() {
	rt_pixtype pixtype = PT_32BSI;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65535, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);

	err = rt_band_set_nodata(band, 2147483647, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	/*printf("32BSI pix is %ld\n", (long int)val);*/
	CU_ASSERT_DOUBLE_EQUAL(val, 2147483647, DBL_EPSILON);

	/* out of range */
	err = rt_band_set_nodata(band, 2147483648UL, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of value range */
	err = rt_band_set_pixel(band, 0, 0, 2147483648UL, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(clamped);

	/* out of dimensions range */
	err = rt_band_set_pixel(band, rt_band_get_width(band), 0, 0, NULL);
	CU_ASSERT((err != ES_NONE));

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 65535, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 65535, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 2147483647, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 2147483647, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_32BF() {
	rt_pixtype pixtype = PT_32BF;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65535.5, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 65535.5, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0.006, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0.0060000000521540, DBL_EPSILON);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 65535.5, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 65535.5, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0.006, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0.0060000000521540, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_pixtype_64BF() {
	rt_pixtype pixtype = PT_64BF;
	uint8_t *data = NULL;
	rt_band band = NULL;
	int width = 5;
	int height = 5;
	int err = 0;
	int clamped = 0;
	double val = 0;
	int x;
	int y;

	/* inline band */
	data = rtalloc(rt_pixtype_size(pixtype) * width * height);
	CU_ASSERT(data != NULL);
	memset(data, 0, rt_pixtype_size(pixtype) * width * height);

	band = rt_band_new_inline(
		width, height,
		pixtype,
		0, 0,
		data
	);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);
	CU_ASSERT(rt_band_get_ownsdata_flag(band));

	err = rt_band_set_nodata(band, 1, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	err = rt_band_set_nodata(band, 65535.56, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 65535.56, DBL_EPSILON);

	err = rt_band_set_nodata(band, 0.006, &clamped);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT(!clamped);
	rt_band_get_nodata(band, &val);
	CU_ASSERT_DOUBLE_EQUAL(val, 0.006, DBL_EPSILON);

	for (x=0; x<rt_band_get_width(band); ++x) {
		for (y=0; y<rt_band_get_height(band); ++y) {
			err = rt_band_set_pixel(band, x, y, 1, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 65535.56, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 65535.56, DBL_EPSILON);

			err = rt_band_set_pixel(band, x, y, 0.006, NULL);
			CU_ASSERT_EQUAL(err, ES_NONE);
			err = rt_band_get_pixel(band, x, y, &val, NULL);
		 	CU_ASSERT_EQUAL(err, ES_NONE);
			CU_ASSERT_DOUBLE_EQUAL(val, 0.006, DBL_EPSILON);
		}
	}

	rt_band_destroy(band);
}

static void test_band_get_pixel_line() {
	rt_raster rast;
	rt_band band;
	int maxX = 5;
	int maxY = 5;
	int x = 0;
	int y = 0;
	void *vals = NULL;
	uint16_t nvals = 0;
	int err = 0;

	rast = rt_raster_new(maxX, maxY);
	CU_ASSERT(rast != NULL);

	rt_raster_set_scale(rast, 1, -1);

	band = cu_add_band(rast, PT_8BSI, 0, 0);
	CU_ASSERT(band != NULL);

	for (y = 0; y < maxY; y++) {
		for (x = 0; x < maxX; x++)
			rt_band_set_pixel(band, x, y, x + (y * maxX), NULL);
	}

	err = rt_band_get_pixel_line(band, 0, 0, maxX, &vals, &nvals);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT_EQUAL(nvals, maxX);
	CU_ASSERT_EQUAL(((int8_t *) vals)[3], 3);
	rtdealloc(vals);
	
	err = rt_band_get_pixel_line(band, 4, 4, maxX, &vals, &nvals);
	CU_ASSERT_EQUAL(err, ES_NONE);
	CU_ASSERT_EQUAL(nvals, 1);
	CU_ASSERT_EQUAL(((int8_t *) vals)[0], 24);
	rtdealloc(vals);

	err = rt_band_get_pixel_line(band, maxX, maxY, maxX, &vals, &nvals);
	CU_ASSERT_NOT_EQUAL(err, ES_NONE);

	cu_free_raster(rast);
}

/* register tests */
void band_basics_suite_setup(void);
void band_basics_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("band_basics", NULL, NULL);
	PG_ADD_TEST(suite, test_band_metadata);
	PG_ADD_TEST(suite, test_band_pixtype_1BB);
	PG_ADD_TEST(suite, test_band_pixtype_2BUI);
	PG_ADD_TEST(suite, test_band_pixtype_4BUI);
	PG_ADD_TEST(suite, test_band_pixtype_8BUI);
	PG_ADD_TEST(suite, test_band_pixtype_8BSI);
	PG_ADD_TEST(suite, test_band_pixtype_16BUI);
	PG_ADD_TEST(suite, test_band_pixtype_16BSI);
	PG_ADD_TEST(suite, test_band_pixtype_32BUI);
	PG_ADD_TEST(suite, test_band_pixtype_32BSI);
	PG_ADD_TEST(suite, test_band_pixtype_32BF);
	PG_ADD_TEST(suite, test_band_pixtype_64BF);
	PG_ADD_TEST(suite, test_band_get_pixel_line);
}

