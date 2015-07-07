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

static void test_raster_geos_overlaps() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	rtn = rt_raster_overlaps(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_overlaps(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_geos_touches() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_touches(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_touches(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 3)
	*/
	rt_raster_set_offsets(rast2, 0, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-1, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 3)
	*/
	rt_raster_set_offsets(rast2, -1, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_touches(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_geos_contains() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_contains(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 3)
	*/
	rt_raster_set_offsets(rast2, 0, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-1, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 3)
	*/
	rt_raster_set_offsets(rast2, -1, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_contains(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_geos_contains_properly() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_contains_properly(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 3)
	*/
	rt_raster_set_offsets(rast2, 0, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-1, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 3)
	*/
	rt_raster_set_offsets(rast2, -1, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_contains_properly(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_geos_covers() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_covers(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_covers(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 3)
	*/
	rt_raster_set_offsets(rast2, 0, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-1, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 3)
	*/
	rt_raster_set_offsets(rast2, -1, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_covers(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_geos_covered_by() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_coveredby(
		rast1, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_coveredby(
		rast2, -1,
		rast1, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 3)
	*/
	rt_raster_set_offsets(rast2, 0, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-1, 1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 3)
	*/
	rt_raster_set_offsets(rast2, -1, 1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_coveredby(
		rast2, 0,
		rast1, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_within_distance() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast1, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast1, 0,
		-1.,
		&result
	);
	CU_ASSERT_NOT_EQUAL(rtn, ES_NONE);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		1.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	rtn = rt_raster_within_distance(
		rast1, -1,
		rast2, -1,
		2.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		1.1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-10, -1)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(-7, 2)
	*/
	rt_raster_set_offsets(rast2, -10, -1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		5,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		6,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_fully_within_distance() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast1, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast1, 0,
		-1.,
		&result
	);
	CU_ASSERT_NOT_EQUAL(rtn, ES_NONE);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		0.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		1.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, -1,
		rast2, -1,
		5.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		2.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		5.,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		5,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		10,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		5.9,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		3,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		2,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		6,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		4.25,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		3.5,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		3.65,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		3.6,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(-10, -1)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(-7, 2)
	*/
	rt_raster_set_offsets(rast2, -10, -1);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		5,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		11.5,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		6.1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		7.1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_fully_within_distance(
		rast1, 0,
		rast2, 0,
		8,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_intersects() {
	rt_raster rast1;
	rt_raster rast2;
	rt_band band1;
	rt_band band2;
	double nodata;
	int rtn;
	int result;

	/*
		rast1

		(-1, -1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(1, 1)
	*/
	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);
	rt_raster_set_offsets(rast1, -1, -1);

	band1 = cu_add_band(rast1, PT_8BUI, 1, 0);
	CU_ASSERT(band1 != NULL);
	rt_band_set_nodata(band1, 0, NULL);
	rtn = rt_band_set_pixel(band1, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band1, 1, 1, 1, NULL);

	rt_band_get_nodata(band1, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rast2 = rt_raster_new(2, 2);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	rtn = rt_raster_intersects(
		rast1, -1,
		rast2, -1,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|1|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|1|0|
						+-+-+
						|1|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|1|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(0, 0)
						+-+-+
						|0|0|
						+-+-+
						|0|0|
						+-+-+
								(2, 2)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(2, 0)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(4, 2)
	*/
	rt_raster_set_offsets(rast2, 2, 0);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(result, 1);

	/*
		rast2

		(0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rt_raster_set_scale(rast2, 0.4, 0.4);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-0.1, 0.1)
						+-+-+
						|1|1|
						+-+-+
						|1|1|
						+-+-+
								(0.9, 0.9)
	*/
	rt_raster_set_offsets(rast2, -0.1, 0.1);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/*
		rast2

		(0, 0)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(3, 3)
	*/
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rt_band_get_nodata(band2, &nodata);
	CU_ASSERT_EQUAL(nodata, 0);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
						|1|1|1|
						+-+-+-+
									(1, 1)
	*/
	rt_raster_set_offsets(rast2, -2, -2);

	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 1, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|1|
						+-+-+-+
						|1|1|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|1|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/*
		rast2

		(-2, -2)
						+-+-+-+
						|0|1|0|
						+-+-+-+
						|1|0|0|
						+-+-+-+
						|0|0|0|
						+-+-+-+
									(1, 1)
	*/
	rtn = rt_band_set_pixel(band2, 0, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 0, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 0, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);

	/* skew tests */
	/* rast2 (skewed by -0.5, 0.5) */
	rast2 = rt_raster_new(3, 3);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);
	rt_raster_set_skews(rast2, -0.5, 0.5);

	band2 = cu_add_band(rast2, PT_8BUI, 1, 0);
	CU_ASSERT(band2 != NULL);
	rt_band_set_nodata(band2, 0, NULL);
	rtn = rt_band_set_pixel(band2, 0, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 0, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 0, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 1, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 1, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 1, 2, 3, NULL);
	rtn = rt_band_set_pixel(band2, 2, 0, 1, NULL);
	rtn = rt_band_set_pixel(band2, 2, 1, 2, NULL);
	rtn = rt_band_set_pixel(band2, 2, 2, 3, NULL);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by -1, 1) */
	rt_raster_set_skews(rast2, -1, 1);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	/* rast2 (skewed by 1, -1) */
	rt_raster_set_skews(rast2, 1, -1);

	rtn = rt_raster_intersects(
		rast1, 0,
		rast2, 0,
		&result
	);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(result, 1);

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

static void test_raster_same_alignment() {
	rt_raster rast1;
	rt_raster rast2;
	int rtn;
	int aligned;
	char *reason;

	rast1 = rt_raster_new(2, 2);
	CU_ASSERT(rast1 != NULL);
	rt_raster_set_scale(rast1, 1, 1);

	rast2 = rt_raster_new(10, 10);
	CU_ASSERT(rast2 != NULL);
	rt_raster_set_scale(rast2, 1, 1);

	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, NULL);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(aligned, 0);

	rt_raster_set_scale(rast2, 0.1, 0.1);
	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, &reason);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(aligned, 0);
	CU_ASSERT_STRING_EQUAL(reason, "The rasters have different scales on the X axis");
	rt_raster_set_scale(rast2, 1, 1);

	rt_raster_set_skews(rast2, -0.5, 0.5);
	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, &reason);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(aligned, 0);
	CU_ASSERT_STRING_EQUAL(reason, "The rasters have different skews on the X axis");
	rt_raster_set_skews(rast2, 0, 0);

	rt_raster_set_offsets(rast2, 1, 1);
	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, NULL);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(aligned, 0);

	rt_raster_set_offsets(rast2, 2, 3);
	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, NULL);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_NOT_EQUAL(aligned, 0);

	rt_raster_set_offsets(rast2, 0.1, 0.1);
	rtn = rt_raster_same_alignment(rast1, rast2, &aligned, &reason);
	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_EQUAL(aligned, 0);
	CU_ASSERT_STRING_EQUAL(reason, "The rasters (pixel corner coordinates) are not aligned");

	cu_free_raster(rast2);
	cu_free_raster(rast1);
}

/* register tests */
void spatial_relationship_suite_setup(void);
void spatial_relationship_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("spatial_relationship", NULL, NULL);
	PG_ADD_TEST(suite, test_raster_geos_overlaps);
	PG_ADD_TEST(suite, test_raster_geos_touches);
	PG_ADD_TEST(suite, test_raster_geos_contains);
	PG_ADD_TEST(suite, test_raster_geos_contains_properly);
	PG_ADD_TEST(suite, test_raster_geos_covers);
	PG_ADD_TEST(suite, test_raster_geos_covered_by);
	PG_ADD_TEST(suite, test_raster_within_distance);
	PG_ADD_TEST(suite, test_raster_fully_within_distance);
	PG_ADD_TEST(suite, test_raster_intersects);
	PG_ADD_TEST(suite, test_raster_same_alignment);
}

