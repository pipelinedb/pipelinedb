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

typedef struct _callback_userargs_t* _callback_userargs;
struct _callback_userargs_t {
	uint16_t rasters;
	uint32_t rows;
	uint32_t columns;
};

/* callback for 1 raster, 0 distance, FIRST or SECOND or LAST or UNION or INTERSECTION */
static int testRasterIterator1_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 0, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);
	}
	/* 4,4 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 4
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 24, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);
	}
	/* 1,1 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);
	}
	/* 2,2 */
	else if (
		arg->dst_pixel[0] == 2 &&
		arg->dst_pixel[1] == 2
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 12, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);
	}
	/* 3,1 */
	else if (
		arg->dst_pixel[0] == 3 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 8, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);
	}
	/* 1,0 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 1, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);
	}

	return 1;
}

/* callback for 2 raster, 0 distance, UNION */
static int testRasterIterator2_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 0, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 4,4 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 4
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 24, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 118, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 1,1 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 100, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 2,2 */
	else if (
		arg->dst_pixel[0] == 2 &&
		arg->dst_pixel[1] == 2
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 12, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 106, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 3,1 */
	else if (
		arg->dst_pixel[0] == 3 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 8, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 102, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 1,0 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 1, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 1,3 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 3
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 5,0 */
	else if (
		arg->dst_pixel[0] == 5 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}

	return 1;
}

/* callback for 2 raster, 0 distance, INTERSECTION */
static int testRasterIterator3_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 100, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 0,3 */
	else if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 3
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 21, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 115, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 3,0 */
	else if (
		arg->dst_pixel[0] == 3 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 9, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 103, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 3,3 */
	else if (
		arg->dst_pixel[0] == 3 &&
		arg->dst_pixel[1] == 3
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 24, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 118, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 0,2 */
	else if (
		arg->dst_pixel[0] == 3 &&
		arg->dst_pixel[1] == 3
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}

	return 1;
}

/* callback for 2 raster, 0 distance, FIRST */
static int testRasterIterator4_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 0, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 4,4 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 4
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 24, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 118, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 4,1 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 9, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 103, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 4,0 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 4, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}

	return 1;
}

/* callback for 2 raster, 0 distance, SECOND or LAST */
static int testRasterIterator5_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 100, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 4,4 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 4
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 124, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 4,1 */
	else if (
		arg->dst_pixel[0] == 4 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 109, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 0,2 */
	else if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 2
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}

	return 1;
}

/* callback for 2 raster, 0 distance, CUSTOM */
static int testRasterIterator6_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 1,0 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 17, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 111, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 0,1 */
	else if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 21, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 115, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}
	/* 1,1 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 22, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][0][0], 116, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 0);
	}

	return 1;
}

/* callback for 2 raster, 1 distance, CUSTOM */
static int testRasterIterator7_callback(rt_iterator_arg arg, void *userarg, double *value, int *nodata) {
	_callback_userargs _userarg = (_callback_userargs) userarg;

	/* check that we're getting what we expect from userarg */
	CU_ASSERT_EQUAL(arg->rasters, _userarg->rasters);
	CU_ASSERT_EQUAL(arg->rows, _userarg->rows);
	CU_ASSERT_EQUAL(arg->columns, _userarg->columns);

	/* 0,0 */
	if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][1][1], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][1][1], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][1][1], 1);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 10, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}
	/* 1,0 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 0
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][1][1], 17, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][1][1], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][1][1], 111, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][1][1], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][2][2], 23, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][2][2], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][2][2], 117, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][2][2], 0);
	}
	/* 0,1 */
	else if (
		arg->dst_pixel[0] == 0 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][1][1], 21, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][1][1], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][1][1], 115, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][1][1], 0);

		CU_ASSERT_EQUAL(arg->nodata[0][2][0], 1);

		CU_ASSERT_EQUAL(arg->nodata[1][2][0], 1);
	}
	/* 1,1 */
	else if (
		arg->dst_pixel[0] == 1 &&
		arg->dst_pixel[1] == 1
	) {
		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][1][1], 22, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][1][1], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[1][1][1], 116, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[1][1][1], 0);

		CU_ASSERT_DOUBLE_EQUAL(arg->values[0][0][0], 16, DBL_EPSILON);
		CU_ASSERT_EQUAL(arg->nodata[0][0][0], 0);

		CU_ASSERT_EQUAL(arg->nodata[1][0][0], 1);
	}

	return 1;
}

static void test_raster_iterator() {
	rt_raster rast1;
	rt_raster rast2;
	rt_raster rast3;

	int num = 2;

	rt_raster rtn = NULL;
	rt_band band;
	int maxX = 5;
	int maxY = 5;
	rt_iterator itrset;
	_callback_userargs userargs;
	int noerr = 0;
	int x = 0;
	int y = 0;

	rast1 = rt_raster_new(maxX, maxY);
	CU_ASSERT(rast1 != NULL);

	rt_raster_set_offsets(rast1, 0, 0);
	rt_raster_set_scale(rast1, 1, -1);

	band = cu_add_band(rast1, PT_32BUI, 1, 6);
	CU_ASSERT(band != NULL);

	for (y = 0; y < maxY; y++) {
		for (x = 0; x < maxX; x++) {
			rt_band_set_pixel(band, x, y, x + (y * maxX), NULL);
		}
	}

	rast2 = rt_raster_new(maxX, maxY);
	CU_ASSERT(rast2 != NULL);

	rt_raster_set_offsets(rast2, 1, -1);
	rt_raster_set_scale(rast2, 1, -1);

	band = cu_add_band(rast2, PT_32BUI, 1, 110);
	CU_ASSERT(band != NULL);

	for (y = 0; y < maxY; y++) {
		for (x = 0; x < maxX; x++) {
			rt_band_set_pixel(band, x, y, (x + (y * maxX)) + 100, NULL);
		}
	}

	rast3 = rt_raster_new(2, 2);
	CU_ASSERT(rast3 != NULL);

	rt_raster_set_offsets(rast3, 1, -3);
	rt_raster_set_scale(rast3, 1, -1);

	/* allocate user args */
	userargs = rtalloc(sizeof(struct _callback_userargs_t));
	CU_ASSERT(userargs != NULL);

	/* allocate itrset */
	itrset = rtalloc(sizeof(struct rt_iterator_t) * num);
	CU_ASSERT(itrset != NULL);
	itrset[0].raster = rast1;
	itrset[0].nband = 0;
	itrset[0].nbnodata = 1;
	itrset[1].raster = rast2;
	itrset[1].nband = 0;
	itrset[1].nbnodata = 1;

	/* 1 raster, 0 distance, FIRST or SECOND or LAST or UNION or INTERSECTION */
	userargs->rasters = 1;
	userargs->rows = 1;
	userargs->columns = 1;

	noerr = rt_raster_iterator(
		itrset, 1,
		ET_INTERSECTION, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator1_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 5);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 5);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 1 raster, 0 distance, FIRST or SECOND or LAST or UNION or INTERSECTION */
	userargs->rasters = 1;
	userargs->rows = 1;
	userargs->columns = 1;

	noerr = rt_raster_iterator(
		itrset, 1,
		ET_UNION, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator1_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 5);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 5);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 0 distance, UNION */
	userargs->rasters = 2;
	userargs->rows = 1;
	userargs->columns = 1;

	noerr = rt_raster_iterator(
		itrset, 2,
		ET_UNION, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator2_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 6);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 6);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 0 distance, INTERSECTION */
	noerr = rt_raster_iterator(
		itrset, 2,
		ET_INTERSECTION, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator3_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 4);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 4);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 0 distance, FIRST */
	noerr = rt_raster_iterator(
		itrset, 2,
		ET_FIRST, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator4_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 5);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 5);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 0 distance, LAST or SECOND */
	noerr = rt_raster_iterator(
		itrset, 2,
		ET_LAST, NULL,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator5_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 5);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 5);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 0 distance, CUSTOM */
	noerr = rt_raster_iterator(
		itrset, 2,
		ET_CUSTOM, rast3,
		PT_32BUI,
		1, 0,
		0, 0,
		NULL,
		userargs,
		testRasterIterator6_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 2);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 2);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), -3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	/* 2 raster, 1 distance, CUSTOM */
	userargs->rasters = 2;
	userargs->rows = 3;
	userargs->columns = 3;

	noerr = rt_raster_iterator(
		itrset, 2,
		ET_CUSTOM, rast3,
		PT_32BUI,
		1, 0,
		1, 1,
		NULL,
		userargs,
		testRasterIterator7_callback,
		&rtn
	);
	CU_ASSERT_EQUAL(noerr, ES_NONE);
	CU_ASSERT_EQUAL(rt_raster_get_width(rtn), 2);
	CU_ASSERT_EQUAL(rt_raster_get_height(rtn), 2);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(rtn), -3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(rtn), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(rtn), -1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(rtn), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(rtn), 0);

	if (rtn != NULL) cu_free_raster(rtn);
	rtn = NULL;

	rtdealloc(userargs);
	rtdealloc(itrset);

	cu_free_raster(rast1);
	cu_free_raster(rast2);
	cu_free_raster(rast3);

	if (rtn != NULL) cu_free_raster(rtn);
}

static void test_band_reclass() {
	rt_reclassexpr *exprset;

	rt_raster raster;
	rt_band band;
	uint16_t x;
	uint16_t y;
	double nodata;
	int cnt = 2;
	int i = 0;
	int rtn;
	rt_band newband;
	double val;

	raster = rt_raster_new(100, 10);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	band = cu_add_band(raster, PT_16BUI, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (x = 0; x < 100; x++) {
		for (y = 0; y < 10; y++) {
			rtn = rt_band_set_pixel(band, x, y, x * y + (x + y), NULL);
		}
	}

	rt_band_get_nodata(band, &nodata);
	CU_ASSERT_DOUBLE_EQUAL(nodata, 0, DBL_EPSILON);

	exprset = rtalloc(cnt * sizeof(rt_reclassexpr));
	CU_ASSERT(exprset != NULL);

	for (i = 0; i < cnt; i++) {
		exprset[i] = rtalloc(sizeof(struct rt_reclassexpr_t));
		CU_ASSERT(exprset[i] != NULL);

		if (i == 0) {
			/* nodata */
			exprset[i]->src.min = 0;
			exprset[i]->src.inc_min = 0;
			exprset[i]->src.exc_min = 0;

			exprset[i]->src.max = 0;
			exprset[i]->src.inc_max = 0;
			exprset[i]->src.exc_max = 0;

			exprset[i]->dst.min = 0;
			exprset[i]->dst.max = 0;
		}
		else {
			/* range */
			exprset[i]->src.min = 0;
			exprset[i]->src.inc_min = 0;
			exprset[i]->src.exc_min = 0;

			exprset[i]->src.max = 1000;
			exprset[i]->src.inc_max = 1;
			exprset[i]->src.exc_max = 0;

			exprset[i]->dst.min = 1;
			exprset[i]->dst.max = 255;
		}
	}

	newband = rt_band_reclass(band, PT_8BUI, 0, 0, exprset, cnt);
	CU_ASSERT(newband != NULL);

	rtn = rt_band_get_pixel(newband, 0, 0, &val, NULL);
 	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

	rtn = rt_band_get_pixel(newband, 49, 5, &val, NULL);
 	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(val, 77, DBL_EPSILON);

	rtn = rt_band_get_pixel(newband, 99, 9, &val, NULL);
 	CU_ASSERT_EQUAL(rtn, ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(val, 255, DBL_EPSILON);

	for (i = cnt - 1; i >= 0; i--) rtdealloc(exprset[i]);
	rtdealloc(exprset);
	cu_free_raster(raster);

	rt_band_destroy(newband);
}

static void test_raster_colormap() {
	rt_raster raster;
	rt_raster rtn;
	rt_band band;
	int x;
	int y;
	rt_colormap colormap = NULL;
	double value;
	int nodata;

	raster = rt_raster_new(9, 9);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	band = cu_add_band(raster, PT_8BUI, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (y = 0; y < 9; y++) {
		for (x = 0; x < 9; x++) {
			rt_band_set_pixel(band, x, y, x, NULL);
		}
	}

	colormap = (rt_colormap) rtalloc(sizeof(struct rt_colormap_t));
	CU_ASSERT(colormap != NULL);
	colormap->nentry = 3;
	colormap->entry = (rt_colormap_entry) rtalloc(sizeof(struct rt_colormap_entry_t) * colormap->nentry);
	CU_ASSERT(colormap->entry != NULL);

	colormap->entry[0].isnodata = 0;
	colormap->entry[0].value = 8;
	colormap->entry[0].color[0] = 255;
	colormap->entry[0].color[1] = 255;
	colormap->entry[0].color[2] = 255;
	colormap->entry[0].color[3] = 255;

	colormap->entry[1].isnodata = 0;
	colormap->entry[1].value = 3;
	colormap->entry[1].color[0] = 127;
	colormap->entry[1].color[1] = 127;
	colormap->entry[1].color[2] = 127;
	colormap->entry[1].color[3] = 255;

	colormap->entry[2].isnodata = 0;
	colormap->entry[2].value = 0;
	colormap->entry[2].color[0] = 0;
	colormap->entry[2].color[1] = 0;
	colormap->entry[2].color[2] = 0;
	colormap->entry[2].color[3] = 255;

	/* 2 colors, 3 entries, INTERPOLATE */
	colormap->ncolor = 2;
	colormap->method = CM_INTERPOLATE;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);

	band = rt_raster_get_band(rtn, 0);
	CU_ASSERT(band != NULL);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 0, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 3, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 8, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);

	cu_free_raster(rtn);

	/* 4 colors, 3 entries, INTERPOLATE */
	colormap->ncolor = 4;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);
	cu_free_raster(rtn);

	/* 4 colors, 3 entries, EXACT */
	colormap->method = CM_EXACT;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);

	band = rt_raster_get_band(rtn, 0);
	CU_ASSERT(band != NULL);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 0, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 3, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 8, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 1, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 7, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);

	cu_free_raster(rtn);

	/* 4 colors, 3 entries, NEAREST */
	colormap->method = CM_NEAREST;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);

	band = rt_raster_get_band(rtn, 0);
	CU_ASSERT(band != NULL);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 0, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 3, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 8, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 1, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 2, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 4, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 7, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);

	cu_free_raster(rtn);

	/* 4 colors, 2 entries, NEAREST */
	colormap->nentry = 2;
	colormap->method = CM_NEAREST;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);

	band = rt_raster_get_band(rtn, 0);
	CU_ASSERT(band != NULL);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 0, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 3, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 8, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 1, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 2, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 4, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 127, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 7, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 255, DBL_EPSILON);

	cu_free_raster(rtn);

	rtdealloc(colormap->entry);
	rtdealloc(colormap);

	cu_free_raster(raster);

	/* new set of tests */
	raster = rt_raster_new(10, 10);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	band = cu_add_band(raster, PT_8BUI, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (y = 0; y < 10; y++) {
		for (x = 0; x < 10; x++) {
			rt_band_set_pixel(band, x, y, (x * y) + x, NULL);
		}
	}

	colormap = (rt_colormap) rtalloc(sizeof(struct rt_colormap_t));
	CU_ASSERT(colormap != NULL);
	colormap->nentry = 10;
	colormap->entry = (rt_colormap_entry) rtalloc(sizeof(struct rt_colormap_entry_t) * colormap->nentry);
	CU_ASSERT(colormap->entry != NULL);

	colormap->entry[0].isnodata = 0;
	colormap->entry[0].value = 90;
	colormap->entry[0].color[0] = 255;
	colormap->entry[0].color[1] = 255;
	colormap->entry[0].color[2] = 255;
	colormap->entry[0].color[3] = 255;

	colormap->entry[1].isnodata = 0;
	colormap->entry[1].value = 80;
	colormap->entry[1].color[0] = 255;
	colormap->entry[1].color[1] = 227;
	colormap->entry[1].color[2] = 227;
	colormap->entry[1].color[3] = 255;

	colormap->entry[2].isnodata = 0;
	colormap->entry[2].value = 70;
	colormap->entry[2].color[0] = 255;
	colormap->entry[2].color[1] = 198;
	colormap->entry[2].color[2] = 198;
	colormap->entry[2].color[3] = 255;

	colormap->entry[3].isnodata = 0;
	colormap->entry[3].value = 60;
	colormap->entry[3].color[0] = 255;
	colormap->entry[3].color[1] = 170;
	colormap->entry[3].color[2] = 170;
	colormap->entry[3].color[3] = 255;

	colormap->entry[4].isnodata = 0;
	colormap->entry[4].value = 50;
	colormap->entry[4].color[0] = 255;
	colormap->entry[4].color[1] = 142;
	colormap->entry[4].color[2] = 142;
	colormap->entry[4].color[3] = 255;

	colormap->entry[5].isnodata = 0;
	colormap->entry[5].value = 40;
	colormap->entry[5].color[0] = 255;
	colormap->entry[5].color[1] = 113;
	colormap->entry[5].color[2] = 113;
	colormap->entry[5].color[3] = 255;

	colormap->entry[6].isnodata = 0;
	colormap->entry[6].value = 30;
	colormap->entry[6].color[0] = 255;
	colormap->entry[6].color[1] = 85;
	colormap->entry[6].color[2] = 85;
	colormap->entry[6].color[3] = 255;

	colormap->entry[7].isnodata = 0;
	colormap->entry[7].value = 20;
	colormap->entry[7].color[0] = 255;
	colormap->entry[7].color[1] = 57;
	colormap->entry[7].color[2] = 57;
	colormap->entry[7].color[3] = 255;

	colormap->entry[8].isnodata = 0;
	colormap->entry[8].value = 10;
	colormap->entry[8].color[0] = 255;
	colormap->entry[8].color[1] = 28;
	colormap->entry[8].color[2] = 28;
	colormap->entry[8].color[3] = 255;

	colormap->entry[9].isnodata = 0;
	colormap->entry[9].value = 0;
	colormap->entry[9].color[0] = 255;
	colormap->entry[9].color[1] = 0;
	colormap->entry[9].color[2] = 0;
	colormap->entry[9].color[3] = 255;

	/* 2 colors, 3 entries, INTERPOLATE */
	colormap->ncolor = 4;
	colormap->method = CM_INTERPOLATE;

	rtn = rt_raster_colormap(
		raster, 0, 
		colormap
	);
	CU_ASSERT(rtn != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(rtn), colormap->ncolor);

	band = rt_raster_get_band(rtn, 2);
	CU_ASSERT(band != NULL);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 0, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 5, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 14, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 6, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 17, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 9, 0, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 25, DBL_EPSILON);

	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 2, 4, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 28, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 3, 4, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 43, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 4, 4, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 57, DBL_EPSILON);

	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 6, 9, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 170, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 7, 9, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 198, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_band_get_pixel(band, 8, 9, &value, &nodata), ES_NONE);
	CU_ASSERT_DOUBLE_EQUAL(value, 227, DBL_EPSILON);

	cu_free_raster(rtn);

	rtdealloc(colormap->entry);
	rtdealloc(colormap);

	cu_free_raster(raster);
}

/* register tests */
void mapalgebra_suite_setup(void);
void mapalgebra_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("mapalgebra", NULL, NULL);
	PG_ADD_TEST(suite, test_raster_iterator);
	PG_ADD_TEST(suite, test_band_reclass);
	PG_ADD_TEST(suite, test_raster_colormap);
}

