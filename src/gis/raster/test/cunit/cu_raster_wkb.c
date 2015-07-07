/*
 * PostGIS Raster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2012 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 * Copyright (C) 2009  Sandro Santilli <strk@keybit.net>
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

static void test_raster_wkb() {
	/* will use default allocators and message handlers */
	rt_raster raster = NULL;
	const char *hexwkb = NULL;
	const char *out = NULL;
	uint32_t len = 0;
	int i = 0;

	/* ------------------------------------------------------ */
	/* No bands, 7x8 - little endian                          */
	/* ------------------------------------------------------ */

	hexwkb =
"01"               /* little endian (uint8 ndr) */
"0000"             /* version (uint16 0) */
"0000"             /* nBands (uint16 0) */
"000000000000F03F" /* scaleX (float64 1) */
"0000000000000040" /* scaleY (float64 2) */
"0000000000000840" /* ipX (float64 3) */
"0000000000001040" /* ipY (float64 4) */
"0000000000001440" /* skewX (float64 5) */
"0000000000001840" /* skewY (float64 6) */
"0A000000"         /* SRID (int32 10) */
"0700"             /* width (uint16 7) */
"0800"             /* height (uint16 8) */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 0);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 7);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 8);

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %d\n", strlen(hexwkb));
	printf("out hexwkb len: %d\n", len);
	printf(" in hexwkb: %s\n", hexwkb);
	printf("out hexwkb: %s\n", out);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian...
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/
	free((/*no const*/ void*)out);

	{
		void *serialized;
		rt_raster rast2;

		serialized = rt_raster_serialize(raster);
		rast2 = rt_raster_deserialize(serialized, FALSE);

		cu_free_raster(rast2);
		free(serialized);
	}

	cu_free_raster(raster);

	/* ------------------------------------------------------ */
	/* No bands, 7x8 - big endian                             */
	/* ------------------------------------------------------ */

	hexwkb =
"00"               /* big endian (uint8 xdr) */
"0000"             /* version (uint16 0) */
"0000"             /* nBands (uint16 0) */
"3FF0000000000000" /* scaleX (float64 1) */
"4000000000000000" /* scaleY (float64 2) */
"4008000000000000" /* ipX (float64 3) */
"4010000000000000" /* ipY (float64 4) */
"4014000000000000" /* skewX (float64 5) */
"4018000000000000" /* skewY (float64 6) */
"0000000A"         /* SRID (int32 10) */
"0007"             /* width (uint16 7) */
"0008"             /* height (uint16 8) */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 0);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 7);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 8);

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
	printf(" in hexwkb: %s\n", hexwkb);
	printf("out hexwkb: %s\n", out);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian...
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	cu_free_raster(raster);
	free((/*no const*/ void*)out);

	/* ------------------------------------------------------ */
	/* 1x1, little endian, band0(1bb)                         */
	/* ------------------------------------------------------ */

	hexwkb =
"01"               /* little endian (uint8 ndr) */
"0000"             /* version (uint16 0) */
"0100"             /* nBands (uint16 1) */
"000000000000F03F" /* scaleX (float64 1) */
"0000000000000040" /* scaleY (float64 2) */
"0000000000000840" /* ipX (float64 3) */
"0000000000001040" /* ipY (float64 4) */
"0000000000001440" /* skewX (float64 5) */
"0000000000001840" /* skewY (float64 6) */
"0A000000"         /* SRID (int32 10) */
"0100"             /* width (uint16 1) */
"0100"             /* height (uint16 1) */
"40"               /* First band type (1BB, in memory, hasnodata) */
"00"               /* nodata value (0) */
"01"               /* pix(0,0) == 1 */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 1);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 1);
	{
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_1BB);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);
		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian...
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	cu_free_raster(raster);
	free((/*no const*/ void*)out);

	/* ------------------------------------------------------ */
	/* 3x2, big endian, band0(8BSI)                           */
	/* ------------------------------------------------------ */

	hexwkb =
"01"               /* little endian (uint8 ndr) */
"0000"             /* version (uint16 0) */
"0100"             /* nBands (uint16 1) */
"000000000000F03F" /* scaleX (float64 1) */
"0000000000000040" /* scaleY (float64 2) */
"0000000000000840" /* ipX (float64 3) */
"0000000000001040" /* ipY (float64 4) */
"0000000000001440" /* skewX (float64 5) */
"0000000000001840" /* skewY (float64 6) */
"0A000000"         /* SRID (int32 10) */
"0300"             /* width (uint16 3) */
"0200"             /* height (uint16 2) */
"43"               /* First band type (8BSI, in memory, hasnodata) */
"FF"               /* nodata value (-1) */
"FF"               /* pix(0,0) == -1 */
"00"               /* pix(1,0) ==  0 */
"01"               /* pix(2,0) == 1 */
"7F"               /* pix(0,1) == 127 */
"0A"               /* pix(1,1) == 10 */
"02"               /* pix(2,1) == 2 */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 3);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 2);
	{
		double val;
		int failure;

		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_8BSI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, -1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, -1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 127, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 10, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian...
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	free((/*no const*/ void*)out);

	{
		void *serialized;
		rt_raster rast2;

		serialized = rt_raster_serialize(raster);
		rast2 = rt_raster_deserialize(serialized, FALSE);

		cu_free_raster(rast2);
		free(serialized);

	}

	cu_free_raster(raster);

	/* ------------------------------------------------------ */
	/* 3x2, little endian, band0(16BSI)                       */
	/* ------------------------------------------------------ */

	hexwkb =
"01"               /* little endian (uint8 ndr) */
"0000"             /* version (uint16 0) */
"0100"             /* nBands (uint16 1) */
"000000000000F03F" /* scaleX (float64 1) */
"0000000000000040" /* scaleY (float64 2) */
"0000000000000840" /* ipX (float64 3) */
"0000000000001040" /* ipY (float64 4) */
"0000000000001440" /* skewX (float64 5) */
"0000000000001840" /* skewY (float64 6) */
"0A000000"         /* SRID (int32 10) */
"0300"             /* width (uint16 3) */
"0200"             /* height (uint16 2) */
"05"               /* First band type (16BSI, in memory) */
"FFFF"               /* nodata value (-1) */
"FFFF"               /* pix(0,0) == -1 */
"0000"               /* pix(1,0) ==  0 */
"F0FF"               /* pix(2,0) == -16 */
"7F00"               /* pix(0,1) == 127 */
"0A00"               /* pix(1,1) == 10 */
"0200"               /* pix(2,1) == 2 */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 3);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 2);
	{
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_16BSI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(!rt_band_get_hasnodata_flag(band));

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, -1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, -16, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 127, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 10, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	cu_free_raster(raster);
	free((/*no const*/ void*)out);

	/* ------------------------------------------------------ */
	/* 3x2, big endian, band0(16BSI)                          */
	/* ------------------------------------------------------ */

	hexwkb =
"00"               /* big endian (uint8 xdr) */
"0000"             /* version (uint16 0) */
"0001"             /* nBands (uint16 1) */
"3FF0000000000000" /* scaleX (float64 1) */
"4000000000000000" /* scaleY (float64 2) */
"4008000000000000" /* ipX (float64 3) */
"4010000000000000" /* ipY (float64 4) */
"4014000000000000" /* skewX (float64 5) */
"4018000000000000" /* skewY (float64 6) */
"0000000A"         /* SRID (int32 10) */
"0003"             /* width (uint16 3) */
"0002"             /* height (uint16 2) */
"05"               /* First band type (16BSI, in memory) */
"FFFF"             /* nodata value (-1) */
"FFFF"             /* pix(0,0) == -1 */
"0000"             /* pix(1,0) ==  0 */
"FFF0"             /* pix(2,0) == -16 */
"007F"             /* pix(0,1) == 127 */
"000A"             /* pix(1,1) == 10 */
"0002"             /* pix(2,1) == 2 */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 3);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 2);
	{
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_16BSI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(!rt_band_get_hasnodata_flag(band));

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, -1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, -16, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 127, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 10, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 1, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 2, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	cu_free_raster(raster);
	free((/*no const*/ void*)out);

	/* ------------------------------------------------------ */
	/* 3x2, bit endian, band0(16BSI ext: 3;/tmp/t.tif)        */
	/* ------------------------------------------------------ */

	hexwkb =
"00"               /* big endian (uint8 xdr) */
"0000"             /* version (uint16 0) */
"0001"             /* nBands (uint16 1) */
"3FF0000000000000" /* scaleX (float64 1) */
"4000000000000000" /* scaleY (float64 2) */
"4008000000000000" /* ipX (float64 3) */
"4010000000000000" /* ipY (float64 4) */
"4014000000000000" /* skewX (float64 5) */
"4018000000000000" /* skewY (float64 6) */
"0000000A"         /* SRID (int32 10) */
"0003"             /* width (uint16 3) */
"0002"             /* height (uint16 2) */
"C5"               /* First band type (16BSI, on disk, hasnodata) */
"FFFF"             /* nodata value (-1) */
"03"               /* ext band num == 3 */
/* ext band path == /tmp/t.tif */
"2F746D702F742E74696600"
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), 2, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 4, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 5, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 6, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 10);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 3);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 2);
	{
		double val;
		uint8_t bandnum = 0;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_16BSI);
		CU_ASSERT(rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, -1, DBL_EPSILON);
		CU_ASSERT_STRING_EQUAL(rt_band_get_ext_path(band), "/tmp/t.tif");
		CU_ASSERT_EQUAL(rt_band_get_ext_band_num(band, &bandnum), ES_NONE);
		CU_ASSERT_EQUAL(bandnum, 3);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	cu_free_raster(raster);
	free((/*no const*/ void*)out);

	/* ------------------------------------------------------ */
	/* 1x3, little endian, band0 16BSI, nodata 1, srid -1     */
	/* ------------------------------------------------------ */

	hexwkb =
"01"               /* little endian (uint8 ndr) */
"0000"             /* version (uint16 0) */
"0100"             /* nBands (uint16 1) */
"0000000000805640" /* scaleX (float64 90.0) */
"00000000008056C0" /* scaleY (float64 -90.0) */
"000000001C992D41" /* ipX (float64 969870.0) */
"00000000E49E2341" /* ipY (float64 642930.0) */
"0000000000000000" /* skewX (float64 0) */
"0000000000000000" /* skewY (float64 0) */
"FFFFFFFF"         /* SRID (int32 -1) */
"0300"             /* width (uint16 3) */
"0100"             /* height (uint16 1) */
"45"               /* First band type (16BSI, in memory, hasnodata) */
"0100"             /* nodata value (1) */
"0100"             /* pix(0,0) == 1 */
"B401"             /* pix(1,0) == 436 */
"AF01"             /* pix(2,0) == 431 */
	;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 1);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 90, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), -90, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 969870.0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 642930.0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 0);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 3);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 1);
	{
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_16BSI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 1, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 436, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 431, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %d\n", strlen(hexwkb));
	printf("out hexwkb len: %d\n", len);
	printf(" in hexwkb: %s\n", hexwkb);
	printf("out hexwkb: %s\n", out);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/
	free((/*no const*/ void*)out);

	{
		void *serialized;
		rt_raster rast2;

		serialized = rt_raster_serialize(raster);
		rast2 = rt_raster_deserialize(serialized, FALSE);

		cu_free_raster(rast2);
		free(serialized);
	}

	cu_free_raster(raster);

	/* ------------------------------------------------------ */
	/* 5x5, little endian, 3 x band 8BUI (RGB),               */
	/* nodata 0, srid -1                                      */
	/* Test case completes regress/bug_test_car5.sql          */
	/* Test case repeated 4 times to mimic 4 tiles insertion  */
	/* ------------------------------------------------------ */
	for (i = 0; i < 5; ++i)
	{

	hexwkb =
"01"	              /* little endian (uint8 ndr) */
"0000"              /* version (uint16 0) */
"0300"              /* nBands (uint16 3) */
"9A9999999999A93F"  /* scaleX (float64 0.050000) */
"9A9999999999A9BF"  /* scaleY (float64 -0.050000) */
"000000E02B274A41"  /* ipX (float64 3427927.750000) */
"0000000077195641"  /* ipY (float64 5793244.000000) */
"0000000000000000"  /* skewX (float64 0.000000) */
"0000000000000000"  /* skewY (float64 0.000000) */
"FFFFFFFF"          /* srid (int32 -1) */
"0500"              /* width (uint16 5) */
"0500"              /* height (uint16 5) */
"44"                /* 1st band pixel type (8BUI, in memory, hasnodata) */
"00"                /* 1st band nodata 0 */
"FDFEFDFEFEFDFEFEFDF9FAFEFEFCF9FBFDFEFEFDFCFAFEFEFE" /* 1st band pixels */
"44"                /* 2nd band pixel type (8BUI, in memory, hasnodata) */
"00"                /* 2nd band nodata 0 */
"4E627AADD16076B4F9FE6370A9F5FE59637AB0E54F58617087" /* 2nd band pixels */
"44"                /* 3rd band pixel type (8BUI, in memory, hasnodata) */
"00"                /* 3rd band nodata 0 */
"46566487A1506CA2E3FA5A6CAFFBFE4D566DA4CB3E454C5665" /* 3rd band pixels */
;

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	CU_ASSERT(raster != NULL);
	CU_ASSERT_EQUAL(rt_raster_get_num_bands(raster), 3);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_scale(raster), 0.05, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_scale(raster), -0.05, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_offset(raster), 3427927.75, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_offset(raster), 5793244.00, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_x_skew(raster), 0.0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_raster_get_y_skew(raster), 0.0, DBL_EPSILON);
	CU_ASSERT_EQUAL(rt_raster_get_srid(raster), 0);
	CU_ASSERT_EQUAL(rt_raster_get_width(raster), 5);
	CU_ASSERT_EQUAL(rt_raster_get_height(raster), 5);
	{
		/* Test 1st band */
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 0);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_8BUI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 253, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 254, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 253, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 3, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 254, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 4, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 254, DBL_EPSILON);
	}

	{
		/* Test 2nd band */
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 1);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_8BUI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 78, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 98, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 122, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 3, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 173, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 4, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 209, DBL_EPSILON);
	}

	{
		/* Test 3rd band */
		double val;
		int failure;
		rt_band band = rt_raster_get_band(raster, 2);
		CU_ASSERT(band != NULL);
		CU_ASSERT_EQUAL(rt_band_get_pixtype(band), PT_8BUI);
		CU_ASSERT(!rt_band_is_offline(band));
		CU_ASSERT(rt_band_get_hasnodata_flag(band));
		rt_band_get_nodata(band, &val);
		CU_ASSERT_DOUBLE_EQUAL(val, 0, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 0, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 70, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 1, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 86, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 2, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 100, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 3, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 135, DBL_EPSILON);

		failure = rt_band_get_pixel(band, 4, 0, &val, NULL);
		CU_ASSERT_EQUAL(failure, ES_NONE);
		CU_ASSERT_DOUBLE_EQUAL(val, 161, DBL_EPSILON);
	}

	out  = rt_raster_to_hexwkb(raster, FALSE, &len);
/*
	printf(" in hexwkb len: %u\n", (uint32_t) strlen(hexwkb));
	printf("out hexwkb len: %u\n", len);
*/
	CU_ASSERT_EQUAL(len, strlen(hexwkb));
/* would depend on machine endian
	CU_ASSERT_STRING_EQUAL(hexwkb, out);
*/

	free((/*no const*/ void*)out);
	{
		void *serialized;
		rt_raster rast2;

		serialized = rt_raster_serialize(raster);
		rast2 = rt_raster_deserialize(serialized, FALSE);

		cu_free_raster(rast2);
		free(serialized);
	}
	cu_free_raster(raster);

	} /* for-loop running car5 tests */

	/* ------------------------------------------------------ */
	/* TODO: New test cases	                                  */
	/* ------------------------------------------------------ */

	/* new test case */

	/* ------------------------------------------------------ */
	/*  Success summary                                       */
	/* ------------------------------------------------------ */

/*
	printf("All tests successful !\n");
*/
}

/* register tests */
void raster_wkb_suite_setup(void);
void raster_wkb_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("raster_wkb", NULL, NULL);
	PG_ADD_TEST(suite, test_raster_wkb);
}

