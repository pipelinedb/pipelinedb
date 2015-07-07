/*
 * PostGIS Raster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2012 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 * Portions Copyright 2013-2015 PipelineDB
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

static void test_band_stats() {
	rt_bandstats stats = NULL;
	rt_histogram histogram = NULL;
	double bin_width[] = {100};
	double quantiles[] = {0.1, 0.3, 0.5, 0.7, 0.9};
	double quantiles2[] = {0.66666667};
	rt_quantile quantile = NULL;
	uint32_t count = 0;

	rt_raster raster;
	rt_band band;
	uint32_t x;
	uint32_t xmax = 100;
	uint32_t y;
	uint32_t ymax = 100;
	uint32_t max_run;
	double nodata;

	uint32_t values[] = {0, 91, 55, 86, 76, 41, 36, 97, 25, 63, 68, 2, 78, 15, 82, 47};
	struct quantile_llist *qlls = NULL;
	uint32_t qlls_count;

	raster = rt_raster_new(xmax, ymax);
	CU_ASSERT(raster != NULL);
	band = cu_add_band(raster, PT_32BUI, 1, 0);
	CU_ASSERT(band != NULL);

	for (x = 0; x < xmax; x++) {
		for (y = 0; y < ymax; y++) {
			rt_band_set_pixel(band, x, y, x + y, NULL);
		}
	}

	rt_band_get_nodata(band, &nodata);
	CU_ASSERT_DOUBLE_EQUAL(nodata, 0, DBL_EPSILON);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 1, 0, 1, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);
	CU_ASSERT_DOUBLE_EQUAL(stats->min, 1, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(stats->max, 198, DBL_EPSILON);

	quantile = (rt_quantile) rt_band_get_quantiles(stats, NULL, 0, &count);
	CU_ASSERT(quantile != NULL);
	rtdealloc(quantile);

	histogram = (rt_histogram) rt_band_get_histogram(stats, 0, NULL, 0, 0, 0, 0, &count);
	CU_ASSERT(histogram != NULL);
	rtdealloc(histogram);

	histogram = (rt_histogram) rt_band_get_histogram(stats, 0, NULL, 0, 1, 0, 0, &count);
	CU_ASSERT(histogram != NULL);
	rtdealloc(histogram);

	histogram = (rt_histogram) rt_band_get_histogram(stats, 0, bin_width, 1, 0, 0, 0, &count);
	CU_ASSERT(histogram != NULL);
	rtdealloc(histogram);

	rtdealloc(stats->values);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 1, 0.1, 1, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);

	quantile = (rt_quantile) rt_band_get_quantiles(stats, NULL, 0, &count);
	CU_ASSERT(quantile != NULL);
	rtdealloc(quantile);

	quantile = (rt_quantile) rt_band_get_quantiles(stats, quantiles, 5, &count);
	CU_ASSERT(quantile != NULL);
	CU_ASSERT_EQUAL(count, 5);
	rtdealloc(quantile);

	histogram = (rt_histogram) rt_band_get_histogram(stats, 0, NULL, 0, 0, 0, 0, &count);
	CU_ASSERT(histogram != NULL);
	rtdealloc(histogram);

	rtdealloc(stats->values);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 1, 0.15, 0, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 1, 0.2, 0, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 1, 0.25, 0, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 0, 0, 1, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);
	CU_ASSERT_DOUBLE_EQUAL(stats->min, 0, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(stats->max, 198, DBL_EPSILON);

	quantile = (rt_quantile) rt_band_get_quantiles(stats, NULL, 0, &count);
	CU_ASSERT(quantile != NULL);
	rtdealloc(quantile);

	rtdealloc(stats->values);
	rtdealloc(stats);

	stats = (rt_bandstats) rt_band_get_summary_stats(band, 0, 0.1, 1, NULL, NULL, NULL, NULL);
	CU_ASSERT(stats != NULL);

	quantile = (rt_quantile) rt_band_get_quantiles(stats, NULL, 0, &count);
	CU_ASSERT(quantile != NULL);
	rtdealloc(quantile);

	rtdealloc(stats->values);
	rtdealloc(stats);

	cu_free_raster(raster);

	xmax = 4;
	ymax = 4;
	raster = rt_raster_new(4, 4);
	CU_ASSERT(raster != NULL);
	band = cu_add_band(raster, PT_8BUI, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (x = 0; x < xmax; x++) {
		for (y = 0; y < ymax; y++) {
			rt_band_set_pixel(band, x, y, values[(x * ymax) + y], NULL);
		}
	}

	rt_band_get_nodata(band, &nodata);
	CU_ASSERT_DOUBLE_EQUAL(nodata, 0, DBL_EPSILON);

	quantile = (rt_quantile) rt_band_get_quantiles_stream(
		band, 1, 1, 15,
		&qlls, &qlls_count,
		quantiles2, 1,
		&count);
	CU_ASSERT(quantile != NULL);
	CU_ASSERT_NOT_EQUAL(count, 0);
	CU_ASSERT_NOT_EQUAL(qlls_count, 0);
	CU_ASSERT_DOUBLE_EQUAL(quantile[0].value, 78, DBL_EPSILON);
	rtdealloc(quantile);
	quantile_llist_destroy(&qlls, qlls_count);
	qlls = NULL;
	qlls_count = 0;

	cu_free_raster(raster);

	xmax = 100;
	ymax = 100;
	raster = rt_raster_new(xmax, ymax);
	CU_ASSERT(raster != NULL);
	band = cu_add_band(raster, PT_64BF, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (x = 0; x < xmax; x++) {
		for (y = 0; y < ymax; y++) {
			rt_band_set_pixel(band, x, y, (((double) x * y) + (x + y) + (x + y * x)) / (x + y + 1), NULL);
		}
	}

	rt_band_get_nodata(band, &nodata);
	CU_ASSERT_DOUBLE_EQUAL(nodata, 0, DBL_EPSILON);

	max_run = 5;
	for (x = 0; x < max_run; x++) {
		quantile = (rt_quantile) rt_band_get_quantiles_stream(
			band, 1, 1, xmax * ymax * max_run,
			&qlls, &qlls_count,
			quantiles2, 1,
			&count);
		CU_ASSERT(quantile != NULL);
		CU_ASSERT_NOT_EQUAL(count, 0);
		CU_ASSERT_NOT_EQUAL(qlls_count, 0);
		rtdealloc(quantile);
	}

	quantile_llist_destroy(&qlls, qlls_count);
	qlls = NULL;
	qlls_count = 0;

	cu_free_raster(raster);
}

static void test_band_value_count() {
	rt_valuecount vcnts = NULL;

	rt_raster raster;
	rt_band band;
	uint32_t x;
	uint32_t xmax = 100;
	uint32_t y;
	uint32_t ymax = 100;
	uint32_t rtn = 0;

	double count[] = {3, 4, 5};

	raster = rt_raster_new(xmax, ymax);
	CU_ASSERT(raster != NULL); /* or we're out of virtual memory */
	band = cu_add_band(raster, PT_64BF, 0, 0);
	CU_ASSERT(band != NULL);
	rt_band_set_nodata(band, 0, NULL);

	for (x = 0; x < xmax; x++) {
		for (y = 0; y < ymax; y++) {
			rt_band_set_pixel(band, x, y, (((double) x * y) + (x + y) + (x + y * x)) / (x + y + 1), NULL);
		}
	}
	vcnts = rt_band_get_value_count(band, 1, NULL, 0, 0, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	vcnts = rt_band_get_value_count(band, 1, NULL, 0, 0.01, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	vcnts = rt_band_get_value_count(band, 1, NULL, 0, 0.1, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	vcnts = rt_band_get_value_count(band, 1, NULL, 0, 1, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	vcnts = rt_band_get_value_count(band, 1, NULL, 0, 10, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	vcnts = rt_band_get_value_count(band, 1, count, 3, 1, NULL, &rtn);
	CU_ASSERT(vcnts != NULL);
	CU_ASSERT_NOT_EQUAL(rtn, 0);
	rtdealloc(vcnts);

	cu_free_raster(raster);
}

/* register tests */
void band_stats_suite_setup(void);
void band_stats_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("band_stats", NULL, NULL);
	PG_ADD_TEST(suite, test_band_stats);
	PG_ADD_TEST(suite, test_band_value_count);
}

