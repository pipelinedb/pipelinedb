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

#include "librtcore.h"
#include "librtcore_internal.h"

/******************************************************************************
* quicksort
******************************************************************************/

#define SWAP(x, y) { double t; t = x; x = y; y = t; }
#define ORDER(x, y) if (x > y) SWAP(x, y)

static double pivot(double *left, double *right) {
	double l, m, r, *p;

	l = *left;
	m = *(left + (right - left) / 2);
	r = *right;

	/* order */
	ORDER(l, m);
	ORDER(l, r);
	ORDER(m, r);

	/* pivot is higher of two values */
	if (l < m) return m;
	if (m < r) return r;

	/* find pivot that isn't left */
	for (p = left + 1; p <= right; ++p) {
		if (*p != *left)
			return (*p < *left) ? *left : *p;
	}

	/* all values are same */
	return -1;
}

static double *partition(double *left, double *right, double pivot) {
	while (left <= right) {
		while (*left < pivot) ++left;
		while (*right >= pivot) --right;

		if (left < right) {
			SWAP(*left, *right);
			++left;
			--right;
		}
	}

	return left;
}

static void quicksort(double *left, double *right) {
	double p = pivot(left, right);
	double *pos;

	if (p != -1) {
		pos = partition(left, right, p);
		quicksort(left, pos - 1);
		quicksort(pos, right);
	}
}

/******************************************************************************
* rt_band_get_summary_stats()
******************************************************************************/

/**
 * Compute summary statistics for a band
 *
 * @param band : the band to query for summary stats
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * @param sample : percentage of pixels to sample
 * @param inc_vals : flag to include values in return struct
 * @param cK : number of pixels counted thus far in coverage
 * @param cM : M component of 1-pass stddev for coverage
 * @param cQ : Q component of 1-pass stddev for coverage
 * @param sum2: sum of the square of each input value, used for combining stddevs
 *
 * @return the summary statistics for a band or NULL
 */
rt_bandstats
rt_band_get_summary_stats(
	rt_band band,
	int exclude_nodata_value, double sample, int inc_vals,
	uint64_t *cK, double *cM, double *cQ, double *sum2
) {
	uint32_t x = 0;
	uint32_t y = 0;
	uint32_t z = 0;
	uint32_t offset = 0;
	uint32_t diff = 0;
	int rtn;
	int hasnodata = FALSE;
	double nodata = 0;
	double *values = NULL;
	double value;
	int isnodata = 0;
	rt_bandstats stats = NULL;

	uint32_t do_sample = 0;
	uint32_t sample_size = 0;
	uint32_t sample_per = 0;
	uint32_t sample_int = 0;
	uint32_t i = 0;
	uint32_t j = 0;
	double sum = 0;
	uint32_t k = 0;
	double M = 0;
	double Q = 0;

#if POSTGIS_DEBUG_LEVEL > 0
	clock_t start, stop;
	double elapsed = 0;
#endif

	RASTER_DEBUG(3, "starting");
#if POSTGIS_DEBUG_LEVEL > 0
	start = clock();
#endif

	assert(NULL != band);

	/* band is empty (width < 1 || height < 1) */
	if (band->width < 1 || band->height < 1) {
		stats = (rt_bandstats) rtalloc(sizeof(struct rt_bandstats_t));
		if (NULL == stats) {
			rterror("rt_band_get_summary_stats: Could not allocate memory for stats");
			return NULL;
		}

		rtwarn("Band is empty as width and/or height is 0");

		stats->sample = 1;
		stats->sorted = 0;
		stats->values = NULL;
		stats->count = 0;
		stats->min = stats->max = 0;
		stats->sum = 0;
		stats->mean = 0;
		stats->stddev = -1;

		return stats;
	}

	hasnodata = rt_band_get_hasnodata_flag(band);
	if (hasnodata != FALSE)
		rt_band_get_nodata(band, &nodata);
	else
		exclude_nodata_value = 0;

	RASTER_DEBUGF(3, "nodata = %f", nodata);
	RASTER_DEBUGF(3, "hasnodata = %d", hasnodata);
	RASTER_DEBUGF(3, "exclude_nodata_value = %d", exclude_nodata_value);

	/* entire band is nodata */
	if (rt_band_get_isnodata_flag(band) != FALSE) {
		stats = (rt_bandstats) rtalloc(sizeof(struct rt_bandstats_t));
		if (NULL == stats) {
			rterror("rt_band_get_summary_stats: Could not allocate memory for stats");
			return NULL;
		}

		stats->sample = 1;
		stats->sorted = 0;
		stats->values = NULL;

		if (exclude_nodata_value) {
			rtwarn("All pixels of band have the NODATA value");

			stats->count = 0;
			stats->min = stats->max = 0;
			stats->sum = 0;
			stats->mean = 0;
			stats->stddev = -1;
		}
		else {
			stats->count = band->width * band->height;
			stats->min = stats->max = nodata;
			stats->sum = stats->count * nodata;
			stats->mean = nodata;
			stats->stddev = 0;
		}

		return stats;
	}

	/* clamp percentage */
	if (
		(sample < 0 || FLT_EQ(sample, 0.0)) ||
		(sample > 1 || FLT_EQ(sample, 1.0))
	) {
		do_sample = 0;
		sample = 1;
	}
	else
		do_sample = 1;
	RASTER_DEBUGF(3, "do_sample = %d", do_sample);

	/* sample all pixels */
	if (!do_sample) {
		sample_size = band->width * band->height;
		sample_per = band->height;
	}
	/*
	 randomly sample a percentage of available pixels
	 sampling method is known as
	 	"systematic random sample without replacement"
	*/
	else {
		sample_size = round((band->width * band->height) * sample);
		sample_per = round(sample_size / band->width);
		if (sample_per < 1)
			sample_per = 1;
		sample_int = round(band->height / sample_per);
		srand(time(NULL));
	}

	RASTER_DEBUGF(3, "sampling %d of %d available pixels w/ %d per set"
		, sample_size, (band->width * band->height), sample_per);

	if (inc_vals) {
		values = rtalloc(sizeof(double) * sample_size);
		if (NULL == values) {
			rtwarn("Could not allocate memory for values");
			inc_vals = 0;
		}
	}

	/* initialize stats */
	stats = (rt_bandstats) rtalloc(sizeof(struct rt_bandstats_t));
	if (NULL == stats) {
		rterror("rt_band_get_summary_stats: Could not allocate memory for stats");
		return NULL;
	}
	stats->sample = sample;
	stats->count = 0;
	stats->sum = 0;
	stats->mean = 0;
	stats->stddev = -1;
	stats->min = stats->max = 0;
	stats->values = NULL;
	stats->sorted = 0;

	for (x = 0, j = 0, k = 0; x < band->width; x++) {
		y = -1;
		diff = 0;

		for (i = 0, z = 0; i < sample_per; i++) {
			if (!do_sample)
				y = i;
			else {
				offset = (rand() % sample_int) + 1;
				y += diff + offset;
				diff = sample_int - offset;
			}
			RASTER_DEBUGF(5, "(x, y, z) = (%d, %d, %d)", x, y, z);
			if (y >= band->height || z > sample_per) break;

			rtn = rt_band_get_pixel(band, x, y, &value, &isnodata);

			j++;
			if (rtn == ES_NONE && (!exclude_nodata_value || (exclude_nodata_value && !isnodata))) {

				/* inc_vals set, collect pixel values */
				if (inc_vals) values[k] = value;

				/* average */
				k++;
				sum += value;

				/*
					one-pass standard deviation
					http://www.eecs.berkeley.edu/~mhoemmen/cs194/Tutorials/variance.pdf
				*/
				if (k == 1) {
					Q = 0;
					M = value;
				}
				else {
					Q += (((k  - 1) * pow(value - M, 2)) / k);
					M += ((value - M ) / k);
				}

				/* coverage one-pass standard deviation */
				if (NULL != cK) {
					(*cK)++;
					if (*cK == 1) {
						*cQ = 0;
						*cM = value;
					}
					else {
						*cQ += (((*cK  - 1) * pow(value - *cM, 2)) / *cK);
						*cM += ((value - *cM ) / *cK);
					}
				}

        if (sum2)
          *sum2 += pow(value, 2);

				/* min/max */
				if (stats->count < 1) {
					stats->count = 1;
					stats->min = stats->max = value;
				}
				else {
					if (value < stats->min)
						stats->min = value;
					if (value > stats->max)
						stats->max = value;
				}

			}

			z++;
		}
	}

	RASTER_DEBUG(3, "sampling complete");

	stats->count = k;
	if (k > 0) {
		if (inc_vals) {
			/* free unused memory */
			if (sample_size != k) {
				values = rtrealloc(values, k * sizeof(double));
			}

			stats->values = values;
		}

		stats->sum = sum;
		stats->mean = sum / k;

		/* standard deviation */
		if (!do_sample)
			stats->stddev = sqrt(Q / k);
		/* sample deviation */
		else {
			if (k < 2)
				stats->stddev = -1;
			else
				stats->stddev = sqrt(Q / (k - 1));
		}
	}
	/* inc_vals thus values allocated but not used */
	else if (inc_vals)
		rtdealloc(values);

	/* if do_sample is one */
	if (do_sample && k < 1)
		rtwarn("All sampled pixels of band have the NODATA value");

#if POSTGIS_DEBUG_LEVEL > 0
	stop = clock();
	elapsed = ((double) (stop - start)) / CLOCKS_PER_SEC;
	RASTER_DEBUGF(3, "(time, count, mean, stddev, min, max) = (%0.4f, %d, %f, %f, %f, %f)",
		elapsed, stats->count, stats->mean, stats->stddev, stats->min, stats->max);
#endif

	RASTER_DEBUG(3, "done");
	return stats;
}

/******************************************************************************
* rt_band_get_histogram()
******************************************************************************/

/**
 * Count the distribution of data
 *
 * @param stats : a populated stats struct for processing
 * @param bin_count : the number of bins to group the data by
 * @param bin_width : the width of each bin as an array
 * @param bin_width_count : number of values in bin_width
 * @param right : evaluate bins by (a,b] rather than default [a,b)
 * @param min : user-defined minimum value of the histogram
 *   a value less than the minimum value is not counted in any bins
 *   if min = max, min and max are not used
 * @param max : user-defined maximum value of the histogram
 *   a value greater than the max value is not counted in any bins
 *   if min = max, min and max are not used
 * @param rtn_count : set to the number of bins being returned
 *
 * @return the histogram of the data or NULL
 */
rt_histogram
rt_band_get_histogram(
	rt_bandstats stats,
	int bin_count, double *bin_width, int bin_width_count,
	int right, double min, double max,
	uint32_t *rtn_count
) {
	rt_histogram bins = NULL;
	int init_width = 0;
	int i;
	int j;
	double tmp;
	double value;
	int sum = 0;
	double qmin;
	double qmax;

#if POSTGIS_DEBUG_LEVEL > 0
	clock_t start, stop;
	double elapsed = 0;
#endif

	RASTER_DEBUG(3, "starting");
#if POSTGIS_DEBUG_LEVEL > 0
	start = clock();
#endif

	assert(NULL != stats);
	assert(NULL != rtn_count);

	if (stats->count < 1 || NULL == stats->values) {
		rterror("rt_util_get_histogram: rt_bandstats object has no value");
		return NULL;
	}

	/* bin width must be positive numbers and not zero */
	if (NULL != bin_width && bin_width_count > 0) {
		for (i = 0; i < bin_width_count; i++) {
			if (bin_width[i] < 0 || FLT_EQ(bin_width[i], 0.0)) {
				rterror("rt_util_get_histogram: bin_width element is less than or equal to zero");
				return NULL;
			}
		}
	}

	/* ignore min and max parameters */
	if (FLT_EQ(max, min)) {
		qmin = stats->min;
		qmax = stats->max;
	}
	else {
		qmin = min;
		qmax = max;
		if (qmin > qmax) {
			qmin = max;
			qmax = min;
		}
	}

	/* # of bins not provided */
	if (bin_count <= 0) {
		/*
			determine # of bins
			http://en.wikipedia.org/wiki/Histogram

			all computed bins are assumed to have equal width
		*/
		/* Square-root choice for stats->count < 30 */
		if (stats->count < 30)
			bin_count = ceil(sqrt(stats->count));
		/* Sturges' formula for stats->count >= 30 */
		else
			bin_count = ceil(log2((double) stats->count) + 1.);

		/* bin_width_count provided and bin_width has value */
		if (bin_width_count > 0 && NULL != bin_width) {
			/* user has defined something specific */
			if (bin_width_count > bin_count)
				bin_count = bin_width_count;
			else if (bin_width_count > 1) {
				tmp = 0;
				for (i = 0; i < bin_width_count; i++) tmp += bin_width[i];
				bin_count = ceil((qmax - qmin) / tmp) * bin_width_count;
			}
			else
				bin_count = ceil((qmax - qmin) / bin_width[0]);
		}
		/* set bin width count to zero so that one can be calculated */
		else {
			bin_width_count = 0;
		}
	}

	/* min and max the same */
	if (FLT_EQ(qmax, qmin)) bin_count = 1;

	RASTER_DEBUGF(3, "bin_count = %d", bin_count);

	/* bin count = 1, all values are in one bin */
	if (bin_count < 2) {
		bins = rtalloc(sizeof(struct rt_histogram_t));
		if (NULL == bins) {
			rterror("rt_util_get_histogram: Could not allocate memory for histogram");
			return NULL;
		}

		bins->count = stats->count;
		bins->percent = -1;
		bins->min = qmin;
		bins->max = qmax;
		bins->inc_min = bins->inc_max = 1;

		*rtn_count = bin_count;
		return bins;
	}

	/* establish bin width */
	if (bin_width_count == 0) {
		bin_width_count = 1;

		/* bin_width unallocated */
		if (NULL == bin_width) {
			bin_width = rtalloc(sizeof(double));
			if (NULL == bin_width) {
				rterror("rt_util_get_histogram: Could not allocate memory for bin widths");
				return NULL;
			}
			init_width = 1;
		}

		bin_width[0] = (qmax - qmin) / bin_count;
	}

	/* initialize bins */
	bins = rtalloc(bin_count * sizeof(struct rt_histogram_t));
	if (NULL == bins) {
		rterror("rt_util_get_histogram: Could not allocate memory for histogram");
		if (init_width) rtdealloc(bin_width);
		return NULL;
	}
	if (!right)
		tmp = qmin;
	else
		tmp = qmax;
	for (i = 0; i < bin_count;) {
		for (j = 0; j < bin_width_count; j++) {
			bins[i].count = 0;
			bins->percent = -1;

			if (!right) {
				bins[i].min = tmp;
				tmp += bin_width[j];
				bins[i].max = tmp;

				bins[i].inc_min = 1;
				bins[i].inc_max = 0;
			}
			else {
				bins[i].max = tmp;
				tmp -= bin_width[j];
				bins[i].min = tmp;

				bins[i].inc_min = 0;
				bins[i].inc_max = 1;
			}

			i++;
		}
	}
	if (!right) {
		bins[bin_count - 1].inc_max = 1;

		/* align last bin to the max value */
		if (bins[bin_count - 1].max < qmax)
			bins[bin_count - 1].max = qmax;
	}
	else {
		bins[bin_count - 1].inc_min = 1;

		/* align first bin to the min value */
		if (bins[bin_count - 1].min > qmin)
			bins[bin_count - 1].min = qmin;
	}

	/* process the values */
	for (i = 0; i < stats->count; i++) {
		value = stats->values[i];

		/* default, [a, b) */
		if (!right) {
			for (j = 0; j < bin_count; j++) {
				if (
					(!bins[j].inc_max && value < bins[j].max) || (
						bins[j].inc_max && (
							(value < bins[j].max) ||
							FLT_EQ(value, bins[j].max)
						)
					)
				) {
					bins[j].count++;
					sum++;
					break;
				}
			}
		}
		/* (a, b] */
		else {
			for (j = 0; j < bin_count; j++) {
				if (
					(!bins[j].inc_min && value > bins[j].min) || (
						bins[j].inc_min && (
							(value > bins[j].min) ||
							FLT_EQ(value, bins[j].min)
						)
					)
				) {
					bins[j].count++;
					sum++;
					break;
				}
			}
		}
	}

	for (i = 0; i < bin_count; i++) {
		bins[i].percent = ((double) bins[i].count) / sum;
	}

#if POSTGIS_DEBUG_LEVEL > 0
	stop = clock();
	elapsed = ((double) (stop - start)) / CLOCKS_PER_SEC;
	RASTER_DEBUGF(3, "elapsed time = %0.4f", elapsed);

	for (j = 0; j < bin_count; j++) {
		RASTER_DEBUGF(5, "(min, max, inc_min, inc_max, count, sum, percent) = (%f, %f, %d, %d, %d, %d, %f)",
			bins[j].min, bins[j].max, bins[j].inc_min, bins[j].inc_max, bins[j].count, sum, bins[j].percent);
	}
#endif

	if (init_width) rtdealloc(bin_width);
	*rtn_count = bin_count;
	RASTER_DEBUG(3, "done");
	return bins;
}

/******************************************************************************
* rt_band_get_quantiles()
******************************************************************************/

/**
 * Compute the default set of or requested quantiles for a set of data
 * the quantile formula used is same as Excel and R default method
 *
 * @param stats : a populated stats struct for processing
 * @param quantiles : the quantiles to be computed
 * @param quantiles_count : the number of quantiles to be computed
 * @param rtn_count : set to the number of quantiles being returned
 *
 * @return the default set of or requested quantiles for a band or NULL
 */
rt_quantile
rt_band_get_quantiles(
	rt_bandstats stats,
	double *quantiles, int quantiles_count,
	uint32_t *rtn_count
) {
	rt_quantile rtn;
	int init_quantiles = 0;
	int i = 0;
	double h;
	int hl;

#if POSTGIS_DEBUG_LEVEL > 0
	clock_t start, stop;
	double elapsed = 0;
#endif

	RASTER_DEBUG(3, "starting");
#if POSTGIS_DEBUG_LEVEL > 0
	start = clock();
#endif

	assert(NULL != stats);
	assert(NULL != rtn_count);

	if (stats->count < 1 || NULL == stats->values) {
		rterror("rt_band_get_quantiles: rt_bandstats object has no value");
		return NULL;
	}

	/* quantiles not provided */
	if (NULL == quantiles) {
		/* quantile count not specified, default to quartiles */
		if (quantiles_count < 2)
			quantiles_count = 5;

		quantiles = rtalloc(sizeof(double) * quantiles_count);
		init_quantiles = 1;
		if (NULL == quantiles) {
			rterror("rt_band_get_quantiles: Could not allocate memory for quantile input");
			return NULL;
		}

		quantiles_count--;
		for (i = 0; i <= quantiles_count; i++)
			quantiles[i] = ((double) i) / quantiles_count;
		quantiles_count++;
	}

	/* check quantiles */
	for (i = 0; i < quantiles_count; i++) {
		if (quantiles[i] < 0. || quantiles[i] > 1.) {
			rterror("rt_band_get_quantiles: Quantile value not between 0 and 1");
			if (init_quantiles) rtdealloc(quantiles);
			return NULL;
		}
	}
	quicksort(quantiles, quantiles + quantiles_count - 1);

	/* initialize rt_quantile */
	rtn = rtalloc(sizeof(struct rt_quantile_t) * quantiles_count);
	if (NULL == rtn) {
		rterror("rt_band_get_quantiles: Could not allocate memory for quantile output");
		if (init_quantiles) rtdealloc(quantiles);
		return NULL;
	}

	/* sort values */
	if (!stats->sorted) {
		quicksort(stats->values, stats->values + stats->count - 1);
		stats->sorted = 1;
	}

	/*
		make quantiles

		formula is that used in R (method 7) and Excel from
			http://en.wikipedia.org/wiki/Quantile
	*/
	for (i = 0; i < quantiles_count; i++) {
		rtn[i].quantile = quantiles[i];

		h = ((stats->count - 1.) * quantiles[i]) + 1.;
		hl = floor(h);

		/* h greater than hl, do full equation */
		if (h > hl)
			rtn[i].value = stats->values[hl - 1] + ((h - hl) * (stats->values[hl] - stats->values[hl - 1]));
		/* shortcut as second part of equation is zero */
		else
			rtn[i].value = stats->values[hl - 1];
	}

#if POSTGIS_DEBUG_LEVEL > 0
	stop = clock();
	elapsed = ((double) (stop - start)) / CLOCKS_PER_SEC;
	RASTER_DEBUGF(3, "elapsed time = %0.4f", elapsed);
#endif

	*rtn_count = quantiles_count;
	if (init_quantiles) rtdealloc(quantiles);
	RASTER_DEBUG(3, "done");
	return rtn;
}

/******************************************************************************
* rt_band_get_quantiles_stream()
******************************************************************************/

static struct quantile_llist_element *quantile_llist_search(
	struct quantile_llist_element *element,
	double needle
) {
	if (NULL == element)
		return NULL;
	else if (FLT_NEQ(needle, element->value)) {
		if (NULL != element->next)
			return quantile_llist_search(element->next, needle);
		else
			return NULL;
	}
	else
		return element;
}

static struct quantile_llist_element *quantile_llist_insert(
	struct quantile_llist_element *element,
	double value,
	uint32_t *idx
) {
	struct quantile_llist_element *qle = NULL;

	if (NULL == element) {
		qle = rtalloc(sizeof(struct quantile_llist_element));
		RASTER_DEBUGF(4, "qle @ %p is only element in list", qle);
		if (NULL == qle) return NULL;

		qle->value = value;
		qle->count = 1;

		qle->prev = NULL;
		qle->next = NULL;

		if (NULL != idx) *idx = 0;
		return qle;
	}
	else if (value > element->value) {
		if (NULL != idx) *idx += 1;
		if (NULL != element->next)
			return quantile_llist_insert(element->next, value, idx);
		/* insert as last element in list */
		else {
			qle = rtalloc(sizeof(struct quantile_llist_element));
			RASTER_DEBUGF(4, "insert qle @ %p as last element", qle);
			if (NULL == qle) return NULL;

			qle->value = value;
			qle->count = 1;

			qle->prev = element;
			qle->next = NULL;
			element->next = qle;

			return qle;
		}
	}
	/* insert before current element */
	else {
		qle = rtalloc(sizeof(struct quantile_llist_element));
		RASTER_DEBUGF(4, "insert qle @ %p before current element", qle);
		if (NULL == qle) return NULL;

		qle->value = value;
		qle->count = 1;

		if (NULL != element->prev) element->prev->next = qle;
		qle->next = element;
		qle->prev = element->prev;
		element->prev = qle;

		return qle;
	}
}

static int quantile_llist_delete(struct quantile_llist_element *element) {
	if (NULL == element) return 0;

	/* beginning of list */
	if (NULL == element->prev && NULL != element->next) {
		element->next->prev = NULL;
	}
	/* end of list */
	else if (NULL != element->prev && NULL == element->next) {
		element->prev->next = NULL;
	}
	/* within list */
	else if (NULL != element->prev && NULL != element->next) {
		element->prev->next = element->next;
		element->next->prev = element->prev;
	}

	RASTER_DEBUGF(4, "qle @ %p destroyed", element);
	rtdealloc(element);

	return 1;
}

int quantile_llist_destroy(struct quantile_llist **list, uint32_t list_count) {
	struct quantile_llist_element *element = NULL;
	uint32_t i;

	if (NULL == *list) return 0;

	for (i = 0; i < list_count; i++) {
		element = (*list)[i].head;
		while (NULL != element->next) {
			quantile_llist_delete(element->next);
		}
		quantile_llist_delete(element);

		rtdealloc((*list)[i].index);
	}

	rtdealloc(*list);
	return 1;
}

static void quantile_llist_index_update(struct quantile_llist *qll, struct quantile_llist_element *qle, uint32_t idx) {
	uint32_t anchor = (uint32_t) floor(idx / 100);

	if (qll->tail == qle) return;

	if (
		(anchor != 0) && (
			NULL == qll->index[anchor].element ||
			idx <= qll->index[anchor].index
		)
	) {
		qll->index[anchor].index = idx;
		qll->index[anchor].element = qle;
	}

	if (anchor != 0 && NULL == qll->index[0].element) {
		qll->index[0].index = 0;
		qll->index[0].element = qll->head;
	}
}

static void quantile_llist_index_delete(struct quantile_llist *qll, struct quantile_llist_element *qle) {
	uint32_t i = 0;

	for (i = 0; i < qll->index_max; i++) {
		if (
			NULL == qll->index[i].element ||
			(qll->index[i].element) != qle
		) {
			continue;
		}

		RASTER_DEBUGF(5, "deleting index: %d => %f", i, qle->value);
		qll->index[i].index = -1;
		qll->index[i].element = NULL;
	}
}

static struct quantile_llist_element *quantile_llist_index_search(
	struct quantile_llist *qll,
	double value,
	uint32_t *index
) {
	uint32_t i = 0, j = 0;
	RASTER_DEBUGF(5, "searching index for value %f", value);

	for (i = 0; i < qll->index_max; i++) {
		if (NULL == qll->index[i].element) {
			if (i < 1) break;
			continue;
		}
		if (value > (qll->index[i]).element->value) continue;

		if (FLT_EQ(value, qll->index[i].element->value)) {
			RASTER_DEBUGF(5, "using index value at %d = %f", i, qll->index[i].element->value);
			*index = i * 100;
			return qll->index[i].element;
		}
		else if (i > 0) {
			for (j = 1; j < i; j++) {
				if (NULL != qll->index[i - j].element) {
					RASTER_DEBUGF(5, "using index value at %d = %f", i - j, qll->index[i - j].element->value);
					*index = (i - j) * 100;
					return qll->index[i - j].element;
				}
			}
		}
	}

	*index = 0;
	return qll->head;
}

static void quantile_llist_index_reset(struct quantile_llist *qll) {
	uint32_t i = 0;

	RASTER_DEBUG(5, "resetting index");
	for (i = 0; i < qll->index_max; i++) {
		qll->index[i].index = -1;
		qll->index[i].element = NULL;
	}
}


/**
 * Compute the default set of or requested quantiles for a coverage
 *
 * This function is based upon the algorithm described in:
 *
 * A One-Pass Space-Efficient Algorithm for Finding Quantiles (1995)
 *   by Rakesh Agrawal, Arun Swami
 *   in Proc. 7th Intl. Conf. Management of Data (COMAD-95)
 *
 * http://www.almaden.ibm.com/cs/projects/iis/hdb/Publications/papers/comad95.pdf
 *
 * In the future, it may be worth exploring algorithms that don't
 *   require the size of the coverage
 *
 * @param band : the band to include in the quantile search
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * @param sample : percentage of pixels to sample
 * @param cov_count : number of values in coverage
 * @param qlls : set of quantile_llist structures
 * @param qlls_count : the number of quantile_llist structures
 * @param quantiles : the quantiles to be computed
 *   if bot qlls and quantiles provided, qlls is used
 * @param quantiles_count : the number of quantiles to be computed
 * @param rtn_count : the number of quantiles being returned
 *
 * @return the default set of or requested quantiles for a band or NULL
 */
rt_quantile
rt_band_get_quantiles_stream(
	rt_band band,
	int exclude_nodata_value, double sample,
	uint64_t cov_count,
	struct quantile_llist **qlls, uint32_t *qlls_count,
	double *quantiles, int quantiles_count,
	uint32_t *rtn_count
) {
	rt_quantile rtn = NULL;
	int init_quantiles = 0;

	struct quantile_llist *qll = NULL;
	struct quantile_llist_element *qle = NULL;
	struct quantile_llist_element *qls = NULL;
	const uint32_t MAX_VALUES = 750;

	uint8_t *data = NULL;
	double value;
	int isnodata = 0;

	uint32_t a = 0;
	uint32_t i = 0;
	uint32_t j = 0;
	uint32_t k = 0;
	uint32_t x = 0;
	uint32_t y = 0;
	uint32_t z = 0;
	uint32_t idx = 0;
	uint32_t offset = 0;
	uint32_t diff = 0;
	uint8_t exists = 0;

	uint32_t do_sample = 0;
	uint32_t sample_size = 0;
	uint32_t sample_per = 0;
	uint32_t sample_int = 0;
	int status;

	RASTER_DEBUG(3, "starting");

	assert(NULL != band);
	assert(cov_count > 1);
	assert(NULL != rtn_count);
	RASTER_DEBUGF(3, "cov_count = %d", cov_count);

	data = rt_band_get_data(band);
	if (data == NULL) {
		rterror("rt_band_get_summary_stats: Cannot get band data");
		return NULL;
	}

	if (!rt_band_get_hasnodata_flag(band))
		exclude_nodata_value = 0;
	RASTER_DEBUGF(3, "exclude_nodata_value = %d", exclude_nodata_value);

	/* quantile_llist not provided */
	if (NULL == *qlls) {
		/* quantiles not provided */
		if (NULL == quantiles) {
			/* quantile count not specified, default to quartiles */
			if (quantiles_count < 2)
				quantiles_count = 5;

			quantiles = rtalloc(sizeof(double) * quantiles_count);
			init_quantiles = 1;
			if (NULL == quantiles) {
				rterror("rt_band_get_quantiles_stream: Could not allocate memory for quantile input");
				return NULL;
			}

			quantiles_count--;
			for (i = 0; i <= quantiles_count; i++)
				quantiles[i] = ((double) i) / quantiles_count;
			quantiles_count++;
		}

		/* check quantiles */
		for (i = 0; i < quantiles_count; i++) {
			if (quantiles[i] < 0. || quantiles[i] > 1.) {
				rterror("rt_band_get_quantiles_stream: Quantile value not between 0 and 1");
				if (init_quantiles) rtdealloc(quantiles);
				return NULL;
			}
		}
		quicksort(quantiles, quantiles + quantiles_count - 1);

		/* initialize linked-list set */
		*qlls_count = quantiles_count * 2;
		RASTER_DEBUGF(4, "qlls_count = %d", *qlls_count);
		*qlls = rtalloc(sizeof(struct quantile_llist) * *qlls_count);
		if (NULL == *qlls) {
			rterror("rt_band_get_quantiles_stream: Could not allocate memory for quantile output");
			if (init_quantiles) rtdealloc(quantiles);
			return NULL;
		}

		j = (uint32_t) floor(MAX_VALUES / 100.) + 1;
		for (i = 0; i < *qlls_count; i++) {
			qll = &((*qlls)[i]);
			qll->quantile = quantiles[(i * quantiles_count) / *qlls_count];
			qll->count = 0;
			qll->sum1 = 0;
			qll->sum2 = 0;
			qll->head = NULL;
			qll->tail = NULL;

			/* initialize index */
			qll->index = rtalloc(sizeof(struct quantile_llist_index) * j);
			if (NULL == qll->index) {
				rterror("rt_band_get_quantiles_stream: Could not allocate memory for quantile output");
				if (init_quantiles) rtdealloc(quantiles);
				return NULL;
			}
			qll->index_max = j;
			quantile_llist_index_reset(qll);

			/* AL-GEQ */
			if (!(i % 2)) {
				qll->algeq = 1;
				qll->tau = (uint64_t) ROUND(cov_count - (cov_count * qll->quantile), 0);
				if (qll->tau < 1) qll->tau = 1;
			}
			/* AL-GT */
			else {
				qll->algeq = 0;
				qll->tau = cov_count - (*qlls)[i - 1].tau + 1;
			}

			RASTER_DEBUGF(4, "qll init: (algeq, quantile, count, tau, sum1, sum2) = (%d, %f, %d, %d, %d, %d)",
				qll->algeq, qll->quantile, qll->count, qll->tau, qll->sum1, qll->sum2);
			RASTER_DEBUGF(4, "qll init: (head, tail) = (%p, %p)", qll->head, qll->tail);
		}

		if (init_quantiles) rtdealloc(quantiles);
	}

	/* clamp percentage */
	if (
		(sample < 0 || FLT_EQ(sample, 0.0)) ||
		(sample > 1 || FLT_EQ(sample, 1.0))
	) {
		do_sample = 0;
		sample = 1;
	}
	else
		do_sample = 1;
	RASTER_DEBUGF(3, "do_sample = %d", do_sample);

	/* sample all pixels */
	if (!do_sample) {
		sample_size = band->width * band->height;
		sample_per = band->height;
	}
	/*
	 randomly sample a percentage of available pixels
	 sampling method is known as
	 	"systematic random sample without replacement"
	*/
	else {
		sample_size = round((band->width * band->height) * sample);
		sample_per = round(sample_size / band->width);
		sample_int = round(band->height / sample_per);
		srand(time(NULL));
	}
	RASTER_DEBUGF(3, "sampling %d of %d available pixels w/ %d per set"
		, sample_size, (band->width * band->height), sample_per);

	for (x = 0, j = 0, k = 0; x < band->width; x++) {
		y = -1;
		diff = 0;

		/* exclude_nodata_value = TRUE and band is NODATA */
		if (exclude_nodata_value && rt_band_get_isnodata_flag(band)) {
			RASTER_DEBUG(3, "Skipping quantile calcuation as band is NODATA");
			break;
		}

		for (i = 0, z = 0; i < sample_per; i++) {
			if (do_sample != 1)
				y = i;
			else {
				offset = (rand() % sample_int) + 1;
				y += diff + offset;
				diff = sample_int - offset;
			}
			RASTER_DEBUGF(5, "(x, y, z) = (%d, %d, %d)", x, y, z);
			if (y >= band->height || z > sample_per) break;

			status = rt_band_get_pixel(band, x, y, &value, &isnodata);

			j++;
			if (status == ES_NONE && (!exclude_nodata_value || (exclude_nodata_value && !isnodata))) {

				/* process each quantile */
				for (a = 0; a < *qlls_count; a++) {
					qll = &((*qlls)[a]);
					qls = NULL;
					RASTER_DEBUGF(4, "%d of %d (%f)", a + 1, *qlls_count, qll->quantile);
					RASTER_DEBUGF(5, "qll before: (algeq, quantile, count, tau, sum1, sum2) = (%d, %f, %d, %d, %d, %d)",
						qll->algeq, qll->quantile, qll->count, qll->tau, qll->sum1, qll->sum2);
					RASTER_DEBUGF(5, "qll before: (head, tail) = (%p, %p)", qll->head, qll->tail);

					/* OPTIMIZATION: shortcuts for quantiles of zero or one */
					if (FLT_EQ(qll->quantile, 0.)) {
						if (NULL != qll->head) {
							if (value < qll->head->value)
								qll->head->value = value;
						}
						else {
							qle = quantile_llist_insert(qll->head, value, NULL);
							qll->head = qle;
							qll->tail = qle;
							qll->count = 1;
						}

						RASTER_DEBUGF(4, "quantile shortcut for %f\n\n", qll->quantile);
						continue;
					}
					else if (FLT_EQ(qll->quantile, 1.)) {
						if (NULL != qll->head) {
							if (value > qll->head->value)
								qll->head->value = value;
						}
						else {
							qle = quantile_llist_insert(qll->head, value, NULL);
							qll->head = qle;
							qll->tail = qle;
							qll->count = 1;
						}

						RASTER_DEBUGF(4, "quantile shortcut for %f\n\n", qll->quantile);
						continue;
					}

					/* value exists in list */
					/* OPTIMIZATION: check to see if value exceeds last value */
					if (NULL != qll->tail && value > qll->tail->value)
						qle = NULL;
					/* OPTIMIZATION: check to see if value equals last value */
					else if (NULL != qll->tail && FLT_EQ(value, qll->tail->value))
						qle = qll->tail;
					/* OPTIMIZATION: use index if possible */
					else {
						qls = quantile_llist_index_search(qll, value, &idx);
						qle = quantile_llist_search(qls, value);
					}

					/* value found */
					if (NULL != qle) {
						RASTER_DEBUGF(4, "%f found in list", value);
						RASTER_DEBUGF(5, "qle before: (value, count) = (%f, %d)", qle->value, qle->count);

						qle->count++;
						qll->sum1++;

						if (qll->algeq)
							qll->sum2 = qll->sum1 - qll->head->count;
						else
							qll->sum2 = qll->sum1 - qll->tail->count;

						RASTER_DEBUGF(4, "qle after: (value, count) = (%f, %d)", qle->value, qle->count);
					}
					/* can still add element */
					else if (qll->count < MAX_VALUES) {
						RASTER_DEBUGF(4, "Adding %f to list", value);

						/* insert */
						/* OPTIMIZATION: check to see if value exceeds last value */
						if (NULL != qll->tail && (value > qll->tail->value || FLT_EQ(value, qll->tail->value))) {
							idx = qll->count;
							qle = quantile_llist_insert(qll->tail, value, &idx);
						}
						/* OPTIMIZATION: use index if possible */
						else
							qle = quantile_llist_insert(qls, value, &idx);
						if (NULL == qle) return NULL;
						RASTER_DEBUGF(5, "value added at index: %d => %f", idx, value);
						qll->count++;
						qll->sum1++;

						/* first element */
						if (NULL == qle->prev)
							qll->head = qle;
						/* last element */
						if (NULL == qle->next)
							qll->tail = qle;

						if (qll->algeq)
							qll->sum2 = qll->sum1 - qll->head->count;
						else
							qll->sum2 = qll->sum1 - qll->tail->count;

						/* index is only needed if there are at least 100 values */
						quantile_llist_index_update(qll, qle, idx);

						RASTER_DEBUGF(5, "qle, prev, next, head, tail = %p, %p, %p, %p, %p", qle, qle->prev, qle->next, qll->head, qll->tail);
					}
					/* AL-GEQ */
					else if (qll->algeq) {
						RASTER_DEBUGF(4, "value, head->value = %f, %f", value, qll->head->value);

						if (value < qll->head->value) {
							/* ignore value if test isn't true */
							if (qll->sum1 >= qll->tau) {
								RASTER_DEBUGF(4, "skipping %f", value);
							}
							else {

								/* delete last element */
								RASTER_DEBUGF(4, "deleting %f from list", qll->tail->value);
								qle = qll->tail->prev;
								RASTER_DEBUGF(5, "to-be tail is %f with count %d", qle->value, qle->count);
								qle->count += qll->tail->count;
								quantile_llist_index_delete(qll, qll->tail);
								quantile_llist_delete(qll->tail);
								qll->tail = qle;
								qll->count--;
								RASTER_DEBUGF(5, "tail is %f with count %d", qll->tail->value, qll->tail->count);

								/* insert value */
								RASTER_DEBUGF(4, "adding %f to list", value);
								/* OPTIMIZATION: check to see if value exceeds last value */
								if (NULL != qll->tail && (value > qll->tail->value || FLT_EQ(value, qll->tail->value))) {
									idx = qll->count;
									qle = quantile_llist_insert(qll->tail, value, &idx);
								}
								/* OPTIMIZATION: use index if possible */
								else {
									qls = quantile_llist_index_search(qll, value, &idx);
									qle = quantile_llist_insert(qls, value, &idx);
								}
								if (NULL == qle) return NULL;
								RASTER_DEBUGF(5, "value added at index: %d => %f", idx, value);
								qll->count++;
								qll->sum1++;

								/* first element */
								if (NULL == qle->prev)
									qll->head = qle;
								/* last element */
								if (NULL == qle->next)
									qll->tail = qle;

								qll->sum2 = qll->sum1 - qll->head->count;

								quantile_llist_index_update(qll, qle, idx);

								RASTER_DEBUGF(5, "qle, head, tail = %p, %p, %p", qle, qll->head, qll->tail);

							}
						}
						else {
							qle = qll->tail;
							while (NULL != qle) {
								if (qle->value < value) {
									qle->count++;
									qll->sum1++;
									qll->sum2 = qll->sum1 - qll->head->count;
									RASTER_DEBUGF(4, "incremented count of %f by 1 to %d", qle->value, qle->count);
									break;
								}

								qle = qle->prev;
							}
						}
					}
					/* AL-GT */
					else {
						RASTER_DEBUGF(4, "value, tail->value = %f, %f", value, qll->tail->value);

						if (value > qll->tail->value) {
							/* ignore value if test isn't true */
							if (qll->sum1 >= qll->tau) {
								RASTER_DEBUGF(4, "skipping %f", value);
							}
							else {

								/* delete last element */
								RASTER_DEBUGF(4, "deleting %f from list", qll->head->value);
								qle = qll->head->next;
								RASTER_DEBUGF(5, "to-be tail is %f with count %d", qle->value, qle->count);
								qle->count += qll->head->count;
								quantile_llist_index_delete(qll, qll->head);
								quantile_llist_delete(qll->head);
								qll->head = qle;
								qll->count--;
								quantile_llist_index_update(qll, NULL, 0);
								RASTER_DEBUGF(5, "tail is %f with count %d", qll->head->value, qll->head->count);

								/* insert value */
								RASTER_DEBUGF(4, "adding %f to list", value);
								/* OPTIMIZATION: check to see if value exceeds last value */
								if (NULL != qll->tail && (value > qll->tail->value || FLT_EQ(value, qll->tail->value))) {
									idx = qll->count;
									qle = quantile_llist_insert(qll->tail, value, &idx);
								}
								/* OPTIMIZATION: use index if possible */
								else {
									qls = quantile_llist_index_search(qll, value, &idx);
									qle = quantile_llist_insert(qls, value, &idx);
								}
								if (NULL == qle) return NULL;
								RASTER_DEBUGF(5, "value added at index: %d => %f", idx, value);
								qll->count++;
								qll->sum1++;

								/* first element */
								if (NULL == qle->prev)
									qll->head = qle;
								/* last element */
								if (NULL == qle->next)
									qll->tail = qle;

								qll->sum2 = qll->sum1 - qll->tail->count;

								quantile_llist_index_update(qll, qle, idx);

								RASTER_DEBUGF(5, "qle, head, tail = %p, %p, %p", qle, qll->head, qll->tail);

							}
						}
						else {
							qle = qll->head;
							while (NULL != qle) {
								if (qle->value > value) {
									qle->count++;
									qll->sum1++;
									qll->sum2 = qll->sum1 - qll->tail->count;
									RASTER_DEBUGF(4, "incremented count of %f by 1 to %d", qle->value, qle->count);
									break;
								}

								qle = qle->next;
							}
						}
					}

					RASTER_DEBUGF(5, "sum2, tau = %d, %d", qll->sum2, qll->tau);
					if (qll->sum2 >= qll->tau) {
						/* AL-GEQ */
						if (qll->algeq) {
							RASTER_DEBUGF(4, "deleting first element %f from list", qll->head->value);

							if (NULL != qll->head->next) {
								qle = qll->head->next;
								qll->sum1 -= qll->head->count;
								qll->sum2 = qll->sum1 - qle->count;
								quantile_llist_index_delete(qll, qll->head);
								quantile_llist_delete(qll->head);
								qll->head = qle;
								qll->count--;

								quantile_llist_index_update(qll, NULL, 0);
							}
							else {
								quantile_llist_delete(qll->head);
								qll->head = NULL;
								qll->tail = NULL;
								qll->sum1 = 0;
								qll->sum2 = 0;
								qll->count = 0;

								quantile_llist_index_reset(qll);
							}
						}
						/* AL-GT */
						else {
							RASTER_DEBUGF(4, "deleting first element %f from list", qll->tail->value);

							if (NULL != qll->tail->prev) {
								qle = qll->tail->prev;
								qll->sum1 -= qll->tail->count;
								qll->sum2 = qll->sum1 - qle->count;
								quantile_llist_index_delete(qll, qll->tail);
								quantile_llist_delete(qll->tail);
								qll->tail = qle;
								qll->count--;
							}
							else {
								quantile_llist_delete(qll->tail);
								qll->head = NULL;
								qll->tail = NULL;
								qll->sum1 = 0;
								qll->sum2 = 0;
								qll->count = 0;

								quantile_llist_index_reset(qll);
							}
						}
					}

					RASTER_DEBUGF(5, "qll after: (algeq, quantile, count, tau, sum1, sum2) = (%d, %f, %d, %d, %d, %d)",
						qll->algeq, qll->quantile, qll->count, qll->tau, qll->sum1, qll->sum2);
					RASTER_DEBUGF(5, "qll after: (head, tail) = (%p, %p)\n\n", qll->head, qll->tail);
				}

			}
			else {
				RASTER_DEBUGF(5, "skipping value at (x, y) = (%d, %d)", x, y);
			}

			z++;
		}
	}

	/* process quantiles */
	*rtn_count = *qlls_count / 2;
	rtn = rtalloc(sizeof(struct rt_quantile_t) * *rtn_count);
	if (NULL == rtn) return NULL;

	RASTER_DEBUGF(3, "returning %d quantiles", *rtn_count);
	for (i = 0, k = 0; i < *qlls_count; i++) {
		qll = &((*qlls)[i]);

		exists = 0;
		for (x = 0; x < k; x++) {
			if (FLT_EQ(qll->quantile, rtn[x].quantile)) {
				exists = 1;
				break;
			}
		}
		if (exists) continue;

		RASTER_DEBUGF(5, "qll: (algeq, quantile, count, tau, sum1, sum2) = (%d, %f, %d, %d, %d, %d)",
			qll->algeq, qll->quantile, qll->count, qll->tau, qll->sum1, qll->sum2);
		RASTER_DEBUGF(5, "qll: (head, tail) = (%p, %p)", qll->head, qll->tail);

		rtn[k].quantile = qll->quantile;
		rtn[k].has_value = 0;

		/* check that qll->head and qll->tail have value */
		if (qll->head == NULL || qll->tail == NULL)
			continue;

		/* AL-GEQ */
		if (qll->algeq)
			qle = qll->head;
		/* AM-GT */
		else
			qle = qll->tail;

		exists = 0;
		for (j = i + 1; j < *qlls_count; j++) {
			if (FLT_EQ((*qlls)[j].quantile, qll->quantile)) {

				RASTER_DEBUGF(5, "qlls[%d]: (algeq, quantile, count, tau, sum1, sum2) = (%d, %f, %d, %d, %d, %d)",
					j, (*qlls)[j].algeq, (*qlls)[j].quantile, (*qlls)[j].count, (*qlls)[j].tau, (*qlls)[j].sum1, (*qlls)[j].sum2);
				RASTER_DEBUGF(5, "qlls[%d]: (head, tail) = (%p, %p)", j, (*qlls)[j].head, (*qlls)[j].tail);

				exists = 1;
				break;
			}
		}

		/* weighted average for quantile */
		if (exists) {
			if ((*qlls)[j].algeq) {
				rtn[k].value = ((qle->value * qle->count) + ((*qlls)[j].head->value * (*qlls)[j].head->count)) / (qle->count + (*qlls)[j].head->count);
				RASTER_DEBUGF(5, "qlls[%d].head: (value, count) = (%f, %d)", j, (*qlls)[j].head->value, (*qlls)[j].head->count);
			}
			else {
				rtn[k].value = ((qle->value * qle->count) + ((*qlls)[j].tail->value * (*qlls)[j].tail->count)) / (qle->count + (*qlls)[j].tail->count);
				RASTER_DEBUGF(5, "qlls[%d].tail: (value, count) = (%f, %d)", j, (*qlls)[j].tail->value, (*qlls)[j].tail->count);
			}
		}
		/* straight value for quantile */
		else {
			rtn[k].value = qle->value;
		}
		rtn[k].has_value = 1;
		RASTER_DEBUGF(3, "(quantile, value) = (%f, %f)\n\n", rtn[k].quantile, rtn[k].value);

		k++;
	}

	RASTER_DEBUG(3, "done");
	return rtn;
}

/******************************************************************************
* rt_band_get_value_count()
******************************************************************************/

/**
 * Count the number of times provided value(s) occur in
 * the band
 *
 * @param band : the band to query for minimum and maximum pixel values
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * @param search_values : array of values to count
 * @param search_values_count : the number of search values
 * @param roundto : the decimal place to round the values to
 * @param rtn_total : the number of pixels examined in the band
 * @param rtn_count : the number of value counts being returned
 *
 * @return the number of times the provide value(s) occur or NULL
 */
rt_valuecount
rt_band_get_value_count(
	rt_band band, int exclude_nodata_value,
	double *search_values, uint32_t search_values_count, double roundto,
	uint32_t *rtn_total, uint32_t *rtn_count
) {
	rt_valuecount vcnts = NULL;
	rt_pixtype pixtype = PT_END;
	uint8_t *data = NULL;
	double nodata = 0;

	int scale = 0;
	int doround = 0;
	double tmpd = 0;
	int i = 0;

	uint32_t x = 0;
	uint32_t y = 0;
	int rtn;
	double pxlval;
	int isnodata = 0;
	double rpxlval;
	uint32_t total = 0;
	int vcnts_count = 0;
	int new_valuecount = 0;

#if POSTGIS_DEBUG_LEVEL > 0
	clock_t start, stop;
	double elapsed = 0;
#endif

	RASTER_DEBUG(3, "starting");
#if POSTGIS_DEBUG_LEVEL > 0
	start = clock();
#endif

	assert(NULL != band);
	assert(NULL != rtn_count);

	data = rt_band_get_data(band);
	if (data == NULL) {
		rterror("rt_band_get_summary_stats: Cannot get band data");
		return NULL;
	}

	pixtype = band->pixtype;

	if (rt_band_get_hasnodata_flag(band)) {
		rt_band_get_nodata(band, &nodata);
		RASTER_DEBUGF(3, "hasnodata, nodataval = 1, %f", nodata);
	}
	else {
		exclude_nodata_value = 0;
		RASTER_DEBUG(3, "hasnodata, nodataval = 0, 0");
	}

	RASTER_DEBUGF(3, "exclude_nodata_value = %d", exclude_nodata_value);

	/* process roundto */
	if (roundto < 0 || FLT_EQ(roundto, 0.0)) {
		roundto = 0;
		scale = 0;
	}
	/* tenths, hundredths, thousandths, etc */
	else if (roundto < 1) {
    switch (pixtype) {
			/* integer band types don't have digits after the decimal place */
			case PT_1BB:
			case PT_2BUI:
			case PT_4BUI:
			case PT_8BSI:
			case PT_8BUI:
			case PT_16BSI:
			case PT_16BUI:
			case PT_32BSI:
			case PT_32BUI:
				roundto = 0;
				break;
			/* floating points, check the rounding */
			case PT_32BF:
			case PT_64BF:
				for (scale = 0; scale <= 20; scale++) {
					tmpd = roundto * pow(10, scale);
					if (FLT_EQ((tmpd - ((int) tmpd)), 0.0)) break;
				}
				break;
			case PT_END:
				break;
		}
	}
	/* ones, tens, hundreds, etc */
	else {
		for (scale = 0; scale >= -20; scale--) {
			tmpd = roundto * pow(10, scale);
			if (tmpd < 1 || FLT_EQ(tmpd, 1.0)) {
				if (scale == 0) doround = 1;
				break;
			}
		}
	}

	if (scale != 0 || doround)
		doround = 1;
	else
		doround = 0;
	RASTER_DEBUGF(3, "scale = %d", scale);
	RASTER_DEBUGF(3, "doround = %d", doround);

	/* process search_values */
	if (search_values_count > 0 && NULL != search_values) {
		vcnts = (rt_valuecount) rtalloc(sizeof(struct rt_valuecount_t) * search_values_count);
		if (NULL == vcnts) {
			rterror("rt_band_get_count_of_values: Could not allocate memory for value counts");
			*rtn_count = 0;
			return NULL;
		}

		for (i = 0; i < search_values_count; i++) {
			vcnts[i].count = 0;
			vcnts[i].percent = 0;
			if (!doround)
				vcnts[i].value = search_values[i];
			else
				vcnts[i].value = ROUND(search_values[i], scale);
		}
		vcnts_count = i;
	}
	else
		search_values_count = 0;
	RASTER_DEBUGF(3, "search_values_count = %d", search_values_count);

	/* entire band is nodata */
	if (rt_band_get_isnodata_flag(band) != FALSE) {
		if (exclude_nodata_value) {
			rtwarn("All pixels of band have the NODATA value");
			return NULL;
		}
		else {
			if (search_values_count > 0) {
				/* check for nodata match */
				for (i = 0; i < search_values_count; i++) {
					if (!doround)
						tmpd = nodata;
					else
						tmpd = ROUND(nodata, scale);

					if (FLT_NEQ(tmpd, vcnts[i].value))
						continue;

					vcnts[i].count = band->width * band->height;
					if (NULL != rtn_total) *rtn_total = vcnts[i].count;
					vcnts->percent = 1.0;
				}

				*rtn_count = vcnts_count;
			}
			/* no defined search values */
			else {
				vcnts = (rt_valuecount) rtalloc(sizeof(struct rt_valuecount_t));
				if (NULL == vcnts) {
					rterror("rt_band_get_count_of_values: Could not allocate memory for value counts");
					*rtn_count = 0;
					return NULL;
				}

				vcnts->value = nodata;
				vcnts->count = band->width * band->height;
				if (NULL != rtn_total) *rtn_total = vcnts[i].count;
				vcnts->percent = 1.0;

				*rtn_count = 1;
			}

			return vcnts;
		}
	}

	for (x = 0; x < band->width; x++) {
		for (y = 0; y < band->height; y++) {
			rtn = rt_band_get_pixel(band, x, y, &pxlval, &isnodata);

			/* error getting value, continue */
			if (rtn != ES_NONE)
				continue;

			if (!exclude_nodata_value || (exclude_nodata_value && !isnodata)) {
				total++;
				if (doround) {
					rpxlval = ROUND(pxlval, scale);
				}
				else
					rpxlval = pxlval;
				RASTER_DEBUGF(5, "(pxlval, rpxlval) => (%0.6f, %0.6f)", pxlval, rpxlval);

				new_valuecount = 1;
				/* search for match in existing valuecounts */
				for (i = 0; i < vcnts_count; i++) {
					/* match found */
					if (FLT_EQ(vcnts[i].value, rpxlval)) {
						vcnts[i].count++;
						new_valuecount = 0;
						RASTER_DEBUGF(5, "(value, count) => (%0.6f, %d)", vcnts[i].value, vcnts[i].count);
						break;
					}
				}

				/*
					don't add new valuecount either because
						- no need for new one
						- user-defined search values
				*/
				if (!new_valuecount || search_values_count > 0) continue;

				/* add new valuecount */
				vcnts = rtrealloc(vcnts, sizeof(struct rt_valuecount_t) * (vcnts_count + 1));
				if (NULL == vcnts) {
					rterror("rt_band_get_count_of_values: Could not allocate memory for value counts");
					*rtn_count = 0;
					return NULL;
				}

				vcnts[vcnts_count].value = rpxlval;
				vcnts[vcnts_count].count = 1;
				vcnts[vcnts_count].percent = 0;
				RASTER_DEBUGF(5, "(value, count) => (%0.6f, %d)", vcnts[vcnts_count].value, vcnts[vcnts_count].count);
				vcnts_count++;
			}
		}
	}

#if POSTGIS_DEBUG_LEVEL > 0
	stop = clock();
	elapsed = ((double) (stop - start)) / CLOCKS_PER_SEC;
	RASTER_DEBUGF(3, "elapsed time = %0.4f", elapsed);
#endif

	for (i = 0; i < vcnts_count; i++) {
		vcnts[i].percent = (double) vcnts[i].count / total;
		RASTER_DEBUGF(5, "(value, count) => (%0.6f, %d)", vcnts[i].value, vcnts[i].count);
	}

	RASTER_DEBUG(3, "done");
	if (NULL != rtn_total) *rtn_total = total;
	*rtn_count = vcnts_count;
	return vcnts;
}
