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

/******************************************************************************
* rt_band_reclass()
******************************************************************************/

/**
 * Returns new band with values reclassified
 *
 * @param srcband : the band who's values will be reclassified
 * @param pixtype : pixel type of the new band
 * @param hasnodata : indicates if the band has a nodata value
 * @param nodataval : nodata value for the new band
 * @param exprset : array of rt_reclassexpr structs
 * @param exprcount : number of elements in expr
 *
 * @return a new rt_band or NULL on error
 */
rt_band
rt_band_reclass(
	rt_band srcband, rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	rt_reclassexpr *exprset, int exprcount
) {
	rt_band band = NULL;
	uint32_t width = 0;
	uint32_t height = 0;
	int numval = 0;
	int memsize = 0;
	void *mem = NULL;
	uint32_t src_hasnodata = 0;
	double src_nodataval = 0.0;
	int isnodata = 0;

	int rtn;
	uint32_t x;
	uint32_t y;
	int i;
	double or = 0;
	double ov = 0;
	double nr = 0;
	double nv = 0;
	int do_nv = 0;
	rt_reclassexpr expr = NULL;

	assert(NULL != srcband);
	assert(NULL != exprset && exprcount > 0);
	RASTER_DEBUGF(4, "exprcount = %d", exprcount);
	RASTER_DEBUGF(4, "exprset @ %p", exprset);

	/* source nodata */
	src_hasnodata = rt_band_get_hasnodata_flag(srcband);
	if (src_hasnodata)
		rt_band_get_nodata(srcband, &src_nodataval);

	/* size of memory block to allocate */
	width = rt_band_get_width(srcband);
	height = rt_band_get_height(srcband);
	numval = width * height;
	memsize = rt_pixtype_size(pixtype) * numval;
	mem = (int *) rtalloc(memsize);
	if (!mem) {
		rterror("rt_band_reclass: Could not allocate memory for band");
		return 0;
	}

	/* initialize to zero */
	if (!hasnodata) {
		memset(mem, 0, memsize);
	}
	/* initialize to nodataval */
	else {
		int32_t checkvalint = 0;
		uint32_t checkvaluint = 0;
		double checkvaldouble = 0;
		float checkvalfloat = 0;

		switch (pixtype) {
			case PT_1BB:
			{
				uint8_t *ptr = mem;
				uint8_t clamped_initval = rt_util_clamp_to_1BB(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_2BUI:
			{
				uint8_t *ptr = mem;
				uint8_t clamped_initval = rt_util_clamp_to_2BUI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_4BUI:
			{
				uint8_t *ptr = mem;
				uint8_t clamped_initval = rt_util_clamp_to_4BUI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_8BSI:
			{
				int8_t *ptr = mem;
				int8_t clamped_initval = rt_util_clamp_to_8BSI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_8BUI:
			{
				uint8_t *ptr = mem;
				uint8_t clamped_initval = rt_util_clamp_to_8BUI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_16BSI:
			{
				int16_t *ptr = mem;
				int16_t clamped_initval = rt_util_clamp_to_16BSI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_16BUI:
			{
				uint16_t *ptr = mem;
				uint16_t clamped_initval = rt_util_clamp_to_16BUI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_32BSI:
			{
				int32_t *ptr = mem;
				int32_t clamped_initval = rt_util_clamp_to_32BSI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalint = ptr[0];
				break;
			}
			case PT_32BUI:
			{
				uint32_t *ptr = mem;
				uint32_t clamped_initval = rt_util_clamp_to_32BUI(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvaluint = ptr[0];
				break;
			}
			case PT_32BF:
			{
				float *ptr = mem;
				float clamped_initval = rt_util_clamp_to_32F(nodataval);
				for (i = 0; i < numval; i++)
					ptr[i] = clamped_initval;
				checkvalfloat = ptr[0];
				break;
			}
			case PT_64BF:
			{
				double *ptr = mem;
				for (i = 0; i < numval; i++)
					ptr[i] = nodataval;
				checkvaldouble = ptr[0];
				break;
			}
			default:
			{
				rterror("rt_band_reclass: Unknown pixeltype %d", pixtype);
				rtdealloc(mem);
				return 0;
			}
		}

		/* Overflow checking */
		rt_util_dbl_trunc_warning(
			nodataval,
			checkvalint, checkvaluint,
			checkvalfloat, checkvaldouble,
			pixtype
		);
	}
	RASTER_DEBUGF(3, "rt_band_reclass: width = %d height = %d", width, height);

	band = rt_band_new_inline(width, height, pixtype, hasnodata, nodataval, mem);
	if (!band) {
		rterror("rt_band_reclass: Could not create new band");
		rtdealloc(mem);
		return 0;
	}
	rt_band_set_ownsdata_flag(band, 1); /* we DO own this data!!! */
	RASTER_DEBUGF(3, "rt_band_reclass: new band @ %p", band);

	for (x = 0; x < width; x++) {
		for (y = 0; y < height; y++) {
			rtn = rt_band_get_pixel(srcband, x, y, &ov, &isnodata);

			/* error getting value, skip */
			if (rtn != ES_NONE) {
				RASTER_DEBUGF(3, "Cannot get value at %d, %d", x, y);
				continue;
			}
			RASTER_DEBUGF(4, "(x, y, ov, isnodata) = (%d, %d, %f, %d)", x, y, ov, isnodata);

			do {
				do_nv = 0;

				/* no data*/
				if (hasnodata && isnodata) {
					do_nv = 1;
					break;
				}

				for (i = 0; i < exprcount; i++) {
					expr = exprset[i];

					/* ov matches min and max*/
					if (
						FLT_EQ(expr->src.min, ov) &&
						FLT_EQ(expr->src.max, ov)
					) {
						do_nv = 1;
						break;
					}

					/* process min */
					if ((
						expr->src.exc_min && (
							expr->src.min > ov ||
							FLT_EQ(expr->src.min, ov)
						)) || (
						expr->src.inc_min && (
							expr->src.min < ov ||
							FLT_EQ(expr->src.min, ov)
						)) || (
						expr->src.min < ov
					)) {
						/* process max */
						if ((
							expr->src.exc_max && (
								ov > expr->src.max ||
								FLT_EQ(expr->src.max, ov)
							)) || (
								expr->src.inc_max && (
								ov < expr->src.max ||
								FLT_EQ(expr->src.max, ov)
							)) || (
							ov < expr->src.max
						)) {
							do_nv = 1;
							break;
						}
					}
				}
			}
			while (0);

			/* no expression matched, do not continue */
			if (!do_nv) continue;
			RASTER_DEBUGF(3, "Using exprset[%d] unless NODATA", i);

			/* converting a value from one range to another range
			OldRange = (OldMax - OldMin)
			NewRange = (NewMax - NewMin)
			NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
			*/

			/* NODATA */
			if (hasnodata && isnodata) {
				nv = nodataval;
			}
			/*
				"src" min and max is the same, prevent division by zero
				set nv to "dst" min, which should be the same as "dst" max
			*/
			else if (FLT_EQ(expr->src.max, expr->src.min)) {
				nv = expr->dst.min;
			}
			else {
				or = expr->src.max - expr->src.min;
				nr = expr->dst.max - expr->dst.min;
				nv = (((ov - expr->src.min) * nr) / or) + expr->dst.min;

				/* if dst range is from high to low */
				if (expr->dst.min > expr->dst.max) {
					if (nv > expr->dst.min)
						nv = expr->dst.min;
					else if (nv < expr->dst.max)
						nv = expr->dst.max;
				}
				/* if dst range is from low to high */
				else {
					if (nv < expr->dst.min)
						nv = expr->dst.min;
					else if (nv > expr->dst.max)
						nv = expr->dst.max;
				}
			}

			/* round the value for integers */
			switch (pixtype) {
				case PT_1BB:
				case PT_2BUI:
				case PT_4BUI:
				case PT_8BSI:
				case PT_8BUI:
				case PT_16BSI:
				case PT_16BUI:
				case PT_32BSI:
				case PT_32BUI:
					nv = round(nv);
					break;
				default:
					break;
			}

			RASTER_DEBUGF(4, "(%d, %d) ov: %f or: %f - %f nr: %f - %f nv: %f"
				, x
				, y
				, ov
				, (NULL != expr) ? expr->src.min : 0
				, (NULL != expr) ? expr->src.max : 0
				, (NULL != expr) ? expr->dst.min : 0
				, (NULL != expr) ? expr->dst.max : 0
				, nv
			);
			if (rt_band_set_pixel(band, x, y, nv, NULL) != ES_NONE) {
				rterror("rt_band_reclass: Could not assign value to new band");
				rt_band_destroy(band);
				rtdealloc(mem);
				return 0;
			}

			expr = NULL;
		}
	}

	return band;
}

/******************************************************************************
* rt_raster_iterator()
******************************************************************************/

typedef struct _rti_iterator_arg_t* _rti_iterator_arg;
struct _rti_iterator_arg_t {
	int count;

	rt_raster *raster;
	int *isempty;
	double **offset;
	int *width;
	int *height;

	struct {
		rt_band *rtband;
		int *hasnodata;
		int *isnodata;
		double *nodataval;
		double *minval;
	} band;

	struct {
		uint16_t x;
		uint16_t y;
	} distance;

	struct {
		uint32_t rows;
		uint32_t columns;
	} dimension;

	struct {
		double **values;
		int **nodata;
	} empty;

	rt_iterator_arg arg;
};

static _rti_iterator_arg
_rti_iterator_arg_init() {
	_rti_iterator_arg _param;

	_param = rtalloc(sizeof(struct _rti_iterator_arg_t));
	if (_param == NULL) {
		rterror("_rti_iterator_arg_init: Could not allocate memory for _rti_iterator_arg");
		return NULL;
	}

	_param->count = 0;

	_param->raster = NULL;
	_param->isempty = NULL;
	_param->offset = NULL;
	_param->width = NULL;
	_param->height = NULL;

	_param->band.rtband = NULL;
	_param->band.hasnodata = NULL;
	_param->band.isnodata = NULL;
	_param->band.nodataval = NULL;
	_param->band.minval = NULL;

	_param->distance.x = 0;
	_param->distance.y = 0;

	_param->dimension.rows = 0;
	_param->dimension.columns = 0;

	_param->empty.values = NULL;
	_param->empty.nodata = NULL;

	_param->arg = NULL;

	return _param;
}

static void
_rti_iterator_arg_destroy(_rti_iterator_arg _param) {
	int i = 0;

	if (_param->raster != NULL)
		rtdealloc(_param->raster);
	if (_param->isempty != NULL)
		rtdealloc(_param->isempty);
	if (_param->width != NULL)
		rtdealloc(_param->width);
	if (_param->height != NULL)
		rtdealloc(_param->height);

	if (_param->band.rtband != NULL)
		rtdealloc(_param->band.rtband);
	if (_param->band.hasnodata != NULL)
		rtdealloc(_param->band.hasnodata);
	if (_param->band.isnodata != NULL)
		rtdealloc(_param->band.isnodata);
	if (_param->band.nodataval != NULL)
		rtdealloc(_param->band.nodataval);
	if (_param->band.minval != NULL)
		rtdealloc(_param->band.minval);

	if (_param->offset != NULL) {
		for (i = 0; i < _param->count; i++) {
			if (_param->offset[i] == NULL)
				continue;
			rtdealloc(_param->offset[i]);
		}
		rtdealloc(_param->offset);
	}

	if (_param->empty.values != NULL) {
		for (i = 0; i < _param->dimension.rows; i++) {
			if (_param->empty.values[i] == NULL)
				continue;
			rtdealloc(_param->empty.values[i]);
		}
		rtdealloc(_param->empty.values);
	}
	if (_param->empty.nodata != NULL) {
		for (i = 0; i < _param->dimension.rows; i++) {
			if (_param->empty.nodata[i] == NULL)
				continue;
			rtdealloc(_param->empty.nodata[i]);
		}
		rtdealloc(_param->empty.nodata);
	}

	if (_param->arg != NULL) {
		if (_param->arg->values != NULL)
			rtdealloc(_param->arg->values);
		if (_param->arg->nodata != NULL)
			rtdealloc(_param->arg->nodata);
		if (_param->arg->src_pixel != NULL) {
			for (i = 0; i < _param->count; i++) {
				if (_param->arg->src_pixel[i] == NULL)
					continue;
				rtdealloc(_param->arg->src_pixel[i]);
			}

			rtdealloc(_param->arg->src_pixel);
		}

		rtdealloc(_param->arg);
	}

	rtdealloc(_param);
}

static int
_rti_iterator_arg_populate(
	_rti_iterator_arg _param,
	rt_iterator itrset, uint16_t itrcount,
	uint16_t distancex, uint16_t distancey,
	int *allnull, int *allempty
) {
	int i = 0;
	int hasband = 0;

	_param->count = itrcount;
	_param->distance.x = distancex;
	_param->distance.y = distancey;
	_param->dimension.columns = distancex * 2 + 1;
	_param->dimension.rows = distancey * 2 + 1;

	/* allocate memory for children */
	_param->raster = rtalloc(sizeof(rt_raster) * itrcount);
	_param->isempty = rtalloc(sizeof(int) * itrcount);
	_param->width = rtalloc(sizeof(int) * itrcount);
	_param->height = rtalloc(sizeof(int) * itrcount);

	_param->offset = rtalloc(sizeof(double *) * itrcount);

	_param->band.rtband = rtalloc(sizeof(rt_band) * itrcount);
	_param->band.hasnodata = rtalloc(sizeof(int) * itrcount);
	_param->band.isnodata = rtalloc(sizeof(int) * itrcount);
	_param->band.nodataval = rtalloc(sizeof(double) * itrcount);
	_param->band.minval = rtalloc(sizeof(double) * itrcount);

	if (
		_param->raster == NULL ||
		_param->isempty == NULL ||
		_param->width == NULL ||
		_param->height == NULL ||
		_param->offset == NULL ||
		_param->band.rtband == NULL ||
		_param->band.hasnodata == NULL ||
		_param->band.isnodata == NULL ||
		_param->band.nodataval == NULL ||
		_param->band.minval == NULL
	) {
		rterror("_rti_iterator_arg_populate: Could not allocate memory for children of _rti_iterator_arg");
		return 0;
	}

	*allnull = 0;
	*allempty = 0;

	/*
		check input rasters
			not empty, band # is valid
			copy raster pointers and set flags
	*/
	for (i = 0; i < itrcount; i++) {
		/* initialize elements */
		_param->raster[i] = NULL;
		_param->isempty[i] = 0;
		_param->width[i] = 0;
		_param->height[i] = 0;

		_param->offset[i] = NULL;

		_param->band.rtband[i] = NULL;
		_param->band.hasnodata[i] = 0;
		_param->band.isnodata[i] = 0;
		_param->band.nodataval[i] = 0;
		_param->band.minval[i] = 0;

		/* set isempty */
		if (itrset[i].raster == NULL) {
			_param->isempty[i] = 1;

			(*allnull)++;
			(*allempty)++;

			continue;
		}
		else if (rt_raster_is_empty(itrset[i].raster)) {
			_param->isempty[i] = 1;

			(*allempty)++;

			continue;
		}

		/* check band number */
		hasband = rt_raster_has_band(itrset[i].raster, itrset[i].nband);
		if (!hasband) {
			if (!itrset[i].nbnodata) {
				rterror("_rti_iterator_arg_populate: Band %d not found for raster %d", itrset[i].nband, i);
				return 0;
			}
			else {
				RASTER_DEBUGF(4, "Band %d not found for raster %d. Using NODATA", itrset[i].nband, i);
			}
		}

		_param->raster[i] = itrset[i].raster;
		if (hasband) {
			_param->band.rtband[i] = rt_raster_get_band(itrset[i].raster, itrset[i].nband);
			if (_param->band.rtband[i] == NULL) {
				rterror("_rti_iterator_arg_populate: Could not get band %d for raster %d", itrset[i].nband, i);
				return 0;
			}

			/* hasnodata */
			_param->band.hasnodata[i] = rt_band_get_hasnodata_flag(_param->band.rtband[i]);

			/* hasnodata = TRUE */
			if (_param->band.hasnodata[i]) {
				/* nodataval */
				rt_band_get_nodata(_param->band.rtband[i], &(_param->band.nodataval[i]));

				/* isnodata */
				_param->band.isnodata[i] = rt_band_get_isnodata_flag(_param->band.rtband[i]);
			}
			/* hasnodata = FALSE */
			else {
				/* minval */
				_param->band.minval[i] = rt_band_get_min_value(_param->band.rtband[i]);
			}
		}

		/* width, height */
		_param->width[i] = rt_raster_get_width(_param->raster[i]);
		_param->height[i] = rt_raster_get_height(_param->raster[i]);

		/* init offset */
		_param->offset[i] = rtalloc(sizeof(double) * 2);
		if (_param->offset[i] == NULL) {
			rterror("_rti_iterator_arg_populate: Could not allocate memory for offsets");
			return 0;
		}
	}

	return 1;
}

static int
_rti_iterator_arg_empty_init(_rti_iterator_arg _param) {
	int x = 0;
	int y = 0;

	_param->empty.values = rtalloc(sizeof(double *) * _param->dimension.rows);
	_param->empty.nodata = rtalloc(sizeof(int *) * _param->dimension.rows);
	if (_param->empty.values == NULL || _param->empty.nodata == NULL) {
		rterror("_rti_iterator_arg_empty_init: Could not allocate memory for empty values and NODATA");
		return 0;
	}

	for (y = 0; y < _param->dimension.rows; y++) {
		_param->empty.values[y] = rtalloc(sizeof(double) * _param->dimension.columns);
		_param->empty.nodata[y] = rtalloc(sizeof(int) * _param->dimension.columns);

		if (_param->empty.values[y] == NULL || _param->empty.nodata[y] == NULL) {
			rterror("_rti_iterator_arg_empty_init: Could not allocate memory for elements of empty values and NODATA");
			return 0;
		}

		for (x = 0; x < _param->dimension.columns; x++) {
			_param->empty.values[y][x] = 0;
			_param->empty.nodata[y][x] = 1;
		}
	}

	return 1;
}

static int
_rti_iterator_arg_callback_init(_rti_iterator_arg _param) {
	int i = 0;

	_param->arg = rtalloc(sizeof(struct rt_iterator_arg_t));
	if (_param->arg == NULL) {
		rterror("_rti_iterator_arg_callback_init: Could not allocate memory for rt_iterator_arg");
		return 0;
	}

	_param->arg->values = NULL;
	_param->arg->nodata = NULL;
	_param->arg->src_pixel = NULL;

	/* initialize argument components */
	_param->arg->values = rtalloc(sizeof(double **) * _param->count);
	_param->arg->nodata = rtalloc(sizeof(int **) * _param->count);
	_param->arg->src_pixel = rtalloc(sizeof(int *) * _param->count);
	if (_param->arg->values == NULL || _param->arg->nodata == NULL || _param->arg->src_pixel == NULL) {
		rterror("_rti_iterator_arg_callback_init: Could not allocate memory for element of rt_iterator_arg");
		return 0;
	}
	memset(_param->arg->values, 0, sizeof(double **) * _param->count);
	memset(_param->arg->nodata, 0, sizeof(int **) * _param->count);

	/* initialize pos */
	for (i = 0; i < _param->count; i++) {

		_param->arg->src_pixel[i] = rtalloc(sizeof(int) * 2);
		if (_param->arg->src_pixel[i] == NULL) {
			rterror("_rti_iterator_arg_callback_init: Could not allocate memory for position elements of rt_iterator_arg");
			return 0;
		}
		memset(_param->arg->src_pixel[i], 0, sizeof(int) * 2);
	}

	_param->arg->rasters = _param->count;
	_param->arg->rows = _param->dimension.rows;
	_param->arg->columns = _param->dimension.columns;

	_param->arg->dst_pixel[0] = 0;
	_param->arg->dst_pixel[1] = 0;

	return 1;
}

static void
_rti_iterator_arg_callback_clean(_rti_iterator_arg _param) {
	int i = 0;
	int y = 0;

	for (i = 0; i < _param->count; i++) {
		RASTER_DEBUGF(5, "empty at @ %p", _param->empty.values);
		RASTER_DEBUGF(5, "values at @ %p", _param->arg->values[i]);

		if (_param->arg->values[i] == _param->empty.values) {
			_param->arg->values[i] = NULL;
			_param->arg->nodata[i] = NULL;

			continue;
		}

		for (y = 0; y < _param->dimension.rows; y++) {
			rtdealloc(_param->arg->values[i][y]);
			rtdealloc(_param->arg->nodata[i][y]);
		}

		rtdealloc(_param->arg->values[i]);
		rtdealloc(_param->arg->nodata[i]);

		_param->arg->values[i] = NULL;
		_param->arg->nodata[i] = NULL;
	}
}

/**
 * n-raster iterator.
 * The raster returned should be freed by the caller
 *
 * @param itrset : set of rt_iterator objects.
 * @param itrcount : number of objects in itrset.
 * @param extenttype : type of extent for the output raster.
 * @param customextent : raster specifying custom extent.
 * is only used if extenttype is ET_CUSTOM.
 * @param pixtype : the desired pixel type of the output raster's band.
 * @param hasnodata : indicates if the band has nodata value
 * @param nodataval : the nodata value, will be appropriately
 * truncated to fit the pixtype size.
 * @param distancex : the number of pixels around the specified pixel
 * along the X axis
 * @param distancey : the number of pixels around the specified pixel
 * along the Y axis
 * @param mask : the object of mask
 * @param userarg : pointer to any argument that is passed as-is to callback.
 * @param callback : callback function for actual processing of pixel values.
 * @param *rtnraster : return one band raster from iterator process
 *
 * The callback function _must_ have the following signature.
 *
 *    int FNAME(rt_iterator_arg arg, void *userarg, double *value, int *nodata)
 *
 * The callback function _must_ return zero (error) or non-zero (success)
 * indicating whether the function ran successfully.
 * The parameters passed to the callback function are as follows.
 *
 * - rt_iterator_arg arg: struct containing pixel values, NODATA flags and metadata
 * - void *userarg: NULL or calling function provides to rt_raster_iterator() for use by callback function
 * - double *value: value of pixel to be burned by rt_raster_iterator()
 * - int *nodata: flag (0 or 1) indicating that pixel to be burned is NODATA
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate
rt_raster_iterator(
	rt_iterator itrset, uint16_t itrcount,
	rt_extenttype extenttype, rt_raster customextent,
	rt_pixtype pixtype,
	uint8_t hasnodata, double nodataval,
	uint16_t distancex, uint16_t distancey,
	rt_mask mask,
	void *userarg,
	int (*callback)(
		rt_iterator_arg arg,
		void *userarg,
		double *value,
		int *nodata
	),
	rt_raster *rtnraster
) {
	/* output raster */
	rt_raster rtnrast = NULL;
	/* output raster's band */
	rt_band rtnband = NULL;

	/* working raster */
	rt_raster rast = NULL;

	_rti_iterator_arg _param = NULL;
	int allnull = 0;
	int allempty = 0;
	int aligned = 0;
	double offset[4] = {0.};
	rt_pixel npixels;

	int i = 0;
	int status = 0;
	int inextent = 0;
	int x = 0;
	int y = 0;
	int _x = 0;
	int _y = 0;

	int _width = 0;
	int _height = 0;

	double minval;
	double value;
	int isnodata;
	int nodata;

	RASTER_DEBUG(3, "Starting...");

	assert(itrset != NULL && itrcount > 0);
	assert(rtnraster != NULL);

	/* init rtnraster to NULL */
	*rtnraster = NULL;

	/* check that callback function is not NULL */
	if (callback == NULL) {
		rterror("rt_raster_iterator: Callback function not provided");
		return ES_ERROR;
	}

	/* check that custom extent is provided if extenttype = ET_CUSTOM */
	if (extenttype == ET_CUSTOM && rt_raster_is_empty(customextent)) {
		rterror("rt_raster_iterator: Custom extent cannot be empty if extent type is ET_CUSTOM");
		return ES_ERROR;
	}

	/* check that pixtype != PT_END */
	if (pixtype == PT_END) {
		rterror("rt_raster_iterator: Pixel type cannot be PT_END");
		return ES_ERROR;
	}

	/* initialize _param */
	if ((_param = _rti_iterator_arg_init()) == NULL) {
		rterror("rt_raster_iterator: Could not initialize internal variables");
		return ES_ERROR;
	}

	/* fill _param */
	if (!_rti_iterator_arg_populate(_param, itrset, itrcount, distancex, distancey, &allnull, &allempty)) {
		rterror("rt_raster_iterator: Could not populate for internal variables");
		_rti_iterator_arg_destroy(_param);
		return ES_ERROR;
	}

	/* shortcut if all null, return NULL */
	if (allnull == itrcount) {
		RASTER_DEBUG(3, "all rasters are NULL, returning NULL");

		_rti_iterator_arg_destroy(_param);

		return ES_NONE;
	}
	/* shortcut if all empty, return empty raster */
	else if (allempty == itrcount) {
		RASTER_DEBUG(3, "all rasters are empty, returning empty raster");

		_rti_iterator_arg_destroy(_param);

		rtnrast = rt_raster_new(0, 0);
		if (rtnrast == NULL) {
			rterror("rt_raster_iterator: Could not create empty raster");
			return ES_ERROR;
		}
		rt_raster_set_scale(rtnrast, 0, 0);

		*rtnraster = rtnrast;
		return ES_NONE;
	}

	/* check that all rasters are aligned */
	RASTER_DEBUG(3, "checking alignment of all rasters");
	rast = NULL;

	/* find raster to use as reference */
	/* use custom if provided */
	if (extenttype == ET_CUSTOM) {
		RASTER_DEBUG(4, "using custom extent as reference raster");
		rast = customextent;
	}
	/* use first valid one in _param->raster */
	else {
		for (i = 0; i < itrcount; i++) {
			if (!_param->isempty[i]) {
				RASTER_DEBUGF(4, "using raster at index %d as reference raster", i);
				rast = _param->raster[i];
				break;
			}
		}
	}

	/* no rasters found, SHOULD NEVER BE HERE! */
	if (rast == NULL) {
		rterror("rt_raster_iterator: Could not find reference raster to use for alignment tests");

		_rti_iterator_arg_destroy(_param);

		return ES_ERROR;
	}

	do {
		aligned = 1;

		/* check custom first if set. also skip if rasters are the same */
		if (extenttype == ET_CUSTOM && rast != customextent) {
			if (rt_raster_same_alignment(rast, customextent, &aligned, NULL) != ES_NONE) {
				rterror("rt_raster_iterator: Could not test for alignment between reference raster and custom extent");

				_rti_iterator_arg_destroy(_param);

				return ES_ERROR;
			}

			RASTER_DEBUGF(5, "custom extent alignment: %d", aligned);
			if (!aligned)
				break;
		}

		for (i = 0; i < itrcount; i++) {
			/* skip NULL rasters and if rasters are the same */
			if (_param->isempty[i] || rast == _param->raster[i])
				continue;

			if (rt_raster_same_alignment(rast, _param->raster[i], &aligned, NULL) != ES_NONE) {
				rterror("rt_raster_iterator: Could not test for alignment between reference raster and raster %d", i);

				_rti_iterator_arg_destroy(_param);

				return ES_ERROR;
			}
			RASTER_DEBUGF(5, "raster at index %d alignment: %d", i, aligned);

			/* abort checking since a raster isn't aligned */
			if (!aligned)
				break;
		}
	}
	while (0);

	/* not aligned, error */
	if (!aligned) {
		rterror("rt_raster_iterator: The set of rasters provided (custom extent included, if appropriate) do not have the same alignment");

		_rti_iterator_arg_destroy(_param);

		return ES_ERROR;
	}

	/* use extenttype to build output raster (no bands though) */
	i = -1;
	switch (extenttype) {
		case ET_INTERSECTION:
		case ET_UNION:
			/* make copy of first "real" raster */
			rtnrast = rtalloc(sizeof(struct rt_raster_t));
			if (rtnrast == NULL) {
				rterror("rt_raster_iterator: Could not allocate memory for output raster");

				_rti_iterator_arg_destroy(_param);

				return ES_ERROR;
			}

			for (i = 0; i < itrcount; i++) {
				if (!_param->isempty[i]) {
					memcpy(rtnrast, _param->raster[i], sizeof(struct rt_raster_serialized_t));
					break;
				}
			}
			rtnrast->numBands = 0;
			rtnrast->bands = NULL;

			/* get extent of output raster */
			rast = NULL;
			for (i = i + 1; i < itrcount; i++) {
				if (_param->isempty[i])
					continue;

				status = rt_raster_from_two_rasters(rtnrast, _param->raster[i], extenttype, &rast, NULL);
				rtdealloc(rtnrast);

				if (rast == NULL || status != ES_NONE) {
					rterror("rt_raster_iterator: Could not compute %s extent of rasters",
						extenttype == ET_UNION ? "union" : "intersection"
					);

					_rti_iterator_arg_destroy(_param);

					return ES_ERROR;
				}
				else if (rt_raster_is_empty(rast)) {
					rtinfo("rt_raster_iterator: Computed raster for %s extent is empty",
						extenttype == ET_UNION ? "union" : "intersection"
					);

					_rti_iterator_arg_destroy(_param);

					*rtnraster = rast;
					return ES_NONE;
				}

				rtnrast = rast;
				rast = NULL;
			}

			break;
		/*
			first, second and last have similar checks
			and continue into custom
		*/
		case ET_FIRST:
			i = 0;
		case ET_SECOND:
			if (i < 0) {
				if (itrcount < 2)
					i = 0;
				else
					i = 1;
			}
		case ET_LAST:
			if (i < 0) i = itrcount - 1;
			
			/* input raster is null, return NULL */
			if (_param->raster[i] == NULL) {
				RASTER_DEBUGF(3, "returning NULL as %s raster is NULL and extent type is ET_%s",
					(i == 0 ? "first" : (i == 1 ? "second" : "last")),
					(i == 0 ? "FIRST" : (i == 1 ? "SECOND" : "LAST"))
				);

				_rti_iterator_arg_destroy(_param);

				return ES_NONE;
			}
			/* input raster is empty, return empty raster */
			else if (_param->isempty[i]) {
				RASTER_DEBUGF(3, "returning empty raster as %s raster is empty and extent type is ET_%s",
					(i == 0 ? "first" : (i == 1 ? "second" : "last")),
					(i == 0 ? "FIRST" : (i == 1 ? "SECOND" : "LAST"))
				);

				_rti_iterator_arg_destroy(_param);

				rtnrast = rt_raster_new(0, 0);
				if (rtnrast == NULL) {
					rterror("rt_raster_iterator: Could not create empty raster");
					return ES_ERROR;
				}
				rt_raster_set_scale(rtnrast, 0, 0);

				*rtnraster = rtnrast;
				return ES_NONE;
			}
		/* copy the custom extent raster */
		case ET_CUSTOM:
			rtnrast = rtalloc(sizeof(struct rt_raster_t));
			if (rtnrast == NULL) {
				rterror("rt_raster_iterator: Could not allocate memory for output raster");

				_rti_iterator_arg_destroy(_param);

				return ES_ERROR;
			}

			switch (extenttype) {
				case ET_CUSTOM:
					memcpy(rtnrast, customextent, sizeof(struct rt_raster_serialized_t));
					break;
				/* first, second, last */
				default:
					memcpy(rtnrast, _param->raster[i], sizeof(struct rt_raster_serialized_t));
					break;
			}
			rtnrast->numBands = 0;
			rtnrast->bands = NULL;
			break;
	}

	_width = rt_raster_get_width(rtnrast);
	_height = rt_raster_get_height(rtnrast);

	RASTER_DEBUGF(4, "rtnrast (width, height, ulx, uly, scalex, scaley, skewx, skewy, srid) = (%d, %d, %f, %f, %f, %f, %f, %f, %d)",
		_width,
		_height,
		rt_raster_get_x_offset(rtnrast),
		rt_raster_get_y_offset(rtnrast),
		rt_raster_get_x_scale(rtnrast),
		rt_raster_get_y_scale(rtnrast),
		rt_raster_get_x_skew(rtnrast),
		rt_raster_get_y_skew(rtnrast),
		rt_raster_get_srid(rtnrast)
	);

	/* init values and NODATA for use with empty rasters */
	if (!_rti_iterator_arg_empty_init(_param)) {
		rterror("rt_raster_iterator: Could not initialize empty values and NODATA");

		_rti_iterator_arg_destroy(_param);
		rt_raster_destroy(rtnrast);
		
		return ES_ERROR;
	}

	/* create output band */
	if (rt_raster_generate_new_band(
		rtnrast,
		pixtype,
		nodataval,
		hasnodata, nodataval,
		0
	) < 0) {
		rterror("rt_raster_iterator: Could not add new band to output raster");

		_rti_iterator_arg_destroy(_param);
		rt_raster_destroy(rtnrast);

		return ES_ERROR;
	}

	/* get output band */
	rtnband = rt_raster_get_band(rtnrast, 0);
	if (rtnband == NULL) {
		rterror("rt_raster_iterator: Could not get new band from output raster");

		_rti_iterator_arg_destroy(_param);
		rt_raster_destroy(rtnrast);

		return ES_ERROR;
	}

	/* output band's minimum value */
	minval = rt_band_get_min_value(rtnband);

	/* initialize argument for callback function */
	if (!_rti_iterator_arg_callback_init(_param)) {
		rterror("rt_raster_iterator: Could not initialize callback function argument");

		_rti_iterator_arg_destroy(_param);
		rt_band_destroy(rtnband);
		rt_raster_destroy(rtnrast);

		return ES_ERROR;
	}

	/* fill _param->offset */
	for (i = 0; i < itrcount; i++) {
		if (_param->isempty[i])
			continue;

		status = rt_raster_from_two_rasters(rtnrast, _param->raster[i], ET_FIRST, &rast, offset);
		rtdealloc(rast);
		if (status != ES_NONE) {
			rterror("rt_raster_iterator: Could not compute raster offsets");

			_rti_iterator_arg_destroy(_param);
			rt_band_destroy(rtnband);
			rt_raster_destroy(rtnrast);

			return ES_ERROR;
		}

		_param->offset[i][0] = offset[2];
		_param->offset[i][1] = offset[3];
		RASTER_DEBUGF(4, "rast %d offset: %f %f", i, offset[2], offset[3]);
	}

	/* loop over each pixel (POI) of output raster */
	/* _x,_y are for output raster */
	/* x,y are for input raster */
	for (_y = 0; _y < _height; _y++) {
		for (_x = 0; _x < _width; _x++) {
			RASTER_DEBUGF(4, "iterating output pixel (x, y) = (%d, %d)", _x, _y);
			_param->arg->dst_pixel[0] = _x;
			_param->arg->dst_pixel[1] = _y;

			/* loop through each input raster */
			for (i = 0; i < itrcount; i++) {
				RASTER_DEBUGF(4, "raster %d", i);

				/*
					empty raster
					OR band does not exist and flag set to use NODATA
					OR band is NODATA
				*/
				if (
					_param->isempty[i] ||
					(_param->band.rtband[i] == NULL && itrset[i].nbnodata) ||
					_param->band.isnodata[i]
				) {
					RASTER_DEBUG(4, "empty raster, band does not exist or band is NODATA. using empty values and NODATA");
					
					x = _x;
					y = _y;

					_param->arg->values[i] = _param->empty.values;
					_param->arg->nodata[i] = _param->empty.nodata;

					continue;
				}

				/* input raster's X,Y */
				x = _x - (int) _param->offset[i][0];
				y = _y - (int) _param->offset[i][1];
				RASTER_DEBUGF(4, "source pixel (x, y) = (%d, %d)", x, y);

				_param->arg->src_pixel[i][0] = x;
				_param->arg->src_pixel[i][1] = y;

				/* neighborhood */
				npixels = NULL;
				status = 0;
				if (distancex > 0 && distancey > 0) {
					RASTER_DEBUG(4, "getting neighborhood");

					status = rt_band_get_nearest_pixel(
						_param->band.rtband[i],
						x, y,
						distancex, distancey,
						1,
						&npixels
					);
					if (status < 0) {
						rterror("rt_raster_iterator: Could not get pixel neighborhood");

						_rti_iterator_arg_destroy(_param);
						rt_band_destroy(rtnband);
						rt_raster_destroy(rtnrast);

						return ES_ERROR;
					}
				}

				/* get value of POI */
				/* get pixel's value */
				if (
					(x >= 0 && x < _param->width[i]) &&
					(y >= 0 && y < _param->height[i])
				) {
					RASTER_DEBUG(4, "getting value of POI");
					if (rt_band_get_pixel(
						_param->band.rtband[i],
						x, y,
						&value,
						&isnodata
					) != ES_NONE) {
						rterror("rt_raster_iterator: Could not get the pixel value of band");

						_rti_iterator_arg_destroy(_param);
						rt_band_destroy(rtnband);
						rt_raster_destroy(rtnrast);

						return ES_ERROR;
					}
					inextent = 1;
				}
				/* outside band extent, set to NODATA */
				else {
					RASTER_DEBUG(4, "Outside band extent, setting value to NODATA");
					/* has NODATA, use NODATA */
					if (_param->band.hasnodata[i])
						value = _param->band.nodataval[i];
					/* no NODATA, use min possible value */
					else
						value = _param->band.minval[i];

					inextent = 0;
					isnodata = 1;
				}

				/* add pixel to neighborhood */
				status++;
				if (status > 1)
					npixels = (rt_pixel) rtrealloc(npixels, sizeof(struct rt_pixel_t) * status);
				else
					npixels = (rt_pixel) rtalloc(sizeof(struct rt_pixel_t));

				if (npixels == NULL) {
					rterror("rt_raster_iterator: Could not reallocate memory for neighborhood");

					_rti_iterator_arg_destroy(_param);
					rt_band_destroy(rtnband);
					rt_raster_destroy(rtnrast);

					return ES_ERROR;
				}

				npixels[status - 1].x = x;
				npixels[status - 1].y = y;
				npixels[status - 1].nodata = 1;
				npixels[status - 1].value = value;

				/* set nodata flag */
				if ((!_param->band.hasnodata[i] && inextent) || !isnodata) {
					npixels[status - 1].nodata = 0;
				}
				RASTER_DEBUGF(4, "value, nodata: %f, %d", value, npixels[status - 1].nodata);

				/* convert set of rt_pixel to 2D array */
				status = rt_pixel_set_to_array(
					npixels, status,mask,
					x, y,
					distancex, distancey,
					&(_param->arg->values[i]),
					&(_param->arg->nodata[i]),
					NULL, NULL
				);
				rtdealloc(npixels);
				if (status != ES_NONE) {
					rterror("rt_raster_iterator: Could not create 2D array of neighborhood");

					_rti_iterator_arg_destroy(_param);
					rt_band_destroy(rtnband);
					rt_raster_destroy(rtnrast);

					return ES_ERROR;
				}
			}
	
			/* callback */
			RASTER_DEBUG(4, "calling callback function");
			value = 0;
			nodata = 0;
			status = callback(_param->arg, userarg, &value, &nodata);

			/* free memory from callback */
			_rti_iterator_arg_callback_clean(_param);

			/* handle callback status */
			if (status == 0) {
				rterror("rt_raster_iterator: Callback function returned an error");

				_rti_iterator_arg_destroy(_param);
				rt_band_destroy(rtnband);
				rt_raster_destroy(rtnrast);

				return ES_ERROR;
			}

			/* burn value to pixel */
			status = 0;
			if (!nodata) {
				status = rt_band_set_pixel(rtnband, _x, _y, value, NULL);
				RASTER_DEBUGF(4, "burning pixel (%d, %d) with value: %f", _x, _y, value);
			}
			else if (!hasnodata) {
				status = rt_band_set_pixel(rtnband, _x, _y, minval, NULL);
				RASTER_DEBUGF(4, "burning pixel (%d, %d) with minval: %f", _x, _y, minval);
			}
			else {
				RASTER_DEBUGF(4, "NOT burning pixel (%d, %d)", _x, _y);
			}
			if (status != ES_NONE) {
				rterror("rt_raster_iterator: Could not set pixel value");

				_rti_iterator_arg_destroy(_param);
				rt_band_destroy(rtnband);
				rt_raster_destroy(rtnrast);

				return ES_ERROR;
			}
		}
	}

	/* lots of cleanup */
	_rti_iterator_arg_destroy(_param);

	*rtnraster = rtnrast;
	return ES_NONE;
}

/******************************************************************************
* rt_raster_colormap()
******************************************************************************/

typedef struct _rti_colormap_arg_t* _rti_colormap_arg;
struct _rti_colormap_arg_t {
	rt_raster raster;
	rt_band band;

	rt_colormap_entry nodataentry;
	int hasnodata;
	double nodataval;

	int nexpr;
	rt_reclassexpr *expr;

	int npos;
	uint16_t *pos;

};

static _rti_colormap_arg
_rti_colormap_arg_init(rt_raster raster) {
	_rti_colormap_arg arg = NULL;

	arg = rtalloc(sizeof(struct _rti_colormap_arg_t));
	if (arg == NULL) {
		rterror("_rti_colormap_arg_init: Could not allocate memory for _rti_color_arg");
		return NULL;
	}

	arg->band = NULL;
	arg->nodataentry = NULL;
	arg->hasnodata = 0;
	arg->nodataval = 0;

	if (raster == NULL)
		arg->raster = NULL;
	/* raster provided */
	else {
		arg->raster = rt_raster_clone(raster, 0);
		if (arg->raster == NULL) {
			rterror("_rti_colormap_arg_init: Could not create output raster");
			return NULL;
		}
	}

	arg->nexpr = 0;
	arg->expr = NULL;

	arg->npos = 0;
	arg->pos = NULL;

	return arg;
}

static void
_rti_colormap_arg_destroy(_rti_colormap_arg arg) {
	int i = 0;

	if (arg->raster != NULL) {
		rt_band band = NULL;

		for (i = rt_raster_get_num_bands(arg->raster) - 1; i >= 0; i--) {
			band = rt_raster_get_band(arg->raster, i);
			if (band != NULL)
				rt_band_destroy(band);
		}

		rt_raster_destroy(arg->raster);
	}

	if (arg->nexpr) {
		for (i = 0; i < arg->nexpr; i++) {
			if (arg->expr[i] != NULL)
				rtdealloc(arg->expr[i]);
		}
		rtdealloc(arg->expr);
	}

	if (arg->npos)
		rtdealloc(arg->pos);

	rtdealloc(arg);
	arg = NULL;
}

/**
 * Returns a new raster with up to four 8BUI bands (RGBA) from
 * applying a colormap to the user-specified band of the
 * input raster.
 *
 * @param raster: input raster
 * @param nband: 0-based index of the band to process with colormap
 * @param colormap: rt_colormap object of colormap to apply to band
 *
 * @return new raster or NULL on error
 */
rt_raster rt_raster_colormap(
	rt_raster raster, int nband,
	rt_colormap colormap
) {
	_rti_colormap_arg arg = NULL;
	rt_raster rtnraster = NULL;
	rt_band band = NULL;
	int i = 0;
	int j = 0;
	int k = 0;

	assert(colormap != NULL);

	/* empty raster */
	if (rt_raster_is_empty(raster))
		return NULL;

	/* no colormap entries */
	if (colormap->nentry < 1) {
		rterror("rt_raster_colormap: colormap must have at least one entry");
		return NULL;
	}

	/* nband is valid */
	if (!rt_raster_has_band(raster, nband)) {
		rterror("rt_raster_colormap: raster has no band at index %d", nband);
		return NULL;
	}

	band = rt_raster_get_band(raster, nband);
	if (band == NULL) {
		rterror("rt_raster_colormap: Could not get band at index %d", nband);
		return NULL;
	}

	/* init internal variables */
	arg = _rti_colormap_arg_init(raster);
	if (arg == NULL) {
		rterror("rt_raster_colormap: Could not initialize internal variables");
		return NULL;
	}

	/* handle NODATA */
	if (rt_band_get_hasnodata_flag(band)) {
		arg->hasnodata = 1;
		rt_band_get_nodata(band, &(arg->nodataval));
	}

	/* # of colors */
	if (colormap->ncolor < 1) {
		rterror("rt_raster_colormap: At least one color must be provided");
		_rti_colormap_arg_destroy(arg);
		return NULL;
	}
	else if (colormap->ncolor > 4) {
		rtinfo("More than four colors indicated. Using only the first four colors");
		colormap->ncolor = 4;
	}

	/* find non-NODATA entries */
	arg->npos = 0;
	arg->pos = rtalloc(sizeof(uint16_t) * colormap->nentry);
	if (arg->pos == NULL) {
		rterror("rt_raster_colormap: Could not allocate memory for valid entries");
		_rti_colormap_arg_destroy(arg);
		return NULL;
	}
	for (i = 0, j = 0; i < colormap->nentry; i++) {
		/* special handling for NODATA entries */
		if (colormap->entry[i].isnodata) {
			/* first NODATA entry found, use it */
			if (arg->nodataentry == NULL)
				arg->nodataentry = &(colormap->entry[i]);
			else
				rtwarn("More than one colormap entry found for NODATA value. Only using first NOTDATA entry");

			continue;
		}

		(arg->npos)++;
		arg->pos[j++] = i;
	}

	/* INTERPOLATE and only one non-NODATA entry */
	if (colormap->method == CM_INTERPOLATE && arg->npos < 2) {
		rtinfo("Method INTERPOLATE requires at least two non-NODATA colormap entries. Using NEAREST instead");
		colormap->method = CM_NEAREST;
	}

	/* NODATA entry but band has no NODATA value */
	if (!arg->hasnodata && arg->nodataentry != NULL) {
		rtinfo("Band at index %d has no NODATA value. Ignoring NODATA entry", nband);
		arg->nodataentry = NULL;
	}

	/* allocate expr */
	arg->nexpr = arg->npos;

	/* INTERPOLATE needs one less than the number of entries */
	if (colormap->method == CM_INTERPOLATE)
		arg->nexpr -= 1;
	/* EXACT requires a no matching expression */
	else if (colormap->method == CM_EXACT)
		arg->nexpr += 1;

	/* NODATA entry exists, add expression */
	if (arg->nodataentry != NULL)
		arg->nexpr += 1;
	arg->expr = rtalloc(sizeof(rt_reclassexpr) * arg->nexpr);
	if (arg->expr == NULL) {
		rterror("rt_raster_colormap: Could not allocate memory for reclass expressions");
		_rti_colormap_arg_destroy(arg);
		return NULL;
	}
	RASTER_DEBUGF(4, "nexpr = %d", arg->nexpr);
	RASTER_DEBUGF(4, "expr @ %p", arg->expr);

	for (i = 0; i < arg->nexpr; i++) {
		arg->expr[i] = rtalloc(sizeof(struct rt_reclassexpr_t));
		if (arg->expr[i] == NULL) {
			rterror("rt_raster_colormap: Could not allocate memory for reclass expression");
			_rti_colormap_arg_destroy(arg);
			return NULL;
		}
	}

	/* reclassify bands */
	/* by # of colors */
	for (i = 0; i < colormap->ncolor; i++) {
		k = 0;

		/* handle NODATA entry first */
		if (arg->nodataentry != NULL) {
			arg->expr[k]->src.min = arg->nodataentry->value;
			arg->expr[k]->src.max = arg->nodataentry->value;
			arg->expr[k]->src.inc_min = 1;
			arg->expr[k]->src.inc_max = 1;
			arg->expr[k]->src.exc_min = 0;
			arg->expr[k]->src.exc_max = 0;

			arg->expr[k]->dst.min = arg->nodataentry->color[i];
			arg->expr[k]->dst.max = arg->nodataentry->color[i];

			arg->expr[k]->dst.inc_min = 1;
			arg->expr[k]->dst.inc_max = 1;
			arg->expr[k]->dst.exc_min = 0;
			arg->expr[k]->dst.exc_max = 0;

			RASTER_DEBUGF(4, "NODATA expr[%d]->src (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->src.min,
				arg->expr[k]->src.max,
				arg->expr[k]->src.inc_min,
				arg->expr[k]->src.inc_max,
				arg->expr[k]->src.exc_min,
				arg->expr[k]->src.exc_max
			);
			RASTER_DEBUGF(4, "NODATA expr[%d]->dst (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->dst.min,
				arg->expr[k]->dst.max,
				arg->expr[k]->dst.inc_min,
				arg->expr[k]->dst.inc_max,
				arg->expr[k]->dst.exc_min,
				arg->expr[k]->dst.exc_max
			);

			k++;
		}

		/* by non-NODATA entry */
		for (j = 0; j < arg->npos; j++) {
			if (colormap->method == CM_INTERPOLATE) {
				if (j == arg->npos - 1)
					continue;

				arg->expr[k]->src.min = colormap->entry[arg->pos[j + 1]].value;
				arg->expr[k]->src.inc_min = 1;
				arg->expr[k]->src.exc_min = 0;

				arg->expr[k]->src.max = colormap->entry[arg->pos[j]].value;
				arg->expr[k]->src.inc_max = 1;
				arg->expr[k]->src.exc_max = 0;

				arg->expr[k]->dst.min = colormap->entry[arg->pos[j + 1]].color[i];
				arg->expr[k]->dst.max = colormap->entry[arg->pos[j]].color[i];

				arg->expr[k]->dst.inc_min = 1;
				arg->expr[k]->dst.exc_min = 0;

				arg->expr[k]->dst.inc_max = 1;
				arg->expr[k]->dst.exc_max = 0;
			}
			else if (colormap->method == CM_NEAREST) {

				/* NOT last entry */
				if (j != arg->npos - 1) {
					arg->expr[k]->src.min = ((colormap->entry[arg->pos[j]].value - colormap->entry[arg->pos[j + 1]].value) / 2.) + colormap->entry[arg->pos[j + 1]].value;
					arg->expr[k]->src.inc_min = 0;
					arg->expr[k]->src.exc_min = 0;
				}
				/* last entry */
				else {
					arg->expr[k]->src.min = colormap->entry[arg->pos[j]].value;
					arg->expr[k]->src.inc_min = 1;
					arg->expr[k]->src.exc_min = 1;
				}

				/* NOT first entry */
				if (j > 0) {
					arg->expr[k]->src.max = arg->expr[k - 1]->src.min;
					arg->expr[k]->src.inc_max = 1;
					arg->expr[k]->src.exc_max = 0;
				}
				/* first entry */
				else {
					arg->expr[k]->src.max = colormap->entry[arg->pos[j]].value;
					arg->expr[k]->src.inc_max = 1;
					arg->expr[k]->src.exc_max = 1;
				}

				arg->expr[k]->dst.min = colormap->entry[arg->pos[j]].color[i];
				arg->expr[k]->dst.inc_min = 1;
				arg->expr[k]->dst.exc_min = 0;

				arg->expr[k]->dst.max = colormap->entry[arg->pos[j]].color[i];
				arg->expr[k]->dst.inc_max = 1;
				arg->expr[k]->dst.exc_max = 0;
			}
			else if (colormap->method == CM_EXACT) {
				arg->expr[k]->src.min = colormap->entry[arg->pos[j]].value;
				arg->expr[k]->src.inc_min = 1;
				arg->expr[k]->src.exc_min = 0;

				arg->expr[k]->src.max = colormap->entry[arg->pos[j]].value;
				arg->expr[k]->src.inc_max = 1;
				arg->expr[k]->src.exc_max = 0;

				arg->expr[k]->dst.min = colormap->entry[arg->pos[j]].color[i];
				arg->expr[k]->dst.inc_min = 1;
				arg->expr[k]->dst.exc_min = 0;

				arg->expr[k]->dst.max = colormap->entry[arg->pos[j]].color[i];
				arg->expr[k]->dst.inc_max = 1;
				arg->expr[k]->dst.exc_max = 0;
			}

			RASTER_DEBUGF(4, "expr[%d]->src (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->src.min,
				arg->expr[k]->src.max,
				arg->expr[k]->src.inc_min,
				arg->expr[k]->src.inc_max,
				arg->expr[k]->src.exc_min,
				arg->expr[k]->src.exc_max
			);

			RASTER_DEBUGF(4, "expr[%d]->dst (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->dst.min,
				arg->expr[k]->dst.max,
				arg->expr[k]->dst.inc_min,
				arg->expr[k]->dst.inc_max,
				arg->expr[k]->dst.exc_min,
				arg->expr[k]->dst.exc_max
			);

			k++;
		}

		/* EXACT has one last expression for catching all uncaught values */
		if (colormap->method == CM_EXACT) {
			arg->expr[k]->src.min = 0;
			arg->expr[k]->src.inc_min = 1;
			arg->expr[k]->src.exc_min = 1;

			arg->expr[k]->src.max = 0;
			arg->expr[k]->src.inc_max = 1;
			arg->expr[k]->src.exc_max = 1;

			arg->expr[k]->dst.min = 0;
			arg->expr[k]->dst.inc_min = 1;
			arg->expr[k]->dst.exc_min = 0;

			arg->expr[k]->dst.max = 0;
			arg->expr[k]->dst.inc_max = 1;
			arg->expr[k]->dst.exc_max = 0;

			RASTER_DEBUGF(4, "expr[%d]->src (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->src.min,
				arg->expr[k]->src.max,
				arg->expr[k]->src.inc_min,
				arg->expr[k]->src.inc_max,
				arg->expr[k]->src.exc_min,
				arg->expr[k]->src.exc_max
			);

			RASTER_DEBUGF(4, "expr[%d]->dst (min, max, in, ix, en, ex) = (%f, %f, %d, %d, %d, %d)",
				k,
				arg->expr[k]->dst.min,
				arg->expr[k]->dst.max,
				arg->expr[k]->dst.inc_min,
				arg->expr[k]->dst.inc_max,
				arg->expr[k]->dst.exc_min,
				arg->expr[k]->dst.exc_max
			);

			k++;
		}

		/* call rt_band_reclass */
		arg->band = rt_band_reclass(band, PT_8BUI, 0, 0, arg->expr, arg->nexpr);
		if (arg->band == NULL) {
			rterror("rt_raster_colormap: Could not reclassify band");
			_rti_colormap_arg_destroy(arg);
			return NULL;
		}

		/* add reclassified band to raster */
		if (rt_raster_add_band(arg->raster, arg->band, rt_raster_get_num_bands(arg->raster)) < 0) {
			rterror("rt_raster_colormap: Could not add reclassified band to output raster");
			_rti_colormap_arg_destroy(arg);
			return NULL;
		}
	}

	rtnraster = arg->raster;
	arg->raster = NULL;
	_rti_colormap_arg_destroy(arg);

	return rtnraster;
}
