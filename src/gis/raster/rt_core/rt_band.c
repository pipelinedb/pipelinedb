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

#include <stdio.h>

#include "librtcore.h"
#include "librtcore_internal.h"

#include "gdal_vrt.h"

/**
 * Create an in-db rt_band with no data
 *
 * @param width     : number of pixel columns
 * @param height    : number of pixel rows
 * @param pixtype   : pixel type for the band
 * @param hasnodata : indicates if the band has nodata value
 * @param nodataval : the nodata value, will be appropriately
 *                    truncated to fit the pixtype size.
 * @param data      : pointer to actual band data, required to
 *                    be aligned accordingly to
 *                    rt_pixtype_aligment(pixtype) and big enough
 *                    to hold raster width*height values.
 *                    Data will NOT be copied, ownership is left
 *                    to caller which is responsible to keep it
 *                    allocated for the whole lifetime of the returned
 *                    rt_band.
 *
 * @return an rt_band, or 0 on failure
 */
rt_band
rt_band_new_inline(
	uint16_t width, uint16_t height,
	rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	uint8_t* data
) {
	rt_band band = NULL;

	assert(NULL != data);

	band = rtalloc(sizeof(struct rt_band_t));
	if (band == NULL) {
		rterror("rt_band_new_inline: Out of memory allocating rt_band");
		return NULL;
	}

	RASTER_DEBUGF(3, "Created rt_band @ %p with pixtype %s", band, rt_pixtype_name(pixtype));

	band->pixtype = pixtype;
	band->offline = 0;
	band->width = width;
	band->height = height;
	band->hasnodata = hasnodata ? 1 : 0;
	band->isnodata = FALSE; /* we don't know what is in data, so must be FALSE */
	band->nodataval = 0;
	band->data.mem = data;
	band->ownsdata = 0; /* we do NOT own this data!!! */
	band->raster = NULL;

	RASTER_DEBUGF(3, "Created rt_band with dimensions %d x %d", band->width, band->height);

	/* properly set nodataval as it may need to be constrained to the data type */
	if (hasnodata && rt_band_set_nodata(band, nodataval, NULL) != ES_NONE) {
		rterror("rt_band_new_inline: Could not set NODATA value");
		rt_band_destroy(band);
		return NULL;
	}

	return band;
}

/**
 * Create an out-db rt_band
 *
 * @param width     : number of pixel columns
 * @param height    : number of pixel rows
 * @param pixtype   : pixel type for the band
 * @param hasnodata : indicates if the band has nodata value
 * @param nodataval : the nodata value, will be appropriately
 *                    truncated to fit the pixtype size.
 * @param bandNum   : 0-based band number in the external file
 *                    to associate this band with.
 * @param path      : NULL-terminated path string pointing to the file
 *                    containing band data. The string will NOT be
 *                    copied, ownership is left to caller which is
 *                    responsible to keep it allocated for the whole
 *                    lifetime of the returned rt_band.
 *
 * @return an rt_band, or 0 on failure
 */
rt_band
rt_band_new_offline(
	uint16_t width, uint16_t height,
	rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	uint8_t bandNum, const char* path
) {
	rt_band band = NULL;
	int pathlen = 0;

	assert(NULL != path);

	band = rtalloc(sizeof(struct rt_band_t));
	if (band == NULL) {
		rterror("rt_band_new_offline: Out of memory allocating rt_band");
		return NULL;
	}

	RASTER_DEBUGF(3, "Created rt_band @ %p with pixtype %s",
		band, rt_pixtype_name(pixtype)
	); 

	band->pixtype = pixtype;
	band->offline = 1;
	band->width = width;
	band->height = height;
	band->hasnodata = hasnodata ? 1 : 0;
	band->nodataval = 0;
	band->isnodata = FALSE; /* we don't know if the offline band is NODATA */
	band->ownsdata = 0; /* offline, flag is useless as all offline data cache is owned internally */
	band->raster = NULL;

	/* properly set nodataval as it may need to be constrained to the data type */
	if (hasnodata && rt_band_set_nodata(band, nodataval, NULL) != ES_NONE) {
		rterror("rt_band_new_offline: Could not set NODATA value");
		rt_band_destroy(band);
		return NULL;
	}

	band->data.offline.bandNum = bandNum;

	/* memory for data.offline.path is managed internally */
	pathlen = strlen(path);
	band->data.offline.path = rtalloc(sizeof(char) * (pathlen + 1));
	if (band->data.offline.path == NULL) {
		rterror("rt_band_new_offline: Out of memory allocating offline path");
		rt_band_destroy(band);
		return NULL;
	}
	memcpy(band->data.offline.path, path, pathlen);
	band->data.offline.path[pathlen] = '\0';

	band->data.offline.mem = NULL;

	return band;
}

/**
 * Create a new band duplicated from source band.  Memory is allocated
 * for band path (if band is offline) or band data (if band is online).
 * The caller is responsible for freeing the memory when the returned
 * rt_band is destroyed.
 *
 * @param : the band to copy
 *
 * @return an rt_band or NULL on failure
 */
rt_band
rt_band_duplicate(rt_band band) {
	rt_band rtn = NULL;

	assert(band != NULL);

	/* offline */
	if (band->offline) {
		rtn = rt_band_new_offline(
			band->width, band->height,
			band->pixtype,
			band->hasnodata, band->nodataval,
			band->data.offline.bandNum, (const char *) band->data.offline.path 
		);
	}
	/* online */
	else {
		uint8_t *data = NULL;
		data = rtalloc(rt_pixtype_size(band->pixtype) * band->width * band->height);
		if (data == NULL) {
			rterror("rt_band_duplicate: Out of memory allocating online band data");
			return NULL;
		}
		memcpy(data, band->data.mem, rt_pixtype_size(band->pixtype) * band->width * band->height);

		rtn = rt_band_new_inline(
			band->width, band->height,
			band->pixtype,
			band->hasnodata, band->nodataval,
			data
		);
		rt_band_set_ownsdata_flag(rtn, 1); /* we DO own this data!!! */
	}

	if (rtn == NULL) {
		rterror("rt_band_duplicate: Could not copy band");
		return NULL;
	}

	return rtn;
}

int
rt_band_is_offline(rt_band band) {

    assert(NULL != band);


    return band->offline ? 1 : 0;
}

/**
 * Destroy a raster band
 *
 * @param band : the band to destroy
 */
void
rt_band_destroy(rt_band band) { 
	if (band == NULL)
		return;

	RASTER_DEBUGF(3, "Destroying rt_band @ %p", band);

	/* offline band */
	if (band->offline) {
		/* memory cache */
		if (band->data.offline.mem != NULL)
			rtdealloc(band->data.offline.mem);
		/* offline file path */
		if (band->data.offline.path != NULL)
			rtdealloc(band->data.offline.path);
	}
	/* inline band and band owns the data */
	else if (band->data.mem != NULL && band->ownsdata)
		rtdealloc(band->data.mem);

	rtdealloc(band);
}

const char*
rt_band_get_ext_path(rt_band band) {

    assert(NULL != band);


    if (!band->offline) {
        RASTER_DEBUG(3, "rt_band_get_ext_path: Band is not offline");
        return NULL;
    }
    return band->data.offline.path;
}

rt_errorstate
rt_band_get_ext_band_num(rt_band band, uint8_t *bandnum) {
	assert(NULL != band);
	assert(NULL != bandnum);

	*bandnum = 0;

	if (!band->offline) {
		RASTER_DEBUG(3, "rt_band_get_ext_band_num: Band is not offline");
		return ES_ERROR;
	}

	*bandnum = band->data.offline.bandNum;

	return ES_NONE;
}

/**
	* Get pointer to raster band data
	*
	* @param band : the band who's data to get
	*
	* @return pointer to band data or NULL if error
	*/
void *
rt_band_get_data(rt_band band) {
	assert(NULL != band);

	if (band->offline) {
		if (band->data.offline.mem != NULL)
			return band->data.offline.mem;

		if (rt_band_load_offline_data(band) != ES_NONE)
			return NULL;
		else
			return band->data.offline.mem;
	}
	else
		return band->data.mem;
}

/* variable for PostgreSQL GUC: postgis.enable_outdb_rasters */
char enable_outdb_rasters = 1;

/**
	* Load offline band's data.  Loaded data is internally owned
	* and should not be released by the caller.  Data will be
	* released when band is destroyed with rt_band_destroy().
	*
	* @param band : the band who's data to get
	*
	* @return ES_NONE if success, ES_ERROR if failure
	*/
rt_errorstate
rt_band_load_offline_data(rt_band band) {
	GDALDatasetH hdsSrc = NULL;
	int nband = 0;
	VRTDatasetH hdsDst = NULL;
	VRTSourcedRasterBandH hbandDst = NULL;
	double gt[6] = {0.};
	double ogt[6] = {0};
	double offset[2] = {0};

	rt_raster _rast = NULL;
	rt_band _band = NULL;
	int aligned = 0;
	int err = ES_NONE;

	assert(band != NULL);
	assert(band->raster != NULL);

	if (!band->offline) {
		rterror("rt_band_load_offline_data: Band is not offline");
		return ES_ERROR;
	}
	else if (!strlen(band->data.offline.path)) {
		rterror("rt_band_load_offline_data: Offline band does not a have a specified file");
		return ES_ERROR;
	}

	/* offline_data is disabled */
	if (!enable_outdb_rasters) {
		rterror("rt_band_load_offline_data: Access to offline bands disabled");
		return ES_ERROR;
	}

	rt_util_gdal_register_all(0);
	/*
	hdsSrc = rt_util_gdal_open(band->data.offline.path, GA_ReadOnly, 1);
	*/
	hdsSrc = rt_util_gdal_open(band->data.offline.path, GA_ReadOnly, 0);
	if (hdsSrc == NULL) {
		rterror("rt_band_load_offline_data: Cannot open offline raster: %s", band->data.offline.path);
		return ES_ERROR;
	}

	/* # of bands */
	nband = GDALGetRasterCount(hdsSrc);
	if (!nband) {
		rterror("rt_band_load_offline_data: No bands found in offline raster: %s", band->data.offline.path);
		GDALClose(hdsSrc);
		return ES_ERROR;
	}
	/* bandNum is 0-based */
	else if (band->data.offline.bandNum + 1 > nband) {
		rterror("rt_band_load_offline_data: Specified band %d not found in offline raster: %s", band->data.offline.bandNum, band->data.offline.path);
		GDALClose(hdsSrc);
		return ES_ERROR;
	}

	/* get raster's geotransform */
	rt_raster_get_geotransform_matrix(band->raster, gt);
	RASTER_DEBUGF(3, "Raster geotransform (%f, %f, %f, %f, %f, %f)",
		gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]);

	/* get offline raster's geotransform */
	if (GDALGetGeoTransform(hdsSrc, ogt) != CE_None) {
		RASTER_DEBUG(4, "Using default geotransform matrix (0, 1, 0, 0, 0, -1)");
		ogt[0] = 0;
		ogt[1] = 1;
		ogt[2] = 0;
		ogt[3] = 0;
		ogt[4] = 0;
		ogt[5] = -1;
	}
	RASTER_DEBUGF(3, "Offline geotransform (%f, %f, %f, %f, %f, %f)",
		ogt[0], ogt[1], ogt[2], ogt[3], ogt[4], ogt[5]);

	/* are rasters aligned? */
	_rast = rt_raster_new(1, 1);
	rt_raster_set_geotransform_matrix(_rast, ogt);
	rt_raster_set_srid(_rast, band->raster->srid);
	err = rt_raster_same_alignment(band->raster, _rast, &aligned, NULL);
	rt_raster_destroy(_rast);

	if (err != ES_NONE) {
		rterror("rt_band_load_offline_data: Could not test alignment of in-db representation of out-db raster");
		GDALClose(hdsSrc);
		return ES_ERROR;
	}
	else if (!aligned) {
		rtwarn("The in-db representation of the out-db raster is not aligned. Band data may be incorrect");
	}

	/* get offsets */
	rt_raster_geopoint_to_cell(
		band->raster,
		ogt[0], ogt[3],
		&(offset[0]), &(offset[1]),
		NULL
	);

	RASTER_DEBUGF(4, "offsets: (%f, %f)", offset[0], offset[1]);

	/* create VRT dataset */
	hdsDst = VRTCreate(band->width, band->height);
	GDALSetGeoTransform(hdsDst, gt);
	/*
	GDALSetDescription(hdsDst, "/tmp/offline.vrt");
	*/

	/* add band as simple sources */
	GDALAddBand(hdsDst, rt_util_pixtype_to_gdal_datatype(band->pixtype), NULL);
	hbandDst = (VRTSourcedRasterBandH) GDALGetRasterBand(hdsDst, 1);

	if (band->hasnodata)
		GDALSetRasterNoDataValue(hbandDst, band->nodataval);

	VRTAddSimpleSource(
		hbandDst, GDALGetRasterBand(hdsSrc, band->data.offline.bandNum + 1),
		fabs(offset[0]), fabs(offset[1]),
		band->width, band->height,
		0, 0,
		band->width, band->height,
		"near", VRT_NODATA_UNSET
	);

	/* make sure VRT reflects all changes */
	VRTFlushCache(hdsDst);

	/* convert VRT dataset to rt_raster */
	_rast = rt_raster_from_gdal_dataset(hdsDst);

	GDALClose(hdsDst);
	GDALClose(hdsSrc);
	/*
	{
		FILE *fp;
		fp = fopen("/tmp/gdal_open_files.log", "w");
		GDALDumpOpenDatasets(fp);
		fclose(fp);
	}
	*/

	if (_rast == NULL) {
		rterror("rt_band_load_offline_data: Cannot load data from offline raster: %s", band->data.offline.path);
		return ES_ERROR;
	}

	_band = rt_raster_get_band(_rast, 0);
	if (_band == NULL) {
		rterror("rt_band_load_offline_data: Cannot load data from offline raster: %s", band->data.offline.path);
		rt_raster_destroy(_rast);
		return ES_ERROR;
	}

	/* band->data.offline.mem not NULL, free first */
	if (band->data.offline.mem != NULL) {
		rtdealloc(band->data.offline.mem);
		band->data.offline.mem = NULL;
	}

	band->data.offline.mem = _band->data.mem;

	rtdealloc(_band); /* cannot use rt_band_destroy */
	rt_raster_destroy(_rast);

	return ES_NONE;
}

rt_pixtype
rt_band_get_pixtype(rt_band band) {

    assert(NULL != band);


    return band->pixtype;
}

uint16_t
rt_band_get_width(rt_band band) {

    assert(NULL != band);


    return band->width;
}

uint16_t
rt_band_get_height(rt_band band) {

    assert(NULL != band);


    return band->height;
}

/* Get ownsdata flag */
int
rt_band_get_ownsdata_flag(rt_band band) {
	assert(NULL != band);

	return band->ownsdata ? 1 : 0;
}

/* set ownsdata flag */
void
rt_band_set_ownsdata_flag(rt_band band, int flag) {
	assert(NULL != band);

	band->ownsdata = flag ? 1 : 0;
}

int
rt_band_get_hasnodata_flag(rt_band band) {
	assert(NULL != band);

	return band->hasnodata ? 1 : 0;
}

void
rt_band_set_hasnodata_flag(rt_band band, int flag) {

    assert(NULL != band);

    band->hasnodata = (flag) ? 1 : 0;

		/* isnodata depends on hasnodata */
		if (!band->hasnodata && band->isnodata) {
			RASTER_DEBUG(3, "Setting isnodata to FALSE as band no longer has NODATA");
			band->isnodata = 0;
		}
}

rt_errorstate
rt_band_set_isnodata_flag(rt_band band, int flag) {
	assert(NULL != band);

	if (!band->hasnodata) {
		/* silently permit setting isnodata flag to FALSE */
		if (!flag)
			band->isnodata = 0;
		else {
			rterror("rt_band_set_isnodata_flag: Cannot set isnodata flag as band has no NODATA");
			return ES_ERROR;
		}
	}
	else 
		band->isnodata = (flag) ? 1 : 0;

	return ES_NONE;
}

int
rt_band_get_isnodata_flag(rt_band band) {
	assert(NULL != band);

	if (band->hasnodata)
		return band->isnodata ? 1 : 0;
	else
		return 0;
}

/**
 * Set nodata value
 *
 * @param band : the band to set nodata value to
 * @param val : the nodata value
 * @param converted : if non-zero, value was truncated/clamped/coverted
 *
 * @return ES_NONE or ES_ERROR
 */
rt_errorstate
rt_band_set_nodata(rt_band band, double val, int *converted) {
	rt_pixtype pixtype = PT_END;
	int32_t checkvalint = 0;
	uint32_t checkvaluint = 0;
	float checkvalfloat = 0;
	double checkvaldouble = 0;

	assert(NULL != band);

	if (converted != NULL)
		*converted = 0;

	pixtype = band->pixtype;

	RASTER_DEBUGF(3, "rt_band_set_nodata: setting nodata value %g with band type %s", val, rt_pixtype_name(pixtype));

	/* return -1 on out of range */
	switch (pixtype) {
		case PT_1BB: {
			band->nodataval = rt_util_clamp_to_1BB(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_2BUI: {
			band->nodataval = rt_util_clamp_to_2BUI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_4BUI: {
			band->nodataval = rt_util_clamp_to_4BUI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_8BSI: {
			band->nodataval = rt_util_clamp_to_8BSI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_8BUI: {
			band->nodataval = rt_util_clamp_to_8BUI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_16BSI: {
			band->nodataval = rt_util_clamp_to_16BSI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_16BUI: {
			band->nodataval = rt_util_clamp_to_16BUI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_32BSI: {
			band->nodataval = rt_util_clamp_to_32BSI(val);
			checkvalint = band->nodataval;
			break;
		}
		case PT_32BUI: {
			band->nodataval = rt_util_clamp_to_32BUI(val);
			checkvaluint = band->nodataval;
			break;
		}
		case PT_32BF: {
			band->nodataval = rt_util_clamp_to_32F(val);
			checkvalfloat = band->nodataval;
			break;
		}
		case PT_64BF: {
			band->nodataval = val;
			checkvaldouble = band->nodataval;
			break;
		}
		default: {
			rterror("rt_band_set_nodata: Unknown pixeltype %d", pixtype);
			band->hasnodata = 0;
			return ES_ERROR;
		}
	}

	RASTER_DEBUGF(3, "rt_band_set_nodata: band->hasnodata = %d", band->hasnodata);
	RASTER_DEBUGF(3, "rt_band_set_nodata: band->nodataval = %f", band->nodataval); 
	/* the nodata value was just set, so this band has NODATA */
	band->hasnodata = 1;

	/* also set isnodata flag to false */
	band->isnodata = 0; 

	if (rt_util_dbl_trunc_warning(
		val,
		checkvalint, checkvaluint,
		checkvalfloat, checkvaldouble,
		pixtype
	) && converted != NULL) {
		*converted = 1;
	}

	return ES_NONE;
}

/**
 * Set values of multiple pixels.  Unlike rt_band_set_pixel,
 * values in vals are expected to be of the band's pixel type
 * as this function uses memcpy.
 *
 * It is important to be careful when using this function as
 * the number of values being set may exceed a pixel "row".
 * Remember that the band values are stored in a stream (1-D array)
 * regardless of what the raster's width and height might be.
 * So, setting a number of values may cross multiple pixel "rows".
 *
 * @param band : the band to set value to
 * @param x : X coordinate (0-based)
 * @param y : Y coordinate (0-based)
 * @param vals : the pixel values to apply
 * @param len : # of elements in vals
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate
rt_band_set_pixel_line(
	rt_band band,
	int x, int y,
	void *vals, uint32_t len
) {
	rt_pixtype pixtype = PT_END;
	int size = 0;
	uint8_t *data = NULL;
	uint32_t offset = 0;

	assert(NULL != band);
	assert(vals != NULL && len > 0);

	RASTER_DEBUGF(3, "length of values = %d", len);

	if (band->offline) {
		rterror("rt_band_set_pixel_line not implemented yet for OFFDB bands");
		return ES_ERROR;
	}

	pixtype = band->pixtype;
	size = rt_pixtype_size(pixtype);

	if (
		x < 0 || x >= band->width ||
		y < 0 || y >= band->height
	) {
		rterror("rt_band_set_pixel_line: Coordinates out of range (%d, %d) vs (%d, %d)", x, y, band->width, band->height);
		return ES_ERROR;
	}

	data = rt_band_get_data(band);
	offset = x + (y * band->width);
	RASTER_DEBUGF(4, "offset = %d", offset);

	/* make sure len of values to copy don't exceed end of data */
	if (len > (band->width * band->height) - offset) {
		rterror("rt_band_set_pixel_line: Could not apply pixels as values length exceeds end of data");
		return ES_ERROR;
	}

	switch (pixtype) {
		case PT_1BB:
		case PT_2BUI:
		case PT_4BUI:
		case PT_8BUI:
		case PT_8BSI: {
			uint8_t *ptr = data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_16BUI: {
			uint16_t *ptr = (uint16_t *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_16BSI: {
			int16_t *ptr = (int16_t *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_32BUI: {
			uint32_t *ptr = (uint32_t *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_32BSI: {
			int32_t *ptr = (int32_t *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_32BF: {
			float *ptr = (float *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		case PT_64BF: {
			double *ptr = (double *) data;
			ptr += offset;
			memcpy(ptr, vals, size * len);
			break;
		}
		default: {
			rterror("rt_band_set_pixel_line: Unknown pixeltype %d", pixtype);
			return ES_ERROR;
		}
	}

#if POSTGIS_DEBUG_LEVEL > 0
	{
		double value;
		rt_band_get_pixel(band, x, y, &value, NULL);
		RASTER_DEBUGF(4, "pixel at (%d, %d) = %f", x, y, value);
	}
#endif

	/* set band's isnodata flag to FALSE */
	if (rt_band_get_hasnodata_flag(band))
		rt_band_set_isnodata_flag(band, 0);

	return ES_NONE;
}

/**
 * Set single pixel's value
 *
 * @param band : the band to set value to
 * @param x : x ordinate (0-based)
 * @param y : y ordinate (0-based)
 * @param val : the pixel value
 * @param converted : (optional) non-zero if value truncated/clamped/converted
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate
rt_band_set_pixel(
	rt_band band,
	int x, int y,
	double val,
	int *converted
) {
	rt_pixtype pixtype = PT_END;
	unsigned char* data = NULL;
	uint32_t offset = 0;

	int32_t checkvalint = 0;
	uint32_t checkvaluint = 0;
	float checkvalfloat = 0;
	double checkvaldouble = 0;

	assert(NULL != band);

	if (converted != NULL)
		*converted = 0;

	if (band->offline) {
		rterror("rt_band_set_pixel not implemented yet for OFFDB bands");
		return ES_ERROR;
	}

	pixtype = band->pixtype;

	if (
		x < 0 || x >= band->width ||
		y < 0 || y >= band->height
	) {
		rterror("rt_band_set_pixel: Coordinates out of range");
		return ES_ERROR;
	}

	/* check that clamped value isn't clamped NODATA */
	if (band->hasnodata && pixtype != PT_64BF) {
		double newval;
		int corrected;

		rt_band_corrected_clamped_value(band, val, &newval, &corrected);

		if (corrected) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
			rtwarn("Value for pixel %d x %d has been corrected as clamped value becomes NODATA", x, y);
#endif
			val = newval;

			if (converted != NULL)
				*converted = 1;
		}
	}

	data = rt_band_get_data(band);
	offset = x + (y * band->width);

	switch (pixtype) {
		case PT_1BB: {
			data[offset] = rt_util_clamp_to_1BB(val);
			checkvalint = data[offset];
			break;
		}
		case PT_2BUI: {
			data[offset] = rt_util_clamp_to_2BUI(val);
			checkvalint = data[offset];
			break;
		}
		case PT_4BUI: {
			data[offset] = rt_util_clamp_to_4BUI(val);
			checkvalint = data[offset];
			break;
		}
		case PT_8BSI: {
			data[offset] = rt_util_clamp_to_8BSI(val);
			checkvalint = (int8_t) data[offset];
			break;
		}
		case PT_8BUI: {
			data[offset] = rt_util_clamp_to_8BUI(val);
			checkvalint = data[offset];
			break;
		}
		case PT_16BSI: {
			int16_t *ptr = (int16_t*) data; /* we assume correct alignment */
			ptr[offset] = rt_util_clamp_to_16BSI(val);
			checkvalint = (int16_t) ptr[offset];
			break;
		}
		case PT_16BUI: {
			uint16_t *ptr = (uint16_t*) data; /* we assume correct alignment */
			ptr[offset] = rt_util_clamp_to_16BUI(val);
			checkvalint = ptr[offset];
			break;
		}
		case PT_32BSI: {
			int32_t *ptr = (int32_t*) data; /* we assume correct alignment */
			ptr[offset] = rt_util_clamp_to_32BSI(val);
			checkvalint = (int32_t) ptr[offset];
			break;
		}
		case PT_32BUI: {
			uint32_t *ptr = (uint32_t*) data; /* we assume correct alignment */
			ptr[offset] = rt_util_clamp_to_32BUI(val);
			checkvaluint = ptr[offset];
			break;
		}
		case PT_32BF: {
			float *ptr = (float*) data; /* we assume correct alignment */
			ptr[offset] = rt_util_clamp_to_32F(val);
			checkvalfloat = ptr[offset];
			break;
		}
		case PT_64BF: {
			double *ptr = (double*) data; /* we assume correct alignment */
			ptr[offset] = val;
			checkvaldouble = ptr[offset];
			break;
		}
		default: {
			rterror("rt_band_set_pixel: Unknown pixeltype %d", pixtype);
			return ES_ERROR;
		}
	}

	/* If the stored value is not NODATA, reset the isnodata flag */
	if (!rt_band_clamped_value_is_nodata(band, val)) {
		RASTER_DEBUG(3, "Band has a value that is not NODATA. Setting isnodata to FALSE");
		band->isnodata = FALSE;
	}

	/* Overflow checking */
	if (rt_util_dbl_trunc_warning(
		val,
		checkvalint, checkvaluint,
		checkvalfloat, checkvaldouble,
		pixtype
	) && converted != NULL) {
		*converted = 1;
	}

	return ES_NONE;
}

/**
 * Get values of multiple pixels.  Unlike rt_band_get_pixel,
 * values in vals are of the band's pixel type so cannot be
 * assumed to be double.  Function uses memcpy.
 *
 * It is important to be careful when using this function as
 * the number of values being fetched may exceed a pixel "row".
 * Remember that the band values are stored in a stream (1-D array)
 * regardless of what the raster's width and height might be.
 * So, getting a number of values may cross multiple pixel "rows".
 *
 * @param band : the band to get pixel value from
 * @param x : pixel column (0-based)
 * @param y : pixel row (0-based)
 * @param len : the number of pixels to get
 * @param **vals : the pixel values
 * @param *nvals : the number of pixel values being returned
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_band_get_pixel_line(
	rt_band band,
	int x, int y,
	uint16_t len,
	void **vals, uint16_t *nvals
) {
	uint8_t *_vals = NULL;
	int pixsize = 0;
	uint8_t *data = NULL;
	uint32_t offset = 0; 
	uint16_t _nvals = 0;
	int maxlen = 0;
	uint8_t *ptr = NULL;

	assert(NULL != band);
	assert(vals != NULL && nvals != NULL);

	/* initialize to no values */
	*nvals = 0;

	if (
		x < 0 || x >= band->width ||
		y < 0 || y >= band->height
	) {
		rtwarn("Attempting to get pixel values with out of range raster coordinates: (%d, %d)", x, y);
		return ES_ERROR;
	}

	if (len < 1)
		return ES_NONE;

	data = rt_band_get_data(band);
	if (data == NULL) {
		rterror("rt_band_get_pixel_line: Cannot get band data");
		return ES_ERROR;
	}

	/* +1 for the nodata value */
	offset = x + (y * band->width);
	RASTER_DEBUGF(4, "offset = %d", offset);

	pixsize = rt_pixtype_size(band->pixtype);
	RASTER_DEBUGF(4, "pixsize = %d", pixsize);

	/* cap _nvals so that it doesn't overflow */
	_nvals = len;
	maxlen = band->width * band->height;

	if (((int) (offset + _nvals)) > maxlen) {
		_nvals = maxlen - offset;
		rtwarn("Limiting returning number values to %d", _nvals);
	}
	RASTER_DEBUGF(4, "_nvals = %d", _nvals);

	ptr = data + (offset * pixsize);

	_vals = rtalloc(_nvals * pixsize);
	if (_vals == NULL) {
		rterror("rt_band_get_pixel_line: Could not allocate memory for pixel values");
		return ES_ERROR;
	}

	/* copy pixels */
	memcpy(_vals, ptr, _nvals * pixsize);

	*vals = _vals;
	*nvals = _nvals;

	return ES_NONE;
}

/**
 * Get pixel value. If band's isnodata flag is TRUE, value returned 
 * will be the band's NODATA value
 *
 * @param band : the band to set nodata value to
 * @param x : x ordinate (0-based)
 * @param y : x ordinate (0-based)
 * @param *value : pixel value
 * @param *nodata : 0 if pixel is not NODATA
 *
 * @return 0 on success, -1 on error (value out of valid range).
 */
rt_errorstate
rt_band_get_pixel(
	rt_band band,
	int x, int y,
	double *value,
	int *nodata
) {
	rt_pixtype pixtype = PT_END;
	uint8_t* data = NULL;
	uint32_t offset = 0; 

	assert(NULL != band);
	assert(NULL != value);

	/* set nodata to 0 */
	if (nodata != NULL)
		*nodata = 0;

	if (
		x < 0 || x >= band->width ||
		y < 0 || y >= band->height
	) {
		rtwarn("Attempting to get pixel value with out of range raster coordinates: (%d, %d)", x, y);
		return ES_ERROR;
	}

	/* band is NODATA */
	if (band->isnodata) {
		RASTER_DEBUG(3, "Band's isnodata flag is TRUE. Returning NODATA value");
		*value = band->nodataval;
		if (nodata != NULL) *nodata = 1;
		return ES_NONE;
	}

	data = rt_band_get_data(band);
	if (data == NULL) {
		rterror("rt_band_get_pixel: Cannot get band data");
		return ES_ERROR;
	}

	/* +1 for the nodata value */
	offset = x + (y * band->width);

	pixtype = band->pixtype;

	switch (pixtype) {
		case PT_1BB:
#ifdef OPTIMIZE_SPACE
			{
				int byteOffset = offset / 8;
				int bitOffset = offset % 8;
				data += byteOffset;

				/* Bit to set is bitOffset into data */
				*value = getBits(data, val, 1, bitOffset);
				break;
			}
#endif
		case PT_2BUI:
#ifdef OPTIMIZE_SPACE
			{
				int byteOffset = offset / 4;
				int bitOffset = offset % 4;
				data += byteOffset;

				/* Bits to set start at bitOffset into data */
				*value = getBits(data, val, 2, bitOffset);
				break;
			}
#endif
		case PT_4BUI:
#ifdef OPTIMIZE_SPACE
			{
				int byteOffset = offset / 2;
				int bitOffset = offset % 2;
				data += byteOffset;

				/* Bits to set start at bitOffset into data */
				*value = getBits(data, val, 2, bitOffset);
				break;
			}
#endif
		case PT_8BSI: {
			int8_t val = data[offset];
			*value = val;
			break;
		}
		case PT_8BUI: {
			uint8_t val = data[offset];
			*value = val;
			break;
		}
		case PT_16BSI: {
			int16_t *ptr = (int16_t*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		case PT_16BUI: {
			uint16_t *ptr = (uint16_t*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		case PT_32BSI: {
			int32_t *ptr = (int32_t*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		case PT_32BUI: {
			uint32_t *ptr = (uint32_t*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		case PT_32BF: {
			float *ptr = (float*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		case PT_64BF: {
			double *ptr = (double*) data; /* we assume correct alignment */
			*value = ptr[offset];
			break;
		}
		default: {
			rterror("rt_band_get_pixel: Unknown pixeltype %d", pixtype);
			return ES_ERROR;
		}
	}

	/* set NODATA flag */
	if (band->hasnodata && nodata != NULL) {
		if (rt_band_clamped_value_is_nodata(band, *value))
			*nodata = 1;
	}

	return ES_NONE;
}

/**
 * Get nearest pixel(s) with value (not NODATA) to specified pixel
 *
 * @param band : the band to get nearest pixel(s) from
 * @param x : the column of the pixel (0-based)
 * @param y : the line of the pixel (0-based)
 * @param distancex : the number of pixels around the specified pixel
 * along the X axis
 * @param distancey : the number of pixels around the specified pixel
 * along the Y axis
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * to check for pixels with value
 * @param npixels : return set of rt_pixel object or NULL
 *
 * @return -1 on error, otherwise the number of rt_pixel objects
 * in npixels
 */
int rt_band_get_nearest_pixel(
	rt_band band,
	int x, int y,
	uint16_t distancex, uint16_t distancey,
	int exclude_nodata_value,
	rt_pixel *npixels
) {
	rt_pixel npixel = NULL;
	int extent[4] = {0};
	int max_extent[4] = {0};
	int d0 = 0;
	int distance[2] = {0};
	uint32_t _d[2] = {0};
	uint32_t i = 0;
	uint32_t j = 0;
	uint32_t k = 0;
	int _max = 0;
	int _x = 0;
	int _y = 0;
	int *_min = NULL;
	double pixval = 0;
	double minval = 0;
	uint32_t count = 0;
	int isnodata = 0;

	int inextent = 0;

	assert(NULL != band);
	assert(NULL != npixels);

	RASTER_DEBUG(3, "Starting");

	/* process distance */
	distance[0] = distancex;
	distance[1] = distancey;

	/* no distance, means get nearest pixels and return */
	if (!distance[0] && !distance[1])
		d0 = 1;

	RASTER_DEBUGF(4, "Selected pixel: %d x %d", x, y);
	RASTER_DEBUGF(4, "Distances: %d x %d", distance[0], distance[1]);

	/* shortcuts if outside band extent */
	if (
		exclude_nodata_value && (
			(x < 0 || x > band->width) ||
			(y < 0 || y > band->height)
		)
	) {
		/* no distances specified, jump to pixel close to extent */
		if (d0) {
			if (x < 0)
				x = -1;
			else if (x > band->width)
				x = band->width;

			if (y < 0)
				y = -1;
			else if (y > band->height)
				y = band->height;

			RASTER_DEBUGF(4, "Moved selected pixel: %d x %d", x, y);
		}
		/*
			distances specified
			if distances won't capture extent of band, return 0
		*/
		else if (
			((x < 0 && abs(x) > distance[0]) || (x - band->width >= distance[0])) ||
			((y < 0 && abs(y) > distance[1]) || (y - band->height >= distance[1]))
		) {
			RASTER_DEBUG(4, "No nearest pixels possible for provided pixel and distances");
			return 0;
		}
	}

	/* no NODATA, exclude is FALSE */
	if (!band->hasnodata)
		exclude_nodata_value = FALSE;
	/* band is NODATA and excluding NODATA */
	else if (exclude_nodata_value && band->isnodata) {
		RASTER_DEBUG(4, "No nearest pixels possible as band is NODATA and excluding NODATA values");
		return 0;
	}

	/* determine the maximum distance to prevent an infinite loop */
	if (d0) {
		int a, b;

		/* X axis */
		a = abs(x);
		b = abs(x - band->width);

		if (a > b)
			distance[0] = a;
		else
			distance[0] = b;

		/* Y axis */
		a = abs(y);
		b = abs(y - band->height);
		if (a > b)
			distance[1] = a;
		else
			distance[1] = b;

		RASTER_DEBUGF(4, "Maximum distances: %d x %d", distance[0], distance[1]);
	}

	/* minimum possible value for pixel type */
	minval = rt_pixtype_get_min_value(band->pixtype);
	RASTER_DEBUGF(4, "pixtype: %s", rt_pixtype_name(band->pixtype));
	RASTER_DEBUGF(4, "minval: %f", minval);

	/* set variables */
	count = 0;
	*npixels = NULL;

	/* maximum extent */
	max_extent[0] = x - distance[0]; /* min X */
	max_extent[1] = y - distance[1]; /* min Y */
	max_extent[2] = x + distance[0]; /* max X */
	max_extent[3] = y + distance[1]; /* max Y */
	RASTER_DEBUGF(4, "Maximum Extent: (%d, %d, %d, %d)",
		max_extent[0], max_extent[1], max_extent[2], max_extent[3]);

	_d[0] = 0;
	_d[1] = 0;
	do {
		_d[0]++;
		_d[1]++;

		extent[0] = x - _d[0]; /* min x */
		extent[1] = y - _d[1]; /* min y */
		extent[2] = x + _d[0]; /* max x */
		extent[3] = y + _d[1]; /* max y */

		RASTER_DEBUGF(4, "Processing distances: %d x %d", _d[0], _d[1]);
		RASTER_DEBUGF(4, "Extent: (%d, %d, %d, %d)",
			extent[0], extent[1], extent[2], extent[3]);

		for (i = 0; i < 2; i++) {

			/* by row */
			if (i < 1)
				_max = extent[2] - extent[0] + 1;
			/* by column */
			else
				_max = extent[3] - extent[1] + 1;
			_max = abs(_max);

			for (j = 0; j < 2; j++) {
				/* by row */
				if (i < 1) {
					_x = extent[0];
					_min = &_x;

					/* top row */
					if (j < 1)
						_y = extent[1];
					/* bottom row */
					else
						_y = extent[3];
				}
				/* by column */
				else {
					_y = extent[1] + 1;
					_min = &_y;

					/* left column */
					if (j < 1) {
						_x = extent[0];
						_max -= 2;
					}
					/* right column */
					else
						_x = extent[2];
				}

				RASTER_DEBUGF(4, "_min, _max: %d, %d", *_min, _max);
				for (k = 0; k < _max; k++) {
					/* check that _x and _y are not outside max extent */
					if (
						_x < max_extent[0] || _x > max_extent[2] ||
						_y < max_extent[1] || _y > max_extent[3]
					) {
						(*_min)++;
						continue;
					}

					/* outside band extent, set to NODATA */
					if (
						(_x < 0 || _x >= band->width) ||
						(_y < 0 || _y >= band->height)
					) {
						/* no NODATA, set to minimum possible value */
						if (!band->hasnodata)
							pixval = minval;
						/* has NODATA, use NODATA */
						else
							pixval = band->nodataval;
						RASTER_DEBUGF(4, "NODATA pixel outside band extent: (x, y, val) = (%d, %d, %f)", _x, _y, pixval);
						inextent = 0;
						isnodata = 1;
					}
					else {
						if (rt_band_get_pixel(
							band,
							_x, _y,
							&pixval,
							&isnodata
						) != ES_NONE) {
							rterror("rt_band_get_nearest_pixel: Could not get pixel value");
							if (count) rtdealloc(*npixels);
							return -1;
						}
						RASTER_DEBUGF(4, "Pixel: (x, y, val) = (%d, %d, %f)", _x, _y, pixval);
						inextent = 1;
					}

					/* use pixval? */
					if (!exclude_nodata_value || (exclude_nodata_value && !isnodata)) {
						/* add pixel to result set */
						RASTER_DEBUGF(4, "Adding pixel to set of nearest pixels: (x, y, val) = (%d, %d, %f)", _x, _y, pixval);
						count++;

						if (*npixels == NULL)
							*npixels = (rt_pixel) rtalloc(sizeof(struct rt_pixel_t) * count);
						else
							*npixels = (rt_pixel) rtrealloc(*npixels, sizeof(struct rt_pixel_t) * count);
						if (*npixels == NULL) {
							rterror("rt_band_get_nearest_pixel: Could not allocate memory for nearest pixel(s)");
							return -1;
						}

						npixel = &((*npixels)[count - 1]);
						npixel->x = _x;
						npixel->y = _y;
						npixel->value = pixval;

						/* special case for when outside band extent */
						if (!inextent && !band->hasnodata)
							npixel->nodata = 1;
						else
							npixel->nodata = 0;
					}

					(*_min)++;
				}
			}
		}

		/* distance threshholds met */
		if (_d[0] >= distance[0] && _d[1] >= distance[1])
			break;
		else if (d0 && count)
			break;
	}
	while (1);

	RASTER_DEBUGF(3, "Nearest pixels in return: %d", count);

	return count;
}

/**
 * Search band for pixel(s) with search values
 *
 * @param band : the band to query for minimum and maximum pixel values
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * @param searchset : array of values to count
 * @param searchcount : the number of search values
 * @param pixels : pixels with the search value
 *
 * @return -1 on error, otherwise number of pixels
 */
int
rt_band_get_pixel_of_value(
	rt_band band, int exclude_nodata_value,
	double *searchset, int searchcount,
	rt_pixel *pixels
) {
	int x;
	int y;
	int i;
	double pixval;
	int err;
	int count = 0;
	int isnodata = 0;
	int isequal = 0;

	rt_pixel pixel = NULL;

	assert(NULL != band);
	assert(NULL != pixels);
	assert(NULL != searchset && searchcount > 0);

	if (!band->hasnodata)
		exclude_nodata_value = FALSE;
	/* band is NODATA and exclude_nodata_value = TRUE, nothing to search */
	else if (exclude_nodata_value && band->isnodata) {
		RASTER_DEBUG(4, "Pixels cannot be searched as band is NODATA and excluding NODATA values");
		return 0;
	}

	for (x = 0; x < band->width; x++) {
		for (y = 0; y < band->height; y++) {
			err = rt_band_get_pixel(band, x, y, &pixval, &isnodata);
			if (err != ES_NONE) {
				rterror("rt_band_get_pixel_of_value: Cannot get band pixel");
				return -1;
			}
			else if (exclude_nodata_value && isnodata)
				continue;

			for (i = 0; i < searchcount; i++) {
				if (rt_pixtype_compare_clamped_values(band->pixtype, searchset[i], pixval, &isequal) != ES_NONE) {
					continue;
				}

				if (FLT_NEQ(pixval, searchset[i]) || !isequal)
					continue;

				/* match found */
				count++;
				if (*pixels == NULL)
					*pixels = (rt_pixel) rtalloc(sizeof(struct rt_pixel_t) * count);
				else
					*pixels = (rt_pixel) rtrealloc(*pixels, sizeof(struct rt_pixel_t) * count);
				if (*pixels == NULL) {
					rterror("rt_band_get_pixel_of_value: Could not allocate memory for pixel(s)");
					return -1;
				}

				pixel = &((*pixels)[count - 1]);
				pixel->x = x;
				pixel->y = y;
				pixel->nodata = 0;
				pixel->value = pixval;
			}
		}
	}

	return count;
}

/**
 * Get NODATA value
 *
 * @param band : the band whose NODATA value will be returned
 * @param nodata : the band's NODATA value
 *
 * @return ES_NONE or ES_ERROR
 */
rt_errorstate
rt_band_get_nodata(rt_band band, double *nodata) { 
	assert(NULL != band);
	assert(NULL != nodata);

	*nodata = band->nodataval;

	if (!band->hasnodata) {
		rterror("rt_band_get_nodata: Band has no NODATA value");
		return ES_ERROR;
	}

	return ES_NONE;
}

double
rt_band_get_min_value(rt_band band) {
	assert(NULL != band);

	return rt_pixtype_get_min_value(band->pixtype);
}

int
rt_band_check_is_nodata(rt_band band) {
	int i, j, err;
	double pxValue;
	int isnodata = 0;

	assert(NULL != band);

	/* Check if band has nodata value */
	if (!band->hasnodata) {
		RASTER_DEBUG(3, "Band has no NODATA value");
		band->isnodata = FALSE;
		return FALSE;
	}

	pxValue = band->nodataval;

	/* Check all pixels */
	for (i = 0; i < band->width; i++) {
		for (j = 0; j < band->height; j++) {
			err = rt_band_get_pixel(band, i, j, &pxValue, &isnodata);
			if (err != ES_NONE) {
				rterror("rt_band_check_is_nodata: Cannot get band pixel");
				return FALSE;
			}
			else if (!isnodata) {
				band->isnodata = FALSE;
				return FALSE;
			}
		}
	}

	band->isnodata = TRUE;
	return TRUE;
}

/**
 * Compare clamped value to band's clamped NODATA value.
 *
 * @param band : the band whose NODATA value will be used for comparison
 * @param val : the value to compare to the NODATA value
 *
 * @return 2 if unclamped value is unclamped NODATA
 *         1 if clamped value is clamped NODATA
 *         0 if clamped value is NOT clamped NODATA
 */
int
rt_band_clamped_value_is_nodata(rt_band band, double val) {
	int isequal = 0;

	assert(NULL != band);

	/* no NODATA, so never equal */
	if (!band->hasnodata)
		return 0;

	/* value is exactly NODATA */
	if (FLT_EQ(val, band->nodataval))
		return 2;

	/* ignore error from rt_pixtype_compare_clamped_values */
	rt_pixtype_compare_clamped_values(
		band->pixtype,
		val, band->nodataval,
		&isequal
	);

	return isequal ? 1 : 0;
}

/**
 * Correct value when clamped value is equal to clamped NODATA value.
 * Correction does NOT occur if unclamped value is exactly unclamped
 * NODATA value.
 * 
 * @param band : the band whose NODATA value will be used for comparison
 * @param val : the value to compare to the NODATA value and correct
 * @param *newval : pointer to corrected value
 * @param *corrected : (optional) non-zero if val was corrected
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_band_corrected_clamped_value(
	rt_band band,
	double val,
	double *newval, int *corrected
) {
	double minval = 0.;

	assert(NULL != band);
	assert(NULL != newval);

	if (corrected != NULL)
		*corrected = 0;

	/* no need to correct if clamped values IS NOT clamped NODATA */
	if (rt_band_clamped_value_is_nodata(band, val) != 1) {
		*newval = val;
		return ES_NONE;
	}

	minval = rt_pixtype_get_min_value(band->pixtype);
	*newval = val;

	switch (band->pixtype) {
		case PT_1BB:
			*newval = !band->nodataval;
			break;
		case PT_2BUI:
			if (rt_util_clamp_to_2BUI(val) == rt_util_clamp_to_2BUI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_4BUI:
			if (rt_util_clamp_to_4BUI(val) == rt_util_clamp_to_4BUI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_8BSI:
			if (rt_util_clamp_to_8BSI(val) == rt_util_clamp_to_8BSI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_8BUI:
			if (rt_util_clamp_to_8BUI(val) == rt_util_clamp_to_8BUI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_16BSI:
			if (rt_util_clamp_to_16BSI(val) == rt_util_clamp_to_16BSI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_16BUI:
			if (rt_util_clamp_to_16BUI(val) == rt_util_clamp_to_16BUI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_32BSI:
			if (rt_util_clamp_to_32BSI(val) == rt_util_clamp_to_32BSI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_32BUI:
			if (rt_util_clamp_to_32BUI(val) == rt_util_clamp_to_32BUI(minval))
				(*newval)++;
			else
				(*newval)--;
			break;
		case PT_32BF:
			if (FLT_EQ(rt_util_clamp_to_32F(val), rt_util_clamp_to_32F(minval)))
				*newval += FLT_EPSILON;
			else
				*newval -= FLT_EPSILON;
			break;
		case PT_64BF:
			break;
		default:
			rterror("rt_band_corrected_clamped_value: Unknown pixeltype %d", band->pixtype);
			return ES_ERROR;
	}

	if (corrected != NULL)
		*corrected = 1;

	return ES_NONE;
}

