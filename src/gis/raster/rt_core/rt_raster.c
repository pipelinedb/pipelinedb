/*
 *
 * WKTRaster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2013 Bborie Park <dustymugs@gmail.com> 
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

#include <math.h>

/**
 * Construct a raster with given dimensions.
 *
 * Transform will be set to identity.
 * Will contain no bands.
 *
 * @param width : number of pixel columns
 * @param height : number of pixel rows
 *
 * @return an rt_raster or NULL if out of memory
 */
rt_raster
rt_raster_new(uint32_t width, uint32_t height) {
	rt_raster ret = NULL;

	ret = (rt_raster) rtalloc(sizeof (struct rt_raster_t));
	if (!ret) {
		rterror("rt_raster_new: Out of virtual memory creating an rt_raster");
		return NULL;
	}

	RASTER_DEBUGF(3, "Created rt_raster @ %p", ret);

	if (width > 65535 || height > 65535) {
		rterror("rt_raster_new: Dimensions requested exceed the maximum (65535 x 65535) permitted for a raster");
		rt_raster_destroy(ret);
		return NULL;
	}

	ret->width = width;
	ret->height = height;
	ret->scaleX = 1;
	ret->scaleY = -1;
	ret->ipX = 0.0;
	ret->ipY = 0.0;
	ret->skewX = 0.0;
	ret->skewY = 0.0;
	ret->srid = SRID_UNKNOWN;

	ret->numBands = 0;
	ret->bands = NULL; 

	return ret;
}

void
rt_raster_destroy(rt_raster raster) {
	if (raster == NULL)
		return;

	RASTER_DEBUGF(3, "Destroying rt_raster @ %p", raster);

	if (raster->bands)
		rtdealloc(raster->bands);

	rtdealloc(raster);
}

static void
_rt_raster_geotransform_warn_offline_band(rt_raster raster) {
	int numband = 0;
	int i = 0;
	rt_band band = NULL;

	if (raster == NULL)
		return;

	numband = rt_raster_get_num_bands(raster);
	if (numband < 1)
		return;

	for (i = 0; i < numband; i++) {
		band = rt_raster_get_band(raster, i);
		if (NULL == band)
			continue;

		if (!rt_band_is_offline(band))
			continue;

		rtwarn("Changes made to raster geotransform matrix may affect out-db band data. Returned band data may be incorrect");
		break;
	}
}

uint16_t
rt_raster_get_width(rt_raster raster) {

    assert(NULL != raster);

    return raster->width;
}

uint16_t
rt_raster_get_height(rt_raster raster) {

    assert(NULL != raster);

    return raster->height;
}

void
rt_raster_set_scale(
	rt_raster raster,
	double scaleX, double scaleY
) {
	assert(NULL != raster);

	raster->scaleX = scaleX;
	raster->scaleY = scaleY;

	_rt_raster_geotransform_warn_offline_band(raster);
}

double
rt_raster_get_x_scale(rt_raster raster) {


    assert(NULL != raster);

    return raster->scaleX;
}

double
rt_raster_get_y_scale(rt_raster raster) {


    assert(NULL != raster);

    return raster->scaleY;
}

void
rt_raster_set_skews(
	rt_raster raster,
	double skewX, double skewY
) {
	assert(NULL != raster);

	raster->skewX = skewX;
	raster->skewY = skewY;

	_rt_raster_geotransform_warn_offline_band(raster);
}

double
rt_raster_get_x_skew(rt_raster raster) {


    assert(NULL != raster);

    return raster->skewX;
}

double
rt_raster_get_y_skew(rt_raster raster) {


    assert(NULL != raster);

    return raster->skewY;
}

void
rt_raster_set_offsets(
	rt_raster raster,
	double x, double y
) {

	assert(NULL != raster);

	raster->ipX = x;
	raster->ipY = y;

	_rt_raster_geotransform_warn_offline_band(raster);
}

double
rt_raster_get_x_offset(rt_raster raster) {


    assert(NULL != raster);

    return raster->ipX;
}

double
rt_raster_get_y_offset(rt_raster raster) {


    assert(NULL != raster);

    return raster->ipY;
}

void
rt_raster_get_phys_params(rt_raster rast,
        double *i_mag, double *j_mag, double *theta_i, double *theta_ij)
{
    double o11, o12, o21, o22 ; /* geotransform coefficients */

    if (rast == NULL) return ;
    if ( (i_mag==NULL) || (j_mag==NULL) || (theta_i==NULL) || (theta_ij==NULL))
        return ;

    /* retrieve coefficients from raster */
    o11 = rt_raster_get_x_scale(rast) ;
    o12 = rt_raster_get_x_skew(rast) ;
    o21 = rt_raster_get_y_skew(rast) ;
    o22 = rt_raster_get_y_scale(rast) ;

    rt_raster_calc_phys_params(o11, o12, o21, o22, i_mag, j_mag, theta_i, theta_ij);
}

void
rt_raster_calc_phys_params(double xscale, double xskew, double yskew, double yscale,
                           double *i_mag, double *j_mag, double *theta_i, double *theta_ij)

{
    double theta_test ;

    if ( (i_mag==NULL) || (j_mag==NULL) || (theta_i==NULL) || (theta_ij==NULL))
        return ;

    /* pixel size in the i direction */
    *i_mag = sqrt(xscale*xscale + yskew*yskew) ;

    /* pixel size in the j direction */
    *j_mag = sqrt(xskew*xskew + yscale*yscale) ;

    /* Rotation
     * ========
     * Two steps:
     * 1] calculate the magnitude of the angle between the x axis and
     *     the i basis vector.
     * 2] Calculate the sign of theta_i based on the angle between the y axis
     *     and the i basis vector.
     */
    *theta_i = acos(xscale/(*i_mag)) ;  /* magnitude */
    theta_test = acos(yskew/(*i_mag)) ; /* sign */
    if (theta_test < M_PI_2){
        *theta_i = -(*theta_i) ;
    }


    /* Angular separation of basis vectors
     * ===================================
     * Two steps:
     * 1] calculate the magnitude of the angle between the j basis vector and
     *     the i basis vector.
     * 2] Calculate the sign of theta_ij based on the angle between the
     *    perpendicular of the i basis vector and the j basis vector.
     */
    *theta_ij = acos(((xscale*xskew) + (yskew*yscale))/((*i_mag)*(*j_mag))) ;
    theta_test = acos( ((-yskew*xskew)+(xscale*yscale)) /
            ((*i_mag)*(*j_mag)));
    if (theta_test > M_PI_2) {
        *theta_ij = -(*theta_ij) ;
    }
}

void
rt_raster_set_phys_params(rt_raster rast,double i_mag, double j_mag, double theta_i, double theta_ij)
{
    double o11, o12, o21, o22 ; /* calculated geotransform coefficients */
    int success ;

    if (rast == NULL) return ;

    success = rt_raster_calc_gt_coeff(i_mag, j_mag, theta_i, theta_ij,
                            &o11, &o12, &o21, &o22) ;

    if (success) {
        rt_raster_set_scale(rast, o11, o22) ;
        rt_raster_set_skews(rast, o12, o21) ;
    }
}

int
rt_raster_calc_gt_coeff(double i_mag, double j_mag, double theta_i, double theta_ij,
                        double *xscale, double *xskew, double *yskew, double *yscale)
{
    double f ;        /* reflection flag 1.0 or -1.0 */
    double k_i ;      /* shearing coefficient */
    double s_i, s_j ; /* scaling coefficients */
    double cos_theta_i, sin_theta_i ;

    if ( (xscale==NULL) || (xskew==NULL) || (yskew==NULL) || (yscale==NULL)) {
        return 0;
    }

    if ( (theta_ij == 0.0) || (theta_ij == M_PI)) {
        return 0;
    }

    /* Reflection across the i axis */
    f=1.0 ;
    if (theta_ij < 0) {
        f = -1.0;
    }

    /* scaling along i axis */
    s_i = i_mag ;

    /* shearing parallel to i axis */
    k_i = tan(f*M_PI_2 - theta_ij) ;

    /* scaling along j axis */
    s_j = j_mag / (sqrt(k_i*k_i + 1)) ;

    /* putting it altogether */
    cos_theta_i = cos(theta_i) ;
    sin_theta_i = sin(theta_i) ;
    *xscale = s_i * cos_theta_i ;
    *xskew  = k_i * s_j * f * cos_theta_i + s_j * f * sin_theta_i ;
    *yskew  = -s_i * sin_theta_i ;
    *yscale = -k_i * s_j * f * sin_theta_i + s_j * f * cos_theta_i ;
    return 1;
}

int32_t
rt_raster_get_srid(rt_raster raster) {
	assert(NULL != raster);

	return clamp_srid(raster->srid);
}

void
rt_raster_set_srid(rt_raster raster, int32_t srid) {
	assert(NULL != raster);

	raster->srid = clamp_srid(srid);

	_rt_raster_geotransform_warn_offline_band(raster);
}

int
rt_raster_get_num_bands(rt_raster raster) {


    assert(NULL != raster);

    return raster->numBands;
}

rt_band
rt_raster_get_band(rt_raster raster, int n) {
	assert(NULL != raster);

	if (n >= raster->numBands || n < 0)
		return NULL;

	return raster->bands[n];
}

/******************************************************************************
* rt_raster_add_band()
******************************************************************************/

/**
 * Add band data to a raster.
 *
 * @param raster : the raster to add a band to
 * @param band : the band to add, ownership left to caller.
 *               Band dimensions are required to match with raster ones.
 * @param index : the position where to insert the new band (0 based)
 *
 * @return identifier (position) for the just-added raster, or -1 on error
 */
int
rt_raster_add_band(rt_raster raster, rt_band band, int index) {
    rt_band *oldbands = NULL;
    rt_band oldband = NULL;
    rt_band tmpband = NULL;
    uint16_t i = 0;

    assert(NULL != raster);
		assert(NULL != band);

    RASTER_DEBUGF(3, "Adding band %p to raster %p", band, raster);

    if (band->width != raster->width || band->height != raster->height) {
        rterror("rt_raster_add_band: Can't add a %dx%d band to a %dx%d raster",
                band->width, band->height, raster->width, raster->height);
        return -1;
    }

    if (index > raster->numBands)
        index = raster->numBands;

    if (index < 0)
        index = 0;

    oldbands = raster->bands;

    RASTER_DEBUGF(3, "Oldbands at %p", oldbands);

    raster->bands = (rt_band*) rtrealloc(raster->bands,
            sizeof (rt_band)*(raster->numBands + 1)
            );

    RASTER_DEBUG(3, "Checking bands");

    if (NULL == raster->bands) {
        rterror("rt_raster_add_band: Out of virtual memory "
                "reallocating band pointers");
        raster->bands = oldbands;
        return -1;
    }

    RASTER_DEBUGF(4, "realloc returned %p", raster->bands);

    for (i = 0; i <= raster->numBands; ++i) {
        if (i == index) {
            oldband = raster->bands[i];
            raster->bands[i] = band;
        } else if (i > index) {
            tmpband = raster->bands[i];
            raster->bands[i] = oldband;
            oldband = tmpband;
        }
    }

		band->raster = raster;

    raster->numBands++;

    RASTER_DEBUGF(4, "Raster now has %d bands", raster->numBands);

    return index;
}

/******************************************************************************
* rt_raster_generate_new_band()
******************************************************************************/

/**
 * Generate a new inline band and add it to a raster.
 * Memory is allocated in this function for band data.
 *
 * @param raster : the raster to add a band to
 * @param pixtype : the pixel type for the new band
 * @param initialvalue : initial value for pixels
 * @param hasnodata : indicates if the band has a nodata value
 * @param nodatavalue : nodata value for the new band
 * @param index : position to add the new band in the raster
 *
 * @return identifier (position) for the just-added raster, or -1 on error
 */
int
rt_raster_generate_new_band(
	rt_raster raster, rt_pixtype pixtype,
	double initialvalue, uint32_t hasnodata, double nodatavalue,
	int index
) {
    rt_band band = NULL;
    int width = 0;
    int height = 0;
    int numval = 0;
    int datasize = 0;
    int oldnumbands = 0;
    int numbands = 0;
    void * mem = NULL;
    int32_t checkvalint = 0;
    uint32_t checkvaluint = 0;
    double checkvaldouble = 0;
    float checkvalfloat = 0;
    int i;


    assert(NULL != raster);

    /* Make sure index is in a valid range */
    oldnumbands = rt_raster_get_num_bands(raster);
    if (index < 0)
        index = 0;
    else if (index > oldnumbands + 1)
        index = oldnumbands + 1;

    /* Determine size of memory block to allocate and allocate it */
    width = rt_raster_get_width(raster);
    height = rt_raster_get_height(raster);
    numval = width * height;
    datasize = rt_pixtype_size(pixtype) * numval;

    mem = (int *)rtalloc(datasize);
    if (!mem) {
        rterror("rt_raster_generate_new_band: Could not allocate memory for band");
        return -1;
    }

    if (FLT_EQ(initialvalue, 0.0))
        memset(mem, 0, datasize);
    else {
        switch (pixtype)
        {
            case PT_1BB:
            {
                uint8_t *ptr = mem;
                uint8_t clamped_initval = rt_util_clamp_to_1BB(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_2BUI:
            {
                uint8_t *ptr = mem;
                uint8_t clamped_initval = rt_util_clamp_to_2BUI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_4BUI:
            {
                uint8_t *ptr = mem;
                uint8_t clamped_initval = rt_util_clamp_to_4BUI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_8BSI:
            {
                int8_t *ptr = mem;
                int8_t clamped_initval = rt_util_clamp_to_8BSI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_8BUI:
            {
                uint8_t *ptr = mem;
                uint8_t clamped_initval = rt_util_clamp_to_8BUI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_16BSI:
            {
                int16_t *ptr = mem;
                int16_t clamped_initval = rt_util_clamp_to_16BSI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_16BUI:
            {
                uint16_t *ptr = mem;
                uint16_t clamped_initval = rt_util_clamp_to_16BUI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_32BSI:
            {
                int32_t *ptr = mem;
                int32_t clamped_initval = rt_util_clamp_to_32BSI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalint = ptr[0];
                break;
            }
            case PT_32BUI:
            {
                uint32_t *ptr = mem;
                uint32_t clamped_initval = rt_util_clamp_to_32BUI(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvaluint = ptr[0];
                break;
            }
            case PT_32BF:
            {
                float *ptr = mem;
                float clamped_initval = rt_util_clamp_to_32F(initialvalue);
                for (i = 0; i < numval; i++)
                    ptr[i] = clamped_initval;
                checkvalfloat = ptr[0];
                break;
            }
            case PT_64BF:
            {
                double *ptr = mem;
                for (i = 0; i < numval; i++)
                    ptr[i] = initialvalue;
                checkvaldouble = ptr[0];
                break;
            }
            default:
            {
                rterror("rt_raster_generate_new_band: Unknown pixeltype %d", pixtype);
                rtdealloc(mem);
                return -1;
            }
        }
    }

    /* Overflow checking */
    rt_util_dbl_trunc_warning(
			initialvalue,
			checkvalint, checkvaluint,
			checkvalfloat, checkvaldouble,
			pixtype
		);

    band = rt_band_new_inline(width, height, pixtype, hasnodata, nodatavalue, mem);
    if (! band) {
        rterror("rt_raster_generate_new_band: Could not add band to raster. Aborting");
        rtdealloc(mem);
        return -1;
    }
		rt_band_set_ownsdata_flag(band, 1); /* we DO own this data!!! */
    index = rt_raster_add_band(raster, band, index);
    numbands = rt_raster_get_num_bands(raster);
    if (numbands == oldnumbands || index == -1) {
        rterror("rt_raster_generate_new_band: Could not add band to raster. Aborting");
        rt_band_destroy(band);
    }

		/* set isnodata if hasnodata = TRUE and initial value = nodatavalue */
		if (hasnodata && FLT_EQ(initialvalue, nodatavalue))
			rt_band_set_isnodata_flag(band, 1);

    return index;
}

/**
 * Get 6-element array of raster inverse geotransform matrix
 *
 * @param raster : the raster to get matrix of
 * @param gt : optional input parameter, 6-element geotransform matrix
 * @param igt : output parameter, 6-element inverse geotransform matrix
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_get_inverse_geotransform_matrix(
	rt_raster raster,
	double *gt, double *igt
) {
	double _gt[6] = {0};

	assert((raster != NULL || gt != NULL));
	assert(igt != NULL);

	if (gt == NULL)
		rt_raster_get_geotransform_matrix(raster, _gt);
	else
		memcpy(_gt, gt, sizeof(double) * 6);
	
	if (!GDALInvGeoTransform(_gt, igt)) {
		rterror("rt_raster_get_inverse_geotransform_matrix: Could not compute inverse geotransform matrix");
		return ES_ERROR;
	}

	return ES_NONE;
}

/**
 * Get 6-element array of raster geotransform matrix
 *
 * @param raster : the raster to get matrix of
 * @param gt : output parameter, 6-element geotransform matrix
 *
 */
void
rt_raster_get_geotransform_matrix(rt_raster raster,
	double *gt) {
	assert(NULL != raster);
	assert(NULL != gt);

	gt[0] = raster->ipX;
	gt[1] = raster->scaleX;
	gt[2] = raster->skewX;
	gt[3] = raster->ipY;
	gt[4] = raster->skewY;
	gt[5] = raster->scaleY;
}

/**
 * Set raster's geotransform using 6-element array
 *
 * @param raster : the raster to set matrix of
 * @param gt : intput parameter, 6-element geotransform matrix
 *
 */
void
rt_raster_set_geotransform_matrix(rt_raster raster,
	double *gt) {
	assert(NULL != raster);
	assert(NULL != gt);

	raster->ipX = gt[0];
	raster->scaleX = gt[1];
	raster->skewX = gt[2];
	raster->ipY = gt[3];
	raster->skewY = gt[4];
	raster->scaleY = gt[5];

	_rt_raster_geotransform_warn_offline_band(raster);
}

/**
 * Convert an xr, yr raster point to an xw, yw point on map
 *
 * @param raster : the raster to get info from
 * @param xr : the pixel's column
 * @param yr : the pixel's row
 * @param xw : output parameter, X ordinate of the geographical point
 * @param yw : output parameter, Y ordinate of the geographical point
 * @param gt : input/output parameter, 3x2 geotransform matrix
 *
 * @return ES_NONE if success, ES_ERROR if error 
 */
rt_errorstate
rt_raster_cell_to_geopoint(
	rt_raster raster,
	double xr, double yr,
	double *xw, double *yw,
	double *gt
) {
	double _gt[6] = {0};

	assert(NULL != raster);
	assert(NULL != xw && NULL != yw);

	if (NULL != gt)
		memcpy(_gt, gt, sizeof(double) * 6);

	/* scale of matrix is not set */
	if (
		FLT_EQ(_gt[1], 0) ||
		FLT_EQ(_gt[5], 0)
	) {
		rt_raster_get_geotransform_matrix(raster, _gt);
	}

	RASTER_DEBUGF(4, "gt = (%f, %f, %f, %f, %f, %f)",
		_gt[0],
		_gt[1],
		_gt[2],
		_gt[3],
		_gt[4],
		_gt[5]
	);

	GDALApplyGeoTransform(_gt, xr, yr, xw, yw);
	RASTER_DEBUGF(4, "GDALApplyGeoTransform (c -> g) for (%f, %f) = (%f, %f)",
		xr, yr, *xw, *yw);

	return ES_NONE;
}

/**
 * Convert an xw,yw map point to a xr,yr raster point
 *
 * @param raster : the raster to get info from
 * @param xw : X ordinate of the geographical point
 * @param yw : Y ordinate of the geographical point
 * @param xr : output parameter, the pixel's column
 * @param yr : output parameter, the pixel's row
 * @param igt : input/output parameter, inverse geotransform matrix
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_geopoint_to_cell(
	rt_raster raster,
	double xw, double yw,
	double *xr, double *yr,
	double *igt
) {
	double _igt[6] = {0};
	double rnd = 0;

	assert(NULL != raster);
	assert(NULL != xr && NULL != yr);

	if (igt != NULL)
		memcpy(_igt, igt, sizeof(double) * 6);

	/* matrix is not set */
	if (
		FLT_EQ(_igt[0], 0.) &&
		FLT_EQ(_igt[1], 0.) &&
		FLT_EQ(_igt[2], 0.) &&
		FLT_EQ(_igt[3], 0.) &&
		FLT_EQ(_igt[4], 0.) &&
		FLT_EQ(_igt[5], 0.)
	) {
		if (rt_raster_get_inverse_geotransform_matrix(raster, NULL, _igt) != ES_NONE) {
			rterror("rt_raster_geopoint_to_cell: Could not get inverse geotransform matrix");
			return ES_ERROR;
		}
	}

	GDALApplyGeoTransform(_igt, xw, yw, xr, yr);
	RASTER_DEBUGF(4, "GDALApplyGeoTransform (g -> c) for (%f, %f) = (%f, %f)",
		xw, yw, *xr, *yr);

	rnd = ROUND(*xr, 0);
	if (FLT_EQ(rnd, *xr))
		*xr = rnd;
	else
		*xr = floor(*xr);

	rnd = ROUND(*yr, 0);
	if (FLT_EQ(rnd, *yr))
		*yr = rnd;
	else
		*yr = floor(*yr);

	RASTER_DEBUGF(4, "Corrected GDALApplyGeoTransform (g -> c) for (%f, %f) = (%f, %f)",
		xw, yw, *xr, *yr);

	return ES_NONE;
}

/******************************************************************************
* rt_raster_get_envelope()
******************************************************************************/

/**
 * Get raster's envelope.
 *
 * The envelope is the minimum bounding rectangle of the raster
 *
 * @param raster : the raster to get envelope of
 * @param env : pointer to rt_envelope
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_get_envelope(
	rt_raster raster,
	rt_envelope *env
) {
	int i;
	int rtn;
	int set = 0;
	double _r[2] = {0.};
	double _w[2] = {0.};
	double _gt[6] = {0.};

	assert(raster != NULL);
	assert(env != NULL);

	rt_raster_get_geotransform_matrix(raster, _gt);

	for (i = 0; i < 4; i++) {
		switch (i) {
			case 0:
				_r[0] = 0;
				_r[1] = 0;
				break;
			case 1:
				_r[0] = 0;
				_r[1] = raster->height;
				break;
			case 2:
				_r[0] = raster->width;
				_r[1] = raster->height;
				break;
			case 3:
				_r[0] = raster->width;
				_r[1] = 0;
				break;
		}

		rtn = rt_raster_cell_to_geopoint(
			raster,
			_r[0], _r[1],
			&(_w[0]), &(_w[1]),
			_gt
		);
		if (rtn != ES_NONE) {
			rterror("rt_raster_get_envelope: Could not compute spatial coordinates for raster pixel");
			return ES_ERROR;
		}

		if (!set) {
			set = 1;
			env->MinX = _w[0];
			env->MaxX = _w[0];
			env->MinY = _w[1];
			env->MaxY = _w[1];
		}
		else {
			if (_w[0] < env->MinX)
				env->MinX = _w[0];
			else if (_w[0] > env->MaxX)
				env->MaxX = _w[0];

			if (_w[1] < env->MinY)
				env->MinY = _w[1];
			else if (_w[1] > env->MaxY)
				env->MaxY = _w[1];
		}
	}

	return ES_NONE;
}

/******************************************************************************
* rt_raster_compute_skewed_raster()
******************************************************************************/

/*
 * Compute skewed extent that covers unskewed extent.
 *
 * @param envelope : unskewed extent of type rt_envelope
 * @param skew : pointer to 2-element array (x, y) of skew
 * @param scale : pointer to 2-element array (x, y) of scale
 * @param tolerance : value between 0 and 1 where the smaller the tolerance
 * results in an extent approaching the "minimum" skewed extent.
 * If value <= 0, tolerance = 0.1. If value > 1, tolerance = 1.
 *
 * @return skewed raster who's extent covers unskewed extent, NULL on error
 */
rt_raster
rt_raster_compute_skewed_raster(
	rt_envelope extent,
	double *skew,
	double *scale,
	double tolerance
) {
	uint32_t run = 0;
	uint32_t max_run = 1;
	double dbl_run = 0;

	int rtn;
	int covers = 0;
	rt_raster raster;
	double _gt[6] = {0};
	double _igt[6] = {0};
	int _d[2] = {1, -1};
	int _dlast = 0;
	int _dlastpos = 0;
	double _w[2] = {0};
	double _r[2] = {0};
	double _xy[2] = {0};
	int i;
	int j;
	int x;
	int y;

	LWGEOM *geom = NULL;
	GEOSGeometry *sgeom = NULL;
	GEOSGeometry *ngeom = NULL;

	if (
		(tolerance < 0.) ||
		FLT_EQ(tolerance, 0.)
	) {
		tolerance = 0.1;
	}
	else if (tolerance > 1.)
		tolerance = 1;

	dbl_run = tolerance;
	while (dbl_run < 10) {
		dbl_run *= 10.;
		max_run *= 10;
	}

	/* scale must be provided */
	if (scale == NULL)
		return NULL;
	for (i = 0; i < 2; i++) {
		if (FLT_EQ(scale[i], 0)) {
			rterror("rt_raster_compute_skewed_raster: Scale cannot be zero");
			return 0;
		}

		if (i < 1)
			_gt[1] = fabs(scale[i] * tolerance);
		else
			_gt[5] = fabs(scale[i] * tolerance);
	}
	/* conform scale-y to be negative */
	_gt[5] *= -1;

	/* skew not provided or skew is zero, return raster of correct dim and spatial attributes */
	if (
		(skew == NULL) || (
			FLT_EQ(skew[0], 0) &&
			FLT_EQ(skew[1], 0)
		)
	) {
		int _dim[2] = {
			(int) fmax((fabs(extent.MaxX - extent.MinX) + (fabs(scale[0]) / 2.)) / fabs(scale[0]), 1),
			(int) fmax((fabs(extent.MaxY - extent.MinY) + (fabs(scale[1]) / 2.)) / fabs(scale[1]), 1)
		};

		raster = rt_raster_new(_dim[0], _dim[1]);
		if (raster == NULL) {
			rterror("rt_raster_compute_skewed_raster: Could not create output raster");
			return NULL;
		}

		rt_raster_set_offsets(raster, extent.MinX, extent.MaxY);
		rt_raster_set_scale(raster, fabs(scale[0]), -1 * fabs(scale[1]));
		rt_raster_set_skews(raster, skew[0], skew[1]);

		return raster;
	}

	/* direction to shift upper-left corner */
	if (skew[0] > 0.)
		_d[0] = -1;
	if (skew[1] < 0.)
		_d[1] = 1;

	/* geotransform */
	_gt[0] = extent.UpperLeftX;
	_gt[2] = skew[0] * tolerance;
	_gt[3] = extent.UpperLeftY;
	_gt[4] = skew[1] * tolerance;

	RASTER_DEBUGF(4, "Initial geotransform: %f, %f, %f, %f, %f, %f",
		_gt[0], _gt[1], _gt[2], _gt[3], _gt[4], _gt[5]
	);
	RASTER_DEBUGF(4, "Delta: %d, %d", _d[0], _d[1]);

	/* simple raster */
	if ((raster = rt_raster_new(1, 1)) == NULL) {
		rterror("rt_raster_compute_skewed_raster: Out of memory allocating extent raster");
		return NULL;
	}
	rt_raster_set_geotransform_matrix(raster, _gt);

	/* get inverse geotransform matrix */
	if (!GDALInvGeoTransform(_gt, _igt)) {
		rterror("rt_raster_compute_skewed_raster: Could not compute inverse geotransform matrix");
		rt_raster_destroy(raster);
		return NULL;
	}
	RASTER_DEBUGF(4, "Inverse geotransform: %f, %f, %f, %f, %f, %f",
		_igt[0], _igt[1], _igt[2], _igt[3], _igt[4], _igt[5]
	);

	/* shift along axis */
	for (i = 0; i < 2; i++) {
		covers = 0;
		run = 0;

		RASTER_DEBUGF(3, "Shifting along %s axis", i < 1 ? "X" : "Y");

		do {

			/* prevent possible infinite loop */
			if (run > max_run) {
				rterror("rt_raster_compute_skewed_raster: Could not compute skewed extent due to check preventing infinite loop");
				rt_raster_destroy(raster);
				return NULL;
			}

			/*
				check the four corners that they are covered along the specific axis
				pixel column should be >= 0
			*/
			for (j = 0; j < 4; j++) {
				switch (j) {
					/* upper-left */
					case 0:
						_xy[0] = extent.MinX;
						_xy[1] = extent.MaxY;
						break;
					/* lower-left */
					case 1:
						_xy[0] = extent.MinX;
						_xy[1] = extent.MinY;
						break;
					/* lower-right */
					case 2:
						_xy[0] = extent.MaxX;
						_xy[1] = extent.MinY;
						break;
					/* upper-right */
					case 3:
						_xy[0] = extent.MaxX;
						_xy[1] = extent.MaxY;
						break;
				}

				rtn = rt_raster_geopoint_to_cell(
					raster,
					_xy[0], _xy[1],
					&(_r[0]), &(_r[1]),
					_igt
				);
				if (rtn != ES_NONE) {
					rterror("rt_raster_compute_skewed_raster: Could not compute raster pixel for spatial coordinates");
					rt_raster_destroy(raster);
					return NULL;
				}

				RASTER_DEBUGF(4, "Point %d at cell %d x %d", j, (int) _r[0], (int) _r[1]);

				/* raster doesn't cover point */
				if ((int) _r[i] < 0) {
					RASTER_DEBUGF(4, "Point outside of skewed extent: %d", j);
					covers = 0;

					if (_dlastpos != j) {
						_dlast = (int) _r[i];
						_dlastpos = j;
					}
					else if ((int) _r[i] < _dlast) {
						RASTER_DEBUG(4, "Point going in wrong direction.  Reversing direction");
						_d[i] *= -1;
						_dlastpos = -1;
						run = 0;
					}

					break;
				}

				covers++;
			}

			if (!covers) {
				x = 0;
				y = 0;
				if (i < 1)
					x = _d[i] * fabs(_r[i]);
				else
					y = _d[i] * fabs(_r[i]);

				rtn = rt_raster_cell_to_geopoint(
					raster,
					x, y,
					&(_w[0]), &(_w[1]),
					_gt
				);
				if (rtn != ES_NONE) {
					rterror("rt_raster_compute_skewed_raster: Could not compute spatial coordinates for raster pixel");
					rt_raster_destroy(raster);
					return NULL;
				}

				/* adjust ul */
				if (i < 1)
					_gt[0] = _w[i];
				else
					_gt[3] = _w[i];
				rt_raster_set_geotransform_matrix(raster, _gt);
				RASTER_DEBUGF(4, "Shifted geotransform: %f, %f, %f, %f, %f, %f",
					_gt[0], _gt[1], _gt[2], _gt[3], _gt[4], _gt[5]
				);

				/* get inverse geotransform matrix */
				if (!GDALInvGeoTransform(_gt, _igt)) {
					rterror("rt_raster_compute_skewed_raster: Could not compute inverse geotransform matrix");
					rt_raster_destroy(raster);
					return NULL;
				}
				RASTER_DEBUGF(4, "Inverse geotransform: %f, %f, %f, %f, %f, %f",
					_igt[0], _igt[1], _igt[2], _igt[3], _igt[4], _igt[5]
				);
			}

			run++;
		}
		while (!covers);
	}

	/* covers test */
	rtn = rt_raster_geopoint_to_cell(
		raster,
		extent.MaxX, extent.MinY,
		&(_r[0]), &(_r[1]),
		_igt
	);
	if (rtn != ES_NONE) {
		rterror("rt_raster_compute_skewed_raster: Could not compute raster pixel for spatial coordinates");
		rt_raster_destroy(raster);
		return NULL;
	}

	RASTER_DEBUGF(4, "geopoint %f x %f at cell %d x %d", extent.MaxX, extent.MinY, (int) _r[0], (int) _r[1]);

	raster->width = _r[0];
	raster->height = _r[1];

	/* initialize GEOS */
	initGEOS(lwnotice, lwgeom_geos_error);

	/* create reference LWPOLY */
	{
		LWPOLY *npoly = rt_util_envelope_to_lwpoly(extent);
		if (npoly == NULL) {
			rterror("rt_raster_compute_skewed_raster: Could not build extent's geometry for covers test");
			rt_raster_destroy(raster);
			return NULL;
		}

		ngeom = (GEOSGeometry *) LWGEOM2GEOS(lwpoly_as_lwgeom(npoly), 0);
		lwpoly_free(npoly);
	}

	do {
		covers = 0;

		/* construct sgeom from raster */
		if ((rt_raster_get_convex_hull(raster, &geom) != ES_NONE) || geom == NULL) {
			rterror("rt_raster_compute_skewed_raster: Could not build skewed extent's geometry for covers test");
			GEOSGeom_destroy(ngeom);
			rt_raster_destroy(raster);
			return NULL;
		}

		sgeom = (GEOSGeometry *) LWGEOM2GEOS(geom, 0);
		lwgeom_free(geom);

		covers = GEOSRelatePattern(sgeom, ngeom, "******FF*");
		GEOSGeom_destroy(sgeom);

		if (covers == 2) {
			rterror("rt_raster_compute_skewed_raster: Could not run covers test");
			GEOSGeom_destroy(ngeom);
			rt_raster_destroy(raster);
			return NULL;
		}

		if (covers)
			break;

		raster->width++;
		raster->height++;
	}
	while (!covers);

	RASTER_DEBUGF(4, "Skewed extent does cover normal extent with dimensions %d x %d", raster->width, raster->height);

	raster->width = (int) ((((double) raster->width) * fabs(_gt[1]) + fabs(scale[0] / 2.)) / fabs(scale[0]));
	raster->height = (int) ((((double) raster->height) * fabs(_gt[5]) + fabs(scale[1] / 2.)) / fabs(scale[1]));
	_gt[1] = fabs(scale[0]);
	_gt[5] = -1 * fabs(scale[1]);
	_gt[2] = skew[0];
	_gt[4] = skew[1];
	rt_raster_set_geotransform_matrix(raster, _gt);

	/* minimize width/height */
	for (i = 0; i < 2; i++) {
		covers = 1;
		do {
			if (i < 1)
				raster->width--;
			else
				raster->height--;
			
			/* construct sgeom from raster */
			if ((rt_raster_get_convex_hull(raster, &geom) != ES_NONE) || geom == NULL) {
				rterror("rt_raster_compute_skewed_raster: Could not build skewed extent's geometry for minimizing dimensions");
				GEOSGeom_destroy(ngeom);
				rt_raster_destroy(raster);
				return NULL;
			}

			sgeom = (GEOSGeometry *) LWGEOM2GEOS(geom, 0);
			lwgeom_free(geom);

			covers = GEOSRelatePattern(sgeom, ngeom, "******FF*");
			GEOSGeom_destroy(sgeom);

			if (covers == 2) {
				rterror("rt_raster_compute_skewed_raster: Could not run covers test for minimizing dimensions");
				GEOSGeom_destroy(ngeom);
				rt_raster_destroy(raster);
				return NULL;
			}

			if (!covers) {
				if (i < 1)
					raster->width++;
				else
					raster->height++;

				break;
			}
		}
		while (covers);
	}

	GEOSGeom_destroy(ngeom);

	return raster;
}

/**
 * Return TRUE if the raster is empty. i.e. is NULL, width = 0 or height = 0
 *
 * @param raster : the raster to get info from
 *
 * @return TRUE if the raster is empty, FALSE otherwise
 */
int
rt_raster_is_empty(rt_raster raster) {
	return (NULL == raster || raster->height <= 0 || raster->width <= 0);
}

/**
 * Return TRUE if the raster has a band of this number.
 *
 * @param raster : the raster to get info from
 * @param nband : the band number. 0-based
 *
 * @return TRUE if the raster has a band of this number, FALSE otherwise
 */
int
rt_raster_has_band(rt_raster raster, int nband) {
	return !(NULL == raster || nband >= raster->numBands || nband < 0);
}

/******************************************************************************
* rt_raster_copy_band()
******************************************************************************/

/**
 * Copy one band from one raster to another.  Bands are duplicated from
 * fromrast to torast using rt_band_duplicate.  The caller will need
 * to ensure that the copied band's data or path remains allocated
 * for the lifetime of the copied bands.
 *
 * @param torast : raster to copy band to
 * @param fromrast : raster to copy band from
 * @param fromindex : index of band in source raster, 0-based
 * @param toindex : index of new band in destination raster, 0-based
 *
 * @return The band index of the second raster where the new band is copied.
 *   -1 if error
 */
int
rt_raster_copy_band(
	rt_raster torast, rt_raster fromrast,
	int fromindex, int toindex
) {
	rt_band srcband = NULL;
	rt_band dstband = NULL;

	assert(NULL != torast);
	assert(NULL != fromrast);

	/* Check raster dimensions */
	if (torast->height != fromrast->height || torast->width != fromrast->width) {
		rtwarn("rt_raster_copy_band: Attempting to add a band with different width or height");
		return -1;
	}

	/* Check bands limits */
	if (fromrast->numBands < 1) {
		rtwarn("rt_raster_copy_band: Second raster has no band");
		return -1;
	}
	else if (fromindex < 0) {
		rtwarn("rt_raster_copy_band: Band index for second raster < 0. Defaulted to 0");
		fromindex = 0;
	}
	else if (fromindex >= fromrast->numBands) {
		rtwarn("rt_raster_copy_band: Band index for second raster > number of bands, truncated from %u to %u", fromindex, fromrast->numBands - 1);
		fromindex = fromrast->numBands - 1;
	}

	if (toindex < 0) {
		rtwarn("rt_raster_copy_band: Band index for first raster < 0. Defaulted to 0");
		toindex = 0;
	}
	else if (toindex > torast->numBands) {
		rtwarn("rt_raster_copy_band: Band index for first raster > number of bands, truncated from %u to %u", toindex, torast->numBands);
		toindex = torast->numBands;
	}

	/* Get band from source raster */
	srcband = rt_raster_get_band(fromrast, fromindex);

	/* duplicate band */
	dstband = rt_band_duplicate(srcband);

	/* Add band to the second raster */
	return rt_raster_add_band(torast, dstband, toindex);
}

/******************************************************************************
* rt_raster_from_band()
******************************************************************************/

/**
 * Construct a new rt_raster from an existing rt_raster and an array
 * of band numbers
 *
 * @param raster : the source raster
 * @param bandNums : array of band numbers to extract from source raster
 *                   and add to the new raster (0 based)
 * @param count : number of elements in bandNums
 *
 * @return a new rt_raster or NULL on error
 */
rt_raster
rt_raster_from_band(rt_raster raster, uint32_t *bandNums, int count) {
	rt_raster rast = NULL;
	int i = 0;
	int j = 0;
	int idx;
	int32_t flag;
	double gt[6] = {0.};

	assert(NULL != raster);
	assert(NULL != bandNums);

	RASTER_DEBUGF(3, "rt_raster_from_band: source raster has %d bands",
		rt_raster_get_num_bands(raster));

	/* create new raster */
	rast = rt_raster_new(raster->width, raster->height);
	if (NULL == rast) {
		rterror("rt_raster_from_band: Out of memory allocating new raster");
		return NULL;
	}

	/* copy raster attributes */
	rt_raster_get_geotransform_matrix(raster, gt);
	rt_raster_set_geotransform_matrix(rast, gt);

	/* srid */
	rt_raster_set_srid(rast, raster->srid);

	/* copy bands */
	for (i = 0; i < count; i++) {
		idx = bandNums[i];
		flag = rt_raster_copy_band(rast, raster, idx, i);

		if (flag < 0) {
			rterror("rt_raster_from_band: Could not copy band");
			for (j = 0; j < i; j++) rt_band_destroy(rast->bands[j]);
			rt_raster_destroy(rast);
			return NULL;
		}

		RASTER_DEBUGF(3, "rt_raster_from_band: band created at index %d",
			flag);
	}

	RASTER_DEBUGF(3, "rt_raster_from_band: new raster has %d bands",
		rt_raster_get_num_bands(rast));
	return rast;
}

/******************************************************************************
* rt_raster_replace_band()
******************************************************************************/

/**
 * Replace band at provided index with new band
 *
 * @param raster: raster of band to be replaced
 * @param band : new band to add to raster
 * @param index : index of band to replace (0-based)
 *
 * @return NULL on error or replaced band
 */
rt_band
rt_raster_replace_band(rt_raster raster, rt_band band, int index) {
	rt_band oldband = NULL;
	assert(NULL != raster);
	assert(NULL != band);

	if (band->width != raster->width || band->height != raster->height) {
		rterror("rt_raster_replace_band: Band does not match raster's dimensions: %dx%d band to %dx%d raster",
			band->width, band->height, raster->width, raster->height);
		return 0;
	}

	if (index >= raster->numBands || index < 0) {
		rterror("rt_raster_replace_band: Band index is not valid");
		return 0;
	}

	oldband = rt_raster_get_band(raster, index);
	RASTER_DEBUGF(3, "rt_raster_replace_band: old band at %p", oldband);
	RASTER_DEBUGF(3, "rt_raster_replace_band: new band at %p", band);

	raster->bands[index] = band;
	RASTER_DEBUGF(3, "rt_raster_replace_band: new band at %p", raster->bands[index]);

	band->raster = raster;
	oldband->raster = NULL;

	return oldband;
}

/******************************************************************************
* rt_raster_clone()
******************************************************************************/

/**
 * Clone an existing raster
 *
 * @param raster : raster to clone
 * @param deep : flag indicating if bands should be cloned
 *
 * @return a new rt_raster or NULL on error
 */
rt_raster
rt_raster_clone(rt_raster raster, uint8_t deep) {
	rt_raster rtn = NULL;
	double gt[6] = {0};

	assert(NULL != raster);

	if (deep) {
		int numband = rt_raster_get_num_bands(raster);
		uint32_t *nband = NULL;
		int i = 0;

		nband = rtalloc(sizeof(uint32_t) * numband);
		if (nband == NULL) {
			rterror("rt_raster_clone: Could not allocate memory for deep clone");
			return NULL;
		}
		for (i = 0; i < numband; i++)
			nband[i] = i;

		rtn = rt_raster_from_band(raster, nband, numband);
		rtdealloc(nband);

		return rtn;
	}

	rtn = rt_raster_new(
		rt_raster_get_width(raster),
		rt_raster_get_height(raster)
	);
	if (rtn == NULL) {
		rterror("rt_raster_clone: Could not create cloned raster");
		return NULL;
	}

	rt_raster_get_geotransform_matrix(raster, gt);
	rt_raster_set_geotransform_matrix(rtn, gt);
	rt_raster_set_srid(rtn, rt_raster_get_srid(raster));

	return rtn;
}

/******************************************************************************
* rt_raster_to_gdal()
******************************************************************************/

/**
 * Return formatted GDAL raster from raster
 *
 * @param raster : the raster to convert
 * @param srs : the raster's coordinate system in OGC WKT
 * @param format : format to convert to. GDAL driver short name
 * @param options : list of format creation options. array of strings
 * @param gdalsize : will be set to the size of returned bytea
 *
 * @return formatted GDAL raster.  the calling function is responsible
 *   for freeing the returned data using CPLFree()
 */
uint8_t*
rt_raster_to_gdal(
	rt_raster raster, const char *srs,
	char *format, char **options, uint64_t *gdalsize
) {
	GDALDriverH src_drv = NULL;
	int destroy_src_drv = 0;
	GDALDatasetH src_ds = NULL;

	vsi_l_offset rtn_lenvsi;
	uint64_t rtn_len = 0;

	GDALDriverH rtn_drv = NULL;
	GDALDatasetH rtn_ds = NULL;
	uint8_t *rtn = NULL;

	assert(NULL != raster);
	assert(NULL != gdalsize);

	/* any supported format is possible */
	rt_util_gdal_register_all(0);
	RASTER_DEBUG(3, "loaded all supported GDAL formats");

	/* output format not specified */
	if (format == NULL || !strlen(format))
		format = "GTiff";
	RASTER_DEBUGF(3, "output format is %s", format);

	/* load raster into a GDAL MEM raster */
	src_ds = rt_raster_to_gdal_mem(raster, srs, NULL, NULL, 0, &src_drv, &destroy_src_drv);
	if (NULL == src_ds) {
		rterror("rt_raster_to_gdal: Could not convert raster to GDAL MEM format");
		return 0;
	}

	/* load driver */
	rtn_drv = GDALGetDriverByName(format);
	if (NULL == rtn_drv) {
		rterror("rt_raster_to_gdal: Could not load the output GDAL driver");
		GDALClose(src_ds);
		if (destroy_src_drv) GDALDestroyDriver(src_drv);
		return 0;
	}
	RASTER_DEBUG(3, "Output driver loaded");

	/* convert GDAL MEM raster to output format */
	RASTER_DEBUG(3, "Copying GDAL MEM raster to memory file in output format");
	rtn_ds = GDALCreateCopy(
		rtn_drv,
		"/vsimem/out.dat", /* should be fine assuming this is in a process */
		src_ds,
		FALSE, /* should copy be strictly equivelent? */
		options, /* format options */
		NULL, /* progress function */
		NULL /* progress data */
	);

	/* close source dataset */
	GDALClose(src_ds);
	if (destroy_src_drv) GDALDestroyDriver(src_drv);
	RASTER_DEBUG(3, "Closed GDAL MEM raster");

	if (NULL == rtn_ds) {
		rterror("rt_raster_to_gdal: Could not create the output GDAL dataset");
		return 0;
	}

	RASTER_DEBUGF(4, "dataset SRS: %s", GDALGetProjectionRef(rtn_ds));

	/* close dataset, this also flushes any pending writes */
	GDALClose(rtn_ds);
	RASTER_DEBUG(3, "Closed GDAL output raster");

	RASTER_DEBUG(3, "Done copying GDAL MEM raster to memory file in output format");

	/* from memory file to buffer */
	RASTER_DEBUG(3, "Copying GDAL memory file to buffer");
	rtn = VSIGetMemFileBuffer("/vsimem/out.dat", &rtn_lenvsi, TRUE);
	RASTER_DEBUG(3, "Done copying GDAL memory file to buffer");
	if (NULL == rtn) {
		rterror("rt_raster_to_gdal: Could not create the output GDAL raster");
		return 0;
	}

	rtn_len = (uint64_t) rtn_lenvsi;
	*gdalsize = rtn_len;

	return rtn;
}

/******************************************************************************
* rt_raster_gdal_drivers()
******************************************************************************/

/**
 * Returns a set of available GDAL drivers
 *
 * @param drv_count : number of GDAL drivers available
 * @param cancc : if non-zero, filter drivers to only those
 *   with support for CreateCopy and VirtualIO
 *
 * @return set of "gdaldriver" values of available GDAL drivers
 */
rt_gdaldriver
rt_raster_gdal_drivers(uint32_t *drv_count, uint8_t cancc) {
	const char *state;
	const char *txt;
	int txt_len;
	GDALDriverH *drv = NULL;
	rt_gdaldriver rtn = NULL;
	int count;
	int i;
	uint32_t j;

	assert(drv_count != NULL);

	rt_util_gdal_register_all(0);
	count = GDALGetDriverCount();
	RASTER_DEBUGF(3, "%d drivers found", count);

	rtn = (rt_gdaldriver) rtalloc(count * sizeof(struct rt_gdaldriver_t));
	if (NULL == rtn) {
		rterror("rt_raster_gdal_drivers: Could not allocate memory for gdaldriver structure");
		return 0;
	}

	for (i = 0, j = 0; i < count; i++) {
		drv = GDALGetDriver(i);

#ifdef GDAL_DCAP_RASTER
		/* Starting with GDAL 2.0, vector drivers can also be returned */
		/* Only keep raster drivers */
		state = GDALGetMetadataItem(drv, GDAL_DCAP_RASTER, NULL);
		if (state == NULL || !EQUAL(state, "YES"))
			continue;
#endif

		if (cancc) {
			/* CreateCopy support */
			state = GDALGetMetadataItem(drv, GDAL_DCAP_CREATECOPY, NULL);
			if (state == NULL) continue;

			/* VirtualIO support */
			state = GDALGetMetadataItem(drv, GDAL_DCAP_VIRTUALIO, NULL);
			if (state == NULL) continue;
		}

		/* index of driver */
		rtn[j].idx = i;

		/* short name */
		txt = GDALGetDriverShortName(drv);
		txt_len = strlen(txt);

		if (cancc) {
			RASTER_DEBUGF(3, "driver %s (%d) supports CreateCopy() and VirtualIO()", txt, i);
		}

		txt_len = (txt_len + 1) * sizeof(char);
		rtn[j].short_name = (char *) rtalloc(txt_len);
		memcpy(rtn[j].short_name, txt, txt_len);

		/* long name */
		txt = GDALGetDriverLongName(drv);
		txt_len = strlen(txt);

		txt_len = (txt_len + 1) * sizeof(char);
		rtn[j].long_name = (char *) rtalloc(txt_len);
		memcpy(rtn[j].long_name, txt, txt_len);

		/* creation options */
		txt = GDALGetDriverCreationOptionList(drv);
		txt_len = strlen(txt);

		txt_len = (txt_len + 1) * sizeof(char);
		rtn[j].create_options = (char *) rtalloc(txt_len);
		memcpy(rtn[j].create_options, txt, txt_len);

		j++;
	}

	/* free unused memory */
	rtn = rtrealloc(rtn, j * sizeof(struct rt_gdaldriver_t));
	*drv_count = j;

	return rtn;
}

/******************************************************************************
* rt_raster_to_gdal_mem()
******************************************************************************/

/**
 * Return GDAL dataset using GDAL MEM driver from raster.
 *
 * @param raster : raster to convert to GDAL MEM
 * @param srs : the raster's coordinate system in OGC WKT
 * @param bandNums : array of band numbers to extract from raster
 *   and include in the GDAL dataset (0 based)
 * @param excludeNodataValues : array of zero, nonzero where if non-zero,
 *   ignore nodata values for the band
 * @param count : number of elements in bandNums
 * @param rtn_drv : is set to the GDAL driver object
 * @param destroy_rtn_drv : if non-zero, caller must destroy the MEM driver
 *
 * @return GDAL dataset using GDAL MEM driver
 */
GDALDatasetH
rt_raster_to_gdal_mem(
	rt_raster raster,
	const char *srs,
	uint32_t *bandNums,
	int *excludeNodataValues,
	int count,
	GDALDriverH *rtn_drv, int *destroy_rtn_drv
) {
	GDALDriverH drv = NULL;
	GDALDatasetH ds = NULL;
	double gt[6] = {0.0};
	CPLErr cplerr;
	GDALDataType gdal_pt = GDT_Unknown;
	GDALRasterBandH band;
	void *pVoid;
	char *pszDataPointer;
	char szGDALOption[50];
	char *apszOptions[4];
	double nodata = 0.0;
	int allocBandNums = 0;
	int allocNodataValues = 0;

	int i;
	int numBands;
	uint32_t width = 0;
	uint32_t height = 0;
	rt_band rtband = NULL;
	rt_pixtype pt = PT_END;

	assert(NULL != raster);
	assert(NULL != rtn_drv);
	assert(NULL != destroy_rtn_drv);

	*destroy_rtn_drv = 0;

	/* store raster in GDAL MEM raster */
	if (!rt_util_gdal_driver_registered("MEM")) {
		RASTER_DEBUG(4, "Registering MEM driver");
		GDALRegister_MEM();
		*destroy_rtn_drv = 1;
	}
	drv = GDALGetDriverByName("MEM");
	if (NULL == drv) {
		rterror("rt_raster_to_gdal_mem: Could not load the MEM GDAL driver");
		return 0;
	}
	*rtn_drv = drv;

	/* unload driver from GDAL driver manager */
	if (*destroy_rtn_drv) {
		RASTER_DEBUG(4, "Deregistering MEM driver");
		GDALDeregisterDriver(drv);
	}

	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);
	ds = GDALCreate(
		drv, "",
		width, height,
		0, GDT_Byte, NULL
	);
	if (NULL == ds) {
		rterror("rt_raster_to_gdal_mem: Could not create a GDALDataset to convert into");
		return 0;
	}

	/* add geotransform */
	rt_raster_get_geotransform_matrix(raster, gt);
	cplerr = GDALSetGeoTransform(ds, gt);
	if (cplerr != CE_None) {
		rterror("rt_raster_to_gdal_mem: Could not set geotransformation");
		GDALClose(ds);
		return 0;
	}

	/* set spatial reference */
	if (NULL != srs && strlen(srs)) {
		char *_srs = rt_util_gdal_convert_sr(srs, 0);
		if (_srs == NULL) {
			rterror("rt_raster_to_gdal_mem: Could not convert srs to GDAL accepted format");
			GDALClose(ds);
			return 0;
		}

		cplerr = GDALSetProjection(ds, _srs);
		CPLFree(_srs);
		if (cplerr != CE_None) {
			rterror("rt_raster_to_gdal_mem: Could not set projection");
			GDALClose(ds);
			return 0;
		}
		RASTER_DEBUGF(3, "Projection set to: %s", GDALGetProjectionRef(ds));
	}

	/* process bandNums */
	numBands = rt_raster_get_num_bands(raster);
	if (NULL != bandNums && count > 0) {
		for (i = 0; i < count; i++) {
			if (bandNums[i] >= numBands) {
				rterror("rt_raster_to_gdal_mem: The band index %d is invalid", bandNums[i]);
				GDALClose(ds);
				return 0;
			}
		}
	}
	else {
		count = numBands;
		bandNums = (uint32_t *) rtalloc(sizeof(uint32_t) * count);
		if (NULL == bandNums) {
			rterror("rt_raster_to_gdal_mem: Could not allocate memory for band indices");
			GDALClose(ds);
			return 0;
		}
		allocBandNums = 1;
		for (i = 0; i < count; i++) bandNums[i] = i;
	}

	/* process exclude_nodata_values */
	if (NULL == excludeNodataValues) {
		excludeNodataValues = (int *) rtalloc(sizeof(int) * count);
		if (NULL == excludeNodataValues) {
			rterror("rt_raster_to_gdal_mem: Could not allocate memory for NODATA flags");
			GDALClose(ds);
			return 0;
		}
		allocNodataValues = 1;
		for (i = 0; i < count; i++) excludeNodataValues[i] = 1;
	}

	/* add band(s) */
	for (i = 0; i < count; i++) {
		rtband = rt_raster_get_band(raster, bandNums[i]);
		if (NULL == rtband) {
			rterror("rt_raster_to_gdal_mem: Could not get requested band index %d", bandNums[i]);
			if (allocBandNums) rtdealloc(bandNums);
			if (allocNodataValues) rtdealloc(excludeNodataValues);
			GDALClose(ds);
			return 0;
		}

		pt = rt_band_get_pixtype(rtband);
		gdal_pt = rt_util_pixtype_to_gdal_datatype(pt);
		if (gdal_pt == GDT_Unknown)
			rtwarn("rt_raster_to_gdal_mem: Unknown pixel type for band");

		/*
			For all pixel types other than PT_8BSI, set pointer to start of data
		*/
		if (pt != PT_8BSI) {
			pVoid = rt_band_get_data(rtband);
			RASTER_DEBUGF(4, "Band data is at pos %p", pVoid);

			pszDataPointer = (char *) rtalloc(20 * sizeof (char));
			sprintf(pszDataPointer, "%p", pVoid);
			RASTER_DEBUGF(4, "rt_raster_to_gdal_mem: szDatapointer is %p",
				pszDataPointer);

			if (strnicmp(pszDataPointer, "0x", 2) == 0)
				sprintf(szGDALOption, "DATAPOINTER=%s", pszDataPointer);
			else
				sprintf(szGDALOption, "DATAPOINTER=0x%s", pszDataPointer);

			RASTER_DEBUG(3, "Storing info for GDAL MEM raster band");

			apszOptions[0] = szGDALOption;
			apszOptions[1] = NULL; /* pixel offset, not needed */
			apszOptions[2] = NULL; /* line offset, not needed */
			apszOptions[3] = NULL;

			/* free */
			rtdealloc(pszDataPointer);

			/* add band */
			if (GDALAddBand(ds, gdal_pt, apszOptions) == CE_Failure) {
				rterror("rt_raster_to_gdal_mem: Could not add GDAL raster band");
				if (allocBandNums) rtdealloc(bandNums);
				GDALClose(ds);
				return 0;
			}
		}
		/*
			PT_8BSI is special as GDAL has no equivalent pixel type.
			Must convert 8BSI to 16BSI so create basic band
		*/
		else {
			/* add band */
			if (GDALAddBand(ds, gdal_pt, NULL) == CE_Failure) {
				rterror("rt_raster_to_gdal_mem: Could not add GDAL raster band");
				if (allocBandNums) rtdealloc(bandNums);
				if (allocNodataValues) rtdealloc(excludeNodataValues);
				GDALClose(ds);
				return 0;
			}
		}

		/* check band count */
		if (GDALGetRasterCount(ds) != i + 1) {
			rterror("rt_raster_to_gdal_mem: Error creating GDAL MEM raster band");
			if (allocBandNums) rtdealloc(bandNums);
			if (allocNodataValues) rtdealloc(excludeNodataValues);
			GDALClose(ds);
			return 0;
		}

		/* get new band */
		band = NULL;
		band = GDALGetRasterBand(ds, i + 1);
		if (NULL == band) {
			rterror("rt_raster_to_gdal_mem: Could not get GDAL band for additional processing");
			if (allocBandNums) rtdealloc(bandNums);
			if (allocNodataValues) rtdealloc(excludeNodataValues);
			GDALClose(ds);
			return 0;
		}

		/* PT_8BSI requires manual setting of pixels */
		if (pt == PT_8BSI) {
			int nXBlocks, nYBlocks;
			int nXBlockSize, nYBlockSize;
			int iXBlock, iYBlock;
			int nXValid, nYValid;
			int iX, iY;
			int iXMax, iYMax;

			int x, y, z;
			uint32_t valueslen = 0;
			int16_t *values = NULL;
			double value = 0.;

			/* this makes use of GDAL's "natural" blocks */
			GDALGetBlockSize(band, &nXBlockSize, &nYBlockSize);
			nXBlocks = (width + nXBlockSize - 1) / nXBlockSize;
			nYBlocks = (height + nYBlockSize - 1) / nYBlockSize;
			RASTER_DEBUGF(4, "(nXBlockSize, nYBlockSize) = (%d, %d)", nXBlockSize, nYBlockSize);
			RASTER_DEBUGF(4, "(nXBlocks, nYBlocks) = (%d, %d)", nXBlocks, nYBlocks);

			/* length is for the desired pixel type */
			valueslen = rt_pixtype_size(PT_16BSI) * nXBlockSize * nYBlockSize;
			values = rtalloc(valueslen);
			if (NULL == values) {
				rterror("rt_raster_to_gdal_mem: Could not allocate memory for GDAL band pixel values");
				if (allocBandNums) rtdealloc(bandNums);
				if (allocNodataValues) rtdealloc(excludeNodataValues);
				GDALClose(ds);
				return 0;
			}

			for (iYBlock = 0; iYBlock < nYBlocks; iYBlock++) {
				for (iXBlock = 0; iXBlock < nXBlocks; iXBlock++) {
					memset(values, 0, valueslen);

					x = iXBlock * nXBlockSize;
					y = iYBlock * nYBlockSize;
					RASTER_DEBUGF(4, "(iXBlock, iYBlock) = (%d, %d)", iXBlock, iYBlock);
					RASTER_DEBUGF(4, "(x, y) = (%d, %d)", x, y);

					/* valid block width */
					if ((iXBlock + 1) * nXBlockSize > width)
						nXValid = width - (iXBlock * nXBlockSize);
					else
						nXValid = nXBlockSize;

					/* valid block height */
					if ((iYBlock + 1) * nYBlockSize > height)
						nYValid = height - (iYBlock * nYBlockSize);
					else
						nYValid = nYBlockSize;

					RASTER_DEBUGF(4, "(nXValid, nYValid) = (%d, %d)", nXValid, nYValid);

					/* convert 8BSI values to 16BSI */
					z = 0;
					iYMax = y + nYValid;
					iXMax = x + nXValid;
					for (iY = y; iY < iYMax; iY++)  {
						for (iX = x; iX < iXMax; iX++)  {
							if (rt_band_get_pixel(rtband, iX, iY, &value, NULL) != ES_NONE) {
								rterror("rt_raster_to_gdal_mem: Could not get pixel value to convert from 8BSI to 16BSI");
								rtdealloc(values);
								if (allocBandNums) rtdealloc(bandNums);
								if (allocNodataValues) rtdealloc(excludeNodataValues);
								GDALClose(ds);
								return 0;
							}

							values[z++] = rt_util_clamp_to_16BSI(value);
						}
					}

					/* burn values */
					if (GDALRasterIO(
						band, GF_Write,
						x, y,
						nXValid, nYValid,
						values, nXValid, nYValid,
						gdal_pt,
						0, 0
					) != CE_None) {
						rterror("rt_raster_to_gdal_mem: Could not write converted 8BSI to 16BSI values to GDAL band");
						rtdealloc(values);
						if (allocBandNums) rtdealloc(bandNums);
						if (allocNodataValues) rtdealloc(excludeNodataValues);
						GDALClose(ds);
						return 0;
					}
				}
			}

			rtdealloc(values);
		}

		/* Add nodata value for band */
		if (rt_band_get_hasnodata_flag(rtband) != FALSE && excludeNodataValues[i]) {
			rt_band_get_nodata(rtband, &nodata);
			if (GDALSetRasterNoDataValue(band, nodata) != CE_None)
				rtwarn("rt_raster_to_gdal_mem: Could not set nodata value for band");
			RASTER_DEBUGF(3, "nodata value set to %f", GDALGetRasterNoDataValue(band, NULL));
		}

#if POSTGIS_DEBUG_LEVEL > 3
		{
			GDALRasterBandH _grb = NULL;
			double _min;
			double _max;
			double _mean;
			double _stddev;

			_grb = GDALGetRasterBand(ds, i + 1);
			GDALComputeRasterStatistics(_grb, FALSE, &_min, &_max, &_mean, &_stddev, NULL, NULL);
			RASTER_DEBUGF(4, "GDAL Band %d stats: %f, %f, %f, %f", i + 1, _min, _max, _mean, _stddev);
		}
#endif

	}

	/* necessary??? */
	GDALFlushCache(ds);

	if (allocBandNums) rtdealloc(bandNums);
	if (allocNodataValues) rtdealloc(excludeNodataValues);

	return ds;
}

/******************************************************************************
* rt_raster_from_gdal_dataset()
******************************************************************************/

/**
 * Return a raster from a GDAL dataset
 *
 * @param ds : the GDAL dataset to convert to a raster
 *
 * @return raster or NULL
 */
rt_raster
rt_raster_from_gdal_dataset(GDALDatasetH ds) {
	rt_raster rast = NULL;
	double gt[6] = {0};
	CPLErr cplerr;
	uint32_t width = 0;
	uint32_t height = 0;
	uint32_t numBands = 0;
	int i = 0;
	char *authname = NULL;
	char *authcode = NULL;

	GDALRasterBandH gdband = NULL;
	GDALDataType gdpixtype = GDT_Unknown;
	rt_band band;
	int32_t idx;
	rt_pixtype pt = PT_END;
	uint32_t ptlen = 0;
	int hasnodata = 0;
	double nodataval;

	int x;
	int y;

	int nXBlocks, nYBlocks;
	int nXBlockSize, nYBlockSize;
	int iXBlock, iYBlock;
	int nXValid, nYValid;
	int iY;

	uint8_t *values = NULL;
	uint32_t valueslen = 0;
	uint8_t *ptr = NULL;

	assert(NULL != ds);

	/* raster size */
	width = GDALGetRasterXSize(ds);
	height = GDALGetRasterYSize(ds);
	RASTER_DEBUGF(3, "Raster dimensions (width x height): %d x %d", width, height);

	/* create new raster */
	RASTER_DEBUG(3, "Creating new raster");
	rast = rt_raster_new(width, height);
	if (NULL == rast) {
		rterror("rt_raster_from_gdal_dataset: Out of memory allocating new raster");
		return NULL;
	}
	RASTER_DEBUGF(3, "Created raster dimensions (width x height): %d x %d", rast->width, rast->height);

	/* get raster attributes */
	cplerr = GDALGetGeoTransform(ds, gt);
	if (GDALGetGeoTransform(ds, gt) != CE_None) {
		RASTER_DEBUG(4, "Using default geotransform matrix (0, 1, 0, 0, 0, -1)");
		gt[0] = 0;
		gt[1] = 1;
		gt[2] = 0;
		gt[3] = 0;
		gt[4] = 0;
		gt[5] = -1;
	}

	/* apply raster attributes */
	rt_raster_set_geotransform_matrix(rast, gt);

	RASTER_DEBUGF(3, "Raster geotransform (%f, %f, %f, %f, %f, %f)",
		gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]);

	/* srid */
	if (rt_util_gdal_sr_auth_info(ds, &authname, &authcode) == ES_NONE) {
		if (
			authname != NULL &&
			strcmp(authname, "EPSG") == 0 &&
			authcode != NULL
		) {
			rt_raster_set_srid(rast, atoi(authcode));
			RASTER_DEBUGF(3, "New raster's SRID = %d", rast->srid);
		}

		if (authname != NULL)
			rtdealloc(authname);
		if (authcode != NULL)
			rtdealloc(authcode);
	}

	numBands = GDALGetRasterCount(ds);

#if POSTGIS_DEBUG_LEVEL > 3
	for (i = 1; i <= numBands; i++) {
		GDALRasterBandH _grb = NULL;
		double _min;
		double _max;
		double _mean;
		double _stddev;

		_grb = GDALGetRasterBand(ds, i);
		GDALComputeRasterStatistics(_grb, FALSE, &_min, &_max, &_mean, &_stddev, NULL, NULL);
		RASTER_DEBUGF(4, "GDAL Band %d stats: %f, %f, %f, %f", i, _min, _max, _mean, _stddev);
	}
#endif

	/* copy bands */
	for (i = 1; i <= numBands; i++) {
		RASTER_DEBUGF(3, "Processing band %d of %d", i, numBands);
		gdband = NULL;
		gdband = GDALGetRasterBand(ds, i);
		if (NULL == gdband) {
			rterror("rt_raster_from_gdal_dataset: Could not get GDAL band");
			rt_raster_destroy(rast);
			return NULL;
		}
		RASTER_DEBUGF(4, "gdband @ %p", gdband);

		/* pixtype */
		gdpixtype = GDALGetRasterDataType(gdband);
		RASTER_DEBUGF(4, "gdpixtype, size = %s, %d", GDALGetDataTypeName(gdpixtype), GDALGetDataTypeSize(gdpixtype) / 8);
		pt = rt_util_gdal_datatype_to_pixtype(gdpixtype);
		if (pt == PT_END) {
			rterror("rt_raster_from_gdal_dataset: Unknown pixel type for GDAL band");
			rt_raster_destroy(rast);
			return NULL;
		}
		ptlen = rt_pixtype_size(pt);

		/* size: width and height */
		width = GDALGetRasterBandXSize(gdband);
		height = GDALGetRasterBandYSize(gdband);
		RASTER_DEBUGF(3, "GDAL band dimensions (width x height): %d x %d", width, height);

		/* nodata */
		nodataval = GDALGetRasterNoDataValue(gdband, &hasnodata);
		RASTER_DEBUGF(3, "(hasnodata, nodataval) = (%d, %f)", hasnodata, nodataval);

		/* create band object */
		idx = rt_raster_generate_new_band(
			rast, pt,
			(hasnodata ? nodataval : 0),
			hasnodata, nodataval, rt_raster_get_num_bands(rast)
		);
		if (idx < 0) {
			rterror("rt_raster_from_gdal_dataset: Could not allocate memory for raster band");
			rt_raster_destroy(rast);
			return NULL;
		}
		band = rt_raster_get_band(rast, idx);
		RASTER_DEBUGF(3, "Created band of dimension (width x height): %d x %d", band->width, band->height);

		/* this makes use of GDAL's "natural" blocks */
		GDALGetBlockSize(gdband, &nXBlockSize, &nYBlockSize);
		nXBlocks = (width + nXBlockSize - 1) / nXBlockSize;
		nYBlocks = (height + nYBlockSize - 1) / nYBlockSize;
		RASTER_DEBUGF(4, "(nXBlockSize, nYBlockSize) = (%d, %d)", nXBlockSize, nYBlockSize);
		RASTER_DEBUGF(4, "(nXBlocks, nYBlocks) = (%d, %d)", nXBlocks, nYBlocks);

		/* allocate memory for values */
		valueslen = ptlen * nXBlockSize * nYBlockSize;
		values = rtalloc(valueslen);
		if (values == NULL) {
			rterror("rt_raster_from_gdal_dataset: Could not allocate memory for GDAL band pixel values");
			rt_raster_destroy(rast);
			return NULL;
		}
		RASTER_DEBUGF(3, "values @ %p of length = %d", values, valueslen);

		for (iYBlock = 0; iYBlock < nYBlocks; iYBlock++) {
			for (iXBlock = 0; iXBlock < nXBlocks; iXBlock++) {
				x = iXBlock * nXBlockSize;
				y = iYBlock * nYBlockSize;
				RASTER_DEBUGF(4, "(iXBlock, iYBlock) = (%d, %d)", iXBlock, iYBlock);
				RASTER_DEBUGF(4, "(x, y) = (%d, %d)", x, y);

				memset(values, 0, valueslen);

				/* valid block width */
				if ((iXBlock + 1) * nXBlockSize > width)
					nXValid = width - (iXBlock * nXBlockSize);
				else
					nXValid = nXBlockSize;

				/* valid block height */
				if ((iYBlock + 1) * nYBlockSize > height)
					nYValid = height - (iYBlock * nYBlockSize);
				else
					nYValid = nYBlockSize;

				RASTER_DEBUGF(4, "(nXValid, nYValid) = (%d, %d)", nXValid, nYValid);

				cplerr = GDALRasterIO(
					gdband, GF_Read,
					x, y,
					nXValid, nYValid,
					values, nXValid, nYValid,
					gdpixtype,
					0, 0
				);
				if (cplerr != CE_None) {
					rterror("rt_raster_from_gdal_dataset: Could not get data from GDAL raster");
					rtdealloc(values);
					rt_raster_destroy(rast);
					return NULL;
				}

				/* if block width is same as raster width, shortcut */
				if (nXBlocks == 1 && nYBlockSize > 1 && nXValid == width) {
					x = 0;
					y = nYBlockSize * iYBlock;

					RASTER_DEBUGF(4, "Setting set of pixel lines at (%d, %d) for %d pixels", x, y, nXValid * nYValid);
					rt_band_set_pixel_line(band, x, y, values, nXValid * nYValid);
				}
				else {
					ptr = values;
					x = nXBlockSize * iXBlock;
					for (iY = 0; iY < nYValid; iY++) {
						y = iY + (nYBlockSize * iYBlock);

						RASTER_DEBUGF(4, "Setting pixel line at (%d, %d) for %d pixels", x, y, nXValid);
						rt_band_set_pixel_line(band, x, y, ptr, nXValid);
						ptr += (nXValid * ptlen);
					}
				}
			}
		}

		/* free memory */
		rtdealloc(values);
	}

	return rast;
}

/******************************************************************************
* rt_raster_gdal_rasterize()
******************************************************************************/

typedef struct _rti_rasterize_arg_t* _rti_rasterize_arg;
struct _rti_rasterize_arg_t {
	uint8_t noband;

	uint32_t numbands; 

	OGRSpatialReferenceH src_sr;

	rt_pixtype *pixtype;
	double *init;
	double *nodata;
	uint8_t *hasnodata;
	double *value;
	int *bandlist;
};

static _rti_rasterize_arg
_rti_rasterize_arg_init() {
	_rti_rasterize_arg arg = NULL;

	arg = rtalloc(sizeof(struct _rti_rasterize_arg_t));
	if (arg == NULL) {
		rterror("_rti_rasterize_arg_init: Could not allocate memory for _rti_rasterize_arg");
		return NULL;
	}

	arg->noband = 0;

	arg->numbands = 0;

	arg->src_sr = NULL;

	arg->pixtype = NULL;
	arg->init = NULL;
	arg->nodata = NULL;
	arg->hasnodata = NULL;
	arg->value = NULL;
	arg->bandlist = NULL;

	return arg;
}

static void
_rti_rasterize_arg_destroy(_rti_rasterize_arg arg) {
	if (arg->noband) {
		if (arg->pixtype != NULL)
			rtdealloc(arg->pixtype);
		if (arg->init != NULL)
			rtdealloc(arg->init);
		if (arg->nodata != NULL)
			rtdealloc(arg->nodata);
		if (arg->hasnodata != NULL)
			rtdealloc(arg->hasnodata);
		if (arg->value != NULL)
			rtdealloc(arg->value);
	}

	if (arg->bandlist != NULL)
		rtdealloc(arg->bandlist);

	if (arg->src_sr != NULL)
		OSRDestroySpatialReference(arg->src_sr);

	rtdealloc(arg);
}

/**
 * Return a raster of the provided geometry
 *
 * @param wkb : WKB representation of the geometry to convert
 * @param wkb_len : length of the WKB representation of the geometry
 * @param srs : the geometry's coordinate system in OGC WKT
 * @param num_bands : number of bands in the output raster
 * @param pixtype : data type of each band
 * @param init : array of values to initialize each band with
 * @param value : array of values for pixels of geometry
 * @param nodata : array of nodata values for each band
 * @param hasnodata : array flagging the presence of nodata for each band
 * @param width : the number of columns of the raster
 * @param height : the number of rows of the raster
 * @param scale_x : the pixel width of the raster
 * @param scale_y : the pixel height of the raster
 * @param ul_xw : the X value of upper-left corner of the raster
 * @param ul_yw : the Y value of upper-left corner of the raster
 * @param grid_xw : the X value of point on grid to align raster to
 * @param grid_yw : the Y value of point on grid to align raster to
 * @param skew_x : the X skew of the raster
 * @param skew_y : the Y skew of the raster
 * @param options : array of options.  only option is "ALL_TOUCHED"
 *
 * @return the raster of the provided geometry or NULL
 */
rt_raster
rt_raster_gdal_rasterize(
	const unsigned char *wkb, uint32_t wkb_len,
	const char *srs,
	uint32_t num_bands, rt_pixtype *pixtype,
	double *init, double *value,
	double *nodata, uint8_t *hasnodata,
	int *width, int *height,
	double *scale_x, double *scale_y,
	double *ul_xw, double *ul_yw,
	double *grid_xw, double *grid_yw,
	double *skew_x, double *skew_y,
	char **options
) {
	rt_raster rast = NULL;
	int i = 0;
	int err = 0;

	_rti_rasterize_arg arg = NULL;

	int _dim[2] = {0};
	double _scale[2] = {0};
	double _skew[2] = {0};

	OGRErr ogrerr;
	OGRGeometryH src_geom;
	OGREnvelope src_env;
	rt_envelope extent;
	OGRwkbGeometryType wkbtype = wkbUnknown;

	int ul_user = 0;

	CPLErr cplerr;
	double _gt[6] = {0};
	GDALDriverH _drv = NULL;
	int unload_drv = 0;
	GDALDatasetH _ds = NULL;
	GDALRasterBandH _band = NULL;

	uint16_t _width = 0;
	uint16_t _height = 0;

	RASTER_DEBUG(3, "starting");

	assert(NULL != wkb);
	assert(0 != wkb_len);

	/* internal variables */
	arg = _rti_rasterize_arg_init();
	if (arg == NULL) {
		rterror("rt_raster_gdal_rasterize: Could not initialize internal variables");
		return NULL;
	}

	/* no bands, raster is a mask */
	if (num_bands < 1) {
		arg->noband = 1;
		arg->numbands = 1;

		arg->pixtype = (rt_pixtype *) rtalloc(sizeof(rt_pixtype));
		arg->pixtype[0] = PT_8BUI;

		arg->init = (double *) rtalloc(sizeof(double));
		arg->init[0] = 0;

		arg->nodata = (double *) rtalloc(sizeof(double));
		arg->nodata[0] = 0;

		arg->hasnodata = (uint8_t *) rtalloc(sizeof(uint8_t));
		arg->hasnodata[0] = 1;

		arg->value = (double *) rtalloc(sizeof(double));
		arg->value[0] = 1;
	}
	else {
		arg->noband = 0;
		arg->numbands = num_bands;

		arg->pixtype = pixtype;
		arg->init = init;
		arg->nodata = nodata;
		arg->hasnodata = hasnodata;
		arg->value = value;
	}

	/* OGR spatial reference */
	if (NULL != srs && strlen(srs)) {
		arg->src_sr = OSRNewSpatialReference(NULL);
		if (OSRSetFromUserInput(arg->src_sr, srs) != OGRERR_NONE) {
			rterror("rt_raster_gdal_rasterize: Could not create OSR spatial reference using the provided srs: %s", srs);
			_rti_rasterize_arg_destroy(arg);
			return NULL;
		}
	}

	/* convert WKB to OGR Geometry */
	ogrerr = OGR_G_CreateFromWkb((unsigned char *) wkb, arg->src_sr, &src_geom, wkb_len);
	if (ogrerr != OGRERR_NONE) {
		rterror("rt_raster_gdal_rasterize: Could not create OGR Geometry from WKB");

		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return NULL;
	}

	/* OGR Geometry is empty */
	if (OGR_G_IsEmpty(src_geom)) {
		rtinfo("Geometry provided is empty. Returning empty raster");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return rt_raster_new(0, 0);
	}

	/* get envelope */
	OGR_G_GetEnvelope(src_geom, &src_env);
	rt_util_from_ogr_envelope(src_env, &extent);

	RASTER_DEBUGF(3, "Suggested raster envelope: %f, %f, %f, %f",
		extent.MinX, extent.MinY, extent.MaxX, extent.MaxY);

	/* user-defined scale */
	if (
		(NULL != scale_x) &&
		(NULL != scale_y) &&
		(FLT_NEQ(*scale_x, 0.0)) &&
		(FLT_NEQ(*scale_y, 0.0))
	) {
		/* for now, force scale to be in left-right, top-down orientation */
		_scale[0] = fabs(*scale_x);
		_scale[1] = fabs(*scale_y);
	}
	/* user-defined width/height */
	else if (
		(NULL != width) &&
		(NULL != height) &&
		(FLT_NEQ(*width, 0.0)) &&
		(FLT_NEQ(*height, 0.0))
	) {
		_dim[0] = abs(*width);
		_dim[1] = abs(*height);

		if (FLT_NEQ(extent.MaxX, extent.MinX))
			_scale[0] = fabs((extent.MaxX - extent.MinX) / _dim[0]);
		else
			_scale[0] = 1.;

		if (FLT_NEQ(extent.MaxY, extent.MinY))
			_scale[1] = fabs((extent.MaxY - extent.MinY) / _dim[1]);
		else
			_scale[1] = 1.;
	}
	else {
		rterror("rt_raster_gdal_rasterize: Values must be provided for width and height or X and Y of scale");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return NULL;
	}
	RASTER_DEBUGF(3, "scale (x, y) = %f, %f", _scale[0], -1 * _scale[1]);
	RASTER_DEBUGF(3, "dim (x, y) = %d, %d", _dim[0], _dim[1]);

	/* user-defined skew */
	if (NULL != skew_x) {
		_skew[0] = *skew_x;

		/*
			negative scale-x affects skew
			for now, force skew to be in left-right, top-down orientation
		*/
		if (
			NULL != scale_x &&
			*scale_x < 0.
		) {
			_skew[0] *= -1;
		}
	}
	if (NULL != skew_y) {
		_skew[1] = *skew_y;

		/*
			positive scale-y affects skew
			for now, force skew to be in left-right, top-down orientation
		*/
		if (
			NULL != scale_y &&
			*scale_y > 0.
		) {
			_skew[1] *= -1;
		}
	}

	/*
	 	if geometry is a point, a linestring or set of either and bounds not set,
		increase extent by a pixel to avoid missing points on border

		a whole pixel is used instead of half-pixel due to backward
		compatibility with GDAL 1.6, 1.7 and 1.8.  1.9+ works fine with half-pixel.
	*/
	wkbtype = wkbFlatten(OGR_G_GetGeometryType(src_geom));
	if ((
			(wkbtype == wkbPoint) ||
			(wkbtype == wkbMultiPoint) ||
			(wkbtype == wkbLineString) ||
			(wkbtype == wkbMultiLineString)
		) &&
		_dim[0] == 0 &&
		_dim[1] == 0
	) {
		int result;
		LWPOLY *epoly = NULL;
		LWGEOM *lwgeom = NULL;
		GEOSGeometry *egeom = NULL;
		GEOSGeometry *geom = NULL;

		RASTER_DEBUG(3, "Testing geometry is properly contained by extent");

		/*
			see if geometry is properly contained by extent
			all parts of geometry lies within extent
		*/

		/* initialize GEOS */
		initGEOS(lwnotice, lwgeom_geos_error);

		/* convert envelope to geometry */
		RASTER_DEBUG(4, "Converting envelope to geometry");
		epoly = rt_util_envelope_to_lwpoly(extent);
		if (epoly == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not create envelope's geometry to test if geometry is properly contained by extent");

			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);
			/* OGRCleanupAll(); */

			return NULL;
		}

		egeom = (GEOSGeometry *) LWGEOM2GEOS(lwpoly_as_lwgeom(epoly), 0);
		lwpoly_free(epoly);

		/* convert WKB to geometry */
		RASTER_DEBUG(4, "Converting WKB to geometry");
		lwgeom = lwgeom_from_wkb(wkb, wkb_len, LW_PARSER_CHECK_NONE);
		geom = (GEOSGeometry *) LWGEOM2GEOS(lwgeom, 0);
		lwgeom_free(lwgeom);

		result = GEOSRelatePattern(egeom, geom, "T**FF*FF*");
		GEOSGeom_destroy(geom);
		GEOSGeom_destroy(egeom);

		if (result == 2) {
			rterror("rt_raster_gdal_rasterize: Could not test if geometry is properly contained by extent for geometry within extent");

			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);
			/* OGRCleanupAll(); */

			return NULL;
		}

		/* geometry NOT properly contained by extent */
		if (!result) {

#if POSTGIS_GDAL_VERSION > 18

			/* check alignment flag: grid_xw */
			if (
				(NULL == ul_xw && NULL == ul_yw) &&
				(NULL != grid_xw && NULL != grid_xw) &&
				FLT_NEQ(*grid_xw, extent.MinX)
			) {
				/* do nothing */
				RASTER_DEBUG(3, "Skipping extent adjustment on X-axis due to upcoming alignment");
			}
			else {
				RASTER_DEBUG(3, "Adjusting extent for GDAL > 1.8 by half the scale on X-axis");
				extent.MinX -= (_scale[0] / 2.);
				extent.MaxX += (_scale[0] / 2.);
			}

			/* check alignment flag: grid_yw */
			if (
				(NULL == ul_xw && NULL == ul_yw) &&
				(NULL != grid_xw && NULL != grid_xw) &&
				FLT_NEQ(*grid_yw, extent.MaxY)
			) {
				/* do nothing */
				RASTER_DEBUG(3, "Skipping extent adjustment on Y-axis due to upcoming alignment");
			}
			else {
				RASTER_DEBUG(3, "Adjusting extent for GDAL > 1.8 by half the scale on Y-axis");
				extent.MinY -= (_scale[1] / 2.);
				extent.MaxY += (_scale[1] / 2.);
			}

#else

			/* check alignment flag: grid_xw */
			if (
				(NULL == ul_xw && NULL == ul_yw) &&
				(NULL != grid_xw && NULL != grid_xw) &&
				FLT_NEQ(*grid_xw, extent.MinX)
			) {
				/* do nothing */
				RASTER_DEBUG(3, "Skipping extent adjustment on X-axis due to upcoming alignment");
			}
			else {
				RASTER_DEBUG(3, "Adjusting extent for GDAL <= 1.8 by the scale on X-axis");
				extent.MinX -= _scale[0];
				extent.MaxX += _scale[0];
			}


			/* check alignment flag: grid_yw */
			if (
				(NULL == ul_xw && NULL == ul_yw) &&
				(NULL != grid_xw && NULL != grid_xw) &&
				FLT_NEQ(*grid_yw, extent.MaxY)
			) {
				/* do nothing */
				RASTER_DEBUG(3, "Skipping extent adjustment on Y-axis due to upcoming alignment");
			}
			else {
				RASTER_DEBUG(3, "Adjusting extent for GDAL <= 1.8 by the scale on Y-axis");
				extent.MinY -= _scale[1];
				extent.MaxY += _scale[1];
			}

#endif

		}

		RASTER_DEBUGF(3, "Adjusted extent: %f, %f, %f, %f",
			extent.MinX, extent.MinY, extent.MaxX, extent.MaxY);

		extent.UpperLeftX = extent.MinX;
		extent.UpperLeftY = extent.MaxY;
	}

	/* reprocess extent if skewed */
	if (
		FLT_NEQ(_skew[0], 0) ||
		FLT_NEQ(_skew[1], 0)
	) {
		rt_raster skewedrast;

		RASTER_DEBUG(3, "Computing skewed extent's envelope");

		skewedrast = rt_raster_compute_skewed_raster(
			extent,
			_skew,
			_scale,
			0.01
		);
		if (skewedrast == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not compute skewed raster");

			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);
			/* OGRCleanupAll(); */

			return NULL;
		}

		_dim[0] = skewedrast->width;
		_dim[1] = skewedrast->height;

		extent.UpperLeftX = skewedrast->ipX;
		extent.UpperLeftY = skewedrast->ipY;

		rt_raster_destroy(skewedrast);
	}

	/* raster dimensions */
	if (!_dim[0])
		_dim[0] = (int) fmax((fabs(extent.MaxX - extent.MinX) + (_scale[0] / 2.)) / _scale[0], 1);
	if (!_dim[1])
		_dim[1] = (int) fmax((fabs(extent.MaxY - extent.MinY) + (_scale[1] / 2.)) / _scale[1], 1);

	/* temporary raster */
	rast = rt_raster_new(_dim[0], _dim[1]);
	if (rast == NULL) {
		rterror("rt_raster_gdal_rasterize: Out of memory allocating temporary raster");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return NULL;
	}

	/* set raster's spatial attributes */
	rt_raster_set_offsets(rast, extent.UpperLeftX, extent.UpperLeftY);
	rt_raster_set_scale(rast, _scale[0], -1 * _scale[1]);
	rt_raster_set_skews(rast, _skew[0], _skew[1]);

	rt_raster_get_geotransform_matrix(rast, _gt);
	RASTER_DEBUGF(3, "Temp raster's geotransform: %f, %f, %f, %f, %f, %f",
		_gt[0], _gt[1], _gt[2], _gt[3], _gt[4], _gt[5]);
	RASTER_DEBUGF(3, "Temp raster's dimensions (width x height): %d x %d",
		_dim[0], _dim[1]);

	/* user-specified upper-left corner */
	if (
		NULL != ul_xw &&
		NULL != ul_yw
	) {
		ul_user = 1;

		RASTER_DEBUGF(4, "Using user-specified upper-left corner: %f, %f", *ul_xw, *ul_yw);

		/* set upper-left corner */
		rt_raster_set_offsets(rast, *ul_xw, *ul_yw);
		extent.UpperLeftX = *ul_xw;
		extent.UpperLeftY = *ul_yw;
	}
	else if (
		((NULL != ul_xw) && (NULL == ul_yw)) ||
		((NULL == ul_xw) && (NULL != ul_yw))
	) {
		rterror("rt_raster_gdal_rasterize: Both X and Y upper-left corner values must be provided");

		rt_raster_destroy(rast);
		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return NULL;
	}

	/* alignment only considered if upper-left corner not provided */
	if (
		!ul_user && (
			(NULL != grid_xw) || (NULL != grid_yw)
		)
	) {

		if (
			((NULL != grid_xw) && (NULL == grid_yw)) ||
			((NULL == grid_xw) && (NULL != grid_yw))
		) {
			rterror("rt_raster_gdal_rasterize: Both X and Y alignment values must be provided");

			rt_raster_destroy(rast);
			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);
			/* OGRCleanupAll(); */

			return NULL;
		}

		RASTER_DEBUGF(4, "Aligning extent to user-specified grid: %f, %f", *grid_xw, *grid_yw);

		do {
			double _r[2] = {0};
			double _w[2] = {0};

			/* raster is already aligned */
			if (FLT_EQ(*grid_xw, extent.UpperLeftX) && FLT_EQ(*grid_yw, extent.UpperLeftY)) {
				RASTER_DEBUG(3, "Skipping raster alignment as it is already aligned to grid");
				break;
			}

			extent.UpperLeftX = rast->ipX;
			extent.UpperLeftY = rast->ipY;
			rt_raster_set_offsets(rast, *grid_xw, *grid_yw);

			/* process upper-left corner */
			if (rt_raster_geopoint_to_cell(
				rast,
				extent.UpperLeftX, extent.UpperLeftY,
				&(_r[0]), &(_r[1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_gdal_rasterize: Could not compute raster pixel for spatial coordinates");

				rt_raster_destroy(rast);
				OGR_G_DestroyGeometry(src_geom);
				_rti_rasterize_arg_destroy(arg);
				/* OGRCleanupAll(); */

				return NULL;
			}

			if (rt_raster_cell_to_geopoint(
				rast,
				_r[0], _r[1],
				&(_w[0]), &(_w[1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_gdal_rasterize: Could not compute spatial coordinates for raster pixel");

				rt_raster_destroy(rast);
				OGR_G_DestroyGeometry(src_geom);
				_rti_rasterize_arg_destroy(arg);
				/* OGRCleanupAll(); */

				return NULL;
			}

			/* shift occurred */
			if (FLT_NEQ(_w[0], extent.UpperLeftX)) {
				if (NULL == width)
					rast->width++;
				else if (NULL == scale_x) {
					double _c[2] = {0};

					rt_raster_set_offsets(rast, extent.UpperLeftX, extent.UpperLeftY);

					/* get upper-right corner */
					if (rt_raster_cell_to_geopoint(
						rast,
						rast->width, 0,
						&(_c[0]), &(_c[1]),
						NULL
					) != ES_NONE) {
						rterror("rt_raster_gdal_rasterize: Could not compute spatial coordinates for raster pixel");

						rt_raster_destroy(rast);
						OGR_G_DestroyGeometry(src_geom);
						_rti_rasterize_arg_destroy(arg);
						/* OGRCleanupAll(); */

						return NULL;
					}

					rast->scaleX = fabs((_c[0] - _w[0]) / ((double) rast->width));
				}
			}
			if (FLT_NEQ(_w[1], extent.UpperLeftY)) {
				if (NULL == height)
					rast->height++;
				else if (NULL == scale_y) {
					double _c[2] = {0};

					rt_raster_set_offsets(rast, extent.UpperLeftX, extent.UpperLeftY);

					/* get upper-right corner */
					if (rt_raster_cell_to_geopoint(
						rast,
						0, rast->height,
						&(_c[0]), &(_c[1]),
						NULL
					) != ES_NONE) {
						rterror("rt_raster_gdal_rasterize: Could not compute spatial coordinates for raster pixel");

						rt_raster_destroy(rast);
						OGR_G_DestroyGeometry(src_geom);
						_rti_rasterize_arg_destroy(arg);
						/* OGRCleanupAll(); */

						return NULL;
					}

					rast->scaleY = -1 * fabs((_c[1] - _w[1]) / ((double) rast->height));
				}
			}

			rt_raster_set_offsets(rast, _w[0], _w[1]);
		}
		while (0);
	}

	/*
		after this point, rt_envelope extent is no longer used
	*/

	/* get key attributes from rast */
	_dim[0] = rast->width;
	_dim[1] = rast->height;
	rt_raster_get_geotransform_matrix(rast, _gt);

	/* scale-x is negative or scale-y is positive */
	if ((
		(NULL != scale_x) && (*scale_x < 0.)
	) || (
		(NULL != scale_y) && (*scale_y > 0)
	)) {
		double _w[2] = {0};

		/* negative scale-x */
		if (
			(NULL != scale_x) &&
			(*scale_x < 0.)
		) {
			RASTER_DEBUG(3, "Processing negative scale-x");

			if (rt_raster_cell_to_geopoint(
				rast,
				_dim[0], 0,
				&(_w[0]), &(_w[1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_gdal_rasterize: Could not compute spatial coordinates for raster pixel");

				rt_raster_destroy(rast);
				OGR_G_DestroyGeometry(src_geom);
				_rti_rasterize_arg_destroy(arg);
				/* OGRCleanupAll(); */

				return NULL;
			}

			_gt[0] = _w[0];
			_gt[1] = *scale_x;

			/* check for skew */
			if (NULL != skew_x && FLT_NEQ(*skew_x, 0))
				_gt[2] = *skew_x;
		}
		/* positive scale-y */
		if (
			(NULL != scale_y) &&
			(*scale_y > 0)
		) {
			RASTER_DEBUG(3, "Processing positive scale-y");

			if (rt_raster_cell_to_geopoint(
				rast,
				0, _dim[1],
				&(_w[0]), &(_w[1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_gdal_rasterize: Could not compute spatial coordinates for raster pixel");

				rt_raster_destroy(rast);
				OGR_G_DestroyGeometry(src_geom);
				_rti_rasterize_arg_destroy(arg);
				/* OGRCleanupAll(); */

				return NULL;
			}

			_gt[3] = _w[1];
			_gt[5] = *scale_y;

			/* check for skew */
			if (NULL != skew_y && FLT_NEQ(*skew_y, 0))
				_gt[4] = *skew_y;
		}
	}

	rt_raster_destroy(rast);
	rast = NULL;

	RASTER_DEBUGF(3, "Applied geotransform: %f, %f, %f, %f, %f, %f",
		_gt[0], _gt[1], _gt[2], _gt[3], _gt[4], _gt[5]);
	RASTER_DEBUGF(3, "Raster dimensions (width x height): %d x %d",
		_dim[0], _dim[1]);

	/* load GDAL mem */
	if (!rt_util_gdal_driver_registered("MEM")) {
		RASTER_DEBUG(4, "Registering MEM driver");
		GDALRegister_MEM();
		unload_drv = 1;
	}
	_drv = GDALGetDriverByName("MEM");
	if (NULL == _drv) {
		rterror("rt_raster_gdal_rasterize: Could not load the MEM GDAL driver");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		return NULL;
	}

	/* unload driver from GDAL driver manager */
	if (unload_drv) {
		RASTER_DEBUG(4, "Deregistering MEM driver");
		GDALDeregisterDriver(_drv);
	}

	_ds = GDALCreate(_drv, "", _dim[0], _dim[1], 0, GDT_Byte, NULL);
	if (NULL == _ds) {
		rterror("rt_raster_gdal_rasterize: Could not create a GDALDataset to rasterize the geometry into");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */
		if (unload_drv) GDALDestroyDriver(_drv);

		return NULL;
	}

	/* set geotransform */
	cplerr = GDALSetGeoTransform(_ds, _gt);
	if (cplerr != CE_None) {
		rterror("rt_raster_gdal_rasterize: Could not set geotransform on GDALDataset");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		GDALClose(_ds);
		if (unload_drv) GDALDestroyDriver(_drv);

		return NULL;
	}

	/* set SRS */
	if (NULL != arg->src_sr) {
		char *_srs = NULL;
		OSRExportToWkt(arg->src_sr, &_srs);

		cplerr = GDALSetProjection(_ds, _srs);
		CPLFree(_srs);
		if (cplerr != CE_None) {
			rterror("rt_raster_gdal_rasterize: Could not set projection on GDALDataset");

			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);
			/* OGRCleanupAll(); */

			GDALClose(_ds);
		if (unload_drv) GDALDestroyDriver(_drv);

			return NULL;
		}
	}

	/* set bands */
	for (i = 0; i < arg->numbands; i++) {
		err = 0;

		do {
			/* add band */
			cplerr = GDALAddBand(_ds, rt_util_pixtype_to_gdal_datatype(arg->pixtype[i]), NULL);
			if (cplerr != CE_None) {
				rterror("rt_raster_gdal_rasterize: Could not add band to GDALDataset");
				err = 1;
				break;
			}

			_band = GDALGetRasterBand(_ds, i + 1);
			if (NULL == _band) {
				rterror("rt_raster_gdal_rasterize: Could not get band %d from GDALDataset", i + 1);
				err = 1;
				break;
			}

			/* nodata value */
			if (arg->hasnodata[i]) {
				RASTER_DEBUGF(4, "Setting NODATA value of band %d to %f", i, arg->nodata[i]);
				cplerr = GDALSetRasterNoDataValue(_band, arg->nodata[i]);
				if (cplerr != CE_None) {
					rterror("rt_raster_gdal_rasterize: Could not set nodata value");
					err = 1;
					break;
				}
				RASTER_DEBUGF(4, "NODATA value set to %f", GDALGetRasterNoDataValue(_band, NULL));
			}

			/* initial value */
			cplerr = GDALFillRaster(_band, arg->init[i], 0);
			if (cplerr != CE_None) {
				rterror("rt_raster_gdal_rasterize: Could not set initial value");
				err = 1;
				break;
			}
		}
		while (0);

		if (err) {

			OGR_G_DestroyGeometry(src_geom);
			_rti_rasterize_arg_destroy(arg);

			/* OGRCleanupAll(); */

			GDALClose(_ds);
			if (unload_drv) GDALDestroyDriver(_drv);

			return NULL;
		}
	}

	arg->bandlist = (int *) rtalloc(sizeof(int) * arg->numbands);
	for (i = 0; i < arg->numbands; i++) arg->bandlist[i] = i + 1;

	/* burn geometry */
	cplerr = GDALRasterizeGeometries(
		_ds,
		arg->numbands, arg->bandlist,
		1, &src_geom,
		NULL, NULL,
		arg->value,
		options,
		NULL, NULL
	);
	if (cplerr != CE_None) {
		rterror("rt_raster_gdal_rasterize: Could not rasterize geometry");

		OGR_G_DestroyGeometry(src_geom);
		_rti_rasterize_arg_destroy(arg);
		/* OGRCleanupAll(); */

		GDALClose(_ds);
		if (unload_drv) GDALDestroyDriver(_drv);

		return NULL;
	}

	/* convert gdal dataset to raster */
	GDALFlushCache(_ds);
	RASTER_DEBUG(3, "Converting GDAL dataset to raster");
	rast = rt_raster_from_gdal_dataset(_ds);

	OGR_G_DestroyGeometry(src_geom);
	/* OGRCleanupAll(); */

	GDALClose(_ds);
	if (unload_drv) GDALDestroyDriver(_drv);

	if (NULL == rast) {
		rterror("rt_raster_gdal_rasterize: Could not rasterize geometry");
		return NULL;
	}

	/* width, height */
	_width = rt_raster_get_width(rast);
	_height = rt_raster_get_height(rast);

	/* check each band for pixtype */
	for (i = 0; i < arg->numbands; i++) {
		uint8_t *data = NULL;
		rt_band band = NULL;
		rt_band oldband = NULL;

		double val = 0;
		int nodata = 0;
		int hasnodata = 0;
		double nodataval = 0;
		int x = 0;
		int y = 0;

		oldband = rt_raster_get_band(rast, i);
		if (oldband == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not get band %d of output raster", i);
			_rti_rasterize_arg_destroy(arg);
			rt_raster_destroy(rast);
			return NULL;
		}

		/* band is of user-specified type */
		if (rt_band_get_pixtype(oldband) == arg->pixtype[i])
			continue;

		/* hasnodata, nodataval */
		hasnodata = rt_band_get_hasnodata_flag(oldband);
		if (hasnodata)
			rt_band_get_nodata(oldband, &nodataval);

		/* allocate data */
		data = rtalloc(rt_pixtype_size(arg->pixtype[i]) * _width * _height);
		if (data == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not allocate memory for band data");
			_rti_rasterize_arg_destroy(arg);
			rt_raster_destroy(rast);
			return NULL;
		}
		memset(data, 0, rt_pixtype_size(arg->pixtype[i]) * _width * _height);

		/* create new band of correct type */
		band = rt_band_new_inline(
			_width, _height,
			arg->pixtype[i],
			hasnodata, nodataval,
			data
		);
		if (band == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not create band");
			rtdealloc(data);
			_rti_rasterize_arg_destroy(arg);
			rt_raster_destroy(rast);
			return NULL;
		}

		/* give ownership of data to band */
		rt_band_set_ownsdata_flag(band, 1);

		/* copy pixel by pixel */
		for (x = 0; x < _width; x++) {
			for (y = 0; y < _height; y++) {
				err = rt_band_get_pixel(oldband, x, y, &val, &nodata);
				if (err != ES_NONE) {
					rterror("rt_raster_gdal_rasterize: Could not get pixel value");
					_rti_rasterize_arg_destroy(arg);
					rt_raster_destroy(rast);
					rt_band_destroy(band);
					return NULL;
				}

				if (nodata)
					val = nodataval;

				err = rt_band_set_pixel(band, x, y, val, NULL);
				if (err != ES_NONE) {
					rterror("rt_raster_gdal_rasterize: Could not set pixel value");
					_rti_rasterize_arg_destroy(arg);
					rt_raster_destroy(rast);
					rt_band_destroy(band);
					return NULL;
				}
			}
		}

		/* replace band */
		oldband = rt_raster_replace_band(rast, band, i);
		if (oldband == NULL) {
			rterror("rt_raster_gdal_rasterize: Could not replace band %d of output raster", i);
			_rti_rasterize_arg_destroy(arg);
			rt_raster_destroy(rast);
			rt_band_destroy(band);
			return NULL;
		}

		/* free oldband */
		rt_band_destroy(oldband);
	}

	_rti_rasterize_arg_destroy(arg);

	RASTER_DEBUG(3, "done");

	return rast;
}

/******************************************************************************
* rt_raster_from_two_rasters()
******************************************************************************/

/*
 * Return raster of computed extent specified extenttype applied
 * on two input rasters.  The raster returned should be freed by
 * the caller
 *
 * @param rast1 : the first raster
 * @param rast2 : the second raster
 * @param extenttype : type of extent for the output raster
 * @param *rtnraster : raster of computed extent
 * @param *offset : 4-element array indicating the X,Y offsets
 * for each raster. 0,1 for rast1 X,Y. 2,3 for rast2 X,Y.
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_from_two_rasters(
	rt_raster rast1, rt_raster rast2,
	rt_extenttype extenttype,
	rt_raster *rtnraster, double *offset
) {
	int i;

	rt_raster _rast[2] = {rast1, rast2};
	double _offset[2][4] = {{0.}};
	uint16_t _dim[2][2] = {{0}};

	rt_raster raster = NULL;
	int aligned = 0;
	int dim[2] = {0};
	double gt[6] = {0.};

	assert(NULL != rast1);
	assert(NULL != rast2);
	assert(NULL != rtnraster);

	/* set rtnraster to NULL */
	*rtnraster = NULL;

	/* rasters must be aligned */
	if (rt_raster_same_alignment(rast1, rast2, &aligned, NULL) != ES_NONE) {
		rterror("rt_raster_from_two_rasters: Could not test for alignment on the two rasters");
		return ES_ERROR;
	}
	if (!aligned) {
		rterror("rt_raster_from_two_rasters: The two rasters provided do not have the same alignment");
		return ES_ERROR;
	}

	/* dimensions */
	_dim[0][0] = rast1->width;
	_dim[0][1] = rast1->height;
	_dim[1][0] = rast2->width;
	_dim[1][1] = rast2->height;

	/* get raster offsets */
	if (rt_raster_geopoint_to_cell(
		_rast[1],
		_rast[0]->ipX, _rast[0]->ipY,
		&(_offset[1][0]), &(_offset[1][1]),
		NULL
	) != ES_NONE) {
		rterror("rt_raster_from_two_rasters: Could not compute offsets of the second raster relative to the first raster");
		return ES_ERROR;
	}
	_offset[1][0] = -1 * _offset[1][0];
	_offset[1][1] = -1 * _offset[1][1];
	_offset[1][2] = _offset[1][0] + _dim[1][0] - 1;
	_offset[1][3] = _offset[1][1] + _dim[1][1] - 1;

	i = -1;
	switch (extenttype) {
		case ET_FIRST:
			i = 0;
			_offset[0][0] = 0.;
			_offset[0][1] = 0.;
		case ET_LAST:
		case ET_SECOND:
			if (i < 0) {
				i = 1;
				_offset[0][0] = -1 * _offset[1][0];
				_offset[0][1] = -1 * _offset[1][1];
				_offset[1][0] = 0.;
				_offset[1][1] = 0.;
			}

			dim[0] = _dim[i][0];
			dim[1] = _dim[i][1];
			raster = rt_raster_new(
				dim[0],
				dim[1]
			);
			if (raster == NULL) {
				rterror("rt_raster_from_two_rasters: Could not create output raster");
				return ES_ERROR;
			}
			rt_raster_set_srid(raster, _rast[i]->srid);
			rt_raster_get_geotransform_matrix(_rast[i], gt);
			rt_raster_set_geotransform_matrix(raster, gt);
			break;
		case ET_UNION: {
			double off[4] = {0};

			rt_raster_get_geotransform_matrix(_rast[0], gt);
			RASTER_DEBUGF(4, "gt = (%f, %f, %f, %f, %f, %f)",
				gt[0],
				gt[1],
				gt[2],
				gt[3],
				gt[4],
				gt[5]
			);

			/* new raster upper left offset */
			off[0] = 0;
			if (_offset[1][0] < 0)
				off[0] = _offset[1][0];
			off[1] = 0;
			if (_offset[1][1] < 0)
				off[1] = _offset[1][1];

			/* new raster lower right offset */
			off[2] = _dim[0][0] - 1;
			if ((int) _offset[1][2] >= _dim[0][0])
				off[2] = _offset[1][2];
			off[3] = _dim[0][1] - 1;
			if ((int) _offset[1][3] >= _dim[0][1])
				off[3] = _offset[1][3];

			/* upper left corner */
			if (rt_raster_cell_to_geopoint(
				_rast[0],
				off[0], off[1],
				&(gt[0]), &(gt[3]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get spatial coordinates of upper-left pixel of output raster");
				return ES_ERROR;
			}

			dim[0] = off[2] - off[0] + 1;
			dim[1] = off[3] - off[1] + 1;
			RASTER_DEBUGF(4, "off = (%f, %f, %f, %f)",
				off[0],
				off[1],
				off[2],
				off[3]
			);
			RASTER_DEBUGF(4, "dim = (%d, %d)", dim[0], dim[1]);

			raster = rt_raster_new(
				dim[0],
				dim[1]
			);
			if (raster == NULL) {
				rterror("rt_raster_from_two_rasters: Could not create output raster");
				return ES_ERROR;
			}
			rt_raster_set_srid(raster, _rast[0]->srid);
			rt_raster_set_geotransform_matrix(raster, gt);
			RASTER_DEBUGF(4, "gt = (%f, %f, %f, %f, %f, %f)",
				gt[0],
				gt[1],
				gt[2],
				gt[3],
				gt[4],
				gt[5]
			);

			/* get offsets */
			if (rt_raster_geopoint_to_cell(
				_rast[0],
				gt[0], gt[3],
				&(_offset[0][0]), &(_offset[0][1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get offsets of the FIRST raster relative to the output raster");
				rt_raster_destroy(raster);
				return ES_ERROR;
			}
			_offset[0][0] *= -1;
			_offset[0][1] *= -1;

			if (rt_raster_geopoint_to_cell(
				_rast[1],
				gt[0], gt[3],
				&(_offset[1][0]), &(_offset[1][1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get offsets of the SECOND raster relative to the output raster");
				rt_raster_destroy(raster);
				return ES_ERROR;
			}
			_offset[1][0] *= -1;
			_offset[1][1] *= -1;
			break;
		}
		case ET_INTERSECTION: {
			double off[4] = {0};

			/* no intersection */
			if (
				(_offset[1][2] < 0 || _offset[1][0] > (_dim[0][0] - 1)) ||
				(_offset[1][3] < 0 || _offset[1][1] > (_dim[0][1] - 1))
			) {
				RASTER_DEBUG(3, "The two rasters provided have no intersection.  Returning no band raster");

				raster = rt_raster_new(0, 0);
				if (raster == NULL) {
					rterror("rt_raster_from_two_rasters: Could not create output raster");
					return ES_ERROR;
				}
				rt_raster_set_srid(raster, _rast[0]->srid);
				rt_raster_set_scale(raster, 0, 0);

				/* set offsets if provided */
				if (NULL != offset) {
					for (i = 0; i < 4; i++)
						offset[i] = _offset[i / 2][i % 2];
				}

				*rtnraster = raster;
				return ES_NONE;
			}

			if (_offset[1][0] > 0)
				off[0] = _offset[1][0];
			if (_offset[1][1] > 0)
				off[1] = _offset[1][1];

			off[2] = _dim[0][0] - 1;
			if (_offset[1][2] < _dim[0][0])
				off[2] = _offset[1][2];
			off[3] = _dim[0][1] - 1;
			if (_offset[1][3] < _dim[0][1])
				off[3] = _offset[1][3];

			dim[0] = off[2] - off[0] + 1;
			dim[1] = off[3] - off[1] + 1;
			raster = rt_raster_new(
				dim[0],
				dim[1]
			);
			if (raster == NULL) {
				rterror("rt_raster_from_two_rasters: Could not create output raster");
				return ES_ERROR;
			}
			rt_raster_set_srid(raster, _rast[0]->srid);

			/* get upper-left corner */
			rt_raster_get_geotransform_matrix(_rast[0], gt);
			if (rt_raster_cell_to_geopoint(
				_rast[0],
				off[0], off[1],
				&(gt[0]), &(gt[3]),
				gt
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get spatial coordinates of upper-left pixel of output raster");
				rt_raster_destroy(raster);
				return ES_ERROR;
			}

			rt_raster_set_geotransform_matrix(raster, gt);

			/* get offsets */
			if (rt_raster_geopoint_to_cell(
				_rast[0],
				gt[0], gt[3],
				&(_offset[0][0]), &(_offset[0][1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get pixel coordinates to compute the offsets of the FIRST raster relative to the output raster");
				rt_raster_destroy(raster);
				return ES_ERROR;
			}
			_offset[0][0] *= -1;
			_offset[0][1] *= -1;

			if (rt_raster_geopoint_to_cell(
				_rast[1],
				gt[0], gt[3],
				&(_offset[1][0]), &(_offset[1][1]),
				NULL
			) != ES_NONE) {
				rterror("rt_raster_from_two_rasters: Could not get pixel coordinates to compute the offsets of the SECOND raster relative to the output raster");
				rt_raster_destroy(raster);
				return ES_ERROR;
			}
			_offset[1][0] *= -1;
			_offset[1][1] *= -1;
			break;
		}
		case ET_CUSTOM:
			rterror("rt_raster_from_two_rasters: Extent type ET_CUSTOM is not supported by this function");
			break;
	}

	/* set offsets if provided */
	if (NULL != offset) {
		for (i = 0; i < 4; i++)
			offset[i] = _offset[i / 2][i % 2];
	}

	*rtnraster = raster;
	return ES_NONE;
}
