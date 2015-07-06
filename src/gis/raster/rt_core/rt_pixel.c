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
 * Copyright (C) 2013  Nathaniel Hunter Clay <clay.nathaniel@gmail.com>
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
* rt_pixeltype
******************************************************************************/

int
rt_pixtype_size(rt_pixtype pixtype) {
	int pixbytes = -1;

	switch (pixtype) {
		case PT_1BB:
		case PT_2BUI:
		case PT_4BUI:
		case PT_8BSI:
		case PT_8BUI:
			pixbytes = 1;
			break;
		case PT_16BSI:
		case PT_16BUI:
			pixbytes = 2;
			break;
		case PT_32BSI:
		case PT_32BUI:
		case PT_32BF:
			pixbytes = 4;
			break;
		case PT_64BF:
			pixbytes = 8;
			break;
		default:
			rterror("rt_pixtype_size: Unknown pixeltype %d", pixtype);
			pixbytes = -1;
			break;
	}

	RASTER_DEBUGF(3, "Pixel type = %s and size = %d bytes",
		rt_pixtype_name(pixtype), pixbytes);

	return pixbytes;
}

int
rt_pixtype_alignment(rt_pixtype pixtype) {
	return rt_pixtype_size(pixtype);
}

rt_pixtype
rt_pixtype_index_from_name(const char* pixname) {
	assert(pixname && strlen(pixname) > 0);

	if (strcmp(pixname, "1BB") == 0)
		return PT_1BB;
	else if (strcmp(pixname, "2BUI") == 0)
		return PT_2BUI;
	else if (strcmp(pixname, "4BUI") == 0)
		return PT_4BUI;
	else if (strcmp(pixname, "8BSI") == 0)
		return PT_8BSI;
	else if (strcmp(pixname, "8BUI") == 0)
		return PT_8BUI;
	else if (strcmp(pixname, "16BSI") == 0)
		return PT_16BSI;
	else if (strcmp(pixname, "16BUI") == 0)
		return PT_16BUI;
	else if (strcmp(pixname, "32BSI") == 0)
		return PT_32BSI;
	else if (strcmp(pixname, "32BUI") == 0)
		return PT_32BUI;
	else if (strcmp(pixname, "32BF") == 0)
		return PT_32BF;
	else if (strcmp(pixname, "64BF") == 0)
		return PT_64BF;

	return PT_END;
}

const char*
rt_pixtype_name(rt_pixtype pixtype) {
	switch (pixtype) {
		case PT_1BB:
			return "1BB";
		case PT_2BUI:
			return "2BUI";
		case PT_4BUI:
			return "4BUI";
		case PT_8BSI:
			return "8BSI";
		case PT_8BUI:
			return "8BUI";
		case PT_16BSI:
			return "16BSI";
		case PT_16BUI:
			return "16BUI";
		case PT_32BSI:
			return "32BSI";
		case PT_32BUI:
			return "32BUI";
		case PT_32BF:
			return "32BF";
		case PT_64BF:
			return "64BF";
		default:
			rterror("rt_pixtype_name: Unknown pixeltype %d", pixtype);
			return "Unknown";
	}
}

/**
 * Return minimum value possible for pixel type
 *
 * @param pixtype : the pixel type to get minimum possible value for
 *
 * @return the minimum possible value for the pixel type.
 */
double
rt_pixtype_get_min_value(rt_pixtype pixtype) {
	switch (pixtype) {
		case PT_1BB: {
			return (double) rt_util_clamp_to_1BB((double) CHAR_MIN);
		}
		case PT_2BUI: {
			return (double) rt_util_clamp_to_2BUI((double) CHAR_MIN);
		}
		case PT_4BUI: {
			return (double) rt_util_clamp_to_4BUI((double) CHAR_MIN);
		}
		case PT_8BUI: {
			return (double) rt_util_clamp_to_8BUI((double) CHAR_MIN);
		}
		case PT_8BSI: {
			return (double) rt_util_clamp_to_8BSI((double) SCHAR_MIN);
		}
		case PT_16BSI: {
			return (double) rt_util_clamp_to_16BSI((double) SHRT_MIN);
		}
		case PT_16BUI: {
			return (double) rt_util_clamp_to_16BUI((double) SHRT_MIN);
		}
		case PT_32BSI: {
			return (double) rt_util_clamp_to_32BSI((double) INT_MIN);
		}
		case PT_32BUI: {
			return (double) rt_util_clamp_to_32BUI((double) INT_MIN);
		}
		case PT_32BF: {
			return (double) -FLT_MAX;
		}
		case PT_64BF: {
			return (double) -DBL_MAX;
		}
		default: {
			rterror("rt_pixtype_get_min_value: Unknown pixeltype %d", pixtype);
			return (double) rt_util_clamp_to_8BUI((double) CHAR_MIN);
		}
	}
}

/**
 * Returns 1 if clamped values are equal, 0 if not equal, -1 if error
 *
 * @param pixtype : the pixel type to clamp the provided values
 * @param val : value to compare to reference value
 * @param refval : reference value to be compared with
 * @param isequal : non-zero if clamped values are equal, 0 otherwise
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_pixtype_compare_clamped_values(
	rt_pixtype pixtype,
	double val, double refval,
	int *isequal
) {
	assert(isequal != NULL);
	*isequal = 0;

	switch (pixtype) {
		case PT_1BB:
			if (rt_util_clamp_to_1BB(val) == rt_util_clamp_to_1BB(refval))
				*isequal = 1;
			break;
		case PT_2BUI:
			if (rt_util_clamp_to_2BUI(val) == rt_util_clamp_to_2BUI(refval))
				*isequal = 1;
			break;
		case PT_4BUI:
			if (rt_util_clamp_to_4BUI(val) == rt_util_clamp_to_4BUI(refval))
				*isequal = 1;
			break;
		case PT_8BSI:
			if (rt_util_clamp_to_8BSI(val) == rt_util_clamp_to_8BSI(refval))
				*isequal = 1;
			break;
		case PT_8BUI:
			if (rt_util_clamp_to_8BUI(val) == rt_util_clamp_to_8BUI(refval))
				*isequal = 1;
			break;
		case PT_16BSI:
			if (rt_util_clamp_to_16BSI(val) == rt_util_clamp_to_16BSI(refval))
				*isequal = 1;
			break;
		case PT_16BUI:
			if (rt_util_clamp_to_16BUI(val) == rt_util_clamp_to_16BUI(refval))
				*isequal = 1;
			break;
		case PT_32BSI:
			if (rt_util_clamp_to_32BSI(val) == rt_util_clamp_to_32BSI(refval))
				*isequal = 1;
			break;
		case PT_32BUI:
			if (rt_util_clamp_to_32BUI(val) == rt_util_clamp_to_32BUI(refval))
				*isequal = 1;
			break;
		case PT_32BF:
			if (FLT_EQ(rt_util_clamp_to_32F(val), rt_util_clamp_to_32F(refval)))
				*isequal = 1;
			break;
		case PT_64BF:
			if (FLT_EQ(val, refval))
				*isequal = 1;
			break;
		default:
			rterror("rt_pixtype_compare_clamped_values: Unknown pixeltype %d", pixtype);
			return ES_ERROR;
	}

	return ES_NONE;
}

/******************************************************************************
* rt_pixel
******************************************************************************/

/*
 * Convert an array of rt_pixel objects to two 2D arrays of value and NODATA.
 * The dimensions of the returned 2D array are [Y][X], going by row Y and
 * then column X.
 *
 * @param npixel : array of rt_pixel objects
 * @param count : number of elements in npixel
 * @param mask : mask to be respected when retruning array
 * @param x : the column of the center pixel (0-based)
 * @param y : the line of the center pixel (0-based)
 * @param distancex : the number of pixels around the specified pixel
 * along the X axis
 * @param distancey : the number of pixels around the specified pixel
 * along the Y axis
 * @param value : pointer to pointer for 2D value array
 * @param nodata : pointer to pointer for 2D NODATA array
 * @param dimx : size of value and nodata along the X axis
 * @param dimy : size of value and nodata along the Y axis
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_pixel_set_to_array(
	rt_pixel npixel, int count, rt_mask mask,
	int x, int y,
	uint16_t distancex, uint16_t distancey,
	double ***value,
	int ***nodata,
	int *dimx, int *dimy
) {
	uint32_t i;
	uint32_t j;
	uint32_t dim[2] = {0};
	double **values = NULL;
	int **nodatas = NULL;
	int zero[2] = {0};
	int _x;
	int _y;

	assert(npixel != NULL && count > 0);
	assert(value != NULL);
	assert(nodata != NULL);

	/* dimensions */
	dim[0] = distancex * 2 + 1;
	dim[1] = distancey * 2 + 1;
	RASTER_DEBUGF(4, "dimensions = %d x %d", dim[0], dim[1]);

	/* make sure that the dimx and dimy match mask */
	if( mask != NULL) {
	  if ( dim[0] != mask-> dimx || dim[1] != mask->dimy ){
	    rterror("rt_pixel_set_array: mask dimentions do not match given dims");
	    return ES_ERROR;
	  }
	  
	  if ( mask->values == NULL || mask->nodata == NULL ) {
	    rterror("rt_pixel_set_array: was not properly setup");
	    return ES_ERROR;
	  }

	}
	/* establish 2D arrays (Y axis) */
	values = rtalloc(sizeof(double *) * dim[1]);
	nodatas = rtalloc(sizeof(int *) * dim[1]);

	if (values == NULL || nodatas == NULL) {
		rterror("rt_pixel_set_to_array: Could not allocate memory for 2D array");
		return ES_ERROR;
	}

	/* initialize X axis */
	for (i = 0; i < dim[1]; i++) {
		values[i] = rtalloc(sizeof(double) * dim[0]);
		nodatas[i] = rtalloc(sizeof(int) * dim[0]);

		if (values[i] == NULL || nodatas[i] == NULL) {
			rterror("rt_pixel_set_to_array: Could not allocate memory for dimension of 2D array");

			if (values[i] == NULL) {
				for (j = 0; j < i; j++) {
					rtdealloc(values[j]);
					rtdealloc(nodatas[j]);
				}
			}
			else {
				for (j = 0; j <= i; j++) {
					rtdealloc(values[j]);
					if (j < i)
						rtdealloc(nodatas[j]);
				}
			}

			rtdealloc(values);
			rtdealloc(nodatas);
			
			return ES_ERROR;
		}

		/* set values to 0 */
		memset(values[i], 0, sizeof(double) * dim[0]);

		/* set nodatas to 1 */
		for (j = 0; j < dim[0]; j++)
			nodatas[i][j] = 1;
	}

	/* get 0,0 of grid */
	zero[0] = x - distancex;
	zero[1] = y - distancey;

	/* populate 2D arrays */
	for (i = 0; i < count; i++) {
		if (npixel[i].nodata)
			continue;

		_x = npixel[i].x - zero[0];
		_y = npixel[i].y - zero[1];

		RASTER_DEBUGF(4, "absolute x,y: %d x %d", npixel[i].x, npixel[i].y);
		RASTER_DEBUGF(4, "relative x,y: %d x %d", _x, _y);

		if ( mask == NULL ) {
		  values[_y][_x] = npixel[i].value;
		  nodatas[_y][_x] = 0;
		}else{ 
		  if( mask->weighted == 0 ){
		    if( FLT_EQ( mask->values[_y][_x],0) || mask->nodata[_y][_x] == 1 ){
		      values[_y][_x] = 0;
		      nodatas[_y][_x] = 1;
		    }else{
		      values[_y][_x] = npixel[i].value;
		      nodatas[_y][_x] = 0;
		    }
		  }else{
		    if( mask->nodata[_y][_x] == 1 ){
		      values[_y][_x] = 0;
		      nodatas[_y][_x] = 1;
		    }else{
		      values[_y][_x] = npixel[i].value * mask->values[_y][_x];
		      nodatas[_y][_x] = 0;
		    }
		  }
		}

		RASTER_DEBUGF(4, "(x, y, nodata, value) = (%d, %d, %d, %f)", _x, _y, nodatas[_y][_x], values[_y][_x]);
	}

	*value = &(*values);
	*nodata = &(*nodatas);
	if (dimx != NULL)
		*dimx = dim[0];
	if (dimy != NULL)
		*dimy = dim[1];

	return ES_NONE;
}
