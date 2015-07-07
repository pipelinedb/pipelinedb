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
 * Copyright (C) 2013 Nathaneil Hunter Clay <clay.nathaniel@gmail.com
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

/**
 * @file librtcore.h
 *
 * This library is the generic raster handling section of PostGIS. The raster
 * objects, constructors, destructors, and a set of spatial processing functions
 * are implemented here.
 *
 * The library is designed for use in non-PostGIS applications if necessary. The
 * units tests at test/core (and the future loader/dumper programs) are examples
 * of non-PostGIS applications using rt_core.
 *
 * Programs using this library should set up the default memory managers and error
 * handlers by implementing an rt_init_allocators() function, which can be as
 * a wrapper around the rt_install_default_allocators() function if you want
 * no special handling for memory management and error reporting.
 *
 **/

/******************************************************************************
* Some rules for *.(c|h) files in rt_core
*
* All functions in rt_core that receive a band index parameter
*   must be 0-based
*
* Variables and functions internal for a public function should be prefixed
*   with _rti_, e.g. _rti_iterator_arg.
******************************************************************************/

#ifndef LIBRTCORE_H_INCLUDED
#define LIBRTCORE_H_INCLUDED

/* define the systems */
#if defined(__linux__)  /* (predefined) */
#if !defined(LINUX)
#define LINUX
#endif
#if !defined(UNIX)
#define UNIX        /* make sure this is defined */
#endif
#endif


#if defined(__FreeBSD__) || defined(__FreeBSD_kernel__) || defined(__OpenBSD__)    /* seems to work like Linux... */
#if !defined(LINUX)
#define LINUX
#endif
#if !defined(UNIX)
#define UNIX        /* make sure this is defined */
#endif
#endif

#if defined(__MSDOS__)
#if !defined(MSDOS)
#define MSDOS       /* make sure this is defined */
#endif
#endif

#if defined(__WIN32__) || defined(__NT__) || defined(_WIN32)
#if !defined(WIN32)
#define WIN32
#endif
#if defined(__BORLANDC__) && defined(MSDOS) /* Borland always defines MSDOS */
#undef  MSDOS
#endif
#endif

#if defined(__APPLE__)
#if !defined(UNIX)
#define UNIX
#endif
#endif

#if defined(sun) || defined(__sun) 
#if !defined(UNIX) 
#define UNIX 
#endif 
#endif

/* if we are in Unix define stricmp to be strcasecmp and strnicmp to */
/* be strncasecmp. I'm not sure if all Unices have these, but Linux */
/* does. */
#if defined(UNIX)
#if !defined(HAVE_STRICMP)
#define stricmp     strcasecmp
#endif
#if !defined(HAVE_STRNICMP)
#define strnicmp    strncasecmp
#endif
#endif

#include <stdio.h> /* for printf, sprintf */
#include <stdlib.h> /* For size_t, srand and rand */
#include <stdint.h> /* For C99 int types */
#include <string.h> /* for memcpy, strlen, etc */
#include <float.h> /* for FLT_EPSILON, DBL_EPSILON and float type limits */
#include <limits.h> /* for integer type limits */

#include "liblwgeom.h"

#include "gdal_alg.h"
#include "gdal_frmts.h"
#include "gdal.h"
#include "gdalwarper.h"
#include "cpl_vsi.h"
#include "cpl_conv.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"

#include "../../postgis_config.h"
#include "../raster_config.h"

/**
 * Types definitions
 */
typedef struct rt_raster_t* rt_raster;
typedef struct rt_band_t* rt_band;
typedef struct rt_pixel_t* rt_pixel;
typedef struct rt_mask_t* rt_mask;
typedef struct rt_geomval_t* rt_geomval;
typedef struct rt_bandstats_t* rt_bandstats;
typedef struct rt_histogram_t* rt_histogram;
typedef struct rt_quantile_t* rt_quantile;
typedef struct rt_valuecount_t* rt_valuecount;
typedef struct rt_gdaldriver_t* rt_gdaldriver;
typedef struct rt_reclassexpr_t* rt_reclassexpr;

typedef struct rt_iterator_t* rt_iterator;
typedef struct rt_iterator_arg_t* rt_iterator_arg;

typedef struct rt_colormap_entry_t* rt_colormap_entry;
typedef struct rt_colormap_t* rt_colormap;

/* envelope information */
typedef struct {
	double MinX;
	double MaxX;
	double MinY;
	double MaxY;

	double UpperLeftX;
	double UpperLeftY;
} rt_envelope;

/**
 * Enum definitions
 */

/* function return error states */
typedef enum {
	ES_NONE = 0, /* no error */
	ES_ERROR = 1 /* generic error */
} rt_errorstate;

/* Pixel types */
typedef enum {
    PT_1BB=0,     /* 1-bit boolean            */
    PT_2BUI=1,    /* 2-bit unsigned integer   */
    PT_4BUI=2,    /* 4-bit unsigned integer   */
    PT_8BSI=3,    /* 8-bit signed integer     */
    PT_8BUI=4,    /* 8-bit unsigned integer   */
    PT_16BSI=5,   /* 16-bit signed integer    */
    PT_16BUI=6,   /* 16-bit unsigned integer  */
    PT_32BSI=7,   /* 32-bit signed integer    */
    PT_32BUI=8,   /* 32-bit unsigned integer  */
    PT_32BF=10,   /* 32-bit float             */
    PT_64BF=11,   /* 64-bit float             */
    PT_END=13
} rt_pixtype;

typedef enum {
	ET_INTERSECTION = 0,
	ET_UNION,
	ET_FIRST,
	ET_SECOND,
	ET_LAST,
	ET_CUSTOM
} rt_extenttype;

/**
 * GEOS spatial relationship tests available
 *
 * GEOS tests not available are:
 *   intersects
 *   crosses
 *   disjoint
 */
typedef enum {
	GSR_OVERLAPS = 0,
	GSR_TOUCHES,
	GSR_CONTAINS,
	GSR_CONTAINSPROPERLY,
	GSR_COVERS,
	GSR_COVEREDBY
} rt_geos_spatial_test;

/**
* Global functions for memory/logging handlers.
*/
typedef void* (*rt_allocator)(size_t size);
typedef void* (*rt_reallocator)(void *mem, size_t size);
typedef void  (*rt_deallocator)(void *mem);
typedef void  (*rt_message_handler)(const char* string, va_list ap);

/****************************************************************************
 * Functions that must be implemented for the raster core function's caller
 * (for example: rt_pg functions, test functions, future loader/exporter)
 ****************************************************************************/

/**
 * Supply the memory management and error handling functions you want your
 * application to use
 */
extern void rt_init_allocators(void);

/*********************************************************************/


/*******************************************************************
 * Functions that may be used by the raster core function's caller
 * (for example: rt_pg functions, test functions, future loader/exporter)
 *******************************************************************/
/**
 * Apply the default memory management (malloc() and free()) and error handlers.
 * Called inside rt_init_allocators() generally.
 */
extern void rt_install_default_allocators(void);


/**
 * Wrappers used for managing memory. They simply call the functions defined by
 * the caller
 **/
extern void* rtalloc(size_t size);
extern void* rtrealloc(void* mem, size_t size);
extern void rtdealloc(void* mem);

/******************************************************************/


/**
 * Wrappers used for reporting errors and info.
 **/
void rterror(const char *fmt, ...);
void rtinfo(const char *fmt, ...);
void rtwarn(const char *fmt, ...);


/**
* The default memory/logging handlers installed by lwgeom_install_default_allocators()
*/
void * default_rt_allocator(size_t size);
void * default_rt_reallocator(void * mem, size_t size);
void default_rt_deallocator(void * mem);
void default_rt_error_handler(const char * fmt, va_list ap);
void default_rt_warning_handler(const char * fmt, va_list ap);
void default_rt_info_handler(const char * fmt, va_list ap);


/* Debugging macros */
#if POSTGIS_DEBUG_LEVEL > 0

/* Display a simple message at NOTICE level */
#define RASTER_DEBUG(level, msg) \
    do { \
        if (POSTGIS_DEBUG_LEVEL >= level) \
            rtinfo("[%s:%s:%d] " msg, __FILE__, __func__, __LINE__); \
    } while (0);

/* Display a formatted message at NOTICE level (like printf, with variadic arguments) */
#define RASTER_DEBUGF(level, msg, ...) \
    do { \
        if (POSTGIS_DEBUG_LEVEL >= level) \
            rtinfo("[%s:%s:%d] " msg, __FILE__, __func__, __LINE__, __VA_ARGS__); \
    } while (0);

#else

/* Empty prototype that can be optimised away by the compiler for non-debug builds */
#define RASTER_DEBUG(level, msg) \
    ((void) 0)

/* Empty prototype that can be optimised away by the compiler for non-debug builds */
#define RASTER_DEBUGF(level, msg, ...) \
    ((void) 0)

#endif

/*- memory context -------------------------------------------------------*/

void rt_set_handlers(rt_allocator allocator, rt_reallocator reallocator,
        rt_deallocator deallocator, rt_message_handler error_handler,
        rt_message_handler info_handler, rt_message_handler warning_handler);



/*- rt_pixtype --------------------------------------------------------*/

/**
 * Return size in bytes of a value in the given pixtype
 *
 * @param pixtype : the pixel type to get byte size for
 *
 * @return the pixel type's byte size
 */
int rt_pixtype_size(rt_pixtype pixtype);

/**
 * Return alignment requirements for data in the given pixel type.
 * Fast access to pixel values of this type must be aligned to as
 * many bytes as returned by this function.
 *
 * @param pixtype : the pixel type to get alignment requirements for
 *
 * @return the alignment requirements
 */
int rt_pixtype_alignment(rt_pixtype pixtype);

/* Return human-readable name of pixel type */
const char* rt_pixtype_name(rt_pixtype pixtype);

/* Return pixel type index from human-readable name */
rt_pixtype rt_pixtype_index_from_name(const char* pixname);

/**
 * Return minimum value possible for pixel type
 *
 * @param pixtype : the pixel type to get minimum possible value for
 *
 * @return the minimum possible value for the pixel type.
 */
double rt_pixtype_get_min_value(rt_pixtype pixtype);

/**
 * Test to see if two values are equal when clamped
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
);

/*- rt_pixel ----------------------------------------------------------*/

/*
 * Convert an array of rt_pixel objects to two 2D arrays of value and NODATA.
 * The dimensions of the returned 2D array are [Y][X], going by row Y and
 * then column X.
 *
 * @param npixel : array of rt_pixel objects
 * @param count : number of elements in npixel
 * @param mask : mask to be respected when returning array
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
	rt_pixel npixel,int count,
	rt_mask mask,
	int x, int y,
	uint16_t distancex, uint16_t distancey,
	double ***value,
	int ***nodata,
	int *dimx, int *dimy
);

/*- rt_band ----------------------------------------------------------*/

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
 * @return an rt_band or NULL on failure
 */
rt_band rt_band_new_inline(
	uint16_t width, uint16_t height,
	rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	uint8_t* data
);

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
 * @return an rt_band or NULL on failure
 */
rt_band rt_band_new_offline(
	uint16_t width, uint16_t height,
	rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	uint8_t bandNum, const char* path
);

/**
 * Create a new band duplicated from source band.  Memory is allocated
 * for band path (if band is offline) or band data (if band is online).
 * The caller is responsible for freeing the memory when the returned
 * rt_band is destroyed.
 *
 * @param : the band to duplicate
 *
 * @return an rt_band or NULL on failure
 */
rt_band rt_band_duplicate(rt_band band);

/**
 * Return non-zero if the given band data is on
 * the filesystem.
 *
 * @param band : the band
 *
 * @return non-zero if the given band data is on
 *         the filesystem.
 */
int rt_band_is_offline(rt_band band);

/**
 * Return band's external path (only valid when rt_band_is_offline
 * returns non-zero).
 *
 * @param band : the band
 *
 * @return string or NULL if band is not offline
 */
const char* rt_band_get_ext_path(rt_band band);

/**
 * Return bands' external band number (only valid when
 * rt_band_is_offline returns non-zero).
 *
 * @param band : the band
 * @param bandnum : external band number (0-based)
 *
 * @return ES_NONE or ES_ERROR if band is NOT out-db
 */
rt_errorstate rt_band_get_ext_band_num(rt_band band, uint8_t *bandnum);

/**
 * Return pixeltype of this band
 *
 * @param band : the band
 *
 * @return band's pixeltype
 */
rt_pixtype rt_band_get_pixtype(rt_band band);

/**
 * Return width of this band
 *
 * @param band : the band
 *
 * @return band's width
 */
uint16_t rt_band_get_width(rt_band band);

/**
 * Return height of this band
 *
 * @param band : the band
 *
 * @return band's height
 */
uint16_t rt_band_get_height(rt_band band);

/**
 * Return 0 (FALSE) or non-zero (TRUE) indicating if rt_band is responsible
 * for managing the memory for band data
 *
 * @param band : the band
 *
 * @return non-zero indicates that rt_band manages the memory
 * allocated for band data
 */
int rt_band_get_ownsdata_flag(rt_band band);

/* set ownsdata flag */
void rt_band_set_ownsdata_flag(rt_band band, int flag);

/**
	* Get pointer to raster band data
	*
	* @param band : the band who's data to get
	*
	* @return pointer to band data or NULL if error
	*/
void* rt_band_get_data(rt_band band);

/**
	* Load offline band's data.  Loaded data is internally owned
	* and should not be released by the caller.  Data will be
	* released when band is destroyed with rt_band_destroy().
	*
	* @param band : the band who's data to get
	*
	* @return ES_NONE if success, ES_ERROR if failure
	*/
rt_errorstate rt_band_load_offline_data(rt_band band);

/**
 * Destroy a raster band
 *
 * @param band : the band to destroy
 */
void rt_band_destroy(rt_band band);

/**
 * Get hasnodata flag value
 *
 * @param band : the band on which to check the hasnodata flag
 *
 * @return the hasnodata flag.
 */
int rt_band_get_hasnodata_flag(rt_band band);

/**
 * Set hasnodata flag value
 * @param band : the band on which to set the hasnodata flag
 * @param flag : the new hasnodata flag value. Must be 1 or 0.
 */
void rt_band_set_hasnodata_flag(rt_band band, int flag);

/**
 * Set isnodata flag value
 *
 * @param band : the band on which to set the isnodata flag
 * @param flag : the new isnodata flag value. Must be 1 or 0
 *
 * @return ES_NONE or ES_ERROR
 */
rt_errorstate rt_band_set_isnodata_flag(rt_band band, int flag);

/**
 * Get isnodata flag value
 *
 * @param band : the band on which to check the isnodata flag
 *
 * @return the hasnodata flag.
 */
int rt_band_get_isnodata_flag(rt_band band);

/**
 * Set nodata value
 *
 * @param band : the band to set nodata value to
 * @param val : the nodata value
 * @param converted : (optional) if non-zero, value was
 * truncated/clamped/converted
 *
 * @return ES_NONE or ES_ERROR
 */
rt_errorstate rt_band_set_nodata(rt_band band, double val, int *converted);

/**
 * Get NODATA value
 *
 * @param band: the band whose NODATA value will be returned
 * @param nodata: the band's NODATA value
 *
 * @return ES_NONE or ES_ERROR
 */
rt_errorstate rt_band_get_nodata(rt_band band, double *nodata);

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
 * @param x : pixel column (0-based)
 * @param y : pixel row (0-based)
 * @param vals : the pixel values to apply
 * @param len : # of elements in vals
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_band_set_pixel_line(
	rt_band band,
	int x, int y,
	void *vals, uint32_t len
);

/**
 * Set single pixel's value
 *
 * @param band : the band to set value to
 * @param x : pixel column (0-based)
 * @param y : pixel row (0-based)
 * @param val : the pixel value
 * @param converted : (optional) non-zero if value truncated/clamped/converted
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_band_set_pixel(
	rt_band band,
	int x, int y,
	double val,
	int *converted
);

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
);

/**
 * Get pixel value. If band's isnodata flag is TRUE, value returned 
 * will be the band's NODATA value
 *
 * @param band : the band to get pixel value from
 * @param x : pixel column (0-based)
 * @param y : pixel row (0-based)
 * @param *value : pixel value
 * @param *nodata : 0 if pixel is not NODATA
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_band_get_pixel(
	rt_band band,
	int x, int y,
	double *value,
	int *nodata
);

/**
 * Get nearest pixel(s) with value (not NODATA) to specified pixel
 *
 * @param band : the band to get nearest pixel(s) from
 * @param x : pixel column (0-based)
 * @param y : pixel row (0-based)
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
);

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
int rt_band_get_pixel_of_value(
	rt_band band, int exclude_nodata_value,
	double *searchset, int searchcount,
	rt_pixel *pixels
);

/**
 * Returns the minimal possible value for the band according to the pixel type.
 *
 * @param band : the band to get info from
 *
 * @return the minimal possible value for the band.
 */
double rt_band_get_min_value(rt_band band);

/**
 * Returns TRUE if the band is only nodata values
 *
 * @param band : the band to get info from
 *
 * @return TRUE if the band is only nodata values, FALSE otherwise
 */
int rt_band_check_is_nodata(rt_band band);

/**
 * Compare clamped value to band's clamped NODATA value
 *
 * @param band : the band whose NODATA value will be used for comparison
 * @param val : the value to compare to the NODATA value
 *
 * @return 2 if unclamped value is unclamped NODATA
 *         1 if clamped value is clamped NODATA
 *         0 if clamped values is NOT clamped NODATA
 */
int rt_band_clamped_value_is_nodata(rt_band band, double val);

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
);

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
rt_bandstats rt_band_get_summary_stats(
	rt_band band,
	int exclude_nodata_value, double sample, int inc_vals,
	uint64_t *cK, double *cM, double *cQ, double *sum2
);
	
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
rt_histogram rt_band_get_histogram(
	rt_bandstats stats,
	int bin_count, double *bin_widths, int bin_widths_count,
	int right, double min, double max,
	uint32_t *rtn_count
);

/**
 * Compute the default set of or requested quantiles for a set of data
 * the quantile formula used is same as Excel and R default method
 *
 * @param stats : a populated stats struct for processing
 * @param quantiles : the quantiles to be computed
 * @param quantiles_count : the number of quantiles to be computed
 * @param rtn_count : the number of quantiles being returned
 *
 * @return the default set of or requested quantiles for a band or NULL
 */
rt_quantile rt_band_get_quantiles(rt_bandstats stats,
	double *quantiles, int quantiles_count, uint32_t *rtn_count);

struct quantile_llist;
int quantile_llist_destroy(
	struct quantile_llist **list,
	uint32_t list_count
);

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
rt_quantile rt_band_get_quantiles_stream(
	rt_band band,
	int exclude_nodata_value, double sample,
	uint64_t cov_count,
	struct quantile_llist **qlls, uint32_t *qlls_count,
	double *quantiles, int quantiles_count,
	uint32_t *rtn_count
);

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
rt_valuecount rt_band_get_value_count(
	rt_band band, int exclude_nodata_value,
	double *search_values, uint32_t search_values_count, double roundto,
	uint32_t *rtn_total, uint32_t *rtn_count
);

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
rt_band rt_band_reclass(
	rt_band srcband, rt_pixtype pixtype,
	uint32_t hasnodata, double nodataval,
	rt_reclassexpr *exprset, int exprcount
);

/*- rt_raster --------------------------------------------------------*/

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
rt_raster rt_raster_new(uint32_t width, uint32_t height);

/**
 * Construct an rt_raster from a binary WKB representation
 *
 * @param wkb : an octet stream
 * @param wkbsize : size (in bytes) of the wkb octet stream
 *
 * @return an rt_raster or NULL on error (out of memory or
 *         malformed WKB).
 *
 */
rt_raster rt_raster_from_wkb(const uint8_t* wkb, uint32_t wkbsize);

/**
 * Construct an rt_raster from a text HEXWKB representation
 *
 * @param hexwkb : an hex-encoded stream
 * @param hexwkbsize : size (in bytes) of the hexwkb stream
 *
 * @return an rt_raster or NULL on error (out of memory or
 *         malformed WKB).
 *
 */
rt_raster rt_raster_from_hexwkb(const char* hexwkb, uint32_t hexwkbsize);

/**
 * Return this raster in WKB form
 *
 * @param raster : the raster
 * @param outasin : if TRUE, out-db bands are treated as in-db
 * @param wkbsize : will be set to the size of returned wkb form
 *
 * @return WKB of raster or NULL on error
 */
uint8_t *rt_raster_to_wkb(rt_raster raster, int outasin, uint32_t *wkbsize);

/**
 * Return this raster in HEXWKB form (null-terminated hex)
 *
 * @param raster : the raster
 * @param outasin : if TRUE, out-db bands are treated as in-db
 * @param hexwkbsize : will be set to the size of returned wkb form,
 *                     not including the null termination
 *
 * @return HEXWKB of raster or NULL on error
 */
char *rt_raster_to_hexwkb(rt_raster raster, int outasin, uint32_t *hexwkbsize);

/**
 * Release memory associated to a raster
 *
 * Note that this will not release data
 * associated to the band themselves (but only
 * the one associated with the pointers pointing
 * at them).
 *
 * @param raster : the raster to destroy
 */
void rt_raster_destroy(rt_raster raster);

/* Get number of bands */
int rt_raster_get_num_bands(rt_raster raster);

/**
 * Return Nth band, or NULL if unavailable
 *
 * @param raster : the raster
 * @param bandNum : 0-based index of the band to return
 *
 * Return band at specified index or NULL if error
 */
rt_band rt_raster_get_band(rt_raster raster, int bandNum);

/* Get number of rows */
uint16_t rt_raster_get_width(rt_raster raster);

/* Get number of columns */
uint16_t rt_raster_get_height(rt_raster raster);

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
int rt_raster_add_band(rt_raster raster, rt_band band, int index);

/**
 * Generate a new inline band and add it to a raster.
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
int rt_raster_generate_new_band(
	rt_raster raster,
	rt_pixtype pixtype,
	double initialvalue,
	uint32_t hasnodata, double nodatavalue,
	int index
);

/**
 * Set scale in projection units
 *
 * @param raster : the raster to set georeference of
 * @param scaleX : scale X in projection units
 * @param scaleY : scale Y height in projection units
 *
 * NOTE: doesn't recompute offsets
 */
void rt_raster_set_scale(rt_raster raster,
                               double scaleX, double scaleY);

/**
 * Get scale X in projection units
 *
 * @param raster : the raster to get georeference of
 *
 * @return scale X in projection units
 */
double rt_raster_get_x_scale(rt_raster raster);

/**
 * Get scale Y in projection units
 *
 * @param raster : the raster to get georeference of
 *
 * @return scale Y in projection units
 */
double rt_raster_get_y_scale(rt_raster raster);

/**
 * Set insertion points in projection units
 *
 * @param raster : the raster to set georeference of
 * @param x : x ordinate of the upper-left corner of upper-left pixel,
 *            in projection units
 * @param y : y ordinate of the upper-left corner of upper-left pixel,
 *            in projection units
 */
void rt_raster_set_offsets(rt_raster raster,
                           double x, double y);

/**
 * Get raster x offset, in projection units
 *
 * @param raster : the raster to get georeference of
 *
 * @return  x ordinate of the upper-left corner of upper-left pixel,
 *          in projection units
 */
double rt_raster_get_x_offset(rt_raster raster);

/**
 * Get raster y offset, in projection units
 *
 * @param raster : the raster to get georeference of
 *
 * @return  y ordinate of the upper-left corner of upper-left pixel,
 *          in projection units
 */
double rt_raster_get_y_offset(rt_raster raster);

/**
 * Set skews about the X and Y axis
 *
 * @param raster : the raster to set georeference of
 * @param skewX : skew about the x axis
 * @param skewY : skew about the y axis
 */
void rt_raster_set_skews(rt_raster raster,
                             double skewX, double skewY);

/**
 * Get skew about the X axis
 *
 * @param raster : the raster to set georeference of
 * @return skew about the Y axis
 */
double rt_raster_get_x_skew(rt_raster raster);

/**
 * Get skew about the Y axis
 *
 * @param raster : the raster to set georeference of
 * @return skew about the Y axis
 */
double rt_raster_get_y_skew(rt_raster raster);

/**
* Calculates and returns the physically significant descriptors embodied
* in the geotransform attached to the provided raster.
*
* @param rast the raster containing the geotransform of interest
* @param i_mag size of a pixel along the transformed i axis
* @param j_mag size of a pixel along the transformed j axis
* @param theta_i angle by which the raster is rotated (radians positive clockwise)
* @param theta_ij angle from transformed i axis to transformed j axis
* (radians positive counterclockwise)
*
*/
void rt_raster_get_phys_params(rt_raster rast,
        double *i_mag, double *j_mag, double *theta_i, double *theta_ij) ;

/**
* Calculates the geotransform coefficients and applies them to the
* supplied raster. The coefficients will not be applied if there was an
* error during the calculation.
*
* This method affects only the scale and skew coefficients. The offset
* parameters are not changed.
*
* @param rast the raster in which the geotransform will be stored.
* @param i_mag size of a pixel along the transformed i axis
* @param j_mag size of a pixel along the transformed j axis
* @param theta_i angle by which the raster is rotated (radians positive clockwise)
* @param theta_ij angle from transformed i axis to transformed j axis
* (radians positive counterclockwise)
*/
void rt_raster_set_phys_params(rt_raster rast,
        double i_mag, double j_mag, double theta_i, double theta_ij) ;


/**
* Calculates the physically significant descriptors embodied in an
* arbitrary geotransform. Always succeeds unless one or more of the
* output pointers is set to NULL.
*
* @param xscale geotransform coefficient o_11
* @param xskew geotransform coefficient o_12
* @param yskew geotransform coefficient o_21
* @param yscale geotransform coefficient o_22
* @param i_mag size of a pixel along the transformed i axis
* @param j_mag size of a pixel along the transformed j axis
* @param theta_i angle by which the raster is rotated (radians positive clockwise)
* @param theta_ij angle from transformed i axis to transformed j axis
* (radians positive counterclockwise)
*/
void rt_raster_calc_phys_params(double xscale,
        double xskew, double yskew, double yscale,
        double *i_mag, double *j_mag, double *theta_i, double *theta_ij) ;


/**
* Calculates the coefficients of a geotransform given the physically
* significant parameters describing the transform. Will fail if any of the
* result pointers is NULL, or if theta_ij has an illegal value (0 or PI).
*
* @param i_mag size of a pixel along the transformed i axis
* @param j_mag size of a pixel along the transformed j axis
* @param theta_i angle by which the raster is rotated (radians positive clockwise)
* @param theta_ij angle from transformed i axis to transformed j axis
* (radians positive counterclockwise)
* @param xscale geotransform coefficient o_11
* @param xskew geotransform coefficient o_12
* @param yskew geotransform coefficient o_21
* @param yscale geotransform coefficient o_22
* @return 1 if the calculation succeeded, 0 if error.
*/
int rt_raster_calc_gt_coeff(double i_mag,
        double j_mag, double theta_i, double theta_ij,
        double *xscale, double *xskew, double *yskew, double *yscale) ;

/**
 * Set raster's SRID
 *
 * @param raster : the raster to set SRID of
 * @param srid : the SRID to set for the raster
 */
void rt_raster_set_srid(rt_raster raster, int32_t srid);

/**
 * Get raster's SRID
 * @param raster : the raster to set SRID of
 *
 * @return the raster's SRID
 */
int32_t rt_raster_get_srid(rt_raster raster);

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
);

/**
 * Get 6-element array of raster geotransform matrix
 *
 * @param raster : the raster to get matrix of
 * @param gt : output parameter, 6-element geotransform matrix
 */
void rt_raster_get_geotransform_matrix(rt_raster raster,
	double *gt);

/**
 * Set raster's geotransform using 6-element array
 *
 * @param raster : the raster to set matrix of
 * @param gt : intput parameter, 6-element geotransform matrix
 *
 */
void rt_raster_set_geotransform_matrix(rt_raster raster,
	double *gt);

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
rt_errorstate rt_raster_cell_to_geopoint(
	rt_raster raster,
	double xr, double yr,
	double* xw, double* yw,
	double *gt
);

/**
 * Convert an xw, yw map point to a xr, yr raster point
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
rt_errorstate rt_raster_geopoint_to_cell(
	rt_raster raster,
	double xw, double yw,
	double *xr, double *yr,
	double *igt
);

/**
 * Get raster's convex hull.
 *
 * The convex hull is a 4 vertices (5 to be closed)
 * single ring polygon bearing the raster's rotation and using
 * projection coordinates.
 *
 * @param raster : the raster to get info from
 * @param **hull : pointer to convex hull
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_get_convex_hull(rt_raster raster, LWGEOM **hull);

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
rt_errorstate rt_raster_get_envelope(rt_raster raster, rt_envelope *env);

/**
 * Get raster's envelope as a geometry
 *
 * @param raster : the raster to get info from
 * @param **env : pointer to envelope geom
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_get_envelope_geom(rt_raster raster, LWGEOM **env);

/**
 * Get raster perimeter
 *
 * The perimeter is a 4 vertices (5 to be closed)
 * single ring polygon bearing the raster's rotation and using
 * projection coordinates.
 *
 * @param raster : the raster to get info from
 * @param nband : the band for the perimeter. 0-based
 * @param **perimeter : pointer to perimeter
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_get_perimeter(
	rt_raster raster, int nband,
	LWGEOM **perimeter
);

/*
 * Compute skewed extent that covers unskewed extent.
 *
 * @param envelope : unskewed extent of type rt_envelope
 * @param skew : pointer to 2-element array (x, y) of skew
 * @param scale : pointer to 2-element array (x, y) of scale
 * @param tolerance : value between 0 and 1 where the smaller the tolerance
 * results in an extent approaching the "minimum" skewed extent.
 * If value <= 0, tolerance = 0.1.  If value > 1, tolerance = 1.
 *
 * @return skewed raster who's extent covers unskewed extent, NULL on error
 */
rt_raster
rt_raster_compute_skewed_raster(
	rt_envelope extent,
	double *skew,
	double *scale,
	double tolerance
);

/**
 * Get a raster pixel as a polygon.
 *
 * The pixel shape is a 4 vertices (5 to be closed) single
 * ring polygon bearing the raster's rotation
 * and using projection coordinates
 *
 * @param raster : the raster to get pixel from
 * @param x : the column number
 * @param y : the row number
 *
 * @return the pixel polygon, or NULL on error.
 *
 */
LWPOLY* rt_raster_pixel_as_polygon(rt_raster raster, int x, int y);

/**
 * Get a raster as a surface (multipolygon).  If a band is specified,
 * those pixels with value (not NODATA) contribute to the area
 * of the output multipolygon.
 *
 * @param raster : the raster to convert to a multipolygon
 * @param nband : the 0-based band of raster rast to use
 *   if value is less than zero, bands are ignored.
 * @param **surface : raster as a surface (multipolygon).
 *   if all pixels are NODATA, NULL is set
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_raster_surface(rt_raster raster, int nband, LWMPOLY **surface);

/**
 * Returns a set of "geomval" value, one for each group of pixel
 * sharing the same value for the provided band.
 *
 * A "geomval" value is a complex type composed of a geometry 
 * in LWPOLY representation (one for each group of pixel sharing
 * the same value) and the value associated with this geometry.
 *
 * @param raster : the raster to get info from.
 * @param nband : the band to polygonize. 0-based
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * to check for pixels with value
 *
 * @return A set of "geomval" values, one for each group of pixels
 * sharing the same value for the provided band. The returned values are
 * LWPOLY geometries.
 */
rt_geomval
rt_raster_gdal_polygonize(
	rt_raster raster, int nband,
	int exclude_nodata_value,
	int * pnElements
);

/**
 * Return this raster in serialized form.
 * Memory (band data included) is copied from rt_raster.
 *
 * Serialized form is documented in doc/RFC1-SerializedFormat.
 */
void* rt_raster_serialize(rt_raster raster);

/**
 * Return a raster from a serialized form.
 *
 * Serialized form is documented in doc/RFC1-SerializedFormat.
 *
 * NOTE: the raster will contain pointer to the serialized
 * form (including band data), which must be kept alive.
 */
rt_raster rt_raster_deserialize(void* serialized, int header_only);

/**
 * Return TRUE if the raster is empty. i.e. is NULL, width = 0 or height = 0
 *
 * @param raster : the raster to get info from
 *
 * @return TRUE if the raster is empty, FALSE otherwise
 */
int rt_raster_is_empty(rt_raster raster);

/**
 * Return TRUE if the raster has a band of this number.
 *
 * @param raster : the raster to get info from
 * @param nband : the band number. 0-based
 *
 * @return TRUE if the raster has a band of this number, FALSE otherwise
 */
int rt_raster_has_band(rt_raster raster, int nband);

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
int rt_raster_copy_band(
	rt_raster torast, rt_raster fromrast,
	int fromindex, int toindex
);

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
rt_raster rt_raster_from_band(rt_raster raster, uint32_t *bandNums,
	int count);

/**
 * Replace band at provided index with new band
 * 
 * @param raster: raster of band to be replaced
 * @param band : new band to add to raster
 * @param index : index of band to replace (0-based)
 *
 * @return NULL on error or replaced band
 */
rt_band rt_raster_replace_band(rt_raster raster, rt_band band,
	int index);

/**
 * Clone an existing raster
 *
 * @param raster : raster to clone
 * @param deep : flag indicating if bands should be cloned
 *
 * @return a new rt_raster or NULL on error
 */
rt_raster rt_raster_clone(rt_raster raster, uint8_t deep);

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
uint8_t *rt_raster_to_gdal(rt_raster raster, const char *srs,
	char *format, char **options, uint64_t *gdalsize);

/**
 * Returns a set of available GDAL drivers
 *
 * @param drv_count : number of GDAL drivers available
 * @param cancc : if non-zero, filter drivers to only those
 *   with support for CreateCopy and VirtualIO
 *
 * @return set of "gdaldriver" values of available GDAL drivers
 */
rt_gdaldriver rt_raster_gdal_drivers(uint32_t *drv_count, uint8_t cancc);

/**
 * Return GDAL dataset using GDAL MEM driver from raster
 *
 * @param raster : raster to convert to GDAL MEM
 * @param srs : the raster's coordinate system in OGC WKT
 * @param bandNums : array of band numbers to extract from raster
 *   and include in the GDAL dataset (0 based)
 * @param excludeNodataValues : array of zero, nonzero where if non-zero,
 *   ignore nodata values for the band
 * to check for pixels with value
 * @param count : number of elements in bandNums and exclude_nodata_values
 * @param rtn_drv : is set to the GDAL driver object
 * @param destroy_rtn_drv : if non-zero, caller must destroy the MEM driver
 *
 * @return GDAL dataset using GDAL MEM driver
 */
GDALDatasetH rt_raster_to_gdal_mem(
	rt_raster raster,
	const char *srs,
	uint32_t *bandNums,
	int *excludeNodataValues,
	int count,
	GDALDriverH *rtn_drv, int *destroy_rtn_drv
);

/**
 * Return a raster from a GDAL dataset
 *
 * @param ds : the GDAL dataset to convert to a raster
 *
 * @return raster or NULL
 */
rt_raster rt_raster_from_gdal_dataset(GDALDatasetH ds);

/**
 * Return a warped raster using GDAL Warp API
 *
 * @param raster : raster to transform
 * @param src_srs : the raster's coordinate system in OGC WKT
 * @param dst_srs : the warped raster's coordinate system in OGC WKT
 * @param scale_x : the x size of pixels of the warped raster's pixels in
 *   units of dst_srs
 * @param scale_y : the y size of pixels of the warped raster's pixels in
 *   units of dst_srs
 * @param width : the number of columns of the warped raster.  note that
 *   width/height CANNOT be used with scale_x/scale_y
 * @param height : the number of rows of the warped raster.  note that
 *   width/height CANNOT be used with scale_x/scale_y
 * @param ul_xw : the X value of upper-left corner of the warped raster in
 *   units of dst_srs
 * @param ul_yw : the Y value of upper-left corner of the warped raster in
 *   units of dst_srs
 * @param grid_xw : the X value of point on a grid to align warped raster
 *   to in units of dst_srs
 * @param grid_yw : the Y value of point on a grid to align warped raster
 *   to in units of dst_srs
 * @param skew_x : the X skew of the warped raster in units of dst_srs
 * @param skew_y : the Y skew of the warped raster in units of dst_srs
 * @param resample_alg : the resampling algorithm
 * @param max_err : maximum error measured in input pixels permitted
 *   (0.0 for exact calculations)
 *
 * @return the warped raster or NULL
 */
rt_raster rt_raster_gdal_warp(
	rt_raster raster,
	const char *src_srs, const char *dst_srs,
	double *scale_x, double *scale_y,
	int *width, int *height,
	double *ul_xw, double *ul_yw,
	double *grid_xw, double *grid_yw,
	double *skew_x, double *skew_y,
	GDALResampleAlg resample_alg, double max_err);

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
rt_raster rt_raster_gdal_rasterize(const unsigned char *wkb,
	uint32_t wkb_len, const char *srs,
	uint32_t num_bands, rt_pixtype *pixtype,
	double *init, double *value,
	double *nodata, uint8_t *hasnodata,
	int *width, int *height,
	double *scale_x, double *scale_y,
	double *ul_xw, double *ul_yw,
	double *grid_xw, double *grid_yw,
	double *skew_x, double *skew_y,
	char **options
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter intersects returns non-zero if two rasters intersect
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param intersects : non-zero value if the two rasters' bands intersects
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_intersects(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *intersects
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter overlaps returns non-zero if two rasters overlap
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param overlaps : non-zero value if the two rasters' bands overlaps
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_overlaps(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *overlaps
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter contains returns non-zero if rast1 contains rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param contains : non-zero value if rast1 contains rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_contains(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *contains
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter contains returns non-zero if rast1 contains properly rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param contains : non-zero value if rast1 contains properly rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_contains_properly(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *contains
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter touches returns non-zero if two rasters touch
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param touches : non-zero value if the two rasters' bands touch
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_touches(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *touches
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter covers returns non-zero if rast1 covers rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param covers : non-zero value if rast1 covers rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_covers(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *covers
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter coveredby returns non-zero if rast1 is covered by rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param coveredby : non-zero value if rast1 is covered by rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_coveredby(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	int *coveredby
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter dwithin returns non-zero if rast1 is within the specified
 *   distance of rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param dwithin : non-zero value if rast1 is within the specified distance
 *   of rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_within_distance(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	double distance,
	int *dwithin
);

/**
 * Return ES_ERROR if error occurred in function.
 * Parameter dfwithin returns non-zero if rast1 is fully within the specified
 *   distance of rast2
 *
 * @param rast1 : the first raster whose band will be tested
 * @param nband1 : the 0-based band of raster rast1 to use
 *   if value is less than zero, bands are ignored.
 *   if nband1 gte zero, nband2 must be gte zero
 * @param rast2 : the second raster whose band will be tested
 * @param nband2 : the 0-based band of raster rast2 to use
 *   if value is less than zero, bands are ignored
 *   if nband2 gte zero, nband1 must be gte zero
 * @param dfwithin : non-zero value if rast1 is fully within the specified
 *   distance of rast2
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_fully_within_distance(
	rt_raster rast1, int nband1,
	rt_raster rast2, int nband2,
	double distance,
	int *dfwithin
);

/*
 * Return ES_ERROR if error occurred in function.
 * Paramter aligned returns non-zero if two rasters are aligned
 *
 * @param rast1 : the first raster for alignment test
 * @param rast2 : the second raster for alignment test
 * @param *aligned : non-zero value if the two rasters are aligned
 * @param *reason : reason why rasters are not aligned
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_same_alignment(
	rt_raster rast1,
	rt_raster rast2,
	int *aligned, char **reason
);

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
);

/**
 * n-raster iterator.  Returns a raster with one band.
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
 * @param *userarg : pointer to any argument that is passed as-is to callback.
 * @param *callback : callback function for actual processing of pixel values.
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
 * - rt_iterator_arg arg : struct containing pixel values, NODATA flags and metadata
 * - void *userarg : NULL or calling function provides to rt_raster_iterator() for use by callback function
 * - double *value : value of pixel to be burned by rt_raster_iterator()
 * - int *nodata : flag (0 or 1) indicating that pixel to be burned is NODATA
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
);

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
);

/*- utilities -------------------------------------------------------*/

/*
 * rt_core memory functions
 */
extern void *rtalloc(size_t size);
extern void *rtrealloc(void *mem, size_t size);
extern void rtdealloc(void *mem);

/*
 * GDAL driver flags
 */

#define GDAL_ENABLE_ALL "ENABLE_ALL"
#define GDAL_DISABLE_ALL "DISABLE_ALL"
#define GDAL_VSICURL "VSICURL"

/*
 * Set of functions to clamp double to int of different size
 */

#if !defined(POSTGIS_RASTER_WARN_ON_TRUNCATION)
#define POSTGIS_RASTER_WARN_ON_TRUNCATION 0
#endif

#define POSTGIS_RT_1BBMAX 1
#define POSTGIS_RT_2BUIMAX 3
#define POSTGIS_RT_4BUIMAX 15

uint8_t
rt_util_clamp_to_1BB(double value);

uint8_t
rt_util_clamp_to_2BUI(double value);

uint8_t
rt_util_clamp_to_4BUI(double value);

int8_t
rt_util_clamp_to_8BSI(double value);

uint8_t
rt_util_clamp_to_8BUI(double value);

int16_t
rt_util_clamp_to_16BSI(double value);

uint16_t
rt_util_clamp_to_16BUI(double value);

int32_t
rt_util_clamp_to_32BSI(double value);

uint32_t
rt_util_clamp_to_32BUI(double value);

float
rt_util_clamp_to_32F(double value);

int
rt_util_dbl_trunc_warning(
	double initialvalue,
	int32_t checkvalint, uint32_t checkvaluint,
	float checkvalfloat, double checkvaldouble,
	rt_pixtype pixtype
);

/**
 * Convert cstring name to GDAL Resample Algorithm
 *
 * @param algname : cstring name to convert
 *
 * @return valid GDAL resampling algorithm
 */
GDALResampleAlg
rt_util_gdal_resample_alg(const char *algname);

/**
 * Convert rt_pixtype to GDALDataType
 *
 * @param pt : pixeltype to convert
 *
 * @return valid GDALDataType
 */
GDALDataType
rt_util_pixtype_to_gdal_datatype(rt_pixtype pt);

/**
 * Convert GDALDataType to rt_pixtype
 *
 * @param gdt : GDAL datatype to convert
 *
 * @return valid rt_pixtype
 */
rt_pixtype
rt_util_gdal_datatype_to_pixtype(GDALDataType gdt);

/*
	get GDAL runtime version information
*/
const char*
rt_util_gdal_version(const char *request);

/*
	computed extent type from c string
*/
rt_extenttype
rt_util_extent_type(const char *name);

/*
	convert the spatial reference string from a GDAL recognized format to either WKT or Proj4
*/
char*
rt_util_gdal_convert_sr(const char *srs, int proj4);

/*
	is the spatial reference string supported by GDAL
*/
int
rt_util_gdal_supported_sr(const char *srs);

/**
 * Get auth name and code
 *
 * @param authname: authority organization of code. calling function
 * is expected to free the memory allocated for value
 * @param authcode: code assigned by authority organization. calling function
 * is expected to free the memory allocated for value
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate
rt_util_gdal_sr_auth_info(GDALDatasetH hds, char **authname, char **authcode);

/*
	is GDAL configured correctly?
*/
int
rt_util_gdal_configured(void);

/*
	register all GDAL drivers
*/
int
rt_util_gdal_register_all(int force_register_all);

/*
	is the driver registered?
*/
int
rt_util_gdal_driver_registered(const char *drv);

/*
	wrapper for GDALOpen and GDALOpenShared
*/
GDALDatasetH
rt_util_gdal_open(const char *fn, GDALAccess fn_access, int shared);

void
rt_util_from_ogr_envelope(
	OGREnvelope	env,
	rt_envelope *ext
);

void
rt_util_to_ogr_envelope(
	rt_envelope ext,
	OGREnvelope	*env
);

LWPOLY *
rt_util_envelope_to_lwpoly(
	rt_envelope ext
);

int
rt_util_same_geotransform_matrix(
	double *gt1,
	double *gt2
);

/* coordinates in RGB and HSV are floating point values between 0 and 1 */
rt_errorstate
rt_util_rgb_to_hsv(
	double rgb[3],
	double hsv[3]
);

/* coordinates in RGB and HSV are floating point values between 0 and 1 */
rt_errorstate
rt_util_hsv_to_rgb(
	double hsv[3],
	double rgb[3]
);

/*
	helper macros for consistent floating point equality checks
*/
#define FLT_NEQ(x, y) (fabs(x - y) > FLT_EPSILON)
#define FLT_EQ(x, y) (!FLT_NEQ(x, y))
#define DBL_NEQ(x, y) (fabs(x - y) > DBL_EPSILON)
#define DBL_EQ(x, y) (!DBL_NEQ(x, y))

/*
	helper macro for symmetrical rounding
*/
#define ROUND(x, y) (((x > 0.0) ? floor((x * pow(10, y) + 0.5)) : ceil((x * pow(10, y) - 0.5))) / pow(10, y))

/**
 * Struct definitions
 *
 * These structs are defined here as they are needed elsewhere
 * including rt_pg/rt_pg.c and reduce duplicative declarations
 *
 */
struct rt_raster_serialized_t {
    /*---[ 8 byte boundary ]---{ */
    uint32_t size; /* required by postgresql: 4 bytes */
    uint16_t version; /* format version (this is version 0): 2 bytes */
    uint16_t numBands; /* Number of bands: 2 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double scaleX; /* pixel width: 8 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double scaleY; /* pixel height: 8 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double ipX; /* insertion point X: 8 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double ipY; /* insertion point Y: 8 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double skewX; /* skew about the X axis: 8 bytes */

    /* }---[ 8 byte boundary ]---{ */
    double skewY; /* skew about the Y axis: 8 bytes */

    /* }---[ 8 byte boundary ]--- */
    int32_t srid; /* Spatial reference id: 4 bytes */
    uint16_t width; /* pixel columns: 2 bytes */
    uint16_t height; /* pixel rows: 2 bytes */
};

/* NOTE: the initial part of this structure matches the layout
 *       of data in the serialized form version 0, starting
 *       from the numBands element
 */
struct rt_raster_t {
    uint32_t size;
    uint16_t version;

    /* Number of bands, all share the same dimension
     * and georeference */
    uint16_t numBands;

    /* Georeference (in projection units) */
    double scaleX; /* pixel width */
    double scaleY; /* pixel height */
    double ipX; /* geo x ordinate of the corner of upper-left pixel */
    double ipY; /* geo y ordinate of the corner of bottom-right pixel */
    double skewX; /* skew about the X axis*/
    double skewY; /* skew about the Y axis */

    int32_t srid; /* spatial reference id */
    uint16_t width; /* pixel columns - max 65535 */
    uint16_t height; /* pixel rows - max 65535 */
    rt_band *bands; /* actual bands */

};

struct rt_extband_t {
    uint8_t bandNum; /* 0-based */
    char* path; /* internally owned */
		void *mem; /* loaded external band data, internally owned */
};

struct rt_band_t {
    rt_pixtype pixtype;
    int32_t offline;
    uint16_t width;
    uint16_t height;
    int32_t hasnodata; /* a flag indicating if this band contains nodata values */
    int32_t isnodata;   /* a flag indicating if this band is filled only with
                           nodata values. flag CANNOT be TRUE if hasnodata is FALSE */
    double nodataval; /* int will be converted ... */
    int8_t ownsdata; /* 0, externally owned. 1, internally owned. only applies to data.mem */

		rt_raster raster; /* reference to parent raster */

    union {
        void* mem; /* actual data, externally owned */
        struct rt_extband_t offline;
    } data;

};

struct rt_pixel_t {
	int x; /* column */
	int y; /* line */

	uint8_t nodata;
	double value;

	LWGEOM *geom;
};

struct rt_mask_t {
  uint16_t dimx;
  uint16_t dimy;
  double **values;
  int **nodata;
  int weighted; /* 0 if not weighted values 1 if weighted values */
};

/* polygon as LWPOLY with associated value */
struct rt_geomval_t {
	LWPOLY *geom;
	double val;
};

/* summary stats of specified band */
struct rt_bandstats_t {
	double sample;
	uint32_t count;

	double min;
	double max;
	double sum;
  double sum2;
	double mean;
	double stddev;

	double *values;
	int sorted; /* flag indicating that values is sorted ascending by value */
};

/* histogram bin(s) of specified band */
struct rt_histogram_t {
	uint32_t count;
	double percent;

	double min;
	double max;

	int inc_min;
	int inc_max;
};

/* quantile(s) of the specified band */
struct rt_quantile_t {
	double quantile;
	double value;
	uint32_t has_value;
};

/* listed-list structures for rt_band_get_quantiles_stream */
struct quantile_llist {
	uint8_t algeq; /* AL-GEQ (1) or AL-GT (0) */
	double quantile;
	uint64_t tau; /* position in sequence */

	struct quantile_llist_element *head; /* H index 0 */
	struct quantile_llist_element *tail; /* H index last */
	uint32_t count; /* # of elements in H */

	/* faster access to elements at specific intervals */
	struct quantile_llist_index *index;
	uint32_t index_max; /* max # of elements in index */

	uint64_t sum1; /* N1H */
	uint64_t sum2; /* N2H */
};

struct quantile_llist_element {
	double value;
	uint32_t count;

	struct quantile_llist_element *prev;
	struct quantile_llist_element *next;
};

struct quantile_llist_index {
	struct quantile_llist_element *element;
	uint32_t index;
};

/* number of times a value occurs */
struct rt_valuecount_t {
	double value;
	uint32_t count;
	double percent;
};

/* reclassification expression */
struct rt_reclassexpr_t {
	struct rt_reclassrange {
		double min;
		double max;
		int inc_min; /* include min */
		int inc_max; /* include max */
		int exc_min; /* exceed min */
		int exc_max; /* exceed max */
	} src, dst;
};

/* raster iterator */
struct rt_iterator_t {
	rt_raster raster;
	uint16_t nband; /* 0-based */
	uint8_t nbnodata; /* no band = treat as NODATA  */
};

/* callback argument from raster iterator */
struct rt_iterator_arg_t {
	/* # of rasters, Z-axis */
	uint16_t rasters;
	/* # of rows, Y-axis */
	uint32_t rows;
	/* # of columns, X-axis */
	uint32_t columns;

	/* axis order: Z,X,Y */
	/* individual pixel values */
	double ***values;
	/* 0,1 value of nodata flag */
	int ***nodata;

	/* X,Y of pixel from each input raster */
	int **src_pixel;

	/* X,Y of pixel from output raster */
	int dst_pixel[2];
};

/* gdal driver information */
struct rt_gdaldriver_t {
	int idx;
	char *short_name;
	char *long_name;
	char *create_options;
};

/* raster colormap entry */
struct rt_colormap_entry_t {
	int isnodata;
	double value;
	uint8_t color[4]; /* RGBA */
};

struct rt_colormap_t {
	enum {
		CM_INTERPOLATE,
		CM_EXACT,
		CM_NEAREST
	} method;

	int ncolor;
	uint16_t nentry;
	rt_colormap_entry entry;
};

#endif /* LIBRTCORE_H_INCLUDED */
