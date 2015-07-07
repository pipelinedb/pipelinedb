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

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>

#include "../../postgis_config.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "rtpostgis.h"

/* Get all the properties of a raster */
Datum RASTER_getSRID(PG_FUNCTION_ARGS);
Datum RASTER_getWidth(PG_FUNCTION_ARGS);
Datum RASTER_getHeight(PG_FUNCTION_ARGS);
Datum RASTER_getNumBands(PG_FUNCTION_ARGS);
Datum RASTER_getXScale(PG_FUNCTION_ARGS);
Datum RASTER_getYScale(PG_FUNCTION_ARGS);
Datum RASTER_getXSkew(PG_FUNCTION_ARGS);
Datum RASTER_getYSkew(PG_FUNCTION_ARGS);
Datum RASTER_getXUpperLeft(PG_FUNCTION_ARGS);
Datum RASTER_getYUpperLeft(PG_FUNCTION_ARGS);
Datum RASTER_getPixelWidth(PG_FUNCTION_ARGS);
Datum RASTER_getPixelHeight(PG_FUNCTION_ARGS);
Datum RASTER_getGeotransform(PG_FUNCTION_ARGS);
Datum RASTER_isEmpty(PG_FUNCTION_ARGS);
Datum RASTER_hasNoBand(PG_FUNCTION_ARGS);

/* get raster's meta data */
Datum RASTER_metadata(PG_FUNCTION_ARGS);

/* convert pixel/line to spatial coordinates */
Datum RASTER_rasterToWorldCoord(PG_FUNCTION_ARGS);

/* convert spatial coordinates to pixel/line*/
Datum RASTER_worldToRasterCoord(PG_FUNCTION_ARGS);

/* Set all the properties of a raster */
Datum RASTER_setSRID(PG_FUNCTION_ARGS);
Datum RASTER_setScale(PG_FUNCTION_ARGS);
Datum RASTER_setScaleXY(PG_FUNCTION_ARGS);
Datum RASTER_setSkew(PG_FUNCTION_ARGS);
Datum RASTER_setSkewXY(PG_FUNCTION_ARGS);
Datum RASTER_setUpperLeftXY(PG_FUNCTION_ARGS);
Datum RASTER_setRotation(PG_FUNCTION_ARGS);
Datum RASTER_setGeotransform(PG_FUNCTION_ARGS);

/**
 * Return the SRID associated with the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getSRID);
Datum RASTER_getSRID(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    int32_t srid;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getSRID: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    srid = rt_raster_get_srid(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_INT32(srid);
}

/**
 * Return the width of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getWidth);
Datum RASTER_getWidth(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    uint16_t width;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getWidth: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    width = rt_raster_get_width(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_INT32(width);
}

/**
 * Return the height of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getHeight);
Datum RASTER_getHeight(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    uint16_t height;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getHeight: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    height = rt_raster_get_height(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_INT32(height);
}

/**
 * Return the number of bands included in the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getNumBands);
Datum RASTER_getNumBands(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    int32_t num_bands;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getNumBands: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    num_bands = rt_raster_get_num_bands(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_INT32(num_bands);
}

/**
 * Return X scale from georeference of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getXScale);
Datum RASTER_getXScale(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double xsize;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getXScale: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    xsize = rt_raster_get_x_scale(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(xsize);
}

/**
 * Return Y scale from georeference of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getYScale);
Datum RASTER_getYScale(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double ysize;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getYScale: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    ysize = rt_raster_get_y_scale(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(ysize);
}

/**
 * Return value of the raster skew about the X axis.
 */
PG_FUNCTION_INFO_V1(RASTER_getXSkew);
Datum RASTER_getXSkew(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double xskew;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getXSkew: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    xskew = rt_raster_get_x_skew(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(xskew);
}

/**
 * Return value of the raster skew about the Y axis.
 */
PG_FUNCTION_INFO_V1(RASTER_getYSkew);
Datum RASTER_getYSkew(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double yskew;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getYSkew: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    yskew = rt_raster_get_y_skew(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(yskew);
}

/**
 * Return value of the raster offset in the X dimension.
 */
PG_FUNCTION_INFO_V1(RASTER_getXUpperLeft);
Datum RASTER_getXUpperLeft(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double xul;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getXUpperLeft: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    xul = rt_raster_get_x_offset(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(xul);
}

/**
 * Return value of the raster offset in the Y dimension.
 */
PG_FUNCTION_INFO_V1(RASTER_getYUpperLeft);
Datum RASTER_getYUpperLeft(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double yul;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getYUpperLeft: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    yul = rt_raster_get_y_offset(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(yul);
}

/**
 * Return the pixel width of the raster. The pixel width is
 * a read-only, dynamically computed value derived from the 
 * X Scale and the Y Skew.
 *
 * Pixel Width = sqrt( X Scale * X Scale + Y Skew * Y Skew )
 */
PG_FUNCTION_INFO_V1(RASTER_getPixelWidth);
Datum RASTER_getPixelWidth(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double xscale;
    double yskew;
    double pwidth;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if (!raster) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getPixelWidth: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    xscale = rt_raster_get_x_scale(raster);
    yskew = rt_raster_get_y_skew(raster);
    pwidth = sqrt(xscale*xscale + yskew*yskew);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(pwidth);
}

/**
 * Return the pixel height of the raster. The pixel height is
 * a read-only, dynamically computed value derived from the 
 * Y Scale and the X Skew.
 *
 * Pixel Height = sqrt( Y Scale * Y Scale + X Skew * X Skew )
 */
PG_FUNCTION_INFO_V1(RASTER_getPixelHeight);
Datum RASTER_getPixelHeight(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster;
    rt_raster raster;
    double yscale;
    double xskew;
    double pheight;

    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if (!raster) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getPixelHeight: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    yscale = rt_raster_get_y_scale(raster);
    xskew = rt_raster_get_x_skew(raster);
    pheight = sqrt(yscale*yscale + xskew*xskew);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(pheight);
}

/**
 * Calculates the physically relevant parameters of the supplied raster's
 * geotransform. Returns them as a set.
 */
PG_FUNCTION_INFO_V1(RASTER_getGeotransform);
Datum RASTER_getGeotransform(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;

    double imag;
    double jmag;
    double theta_i;
    double theta_ij;
		/*
    double xoffset;
    double yoffset;
		*/

    TupleDesc result_tuple; /* for returning a composite */
    int values_length = 6;
    Datum values[values_length];
    bool nulls[values_length];
    HeapTuple heap_tuple ;   /* instance of the tuple to return */
    Datum result;

    POSTGIS_RT_DEBUG(3, "RASTER_getGeotransform: Starting");

    /* get argument */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    /* raster */
    raster = rt_raster_deserialize(pgraster, TRUE);
    if (!raster) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getGeotransform: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* do the calculation */
    rt_raster_calc_phys_params(
            rt_raster_get_x_scale(raster),
            rt_raster_get_x_skew(raster),
            rt_raster_get_y_skew(raster),
            rt_raster_get_y_scale(raster),
            &imag, &jmag, &theta_i, &theta_ij) ;

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    /* setup the return value infrastructure */
    if (get_call_result_type(fcinfo, NULL, &result_tuple) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("RASTER_getGeotransform(): function returning record called in context that cannot accept type record"
            )
        ));
        PG_RETURN_NULL();
    }

    BlessTupleDesc(result_tuple);

    /* get argument */
    /* prep the composite return value */
    /* construct datum array */
    values[0] = Float8GetDatum(imag);
    values[1] = Float8GetDatum(jmag);
    values[2] = Float8GetDatum(theta_i);
    values[3] = Float8GetDatum(theta_ij);
    values[4] = Float8GetDatum(rt_raster_get_x_offset(raster));
    values[5] = Float8GetDatum(rt_raster_get_y_offset(raster));

    memset(nulls, FALSE, sizeof(bool) * values_length);

    /* stick em on the heap */
    heap_tuple = heap_form_tuple(result_tuple, values, nulls);

    /* make the tuple into a datum */
    result = HeapTupleGetDatum(heap_tuple);

    PG_RETURN_DATUM(result);
}

/**
 * Check if raster is empty or not
 */
PG_FUNCTION_INFO_V1(RASTER_isEmpty);
Datum RASTER_isEmpty(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    bool isempty = FALSE;

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster )
    {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("RASTER_isEmpty: Could not deserialize raster")));
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    isempty = rt_raster_is_empty(raster);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_BOOL(isempty);
}

/**
 * Check if the raster has a given band or not
 */
PG_FUNCTION_INFO_V1(RASTER_hasNoBand);
Datum RASTER_hasNoBand(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    int bandindex = 0;
    bool hasnoband = FALSE;

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

    raster = rt_raster_deserialize(pgraster, TRUE);
    if ( ! raster )
    {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("RASTER_hasNoBand: Could not deserialize raster")));
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    /* Get band number */
    bandindex = PG_GETARG_INT32(1);
    hasnoband = !rt_raster_has_band(raster, bandindex - 1);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_BOOL(hasnoband);
}

/**
 * Get raster's meta data
 */
PG_FUNCTION_INFO_V1(RASTER_metadata);
Datum RASTER_metadata(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;

	uint32_t numBands;
	double scaleX;
	double scaleY;
	double ipX;
	double ipY;
	double skewX;
	double skewY;
	int32_t srid;
	uint32_t width;
	uint32_t height;

	TupleDesc tupdesc;
	int values_length = 10;
	Datum values[values_length];
	bool nulls[values_length];
	HeapTuple tuple;
	Datum result;

	POSTGIS_RT_DEBUG(3, "RASTER_metadata: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

	/* raster */
	raster = rt_raster_deserialize(pgraster, TRUE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_metadata; Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* upper left x, y */
	ipX = rt_raster_get_x_offset(raster);
	ipY = rt_raster_get_y_offset(raster);

	/* width, height */
	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);

	/* scale x, y */
	scaleX = rt_raster_get_x_scale(raster);
	scaleY = rt_raster_get_y_scale(raster);

	/* skew x, y */
	skewX = rt_raster_get_x_skew(raster);
	skewY = rt_raster_get_y_skew(raster);

	/* srid */
	srid = rt_raster_get_srid(raster);

	/* numbands */
	numBands = rt_raster_get_num_bands(raster);

	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (
			errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg(
				"function returning record called in context "
				"that cannot accept type record"
			)
		));
	}

	BlessTupleDesc(tupdesc);

	values[0] = Float8GetDatum(ipX);
	values[1] = Float8GetDatum(ipY);
	values[2] = UInt32GetDatum(width);
	values[3] = UInt32GetDatum(height);
	values[4] = Float8GetDatum(scaleX);
	values[5] = Float8GetDatum(scaleY);
	values[6] = Float8GetDatum(skewX);
	values[7] = Float8GetDatum(skewY);
	values[8] = Int32GetDatum(srid);
	values[9] = UInt32GetDatum(numBands);

	memset(nulls, FALSE, sizeof(bool) * values_length);

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(RASTER_rasterToWorldCoord);
Datum RASTER_rasterToWorldCoord(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	int i = 0;
	int cr[2] = {0};
	bool skewed[2] = {false, false};
	double cw[2] = {0};

	TupleDesc tupdesc;
	int values_length = 2;
	Datum values[values_length];
	bool nulls[values_length];
	HeapTuple tuple;
	Datum result;

	POSTGIS_RT_DEBUG(3, "RASTER_rasterToWorldCoord: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

	/* raster */
	raster = rt_raster_deserialize(pgraster, TRUE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_rasterToWorldCoord: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* raster skewed? */
	skewed[0] = FLT_NEQ(rt_raster_get_x_skew(raster), 0) ? true : false;
	skewed[1] = FLT_NEQ(rt_raster_get_y_skew(raster), 0) ? true : false;

	/* column and row */
	for (i = 1; i <= 2; i++) {
		if (PG_ARGISNULL(i)) {
			/* if skewed on same axis, parameter is required */
			if (skewed[i - 1]) {
				elog(NOTICE, "Pixel row and column required for computing longitude and latitude of a rotated raster");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				PG_RETURN_NULL();
			}

			continue;
		}

		cr[i - 1] = PG_GETARG_INT32(i);
	}

	/* user-provided value is 1-based but needs to be 0-based */
	if (rt_raster_cell_to_geopoint(
		raster,
		(double) cr[0] - 1, (double) cr[1] - 1,
		&(cw[0]), &(cw[1]),
		NULL
	) != ES_NONE) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_rasterToWorldCoord: Could not compute longitude and latitude from pixel row and column");
		PG_RETURN_NULL();
	}
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (
			errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg(
				"function returning record called in context "
				"that cannot accept type record"
			)
		));
	}

	BlessTupleDesc(tupdesc);

	values[0] = Float8GetDatum(cw[0]);
	values[1] = Float8GetDatum(cw[1]);

	memset(nulls, FALSE, sizeof(bool) * values_length);

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(RASTER_worldToRasterCoord);
Datum RASTER_worldToRasterCoord(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	int i = 0;
	double cw[2] = {0};
	double _cr[2] = {0};
	int cr[2] = {0};
	bool skewed = false;

	TupleDesc tupdesc;
	int values_length = 2;
	Datum values[values_length];
	bool nulls[values_length];
	HeapTuple tuple;
	Datum result;

	POSTGIS_RT_DEBUG(3, "RASTER_worldToRasterCoord: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));

	/* raster */
	raster = rt_raster_deserialize(pgraster, TRUE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_worldToRasterCoord: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* raster skewed? */
	skewed = FLT_NEQ(rt_raster_get_x_skew(raster), 0) ? true : false;
	if (!skewed)
		skewed = FLT_NEQ(rt_raster_get_y_skew(raster), 0) ? true : false;

	/* longitude and latitude */
	for (i = 1; i <= 2; i++) {
		if (PG_ARGISNULL(i)) {
			/* if skewed, parameter is required */
			if (skewed) {
				elog(NOTICE, "Latitude and longitude required for computing pixel row and column of a rotated raster");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				PG_RETURN_NULL();
			}

			continue;
		}

		cw[i - 1] = PG_GETARG_FLOAT8(i);
	}

	/* return pixel row and column values are 0-based */
	if (rt_raster_geopoint_to_cell(
		raster,
		cw[0], cw[1],
		&(_cr[0]), &(_cr[1]),
		NULL
	) != ES_NONE) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_worldToRasterCoord: Could not compute pixel row and column from longitude and latitude");
		PG_RETURN_NULL();
	}
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	/* force to integer and add one to make 1-based */
	cr[0] = ((int) _cr[0]) + 1;
	cr[1] = ((int) _cr[1]) + 1;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (
			errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg(
				"function returning record called in context "
				"that cannot accept type record"
			)
		));
	}

	BlessTupleDesc(tupdesc);

	values[0] = Int32GetDatum(cr[0]);
	values[1] = Int32GetDatum(cr[1]);

	memset(nulls, FALSE, sizeof(bool) * values_length);

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/**
 * Set the SRID associated with the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setSRID);
Datum RASTER_setSRID(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	int32_t newSRID = PG_GETARG_INT32(1);

	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setSRID: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_srid(raster, newSRID);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn) PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);

	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the scale of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setScale);
Datum RASTER_setScale(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double size = PG_GETARG_FLOAT8(1);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setScale: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_scale(raster, size, size);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the pixel size of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setScaleXY);
Datum RASTER_setScaleXY(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double xscale = PG_GETARG_FLOAT8(1);
	double yscale = PG_GETARG_FLOAT8(2);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setScaleXY: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_scale(raster, xscale, yscale);
	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the skew of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setSkew);
Datum RASTER_setSkew(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double skew = PG_GETARG_FLOAT8(1);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setSkew: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_skews(raster, skew, skew);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the skew of the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setSkewXY);
Datum RASTER_setSkewXY(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double xskew = PG_GETARG_FLOAT8(1);
	double yskew = PG_GETARG_FLOAT8(2);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setSkewXY: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_skews(raster, xskew, yskew);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the raster offset in the X and Y dimension.
 */
PG_FUNCTION_INFO_V1(RASTER_setUpperLeftXY);
Datum RASTER_setUpperLeftXY(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double xoffset = PG_GETARG_FLOAT8(1);
	double yoffset = PG_GETARG_FLOAT8(2);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setUpperLeftXY: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	rt_raster_set_offsets(raster, xoffset, yoffset);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size); 
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the geotransform of the supplied raster. Returns the raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setGeotransform);
Datum RASTER_setGeotransform(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	float8 imag, jmag, theta_i, theta_ij, xoffset, yoffset;

	if (
		PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4) ||
		PG_ARGISNULL(5) || PG_ARGISNULL(6)
	) {
		PG_RETURN_NULL();
	}

	/* get the inputs */
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	imag = PG_GETARG_FLOAT8(1);
	jmag = PG_GETARG_FLOAT8(2);
	theta_i = PG_GETARG_FLOAT8(3);
	theta_ij = PG_GETARG_FLOAT8(4);
	xoffset = PG_GETARG_FLOAT8(5);
	yoffset = PG_GETARG_FLOAT8(6);

	raster = rt_raster_deserialize(pgraster, TRUE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setGeotransform: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* store the new geotransform */
	rt_raster_set_phys_params(raster, imag,jmag,theta_i,theta_ij);
	rt_raster_set_offsets(raster, xoffset, yoffset);

	/* prep the return value */
	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL(); 

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set the rotation of the raster. This method will change the X Scale,
 * Y Scale, X Skew and Y Skew properties all at once to keep the rotations
 * about the X and Y axis uniform.
 *
 * This method will set the rotation about the X axis and Y axis based on
 * the pixel size. This pixel size may not be uniform if rasters have different
 * skew values (the raster cells are diamond-shaped). If a raster has different
 * skew values has a rotation set upon it, this method will remove the 
 * diamond distortions of the cells, as each axis will have the same rotation.
 */
PG_FUNCTION_INFO_V1(RASTER_setRotation);
Datum RASTER_setRotation(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster;
	double rotation = PG_GETARG_FLOAT8(1);
	double imag, jmag, theta_i, theta_ij;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (! raster ) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setRotation: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* preserve all defining characteristics of the grid except for rotation */
	rt_raster_get_phys_params(raster, &imag, &jmag, &theta_i, &theta_ij);
	rt_raster_set_phys_params(raster, imag, jmag, rotation, theta_ij);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}
