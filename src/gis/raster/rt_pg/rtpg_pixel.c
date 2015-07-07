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
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */
#include <funcapi.h>
#include "utils/array.h" /* for ArrayType */
#include "catalog/pg_type.h" /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */

#include "../../postgis_config.h"
#include "lwgeom_pg.h"


#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "rtpostgis.h"

/* Get pixel value */
Datum RASTER_getPixelValue(PG_FUNCTION_ARGS);
Datum RASTER_dumpValues(PG_FUNCTION_ARGS);

/* Set pixel value(s) */
Datum RASTER_setPixelValue(PG_FUNCTION_ARGS);
Datum RASTER_setPixelValuesArray(PG_FUNCTION_ARGS);
Datum RASTER_setPixelValuesGeomval(PG_FUNCTION_ARGS);

/* Get pixels of value */
Datum RASTER_pixelOfValue(PG_FUNCTION_ARGS);

/* Get nearest value to a point */
Datum RASTER_nearestValue(PG_FUNCTION_ARGS);

/* Get the neighborhood around a pixel */
Datum RASTER_neighborhood(PG_FUNCTION_ARGS);

/**
 * Return value of a single pixel.
 * Pixel location is specified by 1-based index of Nth band of raster and
 * X,Y coordinates (X <= RT_Width(raster) and Y <= RT_Height(raster)).
 *
 * TODO: Should we return NUMERIC instead of FLOAT8 ?
 */
PG_FUNCTION_INFO_V1(RASTER_getPixelValue);
Datum RASTER_getPixelValue(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    rt_band band = NULL;
    double pixvalue = 0;
    int32_t bandindex = 0;
    int32_t x = 0;
    int32_t y = 0;
    int result = 0;
    bool exclude_nodata_value = TRUE;
		int isnodata = 0;

    /* Index is 1-based */
    bandindex = PG_GETARG_INT32(1);
    if ( bandindex < 1 ) {
        elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
        PG_RETURN_NULL();
    }

    x = PG_GETARG_INT32(2);

    y = PG_GETARG_INT32(3);

    exclude_nodata_value = PG_GETARG_BOOL(4);

    POSTGIS_RT_DEBUGF(3, "Pixel coordinates (%d, %d)", x, y);

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    raster = rt_raster_deserialize(pgraster, FALSE);
    if (!raster) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getPixelValue: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* Fetch Nth band using 0-based internal index */
    band = rt_raster_get_band(raster, bandindex - 1);
    if (! band) {
        elog(NOTICE, "Could not find raster band of index %d when getting pixel "
                "value. Returning NULL", bandindex);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }
    /* Fetch pixel using 0-based coordinates */
    result = rt_band_get_pixel(band, x - 1, y - 1, &pixvalue, &isnodata);

    /* If the result is -1 or the value is nodata and we take nodata into account
     * then return nodata = NULL */
    if (result != ES_NONE || (exclude_nodata_value && isnodata)) {
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(pixvalue);
}

/* ---------------------------------------------------------------- */
/*  ST_DumpValues function                                          */
/* ---------------------------------------------------------------- */

typedef struct rtpg_dumpvalues_arg_t *rtpg_dumpvalues_arg;
struct rtpg_dumpvalues_arg_t {
	int numbands;
	int rows;
	int columns;

	int *nbands; /* 0-based */
	Datum **values;
	bool **nodata;
};

static rtpg_dumpvalues_arg rtpg_dumpvalues_arg_init() {
	rtpg_dumpvalues_arg arg = NULL;

	arg = palloc(sizeof(struct rtpg_dumpvalues_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_dumpvalues_arg_init: Could not allocate memory for arguments");
		return NULL;
	}

	arg->numbands = 0;
	arg->rows = 0;
	arg->columns = 0;

	arg->nbands = NULL;
	arg->values = NULL;
	arg->nodata = NULL;

	return arg;
}

static void rtpg_dumpvalues_arg_destroy(rtpg_dumpvalues_arg arg) {
	int i = 0;

	if (arg->numbands > 0) {
		if (arg->nbands != NULL)
			pfree(arg->nbands);

		if (arg->values != NULL) {
			for (i = 0; i < arg->numbands; i++) {

				if (arg->values[i] != NULL)
					pfree(arg->values[i]);

				if (arg->nodata[i] != NULL)
					pfree(arg->nodata[i]);
			}

			pfree(arg->values);
		}

		if (arg->nodata != NULL)
			pfree(arg->nodata);
	}

	pfree(arg);
}

PG_FUNCTION_INFO_V1(RASTER_dumpValues);
Datum RASTER_dumpValues(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	int call_cntr;
	int max_calls;
	int i = 0;
	int x = 0;
	int y = 0;
	int z = 0;

	int16 typlen;
	bool typbyval;
	char typalign;

	rtpg_dumpvalues_arg arg1 = NULL;
	rtpg_dumpvalues_arg arg2 = NULL;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int numbands = 0;
		int j = 0;
		bool exclude_nodata_value = TRUE;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;

		double val = 0;
		int isnodata = 0;

		POSTGIS_RT_DEBUG(2, "RASTER_dumpValues first call");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Get input arguments */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			ereport(ERROR, (
				errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Could not deserialize raster")
			));
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* check that raster is not empty */
		/*
		if (rt_raster_is_empty(raster)) {
			elog(NOTICE, "Raster provided is empty");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		*/

		/* raster has bands */
		numbands = rt_raster_get_num_bands(raster); 
		if (!numbands) {
			elog(NOTICE, "Raster provided has no bands");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* initialize arg1 */
		arg1 = rtpg_dumpvalues_arg_init();
		if (arg1 == NULL) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_dumpValues: Could not initialize argument structure");
			SRF_RETURN_DONE(funcctx);
		}

		/* nband, array */
		if (!PG_ARGISNULL(1)) {
			array = PG_GETARG_ARRAYTYPE_P(1);
			etype = ARR_ELEMTYPE(array);
			get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

			switch (etype) {
				case INT2OID:
				case INT4OID:
					break;
				default:
					rtpg_dumpvalues_arg_destroy(arg1);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_dumpValues: Invalid data type for band indexes");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e, &nulls, &(arg1->numbands));

			arg1->nbands = palloc(sizeof(int) * arg1->numbands);
			if (arg1->nbands == NULL) {
				rtpg_dumpvalues_arg_destroy(arg1);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_dumpValues: Could not allocate memory for band indexes");
				SRF_RETURN_DONE(funcctx);
			}

			for (i = 0, j = 0; i < arg1->numbands; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case INT2OID:
						arg1->nbands[j] = DatumGetInt16(e[i]) - 1;
						break;
					case INT4OID:
						arg1->nbands[j] = DatumGetInt32(e[i]) - 1;
						break;
				}

				j++;
			}

			if (j < arg1->numbands) {
				arg1->nbands = repalloc(arg1->nbands, sizeof(int) * j);
				if (arg1->nbands == NULL) {
					rtpg_dumpvalues_arg_destroy(arg1);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_dumpValues: Could not reallocate memory for band indexes");
					SRF_RETURN_DONE(funcctx);
				}

				arg1->numbands = j;
			}

			/* validate nbands */
			for (i = 0; i < arg1->numbands; i++) {
				if (!rt_raster_has_band(raster, arg1->nbands[i])) {
					elog(NOTICE, "Band at index %d not found in raster", arg1->nbands[i] + 1);
					rtpg_dumpvalues_arg_destroy(arg1);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}
			}

		}
		/* no bands specified, return all bands */
		else {
			arg1->numbands = numbands;
			arg1->nbands = palloc(sizeof(int) * arg1->numbands);

			if (arg1->nbands == NULL) {
				rtpg_dumpvalues_arg_destroy(arg1);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_dumpValues: Could not allocate memory for band indexes");
				SRF_RETURN_DONE(funcctx);
			}

			for (i = 0; i < arg1->numbands; i++) {
				arg1->nbands[i] = i;
				POSTGIS_RT_DEBUGF(4, "arg1->nbands[%d] = %d", arg1->nbands[i], i);
			}
		}

		arg1->rows = rt_raster_get_height(raster);
		arg1->columns = rt_raster_get_width(raster);

		/* exclude_nodata_value */
		if (!PG_ARGISNULL(2))
			exclude_nodata_value = PG_GETARG_BOOL(2);
		POSTGIS_RT_DEBUGF(4, "exclude_nodata_value = %d", exclude_nodata_value);

		/* allocate memory for each band's values and nodata flags */
		arg1->values = palloc(sizeof(Datum *) * arg1->numbands);
		arg1->nodata = palloc(sizeof(bool *) * arg1->numbands);
		if (arg1->values == NULL || arg1->nodata == NULL) {
			rtpg_dumpvalues_arg_destroy(arg1);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_dumpValues: Could not allocate memory for pixel values");
			SRF_RETURN_DONE(funcctx);
		}
		memset(arg1->values, 0, sizeof(Datum *) * arg1->numbands);
		memset(arg1->nodata, 0, sizeof(bool *) * arg1->numbands);

		/* get each band and dump data */
		for (z = 0; z < arg1->numbands; z++) {
			/* shortcut if raster is empty */
			if (rt_raster_is_empty(raster))
				break;

			band = rt_raster_get_band(raster, arg1->nbands[z]);
			if (!band) {
				int nband = arg1->nbands[z] + 1;
				rtpg_dumpvalues_arg_destroy(arg1);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_dumpValues: Could not get band at index %d", nband);
				SRF_RETURN_DONE(funcctx);
			}

			/* allocate memory for values and nodata flags */
			arg1->values[z] = palloc(sizeof(Datum) * arg1->rows * arg1->columns);
			arg1->nodata[z] = palloc(sizeof(bool) * arg1->rows * arg1->columns);
			if (arg1->values[z] == NULL || arg1->nodata[z] == NULL) {
				rtpg_dumpvalues_arg_destroy(arg1);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_dumpValues: Could not allocate memory for pixel values");
				SRF_RETURN_DONE(funcctx);
			}
			memset(arg1->values[z], 0, sizeof(Datum) * arg1->rows * arg1->columns);
			memset(arg1->nodata[z], 0, sizeof(bool) * arg1->rows * arg1->columns);

			i = 0;

			/* shortcut if band is NODATA */
			if (rt_band_get_isnodata_flag(band)) {
				for (i = (arg1->rows * arg1->columns) - 1; i >= 0; i--)
					arg1->nodata[z][i] = TRUE;
				continue;
			}

			for (y = 0; y < arg1->rows; y++) {
				for (x = 0; x < arg1->columns; x++) {
					/* get pixel */
					if (rt_band_get_pixel(band, x, y, &val, &isnodata) != ES_NONE) {
						int nband = arg1->nbands[z] + 1;
						rtpg_dumpvalues_arg_destroy(arg1);
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 0);
						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_dumpValues: Could not pixel (%d, %d) of band %d", x, y, nband);
						SRF_RETURN_DONE(funcctx);
					}

					arg1->values[z][i] = Float8GetDatum(val);
					POSTGIS_RT_DEBUGF(5, "arg1->values[z][i] = %f", DatumGetFloat8(arg1->values[z][i]));
					POSTGIS_RT_DEBUGF(5, "clamped is?: %d", rt_band_clamped_value_is_nodata(band, val));

					if (exclude_nodata_value && isnodata) {
						arg1->nodata[z][i] = TRUE;
						POSTGIS_RT_DEBUG(5, "nodata = 1");
					}
					else
						POSTGIS_RT_DEBUG(5, "nodata = 0");

					i++;
				}
			}
		}

		/* cleanup */
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);

		/* Store needed information */
		funcctx->user_fctx = arg1;

		/* total number of tuples to be returned */
		funcctx->max_calls = arg1->numbands;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
			MemoryContextSwitchTo(oldcontext);
			ereport(ERROR, (
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg(
					"function returning record called in context "
					"that cannot accept type record"
				)
			));
		}

		BlessTupleDesc(tupdesc);
		funcctx->tuple_desc = tupdesc;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	tupdesc = funcctx->tuple_desc;
	arg2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 2;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;
		ArrayType *mdValues = NULL;
		int ndim = 2;
		int dim[2] = {arg2->rows, arg2->columns};
		int lbound[2] = {1, 1};

		POSTGIS_RT_DEBUGF(3, "call number %d", call_cntr);
		POSTGIS_RT_DEBUGF(4, "dim = %d, %d", dim[0], dim[1]);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Int32GetDatum(arg2->nbands[call_cntr] + 1);

		/* info about the type of item in the multi-dimensional array (float8). */
		get_typlenbyvalalign(FLOAT8OID, &typlen, &typbyval, &typalign);

		/* if values is NULL, return empty array */
		if (arg2->values[call_cntr] == NULL)
			ndim = 0;

		/* assemble 3-dimension array of values */
		mdValues = construct_md_array(
			arg2->values[call_cntr], arg2->nodata[call_cntr],
			ndim, dim, lbound,
			FLOAT8OID,
			typlen, typbyval, typalign
		);
		values[1] = PointerGetDatum(mdValues);

		/* build a tuple and datum */
		tuple = heap_form_tuple(tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		rtpg_dumpvalues_arg_destroy(arg2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Write value of raster sample on given position and in specified band.
 */
PG_FUNCTION_INFO_V1(RASTER_setPixelValue);
Datum RASTER_setPixelValue(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	double pixvalue = 0;
	int32_t bandindex = 0;
	int32_t x = 0;
	int32_t y = 0;
	bool skipset = FALSE;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* Check index is not NULL or < 1 */
	if (PG_ARGISNULL(1))
		bandindex = -1;
	else
		bandindex = PG_GETARG_INT32(1);
	
	if (bandindex < 1) {
		elog(NOTICE, "Invalid band index (must use 1-based). Value not set. Returning original raster");
		skipset = TRUE;
	}

	/* Validate pixel coordinates are not null */
	if (PG_ARGISNULL(2)) {
		elog(NOTICE, "X coordinate can not be NULL when setting pixel value. Value not set. Returning original raster");
		skipset = TRUE;
	}
	else
		x = PG_GETARG_INT32(2);

	if (PG_ARGISNULL(3)) {
		elog(NOTICE, "Y coordinate can not be NULL when setting pixel value. Value not set. Returning original raster");
		skipset = TRUE;
	}
	else
		y = PG_GETARG_INT32(3);

	POSTGIS_RT_DEBUGF(3, "Pixel coordinates (%d, %d)", x, y);

	/* Deserialize raster */
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValue: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	if (!skipset) {
		/* Fetch requested band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Could not find raster band of index %d when setting "
				"pixel value. Value not set. Returning original raster",
				bandindex);
			PG_RETURN_POINTER(pgraster);
		}
		else {
			/* Set the pixel value */
			if (PG_ARGISNULL(4)) {
				if (!rt_band_get_hasnodata_flag(band)) {
					elog(NOTICE, "Raster do not have a nodata value defined. "
						"Set band nodata value first. Nodata value not set. "
						"Returning original raster");
					PG_RETURN_POINTER(pgraster);
				}
				else {
					rt_band_get_nodata(band, &pixvalue);
					rt_band_set_pixel(band, x - 1, y - 1, pixvalue, NULL);
				}
			}
			else {
				pixvalue = PG_GETARG_FLOAT8(4);
				rt_band_set_pixel(band, x - 1, y - 1, pixvalue, NULL);
			}
		}
	}

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Set pixels to value from array
 */
PG_FUNCTION_INFO_V1(RASTER_setPixelValuesArray);
Datum RASTER_setPixelValuesArray(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int numbands = 0;

	int nband = 0;
	int width = 0;
	int height = 0;

	ArrayType *array;
	Oid etype;
	Datum *elements;
	bool *nulls;
	int16 typlen;
	bool typbyval;
	char typalign;
	int ndims = 1;
	int *dims;
	int num = 0;

	int ul[2] = {0};
	struct pixelvalue {
		int x;
		int y;

		bool noset;
		bool nodata;
		double value;
	};
	struct pixelvalue *pixval = NULL;
	int numpixval = 0;
	int dimpixval[2] = {1, 1};
	int dimnoset[2] = {1, 1};
	int hasnodata = FALSE;
	double nodataval = 0;
	bool keepnodata = FALSE;
	bool hasnosetval = FALSE;
	bool nosetvalisnull = FALSE;
	double nosetval = 0;

	int rtn = 0;
	double val = 0;
	int isnodata = 0;

	int i = 0;
	int j = 0;
	int x = 0;
	int y = 0;

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));

	/* raster */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesArray: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* raster attributes */
	numbands = rt_raster_get_num_bands(raster);
	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);

	/* nband */
	if (PG_ARGISNULL(1)) {
		elog(NOTICE, "Band index cannot be NULL.  Value must be 1-based.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	nband = PG_GETARG_INT32(1);
	if (nband < 1 || nband > numbands) {
		elog(NOTICE, "Band index is invalid.  Value must be 1-based.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	/* x, y */
	for (i = 2, j = 0; i < 4; i++, j++) {
		if (PG_ARGISNULL(i)) {
			elog(NOTICE, "%s cannot be NULL.  Value must be 1-based.  Returning original raster", j < 1 ? "X" : "Y");
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}

		ul[j] = PG_GETARG_INT32(i);
		if (
			(ul[j] < 1) || (
				(j < 1 && ul[j] > width) ||
				(j > 0 && ul[j] > height)
			)
		) {
			elog(NOTICE, "%s is invalid.  Value must be 1-based.  Returning original raster", j < 1 ? "X" : "Y");
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}

		/* force 0-based from 1-based */
		ul[j] -= 1;
	}

	/* new value set */
	if (PG_ARGISNULL(4)) {
		elog(NOTICE, "No values to set.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	array = PG_GETARG_ARRAYTYPE_P(4);
	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	switch (etype) {
		case FLOAT4OID:
		case FLOAT8OID:
			break;
		default:
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesArray: Invalid data type for new values");
			PG_RETURN_NULL();
			break;
	}

	ndims = ARR_NDIM(array);
	dims = ARR_DIMS(array);
	POSTGIS_RT_DEBUGF(4, "ndims = %d", ndims);

	if (ndims < 1 || ndims > 2) {
		elog(NOTICE, "New values array must be of 1 or 2 dimensions.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}
	/* outer element, then inner element */
	/* i = 0, y */
	/* i = 1, x */
	if (ndims != 2)
		dimpixval[1] = dims[0];
	else {
		dimpixval[0] = dims[0];
		dimpixval[1] = dims[1];
	}
	POSTGIS_RT_DEBUGF(4, "dimpixval = (%d, %d)", dimpixval[0], dimpixval[1]);

	deconstruct_array(
		array,
		etype,
		typlen, typbyval, typalign,
		&elements, &nulls, &num
	);

	/* # of elements doesn't match dims */
	if (num < 1 || num != (dimpixval[0] * dimpixval[1])) {
		if (num) {
			pfree(elements);
			pfree(nulls);
		}
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesArray: Could not deconstruct new values array");
		PG_RETURN_NULL();
	}

	/* allocate memory for pixval */
	numpixval = num;
	pixval = palloc(sizeof(struct pixelvalue) * numpixval);
	if (pixval == NULL) {
		pfree(elements);
		pfree(nulls);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesArray: Could not allocate memory for new pixel values");
		PG_RETURN_NULL();
	}

	/* load new values into pixval */
	i = 0;
	for (y = 0; y < dimpixval[0]; y++) {
		for (x = 0; x < dimpixval[1]; x++) {
			/* 0-based */
			pixval[i].x = ul[0] + x;
			pixval[i].y = ul[1] + y;

			pixval[i].noset = FALSE;
			pixval[i].nodata = FALSE;
			pixval[i].value = 0;

			if (nulls[i])
				pixval[i].nodata = TRUE;
			else {
				switch (etype) {
					case FLOAT4OID:
						pixval[i].value = DatumGetFloat4(elements[i]);
						break;
					case FLOAT8OID:
						pixval[i].value = DatumGetFloat8(elements[i]);
						break;
				}
			}

			i++;
		}
	}

	pfree(elements);
	pfree(nulls);

	/* now load noset flags */
	if (!PG_ARGISNULL(5)) {
		array = PG_GETARG_ARRAYTYPE_P(5);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case BOOLOID:
				break;
			default:
				pfree(pixval);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_setPixelValuesArray: Invalid data type for noset flags");
				PG_RETURN_NULL();
				break;
		}

		ndims = ARR_NDIM(array);
		dims = ARR_DIMS(array);
		POSTGIS_RT_DEBUGF(4, "ndims = %d", ndims);

		if (ndims < 1 || ndims > 2) {
			elog(NOTICE, "Noset flags array must be of 1 or 2 dimensions.  Returning original raster");
			pfree(pixval);
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}
		/* outer element, then inner element */
		/* i = 0, y */
		/* i = 1, x */
		if (ndims != 2)
			dimnoset[1] = dims[0];
		else {
			dimnoset[0] = dims[0];
			dimnoset[1] = dims[1];
		}
		POSTGIS_RT_DEBUGF(4, "dimnoset = (%d, %d)", dimnoset[0], dimnoset[1]);

		deconstruct_array(
			array,
			etype,
			typlen, typbyval, typalign,
			&elements, &nulls, &num
		);

		/* # of elements doesn't match dims */
		if (num < 1 || num != (dimnoset[0] * dimnoset[1])) {
			pfree(pixval);
			if (num) {
				pfree(elements);
				pfree(nulls);
			}
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesArray: Could not deconstruct noset flags array");
			PG_RETURN_NULL();
		}

		i = 0;
		j = 0;
		for (y = 0; y < dimnoset[0]; y++) {
			if (y >= dimpixval[0]) break;

			for (x = 0; x < dimnoset[1]; x++) {
				/* fast forward noset elements */
				if (x >= dimpixval[1]) {
					i += (dimnoset[1] - dimpixval[1]);
					break;
				}

				if (!nulls[i] && DatumGetBool(elements[i]))
					pixval[j].noset = TRUE;

				i++;
				j++;
			}

			/* fast forward pixval */
			if (x < dimpixval[1])
				j += (dimpixval[1] - dimnoset[1]);
		}

		pfree(elements);
		pfree(nulls);
	}
	/* hasnosetvalue and nosetvalue */
	else if (!PG_ARGISNULL(6) && PG_GETARG_BOOL(6)) {
		hasnosetval = TRUE;
		if (PG_ARGISNULL(7))
			nosetvalisnull = TRUE;
		else
			nosetval = PG_GETARG_FLOAT8(7);
	}

#if POSTGIS_DEBUG_LEVEL > 0
	for (i = 0; i < numpixval; i++) {
		POSTGIS_RT_DEBUGF(4, "pixval[%d](x, y, noset, nodata, value) = (%d, %d, %d, %d, %f)",
			i,
			pixval[i].x,
			pixval[i].y,
			pixval[i].noset,
			pixval[i].nodata,
			pixval[i].value
		);
	}
#endif

	/* keepnodata flag */
	if (!PG_ARGISNULL(8))
		keepnodata = PG_GETARG_BOOL(8);

	/* get band */
	band = rt_raster_get_band(raster, nband - 1);
	if (!band) {
		elog(NOTICE, "Could not find band at index %d. Returning original raster", nband);
		pfree(pixval);
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	/* get band nodata info */
	/* has NODATA, use NODATA */
	hasnodata = rt_band_get_hasnodata_flag(band);
	if (hasnodata)
		rt_band_get_nodata(band, &nodataval);
	/* no NODATA, use min possible value */
	else
		nodataval = rt_band_get_min_value(band);

	/* set pixels */
	for (i = 0; i < numpixval; i++) {
		/* noset = true, skip */
		if (pixval[i].noset)
			continue;
		/* check against nosetval */
		else if (hasnosetval) {
			/* pixel = NULL AND nosetval = NULL */
			if (pixval[i].nodata && nosetvalisnull)
				continue;
			/* pixel value = nosetval */
			else if (!pixval[i].nodata && !nosetvalisnull && FLT_EQ(pixval[i].value, nosetval))
				continue;
		}

		/* if pixel is outside bounds, skip */
		if (
			(pixval[i].x < 0 || pixval[i].x >= width) ||
			(pixval[i].y < 0 || pixval[i].y >= height)
		) {
			elog(NOTICE, "Cannot set value for pixel (%d, %d) outside raster bounds: %d x %d",
				pixval[i].x + 1, pixval[i].y + 1,
				width, height
			);
			continue;
		}

		/* if hasnodata = TRUE and keepnodata = TRUE, inspect pixel value */
		if (hasnodata && keepnodata) {
			rtn = rt_band_get_pixel(band, pixval[i].x, pixval[i].y, &val, &isnodata);
			if (rtn != ES_NONE) {
				pfree(pixval);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "Cannot get value of pixel");
				PG_RETURN_NULL();
			}

			/* pixel value = NODATA, skip */
			if (isnodata) {
				continue;
			}
		}

		if (pixval[i].nodata)
			rt_band_set_pixel(band, pixval[i].x, pixval[i].y, nodataval, NULL);
		else
			rt_band_set_pixel(band, pixval[i].x, pixval[i].y, pixval[i].value, NULL);
	}

	pfree(pixval);

	/* serialize new raster */
	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/* ---------------------------------------------------------------- */
/*  ST_SetValues using geomval array                                */
/* ---------------------------------------------------------------- */

typedef struct rtpg_setvaluesgv_arg_t *rtpg_setvaluesgv_arg;
typedef struct rtpg_setvaluesgv_geomval_t *rtpg_setvaluesgv_geomval;

struct rtpg_setvaluesgv_arg_t {
	int ngv;
	rtpg_setvaluesgv_geomval gv;

	bool keepnodata;
};

struct rtpg_setvaluesgv_geomval_t {
	struct {
		int nodata;
		double value;
	} pixval;

	LWGEOM *geom;
	rt_raster mask;
};

static rtpg_setvaluesgv_arg rtpg_setvaluesgv_arg_init() {
	rtpg_setvaluesgv_arg arg = palloc(sizeof(struct rtpg_setvaluesgv_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_setvaluesgv_arg_init: Could not allocate memory for function arguments");
		return NULL;
	}

	arg->ngv = 0;
	arg->gv = NULL;
	arg->keepnodata = 0;

	return arg;
}

static void rtpg_setvaluesgv_arg_destroy(rtpg_setvaluesgv_arg arg) {
	int i = 0;

	if (arg->gv != NULL) {
		for (i = 0; i < arg->ngv; i++) {
			if (arg->gv[i].geom != NULL)
				lwgeom_free(arg->gv[i].geom);
			if (arg->gv[i].mask != NULL)
				rt_raster_destroy(arg->gv[i].mask);
		}

		pfree(arg->gv);
	}

	pfree(arg);
}

static int rtpg_setvalues_geomval_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	rtpg_setvaluesgv_arg funcarg = (rtpg_setvaluesgv_arg) userarg;
	int i = 0;
	int j = 0;

	*value = 0;
	*nodata = 0;

	POSTGIS_RT_DEBUGF(4, "keepnodata = %d", funcarg->keepnodata);

	/* keepnodata = TRUE */
	if (funcarg->keepnodata && arg->nodata[0][0][0]) {
		POSTGIS_RT_DEBUG(3, "keepnodata = 1 AND source raster pixel is NODATA");
		*nodata = 1;
		return 1;
	}

	for (i = arg->rasters - 1, j = funcarg->ngv - 1; i > 0; i--, j--) {
		POSTGIS_RT_DEBUGF(4, "checking raster %d", i);

		/* mask is NODATA */
		if (arg->nodata[i][0][0])
			continue;
		/* mask is NOT NODATA */
		else {
			POSTGIS_RT_DEBUGF(4, "Using information from geometry %d", j);

			if (funcarg->gv[j].pixval.nodata)
				*nodata = 1;
			else
				*value = funcarg->gv[j].pixval.value;

			return 1;
		}
	}

	POSTGIS_RT_DEBUG(4, "Using information from source raster");

	/* still here */
	if (arg->nodata[0][0][0])
		*nodata = 1;
	else
		*value = arg->values[0][0][0];

	return 1;
}

PG_FUNCTION_INFO_V1(RASTER_setPixelValuesGeomval);
Datum RASTER_setPixelValuesGeomval(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	rt_raster _raster = NULL;
	rt_band _band = NULL;
	int nband = 0; /* 1-based */

	int numbands = 0;
	int width = 0;
	int height = 0;
	int srid = 0;
	double gt[6] = {0};

	rt_pixtype pixtype = PT_END;
	int hasnodata = 0;
	double nodataval = 0;

	rtpg_setvaluesgv_arg arg = NULL;
	int allpoint = 0;

	ArrayType *array;
	Oid etype;
	Datum *e;
	bool *nulls;
	int16 typlen;
	bool typbyval;
	char typalign;
	int n = 0;

	HeapTupleHeader tup;
	bool isnull;
	Datum tupv;

	GSERIALIZED *gser = NULL;
	uint8_t gtype;
	unsigned char *wkb = NULL;
	size_t wkb_len;

	int i = 0;
	int j = 0;
	int noerr = 1;

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));

	/* raster */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesGeomval: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* raster attributes */
	numbands = rt_raster_get_num_bands(raster);
	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);
	srid = clamp_srid(rt_raster_get_srid(raster));
	rt_raster_get_geotransform_matrix(raster, gt);

	/* nband */
	if (PG_ARGISNULL(1)) {
		elog(NOTICE, "Band index cannot be NULL.  Value must be 1-based.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	nband = PG_GETARG_INT32(1);
	if (nband < 1 || nband > numbands) {
		elog(NOTICE, "Band index is invalid.  Value must be 1-based.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	/* get band attributes */
	band = rt_raster_get_band(raster, nband - 1);
	pixtype = rt_band_get_pixtype(band);
	hasnodata = rt_band_get_hasnodata_flag(band);
	if (hasnodata)
		rt_band_get_nodata(band, &nodataval);

	/* array of geomval (2) */
	if (PG_ARGISNULL(2)) {
		elog(NOTICE, "No values to set.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	array = PG_GETARG_ARRAYTYPE_P(2);
	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(
		array,
		etype,
		typlen, typbyval, typalign,
		&e, &nulls, &n
	);

	if (!n) {
		elog(NOTICE, "No values to set.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	/* init arg */
	arg = rtpg_setvaluesgv_arg_init();
	if (arg == NULL) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesGeomval: Could not intialize argument structure");
		PG_RETURN_NULL();
	}

	arg->gv = palloc(sizeof(struct rtpg_setvaluesgv_geomval_t) * n);
	if (arg->gv == NULL) {
		rtpg_setvaluesgv_arg_destroy(arg);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setPixelValuesGeomval: Could not allocate memory for geomval array");
		PG_RETURN_NULL();
	}

	/* process each element */
	arg->ngv = 0;
	for (i = 0; i < n; i++) {
		if (nulls[i])
			continue;

		arg->gv[arg->ngv].pixval.nodata = 0;
		arg->gv[arg->ngv].pixval.value = 0;
		arg->gv[arg->ngv].geom = NULL;
		arg->gv[arg->ngv].mask = NULL;

		/* each element is a tuple */
		tup = (HeapTupleHeader) DatumGetPointer(e[i]);
		if (NULL == tup) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Invalid argument for geomval at index %d", i);
			PG_RETURN_NULL();
		}

		/* first element, geometry */
		POSTGIS_RT_DEBUG(4, "Processing first element (geometry)");
		tupv = GetAttributeByName(tup, "geom", &isnull);
		if (isnull) {
			elog(NOTICE, "First argument (geom) of geomval at index %d is NULL. Skipping", i);
			continue;
		}

		gser = (GSERIALIZED *) PG_DETOAST_DATUM(tupv);
		arg->gv[arg->ngv].geom = lwgeom_from_gserialized(gser);
		if (arg->gv[arg->ngv].geom == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not deserialize geometry of geomval at index %d", i);
			PG_RETURN_NULL();
		}

		/* empty geometry */
		if (lwgeom_is_empty(arg->gv[arg->ngv].geom)) {
			elog(NOTICE, "First argument (geom) of geomval at index %d is an empty geometry. Skipping", i);
			continue;
		}

		/* check SRID */
		if (clamp_srid(gserialized_get_srid(gser)) != srid) {
			elog(NOTICE, "Geometry provided for geomval at index %d does not have the same SRID as the raster: %d. Returning original raster", i, srid);
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}

		/* Get a 2D version of the geometry if necessary */
		if (lwgeom_ndims(arg->gv[arg->ngv].geom) > 2) {
			LWGEOM *geom2d = lwgeom_force_2d(arg->gv[arg->ngv].geom);
			lwgeom_free(arg->gv[arg->ngv].geom);
			arg->gv[arg->ngv].geom = geom2d;
		}

		/* filter for types */
		gtype = gserialized_get_type(gser);

		/* shortcuts for POINT and MULTIPOINT */
		if (gtype == POINTTYPE || gtype == MULTIPOINTTYPE)
			allpoint++;

		/* get wkb of geometry */
		POSTGIS_RT_DEBUG(3, "getting wkb of geometry");
		wkb = lwgeom_to_wkb(arg->gv[arg->ngv].geom, WKB_SFSQL, &wkb_len);

		/* rasterize geometry */
		arg->gv[arg->ngv].mask = rt_raster_gdal_rasterize(
			wkb, wkb_len,
			NULL,
			0, NULL,
			NULL, NULL,
			NULL, NULL,
			NULL, NULL,
			&(gt[1]), &(gt[5]),
			NULL, NULL,
			&(gt[0]), &(gt[3]),
			&(gt[2]), &(gt[4]),
			NULL
		);

		pfree(wkb);
		if (gtype != POINTTYPE && gtype != MULTIPOINTTYPE) {
			lwgeom_free(arg->gv[arg->ngv].geom);
			arg->gv[arg->ngv].geom = NULL;
		}

		if (arg->gv[arg->ngv].mask == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not rasterize geometry of geomval at index %d", i);
			PG_RETURN_NULL();
		}

		/* set SRID */
		rt_raster_set_srid(arg->gv[arg->ngv].mask, srid);

		/* second element, value */
		POSTGIS_RT_DEBUG(4, "Processing second element (val)");
		tupv = GetAttributeByName(tup, "val", &isnull);
		if (isnull) {
			elog(NOTICE, "Second argument (val) of geomval at index %d is NULL. Treating as NODATA", i);
			arg->gv[arg->ngv].pixval.nodata = 1;
		}
		else
			arg->gv[arg->ngv].pixval.value = DatumGetFloat8(tupv);

		(arg->ngv)++;
	}

	/* redim arg->gv if needed */
	if (arg->ngv < n) {
		arg->gv = repalloc(arg->gv, sizeof(struct rtpg_setvaluesgv_geomval_t) * arg->ngv);
		if (arg->gv == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not reallocate memory for geomval array");
			PG_RETURN_NULL();
		}
	}

	/* keepnodata */
	if (!PG_ARGISNULL(3))
		arg->keepnodata = PG_GETARG_BOOL(3);
	POSTGIS_RT_DEBUGF(3, "keepnodata = %d", arg->keepnodata);

	/* keepnodata = TRUE and band is NODATA */
	if (arg->keepnodata && rt_band_get_isnodata_flag(band)) {
		POSTGIS_RT_DEBUG(3, "keepnodata = TRUE and band is NODATA. Not doing anything");
	}
	/* all elements are points */
	else if (allpoint == arg->ngv) {
		double igt[6] = {0};
		double xy[2] = {0};
		double value = 0;
		int isnodata = 0;

		LWCOLLECTION *coll = NULL;
		LWPOINT *point = NULL;
		POINT2D p;

		POSTGIS_RT_DEBUG(3, "all geometries are points, using direct to pixel method");

		/* cache inverse gretransform matrix */
		rt_raster_get_inverse_geotransform_matrix(NULL, gt, igt);

		/* process each geometry */
		for (i = 0; i < arg->ngv; i++) {
			/* convert geometry to collection */
			coll = lwgeom_as_lwcollection(lwgeom_as_multi(arg->gv[i].geom));

			/* process each point in collection */
			for (j = 0; j < coll->ngeoms; j++) {
				point = lwgeom_as_lwpoint(coll->geoms[j]);
				getPoint2d_p(point->point, 0, &p);

				if (rt_raster_geopoint_to_cell(raster, p.x, p.y, &(xy[0]), &(xy[1]), igt) != ES_NONE) {
					rtpg_setvaluesgv_arg_destroy(arg);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					elog(ERROR, "RASTER_setPixelValuesGeomval: Could not process coordinates of point");
					PG_RETURN_NULL();
				}

				/* skip point if outside raster */
				if (
					(xy[0] < 0 || xy[0] >= width) ||
					(xy[1] < 0 || xy[1] >= height)
				) {
					elog(NOTICE, "Point is outside raster extent. Skipping");
					continue;
				}

				/* get pixel value */
				if (rt_band_get_pixel(band, xy[0], xy[1], &value, &isnodata) != ES_NONE) {
					rtpg_setvaluesgv_arg_destroy(arg);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					elog(ERROR, "RASTER_setPixelValuesGeomval: Could not get pixel value");
					PG_RETURN_NULL();
				}

				/* keepnodata = TRUE AND pixel value is NODATA */
				if (arg->keepnodata && isnodata)
					continue;

				/* set pixel */
				if (arg->gv[i].pixval.nodata)
					noerr = rt_band_set_pixel(band, xy[0], xy[1], nodataval, NULL);
				else
					noerr = rt_band_set_pixel(band, xy[0], xy[1], arg->gv[i].pixval.value, NULL);

				if (noerr != ES_NONE) {
					rtpg_setvaluesgv_arg_destroy(arg);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					elog(ERROR, "RASTER_setPixelValuesGeomval: Could not set pixel value");
					PG_RETURN_NULL();
				}
			}
		}
	}
	/* run iterator otherwise */
	else {
		rt_iterator itrset;

		POSTGIS_RT_DEBUG(3, "a mix of geometries, using iterator method");

		/* init itrset */
		itrset = palloc(sizeof(struct rt_iterator_t) * (arg->ngv + 1));
		if (itrset == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not allocate memory for iterator arguments");
			PG_RETURN_NULL();
		}

		/* set first raster's info */
		itrset[0].raster = raster;
		itrset[0].nband = nband - 1;
		itrset[0].nbnodata = 1;

		/* set other raster's info */
		for (i = 0, j = 1; i < arg->ngv; i++, j++) {
			itrset[j].raster = arg->gv[i].mask;
			itrset[j].nband = 0;
			itrset[j].nbnodata = 1;
		}

		/* pass to iterator */
		noerr = rt_raster_iterator(
			itrset, arg->ngv + 1,
			ET_FIRST, NULL,
			pixtype,
			hasnodata, nodataval,
			0, 0,
			NULL,
			arg,
			rtpg_setvalues_geomval_callback,
			&_raster
		);
		pfree(itrset);

		if (noerr != ES_NONE) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not run raster iterator function");
			PG_RETURN_NULL();
		}

		/* copy band from _raster to raster */
		_band = rt_raster_get_band(_raster, 0);
		if (_band == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(_raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not get band from working raster");
			PG_RETURN_NULL();
		}

		_band = rt_raster_replace_band(raster, _band, nband - 1);
		if (_band == NULL) {
			rtpg_setvaluesgv_arg_destroy(arg);
			rt_raster_destroy(_raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_setPixelValuesGeomval: Could not replace band in output raster");
			PG_RETURN_NULL();
		}

		rt_band_destroy(_band);
		rt_raster_destroy(_raster);
	}

	rtpg_setvaluesgv_arg_destroy(arg);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	POSTGIS_RT_DEBUG(3, "Finished");

	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Get pixels of value
 */
PG_FUNCTION_INFO_V1(RASTER_pixelOfValue);
Datum RASTER_pixelOfValue(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	rt_pixel pixels = NULL;
	rt_pixel pixels2 = NULL;
	int count = 0;
	int i = 0;
	int n = 0;
	int call_cntr;
	int max_calls;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int nband = 1;
		int num_bands = 0;
		double *search = NULL;
		int nsearch = 0;
		double val;
		bool exclude_nodata_value = TRUE;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;
		int16 typlen;
		bool typbyval;
		char typalign;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_pixelOfValue: Could not deserialize raster");
			SRF_RETURN_DONE(funcctx);
		}

		/* num_bands */
		num_bands = rt_raster_get_num_bands(raster);
		if (num_bands < 1) {
			elog(NOTICE, "Raster provided has no bands");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* band index is 1-based */
		if (!PG_ARGISNULL(1))
			nband = PG_GETARG_INT32(1);
		if (nband < 1 || nband > num_bands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* search values */
		array = PG_GETARG_ARRAYTYPE_P(2);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case FLOAT4OID:
			case FLOAT8OID:
				break;
			default:
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_pixelOfValue: Invalid data type for pixel values");
				SRF_RETURN_DONE(funcctx);
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		search = palloc(sizeof(double) * n);
		for (i = 0, nsearch = 0; i < n; i++) {
			if (nulls[i]) continue;

			switch (etype) {
				case FLOAT4OID:
					val = (double) DatumGetFloat4(e[i]);
					break;
				case FLOAT8OID:
					val = (double) DatumGetFloat8(e[i]);
					break;
			}

			search[nsearch] = val;
			POSTGIS_RT_DEBUGF(3, "search[%d] = %f", nsearch, search[nsearch]);
			nsearch++;
		}

		/* not searching for anything */
		if (nsearch < 1) {
			elog(NOTICE, "No search values provided. Returning NULL");
			pfree(search);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		else if (nsearch < n)
			search = repalloc(search, sizeof(double) * nsearch);

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(3))
			exclude_nodata_value = PG_GETARG_BOOL(3);

		/* get band */
		band = rt_raster_get_band(raster, nband - 1);
		if (!band) {
			elog(NOTICE, "Could not find band at index %d. Returning NULL", nband);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get pixels of values */
		count = rt_band_get_pixel_of_value(
			band, exclude_nodata_value,
			search, nsearch,
			&pixels
		);
		pfree(search);
		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (count < 1) {
			/* error */
			if (count < 0)
				elog(NOTICE, "Could not get the pixels of search values for band at index %d", nband);
			/* no nearest pixel */
			else
				elog(NOTICE, "No pixels of search values found for band at index %d", nband);

			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* Store needed information */
		funcctx->user_fctx = pixels;

		/* total number of tuples to be returned */
		funcctx->max_calls = count;

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
		funcctx->tuple_desc = tupdesc;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	tupdesc = funcctx->tuple_desc;
	pixels2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 3;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		memset(nulls, FALSE, sizeof(bool) * values_length);

		/* 0-based to 1-based */
		pixels2[call_cntr].x += 1;
		pixels2[call_cntr].y += 1;

		values[0] = Float8GetDatum(pixels2[call_cntr].value);
		values[1] = Int32GetDatum(pixels2[call_cntr].x);
		values[2] = Int32GetDatum(pixels2[call_cntr].y);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else {
		pfree(pixels2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Return nearest value to a point
 */
PG_FUNCTION_INFO_V1(RASTER_nearestValue);
Datum RASTER_nearestValue(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int bandindex = 1;
	int num_bands = 0;
	GSERIALIZED *geom;
	bool exclude_nodata_value = TRUE;
	LWGEOM *lwgeom;
	LWPOINT *point = NULL;
	POINT2D p;

	double x;
	double y;
	int count;
	rt_pixel npixels = NULL;
	double value = 0;
	int hasvalue = 0;
	int isnodata = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_nearestValue: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* band index is 1-based */
	if (!PG_ARGISNULL(1))
		bandindex = PG_GETARG_INT32(1);
	num_bands = rt_raster_get_num_bands(raster);
	if (bandindex < 1 || bandindex > num_bands) {
		elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* point */
	geom = PG_GETARG_GSERIALIZED_P(2);
	if (gserialized_get_type(geom) != POINTTYPE) {
		elog(NOTICE, "Geometry provided must be a point");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_FREE_IF_COPY(geom, 2);
		PG_RETURN_NULL();
	}

	/* exclude_nodata_value flag */
	if (!PG_ARGISNULL(3))
		exclude_nodata_value = PG_GETARG_BOOL(3);

	/* SRIDs of raster and geometry must match  */
	if (clamp_srid(gserialized_get_srid(geom)) != clamp_srid(rt_raster_get_srid(raster))) {
		elog(NOTICE, "SRIDs of geometry and raster do not match");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_FREE_IF_COPY(geom, 2);
		PG_RETURN_NULL();
	}

	/* get band */
	band = rt_raster_get_band(raster, bandindex - 1);
	if (!band) {
		elog(NOTICE, "Could not find band at index %d. Returning NULL", bandindex);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_FREE_IF_COPY(geom, 2);
		PG_RETURN_NULL();
	}

	/* process geometry */
	lwgeom = lwgeom_from_gserialized(geom);

	if (lwgeom_is_empty(lwgeom)) {
		elog(NOTICE, "Geometry provided cannot be empty");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_FREE_IF_COPY(geom, 2);
		PG_RETURN_NULL();
	}

	/* Get a 2D version of the geometry if necessary */
	if (lwgeom_ndims(lwgeom) > 2) {
		LWGEOM *lwgeom2d = lwgeom_force_2d(lwgeom);
		lwgeom_free(lwgeom);
		lwgeom = lwgeom2d;
	}

	point = lwgeom_as_lwpoint(lwgeom);
	getPoint2d_p(point->point, 0, &p);

	if (rt_raster_geopoint_to_cell(
		raster,
		p.x, p.y,
		&x, &y,
		NULL
	) != ES_NONE) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		lwgeom_free(lwgeom);
		PG_FREE_IF_COPY(geom, 2);
		elog(ERROR, "RASTER_nearestValue: Could not compute pixel coordinates from spatial coordinates");
		PG_RETURN_NULL();
	}

	/* get value at point */
	if (
		(x >= 0 && x < rt_raster_get_width(raster)) &&
		(y >= 0 && y < rt_raster_get_height(raster))
	) {
		if (rt_band_get_pixel(band, x, y, &value, &isnodata) != ES_NONE) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			lwgeom_free(lwgeom);
			PG_FREE_IF_COPY(geom, 2);
			elog(ERROR, "RASTER_nearestValue: Could not get pixel value for band at index %d", bandindex);
			PG_RETURN_NULL();
		}

		/* value at point, return value */
		if (!exclude_nodata_value || !isnodata) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			lwgeom_free(lwgeom);
			PG_FREE_IF_COPY(geom, 2);

			PG_RETURN_FLOAT8(value);
		}
	}

	/* get neighborhood */
	count = rt_band_get_nearest_pixel(
		band,
		x, y,
		0, 0,
		exclude_nodata_value,
		&npixels
	);
	rt_band_destroy(band);
	/* error or no neighbors */
	if (count < 1) {
		/* error */
		if (count < 0)
			elog(NOTICE, "Could not get the nearest value for band at index %d", bandindex);
		/* no nearest pixel */
		else
			elog(NOTICE, "No nearest value found for band at index %d", bandindex);

		lwgeom_free(lwgeom);
		PG_FREE_IF_COPY(geom, 2);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* more than one nearest value, see which one is closest */
	if (count > 1) {
		int i = 0;
		LWPOLY *poly = NULL;
		double lastdist = -1;
		double dist;

		for (i = 0; i < count; i++) {
			/* convex-hull of pixel */
			poly = rt_raster_pixel_as_polygon(raster, npixels[i].x, npixels[i].y);
			if (!poly) {
				lwgeom_free(lwgeom);
				PG_FREE_IF_COPY(geom, 2);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_nearestValue: Could not get polygon of neighboring pixel");
				PG_RETURN_NULL();
			}

			/* distance between convex-hull and point */
			dist = lwgeom_mindistance2d(lwpoly_as_lwgeom(poly), lwgeom);
			if (lastdist < 0 || dist < lastdist) {
				value = npixels[i].value;
				hasvalue = 1;
			}
			lastdist = dist;

			lwpoly_free(poly);
		}
	}
	else {
		value = npixels[0].value;
		hasvalue = 1;
	}

	pfree(npixels);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(geom, 2);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	if (hasvalue)
		PG_RETURN_FLOAT8(value);
	else
		PG_RETURN_NULL();
}

/**
 * Return the neighborhood around a pixel
 */
PG_FUNCTION_INFO_V1(RASTER_neighborhood);
Datum RASTER_neighborhood(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int bandindex = 1;
	int num_bands = 0;
	int x = 0;
	int y = 0;
	int _x = 0;
	int _y = 0;
	int distance[2] = {0};
	bool exclude_nodata_value = TRUE;
	double pixval;
	int isnodata = 0;

	rt_pixel npixels = NULL;
	int count;
	double **value2D = NULL;
	int **nodata2D = NULL;

	int i = 0;
	int j = 0;
	int k = 0;
	Datum *value1D = NULL;
	bool *nodata1D = NULL;
	int dim[2] = {0};
	int lbound[2] = {1, 1};
	ArrayType *mdArray = NULL;

	int16 typlen;
	bool typbyval;
	char typalign;

	/* pgraster is null, return nothing */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_neighborhood: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* band index is 1-based */
	if (!PG_ARGISNULL(1))
		bandindex = PG_GETARG_INT32(1);
	num_bands = rt_raster_get_num_bands(raster);
	if (bandindex < 1 || bandindex > num_bands) {
		elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* pixel column, 1-based */
	x = PG_GETARG_INT32(2);
	_x = x - 1;

	/* pixel row, 1-based */
	y = PG_GETARG_INT32(3);
	_y = y - 1;

	/* distance X axis */
	distance[0] = PG_GETARG_INT32(4);
	if (distance[0] < 0) {
		elog(NOTICE, "Invalid value for distancex (must be >= zero). Returning NULL");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}
	distance[0] = (uint16_t) distance[0];

	/* distance Y axis */
	distance[1] = PG_GETARG_INT32(5);
	if (distance[1] < 0) {
		elog(NOTICE, "Invalid value for distancey (must be >= zero). Returning NULL");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}
	distance[1] = (uint16_t) distance[1];

	/* exclude_nodata_value flag */
	if (!PG_ARGISNULL(6))
		exclude_nodata_value = PG_GETARG_BOOL(6);

	/* get band */
	band = rt_raster_get_band(raster, bandindex - 1);
	if (!band) {
		elog(NOTICE, "Could not find band at index %d. Returning NULL", bandindex);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* get neighborhood */
	count = 0;
	npixels = NULL;
	if (distance[0] > 0 || distance[1] > 0) {
		count = rt_band_get_nearest_pixel(
			band,
			_x, _y,
			distance[0], distance[1],
			exclude_nodata_value,
			&npixels
		);
		/* error */
		if (count < 0) {
			elog(NOTICE, "Could not get the pixel's neighborhood for band at index %d", bandindex);
			
			rt_band_destroy(band);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);

			PG_RETURN_NULL();
		}
	}

	/* get pixel's value */
	if (
		(_x >= 0 && _x < rt_band_get_width(band)) &&
		(_y >= 0 && _y < rt_band_get_height(band))
	) {
		if (rt_band_get_pixel(
			band,
			_x, _y,
			&pixval,
			&isnodata
		) != ES_NONE) {
			elog(NOTICE, "Could not get the pixel of band at index %d. Returning NULL", bandindex);
			rt_band_destroy(band);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			PG_RETURN_NULL();
		}
	}
	/* outside band extent, set to NODATA */
	else {
		/* has NODATA, use NODATA */
		if (rt_band_get_hasnodata_flag(band))
			rt_band_get_nodata(band, &pixval);
		/* no NODATA, use min possible value */
		else
			pixval = rt_band_get_min_value(band);
		isnodata = 1;
	}
	POSTGIS_RT_DEBUGF(4, "pixval: %f", pixval);


	/* add pixel to neighborhood */
	count++;
	if (count > 1)
		npixels = (rt_pixel) repalloc(npixels, sizeof(struct rt_pixel_t) * count);
	else
		npixels = (rt_pixel) palloc(sizeof(struct rt_pixel_t));
	if (npixels == NULL) {

		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);

		elog(ERROR, "RASTER_neighborhood: Could not reallocate memory for neighborhood");
		PG_RETURN_NULL();
	}
	npixels[count - 1].x = _x;
	npixels[count - 1].y = _y;
	npixels[count - 1].nodata = 1;
	npixels[count - 1].value = pixval;

	/* set NODATA */
	if (!exclude_nodata_value || !isnodata) {
		npixels[count - 1].nodata = 0;
	}

	/* free unnecessary stuff */
	rt_band_destroy(band);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	/* convert set of rt_pixel to 2D array */
	/* dim is passed with element 0 being Y-axis and element 1 being X-axis */
	count = rt_pixel_set_to_array(
		npixels, count, NULL,
		_x, _y,
		distance[0], distance[1],
		&value2D,
		&nodata2D,
		&(dim[1]), &(dim[0])
	);
	pfree(npixels);
	if (count != ES_NONE) {
		elog(NOTICE, "Could not create 2D array of neighborhood");
		PG_RETURN_NULL();
	}

	/* 1D arrays for values and nodata from 2D arrays */
	value1D = palloc(sizeof(Datum) * dim[0] * dim[1]);
	nodata1D = palloc(sizeof(bool) * dim[0] * dim[1]);

	if (value1D == NULL || nodata1D == NULL) {

		for (i = 0; i < dim[0]; i++) {
			pfree(value2D[i]);
			pfree(nodata2D[i]);
		}
		pfree(value2D);
		pfree(nodata2D);

		elog(ERROR, "RASTER_neighborhood: Could not allocate memory for return 2D array");
		PG_RETURN_NULL();
	}

	/* copy values from 2D arrays to 1D arrays */
	k = 0;
	/* Y-axis */
	for (i = 0; i < dim[0]; i++) {
		/* X-axis */
		for (j = 0; j < dim[1]; j++) {
			nodata1D[k] = (bool) nodata2D[i][j];
			if (!nodata1D[k])
				value1D[k] = Float8GetDatum(value2D[i][j]);
			else
				value1D[k] = PointerGetDatum(NULL);

			k++;
		}
	}

	/* no more need for 2D arrays */
	for (i = 0; i < dim[0]; i++) {
		pfree(value2D[i]);
		pfree(nodata2D[i]);
	}
	pfree(value2D);
	pfree(nodata2D);

	/* info about the type of item in the multi-dimensional array (float8). */
	get_typlenbyvalalign(FLOAT8OID, &typlen, &typbyval, &typalign);

	mdArray = construct_md_array(
		value1D, nodata1D,
		2, dim, lbound,
		FLOAT8OID,
		typlen, typbyval, typalign
	);

	pfree(value1D);
	pfree(nodata1D);

	PG_RETURN_ARRAYTYPE_P(mdArray);
}

