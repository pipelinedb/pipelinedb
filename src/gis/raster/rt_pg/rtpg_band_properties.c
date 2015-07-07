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
#include <utils/builtins.h>
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */
#include "utils/array.h" /* for ArrayType */
#include "catalog/pg_type.h" /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */

#include "../../postgis_config.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "rtpostgis.h"

/* Get all the properties of a raster band */
Datum RASTER_getBandPixelType(PG_FUNCTION_ARGS);
Datum RASTER_getBandPixelTypeName(PG_FUNCTION_ARGS);
Datum RASTER_getBandNoDataValue(PG_FUNCTION_ARGS);
Datum RASTER_getBandPath(PG_FUNCTION_ARGS);
Datum RASTER_bandIsNoData(PG_FUNCTION_ARGS);

/* get raster band's meta data */
Datum RASTER_bandmetadata(PG_FUNCTION_ARGS);

/* Set all the properties of a raster band */
Datum RASTER_setBandIsNoData(PG_FUNCTION_ARGS);
Datum RASTER_setBandNoDataValue(PG_FUNCTION_ARGS);

/**
 * Return pixel type of the specified band of raster.
 * Band index is 1-based.
 */
PG_FUNCTION_INFO_V1(RASTER_getBandPixelType);
Datum RASTER_getBandPixelType(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    rt_band band = NULL;
    rt_pixtype pixtype;
    int32_t bandindex;

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    /* Index is 1-based */
    bandindex = PG_GETARG_INT32(1);
    if ( bandindex < 1 ) {
        elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    raster = rt_raster_deserialize(pgraster, FALSE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getBandPixelType: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* Fetch requested band and its pixel type */
    band = rt_raster_get_band(raster, bandindex - 1);
    if ( ! band ) {
        elog(NOTICE, "Could not find raster band of index %d when getting pixel type. Returning NULL", bandindex);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    pixtype = rt_band_get_pixtype(band);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_INT32(pixtype);
}

/**
 * Return name of pixel type of the specified band of raster.
 * Band index is 1-based.
 * NOTE: This is unofficial utility not included in the spec.
 */
PG_FUNCTION_INFO_V1(RASTER_getBandPixelTypeName);
Datum RASTER_getBandPixelTypeName(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    rt_band band = NULL;
    rt_pixtype pixtype;
    int32_t bandindex;
    const size_t name_size = 8; /* size of type name */
    size_t size = 0;
    char *ptr = NULL;
    text *result = NULL;

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    /* Index is 1-based */
    bandindex = PG_GETARG_INT32(1);
    if ( bandindex < 1 ) {
        elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    raster = rt_raster_deserialize(pgraster, FALSE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getBandPixelTypeName: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* Fetch requested band and its pixel type */
    band = rt_raster_get_band(raster, bandindex - 1);
    if ( ! band ) {
        elog(NOTICE, "Could not find raster band of index %d when getting pixel type name. Returning NULL", bandindex);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    pixtype = rt_band_get_pixtype(band);

    result = palloc(VARHDRSZ + name_size);
    /* We don't need to check for NULL pointer, because if out of memory, palloc
     * exit via elog(ERROR). It never returns NULL.
     */

    memset(VARDATA(result), 0, name_size);
    ptr = (char *)result + VARHDRSZ;
		strcpy(ptr, rt_pixtype_name(pixtype));

    size = VARHDRSZ + strlen(ptr);
    SET_VARSIZE(result, size);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_TEXT_P(result);
}

/**
 * Return nodata value of the specified band of raster.
 * The value is always returned as FLOAT32 even if the pixel type is INTEGER.
 */
PG_FUNCTION_INFO_V1(RASTER_getBandNoDataValue);
Datum RASTER_getBandNoDataValue(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    rt_band band = NULL;
    int32_t bandindex;
    double nodata;

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    /* Index is 1-based */
    bandindex = PG_GETARG_INT32(1);
    if ( bandindex < 1 ) {
        elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    raster = rt_raster_deserialize(pgraster, FALSE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_getBandNoDataValue: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* Fetch requested band and its nodata value */
    band = rt_raster_get_band(raster, bandindex - 1);
    if ( ! band ) {
        elog(NOTICE, "Could not find raster band of index %d when getting band nodata value. Returning NULL", bandindex);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    if ( ! rt_band_get_hasnodata_flag(band) ) {
        /* Raster does not have a nodata value set so we return NULL */
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    rt_band_get_nodata(band, &nodata);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_FLOAT8(nodata);
}


PG_FUNCTION_INFO_V1(RASTER_bandIsNoData);
Datum RASTER_bandIsNoData(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_raster raster = NULL;
    rt_band band = NULL;
    int32_t bandindex;
    bool forcechecking = FALSE;
    bool bandisnodata = FALSE;

    /* Index is 1-based */
    bandindex = PG_GETARG_INT32(1);
    if ( bandindex < 1 ) {
        elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
        PG_RETURN_NULL();
    }

    /* Deserialize raster */
    if (PG_ARGISNULL(0)) PG_RETURN_NULL();
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    raster = rt_raster_deserialize(pgraster, FALSE);
    if ( ! raster ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_bandIsNoData: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    /* Fetch requested band and its nodata value */
    band = rt_raster_get_band(raster, bandindex - 1);
    if ( ! band ) {
        elog(NOTICE, "Could not find raster band of index %d when determining if band is nodata. Returning NULL", bandindex);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        PG_RETURN_NULL();
    }

    forcechecking = PG_GETARG_BOOL(2);

    bandisnodata = (forcechecking) ?
        rt_band_check_is_nodata(band) : rt_band_get_isnodata_flag(band);

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    PG_RETURN_BOOL(bandisnodata);
}

/**
 * Return the path of the raster for out-db raster.
 */
PG_FUNCTION_INFO_V1(RASTER_getBandPath);
Datum RASTER_getBandPath(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int32_t bandindex;
	const char *bandpath;
	text *result;

	/* Index is 1-based */
	bandindex = PG_GETARG_INT32(1);
	if ( bandindex < 1 ) {
		elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
		PG_RETURN_NULL();
	}

	/* Deserialize raster */
	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_getBandPath: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* Fetch requested band */
	band = rt_raster_get_band(raster, bandindex - 1);
	if (!band) {
		elog(
			NOTICE,
			"Could not find raster band of index %d when getting band path. Returning NULL",
			bandindex
		);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	bandpath = rt_band_get_ext_path(band);
	if (!bandpath) {
		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	result = (text *) palloc(VARHDRSZ + strlen(bandpath) + 1);
	SET_VARSIZE(result, VARHDRSZ + strlen(bandpath) + 1); 
	strcpy((char *) VARDATA(result), bandpath);

	rt_band_destroy(band);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	PG_RETURN_TEXT_P(result);
}

/**
 * Get raster bands' meta data
 */
PG_FUNCTION_INFO_V1(RASTER_bandmetadata);
Datum RASTER_bandmetadata(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	int call_cntr;
	int max_calls;

	struct bandmetadata {
		uint32_t bandnum;
		char *pixeltype;
		bool hasnodata;
		double nodataval;
		bool isoutdb;
		char *bandpath;
	};
	struct bandmetadata *bmd = NULL;
	struct bandmetadata *bmd2 = NULL;

	HeapTuple tuple;
	Datum result;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;
		int16 typlen;
		bool typbyval;
		char typalign;
		int i = 0;
		int j = 0;
		int n = 0;

		uint32_t numBands;
		uint32_t idx = 1;
		uint32_t *bandNums = NULL;
		const char *tmp = NULL;

		POSTGIS_RT_DEBUG(3, "RASTER_bandmetadata: Starting");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* pgraster is null, return null */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		/* raster */
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_bandmetadata: Could not deserialize raster");
			SRF_RETURN_DONE(funcctx);
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(raster);
		if (numBands < 1) {
			elog(NOTICE, "Raster provided has no bands");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* band index */
		array = PG_GETARG_ARRAYTYPE_P(1);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case INT2OID:
			case INT4OID:
				break;
			default:
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_bandmetadata: Invalid data type for band number(s)");
				SRF_RETURN_DONE(funcctx);
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		bandNums = palloc(sizeof(uint32_t) * n);
		for (i = 0, j = 0; i < n; i++) {
			if (nulls[i]) continue;

			switch (etype) {
				case INT2OID:
					idx = (uint32_t) DatumGetInt16(e[i]);
					break;
				case INT4OID:
					idx = (uint32_t) DatumGetInt32(e[i]);
					break;
			}

			POSTGIS_RT_DEBUGF(3, "band idx (before): %d", idx);
			if (idx > numBands || idx < 1) {
				elog(NOTICE, "Invalid band index: %d. Indices must be 1-based. Returning NULL", idx);
				pfree(bandNums);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			bandNums[j] = idx;
			POSTGIS_RT_DEBUGF(3, "bandNums[%d] = %d", j, bandNums[j]);
			j++;
		}

		if (j < 1) {
			j = numBands;
			bandNums = repalloc(bandNums, sizeof(uint32_t) * j);
			for (i = 0; i < j; i++)
				bandNums[i] = i + 1;
		}
		else if (j < n)
			bandNums = repalloc(bandNums, sizeof(uint32_t) * j);

		bmd = (struct bandmetadata *) palloc(sizeof(struct bandmetadata) * j);

		for (i = 0; i < j; i++) {
			band = rt_raster_get_band(raster, bandNums[i] - 1);
			if (NULL == band) {
				elog(NOTICE, "Could not get raster band at index %d", bandNums[i]);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* bandnum */
			bmd[i].bandnum = bandNums[i];

			/* pixeltype */
			tmp = rt_pixtype_name(rt_band_get_pixtype(band));
			bmd[i].pixeltype = palloc(sizeof(char) * (strlen(tmp) + 1));
			strncpy(bmd[i].pixeltype, tmp, strlen(tmp) + 1);

			/* hasnodatavalue */
			if (rt_band_get_hasnodata_flag(band))
				bmd[i].hasnodata = TRUE;
			else
				bmd[i].hasnodata = FALSE;

			/* nodatavalue */
			if (bmd[i].hasnodata)
				rt_band_get_nodata(band, &(bmd[i].nodataval));
			else
				bmd[i].nodataval = 0;

			/* path */
			tmp = rt_band_get_ext_path(band);
			if (tmp) {
				bmd[i].bandpath = palloc(sizeof(char) * (strlen(tmp) + 1));
				strncpy(bmd[i].bandpath, tmp, strlen(tmp) + 1);
			}
			else
				bmd[i].bandpath = NULL;

			/* isoutdb */
			bmd[i].isoutdb = bmd[i].bandpath ? TRUE : FALSE;

			rt_band_destroy(band);
		}

		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);

		/* Store needed information */
		funcctx->user_fctx = bmd;

		/* total number of tuples to be returned */
		funcctx->max_calls = j;

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
	bmd2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 5;
		Datum values[values_length];
		bool nulls[values_length];

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = UInt32GetDatum(bmd2[call_cntr].bandnum);
		values[1] = CStringGetTextDatum(bmd2[call_cntr].pixeltype);

		if (bmd2[call_cntr].hasnodata)
			values[2] = Float8GetDatum(bmd2[call_cntr].nodataval);
		else
			nulls[2] = TRUE;

		values[3] = BoolGetDatum(bmd2[call_cntr].isoutdb);
		if (bmd2[call_cntr].bandpath && strlen(bmd2[call_cntr].bandpath))
			values[4] = CStringGetTextDatum(bmd2[call_cntr].bandpath);
		else
			nulls[4] = TRUE;

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up */
		pfree(bmd2[call_cntr].pixeltype);
		if (bmd2[call_cntr].bandpath) pfree(bmd2[call_cntr].bandpath);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(bmd2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Set the nodata value of the specified band of raster.
 */
PG_FUNCTION_INFO_V1(RASTER_setBandNoDataValue);
Datum RASTER_setBandNoDataValue(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	double nodata;
	int32_t bandindex;
	bool forcechecking = FALSE;
	bool skipset = FALSE;

	/* Deserialize raster */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* Check index is not NULL or smaller than 1 */
	if (PG_ARGISNULL(1))
		bandindex = -1;
	else
		bandindex = PG_GETARG_INT32(1);
	if (bandindex < 1) {
		elog(NOTICE, "Invalid band index (must use 1-based). Nodata value not set. Returning original raster");
		skipset = TRUE;
	}

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setBandNoDataValue: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	if (!skipset) {
		/* Fetch requested band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Could not find raster band of index %d when setting pixel value. Nodata value not set. Returning original raster", bandindex);
		}
		else {
			if (!PG_ARGISNULL(3))
				forcechecking = PG_GETARG_BOOL(3);

			if (PG_ARGISNULL(2)) {
				/* Set the hasnodata flag to FALSE */
				rt_band_set_hasnodata_flag(band, FALSE);
				POSTGIS_RT_DEBUGF(3, "Raster band %d does not have a nodata value", bandindex);
			}
			else {
				/* Get the nodata value */
				nodata = PG_GETARG_FLOAT8(2);

				/* Set the band's nodata value */
				rt_band_set_nodata(band, nodata, NULL);

				/* Recheck all pixels if requested */
				if (forcechecking)
					rt_band_check_is_nodata(band);
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

PG_FUNCTION_INFO_V1(RASTER_setBandIsNoData);
Datum RASTER_setBandIsNoData(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int32_t bandindex;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_setBandIsNoData: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* Check index is not NULL or smaller than 1 */
	if (PG_ARGISNULL(1))
		bandindex = -1;
	else
		bandindex = PG_GETARG_INT32(1);

	if (bandindex < 1)
		elog(NOTICE, "Invalid band index (must use 1-based). Isnodata flag not set. Returning original raster");
	else {
		/* Fetch requested band */
		band = rt_raster_get_band(raster, bandindex - 1);

		if (!band)
			elog(NOTICE, "Could not find raster band of index %d. Isnodata flag not set. Returning original raster", bandindex);
		else {
			if (!rt_band_get_hasnodata_flag(band)) {
				elog(NOTICE, "Band of index %d has no NODATA so cannot be NODATA. Returning original raster", bandindex);
			}
			/* Set the band's nodata value */
			else {
				rt_band_set_isnodata_flag(band, 1);
			}
		}
	}

	/* Serialize raster again */
	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn) PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

