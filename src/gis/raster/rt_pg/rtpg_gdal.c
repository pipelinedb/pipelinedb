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
#include <funcapi.h> /* for SRF */
#include <utils/builtins.h> /* for text_to_cstring() */
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */
#include "utils/array.h" /* for ArrayType */
#include "catalog/pg_type.h" /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */

#include "../../postgis_config.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "rtpostgis.h"
#include "rtpg_internal.h"

/* convert GDAL raster to raster */
Datum RASTER_fromGDALRaster(PG_FUNCTION_ARGS);

/* convert raster to GDAL raster */
Datum RASTER_asGDALRaster(PG_FUNCTION_ARGS);
Datum RASTER_getGDALDrivers(PG_FUNCTION_ARGS);

/* warp a raster using GDAL Warp API */
Datum RASTER_GDALWarp(PG_FUNCTION_ARGS);

/* ---------------------------------------------------------------- */
/* Returns raster from GDAL raster                                  */
/* ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(RASTER_fromGDALRaster);
Datum RASTER_fromGDALRaster(PG_FUNCTION_ARGS)
{
	bytea *bytea_data;
	uint8_t *data;
	int data_len = 0;
	VSILFILE *vsifp = NULL;
	GDALDatasetH hdsSrc;
	int srid = -1; /* -1 for NULL */

	rt_pgraster *pgraster = NULL;
	rt_raster raster;

	/* NULL if NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* get data */
	bytea_data = (bytea *) PG_GETARG_BYTEA_P(0);
	data = (uint8_t *) VARDATA(bytea_data);
	data_len = VARSIZE(bytea_data) - VARHDRSZ;

	/* process srid */
	/* NULL srid means try to determine SRID from bytea */
	if (!PG_ARGISNULL(1))
		srid = clamp_srid(PG_GETARG_INT32(1));

	/* create memory "file" */
	vsifp = VSIFileFromMemBuffer("/vsimem/in.dat", data, data_len, FALSE);
	if (vsifp == NULL) {
		PG_FREE_IF_COPY(bytea_data, 0);
		elog(ERROR, "RASTER_fromGDALRaster: Could not load bytea into memory file for use by GDAL");
		PG_RETURN_NULL();
	}

	/* register all GDAL drivers */
	rt_util_gdal_register_all(0);

	/* open GDAL raster */
	hdsSrc = rt_util_gdal_open("/vsimem/in.dat", GA_ReadOnly, 1);
	if (hdsSrc == NULL) {
		VSIFCloseL(vsifp);
		PG_FREE_IF_COPY(bytea_data, 0);
		elog(ERROR, "RASTER_fromGDALRaster: Could not open bytea with GDAL. Check that the bytea is of a GDAL supported format");
		PG_RETURN_NULL();
	}
	
#if POSTGIS_DEBUG_LEVEL > 3
	{
		GDALDriverH hdrv = GDALGetDatasetDriver(hdsSrc);

		POSTGIS_RT_DEBUGF(4, "Input GDAL Raster info: %s, (%d x %d)",
			GDALGetDriverShortName(hdrv),
			GDALGetRasterXSize(hdsSrc),
			GDALGetRasterYSize(hdsSrc)
		);
	}
#endif

	/* convert GDAL raster to raster */
	raster = rt_raster_from_gdal_dataset(hdsSrc);

	GDALClose(hdsSrc);
	VSIFCloseL(vsifp);
	PG_FREE_IF_COPY(bytea_data, 0);

	if (raster == NULL) {
		elog(ERROR, "RASTER_fromGDALRaster: Could not convert GDAL raster to raster");
		PG_RETURN_NULL();
	}

	/* apply SRID if set */
	if (srid != -1)
		rt_raster_set_srid(raster, srid);
 
	pgraster = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (!pgraster)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, pgraster->size);
	PG_RETURN_POINTER(pgraster);
}

/**
 * Returns formatted GDAL raster as bytea object of raster
 */
PG_FUNCTION_INFO_V1(RASTER_asGDALRaster);
Datum RASTER_asGDALRaster(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster;

	text *formattext = NULL;
	char *format = NULL;
	char **options = NULL;
	text *optiontext = NULL;
	char *option = NULL;
	int srid = SRID_UNKNOWN;
	char *srs = NULL;

	ArrayType *array;
	Oid etype;
	Datum *e;
	bool *nulls;
	int16 typlen;
	bool typbyval;
	char typalign;
	int n = 0;
	int i = 0;
	int j = 0;

	uint8_t *gdal = NULL;
	uint64_t gdal_size = 0;
	bytea *result = NULL;
	uint64_t result_size = 0;

	POSTGIS_RT_DEBUG(3, "RASTER_asGDALRaster: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_asGDALRaster: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* format is required */
	if (PG_ARGISNULL(1)) {
		elog(NOTICE, "Format must be provided");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}
	else {
		formattext = PG_GETARG_TEXT_P(1);
		format = text_to_cstring(formattext);
	}
		
	POSTGIS_RT_DEBUGF(3, "RASTER_asGDALRaster: Arg 1 (format) is %s", format);

	/* process options */
	if (!PG_ARGISNULL(2)) {
		POSTGIS_RT_DEBUG(3, "RASTER_asGDALRaster: Processing Arg 2 (options)");
		array = PG_GETARG_ARRAYTYPE_P(2);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case TEXTOID:
				break;
			default:
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_asGDALRaster: Invalid data type for options");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		if (n) {
			options = (char **) palloc(sizeof(char *) * (n + 1));
			if (options == NULL) {
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_asGDALRaster: Could not allocate memory for options");
				PG_RETURN_NULL();
			}

			/* clean each option */
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				option = NULL;
				switch (etype) {
					case TEXTOID:
						optiontext = (text *) DatumGetPointer(e[i]);
						if (NULL == optiontext) break;
						option = text_to_cstring(optiontext);

						/* trim string */
						option = rtpg_trim(option);
						POSTGIS_RT_DEBUGF(3, "RASTER_asGDALRaster: option is '%s'", option);
						break;
				}

				if (strlen(option)) {
					options[j] = (char *) palloc(sizeof(char) * (strlen(option) + 1));
					options[j] = option;
					j++;
				}
			}

			if (j > 0) {
				/* trim allocation */
				options = repalloc(options, (j + 1) * sizeof(char *));

				/* add NULL to end */
				options[j] = NULL;

			}
			else {
				pfree(options);
				options = NULL;
			}
		}
	}

	/* process srid */
	/* NULL srid means use raster's srid */
	if (PG_ARGISNULL(3))
		srid = rt_raster_get_srid(raster);
	else 
		srid = PG_GETARG_INT32(3);

	/* get srs from srid */
	if (clamp_srid(srid) != SRID_UNKNOWN) {
		srs = rtpg_getSR(srid);
		if (NULL == srs) {
			if (NULL != options) {
				for (i = j - 1; i >= 0; i--) pfree(options[i]);
				pfree(options);
			}
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_asGDALRaster: Could not find srtext for SRID (%d)", srid);
			PG_RETURN_NULL();
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_asGDALRaster: Arg 3 (srs) is %s", srs);
	}
	else
		srs = NULL;

	POSTGIS_RT_DEBUG(3, "RASTER_asGDALRaster: Generating GDAL raster");
	gdal = rt_raster_to_gdal(raster, srs, format, options, &gdal_size);

	/* free memory */
	if (NULL != options) {
		for (i = j - 1; i >= 0; i--) pfree(options[i]);
		pfree(options);
	}
	if (NULL != srs) pfree(srs);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	if (!gdal) {
		elog(ERROR, "RASTER_asGDALRaster: Could not allocate and generate GDAL raster");
		PG_RETURN_NULL();
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asGDALRaster: GDAL raster generated with %d bytes", (int) gdal_size);

	/* result is a varlena */
	result_size = gdal_size + VARHDRSZ;
	result = (bytea *) palloc(result_size);
	if (NULL == result) {
		elog(ERROR, "RASTER_asGDALRaster: Insufficient virtual memory for GDAL raster");
		PG_RETURN_NULL();
	}
	SET_VARSIZE(result, result_size);
	memcpy(VARDATA(result), gdal, VARSIZE(result) - VARHDRSZ);

	/* for test output
	FILE *fh = NULL;
	fh = fopen("/tmp/out.dat", "w");
	fwrite(gdal, sizeof(uint8_t), gdal_size, fh);
	fclose(fh);
	*/

	/* free gdal mem buffer */
	if (gdal) CPLFree(gdal);

	POSTGIS_RT_DEBUG(3, "RASTER_asGDALRaster: Returning pointer to GDAL raster");
	PG_RETURN_POINTER(result);
}

/**
 * Returns available GDAL drivers
 */
PG_FUNCTION_INFO_V1(RASTER_getGDALDrivers);
Datum RASTER_getGDALDrivers(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	uint32_t drv_count;
	rt_gdaldriver drv_set;
	rt_gdaldriver drv_set2;
	int call_cntr;
	int max_calls;

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		drv_set = rt_raster_gdal_drivers(&drv_count, 1);
		if (NULL == drv_set || !drv_count) {
			elog(NOTICE, "No GDAL drivers found");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		POSTGIS_RT_DEBUGF(3, "%d drivers returned", (int) drv_count);

		/* Store needed information */
		funcctx->user_fctx = drv_set;

		/* total number of tuples to be returned */
		funcctx->max_calls = drv_count;

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
	drv_set2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 4;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Int32GetDatum(drv_set2[call_cntr].idx);
		values[1] = CStringGetTextDatum(drv_set2[call_cntr].short_name);
		values[2] = CStringGetTextDatum(drv_set2[call_cntr].long_name);
		values[3] = CStringGetTextDatum(drv_set2[call_cntr].create_options);

		POSTGIS_RT_DEBUGF(4, "Result %d, Index %d", call_cntr, drv_set2[call_cntr].idx);
		POSTGIS_RT_DEBUGF(4, "Result %d, Short Name %s", call_cntr, drv_set2[call_cntr].short_name);
		POSTGIS_RT_DEBUGF(4, "Result %d, Full Name %s", call_cntr, drv_set2[call_cntr].long_name);
		POSTGIS_RT_DEBUGF(5, "Result %d, Create Options %s", call_cntr, drv_set2[call_cntr].create_options);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* clean up */
		pfree(drv_set2[call_cntr].short_name);
		pfree(drv_set2[call_cntr].long_name);
		pfree(drv_set2[call_cntr].create_options);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(drv_set2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * warp a raster using GDAL Warp API
 */
PG_FUNCTION_INFO_V1(RASTER_GDALWarp);
Datum RASTER_GDALWarp(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrast = NULL;
	rt_raster raster = NULL;
	rt_raster rast = NULL;

	text *algtext = NULL;
	char *algchar = NULL;
	GDALResampleAlg alg = GRA_NearestNeighbour;
	double max_err = 0.125;

	int src_srid = SRID_UNKNOWN;
	char *src_srs = NULL;
	int dst_srid = SRID_UNKNOWN;
	char *dst_srs = NULL;
	int no_srid = 0;

	double scale[2] = {0};
	double *scale_x = NULL;
	double *scale_y = NULL;

	double gridw[2] = {0};
	double *grid_xw = NULL;
	double *grid_yw = NULL;

	double skew[2] = {0};
	double *skew_x = NULL;
	double *skew_y = NULL;

	int dim[2] = {0};
	int *dim_x = NULL;
	int *dim_y = NULL;

	POSTGIS_RT_DEBUG(3, "RASTER_GDALWarp: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* raster */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_GDALWarp: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* resampling algorithm */
	if (!PG_ARGISNULL(1)) {
		algtext = PG_GETARG_TEXT_P(1);
		algchar = rtpg_trim(rtpg_strtoupper(text_to_cstring(algtext)));
		alg = rt_util_gdal_resample_alg(algchar);
	}
	POSTGIS_RT_DEBUGF(4, "Resampling algorithm: %d", alg);

	/* max error */
	if (!PG_ARGISNULL(2)) {
		max_err = PG_GETARG_FLOAT8(2);
		if (max_err < 0.) max_err = 0.;
	}
	POSTGIS_RT_DEBUGF(4, "max_err: %f", max_err);

	/* source SRID */
	src_srid = clamp_srid(rt_raster_get_srid(raster));
	POSTGIS_RT_DEBUGF(4, "source SRID: %d", src_srid);

	/* target SRID */
	if (!PG_ARGISNULL(3)) {
		dst_srid = clamp_srid(PG_GETARG_INT32(3));
		if (dst_srid == SRID_UNKNOWN) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_GDALWarp: %d is an invalid target SRID", dst_srid);
			PG_RETURN_NULL();
		}
	}
	else
		dst_srid = src_srid;
	POSTGIS_RT_DEBUGF(4, "destination SRID: %d", dst_srid);

	/* target SRID != src SRID, error */
	if (src_srid == SRID_UNKNOWN && dst_srid != src_srid) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_GDALWarp: Input raster has unknown (%d) SRID", src_srid);
		PG_RETURN_NULL();
	}
	/* target SRID == src SRID, no reprojection */
	else if (dst_srid == src_srid) {
		no_srid = 1;
	}

	/* scale x */
	if (!PG_ARGISNULL(4)) {
		scale[0] = PG_GETARG_FLOAT8(4);
		if (FLT_NEQ(scale[0], 0)) scale_x = &scale[0];
	}

	/* scale y */
	if (!PG_ARGISNULL(5)) {
		scale[1] = PG_GETARG_FLOAT8(5);
		if (FLT_NEQ(scale[1], 0)) scale_y = &scale[1];
	}

	/* grid alignment x */
	if (!PG_ARGISNULL(6)) {
		gridw[0] = PG_GETARG_FLOAT8(6);
		grid_xw = &gridw[0];
	}

	/* grid alignment y */
	if (!PG_ARGISNULL(7)) {
		gridw[1] = PG_GETARG_FLOAT8(7);
		grid_yw = &gridw[1];
	}

	/* skew x */
	if (!PG_ARGISNULL(8)) {
		skew[0] = PG_GETARG_FLOAT8(8);
		if (FLT_NEQ(skew[0], 0)) skew_x = &skew[0];
	}

	/* skew y */
	if (!PG_ARGISNULL(9)) {
		skew[1] = PG_GETARG_FLOAT8(9);
		if (FLT_NEQ(skew[1], 0)) skew_y = &skew[1];
	}

	/* width */
	if (!PG_ARGISNULL(10)) {
		dim[0] = PG_GETARG_INT32(10);
		if (dim[0] < 0) dim[0] = 0;
		if (dim[0] > 0) dim_x = &dim[0];
	}

	/* height */
	if (!PG_ARGISNULL(11)) {
		dim[1] = PG_GETARG_INT32(11);
		if (dim[1] < 0) dim[1] = 0;
		if (dim[1] > 0) dim_y = &dim[1];
	}

	/* check that at least something is to be done */
	if (
		(dst_srid == SRID_UNKNOWN) &&
		(scale_x == NULL) && (scale_y == NULL) &&
		(grid_xw == NULL) && (grid_yw == NULL) &&
		(skew_x == NULL) && (skew_y == NULL) &&
		(dim_x == NULL) && (dim_y == NULL)
	) {
		elog(NOTICE, "No resampling parameters provided.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}
	/* both values of alignment must be provided if any one is provided */
	else if (
		(grid_xw != NULL && grid_yw == NULL) ||
		(grid_xw == NULL && grid_yw != NULL)
	) {
		elog(NOTICE, "Values must be provided for both X and Y when specifying the alignment.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}
	/* both values of scale must be provided if any one is provided */
	else if (
		(scale_x != NULL && scale_y == NULL) ||
		(scale_x == NULL && scale_y != NULL)
	) {
		elog(NOTICE, "Values must be provided for both X and Y when specifying the scale.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}
	/* scale and width/height provided */
	else if (
		(scale_x != NULL || scale_y != NULL) &&
		(dim_x != NULL || dim_y != NULL)
	) {
		elog(NOTICE, "Scale X/Y and width/height are mutually exclusive.  Only provide one.  Returning original raster");
		rt_raster_destroy(raster);
		PG_RETURN_POINTER(pgraster);
	}

	/* get srses from srids */
	if (!no_srid) {
		/* source srs */
		src_srs = rtpg_getSR(src_srid);
		if (NULL == src_srs) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_GDALWarp: Input raster has unknown SRID (%d)", src_srid);
			PG_RETURN_NULL();
		}
		POSTGIS_RT_DEBUGF(4, "src srs: %s", src_srs);

		dst_srs = rtpg_getSR(dst_srid);
		if (NULL == dst_srs) {
			if (!no_srid) pfree(src_srs);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_GDALWarp: Target SRID (%d) is unknown", dst_srid);
			PG_RETURN_NULL();
		}
		POSTGIS_RT_DEBUGF(4, "dst srs: %s", dst_srs);
	}

	rast = rt_raster_gdal_warp(
		raster,
		src_srs, dst_srs,
		scale_x, scale_y,
		dim_x, dim_y,
		NULL, NULL,
		grid_xw, grid_yw,
		skew_x, skew_y,
		alg, max_err);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!no_srid) {
		pfree(src_srs);
		pfree(dst_srs);
	}
	if (!rast) {
		elog(ERROR, "RASTER_band: Could not create transformed raster");
		PG_RETURN_NULL();
	}

	/* add target SRID */
	rt_raster_set_srid(rast, dst_srid);

	pgrast = rt_raster_serialize(rast);
	rt_raster_destroy(rast);

	if (NULL == pgrast) PG_RETURN_NULL();

	POSTGIS_RT_DEBUG(3, "RASTER_GDALWarp: done");

	SET_VARSIZE(pgrast, pgrast->size);
	PG_RETURN_POINTER(pgrast);
}

