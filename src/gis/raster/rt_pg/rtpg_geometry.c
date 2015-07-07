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
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */
#include <utils/array.h> /* for ArrayType */
#include <catalog/pg_type.h> /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */
#include <utils/builtins.h> /* for text_to_cstring() */

#include "../../postgis_config.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "lwgeom_pg.h"

#include "rtpostgis.h"
#include "rtpg_internal.h"

Datum RASTER_envelope(PG_FUNCTION_ARGS);
Datum RASTER_convex_hull(PG_FUNCTION_ARGS);
Datum RASTER_dumpAsPolygons(PG_FUNCTION_ARGS);

/* Get pixel geographical shape */
Datum RASTER_getPixelPolygons(PG_FUNCTION_ARGS);

/* Get raster band's polygon */
Datum RASTER_getPolygon(PG_FUNCTION_ARGS);

/* rasterize a geometry */
Datum RASTER_asRaster(PG_FUNCTION_ARGS);

/* ---------------------------------------------------------------- */
/*  Raster envelope                                                 */
/* ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(RASTER_envelope);
Datum RASTER_envelope(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster;
	rt_raster raster;
	LWGEOM *geom = NULL;
	GSERIALIZED* gser = NULL;
	size_t gser_size;
	int err = ES_NONE;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(
		PG_GETARG_DATUM(0),
		0,
		sizeof(struct rt_raster_serialized_t)
	);
	raster = rt_raster_deserialize(pgraster, TRUE);

	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_envelope: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	err = rt_raster_get_envelope_geom(raster, &geom);

	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	if (err != ES_NONE) {
		elog(ERROR, "RASTER_envelope: Could not get raster's envelope");
		PG_RETURN_NULL();
	}
	else if (geom == NULL) {
		elog(NOTICE, "Raster's envelope is NULL");
		PG_RETURN_NULL();
	}

	gser = gserialized_from_lwgeom(geom, 0, &gser_size);
	lwgeom_free(geom);

	SET_VARSIZE(gser, gser_size);
	PG_RETURN_POINTER(gser);
}

/**
 * Return the convex hull of this raster
 */
/* ---------------------------------------------------------------- */
/*  Raster convex hull                                              */
/* ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(RASTER_convex_hull);
Datum RASTER_convex_hull(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster;
	rt_raster raster;
	LWGEOM *geom = NULL;
	GSERIALIZED* gser = NULL;
	size_t gser_size;
	int err = ES_NONE;

	bool minhull = FALSE;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* # of args */
	if (PG_NARGS() > 1)
		minhull = TRUE;

	if (!minhull) {
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0, sizeof(struct rt_raster_serialized_t));
		raster = rt_raster_deserialize(pgraster, TRUE);
	}
	else {
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
		raster = rt_raster_deserialize(pgraster, FALSE);
	}

	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_convex_hull: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	if (!minhull)
		err = rt_raster_get_convex_hull(raster, &geom);
	else {
		int nband = -1;

		/* get arg 1 */
		if (!PG_ARGISNULL(1)) {
			nband = PG_GETARG_INT32(1);
			if (!rt_raster_has_band(raster, nband - 1)) {
				elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				PG_RETURN_NULL();
			}
			nband = nband - 1;
		}

		err = rt_raster_get_perimeter(raster, nband, &geom);
	}

	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	if (err != ES_NONE) {
		elog(ERROR, "RASTER_convex_hull: Could not get raster's convex hull");
		PG_RETURN_NULL();
	}
	else if (geom == NULL) {
		elog(NOTICE, "Raster's convex hull is NULL");
		PG_RETURN_NULL();
	}

	gser = gserialized_from_lwgeom(geom, 0, &gser_size);
	lwgeom_free(geom);

	SET_VARSIZE(gser, gser_size);
	PG_RETURN_POINTER(gser);
}

PG_FUNCTION_INFO_V1(RASTER_dumpAsPolygons);
Datum RASTER_dumpAsPolygons(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	rt_geomval geomval;
	rt_geomval geomval2;
	int call_cntr;
	int max_calls;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		int numbands;
		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		int nband;
		bool exclude_nodata_value = TRUE;
		int nElements;

		POSTGIS_RT_DEBUG(2, "RASTER_dumpAsPolygons first call");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Get input arguments */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
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

		if (!PG_ARGISNULL(1))
			nband = PG_GETARG_UINT32(1);
		else
			nband = 1; /* By default, first band */

		POSTGIS_RT_DEBUGF(3, "band %d", nband);
		numbands = rt_raster_get_num_bands(raster);

		if (nband < 1 || nband > numbands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		if (!PG_ARGISNULL(2))
			exclude_nodata_value = PG_GETARG_BOOL(2);

		/* see if band is NODATA */
		if (rt_band_get_isnodata_flag(rt_raster_get_band(raster, nband - 1))) {
			POSTGIS_RT_DEBUGF(3, "Band at index %d is NODATA. Returning NULL", nband);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* Polygonize raster */

		/**
		 * Dump raster
		 */
		geomval = rt_raster_gdal_polygonize(raster, nband - 1, exclude_nodata_value, &nElements);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (NULL == geomval) {
			ereport(ERROR, (
				errcode(ERRCODE_NO_DATA_FOUND),
				errmsg("Could not polygonize raster")
			));
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		POSTGIS_RT_DEBUGF(3, "raster dump, %d elements returned", nElements);

		/* Store needed information */
		funcctx->user_fctx = geomval;

		/* total number of tuples to be returned */
		funcctx->max_calls = nElements;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("function returning record called in context that cannot accept type record")
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
	geomval2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 2;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple    tuple;
		Datum        result;

		GSERIALIZED *gser = NULL;
		size_t gser_size = 0;

		POSTGIS_RT_DEBUGF(3, "call number %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		/* convert LWGEOM to GSERIALIZED */
		gser = gserialized_from_lwgeom(lwpoly_as_lwgeom(geomval2[call_cntr].geom), 0, &gser_size);
		lwgeom_free(lwpoly_as_lwgeom(geomval2[call_cntr].geom));

		values[0] = PointerGetDatum(gser);
		values[1] = Float8GetDatum(geomval2[call_cntr].val);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(geomval2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Return the geographical shape of all pixels
 */
PG_FUNCTION_INFO_V1(RASTER_getPixelPolygons);
Datum RASTER_getPixelPolygons(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	rt_pixel pix = NULL;
	rt_pixel pix2;
	int call_cntr;
	int max_calls;
	int i = 0;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int nband = 1;
		int numbands;
		bool hasband = TRUE;
		bool exclude_nodata_value = TRUE;
		bool nocolumnx = FALSE;
		bool norowy = FALSE;
		int x = 0;
		int y = 0;
		int bounds[4] = {0};
		int pixcount = 0;
		double value = 0;
		int isnodata = 0;

		LWPOLY *poly;

		POSTGIS_RT_DEBUG(3, "RASTER_getPixelPolygons first call");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		/* band */
		if (PG_ARGISNULL(1))
			hasband = FALSE;
		else {
			nband = PG_GETARG_INT32(1);
			hasband = TRUE;
		}

		/* column */
		if (PG_ARGISNULL(2))
			nocolumnx = TRUE;
		else {
			bounds[0] = PG_GETARG_INT32(2);
			bounds[1] = bounds[0];
		}

		/* row */
		if (PG_ARGISNULL(3))
			norowy = TRUE;
		else {
			bounds[2] = PG_GETARG_INT32(3);
			bounds[3] = bounds[2];
		}

		/* exclude NODATA */
		if (!PG_ARGISNULL(4))
			exclude_nodata_value = PG_GETARG_BOOL(4);

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

		/* raster empty, return NULL */
		if (rt_raster_is_empty(raster)) {
			elog(NOTICE, "Raster is empty. Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* band specified, load band and info */
		if (hasband) {
			numbands = rt_raster_get_num_bands(raster);
			POSTGIS_RT_DEBUGF(3, "band %d", nband);
			POSTGIS_RT_DEBUGF(3, "# of bands %d", numbands);

			if (nband < 1 || nband > numbands) {
				elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			band = rt_raster_get_band(raster, nband - 1);
			if (!band) {
				elog(NOTICE, "Could not find band at index %d. Returning NULL", nband);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			if (!rt_band_get_hasnodata_flag(band))
				exclude_nodata_value = FALSE;
		}

		/* set bounds if columnx, rowy not set */
		if (nocolumnx) {
			bounds[0] = 1;
			bounds[1] = rt_raster_get_width(raster);
		}
		if (norowy) {
			bounds[2] = 1;
			bounds[3] = rt_raster_get_height(raster);
		}
		POSTGIS_RT_DEBUGF(3, "bounds (min x, max x, min y, max y) = (%d, %d, %d, %d)", 
			bounds[0], bounds[1], bounds[2], bounds[3]);

		/* rowy */
		pixcount = 0;
		for (y = bounds[2]; y <= bounds[3]; y++) {
			/* columnx */
			for (x = bounds[0]; x <= bounds[1]; x++) {

				value = 0;
				isnodata = TRUE;

				if (hasband) {
					if (rt_band_get_pixel(band, x - 1, y - 1, &value, &isnodata) != ES_NONE) {

						for (i = 0; i < pixcount; i++)
							lwgeom_free(pix[i].geom);
						if (pixcount) pfree(pix);

						rt_band_destroy(band);
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 0);

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_getPixelPolygons: Could not get pixel value");
						SRF_RETURN_DONE(funcctx);
					}

					/* don't continue if pixel is NODATA and to exclude NODATA */
					if (isnodata && exclude_nodata_value) {
						POSTGIS_RT_DEBUG(5, "pixel value is NODATA and exclude_nodata_value = TRUE");
						continue;
					}
				}

				/* geometry */
				poly = rt_raster_pixel_as_polygon(raster, x - 1, y - 1);
				if (!poly) {
					for (i = 0; i < pixcount; i++)
						lwgeom_free(pix[i].geom);
					if (pixcount) pfree(pix);

					if (hasband) rt_band_destroy(band);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_getPixelPolygons: Could not get pixel polygon");
					SRF_RETURN_DONE(funcctx);
				}

				if (!pixcount)
					pix = palloc(sizeof(struct rt_pixel_t) * (pixcount + 1));
				else
					pix = repalloc(pix, sizeof(struct rt_pixel_t) * (pixcount + 1));
				if (pix == NULL) {

					lwpoly_free(poly);
					if (hasband) rt_band_destroy(band);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_getPixelPolygons: Could not allocate memory for storing pixel polygons");
					SRF_RETURN_DONE(funcctx);
				}
				pix[pixcount].geom = (LWGEOM *) poly;
				POSTGIS_RT_DEBUGF(5, "poly @ %p", poly);
				POSTGIS_RT_DEBUGF(5, "geom @ %p", pix[pixcount].geom);

				/* x, y */
				pix[pixcount].x = x;
				pix[pixcount].y = y;

				/* value */
				pix[pixcount].value = value;

				/* NODATA */
				if (hasband) {
					if (exclude_nodata_value)
						pix[pixcount].nodata = isnodata;
					else
						pix[pixcount].nodata = FALSE;
				}
				else {
					pix[pixcount].nodata = isnodata;
				}

				pixcount++;
			}
		}

		if (hasband) rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);

		/* shortcut if no pixcount */
		if (pixcount < 1) {
			elog(NOTICE, "No pixels found for band %d", nband);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* Store needed information */
		funcctx->user_fctx = pix;

		/* total number of tuples to be returned */
		funcctx->max_calls = pixcount;
		POSTGIS_RT_DEBUGF(3, "pixcount = %d", pixcount);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("function returning record called in context that cannot accept type record")
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
	pix2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 4;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		GSERIALIZED *gser = NULL;
		size_t gser_size = 0;

		POSTGIS_RT_DEBUGF(3, "call number %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		/* convert LWGEOM to GSERIALIZED */
		gser = gserialized_from_lwgeom(pix2[call_cntr].geom, 0, &gser_size);
		lwgeom_free(pix2[call_cntr].geom);

		values[0] = PointerGetDatum(gser);
		if (pix2[call_cntr].nodata)
			nulls[1] = TRUE;
		else
			values[1] = Float8GetDatum(pix2[call_cntr].value);
		values[2] = Int32GetDatum(pix2[call_cntr].x);
		values[3] = Int32GetDatum(pix2[call_cntr].y);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(pix2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Get raster band's polygon
 */
PG_FUNCTION_INFO_V1(RASTER_getPolygon);
Datum RASTER_getPolygon(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	int num_bands = 0;
	int nband = 1;
	int err;
	LWMPOLY *surface = NULL;
	GSERIALIZED *rtn = NULL;

	/* raster */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_getPolygon: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* num_bands */
	num_bands = rt_raster_get_num_bands(raster);
	if (num_bands < 1) {
		elog(NOTICE, "Raster provided has no bands");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* band index is 1-based */
	if (!PG_ARGISNULL(1))
		nband = PG_GETARG_INT32(1);
	if (nband < 1 || nband > num_bands) {
		elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* get band surface */
	err = rt_raster_surface(raster, nband - 1, &surface);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	if (err != ES_NONE) {
		elog(ERROR, "RASTER_getPolygon: Could not get raster band's surface");
		PG_RETURN_NULL();
	}
	else if (surface == NULL) {
		elog(NOTICE, "Raster is empty or all pixels of band are NODATA. Returning NULL");
		PG_RETURN_NULL();
	}

	rtn = geometry_serialize(lwmpoly_as_lwgeom(surface));
	lwmpoly_free(surface);

	PG_RETURN_POINTER(rtn);
}

/**
 * Rasterize a geometry
 */
PG_FUNCTION_INFO_V1(RASTER_asRaster);
Datum RASTER_asRaster(PG_FUNCTION_ARGS)
{
	GSERIALIZED *gser = NULL;

	LWGEOM *geom = NULL;
	rt_raster rast = NULL;
	rt_pgraster *pgrast = NULL;

	unsigned char *wkb;
	size_t wkb_len = 0;
	unsigned char variant = WKB_SFSQL;

	double scale[2] = {0};
	double *scale_x = NULL;
	double *scale_y = NULL;

	int dim[2] = {0};
	int *dim_x = NULL;
	int *dim_y = NULL;

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
	int haserr = 0;

	text *pixeltypetext = NULL;
	char *pixeltype = NULL;
	rt_pixtype pixtype = PT_END;
	rt_pixtype *pixtypes = NULL;
	int pixtypes_len = 0;

	double *values = NULL;
	int values_len = 0;

	uint8_t *hasnodatas = NULL;
	double *nodatavals = NULL;
	int nodatavals_len = 0;

	double ulw[2] = {0};
	double *ul_xw = NULL;
	double *ul_yw = NULL;

	double gridw[2] = {0};
	double *grid_xw = NULL;
	double *grid_yw = NULL;

	double skew[2] = {0};
	double *skew_x = NULL;
	double *skew_y = NULL;

	char **options = NULL;
	int options_len = 0;

	uint32_t num_bands = 0;

	int srid = SRID_UNKNOWN;
	char *srs = NULL;

	POSTGIS_RT_DEBUG(3, "RASTER_asRaster: Starting");

	/* based upon LWGEOM_asBinary function in postgis/lwgeom_ogc.c */

	/* Get the geometry */
	if (PG_ARGISNULL(0)) 
		PG_RETURN_NULL();

	gser = PG_GETARG_GSERIALIZED_P(0);
	geom = lwgeom_from_gserialized(gser);

	/* Get a 2D version of the geometry if necessary */
	if (lwgeom_ndims(geom) > 2) {
		LWGEOM *geom2d = lwgeom_force_2d(geom);
		lwgeom_free(geom);
		geom = geom2d;
	}

	/* empty geometry, return empty raster */
	if (lwgeom_is_empty(geom)) {
		POSTGIS_RT_DEBUG(3, "Input geometry is empty. Returning empty raster");
		lwgeom_free(geom);
		PG_FREE_IF_COPY(gser, 0);

		rast = rt_raster_new(0, 0);
		if (rast == NULL)
			PG_RETURN_NULL();

		pgrast = rt_raster_serialize(rast);
		rt_raster_destroy(rast);

		if (NULL == pgrast)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrast, pgrast->size);
		PG_RETURN_POINTER(pgrast);
	}

	/* scale x */
	if (!PG_ARGISNULL(1)) {
		scale[0] = PG_GETARG_FLOAT8(1);
		if (FLT_NEQ(scale[0], 0)) scale_x = &scale[0];
	}

	/* scale y */
	if (!PG_ARGISNULL(2)) {
		scale[1] = PG_GETARG_FLOAT8(2);
		if (FLT_NEQ(scale[1], 0)) scale_y = &scale[1];
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: scale (x, y) = %f, %f", scale[0], scale[1]);

	/* width */
	if (!PG_ARGISNULL(3)) {
		dim[0] = PG_GETARG_INT32(3);
		if (dim[0] < 0) dim[0] = 0;
		if (dim[0] != 0) dim_x = &dim[0];
	}

	/* height */
	if (!PG_ARGISNULL(4)) {
		dim[1] = PG_GETARG_INT32(4);
		if (dim[1] < 0) dim[1] = 0;
		if (dim[1] != 0) dim_y = &dim[1];
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: dim (x, y) = %d, %d", dim[0], dim[1]);

	/* pixeltype */
	if (!PG_ARGISNULL(5)) {
		array = PG_GETARG_ARRAYTYPE_P(5);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case TEXTOID:
				break;
			default:

				lwgeom_free(geom);
				PG_FREE_IF_COPY(gser, 0);

				elog(ERROR, "RASTER_asRaster: Invalid data type for pixeltype");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		if (n) {
			pixtypes = (rt_pixtype *) palloc(sizeof(rt_pixtype) * n);
			/* clean each pixeltype */
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) {
					pixtypes[j++] = PT_64BF;
					continue;
				}

				pixeltype = NULL;
				switch (etype) {
					case TEXTOID:
						pixeltypetext = (text *) DatumGetPointer(e[i]);
						if (NULL == pixeltypetext) break;
						pixeltype = text_to_cstring(pixeltypetext);

						/* trim string */
						pixeltype = rtpg_trim(pixeltype);
						POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: pixeltype is '%s'", pixeltype);
						break;
				}

				if (strlen(pixeltype)) {
					pixtype = rt_pixtype_index_from_name(pixeltype);
					if (pixtype == PT_END) {

						pfree(pixtypes);

						lwgeom_free(geom);
						PG_FREE_IF_COPY(gser, 0);

						elog(ERROR, "RASTER_asRaster: Invalid pixel type provided: %s", pixeltype);
						PG_RETURN_NULL();
					}

					pixtypes[j] = pixtype;
					j++;
				}
			}

			if (j > 0) {
				/* trim allocation */
				pixtypes = repalloc(pixtypes, j * sizeof(rt_pixtype));
				pixtypes_len = j;
			}
			else {
				pfree(pixtypes);
				pixtypes = NULL;
				pixtypes_len = 0;
			}
		}
	}
#if POSTGIS_DEBUG_LEVEL > 0
	for (i = 0; i < pixtypes_len; i++)
		POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: pixtypes[%d] = %d", i, (int) pixtypes[i]);
#endif

	/* value */
	if (!PG_ARGISNULL(6)) {
		array = PG_GETARG_ARRAYTYPE_P(6);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case FLOAT4OID:
			case FLOAT8OID:
				break;
			default:

				if (pixtypes_len) pfree(pixtypes);

				lwgeom_free(geom);
				PG_FREE_IF_COPY(gser, 0);

				elog(ERROR, "RASTER_asRaster: Invalid data type for value");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		if (n) {
			values = (double *) palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) {
					values[j++] = 1;
					continue;
				}

				switch (etype) {
					case FLOAT4OID:
						values[j] = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						values[j] = (double) DatumGetFloat8(e[i]);
						break;
				}
				POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: values[%d] = %f", j, values[j]);

				j++;
			}

			if (j > 0) {
				/* trim allocation */
				values = repalloc(values, j * sizeof(double));
				values_len = j;
			}
			else {
				pfree(values);
				values = NULL;
				values_len = 0;
			}
		}
	}
#if POSTGIS_DEBUG_LEVEL > 0
	for (i = 0; i < values_len; i++)
		POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: values[%d] = %f", i, values[i]);
#endif

	/* nodataval */
	if (!PG_ARGISNULL(7)) {
		array = PG_GETARG_ARRAYTYPE_P(7);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case FLOAT4OID:
			case FLOAT8OID:
				break;
			default:

				if (pixtypes_len) pfree(pixtypes);
				if (values_len) pfree(values);

				lwgeom_free(geom);
				PG_FREE_IF_COPY(gser, 0);

				elog(ERROR, "RASTER_asRaster: Invalid data type for nodataval");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
			&nulls, &n);

		if (n) {
			nodatavals = (double *) palloc(sizeof(double) * n);
			hasnodatas = (uint8_t *) palloc(sizeof(uint8_t) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) {
					hasnodatas[j] = 0;
					nodatavals[j] = 0;
					j++;
					continue;
				}

				hasnodatas[j] = 1;
				switch (etype) {
					case FLOAT4OID:
						nodatavals[j] = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						nodatavals[j] = (double) DatumGetFloat8(e[i]);
						break;
				}
				POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: hasnodatas[%d] = %d", j, hasnodatas[j]);
				POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: nodatavals[%d] = %f", j, nodatavals[j]);

				j++;
			}

			if (j > 0) {
				/* trim allocation */
				nodatavals = repalloc(nodatavals, j * sizeof(double));
				hasnodatas = repalloc(hasnodatas, j * sizeof(uint8_t));
				nodatavals_len = j;
			}
			else {
				pfree(nodatavals);
				pfree(hasnodatas);
				nodatavals = NULL;
				hasnodatas = NULL;
				nodatavals_len = 0;
			}
		}
	}
#if POSTGIS_DEBUG_LEVEL > 0
	for (i = 0; i < nodatavals_len; i++) {
		POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: hasnodatas[%d] = %d", i, hasnodatas[i]);
		POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: nodatavals[%d] = %f", i, nodatavals[i]);
	}
#endif

	/* upperleftx */
	if (!PG_ARGISNULL(8)) {
		ulw[0] = PG_GETARG_FLOAT8(8);
		ul_xw = &ulw[0];
	}

	/* upperlefty */
	if (!PG_ARGISNULL(9)) {
		ulw[1] = PG_GETARG_FLOAT8(9);
		ul_yw = &ulw[1];
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: upperleft (x, y) = %f, %f", ulw[0], ulw[1]);

	/* gridx */
	if (!PG_ARGISNULL(10)) {
		gridw[0] = PG_GETARG_FLOAT8(10);
		grid_xw = &gridw[0];
	}

	/* gridy */
	if (!PG_ARGISNULL(11)) {
		gridw[1] = PG_GETARG_FLOAT8(11);
		grid_yw = &gridw[1];
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: grid (x, y) = %f, %f", gridw[0], gridw[1]);

	/* check dependent variables */
	haserr = 0;
	do {
		/* only part of scale provided */
		if (
			(scale_x == NULL && scale_y != NULL) ||
			(scale_x != NULL && scale_y == NULL)
		) {
			elog(NOTICE, "Values must be provided for both X and Y of scale if one is specified");
			haserr = 1;
			break;
		}

		/* only part of dimension provided */
		if (
			(dim_x == NULL && dim_y != NULL) ||
			(dim_x != NULL && dim_y == NULL)
		) {
			elog(NOTICE, "Values must be provided for both width and height if one is specified");
			haserr = 1;
			break;
		}

		/* scale and dimension provided */
		if (
			(scale_x != NULL && scale_y != NULL) &&
			(dim_x != NULL && dim_y != NULL)
		) {
			elog(NOTICE, "Values provided for X and Y of scale and width and height.  Using the width and height");
			scale_x = NULL;
			scale_y = NULL;
			break;
		}

		/* neither scale or dimension provided */
		if (
			(scale_x == NULL && scale_y == NULL) &&
			(dim_x == NULL && dim_y == NULL)
		) {
			elog(NOTICE, "Values must be provided for X and Y of scale or width and height");
			haserr = 1;
			break;
		}

		/* only part of upper-left provided */
		if (
			(ul_xw == NULL && ul_yw != NULL) ||
			(ul_xw != NULL && ul_yw == NULL)
		) {
			elog(NOTICE, "Values must be provided for both X and Y when specifying the upper-left corner");
			haserr = 1;
			break;
		}

		/* only part of alignment provided */
		if (
			(grid_xw == NULL && grid_yw != NULL) ||
			(grid_xw != NULL && grid_yw == NULL)
		) {
			elog(NOTICE, "Values must be provided for both X and Y when specifying the alignment");
			haserr = 1;
			break;
		}

		/* upper-left and alignment provided */
		if (
			(ul_xw != NULL && ul_yw != NULL) &&
			(grid_xw != NULL && grid_yw != NULL)
		) {
			elog(NOTICE, "Values provided for both X and Y of upper-left corner and alignment.  Using the values of upper-left corner");
			grid_xw = NULL;
			grid_yw = NULL;
			break;
		}
	}
	while (0);

	if (haserr) {
		if (pixtypes_len) pfree(pixtypes);
		if (values_len) pfree(values);
		if (nodatavals_len) {
			pfree(nodatavals);
			pfree(hasnodatas);
		}

		lwgeom_free(geom);
		PG_FREE_IF_COPY(gser, 0);

		PG_RETURN_NULL();
	}

	/* skewx */
	if (!PG_ARGISNULL(12)) {
		skew[0] = PG_GETARG_FLOAT8(12);
		if (FLT_NEQ(skew[0], 0)) skew_x = &skew[0];
	}

	/* skewy */
	if (!PG_ARGISNULL(13)) {
		skew[1] = PG_GETARG_FLOAT8(13);
		if (FLT_NEQ(skew[1], 0)) skew_y = &skew[1];
	}
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: skew (x, y) = %f, %f", skew[0], skew[1]);

	/* all touched */
	if (!PG_ARGISNULL(14) && PG_GETARG_BOOL(14) == TRUE) {
		if (options_len < 1) {
			options_len = 1;
			options = (char **) palloc(sizeof(char *) * options_len);
		}
		else {
			options_len++;
			options = (char **) repalloc(options, sizeof(char *) * options_len);
		}

		options[options_len - 1] = palloc(sizeof(char*) * (strlen("ALL_TOUCHED=TRUE") + 1));
		options[options_len - 1] = "ALL_TOUCHED=TRUE";
	}

	if (options_len) {
		options_len++;
		options = (char **) repalloc(options, sizeof(char *) * options_len);
		options[options_len - 1] = NULL;
	}

	/* get geometry's srid */
	srid = gserialized_get_srid(gser);

	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: srid = %d", srid);
	if (clamp_srid(srid) != SRID_UNKNOWN) {
		srs = rtpg_getSR(srid);
		if (NULL == srs) {

			if (pixtypes_len) pfree(pixtypes);
			if (values_len) pfree(values);
			if (nodatavals_len) {
				pfree(hasnodatas);
				pfree(nodatavals);
			}
			if (options_len) pfree(options);

			lwgeom_free(geom);
			PG_FREE_IF_COPY(gser, 0);

			elog(ERROR, "RASTER_asRaster: Could not find srtext for SRID (%d)", srid);
			PG_RETURN_NULL();
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: srs is %s", srs);
	}
	else
		srs = NULL;

	/* determine number of bands */
	/* MIN macro is from GDAL's cpl_port.h */
	num_bands = MIN(pixtypes_len, values_len);
	num_bands = MIN(num_bands, nodatavals_len);
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: pixtypes_len = %d", pixtypes_len);
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: values_len = %d", values_len);
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: nodatavals_len = %d", nodatavals_len);
	POSTGIS_RT_DEBUGF(3, "RASTER_asRaster: num_bands = %d", num_bands);

	/* warn of imbalanced number of band elements */
	if (!(
		(pixtypes_len == values_len) &&
		(values_len == nodatavals_len)
	)) {
		elog(
			NOTICE,
			"Imbalanced number of values provided for pixeltype (%d), value (%d) and nodataval (%d).  Using the first %d values of each parameter",
			pixtypes_len,
			values_len,
			nodatavals_len,
			num_bands
		);
	}

	/* get wkb of geometry */
	POSTGIS_RT_DEBUG(3, "RASTER_asRaster: getting wkb of geometry");
	wkb = lwgeom_to_wkb(geom, variant, &wkb_len);
	lwgeom_free(geom);
	PG_FREE_IF_COPY(gser, 0);

	/* rasterize geometry */
	POSTGIS_RT_DEBUG(3, "RASTER_asRaster: rasterizing geometry");
	/* use nodatavals for the init parameter */
	rast = rt_raster_gdal_rasterize(wkb,
		(uint32_t) wkb_len, srs,
		num_bands, pixtypes,
		nodatavals, values,
		nodatavals, hasnodatas,
		dim_x, dim_y,
		scale_x, scale_y,
		ul_xw, ul_yw,
		grid_xw, grid_yw,
		skew_x, skew_y,
		options
	);

	if (pixtypes_len) pfree(pixtypes);
	if (values_len) pfree(values);
	if (nodatavals_len) {
		pfree(hasnodatas);
		pfree(nodatavals);
	}
	if (options_len) pfree(options);

	if (!rast) {
		elog(ERROR, "RASTER_asRaster: Could not rasterize geometry");
		PG_RETURN_NULL();
	}

	/* add target srid */
	rt_raster_set_srid(rast, srid);

	pgrast = rt_raster_serialize(rast);
	rt_raster_destroy(rast);

	if (NULL == pgrast) PG_RETURN_NULL();

	POSTGIS_RT_DEBUG(3, "RASTER_asRaster: done");

	SET_VARSIZE(pgrast, pgrast->size);
	PG_RETURN_POINTER(pgrast);
}
