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

#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h> /* for text_to_cstring() */
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */
#include "utils/array.h" /* for ArrayType */
#include "catalog/pg_type.h" /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */
#include <executor/spi.h>
#include <funcapi.h> /* for SRF */

#include "../../postgis_config.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h" /* for heap_form_tuple() */
#endif

#include "rtpostgis.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

/* Get summary stats */
Datum RASTER_summaryStats(PG_FUNCTION_ARGS);
Datum RASTER_summaryStatsCoverage(PG_FUNCTION_ARGS);

Datum RASTER_summaryStats_transfn(PG_FUNCTION_ARGS);
Datum RASTER_summaryStats_finalfn(PG_FUNCTION_ARGS);

/* get histogram */
Datum RASTER_histogram(PG_FUNCTION_ARGS);
Datum RASTER_histogramCoverage(PG_FUNCTION_ARGS);

/* get quantiles */
Datum RASTER_quantile(PG_FUNCTION_ARGS);
Datum RASTER_quantileCoverage(PG_FUNCTION_ARGS);

/* get counts of values */
Datum RASTER_valueCount(PG_FUNCTION_ARGS);
Datum RASTER_valueCountCoverage(PG_FUNCTION_ARGS);

/**
 * Get summary stats of a band
 */
PG_FUNCTION_INFO_V1(RASTER_summaryStats);
Datum RASTER_summaryStats(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int32_t bandindex = 1;
	bool exclude_nodata_value = TRUE;
	int num_bands = 0;
	double sample = 0;
	rt_bandstats stats = NULL;

	TupleDesc tupdesc;
	int values_length = 6;
	Datum values[values_length];
	bool nulls[values_length];
	HeapTuple tuple;
	Datum result;

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_summaryStats: Cannot deserialize raster");
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

	/* exclude_nodata_value flag */
	if (!PG_ARGISNULL(2))
		exclude_nodata_value = PG_GETARG_BOOL(2);

	/* sample % */
	if (!PG_ARGISNULL(3)) {
		sample = PG_GETARG_FLOAT8(3);
		if (sample < 0 || sample > 1) {
			elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			PG_RETURN_NULL();
		}
		else if (FLT_EQ(sample, 0.0))
			sample = 1;
	}
	else
		sample = 1;

	/* get band */
	band = rt_raster_get_band(raster, bandindex - 1);
	if (!band) {
		elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		PG_RETURN_NULL();
	}

	/* we don't need the raw values, hence the zero parameter */
	stats = rt_band_get_summary_stats(band, (int) exclude_nodata_value, sample, 0, NULL, NULL, NULL, NULL);
	rt_band_destroy(band);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (NULL == stats) {
		elog(NOTICE, "Cannot compute summary statistics for band at index %d. Returning NULL", bandindex);
		PG_RETURN_NULL();
	}

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

	memset(nulls, FALSE, sizeof(bool) * values_length);

	values[0] = Int64GetDatum(stats->count);
	if (stats->count > 0) {
		values[1] = Float8GetDatum(stats->sum);
		values[2] = Float8GetDatum(stats->mean);
		values[3] = Float8GetDatum(stats->stddev);
		values[4] = Float8GetDatum(stats->min);
		values[5] = Float8GetDatum(stats->max);
	}
	else {
		nulls[1] = TRUE;
		nulls[2] = TRUE;
		nulls[3] = TRUE;
		nulls[4] = TRUE;
		nulls[5] = TRUE;
	}

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	/* clean up */
	pfree(stats);

	PG_RETURN_DATUM(result);
}

/**
 * Get summary stats of a coverage for a specific band
 */
PG_FUNCTION_INFO_V1(RASTER_summaryStatsCoverage);
Datum RASTER_summaryStatsCoverage(PG_FUNCTION_ARGS)
{
	text *tablenametext = NULL;
	char *tablename = NULL;
	text *colnametext = NULL;
	char *colname = NULL;
	int32_t bandindex = 1;
	bool exclude_nodata_value = TRUE;
	double sample = 0;

	int len = 0;
	char *sql = NULL;
	int spi_result;
	Portal portal;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple;
	Datum datum;
	bool isNull = FALSE;

	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int num_bands = 0;
	uint64_t cK = 0;
	double cM = 0;
	double cQ = 0;
	rt_bandstats stats = NULL;
	rt_bandstats rtn = NULL;

	int values_length = 6;
	Datum values[values_length];
	bool nulls[values_length];
	Datum result;

	/* tablename is null, return null */
	if (PG_ARGISNULL(0)) {
		elog(NOTICE, "Table name must be provided");
		PG_RETURN_NULL();
	}
	tablenametext = PG_GETARG_TEXT_P(0);
	tablename = text_to_cstring(tablenametext);
	if (!strlen(tablename)) {
		elog(NOTICE, "Table name must be provided");
		PG_RETURN_NULL();
	}

	/* column name is null, return null */
	if (PG_ARGISNULL(1)) {
		elog(NOTICE, "Column name must be provided");
		PG_RETURN_NULL();
	}
	colnametext = PG_GETARG_TEXT_P(1);
	colname = text_to_cstring(colnametext);
	if (!strlen(colname)) {
		elog(NOTICE, "Column name must be provided");
		PG_RETURN_NULL();
	}

	/* band index is 1-based */
	if (!PG_ARGISNULL(2))
		bandindex = PG_GETARG_INT32(2);

	/* exclude_nodata_value flag */
	if (!PG_ARGISNULL(3))
		exclude_nodata_value = PG_GETARG_BOOL(3);

	/* sample % */
	if (!PG_ARGISNULL(4)) {
		sample = PG_GETARG_FLOAT8(4);
		if (sample < 0 || sample > 1) {
			elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
			rt_raster_destroy(raster);
			PG_RETURN_NULL();
		}
		else if (FLT_EQ(sample, 0.0))
			sample = 1;
	}
	else
		sample = 1;

	/* iterate through rasters of coverage */
	/* connect to database */
	spi_result = SPI_connect();
	if (spi_result != SPI_OK_CONNECT) {
		pfree(sql);
		elog(ERROR, "RASTER_summaryStatsCoverage: Cannot connect to database using SPI");
		PG_RETURN_NULL();
	}

	/* create sql */
	len = sizeof(char) * (strlen("SELECT \"\" FROM \"\" WHERE \"\" IS NOT NULL") + (strlen(colname) * 2) + strlen(tablename) + 1);
	sql = (char *) palloc(len);
	if (NULL == sql) {
		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_finish();
		elog(ERROR, "RASTER_summaryStatsCoverage: Cannot allocate memory for sql");
		PG_RETURN_NULL();
	}

	/* get cursor */
	snprintf(sql, len, "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IS NOT NULL", colname, tablename, colname);
	portal = SPI_cursor_open_with_args(
		"coverage",
		sql,
		0, NULL,
		NULL, NULL,
		TRUE, 0
	);
	pfree(sql);

	/* process resultset */
	SPI_cursor_fetch(portal, TRUE, 1);
	while (SPI_processed == 1 && SPI_tuptable != NULL) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		datum = SPI_getbinval(tuple, tupdesc, 1, &isNull);
		if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_cursor_close(portal);
			SPI_finish();

			if (NULL != rtn) pfree(rtn);
			elog(ERROR, "RASTER_summaryStatsCoverage: Cannot get raster of coverage");
			PG_RETURN_NULL();
		}
		else if (isNull) {
			SPI_cursor_fetch(portal, TRUE, 1);
			continue;
		}

		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(datum);

		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_cursor_close(portal);
			SPI_finish();

			if (NULL != rtn) pfree(rtn);
			elog(ERROR, "RASTER_summaryStatsCoverage: Cannot deserialize raster");
			PG_RETURN_NULL();
		}

		/* inspect number of bands */
		num_bands = rt_raster_get_num_bands(raster);
		if (bandindex < 1 || bandindex > num_bands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");

			rt_raster_destroy(raster);

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_cursor_close(portal);
			SPI_finish();

			if (NULL != rtn) pfree(rtn);
			PG_RETURN_NULL();
		}

		/* get band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);

			rt_raster_destroy(raster);

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_cursor_close(portal);
			SPI_finish();

			if (NULL != rtn) pfree(rtn);
			PG_RETURN_NULL();
		}

		/* we don't need the raw values, hence the zero parameter */
		stats = rt_band_get_summary_stats(band, (int) exclude_nodata_value, sample, 0, &cK, &cM, &cQ, NULL);

		rt_band_destroy(band);
		rt_raster_destroy(raster);

		if (NULL == stats) {
			elog(NOTICE, "Cannot compute summary statistics for band at index %d. Returning NULL", bandindex);

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_cursor_close(portal);
			SPI_finish();

			if (NULL != rtn) pfree(rtn);
			PG_RETURN_NULL();
		}

		/* initialize rtn */
		if (stats->count > 0) {
			if (NULL == rtn) {
				rtn = (rt_bandstats) SPI_palloc(sizeof(struct rt_bandstats_t));
				if (NULL == rtn) {

					if (SPI_tuptable) SPI_freetuptable(tuptable);
					SPI_cursor_close(portal);
					SPI_finish();

					elog(ERROR, "RASTER_summaryStatsCoverage: Cannot allocate memory for summary stats of coverage");
					PG_RETURN_NULL();
				}

				rtn->sample = stats->sample;
				rtn->count = stats->count;
				rtn->min = stats->min;
				rtn->max = stats->max;
				rtn->sum = stats->sum;
				rtn->mean = stats->mean;
				rtn->stddev = -1;

				rtn->values = NULL;
				rtn->sorted = 0;
			}
			else {
				rtn->count += stats->count;
				rtn->sum += stats->sum;

				if (stats->min < rtn->min)
					rtn->min = stats->min;
				if (stats->max > rtn->max)
					rtn->max = stats->max;
			}
		}

		pfree(stats);

		/* next record */
		SPI_cursor_fetch(portal, TRUE, 1);
	}

	if (SPI_tuptable) SPI_freetuptable(tuptable);
	SPI_cursor_close(portal);
	SPI_finish();

	if (NULL == rtn) {
		elog(ERROR, "RASTER_summaryStatsCoverage: Cannot compute coverage summary stats");
		PG_RETURN_NULL();
	}

	/* coverage mean and deviation */
	rtn->mean = rtn->sum / rtn->count;
	/* sample deviation */
	if (rtn->sample > 0 && rtn->sample < 1)
		rtn->stddev = sqrt(cQ / (rtn->count - 1));
	/* standard deviation */
	else
		rtn->stddev = sqrt(cQ / rtn->count);

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

	memset(nulls, FALSE, sizeof(bool) * values_length);

	values[0] = Int64GetDatum(rtn->count);
	if (rtn->count > 0) {
		values[1] = Float8GetDatum(rtn->sum);
		values[2] = Float8GetDatum(rtn->mean);
		values[3] = Float8GetDatum(rtn->stddev);
		values[4] = Float8GetDatum(rtn->min);
		values[5] = Float8GetDatum(rtn->max);
	}
	else {
		nulls[1] = TRUE;
		nulls[2] = TRUE;
		nulls[3] = TRUE;
		nulls[4] = TRUE;
		nulls[5] = TRUE;
	}

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	/* clean up */
	pfree(rtn);

	PG_RETURN_DATUM(result);
}

/* ---------------------------------------------------------------- */
/* Aggregate ST_SummaryStats                                        */
/* ---------------------------------------------------------------- */

typedef struct rtpg_summarystats_arg_t *rtpg_summarystats_arg;
struct rtpg_summarystats_arg_t {
	rt_bandstats stats;

	/* coefficients for one-pass standard deviation */
	uint64_t cK;
	double cM;
	double cQ;

	int32_t band_index; /* one-based */
	bool exclude_nodata_value;
	double sample; /* value between 0 and 1 */
};

static void
rtpg_summarystats_arg_destroy(rtpg_summarystats_arg arg) {
	if (arg->stats != NULL)
		pfree(arg->stats);

	pfree(arg);
}

static rtpg_summarystats_arg
rtpg_summarystats_arg_init() {
	rtpg_summarystats_arg arg = NULL;

	arg = palloc(sizeof(struct rtpg_summarystats_arg_t));
	if (arg == NULL) {
		elog(
			ERROR,
			"rtpg_summarystats_arg_init: Cannot allocate memory for function arguments"
		);
		return NULL;
	}

	arg->stats = (rt_bandstats) palloc(sizeof(struct rt_bandstats_t));
	if (arg->stats == NULL) {
		rtpg_summarystats_arg_destroy(arg);
		elog(
			ERROR,
			"rtpg_summarystats_arg_init: Cannot allocate memory for stats function argument"
		);
		return NULL;
	}

	arg->stats->sample = 0;
	arg->stats->count = 0;
	arg->stats->min = 0;
	arg->stats->max = 0;
	arg->stats->sum = 0;
  arg->stats->sum2 = 0;
	arg->stats->mean = 0;
	arg->stats->stddev = -1;
	arg->stats->values = NULL;
	arg->stats->sorted = 0;

	arg->cK = 0;
	arg->cM = 0;
	arg->cQ = 0;

	arg->band_index = 1;
	arg->exclude_nodata_value = TRUE;
	arg->sample = 1;

	return arg;
}

/*
 * Serialize a rtpg_summarystats_arg
 */
PG_FUNCTION_INFO_V1(summarystatssend);
Datum
summarystatssend(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	rtpg_summarystats_arg state = (rtpg_summarystats_arg) PG_GETARG_POINTER(0);
	int i;
	int nbytes;
	int vlen;
	bytea *result;

	initStringInfo(&buf);

	pq_sendint64(&buf, state->cK);
	pq_sendfloat8(&buf, state->cM);
	pq_sendfloat8(&buf, state->cQ);

	pq_sendint(&buf, state->band_index, sizeof(state->band_index));
	pq_sendint(&buf, state->exclude_nodata_value, 1);
	pq_sendfloat8(&buf, state->sample);

	pq_sendfloat8(&buf, state->stats->sample);
	pq_sendint(&buf, state->stats->count, sizeof(state->stats->count));
	pq_sendfloat8(&buf, state->stats->min);
	pq_sendfloat8(&buf, state->stats->max);
	pq_sendfloat8(&buf, state->stats->sum);
	pq_sendfloat8(&buf, state->stats->sum2);
	pq_sendfloat8(&buf, state->stats->mean);
	pq_sendfloat8(&buf, state->stats->stddev);
	pq_sendint(&buf, state->stats->sorted, sizeof(state->stats->sorted));

	vlen = state->stats->values == NULL ? 0 : state->stats->count;
	pq_sendint(&buf, vlen, sizeof(vlen));
	for (i=0; i<vlen; i++)
	{
		pq_sendfloat8(&buf, state->stats->values[i]);
	}

	nbytes = buf.len - buf.cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pq_copymsgbytes(&buf, VARDATA(result), nbytes);

	PG_RETURN_POINTER(result);
}

/*
 * Deserialize a rtpg_summarystats_arg
 */
PG_FUNCTION_INFO_V1(summarystatsrecv);
Datum
summarystatsrecv(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	bytea *bytesin;
	StringInfoData buf;
	rtpg_summarystats_arg result;
	int nbytes;
	int vlen;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	bytesin = (bytea *) PG_GETARG_BYTEA_P(0);
	nbytes = VARSIZE(bytesin) - VARHDRSZ;

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytesin), nbytes);

	result = palloc0(sizeof(struct rtpg_summarystats_arg_t));

	result->cK = pq_getmsgint64(&buf);
	result->cM = pq_getmsgfloat8(&buf);
	result->cQ = pq_getmsgfloat8(&buf);

	result->band_index = pq_getmsgint(&buf, sizeof(result->band_index));
	result->exclude_nodata_value = pq_getmsgint(&buf, 1);
	result->sample = pq_getmsgfloat8(&buf);

	result->stats = palloc0(sizeof(struct rt_bandstats_t));
	result->stats->sample = pq_getmsgfloat8(&buf);
	result->stats->count = pq_getmsgint(&buf, sizeof(result->stats->count));
	result->stats->min = pq_getmsgfloat8(&buf);
	result->stats->max = pq_getmsgfloat8(&buf);
	result->stats->sum = pq_getmsgfloat8(&buf);
	result->stats->sum2 = pq_getmsgfloat8(&buf);
	result->stats->mean = pq_getmsgfloat8(&buf);
	result->stats->stddev = pq_getmsgfloat8(&buf);
	result->stats->sorted = pq_getmsgint(&buf, sizeof(result->stats->sorted));
	result->stats->values = NULL;

	vlen = pq_getmsgint(&buf, sizeof(vlen));
	if (vlen > 0)
	{
		int i;

		result->stats->values = palloc0(vlen * sizeof(double));
		for (i=0; i<vlen; i++)
		{
			result->stats->values[i] = pq_getmsgfloat8(&buf);
		}
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * Combines two rtpg_summarystats_args into one
 */
PG_FUNCTION_INFO_V1(RASTER_summaryStats_combinefn);
Datum RASTER_summaryStats_combinefn(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext aggcontext;
	rtpg_summarystats_arg state = PG_ARGISNULL(0) ? NULL : (rtpg_summarystats_arg) PG_GETARG_POINTER(0);
	rtpg_summarystats_arg incoming = (rtpg_summarystats_arg) PG_GETARG_POINTER(1);
	int n;
	double s;
	double s2;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "_summaryStats_combinefn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_POINTER(incoming);

	old = MemoryContextSwitchTo(aggcontext);

	state->cK += incoming->cK;
	state->cQ += incoming->cQ;
	state->cM += incoming->cM;
	state->stats->sum += incoming->stats->sum;
	state->stats->sum2 += incoming->stats->sum2;
	state->stats->count += incoming->stats->count;
	state->stats->min = Min(state->stats->min, incoming->stats->min);
	state->stats->max = Max(state->stats->max, incoming->stats->max);

	n = state->stats->count;
	s = state->stats->sum;
	s2 = state->stats->sum2;

	/* numerator of final stddev formula's radicand */
	state->cQ = (((n * s2) - s * s) / n);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(RASTER_summaryStats_transfn);
Datum RASTER_summaryStats_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	rtpg_summarystats_arg state = NULL;
	bool skiparg = FALSE;

	int i = 0;

	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	int num_bands = 0;
	rt_bandstats stats = NULL;

	POSTGIS_RT_DEBUG(3, "Starting...");

	/* cannot be called directly as this is exclusive aggregate function */
	if (!AggCheckCallContext(fcinfo, &aggcontext)) {
		elog(
			ERROR,
			"RASTER_summaryStats_transfn: Cannot be called in a non-aggregate context"
		);
		PG_RETURN_NULL();
	}

	/* switch to aggcontext */
	oldcontext = MemoryContextSwitchTo(aggcontext);

	if (PG_ARGISNULL(0)) {
		POSTGIS_RT_DEBUG(3, "Creating state variable");

		state = rtpg_summarystats_arg_init();
		if (state == NULL) {
			MemoryContextSwitchTo(oldcontext);
			elog(
				ERROR,
				"RASTER_summaryStats_transfn: Cannot allocate memory for state variable"
			);
			PG_RETURN_NULL();
		}

		skiparg = FALSE;
	}
	else {
		POSTGIS_RT_DEBUG(3, "State variable already exists");
		state = (rtpg_summarystats_arg) PG_GETARG_POINTER(0);
		skiparg = TRUE;
	}

	/* raster arg is NOT NULL */
	if (!PG_ARGISNULL(1)) {
		/* deserialize raster */
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

		/* Get raster object */
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (raster == NULL) {

			rtpg_summarystats_arg_destroy(state);
			PG_FREE_IF_COPY(pgraster, 1);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_summaryStats_transfn: Cannot deserialize raster");
			PG_RETURN_NULL();
		}
	}

	do {
		Oid calltype;
		int nargs = 0;

		if (skiparg)
			break;

		/* 4 or 5 total possible args */
		nargs = PG_NARGS();
		POSTGIS_RT_DEBUGF(4, "nargs = %d", nargs);

		for (i = 2; i < nargs; i++) {
			if (PG_ARGISNULL(i))
				continue;

			calltype = get_fn_expr_argtype(fcinfo->flinfo, i);

			/* band index */
			if (
				(calltype == INT2OID || calltype == INT4OID) &&
				i == 2
			) {
				if (calltype == INT2OID)
					state->band_index = PG_GETARG_INT16(i);
				else
					state->band_index = PG_GETARG_INT32(i);

				/* basic check, > 0 */
				if (state->band_index < 1) {

					rtpg_summarystats_arg_destroy(state);
					if (raster != NULL) {
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 1);
					}

					MemoryContextSwitchTo(oldcontext);
					elog(
						ERROR,
						"RASTER_summaryStats_transfn: Invalid band index (must use 1-based). Returning NULL"
					);
					PG_RETURN_NULL();
				}
			}
			/* exclude_nodata_value */
			else if (
				calltype == BOOLOID && (
					i == 2 || i == 3
				)
			) {
				state->exclude_nodata_value = PG_GETARG_BOOL(i);
			}
			/* sample rate */
			else if (
				(calltype == FLOAT4OID || calltype == FLOAT8OID) &&
				(i == 3 || i == 4)
			) {
				if (calltype == FLOAT4OID)
					state->sample = PG_GETARG_FLOAT4(i);
				else
					state->sample = PG_GETARG_FLOAT8(i);

				/* basic check, 0 <= sample <= 1 */
				if (state->sample < 0. || state->sample > 1.) {

					rtpg_summarystats_arg_destroy(state);
					if (raster != NULL) {
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 1);
					}

					MemoryContextSwitchTo(oldcontext);
					elog(
						ERROR,
						"Invalid sample percentage (must be between 0 and 1). Returning NULL"
					);

					PG_RETURN_NULL();
				}
				else if (FLT_EQ(state->sample, 0.0))
					state->sample = 1;
			}
			/* unknown arg */
			else {
				rtpg_summarystats_arg_destroy(state);
				if (raster != NULL) {
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 1);
				}

				MemoryContextSwitchTo(oldcontext);
				elog(
					ERROR,
					"RASTER_summaryStats_transfn: Unknown function parameter at index %d",
					i
				);
				PG_RETURN_NULL();
			}
		}
	}
	while (0);

	/* null raster, return */
	if (PG_ARGISNULL(1)) {
		POSTGIS_RT_DEBUG(4, "NULL raster so processing required");
		MemoryContextSwitchTo(oldcontext);
		PG_RETURN_POINTER(state);
	}

	/* inspect number of bands */
	num_bands = rt_raster_get_num_bands(raster);
	if (state->band_index > num_bands) {
		elog(
			NOTICE,
			"Raster does not have band at index %d. Skipping raster",
			state->band_index
		);

		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 1);

		MemoryContextSwitchTo(oldcontext);
		PG_RETURN_POINTER(state);
	}

	/* get band */
	band = rt_raster_get_band(raster, state->band_index - 1);
	if (!band) {
		elog(
			NOTICE, "Cannot find band at index %d. Skipping raster",
			state->band_index
		);

		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 1);

		MemoryContextSwitchTo(oldcontext);
		PG_RETURN_POINTER(state);
	}

	/* we don't need the raw values, hence the zero parameter */
	stats = rt_band_get_summary_stats(
		band, (int) state->exclude_nodata_value,
		state->sample, 0,
		&(state->cK), &(state->cM), &(state->cQ), &(state->stats->sum2)
	);

	rt_band_destroy(band);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 1);

	if (NULL == stats) {
		elog(
			NOTICE,
			"Cannot compute summary statistics for band at index %d. Returning NULL",
			state->band_index
		);

		rtpg_summarystats_arg_destroy(state);

		MemoryContextSwitchTo(oldcontext);
		PG_RETURN_NULL();
	}

	if (stats->count > 0) {
		if (state->stats->count < 1) {
			state->stats->sample = stats->sample;
			state->stats->count = stats->count;
			state->stats->min = stats->min;
			state->stats->max = stats->max;
			state->stats->sum = stats->sum;
			state->stats->mean = stats->mean;
			state->stats->stddev = -1;
		}
		else {
			state->stats->count += stats->count;
			state->stats->sum += stats->sum;

			if (stats->min < state->stats->min)
				state->stats->min = stats->min;
			if (stats->max > state->stats->max)
				state->stats->max = stats->max;
		}
	}

	pfree(stats);

	/* switch back to local context */
	MemoryContextSwitchTo(oldcontext);

	POSTGIS_RT_DEBUG(3, "Finished");

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(RASTER_summaryStats_finalfn);
Datum RASTER_summaryStats_finalfn(PG_FUNCTION_ARGS)
{
	rtpg_summarystats_arg state = NULL;

	TupleDesc tupdesc;
	HeapTuple tuple;
	int values_length = 6;
	Datum values[values_length];
	bool nulls[values_length];
	Datum result;

	POSTGIS_RT_DEBUG(3, "Starting...");

	/* NULL, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (rtpg_summarystats_arg) PG_GETARG_POINTER(0);

	if (NULL == state) {
		elog(ERROR, "RASTER_summaryStats_finalfn: Cannot compute coverage summary stats");
		PG_RETURN_NULL();
	}

	/* coverage mean and deviation */
	if (state->stats->count > 0) {
		state->stats->mean = state->stats->sum / state->stats->count;
		/* sample deviation */
		if (state->stats->sample > 0 && state->stats->sample < 1)
			state->stats->stddev = sqrt(state->cQ / (state->stats->count - 1));
		/* standard deviation */
		else
			state->stats->stddev = sqrt(state->cQ / state->stats->count);
	}

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		rtpg_summarystats_arg_destroy(state);
		ereport(ERROR, (
			errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg(
				"function returning record called in context "
				"that cannot accept type record"
			)
		));
	}

	BlessTupleDesc(tupdesc);

	memset(nulls, FALSE, sizeof(bool) * values_length);

	values[0] = Int64GetDatum(state->stats->count);
	if (state->stats->count > 0) {
		values[1] = Float8GetDatum(state->stats->sum);
		values[2] = Float8GetDatum(state->stats->mean);
		values[3] = Float8GetDatum(state->stats->stddev);
		values[4] = Float8GetDatum(state->stats->min);
		values[5] = Float8GetDatum(state->stats->max);
	}
	else {
		nulls[1] = TRUE;
		nulls[2] = TRUE;
		nulls[3] = TRUE;
		nulls[4] = TRUE;
		nulls[5] = TRUE;
	}

	/* build a tuple */
	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* make the tuple into a datum */
	result = HeapTupleGetDatum(tuple);

	/* clean up */
	rtpg_summarystats_arg_destroy(state);

	PG_RETURN_DATUM(result);
}

/**
 * Returns histogram for a band
 */
PG_FUNCTION_INFO_V1(RASTER_histogram);
Datum RASTER_histogram(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	rt_histogram hist;
	rt_histogram hist2;
	int call_cntr;
	int max_calls;

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int32_t bandindex = 1;
		int num_bands = 0;
		bool exclude_nodata_value = TRUE;
		double sample = 0;
		uint32_t bin_count = 0;
		double *bin_width = NULL;
		uint32_t bin_width_count = 0;
		double width = 0;
		bool right = FALSE;
		double min = 0;
		double max = 0;
		rt_bandstats stats = NULL;
		uint32_t count;

		int j;
		int n;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;
		int16 typlen;
		bool typbyval;
		char typalign;

		POSTGIS_RT_DEBUG(3, "RASTER_histogram: Starting");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* pgraster is null, return nothing */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogram: Cannot deserialize raster");
			SRF_RETURN_DONE(funcctx);
		}

		/* band index is 1-based */
		if (!PG_ARGISNULL(1))
			bandindex = PG_GETARG_INT32(1);
		num_bands = rt_raster_get_num_bands(raster);
		if (bandindex < 1 || bandindex > num_bands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(2))
			exclude_nodata_value = PG_GETARG_BOOL(2);

		/* sample % */
		if (!PG_ARGISNULL(3)) {
			sample = PG_GETARG_FLOAT8(3);
			if (sample < 0 || sample > 1) {
				elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}
			else if (FLT_EQ(sample, 0.0))
				sample = 1;
		}
		else
			sample = 1;

		/* bin_count */
		if (!PG_ARGISNULL(4)) {
			bin_count = PG_GETARG_INT32(4);
			if (bin_count < 1) bin_count = 0;
		}

		/* bin_width */
		if (!PG_ARGISNULL(5)) {
			array = PG_GETARG_ARRAYTYPE_P(5);
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
					elog(ERROR, "RASTER_histogram: Invalid data type for width");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			bin_width = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						width = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						width = (double) DatumGetFloat8(e[i]);
						break;
				}

				if (width < 0 || FLT_EQ(width, 0.0)) {
					elog(NOTICE, "Invalid value for width (must be greater than 0). Returning NULL");
					pfree(bin_width);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}

				bin_width[j] = width;
				POSTGIS_RT_DEBUGF(5, "bin_width[%d] = %f", j, bin_width[j]);
				j++;
			}
			bin_width_count = j;

			if (j < 1) {
				pfree(bin_width);
				bin_width = NULL;
			}
		}

		/* right */
		if (!PG_ARGISNULL(6))
			right = PG_GETARG_BOOL(6);

		/* min */
		if (!PG_ARGISNULL(7)) min = PG_GETARG_FLOAT8(7);

		/* max */
		if (!PG_ARGISNULL(8)) max = PG_GETARG_FLOAT8(8);

		/* get band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get stats */
		stats = rt_band_get_summary_stats(band, (int) exclude_nodata_value, sample, 1, NULL, NULL, NULL, NULL);
		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (NULL == stats || NULL == stats->values) {
			elog(NOTICE, "Cannot compute summary statistics for band at index %d", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		else if (stats->count < 1) {
			elog(NOTICE, "Cannot compute histogram for band at index %d as the band has no values", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get histogram */
		hist = rt_band_get_histogram(stats, bin_count, bin_width, bin_width_count, right, min, max, &count);
		if (bin_width_count) pfree(bin_width);
		pfree(stats);
		if (NULL == hist || !count) {
			elog(NOTICE, "Cannot compute histogram for band at index %d", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		POSTGIS_RT_DEBUGF(3, "%d bins returned", count);

		/* Store needed information */
		funcctx->user_fctx = hist;

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
	hist2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 4;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(hist2[call_cntr].min);
		values[1] = Float8GetDatum(hist2[call_cntr].max);
		values[2] = Int64GetDatum(hist2[call_cntr].count);
		values[3] = Float8GetDatum(hist2[call_cntr].percent);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(hist2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Returns histogram of a coverage for a specified band
 */
PG_FUNCTION_INFO_V1(RASTER_histogramCoverage);
Datum RASTER_histogramCoverage(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	rt_histogram covhist = NULL;
	rt_histogram covhist2;
	int call_cntr;
	int max_calls;

	POSTGIS_RT_DEBUG(3, "RASTER_histogramCoverage: Starting");

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		text *tablenametext = NULL;
		char *tablename = NULL;
		text *colnametext = NULL;
		char *colname = NULL;
		int32_t bandindex = 1;
		bool exclude_nodata_value = TRUE;
		double sample = 0;
		uint32_t bin_count = 0;
		double *bin_width = NULL;
		uint32_t bin_width_count = 0;
		double width = 0;
		bool right = FALSE;
		uint32_t count;

		int len = 0;
		char *sql = NULL;
		char *tmp = NULL;
		double min = 0;
		double max = 0;
		int spi_result;
		Portal portal;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;
		Datum datum;
		bool isNull = FALSE;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int num_bands = 0;
		rt_bandstats stats = NULL;
		rt_histogram hist;
		uint64_t sum = 0;

		int j;
		int n;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;
		int16 typlen;
		bool typbyval;
		char typalign;

		POSTGIS_RT_DEBUG(3, "RASTER_histogramCoverage: first call of function");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* tablename is null, return null */
		if (PG_ARGISNULL(0)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		tablenametext = PG_GETARG_TEXT_P(0);
		tablename = text_to_cstring(tablenametext);
		if (!strlen(tablename)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: tablename = %s", tablename);

		/* column name is null, return null */
		if (PG_ARGISNULL(1)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		colnametext = PG_GETARG_TEXT_P(1);
		colname = text_to_cstring(colnametext);
		if (!strlen(colname)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: colname = %s", colname);

		/* band index is 1-based */
		if (!PG_ARGISNULL(2))
			bandindex = PG_GETARG_INT32(2);

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(3))
			exclude_nodata_value = PG_GETARG_BOOL(3);

		/* sample % */
		if (!PG_ARGISNULL(4)) {
			sample = PG_GETARG_FLOAT8(4);
			if (sample < 0 || sample > 1) {
				elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}
			else if (FLT_EQ(sample, 0.0))
				sample = 1;
		}
		else
			sample = 1;

		/* bin_count */
		if (!PG_ARGISNULL(5)) {
			bin_count = PG_GETARG_INT32(5);
			if (bin_count < 1) bin_count = 0;
		}

		/* bin_width */
		if (!PG_ARGISNULL(6)) {
			array = PG_GETARG_ARRAYTYPE_P(6);
			etype = ARR_ELEMTYPE(array);
			get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

			switch (etype) {
				case FLOAT4OID:
				case FLOAT8OID:
					break;
				default:
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_histogramCoverage: Invalid data type for width");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			bin_width = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						width = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						width = (double) DatumGetFloat8(e[i]);
						break;
				}

				if (width < 0 || FLT_EQ(width, 0.0)) {
					elog(NOTICE, "Invalid value for width (must be greater than 0). Returning NULL");
					pfree(bin_width);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}

				bin_width[j] = width;
				POSTGIS_RT_DEBUGF(5, "bin_width[%d] = %f", j, bin_width[j]);
				j++;
			}
			bin_width_count = j;

			if (j < 1) {
				pfree(bin_width);
				bin_width = NULL;
			}
		}

		/* right */
		if (!PG_ARGISNULL(7))
			right = PG_GETARG_BOOL(7);

		/* connect to database */
		spi_result = SPI_connect();
		if (spi_result != SPI_OK_CONNECT) {

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot connect to database using SPI");
			SRF_RETURN_DONE(funcctx);
		}

		/* coverage stats */
		len = sizeof(char) * (strlen("SELECT min, max FROM _st_summarystats('','',,::boolean,)") + strlen(tablename) + strlen(colname) + (MAX_INT_CHARLEN * 2) + MAX_DBL_CHARLEN + 1);
		sql = (char *) palloc(len);
		if (NULL == sql) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot allocate memory for sql");
			SRF_RETURN_DONE(funcctx);
		}

		/* get stats */
		snprintf(sql, len, "SELECT min, max FROM _st_summarystats('%s','%s',%d,%d::boolean,%f)", tablename, colname, bandindex, (exclude_nodata_value ? 1 : 0), sample);
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: %s", sql);
		spi_result = SPI_execute(sql, TRUE, 0);
		pfree(sql);
		if (spi_result != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot get summary stats of coverage");
			SRF_RETURN_DONE(funcctx);
		}

		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		tmp = SPI_getvalue(tuple, tupdesc, 1);
		if (NULL == tmp || !strlen(tmp)) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot get summary stats of coverage");
			SRF_RETURN_DONE(funcctx);
		}
		min = strtod(tmp, NULL);
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: min = %f", min);
		pfree(tmp);

		tmp = SPI_getvalue(tuple, tupdesc, 2);
		if (NULL == tmp || !strlen(tmp)) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot get summary stats of coverage");
			SRF_RETURN_DONE(funcctx);
		}
		max = strtod(tmp, NULL);
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: max = %f", max);
		pfree(tmp);

		/* iterate through rasters of coverage */
		/* create sql */
		len = sizeof(char) * (strlen("SELECT \"\" FROM \"\" WHERE \"\" IS NOT NULL") + (strlen(colname) * 2) + strlen(tablename) + 1);
		sql = (char *) palloc(len);
		if (NULL == sql) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (bin_width_count) pfree(bin_width);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_histogramCoverage: Cannot allocate memory for sql");
			SRF_RETURN_DONE(funcctx);
		}

		/* get cursor */
		snprintf(sql, len, "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IS NOT NULL", colname, tablename, colname);
		POSTGIS_RT_DEBUGF(3, "RASTER_histogramCoverage: %s", sql);
		portal = SPI_cursor_open_with_args(
			"coverage",
			sql,
			0, NULL,
			NULL, NULL,
			TRUE, 0
		);
		pfree(sql);

		/* process resultset */
		SPI_cursor_fetch(portal, TRUE, 1);
		while (SPI_processed == 1 && SPI_tuptable != NULL) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			datum = SPI_getbinval(tuple, tupdesc, 1, &isNull);
			if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covhist) pfree(covhist);
				if (bin_width_count) pfree(bin_width);

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_histogramCoverage: Cannot get raster of coverage");
				SRF_RETURN_DONE(funcctx);
			}
			else if (isNull) {
				SPI_cursor_fetch(portal, TRUE, 1);
				continue;
			}

			pgraster = (rt_pgraster *) PG_DETOAST_DATUM(datum);

			raster = rt_raster_deserialize(pgraster, FALSE);
			if (!raster) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covhist) pfree(covhist);
				if (bin_width_count) pfree(bin_width);

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_histogramCoverage: Cannot deserialize raster");
				SRF_RETURN_DONE(funcctx);
			}

			/* inspect number of bands*/
			num_bands = rt_raster_get_num_bands(raster);
			if (bandindex < 1 || bandindex > num_bands) {
				elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covhist) pfree(covhist);
				if (bin_width_count) pfree(bin_width);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* get band */
			band = rt_raster_get_band(raster, bandindex - 1);
			if (!band) {
				elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covhist) pfree(covhist);
				if (bin_width_count) pfree(bin_width);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* we need the raw values, hence the non-zero parameter */
			stats = rt_band_get_summary_stats(band, (int) exclude_nodata_value, sample, 1, NULL, NULL, NULL, NULL);

			rt_band_destroy(band);
			rt_raster_destroy(raster);

			if (NULL == stats) {
				elog(NOTICE, "Cannot compute summary statistics for band at index %d. Returning NULL", bandindex);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covhist) pfree(covhist);
				if (bin_width_count) pfree(bin_width);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* get histogram */
			if (stats->count > 0) {
				hist = rt_band_get_histogram(stats, bin_count, bin_width, bin_width_count, right, min, max, &count);
				pfree(stats);
				if (NULL == hist || !count) {
					elog(NOTICE, "Cannot compute histogram for band at index %d", bandindex);

					if (SPI_tuptable) SPI_freetuptable(tuptable);
					SPI_cursor_close(portal);
					SPI_finish();

					if (NULL != covhist) pfree(covhist);
					if (bin_width_count) pfree(bin_width);

					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}

				POSTGIS_RT_DEBUGF(3, "%d bins returned", count);

				/* coverage histogram */
				if (NULL == covhist) {
					covhist = (rt_histogram) SPI_palloc(sizeof(struct rt_histogram_t) * count);
					if (NULL == covhist) {

						pfree(hist);
						if (SPI_tuptable) SPI_freetuptable(tuptable);
						SPI_cursor_close(portal);
						SPI_finish();

						if (bin_width_count) pfree(bin_width);

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_histogramCoverage: Cannot allocate memory for histogram of coverage");
						SRF_RETURN_DONE(funcctx);
					}

					for (i = 0; i < count; i++) {
						sum += hist[i].count;
						covhist[i].count = hist[i].count;
						covhist[i].percent = 0;
						covhist[i].min = hist[i].min;
						covhist[i].max = hist[i].max;
					}
				}
				else {
					for (i = 0; i < count; i++) {
						sum += hist[i].count;
						covhist[i].count += hist[i].count;
					}
				}

				pfree(hist);

				/* assuming bin_count wasn't set, force consistency */
				if (bin_count <= 0) bin_count = count;
			}

			/* next record */
			SPI_cursor_fetch(portal, TRUE, 1);
		}

		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_cursor_close(portal);
		SPI_finish();

		if (bin_width_count) pfree(bin_width);

		/* finish percent of histogram */
		if (sum > 0) {
			for (i = 0; i < count; i++)
				covhist[i].percent = covhist[i].count / (double) sum;
		}

		/* Store needed information */
		funcctx->user_fctx = covhist;

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
	covhist2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 4;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(covhist2[call_cntr].min);
		values[1] = Float8GetDatum(covhist2[call_cntr].max);
		values[2] = Int64GetDatum(covhist2[call_cntr].count);
		values[3] = Float8GetDatum(covhist2[call_cntr].percent);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(covhist2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Returns quantiles for a band
 */
PG_FUNCTION_INFO_V1(RASTER_quantile);
Datum RASTER_quantile(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	rt_quantile quant;
	rt_quantile quant2;
	int call_cntr;
	int max_calls;

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int32_t bandindex = 0;
		int num_bands = 0;
		bool exclude_nodata_value = TRUE;
		double sample = 0;
		double *quantiles = NULL;
		uint32_t quantiles_count = 0;
		double quantile = 0;
		rt_bandstats stats = NULL;
		uint32_t count;

		int j;
		int n;

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

		/* pgraster is null, return nothing */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantile: Cannot deserialize raster");
			SRF_RETURN_DONE(funcctx);
		}

		/* band index is 1-based */
		bandindex = PG_GETARG_INT32(1);
		num_bands = rt_raster_get_num_bands(raster);
		if (bandindex < 1 || bandindex > num_bands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(2))
			exclude_nodata_value = PG_GETARG_BOOL(2);

		/* sample % */
		if (!PG_ARGISNULL(3)) {
			sample = PG_GETARG_FLOAT8(3);
			if (sample < 0 || sample > 1) {
				elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}
			else if (FLT_EQ(sample, 0.0))
				sample = 1;
		}
		else
			sample = 1;

		/* quantiles */
		if (!PG_ARGISNULL(4)) {
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
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_quantile: Invalid data type for quantiles");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			quantiles = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						quantile = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						quantile = (double) DatumGetFloat8(e[i]);
						break;
				}

				if (quantile < 0 || quantile > 1) {
					elog(NOTICE, "Invalid value for quantile (must be between 0 and 1). Returning NULL");
					pfree(quantiles);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}

				quantiles[j] = quantile;
				POSTGIS_RT_DEBUGF(5, "quantiles[%d] = %f", j, quantiles[j]);
				j++;
			}
			quantiles_count = j;

			if (j < 1) {
				pfree(quantiles);
				quantiles = NULL;
			}
		}

		/* get band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get stats */
		stats = rt_band_get_summary_stats(band, (int) exclude_nodata_value, sample, 1, NULL, NULL, NULL, NULL);
		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (NULL == stats || NULL == stats->values) {
			elog(NOTICE, "Cannot retrieve summary statistics for band at index %d", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		else if (stats->count < 1) {
			elog(NOTICE, "Cannot compute quantiles for band at index %d as the band has no values", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get quantiles */
		quant = rt_band_get_quantiles(stats, quantiles, quantiles_count, &count);
		if (quantiles_count) pfree(quantiles);
		pfree(stats);
		if (NULL == quant || !count) {
			elog(NOTICE, "Cannot compute quantiles for band at index %d", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		POSTGIS_RT_DEBUGF(3, "%d quantiles returned", count);

		/* Store needed information */
		funcctx->user_fctx = quant;

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
	quant2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 2;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(quant2[call_cntr].quantile);
		values[1] = Float8GetDatum(quant2[call_cntr].value);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(quant2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Returns selected quantiles of a coverage for a specified band
 */
PG_FUNCTION_INFO_V1(RASTER_quantileCoverage);
Datum RASTER_quantileCoverage(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	rt_quantile covquant = NULL;
	rt_quantile covquant2;
	int call_cntr;
	int max_calls;

	POSTGIS_RT_DEBUG(3, "RASTER_quantileCoverage: Starting");

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		text *tablenametext = NULL;
		char *tablename = NULL;
		text *colnametext = NULL;
		char *colname = NULL;
		int32_t bandindex = 1;
		bool exclude_nodata_value = TRUE;
		double sample = 0;
		double *quantiles = NULL;
		uint32_t quantiles_count = 0;
		double quantile = 0;
		uint32_t count;

		int len = 0;
		char *sql = NULL;
		char *tmp = NULL;
		uint64_t cov_count = 0;
		int spi_result;
		Portal portal;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;
		Datum datum;
		bool isNull = FALSE;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int num_bands = 0;
		struct quantile_llist *qlls = NULL;
		uint32_t qlls_count;

		int j;
		int n;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;
		int16 typlen;
		bool typbyval;
		char typalign;

		POSTGIS_RT_DEBUG(3, "RASTER_quantileCoverage: first call of function");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* tablename is null, return null */
		if (PG_ARGISNULL(0)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		tablenametext = PG_GETARG_TEXT_P(0);
		tablename = text_to_cstring(tablenametext);
		if (!strlen(tablename)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_quantileCoverage: tablename = %s", tablename);

		/* column name is null, return null */
		if (PG_ARGISNULL(1)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		colnametext = PG_GETARG_TEXT_P(1);
		colname = text_to_cstring(colnametext);
		if (!strlen(colname)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_quantileCoverage: colname = %s", colname);

		/* band index is 1-based */
		if (!PG_ARGISNULL(2))
			bandindex = PG_GETARG_INT32(2);

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(3))
			exclude_nodata_value = PG_GETARG_BOOL(3);

		/* sample % */
		if (!PG_ARGISNULL(4)) {
			sample = PG_GETARG_FLOAT8(4);
			if (sample < 0 || sample > 1) {
				elog(NOTICE, "Invalid sample percentage (must be between 0 and 1). Returning NULL");
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}
			else if (FLT_EQ(sample, 0.0))
				sample = 1;
		}
		else
			sample = 1;

		/* quantiles */
		if (!PG_ARGISNULL(5)) {
			array = PG_GETARG_ARRAYTYPE_P(5);
			etype = ARR_ELEMTYPE(array);
			get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

			switch (etype) {
				case FLOAT4OID:
				case FLOAT8OID:
					break;
				default:
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_quantileCoverage: Invalid data type for quantiles");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			quantiles = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						quantile = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						quantile = (double) DatumGetFloat8(e[i]);
						break;
				}

				if (quantile < 0 || quantile > 1) {
					elog(NOTICE, "Invalid value for quantile (must be between 0 and 1). Returning NULL");
					pfree(quantiles);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}

				quantiles[j] = quantile;
				POSTGIS_RT_DEBUGF(5, "quantiles[%d] = %f", j, quantiles[j]);
				j++;
			}
			quantiles_count = j;

			if (j < 1) {
				pfree(quantiles);
				quantiles = NULL;
			}
		}

		/* coverage stats */
		/* connect to database */
		spi_result = SPI_connect();
		if (spi_result != SPI_OK_CONNECT) {
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantileCoverage: Cannot connect to database using SPI");
			SRF_RETURN_DONE(funcctx);
		}

		len = sizeof(char) * (strlen("SELECT count FROM _st_summarystats('','',,::boolean,)") + strlen(tablename) + strlen(colname) + (MAX_INT_CHARLEN * 2) + MAX_DBL_CHARLEN + 1);
		sql = (char *) palloc(len);
		if (NULL == sql) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantileCoverage: Cannot allocate memory for sql");
			SRF_RETURN_DONE(funcctx);
		}

		/* get stats */
		snprintf(sql, len, "SELECT count FROM _st_summarystats('%s','%s',%d,%d::boolean,%f)", tablename, colname, bandindex, (exclude_nodata_value ? 1 : 0), sample);
		POSTGIS_RT_DEBUGF(3, "stats sql:  %s", sql);
		spi_result = SPI_execute(sql, TRUE, 0);
		pfree(sql);
		if (spi_result != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantileCoverage: Cannot get summary stats of coverage");
			SRF_RETURN_DONE(funcctx);
		}

		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		tmp = SPI_getvalue(tuple, tupdesc, 1);
		if (NULL == tmp || !strlen(tmp)) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantileCoverage: Cannot get summary stats of coverage");
			SRF_RETURN_DONE(funcctx);
		}
		cov_count = strtol(tmp, NULL, 10);
		POSTGIS_RT_DEBUGF(3, "covcount = %d", (int) cov_count);
		pfree(tmp);

		/* iterate through rasters of coverage */
		/* create sql */
		len = sizeof(char) * (strlen("SELECT \"\" FROM \"\" WHERE \"\" IS NOT NULL") + (strlen(colname) * 2) + strlen(tablename) + 1);
		sql = (char *) palloc(len);
		if (NULL == sql) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_quantileCoverage: Cannot allocate memory for sql");
			SRF_RETURN_DONE(funcctx);
		}

		/* get cursor */
		snprintf(sql, len, "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IS NOT NULL", colname, tablename, colname);
		POSTGIS_RT_DEBUGF(3, "coverage sql: %s", sql);
		portal = SPI_cursor_open_with_args(
			"coverage",
			sql,
			0, NULL,
			NULL, NULL,
			TRUE, 0
		);
		pfree(sql);

		/* process resultset */
		SPI_cursor_fetch(portal, TRUE, 1);
		while (SPI_processed == 1 && SPI_tuptable != NULL) {
			if (NULL != covquant) pfree(covquant);

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			datum = SPI_getbinval(tuple, tupdesc, 1, &isNull);
			if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_quantileCoverage: Cannot get raster of coverage");
				SRF_RETURN_DONE(funcctx);
			}
			else if (isNull) {
				SPI_cursor_fetch(portal, TRUE, 1);
				continue;
			}

			pgraster = (rt_pgraster *) PG_DETOAST_DATUM(datum);

			raster = rt_raster_deserialize(pgraster, FALSE);
			if (!raster) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_quantileCoverage: Cannot deserialize raster");
				SRF_RETURN_DONE(funcctx);
			}

			/* inspect number of bands*/
			num_bands = rt_raster_get_num_bands(raster);
			if (bandindex < 1 || bandindex > num_bands) {
				elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* get band */
			band = rt_raster_get_band(raster, bandindex - 1);
			if (!band) {
				elog(NOTICE, "Cannot find raster band of index %d. Returning NULL", bandindex);

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			covquant = rt_band_get_quantiles_stream(
				band,
				exclude_nodata_value, sample, cov_count,
				&qlls, &qlls_count,
				quantiles, quantiles_count,
				&count
			);

			rt_band_destroy(band);
			rt_raster_destroy(raster);

			if (NULL == covquant || !count) {
				elog(NOTICE, "Cannot compute quantiles for band at index %d", bandindex);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* next record */
			SPI_cursor_fetch(portal, TRUE, 1);
		}

		covquant2 = SPI_palloc(sizeof(struct rt_quantile_t) * count);
		for (i = 0; i < count; i++) {
			covquant2[i].quantile = covquant[i].quantile;
			covquant2[i].has_value = covquant[i].has_value;
			if (covquant2[i].has_value)
				covquant2[i].value = covquant[i].value;
		}

		if (NULL != covquant) pfree(covquant);
		quantile_llist_destroy(&qlls, qlls_count);

		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_cursor_close(portal);
		SPI_finish();

		if (quantiles_count) pfree(quantiles);

		POSTGIS_RT_DEBUGF(3, "%d quantiles returned", count);

		/* Store needed information */
		funcctx->user_fctx = covquant2;

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
	covquant2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 2;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(covquant2[call_cntr].quantile);
		if (covquant2[call_cntr].has_value)
			values[1] = Float8GetDatum(covquant2[call_cntr].value);
		else
			nulls[1] = TRUE;

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		POSTGIS_RT_DEBUG(3, "done");
		pfree(covquant2);
		SRF_RETURN_DONE(funcctx);
	}
}

/* get counts of values */
PG_FUNCTION_INFO_V1(RASTER_valueCount);
Datum RASTER_valueCount(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	rt_valuecount vcnts;
	rt_valuecount vcnts2;
	int call_cntr;
	int max_calls;

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int32_t bandindex = 0;
		int num_bands = 0;
		bool exclude_nodata_value = TRUE;
		double *search_values = NULL;
		uint32_t search_values_count = 0;
		double roundto = 0;
		uint32_t count;

		int j;
		int n;

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

		/* pgraster is null, return nothing */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_valueCount: Cannot deserialize raster");
			SRF_RETURN_DONE(funcctx);
		}

		/* band index is 1-based */
		bandindex = PG_GETARG_INT32(1);
		num_bands = rt_raster_get_num_bands(raster);
		if (bandindex < 1 || bandindex > num_bands) {
			elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(2))
			exclude_nodata_value = PG_GETARG_BOOL(2);

		/* search values */
		if (!PG_ARGISNULL(3)) {
			array = PG_GETARG_ARRAYTYPE_P(3);
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
					elog(ERROR, "RASTER_valueCount: Invalid data type for values");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			search_values = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						search_values[j] = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						search_values[j] = (double) DatumGetFloat8(e[i]);
						break;
				}

				POSTGIS_RT_DEBUGF(5, "search_values[%d] = %f", j, search_values[j]);
				j++;
			}
			search_values_count = j;

			if (j < 1) {
				pfree(search_values);
				search_values = NULL;
			}
		}

		/* roundto */
		if (!PG_ARGISNULL(4)) {
			roundto = PG_GETARG_FLOAT8(4);
			if (roundto < 0.) roundto = 0;
		}

		/* get band */
		band = rt_raster_get_band(raster, bandindex - 1);
		if (!band) {
			elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* get counts of values */
		vcnts = rt_band_get_value_count(band, (int) exclude_nodata_value, search_values, search_values_count, roundto, NULL, &count);
		rt_band_destroy(band);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (NULL == vcnts || !count) {
			elog(NOTICE, "Cannot count the values for band at index %d", bandindex);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		POSTGIS_RT_DEBUGF(3, "%d value counts returned", count);

		/* Store needed information */
		funcctx->user_fctx = vcnts;

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
	vcnts2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 3;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(vcnts2[call_cntr].value);
		values[1] = UInt32GetDatum(vcnts2[call_cntr].count);
		values[2] = Float8GetDatum(vcnts2[call_cntr].percent);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(vcnts2);
		SRF_RETURN_DONE(funcctx);
	}
}

/* get counts of values for a coverage */
PG_FUNCTION_INFO_V1(RASTER_valueCountCoverage);
Datum RASTER_valueCountCoverage(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	TupleDesc tupdesc;

	int i;
	uint64_t covcount = 0;
	uint64_t covtotal = 0;
	rt_valuecount covvcnts = NULL;
	rt_valuecount covvcnts2;
	int call_cntr;
	int max_calls;

	POSTGIS_RT_DEBUG(3, "RASTER_valueCountCoverage: Starting");

	/* first call of function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		text *tablenametext = NULL;
		char *tablename = NULL;
		text *colnametext = NULL;
		char *colname = NULL;
		int32_t bandindex = 1;
		bool exclude_nodata_value = TRUE;
		double *search_values = NULL;
		uint32_t search_values_count = 0;
		double roundto = 0;

		int len = 0;
		char *sql = NULL;
		int spi_result;
		Portal portal;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;
		Datum datum;
		bool isNull = FALSE;
		rt_pgraster *pgraster = NULL;
		rt_raster raster = NULL;
		rt_band band = NULL;
		int num_bands = 0;
		uint32_t count;
		uint32_t total;
		rt_valuecount vcnts = NULL;
		int exists = 0;

		int j;
		int n;

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

		/* tablename is null, return null */
		if (PG_ARGISNULL(0)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		tablenametext = PG_GETARG_TEXT_P(0);
		tablename = text_to_cstring(tablenametext);
		if (!strlen(tablename)) {
			elog(NOTICE, "Table name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "tablename = %s", tablename);

		/* column name is null, return null */
		if (PG_ARGISNULL(1)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		colnametext = PG_GETARG_TEXT_P(1);
		colname = text_to_cstring(colnametext);
		if (!strlen(colname)) {
			elog(NOTICE, "Column name must be provided");
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		POSTGIS_RT_DEBUGF(3, "colname = %s", colname);

		/* band index is 1-based */
		if (!PG_ARGISNULL(2))
			bandindex = PG_GETARG_INT32(2);

		/* exclude_nodata_value flag */
		if (!PG_ARGISNULL(3))
			exclude_nodata_value = PG_GETARG_BOOL(3);

		/* search values */
		if (!PG_ARGISNULL(4)) {
			array = PG_GETARG_ARRAYTYPE_P(4);
			etype = ARR_ELEMTYPE(array);
			get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

			switch (etype) {
				case FLOAT4OID:
				case FLOAT8OID:
					break;
				default:
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_valueCountCoverage: Invalid data type for values");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
				&nulls, &n);

			search_values = palloc(sizeof(double) * n);
			for (i = 0, j = 0; i < n; i++) {
				if (nulls[i]) continue;

				switch (etype) {
					case FLOAT4OID:
						search_values[j] = (double) DatumGetFloat4(e[i]);
						break;
					case FLOAT8OID:
						search_values[j] = (double) DatumGetFloat8(e[i]);
						break;
				}

				POSTGIS_RT_DEBUGF(5, "search_values[%d] = %f", j, search_values[j]);
				j++;
			}
			search_values_count = j;

			if (j < 1) {
				pfree(search_values);
				search_values = NULL;
			}
		}

		/* roundto */
		if (!PG_ARGISNULL(5)) {
			roundto = PG_GETARG_FLOAT8(5);
			if (roundto < 0.) roundto = 0;
		}

		/* iterate through rasters of coverage */
		/* connect to database */
		spi_result = SPI_connect();
		if (spi_result != SPI_OK_CONNECT) {

			if (search_values_count) pfree(search_values);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_valueCountCoverage: Cannot connect to database using SPI");
			SRF_RETURN_DONE(funcctx);
		}

		/* create sql */
		len = sizeof(char) * (strlen("SELECT \"\" FROM \"\" WHERE \"\" IS NOT NULL") + (strlen(colname) * 2) + strlen(tablename) + 1);
		sql = (char *) palloc(len);
		if (NULL == sql) {

			if (SPI_tuptable) SPI_freetuptable(tuptable);
			SPI_finish();

			if (search_values_count) pfree(search_values);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_valueCountCoverage: Cannot allocate memory for sql");
			SRF_RETURN_DONE(funcctx);
		}

		/* get cursor */
		snprintf(sql, len, "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IS NOT NULL", colname, tablename, colname);
		POSTGIS_RT_DEBUGF(3, "RASTER_valueCountCoverage: %s", sql);
		portal = SPI_cursor_open_with_args(
			"coverage",
			sql,
			0, NULL,
			NULL, NULL,
			TRUE, 0
		);
		pfree(sql);

		/* process resultset */
		SPI_cursor_fetch(portal, TRUE, 1);
		while (SPI_processed == 1 && SPI_tuptable != NULL) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			datum = SPI_getbinval(tuple, tupdesc, 1, &isNull);
			if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covvcnts) pfree(covvcnts);
				if (search_values_count) pfree(search_values);

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_valueCountCoverage: Cannot get raster of coverage");
				SRF_RETURN_DONE(funcctx);
			}
			else if (isNull) {
				SPI_cursor_fetch(portal, TRUE, 1);
				continue;
			}

			pgraster = (rt_pgraster *) PG_DETOAST_DATUM(datum);

			raster = rt_raster_deserialize(pgraster, FALSE);
			if (!raster) {

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covvcnts) pfree(covvcnts);
				if (search_values_count) pfree(search_values);

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_valueCountCoverage: Cannot deserialize raster");
				SRF_RETURN_DONE(funcctx);
			}

			/* inspect number of bands*/
			num_bands = rt_raster_get_num_bands(raster);
			if (bandindex < 1 || bandindex > num_bands) {
				elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covvcnts) pfree(covvcnts);
				if (search_values_count) pfree(search_values);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* get band */
			band = rt_raster_get_band(raster, bandindex - 1);
			if (!band) {
				elog(NOTICE, "Cannot find band at index %d. Returning NULL", bandindex);

				rt_raster_destroy(raster);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covvcnts) pfree(covvcnts);
				if (search_values_count) pfree(search_values);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			/* get counts of values */
			vcnts = rt_band_get_value_count(band, (int) exclude_nodata_value, search_values, search_values_count, roundto, &total, &count);
			rt_band_destroy(band);
			rt_raster_destroy(raster);
			if (NULL == vcnts || !count) {
				elog(NOTICE, "Cannot count the values for band at index %d", bandindex);

				if (SPI_tuptable) SPI_freetuptable(tuptable);
				SPI_cursor_close(portal);
				SPI_finish();

				if (NULL != covvcnts) free(covvcnts);
				if (search_values_count) pfree(search_values);

				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}

			POSTGIS_RT_DEBUGF(3, "%d value counts returned", count);

			if (NULL == covvcnts) {
				covvcnts = (rt_valuecount) SPI_palloc(sizeof(struct rt_valuecount_t) * count);
				if (NULL == covvcnts) {

					if (SPI_tuptable) SPI_freetuptable(tuptable);
					SPI_cursor_close(portal);
					SPI_finish();

					if (search_values_count) pfree(search_values);

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_valueCountCoverage: Cannot allocate memory for value counts of coverage");
					SRF_RETURN_DONE(funcctx);
				}

				for (i = 0; i < count; i++) {
					covvcnts[i].value = vcnts[i].value;
					covvcnts[i].count = vcnts[i].count;
					covvcnts[i].percent = -1;
				}

				covcount = count;
			}
			else {
				for (i = 0; i < count; i++) {
					exists = 0;

					for (j = 0; j < covcount; j++) {
						if (FLT_EQ(vcnts[i].value, covvcnts[j].value)) {
							exists = 1;
							break;
						}
					}

					if (exists) {
						covvcnts[j].count += vcnts[i].count;
					}
					else {
						covcount++;
						covvcnts = SPI_repalloc(covvcnts, sizeof(struct rt_valuecount_t) * covcount);
						if (NULL == covvcnts) {

							if (SPI_tuptable) SPI_freetuptable(tuptable);
							SPI_cursor_close(portal);
							SPI_finish();

							if (search_values_count) pfree(search_values);
							if (NULL != covvcnts) free(covvcnts);

							MemoryContextSwitchTo(oldcontext);
							elog(ERROR, "RASTER_valueCountCoverage: Cannot change allocated memory for value counts of coverage");
							SRF_RETURN_DONE(funcctx);
						}

						covvcnts[covcount - 1].value = vcnts[i].value;
						covvcnts[covcount - 1].count = vcnts[i].count;
						covvcnts[covcount - 1].percent = -1;
					}
				}
			}

			covtotal += total;

			pfree(vcnts);

			/* next record */
			SPI_cursor_fetch(portal, TRUE, 1);
		}

		if (SPI_tuptable) SPI_freetuptable(tuptable);
		SPI_cursor_close(portal);
		SPI_finish();

		if (search_values_count) pfree(search_values);

		/* compute percentages */
		for (i = 0; i < covcount; i++) {
			covvcnts[i].percent = (double) covvcnts[i].count / covtotal;
		}

		/* Store needed information */
		funcctx->user_fctx = covvcnts;

		/* total number of tuples to be returned */
		funcctx->max_calls = covcount;

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
	covvcnts2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		int values_length = 3;
		Datum values[values_length];
		bool nulls[values_length];
		HeapTuple tuple;
		Datum result;

		POSTGIS_RT_DEBUGF(3, "Result %d", call_cntr);

		memset(nulls, FALSE, sizeof(bool) * values_length);

		values[0] = Float8GetDatum(covvcnts2[call_cntr].value);
		values[1] = UInt32GetDatum(covvcnts2[call_cntr].count);
		values[2] = Float8GetDatum(covvcnts2[call_cntr].percent);

		/* build a tuple */
		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	/* do when there is no more left */
	else {
		pfree(covvcnts2);
		SRF_RETURN_DONE(funcctx);
	}
}

