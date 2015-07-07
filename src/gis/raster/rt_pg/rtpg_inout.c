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

#include "rtpostgis.h"

Datum RASTER_in(PG_FUNCTION_ARGS);
Datum RASTER_out(PG_FUNCTION_ARGS);
Datum RASTER_noop(PG_FUNCTION_ARGS);

Datum RASTER_to_bytea(PG_FUNCTION_ARGS);
Datum RASTER_to_binary(PG_FUNCTION_ARGS);

/**
 * Input is a string with hex chars in it.
 * Convert to binary and put in the result
 */
PG_FUNCTION_INFO_V1(RASTER_in);
Datum RASTER_in(PG_FUNCTION_ARGS)
{
	rt_raster raster;
	char *hexwkb = PG_GETARG_CSTRING(0);
	void *result = NULL;

	POSTGIS_RT_DEBUG(3, "Starting");

	raster = rt_raster_from_hexwkb(hexwkb, strlen(hexwkb));
	if (raster == NULL)
		PG_RETURN_NULL();

	result = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (result == NULL)
		PG_RETURN_NULL();

	SET_VARSIZE(result, ((rt_pgraster*)result)->size);
	PG_RETURN_POINTER(result);
}

/**
 * Given a RASTER structure, convert it to Hex and put it in a string
 */
PG_FUNCTION_INFO_V1(RASTER_out);
Datum RASTER_out(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	uint32_t hexwkbsize = 0;
	char *hexwkb = NULL;

	POSTGIS_RT_DEBUG(3, "Starting");

	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_out: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	hexwkb = rt_raster_to_hexwkb(raster, FALSE, &hexwkbsize);
	if (!hexwkb) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_out: Could not HEX-WKBize raster");
		PG_RETURN_NULL();
	}

	/* Free the raster objects used */
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);

	PG_RETURN_CSTRING(hexwkb);
}

/**
 * Return bytea object with raster in Well-Known-Binary form.
 */
PG_FUNCTION_INFO_V1(RASTER_to_bytea);
Datum RASTER_to_bytea(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	uint8_t *wkb = NULL;
	uint32_t wkb_size = 0;
	bytea *result = NULL;
	int result_size = 0;

	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* Get raster object */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_to_bytea: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* Parse raster to wkb object */
	wkb = rt_raster_to_wkb(raster, FALSE, &wkb_size);
	if (!wkb) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_to_bytea: Could not allocate and generate WKB data");
		PG_RETURN_NULL();
	}

	/* Create varlena object */
	result_size = wkb_size + VARHDRSZ;
	result = (bytea *)palloc(result_size);
	SET_VARSIZE(result, result_size);
	memcpy(VARDATA(result), wkb, VARSIZE(result) - VARHDRSZ);

	/* Free raster objects used */
	rt_raster_destroy(raster);
	pfree(wkb);
	PG_FREE_IF_COPY(pgraster, 0);

	PG_RETURN_POINTER(result);
}

/**
 * Return bytea object with raster in Well-Known-Binary form requested using ST_AsBinary function.
 */
PG_FUNCTION_INFO_V1(RASTER_to_binary);
Datum RASTER_to_binary(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	uint8_t *wkb = NULL;
	uint32_t wkb_size = 0;
	char *result = NULL;
	int result_size = 0;
	int outasin = FALSE;

	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* Get raster object */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_to_binary: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	if (!PG_ARGISNULL(1))
		outasin = PG_GETARG_BOOL(1);

	/* Parse raster to wkb object */
	wkb = rt_raster_to_wkb(raster, outasin, &wkb_size);
	if (!wkb) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_to_binary: Could not allocate and generate WKB data");
		PG_RETURN_NULL();
	}

	/* Create varlena object */
	result_size = wkb_size + VARHDRSZ;
	result = (char *)palloc(result_size);
	SET_VARSIZE(result, result_size);
	memcpy(VARDATA(result), wkb, VARSIZE(result) - VARHDRSZ);

	/* Free raster objects used */
	rt_raster_destroy(raster);
	pfree(wkb);
	PG_FREE_IF_COPY(pgraster, 0);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(RASTER_noop);
Datum RASTER_noop(PG_FUNCTION_ARGS)
{
	rt_raster raster;
	rt_pgraster *pgraster, *result;
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_noop: Could not deserialize raster");
		PG_RETURN_NULL();
	}
	result = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (result == NULL)
		PG_RETURN_NULL();

	SET_VARSIZE(result, raster->size);
	PG_RETURN_POINTER(result);
}

