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

#include <postgres.h> /* for palloc */
#include <fmgr.h>

#include "../../postgis_config.h"

#include "lwgeom_pg.h"
#include "rtpostgis.h"

/* determine if two rasters intersect */
Datum RASTER_intersects(PG_FUNCTION_ARGS);

/* determine if two rasters overlap */
Datum RASTER_overlaps(PG_FUNCTION_ARGS);

/* determine if two rasters touch */
Datum RASTER_touches(PG_FUNCTION_ARGS);

/* determine if the first raster contains the second raster */
Datum RASTER_contains(PG_FUNCTION_ARGS);

/* determine if the first raster contains properly the second raster */
Datum RASTER_containsProperly(PG_FUNCTION_ARGS);

/* determine if the first raster covers the second raster */
Datum RASTER_covers(PG_FUNCTION_ARGS);

/* determine if the first raster is covered by the second raster */
Datum RASTER_coveredby(PG_FUNCTION_ARGS);

/* determine if the two rasters are within the specified distance of each other */
Datum RASTER_dwithin(PG_FUNCTION_ARGS);

/* determine if the two rasters are fully within the specified distance of each other */
Datum RASTER_dfullywithin(PG_FUNCTION_ARGS);

/* determine if two rasters are aligned */
Datum RASTER_sameAlignment(PG_FUNCTION_ARGS);
Datum RASTER_notSameAlignmentReason(PG_FUNCTION_ARGS);

/**
 * See if two rasters intersect
 */
PG_FUNCTION_INFO_V1(RASTER_intersects);
Datum RASTER_intersects(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_intersects: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_intersects(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_intersects: Could not test for intersection on the two rasters");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if two rasters overlap
 */
PG_FUNCTION_INFO_V1(RASTER_overlaps);
Datum RASTER_overlaps(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_overlaps: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_overlaps(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_overlaps: Could not test for overlap on the two rasters");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if two rasters touch
 */
PG_FUNCTION_INFO_V1(RASTER_touches);
Datum RASTER_touches(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_touches: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_touches(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_touches: Could not test for touch on the two rasters");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the first raster contains the second raster
 */
PG_FUNCTION_INFO_V1(RASTER_contains);
Datum RASTER_contains(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_contains: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_contains(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_contains: Could not test that the first raster contains the second raster");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the first raster contains properly the second raster
 */
PG_FUNCTION_INFO_V1(RASTER_containsProperly);
Datum RASTER_containsProperly(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_containsProperly: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_contains_properly(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_containsProperly: Could not test that the first raster contains properly the second raster");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the first raster covers the second raster
 */
PG_FUNCTION_INFO_V1(RASTER_covers);
Datum RASTER_covers(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_covers: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_covers(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_covers: Could not test that the first raster covers the second raster");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the first raster is covered by the second raster
 */
PG_FUNCTION_INFO_V1(RASTER_coveredby);
Datum RASTER_coveredby(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_coveredby: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_coveredby(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_coveredby: Could not test that the first raster is covered by the second raster");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the two rasters are within the specified distance of each other
 */
PG_FUNCTION_INFO_V1(RASTER_dwithin);
Datum RASTER_dwithin(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};
	double distance = 0;

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_dwithin: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* distance */
	if (PG_ARGISNULL(4)) {
		elog(NOTICE, "Distance cannot be NULL.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	distance = PG_GETARG_FLOAT8(4);
	if (distance < 0) {
		elog(NOTICE, "Distance cannot be less than zero.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_within_distance(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		distance,
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_dwithin: Could not test that the two rasters are within the specified distance of each other");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if the two rasters are fully within the specified distance of each other
 */
PG_FUNCTION_INFO_V1(RASTER_dfullywithin);
Datum RASTER_dfullywithin(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};
	uint32_t bandindex[2] = {0};
	uint32_t hasbandindex[2] = {0};
	double distance = 0;

	uint32_t i;
	uint32_t j;
	uint32_t k;
	uint32_t numBands;
	int rtn;
	int result;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_dfullywithin: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}

		/* numbands */
		numBands = rt_raster_get_num_bands(rast[i]);
		if (numBands < 1) {
			elog(NOTICE, "The %s raster provided has no bands", i < 1 ? "first" : "second");
			if (i > 0) i++;
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}

		/* band index */
		if (!PG_ARGISNULL(j)) {
			bandindex[i] = PG_GETARG_INT32(j);
			if (bandindex[i] < 1 || bandindex[i] > numBands) {
				elog(NOTICE, "Invalid band index (must use 1-based) for the %s raster. Returning NULL", i < 1 ? "first" : "second");
				if (i > 0) i++;
				for (k = 0; k < i; k++) {
					rt_raster_destroy(rast[k]);
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				PG_RETURN_NULL();
			}
			hasbandindex[i] = 1;
		}
		else
			hasbandindex[i] = 0;
		POSTGIS_RT_DEBUGF(4, "hasbandindex[%d] = %d", i, hasbandindex[i]);
		POSTGIS_RT_DEBUGF(4, "bandindex[%d] = %d", i, bandindex[i]);
		j++;
	}

	/* distance */
	if (PG_ARGISNULL(4)) {
		elog(NOTICE, "Distance cannot be NULL.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	distance = PG_GETARG_FLOAT8(4);
	if (distance < 0) {
		elog(NOTICE, "Distance cannot be less than zero.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* hasbandindex must be balanced */
	if (
		(hasbandindex[0] && !hasbandindex[1]) ||
		(!hasbandindex[0] && hasbandindex[1])
	) {
		elog(NOTICE, "Missing band index.  Band indices must be provided for both rasters if any one is provided");
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* SRID must match */
	if (rt_raster_get_srid(rast[0]) != rt_raster_get_srid(rast[1])) {
		for (k = 0; k < set_count; k++) {
			rt_raster_destroy(rast[k]);
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "The two rasters provided have different SRIDs");
		PG_RETURN_NULL();
	}

	rtn = rt_raster_fully_within_distance(
		rast[0], (hasbandindex[0] ? bandindex[0] - 1 : -1),
		rast[1], (hasbandindex[1] ? bandindex[1] - 1 : -1),
		distance,
		&result
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_dfullywithin: Could not test that the two rasters are fully within the specified distance of each other");
		PG_RETURN_NULL();
	}

	PG_RETURN_BOOL(result);
}

/**
 * See if two rasters are aligned
 */
PG_FUNCTION_INFO_V1(RASTER_sameAlignment);
Datum RASTER_sameAlignment(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	int rtn;
	int aligned = 0;
	char *reason = NULL;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(j), 0, sizeof(struct rt_raster_serialized_t));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], TRUE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_sameAlignment: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}
	}

	rtn = rt_raster_same_alignment(
		rast[0],
		rast[1],
		&aligned,
		&reason
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_sameAlignment: Could not test for alignment on the two rasters");
		PG_RETURN_NULL();
	}

	/* only output reason if not aligned */
	if (reason != NULL && !aligned)
		elog(NOTICE, "%s", reason);

	PG_RETURN_BOOL(aligned);
}

/**
 * Return a reason why two rasters are not aligned
 */
PG_FUNCTION_INFO_V1(RASTER_notSameAlignmentReason);
Datum RASTER_notSameAlignmentReason(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_raster rast[2] = {NULL};

	uint32_t i;
	uint32_t j;
	uint32_t k;
	int rtn;
	int aligned = 0;
	char *reason = NULL;
	text *result = NULL;

	for (i = 0, j = 0; i < set_count; i++) {
		/* pgrast is null, return null */
		if (PG_ARGISNULL(j)) {
			for (k = 0; k < i; k++) {
				rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			PG_RETURN_NULL();
		}
		pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(j), 0, sizeof(struct rt_raster_serialized_t));
		pgrastpos[i] = j;
		j++;

		/* raster */
		rast[i] = rt_raster_deserialize(pgrast[i], TRUE);
		if (!rast[i]) {
			for (k = 0; k <= i; k++) {
				if (k < i)
					rt_raster_destroy(rast[k]);
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_notSameAlignmentReason: Could not deserialize the %s raster", i < 1 ? "first" : "second");
			PG_RETURN_NULL();
		}
	}

	rtn = rt_raster_same_alignment(
		rast[0],
		rast[1],
		&aligned,
		&reason
	);
	for (k = 0; k < set_count; k++) {
		rt_raster_destroy(rast[k]);
		PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	if (rtn != ES_NONE) {
		elog(ERROR, "RASTER_notSameAlignmentReason: Could not test for alignment on the two rasters");
		PG_RETURN_NULL();
	}

	result = cstring2text(reason);
	PG_RETURN_TEXT_P(result);
}
