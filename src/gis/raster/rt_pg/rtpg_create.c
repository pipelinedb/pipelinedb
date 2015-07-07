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
#include <utils/builtins.h> /* for text_to_cstring() */
#include "utils/lsyscache.h" /* for get_typlenbyvalalign */
#include "utils/array.h" /* for ArrayType */
#include "catalog/pg_type.h" /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */

#include "rtpostgis.h"

/* Raster and band creation */
Datum RASTER_makeEmpty(PG_FUNCTION_ARGS);
Datum RASTER_addBand(PG_FUNCTION_ARGS);
Datum RASTER_addBandRasterArray(PG_FUNCTION_ARGS);
Datum RASTER_addBandOutDB(PG_FUNCTION_ARGS);
Datum RASTER_copyBand(PG_FUNCTION_ARGS);
Datum RASTER_tile(PG_FUNCTION_ARGS);

/* create new raster from existing raster's bands */
Datum RASTER_band(PG_FUNCTION_ARGS);

/**
 * Make a new raster with no bands
 */
PG_FUNCTION_INFO_V1(RASTER_makeEmpty);
Datum RASTER_makeEmpty(PG_FUNCTION_ARGS)
{
	uint16 width = 0, height = 0;
	double ipx = 0, ipy = 0, scalex = 0, scaley = 0, skewx = 0, skewy = 0;
	int32_t srid = SRID_UNKNOWN;
	rt_pgraster *pgraster = NULL;
	rt_raster raster;

	if (PG_NARGS() < 9) {
		elog(ERROR, "RASTER_makeEmpty: ST_MakeEmptyRaster requires 9 args");
		PG_RETURN_NULL();
	} 

	if (!PG_ARGISNULL(0))
		width = PG_GETARG_UINT16(0);

	if (!PG_ARGISNULL(1))
		height = PG_GETARG_UINT16(1);

	if (!PG_ARGISNULL(2))
		ipx = PG_GETARG_FLOAT8(2);

	if (!PG_ARGISNULL(3))
		ipy = PG_GETARG_FLOAT8(3);

	if (!PG_ARGISNULL(4))
		scalex = PG_GETARG_FLOAT8(4);

	if (!PG_ARGISNULL(5))
		scaley = PG_GETARG_FLOAT8(5);

	if (!PG_ARGISNULL(6))
		skewx = PG_GETARG_FLOAT8(6);

	if (!PG_ARGISNULL(7))
		skewy = PG_GETARG_FLOAT8(7);

	if (!PG_ARGISNULL(8))
		srid = PG_GETARG_INT32(8);

	POSTGIS_RT_DEBUGF(4, "%dx%d, ip:%g,%g, scale:%g,%g, skew:%g,%g srid:%d",
		width, height, ipx, ipy, scalex, scaley,
		skewx, skewy, srid);

	raster = rt_raster_new(width, height);
	if (raster == NULL)
		PG_RETURN_NULL(); /* error was supposedly printed already */

	rt_raster_set_scale(raster, scalex, scaley);
	rt_raster_set_offsets(raster, ipx, ipy);
	rt_raster_set_skews(raster, skewx, skewy);
	rt_raster_set_srid(raster, srid);

	pgraster = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (!pgraster)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, pgraster->size);
	PG_RETURN_POINTER(pgraster);
}

/**
 * Add band(s) to the given raster at the given position(s).
 */
PG_FUNCTION_INFO_V1(RASTER_addBand);
Datum RASTER_addBand(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	int bandindex = 0;
	int maxbandindex = 0;
	int numbands = 0;
	int lastnumbands = 0;

	text *text_pixtype = NULL;
	char *char_pixtype = NULL;

	struct addbandarg {
		int index;
		bool append;
		rt_pixtype pixtype;
		double initialvalue;
		bool hasnodata;
		double nodatavalue;
	};
	struct addbandarg *arg = NULL;

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

	int i = 0;

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* raster */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_addBand: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* process set of addbandarg */
	POSTGIS_RT_DEBUG(3, "Processing Arg 1 (addbandargset)");
	array = PG_GETARG_ARRAYTYPE_P(1);
	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
		&nulls, &n);

	if (!n) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset");
		PG_RETURN_NULL();
	}

	/* allocate addbandarg */
	arg = (struct addbandarg *) palloc(sizeof(struct addbandarg) * n);
	if (arg == NULL) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_addBand: Could not allocate memory for addbandarg");
		PG_RETURN_NULL();
	}

	/*
		process each element of addbandargset
		each element is the index of where to add the new band,
			new band's pixeltype, the new band's initial value and
			the new band's NODATA value if NOT NULL
	*/
	for (i = 0; i < n; i++) {
		if (nulls[i]) continue;

		POSTGIS_RT_DEBUGF(4, "Processing addbandarg at index %d", i);

		/* each element is a tuple */
		tup = (HeapTupleHeader) DatumGetPointer(e[i]);
		if (NULL == tup) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset");
			PG_RETURN_NULL();
		}

		/* new band index, 1-based */
		arg[i].index = 0;
		arg[i].append = TRUE;
		tupv = GetAttributeByName(tup, "index", &isnull);
		if (!isnull) {
			arg[i].index = DatumGetInt32(tupv);
			arg[i].append = FALSE;
		}

		/* for now, only check that band index is 1-based */
		if (!arg[i].append && arg[i].index < 1) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset. Invalid band index (must be 1-based) for addbandarg of index %d", i);
			PG_RETURN_NULL();
		}

		/* new band pixeltype */
		arg[i].pixtype = PT_END;
		tupv = GetAttributeByName(tup, "pixeltype", &isnull);
		if (isnull) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset. Pixel type cannot be NULL for addbandarg of index %d", i);
			PG_RETURN_NULL();
		}
		text_pixtype = (text *) DatumGetPointer(tupv);
		if (text_pixtype == NULL) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset. Pixel type cannot be NULL for addbandarg of index %d", i);
			PG_RETURN_NULL();
		}
		char_pixtype = text_to_cstring(text_pixtype);

		arg[i].pixtype = rt_pixtype_index_from_name(char_pixtype);
		pfree(char_pixtype);
		if (arg[i].pixtype == PT_END) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Invalid argument for addbandargset. Invalid pixel type for addbandarg of index %d", i);
			PG_RETURN_NULL();
		}

		/* new band initialvalue */
		arg[i].initialvalue = 0;
		tupv = GetAttributeByName(tup, "initialvalue", &isnull);
		if (!isnull)
			arg[i].initialvalue = DatumGetFloat8(tupv);

		/* new band NODATA value */
		arg[i].hasnodata = FALSE;
		arg[i].nodatavalue = 0;
		tupv = GetAttributeByName(tup, "nodataval", &isnull);
		if (!isnull) {
			arg[i].hasnodata = TRUE;
			arg[i].nodatavalue = DatumGetFloat8(tupv);
		}
	}

	/* add new bands to raster */
	lastnumbands = rt_raster_get_num_bands(raster);
	for (i = 0; i < n; i++) {
		if (nulls[i]) continue;

		POSTGIS_RT_DEBUGF(3, "%d bands in old raster", lastnumbands);
		maxbandindex = lastnumbands + 1;

		/* check that new band's index doesn't exceed maxbandindex */
		if (!arg[i].append) {
			if (arg[i].index > maxbandindex) {
				elog(NOTICE, "Band index for addbandarg of index %d exceeds possible value. Adding band at index %d", i, maxbandindex);
				arg[i].index = maxbandindex;
			}
		}
		/* append, so use maxbandindex */
		else
			arg[i].index = maxbandindex;

		POSTGIS_RT_DEBUGF(4, "new band (index, pixtype, initialvalue, hasnodata, nodatavalue) = (%d, %s, %f, %s, %f)",
			arg[i].index,
			rt_pixtype_name(arg[i].pixtype),
			arg[i].initialvalue,
			arg[i].hasnodata ? "TRUE" : "FALSE",
			arg[i].nodatavalue
		);

		bandindex = rt_raster_generate_new_band(
			raster,
			arg[i].pixtype, arg[i].initialvalue,
			arg[i].hasnodata, arg[i].nodatavalue,
			arg[i].index - 1
		);

		numbands = rt_raster_get_num_bands(raster);
		if (numbands == lastnumbands || bandindex == -1) {
			pfree(arg);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBand: Could not add band defined by addbandarg of index %d to raster", i);
			PG_RETURN_NULL();
		}

		lastnumbands = numbands;
		POSTGIS_RT_DEBUGF(3, "%d bands in new raster", lastnumbands);
	}

	pfree(arg);

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Add bands from array of rasters to a destination raster
 */
PG_FUNCTION_INFO_V1(RASTER_addBandRasterArray);
Datum RASTER_addBandRasterArray(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgsrc = NULL;
	rt_pgraster *pgrtn = NULL;

	rt_raster raster = NULL;
	rt_raster src = NULL;

	int srcnband = 1;
	bool appendband = FALSE;
	int dstnband = 1;
	int srcnumbands = 0;
	int dstnumbands = 0;

	ArrayType *array;
	Oid etype;
	Datum *e;
	bool *nulls;
	int16 typlen;
	bool typbyval;
	char typalign;
	int n = 0;

	int rtn = 0;
	int i = 0;

	/* destination raster */
	if (!PG_ARGISNULL(0)) {
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		/* raster */
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandRasterArray: Could not deserialize destination raster");
			PG_RETURN_NULL();
		}

		POSTGIS_RT_DEBUG(4, "destination raster isn't NULL");
	}

	/* source rasters' band index, 1-based */
	if (!PG_ARGISNULL(2))
		srcnband = PG_GETARG_INT32(2);
	if (srcnband < 1) {
		elog(NOTICE, "Invalid band index for source rasters (must be 1-based).  Returning original raster");
		if (raster != NULL) {
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}
		else
			PG_RETURN_NULL();
	}
	POSTGIS_RT_DEBUGF(4, "srcnband = %d", srcnband);

	/* destination raster's band index, 1-based */
	if (!PG_ARGISNULL(3)) {
		dstnband = PG_GETARG_INT32(3);
		appendband = FALSE;

		if (dstnband < 1) {
			elog(NOTICE, "Invalid band index for destination raster (must be 1-based).  Returning original raster");
			if (raster != NULL) {
				rt_raster_destroy(raster);
				PG_RETURN_POINTER(pgraster);
			}
			else
				PG_RETURN_NULL();
		}
	}
	else
		appendband = TRUE;

	/* additional processing of dstnband */
	if (raster != NULL) {
		dstnumbands = rt_raster_get_num_bands(raster);

		if (dstnumbands < 1) {
			appendband = TRUE;
			dstnband = 1;
		}
		else if (appendband)
			dstnband = dstnumbands + 1;
		else if (dstnband > dstnumbands) {
			elog(NOTICE, "Band index provided for destination raster is greater than the number of bands in the raster.  Bands will be appended");
			appendband = TRUE;
			dstnband = dstnumbands + 1;
		}
	}
	POSTGIS_RT_DEBUGF(4, "appendband = %d", appendband);
	POSTGIS_RT_DEBUGF(4, "dstnband = %d", dstnband);

	/* process set of source rasters */
	POSTGIS_RT_DEBUG(3, "Processing array of source rasters");
	array = PG_GETARG_ARRAYTYPE_P(1);
	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
		&nulls, &n);

	/* decrement srcnband and dstnband by 1, now 0-based */
	srcnband--;
	dstnband--;
	POSTGIS_RT_DEBUGF(4, "0-based nband (src, dst) = (%d, %d)", srcnband, dstnband);

	/* time to copy bands */
	for (i = 0; i < n; i++) {
		if (nulls[i]) continue;
		src = NULL;

		pgsrc =	(rt_pgraster *) PG_DETOAST_DATUM(e[i]);
		src = rt_raster_deserialize(pgsrc, FALSE);
		if (src == NULL) {
			pfree(nulls);
			pfree(e);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandRasterArray: Could not deserialize source raster at index %d", i + 1);
			PG_RETURN_NULL();
		}

		srcnumbands = rt_raster_get_num_bands(src);
		POSTGIS_RT_DEBUGF(4, "source raster %d has %d bands", i + 1, srcnumbands);

		/* band index isn't valid */
		if (srcnband > srcnumbands - 1) {
			elog(NOTICE, "Invalid band index for source raster at index %d.  Returning original raster", i + 1);
			pfree(nulls);
			pfree(e);
			rt_raster_destroy(src);
			if (raster != NULL) {
				rt_raster_destroy(raster);
				PG_RETURN_POINTER(pgraster);
			}
			else
				PG_RETURN_NULL();
		}

		/* destination raster is empty, new raster */
		if (raster == NULL) {
			uint32_t srcnbands[1] = {srcnband};

			POSTGIS_RT_DEBUG(4, "empty destination raster, using rt_raster_from_band");

			raster = rt_raster_from_band(src, srcnbands, 1);
			rt_raster_destroy(src);
			if (raster == NULL) {
				pfree(nulls);
				pfree(e);
				if (pgraster != NULL)
					PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_addBandRasterArray: Could not create raster from source raster at index %d", i + 1);
				PG_RETURN_NULL();
			}
		}
		/* copy band */
		else {
			rtn = rt_raster_copy_band(
				raster, src,
				srcnband, dstnband
			);
			rt_raster_destroy(src);

			if (rtn == -1 || rt_raster_get_num_bands(raster) == dstnumbands) {
				elog(NOTICE, "Could not add band from source raster at index %d to destination raster.  Returning original raster", i + 1);
				rt_raster_destroy(raster);
				pfree(nulls);
				pfree(e);
				if (pgraster != NULL)
					PG_RETURN_POINTER(pgraster);
				else
					PG_RETURN_NULL();
			}
		}

		dstnband++;
		dstnumbands++;
	}

	if (raster != NULL) {
		pgrtn = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (pgraster != NULL)
			PG_FREE_IF_COPY(pgraster, 0);
		if (!pgrtn)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	PG_RETURN_NULL();
}

/**
 * Add out-db band to the given raster at the given position
 */
PG_FUNCTION_INFO_V1(RASTER_addBandOutDB);
Datum RASTER_addBandOutDB(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;

	rt_raster raster = NULL;
	rt_band band = NULL;
	int numbands = 0;
	int dstnband = 1; /* 1-based */
	int appendband = FALSE;
	char *outdbfile = NULL;
	int *srcnband = NULL; /* 1-based */
	int numsrcnband = 0;
	int allbands = FALSE;
	int hasnodata = FALSE;
	double nodataval = 0.;
	uint16_t width = 0;
	uint16_t height = 0;
	char *authname = NULL;
	char *authcode = NULL;

	int i = 0;
	int j = 0;

	GDALDatasetH hdsOut;
	GDALRasterBandH hbandOut;
	GDALDataType gdpixtype;

	rt_pixtype pt = PT_END;
	double gt[6] = {0.};
	double ogt[6] = {0.};
	rt_raster _rast = NULL;
	int aligned = 0;
	int err = 0;

	/* destination raster */
	if (!PG_ARGISNULL(0)) {
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

		/* raster */
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (!raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandOutDB: Could not deserialize destination raster");
			PG_RETURN_NULL();
		}

		POSTGIS_RT_DEBUG(4, "destination raster isn't NULL");
	}

	/* destination band index (1) */
	if (!PG_ARGISNULL(1))
		dstnband = PG_GETARG_INT32(1);
	else
		appendband = TRUE;

	/* outdb file (2) */
	if (PG_ARGISNULL(2)) {
		elog(NOTICE, "Out-db raster file not provided. Returning original raster");
		if (pgraster != NULL) {
			rt_raster_destroy(raster);
			PG_RETURN_POINTER(pgraster);
		}
		else
			PG_RETURN_NULL();
	}
	else {
		outdbfile = text_to_cstring(PG_GETARG_TEXT_P(2));
		if (!strlen(outdbfile)) {
			elog(NOTICE, "Out-db raster file not provided. Returning original raster");
			if (pgraster != NULL) {
				rt_raster_destroy(raster);
				PG_RETURN_POINTER(pgraster);
			}
			else
				PG_RETURN_NULL();
		}
	}

	/* outdb band index (3) */
	if (!PG_ARGISNULL(3)) {
		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;

		int16 typlen;
		bool typbyval;
		char typalign;

		allbands = FALSE;

		array = PG_GETARG_ARRAYTYPE_P(3);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case INT2OID:
			case INT4OID:
				break;
			default:
				if (pgraster != NULL) {
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
				}
				elog(ERROR, "RASTER_addBandOutDB: Invalid data type for band indexes");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(array, etype, typlen, typbyval, typalign, &e, &nulls, &numsrcnband);

		srcnband = palloc(sizeof(int) * numsrcnband);
		if (srcnband == NULL) {
			if (pgraster != NULL) {
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
			}
			elog(ERROR, "RASTER_addBandOutDB: Could not allocate memory for band indexes");
			PG_RETURN_NULL();
		}

		for (i = 0, j = 0; i < numsrcnband; i++) {
			if (nulls[i]) continue;

			switch (etype) {
				case INT2OID:
					srcnband[j] = DatumGetInt16(e[i]);
					break;
				case INT4OID:
					srcnband[j] = DatumGetInt32(e[i]);
					break;
			}
			j++;
		}

		if (j < numsrcnband) {
			srcnband = repalloc(srcnband, sizeof(int) * j);
			if (srcnband == NULL) {
				if (pgraster != NULL) {
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
				}
				elog(ERROR, "RASTER_addBandOutDB: Could not reallocate memory for band indexes");
				PG_RETURN_NULL();
			}

			numsrcnband = j;
		}
	}
	else
		allbands = TRUE;

	/* nodataval (4) */
	if (!PG_ARGISNULL(4)) {
		hasnodata = TRUE;
		nodataval = PG_GETARG_FLOAT8(4);
	}
	else
		hasnodata = FALSE;

	/* validate input */

	/* make sure dstnband is valid */
	if (raster != NULL) {
		numbands = rt_raster_get_num_bands(raster);
		if (!appendband) {
			if (dstnband < 1) {
				elog(NOTICE, "Invalid band index %d for adding bands. Using band index 1", dstnband);
				dstnband = 1;
			}
			else if (numbands > 0 && dstnband > numbands) {
				elog(NOTICE, "Invalid band index %d for adding bands. Using band index %d", dstnband, numbands);
				dstnband = numbands + 1; 
			}
		}
		else
			dstnband = numbands + 1; 
	}

	/* open outdb raster file */
	rt_util_gdal_register_all(0);
	hdsOut = rt_util_gdal_open(outdbfile, GA_ReadOnly, 0);
	if (hdsOut == NULL) {
		if (pgraster != NULL) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
		}
		elog(ERROR, "RASTER_addBandOutDB: Could not open out-db file with GDAL");
		PG_RETURN_NULL();
	}

	/* get offline raster's geotransform */
	if (GDALGetGeoTransform(hdsOut, ogt) != CE_None) {
		ogt[0] = 0;
		ogt[1] = 1;
		ogt[2] = 0;
		ogt[3] = 0;
		ogt[4] = 0;
		ogt[5] = -1;
	}

	/* raster doesn't exist, create it now */
	if (raster == NULL) {
		raster = rt_raster_new(GDALGetRasterXSize(hdsOut), GDALGetRasterYSize(hdsOut));
		if (rt_raster_is_empty(raster)) {
			elog(ERROR, "RASTER_addBandOutDB: Could not create new raster");
			PG_RETURN_NULL();
		}
		rt_raster_set_geotransform_matrix(raster, ogt);
		rt_raster_get_geotransform_matrix(raster, gt);

		if (rt_util_gdal_sr_auth_info(hdsOut, &authname, &authcode) == ES_NONE) {
			if (
				authname != NULL &&
				strcmp(authname, "EPSG") == 0 &&
				authcode != NULL
			) {
				rt_raster_set_srid(raster, atoi(authcode));
			}
			else
				elog(INFO, "Unknown SRS auth name and code from out-db file. Defaulting SRID of new raster to %d", SRID_UNKNOWN);
		}
		else
			elog(INFO, "Could not get SRS auth name and code from out-db file. Defaulting SRID of new raster to %d", SRID_UNKNOWN);
	}

	/* some raster info */
	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);

	/* are rasters aligned? */
	_rast = rt_raster_new(1, 1);
	rt_raster_set_geotransform_matrix(_rast, ogt);
	rt_raster_set_srid(_rast, rt_raster_get_srid(raster));
	err = rt_raster_same_alignment(raster, _rast, &aligned, NULL);
	rt_raster_destroy(_rast);

	if (err != ES_NONE) {
		GDALClose(hdsOut);
		if (raster != NULL)
			rt_raster_destroy(raster);
		if (pgraster != NULL)
			PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_addBandOutDB: Could not test alignment of out-db file");
		return ES_ERROR;
	}
	else if (!aligned)
		elog(WARNING, "The in-db representation of the out-db raster is not aligned. Band data may be incorrect");

	numbands = GDALGetRasterCount(hdsOut);

	/* build up srcnband */
	if (allbands) {
		numsrcnband = numbands;
		srcnband = palloc(sizeof(int) * numsrcnband);
		if (srcnband == NULL) {
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandOutDB: Could not allocate memory for band indexes");
			PG_RETURN_NULL();
		}

		for (i = 0, j = 1; i < numsrcnband; i++, j++)
			srcnband[i] = j;
	}

	/* check band properties and add band */
	for (i = 0, j = dstnband - 1; i < numsrcnband; i++, j++) {
		/* valid index? */
		if (srcnband[i] < 1 || srcnband[i] > numbands) {
			elog(NOTICE, "Out-db file does not have a band at index %d. Returning original raster", srcnband[i]);
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_RETURN_POINTER(pgraster);
			else
				PG_RETURN_NULL();
		}

		/* get outdb band */
		hbandOut = NULL;
		hbandOut = GDALGetRasterBand(hdsOut, srcnband[i]);
		if (NULL == hbandOut) {
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandOutDB: Could not get band %d from GDAL dataset", srcnband[i]);
			PG_RETURN_NULL();
		}

		/* supported pixel type */
		gdpixtype = GDALGetRasterDataType(hbandOut);
		pt = rt_util_gdal_datatype_to_pixtype(gdpixtype);
		if (pt == PT_END) {
			elog(NOTICE, "Pixel type %s of band %d from GDAL dataset is not supported. Returning original raster", GDALGetDataTypeName(gdpixtype), srcnband[i]);
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_RETURN_POINTER(pgraster);
			else
				PG_RETURN_NULL();
		}

		/* use out-db band's nodata value if nodataval not already set */
		if (!hasnodata)
			nodataval = GDALGetRasterNoDataValue(hbandOut, &hasnodata);

		/* add band */
		band = rt_band_new_offline(
			width, height,
			pt,
			hasnodata, nodataval,
			srcnband[i] - 1, outdbfile
		);
		if (band == NULL) {
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandOutDB: Could not create new out-db band");
			PG_RETURN_NULL();
		}

		if (rt_raster_add_band(raster, band, j) < 0) {
			GDALClose(hdsOut);
			if (raster != NULL)
				rt_raster_destroy(raster);
			if (pgraster != NULL)
				PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_addBandOutDB: Could not add new out-db band to raster");
			PG_RETURN_NULL();
		}
	}

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (pgraster != NULL)
		PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Copy a band from one raster to another one at the given position.
 */
PG_FUNCTION_INFO_V1(RASTER_copyBand);
Datum RASTER_copyBand(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgto = NULL;
	rt_pgraster *pgfrom = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster torast = NULL;
	rt_raster fromrast = NULL;
	int toindex = 0;
	int fromband = 0;
	int oldtorastnumbands = 0;
	int newtorastnumbands = 0;
	int newbandindex = 0;

	/* Deserialize torast */
	if (PG_ARGISNULL(0)) PG_RETURN_NULL();
	pgto = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	torast = rt_raster_deserialize(pgto, FALSE);
	if (!torast) {
		PG_FREE_IF_COPY(pgto, 0);
		elog(ERROR, "RASTER_copyBand: Could not deserialize first raster");
		PG_RETURN_NULL();
	}

	/* Deserialize fromrast */
	if (!PG_ARGISNULL(1)) {
		pgfrom = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

		fromrast = rt_raster_deserialize(pgfrom, FALSE);
		if (!fromrast) {
			rt_raster_destroy(torast);
			PG_FREE_IF_COPY(pgfrom, 1);
			PG_FREE_IF_COPY(pgto, 0);
			elog(ERROR, "RASTER_copyBand: Could not deserialize second raster");
			PG_RETURN_NULL();
		}

		oldtorastnumbands = rt_raster_get_num_bands(torast);

		if (PG_ARGISNULL(2))
			fromband = 1;
		else
			fromband = PG_GETARG_INT32(2);

		if (PG_ARGISNULL(3))
			toindex = oldtorastnumbands + 1;
		else
			toindex = PG_GETARG_INT32(3);

		/* Copy band fromrast torast */
		newbandindex = rt_raster_copy_band(
			torast, fromrast,
			fromband - 1, toindex - 1
		);

		newtorastnumbands = rt_raster_get_num_bands(torast);
		if (newtorastnumbands == oldtorastnumbands || newbandindex == -1) {
			elog(NOTICE, "RASTER_copyBand: Could not add band to raster. "
				"Returning original raster."
			);
		}

		rt_raster_destroy(fromrast);
		PG_FREE_IF_COPY(pgfrom, 1);
	}

	/* Serialize and return torast */
	pgrtn = rt_raster_serialize(torast);
	rt_raster_destroy(torast);
	PG_FREE_IF_COPY(pgto, 0);
	if (!pgrtn) PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Break up a raster into smaller tiles. SRF function
 */
PG_FUNCTION_INFO_V1(RASTER_tile);
Datum RASTER_tile(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	int i = 0;
	int j = 0;

	struct tile_arg_t {

		struct {
			rt_raster raster;
			double gt[6];
			int srid;
			int width;
			int height;
		} raster;

		struct {
			int width;
			int height;

			int nx;
			int ny;
		} tile;

		int numbands;
		int *nbands;

		struct {
			int pad;
			double hasnodata;
			double nodataval;
		} pad;
	};
	struct tile_arg_t *arg1 = NULL;
	struct tile_arg_t *arg2 = NULL;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;
		rt_pgraster *pgraster = NULL;
		int numbands;

		ArrayType *array;
		Oid etype;
		Datum *e;
		bool *nulls;

		int16 typlen;
		bool typbyval;
		char typalign;

		POSTGIS_RT_DEBUG(2, "RASTER_tile: first call");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Get input arguments */
		if (PG_ARGISNULL(0)) {
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* allocate arg1 */
		arg1 = palloc(sizeof(struct tile_arg_t));
		if (arg1 == NULL) {
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_tile: Could not allocate memory for arguments");
			SRF_RETURN_DONE(funcctx);
		}

		pgraster = (rt_pgraster *) PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0));
		arg1->raster.raster = rt_raster_deserialize(pgraster, FALSE);
		if (!arg1->raster.raster) {
			ereport(ERROR, (
				errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Could not deserialize raster")
			));
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* raster has bands */
		numbands = rt_raster_get_num_bands(arg1->raster.raster); 
		/*
		if (!numbands) {
			elog(NOTICE, "Raster provided has no bands");
			rt_raster_destroy(arg1->raster.raster);
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		*/

		/* width (1) */
		if (PG_ARGISNULL(1)) {
			elog(NOTICE, "Width cannot be NULL. Returning NULL");
			rt_raster_destroy(arg1->raster.raster);
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		arg1->tile.width = PG_GETARG_INT32(1);
		if (arg1->tile.width < 1) {
			elog(NOTICE, "Width must be greater than zero. Returning NULL");
			rt_raster_destroy(arg1->raster.raster);
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* height (2) */
		if (PG_ARGISNULL(2)) {
			elog(NOTICE, "Height cannot be NULL. Returning NULL");
			rt_raster_destroy(arg1->raster.raster);
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}
		arg1->tile.height = PG_GETARG_INT32(2);
		if (arg1->tile.height < 1) {
			elog(NOTICE, "Height must be greater than zero. Returning NULL");
			rt_raster_destroy(arg1->raster.raster);
			pfree(arg1);
			PG_FREE_IF_COPY(pgraster, 0);
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* nband, array (3) */
		if (numbands && !PG_ARGISNULL(3)) {
			array = PG_GETARG_ARRAYTYPE_P(3);
			etype = ARR_ELEMTYPE(array);
			get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

			switch (etype) {
				case INT2OID:
				case INT4OID:
					break;
				default:
					rt_raster_destroy(arg1->raster.raster);
					pfree(arg1);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_tile: Invalid data type for band indexes");
					SRF_RETURN_DONE(funcctx);
					break;
			}

			deconstruct_array(array, etype, typlen, typbyval, typalign, &e, &nulls, &(arg1->numbands));

			arg1->nbands = palloc(sizeof(int) * arg1->numbands);
			if (arg1->nbands == NULL) {
				rt_raster_destroy(arg1->raster.raster);
				pfree(arg1);
				PG_FREE_IF_COPY(pgraster, 0);
				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_tile: Could not allocate memory for band indexes");
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
					rt_raster_destroy(arg1->raster.raster);
					pfree(arg1);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_tile: Could not reallocate memory for band indexes");
					SRF_RETURN_DONE(funcctx);
				}

				arg1->numbands = j;
			}

			/* validate nbands */
			for (i = 0; i < arg1->numbands; i++) {
				if (!rt_raster_has_band(arg1->raster.raster, arg1->nbands[i])) {
					elog(NOTICE, "Band at index %d not found in raster", arg1->nbands[i] + 1);
					rt_raster_destroy(arg1->raster.raster);
					pfree(arg1->nbands);
					pfree(arg1);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					SRF_RETURN_DONE(funcctx);
				}
			}
		}
		else {
			arg1->numbands = numbands;

			if (numbands) {
				arg1->nbands = palloc(sizeof(int) * arg1->numbands);

				if (arg1->nbands == NULL) {
					rt_raster_destroy(arg1->raster.raster);
					pfree(arg1);
					PG_FREE_IF_COPY(pgraster, 0);
					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_dumpValues: Could not allocate memory for pixel values");
					SRF_RETURN_DONE(funcctx);
				}

				for (i = 0; i < arg1->numbands; i++) {
					arg1->nbands[i] = i;
					POSTGIS_RT_DEBUGF(4, "arg1->nbands[%d] = %d", arg1->nbands[i], i);
				}
			}
		}

		/* pad (4) and padnodata (5) */
		if (!PG_ARGISNULL(4)) {
			arg1->pad.pad = PG_GETARG_BOOL(4) ? 1 : 0;

			if (arg1->pad.pad && !PG_ARGISNULL(5)) {
				arg1->pad.hasnodata = 1;
				arg1->pad.nodataval = PG_GETARG_FLOAT8(5);
			}
			else {
				arg1->pad.hasnodata = 0;
				arg1->pad.nodataval = 0;
			}
		}
		else {
			arg1->pad.pad = 0;
			arg1->pad.hasnodata = 0;
			arg1->pad.nodataval = 0;
		}

		/* store some additional metadata */
		arg1->raster.srid = rt_raster_get_srid(arg1->raster.raster);
		arg1->raster.width = rt_raster_get_width(arg1->raster.raster);
		arg1->raster.height = rt_raster_get_height(arg1->raster.raster);
		rt_raster_get_geotransform_matrix(arg1->raster.raster, arg1->raster.gt);

		/* determine maximum number of tiles from raster */
		arg1->tile.nx = ceil(arg1->raster.width / (double) arg1->tile.width);
		arg1->tile.ny = ceil(arg1->raster.height / (double) arg1->tile.height);
		POSTGIS_RT_DEBUGF(4, "# of tiles (x, y) = (%d, %d)", arg1->tile.nx, arg1->tile.ny);

		/* Store needed information */
		funcctx->user_fctx = arg1;

		/* total number of tuples to be returned */
		funcctx->max_calls = (arg1->tile.nx * arg1->tile.ny);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	arg2 = funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < max_calls) {
		rt_pgraster *pgtile = NULL;
		rt_raster tile = NULL;
		rt_band _band = NULL;
		rt_band band = NULL;
		rt_pixtype pixtype = PT_END;
		int hasnodata = 0;
		double nodataval = 0;
		int width = 0;
		int height = 0;

		int k = 0;
		int tx = 0;
		int ty = 0;
		int rx = 0;
		int ry = 0;
		int ex = 0; /* edge tile on right */
		int ey = 0; /* edge tile on bottom */
		double ulx = 0;
		double uly = 0;
		uint16_t len = 0;
		void *vals = NULL;
		uint16_t nvals;

		POSTGIS_RT_DEBUGF(3, "call number %d", call_cntr);

		/*
			find offset based upon tile #

			0 1 2
			3 4 5
			6 7 8
		*/
		ty = call_cntr / arg2->tile.nx;
		tx = call_cntr % arg2->tile.nx;
		POSTGIS_RT_DEBUGF(4, "tile (x, y) = (%d, %d)", tx, ty);

		/* edge tile? only important if padding is false */
		if (!arg2->pad.pad) {
			if (ty + 1 == arg2->tile.ny)
				ey = 1;
			if (tx + 1 == arg2->tile.nx)
				ex = 1;
		}

		/* upper-left of tile in raster coordinates */
		rx = tx * arg2->tile.width;
		ry = ty * arg2->tile.height;
		POSTGIS_RT_DEBUGF(4, "raster coordinates = %d, %d", rx, ry);

		/* determine tile width and height */
		/* default to user-defined */
		width = arg2->tile.width;
		height = arg2->tile.height;

		/* override user-defined if edge tile (only possible if padding is false */
		if (ex || ey) {
			/* right edge */
			if (ex)
				width = arg2->raster.width - rx;
			/* bottom edge */
			if (ey)
				height = arg2->raster.height - ry;
		}

		/* create empty raster */
		tile = rt_raster_new(width, height);
		rt_raster_set_geotransform_matrix(tile, arg2->raster.gt);
		rt_raster_set_srid(tile, arg2->raster.srid);

		/* upper-left of tile in spatial coordinates */
		if (rt_raster_cell_to_geopoint(arg2->raster.raster, rx, ry, &ulx, &uly, arg2->raster.gt) != ES_NONE) {
			rt_raster_destroy(tile);
			rt_raster_destroy(arg2->raster.raster);
			if (arg2->numbands) pfree(arg2->nbands);
			pfree(arg2);
			elog(ERROR, "RASTER_tile: Could not compute the coordinates of the upper-left corner of the output tile");
			SRF_RETURN_DONE(funcctx);
		}
		rt_raster_set_offsets(tile, ulx, uly);
		POSTGIS_RT_DEBUGF(4, "spatial coordinates = %f, %f", ulx, uly);

		/* compute length of pixel line to read */
		len = arg2->tile.width;
		if (rx + arg2->tile.width >= arg2->raster.width)
			len = arg2->raster.width - rx;
		POSTGIS_RT_DEBUGF(3, "read line len = %d", len);

		/* copy bands to tile */
		for (i = 0; i < arg2->numbands; i++) {
			POSTGIS_RT_DEBUGF(4, "copying band %d to tile %d", arg2->nbands[i], call_cntr);

			_band = rt_raster_get_band(arg2->raster.raster, arg2->nbands[i]);
			if (_band == NULL) {
				int nband = arg2->nbands[i] + 1;
				rt_raster_destroy(tile);
				rt_raster_destroy(arg2->raster.raster);
				if (arg2->numbands) pfree(arg2->nbands);
				pfree(arg2);
				elog(ERROR, "RASTER_tile: Could not get band %d from source raster", nband);
				SRF_RETURN_DONE(funcctx);
			}

			pixtype = rt_band_get_pixtype(_band);
			hasnodata = rt_band_get_hasnodata_flag(_band);
			if (hasnodata)
				rt_band_get_nodata(_band, &nodataval);
			else if (arg2->pad.pad && arg2->pad.hasnodata) {
				hasnodata = 1;
				nodataval = arg2->pad.nodataval;
			}
			else
				nodataval = rt_band_get_min_value(_band);

			/* inline band */
			if (!rt_band_is_offline(_band)) {
				if (rt_raster_generate_new_band(tile, pixtype, nodataval, hasnodata, nodataval, i) < 0) {
					rt_raster_destroy(tile);
					rt_raster_destroy(arg2->raster.raster);
					pfree(arg2->nbands);
					pfree(arg2);
					elog(ERROR, "RASTER_tile: Could not add new band to output tile");
					SRF_RETURN_DONE(funcctx);
				}
				band = rt_raster_get_band(tile, i);
				if (band == NULL) {
					rt_raster_destroy(tile);
					rt_raster_destroy(arg2->raster.raster);
					if (arg2->numbands) pfree(arg2->nbands);
					pfree(arg2);
					elog(ERROR, "RASTER_tile: Could not get newly added band from output tile");
					SRF_RETURN_DONE(funcctx);
				}

				/* if isnodata, set flag and continue */
				if (rt_band_get_isnodata_flag(_band)) {
					rt_band_set_isnodata_flag(band, 1);
					continue;
				}

				/* copy data */
				for (j = 0; j < arg2->tile.height; j++) {
					k = ry + j;

					if (k >= arg2->raster.height) {
						POSTGIS_RT_DEBUGF(4, "row %d is beyond extent of source raster. skipping", k);
						continue;
					}

					POSTGIS_RT_DEBUGF(4, "getting pixel line %d, %d for %d pixels", rx, k, len);
					if (rt_band_get_pixel_line(_band, rx, k, len, &vals, &nvals) != ES_NONE) {
						rt_raster_destroy(tile);
						rt_raster_destroy(arg2->raster.raster);
						if (arg2->numbands) pfree(arg2->nbands);
						pfree(arg2);
						elog(ERROR, "RASTER_tile: Could not get pixel line from source raster");
						SRF_RETURN_DONE(funcctx);
					}

					if (nvals && rt_band_set_pixel_line(band, 0, j, vals, nvals) != ES_NONE) {
						rt_raster_destroy(tile);
						rt_raster_destroy(arg2->raster.raster);
						if (arg2->numbands) pfree(arg2->nbands);
						pfree(arg2);
						elog(ERROR, "RASTER_tile: Could not set pixel line of output tile");
						SRF_RETURN_DONE(funcctx);
					}
				}
			}
			/* offline */
			else {
				uint8_t bandnum = 0;
				rt_band_get_ext_band_num(_band, &bandnum);

				band = rt_band_new_offline(
					width, height,
					pixtype,
					hasnodata, nodataval,
					bandnum, rt_band_get_ext_path(_band)
				);

				if (band == NULL) {
					rt_raster_destroy(tile);
					rt_raster_destroy(arg2->raster.raster);
					if (arg2->numbands) pfree(arg2->nbands);
					pfree(arg2);
					elog(ERROR, "RASTER_tile: Could not create new offline band for output tile");
					SRF_RETURN_DONE(funcctx);
				}

				if (rt_raster_add_band(tile, band, i) < 0) {
					rt_band_destroy(band);
					rt_raster_destroy(tile);
					rt_raster_destroy(arg2->raster.raster);
					if (arg2->numbands) pfree(arg2->nbands);
					pfree(arg2);
					elog(ERROR, "RASTER_tile: Could not add new offline band to output tile");
					SRF_RETURN_DONE(funcctx);
				}
			}
		}

		pgtile = rt_raster_serialize(tile);
		rt_raster_destroy(tile);
		if (!pgtile) {
			rt_raster_destroy(arg2->raster.raster);
			if (arg2->numbands) pfree(arg2->nbands);
			pfree(arg2);
			SRF_RETURN_DONE(funcctx);
		}

		SET_VARSIZE(pgtile, pgtile->size);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(pgtile));
	}
	/* do when there is no more left */
	else {
		rt_raster_destroy(arg2->raster.raster);
		if (arg2->numbands) pfree(arg2->nbands);
		pfree(arg2);
		SRF_RETURN_DONE(funcctx);
	}
}

/**
 * Return new raster from selected bands of existing raster through ST_Band.
 * second argument is an array of band numbers (1 based)
 */
PG_FUNCTION_INFO_V1(RASTER_band);
Datum RASTER_band(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster;
	rt_pgraster *pgrast;
	rt_raster raster;
	rt_raster rast;

	bool skip = FALSE;
	ArrayType *array;
	Oid etype;
	Datum *e;
	bool *nulls;
	int16 typlen;
	bool typbyval;
	char typalign;

	uint32_t numBands;
	uint32_t *bandNums;
	uint32 idx = 0;
	int n;
	int i = 0;
	int j = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_band: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* process bandNums */
	if (PG_ARGISNULL(1)) {
		elog(NOTICE, "Band number(s) not provided.  Returning original raster");
		skip = TRUE;
	}
	do {
		if (skip) break;

		numBands = rt_raster_get_num_bands(raster);

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
				elog(ERROR, "RASTER_band: Invalid data type for band number(s)");
				PG_RETURN_NULL();
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
        elog(NOTICE, "Invalid band index (must use 1-based). Returning original raster");
				skip = TRUE;
				break;
			}

			bandNums[j] = idx - 1;
			POSTGIS_RT_DEBUGF(3, "bandNums[%d] = %d", j, bandNums[j]);
			j++;
		}

		if (skip || j < 1) {
			pfree(bandNums);
			skip = TRUE;
		}
	}
	while (0);

	if (!skip) {
		rast = rt_raster_from_band(raster, bandNums, j);
		pfree(bandNums);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (!rast) {
			elog(ERROR, "RASTER_band: Could not create new raster");
			PG_RETURN_NULL();
		}

		pgrast = rt_raster_serialize(rast);
		rt_raster_destroy(rast);

		if (!pgrast)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrast, pgrast->size);
		PG_RETURN_POINTER(pgrast);
	}

	PG_RETURN_POINTER(pgraster);
}
