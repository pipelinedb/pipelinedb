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
 * Copyright (C) 2013 Nathaniel Hunter Clay <clay.nathaniel@gmail.com>
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

#include <assert.h>

#include <postgres.h> /* for palloc */
#include <fmgr.h>
#include <funcapi.h>
#include <executor/spi.h>
#include <utils/lsyscache.h> /* for get_typlenbyvalalign */
#include <utils/array.h> /* for ArrayType */
#include <utils/builtins.h>
#include <catalog/pg_type.h> /* for INT2OID, INT4OID, FLOAT4OID, FLOAT8OID and TEXTOID */
#include <executor/executor.h> /* for GetAttributeByName */

#include "../../postgis_config.h"
#include "lwgeom_pg.h"

#include "rtpostgis.h"
#include "rtpg_internal.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

/* n-raster MapAlgebra */
Datum RASTER_nMapAlgebra(PG_FUNCTION_ARGS);
Datum RASTER_nMapAlgebraExpr(PG_FUNCTION_ARGS);

/* raster union aggregate */
Datum RASTER_union_transfn(PG_FUNCTION_ARGS);
Datum RASTER_union_finalfn(PG_FUNCTION_ARGS);

/* raster clip */
Datum RASTER_clip(PG_FUNCTION_ARGS);

/* reclassify specified bands of a raster */
Datum RASTER_reclass(PG_FUNCTION_ARGS);

/* apply colormap to specified band of a raster */
Datum RASTER_colorMap(PG_FUNCTION_ARGS);

/* one-raster MapAlgebra */
Datum RASTER_mapAlgebraExpr(PG_FUNCTION_ARGS);
Datum RASTER_mapAlgebraFct(PG_FUNCTION_ARGS);

/* one-raster neighborhood MapAlgebra */
Datum RASTER_mapAlgebraFctNgb(PG_FUNCTION_ARGS);

/* two-raster MapAlgebra */
Datum RASTER_mapAlgebra2(PG_FUNCTION_ARGS);

/* ---------------------------------------------------------------- */
/*  n-raster MapAlgebra                                             */
/* ---------------------------------------------------------------- */

typedef struct {
	Oid ufc_noid;
	Oid ufc_rettype;
	FmgrInfo ufl_info;
	FunctionCallInfoData ufc_info;
} rtpg_nmapalgebra_callback_arg;

typedef struct rtpg_nmapalgebra_arg_t *rtpg_nmapalgebra_arg;
struct rtpg_nmapalgebra_arg_t {
	int numraster;
	rt_pgraster **pgraster;
	rt_raster *raster;
	uint8_t *isempty; /* flag indicating if raster is empty */
	uint8_t *ownsdata; /* is the raster self owned or just a pointer to another raster */
	int *nband; /* source raster's band index, 0-based */
	uint8_t *hasband; /* does source raster have band at index nband? */

	rt_pixtype pixtype; /* output raster band's pixel type */
	int hasnodata; /* NODATA flag */
	double nodataval; /* NODATA value */

	int distance[2]; /* distance in X and Y axis */

	rt_extenttype extenttype; /* ouput raster's extent type */
	rt_pgraster *pgcextent; /* custom extent of type rt_pgraster */
	rt_raster cextent; /* custom extent of type rt_raster */
        rt_mask mask; /* mask for the nmapalgebra operation */

	rtpg_nmapalgebra_callback_arg	callback;
};

static rtpg_nmapalgebra_arg rtpg_nmapalgebra_arg_init() {
	rtpg_nmapalgebra_arg arg = NULL;

	arg = palloc(sizeof(struct rtpg_nmapalgebra_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_nmapalgebra_arg_init: Could not allocate memory for arguments");
		return NULL;
	}

	arg->numraster = 0;
	arg->pgraster = NULL;
	arg->raster = NULL;
	arg->isempty = NULL;
	arg->ownsdata = NULL;
	arg->nband = NULL;
	arg->hasband = NULL;

	arg->pixtype = PT_END;
	arg->hasnodata = 1;
	arg->nodataval = 0;

	arg->distance[0] = 0;
	arg->distance[1] = 0;

	arg->extenttype = ET_INTERSECTION;
	arg->pgcextent = NULL;
	arg->cextent = NULL;
	arg->mask = NULL;

	arg->callback.ufc_noid = InvalidOid;
	arg->callback.ufc_rettype = InvalidOid;

	return arg;
}

static void rtpg_nmapalgebra_arg_destroy(rtpg_nmapalgebra_arg arg) {
	int i = 0;

	if (arg->raster != NULL) {
		for (i = 0; i < arg->numraster; i++) {
			if (arg->raster[i] == NULL || !arg->ownsdata[i])
				continue;

			rt_raster_destroy(arg->raster[i]);
		}

		pfree(arg->raster);
		pfree(arg->pgraster);
		pfree(arg->isempty);
		pfree(arg->ownsdata);
		pfree(arg->nband);
	}

	if (arg->cextent != NULL)
		rt_raster_destroy(arg->cextent);
	if( arg->mask != NULL )
	  pfree(arg->mask);

	pfree(arg);
}

static int rtpg_nmapalgebra_rastbandarg_process(rtpg_nmapalgebra_arg arg, ArrayType *array, int *allnull, int *allempty, int *noband) {
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

	int i;
	int j;
	int nband;

	if (arg == NULL || array == NULL) {
		elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: NULL values not permitted for parameters");
		return 0;
	}

	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(
		array,
		etype,
		typlen, typbyval, typalign,
		&e, &nulls, &n
	);

	if (!n) {
		elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Invalid argument for rastbandarg");
		return 0;
	}

	/* prep arg */
	arg->numraster = n;
	arg->pgraster = palloc(sizeof(rt_pgraster *) * arg->numraster);
	arg->raster = palloc(sizeof(rt_raster) * arg->numraster);
	arg->isempty = palloc(sizeof(uint8_t) * arg->numraster);
	arg->ownsdata = palloc(sizeof(uint8_t) * arg->numraster);
	arg->nband = palloc(sizeof(int) * arg->numraster);
	arg->hasband = palloc(sizeof(uint8_t) * arg->numraster);
	arg->mask = palloc(sizeof(struct rt_mask_t));
	if (
		arg->pgraster == NULL ||
		arg->raster == NULL ||
		arg->isempty == NULL ||
		arg->ownsdata == NULL ||
		arg->nband == NULL ||
		arg->hasband == NULL ||
		arg->mask == NULL
	) {
		elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Could not allocate memory for processing rastbandarg");
		return 0;
	}

	*allnull = 0;
	*allempty = 0;
	*noband = 0;

	/* process each element */
	for (i = 0; i < n; i++) {
		if (nulls[i]) {
			arg->numraster--;
			continue;
		}

		POSTGIS_RT_DEBUGF(4, "Processing rastbandarg at index %d", i);

		arg->raster[i] = NULL;
		arg->isempty[i] = 0;
		arg->ownsdata[i] = 1;
		arg->nband[i] = 0;
		arg->hasband[i] = 0;

		/* each element is a tuple */
		tup = (HeapTupleHeader) DatumGetPointer(e[i]);
		if (NULL == tup) {
			elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Invalid argument for rastbandarg at index %d", i);
			return 0;
		}

		/* first element, raster */
		POSTGIS_RT_DEBUG(4, "Processing first element (raster)");
		tupv = GetAttributeByName(tup, "rast", &isnull);
		if (isnull) {
			elog(NOTICE, "First argument (nband) of rastbandarg at index %d is NULL. Assuming NULL raster", i);
			arg->isempty[i] = 1;
			arg->ownsdata[i] = 0;

			(*allnull)++;
			(*allempty)++;
			(*noband)++;

			continue;
		}

		arg->pgraster[i] = (rt_pgraster *) PG_DETOAST_DATUM(tupv);

		/* see if this is a copy of an existing pgraster */
		for (j = 0; j < i; j++) {
			if (!arg->isempty[j] && (arg->pgraster[i] == arg->pgraster[j])) {
				POSTGIS_RT_DEBUG(4, "raster matching existing same raster found");
				arg->raster[i] = arg->raster[j];
				arg->ownsdata[i] = 0;
				break;
			}
		}

		if (arg->ownsdata[i]) {
			POSTGIS_RT_DEBUG(4, "deserializing raster");
			arg->raster[i] = rt_raster_deserialize(arg->pgraster[i], FALSE);
			if (arg->raster[i] == NULL) {
				elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Could not deserialize raster at index %d", i);
				return 0;
			}
		}

		/* is raster empty? */
		arg->isempty[i] = rt_raster_is_empty(arg->raster[i]);
		if (arg->isempty[i]) {
			(*allempty)++;
			(*noband)++;

			continue;
		}

		/* second element, nband */
		POSTGIS_RT_DEBUG(4, "Processing second element (nband)");
		tupv = GetAttributeByName(tup, "nband", &isnull);
		if (isnull) {
			nband = 1;
			elog(NOTICE, "First argument (nband) of rastbandarg at index %d is NULL. Assuming nband = %d", i, nband);
		}
		else
			nband = DatumGetInt32(tupv);

		if (nband < 1) {
			elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Band number provided for rastbandarg at index %d must be greater than zero (1-based)", i);
			return 0;
		}

		arg->nband[i] = nband - 1;
		arg->hasband[i] = rt_raster_has_band(arg->raster[i], arg->nband[i]);
		if (!arg->hasband[i]) {
			(*noband)++;
			POSTGIS_RT_DEBUGF(4, "Band at index %d not found in raster", nband);
		}
	}

	if (arg->numraster < n) {
		arg->pgraster = repalloc(arg->pgraster, sizeof(rt_pgraster *) * arg->numraster);
		arg->raster = repalloc(arg->raster, sizeof(rt_raster) * arg->numraster);
		arg->isempty = repalloc(arg->isempty, sizeof(uint8_t) * arg->numraster);
		arg->ownsdata = repalloc(arg->ownsdata, sizeof(uint8_t) * arg->numraster);
		arg->nband = repalloc(arg->nband, sizeof(int) * arg->numraster);
		arg->hasband = repalloc(arg->hasband, sizeof(uint8_t) * arg->numraster);
		if (
			arg->pgraster == NULL ||
			arg->raster == NULL ||
			arg->isempty == NULL ||
			arg->ownsdata == NULL ||
			arg->nband == NULL ||
			arg->hasband == NULL
		) {
			elog(ERROR, "rtpg_nmapalgebra_rastbandarg_process: Could not reallocate memory for processed rastbandarg");
			return 0;
		}
	}

	POSTGIS_RT_DEBUGF(4, "arg->numraster = %d", arg->numraster);

	return 1;
}

/*
	Callback for RASTER_nMapAlgebra
*/
static int rtpg_nmapalgebra_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	rtpg_nmapalgebra_callback_arg *callback = (rtpg_nmapalgebra_callback_arg *) userarg;

	int16 typlen;
	bool typbyval;
	char typalign;

	ArrayType *mdValues = NULL;
	Datum *_values = NULL;
	bool *_nodata = NULL;

	ArrayType *mdPos = NULL;
	Datum *_pos = NULL;
	bool *_null = NULL;

	int i = 0;
	int x = 0;
	int y = 0;
	int z = 0;
	int dim[3] = {0};
	int lbound[3] = {1, 1, 1};
	Datum datum = (Datum) NULL;

	if (arg == NULL)
		return 0;

	*value = 0;
	*nodata = 0;

	dim[0] = arg->rasters;
	dim[1] = arg->rows;
	dim[2] = arg->columns;

	_values = palloc(sizeof(Datum) * arg->rasters * arg->rows * arg->columns);
	_nodata = palloc(sizeof(bool) * arg->rasters * arg->rows * arg->columns);
	if (_values == NULL || _nodata == NULL) {
		elog(ERROR, "rtpg_nmapalgebra_callback: Could not allocate memory for values array");
		return 0;
	}

	/* build mdValues */
	i = 0;
	/* raster */
	for (z = 0; z < arg->rasters; z++) {
		/* Y axis */
		for (y = 0; y < arg->rows; y++) {
			/* X axis */
			for (x = 0; x < arg->columns; x++) {
				POSTGIS_RT_DEBUGF(4, "(z, y ,x) = (%d, %d, %d)", z, y, x);
				POSTGIS_RT_DEBUGF(4, "(value, nodata) = (%f, %d)", arg->values[z][y][x], arg->nodata[z][y][x]);

				_nodata[i] = (bool) arg->nodata[z][y][x];
				if (!_nodata[i])
					_values[i] = Float8GetDatum(arg->values[z][y][x]);
				else
					_values[i] = (Datum) NULL;

				i++;
			}
		}
	}

	/* info about the type of item in the multi-dimensional array (float8). */
	get_typlenbyvalalign(FLOAT8OID, &typlen, &typbyval, &typalign);

	/* construct mdValues */
	mdValues = construct_md_array(
		_values, _nodata,
		3, dim, lbound,
		FLOAT8OID,
		typlen, typbyval, typalign
	);
	pfree(_nodata);
	pfree(_values);

	_pos = palloc(sizeof(Datum) * (arg->rasters + 1) * 2);
	_null = palloc(sizeof(bool) * (arg->rasters + 1) * 2);
	if (_pos == NULL || _null == NULL) {
		pfree(mdValues);
		elog(ERROR, "rtpg_nmapalgebra_callback: Could not allocate memory for position array");
		return 0;
	}
	memset(_null, 0, sizeof(bool) * (arg->rasters + 1) * 2);

	/* build mdPos */
	i = 0;
	_pos[i] = arg->dst_pixel[0] + 1;
	i++;
	_pos[i] = arg->dst_pixel[1] + 1;
	i++;

	for (z = 0; z < arg->rasters; z++) {
		_pos[i] = arg->src_pixel[z][0] + 1;
		i++;

		_pos[i] = arg->src_pixel[z][1] + 1;
		i++;
	}

	/* info about the type of item in the multi-dimensional array (int4). */
	get_typlenbyvalalign(INT4OID, &typlen, &typbyval, &typalign);

	/* reuse dim and lbound, just tweak to what we need */
	dim[0] = arg->rasters + 1;
	dim[1] = 2;
	lbound[0] = 0;

	/* construct mdPos */
	mdPos = construct_md_array(
		_pos, _null,
		2, dim, lbound,
		INT4OID,
		typlen, typbyval, typalign
	);
	pfree(_pos);
	pfree(_null);

	callback->ufc_info.arg[0] = PointerGetDatum(mdValues);
	callback->ufc_info.arg[1] = PointerGetDatum(mdPos);

	/* call user callback function */
	datum = FunctionCallInvoke(&(callback->ufc_info));
	pfree(mdValues);
	pfree(mdPos);

	/* result is not null*/
	if (!callback->ufc_info.isnull) {
		switch (callback->ufc_rettype) {
			case FLOAT8OID:
				*value = DatumGetFloat8(datum);
				break;
			case FLOAT4OID:
				*value = (double) DatumGetFloat4(datum);
				break;
			case INT4OID:
				*value = (double) DatumGetInt32(datum);
				break;
			case INT2OID:
				*value = (double) DatumGetInt16(datum);
				break;
		}
	}
	else
		*nodata = 1;

	return 1;
}

/*
 ST_MapAlgebra for n rasters
*/
PG_FUNCTION_INFO_V1(RASTER_nMapAlgebra);
Datum RASTER_nMapAlgebra(PG_FUNCTION_ARGS)
{
	rtpg_nmapalgebra_arg arg = NULL;
	rt_iterator itrset;
	ArrayType *maskArray;
	Oid etype;
	Datum *maskElements;
	bool *maskNulls;
	int16 typlen;
	bool typbyval;
	char typalign;
	int ndims = 0;
	int num;
	int *maskDims;
	int x,y;
	

	int i = 0;
	int noerr = 0;
	int allnull = 0;
	int allempty = 0;
	int noband = 0;

	rt_raster raster = NULL;
	rt_band band = NULL;
	rt_pgraster *pgraster = NULL;

	POSTGIS_RT_DEBUG(3, "Starting...");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* init argument struct */
	arg = rtpg_nmapalgebra_arg_init();
	if (arg == NULL) {
		elog(ERROR, "RASTER_nMapAlgebra: Could not initialize argument structure");
		PG_RETURN_NULL();
	}

	/* let helper function process rastbandarg (0) */
	if (!rtpg_nmapalgebra_rastbandarg_process(arg, PG_GETARG_ARRAYTYPE_P(0), &allnull, &allempty, &noband)) {
		rtpg_nmapalgebra_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebra: Could not process rastbandarg");
		PG_RETURN_NULL();
	}

	POSTGIS_RT_DEBUGF(4, "allnull, allempty, noband = %d, %d, %d", allnull, allempty, noband);

	/* all rasters are NULL, return NULL */
	if (allnull == arg->numraster) {
		elog(NOTICE, "All input rasters are NULL. Returning NULL");
		rtpg_nmapalgebra_arg_destroy(arg);
		PG_RETURN_NULL();
	}

	/* pixel type (2) */
	if (!PG_ARGISNULL(2)) {
		char *pixtypename = text_to_cstring(PG_GETARG_TEXT_P(2));

		/* Get the pixel type index */
		arg->pixtype = rt_pixtype_index_from_name(pixtypename);
		if (arg->pixtype == PT_END) {
			rtpg_nmapalgebra_arg_destroy(arg);
			elog(ERROR, "RASTER_nMapAlgebra: Invalid pixel type: %s", pixtypename);
			PG_RETURN_NULL();
		}
	}

	/* distancex (3) */
	if (!PG_ARGISNULL(3)){
		arg->distance[0] = PG_GETARG_INT32(3);
	}else{
	        arg->distance[0] = 0;
	}
        /* distancey (4) */
	if (!PG_ARGISNULL(4)){
		arg->distance[1] = PG_GETARG_INT32(4);
	}else{
	        arg->distance[1] = 0;
	}
	if (arg->distance[0] < 0 || arg->distance[1] < 0) {
		rtpg_nmapalgebra_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebra: Distance for X and Y axis must be greater than or equal to zero");
		PG_RETURN_NULL();
	}

	/* extent type (5) */
	if (!PG_ARGISNULL(5)) {
		char *extenttypename = rtpg_strtoupper(rtpg_trim(text_to_cstring(PG_GETARG_TEXT_P(5))));
		arg->extenttype = rt_util_extent_type(extenttypename);
	}
	POSTGIS_RT_DEBUGF(4, "extenttype: %d", arg->extenttype);

	/* custom extent (6) */
	if (arg->extenttype == ET_CUSTOM) {
		if (PG_ARGISNULL(6)) {
			elog(NOTICE, "Custom extent is NULL. Returning NULL");
			rtpg_nmapalgebra_arg_destroy(arg);
			PG_RETURN_NULL();
		}

		arg->pgcextent = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(6));

		/* only need the raster header */
		arg->cextent = rt_raster_deserialize(arg->pgcextent, TRUE);
		if (arg->cextent == NULL) {
			rtpg_nmapalgebra_arg_destroy(arg);
			elog(ERROR, "RASTER_nMapAlgebra: Could not deserialize custom extent");
			PG_RETURN_NULL();
		}
		else if (rt_raster_is_empty(arg->cextent)) {
			elog(NOTICE, "Custom extent is an empty raster. Returning empty raster");
			rtpg_nmapalgebra_arg_destroy(arg);

			raster = rt_raster_new(0, 0);
			if (raster == NULL) {
				elog(ERROR, "RASTER_nMapAlgebra: Could not create empty raster");
				PG_RETURN_NULL();
			}

			pgraster = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			if (!pgraster) PG_RETURN_NULL();

			SET_VARSIZE(pgraster, pgraster->size);
			PG_RETURN_POINTER(pgraster);
		}
	}

	noerr = 1;
	
	/* mask (7) */

	if( PG_ARGISNULL(7) ){
	  pfree(arg->mask);
	  arg->mask = NULL;
	}else{
	maskArray = PG_GETARG_ARRAYTYPE_P(7);
	etype = ARR_ELEMTYPE(maskArray);
	get_typlenbyvalalign(etype,&typlen,&typbyval,&typalign);
	
	switch(etype){
	      case FLOAT4OID:
	      case FLOAT8OID:
		break;
	      default:
		rtpg_nmapalgebra_arg_destroy(arg);
		elog(ERROR,"RASTER_nMapAlgerbra: Mask data type must be FLOAT8 or FLOAT4.");
		PG_RETURN_NULL();
	}

	ndims = ARR_NDIM(maskArray);
	
	if( ndims != 2 ){ 
	  elog(ERROR, "RASTER_nMapAlgerbra: Mask Must be a 2D array.");
	  rtpg_nmapalgebra_arg_destroy(arg);
	  PG_RETURN_NULL();
	}
	
	maskDims = ARR_DIMS(maskArray);


	if ( maskDims[0] % 2 == 0 || maskDims[1] % 2 == 0 ){
	  elog(ERROR,"RASTER_nMapAlgerbra: Mask dimenstions must be odd.");
	  rtpg_nmapalgebra_arg_destroy(arg);
	  PG_RETURN_NULL();
	} 
	
	deconstruct_array(
			  maskArray,
			  etype,
			  typlen, typbyval,typalign,
			  &maskElements,&maskNulls,&num
			  );

	if (num < 1 || num != (maskDims[0] * maskDims[1])) {
                if (num) {
                        pfree(maskElements);
                        pfree(maskNulls);
                }
		elog(ERROR, "RASTER_nMapAlgerbra: Could not deconstruct new values array.");
                rtpg_nmapalgebra_arg_destroy(arg);
		PG_RETURN_NULL();
	}


	/* allocate mem for mask array */
	arg->mask->values = palloc(sizeof(double*)* maskDims[0]);
	arg->mask->nodata =  palloc(sizeof(int*)*maskDims[0]);
	for(i = 0; i < maskDims[0]; i++){
	  arg->mask->values[i] = (double*) palloc(sizeof(double) * maskDims[1]);
	  arg->mask->nodata[i] = (int*) palloc(sizeof(int) * maskDims[1]);
	}
	/* place values in to mask */
	i = 0;
	for( y = 0; y < maskDims[0]; y++ ){
	  for( x = 0; x < maskDims[1]; x++){
	    if(maskNulls[i]){
	      arg->mask->values[y][x] = 0;
	      arg->mask->nodata[y][x] = 1;
	    }else{
	      switch(etype){
	      case FLOAT4OID:
		arg->mask->values[y][x] = (double) DatumGetFloat4(maskElements[i]);
		arg->mask->nodata[y][x] = 0;
		break;
	      case FLOAT8OID:
		arg->mask->values[y][x] = (double) DatumGetFloat8(maskElements[i]);
		arg->mask->nodata[y][x] = 0;
	      }
	    }
	    i++;
	  }
	}
	/*set mask dimenstions*/ 
	arg->mask->dimx = maskDims[0];
	arg->mask->dimy = maskDims[1];
	if ( maskDims[0] == 1 && maskDims[1] == 1){
	  arg->distance[0] = 0;
	  arg->distance[1] = 0;
	}else{
	  arg->distance[0] = maskDims[0] % 2;
	  arg->distance[1] = maskDims[1] % 2;
	}
	}/*end if else argisnull*/

	/* (8) weighted boolean */
	if (PG_ARGISNULL(8) || !PG_GETARG_BOOL(8) ){
	  if ( arg->mask != NULL ) 
	    arg->mask->weighted = 0;
	}else{
	  if(arg->mask !=NULL )
	    arg->mask->weighted = 1;
	}

	/* all rasters are empty, return empty raster */
	if (allempty == arg->numraster) {
		elog(NOTICE, "All input rasters are empty. Returning empty raster");
		noerr = 0;
	}
	/* all rasters don't have indicated band, return empty raster */
	else if (noband == arg->numraster) {
		elog(NOTICE, "All input rasters do not have bands at indicated indexes. Returning empty raster");
		noerr = 0;
	}
	if (!noerr) {
		rtpg_nmapalgebra_arg_destroy(arg);

		raster = rt_raster_new(0, 0);
		if (raster == NULL) {
			elog(ERROR, "RASTER_nMapAlgebra: Could not create empty raster");
			PG_RETURN_NULL();
		}

		pgraster = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (!pgraster) PG_RETURN_NULL();

		SET_VARSIZE(pgraster, pgraster->size);
		PG_RETURN_POINTER(pgraster);
	}

	/* do regprocedure last (1) */
	if (!PG_ARGISNULL(1) || get_fn_expr_argtype(fcinfo->flinfo, 1) == REGPROCEDUREOID) {
		POSTGIS_RT_DEBUG(4, "processing callbackfunc");
		arg->callback.ufc_noid = PG_GETARG_OID(1);

		/* get function info */
		fmgr_info(arg->callback.ufc_noid, &(arg->callback.ufl_info));

		/* function cannot return set */
		noerr = 0;
		if (arg->callback.ufl_info.fn_retset) {
			noerr = 1;
		}
		/* function should have correct # of args */
		else if (arg->callback.ufl_info.fn_nargs != 3) {
			noerr = 2;
		}

		/* check that callback function return type is supported */
		if (
			get_func_result_type(
				arg->callback.ufc_noid,
				&(arg->callback.ufc_rettype),
				NULL
			) != TYPEFUNC_SCALAR
		) {
			noerr = 3;
		}

		if (!(
			arg->callback.ufc_rettype == FLOAT8OID ||
			arg->callback.ufc_rettype == FLOAT4OID ||
			arg->callback.ufc_rettype == INT4OID ||
			arg->callback.ufc_rettype == INT2OID
		)) {
			noerr = 4;
		}

		/*
			TODO: consider adding checks of the userfunction parameters
				should be able to use get_fn_expr_argtype() of fmgr.c
		*/

		if (noerr != 0) {
			rtpg_nmapalgebra_arg_destroy(arg);
			switch (noerr) {
				case 4:
					elog(ERROR, "RASTER_nMapAlgebra: Function provided must return a double precision, float, int or smallint");
					break;
				case 3:
					elog(ERROR, "RASTER_nMapAlgebra: Function provided must return scalar (double precision, float, int, smallint)");
					break;
				case 2:
					elog(ERROR, "RASTER_nMapAlgebra: Function provided must have three input parameters");
					break;
				case 1:
					elog(ERROR, "RASTER_nMapAlgebra: Function provided must return double precision, not resultset");
					break;
			}
			PG_RETURN_NULL();
		}

		if (func_volatile(arg->callback.ufc_noid) == 'v')
			elog(NOTICE, "Function provided is VOLATILE. Unless required and for best performance, function should be IMMUTABLE or STABLE");

		/* prep function call data */
#if POSTGIS_PGSQL_VERSION > 90
		InitFunctionCallInfoData(arg->callback.ufc_info, &(arg->callback.ufl_info), arg->callback.ufl_info.fn_nargs, InvalidOid, NULL, NULL);
#else
		InitFunctionCallInfoData(arg->callback.ufc_info, &(arg->callback.ufl_info), arg->callback.ufl_info.fn_nargs, NULL, NULL);
#endif
		memset(arg->callback.ufc_info.argnull, FALSE, sizeof(bool) * arg->callback.ufl_info.fn_nargs);

		/* userargs (7) */
		if (!PG_ARGISNULL(9))
			arg->callback.ufc_info.arg[2] = PG_GETARG_DATUM(9);
		else {
      if (arg->callback.ufl_info.fn_strict) {
				/* build and assign an empty TEXT array */
				/* TODO: manually free the empty array? */
				arg->callback.ufc_info.arg[2] = PointerGetDatum(
					construct_empty_array(TEXTOID)
				);
				arg->callback.ufc_info.argnull[2] = FALSE;
      }
			else {
				arg->callback.ufc_info.arg[2] = (Datum) NULL;
				arg->callback.ufc_info.argnull[2] = TRUE;
			}
		}
	}
	else {
		rtpg_nmapalgebra_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebra: callbackfunc must be provided");
		PG_RETURN_NULL();
	}

	/* determine nodataval and possibly pixtype */
	/* band to check */
	switch (arg->extenttype) {
		case ET_LAST:
			i = arg->numraster - 1;
			break;
		case ET_SECOND:
			if (arg->numraster > 1) {
				i = 1;
				break;
			}
		default:
			i = 0;
			break;
	}
	/* find first viable band */
	if (!arg->hasband[i]) {
		for (i = 0; i < arg->numraster; i++) {
			if (arg->hasband[i])
				break;
		}
		if (i >= arg->numraster)
			i = arg->numraster - 1;
	}
	band = rt_raster_get_band(arg->raster[i], arg->nband[i]);

	/* set pixel type if PT_END */
	if (arg->pixtype == PT_END)
		arg->pixtype = rt_band_get_pixtype(band);

	/* set hasnodata and nodataval */
	arg->hasnodata = 1;
	if (rt_band_get_hasnodata_flag(band))
		rt_band_get_nodata(band, &(arg->nodataval));
	else
		arg->nodataval = rt_band_get_min_value(band);

	POSTGIS_RT_DEBUGF(4, "pixtype, hasnodata, nodataval: %s, %d, %f", rt_pixtype_name(arg->pixtype), arg->hasnodata, arg->nodataval);

	/* init itrset */
	itrset = palloc(sizeof(struct rt_iterator_t) * arg->numraster);
	if (itrset == NULL) {
		rtpg_nmapalgebra_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebra: Could not allocate memory for iterator arguments");
		PG_RETURN_NULL();
	}

	/* set itrset */
	for (i = 0; i < arg->numraster; i++) {
		itrset[i].raster = arg->raster[i];
		itrset[i].nband = arg->nband[i];
		itrset[i].nbnodata = 1;
	}

	/* pass everything to iterator */
	noerr = rt_raster_iterator(
		itrset, arg->numraster,
		arg->extenttype, arg->cextent,
		arg->pixtype,
		arg->hasnodata, arg->nodataval,
		arg->distance[0], arg->distance[1],
		arg->mask,
		&(arg->callback),
		rtpg_nmapalgebra_callback,
		&raster
	);

	/* cleanup */
	pfree(itrset);
	rtpg_nmapalgebra_arg_destroy(arg);

	if (noerr != ES_NONE) {
		elog(ERROR, "RASTER_nMapAlgebra: Could not run raster iterator function");
		PG_RETURN_NULL();
	}
	else if (raster == NULL)
		PG_RETURN_NULL();

	pgraster = rt_raster_serialize(raster);
	rt_raster_destroy(raster);

	POSTGIS_RT_DEBUG(3, "Finished");

	if (!pgraster)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, pgraster->size);
	PG_RETURN_POINTER(pgraster);
}

/* ---------------------------------------------------------------- */
/* expression ST_MapAlgebra for n rasters                           */
/* ---------------------------------------------------------------- */

typedef struct {
	int exprcount;

	struct {
		SPIPlanPtr spi_plan;
		uint32_t spi_argcount;
		uint8_t *spi_argpos;

		int hasval;
		double val;
	} expr[3];

	struct {
		int hasval;
		double val;
	} nodatanodata;

	struct {
		int count;
		char **val;
	} kw;

} rtpg_nmapalgebraexpr_callback_arg;

typedef struct rtpg_nmapalgebraexpr_arg_t *rtpg_nmapalgebraexpr_arg;
struct rtpg_nmapalgebraexpr_arg_t {
	rtpg_nmapalgebra_arg bandarg;

	rtpg_nmapalgebraexpr_callback_arg	callback;
};

static rtpg_nmapalgebraexpr_arg rtpg_nmapalgebraexpr_arg_init(int cnt, char **kw) {
	rtpg_nmapalgebraexpr_arg arg = NULL;
	int i = 0;

	arg = palloc(sizeof(struct rtpg_nmapalgebraexpr_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_nmapalgebraexpr_arg_init: Could not allocate memory for arguments");
		return NULL;
	}

	arg->bandarg = rtpg_nmapalgebra_arg_init();
	if (arg->bandarg == NULL) {
		elog(ERROR, "rtpg_nmapalgebraexpr_arg_init: Could not allocate memory for arg->bandarg");
		return NULL;
	}

	arg->callback.kw.count = cnt;
	arg->callback.kw.val = kw;

	arg->callback.exprcount = 3;
	for (i = 0; i < arg->callback.exprcount; i++) {
		arg->callback.expr[i].spi_plan = NULL;
		arg->callback.expr[i].spi_argcount = 0;
		arg->callback.expr[i].spi_argpos = palloc(cnt * sizeof(uint8_t));
		if (arg->callback.expr[i].spi_argpos == NULL) {
			elog(ERROR, "rtpg_nmapalgebraexpr_arg_init: Could not allocate memory for spi_argpos");
			return NULL;
		}
		memset(arg->callback.expr[i].spi_argpos, 0, sizeof(uint8_t) * cnt);
		arg->callback.expr[i].hasval = 0;
		arg->callback.expr[i].val = 0;
	}

	arg->callback.nodatanodata.hasval = 0;
	arg->callback.nodatanodata.val = 0;

	return arg;
}

static void rtpg_nmapalgebraexpr_arg_destroy(rtpg_nmapalgebraexpr_arg arg) {
	int i = 0;

	rtpg_nmapalgebra_arg_destroy(arg->bandarg);

	for (i = 0; i < arg->callback.exprcount; i++) {
		if (arg->callback.expr[i].spi_plan)
			SPI_freeplan(arg->callback.expr[i].spi_plan);
		if (arg->callback.kw.count)
			pfree(arg->callback.expr[i].spi_argpos);
	}

	pfree(arg);
}

static int rtpg_nmapalgebraexpr_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	rtpg_nmapalgebraexpr_callback_arg *callback = (rtpg_nmapalgebraexpr_callback_arg *) userarg;
	SPIPlanPtr plan = NULL;
	int i = 0;
	int id = -1;

	if (arg == NULL)
		return 0;

	*value = 0;
	*nodata = 0;

	/* 2 raster */
	if (arg->rasters > 1) {
		/* nodata1 = 1 AND nodata2 = 1, nodatanodataval */
		if (arg->nodata[0][0][0] && arg->nodata[1][0][0]) {
			if (callback->nodatanodata.hasval)
				*value = callback->nodatanodata.val;
			else
				*nodata = 1;
		}
		/* nodata1 = 1 AND nodata2 != 1, nodata1expr */
		else if (arg->nodata[0][0][0] && !arg->nodata[1][0][0]) {
			id = 1;
			if (callback->expr[id].hasval)
				*value = callback->expr[id].val;
			else if (callback->expr[id].spi_plan)
				plan = callback->expr[id].spi_plan;
			else
				*nodata = 1;
		}
		/* nodata1 != 1 AND nodata2 = 1, nodata2expr */
		else if (!arg->nodata[0][0][0] && arg->nodata[1][0][0]) {
			id = 2;
			if (callback->expr[id].hasval)
				*value = callback->expr[id].val;
			else if (callback->expr[id].spi_plan)
				plan = callback->expr[id].spi_plan;
			else
				*nodata = 1;
		}
		/* expression */
		else {
			id = 0;
			if (callback->expr[id].hasval)
				*value = callback->expr[id].val;
			else if (callback->expr[id].spi_plan)
				plan = callback->expr[id].spi_plan;
			else {
				if (callback->nodatanodata.hasval)
					*value = callback->nodatanodata.val;
				else
					*nodata = 1;
			}
		}
	}
	/* 1 raster */
	else {
		/* nodata = 1, nodata1expr */
		if (arg->nodata[0][0][0]) {
			id = 1;
			if (callback->expr[id].hasval)
				*value = callback->expr[id].val;
			else if (callback->expr[id].spi_plan)
				plan = callback->expr[id].spi_plan;
			else
				*nodata = 1;
		}
		/* expression */
		else {
			id = 0;
			if (callback->expr[id].hasval)
				*value = callback->expr[id].val;
			else if (callback->expr[id].spi_plan)
				plan = callback->expr[id].spi_plan;
			else {
				/* see if nodata1expr is available */
				id = 1;
				if (callback->expr[id].hasval)
					*value = callback->expr[id].val;
				else if (callback->expr[id].spi_plan)
					plan = callback->expr[id].spi_plan;
				else
					*nodata = 1;
			}
		}
	}

	/* run prepared plan */
	if (plan != NULL) {
		Datum values[12];
		bool nulls[12];
		int err = 0;

		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;
		Datum datum;
		bool isnull = FALSE;

		POSTGIS_RT_DEBUGF(4, "Running plan %d", id);

		/* init values and nulls */
		memset(values, (Datum) NULL, sizeof(Datum) * callback->kw.count);
		memset(nulls, FALSE, sizeof(bool) * callback->kw.count);

		if (callback->expr[id].spi_argcount) {
			int idx = 0;

			for (i = 0; i < callback->kw.count; i++) {
				idx = callback->expr[id].spi_argpos[i];
				if (idx < 1) continue;
				idx--; /* 1-based now 0-based */

				switch (i) {
					/* [rast.x] */
					case 0:
						values[idx] = Int32GetDatum(arg->src_pixel[0][0] + 1);
						break;
					/* [rast.y] */
					case 1:
						values[idx] = Int32GetDatum(arg->src_pixel[0][1] + 1);
						break;
					/* [rast.val] */
					case 2:
					/* [rast] */
					case 3:
						if (!arg->nodata[0][0][0])
							values[idx] = Float8GetDatum(arg->values[0][0][0]);
						else
							nulls[idx] = TRUE;
						break;

					/* [rast1.x] */
					case 4:
						values[idx] = Int32GetDatum(arg->src_pixel[0][0] + 1);
						break;
					/* [rast1.y] */
					case 5:
						values[idx] = Int32GetDatum(arg->src_pixel[0][1] + 1);
						break;
					/* [rast1.val] */
					case 6:
					/* [rast1] */
					case 7:
						if (!arg->nodata[0][0][0])
							values[idx] = Float8GetDatum(arg->values[0][0][0]);
						else
							nulls[idx] = TRUE;
						break;

					/* [rast2.x] */
					case 8:
						values[idx] = Int32GetDatum(arg->src_pixel[1][0] + 1);
						break;
					/* [rast2.y] */
					case 9:
						values[idx] = Int32GetDatum(arg->src_pixel[1][1] + 1);
						break;
					/* [rast2.val] */
					case 10:
					/* [rast2] */
					case 11:
						if (!arg->nodata[1][0][0])
							values[idx] = Float8GetDatum(arg->values[1][0][0]);
						else
							nulls[idx] = TRUE;
						break;
				}

			}
		}

		/* run prepared plan */
		err = SPI_execute_plan(plan, values, nulls, TRUE, 1);
		if (err != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {
			elog(ERROR, "rtpg_nmapalgebraexpr_callback: Unexpected error when running prepared statement %d", id);
			return 0;
		}

		/* get output of prepared plan */
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (SPI_result == SPI_ERROR_NOATTRIBUTE) {
			if (SPI_tuptable) SPI_freetuptable(tuptable);
			elog(ERROR, "rtpg_nmapalgebraexpr_callback: Could not get result of prepared statement %d", id);
			return 0;
		}

		if (!isnull) {
			*value = DatumGetFloat8(datum);
			POSTGIS_RT_DEBUG(4, "Getting value from Datum");
		}
		else {
			/* 2 raster, check nodatanodataval */
			if (arg->rasters > 1) {
				if (callback->nodatanodata.hasval)
					*value = callback->nodatanodata.val;
				else
					*nodata = 1;
			}
			/* 1 raster, check nodataval */
			else {
				if (callback->expr[1].hasval)
					*value = callback->expr[1].val;
				else
					*nodata = 1;
			}
		}

		if (SPI_tuptable) SPI_freetuptable(tuptable);
	}

	POSTGIS_RT_DEBUGF(4, "(value, nodata) = (%f, %d)", *value, *nodata);
	return 1;
}

PG_FUNCTION_INFO_V1(RASTER_nMapAlgebraExpr);
Datum RASTER_nMapAlgebraExpr(PG_FUNCTION_ARGS)
{
	MemoryContext mainMemCtx = CurrentMemoryContext;
	rtpg_nmapalgebraexpr_arg arg = NULL;
	rt_iterator itrset;
	uint16_t exprpos[3] = {1, 4, 5};

	int i = 0;
	int j = 0;
	int k = 0;

	int numraster = 0;
	int err = 0;
	int allnull = 0;
	int allempty = 0;
	int noband = 0;
	int len = 0;

	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple;
	Datum datum;
	bool isnull = FALSE;

	rt_raster raster = NULL;
	rt_band band = NULL;
	rt_pgraster *pgraster = NULL;

	const int argkwcount = 12;
	char *argkw[] = {
		"[rast.x]",
		"[rast.y]",
		"[rast.val]",
		"[rast]",
		"[rast1.x]",
		"[rast1.y]",
		"[rast1.val]",
		"[rast1]",
		"[rast2.x]",
		"[rast2.y]",
		"[rast2.val]",
		"[rast2]"
	};

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* init argument struct */
	arg = rtpg_nmapalgebraexpr_arg_init(argkwcount, argkw);
	if (arg == NULL) {
		elog(ERROR, "RASTER_nMapAlgebraExpr: Could not initialize argument structure");
		PG_RETURN_NULL();
	}

	/* let helper function process rastbandarg (0) */
	if (!rtpg_nmapalgebra_rastbandarg_process(arg->bandarg, PG_GETARG_ARRAYTYPE_P(0), &allnull, &allempty, &noband)) {
		rtpg_nmapalgebraexpr_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebra: Could not process rastbandarg");
		PG_RETURN_NULL();
	}

	POSTGIS_RT_DEBUGF(4, "allnull, allempty, noband = %d, %d, %d", allnull, allempty, noband);

	/* all rasters are NULL, return NULL */
	if (allnull == arg->bandarg->numraster) {
		elog(NOTICE, "All input rasters are NULL. Returning NULL");
		rtpg_nmapalgebraexpr_arg_destroy(arg);
		PG_RETURN_NULL();
	}

	/* only work on one or two rasters */
	if (arg->bandarg->numraster > 1)
		numraster = 2;
	else
		numraster = 1;

	/* pixel type (2) */
	if (!PG_ARGISNULL(2)) {
		char *pixtypename = text_to_cstring(PG_GETARG_TEXT_P(2));

		/* Get the pixel type index */
		arg->bandarg->pixtype = rt_pixtype_index_from_name(pixtypename);
		if (arg->bandarg->pixtype == PT_END) {
			rtpg_nmapalgebraexpr_arg_destroy(arg);
			elog(ERROR, "RASTER_nMapAlgebraExpr: Invalid pixel type: %s", pixtypename);
			PG_RETURN_NULL();
		}
	}
	POSTGIS_RT_DEBUGF(4, "pixeltype: %d", arg->bandarg->pixtype);

	/* extent type (3) */
	if (!PG_ARGISNULL(3)) {
		char *extenttypename = rtpg_strtoupper(rtpg_trim(text_to_cstring(PG_GETARG_TEXT_P(3))));
		arg->bandarg->extenttype = rt_util_extent_type(extenttypename);
	}

	if (arg->bandarg->extenttype == ET_CUSTOM) {
		if (numraster < 2) {
			elog(NOTICE, "CUSTOM extent type not supported. Defaulting to FIRST");
			arg->bandarg->extenttype = ET_FIRST;
		}
		else {
			elog(NOTICE, "CUSTOM extent type not supported. Defaulting to INTERSECTION");
			arg->bandarg->extenttype = ET_INTERSECTION;
		}
	}
	else if (numraster < 2)
		arg->bandarg->extenttype = ET_FIRST;

	POSTGIS_RT_DEBUGF(4, "extenttype: %d", arg->bandarg->extenttype);

	/* nodatanodataval (6) */
	if (!PG_ARGISNULL(6)) {
		arg->callback.nodatanodata.hasval = 1;
		arg->callback.nodatanodata.val = PG_GETARG_FLOAT8(6);
	}

	err = 0;
	/* all rasters are empty, return empty raster */
	if (allempty == arg->bandarg->numraster) {
		elog(NOTICE, "All input rasters are empty. Returning empty raster");
		err = 1;
	}
	/* all rasters don't have indicated band, return empty raster */
	else if (noband == arg->bandarg->numraster) {
		elog(NOTICE, "All input rasters do not have bands at indicated indexes. Returning empty raster");
		err = 1;
	}
	if (err) {
		rtpg_nmapalgebraexpr_arg_destroy(arg);

		raster = rt_raster_new(0, 0);
		if (raster == NULL) {
			elog(ERROR, "RASTER_nMapAlgebraExpr: Could not create empty raster");
			PG_RETURN_NULL();
		}

		pgraster = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (!pgraster) PG_RETURN_NULL();

		SET_VARSIZE(pgraster, pgraster->size);
		PG_RETURN_POINTER(pgraster);
	}

	/* connect SPI */
	if (SPI_connect() != SPI_OK_CONNECT) {
		rtpg_nmapalgebraexpr_arg_destroy(arg);
		elog(ERROR, "RASTER_nMapAlgebraExpr: Could not connect to the SPI manager");
		PG_RETURN_NULL();
	}

	/*
		process expressions

		exprpos elements are:
			1 - expression => spi_plan[0]
			4 - nodata1expr => spi_plan[1]
			5 - nodata2expr => spi_plan[2]
	*/
	for (i = 0; i < arg->callback.exprcount; i++) {
		char *expr = NULL;
		char *tmp = NULL;
		char *sql = NULL;
		char place[5] = "$1";

		if (PG_ARGISNULL(exprpos[i]))
			continue;

		expr = text_to_cstring(PG_GETARG_TEXT_P(exprpos[i]));
		POSTGIS_RT_DEBUGF(3, "raw expr of argument #%d: %s", exprpos[i], expr);

		for (j = 0, k = 1; j < argkwcount; j++) {
			/* attempt to replace keyword with placeholder */
			len = 0;
			tmp = rtpg_strreplace(expr, argkw[j], place, &len);
			pfree(expr);
			expr = tmp;

			if (len) {
				POSTGIS_RT_DEBUGF(4, "kw #%d (%s) at pos $%d", j, argkw[j], k);
				arg->callback.expr[i].spi_argcount++;
				arg->callback.expr[i].spi_argpos[j] = k++;

				sprintf(place, "$%d", k);
			}
			else
				arg->callback.expr[i].spi_argpos[j] = 0;
		}

		len = strlen("SELECT (") + strlen(expr) + strlen(")::double precision");
		sql = (char *) palloc(len + 1);
		if (sql == NULL) {
			rtpg_nmapalgebraexpr_arg_destroy(arg);
			SPI_finish();
			elog(ERROR, "RASTER_nMapAlgebraExpr: Could not allocate memory for expression parameter %d", exprpos[i]);
			PG_RETURN_NULL();
		}

		strncpy(sql, "SELECT (", strlen("SELECT ("));
		strncpy(sql + strlen("SELECT ("), expr, strlen(expr));
		strncpy(sql + strlen("SELECT (") + strlen(expr), ")::double precision", strlen(")::double precision"));
		sql[len] = '\0';

		POSTGIS_RT_DEBUGF(3, "sql #%d: %s", exprpos[i], sql);

		/* prepared plan */
		if (arg->callback.expr[i].spi_argcount) {
			Oid *argtype = (Oid *) palloc(arg->callback.expr[i].spi_argcount * sizeof(Oid));
			POSTGIS_RT_DEBUGF(3, "expression parameter %d is a prepared plan", exprpos[i]);
			if (argtype == NULL) {
				pfree(sql);
				rtpg_nmapalgebraexpr_arg_destroy(arg);
				SPI_finish();
				elog(ERROR, "RASTER_nMapAlgebraExpr: Could not allocate memory for prepared plan argtypes of expression parameter %d", exprpos[i]);
				PG_RETURN_NULL();
			}

			/* specify datatypes of parameters */
			for (j = 0, k = 0; j < argkwcount; j++) {
				if (arg->callback.expr[i].spi_argpos[j] < 1) continue;

				/* positions are INT4 */
				if (
					(strstr(argkw[j], "[rast.x]") != NULL) ||
					(strstr(argkw[j], "[rast.y]") != NULL) ||
					(strstr(argkw[j], "[rast1.x]") != NULL) ||
					(strstr(argkw[j], "[rast1.y]") != NULL) ||
					(strstr(argkw[j], "[rast2.x]") != NULL) ||
					(strstr(argkw[j], "[rast2.y]") != NULL)
				)
					argtype[k] = INT4OID;
				/* everything else is FLOAT8 */
				else
					argtype[k] = FLOAT8OID;

				k++;
			}

			arg->callback.expr[i].spi_plan = SPI_prepare(sql, arg->callback.expr[i].spi_argcount, argtype);
			pfree(argtype);
			pfree(sql);

			if (arg->callback.expr[i].spi_plan == NULL) {
				rtpg_nmapalgebraexpr_arg_destroy(arg);
				SPI_finish();
				elog(ERROR, "RASTER_nMapAlgebraExpr: Could not create prepared plan of expression parameter %d", exprpos[i]);
				PG_RETURN_NULL();
			}
		}
		/* no args, just execute query */
		else {
			POSTGIS_RT_DEBUGF(3, "expression parameter %d has no args, simply executing", exprpos[i]);
			err = SPI_execute(sql, TRUE, 0);
			pfree(sql);

			if (err != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {
				rtpg_nmapalgebraexpr_arg_destroy(arg);
				SPI_finish();
				elog(ERROR, "RASTER_nMapAlgebraExpr: Could not evaluate expression parameter %d", exprpos[i]);
				PG_RETURN_NULL();
			}

			/* get output of prepared plan */
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			if (SPI_result == SPI_ERROR_NOATTRIBUTE) {
				if (SPI_tuptable) SPI_freetuptable(tuptable);
				rtpg_nmapalgebraexpr_arg_destroy(arg);
				SPI_finish();
				elog(ERROR, "RASTER_nMapAlgebraExpr: Could not get result of expression parameter %d", exprpos[i]);
				PG_RETURN_NULL();
			}

			if (!isnull) {
				arg->callback.expr[i].hasval = 1;
				arg->callback.expr[i].val = DatumGetFloat8(datum);
			} 

			if (SPI_tuptable) SPI_freetuptable(tuptable);
		}
	}

	/* determine nodataval and possibly pixtype */
	/* band to check */
	switch (arg->bandarg->extenttype) {
		case ET_LAST:
		case ET_SECOND:
			if (numraster > 1)
				i = 1;
			else
				i = 0;
			break;
		default:
			i = 0;
			break;
	}
	/* find first viable band */
	if (!arg->bandarg->hasband[i]) {
		for (i = 0; i < numraster; i++) {
			if (arg->bandarg->hasband[i])
				break;
		}
		if (i >= numraster)
			i = numraster - 1;
	}
	band = rt_raster_get_band(arg->bandarg->raster[i], arg->bandarg->nband[i]);

	/* set pixel type if PT_END */
	if (arg->bandarg->pixtype == PT_END)
		arg->bandarg->pixtype = rt_band_get_pixtype(band);

	/* set hasnodata and nodataval */
	arg->bandarg->hasnodata = 1;
	if (rt_band_get_hasnodata_flag(band))
		rt_band_get_nodata(band, &(arg->bandarg->nodataval));
	else
		arg->bandarg->nodataval = rt_band_get_min_value(band);

	POSTGIS_RT_DEBUGF(4, "pixtype, hasnodata, nodataval: %s, %d, %f", rt_pixtype_name(arg->bandarg->pixtype), arg->bandarg->hasnodata, arg->bandarg->nodataval);

	/* init itrset */
	itrset = palloc(sizeof(struct rt_iterator_t) * numraster);
	if (itrset == NULL) {
		rtpg_nmapalgebraexpr_arg_destroy(arg);
		SPI_finish();
		elog(ERROR, "RASTER_nMapAlgebra: Could not allocate memory for iterator arguments");
		PG_RETURN_NULL();
	}

	/* set itrset */
	for (i = 0; i < numraster; i++) {
		itrset[i].raster = arg->bandarg->raster[i];
		itrset[i].nband = arg->bandarg->nband[i];
		itrset[i].nbnodata = 1;
	}

	/* pass everything to iterator */
	err = rt_raster_iterator(
		itrset, numraster,
		arg->bandarg->extenttype, arg->bandarg->cextent,
		arg->bandarg->pixtype,
		arg->bandarg->hasnodata, arg->bandarg->nodataval,
		0, 0,
		NULL,
		&(arg->callback),
		rtpg_nmapalgebraexpr_callback,
		&raster
	);

	pfree(itrset);
	rtpg_nmapalgebraexpr_arg_destroy(arg);

	if (err != ES_NONE) {
		SPI_finish();
		elog(ERROR, "RASTER_nMapAlgebraExpr: Could not run raster iterator function");
		PG_RETURN_NULL();
	}
	else if (raster == NULL) {
		SPI_finish();
		PG_RETURN_NULL();
	}

	/* switch to prior memory context to ensure memory allocated in correct context */
	MemoryContextSwitchTo(mainMemCtx);

	pgraster = rt_raster_serialize(raster);
	rt_raster_destroy(raster);

	/* finish SPI */
	SPI_finish();

	if (!pgraster)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, pgraster->size);
	PG_RETURN_POINTER(pgraster);
}

/* ---------------------------------------------------------------- */
/*  ST_Union aggregate functions                                    */
/* ---------------------------------------------------------------- */

typedef enum {
	UT_LAST = 0,
	UT_FIRST,
	UT_MIN,
	UT_MAX,
	UT_COUNT,
	UT_SUM,
	UT_MEAN,
	UT_RANGE
} rtpg_union_type;

/* internal function translating text of UNION type to enum */
static rtpg_union_type rtpg_uniontype_index_from_name(const char *cutype) {
	assert(cutype && strlen(cutype) > 0);

	if (strcmp(cutype, "LAST") == 0)
		return UT_LAST;
	else if (strcmp(cutype, "FIRST") == 0)
		return UT_FIRST;
	else if (strcmp(cutype, "MIN") == 0)
		return UT_MIN;
	else if (strcmp(cutype, "MAX") == 0)
		return UT_MAX;
	else if (strcmp(cutype, "COUNT") == 0)
		return UT_COUNT;
	else if (strcmp(cutype, "SUM") == 0)
		return UT_SUM;
	else if (strcmp(cutype, "MEAN") == 0)
		return UT_MEAN;
	else if (strcmp(cutype, "RANGE") == 0)
		return UT_RANGE;

	return UT_LAST;
}

typedef struct rtpg_union_band_arg_t *rtpg_union_band_arg;
struct rtpg_union_band_arg_t {
	int nband; /* source raster's band index, 0-based */
	rtpg_union_type uniontype;

	int numraster;
	rt_raster *raster;
};

typedef struct rtpg_union_arg_t *rtpg_union_arg;
struct rtpg_union_arg_t {
	int numband; /* number of bandargs */
	rtpg_union_band_arg bandarg;
};

static void rtpg_union_arg_destroy(rtpg_union_arg arg) {
	int i = 0;
	int j = 0;
	int k = 0;

	if (arg->bandarg != NULL) {
		for (i = 0; i < arg->numband; i++) {
			if (!arg->bandarg[i].numraster)
				continue;

			for (j = 0; j < arg->bandarg[i].numraster; j++) {
				if (arg->bandarg[i].raster[j] == NULL)
					continue;

				for (k = rt_raster_get_num_bands(arg->bandarg[i].raster[j]) - 1; k >= 0; k--)
					rt_band_destroy(rt_raster_get_band(arg->bandarg[i].raster[j], k));
				rt_raster_destroy(arg->bandarg[i].raster[j]);
			}

			pfree(arg->bandarg[i].raster);
		}

		pfree(arg->bandarg);
	}

	pfree(arg);
}

static int rtpg_union_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	rtpg_union_type *utype = (rtpg_union_type *) userarg;

	if (arg == NULL)
		return 0;

	if (
		arg->rasters != 2 ||
		arg->rows != 1 ||
		arg->columns != 1
	) {
		elog(ERROR, "rtpg_union_callback: Invalid arguments passed to callback");
		return 0;
	}

	*value = 0;
	*nodata = 0;

	/* handle NODATA situations except for COUNT, which is a special case */
	if (*utype != UT_COUNT) {
		/* both NODATA */
		if (arg->nodata[0][0][0] && arg->nodata[1][0][0]) {
			*nodata = 1;
			POSTGIS_RT_DEBUGF(4, "value, nodata = %f, %d", *value, *nodata);
			return 1;
		}
		/* second NODATA */
		else if (!arg->nodata[0][0][0] && arg->nodata[1][0][0]) {
			*value = arg->values[0][0][0];
			POSTGIS_RT_DEBUGF(4, "value, nodata = %f, %d", *value, *nodata);
			return 1;
		}
		/* first NODATA */
		else if (arg->nodata[0][0][0] && !arg->nodata[1][0][0]) {
			*value = arg->values[1][0][0];
			POSTGIS_RT_DEBUGF(4, "value, nodata = %f, %d", *value, *nodata);
			return 1;
		}
	}

	switch (*utype) {
		case UT_FIRST:
			*value = arg->values[0][0][0];
			break;
		case UT_MIN:
			if (arg->values[0][0][0] < arg->values[1][0][0])
				*value = arg->values[0][0][0];
			else
				*value = arg->values[1][0][0];
			break;
		case UT_MAX:
			if (arg->values[0][0][0] > arg->values[1][0][0])
				*value = arg->values[0][0][0];
			else
				*value = arg->values[1][0][0];
			break;
		case UT_COUNT:
			/* both NODATA */
			if (arg->nodata[0][0][0] && arg->nodata[1][0][0])
				*value = 0;
			/* second NODATA */
			else if (!arg->nodata[0][0][0] && arg->nodata[1][0][0])
				*value = arg->values[0][0][0];
			/* first NODATA */
			else if (arg->nodata[0][0][0] && !arg->nodata[1][0][0])
				*value = 1;
			/* has value, increment */
			else
				*value = arg->values[0][0][0] + 1;
			break;
		case UT_SUM:
			*value = arg->values[0][0][0] + arg->values[1][0][0];
			break;
		case UT_MEAN:
		case UT_RANGE:
			break;
		case UT_LAST:
		default:
			*value = arg->values[1][0][0];
			break;
	}

	POSTGIS_RT_DEBUGF(4, "value, nodata = %f, %d", *value, *nodata);


	return 1;
}

static int rtpg_union_mean_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	if (arg == NULL)
		return 0;

	if (
		arg->rasters != 2 ||
		arg->rows != 1 ||
		arg->columns != 1
	) {
		elog(ERROR, "rtpg_union_mean_callback: Invalid arguments passed to callback");
		return 0;
	}

	*value = 0;
	*nodata = 1;

	POSTGIS_RT_DEBUGF(4, "rast0: %f %d", arg->values[0][0][0], arg->nodata[0][0][0]);
	POSTGIS_RT_DEBUGF(4, "rast1: %f %d", arg->values[1][0][0], arg->nodata[1][0][0]);

	if (
		!arg->nodata[0][0][0] &&
		FLT_NEQ(arg->values[0][0][0], 0) &&
		!arg->nodata[1][0][0]
	) {
		*value = arg->values[1][0][0] / arg->values[0][0][0];
		*nodata = 0;
	}

	POSTGIS_RT_DEBUGF(4, "value, nodata = (%f, %d)", *value, *nodata);

	return 1;
}

static int rtpg_union_range_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	if (arg == NULL)
		return 0;

	if (
		arg->rasters != 2 ||
		arg->rows != 1 ||
		arg->columns != 1
	) {
		elog(ERROR, "rtpg_union_range_callback: Invalid arguments passed to callback");
		return 0;
	}

	*value = 0;
	*nodata = 1;

	POSTGIS_RT_DEBUGF(4, "rast0: %f %d", arg->values[0][0][0], arg->nodata[0][0][0]);
	POSTGIS_RT_DEBUGF(4, "rast1: %f %d", arg->values[1][0][0], arg->nodata[1][0][0]);

	if (
		!arg->nodata[0][0][0] &&
		!arg->nodata[1][0][0]
	) {
		*value = arg->values[1][0][0] - arg->values[0][0][0];
		*nodata = 0;
	}

	POSTGIS_RT_DEBUGF(4, "value, nodata = (%f, %d)", *value, *nodata);

	return 1;
}

/* called for ST_Union(raster, unionarg[]) */
static int rtpg_union_unionarg_process(rtpg_union_arg arg, ArrayType *array) {
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

	int i;
	int nband = 1;
	char *utypename = NULL;
	rtpg_union_type utype = UT_LAST;

	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(
		array,
		etype,
		typlen, typbyval, typalign,
		&e, &nulls, &n
	);

	if (!n) {
		elog(ERROR, "rtpg_union_unionarg_process: Invalid argument for unionarg");
		return 0;
	}

	/* prep arg */
	arg->numband = n;
	arg->bandarg = palloc(sizeof(struct rtpg_union_band_arg_t) * arg->numband);
	if (arg->bandarg == NULL) {
		elog(ERROR, "rtpg_union_unionarg_process: Could not allocate memory for band information");
		return 0;
	}

	/* process each element */
	for (i = 0; i < n; i++) {
		if (nulls[i]) {
			arg->numband--;
			continue;
		}

		POSTGIS_RT_DEBUGF(4, "Processing unionarg at index %d", i);

		/* each element is a tuple */
		tup = (HeapTupleHeader) DatumGetPointer(e[i]);
		if (NULL == tup) {
			elog(ERROR, "rtpg_union_unionarg_process: Invalid argument for unionarg");
			return 0;
		}

		/* first element, bandnum */
		tupv = GetAttributeByName(tup, "nband", &isnull);
		if (isnull) {
			nband = i + 1;
			elog(NOTICE, "First argument (nband) of unionarg is NULL.  Assuming nband = %d", nband);
		}
		else
			nband = DatumGetInt32(tupv);

		if (nband < 1) {
			elog(ERROR, "rtpg_union_unionarg_process: Band number must be greater than zero (1-based)");
			return 0;
		}

		/* second element, uniontype */
		tupv = GetAttributeByName(tup, "uniontype", &isnull);
		if (isnull) {
			elog(NOTICE, "Second argument (uniontype) of unionarg is NULL.  Assuming uniontype = LAST");
			utype = UT_LAST;
		}
		else {
			utypename = text_to_cstring((text *) DatumGetPointer(tupv));
			utype = rtpg_uniontype_index_from_name(rtpg_strtoupper(utypename));
		}

		arg->bandarg[i].uniontype = utype;
		arg->bandarg[i].nband = nband - 1;
		arg->bandarg[i].raster = NULL;

		if (
			utype != UT_MEAN &&
			utype != UT_RANGE
		) {
			arg->bandarg[i].numraster = 1;
		}
		else
			arg->bandarg[i].numraster = 2;
	}

	if (arg->numband < n) {
		arg->bandarg = repalloc(arg->bandarg, sizeof(struct rtpg_union_band_arg_t) * arg->numband);
		if (arg->bandarg == NULL) {
			elog(ERROR, "rtpg_union_unionarg_process: Could not reallocate memory for band information");
			return 0;
		}
	}

	return 1;
}

/* called for ST_Union(raster) */
static int rtpg_union_noarg(rtpg_union_arg arg, rt_raster raster) {
	int numbands;
	int i;

	if (rt_raster_is_empty(raster))
		return 1;

	numbands = rt_raster_get_num_bands(raster);
	if (numbands <= arg->numband)
		return 1;

	/* more bands to process */
	POSTGIS_RT_DEBUG(4, "input raster has more bands, adding more bandargs");
	if (arg->numband)
		arg->bandarg = repalloc(arg->bandarg, sizeof(struct rtpg_union_band_arg_t) * numbands);
	else
		arg->bandarg = palloc(sizeof(struct rtpg_union_band_arg_t) * numbands);
	if (arg->bandarg == NULL) {
		elog(ERROR, "rtpg_union_noarg: Could not reallocate memory for band information");
		return 0;
	}

	i = arg->numband;
	arg->numband = numbands;
	for (; i < arg->numband; i++) {
		POSTGIS_RT_DEBUGF(4, "Adding bandarg for band at index %d", i);
		arg->bandarg[i].uniontype = UT_LAST;
		arg->bandarg[i].nband = i;
		arg->bandarg[i].numraster = 1;

		arg->bandarg[i].raster = (rt_raster *) palloc(sizeof(rt_raster) * arg->bandarg[i].numraster);
		if (arg->bandarg[i].raster == NULL) {
			elog(ERROR, "rtpg_union_noarg: Could not allocate memory for working rasters");
			return 0;
		}
		memset(arg->bandarg[i].raster, 0, sizeof(rt_raster) * arg->bandarg[i].numraster);

		/* add new working rt_raster but only if working raster already exists */
		if (!rt_raster_is_empty(arg->bandarg[0].raster[0])) {
			arg->bandarg[i].raster[0] = rt_raster_clone(arg->bandarg[0].raster[0], 0); /* shallow clone */
			if (arg->bandarg[i].raster[0] == NULL) {
				elog(ERROR, "rtpg_union_noarg: Could not create working raster");
				return 0;
			}
		}
	}

	return 1;
}

/*
 * Serialize a rtpg_union_arg
 */
PG_FUNCTION_INFO_V1(unionargsend);
Datum
unionargsend(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	StringInfoData buf;
	rtpg_union_arg state = PG_GETARG_POINTER(0);
	bytea *result;
	int nbytes;
	int i;
	int j;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	initStringInfo(&buf);

	/* Serialize each band's array of rasters */
	pq_sendint(&buf, state->numband, sizeof(state->numband));
	for (i=0; i<state->numband; i++)
	{
		pq_sendint(&buf, state->bandarg[i].nband, sizeof(state->bandarg[i].nband));
		pq_sendint(&buf, state->bandarg[i].uniontype, sizeof(state->bandarg[i].uniontype));
		pq_sendint(&buf, state->bandarg[i].numraster, sizeof(state->bandarg[i].numraster));

		for (j=0; j<state->bandarg[i].numraster; j++)
		{
			uint32_t size;
			rt_raster raster = state->bandarg[i].raster[j];
			uint8_t *bytes = rt_raster_to_wkb(raster, false, &size);

			pq_sendint(&buf, size, sizeof(size));
			pq_sendbytes(&buf, (char *) bytes, size);
		}
	}

	nbytes = buf.len - buf.cursor;
	result = (bytea *) palloc0(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pq_copymsgbytes(&buf, VARDATA(result), nbytes);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * Deserialize a rtpg_union_arg
 */
PG_FUNCTION_INFO_V1(unionargrecv);
Datum
unionargrecv(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	bytea *bytesin;
	StringInfoData buf;
	rtpg_union_arg result;
	int nbytes;
	int i;
	int j;
	int numband;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	bytesin = (bytea *) PG_GETARG_BYTEA_P(0);
	nbytes =  VARSIZE(bytesin) - VARHDRSZ;

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytesin), nbytes);

	numband = pq_getmsgint(&buf, sizeof(int));

	result = palloc0(sizeof(struct rtpg_union_arg_t));
	result->numband = numband;
	result->bandarg = palloc0(numband * sizeof(struct rtpg_union_band_arg_t));

	/* Read each band's array of rasters */
	for (i=0; i<result->numband; i++)
	{
		result->bandarg[i].nband = pq_getmsgint(&buf, sizeof(result->bandarg[i].nband));
		result->bandarg[i].uniontype = pq_getmsgint(&buf, sizeof(result->bandarg[i].uniontype));
		result->bandarg[i].numraster = pq_getmsgint(&buf, sizeof(result->bandarg[i].numraster));
		result->bandarg[i].raster = palloc0(result->bandarg[i].numraster * sizeof(rt_raster));

		for (j=0; j<result->bandarg[i].numraster; j++)
		{
			uint32_t size = pq_getmsgint(&buf, sizeof(uint32_t));
			const char *raw = pq_getmsgbytes(&buf, size);

			result->bandarg[i].raster[j] = rt_raster_from_wkb((uint8_t *) raw, size);
		}
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

static void
raster_union(rtpg_union_arg iwr, rt_raster raster)
{
	rt_raster _raster = NULL;
	rt_band _band = NULL;
	int noerr = 1;
	int isempty[2] = {0};
	int hasband[2] = {0};
	double _offset[4] = {0.};
	int nbnodata = 0; /* 1 if adding bands */
	int i = 0;
	int j = 0;
	int k = 0;
	rt_iterator itrset;
	rtpg_union_type utype = UT_LAST;
	rt_pixtype pixtype = PT_END;
	int hasnodata = 1;
	double nodataval = 0;
	rt_raster iraster = NULL;
	rt_band iband = NULL;
	int reuserast = 0;
	int y = 0;
	uint16_t _dim[2] = {0};
	void *vals = NULL;
	uint16_t nvals = 0;

	POSTGIS_RT_DEBUG(3, "Starting...");

	itrset = palloc(sizeof(struct rt_iterator_t) * 2);

	/* by band to UNION */
	for (i = 0; i < iwr->numband; i++) {

		/* by raster */
		for (j = 0; j < iwr->bandarg[i].numraster; j++) {
			reuserast = 0;

			/* type of union */
			utype = iwr->bandarg[i].uniontype;

			/* raster flags */
			isempty[0] = rt_raster_is_empty(iwr->bandarg[i].raster[j]);
			isempty[1] = rt_raster_is_empty(raster);

			if (!isempty[0])
				hasband[0] = rt_raster_has_band(iwr->bandarg[i].raster[j], 0);
			if (!isempty[1])
				hasband[1] = rt_raster_has_band(raster, iwr->bandarg[i].nband);

			/* determine pixtype, hasnodata and nodataval */
			_band = NULL;
			if (!isempty[0] && hasband[0])
				_band = rt_raster_get_band(iwr->bandarg[i].raster[j], 0);
			else if (!isempty[1] && hasband[1])
				_band = rt_raster_get_band(raster, iwr->bandarg[i].nband);
			else {
				pixtype = PT_64BF;
				hasnodata = 1;
				nodataval = rt_pixtype_get_min_value(pixtype);
			}
			if (_band != NULL) {
				pixtype = rt_band_get_pixtype(_band);
				hasnodata = 1;
				if (rt_band_get_hasnodata_flag(_band))
					rt_band_get_nodata(_band, &nodataval);
				else
					nodataval = rt_band_get_min_value(_band);
			}

			/* UT_MEAN and UT_RANGE require two passes */
			/* UT_MEAN: first for UT_COUNT and second for UT_SUM */
			if (iwr->bandarg[i].uniontype == UT_MEAN) {
				/* first pass, UT_COUNT */
				if (j < 1)
					utype = UT_COUNT;
				else
					utype = UT_SUM;
			}
			/* UT_RANGE: first for UT_MIN and second for UT_MAX */
			else if (iwr->bandarg[i].uniontype == UT_RANGE) {
				/* first pass, UT_MIN */
				if (j < 1)
					utype = UT_MIN;
				else
					utype = UT_MAX;
			}

			/* force band settings for UT_COUNT */
			if (utype == UT_COUNT) {
				pixtype = PT_32BUI;
				hasnodata = 0;
				nodataval = 0;
			}

			POSTGIS_RT_DEBUGF(4, "(pixtype, hasnodata, nodataval) = (%s, %d, %f)", rt_pixtype_name(pixtype), hasnodata, nodataval);

			/* set itrset */
			itrset[0].raster = iwr->bandarg[i].raster[j];
			itrset[0].nband = 0;
			itrset[1].raster = raster;
			itrset[1].nband = iwr->bandarg[i].nband;

			/* allow use NODATA to replace missing bands */
			if (nbnodata) {
				itrset[0].nbnodata = 1;
				itrset[1].nbnodata = 1;
			}
			/* missing bands are not ignored */
			else {
				itrset[0].nbnodata = 0;
				itrset[1].nbnodata = 0;
			}

			/* if rasters AND bands are present, use copy approach */
			if (!isempty[0] && !isempty[1] && hasband[0] && hasband[1]) {
				POSTGIS_RT_DEBUG(3, "using line method");

				/* generate empty out raster */
				if (rt_raster_from_two_rasters(
					iwr->bandarg[i].raster[j], raster,
					ET_UNION,
					&iraster, _offset
				) != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (raster != NULL) {
						rt_raster_destroy(raster);
					}

					elog(ERROR, "RASTER_union_transfn: Could not create internal raster");
				}
				POSTGIS_RT_DEBUGF(4, "_offset = %f, %f, %f, %f",
					_offset[0], _offset[1], _offset[2], _offset[3]);

				/* rasters are spatially the same? */
				if (
					rt_raster_get_width(iwr->bandarg[i].raster[j]) == rt_raster_get_width(iraster) &&
					rt_raster_get_height(iwr->bandarg[i].raster[j]) == rt_raster_get_height(iraster)
				) {
					double igt[6] = {0};
					double gt[6] = {0};

					rt_raster_get_geotransform_matrix(iwr->bandarg[i].raster[j], gt);
					rt_raster_get_geotransform_matrix(iraster, igt);

					reuserast = rt_util_same_geotransform_matrix(gt, igt);
				}

				/* use internal raster */
				if (!reuserast) {
					/* create band of same type */
					if (rt_raster_generate_new_band(
						iraster,
						pixtype,
						nodataval,
						hasnodata, nodataval,
						0
					) == -1) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						rt_raster_destroy(iraster);
						if (raster != NULL) {
							rt_raster_destroy(raster);
						}

						elog(ERROR, "RASTER_union_transfn: Could not add new band to internal raster");
					}
					iband = rt_raster_get_band(iraster, 0);

					/* copy working raster to output raster */
					_dim[0] = rt_raster_get_width(iwr->bandarg[i].raster[j]);
					_dim[1] = rt_raster_get_height(iwr->bandarg[i].raster[j]);
					for (y = 0; y < _dim[1]; y++) {
						POSTGIS_RT_DEBUGF(4, "Getting pixel line of working raster at (x, y, length) = (0, %d, %d)", y, _dim[0]);
						if (rt_band_get_pixel_line(
							_band,
							0, y,
							_dim[0],
							&vals, &nvals
						) != ES_NONE) {

							pfree(itrset);
							rtpg_union_arg_destroy(iwr);
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
							if (raster != NULL) {
								rt_raster_destroy(raster);
							}

							elog(ERROR, "RASTER_union_transfn: Could not get pixel line from band of working raster");
						}

						POSTGIS_RT_DEBUGF(4, "Setting pixel line at (x, y, length) = (%d, %d, %d)", (int) _offset[0], (int) _offset[1] + y, nvals);
						if (rt_band_set_pixel_line(
							iband,
							(int) _offset[0], (int) _offset[1] + y,
							vals, nvals
						) != ES_NONE) {

							pfree(itrset);
							rtpg_union_arg_destroy(iwr);
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
							if (raster != NULL) {
								rt_raster_destroy(raster);
							}

							elog(ERROR, "RASTER_union_transfn: Could not set pixel line to band of internal raster");
						}
					}
				}
				else {
					rt_raster_destroy(iraster);
					iraster = iwr->bandarg[i].raster[j];
					iband = rt_raster_get_band(iraster, 0);
				}

				/* run iterator for extent of input raster */
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_LAST, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					&utype,
					rtpg_union_callback,
					&_raster
				);
				if (noerr != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (!reuserast) {
						rt_band_destroy(iband);
						rt_raster_destroy(iraster);
					}
					if (raster != NULL) {
						rt_raster_destroy(raster);
					}

					elog(ERROR, "RASTER_union_transfn: Could not run raster iterator function");
				}

				/* with iterator raster, copy data to output raster */
				_band = rt_raster_get_band(_raster, 0);
				_dim[0] = rt_raster_get_width(_raster);
				_dim[1] = rt_raster_get_height(_raster);
				for (y = 0; y < _dim[1]; y++) {
					POSTGIS_RT_DEBUGF(4, "Getting pixel line of iterator raster at (x, y, length) = (0, %d, %d)", y, _dim[0]);
					if (rt_band_get_pixel_line(
						_band,
						0, y,
						_dim[0],
						&vals, &nvals
					) != ES_NONE) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						if (!reuserast) {
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
						}
						if (raster != NULL) {
							rt_raster_destroy(raster);
						}

						elog(ERROR, "RASTER_union_transfn: Could not get pixel line from band of working raster");
					}

					POSTGIS_RT_DEBUGF(4, "Setting pixel line at (x, y, length) = (%d, %d, %d)", (int) _offset[2], (int) _offset[3] + y, nvals);
					if (rt_band_set_pixel_line(
						iband,
						(int) _offset[2], (int) _offset[3] + y,
						vals, nvals
					) != ES_NONE) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						if (!reuserast) {
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
						}
						if (raster != NULL) {
							rt_raster_destroy(raster);
						}

						elog(ERROR, "RASTER_union_transfn: Could not set pixel line to band of internal raster");
					}
				}

				/* free _raster */
				rt_band_destroy(_band);
				rt_raster_destroy(_raster);

				/* replace working raster with output raster */
				_raster = iraster;
			}
			else {
				POSTGIS_RT_DEBUG(3, "using pixel method");

				/* pass everything to iterator */
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_UNION, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					&utype,
					rtpg_union_callback,
					&_raster
				);

				if (noerr != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (raster != NULL) {
						rt_raster_destroy(raster);
					}

					elog(ERROR, "RASTER_union_transfn: Could not run raster iterator function");
				}
			}

			/* replace working raster */
			if (iwr->bandarg[i].raster[j] != NULL && !reuserast) {
				for (k = rt_raster_get_num_bands(iwr->bandarg[i].raster[j]) - 1; k >= 0; k--)
					rt_band_destroy(rt_raster_get_band(iwr->bandarg[i].raster[j], k));
				rt_raster_destroy(iwr->bandarg[i].raster[j]);
			}
			iwr->bandarg[i].raster[j] = _raster;
		}

	}
}

/*
 * Combines two rtpg_union_args into one
 */
PG_FUNCTION_INFO_V1(RASTER_union_combinefn);
Datum RASTER_union_combinefn(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext aggcontext;
	rtpg_union_arg state = PG_ARGISNULL(0) ? NULL : (rtpg_union_arg) PG_GETARG_POINTER(0);
	rtpg_union_arg incoming = (rtpg_union_arg) PG_GETARG_POINTER(1);
	int i;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "_st_union_combinefn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_POINTER(incoming);

	old = MemoryContextSwitchTo(aggcontext);

	for (i=0; i<incoming->numband; i++)
	{
		int j;
		for (j=0; j<incoming->bandarg[i].numraster; j++)
		{
			raster_union(state, incoming->bandarg[i].raster[j]);
		}
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/* UNION aggregate transition function */
PG_FUNCTION_INFO_V1(RASTER_union_transfn);
Datum RASTER_union_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	rtpg_union_arg iwr = NULL;
	int skiparg = 0;

	rt_pgraster *pgraster = NULL;
	rt_raster raster = NULL;
	rt_raster _raster = NULL;
	rt_band _band = NULL;
	int nband = 1;
	int noerr = 1;
	int isempty[2] = {0};
	int hasband[2] = {0};
	int nargs = 0;
	double _offset[4] = {0.};
	int nbnodata = 0; /* 1 if adding bands */

	int i = 0;
	int j = 0;
	int k = 0;

	rt_iterator itrset;
	char *utypename = NULL;
	rtpg_union_type utype = UT_LAST;
	rt_pixtype pixtype = PT_END;
	int hasnodata = 1;
	double nodataval = 0;

	rt_raster iraster = NULL;
	rt_band iband = NULL;
	int reuserast = 0;
	int y = 0;
	uint16_t _dim[2] = {0};
	void *vals = NULL;
	uint16_t nvals = 0;

	POSTGIS_RT_DEBUG(3, "Starting...");

	/* cannot be called directly as this is exclusive aggregate function */
	if (!AggCheckCallContext(fcinfo, &aggcontext)) {
		elog(ERROR, "RASTER_union_transfn: Cannot be called in a non-aggregate context");
		PG_RETURN_NULL();
	}

	/* switch to aggcontext */
	oldcontext = MemoryContextSwitchTo(aggcontext);

	if (PG_ARGISNULL(0)) {
		POSTGIS_RT_DEBUG(3, "Creating state variable");
		/* allocate container in aggcontext */
		iwr = palloc(sizeof(struct rtpg_union_arg_t));
		if (iwr == NULL) {
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_union_transfn: Could not allocate memory for state variable");
			PG_RETURN_NULL();
		}

		iwr->numband = 0;
		iwr->bandarg = NULL;

		skiparg = 0;
	}
	else {
		POSTGIS_RT_DEBUG(3, "State variable already exists");
		iwr = (rtpg_union_arg) PG_GETARG_POINTER(0);
		skiparg = 1;
	}

	/* raster arg is NOT NULL */
	if (!PG_ARGISNULL(1)) {
		/* deserialize raster */
		pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

		/* Get raster object */
		raster = rt_raster_deserialize(pgraster, FALSE);
		if (raster == NULL) {

			rtpg_union_arg_destroy(iwr);
			PG_FREE_IF_COPY(pgraster, 1);

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_union_transfn: Could not deserialize raster");
			PG_RETURN_NULL();
		}
	}

	/* process additional args if needed */
	nargs = PG_NARGS();
	POSTGIS_RT_DEBUGF(4, "nargs = %d", nargs);
	if (nargs > 2) {
		POSTGIS_RT_DEBUG(4, "processing additional arguments");

		/* if more than 2 arguments, determine the type of argument 3 */
		/* band number, UNION type or unionarg */
		if (!PG_ARGISNULL(2)) {
			Oid calltype = get_fn_expr_argtype(fcinfo->flinfo, 2);

			switch (calltype) {
				/* UNION type */
				case TEXTOID: {
					int idx = 0;
					int numband = 0;

					POSTGIS_RT_DEBUG(4, "Processing arg 3 as UNION type");
					nbnodata = 1;

					utypename = text_to_cstring(PG_GETARG_TEXT_P(2));
					utype = rtpg_uniontype_index_from_name(rtpg_strtoupper(utypename));
					POSTGIS_RT_DEBUGF(4, "Union type: %s", utypename);

					POSTGIS_RT_DEBUGF(4, "iwr->numband: %d", iwr->numband);
					/* see if we need to append new bands */
					if (raster) {
						idx = iwr->numband;
						numband = rt_raster_get_num_bands(raster);
						POSTGIS_RT_DEBUGF(4, "numband: %d", numband);

						/* only worry about appended bands */
						if (numband > iwr->numband)
							iwr->numband = numband;
					}

					if (!iwr->numband)
						iwr->numband = 1;
					POSTGIS_RT_DEBUGF(4, "iwr->numband: %d", iwr->numband);
					POSTGIS_RT_DEBUGF(4, "numband, idx: %d, %d", numband, idx);

					/* bandarg set. only possible after the first call to function */
					if (iwr->bandarg) {
						/* only reallocate if new bands need to be added */
						if (numband > idx) {
							POSTGIS_RT_DEBUG(4, "Reallocating iwr->bandarg");
							iwr->bandarg = repalloc(iwr->bandarg, sizeof(struct rtpg_union_band_arg_t) * iwr->numband);
						}
						/* prevent initial values step happening below */
						else
							idx = iwr->numband;
					}
					/* bandarg not set, first call to function */
					else {
						POSTGIS_RT_DEBUG(4, "Allocating iwr->bandarg");
						iwr->bandarg = palloc(sizeof(struct rtpg_union_band_arg_t) * iwr->numband);
					}
					if (iwr->bandarg == NULL) {

						rtpg_union_arg_destroy(iwr);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not allocate memory for band information");
						PG_RETURN_NULL();
					}

					/* set initial values for bands that are "new" */
					for (i = idx; i < iwr->numband; i++) {
						iwr->bandarg[i].uniontype = utype;
						iwr->bandarg[i].nband = i;

						if (
							utype == UT_MEAN ||
							utype == UT_RANGE
						) {
							iwr->bandarg[i].numraster = 2;
						}
						else
							iwr->bandarg[i].numraster = 1;
						iwr->bandarg[i].raster = NULL;
					}

					break;
				}
				/* band number */
				case INT2OID:
				case INT4OID:
					if (skiparg)
						break;

					POSTGIS_RT_DEBUG(4, "Processing arg 3 as band number");
					nband = PG_GETARG_INT32(2);
					if (nband < 1) {

						rtpg_union_arg_destroy(iwr);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Band number must be greater than zero (1-based)");
						PG_RETURN_NULL();
					}

					iwr->numband = 1;
					iwr->bandarg = palloc(sizeof(struct rtpg_union_band_arg_t) * iwr->numband);
					if (iwr->bandarg == NULL) {

						rtpg_union_arg_destroy(iwr);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not allocate memory for band information");
						PG_RETURN_NULL();
					}

					iwr->bandarg[0].uniontype = UT_LAST;
					iwr->bandarg[0].nband = nband - 1;

					iwr->bandarg[0].numraster = 1;
					iwr->bandarg[0].raster = NULL;
					break;
				/* only other type allowed is unionarg */
				default: 
					if (skiparg)
						break;

					POSTGIS_RT_DEBUG(4, "Processing arg 3 as unionarg");
					if (!rtpg_union_unionarg_process(iwr, PG_GETARG_ARRAYTYPE_P(2))) {

						rtpg_union_arg_destroy(iwr);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not process unionarg");
						PG_RETURN_NULL();
					}

					break;
			}
		}

		/* UNION type */
		if (nargs > 3 && !PG_ARGISNULL(3)) {
			utypename = text_to_cstring(PG_GETARG_TEXT_P(3));
			utype = rtpg_uniontype_index_from_name(rtpg_strtoupper(utypename));
			iwr->bandarg[0].uniontype = utype;
			POSTGIS_RT_DEBUGF(4, "Union type: %s", utypename);

			if (
				utype == UT_MEAN ||
				utype == UT_RANGE
			) {
				iwr->bandarg[0].numraster = 2;
			}
		}

		/* allocate space for pointers to rt_raster */
		for (i = 0; i < iwr->numband; i++) {
			POSTGIS_RT_DEBUGF(4, "iwr->bandarg[%d].raster @ %p", i, iwr->bandarg[i].raster);

			/* no need to allocate */
			if (iwr->bandarg[i].raster != NULL)
				continue;

			POSTGIS_RT_DEBUGF(4, "Allocating space for working rasters of band %d", i);

			iwr->bandarg[i].raster = (rt_raster *) palloc(sizeof(rt_raster) * iwr->bandarg[i].numraster);
			if (iwr->bandarg[i].raster == NULL) {

				rtpg_union_arg_destroy(iwr);
				if (raster != NULL) {
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 1);
				}

				MemoryContextSwitchTo(oldcontext);
				elog(ERROR, "RASTER_union_transfn: Could not allocate memory for working raster(s)");
				PG_RETURN_NULL();
			}

			memset(iwr->bandarg[i].raster, 0, sizeof(rt_raster) * iwr->bandarg[i].numraster);

			/* add new working rt_raster but only if working raster already exists */
			if (i > 0 && !rt_raster_is_empty(iwr->bandarg[0].raster[0])) {
				for (j = 0; j < iwr->bandarg[i].numraster; j++) {
					iwr->bandarg[i].raster[j] = rt_raster_clone(iwr->bandarg[0].raster[0], 0); /* shallow clone */
					if (iwr->bandarg[i].raster[j] == NULL) {

						rtpg_union_arg_destroy(iwr);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not create working raster");
						PG_RETURN_NULL();
					}
				}
			}
		}
	}
	/* only raster, no additional args */
	/* only do this if raster isn't empty */
	else {
		POSTGIS_RT_DEBUG(4, "no additional args, checking input raster");
		nbnodata = 1;
		if (!rtpg_union_noarg(iwr, raster)) {

			rtpg_union_arg_destroy(iwr);
			if (raster != NULL) {
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 1);
			}

			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "RASTER_union_transfn: Could not check and balance number of bands");
			PG_RETURN_NULL();
		}
	}

	/* init itrset */
	itrset = palloc(sizeof(struct rt_iterator_t) * 2);
	if (itrset == NULL) {

		rtpg_union_arg_destroy(iwr);
		if (raster != NULL) {
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 1);
		}

		MemoryContextSwitchTo(oldcontext);
		elog(ERROR, "RASTER_union_transfn: Could not allocate memory for iterator arguments");
		PG_RETURN_NULL();
	}

	/* by band to UNION */
	for (i = 0; i < iwr->numband; i++) {

		/* by raster */
		for (j = 0; j < iwr->bandarg[i].numraster; j++) {
			reuserast = 0;

			/* type of union */
			utype = iwr->bandarg[i].uniontype;

			/* raster flags */
			isempty[0] = rt_raster_is_empty(iwr->bandarg[i].raster[j]);
			isempty[1] = rt_raster_is_empty(raster);

			if (!isempty[0])
				hasband[0] = rt_raster_has_band(iwr->bandarg[i].raster[j], 0);
			if (!isempty[1])
				hasband[1] = rt_raster_has_band(raster, iwr->bandarg[i].nband);

			/* determine pixtype, hasnodata and nodataval */
			_band = NULL;
			if (!isempty[0] && hasband[0])
				_band = rt_raster_get_band(iwr->bandarg[i].raster[j], 0);
			else if (!isempty[1] && hasband[1])
				_band = rt_raster_get_band(raster, iwr->bandarg[i].nband);
			else {
				pixtype = PT_64BF;
				hasnodata = 1;
				nodataval = rt_pixtype_get_min_value(pixtype);
			}
			if (_band != NULL) {
				pixtype = rt_band_get_pixtype(_band);
				hasnodata = 1;
				if (rt_band_get_hasnodata_flag(_band))
					rt_band_get_nodata(_band, &nodataval);
				else
					nodataval = rt_band_get_min_value(_band);
			}

			/* UT_MEAN and UT_RANGE require two passes */
			/* UT_MEAN: first for UT_COUNT and second for UT_SUM */
			if (iwr->bandarg[i].uniontype == UT_MEAN) {
				/* first pass, UT_COUNT */
				if (j < 1)
					utype = UT_COUNT;
				else
					utype = UT_SUM;
			}
			/* UT_RANGE: first for UT_MIN and second for UT_MAX */
			else if (iwr->bandarg[i].uniontype == UT_RANGE) {
				/* first pass, UT_MIN */
				if (j < 1)
					utype = UT_MIN;
				else
					utype = UT_MAX;
			}

			/* force band settings for UT_COUNT */
			if (utype == UT_COUNT) {
				pixtype = PT_32BUI;
				hasnodata = 0;
				nodataval = 0;
			}

			POSTGIS_RT_DEBUGF(4, "(pixtype, hasnodata, nodataval) = (%s, %d, %f)", rt_pixtype_name(pixtype), hasnodata, nodataval);

			/* set itrset */
			itrset[0].raster = iwr->bandarg[i].raster[j];
			itrset[0].nband = 0;
			itrset[1].raster = raster;
			itrset[1].nband = iwr->bandarg[i].nband;

			/* allow use NODATA to replace missing bands */
			if (nbnodata) {
				itrset[0].nbnodata = 1;
				itrset[1].nbnodata = 1;
			}
			/* missing bands are not ignored */
			else {
				itrset[0].nbnodata = 0;
				itrset[1].nbnodata = 0;
			}

			/* if rasters AND bands are present, use copy approach */
			if (!isempty[0] && !isempty[1] && hasband[0] && hasband[1]) {
				POSTGIS_RT_DEBUG(3, "using line method");

				/* generate empty out raster */
				if (rt_raster_from_two_rasters(
					iwr->bandarg[i].raster[j], raster,
					ET_UNION,
					&iraster, _offset 
				) != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (raster != NULL) {
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 1);
					}

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_union_transfn: Could not create internal raster");
					PG_RETURN_NULL();
				}
				POSTGIS_RT_DEBUGF(4, "_offset = %f, %f, %f, %f",
					_offset[0], _offset[1], _offset[2], _offset[3]);

				/* rasters are spatially the same? */
				if (
					rt_raster_get_width(iwr->bandarg[i].raster[j]) == rt_raster_get_width(iraster) &&
					rt_raster_get_height(iwr->bandarg[i].raster[j]) == rt_raster_get_height(iraster)
				) {
					double igt[6] = {0};
					double gt[6] = {0};

					rt_raster_get_geotransform_matrix(iwr->bandarg[i].raster[j], gt);
					rt_raster_get_geotransform_matrix(iraster, igt);

					reuserast = rt_util_same_geotransform_matrix(gt, igt);
				}

				/* use internal raster */
				if (!reuserast) {
					/* create band of same type */
					if (rt_raster_generate_new_band(
						iraster,
						pixtype,
						nodataval,
						hasnodata, nodataval,
						0
					) == -1) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						rt_raster_destroy(iraster);
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not add new band to internal raster");
						PG_RETURN_NULL();
					}
					iband = rt_raster_get_band(iraster, 0);

					/* copy working raster to output raster */
					_dim[0] = rt_raster_get_width(iwr->bandarg[i].raster[j]);
					_dim[1] = rt_raster_get_height(iwr->bandarg[i].raster[j]);
					for (y = 0; y < _dim[1]; y++) {
						POSTGIS_RT_DEBUGF(4, "Getting pixel line of working raster at (x, y, length) = (0, %d, %d)", y, _dim[0]);
						if (rt_band_get_pixel_line(
							_band,
							0, y,
							_dim[0],
							&vals, &nvals
						) != ES_NONE) {

							pfree(itrset);
							rtpg_union_arg_destroy(iwr);
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
							if (raster != NULL) {
								rt_raster_destroy(raster);
								PG_FREE_IF_COPY(pgraster, 1);
							}

							MemoryContextSwitchTo(oldcontext);
							elog(ERROR, "RASTER_union_transfn: Could not get pixel line from band of working raster");
							PG_RETURN_NULL();
						}

						POSTGIS_RT_DEBUGF(4, "Setting pixel line at (x, y, length) = (%d, %d, %d)", (int) _offset[0], (int) _offset[1] + y, nvals);
						if (rt_band_set_pixel_line(
							iband,
							(int) _offset[0], (int) _offset[1] + y,
							vals, nvals
						) != ES_NONE) {

							pfree(itrset);
							rtpg_union_arg_destroy(iwr);
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
							if (raster != NULL) {
								rt_raster_destroy(raster);
								PG_FREE_IF_COPY(pgraster, 1);
							}

							MemoryContextSwitchTo(oldcontext);
							elog(ERROR, "RASTER_union_transfn: Could not set pixel line to band of internal raster");
							PG_RETURN_NULL();
						}
					}
				}
				else {
					rt_raster_destroy(iraster);
					iraster = iwr->bandarg[i].raster[j];
					iband = rt_raster_get_band(iraster, 0);
				}

				/* run iterator for extent of input raster */
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_LAST, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					&utype,
					rtpg_union_callback,
					&_raster
				);
				if (noerr != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (!reuserast) {
						rt_band_destroy(iband);
						rt_raster_destroy(iraster);
					}
					if (raster != NULL) {
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 1);
					}

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_union_transfn: Could not run raster iterator function");
					PG_RETURN_NULL();
				}

				/* with iterator raster, copy data to output raster */
				_band = rt_raster_get_band(_raster, 0);
				_dim[0] = rt_raster_get_width(_raster);
				_dim[1] = rt_raster_get_height(_raster);
				for (y = 0; y < _dim[1]; y++) {
					POSTGIS_RT_DEBUGF(4, "Getting pixel line of iterator raster at (x, y, length) = (0, %d, %d)", y, _dim[0]);
					if (rt_band_get_pixel_line(
						_band,
						0, y,
						_dim[0],
						&vals, &nvals
					) != ES_NONE) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						if (!reuserast) {
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
						}
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not get pixel line from band of working raster");
						PG_RETURN_NULL();
					}

					POSTGIS_RT_DEBUGF(4, "Setting pixel line at (x, y, length) = (%d, %d, %d)", (int) _offset[2], (int) _offset[3] + y, nvals);
					if (rt_band_set_pixel_line(
						iband,
						(int) _offset[2], (int) _offset[3] + y,
						vals, nvals
					) != ES_NONE) {

						pfree(itrset);
						rtpg_union_arg_destroy(iwr);
						if (!reuserast) {
							rt_band_destroy(iband);
							rt_raster_destroy(iraster);
						}
						if (raster != NULL) {
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 1);
						}

						MemoryContextSwitchTo(oldcontext);
						elog(ERROR, "RASTER_union_transfn: Could not set pixel line to band of internal raster");
						PG_RETURN_NULL();
					}
				}

				/* free _raster */
				rt_band_destroy(_band);
				rt_raster_destroy(_raster);

				/* replace working raster with output raster */
				_raster = iraster;
			}
			else {
				POSTGIS_RT_DEBUG(3, "using pixel method");

				/* pass everything to iterator */
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_UNION, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					&utype,
					rtpg_union_callback,
					&_raster
				);

				if (noerr != ES_NONE) {

					pfree(itrset);
					rtpg_union_arg_destroy(iwr);
					if (raster != NULL) {
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 1);
					}

					MemoryContextSwitchTo(oldcontext);
					elog(ERROR, "RASTER_union_transfn: Could not run raster iterator function");
					PG_RETURN_NULL();
				}
			}

			/* replace working raster */
			if (iwr->bandarg[i].raster[j] != NULL && !reuserast) {
				for (k = rt_raster_get_num_bands(iwr->bandarg[i].raster[j]) - 1; k >= 0; k--)
					rt_band_destroy(rt_raster_get_band(iwr->bandarg[i].raster[j], k));
				rt_raster_destroy(iwr->bandarg[i].raster[j]);
			}
			iwr->bandarg[i].raster[j] = _raster;
		}

	}

	pfree(itrset);
	if (raster != NULL) {
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 1);
	}

	/* switch back to local context */
	MemoryContextSwitchTo(oldcontext);

	POSTGIS_RT_DEBUG(3, "Finished");

	PG_RETURN_POINTER(iwr);
}

/* UNION aggregate final function */
PG_FUNCTION_INFO_V1(RASTER_union_finalfn);
Datum RASTER_union_finalfn(PG_FUNCTION_ARGS)
{
	rtpg_union_arg iwr;
	rt_raster _rtn = NULL;
	rt_raster _raster = NULL;
	rt_pgraster *pgraster = NULL;

	int i = 0;
	int j = 0;
	rt_iterator itrset = NULL;
	rt_band _band = NULL;
	int noerr = 1;
	int status = 0;
	rt_pixtype pixtype = PT_END;
	int hasnodata = 0;
	double nodataval = 0;

	POSTGIS_RT_DEBUG(3, "Starting...");

	/* NULL, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	iwr = (rtpg_union_arg) PG_GETARG_POINTER(0);

	/* init itrset */
	itrset = palloc(sizeof(struct rt_iterator_t) * 2);
	if (itrset == NULL) {
		rtpg_union_arg_destroy(iwr);
		elog(ERROR, "RASTER_union_finalfn: Could not allocate memory for iterator arguments");
		PG_RETURN_NULL();
	}

	for (i = 0; i < iwr->numband; i++) {
		if (
			iwr->bandarg[i].uniontype == UT_MEAN ||
			iwr->bandarg[i].uniontype == UT_RANGE
		) {
			/* raster containing the SUM or MAX is at index 1 */
			_band = rt_raster_get_band(iwr->bandarg[i].raster[1], 0);

			pixtype = rt_band_get_pixtype(_band);
			hasnodata = rt_band_get_hasnodata_flag(_band);
			if (hasnodata)
				rt_band_get_nodata(_band, &nodataval);
			POSTGIS_RT_DEBUGF(4, "(pixtype, hasnodata, nodataval) = (%s, %d, %f)", rt_pixtype_name(pixtype), hasnodata, nodataval);

			itrset[0].raster = iwr->bandarg[i].raster[0];
			itrset[0].nband = 0;
			itrset[1].raster = iwr->bandarg[i].raster[1];
			itrset[1].nband = 0;

			/* pass everything to iterator */
			if (iwr->bandarg[i].uniontype == UT_MEAN) {
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_UNION, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					NULL,
					rtpg_union_mean_callback,
					&_raster
				);
			}
			else if (iwr->bandarg[i].uniontype == UT_RANGE) {
				noerr = rt_raster_iterator(
					itrset, 2,
					ET_UNION, NULL,
					pixtype,
					hasnodata, nodataval,
					0, 0,
					NULL,
					NULL,
					rtpg_union_range_callback,
					&_raster
				);
			}

			if (noerr != ES_NONE) {
				pfree(itrset);
				rtpg_union_arg_destroy(iwr);
				if (_rtn != NULL)
					rt_raster_destroy(_rtn);
				elog(ERROR, "RASTER_union_finalfn: Could not run raster iterator function");
				PG_RETURN_NULL();
			}
		}
		else
			_raster = iwr->bandarg[i].raster[0];

		/* first band, _rtn doesn't exist */
		if (i < 1) {
			uint32_t bandNums[1] = {0};
			_rtn = rt_raster_from_band(_raster, bandNums, 1);
			status = (_rtn == NULL) ? -1 : 0;
		}
		else
			status = rt_raster_copy_band(_rtn, _raster, 0, i);

		POSTGIS_RT_DEBUG(4, "destroying source rasters");

		/* destroy source rasters */
		if (
			iwr->bandarg[i].uniontype == UT_MEAN ||
			iwr->bandarg[i].uniontype == UT_RANGE
		) {
			rt_raster_destroy(_raster);
		}
			
		for (j = 0; j < iwr->bandarg[i].numraster; j++) {
			if (iwr->bandarg[i].raster[j] == NULL)
				continue;
			rt_raster_destroy(iwr->bandarg[i].raster[j]);
			iwr->bandarg[i].raster[j] = NULL;
		}

		if (status < 0) {
			rtpg_union_arg_destroy(iwr);
			rt_raster_destroy(_rtn);
			elog(ERROR, "RASTER_union_finalfn: Could not add band to final raster");
			PG_RETURN_NULL();
		}
	}

	/* cleanup */
	pfree(itrset);
	rtpg_union_arg_destroy(iwr);

	if (!_rtn) PG_RETURN_NULL();

	pgraster = rt_raster_serialize(_rtn);
	rt_raster_destroy(_rtn);

	POSTGIS_RT_DEBUG(3, "Finished");

	if (!pgraster)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, pgraster->size);
	PG_RETURN_POINTER(pgraster);
}

/* ---------------------------------------------------------------- */
/* Clip raster with geometry                                        */
/* ---------------------------------------------------------------- */

typedef struct rtpg_clip_band_t *rtpg_clip_band;
struct rtpg_clip_band_t {
	int nband; /* band index */
	int hasnodata; /* is there a user-specified NODATA? */
	double nodataval; /* user-specified NODATA */
};

typedef struct rtpg_clip_arg_t *rtpg_clip_arg;
struct rtpg_clip_arg_t {
	rt_extenttype extenttype;
	rt_raster raster;
	rt_raster mask;

	int numbands; /* number of bandargs */
	rtpg_clip_band band;
};

static rtpg_clip_arg rtpg_clip_arg_init() {
	rtpg_clip_arg arg = NULL;

	arg = palloc(sizeof(struct rtpg_clip_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_clip_arg_init: Could not allocate memory for function arguments");
		return NULL;
	}

	arg->extenttype = ET_INTERSECTION;
	arg->raster = NULL;
	arg->mask = NULL;
	arg->numbands = 0;
	arg->band = NULL;

	return arg;
}

static void rtpg_clip_arg_destroy(rtpg_clip_arg arg) {
	if (arg->band != NULL)
		pfree(arg->band);

	if (arg->raster != NULL)
		rt_raster_destroy(arg->raster);
	if (arg->mask != NULL)
		rt_raster_destroy(arg->mask);

	pfree(arg);
}

static int rtpg_clip_callback(
	rt_iterator_arg arg, void *userarg,
	double *value, int *nodata
) {
	*value = 0;
	*nodata = 0;

	/* either is NODATA, output is NODATA */
	if (arg->nodata[0][0][0] || arg->nodata[1][0][0])
		*nodata = 1;
	/* set to value */
	else
		*value = arg->values[0][0][0];

	return 1;
}

PG_FUNCTION_INFO_V1(RASTER_clip);
Datum RASTER_clip(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	LWGEOM *rastgeom = NULL;
	double gt[6] = {0};
	int srid = SRID_UNKNOWN;

	rt_pgraster *pgrtn = NULL;
	rt_raster rtn = NULL;

	GSERIALIZED *gser = NULL;
	LWGEOM *geom = NULL;
	unsigned char *wkb = NULL;
	size_t wkb_len;

	ArrayType *array;
	Oid etype;
	Datum *e;
	bool *nulls;

	int16 typlen;
	bool typbyval;
	char typalign;

	int i = 0;
	int j = 0;
	int k = 0;
	rtpg_clip_arg arg = NULL;
	LWGEOM *tmpgeom = NULL;
	rt_iterator itrset;

	rt_raster _raster = NULL;
	rt_band band = NULL;
	rt_pixtype pixtype;
	int hasnodata;
	double nodataval;
	int noerr = 0;

	POSTGIS_RT_DEBUG(3, "Starting...");

	/* raster or geometry is NULL, return NULL */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	/* init arg */
	arg = rtpg_clip_arg_init();
	if (arg == NULL) {
		elog(ERROR, "RASTER_clip: Could not initialize argument structure");
		PG_RETURN_NULL();
	}

	/* raster (0) */
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* Get raster object */
	arg->raster = rt_raster_deserialize(pgraster, FALSE);
	if (arg->raster == NULL) {
		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_clip: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* raster is empty, return empty raster */
	if (rt_raster_is_empty(arg->raster)) {
		elog(NOTICE, "Input raster is empty. Returning empty raster");

		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);

		rtn = rt_raster_new(0, 0);
		if (rtn == NULL) {
			elog(ERROR, "RASTER_clip: Could not create empty raster");
			PG_RETURN_NULL();
		}

		pgrtn = rt_raster_serialize(rtn);
		rt_raster_destroy(rtn);
		if (NULL == pgrtn)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	/* metadata */
	rt_raster_get_geotransform_matrix(arg->raster, gt);
	srid = clamp_srid(rt_raster_get_srid(arg->raster));

	/* geometry (2) */
	gser = PG_GETARG_GSERIALIZED_P(2);
	geom = lwgeom_from_gserialized(gser);

	/* Get a 2D version of the geometry if necessary */
	if (lwgeom_ndims(geom) > 2) {
		LWGEOM *geom2d = lwgeom_force_2d(geom);
		lwgeom_free(geom);
		geom = geom2d;
	}

	/* check that SRIDs match */
	if (srid != clamp_srid(gserialized_get_srid(gser))) {
		elog(NOTICE, "Geometry provided does not have the same SRID as the raster. Returning NULL");

		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		lwgeom_free(geom);
		PG_FREE_IF_COPY(gser, 2);

		PG_RETURN_NULL();
	}

	/* crop (4) */
	if (!PG_ARGISNULL(4) && !PG_GETARG_BOOL(4))
		arg->extenttype = ET_FIRST;

	/* get intersection geometry of input raster and input geometry */
	if (rt_raster_get_convex_hull(arg->raster, &rastgeom) != ES_NONE) {

		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		lwgeom_free(geom);
		PG_FREE_IF_COPY(gser, 2);

		elog(ERROR, "RASTER_clip: Could not get convex hull of raster");
		PG_RETURN_NULL();
	}

	tmpgeom = lwgeom_intersection(rastgeom, geom);
	lwgeom_free(rastgeom);
	lwgeom_free(geom);
	PG_FREE_IF_COPY(gser, 2);
	geom = tmpgeom;

	/* intersection is empty AND extent type is INTERSECTION, return empty */
	if (lwgeom_is_empty(geom) && arg->extenttype == ET_INTERSECTION) {
		elog(NOTICE, "The input raster and input geometry do not intersect. Returning empty raster");

		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		lwgeom_free(geom);

		rtn = rt_raster_new(0, 0);
		if (rtn == NULL) {
			elog(ERROR, "RASTER_clip: Could not create empty raster");
			PG_RETURN_NULL();
		}

		pgrtn = rt_raster_serialize(rtn);
		rt_raster_destroy(rtn);
		if (NULL == pgrtn)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	/* nband (1) */
	if (!PG_ARGISNULL(1)) {
		array = PG_GETARG_ARRAYTYPE_P(1);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case INT2OID:
			case INT4OID:
				break;
			default:
				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				lwgeom_free(geom);
				elog(ERROR, "RASTER_clip: Invalid data type for band indexes");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(
			array, etype,
			typlen, typbyval, typalign,
			&e, &nulls, &(arg->numbands)
		);

		arg->band = palloc(sizeof(struct rtpg_clip_band_t) * arg->numbands);
		if (arg->band == NULL) {
			rtpg_clip_arg_destroy(arg);
			PG_FREE_IF_COPY(pgraster, 0);
			lwgeom_free(geom);
			elog(ERROR, "RASTER_clip: Could not allocate memory for band arguments");
			PG_RETURN_NULL();
		}

		for (i = 0, j = 0; i < arg->numbands; i++) {
			if (nulls[i]) continue;

			switch (etype) {
				case INT2OID:
					arg->band[j].nband = DatumGetInt16(e[i]) - 1;
					break;
				case INT4OID:
					arg->band[j].nband = DatumGetInt32(e[i]) - 1;
					break;
			}

			j++;
		}

		if (j < arg->numbands) {
			arg->band = repalloc(arg->band, sizeof(struct rtpg_clip_band_t) * j);
			if (arg->band == NULL) {
				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				lwgeom_free(geom);
				elog(ERROR, "RASTER_clip: Could not reallocate memory for band arguments");
				PG_RETURN_NULL();
			}

			arg->numbands = j;
		}

		/* validate band */
		for (i = 0; i < arg->numbands; i++) {
			if (!rt_raster_has_band(arg->raster, arg->band[i].nband)) {
				elog(NOTICE, "Band at index %d not found in raster", arg->band[i].nband + 1);
				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				lwgeom_free(geom);
				PG_RETURN_NULL();
			}

			arg->band[i].hasnodata = 0;
			arg->band[i].nodataval = 0;
		}
	}
	else {
		arg->numbands = rt_raster_get_num_bands(arg->raster);

		/* raster may have no bands */
		if (arg->numbands) {
			arg->band = palloc(sizeof(struct rtpg_clip_band_t) * arg->numbands);
			if (arg->band == NULL) {

				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				lwgeom_free(geom);

				elog(ERROR, "RASTER_clip: Could not allocate memory for band arguments");
				PG_RETURN_NULL();
			}

			for (i = 0; i < arg->numbands; i++) {
				arg->band[i].nband = i;
				arg->band[i].hasnodata = 0;
				arg->band[i].nodataval = 0;
			}
		}
	}

	/* nodataval (3) */
	if (!PG_ARGISNULL(3)) {
		array = PG_GETARG_ARRAYTYPE_P(3);
		etype = ARR_ELEMTYPE(array);
		get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

		switch (etype) {
			case FLOAT4OID:
			case FLOAT8OID:
				break;
			default:
				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				lwgeom_free(geom);
				elog(ERROR, "RASTER_clip: Invalid data type for NODATA values");
				PG_RETURN_NULL();
				break;
		}

		deconstruct_array(
			array, etype,
			typlen, typbyval, typalign,
			&e, &nulls, &k
		);

		/* it doesn't matter if there are more nodataval */
		for (i = 0, j = 0; i < arg->numbands; i++, j++) {
			/* cap j to the last nodataval element */
			if (j >= k)
				j = k - 1;

			if (nulls[j])
				continue;

			arg->band[i].hasnodata = 1;
			switch (etype) {
				case FLOAT4OID:
					arg->band[i].nodataval = DatumGetFloat4(e[j]);
					break;
				case FLOAT8OID:
					arg->band[i].nodataval = DatumGetFloat8(e[j]);
					break;
			}
		}
	}

	/* get wkb of geometry */
	POSTGIS_RT_DEBUG(3, "getting wkb of geometry");
	wkb = lwgeom_to_wkb(geom, WKB_SFSQL, &wkb_len);
	lwgeom_free(geom);

	/* rasterize geometry */
	arg->mask = rt_raster_gdal_rasterize(
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
	if (arg->mask == NULL) {
		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_clip: Could not rasterize intersection geometry");
		PG_RETURN_NULL();
	}

	/* set SRID */
	rt_raster_set_srid(arg->mask, srid);

	/* run iterator */

	/* init itrset */
	itrset = palloc(sizeof(struct rt_iterator_t) * 2);
	if (itrset == NULL) {
		rtpg_clip_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_clip: Could not allocate memory for iterator arguments");
		PG_RETURN_NULL();
	}

	/* one band at a time */
	for (i = 0; i < arg->numbands; i++) {
		POSTGIS_RT_DEBUGF(4, "band arg %d (nband, hasnodata, nodataval) = (%d, %d, %f)",
			i, arg->band[i].nband, arg->band[i].hasnodata, arg->band[i].nodataval);

		band = rt_raster_get_band(arg->raster, arg->band[i].nband);

		/* band metadata */
		pixtype = rt_band_get_pixtype(band);

		if (arg->band[i].hasnodata) {
			hasnodata = 1;
			nodataval = arg->band[i].nodataval;
		}
		else if (rt_band_get_hasnodata_flag(band)) {
			hasnodata = 1;
			rt_band_get_nodata(band, &nodataval);
		}
		else {
			hasnodata = 0;
			nodataval = rt_band_get_min_value(band);
		}

		/* band is NODATA, create NODATA band and continue */
		if (rt_band_get_isnodata_flag(band)) {
			/* create raster */
			if (rtn == NULL) {
				noerr = rt_raster_from_two_rasters(arg->raster, arg->mask, arg->extenttype, &rtn, NULL);
				if (noerr != ES_NONE) {
					rtpg_clip_arg_destroy(arg);
					PG_FREE_IF_COPY(pgraster, 0);
					elog(ERROR, "RASTER_clip: Could not create output raster");
					PG_RETURN_NULL();
				}
			}

			/* create NODATA band */
			if (rt_raster_generate_new_band(rtn, pixtype, nodataval, hasnodata, nodataval, i) < 0) {
				rt_raster_destroy(rtn);
				rtpg_clip_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_clip: Could not add NODATA band to output raster");
				PG_RETURN_NULL();
			}

			continue;
		}

		/* raster */
		itrset[0].raster = arg->raster;
		itrset[0].nband = arg->band[i].nband;
		itrset[0].nbnodata = 1;

		/* mask */
		itrset[1].raster = arg->mask;
		itrset[1].nband = 0;
		itrset[1].nbnodata = 1;

		/* pass to iterator */
		noerr = rt_raster_iterator(
			itrset, 2,
			arg->extenttype, NULL,
			pixtype,
			hasnodata, nodataval,
			0, 0,
			NULL,
			NULL,
			rtpg_clip_callback,
			&_raster
		);

		if (noerr != ES_NONE) {
			pfree(itrset);
			rtpg_clip_arg_destroy(arg);
			if (rtn != NULL) rt_raster_destroy(rtn);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_clip: Could not run raster iterator function");
			PG_RETURN_NULL();
		}

		/* new raster */
		if (rtn == NULL)
			rtn = _raster;
		/* copy band */
		else {
			band = rt_raster_get_band(_raster, 0);
			if (band == NULL) {
				pfree(itrset);
				rtpg_clip_arg_destroy(arg);
				rt_raster_destroy(_raster);
				rt_raster_destroy(rtn);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_clip: Could not get band from working raster");
				PG_RETURN_NULL();
			}

			if (rt_raster_add_band(rtn, band, i) < 0) {
				pfree(itrset);
				rtpg_clip_arg_destroy(arg);
				rt_raster_destroy(_raster);
				rt_raster_destroy(rtn);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_clip: Could not add new band to output raster");
				PG_RETURN_NULL();
			}

			rt_raster_destroy(_raster);
		}
	}

	pfree(itrset);
	rtpg_clip_arg_destroy(arg);
	PG_FREE_IF_COPY(pgraster, 0);

	pgrtn = rt_raster_serialize(rtn);
	rt_raster_destroy(rtn);

	POSTGIS_RT_DEBUG(3, "Finished");

	if (!pgrtn)
		PG_RETURN_NULL();

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/**
 * Reclassify the specified bands of the raster
 */
PG_FUNCTION_INFO_V1(RASTER_reclass);
Datum RASTER_reclass(PG_FUNCTION_ARGS) {
	rt_pgraster *pgraster = NULL;
	rt_pgraster *pgrtn = NULL;
	rt_raster raster = NULL;
	rt_band band = NULL;
	rt_band newband = NULL;
	uint32_t numBands = 0;

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
	int k = 0;

	int a = 0;
	int b = 0;
	int c = 0;

	rt_reclassexpr *exprset = NULL;
	HeapTupleHeader tup;
	bool isnull;
	Datum tupv;
	uint32_t nband = 0;
	char *expr = NULL;
	text *exprtext = NULL;
	double val = 0;
	char *junk = NULL;
	int inc_val = 0;
	int exc_val = 0;
	char *pixeltype = NULL;
	text *pixeltypetext = NULL;
	rt_pixtype pixtype = PT_END;
	double nodataval = 0;
	bool hasnodata = FALSE;

	char **comma_set = NULL;
	int comma_n = 0;
	char **colon_set = NULL;
	int colon_n = 0;
	char **dash_set = NULL;
	int dash_n = 0;

	POSTGIS_RT_DEBUG(3, "RASTER_reclass: Starting");

	/* pgraster is null, return null */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* raster */
	raster = rt_raster_deserialize(pgraster, FALSE);
	if (!raster) {
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_reclass: Could not deserialize raster");
		PG_RETURN_NULL();
	}
	numBands = rt_raster_get_num_bands(raster);
	POSTGIS_RT_DEBUGF(3, "RASTER_reclass: %d possible bands to be reclassified", numBands);

	/* process set of reclassarg */
	POSTGIS_RT_DEBUG(3, "RASTER_reclass: Processing Arg 1 (reclassargset)");
	array = PG_GETARG_ARRAYTYPE_P(1);
	etype = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(etype, &typlen, &typbyval, &typalign);

	deconstruct_array(array, etype, typlen, typbyval, typalign, &e,
		&nulls, &n);

	if (!n) {
		elog(NOTICE, "Invalid argument for reclassargset. Returning original raster");

		pgrtn = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		PG_FREE_IF_COPY(pgraster, 0);
		if (!pgrtn)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	/*
		process each element of reclassarg
		each element is the index of the band to process, the set
			of reclass ranges and the output band's pixeltype
	*/
	for (i = 0; i < n; i++) {
		if (nulls[i]) continue;

		/* each element is a tuple */
		tup = (HeapTupleHeader) DatumGetPointer(e[i]);
		if (NULL == tup) {
			elog(NOTICE, "Invalid argument for reclassargset. Returning original raster");

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}

		/* band index (1-based) */
		tupv = GetAttributeByName(tup, "nband", &isnull);
		if (isnull) {
			elog(NOTICE, "Invalid argument for reclassargset. Missing value of nband for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		nband = DatumGetInt32(tupv);
		POSTGIS_RT_DEBUGF(3, "RASTER_reclass: expression for band %d", nband);

		/* valid band index? */
		if (nband < 1 || nband > numBands) {
			elog(NOTICE, "Invalid argument for reclassargset. Invalid band index (must use 1-based) for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}

		/* reclass expr */
		tupv = GetAttributeByName(tup, "reclassexpr", &isnull);
		if (isnull) {
			elog(NOTICE, "Invalid argument for reclassargset. Missing value of reclassexpr for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		exprtext = (text *) DatumGetPointer(tupv);
		if (NULL == exprtext) {
			elog(NOTICE, "Invalid argument for reclassargset. Missing value of reclassexpr for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		expr = text_to_cstring(exprtext);
		POSTGIS_RT_DEBUGF(4, "RASTER_reclass: expr (raw) %s", expr);
		expr = rtpg_removespaces(expr);
		POSTGIS_RT_DEBUGF(4, "RASTER_reclass: expr (clean) %s", expr);

		/* split string to its components */
		/* comma (,) separating rangesets */
		comma_set = rtpg_strsplit(expr, ",", &comma_n);
		if (comma_n < 1) {
			elog(NOTICE, "Invalid argument for reclassargset. Invalid expression of reclassexpr for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}

		/* set of reclass expressions */
		POSTGIS_RT_DEBUGF(4, "RASTER_reclass: %d possible expressions", comma_n);
		exprset = palloc(comma_n * sizeof(rt_reclassexpr));

		for (a = 0, j = 0; a < comma_n; a++) {
			POSTGIS_RT_DEBUGF(4, "RASTER_reclass: map %s", comma_set[a]);

			/* colon (:) separating range "src" and "dst" */
			colon_set = rtpg_strsplit(comma_set[a], ":", &colon_n);
			if (colon_n != 2) {
				elog(NOTICE, "Invalid argument for reclassargset. Invalid expression of reclassexpr for reclassarg of index %d . Returning original raster", i);
				for (k = 0; k < j; k++) pfree(exprset[k]);
				pfree(exprset);

				pgrtn = rt_raster_serialize(raster);
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
				if (!pgrtn)
					PG_RETURN_NULL();

				SET_VARSIZE(pgrtn, pgrtn->size);
				PG_RETURN_POINTER(pgrtn);
			}

			/* allocate mem for reclass expression */
			exprset[j] = palloc(sizeof(struct rt_reclassexpr_t));

			for (b = 0; b < colon_n; b++) {
				POSTGIS_RT_DEBUGF(4, "RASTER_reclass: range %s", colon_set[b]);

				/* dash (-) separating "min" and "max" */
				dash_set = rtpg_strsplit(colon_set[b], "-", &dash_n);
				if (dash_n < 1 || dash_n > 3) {
					elog(NOTICE, "Invalid argument for reclassargset. Invalid expression of reclassexpr for reclassarg of index %d . Returning original raster", i);
					for (k = 0; k < j; k++) pfree(exprset[k]);
					pfree(exprset);

					pgrtn = rt_raster_serialize(raster);
					rt_raster_destroy(raster);
					PG_FREE_IF_COPY(pgraster, 0);
					if (!pgrtn)
						PG_RETURN_NULL();

					SET_VARSIZE(pgrtn, pgrtn->size);
					PG_RETURN_POINTER(pgrtn);
				}

				for (c = 0; c < dash_n; c++) {
					/* need to handle: (-9999-100 -> "(", "9999", "100" */
					if (
						c < 1 && 
						strlen(dash_set[c]) == 1 && (
							strchr(dash_set[c], '(') != NULL ||
							strchr(dash_set[c], '[') != NULL ||
							strchr(dash_set[c], ')') != NULL ||
							strchr(dash_set[c], ']') != NULL
						)
					) {
						junk = palloc(sizeof(char) * (strlen(dash_set[c + 1]) + 2));
						if (NULL == junk) {
							for (k = 0; k <= j; k++) pfree(exprset[k]);
							pfree(exprset);
							rt_raster_destroy(raster);
							PG_FREE_IF_COPY(pgraster, 0);

							elog(ERROR, "RASTER_reclass: Could not allocate memory");
							PG_RETURN_NULL();
						}

						sprintf(junk, "%s%s", dash_set[c], dash_set[c + 1]);
						c++;
						dash_set[c] = repalloc(dash_set[c], sizeof(char) * (strlen(junk) + 1));
						strcpy(dash_set[c], junk);
						pfree(junk);

						/* rebuild dash_set */
						for (k = 1; k < dash_n; k++) {
							dash_set[k - 1] = repalloc(dash_set[k - 1], (strlen(dash_set[k]) + 1) * sizeof(char));
							strcpy(dash_set[k - 1], dash_set[k]);
						}
						dash_n--;
						c--;
						pfree(dash_set[dash_n]);
						dash_set = repalloc(dash_set, sizeof(char *) * dash_n);
					}

					/* there shouldn't be more than two in dash_n */
					if (c < 1 && dash_n > 2) {
						elog(NOTICE, "Invalid argument for reclassargset. Invalid expression of reclassexpr for reclassarg of index %d . Returning original raster", i);
						for (k = 0; k < j; k++) pfree(exprset[k]);
						pfree(exprset);

						pgrtn = rt_raster_serialize(raster);
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 0);
						if (!pgrtn)
							PG_RETURN_NULL();

						SET_VARSIZE(pgrtn, pgrtn->size);
						PG_RETURN_POINTER(pgrtn);
					}

					/* check interval flags */
					exc_val = 0;
					inc_val = 1;
					/* range */
					if (dash_n != 1) {
						/* min */
						if (c < 1) {
							if (
								strchr(dash_set[c], ')') != NULL ||
								strchr(dash_set[c], ']') != NULL
							) {
								exc_val = 1;
								inc_val = 1;
							}
							else if (strchr(dash_set[c], '(') != NULL){
								inc_val = 0;
							}
							else {
								inc_val = 1;
							}
						}
						/* max */
						else {
							if (
								strrchr(dash_set[c], '(') != NULL ||
								strrchr(dash_set[c], '[') != NULL
							) {
								exc_val = 1;
								inc_val = 0;
							}
							else if (strrchr(dash_set[c], ']') != NULL) {
								inc_val = 1;
							}
							else {
								inc_val = 0;
							}
						}
					}
					POSTGIS_RT_DEBUGF(4, "RASTER_reclass: exc_val %d inc_val %d", exc_val, inc_val);

					/* remove interval flags */
					dash_set[c] = rtpg_chartrim(dash_set[c], "()[]");
					POSTGIS_RT_DEBUGF(4, "RASTER_reclass: min/max (char) %s", dash_set[c]);

					/* value from string to double */
					errno = 0;
					val = strtod(dash_set[c], &junk);
					if (errno != 0 || dash_set[c] == junk) {
						elog(NOTICE, "Invalid argument for reclassargset. Invalid expression of reclassexpr for reclassarg of index %d . Returning original raster", i);
						for (k = 0; k < j; k++) pfree(exprset[k]);
						pfree(exprset);

						pgrtn = rt_raster_serialize(raster);
						rt_raster_destroy(raster);
						PG_FREE_IF_COPY(pgraster, 0);
						if (!pgrtn)
							PG_RETURN_NULL();

						SET_VARSIZE(pgrtn, pgrtn->size);
						PG_RETURN_POINTER(pgrtn);
					}
					POSTGIS_RT_DEBUGF(4, "RASTER_reclass: min/max (double) %f", val);

					/* strsplit removes dash (a.k.a. negative sign), compare now to restore */
					if (c < 1)
						junk = strstr(colon_set[b], dash_set[c]);
					else
						junk = rtpg_strrstr(colon_set[b], dash_set[c]);
					POSTGIS_RT_DEBUGF(
						4,
						"(colon_set[%d], dash_set[%d], junk) = (%s, %s, %s)",
						b, c, colon_set[b], dash_set[c], junk
					);
					/* not beginning of string */
					if (junk != colon_set[b]) {
						/* prior is a dash */
						if (*(junk - 1) == '-') {
							/* prior is beginning of string or prior - 1 char is dash, negative number */
							if (
								((junk - 1) == colon_set[b]) ||
								(*(junk - 2) == '-') ||
								(*(junk - 2) == '[') ||
								(*(junk - 2) == '(')
							) {
								val *= -1.;
							}
						}
					}
					POSTGIS_RT_DEBUGF(4, "RASTER_reclass: min/max (double) %f", val);

					/* src */
					if (b < 1) {
						/* singular value */
						if (dash_n == 1) {
							exprset[j]->src.exc_min = exprset[j]->src.exc_max = exc_val;
							exprset[j]->src.inc_min = exprset[j]->src.inc_max = inc_val;
							exprset[j]->src.min = exprset[j]->src.max = val;
						}
						/* min */
						else if (c < 1) {
							exprset[j]->src.exc_min = exc_val;
							exprset[j]->src.inc_min = inc_val;
							exprset[j]->src.min = val;
						}
						/* max */
						else {
							exprset[j]->src.exc_max = exc_val;
							exprset[j]->src.inc_max = inc_val;
							exprset[j]->src.max = val;
						}
					}
					/* dst */
					else {
						/* singular value */
						if (dash_n == 1)
							exprset[j]->dst.min = exprset[j]->dst.max = val;
						/* min */
						else if (c < 1)
							exprset[j]->dst.min = val;
						/* max */
						else
							exprset[j]->dst.max = val;
					}
				}
				pfree(dash_set);
			}
			pfree(colon_set);

			POSTGIS_RT_DEBUGF(3, "RASTER_reclass: or: %f - %f nr: %f - %f"
				, exprset[j]->src.min
				, exprset[j]->src.max
				, exprset[j]->dst.min
				, exprset[j]->dst.max
			);
			j++;
		}
		pfree(comma_set);

		/* pixel type */
		tupv = GetAttributeByName(tup, "pixeltype", &isnull);
		if (isnull) {
			elog(NOTICE, "Invalid argument for reclassargset. Missing value of pixeltype for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		pixeltypetext = (text *) DatumGetPointer(tupv);
		if (NULL == pixeltypetext) {
			elog(NOTICE, "Invalid argument for reclassargset. Missing value of pixeltype for reclassarg of index %d . Returning original raster", i);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		pixeltype = text_to_cstring(pixeltypetext);
		POSTGIS_RT_DEBUGF(3, "RASTER_reclass: pixeltype %s", pixeltype);
		pixtype = rt_pixtype_index_from_name(pixeltype);

		/* nodata */
		tupv = GetAttributeByName(tup, "nodataval", &isnull);
		if (isnull) {
			nodataval = 0;
			hasnodata = FALSE;
		}
		else {
			nodataval = DatumGetFloat8(tupv);
			hasnodata = TRUE;
		}
		POSTGIS_RT_DEBUGF(3, "RASTER_reclass: nodataval %f", nodataval);
		POSTGIS_RT_DEBUGF(3, "RASTER_reclass: hasnodata %d", hasnodata);

		/* do reclass */
		band = rt_raster_get_band(raster, nband - 1);
		if (!band) {
			elog(NOTICE, "Could not find raster band of index %d. Returning original raster", nband);
			for (k = 0; k < j; k++) pfree(exprset[k]);
			pfree(exprset);

			pgrtn = rt_raster_serialize(raster);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);
			if (!pgrtn)
				PG_RETURN_NULL();

			SET_VARSIZE(pgrtn, pgrtn->size);
			PG_RETURN_POINTER(pgrtn);
		}
		newband = rt_band_reclass(band, pixtype, hasnodata, nodataval, exprset, j);
		if (!newband) {
			for (k = 0; k < j; k++) pfree(exprset[k]);
			pfree(exprset);

			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);

			elog(ERROR, "RASTER_reclass: Could not reclassify raster band of index %d", nband);
			PG_RETURN_NULL();
		}

		/* replace old band with new band */
		if (rt_raster_replace_band(raster, newband, nband - 1) == NULL) {
			for (k = 0; k < j; k++) pfree(exprset[k]);
			pfree(exprset);

			rt_band_destroy(newband);
			rt_raster_destroy(raster);
			PG_FREE_IF_COPY(pgraster, 0);

			elog(ERROR, "RASTER_reclass: Could not replace raster band of index %d with reclassified band", nband);
			PG_RETURN_NULL();
		}

		/* old band is in the variable band */
		rt_band_destroy(band);

		/* free exprset */
		for (k = 0; k < j; k++) pfree(exprset[k]);
		pfree(exprset);
	}

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	PG_FREE_IF_COPY(pgraster, 0);
	if (!pgrtn)
		PG_RETURN_NULL();

	POSTGIS_RT_DEBUG(3, "RASTER_reclass: Finished");

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}

/* ---------------------------------------------------------------- */
/* apply colormap to specified band of a raster                     */
/* ---------------------------------------------------------------- */

typedef struct rtpg_colormap_arg_t *rtpg_colormap_arg;
struct rtpg_colormap_arg_t {
	rt_raster raster;
	int nband; /* 1-based */
	rt_band band;
	rt_bandstats bandstats;

	rt_colormap colormap;
	int nodataentry;

	char **entry;
	int nentry;
	char **element;
	int nelement;
};

static rtpg_colormap_arg
rtpg_colormap_arg_init() {
	rtpg_colormap_arg arg = NULL;

	arg = palloc(sizeof(struct rtpg_colormap_arg_t));
	if (arg == NULL) {
		elog(ERROR, "rtpg_colormap_arg: Could not allocate memory for function arguments");
		return NULL;
	}

	arg->raster = NULL;
	arg->nband = 1;
	arg->band = NULL;
	arg->bandstats = NULL;

	arg->colormap = palloc(sizeof(struct rt_colormap_t));
	if (arg->colormap == NULL) {
		elog(ERROR, "rtpg_colormap_arg: Could not allocate memory for function arguments");
		return NULL;
	}
	arg->colormap->nentry = 0;
	arg->colormap->entry = NULL;
	arg->colormap->ncolor = 4; /* assume RGBA */
	arg->colormap->method = CM_INTERPOLATE;
	arg->nodataentry = -1;

	arg->entry = NULL;
	arg->nentry = 0;
	arg->element = NULL;
	arg->nelement = 0;

	return arg;
}

static void
rtpg_colormap_arg_destroy(rtpg_colormap_arg arg) {
	int i = 0;
	if (arg->raster != NULL)
		rt_raster_destroy(arg->raster);

	if (arg->bandstats != NULL)
		pfree(arg->bandstats);

	if (arg->colormap != NULL) {
		if (arg->colormap->entry != NULL)
			pfree(arg->colormap->entry);
		pfree(arg->colormap);
	}

	if (arg->nentry) {
		for (i = 0; i < arg->nentry; i++) {
			if (arg->entry[i] != NULL)
				pfree(arg->entry[i]);
		}
		pfree(arg->entry);
	}

	if (arg->nelement) {
		for (i = 0; i < arg->nelement; i++)
			pfree(arg->element[i]);
		pfree(arg->element);
	}

	pfree(arg);
	arg = NULL;
}

PG_FUNCTION_INFO_V1(RASTER_colorMap);
Datum RASTER_colorMap(PG_FUNCTION_ARGS)
{
	rt_pgraster *pgraster = NULL;
	rtpg_colormap_arg arg = NULL;
	char *junk = NULL;
	rt_raster raster = NULL;

	POSTGIS_RT_DEBUG(3, "RASTER_colorMap: Starting");

	/* pgraster is NULL, return NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* init arg */
	arg = rtpg_colormap_arg_init();
	if (arg == NULL) {
		elog(ERROR, "RASTER_colorMap: Could not initialize argument structure");
		PG_RETURN_NULL();
	}

	/* raster (0) */
	pgraster = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

	/* raster object */
	arg->raster = rt_raster_deserialize(pgraster, FALSE);
	if (!arg->raster) {
		rtpg_colormap_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_colorMap: Could not deserialize raster");
		PG_RETURN_NULL();
	}

	/* nband (1) */
	if (!PG_ARGISNULL(1))
		arg->nband = PG_GETARG_INT32(1);
	POSTGIS_RT_DEBUGF(4, "nband = %d", arg->nband);

	/* check that band works */
	if (!rt_raster_has_band(arg->raster, arg->nband - 1)) {
		elog(NOTICE, "Raster does not have band at index %d. Returning empty raster", arg->nband);

		raster = rt_raster_clone(arg->raster, 0);
		if (raster == NULL) {
			rtpg_colormap_arg_destroy(arg);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_colorMap: Could not create empty raster");
			PG_RETURN_NULL();
		}

		rtpg_colormap_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);

		pgraster = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (pgraster == NULL)
			PG_RETURN_NULL();

		SET_VARSIZE(pgraster, ((rt_pgraster*) pgraster)->size);
		PG_RETURN_POINTER(pgraster);
	}

	/* get band */
	arg->band = rt_raster_get_band(arg->raster, arg->nband - 1);
	if (arg->band == NULL) {
		int nband = arg->nband;
		rtpg_colormap_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_colorMap: Could not get band at index %d", nband);
		PG_RETURN_NULL();
	}

	/* method (3) */
	if (!PG_ARGISNULL(3)) {
		char *method = NULL;
		char *tmp = text_to_cstring(PG_GETARG_TEXT_P(3));
		POSTGIS_RT_DEBUGF(4, "raw method = %s", tmp);

		method = rtpg_trim(tmp);
		pfree(tmp);
		method = rtpg_strtoupper(method);

		if (strcmp(method, "INTERPOLATE") == 0)
			arg->colormap->method = CM_INTERPOLATE;
		else if (strcmp(method, "EXACT") == 0)
			arg->colormap->method = CM_EXACT;
		else if (strcmp(method, "NEAREST") == 0)
			arg->colormap->method = CM_NEAREST;
		else {
			elog(NOTICE, "Unknown value provided for method. Defaulting to INTERPOLATE");
			arg->colormap->method = CM_INTERPOLATE;
		}
	}
	/* default to INTERPOLATE */
	else
		arg->colormap->method = CM_INTERPOLATE;
	POSTGIS_RT_DEBUGF(4, "method = %d", arg->colormap->method);

	/* colormap (2) */
	if (PG_ARGISNULL(2)) {
		rtpg_colormap_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_colorMap: Value must be provided for colormap");
		PG_RETURN_NULL();
	}
	else {
		char *tmp = NULL;
		char *colormap = text_to_cstring(PG_GETARG_TEXT_P(2));
		char *_entry;
		char *_element;
		int i = 0;
		int j = 0;

		POSTGIS_RT_DEBUGF(4, "colormap = %s", colormap);

		/* empty string */
		if (!strlen(colormap)) {
			rtpg_colormap_arg_destroy(arg);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_colorMap: Value must be provided for colormap");
			PG_RETURN_NULL();
		}

		arg->entry = rtpg_strsplit(colormap, "\n", &(arg->nentry));
		pfree(colormap);
		if (arg->nentry < 1) {
			rtpg_colormap_arg_destroy(arg);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_colorMap: Could not process the value provided for colormap");
			PG_RETURN_NULL();
		}

		/* allocate the max # of colormap entries */
		arg->colormap->entry = palloc(sizeof(struct rt_colormap_entry_t) * arg->nentry);
		if (arg->colormap->entry == NULL) {
			rtpg_colormap_arg_destroy(arg);
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_colorMap: Could not allocate memory for colormap entries");
			PG_RETURN_NULL();
		}
		memset(arg->colormap->entry, 0, sizeof(struct rt_colormap_entry_t) * arg->nentry);

		/* each entry */
		for (i = 0; i < arg->nentry; i++) {
			/* substitute space for other delimiters */
			tmp = rtpg_strreplace(arg->entry[i], ":", " ", NULL);
			_entry = rtpg_strreplace(tmp, ",", " ", NULL);
			pfree(tmp);
			tmp = rtpg_strreplace(_entry, "\t", " ", NULL);
			pfree(_entry);
			_entry = rtpg_trim(tmp);
			pfree(tmp);

			POSTGIS_RT_DEBUGF(4, "Processing entry[%d] = %s", i, arg->entry[i]);
			POSTGIS_RT_DEBUGF(4, "Cleaned entry[%d] = %s", i, _entry);

			/* empty entry, continue */
			if (!strlen(_entry)) {
				POSTGIS_RT_DEBUGF(3, "Skipping empty entry[%d]", i);
				pfree(_entry);
				continue;
			}

			arg->element = rtpg_strsplit(_entry, " ", &(arg->nelement));
			pfree(_entry);
			if (arg->nelement < 2) {
				rtpg_colormap_arg_destroy(arg);
				PG_FREE_IF_COPY(pgraster, 0);
				elog(ERROR, "RASTER_colorMap: Could not process colormap entry %d", i + 1);
				PG_RETURN_NULL();
			}
			else if (arg->nelement > 5) {
				elog(NOTICE, "More than five elements in colormap entry %d. Using at most five elements", i + 1);
				arg->nelement = 5;
			}

			/* smallest # of colors */
			if ((arg->nelement - 1) < arg->colormap->ncolor)
				arg->colormap->ncolor = arg->nelement - 1;

			/* each element of entry */
			for (j = 0; j < arg->nelement; j++) {

				_element = rtpg_trim(arg->element[j]);
				_element = rtpg_strtoupper(_element);
				POSTGIS_RT_DEBUGF(4, "Processing entry[%d][%d] = %s", i, j, arg->element[j]);
				POSTGIS_RT_DEBUGF(4, "Cleaned entry[%d][%d] = %s", i, j, _element);

				/* first element is ALWAYS a band value, percentage OR "nv" string */
				if (j == 0) {
					char *percent = NULL;

					/* NODATA */
					if (
						strcmp(_element, "NV") == 0 ||
						strcmp(_element, "NULL") == 0 ||
						strcmp(_element, "NODATA") == 0
					) {
						POSTGIS_RT_DEBUG(4, "Processing NODATA string");

						if (arg->nodataentry > -1) {
							elog(NOTICE, "More than one NODATA entry found. Using only the first one");
						}
						else {
							arg->colormap->entry[arg->colormap->nentry].isnodata = 1;
							/* no need to set value as value comes from band's NODATA */
							arg->colormap->entry[arg->colormap->nentry].value = 0;
						}
					}
					/* percent value */
					else if ((percent = strchr(_element, '%')) != NULL) {
						double value;
						POSTGIS_RT_DEBUG(4, "Processing percent string");

						/* get the band stats */
						if (arg->bandstats == NULL) {
							POSTGIS_RT_DEBUG(4, "Getting band stats");
							
							arg->bandstats = rt_band_get_summary_stats(arg->band, 1, 1, 0, NULL, NULL, NULL, NULL);
							if (arg->bandstats == NULL) {
								pfree(_element);
								rtpg_colormap_arg_destroy(arg);
								PG_FREE_IF_COPY(pgraster, 0);
								elog(ERROR, "RASTER_colorMap: Could not get band's summary stats to process percentages");
								PG_RETURN_NULL();
							}
						}

						/* get the string before the percent char */
						tmp = palloc(sizeof(char) * (percent - _element + 1));
						if (tmp == NULL) {
							pfree(_element);
							rtpg_colormap_arg_destroy(arg);
							PG_FREE_IF_COPY(pgraster, 0);
							elog(ERROR, "RASTER_colorMap: Could not allocate memory for value of percentage");
							PG_RETURN_NULL();
						}

						memcpy(tmp, _element, percent - _element);
						tmp[percent - _element] = '\0';
						POSTGIS_RT_DEBUGF(4, "Percent value = %s", tmp);

						/* get percentage value */
						errno = 0;
						value = strtod(tmp, NULL);
						pfree(tmp);
						if (errno != 0 || _element == junk) {
							pfree(_element);
							rtpg_colormap_arg_destroy(arg);
							PG_FREE_IF_COPY(pgraster, 0);
							elog(ERROR, "RASTER_colorMap: Could not process percent string to value");
							PG_RETURN_NULL();
						}

						/* check percentage */
						if (value < 0.) {
							elog(NOTICE, "Percentage values cannot be less than zero. Defaulting to zero");
							value = 0.;
						}
						else if (value > 100.) {
							elog(NOTICE, "Percentage values cannot be greater than 100. Defaulting to 100");
							value = 100.;
						}

						/* get the true pixel value */
						/* TODO: should the percentage be quantile based? */
						arg->colormap->entry[arg->colormap->nentry].value = ((value / 100.) * (arg->bandstats->max - arg->bandstats->min)) + arg->bandstats->min;
					}
					/* straight value */
					else {
						errno = 0;
						arg->colormap->entry[arg->colormap->nentry].value = strtod(_element, &junk);
						if (errno != 0 || _element == junk) {
							pfree(_element);
							rtpg_colormap_arg_destroy(arg);
							PG_FREE_IF_COPY(pgraster, 0);
							elog(ERROR, "RASTER_colorMap: Could not process string to value");
							PG_RETURN_NULL();
						}
					}

				}
				/* RGB values (0 - 255) */
				else {
					int value = 0;

					errno = 0;
					value = (int) strtod(_element, &junk);
					if (errno != 0 || _element == junk) {
						pfree(_element);
						rtpg_colormap_arg_destroy(arg);
						PG_FREE_IF_COPY(pgraster, 0);
						elog(ERROR, "RASTER_colorMap: Could not process string to value");
						PG_RETURN_NULL();
					}

					if (value > 255) {
						elog(NOTICE, "RGBA value cannot be greater than 255. Defaulting to 255");
						value = 255;
					}
					else if (value < 0) {
						elog(NOTICE, "RGBA value cannot be less than zero. Defaulting to zero");
						value = 0;
					}
					arg->colormap->entry[arg->colormap->nentry].color[j - 1] = value;
				}

				pfree(_element);
			}

			POSTGIS_RT_DEBUGF(4, "colormap->entry[%d] (isnodata, value, R, G, B, A) = (%d, %f, %d, %d, %d, %d)",
				arg->colormap->nentry,
				arg->colormap->entry[arg->colormap->nentry].isnodata,
				arg->colormap->entry[arg->colormap->nentry].value,
				arg->colormap->entry[arg->colormap->nentry].color[0],
				arg->colormap->entry[arg->colormap->nentry].color[1],
				arg->colormap->entry[arg->colormap->nentry].color[2],
				arg->colormap->entry[arg->colormap->nentry].color[3]
			);

			arg->colormap->nentry++;
		}

		POSTGIS_RT_DEBUGF(4, "colormap->nentry = %d", arg->colormap->nentry);
		POSTGIS_RT_DEBUGF(4, "colormap->ncolor = %d", arg->colormap->ncolor);
	}

	/* call colormap */
	raster = rt_raster_colormap(arg->raster, arg->nband - 1, arg->colormap);
	if (raster == NULL) {
		rtpg_colormap_arg_destroy(arg);
		PG_FREE_IF_COPY(pgraster, 0);
		elog(ERROR, "RASTER_colorMap: Could not create new raster with applied colormap");
		PG_RETURN_NULL();
	}

	rtpg_colormap_arg_destroy(arg);
	PG_FREE_IF_COPY(pgraster, 0);
	pgraster = rt_raster_serialize(raster);
	rt_raster_destroy(raster);

	POSTGIS_RT_DEBUG(3, "RASTER_colorMap: Done");

	if (pgraster == NULL)
		PG_RETURN_NULL();

	SET_VARSIZE(pgraster, ((rt_pgraster*) pgraster)->size);
	PG_RETURN_POINTER(pgraster);
}

PG_FUNCTION_INFO_V1(RASTER_mapAlgebraExpr);
Datum RASTER_mapAlgebraExpr(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_pgraster *pgrtn = NULL;
    rt_raster raster = NULL;
    rt_raster newrast = NULL;
    rt_band band = NULL;
    rt_band newband = NULL;
    int x, y, nband, width, height;
    double r;
    double newnodatavalue = 0.0;
    double newinitialvalue = 0.0;
    double newval = 0.0;
    char *newexpr = NULL;
    char *initexpr = NULL;
    char *expression = NULL;
    int hasnodataval = 0;
    double nodataval = 0.;
    rt_pixtype newpixeltype;
    int skipcomputation = 0;
    int len = 0;
    const int argkwcount = 3;
    enum KEYWORDS { kVAL=0, kX=1, kY=2 };
    char *argkw[] = {"[rast]", "[rast.x]", "[rast.y]"};
    Oid argkwtypes[] = { FLOAT8OID, INT4OID, INT4OID };
    int argcount = 0;
    Oid argtype[] = { FLOAT8OID, INT4OID, INT4OID };
    uint8_t argpos[3] = {0};
    char place[5];
    int idx = 0;
    int ret = -1;
    TupleDesc tupdesc;
    SPIPlanPtr spi_plan = NULL;
    SPITupleTable * tuptable = NULL;
    HeapTuple tuple;
    char * strFromText = NULL;
    Datum *values = NULL;
    Datum datum = (Datum)NULL;
    char *nulls = NULL;
    bool isnull = FALSE;
    int i = 0;
    int j = 0;

    POSTGIS_RT_DEBUG(2, "RASTER_mapAlgebraExpr: Starting...");

    /* Check raster */
    if (PG_ARGISNULL(0)) {
        elog(NOTICE, "Raster is NULL. Returning NULL");
        PG_RETURN_NULL();
    }


    /* Deserialize raster */
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    raster = rt_raster_deserialize(pgraster, FALSE);
    if (NULL == raster) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_mapAlgebraExpr: Could not deserialize raster");
        PG_RETURN_NULL();
    }

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: Getting arguments...");

    if (PG_ARGISNULL(1))
        nband = 1;
    else
        nband = PG_GETARG_INT32(1);

    if (nband < 1)
        nband = 1;


    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: Creating new empty raster...");

    /**
     * Create a new empty raster with having the same georeference as the
     * provided raster
     **/
    width = rt_raster_get_width(raster);
    height = rt_raster_get_height(raster);

    newrast = rt_raster_new(width, height);

    if ( NULL == newrast ) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_mapAlgebraExpr: Could not create a new raster");
        PG_RETURN_NULL();
    }

    rt_raster_set_scale(newrast,
            rt_raster_get_x_scale(raster),
            rt_raster_get_y_scale(raster));

    rt_raster_set_offsets(newrast,
            rt_raster_get_x_offset(raster),
            rt_raster_get_y_offset(raster));

    rt_raster_set_skews(newrast,
            rt_raster_get_x_skew(raster),
            rt_raster_get_y_skew(raster));

    rt_raster_set_srid(newrast, rt_raster_get_srid(raster));


    /**
     * If this new raster is empty (width = 0 OR height = 0) then there is
     * nothing to compute and we return it right now
     **/
    if (rt_raster_is_empty(newrast))
    {
        elog(NOTICE, "Raster is empty. Returning an empty raster");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {

            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }


    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: Getting raster band %d...", nband);

    /**
     * Check if the raster has the required band. Otherwise, return a raster
     * without band
     **/
    if (!rt_raster_has_band(raster, nband - 1)) {
        elog(NOTICE, "Raster does not have the required band. Returning a raster "
                "without a band");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /* Get the raster band */
    band = rt_raster_get_band(raster, nband - 1);
    if ( NULL == band ) {
        elog(NOTICE, "Could not get the required band. Returning a raster "
                "without a band");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

     /*
     * Get NODATA value
     */
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: Getting NODATA value for band...");

    if (rt_band_get_hasnodata_flag(band)) {
        rt_band_get_nodata(band, &newnodatavalue);
    }

    else {
        newnodatavalue = rt_band_get_min_value(band);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: NODATA value for band: = %f",
        newnodatavalue);

    /**
     * We set the initial value of the future band to nodata value. If nodata
     * value is null, then the raster will be initialized to
     * rt_band_get_min_value but all the values should be recomputed anyway
     **/
    newinitialvalue = newnodatavalue;

    /**
     * Set the new pixeltype
     **/
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: Setting pixeltype...");

    if (PG_ARGISNULL(2)) {
        newpixeltype = rt_band_get_pixtype(band);
    }

    else {
        strFromText = text_to_cstring(PG_GETARG_TEXT_P(2));
        newpixeltype = rt_pixtype_index_from_name(strFromText);
        pfree(strFromText);
        if (newpixeltype == PT_END)
            newpixeltype = rt_band_get_pixtype(band);
    }

    if (newpixeltype == PT_END) {
        PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_mapAlgebraExpr: Invalid pixeltype");
        PG_RETURN_NULL();
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: Pixeltype set to %s",
        rt_pixtype_name(newpixeltype));


    /* Construct expression for raster values */
    if (!PG_ARGISNULL(3)) {
        expression = text_to_cstring(PG_GETARG_TEXT_P(3));
        len = strlen("SELECT (") + strlen(expression) + strlen(")::double precision");
        initexpr = (char *)palloc(len + 1);

        strncpy(initexpr, "SELECT (", strlen("SELECT ("));
        strncpy(initexpr + strlen("SELECT ("), expression, strlen(expression));
				strncpy(initexpr + strlen("SELECT (") + strlen(expression), ")::double precision", strlen(")::double precision"));
        initexpr[len] = '\0';

        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: Expression is %s", initexpr);

        /* We don't need this memory */
        /*
				pfree(expression);
        expression = NULL;
				*/
    }



    /**
     * Optimization: If a nodataval is provided, use it for newinitialvalue.
     * Then, we can initialize the raster with this value and skip the
     * computation of nodata values one by one in the main computing loop
     **/
    if (!PG_ARGISNULL(4)) {
				hasnodataval = 1;
				nodataval = PG_GETARG_FLOAT8(4);
				newinitialvalue = nodataval;

        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: new initial value = %f",
            newinitialvalue);
    }
    else
        hasnodataval = 0;



    /**
     * Optimization: If the raster is only filled with nodata values return
     * right now a raster filled with the newinitialvalue
     * TODO: Call rt_band_check_isnodata instead?
     **/
    if (rt_band_get_isnodata_flag(band)) {

        POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: Band is a nodata band, returning "
                "a raster filled with nodata");

        ret = rt_raster_generate_new_band(newrast, newpixeltype,
                newinitialvalue, TRUE, newnodatavalue, 0);

        /* Free memory */
        if (initexpr)
            pfree(initexpr);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }


    /**
     * Optimization: If expression resume to 'RAST' and hasnodataval is zero,
		 * we can just return the band from the original raster
     **/
    if (initexpr != NULL && ( !strcmp(initexpr, "SELECT [rast]") || !strcmp(initexpr, "SELECT [rast.val]") ) && !hasnodataval) {

        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: Expression resumes to RAST. "
                "Returning raster with band %d from original raster", nband);

        POSTGIS_RT_DEBUGF(4, "RASTER_mapAlgebraExpr: New raster has %d bands",
                rt_raster_get_num_bands(newrast));

        rt_raster_copy_band(newrast, raster, nband - 1, 0);

        POSTGIS_RT_DEBUGF(4, "RASTER_mapAlgebraExpr: New raster now has %d bands",
                rt_raster_get_num_bands(newrast));

        if (initexpr)
            pfree(initexpr);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /**
     * Optimization: If expression resume to a constant (it does not contain
     * [rast)
     **/
    if (initexpr != NULL && strstr(initexpr, "[rast") == NULL) {
        ret = SPI_connect();
        if (ret != SPI_OK_CONNECT) {
            PG_FREE_IF_COPY(pgraster, 0);
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not connect to the SPI manager");
            PG_RETURN_NULL();
        };

        /* Execute the expresion into newval */
        ret = SPI_execute(initexpr, FALSE, 0);

        if (ret != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {

            /* Free memory allocated out of the current context */
            if (SPI_tuptable)
                SPI_freetuptable(tuptable);
            PG_FREE_IF_COPY(pgraster, 0);

            SPI_finish();
            elog(ERROR, "RASTER_mapAlgebraExpr: Invalid construction for expression");
            PG_RETURN_NULL();
        }

        tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;

        tuple = tuptable->vals[0];
        newexpr = SPI_getvalue(tuple, tupdesc, 1);
        if ( ! newexpr ) {
            POSTGIS_RT_DEBUG(3, "Constant expression evaluated to NULL, keeping initvalue");
            newval = newinitialvalue;
        } else {
            newval = atof(newexpr);
        }

        SPI_freetuptable(tuptable);

        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: New raster value = %f",
                newval);

        SPI_finish();

        skipcomputation = 1;

        /**
         * Compute the new value, set it and we will return after creating the
         * new raster
         **/
        if (!hasnodataval) {
            newinitialvalue = newval;
            skipcomputation = 2;
        }

        /* Return the new raster as it will be before computing pixel by pixel */
        else if (FLT_NEQ(newval, newinitialvalue)) {
            skipcomputation = 2;
        }
    }

    /**
     * Create the raster receiving all the computed values. Initialize it to the
     * new initial value
     **/
    ret = rt_raster_generate_new_band(newrast, newpixeltype,
            newinitialvalue, TRUE, newnodatavalue, 0);

    /**
     * Optimization: If expression is NULL, or all the pixels could be set in
     * one step, return the initialized raster now
     **/
    /*if (initexpr == NULL || skipcomputation == 2) {*/
    if (expression == NULL || skipcomputation == 2) {

        /* Free memory */
        if (initexpr)
            pfree(initexpr);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    RASTER_DEBUG(3, "RASTER_mapAlgebraExpr: Creating new raster band...");

    /* Get the new raster band */
    newband = rt_raster_get_band(newrast, 0);
    if ( NULL == newband ) {
        elog(NOTICE, "Could not modify band for new raster. Returning new "
                "raster with the original band");

        if (initexpr)
            pfree(initexpr);
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraExpr: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: Main computing loop (%d x %d)",
            width, height);

    if (initexpr != NULL) {
    	/* Convert [rast.val] to [rast] */
        newexpr = rtpg_strreplace(initexpr, "[rast.val]", "[rast]", NULL);
        pfree(initexpr); initexpr=newexpr;

        sprintf(place,"$1");
        for (i = 0, j = 1; i < argkwcount; i++) {
            len = 0;
            newexpr = rtpg_strreplace(initexpr, argkw[i], place, &len);
            pfree(initexpr); initexpr=newexpr;
            if (len > 0) {
                argtype[argcount] = argkwtypes[i];
                argcount++;
                argpos[i] = j++;

                sprintf(place, "$%d", j);
            }
            else {
                argpos[i] = 0;
            }
        }

        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: initexpr = %s", initexpr);

        /* define values */
        values = (Datum *) palloc(sizeof(Datum) * argcount);
        if (values == NULL) {

            SPI_finish();

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            elog(ERROR, "RASTER_mapAlgebraExpr: Could not allocate memory for value parameters of prepared statement");
            PG_RETURN_NULL();
        }

        /* define nulls */
        nulls = (char *)palloc(argcount);
        if (nulls == NULL) {

            SPI_finish();

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            elog(ERROR, "RASTER_mapAlgebraExpr: Could not allocate memory for null parameters of prepared statement");
            PG_RETURN_NULL();
        }

        /* Connect to SPI and prepare the expression */
        ret = SPI_connect();
        if (ret != SPI_OK_CONNECT) {

            if (initexpr)
                pfree(initexpr);
            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            elog(ERROR, "RASTER_mapAlgebraExpr: Could not connect to the SPI manager");
            PG_RETURN_NULL();
        };

        /* Type of all arguments is FLOAT8OID */
        spi_plan = SPI_prepare(initexpr, argcount, argtype);

        if (spi_plan == NULL) {

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            SPI_finish();

            pfree(initexpr);

            elog(ERROR, "RASTER_mapAlgebraExpr: Could not prepare expression");
            PG_RETURN_NULL();
        }
    }

    for (x = 0; x < width; x++) {
        for(y = 0; y < height; y++) {
            ret = rt_band_get_pixel(band, x, y, &r, NULL);

            /**
             * We compute a value only for the withdata value pixel since the
             * nodata value has already been set by the first optimization
             **/
            if (ret == ES_NONE && FLT_NEQ(r, newnodatavalue)) {
                if (skipcomputation == 0) {
                    if (initexpr != NULL) {
                        /* Reset the null arg flags. */
                        memset(nulls, 'n', argcount);

                        for (i = 0; i < argkwcount; i++) {
                            idx = argpos[i];
                            if (idx < 1) continue;
                            idx--;

                            if (i == kX ) {
                                /* x is 0 based index, but SQL expects 1 based index */
                                values[idx] = Int32GetDatum(x+1);
                                nulls[idx] = ' ';
                            }
                            else if (i == kY) {
                                /* y is 0 based index, but SQL expects 1 based index */
                                values[idx] = Int32GetDatum(y+1);
                                nulls[idx] = ' ';
                            }
                            else if (i == kVAL ) {
                                values[idx] = Float8GetDatum(r);
                                nulls[idx] = ' ';
                            }

                        }

                        ret = SPI_execute_plan(spi_plan, values, nulls, FALSE, 0);
                        if (ret != SPI_OK_SELECT || SPI_tuptable == NULL ||
                                SPI_processed != 1) {
                            if (SPI_tuptable)
                                SPI_freetuptable(tuptable);

                            SPI_freeplan(spi_plan);
                            SPI_finish();

                            pfree(values);
                            pfree(nulls);
                            pfree(initexpr);

                            rt_raster_destroy(raster);
                            PG_FREE_IF_COPY(pgraster, 0);
                            rt_raster_destroy(newrast);

                            elog(ERROR, "RASTER_mapAlgebraExpr: Error executing prepared plan");

                            PG_RETURN_NULL();
                        }

                        tupdesc = SPI_tuptable->tupdesc;
                        tuptable = SPI_tuptable;

                        tuple = tuptable->vals[0];
                        datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
                        if ( SPI_result == SPI_ERROR_NOATTRIBUTE ) {
                            POSTGIS_RT_DEBUGF(3, "Expression for pixel %d,%d (value %g) errored, skip setting", x+1,y+1,r);
                            newval = newinitialvalue;
                        }
                        else if ( isnull ) {
                            POSTGIS_RT_DEBUGF(3, "Expression for pixel %d,%d (value %g) evaluated to NULL, skip setting", x+1,y+1,r);
                            newval = newinitialvalue;
                        } else {
                            newval = DatumGetFloat8(datum);
                        }

                        SPI_freetuptable(tuptable);
                    }

                    else
                        newval = newinitialvalue;

                    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraExpr: new value = %f",
                        newval);
                }


                rt_band_set_pixel(newband, x, y, newval, NULL);
            }

        }
    }

    if (initexpr != NULL) {
        SPI_freeplan(spi_plan);
        SPI_finish();

        pfree(values);
        pfree(nulls);
        pfree(initexpr);
    }
    else {
        POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: no SPI cleanup");
    }


    /* The newrast band has been modified */

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: raster modified, serializing it.");
    /* Serialize created raster */

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    pgrtn = rt_raster_serialize(newrast);
    rt_raster_destroy(newrast);
    if (NULL == pgrtn)
        PG_RETURN_NULL();

    SET_VARSIZE(pgrtn, pgrtn->size);

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraExpr: raster serialized");


    POSTGIS_RT_DEBUG(4, "RASTER_mapAlgebraExpr: returning raster");


    PG_RETURN_POINTER(pgrtn);
}

/*
 * One raster user function MapAlgebra.
 */
PG_FUNCTION_INFO_V1(RASTER_mapAlgebraFct);
Datum RASTER_mapAlgebraFct(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_pgraster *pgrtn = NULL;
    rt_raster raster = NULL;
    rt_raster newrast = NULL;
    rt_band band = NULL;
    rt_band newband = NULL;
    int x, y, nband, width, height;
    double r;
    double newnodatavalue = 0.0;
    double newinitialvalue = 0.0;
    double newval = 0.0;
    rt_pixtype newpixeltype;
    int ret = -1;
    Oid oid;
    FmgrInfo cbinfo;
    FunctionCallInfoData cbdata;
    Datum tmpnewval;
    char * strFromText = NULL;
    int k = 0;

    POSTGIS_RT_DEBUG(2, "RASTER_mapAlgebraFct: STARTING...");

    /* Check raster */
    if (PG_ARGISNULL(0)) {
        elog(WARNING, "Raster is NULL. Returning NULL");
        PG_RETURN_NULL();
    }


    /* Deserialize raster */
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    raster = rt_raster_deserialize(pgraster, FALSE);
    if (NULL == raster) {
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_mapAlgebraFct: Could not deserialize raster");
			PG_RETURN_NULL();    
    }

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Getting arguments...");

    /* Get the rest of the arguments */

    if (PG_ARGISNULL(1))
        nband = 1;
    else
        nband = PG_GETARG_INT32(1);

    if (nband < 1)
        nband = 1;
    
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Creating new empty raster...");

    /** 
     * Create a new empty raster with having the same georeference as the
     * provided raster
     **/
    width = rt_raster_get_width(raster);
    height = rt_raster_get_height(raster);

    newrast = rt_raster_new(width, height);

    if ( NULL == newrast ) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        elog(ERROR, "RASTER_mapAlgebraFct: Could not create a new raster");
        PG_RETURN_NULL();
    }

    rt_raster_set_scale(newrast, 
            rt_raster_get_x_scale(raster),
            rt_raster_get_y_scale(raster));

    rt_raster_set_offsets(newrast,
            rt_raster_get_x_offset(raster),
            rt_raster_get_y_offset(raster));

    rt_raster_set_skews(newrast,
            rt_raster_get_x_skew(raster),
            rt_raster_get_y_skew(raster));

    rt_raster_set_srid(newrast, rt_raster_get_srid(raster));            


    /**
     * If this new raster is empty (width = 0 OR height = 0) then there is
     * nothing to compute and we return it right now
     **/
    if (rt_raster_is_empty(newrast)) 
    { 
        elog(NOTICE, "Raster is empty. Returning an empty raster");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFct: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: Getting raster band %d...", nband);

    /**
     * Check if the raster has the required band. Otherwise, return a raster
     * without band
     **/
    if (!rt_raster_has_band(raster, nband - 1)) {
        elog(NOTICE, "Raster does not have the required band. Returning a raster "
            "without a band");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFct: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /* Get the raster band */
    band = rt_raster_get_band(raster, nband - 1);
    if ( NULL == band ) {
        elog(NOTICE, "Could not get the required band. Returning a raster "
            "without a band");
        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFct: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /*
    * Get NODATA value
    */
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Getting NODATA value for band...");

    if (rt_band_get_hasnodata_flag(band)) {
        rt_band_get_nodata(band, &newnodatavalue);
    }

    else {
        newnodatavalue = rt_band_get_min_value(band);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: NODATA value for band: %f",
            newnodatavalue);
    /**
     * We set the initial value of the future band to nodata value. If nodata
     * value is null, then the raster will be initialized to
     * rt_band_get_min_value but all the values should be recomputed anyway
     **/
    newinitialvalue = newnodatavalue;

    /**
     * Set the new pixeltype
     **/    
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Setting pixeltype...");

    if (PG_ARGISNULL(2)) {
        newpixeltype = rt_band_get_pixtype(band);
    }

    else {
        strFromText = text_to_cstring(PG_GETARG_TEXT_P(2)); 
        newpixeltype = rt_pixtype_index_from_name(strFromText);
        pfree(strFromText);
        if (newpixeltype == PT_END)
            newpixeltype = rt_band_get_pixtype(band);
    }
    
    if (newpixeltype == PT_END) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFct: Invalid pixeltype");
        PG_RETURN_NULL();
    }    
    
    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: Pixeltype set to %s",
        rt_pixtype_name(newpixeltype));

    /* Get the name of the callback user function for raster values */
    if (PG_ARGISNULL(3)) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFct: Required function is missing. Returning NULL");
        PG_RETURN_NULL();
    }

    oid = PG_GETARG_OID(3);
    if (oid == InvalidOid) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFct: Got invalid function object id. Returning NULL");
        PG_RETURN_NULL();
    }

    fmgr_info(oid, &cbinfo);

    /* function cannot return set */
    if (cbinfo.fn_retset) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFct: Function provided must return double precision not resultset");
        PG_RETURN_NULL();
    }
    /* function should have correct # of args */
    else if (cbinfo.fn_nargs < 2 || cbinfo.fn_nargs > 3) {

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFct: Function does not have two or three input parameters");
        PG_RETURN_NULL();
    }

    if (cbinfo.fn_nargs == 2)
        k = 1;
    else 
        k = 2;

    if (func_volatile(oid) == 'v') {
        elog(NOTICE, "Function provided is VOLATILE. Unless required and for best performance, function should be IMMUTABLE or STABLE");
    }

    /* prep function call data */
#if POSTGIS_PGSQL_VERSION <= 90
    InitFunctionCallInfoData(cbdata, &cbinfo, 2, InvalidOid, NULL);
#else
    InitFunctionCallInfoData(cbdata, &cbinfo, 2, InvalidOid, NULL, NULL);
#endif
    memset(cbdata.argnull, FALSE, sizeof(bool) * cbinfo.fn_nargs);
    
    /* check that the function isn't strict if the args are null. */
    if (PG_ARGISNULL(4)) {
        if (cbinfo.fn_strict) {

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            elog(ERROR, "RASTER_mapAlgebraFct: Strict callback functions cannot have null parameters");
            PG_RETURN_NULL();
        }

        cbdata.arg[k] = (Datum)NULL;
        cbdata.argnull[k] = TRUE;
    }
    else {
        cbdata.arg[k] = PG_GETARG_DATUM(4);
    }

    /**
     * Optimization: If the raster is only filled with nodata values return
     * right now a raster filled with the nodatavalueexpr
     * TODO: Call rt_band_check_isnodata instead?
     **/
    if (rt_band_get_isnodata_flag(band)) {

        POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Band is a nodata band, returning "
                "a raster filled with nodata");

        ret = rt_raster_generate_new_band(newrast, newpixeltype,
                newinitialvalue, TRUE, newnodatavalue, 0);

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFct: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);               
    }


    /**
     * Create the raster receiving all the computed values. Initialize it to the
     * new initial value
     **/
    ret = rt_raster_generate_new_band(newrast, newpixeltype,
            newinitialvalue, TRUE, newnodatavalue, 0);

    /* Get the new raster band */
    newband = rt_raster_get_band(newrast, 0);
    if ( NULL == newband ) {
        elog(NOTICE, "Could not modify band for new raster. Returning new "
            "raster with the original band");

        rt_raster_destroy(raster);
        PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFct: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);      
    }
    
    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: Main computing loop (%d x %d)",
            width, height);

    for (x = 0; x < width; x++) {
        for(y = 0; y < height; y++) {
            ret = rt_band_get_pixel(band, x, y, &r, NULL);

            /**
             * We compute a value only for the withdata value pixel since the
             * nodata value has already been set by the first optimization
             **/
            if (ret == ES_NONE) {
                if (FLT_EQ(r, newnodatavalue)) {
                    if (cbinfo.fn_strict) {
                        POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: Strict callbacks cannot accept NULL arguments, skipping NODATA cell.");
                        continue;
                    }
                    cbdata.argnull[0] = TRUE;
                    cbdata.arg[0] = (Datum)NULL;
                }
                else {
                    cbdata.argnull[0] = FALSE;
                    cbdata.arg[0] = Float8GetDatum(r);
                }

                /* Add pixel positions if callback has proper # of args */
                if (cbinfo.fn_nargs == 3) {
                    Datum d[2];
                    ArrayType *a;

                    d[0] = Int32GetDatum(x+1);
                    d[1] = Int32GetDatum(y+1);

                    a = construct_array(d, 2, INT4OID, sizeof(int32), true, 'i');

                    cbdata.argnull[1] = FALSE;
                    cbdata.arg[1] = PointerGetDatum(a);
                }

                POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: (%dx%d), r = %f",
                    x, y, r);
                   
                tmpnewval = FunctionCallInvoke(&cbdata);

                if (cbdata.isnull) {
                    newval = newnodatavalue;
                }
                else {
                    newval = DatumGetFloat8(tmpnewval);
                }

                POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFct: new value = %f", 
                    newval);
                
                rt_band_set_pixel(newband, x, y, newval, NULL);
            }

        }
    }
    
    /* The newrast band has been modified */

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: raster modified, serializing it.");
    /* Serialize created raster */

    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    pgrtn = rt_raster_serialize(newrast);
    rt_raster_destroy(newrast);
    if (NULL == pgrtn)
        PG_RETURN_NULL();

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFct: raster serialized");

    POSTGIS_RT_DEBUG(4, "RASTER_mapAlgebraFct: returning raster");
    
    SET_VARSIZE(pgrtn, pgrtn->size);    
    PG_RETURN_POINTER(pgrtn);
}

/**
 * One raster neighborhood MapAlgebra
 */
PG_FUNCTION_INFO_V1(RASTER_mapAlgebraFctNgb);
Datum RASTER_mapAlgebraFctNgb(PG_FUNCTION_ARGS)
{
    rt_pgraster *pgraster = NULL;
    rt_pgraster *pgrtn = NULL;
    rt_raster raster = NULL;
    rt_raster newrast = NULL;
    rt_band band = NULL;
    rt_band newband = NULL;
    int x, y, nband, width, height, ngbwidth, ngbheight, winwidth, winheight, u, v, nIndex, nNullItems;
    double r, rpix;
    double newnodatavalue = 0.0;
    double newinitialvalue = 0.0;
    double newval = 0.0;
    rt_pixtype newpixeltype;
    int ret = -1;
    Oid oid;
    FmgrInfo cbinfo;
    FunctionCallInfoData cbdata;
    Datum tmpnewval;
    ArrayType * neighborDatum;
    char * strFromText = NULL;
    text * txtNodataMode = NULL;
    text * txtCallbackParam = NULL;
    int intReplace = 0;
    float fltReplace = 0;
    bool valuereplace = false, pixelreplace, nNodataOnly = true, nNullSkip = false;
    Datum * neighborData = NULL;
    bool * neighborNulls = NULL;
    int neighborDims[2];
    int neighborLbs[2];
    int16 typlen;
    bool typbyval;
    char typalign;

    POSTGIS_RT_DEBUG(2, "RASTER_mapAlgebraFctNgb: STARTING...");

    /* Check raster */
    if (PG_ARGISNULL(0)) {
        elog(WARNING, "Raster is NULL. Returning NULL");
        PG_RETURN_NULL();
    }


    /* Deserialize raster */
    pgraster = (rt_pgraster *)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    raster = rt_raster_deserialize(pgraster, FALSE);
    if (NULL == raster)
    {
			PG_FREE_IF_COPY(pgraster, 0);
			elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not deserialize raster");
			PG_RETURN_NULL();    
    }

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: Getting arguments...");

    /* Get the rest of the arguments */

    if (PG_ARGISNULL(1))
        nband = 1;
    else
        nband = PG_GETARG_INT32(1);

    if (nband < 1)
        nband = 1;
    
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: Creating new empty raster...");

    /** 
     * Create a new empty raster with having the same georeference as the
     * provided raster
     **/
    width = rt_raster_get_width(raster);
    height = rt_raster_get_height(raster);

    newrast = rt_raster_new(width, height);

    if ( NULL == newrast ) {
				rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not create a new raster");
        PG_RETURN_NULL();
    }

    rt_raster_set_scale(newrast, 
            rt_raster_get_x_scale(raster),
            rt_raster_get_y_scale(raster));

    rt_raster_set_offsets(newrast,
            rt_raster_get_x_offset(raster),
            rt_raster_get_y_offset(raster));

    rt_raster_set_skews(newrast,
            rt_raster_get_x_skew(raster),
            rt_raster_get_y_skew(raster));

    rt_raster_set_srid(newrast, rt_raster_get_srid(raster));            


    /**
     * If this new raster is empty (width = 0 OR height = 0) then there is
     * nothing to compute and we return it right now
     **/
    if (rt_raster_is_empty(newrast)) 
    { 
        elog(NOTICE, "Raster is empty. Returning an empty raster");
        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: Getting raster band %d...", nband);

    /**
     * Check if the raster has the required band. Otherwise, return a raster
     * without band
     **/
    if (!rt_raster_has_band(raster, nband - 1)) {
        elog(NOTICE, "Raster does not have the required band. Returning a raster "
            "without a band");
        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /* Get the raster band */
    band = rt_raster_get_band(raster, nband - 1);
    if ( NULL == band ) {
        elog(NOTICE, "Could not get the required band. Returning a raster "
            "without a band");
        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);
    }

    /*
    * Get NODATA value
    */
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: Getting NODATA value for band...");

    if (rt_band_get_hasnodata_flag(band)) {
        rt_band_get_nodata(band, &newnodatavalue);
    }

    else {
        newnodatavalue = rt_band_get_min_value(band);
    }

    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: NODATA value for band: %f",
            newnodatavalue);
    /**
     * We set the initial value of the future band to nodata value. If nodata
     * value is null, then the raster will be initialized to
     * rt_band_get_min_value but all the values should be recomputed anyway
     **/
    newinitialvalue = newnodatavalue;

    /**
     * Set the new pixeltype
     **/    
    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: Setting pixeltype...");

    if (PG_ARGISNULL(2)) {
        newpixeltype = rt_band_get_pixtype(band);
    }

    else {
        strFromText = text_to_cstring(PG_GETARG_TEXT_P(2)); 
        POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: Pixeltype parameter: %s", strFromText);
        newpixeltype = rt_pixtype_index_from_name(strFromText);
        pfree(strFromText);
        if (newpixeltype == PT_END)
            newpixeltype = rt_band_get_pixtype(band);
    }
    
    if (newpixeltype == PT_END) {

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFctNgb: Invalid pixeltype");
        PG_RETURN_NULL();
    }    
    
    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: Pixeltype set to %s (%d)",
        rt_pixtype_name(newpixeltype), newpixeltype);

    /* Get the name of the callback userfunction */
    if (PG_ARGISNULL(5)) {

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFctNgb: Required function is missing");
        PG_RETURN_NULL();
    }

    oid = PG_GETARG_OID(5);
    if (oid == InvalidOid) {

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFctNgb: Got invalid function object id");
        PG_RETURN_NULL();
    }

    fmgr_info(oid, &cbinfo);

    /* function cannot return set */
    if (cbinfo.fn_retset) {

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFctNgb: Function provided must return double precision not resultset");
        PG_RETURN_NULL();
    }
    /* function should have correct # of args */
    else if (cbinfo.fn_nargs != 3) {

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);
        rt_raster_destroy(newrast);

        elog(ERROR, "RASTER_mapAlgebraFctNgb: Function does not have three input parameters");
        PG_RETURN_NULL();
    }

    if (func_volatile(oid) == 'v') {
        elog(NOTICE, "Function provided is VOLATILE. Unless required and for best performance, function should be IMMUTABLE or STABLE");
    }

    /* prep function call data */
#if POSTGIS_PGSQL_VERSION <= 90
    InitFunctionCallInfoData(cbdata, &cbinfo, 3, InvalidOid, NULL);
#else
    InitFunctionCallInfoData(cbdata, &cbinfo, 3, InvalidOid, NULL, NULL);
#endif
    memset(cbdata.argnull, FALSE, sizeof(bool) * 3);

    /* check that the function isn't strict if the args are null. */
    if (PG_ARGISNULL(7)) {
        if (cbinfo.fn_strict) {

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);
            rt_raster_destroy(newrast);

            elog(ERROR, "RASTER_mapAlgebraFctNgb: Strict callback functions cannot have NULL parameters");
            PG_RETURN_NULL();
        }

        cbdata.arg[2] = (Datum)NULL;
        cbdata.argnull[2] = TRUE;
    }
    else {
        cbdata.arg[2] = PG_GETARG_DATUM(7);
    }

    /**
     * Optimization: If the raster is only filled with nodata values return
     * right now a raster filled with the nodatavalueexpr
     * TODO: Call rt_band_check_isnodata instead?
     **/
    if (rt_band_get_isnodata_flag(band)) {

        POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: Band is a nodata band, returning "
                "a raster filled with nodata");

        rt_raster_generate_new_band(newrast, newpixeltype,
                newinitialvalue, TRUE, newnodatavalue, 0);

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);               
    }


    /**
     * Create the raster receiving all the computed values. Initialize it to the
     * new initial value
     **/
    rt_raster_generate_new_band(newrast, newpixeltype,
            newinitialvalue, TRUE, newnodatavalue, 0);

    /* Get the new raster band */
    newband = rt_raster_get_band(newrast, 0);
    if ( NULL == newband ) {
        elog(NOTICE, "Could not modify band for new raster. Returning new "
            "raster with the original band");

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);      
    }

    /* Get the width of the neighborhood */
    if (PG_ARGISNULL(3) || PG_GETARG_INT32(3) <= 0) {
        elog(NOTICE, "Neighborhood width is NULL or <= 0. Returning new "
            "raster with the original band");

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);      
    }

    ngbwidth = PG_GETARG_INT32(3);
    winwidth = ngbwidth * 2 + 1;

    /* Get the height of the neighborhood */
    if (PG_ARGISNULL(4) || PG_GETARG_INT32(4) <= 0) {
        elog(NOTICE, "Neighborhood height is NULL or <= 0. Returning new "
            "raster with the original band");

        rt_raster_destroy(raster);
				PG_FREE_IF_COPY(pgraster, 0);

        /* Serialize created raster */
        pgrtn = rt_raster_serialize(newrast);
        rt_raster_destroy(newrast);
        if (NULL == pgrtn) {
            elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
            PG_RETURN_NULL();
        }

        SET_VARSIZE(pgrtn, pgrtn->size);
        PG_RETURN_POINTER(pgrtn);      
    }

    ngbheight = PG_GETARG_INT32(4);
    winheight = ngbheight * 2 + 1;

    /* Get the type of NODATA behavior for the neighborhoods. */
    if (PG_ARGISNULL(6)) {
        elog(NOTICE, "Neighborhood NODATA behavior defaulting to 'ignore'");
        txtNodataMode = cstring_to_text("ignore");
    }
    else {
        txtNodataMode = PG_GETARG_TEXT_P(6);
    }

    txtCallbackParam = (text*)palloc(VARSIZE(txtNodataMode));
    SET_VARSIZE(txtCallbackParam, VARSIZE(txtNodataMode));
    memcpy((void *)VARDATA(txtCallbackParam), (void *)VARDATA(txtNodataMode), VARSIZE(txtNodataMode) - VARHDRSZ);

    /* pass the nodata mode into the user function */
    cbdata.arg[1] = CStringGetDatum(txtCallbackParam);

    strFromText = text_to_cstring(txtNodataMode);
    strFromText = rtpg_strtoupper(strFromText);

    if (strcmp(strFromText, "VALUE") == 0)
        valuereplace = true;
    else if (strcmp(strFromText, "IGNORE") != 0 && strcmp(strFromText, "NULL") != 0) {
        /* if the text is not "IGNORE" or "NULL", it may be a numerical value */
        if (sscanf(strFromText, "%d", &intReplace) <= 0 && sscanf(strFromText, "%f", &fltReplace) <= 0) {
            /* the value is NOT an integer NOR a floating point */
            elog(NOTICE, "Neighborhood NODATA mode is not recognized. Must be one of 'value', 'ignore', "
                "'NULL', or a numeric value. Returning new raster with the original band");

            /* clean up the nodatamode string */
            pfree(txtCallbackParam);
            pfree(strFromText);

            rt_raster_destroy(raster);
            PG_FREE_IF_COPY(pgraster, 0);

            /* Serialize created raster */
            pgrtn = rt_raster_serialize(newrast);
            rt_raster_destroy(newrast);
            if (NULL == pgrtn) {
                elog(ERROR, "RASTER_mapAlgebraFctNgb: Could not serialize raster");
                PG_RETURN_NULL();
            }

            SET_VARSIZE(pgrtn, pgrtn->size);
            PG_RETURN_POINTER(pgrtn);      
        }
    }
    else if (strcmp(strFromText, "NULL") == 0) {
        /* this setting means that the neighborhood should be skipped if any of the values are null */
        nNullSkip = true;
    }
   
    POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: Main computing loop (%d x %d)",
            width, height);

    /* Allocate room for the neighborhood. */
    neighborData = (Datum *)palloc(winwidth * winheight * sizeof(Datum));
    neighborNulls = (bool *)palloc(winwidth * winheight * sizeof(bool));

    /* The dimensions of the neighborhood array, for creating a multi-dimensional array. */
    neighborDims[0] = winwidth;
    neighborDims[1] = winheight;

    /* The lower bounds for the new multi-dimensional array. */
    neighborLbs[0] = 1;
    neighborLbs[1] = 1;

    /* Get information about the type of item in the multi-dimensional array (float8). */
    get_typlenbyvalalign(FLOAT8OID, &typlen, &typbyval, &typalign);

    for (x = 0 + ngbwidth; x < width - ngbwidth; x++) {
        for(y = 0 + ngbheight; y < height - ngbheight; y++) {
            /* populate an array with the pixel values in the neighborhood */
            nIndex = 0;
            nNullItems = 0;
            nNodataOnly = true;
            pixelreplace = false;
            if (valuereplace) {
                ret = rt_band_get_pixel(band, x, y, &rpix, NULL);
                if (ret == ES_NONE && FLT_NEQ(rpix, newnodatavalue)) {
                    pixelreplace = true;
                }
            }
            for (u = x - ngbwidth; u <= x + ngbwidth; u++) {
                for (v = y - ngbheight; v <= y + ngbheight; v++) {
                    ret = rt_band_get_pixel(band, u, v, &r, NULL);
                    if (ret == ES_NONE) {
                        if (FLT_NEQ(r, newnodatavalue)) {
                            /* If the pixel value for this neighbor cell is not NODATA */
                            neighborData[nIndex] = Float8GetDatum((double)r);
                            neighborNulls[nIndex] = false;
                            nNodataOnly = false;
                        }
                        else {
                            /* If the pixel value for this neighbor cell is NODATA */
                            if (valuereplace && pixelreplace) {
                                /* Replace the NODATA value with the currently processing pixel. */
                                neighborData[nIndex] = Float8GetDatum((double)rpix);
                                neighborNulls[nIndex] = false;
                                /* do not increment nNullItems, since the user requested that the  */
                                /* neighborhood replace NODATA values with the central pixel value */
                            }
                            else {
                                neighborData[nIndex] = PointerGetDatum(NULL);
                                neighborNulls[nIndex] = true;
                                nNullItems++;
                            }
                        }
                    }
                    else {
                        /* Fill this will NULL if we can't read the raster pixel. */
                        neighborData[nIndex] = PointerGetDatum(NULL);
                        neighborNulls[nIndex] = true;
                        nNullItems++;
                    }
                    /* Next neighbor position */
                    nIndex++;
                }
            }

            /**
             * We compute a value only for the withdata value neighborhood since the
             * nodata value has already been set by the first optimization
             **/
            if (!(nNodataOnly ||                     /* neighborhood only contains NODATA -- OR -- */
                (nNullSkip && nNullItems > 0) ||     /* neighborhood should skip any NODATA cells, and a NODATA cell was detected -- OR -- */
                (valuereplace && nNullItems > 0))) { /* neighborhood should replace NODATA cells with the central pixel value, and a NODATA cell was detected */
                POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: (%dx%d), %dx%d neighborhood",
                    x, y, winwidth, winheight);

                neighborDatum = construct_md_array((void *)neighborData, neighborNulls, 2, neighborDims, neighborLbs, 
                    FLOAT8OID, typlen, typbyval, typalign);

                /* Assign the neighbor matrix as the first argument to the user function */
                cbdata.arg[0] = PointerGetDatum(neighborDatum);

                /* Invoke the user function */
                tmpnewval = FunctionCallInvoke(&cbdata);

                /* Get the return value of the user function */
                if (cbdata.isnull) {
                    newval = newnodatavalue;
                }
                else {
                    newval = DatumGetFloat8(tmpnewval);
                }

                POSTGIS_RT_DEBUGF(3, "RASTER_mapAlgebraFctNgb: new value = %f", 
                    newval);
                
                rt_band_set_pixel(newband, x, y, newval, NULL);
            }

            /* reset the number of null items in the neighborhood */
            nNullItems = 0;
        }
    }


    /* clean up */
    pfree(neighborNulls);
    pfree(neighborData);
    pfree(strFromText);
    pfree(txtCallbackParam);
    
    rt_raster_destroy(raster);
    PG_FREE_IF_COPY(pgraster, 0);

    /* The newrast band has been modified */

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: raster modified, serializing it.");
    /* Serialize created raster */

    pgrtn = rt_raster_serialize(newrast);
    rt_raster_destroy(newrast);
    if (NULL == pgrtn)
        PG_RETURN_NULL();

    POSTGIS_RT_DEBUG(3, "RASTER_mapAlgebraFctNgb: raster serialized");
    POSTGIS_RT_DEBUG(4, "RASTER_mapAlgebraFctNgb: returning raster");
    
    SET_VARSIZE(pgrtn, pgrtn->size);    
    PG_RETURN_POINTER(pgrtn);
}

/**
 * Two raster MapAlgebra
 */
PG_FUNCTION_INFO_V1(RASTER_mapAlgebra2);
Datum RASTER_mapAlgebra2(PG_FUNCTION_ARGS)
{
	const int set_count = 2;
	rt_pgraster *pgrast[2];
	int pgrastpos[2] = {-1, -1};
	rt_pgraster *pgrtn;
	rt_raster rast[2] = {NULL};
	int _isempty[2] = {0};
	uint32_t bandindex[2] = {0};
	rt_raster _rast[2] = {NULL};
	rt_band _band[2] = {NULL};
	int _hasnodata[2] = {0};
	double _nodataval[2] = {0};
	double _offset[4] = {0.};
	double _rastoffset[2][4] = {{0.}};
	int _haspixel[2] = {0};
	double _pixel[2] = {0};
	int _pos[2][2] = {{0}};
	uint16_t _dim[2][2] = {{0}};

	char *pixtypename = NULL;
	rt_pixtype pixtype = PT_END;
	char *extenttypename = NULL;
	rt_extenttype extenttype = ET_INTERSECTION;

	rt_raster raster = NULL;
	rt_band band = NULL;
	uint16_t dim[2] = {0};
	int haspixel = 0;
	double pixel = 0.;
	double nodataval = 0;
	double gt[6] = {0.};

	Oid calltype = InvalidOid;

	const int spi_count = 3;
	uint16_t spi_exprpos[3] = {4, 7, 8};
	uint32_t spi_argcount[3] = {0};
	char *expr = NULL;
	char *sql = NULL;
	SPIPlanPtr spi_plan[3] = {NULL};
	uint16_t spi_empty = 0;
	Oid *argtype = NULL;
	const int argkwcount = 8;
	uint8_t argpos[3][8] = {{0}};
	char *argkw[] = {"[rast1.x]", "[rast1.y]", "[rast1.val]", "[rast1]", "[rast2.x]", "[rast2.y]", "[rast2.val]", "[rast2]"};
	Datum values[argkwcount];
	bool nulls[argkwcount];
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple;
	Datum datum;
	bool isnull = FALSE;
	int hasargval[3] = {0};
	double argval[3] = {0.};
	int hasnodatanodataval = 0;
	double nodatanodataval = 0;
	int isnodata = 0;

	Oid ufc_noid = InvalidOid;
	FmgrInfo ufl_info;
	FunctionCallInfoData ufc_info;
	int ufc_nullcount = 0;

	int idx = 0;
	uint32_t i = 0;
	uint32_t j = 0;
	uint32_t k = 0;
	uint32_t x = 0;
	uint32_t y = 0;
	int _x = 0;
	int _y = 0;
	int err;
	int aligned = 0;
	int len = 0;

	POSTGIS_RT_DEBUG(3, "Starting RASTER_mapAlgebra2");

	for (i = 0, j = 0; i < set_count; i++) {
		if (!PG_ARGISNULL(j)) {
			pgrast[i] = (rt_pgraster *) PG_DETOAST_DATUM(PG_GETARG_DATUM(j));
			pgrastpos[i] = j;
			j++;

			/* raster */
			rast[i] = rt_raster_deserialize(pgrast[i], FALSE);
			if (!rast[i]) {
				for (k = 0; k <= i; k++) {
					if (k < i && rast[k] != NULL)
						rt_raster_destroy(rast[k]);
					if (pgrastpos[k] != -1)
						PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				elog(ERROR, "RASTER_mapAlgebra2: Could not deserialize the %s raster", i < 1 ? "first" : "second");
				PG_RETURN_NULL();
			}

			/* empty */
			_isempty[i] = rt_raster_is_empty(rast[i]);

			/* band index */
			if (!PG_ARGISNULL(j)) {
				bandindex[i] = PG_GETARG_INT32(j);
			}
			j++;
		}
		else {
			_isempty[i] = 1;
			j += 2;
		}

		POSTGIS_RT_DEBUGF(3, "_isempty[%d] = %d", i, _isempty[i]);
	}

	/* both rasters are NULL */
	if (rast[0] == NULL && rast[1] == NULL) {
		elog(NOTICE, "The two rasters provided are NULL.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* both rasters are empty */
	if (_isempty[0] && _isempty[1]) {
		elog(NOTICE, "The two rasters provided are empty.  Returning empty raster");

		raster = rt_raster_new(0, 0);
		if (raster == NULL) {
			for (k = 0; k < set_count; k++) {
				if (rast[k] != NULL)
					rt_raster_destroy(rast[k]);
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_mapAlgebra2: Could not create empty raster");
			PG_RETURN_NULL();
		}
		rt_raster_set_scale(raster, 0, 0);

		pgrtn = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (!pgrtn)
			PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	/* replace the empty or NULL raster with one matching the other */
	if (
		(rast[0] == NULL || _isempty[0]) ||
		(rast[1] == NULL || _isempty[1])
	) {
		/* first raster is empty */
		if (rast[0] == NULL || _isempty[0]) {
			i = 0;
			j = 1;
		}
		/* second raster is empty */
		else {
			i = 1;
			j = 0;
		}

		_rast[j] = rast[j];

		/* raster is empty, destroy it */
		if (_rast[i] != NULL)
			rt_raster_destroy(_rast[i]);

		_dim[i][0] = rt_raster_get_width(_rast[j]);
		_dim[i][1] = rt_raster_get_height(_rast[j]);
		_dim[j][0] = rt_raster_get_width(_rast[j]);
		_dim[j][1] = rt_raster_get_height(_rast[j]);

		_rast[i] = rt_raster_new(
			_dim[j][0],
			_dim[j][1]
		);
		if (_rast[i] == NULL) {
			rt_raster_destroy(_rast[j]);
			for (k = 0; k < set_count; k++)	{
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_mapAlgebra2: Could not create NODATA raster");
			PG_RETURN_NULL();
		}
		rt_raster_set_srid(_rast[i], rt_raster_get_srid(_rast[j]));

		rt_raster_get_geotransform_matrix(_rast[j], gt);
		rt_raster_set_geotransform_matrix(_rast[i], gt);
	}
	else {
		_rast[0] = rast[0];
		_dim[0][0] = rt_raster_get_width(_rast[0]);
		_dim[0][1] = rt_raster_get_height(_rast[0]);

		_rast[1] = rast[1];
		_dim[1][0] = rt_raster_get_width(_rast[1]);
		_dim[1][1] = rt_raster_get_height(_rast[1]);
	}

	/* SRID must match */
	/*
	if (rt_raster_get_srid(_rast[0]) != rt_raster_get_srid(_rast[1])) {
		elog(NOTICE, "The two rasters provided have different SRIDs.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}
	*/

	/* same alignment */
	if (rt_raster_same_alignment(_rast[0], _rast[1], &aligned, NULL) != ES_NONE) {
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "RASTER_mapAlgebra2: Could not test for alignment on the two rasters");
		PG_RETURN_NULL();
	}
	if (!aligned) {
		elog(NOTICE, "The two rasters provided do not have the same alignment.  Returning NULL");
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		PG_RETURN_NULL();
	}

	/* pixel type */
	if (!PG_ARGISNULL(5)) {
		pixtypename = text_to_cstring(PG_GETARG_TEXT_P(5));
		/* Get the pixel type index */
		pixtype = rt_pixtype_index_from_name(pixtypename);
		if (pixtype == PT_END ) {
			for (k = 0; k < set_count; k++) {
				if (_rast[k] != NULL)
					rt_raster_destroy(_rast[k]);
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_mapAlgebra2: Invalid pixel type: %s", pixtypename);
			PG_RETURN_NULL();
		}
	}

	/* extent type */
	if (!PG_ARGISNULL(6)) {
		extenttypename = rtpg_strtoupper(rtpg_trim(text_to_cstring(PG_GETARG_TEXT_P(6))));
		extenttype = rt_util_extent_type(extenttypename);
	}
	POSTGIS_RT_DEBUGF(3, "extenttype: %d %s", extenttype, extenttypename);

	/* computed raster from extent type */
	err = rt_raster_from_two_rasters(
		_rast[0], _rast[1],
		extenttype,
		&raster, _offset
	);
	if (err != ES_NONE) {
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		elog(ERROR, "RASTER_mapAlgebra2: Could not get output raster of correct extent");
		PG_RETURN_NULL();
	}

	/* copy offsets */
	_rastoffset[0][0] = _offset[0];
	_rastoffset[0][1] = _offset[1];
	_rastoffset[1][0] = _offset[2];
	_rastoffset[1][1] = _offset[3];

	/* get output raster dimensions */
	dim[0] = rt_raster_get_width(raster);
	dim[1] = rt_raster_get_height(raster);

	i = 2;
	/* handle special cases for extent */
	switch (extenttype) {
		case ET_FIRST:
			i = 0;
		case ET_SECOND:
			if (i > 1)
				i = 1;

			if (
				_isempty[i] && (
					(extenttype == ET_FIRST && i == 0) ||
					(extenttype == ET_SECOND && i == 1)
				)
			) {
				elog(NOTICE, "The %s raster is NULL.  Returning NULL", (i != 1 ? "FIRST" : "SECOND"));
				for (k = 0; k < set_count; k++) {
					if (_rast[k] != NULL)
						rt_raster_destroy(_rast[k]);
					if (pgrastpos[k] != -1)
						PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				rt_raster_destroy(raster);
				PG_RETURN_NULL();
			}

			/* specified band not found */
			if (!rt_raster_has_band(_rast[i], bandindex[i] - 1)) {
				elog(NOTICE, "The %s raster does not have the band at index %d.  Returning no band raster of correct extent",
					(i != 1 ? "FIRST" : "SECOND"), bandindex[i]
				);

				for (k = 0; k < set_count; k++) {
					if (_rast[k] != NULL)
						rt_raster_destroy(_rast[k]);
					if (pgrastpos[k] != -1)
						PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}

				pgrtn = rt_raster_serialize(raster);
				rt_raster_destroy(raster);
				if (!pgrtn) PG_RETURN_NULL();

				SET_VARSIZE(pgrtn, pgrtn->size);
				PG_RETURN_POINTER(pgrtn);
			}
			break;
		case ET_UNION:
			break;
		case ET_INTERSECTION:
			/* no intersection */
			if (
				_isempty[0] || _isempty[1] ||
				!dim[0] || !dim[1]
			) {
				elog(NOTICE, "The two rasters provided have no intersection.  Returning no band raster");

				/* raster has dimension, replace with no band raster */
				if (dim[0] || dim[1]) {
					rt_raster_destroy(raster);

					raster = rt_raster_new(0, 0);
					if (raster == NULL) {
						for (k = 0; k < set_count; k++) {
							if (_rast[k] != NULL)
								rt_raster_destroy(_rast[k]);
							if (pgrastpos[k] != -1)
								PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
						}
						elog(ERROR, "RASTER_mapAlgebra2: Could not create no band raster");
						PG_RETURN_NULL();
					}

					rt_raster_set_scale(raster, 0, 0);
					rt_raster_set_srid(raster, rt_raster_get_srid(_rast[0]));
				}

				for (k = 0; k < set_count; k++) {
					if (_rast[k] != NULL)
						rt_raster_destroy(_rast[k]);
					if (pgrastpos[k] != -1)
						PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}

				pgrtn = rt_raster_serialize(raster);
				rt_raster_destroy(raster);
				if (!pgrtn) PG_RETURN_NULL();

				SET_VARSIZE(pgrtn, pgrtn->size);
				PG_RETURN_POINTER(pgrtn);
			}
			break;
		case ET_LAST:
		case ET_CUSTOM:
			for (k = 0; k < set_count; k++) {
				if (_rast[k] != NULL)
					rt_raster_destroy(_rast[k]);
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			elog(ERROR, "RASTER_mapAlgebra2: ET_LAST and ET_CUSTOM are not implemented");
			PG_RETURN_NULL();
			break;
	}

	/* both rasters do not have specified bands */
	if (
		(!_isempty[0] && !rt_raster_has_band(_rast[0], bandindex[0] - 1)) &&
		(!_isempty[1] && !rt_raster_has_band(_rast[1], bandindex[1] - 1))
	) {
		elog(NOTICE, "The two rasters provided do not have the respectively specified band indices.  Returning no band raster of correct extent");

		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}

		pgrtn = rt_raster_serialize(raster);
		rt_raster_destroy(raster);
		if (!pgrtn) PG_RETURN_NULL();

		SET_VARSIZE(pgrtn, pgrtn->size);
		PG_RETURN_POINTER(pgrtn);
	}

	/* get bands */
	for (i = 0; i < set_count; i++) {
		if (_isempty[i] || !rt_raster_has_band(_rast[i], bandindex[i] - 1)) {
			_hasnodata[i] = 1;
			_nodataval[i] = 0;

			continue;
		}

		_band[i] = rt_raster_get_band(_rast[i], bandindex[i] - 1);
		if (_band[i] == NULL) {
			for (k = 0; k < set_count; k++) {
				if (_rast[k] != NULL)
					rt_raster_destroy(_rast[k]);
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			rt_raster_destroy(raster);
			elog(ERROR, "RASTER_mapAlgebra2: Could not get band %d of the %s raster",
				bandindex[i],
				(i < 1 ? "FIRST" : "SECOND")
			);
			PG_RETURN_NULL();
		}

		_hasnodata[i] = rt_band_get_hasnodata_flag(_band[i]);
		if (_hasnodata[i])
			rt_band_get_nodata(_band[i], &(_nodataval[i]));
	}

	/* pixtype is PT_END, get pixtype based upon extent */
	if (pixtype == PT_END) {
		if ((extenttype == ET_SECOND && !_isempty[1]) || _isempty[0])
			pixtype = rt_band_get_pixtype(_band[1]);
		else
			pixtype = rt_band_get_pixtype(_band[0]);
	}

	/* nodata value for new band */
	if (extenttype == ET_SECOND && !_isempty[1] && _hasnodata[1]) {
		nodataval = _nodataval[1];
	}
	else if (!_isempty[0] && _hasnodata[0]) {
		nodataval = _nodataval[0];
	}
	else if (!_isempty[1] && _hasnodata[1]) {
		nodataval = _nodataval[1];
	}
	else {
		elog(NOTICE, "Neither raster provided has a NODATA value for the specified band indices.  NODATA value set to minimum possible for %s", rt_pixtype_name(pixtype));
		nodataval = rt_pixtype_get_min_value(pixtype);
	}

	/* add band to output raster */
	if (rt_raster_generate_new_band(
		raster,
		pixtype,
		nodataval,
		1, nodataval,
		0
	) < 0) {
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		rt_raster_destroy(raster);
		elog(ERROR, "RASTER_mapAlgebra2: Could not add new band to output raster");
		PG_RETURN_NULL();
	}

	/* get output band */
	band = rt_raster_get_band(raster, 0);
	if (band == NULL) {
		for (k = 0; k < set_count; k++) {
			if (_rast[k] != NULL)
				rt_raster_destroy(_rast[k]);
			if (pgrastpos[k] != -1)
				PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
		}
		rt_raster_destroy(raster);
		elog(ERROR, "RASTER_mapAlgebra2: Could not get newly added band of output raster");
		PG_RETURN_NULL();
	}

	POSTGIS_RT_DEBUGF(4, "offsets = (%d, %d, %d, %d)",
		(int) _rastoffset[0][0],
		(int) _rastoffset[0][1],
		(int) _rastoffset[1][0],
		(int) _rastoffset[1][1]
	);

	POSTGIS_RT_DEBUGF(4, "metadata = (%f, %f, %d, %d, %f, %f, %f, %f, %d)",
		rt_raster_get_x_offset(raster),
		rt_raster_get_y_offset(raster),
		rt_raster_get_width(raster),
		rt_raster_get_height(raster),
		rt_raster_get_x_scale(raster),
		rt_raster_get_y_scale(raster),
		rt_raster_get_x_skew(raster),
		rt_raster_get_y_skew(raster),
		rt_raster_get_srid(raster)
	);

	/*
		determine who called this function
		Arg 4 will either be text or regprocedure
	*/
	POSTGIS_RT_DEBUG(3, "checking parameter type for arg 4");
	calltype = get_fn_expr_argtype(fcinfo->flinfo, 4);

	switch(calltype) {
		case TEXTOID: {
			POSTGIS_RT_DEBUG(3, "arg 4 is \"expression\"!");

			/* connect SPI */
			if (SPI_connect() != SPI_OK_CONNECT) {
				for (k = 0; k < set_count; k++) {
					if (_rast[k] != NULL)
						rt_raster_destroy(_rast[k]);
					if (pgrastpos[k] != -1)
						PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
				}
				rt_raster_destroy(raster);
				elog(ERROR, "RASTER_mapAlgebra2: Could not connect to the SPI manager");
				PG_RETURN_NULL();
			}

			/* reset hasargval */
			memset(hasargval, 0, sizeof(int) * spi_count);

			/*
				process expressions

				spi_exprpos elements are:
					4 - expression => spi_plan[0]
					7 - nodata1expr => spi_plan[1]
					8 - nodata2expr => spi_plan[2]
			*/
			for (i = 0; i < spi_count; i++) {
				if (!PG_ARGISNULL(spi_exprpos[i])) {
					char *tmp = NULL;
					char place[5] = "$1";
					expr = text_to_cstring(PG_GETARG_TEXT_P(spi_exprpos[i]));
					POSTGIS_RT_DEBUGF(3, "raw expr #%d: %s", i, expr);

					for (j = 0, k = 1; j < argkwcount; j++) {
						/* attempt to replace keyword with placeholder */
						len = 0;
						tmp = rtpg_strreplace(expr, argkw[j], place, &len);
						pfree(expr);
						expr = tmp;

						if (len) {
							spi_argcount[i]++;
							argpos[i][j] = k++;

							sprintf(place, "$%d", k);
						}
						else
							argpos[i][j] = 0;
					}

					len = strlen("SELECT (") + strlen(expr) + strlen(")::double precision");
					sql = (char *) palloc(len + 1);
					if (sql == NULL) {

						for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
						SPI_finish();

						for (k = 0; k < set_count; k++) {
							if (_rast[k] != NULL)
								rt_raster_destroy(_rast[k]);
							if (pgrastpos[k] != -1)
								PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
						}
						rt_raster_destroy(raster);

						elog(ERROR, "RASTER_mapAlgebra2: Could not allocate memory for expression parameter %d", spi_exprpos[i]);
						PG_RETURN_NULL();
					}

					strncpy(sql, "SELECT (", strlen("SELECT ("));
					strncpy(sql + strlen("SELECT ("), expr, strlen(expr));
					strncpy(sql + strlen("SELECT (") + strlen(expr), ")::double precision", strlen(")::double precision"));
					sql[len] = '\0';

					POSTGIS_RT_DEBUGF(3, "sql #%d: %s", i, sql);

					/* create prepared plan */
					if (spi_argcount[i]) {
						argtype = (Oid *) palloc(spi_argcount[i] * sizeof(Oid));
						if (argtype == NULL) {

							pfree(sql);
							for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
							SPI_finish();

							for (k = 0; k < set_count; k++) {
								if (_rast[k] != NULL)
									rt_raster_destroy(_rast[k]);
								if (pgrastpos[k] != -1)
									PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
							}
							rt_raster_destroy(raster);

							elog(ERROR, "RASTER_mapAlgebra2: Could not allocate memory for prepared plan argtypes of expression parameter %d", spi_exprpos[i]);
							PG_RETURN_NULL();
						}

						/* specify datatypes of parameters */
						for (j = 0, k = 0; j < argkwcount; j++) {
							if (argpos[i][j] < 1) continue;

							/* positions are INT4 */
							if (
								(strstr(argkw[j], "[rast1.x]") != NULL) ||
								(strstr(argkw[j], "[rast1.y]") != NULL) ||
								(strstr(argkw[j], "[rast2.x]") != NULL) ||
								(strstr(argkw[j], "[rast2.y]") != NULL)
							) {
								argtype[k] = INT4OID;
							}
							/* everything else is FLOAT8 */
							else {
								argtype[k] = FLOAT8OID;
							}

							k++;
						}

						spi_plan[i] = SPI_prepare(sql, spi_argcount[i], argtype);
						pfree(argtype);

						if (spi_plan[i] == NULL) {

							pfree(sql);
							for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
							SPI_finish();

							for (k = 0; k < set_count; k++) {
								if (_rast[k] != NULL)
									rt_raster_destroy(_rast[k]);
								if (pgrastpos[k] != -1)
									PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
							}
							rt_raster_destroy(raster);

							elog(ERROR, "RASTER_mapAlgebra2: Could not create prepared plan of expression parameter %d", spi_exprpos[i]);
							PG_RETURN_NULL();
						}
					}
					/* no args, just execute query */
					else {
						err = SPI_execute(sql, TRUE, 0);
						if (err != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {

							pfree(sql);
							for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
							SPI_finish();

							for (k = 0; k < set_count; k++) {
								if (_rast[k] != NULL)
									rt_raster_destroy(_rast[k]);
								if (pgrastpos[k] != -1)
									PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
							}
							rt_raster_destroy(raster);

							elog(ERROR, "RASTER_mapAlgebra2: Could not evaluate expression parameter %d", spi_exprpos[i]);
							PG_RETURN_NULL();
						}

						/* get output of prepared plan */
						tupdesc = SPI_tuptable->tupdesc;
						tuptable = SPI_tuptable;
						tuple = tuptable->vals[0];

						datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
						if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

							pfree(sql);
							if (SPI_tuptable) SPI_freetuptable(tuptable);
							for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
							SPI_finish();

							for (k = 0; k < set_count; k++) {
								if (_rast[k] != NULL)
									rt_raster_destroy(_rast[k]);
								if (pgrastpos[k] != -1)
									PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
							}
							rt_raster_destroy(raster);

							elog(ERROR, "RASTER_mapAlgebra2: Could not get result of expression parameter %d", spi_exprpos[i]);
							PG_RETURN_NULL();
						}

						if (!isnull) {
							hasargval[i] = 1;
							argval[i] = DatumGetFloat8(datum);
						}

						if (SPI_tuptable) SPI_freetuptable(tuptable);
					}

					pfree(sql);
				}
				else
					spi_empty++;
			}

			/* nodatanodataval */
			if (!PG_ARGISNULL(9)) {
				hasnodatanodataval = 1;
				nodatanodataval = PG_GETARG_FLOAT8(9);
			}
			else
				hasnodatanodataval = 0;
			break;
		}
		case REGPROCEDUREOID: {
			POSTGIS_RT_DEBUG(3, "arg 4 is \"userfunction\"!");
			if (!PG_ARGISNULL(4)) {

				ufc_nullcount = 0;
				ufc_noid = PG_GETARG_OID(4);

				/* get function info */
				fmgr_info(ufc_noid, &ufl_info);

				/* function cannot return set */
				err = 0;
				if (ufl_info.fn_retset) {
					err = 1;
				}
				/* function should have correct # of args */
				else if (ufl_info.fn_nargs < 3 || ufl_info.fn_nargs > 4) {
					err = 2;
				}

				/*
					TODO: consider adding checks of the userfunction parameters
						should be able to use get_fn_expr_argtype() of fmgr.c
				*/

				if (err > 0) {
					for (k = 0; k < set_count; k++) {
						if (_rast[k] != NULL)
							rt_raster_destroy(_rast[k]);
						if (pgrastpos[k] != -1)
							PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
					}
					rt_raster_destroy(raster);

					if (err > 1)
						elog(ERROR, "RASTER_mapAlgebra2: Function provided must have three or four input parameters");
					else
						elog(ERROR, "RASTER_mapAlgebra2: Function provided must return double precision not resultset");
					PG_RETURN_NULL();
				}

				if (func_volatile(ufc_noid) == 'v') {
					elog(NOTICE, "Function provided is VOLATILE. Unless required and for best performance, function should be IMMUTABLE or STABLE");
				}

				/* prep function call data */
#if POSTGIS_PGSQL_VERSION <= 90
				InitFunctionCallInfoData(ufc_info, &ufl_info, ufl_info.fn_nargs, InvalidOid, NULL);
#else
				InitFunctionCallInfoData(ufc_info, &ufl_info, ufl_info.fn_nargs, InvalidOid, NULL, NULL);
#endif
				memset(ufc_info.argnull, FALSE, sizeof(bool) * ufl_info.fn_nargs);

				if (ufl_info.fn_nargs != 4)
					k = 2;
				else
					k = 3;
				if (!PG_ARGISNULL(7)) {
					ufc_info.arg[k] = PG_GETARG_DATUM(7);
				}
				else {
				 ufc_info.arg[k] = (Datum) NULL;
				 ufc_info.argnull[k] = TRUE;
				 ufc_nullcount++;
				}
			}
			break;
		}
		default:
			for (k = 0; k < set_count; k++) {
				if (_rast[k] != NULL)
					rt_raster_destroy(_rast[k]);
				if (pgrastpos[k] != -1)
					PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
			}
			rt_raster_destroy(raster);
			elog(ERROR, "RASTER_mapAlgebra2: Invalid data type for expression or userfunction");
			PG_RETURN_NULL();
			break;
	}

	/* loop over pixels */
	/* if any expression present, run */
	if ((
		(calltype == TEXTOID) && (
			(spi_empty != spi_count) || hasnodatanodataval
		)
	) || (
		(calltype == REGPROCEDUREOID) && (ufc_noid != InvalidOid)
	)) {
		for (x = 0; x < dim[0]; x++) {
			for (y = 0; y < dim[1]; y++) {

				/* get pixel from each raster */
				for (i = 0; i < set_count; i++) {
					_haspixel[i] = 0;
					_pixel[i] = 0;

					/* row/column */
					_x = x - (int) _rastoffset[i][0];
					_y = y - (int) _rastoffset[i][1];

					/* store _x and _y in 1-based */
					_pos[i][0] = _x + 1;
					_pos[i][1] = _y + 1;

					/* get pixel value */
					if (_band[i] == NULL) {
						if (!_hasnodata[i]) {
							_haspixel[i] = 1;
							_pixel[i] = _nodataval[i];
						}
					}
					else if (
						!_isempty[i] &&
						(_x >= 0 && _x < _dim[i][0]) &&
						(_y >= 0 && _y < _dim[i][1])
					) {
						err = rt_band_get_pixel(_band[i], _x, _y, &(_pixel[i]), &isnodata);
						if (err != ES_NONE) {

							if (calltype == TEXTOID) {
								for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
								SPI_finish();
							}

							for (k = 0; k < set_count; k++) {
								if (_rast[k] != NULL)
									rt_raster_destroy(_rast[k]);
								if (pgrastpos[k] != -1)
									PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
							}
							rt_raster_destroy(raster);

							elog(ERROR, "RASTER_mapAlgebra2: Could not get pixel of %s raster", (i < 1 ? "FIRST" : "SECOND"));
							PG_RETURN_NULL();
						}

						if (!_hasnodata[i] || !isnodata)
							_haspixel[i] = 1;
					}

					POSTGIS_RT_DEBUGF(5, "pixel r%d(%d, %d) = %d, %f",
						i,
						_x, _y,
						_haspixel[i],
						_pixel[i]
					);
				}

				haspixel = 0;

				switch (calltype) {
					case TEXTOID: {
						/* which prepared plan to use? */
						/* !pixel0 && !pixel1 */
						/* use nodatanodataval */
						if (!_haspixel[0] && !_haspixel[1])
							i = 3;
						/* pixel0 && !pixel1 */
						/* run spi_plan[2] (nodata2expr) */
						else if (_haspixel[0] && !_haspixel[1])
							i = 2;
						/* !pixel0 && pixel1 */
						/* run spi_plan[1] (nodata1expr) */
						else if (!_haspixel[0] && _haspixel[1])
							i = 1;
						/* pixel0 && pixel1 */
						/* run spi_plan[0] (expression) */
						else
							i = 0;

						/* process values */
						if (i == 3) {
							if (hasnodatanodataval) {
								haspixel = 1;
								pixel = nodatanodataval;
							}
						}
						/* has an evaluated value */
						else if (hasargval[i]) {
							haspixel = 1;
							pixel = argval[i];
						}
						/* prepared plan exists */
						else if (spi_plan[i] != NULL) {
							POSTGIS_RT_DEBUGF(4, "Using prepared plan: %d", i);

							/* expression has argument(s) */
							if (spi_argcount[i]) {
								/* reset values to (Datum) NULL */
								memset(values, (Datum) NULL, sizeof(Datum) * argkwcount);
								/* reset nulls to FALSE */
								memset(nulls, FALSE, sizeof(bool) * argkwcount);

								/* set values and nulls */
								for (j = 0; j < argkwcount; j++) {
									idx = argpos[i][j];
									if (idx < 1) continue;
									idx--; /* 1-based becomes 0-based */

									if (strstr(argkw[j], "[rast1.x]") != NULL) {
										values[idx] = _pos[0][0];
									}
									else if (strstr(argkw[j], "[rast1.y]") != NULL) {
										values[idx] = _pos[0][1];
									}
									else if (
										(strstr(argkw[j], "[rast1.val]") != NULL) ||
										(strstr(argkw[j], "[rast1]") != NULL)
									) {
										if (_isempty[0] || !_haspixel[0])
											nulls[idx] = TRUE;
										else
											values[idx] = Float8GetDatum(_pixel[0]);
									}
									else if (strstr(argkw[j], "[rast2.x]") != NULL) {
										values[idx] = _pos[1][0];
									}
									else if (strstr(argkw[j], "[rast2.y]") != NULL) {
										values[idx] = _pos[1][1];
									}
									else if (
										(strstr(argkw[j], "[rast2.val]") != NULL) ||
										(strstr(argkw[j], "[rast2]") != NULL)
									) {
										if (_isempty[1] || !_haspixel[1])
											nulls[idx] = TRUE;
										else
											values[idx] = Float8GetDatum(_pixel[1]);
									}
								}
							}

							/* run prepared plan */
							err = SPI_execute_plan(spi_plan[i], values, nulls, TRUE, 1);
							if (err != SPI_OK_SELECT || SPI_tuptable == NULL || SPI_processed != 1) {

								for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
								SPI_finish();

								for (k = 0; k < set_count; k++) {
									if (_rast[k] != NULL)
										rt_raster_destroy(_rast[k]);
									if (pgrastpos[k] != -1)
										PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
								}
								rt_raster_destroy(raster);

								elog(ERROR, "RASTER_mapAlgebra2: Unexpected error when running prepared statement %d", i);
								PG_RETURN_NULL();
							}

							/* get output of prepared plan */
							tupdesc = SPI_tuptable->tupdesc;
							tuptable = SPI_tuptable;
							tuple = tuptable->vals[0];

							datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
							if (SPI_result == SPI_ERROR_NOATTRIBUTE) {

								if (SPI_tuptable) SPI_freetuptable(tuptable);
								for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
								SPI_finish();

								for (k = 0; k < set_count; k++) {
									if (_rast[k] != NULL)
										rt_raster_destroy(_rast[k]);
									if (pgrastpos[k] != -1)
										PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
								}
								rt_raster_destroy(raster);

								elog(ERROR, "RASTER_mapAlgebra2: Could not get result of prepared statement %d", i);
								PG_RETURN_NULL();
							}

							if (!isnull) {
								haspixel = 1;
								pixel = DatumGetFloat8(datum);
							}

							if (SPI_tuptable) SPI_freetuptable(tuptable);
						}
					}	break;
					case REGPROCEDUREOID: {
						Datum d[4];
						ArrayType *a;

						/* build fcnarg */
						for (i = 0; i < set_count; i++) {
							ufc_info.arg[i] = Float8GetDatum(_pixel[i]);

							if (_haspixel[i]) {
								ufc_info.argnull[i] = FALSE;
								ufc_nullcount--;
							}
							else {
								ufc_info.argnull[i] = TRUE;
				 				ufc_nullcount++;
							}
						}

						/* function is strict and null parameter is passed */
						/* http://archives.postgresql.org/pgsql-general/2011-11/msg00424.php */
						if (ufl_info.fn_strict && ufc_nullcount)
							break;

						/* 4 parameters, add position */
						if (ufl_info.fn_nargs == 4) {
							/* Datum of 4 element array */
							/* array is (x1, y1, x2, y2) */
							for (i = 0; i < set_count; i++) {
								if (i < 1) {
									d[0] = Int32GetDatum(_pos[i][0]);
									d[1] = Int32GetDatum(_pos[i][1]);
								}
								else {
									d[2] = Int32GetDatum(_pos[i][0]);
									d[3] = Int32GetDatum(_pos[i][1]);
								}
							}

							a = construct_array(d, 4, INT4OID, sizeof(int32), true, 'i');
							ufc_info.arg[2] = PointerGetDatum(a);
							ufc_info.argnull[2] = FALSE;
						}

						datum = FunctionCallInvoke(&ufc_info);

						/* result is not null*/
						if (!ufc_info.isnull) {
							haspixel = 1;
							pixel = DatumGetFloat8(datum);
						}
					}	break;
				}

				/* burn pixel if haspixel != 0 */
				if (haspixel) {
					if (rt_band_set_pixel(band, x, y, pixel, NULL) != ES_NONE) {

						if (calltype == TEXTOID) {
							for (k = 0; k < spi_count; k++) SPI_freeplan(spi_plan[k]);
							SPI_finish();
						}

						for (k = 0; k < set_count; k++) {
							if (_rast[k] != NULL)
								rt_raster_destroy(_rast[k]);
							if (pgrastpos[k] != -1)
								PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
						}
						rt_raster_destroy(raster);

						elog(ERROR, "RASTER_mapAlgebra2: Could not set pixel value of output raster");
						PG_RETURN_NULL();
					}
				}

				POSTGIS_RT_DEBUGF(5, "(x, y, val) = (%d, %d, %f)", x, y, haspixel ? pixel : nodataval);

			} /* y: height */
		} /* x: width */
	}

	/* CLEANUP */
	if (calltype == TEXTOID) {
		for (i = 0; i < spi_count; i++) {
			if (spi_plan[i] != NULL) SPI_freeplan(spi_plan[i]);
		}
		SPI_finish();
	}

	for (k = 0; k < set_count; k++) {
		if (_rast[k] != NULL)
			rt_raster_destroy(_rast[k]);
		if (pgrastpos[k] != -1)
			PG_FREE_IF_COPY(pgrast[k], pgrastpos[k]);
	}

	pgrtn = rt_raster_serialize(raster);
	rt_raster_destroy(raster);
	if (!pgrtn) PG_RETURN_NULL();

	POSTGIS_RT_DEBUG(3, "Finished RASTER_mapAlgebra2");

	SET_VARSIZE(pgrtn, pgrtn->size);
	PG_RETURN_POINTER(pgrtn);
}
