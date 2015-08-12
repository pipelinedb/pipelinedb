/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Portions Copyright 2009 Paul Ramsey <pramsey@opengeo.org>
 * Portions Copyright 2013-2015 PipelineDB
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/tupmacs.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/lsyscache.h"

#include "../postgis_config.h"

#include "liblwgeom.h"
#include "lwgeom_pg.h"
#include "lwgeom_transform.h"

/* Local prototypes */
Datum PGISDirectFunctionCall1(PGFunction func, Datum arg1);
Datum pgis_geometry_accum_transfn(PG_FUNCTION_ARGS);
Datum pgis_geometry_accum_finalfn(PG_FUNCTION_ARGS);
Datum pgis_geometry_union_finalfn(PG_FUNCTION_ARGS);
Datum pgis_geometry_collect_finalfn(PG_FUNCTION_ARGS);
Datum pgis_geometry_polygonize_finalfn(PG_FUNCTION_ARGS);
Datum pgis_geometry_makeline_finalfn(PG_FUNCTION_ARGS);
Datum pgis_abs_in(PG_FUNCTION_ARGS);
Datum pgis_abs_out(PG_FUNCTION_ARGS);

/* External prototypes */
Datum pgis_union_geometry_array(PG_FUNCTION_ARGS);
Datum LWGEOM_collect_garray(PG_FUNCTION_ARGS);
Datum polygonize_garray(PG_FUNCTION_ARGS);
Datum LWGEOM_makeline_garray(PG_FUNCTION_ARGS);


/** @file
** Versions of PostgreSQL < 8.4 perform array accumulation internally using
** pass by value, which is very slow working with large/many geometries.
** Hence PostGIS currently implements its own aggregate for building
** geometry arrays using pass by reference, which is significantly faster and
** similar to the method used in PostgreSQL 8.4.
**
** Hence we can revert this to the original aggregate functions from 1.3 at
** whatever point PostgreSQL 8.4 becomes the minimum version we support :)
*/


/**
** To pass the internal ArrayBuildState pointer between the
** transfn and finalfn we need to wrap it into a custom type first,
** the pgis_abs type in our case.
*/

typedef struct
{
	ArrayBuildState *a;
}
pgis_abs;



/**
** We're never going to use this type externally so the in/out
** functions are dummies.
*/
PG_FUNCTION_INFO_V1(pgis_abs_in);
Datum
pgis_abs_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	               errmsg("function %s not implemented", __func__)));
	PG_RETURN_POINTER(NULL);
}
PG_FUNCTION_INFO_V1(pgis_abs_out);
Datum
pgis_abs_out(PG_FUNCTION_ARGS)
{
	ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	               errmsg("function %s not implemented", __func__)));
	PG_RETURN_POINTER(NULL);
}

/*
 * Serializes as pgis_abs
 */
PG_FUNCTION_INFO_V1(pgis_abs_send);
Datum
pgis_abs_send(PG_FUNCTION_ARGS)
{
	pgis_abs *p = (pgis_abs *) PG_GETARG_POINTER(0);
	ArrayType *arr = DirectFunctionCall1(arrayaggstatesend, (Datum) p->a);

	PG_RETURN_ARRAYTYPE_P(arr);
}

/*
 * Deserializes a pgis_abs
 */
PG_FUNCTION_INFO_V1(pgis_abs_recv);
Datum
pgis_abs_recv(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	pgis_abs *result;
	ArrayType *arr;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	arr = (ArrayType *) PG_GETARG_ARRAYTYPE_P_COPY(0);
	fcinfo->arg[0] = (Datum) arr;
	result = palloc0(sizeof(pgis_abs));
	result->a = (ArrayBuildState *) arrayaggstaterecv(fcinfo);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * Combines two pgis_abs transition states into one
 */
PG_FUNCTION_INFO_V1(pgis_geometry_accum_combinefn);
Datum
pgis_geometry_accum_combinefn(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext aggcontext;
	pgis_abs *state = PG_ARGISNULL(0) ? NULL : (pgis_abs *) PG_GETARG_POINTER(0);
	pgis_abs *incoming = (pgis_abs *) PG_GETARG_POINTER(1);
	int i;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "pgis_geometry_accum_combinefn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_POINTER(incoming);

	old = MemoryContextSwitchTo(aggcontext);

	for (i=0; i<incoming->a->nelems; i++)
	{
		state->a = accumArrayResult(state->a,
				incoming->a->dvalues[i], incoming->a->dnulls[i],
				incoming->a->element_type, aggcontext);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/**
** The transfer function hooks into the PostgreSQL accumArrayResult()
** function (present since 8.0) to build an array in a side memory
** context.
*/
PG_FUNCTION_INFO_V1(pgis_geometry_accum_transfn);
Datum
pgis_geometry_accum_transfn(PG_FUNCTION_ARGS)
{
	Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext;
	ArrayBuildState *state;
	pgis_abs *p;
	Datum elem;

	if (arg1_typeid == InvalidOid)
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		         errmsg("could not determine input data type")));

	if ( ! AggCheckCallContext(fcinfo, &aggcontext) )
	{
		/* cannot be called directly because of dummy-type argument */
		elog(ERROR, "%s called in non-aggregate context", __func__);
		aggcontext = NULL;  /* keep compiler quiet */
	}

	if ( PG_ARGISNULL(0) )
	{
		p = (pgis_abs*) palloc(sizeof(pgis_abs));
		p->a = NULL;
	}
	else
	{
		p = (pgis_abs*) PG_GETARG_POINTER(0);
	}
	state = p->a;
	elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
	state = accumArrayResult(state,
	                         elem,
	                         PG_ARGISNULL(1),
	                         arg1_typeid,
	                         aggcontext);
	p->a = state;

	PG_RETURN_POINTER(p);
}



Datum pgis_accum_finalfn(pgis_abs *p, MemoryContext mctx, FunctionCallInfo fcinfo);

/**
** The final function rescues the built array from the side memory context
** using the PostgreSQL built-in function makeMdArrayResult
*/
Datum
pgis_accum_finalfn(pgis_abs *p, MemoryContext mctx, FunctionCallInfo fcinfo)
{
	int dims[1];
	int lbs[1];
	ArrayBuildState *state;
	Datum result;

	state = p->a;
	dims[0] = state->nelems;
	lbs[0] = 1;
	result = makeMdArrayResult(state, 1, dims, lbs, mctx, false);
	return result;
}

/**
** The "accum" final function just returns the geometry[]
*/
PG_FUNCTION_INFO_V1(pgis_geometry_accum_finalfn);
Datum
pgis_geometry_accum_finalfn(PG_FUNCTION_ARGS)
{
	pgis_abs *p;
	Datum result = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();   /* returns null iff no input values */

	p = (pgis_abs*) PG_GETARG_POINTER(0);

	result = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);

	PG_RETURN_DATUM(result);

}

/**
* The "union" final function passes the geometry[] to a union
* conversion before returning the result.
*/
PG_FUNCTION_INFO_V1(pgis_geometry_union_finalfn);
Datum
pgis_geometry_union_finalfn(PG_FUNCTION_ARGS)
{
	pgis_abs *p;
	Datum result = 0;
	Datum geometry_array = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();   /* returns null iff no input values */

	p = (pgis_abs*) PG_GETARG_POINTER(0);

	geometry_array = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);
	result = PGISDirectFunctionCall1( pgis_union_geometry_array, geometry_array );
	if (!result)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/**
* The "collect" final function passes the geometry[] to a geometrycollection
* conversion before returning the result.
*/
PG_FUNCTION_INFO_V1(pgis_geometry_collect_finalfn);
Datum
pgis_geometry_collect_finalfn(PG_FUNCTION_ARGS)
{
	pgis_abs *p;
	Datum result = 0;
	Datum geometry_array = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();   /* returns null iff no input values */

	p = (pgis_abs*) PG_GETARG_POINTER(0);

	geometry_array = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);
	result = PGISDirectFunctionCall1( LWGEOM_collect_garray, geometry_array );
	if (!result)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}


/**
* The "polygonize" final function passes the geometry[] to a polygonization
* before returning the result.
*/
PG_FUNCTION_INFO_V1(pgis_geometry_polygonize_finalfn);
Datum
pgis_geometry_polygonize_finalfn(PG_FUNCTION_ARGS)
{
	pgis_abs *p;
	Datum result = 0;
	Datum geometry_array = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();   /* returns null iff no input values */

	p = (pgis_abs*) PG_GETARG_POINTER(0);

	geometry_array = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);
	result = DirectFunctionCall1( polygonize_garray, geometry_array );

	PG_RETURN_DATUM(result);
}

/**
* The "makeline" final function passes the geometry[] to a line builder
* before returning the result.
*/
PG_FUNCTION_INFO_V1(pgis_geometry_makeline_finalfn);
Datum
pgis_geometry_makeline_finalfn(PG_FUNCTION_ARGS)
{
	pgis_abs *p;
	Datum result = 0;
	Datum geometry_array = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();   /* returns null iff no input values */

	p = (pgis_abs*) PG_GETARG_POINTER(0);

	geometry_array = pgis_accum_finalfn(p, CurrentMemoryContext, fcinfo);
	result = PGISDirectFunctionCall1( LWGEOM_makeline_garray, geometry_array );
	if (!result)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/**
* A modified version of PostgreSQL's DirectFunctionCall1 which allows NULL results; this
* is required for aggregates that return NULL.
*/
Datum
PGISDirectFunctionCall1(PGFunction func, Datum arg1)
{
	FunctionCallInfoData fcinfo;
	Datum           result;

#if POSTGIS_PGSQL_VERSION > 90

	InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);
#else

	InitFunctionCallInfoData(fcinfo, NULL, 1, NULL, NULL);
#endif

	fcinfo.arg[0] = arg1;
	fcinfo.argnull[0] = false;

	result = (*func) (&fcinfo);

	/* Check for null result, returning a "NULL" Datum if indicated */
	if (fcinfo.isnull)
		return (Datum) 0;

	return result;
}
