/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2009 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/array.h"
#include "utils/geo_decls.h"
#include "funcapi.h"

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"


Datum LWGEOM_dump(PG_FUNCTION_ARGS);
Datum LWGEOM_dump_rings(PG_FUNCTION_ARGS);
Datum ST_Subdivide(PG_FUNCTION_ARGS);

typedef struct GEOMDUMPNODE_T
{
	int idx;
	LWGEOM *geom;
}
GEOMDUMPNODE;

#define MAXDEPTH 32
typedef struct GEOMDUMPSTATE
{
	int stacklen;
	GEOMDUMPNODE *stack[MAXDEPTH];
	LWGEOM *root;
}
GEOMDUMPSTATE;

#define PUSH(x,y) ((x)->stack[(x)->stacklen++]=(y))
#define LAST(x) ((x)->stack[(x)->stacklen-1])
#define POP(x) (--((x)->stacklen))


PG_FUNCTION_INFO_V1(LWGEOM_dump);
Datum LWGEOM_dump(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwgeom;
	LWCOLLECTION *lwcoll;
	LWGEOM *lwgeom;
	FuncCallContext *funcctx;
	GEOMDUMPSTATE *state;
	GEOMDUMPNODE *node;
	TupleDesc tupdesc;
	HeapTuple tuple;
	AttInMetadata *attinmeta;
	MemoryContext oldcontext, newcontext;
	Datum result;
	char address[256];
	char *ptr;
	uint32 i;
	char *values[2];

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		newcontext = funcctx->multi_call_memory_ctx;

		oldcontext = MemoryContextSwitchTo(newcontext);

		pglwgeom = PG_GETARG_GSERIALIZED_P_COPY(0);
		lwgeom = lwgeom_from_gserialized(pglwgeom);

		/* Create function state */
		state = lwalloc(sizeof(GEOMDUMPSTATE));
		state->root = lwgeom;
		state->stacklen=0;

		if ( lwgeom_is_collection(lwgeom) )
		{
			/*
			 * Push a GEOMDUMPNODE on the state stack
			 */
			node = lwalloc(sizeof(GEOMDUMPNODE));
			node->idx=0;
			node->geom = lwgeom;
			PUSH(state, node);
		}

		funcctx->user_fctx = state;

		/*
		 * Build a tuple description for an
		 * geometry_dump tuple
		 */
		tupdesc = RelationNameGetTupleDesc("geometry_dump");

		/*
		 * generate attribute metadata needed later to produce
		 * tuples from raw C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	newcontext = funcctx->multi_call_memory_ctx;

	/* get state */
	state = funcctx->user_fctx;

	/* Handled simple geometries */
	if ( ! state->root ) SRF_RETURN_DONE(funcctx);
	/* Return nothing for empties */
	if ( lwgeom_is_empty(state->root) ) SRF_RETURN_DONE(funcctx);
	if ( ! lwgeom_is_collection(state->root) )
	{
		values[0] = "{}";
		values[1] = lwgeom_to_hexwkb(state->root, WKB_EXTENDED, 0);
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
		result = HeapTupleGetDatum(tuple);

		state->root = NULL;
		SRF_RETURN_NEXT(funcctx, result);
	}

	while (1)
	{
		node = LAST(state);
		lwcoll = (LWCOLLECTION*)node->geom;

		if ( node->idx < lwcoll->ngeoms )
		{
			lwgeom = lwcoll->geoms[node->idx];
			if ( ! lwgeom_is_collection(lwgeom) )
			{
				/* write address of current geom */
				ptr=address;
				*ptr++='{';
				for (i=0; i<state->stacklen; i++)
				{
					if ( i ) ptr += sprintf(ptr, ",");
					ptr += sprintf(ptr, "%d", state->stack[i]->idx+1);
				}
				*ptr++='}';
				*ptr='\0';

				break;
			}

			/*
			 * It's a collection, increment index
			 * of current node, push a new one on the
			 * stack
			 */

			oldcontext = MemoryContextSwitchTo(newcontext);

			node = lwalloc(sizeof(GEOMDUMPNODE));
			node->idx=0;
			node->geom = lwgeom;
			PUSH(state, node);

			MemoryContextSwitchTo(oldcontext);

			continue;
		}

		if ( ! POP(state) ) SRF_RETURN_DONE(funcctx);
		LAST(state)->idx++;
	}

	lwgeom->srid = state->root->srid;

	values[0] = address;
	values[1] = lwgeom_to_hexwkb(lwgeom, WKB_EXTENDED, 0);
	tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
	result = TupleGetDatum(funcctx->slot, tuple);
	node->idx++;
	SRF_RETURN_NEXT(funcctx, result);
}

struct POLYDUMPSTATE
{
	int ringnum;
	LWPOLY *poly;
};

PG_FUNCTION_INFO_V1(LWGEOM_dump_rings);
Datum LWGEOM_dump_rings(PG_FUNCTION_ARGS)
{
	GSERIALIZED *pglwgeom;
	LWGEOM *lwgeom;
	FuncCallContext *funcctx;
	struct POLYDUMPSTATE *state;
	TupleDesc tupdesc;
	HeapTuple tuple;
	AttInMetadata *attinmeta;
	MemoryContext oldcontext, newcontext;
	Datum result;
	char address[256];
	char *values[2];

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		newcontext = funcctx->multi_call_memory_ctx;

		oldcontext = MemoryContextSwitchTo(newcontext);

		pglwgeom = PG_GETARG_GSERIALIZED_P_COPY(0);
		if ( gserialized_get_type(pglwgeom) != POLYGONTYPE )
		{
			elog(ERROR, "Input is not a polygon");
		}

		lwgeom = lwgeom_from_gserialized(pglwgeom);

		/* Create function state */
		state = lwalloc(sizeof(struct POLYDUMPSTATE));
		state->poly = lwgeom_as_lwpoly(lwgeom);
		assert (state->poly);
		state->ringnum=0;

		funcctx->user_fctx = state;

		/*
		 * Build a tuple description for an
		 * geometry_dump tuple
		 */
		tupdesc = RelationNameGetTupleDesc("geometry_dump");

		/*
		 * generate attribute metadata needed later to produce
		 * tuples from raw C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	newcontext = funcctx->multi_call_memory_ctx;

	/* get state */
	state = funcctx->user_fctx;

	/* Loop trough polygon rings */
	while (state->ringnum < state->poly->nrings )
	{
		LWPOLY* poly = state->poly;
		POINTARRAY *ring;
		LWGEOM* ringgeom;

		/* Switch to an appropriate memory context for POINTARRAY
		 * cloning and hexwkb allocation */
		oldcontext = MemoryContextSwitchTo(newcontext);

		/* We need a copy of input ring here */
		ring = ptarray_clone_deep(poly->rings[state->ringnum]);

		/* Construct another polygon with shell only */
		ringgeom = (LWGEOM*)lwpoly_construct(
		               poly->srid,
		               NULL, /* TODO: could use input bounding box here */
		               1, /* one ring */
		               &ring);

		/* Write path as ``{ <ringnum> }'' */
		sprintf(address, "{%d}", state->ringnum);

		values[0] = address;
		values[1] = lwgeom_to_hexwkb(ringgeom, WKB_EXTENDED, 0);

		MemoryContextSwitchTo(oldcontext);

		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);
		result = HeapTupleGetDatum(tuple);
		++state->ringnum;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);

}


struct FLATCOLLECTIONDUMPSTATE
{
	int geomnum;
	LWCOLLECTION *col;
};

/*
* Break an object up into smaller objects of no more than N vertices 
*/
PG_FUNCTION_INFO_V1(ST_Subdivide);
Datum ST_Subdivide(PG_FUNCTION_ARGS)
{
#if POSTGIS_GEOS_VERSION < 35

	elog(ERROR, "The GEOS version this PostGIS binary "
	        "was compiled against (%d) doesn't support "
	        "'%s' function (3.5.0+ required)",
	        POSTGIS_GEOS_VERSION, __func__);
	PG_RETURN_NULL();

#else /* POSTGIS_GEOS_VERSION >= 35 */

	typedef struct
	{
		int nextgeom;
		int numgeoms;
		LWCOLLECTION *col;
	} collection_fctx;

	FuncCallContext *funcctx;
	collection_fctx *fctx;
	MemoryContext oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		GSERIALIZED *gser;
		LWGEOM *geom;
		LWCOLLECTION *col;
		int maxvertices = 256;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		* switch to memory context appropriate for multiple function calls
		*/
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		* Get the geometry value 
		*/
		gser = PG_GETARG_GSERIALIZED_P(0);
		geom = lwgeom_from_gserialized(gser);
		
		/*
		* Get the max vertices value 
		*/
		maxvertices = PG_GETARG_INT32(1);
		
		/*
		* Compute the subdivision of the geometry
		*/
		col = lwgeom_subdivide(geom, maxvertices);
		
		if ( ! col ) 
			SRF_RETURN_DONE(funcctx);

		/* allocate memory for user context */
		fctx = (collection_fctx *) palloc(sizeof(collection_fctx));

		/* initialize state */
		fctx->nextgeom = 0;
		fctx->numgeoms = col->ngeoms;
		fctx->col = col;

		/* save user context, switch back to function context */
		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	if (fctx->nextgeom < fctx->numgeoms)
	{
		GSERIALIZED *gpart = geometry_serialize(fctx->col->geoms[fctx->nextgeom]);
		fctx->nextgeom++;
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(gpart));
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}
	
#endif /* POSTGIS_GEOS_VERSION >= 35 */
}

