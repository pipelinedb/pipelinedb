#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/array.h"
#include "utils/geo_decls.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "funcapi.h"

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h"
#endif

#include "liblwgeom.h"

/* ST_DumpPoints for postgis.
 * By Nathan Wagner, copyright disclaimed,
 * this entire file is in the public domain
 */

Datum LWGEOM_dumppoints(PG_FUNCTION_ARGS);

struct dumpnode {
	LWGEOM *geom;
	int idx; /* which member geom we're working on */
} ;

/* 32 is the max depth for st_dump, so it seems reasonable
 * to use the same here
 */
#define MAXDEPTH 32
struct dumpstate {
	LWGEOM	*root;
	int	stacklen; /* collections/geoms on stack */
	int	pathlen; /* polygon rings and such need extra path info */
	struct	dumpnode stack[MAXDEPTH];
	Datum	path[34]; /* two more than max depth, for ring and point */

	/* used to cache the type attributes for integer arrays */
	int16	typlen;
	bool	byval;
	char	align;

	int ring; /* ring of top polygon */
	int pt; /* point of top geom or current ring */
};

PG_FUNCTION_INFO_V1(LWGEOM_dumppoints);
Datum LWGEOM_dumppoints(PG_FUNCTION_ARGS) {
	FuncCallContext *funcctx;
	MemoryContext oldcontext, newcontext;

	GSERIALIZED *pglwgeom;
	LWCOLLECTION *lwcoll;
	LWGEOM *lwgeom;
	struct dumpstate *state;
	struct dumpnode *node;

	HeapTuple tuple;
	Datum pathpt[2]; /* used to construct the composite return value */
	bool isnull[2] = {0,0}; /* needed to say neither value is null */
	Datum result; /* the actual composite return value */

	if (SRF_IS_FIRSTCALL()) {
		funcctx = SRF_FIRSTCALL_INIT();

		newcontext = funcctx->multi_call_memory_ctx;
		oldcontext = MemoryContextSwitchTo(newcontext);

		/* get a local copy of what we're doing a dump points on */
		pglwgeom = PG_GETARG_GSERIALIZED_P_COPY(0);
		lwgeom = lwgeom_from_gserialized(pglwgeom);

		/* return early if nothing to do */
		if (!lwgeom || lwgeom_is_empty(lwgeom)) {
			MemoryContextSwitchTo(oldcontext);
			funcctx = SRF_PERCALL_SETUP();
			SRF_RETURN_DONE(funcctx);
		}

		/* Create function state */
		state = lwalloc(sizeof *state);
		state->root = lwgeom;
		state->stacklen = 0;
		state->pathlen = 0;
		state->pt = 0;
		state->ring = 0;

		funcctx->user_fctx = state;

		/*
		 * Push a struct dumpnode on the state stack
		 */

		state->stack[0].idx = 0;
		state->stack[0].geom = lwgeom;
		state->stacklen++;

		/*
		 * get tuple description for return type
		 */
		if (get_call_result_type(fcinfo, 0, &funcctx->tuple_desc) != TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("set-valued function called in context that cannot accept a set")));
		}

		BlessTupleDesc(funcctx->tuple_desc);

		/* get and cache data for constructing int4 arrays */
		get_typlenbyvalalign(INT4OID, &state->typlen, &state->byval, &state->align);

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	newcontext = funcctx->multi_call_memory_ctx;

	/* get state */
	state = funcctx->user_fctx;

	while (1) {
		node = &state->stack[state->stacklen-1];
		lwgeom = node->geom;

		/* need to return a point from this geometry */
		if (!lwgeom_is_collection(lwgeom)) {
			/* either return a point, or pop the stack */
			/* TODO use a union?  would save a tiny amount of stack space.
			 * probably not worth the bother
			 */
			LWLINE	*line;
			LWCIRCSTRING *circ;
			LWPOLY	*poly;
			LWTRIANGLE	*tri;
			LWPOINT *lwpoint = NULL;
			POINT4D	pt;

			/*
			 * net result of switch should be to set lwpoint to the
			 * next point to return, or leave at NULL if there
			 * are no more points in the geometry
			 */
			switch(lwgeom->type) {
				case TRIANGLETYPE:
					tri = lwgeom_as_lwtriangle(lwgeom);
					if (state->pt == 0) {
						state->path[state->pathlen++] = Int32GetDatum(state->ring+1);
					}
					if (state->pt <= 3) {
						getPoint4d_p(tri->points, state->pt, &pt);
						lwpoint = lwpoint_make(tri->srid,
								FLAGS_GET_Z(tri->points->flags),
								FLAGS_GET_M(tri->points->flags),
								&pt);
					}
					if (state->pt > 3) {
						state->pathlen--;
					}
					break;
				case POLYGONTYPE:
					poly = lwgeom_as_lwpoly(lwgeom);
					if (state->pt == poly->rings[state->ring]->npoints) {
						state->pt = 0;
						state->ring++;
						state->pathlen--;
					}
					if (state->pt == 0 && state->ring < poly->nrings) {
						/* handle new ring */
						state->path[state->pathlen] = Int32GetDatum(state->ring+1);
						state->pathlen++;
					}
				       	if (state->ring == poly->nrings) {
					} else {
					/* TODO should be able to directly get the point
					 * into the point array of a fixed lwpoint
					 */
					/* can't get the point directly from the ptarray because
					 * it might be aligned wrong, so at least one memcpy
					 * seems unavoidable
					 * It might be possible to pass it directly to gserialized
					 * depending how that works, it might effectively be gserialized
					 * though a brief look at the code indicates not
					 */
						getPoint4d_p(poly->rings[state->ring], state->pt, &pt);
						lwpoint = lwpoint_make(poly->srid,
								FLAGS_GET_Z(poly->rings[state->ring]->flags),
								FLAGS_GET_M(poly->rings[state->ring]->flags),
								&pt);
					}
					break;
				case POINTTYPE:
					if (state->pt == 0) lwpoint = lwgeom_as_lwpoint(lwgeom);
					break;
				case LINETYPE:
					line = lwgeom_as_lwline(lwgeom);
					if (line->points && state->pt <= line->points->npoints) {
						lwpoint = lwline_get_lwpoint((LWLINE*)lwgeom, state->pt);
					}
					break;
				case CIRCSTRINGTYPE:
					circ = lwgeom_as_lwcircstring(lwgeom);
					if (circ->points && state->pt <= circ->points->npoints) {
						lwpoint = lwcircstring_get_lwpoint((LWCIRCSTRING*)lwgeom, state->pt);
					}
					break;
				default:
					ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("Invalid Geometry type %d passed to ST_DumpPoints()", lwgeom->type)));
			}

			/*
			 * At this point, lwpoint is either NULL, in which case
			 * we need to pop the geometry stack and get the next
			 * geometry, if amy, or lwpoint is set and we construct
			 * a record type with the integer array of geometry
			 * indexes and the point number, and the actual point
			 * geometry itself
			 */

			if (!lwpoint) {
				/* no point, so pop the geom and look for more */
				if (--state->stacklen == 0) SRF_RETURN_DONE(funcctx);
				state->pathlen--;
				continue;
			} else {
				/* write address of current geom/pt */
				state->pt++;

				state->path[state->pathlen] = Int32GetDatum(state->pt);
				pathpt[0] = PointerGetDatum(construct_array(state->path, state->pathlen+1,
						INT4OID, state->typlen, state->byval, state->align));
				
				pathpt[1] = PointerGetDatum(gserialized_from_lwgeom((LWGEOM*)lwpoint,0,0));
				
				tuple = heap_form_tuple(funcctx->tuple_desc, pathpt, isnull);
				result = HeapTupleGetDatum(tuple);
				SRF_RETURN_NEXT(funcctx, result);
			}
		}

		lwcoll = (LWCOLLECTION*)node->geom;

		/* if a collection and we have more geoms */
		if (node->idx < lwcoll->ngeoms) {
			/* push the next geom on the path and the stack */
			lwgeom = lwcoll->geoms[node->idx++];
			state->path[state->pathlen++] = Int32GetDatum(node->idx);

			node = &state->stack[state->stacklen++];
			node->idx = 0;
			node->geom = lwgeom;

			state->pt = 0;
			state->ring = 0;

			/* loop back to beginning, which will then check whatever node we just pushed */
			continue;
		}

		/* no more geometries in the current collection */
		if (--state->stacklen == 0) SRF_RETURN_DONE(funcctx);
		state->pathlen--;
		state->stack[state->stacklen-1].idx++;
	}
}

/*
 * Geometry types of collection types for reference
 */

#if 0
        case MULTIPOINTTYPE:
        case MULTILINETYPE:
        case MULTIPOLYGONTYPE:
        case COLLECTIONTYPE:
        case CURVEPOLYTYPE:
        case COMPOUNDTYPE:
        case MULTICURVETYPE:
        case MULTISURFACETYPE:
        case POLYHEDRALSURFACETYPE:
        case TINTYPE:

#endif

