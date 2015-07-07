/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2009-2010 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * ST_MakeValid
 *
 * Attempts to make an invalid geometries valid w/out losing
 * points.
 *
 * Polygons may become lines or points or a collection of
 * polygons lines and points (collapsed ring cases).
 *
 * Author: Sandro Santilli <strk@keybit.net>
 *
 * Work done for Faunalia (http://www.faunalia.it) with fundings
 * from Regione Toscana - Sistema Informativo per il Governo
 * del Territorio e dell'Ambiente (RT-SIGTA).
 *
 * Thanks to Dr. Horst Duester for previous work on a plpgsql version
 * of the cleanup logic [1]
 *
 * Thanks to Andrea Peri for recommandations on constraints.
 *
 * [1] http://www.sogis1.so.ch/sogis/dl/postgis/cleanGeometry.sql
 *
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "../postgis_config.h"
#include "lwgeom_geos.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

#include <string.h>
#include <assert.h>

/* #define POSTGIS_DEBUG_LEVEL 4 */

Datum ST_MakeValid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_MakeValid);
Datum ST_MakeValid(PG_FUNCTION_ARGS)
{
#if POSTGIS_GEOS_VERSION < 33
	elog(ERROR, "You need GEOS-3.3.0 or up for ST_MakeValid");
	PG_RETURN_NULL();
#else /* POSTGIS_GEOS_VERSION >= 33 */

	GSERIALIZED *in, *out;
	LWGEOM *lwgeom_in, *lwgeom_out;

	in = PG_GETARG_GSERIALIZED_P(0);
	lwgeom_in = lwgeom_from_gserialized(in);

	switch ( lwgeom_in->type )
	{
	case POINTTYPE:
	case MULTIPOINTTYPE:
	case LINETYPE:
	case POLYGONTYPE:
	case MULTILINETYPE:
	case MULTIPOLYGONTYPE:
	case COLLECTIONTYPE:
		break;

	default:
		lwerror("ST_MakeValid: unsupported geometry type %s",
		        lwtype_name(lwgeom_in->type));
		PG_RETURN_NULL();
		break;
	}

	lwgeom_out = lwgeom_make_valid(lwgeom_in);
	if ( ! lwgeom_out )
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	out = geometry_serialize(lwgeom_out);

	PG_RETURN_POINTER(out);
#endif /* POSTGIS_GEOS_VERSION >= 33 */
}

#if POSTGIS_GEOS_VERSION >= 33

/* Uses GEOS internally */
static LWGEOM* lwgeom_clean(LWGEOM* lwgeom_in);
static LWGEOM*
lwgeom_clean(LWGEOM* lwgeom_in)
{
	LWGEOM* lwgeom_out;

	lwgeom_out = lwgeom_make_valid(lwgeom_in);
	if ( ! lwgeom_out )
	{
		return NULL;
	}

	/* Check dimensionality is the same as input */
	if ( lwgeom_dimensionality(lwgeom_in) != lwgeom_dimensionality(lwgeom_out) )
	{
		lwnotice("lwgeom_clean: dimensional collapse (%d to %d)",
		         lwgeom_dimensionality(lwgeom_in), lwgeom_dimensionality(lwgeom_out));

		return NULL;
	}

	/* Check that the output is not a collection if the input wasn't */
	if ( lwgeom_out->type == COLLECTIONTYPE &&
	        lwgeom_in->type != COLLECTIONTYPE )
	{
		lwnotice("lwgeom_clean: mixed-type output (%s) "
		         "from single-type input (%s)",
		         lwtype_name(lwgeom_out->type),
		         lwtype_name(lwgeom_in->type));
		return NULL;
	}

	/* Force right-hand-rule (will only affect polygons) */
	/* gout := ST_ForceRHR(gout); */

	/* Remove repeated duplicated points ? */
	/* gout = ST_RemoveRepeatedPoints(gout); */

	return lwgeom_out;
}

#endif /* POSTGIS_GEOS_VERSION >= 33 */


Datum ST_CleanGeometry(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_CleanGeometry);
Datum ST_CleanGeometry(PG_FUNCTION_ARGS)
{
#if POSTGIS_GEOS_VERSION < 33
	elog(ERROR, "You need GEOS-3.3.0 or up for ST_CleanGeometry");
	PG_RETURN_NULL();
#else /* POSTGIS_GEOS_VERSION >= 33 */

	GSERIALIZED *in, *out;
	LWGEOM *lwgeom_in, *lwgeom_out;

	in = PG_GETARG_GSERIALIZED_P(0);
	lwgeom_in = lwgeom_from_gserialized(in);

	/* Short-circuit: empty geometry are the cleanest ! */
#if 0
	if ( lwgeom_is_empty(lwgeom_in) )
	{
		out = geometry_serialize(lwgeom_in);
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_POINTER(out);
	}
#endif

	lwgeom_out = lwgeom_clean(lwgeom_in);
	if ( ! lwgeom_out )
	{
		PG_FREE_IF_COPY(in, 0);
		PG_RETURN_NULL();
	}

	out = geometry_serialize(lwgeom_out);
	PG_RETURN_POINTER(out);

#endif /* POSTGIS_GEOS_VERSION >= 33 */
}

