/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2010 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * ST_RelateMatch
 *
 * DE9 Intersection Matrix pattern matching
 *
 * Developed by Sandro Santilli (strk@keybit.net) for Faunalia
 * (http://www.faunalia.it) with funding from Regione Toscana - Sistema
 * Informativo per la Gestione del Territorio e dell' Ambiente
 * [RT-SIGTA]". For the project: "Sviluppo strumenti software per il
 * trattamento di dati geografici basati su QuantumGIS e Postgis (CIG
 * 0494241492)"
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "../postgis_config.h"
#include "lwgeom_geos.h"
#include "lwgeom_pg.h"

#include <string.h>
#include <assert.h>

/* #define POSTGIS_DEBUG_LEVEL 4 */

Datum ST_RelateMatch(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ST_RelateMatch);
Datum ST_RelateMatch(PG_FUNCTION_ARGS)
{
#if POSTGIS_GEOS_VERSION < 33
	lwerror("The GEOS version this postgis binary "
	        "was compiled against (%d) doesn't support "
	        "'ST_RelateMatch' function (3.3.0+ required)",
	        POSTGIS_GEOS_VERSION);
	PG_RETURN_NULL();
#else /* POSTGIS_GEOS_VERSION >= 33 */

	char *mat, *pat;
	text *mat_text, *pat_text;
	int result;

	/* Read the arguments */
        mat_text = (PG_GETARG_TEXT_P(0));
        pat_text = (PG_GETARG_TEXT_P(1));

        /* Convert from text to cstring */
        mat = text2cstring(mat_text);
        pat = text2cstring(pat_text);

	initGEOS(lwnotice, lwgeom_geos_error);

	result = GEOSRelatePatternMatch(mat, pat);
	if (result == 2)
	{
		lwfree(mat); lwfree(pat);
		lwerror("GEOSRelatePatternMatch: %s", lwgeom_geos_errmsg);
		PG_RETURN_NULL();
	}

	lwfree(mat); lwfree(pat);
	PG_RETURN_BOOL(result);

#endif /* POSTGIS_GEOS_VERSION >= 33 */

}

