/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2011      Sandro Santilli <strk@keybit.net>
 * Copyright (C) 2009-2011 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright (C) 2008      Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 * Copyright (C) 2004-2007 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#ifndef _LWGEOM_PG_H
#define _LWGEOM_PG_H 1

#include "postgres.h"
#include "utils/geo_decls.h"
#include "fmgr.h"

#include "liblwgeom.h"
#include "pgsql_compat.h"

/* Install PosgreSQL handlers for liblwgeom use */
void pg_install_lwgeom_handlers(void);

/* Argument handling macros */
#define PG_GETARG_GSERIALIZED_P(varno) ((GSERIALIZED *)PG_DETOAST_DATUM(PG_GETARG_DATUM(varno)))
#define PG_GETARG_GSERIALIZED_P_COPY(varno) ((GSERIALIZED *)PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(varno)))
#define PG_GETARG_GSERIALIZED_P_SLICE(varno, start, size) ((GSERIALIZED *)PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(varno), start, size))

/* Debugging macros */
#if POSTGIS_DEBUG_LEVEL > 0

/* Display a simple message at NOTICE level */
#define POSTGIS_DEBUG(level, msg) \
        do { \
                if (POSTGIS_DEBUG_LEVEL >= level) \
                        ereport(NOTICE, (errmsg_internal("[%s:%s:%d] " msg, __FILE__, __func__, __LINE__))); \
        } while (0);

/* Display a formatted message at NOTICE level (like printf, with variadic arguments) */
#define POSTGIS_DEBUGF(level, msg, ...) \
        do { \
                if (POSTGIS_DEBUG_LEVEL >= level) \
                        ereport(NOTICE, (errmsg_internal("[%s:%s:%d] " msg, __FILE__, __func__, __LINE__, __VA_ARGS__))); \
        } while (0);

#else

/* Empty prototype that can be optimised away by the compiler for non-debug builds */
#define POSTGIS_DEBUG(level, msg) \
        ((void) 0)

/* Empty prototype that can be optimised away by the compiler for non-debug builds */
#define POSTGIS_DEBUGF(level, msg, ...) \
        ((void) 0)

#endif


/*
 * Standard macro for reporting parser errors to PostgreSQL
 */

extern void pg_parser_errhint(LWGEOM_PARSER_RESULT *lwg_parser_result);
extern void pg_unparser_errhint(LWGEOM_UNPARSER_RESULT *lwg_unparser_result);

#define PG_PARSER_ERROR(lwg_parser_result) \
        do { \
                pg_parser_errhint(&lwg_parser_result); \
        } while(0);

/*
 * Standard macro for reporting unparser errors to PostgreSQL
 */
#define PG_UNPARSER_ERROR(lwg_unparser_result) \
        do { \
                pg_unparser_errhint(&lwg_unparser_result); \
        } while(0);

/* TODO: only cancel the interrupt if inside an outer call ? */
#define LWGEOM_INIT() { \
  lwgeom_cancel_interrupt(); \
}


/*
** GSERIALIED prototypes used outside the index functions
*/

/**
* Remove the embedded bounding box 
*/
GSERIALIZED* gserialized_drop_gidx(GSERIALIZED *g);

/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/
GSERIALIZED *geometry_serialize(LWGEOM *lwgeom);

/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/
GSERIALIZED* geography_serialize(LWGEOM *lwgeom);

/**
* Pull out a gbox bounding box as fast as possible. 
* Tries to read cached box from front of serialized vardata.
* If no cached box, calculates box from scratch.
* Fails on empty.
*/
int gserialized_datum_get_gbox_p(Datum gsdatum, GBOX *gbox);

/**
* Convert cstrings (null-terminated byte array) to textp pointers 
* (PgSQL varlena structure with VARSIZE header).
*/
text* cstring2text(const char *cstring);

/**
* Convert textp (PgSQL varlena structure with VARSIZE header) to 
* cstrings (null-terminated byte array). 
*/
char* text2cstring(const text *textptr);

/* 
 * For PostgreSQL >= 8.5 redefine the STATRELATT macro to its
 * new value of STATRELATTINH 
 */
#if POSTGIS_PGSQL_VERSION >= 85
	#define STATRELATT STATRELATTINH
#endif

/* PG-exposed */
Datum BOX2D_same(PG_FUNCTION_ARGS);
Datum BOX2D_overlap(PG_FUNCTION_ARGS);
Datum BOX2D_overleft(PG_FUNCTION_ARGS);
Datum BOX2D_left(PG_FUNCTION_ARGS);
Datum BOX2D_right(PG_FUNCTION_ARGS);
Datum BOX2D_overright(PG_FUNCTION_ARGS);
Datum BOX2D_overbelow(PG_FUNCTION_ARGS);
Datum BOX2D_below(PG_FUNCTION_ARGS);
Datum BOX2D_above(PG_FUNCTION_ARGS);
Datum BOX2D_overabove(PG_FUNCTION_ARGS);
Datum BOX2D_contained(PG_FUNCTION_ARGS);
Datum BOX2D_contain(PG_FUNCTION_ARGS);
Datum BOX2D_intersects(PG_FUNCTION_ARGS);
Datum BOX2D_union(PG_FUNCTION_ARGS);

Datum LWGEOM_same(PG_FUNCTION_ARGS);
Datum BOX3D_construct(PG_FUNCTION_ARGS);

Datum LWGEOM_force_2d(PG_FUNCTION_ARGS);
Datum LWGEOM_force_3dm(PG_FUNCTION_ARGS);
Datum LWGEOM_force_3dz(PG_FUNCTION_ARGS);
Datum LWGEOM_force_4d(PG_FUNCTION_ARGS);
Datum LWGEOM_force_collection(PG_FUNCTION_ARGS);
Datum LWGEOM_force_multi(PG_FUNCTION_ARGS);
Datum LWGEOM_force_curve(PG_FUNCTION_ARGS);

Datum LWGEOMFromEWKB(PG_FUNCTION_ARGS);
Datum LWGEOMFromTWKB(PG_FUNCTION_ARGS);

Datum LWGEOM_getBBOX(PG_FUNCTION_ARGS);
Datum LWGEOM_addBBOX(PG_FUNCTION_ARGS);
Datum LWGEOM_dropBBOX(PG_FUNCTION_ARGS);

#endif /* !defined _LWGEOM_PG_H */
