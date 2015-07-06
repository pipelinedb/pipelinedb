/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * http://postgis.net
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

#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <miscadmin.h>

#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#define PARANOIA_LEVEL 1

#define PGC_ERRMSG_MAXLEN 256

/**
* Utility to convert cstrings to textp pointers 
*/
text* 
cstring2text(const char *cstring)
{
	text *output;
	size_t sz;
	
	/* Guard against null input */
	if( !cstring )
		return NULL;
		
	sz = strlen(cstring);
	output = palloc(sz + VARHDRSZ);
	if ( ! output ) 
		return NULL;
	SET_VARSIZE(output, sz + VARHDRSZ);
	if ( sz )
		memcpy(VARDATA(output),cstring,sz);
	return output;
}

char*
text2cstring(const text *textptr)
{
	size_t size = VARSIZE(textptr) - VARHDRSZ;
	char *str = lwalloc(size+1);
	memcpy(str, VARDATA(textptr), size);
	str[size]='\0';
	return str;
}


/*
 * Error message parsing functions
 *
 * Produces nicely formatted messages for parser/unparser errors with optional HINT
 */

void
pg_parser_errhint(LWGEOM_PARSER_RESULT *lwg_parser_result)
{
	char *hintbuffer;

	/* Only display the parser position if the location is > 0; this provides a nicer output when the first token
	   within the input stream cannot be matched */
	if (lwg_parser_result->errlocation > 0)
	{
		/* Return a copy of the input string start truncated
		 * at the error location */
		hintbuffer = lwmessage_truncate(
			(char *)lwg_parser_result->wkinput, 0,
			lwg_parser_result->errlocation - 1, 40, 0);

		ereport(ERROR,
		        (errmsg("%s", lwg_parser_result->message),
		         errhint("\"%s\" <-- parse error at position %d within geometry", hintbuffer, lwg_parser_result->errlocation))
		       );
	}
	else
	{
		ereport(ERROR,
		        (errmsg("%s", lwg_parser_result->message),
		         errhint("You must specify a valid OGC WKT geometry type such as POINT, LINESTRING or POLYGON"))
		       );
	}
}

void
pg_unparser_errhint(LWGEOM_UNPARSER_RESULT *lwg_unparser_result)
{
	/* For the unparser simply output the error message without any associated HINT */
	elog(ERROR, "%s", lwg_unparser_result->message);
}


static void *
pg_alloc(size_t size)
{
	void * result;

	CHECK_FOR_INTERRUPTS(); /* give interrupter a chance */

	POSTGIS_DEBUGF(5, "  pg_alloc(%d) called", (int)size);

	result = palloc(size);

	POSTGIS_DEBUGF(5, "  pg_alloc(%d) returning %p", (int)size, result);

	if ( ! result )
	{
		ereport(ERROR, (errmsg_internal("Out of virtual memory")));
		return NULL;
	}
	return result;
}

static void *
pg_realloc(void *mem, size_t size)
{
	void * result;

	CHECK_FOR_INTERRUPTS(); /* give interrupter a chance */

	POSTGIS_DEBUGF(5, "  pg_realloc(%p, %d) called", mem, (int)size);

	result = repalloc(mem, size);

	POSTGIS_DEBUGF(5, "  pg_realloc(%p, %d) returning %p", mem, (int)size, result);

	return result;
}

static void
pg_free(void *ptr)
{
	pfree(ptr);
}

static void
pg_error(const char *fmt, va_list ap)
{
	char errmsg[PGC_ERRMSG_MAXLEN+1];

	vsnprintf (errmsg, PGC_ERRMSG_MAXLEN, fmt, ap);

	errmsg[PGC_ERRMSG_MAXLEN]='\0';
	ereport(ERROR, (errmsg_internal("%s", errmsg)));
}

static void
pg_notice(const char *fmt, va_list ap)
{
	char errmsg[PGC_ERRMSG_MAXLEN+1];

	vsnprintf (errmsg, PGC_ERRMSG_MAXLEN, fmt, ap);

	errmsg[PGC_ERRMSG_MAXLEN]='\0';
	ereport(NOTICE, (errmsg_internal("%s", errmsg)));
}

void
pg_install_lwgeom_handlers(void)
{
	/* install PostgreSQL handlers */
	lwgeom_set_handlers(pg_alloc, pg_realloc, pg_free, pg_error, pg_notice);
}

/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/

/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/
GSERIALIZED* geography_serialize(LWGEOM *lwgeom)
{
	static int is_geodetic = 1;
	size_t ret_size = 0;
	GSERIALIZED *g = NULL;

	g = gserialized_from_lwgeom(lwgeom, is_geodetic, &ret_size);
	if ( ! g ) lwerror("Unable to serialize lwgeom.");
	SET_VARSIZE(g, ret_size);
	return g;
}


/**
* Utility method to call the serialization and then set the
* PgSQL varsize header appropriately with the serialized size.
*/
GSERIALIZED* geometry_serialize(LWGEOM *lwgeom)
{
	static int is_geodetic = 0;
	size_t ret_size = 0;
	GSERIALIZED *g = NULL;

	g = gserialized_from_lwgeom(lwgeom, is_geodetic, &ret_size);
	if ( ! g ) lwerror("Unable to serialize lwgeom.");
	SET_VARSIZE(g, ret_size);
	return g;
}
