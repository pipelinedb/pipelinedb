/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2014 Sandro Santilli <strk@keybit.net>
 * Copyright (C) 2010 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#ifndef SHPCOMMON_H
#define SHPCOMMON_H

/* For internationalization */
#ifdef ENABLE_NLS
#include <libintl.h>
#include <locale.h>
#define _(String) gettext(String)
#define PACKAGE "shp2pgsql"
#else
#define _(String) String
#endif

typedef struct shp_connection_state
{
	/* PgSQL username to log in with */
	char *username;

	/* PgSQL password to log in with */
	char *password;

	/* PgSQL database to connect to */
	char *database;

	/* PgSQL port to connect to */
	char *port;

	/* PgSQL server to connect to */
	char *host;

} SHPCONNECTIONCONFIG;

/* External shared functions */
char *escape_connection_string(char *str);

/* Column map between pgsql and dbf */
typedef struct colmap_t {
	
	/* Column map pgfieldnames */
	char **pgfieldnames;
	
	/* Column map dbffieldnames */
	char **dbffieldnames;
	
	/* Number of entries within column map */
	int size;

} colmap;

/**
 * Read the content of filename into a symbol map 
 *
 * The content of the file is lines of two names separated by
 * white space and no trailing or leading space:
 *
 *    COLUMNNAME DBFFIELD1
 *    AVERYLONGCOLUMNNAME DBFFIELD2
 *
 *    etc.
 *
 * It is the reponsibility of the caller to reclaim the allocated space
 * as follows:
 *
 * free(map->colmap_pgfieldnames[]) to free the column names
 * free(map->colmap_dbffieldnames[]) to free the dbf field names
 *
 * TODO: provide a clean_colmap()
 *
 * @param filename name of the mapping file
 *
 * @param map container of colmap where the malloc'd
 *            symbol map will be stored.
 *
 * @param errbuf buffer to write error messages to
 *
 * @param errbuflen length of buffer to write error messages to
 *
 * @return 1 on success, 0 on error (and errbuf would be filled)
 */
int colmap_read(const char *fname, colmap *map, char *ebuf, size_t ebuflen);

void colmap_init(colmap *map);

void colmap_clean(colmap *map);

const char *colmap_dbf_by_pg(colmap *map, const char *pgname);

const char *colmap_pg_by_dbf(colmap *map, const char *dbfname);

#endif
