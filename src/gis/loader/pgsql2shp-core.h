/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2003 Refractions Research Inc.
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright 2009 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iconv.h>

#include "../postgis_config.h"
#include "libpq-fe.h"
#include "shapefil.h"
#include "shpcommon.h"
#include "getopt.h"

#define P2S_RCSID "$Id$"

/*
 * Error message handling
 */

#define SHPDUMPERMSGLEN		1024

#define SHPDUMPEROK		-1
#define SHPDUMPERERR		0
#define SHPDUMPERWARN		1


/*
 * Structure to hold the dumper configuration options 
 */

typedef struct shp_dumper_config
{
	/* Parameters used to connect to the database */
	SHPCONNECTIONCONFIG *conn;

	/* table to load into */
	char *table;

	/* schema to load into */
	char *schema;

	/* user-specified query, if supplied */
	char *usrquery;

	/* 0=use normal cursor, 1=use binary cursor */
	int binary;

	/* Name of the output shapefile */
	char *shp_file;

	/* TODO: rename? 0=switch not provided, 1=switch provided */
	int dswitchprovided;

	/* TODO: replace and combine with below 0=do not include gid column in shapefile, 1=include gid column in shapefile */ 
	int includegid;

	/* TODO: 0=escape column names, 1=do not escape column names */
	int unescapedattrs;

	/* Name of geometry/geography database column */
	char *geo_col_name;

	/* 0=do not keep fieldname case, 1=keep fieldname case */
	int keep_fieldname_case;

	/* Number of rows to fetch in a cursor batch */
	int fetchsize;

	/* Name of the column map file if specified */
	char *column_map_filename;
	
} SHPDUMPERCONFIG;


/*
 * Structure to holder the current dumper state 
 */

typedef struct shp_dumper_state
{
	/* Configuration for this state */
	SHPDUMPERCONFIG *config;

	/* Database connection being used */
	PGconn *conn;
	
	/* Version of PostGIS being used */
	int pgis_major_version;

	/* 0=dumper running on little endian, 1=dumper running on big endian */
	int big_endian;

	/* OID for geometries */
	int geom_oid;

	/* OID for geographies */
	int geog_oid;

	/* Schema of current working table */
	char *schema;

	/* Name of current working table */
	char *table;

	/* Name of geography/geometry column in current working table */
	char *geo_col_name;

	/* DBF fieldnames for all non-spatial fields */
	char **dbffieldnames;

	/* DBF field types for all non-spatial fields */
	int *dbffieldtypes;

	/* PostgreSQL column names for all non-spatial fields */
	char **pgfieldnames;

	/* PostgreSQL column lengths for all non-spatial fields */
	int *pgfieldlens;
	
	/* PostgreSQL column typmods for all non-spatial fields */
	int *pgfieldtypmods;
	
	/* Number of non-spatial fields in DBF output file */
	int fieldcount;

	/* Number of rows in the database table */
	int num_records;

	/* Name of the current shapefile */
	char *shp_file;

	/* Handle of the current DBF file */
	DBFHandle dbf;

	/* Handle of the current SHP file */
	SHPHandle shp;

	/* Indicate output geometry type: s=2d, z=3dz or 4d, m=3dm */
	char outtype;

	/* Shapefile/DBF geometry type */
	int outshptype;

	/* Number of rows in source database table */
	int rowcount;

	/* The main query being used for the table scan */
	char *main_scan_query;

	/* The current row number */
	int currow;

	/* The result set for the current FETCH batch */
	PGresult *fetchres;

	/* The row number within the current FETCH batch */
	int curresrow;

	/* The number of rows within the current FETCH batch */
	int currescount;

	/* The query being used to fetch records from the table */
	char *fetch_query;

	/* Last (error) message */
	char message[SHPDUMPERMSGLEN];

	/* Column map */
  colmap column_map;

} SHPDUMPERSTATE;


/* Externally accessible functions */
void set_dumper_config_defaults(SHPDUMPERCONFIG *config);
char *shapetypename(int num);

SHPDUMPERSTATE *ShpDumperCreate(SHPDUMPERCONFIG *config);
char *ShpDumperGetConnectionStringFromConn(SHPCONNECTIONCONFIG *config);
int ShpDumperConnectDatabase(SHPDUMPERSTATE *state);
int ShpDumperOpenTable(SHPDUMPERSTATE *state);
int ShpDumperGetRecordCount(SHPDUMPERSTATE *state);
int ShpLoaderGenerateShapeRow(SHPDUMPERSTATE *state);
int ShpDumperCloseTable(SHPDUMPERSTATE *state);
void ShpDumperDestroy(SHPDUMPERSTATE *state);
