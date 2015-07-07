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

/* Standard headers */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <locale.h> 
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iconv.h>

#include "shapefil.h"
#include "shpcommon.h"
#include "getopt.h"

#include "../liblwgeom/stringbuffer.h"

#define S2P_RCSID "$Id$"

/* Number of digits of precision in WKT produced. */
#define WKT_PRECISION 15

/* Loader policy constants */
#define POLICY_NULL_ABORT 	0x0
#define POLICY_NULL_INSERT 	0x1
#define POLICY_NULL_SKIP	0x2

/* Forced dimensionality constants */
#define FORCE_OUTPUT_DISABLE	0x0
#define FORCE_OUTPUT_2D		0x1
#define FORCE_OUTPUT_3DZ	0x2
#define FORCE_OUTPUT_3DM	0x3
#define FORCE_OUTPUT_4D		0x4

/* Error message handling */
#define SHPLOADERMSGLEN		1024

/* Error status codes */
#define SHPLOADEROK		-1
#define SHPLOADERERR		0
#define SHPLOADERWARN		1

/* Record status codes */
#define SHPLOADERRECDELETED	2
#define SHPLOADERRECISNULL	3

/*
 * Shapefile (dbf) field name are at most 10chars + 1 NULL.
 * Postgresql field names are at most 63 bytes + 1 NULL.
 */
#define MAXFIELDNAMELEN 64
#define MAXVALUELEN 1024

/*
 * Default geometry column name
 */
#define GEOMETRY_DEFAULT "geom"
#define GEOGRAPHY_DEFAULT "geog"

/*
 * Default character encoding
 */
#define ENCODING_DEFAULT "UTF-8"

/*
 * Structure to hold the loader configuration options 
 */
typedef struct shp_loader_config
{
	/* load mode: c = create, d = delete, a = append, p = prepare */
	char opt;

	/* table to load into */
	char *table;

	/* schema to load into */
	char *schema;

	/* geometry/geography column name specified by the user, may be null. */
	char *geo_col; 

	/* the shape file (without the .shp extension) */
	char *shp_file;

	/* 0 = SQL inserts, 1 = dump */
	int dump_format;

	/* 0 = MULTIPOLYGON/MULTILINESTRING, 1 = force to POLYGON/LINESTRING */
	int simple_geometries;
	
	/* 0 = geometry, 1 = geography */
	int geography;

	/* 0 = columnname, 1 = "columnName" */
	int quoteidentifiers;

	/* 0 = allow int8 fields, 1 = no int8 fields */
	int forceint4;

	/* 0 = no index, 1 = create index after load */
	int createindex;

	/* 0 = load DBF file only, 1 = load everything */
	int readshape;

	/* Override the output geometry type (a FORCE_OUTPUT_* constant) */
	int force_output;

	/* iconv encoding name */
	char *encoding;

	/* tablespace name for the table */
	char *tablespace;

	/* tablespace name for the indexes */
	char *idxtablespace;

	/* how to handle nulls */
	int null_policy;

	/* SRID specified */
	int sr_id;

	/* SRID of the shape file */
	int shp_sr_id;

	/* 0 = WKB (more precise), 1 = WKT (may have coordinate drift). */
	int use_wkt;

	/* whether to do a single transaction or run each statement on its own */
	int usetransaction;
	
	/* Name of the column map file if specified */
	char *column_map_filename;

} SHPLOADERCONFIG;


/*
 * Structure to holder the current loader state 
 */
typedef struct shp_loader_state
{
	/* Configuration for this state */
	SHPLOADERCONFIG *config;

	/* Shapefile handle */
	SHPHandle hSHPHandle;
	
	/* Shapefile type */
	int shpfiletype;

	/* Data file handle */
	DBFHandle hDBFHandle;

	/* Number of rows in the shapefile */
	int num_entities;

	/* Number of fields in the shapefile */
	int num_fields;

	/* Number of rows in the DBF file */
	int num_records;

	/* Pointer to an array of field names */
	char **field_names;

	/* Field type */
	DBFFieldType *types;

	/* Arrays of field widths and precisions */
	int *widths;
	int *precisions;

	/* Pointer to an array of PostgreSQL field types */
	char **pgfieldtypes;
	
	/* String containing colume name list in the form "(col1, col2, col3 ... , colN)" */
	char *col_names;

	/* String containing the PostGIS geometry type, e.g. POINT, POLYGON etc. */
	char *pgtype;

	/* Flag for whether the output geometry has Z coordinates or not. */
	int has_z;

	/* Flag for whether the output geometry has M coordinates or not. */
	int has_m;

	/* Number of dimensions to output */
	int pgdims;

	/* Last (error) message */
	char message[SHPLOADERMSGLEN];

	/* SRID of the shape file.  If not reprojecting, will be the same as to_srid. */
	int from_srid;

	/* SRID of the table.  If not reprojecting, will be the same as from_srid. */
	int to_srid;

	/* geometry/geography column name to use.  Will be set to the default if the config did
	   not specify a column name. */
	char *geo_col; 
	
	/* Column map */
  colmap column_map;

} SHPLOADERSTATE;


/* Externally accessible functions */
void strtolower(char *s);
void set_loader_config_defaults(SHPLOADERCONFIG *config);

SHPLOADERSTATE *ShpLoaderCreate(SHPLOADERCONFIG *config);
int ShpLoaderOpenShape(SHPLOADERSTATE *state);
int ShpLoaderGetSQLHeader(SHPLOADERSTATE *state, char **strheader);
int ShpLoaderGetSQLCopyStatement(SHPLOADERSTATE *state, char **strheader);
int ShpLoaderGetRecordCount(SHPLOADERSTATE *state);
int ShpLoaderGenerateSQLRowStatement(SHPLOADERSTATE *state, int item, char **strrecord);
int ShpLoaderGetSQLFooter(SHPLOADERSTATE *state, char **strfooter);
void ShpLoaderDestroy(SHPLOADERSTATE *state);
