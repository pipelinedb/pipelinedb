/*
 *
 * PostGIS Raster loader
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright 2001-2003 Refractions Research Inc.
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright 2009 Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 * Copyright (C) 2011 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */

/* For internationalization */
#ifdef ENABLE_NLS
#include <libintl.h>
#include <locale.h>
#define _(String) gettext(String)
#define PACKAGE "raster2pgsql"
#else
#define _(String) String
#endif

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "librtcore.h"

#include "../../postgis_config.h"
#include "../raster_config.h"

#define CSEQUAL(a,b) (strcmp(a,b)==0)

/*
	max length of of "name" data type in PostgreSQL as
	defined in pg_config_manual.h as macro NAMEDATALEN
	default is 64 bytes (63 usable characters plus NULL)
*/
#define MAXNAMELEN 63

/* minimum and maximum overview factor */
#define MINOVFACTOR 2
#define MAXOVFACTOR 1000

/*
	maximum tile size
	based upon maximum field size as defined for PostgreSQl
	http://www.postgresql.org/about/

	1 GB = 1024 x 1024 x 1024
*/
#define MAXTILESIZE 1073741824

#define RCSID "$Id$"

typedef struct raster_loader_config {
	/* raster filename */
	int rt_file_count;
	char **rt_file;
	char **rt_filename;

	/* schema to load into */
	char *schema;

	/* table to load into */
	char *table;

	/* raster column name specified by user */
	char *raster_column;

	/* add column with raster filename, 1 = yes, 0 = no (default) */
	int file_column;
	char *file_column_name;

	/* overview factor */
	int overview_count;
	int *overview;
	char **overview_table;

	/* case-sensitive of identifiers, 1 = yes, 0 = no (default) */
	int quoteident;

	/* SRID of input raster */
	int srid;

	/* SRID of output raster (reprojection) */
	int out_srid;

	/* bands to extract */
	int *nband; /* 1-based */
	int nband_count;

	/* tile size */
	int tile_size[2];

	/* pad tiles */
	int pad_tile;

	/* register raster as of out-of-db, 1 = yes, 0 = no (default) */
	int outdb;

	/* type of operation, (d|a|c|p) */
	char opt;

	/* create index, 1 = yes, 0 = no (default) */
	int idx;

	/* maintenance statements, 1 = yes, 0 = no (default) */
	int maintenance;

	/* set constraints */
	int constraints;

	/* enable max extent constraint, 1 = yes (default), 0 = no */
	int max_extent;

	/* enable regular_blocking constraint, 1 = yes, 0 = no (default) */
	int regular_blocking;

	/* new table's tablespace */
	char *tablespace;

	/* new index's tablespace */
	char *idx_tablespace;

	/* flag indicating that user specified a nodata value */
	int hasnodata;
	/* nodata value for bands with no nodata value */
	double nodataval;

	/* skip NODATA value check for bands */
	int skip_nodataval_check;

	/* endianness of binary output, 0 = XDR, 1 = NDR (default) */
	int endian;

	/* version of output format */
	int version;

	/* use a transaction, 0 = no, 1 = yes (default) */
	int transaction;

	/* use COPY instead of INSERT */
	int copy_statements;

} RTLOADERCFG;

typedef struct rasterinfo_t {
	/* SRID of raster */
	int srid;

	/* srs of raster */
	char *srs;

	/* width, height */
	uint32_t dim[2];

	/* number of bands */
	int *nband; /* 1-based */
	int nband_count;

	/* array of pixeltypes */
	GDALDataType *gdalbandtype;
	rt_pixtype *bandtype;

	/* array of hasnodata flags */
	int *hasnodata;
	/* array of nodatavals */
	double *nodataval;

	/* geotransform matrix */
	double gt[6];

	/* tile size */
	int tile_size[2];

} RASTERINFO;

typedef struct stringbuffer_t {
	uint32_t length;
	char **line;
} STRINGBUFFER;
