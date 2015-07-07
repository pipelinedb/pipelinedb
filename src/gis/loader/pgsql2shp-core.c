/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://www.postgis.org
 *
 * Copyright (C) 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 *
 * PostGIS to Shapefile converter
 *
 * Original Author: Jeff Lounsbury <jeffloun@refractions.net>
 * Contributions by: Sandro Santilli <strk@keybit.bet>
 * Enhanced by: Mark Cave-Ayland <mark.cave-ayland@siriusit.co.uk>
 *
 **********************************************************************/

#include "../postgis_config.h"

#include "pgsql2shp-core.h"

/* Solaris9 does not provide stdint.h */
/* #include <stdint.h> */
#include <inttypes.h>

#ifdef HAVE_UNISTD_H /* for getpid() and getopt */
#include <unistd.h>
#endif

#ifdef __CYGWIN__
#include <sys/param.h>
#endif

#include "../liblwgeom/liblwgeom.h" /* for LWGEOM struct and funx */
#include "../liblwgeom/lwgeom_log.h" /* for LWDEBUG macros */

/* Maximum DBF field width (according to ARCGIS) */
#define MAX_DBF_FIELD_SIZE 254


/* Prototypes */
static int reverse_points(int num_points, double *x, double *y, double *z, double *m);
static int is_clockwise(int num_points,double *x,double *y,double *z);
static int is_bigendian(void);
static SHPObject *create_point(SHPDUMPERSTATE *state, LWPOINT *lwpoint);
static SHPObject *create_multipoint(SHPDUMPERSTATE *state, LWMPOINT *lwmultipoint);
static SHPObject *create_polygon(SHPDUMPERSTATE *state, LWPOLY *lwpolygon);
static SHPObject *create_multipolygon(SHPDUMPERSTATE *state, LWMPOLY *lwmultipolygon);
static SHPObject *create_linestring(SHPDUMPERSTATE *state, LWLINE *lwlinestring);
static SHPObject *create_multilinestring(SHPDUMPERSTATE *state, LWMLINE *lwmultilinestring);
static char *nullDBFValue(char fieldType);
static int getMaxFieldSize(PGconn *conn, char *schema, char *table, char *fname);
static int getTableInfo(SHPDUMPERSTATE *state);
static int projFileCreate(SHPDUMPERSTATE *state);

/**
 * @brief Make appropriate formatting of a DBF value based on type.
 * Might return untouched input or pointer to static private
 * buffer: use return value right away.
 */
static char * goodDBFValue(char *in, char fieldType);

/** @brief Binary to hexewkb conversion function */
char *convert_bytes_to_hex(uint8_t *ewkb, size_t size);


static SHPObject *
create_point(SHPDUMPERSTATE *state, LWPOINT *lwpoint)
{
	SHPObject *obj;
	POINT4D p4d;

	double *xpts, *ypts, *zpts, *mpts;

	/* Allocate storage for points */
	xpts = malloc(sizeof(double));
	ypts = malloc(sizeof(double));
	zpts = malloc(sizeof(double));
	mpts = malloc(sizeof(double));

	/* Grab the point: note getPoint4d will correctly handle
	the case where the POINTs don't contain Z or M coordinates */
	p4d = getPoint4d(lwpoint->point, 0);

	xpts[0] = p4d.x;
	ypts[0] = p4d.y;
	zpts[0] = p4d.z;
	mpts[0] = p4d.m;

	LWDEBUGF(4, "Point: %g %g %g %g", xpts[0], ypts[0], zpts[0], mpts[0]);

	obj = SHPCreateObject(state->outshptype, -1, 0, NULL, NULL, 1, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);

	return obj;
}

static SHPObject *
create_multipoint(SHPDUMPERSTATE *state, LWMPOINT *lwmultipoint)
{
	SHPObject *obj;
	POINT4D p4d;
	int i;

	double *xpts, *ypts, *zpts, *mpts;

	/* Allocate storage for points */
	xpts = malloc(sizeof(double) * lwmultipoint->ngeoms);
	ypts = malloc(sizeof(double) * lwmultipoint->ngeoms);
	zpts = malloc(sizeof(double) * lwmultipoint->ngeoms);
	mpts = malloc(sizeof(double) * lwmultipoint->ngeoms);

	/* Grab the points: note getPoint4d will correctly handle
	the case where the POINTs don't contain Z or M coordinates */
	for (i = 0; i < lwmultipoint->ngeoms; i++)
	{
		p4d = getPoint4d(lwmultipoint->geoms[i]->point, 0);

		xpts[i] = p4d.x;
		ypts[i] = p4d.y;
		zpts[i] = p4d.z;
		mpts[i] = p4d.m;

		LWDEBUGF(4, "MultiPoint %d - Point: %g %g %g %g", i, xpts[i], ypts[i], zpts[i], mpts[i]);
	}

	obj = SHPCreateObject(state->outshptype, -1, 0, NULL, NULL, lwmultipoint->ngeoms, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);

	return obj;
}

static SHPObject *
create_polygon(SHPDUMPERSTATE *state, LWPOLY *lwpolygon)
{
	SHPObject *obj;
	POINT4D p4d;
	int i, j;

	double *xpts, *ypts, *zpts, *mpts;

	int *shpparts, shppointtotal = 0, shppoint = 0;

	/* Allocate storage for ring pointers */
	shpparts = malloc(sizeof(int) * lwpolygon->nrings);

	/* First count through all the points in each ring so we now how much memory is required */
	for (i = 0; i < lwpolygon->nrings; i++)
		shppointtotal += lwpolygon->rings[i]->npoints;

	/* Allocate storage for points */
	xpts = malloc(sizeof(double) * shppointtotal);
	ypts = malloc(sizeof(double) * shppointtotal);
	zpts = malloc(sizeof(double) * shppointtotal);
	mpts = malloc(sizeof(double) * shppointtotal);

	LWDEBUGF(4, "Total number of points: %d", shppointtotal);

	/* Iterate through each ring setting up shpparts to point to the beginning of each ring */
	for (i = 0; i < lwpolygon->nrings; i++)
	{
		/* For each ring, store the integer coordinate offset for the start of each ring */
		shpparts[i] = shppoint;

		for (j = 0; j < lwpolygon->rings[i]->npoints; j++)
		{
			p4d = getPoint4d(lwpolygon->rings[i], j);

			xpts[shppoint] = p4d.x;
			ypts[shppoint] = p4d.y;
			zpts[shppoint] = p4d.z;
			mpts[shppoint] = p4d.m;

			LWDEBUGF(4, "Polygon Ring %d - Point: %g %g %g %g", i, xpts[shppoint], ypts[shppoint], zpts[shppoint], mpts[shppoint]);

			/* Increment the point counter */
			shppoint++;
		}

		/*
		 * First ring should be clockwise,
		 * other rings should be counter-clockwise
		 */
		if ( i == 0 )
		{
			if ( ! is_clockwise(lwpolygon->rings[i]->npoints,
			                    &xpts[shpparts[i]], &ypts[shpparts[i]], NULL) )
			{
				LWDEBUG(4, "Outer ring not clockwise, forcing clockwise\n");

				reverse_points(lwpolygon->rings[i]->npoints,
				               &xpts[shpparts[i]], &ypts[shpparts[i]],
				               &zpts[shpparts[i]], &mpts[shpparts[i]]);
			}
		}
		else
		{
			if ( is_clockwise(lwpolygon->rings[i]->npoints,
			                  &xpts[shpparts[i]], &ypts[shpparts[i]], NULL) )
			{
				LWDEBUGF(4, "Inner ring %d not counter-clockwise, forcing counter-clockwise\n", i);

				reverse_points(lwpolygon->rings[i]->npoints,
				               &xpts[shpparts[i]], &ypts[shpparts[i]],
				               &zpts[shpparts[i]], &mpts[shpparts[i]]);
			}
		}
	}

	obj = SHPCreateObject(state->outshptype, -1, lwpolygon->nrings, shpparts, NULL, shppointtotal, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);
	free(shpparts);

	return obj;
}

static SHPObject *
create_multipolygon(SHPDUMPERSTATE *state, LWMPOLY *lwmultipolygon)
{
	SHPObject *obj;
	POINT4D p4d;
	int i, j, k;

	double *xpts, *ypts, *zpts, *mpts;

	int *shpparts, shppointtotal = 0, shppoint = 0, shpringtotal = 0, shpring = 0;

	/* NOTE: Multipolygons are stored in shapefiles as Polygon* shapes with multiple outer rings */

	/* First count through each ring of each polygon so we now know much memory is required */
	for (i = 0; i < lwmultipolygon->ngeoms; i++)
	{
		for (j = 0; j < lwmultipolygon->geoms[i]->nrings; j++)
		{
			shpringtotal++;
			shppointtotal += lwmultipolygon->geoms[i]->rings[j]->npoints;
		}
	}

	/* Allocate storage for ring pointers */
	shpparts = malloc(sizeof(int) * shpringtotal);

	/* Allocate storage for points */
	xpts = malloc(sizeof(double) * shppointtotal);
	ypts = malloc(sizeof(double) * shppointtotal);
	zpts = malloc(sizeof(double) * shppointtotal);
	mpts = malloc(sizeof(double) * shppointtotal);

	LWDEBUGF(4, "Total number of rings: %d   Total number of points: %d", shpringtotal, shppointtotal);

	/* Iterate through each ring of each polygon in turn */
	for (i = 0; i < lwmultipolygon->ngeoms; i++)
	{
		for (j = 0; j < lwmultipolygon->geoms[i]->nrings; j++)
		{
			/* For each ring, store the integer coordinate offset for the start of each ring */
			shpparts[shpring] = shppoint;

			LWDEBUGF(4, "Ring offset: %d", shpring);

			for (k = 0; k < lwmultipolygon->geoms[i]->rings[j]->npoints; k++)
			{
				p4d = getPoint4d(lwmultipolygon->geoms[i]->rings[j], k);

				xpts[shppoint] = p4d.x;
				ypts[shppoint] = p4d.y;
				zpts[shppoint] = p4d.z;
				mpts[shppoint] = p4d.m;

				LWDEBUGF(4, "MultiPolygon %d Polygon Ring %d - Point: %g %g %g %g", i, j, xpts[shppoint], ypts[shppoint], zpts[shppoint], mpts[shppoint]);

				/* Increment the point counter */
				shppoint++;
			}

			/*
			* First ring should be clockwise,
			* other rings should be counter-clockwise
			*/
			if ( j == 0 )
			{
				if ( ! is_clockwise(lwmultipolygon->geoms[i]->rings[j]->npoints,
				                    &xpts[shpparts[shpring]], &ypts[shpparts[shpring]], NULL) )
				{
					LWDEBUG(4, "Outer ring not clockwise, forcing clockwise\n");

					reverse_points(lwmultipolygon->geoms[i]->rings[j]->npoints,
					               &xpts[shpparts[shpring]], &ypts[shpparts[shpring]],
					               &zpts[shpparts[shpring]], &mpts[shpparts[shpring]]);
				}
			}
			else
			{
				if ( is_clockwise(lwmultipolygon->geoms[i]->rings[j]->npoints,
				                  &xpts[shpparts[shpring]], &ypts[shpparts[shpring]], NULL) )
				{
					LWDEBUGF(4, "Inner ring %d not counter-clockwise, forcing counter-clockwise\n", i);

					reverse_points(lwmultipolygon->geoms[i]->rings[j]->npoints,
					               &xpts[shpparts[shpring]], &ypts[shpparts[shpring]],
					               &zpts[shpparts[shpring]], &mpts[shpparts[shpring]]);
				}
			}

			/* Increment the ring counter */
			shpring++;
		}
	}

	obj = SHPCreateObject(state->outshptype, -1, shpringtotal, shpparts, NULL, shppointtotal, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);
	free(shpparts);

	return obj;
}

static SHPObject *
create_linestring(SHPDUMPERSTATE *state, LWLINE *lwlinestring)
{
	SHPObject *obj;
	POINT4D p4d;
	int i;

	double *xpts, *ypts, *zpts, *mpts;

	/* Allocate storage for points */
	xpts = malloc(sizeof(double) * lwlinestring->points->npoints);
	ypts = malloc(sizeof(double) * lwlinestring->points->npoints);
	zpts = malloc(sizeof(double) * lwlinestring->points->npoints);
	mpts = malloc(sizeof(double) * lwlinestring->points->npoints);

	/* Grab the points: note getPoint4d will correctly handle
	the case where the POINTs don't contain Z or M coordinates */
	for (i = 0; i < lwlinestring->points->npoints; i++)
	{
		p4d = getPoint4d(lwlinestring->points, i);

		xpts[i] = p4d.x;
		ypts[i] = p4d.y;
		zpts[i] = p4d.z;
		mpts[i] = p4d.m;

		LWDEBUGF(4, "Linestring - Point: %g %g %g %g", i, xpts[i], ypts[i], zpts[i], mpts[i]);
	}

	obj = SHPCreateObject(state->outshptype, -1, 0, NULL, NULL, lwlinestring->points->npoints, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);

	return obj;
}

static SHPObject *
create_multilinestring(SHPDUMPERSTATE *state, LWMLINE *lwmultilinestring)
{
	SHPObject *obj;
	POINT4D p4d;
	int i, j;

	double *xpts, *ypts, *zpts, *mpts;

	int *shpparts, shppointtotal = 0, shppoint = 0;

	/* Allocate storage for ring pointers */
	shpparts = malloc(sizeof(int) * lwmultilinestring->ngeoms);

	/* First count through all the points in each linestring so we now how much memory is required */
	for (i = 0; i < lwmultilinestring->ngeoms; i++)
		shppointtotal += lwmultilinestring->geoms[i]->points->npoints;

	LWDEBUGF(3, "Total number of points: %d", shppointtotal);

	/* Allocate storage for points */
	xpts = malloc(sizeof(double) * shppointtotal);
	ypts = malloc(sizeof(double) * shppointtotal);
	zpts = malloc(sizeof(double) * shppointtotal);
	mpts = malloc(sizeof(double) * shppointtotal);

	/* Iterate through each linestring setting up shpparts to point to the beginning of each line */
	for (i = 0; i < lwmultilinestring->ngeoms; i++)
	{
		/* For each linestring, store the integer coordinate offset for the start of each line */
		shpparts[i] = shppoint;

		for (j = 0; j < lwmultilinestring->geoms[i]->points->npoints; j++)
		{
			p4d = getPoint4d(lwmultilinestring->geoms[i]->points, j);

			xpts[shppoint] = p4d.x;
			ypts[shppoint] = p4d.y;
			zpts[shppoint] = p4d.z;
			mpts[shppoint] = p4d.m;

			LWDEBUGF(4, "Linestring %d - Point: %g %g %g %g", i, xpts[shppoint], ypts[shppoint], zpts[shppoint], mpts[shppoint]);

			/* Increment the point counter */
			shppoint++;
		}
	}

	obj = SHPCreateObject(state->outshptype, -1, lwmultilinestring->ngeoms, shpparts, NULL, shppoint, xpts, ypts, zpts, mpts);

	free(xpts);
	free(ypts);
	free(zpts);
	free(mpts);

	return obj;
}



/*Reverse the clockwise-ness of the point list... */
static int
reverse_points(int num_points, double *x, double *y, double *z, double *m)
{

	int i,j;
	double temp;
	j = num_points -1;
	for (i=0; i <num_points; i++)
	{
		if (j <= i)
		{
			break;
		}
		temp = x[j];
		x[j] = x[i];
		x[i] = temp;

		temp = y[j];
		y[j] = y[i];
		y[i] = temp;

		if ( z )
		{
			temp = z[j];
			z[j] = z[i];
			z[i] = temp;
		}

		if ( m )
		{
			temp = m[j];
			m[j] = m[i];
			m[i] = temp;
		}

		j--;
	}
	return 1;
}

/* Return 1 if the points are in clockwise order */
static int
is_clockwise(int num_points, double *x, double *y, double *z)
{
	int i;
	double x_change,y_change,area;
	double *x_new, *y_new; /* the points, translated to the origin
							* for safer accuracy */

	x_new = (double *)malloc(sizeof(double) * num_points);
	y_new = (double *)malloc(sizeof(double) * num_points);
	area=0.0;
	x_change = x[0];
	y_change = y[0];

	for (i=0; i < num_points ; i++)
	{
		x_new[i] = x[i] - x_change;
		y_new[i] = y[i] - y_change;
	}

	for (i=0; i < num_points - 1; i++)
	{
		/* calculate the area	 */
		area += (x[i] * y[i+1]) - (y[i] * x[i+1]);
	}
	if (area > 0 )
	{
		free(x_new);
		free(y_new);
		return 0; /*counter-clockwise */
	}
	else
	{
		free(x_new);
		free(y_new);
		return 1; /*clockwise */
	}
}


/*
 * Return the maximum octet_length from given table.
 * Return -1 on error.
 */
static int
getMaxFieldSize(PGconn *conn, char *schema, char *table, char *fname)
{
	int size;
	char *query;
	PGresult *res;

	/*( this is ugly: don't forget counting the length  */
	/* when changing the fixed query strings ) */

	if ( schema )
	{
		query = (char *)malloc(strlen(fname)+strlen(table)+
		                       strlen(schema)+46);
		sprintf(query,
		        "select max(octet_length(\"%s\"::text)) from \"%s\".\"%s\"",
		        fname, schema, table);
	}
	else
	{
		query = (char *)malloc(strlen(fname)+strlen(table)+46);
		sprintf(query,
		        "select max(octet_length(\"%s\"::text)) from \"%s\"",
		        fname, table);
	}

	LWDEBUGF(4, "maxFieldLenQuery: %s\n", query);

	res = PQexec(conn, query);
	free(query);
	if ( ! res || PQresultStatus(res) != PGRES_TUPLES_OK )
	{
		printf( _("Querying for maximum field length: %s"),
		        PQerrorMessage(conn));
		return -1;
	}

	if (PQntuples(res) <= 0 )
	{
		PQclear(res);
		return -1;
	}
	size = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	return size;
}

static int
is_bigendian(void)
{
	int test = 1;

	if ( (((char *)(&test))[0]) == 1)
	{
		return 0; /*NDR (little_endian) */
	}
	else
	{
		return 1; /*XDR (big_endian) */
	}
}

char *
shapetypename(int num)
{
	switch (num)
	{
	case SHPT_NULL:
		return "Null Shape";
	case SHPT_POINT:
		return "Point";
	case SHPT_ARC:
		return "PolyLine";
	case SHPT_POLYGON:
		return "Polygon";
	case SHPT_MULTIPOINT:
		return "MultiPoint";
	case SHPT_POINTZ:
		return "PointZ";
	case SHPT_ARCZ:
		return "PolyLineZ";
	case SHPT_POLYGONZ:
		return "PolygonZ";
	case SHPT_MULTIPOINTZ:
		return "MultiPointZ";
	case SHPT_POINTM:
		return "PointM";
	case SHPT_ARCM:
		return "PolyLineM";
	case SHPT_POLYGONM:
		return "PolygonM";
	case SHPT_MULTIPOINTM:
		return "MultiPointM";
	case SHPT_MULTIPATCH:
		return "MultiPatch";
	default:
		return "Unknown";
	}
}


/* This is taken and adapted from dbfopen.c of shapelib */
static char *
nullDBFValue(char fieldType)
{
	switch (fieldType)
	{
	case FTInteger:
	case FTDouble:
		/* NULL numeric fields have value "****************" */
		return "****************";

	case FTDate:
		/* NULL date fields have value "00000000" */
		return "        ";

	case FTLogical:
		/* NULL boolean fields have value "?" */
		return "?";

	default:
		/* empty string fields are considered NULL */
		return "";
	}
}

/**
 * @brief Make appropriate formatting of a DBF value based on type.
 * 		Might return untouched input or pointer to static private
 * 		buffer: use return value right away.
 */
static char *
goodDBFValue(char *in, char fieldType)
{
	/*
	 * We only work on FTLogical and FTDate.
	 * FTLogical is 1 byte, FTDate is 8 byte (YYYYMMDD)
	 * We allocate space for 9 bytes to take
	 * terminating null into account
	 */
	static char buf[9];

	switch (fieldType)
	{
	case FTLogical:
		buf[0] = toupper(in[0]);
		buf[1]='\0';
		return buf;
	case FTDate:
		buf[0]=in[0]; /* Y */
		buf[1]=in[1]; /* Y */
		buf[2]=in[2]; /* Y */
		buf[3]=in[3]; /* Y */
		buf[4]=in[5]; /* M */
		buf[5]=in[6]; /* M */
		buf[6]=in[8]; /* D */
		buf[7]=in[9]; /* D */
		buf[8]='\0';
		return buf;
	default:
		return in;
	}
}

char *convert_bytes_to_hex(uint8_t *ewkb, size_t size)
{
	int i;
	char *hexewkb;

	/* Convert the byte stream to a hex string using liblwgeom's deparse_hex function */
	hexewkb = malloc(size * 2 + 1);
	for (i=0; i<size; ++i) deparse_hex(ewkb[i], &hexewkb[i * 2]);
	hexewkb[size * 2] = '\0';

	return hexewkb;
}

/**
 * @brief Creates ESRI .prj file for this shp output
 * 		It looks in the spatial_ref_sys table and outputs the srtext field for this data
 * 		If data is a table will use geometry_columns, if a query or view will read SRID from query output.
 *	@warning Will give warning and not output a .prj file if SRID is -1, Unknown, mixed SRIDS or not found in spatial_ref_sys.  The dbf and shp will still be output.
 */
static int
projFileCreate(SHPDUMPERSTATE *state)
{
	FILE	*fp;
	char	*pszFullname, *pszBasename;
	int	i;

	char *pszFilename = state->shp_file;
	char *schema = state->schema;
	char *table = state->table;
	char *geo_col_name = state->geo_col_name;

	char *srtext;
	char *query;
	char *esc_schema;
	char *esc_table;
	char *esc_geo_col_name;

	int error, result;
	PGresult *res;
	int size;

	/***********
	*** I'm multiplying by 2 instead of 3 because I am too lazy to figure out how many characters to add
	*** after escaping if any **/
	size = 1000;
	if ( schema )
	{
		size += 3 * strlen(schema);
	}
	size += 1000;
	esc_table = (char *) malloc(3 * strlen(table) + 1);
	esc_geo_col_name = (char *) malloc(3 * strlen(geo_col_name) + 1);
	PQescapeStringConn(state->conn, esc_table, table, strlen(table), &error);
	PQescapeStringConn(state->conn, esc_geo_col_name, geo_col_name, strlen(geo_col_name), &error);

	/** make our address space large enough to hold query with table/schema **/
	query = (char *) malloc(size);
	if ( ! query ) return 0; /* out of virtual memory */

	/**************************************************
	 * Get what kind of spatial ref is the selected geometry field
	 * We first check the geometry_columns table for a match and then if no match do a distinct against the table
	 * NOTE: COALESCE does a short-circuit check returning the faster query result and skipping the second if first returns something
	 *	Escaping quotes in the schema and table in query may not be necessary except to prevent malicious attacks
	 *	or should someone be crazy enough to have quotes or other weird character in their table, column or schema names
	 **************************************************/
	if ( schema )
	{
		esc_schema = (char *) malloc(2 * strlen(schema) + 1);
		PQescapeStringConn(state->conn, esc_schema, schema, strlen(schema), &error);
		sprintf(query, "SELECT COALESCE((SELECT sr.srtext "
		        " FROM  geometry_columns As gc INNER JOIN spatial_ref_sys sr ON sr.srid = gc.srid "
		        " WHERE gc.f_table_schema = '%s' AND gc.f_table_name = '%s' AND gc.f_geometry_column = '%s' LIMIT 1),  "
		        " (SELECT CASE WHEN COUNT(DISTINCT sr.srid) > 1 THEN 'm' ELSE MAX(sr.srtext) END As srtext "
		        " FROM \"%s\".\"%s\" As g INNER JOIN spatial_ref_sys sr ON sr.srid = ST_SRID((g.\"%s\")::geometry)) , ' ') As srtext ",
		        esc_schema, esc_table,esc_geo_col_name, schema, table, geo_col_name);
		free(esc_schema);
	}
	else
	{
		sprintf(query, "SELECT COALESCE((SELECT sr.srtext "
		        " FROM  geometry_columns As gc INNER JOIN spatial_ref_sys sr ON sr.srid = gc.srid "
		        " WHERE gc.f_table_name = '%s' AND gc.f_geometry_column = '%s' AND pg_table_is_visible((gc.f_table_schema || '.' || gc.f_table_name)::regclass) LIMIT 1),  "
		        " (SELECT CASE WHEN COUNT(DISTINCT sr.srid) > 1 THEN 'm' ELSE MAX(sr.srtext) END as srtext "
		        " FROM \"%s\" As g INNER JOIN spatial_ref_sys sr ON sr.srid = ST_SRID((g.\"%s\")::geometry)), ' ') As srtext ",
		        esc_table, esc_geo_col_name, table, geo_col_name);
	}

	LWDEBUGF(3,"%s\n",query);
	free(esc_table);
	free(esc_geo_col_name);

	res = PQexec(state->conn, query);

	if ( ! res || PQresultStatus(res) != PGRES_TUPLES_OK )
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("WARNING: Could not execute prj query: %s"), PQresultErrorMessage(res));
		PQclear(res);
		free(query);
		return SHPDUMPERWARN;
	}

	for (i=0; i < PQntuples(res); i++)
	{
		srtext = PQgetvalue(res, i, 0);
		if (strcmp(srtext,"m") == 0)
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("WARNING: Mixed set of spatial references. No prj file will be generated"));
			PQclear(res);
			free(query);
			return SHPDUMPERWARN;
		}
		else
		{
			if (srtext[0] == ' ')
			{
				snprintf(state->message, SHPDUMPERMSGLEN, _("WARNING: Cannot determine spatial reference (empty table or unknown spatial ref). No prj file will be generated."));
				PQclear(res);
				free(query);
				return SHPDUMPERWARN;
			}
			else
			{
				/* -------------------------------------------------------------------- */
				/*	Compute the base (layer) name.  If there is any extension	*/
				/*	on the passed in filename we will strip it off.			*/
				/* -------------------------------------------------------------------- */
				pszBasename = (char *) malloc(strlen(pszFilename)+5);
				strcpy( pszBasename, pszFilename );
				for ( i = strlen(pszBasename)-1;
				        i > 0 && pszBasename[i] != '.' && pszBasename[i] != '/'
				        && pszBasename[i] != '\\';
				        i-- ) {}

				if ( pszBasename[i] == '.' )
					pszBasename[i] = '\0';

				pszFullname = (char *) malloc(strlen(pszBasename) + 5);
				sprintf( pszFullname, "%s.prj", pszBasename );
				free( pszBasename );


				/* -------------------------------------------------------------------- */
				/*      Create the file.                                                */
				/* -------------------------------------------------------------------- */
				fp = fopen( pszFullname, "wb" );
				if ( fp == NULL )
				{
					return 0;
				}
				{
				    result = fputs (srtext,fp);
                    LWDEBUGF(3, "\n result %d proj SRText is %s .\n", result, srtext);
                    if (result == EOF)
                    {
                        fclose( fp );
                        free( pszFullname );
                        PQclear(res);
                        free(query);
                        return 0;
                    }
				}
				fclose( fp );
				free( pszFullname );
			}
		}
	}
	PQclear(res);
	free(query);
	return SHPDUMPEROK;
}


static int 
getTableInfo(SHPDUMPERSTATE *state)
{

	/* Get some more information from the table:
		- count = total number of geometries/geographies in the table

	   and if we have found a suitable geometry column:

		- max = maximum number of dimensions within the geometry/geography column
		- geometrytype = string representing the geometry/geography type, e.g. POINT

	   Since max/geometrytype already require a sequential scan of the table, we may as
	   well get the row count too.
	 */

	PGresult *res;
	char *query;
	int tmpint;


	if (state->geo_col_name)
	{
		/* Include geometry information */
		if (state->schema)
		{
			query = malloc(150 + 4 * strlen(state->geo_col_name) + strlen(state->schema) + strlen(state->table));
	
			sprintf(query, "SELECT count(\"%s\"), max(ST_zmflag(\"%s\"::geometry)), geometrytype(\"%s\"::geometry) FROM \"%s\".\"%s\" GROUP BY geometrytype(\"%s\"::geometry)",
			state->geo_col_name, state->geo_col_name, state->geo_col_name, state->schema, state->table, state->geo_col_name);
		}
		else
		{
			query = malloc(150 + 4 * strlen(state->geo_col_name) + strlen(state->table));
	
			sprintf(query, "SELECT count(\"%s\"), max(ST_zmflag(\"%s\"::geometry)), geometrytype(\"%s\"::geometry) FROM \"%s\" GROUP BY geometrytype(\"%s\"::geometry)",
			state->geo_col_name, state->geo_col_name, state->geo_col_name, state->table, state->geo_col_name);
		}
	}
	else
	{
		/* Otherwise... just a row count will do */
		if (state->schema)
		{
			query = malloc(40 + strlen(state->schema) + strlen(state->table));
			
			sprintf(query, "SELECT count(1) FROM \"%s\".\"%s\"", state->schema, state->table);
		}
		else
		{
			query = malloc(40 + strlen(state->table));

			sprintf(query, "SELECT count(1) FROM \"%s\"", state->table);
		}
	}

	LWDEBUGF(3, "Table metadata query: %s\n", query);

	res = PQexec(state->conn, query);
	free(query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("ERROR: Could not execute table metadata query: %s"), PQresultErrorMessage(res));
		PQclear(res);
		return SHPDUMPERERR;
	}

	/* Make sure we error if the table is empty */
	if (PQntuples(res) == 0)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("ERROR: Could not determine table metadata (empty table)"));
		PQclear(res);
		return SHPDUMPERERR;
	}

	/* If we have a geo* column, get the dimension, type and count information */
	if (state->geo_col_name)
	{
		/* If a table has a geometry column containing mixed types then
		   the metadata query will return multiple rows. We need to cycle
		   through all rows to determine if the type combinations are valid.

		   Note that if we find a combination of a MULTI and non-MULTI geometry
		   of the same type, we always choose MULTI to ensure that everything
		   gets output correctly. The create_* conversion functions are clever
		   enough to up-convert the non-MULTI geometry to a MULTI in this case. */

		int dummy, i;
		uint8_t type = 0;
		int typefound = 0, typemismatch = 0;

		state->rowcount = 0;

		for (i = 0; i < PQntuples(res); i++)
		{
			geometry_type_from_string(PQgetvalue(res, i, 2), &type, &dummy, &dummy);

			if (!type) continue; /* skip null geometries */

			/* We can always set typefound to that of the first column found */
			if (!typefound)
				typefound = type;

			switch (type)
			{
			case MULTIPOINTTYPE:
				if (typefound != MULTIPOINTTYPE && typefound != POINTTYPE)
					typemismatch = 1;
				else
					typefound = MULTIPOINTTYPE;
				break;

			case MULTILINETYPE:
				if (typefound != MULTILINETYPE && typefound != LINETYPE)
					typemismatch = 1;
				else
					typefound = MULTILINETYPE;
				break;					

			case MULTIPOLYGONTYPE:
				if (typefound != MULTIPOLYGONTYPE && typefound != POLYGONTYPE)
					typemismatch = 1;
				else
					typefound = MULTIPOLYGONTYPE;
				break;

			case POINTTYPE:
				if (typefound != POINTTYPE && typefound != MULTIPOINTTYPE)
					typemismatch = 1;
				else if (!lwtype_is_collection(type))
					typefound = POINTTYPE;
				break;

			case LINETYPE:
				if (typefound != LINETYPE && typefound != MULTILINETYPE)
					typemismatch = 1;
				else if (!lwtype_is_collection(type))
					typefound = LINETYPE;
				break;

			case POLYGONTYPE:
				if (typefound != POLYGONTYPE && typefound != MULTIPOLYGONTYPE)
					typemismatch = 1;
				else if (!lwtype_is_collection(type))
					typefound = POLYGONTYPE;
				break;
			}

			/* Update the rowcount for each type */
			state->rowcount += atoi(PQgetvalue(res, i, 0));

			/* Set up the dimension output type (note: regardless of how many rows
				 the table metadata query returns, this value will be the same. But
				 we'll choose to use the first value anyway) */
			tmpint = atoi(PQgetvalue(res, i, 1));
			switch (tmpint)
			{
			case 0:
				state->outtype = 's';
				break;
			case 1:
				state->outtype = 'm';
				break;
			default:
				state->outtype = 'z';
				break;
			}

		}

		/* Flag an error if the table contains incompatible geometry combinations */
		if (typemismatch)
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("ERROR: Incompatible mixed geometry types in table"));
			PQclear(res);
			return SHPDUMPERERR;
		}

		/* Set up the shapefile output type based upon the dimension information */
		switch (typefound)
		{
		case POINTTYPE:
			switch(state->outtype)
			{
			case 'z':
				state->outshptype = SHPT_POINTZ;
				break;
	
			case 'm':
				state->outshptype = SHPT_POINTM;
				break;
	
			default:
				state->outshptype = SHPT_POINT;
			}
			break;

		case MULTIPOINTTYPE:
			switch(state->outtype)
			{
			case 'z':
				state->outshptype = SHPT_MULTIPOINTZ;
				break;
	
			case 'm':
				state->outshptype = SHPT_MULTIPOINTM;
				break;
	
			default:
				state->outshptype = SHPT_MULTIPOINT;
			}
			break;

		case LINETYPE:
		case MULTILINETYPE:
			switch(state->outtype)
			{
			case 'z':
				state->outshptype = SHPT_ARCZ;
				break;
	
			case 'm':
				state->outshptype = SHPT_ARCM;
				break;
	
			default:
				state->outshptype = SHPT_ARC;
			}
			break;

		case POLYGONTYPE:
		case MULTIPOLYGONTYPE:
			switch(state->outtype)
			{
			case 'z':
				state->outshptype = SHPT_POLYGONZ;
				break;
	
			case 'm':
				state->outshptype = SHPT_POLYGONM;
				break;
	
			default:
				state->outshptype = SHPT_POLYGON;
			}
			break;
		}
	}
	else
	{
		/* Without a geo* column the total is simply the first (COUNT) column */
		state->rowcount = atoi(PQgetvalue(res, 0, 0));
	}

	/* Dispose of the result set */
	PQclear(res);

	return SHPDUMPEROK;
}


/* Default configuration settings */
void
set_dumper_config_defaults(SHPDUMPERCONFIG *config)
{
	config->conn = malloc(sizeof(SHPCONNECTIONCONFIG));
	config->conn->host = NULL;
	config->conn->port = NULL;
	config->conn->database = NULL;
	config->conn->username = NULL;
	config->conn->password = NULL;

	config->table = NULL;
	config->schema = NULL;
	config->usrquery = NULL;
	config->binary = 0;
	config->shp_file = NULL;
	config->dswitchprovided = 0;
	config->includegid = 0;
	config->unescapedattrs = 0;
	config->geo_col_name = NULL;
	config->keep_fieldname_case = 0;
	config->fetchsize = 100;
	config->column_map_filename = NULL;
}

/* Create a new shapefile state object */
SHPDUMPERSTATE *
ShpDumperCreate(SHPDUMPERCONFIG *config)
{
	SHPDUMPERSTATE *state;

	/* Create a new state object and assign the config to it */
	state = malloc(sizeof(SHPDUMPERSTATE));
	state->config = config;

	/* Set any state defaults */
	state->conn = NULL;
	state->outtype = 's';
	state->geom_oid = 0;
	state->geog_oid = 0;
	state->schema = NULL;
	state->table = NULL;
	state->geo_col_name = NULL;
	state->fetch_query = NULL;
	state->main_scan_query = NULL;
	state->dbffieldnames = NULL;
	state->dbffieldtypes = NULL;
	state->pgfieldnames = NULL;
	state->big_endian = is_bigendian();
	colmap_init(&state->column_map);
	
	return state;
}

/* Generate the database connection string used by a state */
char *
ShpDumperGetConnectionStringFromConn(SHPCONNECTIONCONFIG *conn)
{
	char *connstring;
	int connlen;
	
	connlen = 64 + 
		(conn->host ? strlen(conn->host) : 0) + (conn->port ? strlen(conn->port) : 0) +
		(conn->username ? strlen(conn->username) : 0) + (conn->password ? strlen(conn->password) : 0) +
		(conn->database ? strlen(conn->database) : 0);

	connstring = malloc(connlen);
	memset(connstring, 0, connlen);

	if (conn->host)
	{
		strcat(connstring, " host=");
		strcat(connstring, conn->host);
	}

	if (conn->port)
	{
		strcat(connstring, " port=");
		strcat(connstring, conn->port);
	}

	if (conn->username)
	{
		strcat(connstring, " user=");
		strcat(connstring, conn->username);
	}

	if (conn->password)
	{	
		strcat(connstring, " password='");
		strcat(connstring, conn->password);
		strcat(connstring, "'");
	}

	if (conn->database)
	{
		strcat(connstring, " dbname=");
		strcat(connstring, conn->database);
	}

	return connstring;
}

/* Connect to the database and identify the version of PostGIS (and any other
capabilities required) */
int
ShpDumperConnectDatabase(SHPDUMPERSTATE *state)
{
	PGresult *res;

	char *connstring, *tmpvalue;

	/* Generate the PostgreSQL connection string */
	connstring = ShpDumperGetConnectionStringFromConn(state->config->conn);

	/* Connect to the database */
	state->conn = PQconnectdb(connstring);
	if (PQstatus(state->conn) == CONNECTION_BAD)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, "%s", PQerrorMessage(state->conn));
		free(connstring);
		return SHPDUMPERERR;
	}

	/* Set datestyle to ISO */
	res = PQexec(state->conn, "SET DATESTYLE='ISO'");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, "%s", PQresultErrorMessage(res));
		PQclear(res);
		free(connstring);
		return SHPDUMPERERR;
	}

	PQclear(res);

	/* Retrieve PostGIS major version */
	res = PQexec(state->conn, "SELECT postgis_version()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, "%s", PQresultErrorMessage(res));
		PQclear(res);
		free(connstring);
		return SHPDUMPERERR;		
	}

	tmpvalue = PQgetvalue(res, 0, 0);
	state->pgis_major_version = atoi(tmpvalue);

	PQclear(res);

	/* Find the OID for the geometry type */
	res = PQexec(state->conn, "SELECT oid FROM pg_type WHERE typname = 'geometry'");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Error looking up geometry oid: %s"), PQresultErrorMessage(res));
		PQclear(res);
		free(connstring);
		return SHPDUMPERERR;		
	}

	if (PQntuples(res) > 0)
	{
		tmpvalue = PQgetvalue(res, 0, 0);
		state->geom_oid = atoi(tmpvalue);
	}
	else
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Geometry type unknown (have you enabled postgis?)"));
		PQclear(res);
		free(connstring);
		return SHPDUMPERERR;
	}

	PQclear(res);

	/* Find the OID for the geography type */
	res = PQexec(state->conn, "SELECT oid FROM pg_type WHERE typname = 'geography'");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Error looking up geography oid: %s"), PQresultErrorMessage(res));
		PQclear(res);
		free(connstring);
		return SHPDUMPERERR;		
	}

	if (PQntuples(res) > 0)
	{
		/* Old databases don't have a geography type, so don't fail if we don't find it */
		tmpvalue = PQgetvalue(res, 0, 0);
		state->geog_oid = atoi(tmpvalue);
	}

	PQclear(res);

	free(connstring);

	return SHPDUMPEROK;
}


/* Open the specified table in preparation for extracting rows */
int
ShpDumperOpenTable(SHPDUMPERSTATE *state)
{
	PGresult *res;

	char buf[256];
	char *query;
	int gidfound = 0, i, j, ret, status;


	/* Open the column map if one was specified */
	if (state->config->column_map_filename)
	{
		ret = colmap_read(state->config->column_map_filename,
		                  &state->column_map, state->message, SHPDUMPERMSGLEN);
		if (!ret) return SHPDUMPERERR;
	}
		
	/* If a user-defined query has been specified, create and point the state to our new table */
	if (state->config->usrquery)
	{
		state->table = malloc(20 + 20);		/* string + max long precision */
		sprintf(state->table, "__pgsql2shp%lu_tmp_table", (long)getpid());

		query = malloc(32 + strlen(state->table) + strlen(state->config->usrquery));

		sprintf(query, "CREATE TEMP TABLE \"%s\" AS %s", state->table, state->config->usrquery);
		res = PQexec(state->conn, query);
		free(query);

		/* Execute the code to create the table */
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("Error executing user query: %s"), PQresultErrorMessage(res));
			PQclear(res);
			return SHPDUMPERERR;
		}
	}
	else
	{
		/* Simply point the state to copies of the supplied schema and table */
		state->table = strdup(state->config->table);
		if (state->config->schema)
			state->schema = strdup(state->config->schema);
	}


	/* Get the list of columns and their types for the selected table */
	if (state->schema)
	{
		query = malloc(250 + strlen(state->schema) + strlen(state->table));

		sprintf(query, "SELECT a.attname, a.atttypid, "
		        "a.atttypmod, a.attlen FROM "
		        "pg_attribute a, pg_class c, pg_namespace n WHERE "
		        "n.nspname = '%s' AND a.attrelid = c.oid AND "
		        "n.oid = c.relnamespace AND "
		        "a.atttypid != 0 AND "
		        "a.attnum > 0 AND c.relname = '%s'", state->schema, state->table);
	}
	else
	{
		query = malloc(250 + strlen(state->table));

		sprintf(query, "SELECT a.attname, a.atttypid, "
		        "a.atttypmod, a.attlen FROM "
		        "pg_attribute a, pg_class c WHERE "
		        "a.attrelid = c.oid and a.attnum > 0 AND "
		        "a.atttypid != 0 AND "
		        "c.relname = '%s' AND "
		        "pg_catalog.pg_table_is_visible(c.oid)", state->table);
	}

	LWDEBUGF(3, "query is: %s\n", query);

	res = PQexec(state->conn, query);
	free(query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Error querying for attributes: %s"), PQresultErrorMessage(res));
		PQclear(res);
		return SHPDUMPERERR;
	}

	if (!PQntuples(res))
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Table %s does not exist"), state->table);
		PQclear(res);
		return SHPDUMPERERR;
	}

	/* If a shapefile name was specified, use it. Otherwise simply use the table name. */
	if (state->config->shp_file != NULL)
		state->shp_file = state->config->shp_file;
	else
		state->shp_file = state->table;

	/* Create the dbf file */
	state->dbf = DBFCreate(state->shp_file);
	if (!state->dbf)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Could not create dbf file %s"), state->shp_file);
		return SHPDUMPERERR;
	}

	/*
	 * Scan the result setting fields to be returned in mainscan
	 * query, filling the type_ary, and creating .dbf and .shp files.
	 */
	state->dbffieldnames = malloc(sizeof(char *) * PQntuples(res));
	state->dbffieldtypes = malloc(sizeof(int) * PQntuples(res));
	state->pgfieldnames = malloc(sizeof(char *) * PQntuples(res));
	state->pgfieldlens = malloc(sizeof(int) * PQntuples(res));
	state->pgfieldtypmods = malloc(sizeof(int) * PQntuples(res));
	state->fieldcount = 0;
	int tmpint = 1;

	for (i = 0; i < PQntuples(res); i++)
	{
		char *ptr;

		int pgfieldtype, pgtypmod, pgfieldlen;
		char *pgfieldname;

		int dbffieldtype, dbffieldsize, dbffielddecs;
		char *dbffieldname;

		pgfieldname = PQgetvalue(res, i, 0);
		pgfieldtype = atoi(PQgetvalue(res, i, 1));
		pgtypmod = atoi(PQgetvalue(res, i, 2));
		pgfieldlen = atoi(PQgetvalue(res, i, 3));
		dbffieldtype = -1;
		dbffieldsize = 0;
		dbffielddecs = 0;

		/*
		 * This is a geometry/geography column
		 */
		if (pgfieldtype == state->geom_oid || pgfieldtype == state->geog_oid)
		{
			/* If no geometry/geography column has been found yet... */
			if (!state->geo_col_name)
			{
				/* If either no geo* column name was provided (in which case this is
				   the first match) or we match the provided column name, we have 
				   found our geo* column */
				if (!state->config->geo_col_name || !strcmp(state->config->geo_col_name, pgfieldname))
				{
					dbffieldtype = 9;
	
					state->geo_col_name = strdup(pgfieldname);
				}
			}
		}

		/*
		 * Everything else (non geometries) will be
		 * a DBF attribute.
		 */

		/* Skip gid (if not asked to do otherwise */
		if (!strcmp(pgfieldname, "gid") )
		{
			gidfound = 1;

			if (!state->config->includegid)
				continue;
		}

		/* Unescape any reserved column names */
		ptr = pgfieldname;
		if (!state->config->unescapedattrs)
		{
			if (*ptr == '_')
				ptr += 2;
		}

		/*
		 * This needs special handling since both xmin and _xmin
		 * becomes __xmin when escaped
		 */

		/* Limit dbf field name to 10-digits */
		dbffieldname = malloc(11);
		strncpy(dbffieldname, ptr, 10);
		dbffieldname[10] = '\0';

		/* If a column map file has been passed in,
		 * use this to create the dbf field name from
		 * the PostgreSQL column name */
		{
		  const char *mapped = colmap_dbf_by_pg(&state->column_map, dbffieldname);
		  if (mapped)
		  {
			  strncpy(dbffieldname, mapped, 10);
			  dbffieldname[10] = '\0';
			}
		}
			
		/*
		 * make sure the fields all have unique names,
		 */
		tmpint = 1;
		for (j = 0; j < state->fieldcount; j++)
		{
			if (!strncasecmp(dbffieldname, state->dbffieldnames[j], 10))
			{
				sprintf(dbffieldname, "%.7s_%.2d", ptr, tmpint++);
				continue;
			}
		}

		/* make UPPERCASE if keep_fieldname_case = 0 */
		if (!state->config->keep_fieldname_case)
			for (j = 0; j < strlen(dbffieldname); j++)
				dbffieldname[j] = toupper(dbffieldname[j]);

		/* Issue warning if column has been renamed */
		if (strcasecmp(dbffieldname, pgfieldname))
		{
			/* Note: we concatenate all warnings from the main loop as this is useful information */
			snprintf(buf, 256, _("Warning, field %s renamed to %s\n"), pgfieldname, dbffieldname);
			strncat(state->message, buf, SHPDUMPERMSGLEN - strlen(state->message));

			ret = SHPDUMPERWARN;
		}


		/*
		 * Find appropriate type of dbf attributes
		 */

		/* int2 type */
		if (pgfieldtype == 21)
		{
			/*
			 * Longest text representation for
			 * an int2 type (16bit) is 6 bytes
			 * (-32768)
			 */
			dbffieldtype = FTInteger;
			dbffieldsize = 6;
			dbffielddecs = 0;
		}

		/* int4 type */
		else if (pgfieldtype == 23)
		{
			/*
			 * Longest text representation for
			 * an int4 type (32bit) is 11 bytes
			 * (-2147483648)
			 */
			dbffieldtype = FTInteger;
			dbffieldsize = 11;
			dbffielddecs = 0;
		}

		/* int8 type */
		else if (pgfieldtype == 20)
		{
			/*
			 * Longest text representation for
			 * an int8 type (64bit) is 20 bytes
			 * (-9223372036854775808)
			 */
			dbffieldtype = FTInteger;
			dbffieldsize = 19;
			dbffielddecs = 0;
		}

		/*
		 * double or numeric types:
		 *    700: float4
		 *    701: float8
		 *   1700: numeric
		 *
		 *
		 * TODO: stricter handling of sizes
		 */
		else if (pgfieldtype == 700 || pgfieldtype == 701 || pgfieldtype == 1700)
		{
			dbffieldtype = FTDouble;
			dbffieldsize = 32;
			dbffielddecs = 10;
		}

		/*
		 * Boolean field, we use FTLogical
		 */
		else if (pgfieldtype == 16)
		{
			dbffieldtype = FTLogical;
			dbffieldsize = 2;
			dbffielddecs = 0;
		}

		/*
		 * Date field
		 */
		else if (pgfieldtype == 1082)
		{
			dbffieldtype = FTDate;
			dbffieldsize = 8;
			dbffielddecs = 0;
		}

		/*
		 * time, timetz, timestamp, or timestamptz field.
		 */
		else if (pgfieldtype == 1083 || pgfieldtype == 1266 || pgfieldtype == 1114 || pgfieldtype == 1184)
		{
			int secondsize;

			switch (pgtypmod)
			{
			case -1:
				secondsize = 6 + 1;
				break;
			case 0:
				secondsize = 0;
				break;
			default:
				secondsize = pgtypmod + 1;
				break;
			}

			/* We assume the worst case scenario for all of these:
			 * date = '5874897-12-31' = 13
			 * date = '294276-11-20' = 12 (with --enable-interger-datetimes)
			 * time = '00:00:00' = 8
			 * zone = '+01:39:52' = 9 (see Europe/Helsinki around 1915)
			 */

			/* time */
			if (pgfieldtype == 1083)
			{
				dbffieldsize = 8 + secondsize;
			}
			/* timetz */
			else if (pgfieldtype == 1266)
			{
				dbffieldsize = 8 + secondsize + 9;
			}
			/* timestamp */
			else if (pgfieldtype == 1114)
			{
				dbffieldsize = 13 + 1 + 8 + secondsize;
			}
			/* timestamptz */
			else if (pgfieldtype == 1184)
			{
				dbffieldsize = 13 + 1 + 8 + secondsize + 9;
			}

			dbffieldtype = FTString;
			dbffielddecs = 0;
		}

		/*
		 * uuid type 36 bytes (12345678-9012-3456-7890-123456789012)
		 */
		else if (pgfieldtype == 2950)
		{
			dbffieldtype = FTString;
			dbffieldsize = 36;
			dbffielddecs = 0;
		}

		/*
		 * For variable-sized fields we know about, we use
		 * the maximum allowed size.
		  * 1042 is bpchar,  1043 is varchar
		 */
		else if ((pgfieldtype == 1042 || pgfieldtype == 1043) && pgtypmod != -1)
		{
			/*
			 * mod is maximum allowed size, including
			 * header which contains *real* size.
			 */
			dbffieldtype = FTString;
			dbffieldsize = pgtypmod - 4; /* 4 is header size */
			dbffielddecs = 0;
		}

		/* For all other valid non-geometry/geography fields... */
		else if (dbffieldtype == -1)
		{
			/*
			* For types we don't know anything about, all
			* we can do is query the table for the maximum field
			* size.
			*/
			dbffieldsize = getMaxFieldSize(state->conn, state->schema, state->table, pgfieldname);
			if (dbffieldsize == -1)
				return 0;

			if (!dbffieldsize)
				dbffieldsize = 32;

			/* might 0 be a good size ? */

			dbffieldtype = FTString;
			dbffielddecs = 0;

			/* Check to make sure the final field size isn't too large */
			if (dbffieldsize > MAX_DBF_FIELD_SIZE)
			{
				/* Note: we concatenate all warnings from the main loop as this is useful information */
				snprintf(buf, 256, _("Warning: values of field '%s' exceeding maximum dbf field width (%d) "
					"will be truncated.\n"), dbffieldname, MAX_DBF_FIELD_SIZE);
				strncat(state->message, buf, SHPDUMPERMSGLEN - strlen(state->message));
				dbffieldsize = MAX_DBF_FIELD_SIZE;				

				ret = SHPDUMPERWARN;
			}
		}

		LWDEBUGF(3, "DBF FIELD_NAME: %s, SIZE: %d\n", dbffieldname, dbffieldsize);
	
		if (dbffieldtype != 9)
		{
			/* Add the field to the DBF file */
			if (DBFAddField(state->dbf, dbffieldname, dbffieldtype, dbffieldsize, dbffielddecs) == -1)
			{
				snprintf(state->message, SHPDUMPERMSGLEN, _("Error: field %s of type %d could not be created."), dbffieldname, dbffieldtype);

				return SHPDUMPERERR;
			}
	
			/* Add the field information to our field arrays */
			state->dbffieldnames[state->fieldcount] = dbffieldname;
			state->dbffieldtypes[state->fieldcount] = dbffieldtype;
			state->pgfieldnames[state->fieldcount] = pgfieldname;
			state->pgfieldlens[state->fieldcount] = pgfieldlen;
			state->pgfieldtypmods[state->fieldcount] = pgtypmod;
			
			state->fieldcount++;
		}
	}

	/* Now we have generated the field lists, grab some info about the table */
	status = getTableInfo(state);
	if (status == SHPDUMPERERR)
		return SHPDUMPERERR;

	LWDEBUGF(3, "rows: %d\n", state->rowcount);
	LWDEBUGF(3, "shptype: %c\n", state->outtype);
	LWDEBUGF(3, "shpouttype: %d\n", state->outshptype);

	/* If we didn't find a geometry/geography column... */
	if (!state->geo_col_name)
	{
		if (state->config->geo_col_name)
		{
			/* A geo* column was specified, but not found */
			snprintf(state->message, SHPDUMPERMSGLEN, _("%s: no such attribute in table %s"), state->config->geo_col_name, state->table);

			return SHPDUMPERERR;
		}
		else
		{
			/* No geo* column specified so we can only create the DBF section -
			   but let's issue a warning... */
			snprintf(buf, 256, _("No geometry column found.\nThe DBF file will be created but not the shx or shp files.\n"));
			strncat(state->message, buf, SHPDUMPERMSGLEN - strlen(state->message));

			state->shp = NULL;
			
			ret = SHPDUMPERWARN;
		}
	}
	else
	{
		/* Since we have found a geo* column, open the shapefile */
		state->shp = SHPCreate(state->shp_file, state->outshptype);
		if (!state->shp)
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("Could not open shapefile %s!"), state->shp_file);

			return SHPDUMPERERR;
		}
	}
	

	/* Now we have the complete list of fieldnames, let's generate the SQL query. First let's make sure
	   we reserve enough space for tables with lots of columns */
	j = 0;
	for (i = 0; i < state->fieldcount; i++)
		j += strlen(state->pgfieldnames[i] + 2);	/* Add 2 for leading and trailing quotes */
	
	state->main_scan_query = malloc(1024 + j);
	
	sprintf(state->main_scan_query, "DECLARE cur ");
	if (state->config->binary)
		strcat(state->main_scan_query, "BINARY ");

	strcat(state->main_scan_query, "CURSOR FOR SELECT ");

	for (i = 0; i < state->fieldcount; i++)
	{
		/* Comma-separated column names */
		if (i > 0)
			strcat(state->main_scan_query, ",");
			
		if (state->config->binary)
			sprintf(buf, "\"%s\"::text", state->pgfieldnames[i]);
		else
			sprintf(buf, "\"%s\"", state->pgfieldnames[i]);

		strcat(state->main_scan_query, buf);
	}

	/* If we found a valid geometry/geography column then use it */
	if (state->geo_col_name)
	{
		/* If this is the (only) column, no need for the initial comma */
		if (state->fieldcount > 0)
			strcat(state->main_scan_query, ",");
		
		if (state->big_endian)
		{
			if (state->pgis_major_version > 0)
			{
				sprintf(buf, "ST_asEWKB(ST_SetSRID(\"%s\"::geometry, 0), 'XDR') AS _geoX", state->geo_col_name);
			}
			else
			{
				sprintf(buf, "asbinary(\"%s\"::geometry, 'XDR') AS _geoX",
					state->geo_col_name);
			}
		}
		else /* little_endian */
		{
			if (state->pgis_major_version > 0)
			{
				sprintf(buf, "ST_AsEWKB(ST_SetSRID(\"%s\"::geometry, 0), 'NDR') AS _geoX", state->geo_col_name);
			}
			else
			{
				sprintf(buf, "asbinary(\"%s\"::geometry, 'NDR') AS _geoX",
					state->geo_col_name);
			}
		}

		strcat(state->main_scan_query, buf);
	}

	if (state->schema)
	{
		sprintf(buf, " FROM \"%s\".\"%s\"", state->schema, state->table);
	}
	else
	{
		sprintf(buf, " FROM \"%s\"", state->table);
	}

	strcat(state->main_scan_query, buf);

	/* Order by 'gid' (if found) */
	if (gidfound)
	{
		sprintf(buf, " ORDER BY \"gid\"");
		strcat(state->main_scan_query, buf);
	}

	/* Now we've finished with the result set, we can dispose of it */
	PQclear(res);

	LWDEBUGF(3, "FINAL QUERY: %s\n", state->main_scan_query);

	/*
	 * Begin the transaction
	 * (a cursor can only be defined inside a transaction block)
	 */
	res = PQexec(state->conn, "BEGIN");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Error starting transaction: %s"), PQresultErrorMessage(res));
		PQclear(res);
		return SHPDUMPERERR;
	}

	PQclear(res);

	/* Execute the main scan query */
	res = PQexec(state->conn, state->main_scan_query);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Error executing main scan query: %s"), PQresultErrorMessage(res));
		PQclear(res);
		return SHPDUMPERERR;
	}

	PQclear(res);

	/* Setup initial scan state */
	state->currow = 0;
	state->curresrow = 0;
	state->currescount = 0;
	state->fetchres = NULL;

	/* Generate the fetch query */
	state->fetch_query = malloc(256);
	sprintf(state->fetch_query, "FETCH %d FROM cur", state->config->fetchsize);

	return SHPDUMPEROK;
}


/* Append the next row to the output shapefile */
int ShpLoaderGenerateShapeRow(SHPDUMPERSTATE *state)
{
	char *hexewkb = NULL;
	unsigned char *hexewkb_binary = NULL;
	size_t hexewkb_len;
	char *val;
	SHPObject *obj = NULL;
	LWGEOM *lwgeom;

	int i, geocolnum = 0;

	/* If we try to go pass the end of the table, fail immediately */
	if (state->currow > state->rowcount)
	{
		snprintf(state->message, SHPDUMPERMSGLEN, _("Tried to read past end of table!"));
		PQclear(state->fetchres);
		return SHPDUMPERERR;
	}

	/* If we have reached the end of the current batch, fetch a new one */
	if (state->curresrow == state->currescount && state->currow < state->rowcount)
	{
		/* Clear the previous batch results */
		if (state->fetchres)
			PQclear(state->fetchres);

		state->fetchres = PQexec(state->conn, state->fetch_query);
		if (PQresultStatus(state->fetchres) != PGRES_TUPLES_OK)
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("Error executing fetch query: %s"), PQresultErrorMessage(state->fetchres));
			PQclear(state->fetchres);
			return SHPDUMPERERR;
		}

		state->curresrow = 0;
		state->currescount = PQntuples(state->fetchres);
	}

	/* Grab the id of the geo column if we have one */
	if (state->geo_col_name)
		geocolnum = PQfnumber(state->fetchres, "_geoX");

	/* Process the next record within the batch. First write out all of 
	the non-geo fields */
	for (i = 0; i < state->fieldcount; i++)
	{
		/*
		* Transform NULL numbers to '0'
		* This is because the shapelib
		* won't easly take care of setting
		* nulls unless paying the acquisition
		* of a bug in long integer values
		*/
		if (PQgetisnull(state->fetchres, state->curresrow, i))
		{
			val = nullDBFValue(state->dbffieldtypes[i]);
		}
		else
		{
			val = PQgetvalue(state->fetchres, state->curresrow, i);
			val = goodDBFValue(val, state->dbffieldtypes[i]);
		}

		/* Write it to the DBF file */
		if (!DBFWriteAttributeDirectly(state->dbf, state->currow, i, val))
		{
			snprintf(state->message, SHPDUMPERMSGLEN, _("Error: record %d could not be created"), state->currow);
			PQclear(state->fetchres);
			return SHPDUMPERERR;
		}
	}

	/* Now process the geo field, if present */
	if (state->geo_col_name)
	{
		/* Handle NULL shapes */
		if (PQgetisnull(state->fetchres, state->curresrow, geocolnum))
		{
			obj = SHPCreateSimpleObject(SHPT_NULL, 0, NULL, NULL, NULL);
			if (SHPWriteObject(state->shp, -1, obj) == -1)
			{
				snprintf(state->message, SHPDUMPERMSGLEN, _("Error writing NULL shape for record %d"), state->currow);
				PQclear(state->fetchres);
				SHPDestroyObject(obj);
				return SHPDUMPERERR;
			}
			SHPDestroyObject(obj);
		}
		else
		{
			/* Get the value from the result set */
			val = PQgetvalue(state->fetchres, state->curresrow, geocolnum);

			if (!state->config->binary)
			{
				if (state->pgis_major_version > 0)
				{
					LWDEBUG(4, "PostGIS >= 1.0, non-binary cursor");

					/* Input is bytea encoded text field, so it must be unescaped and
					then converted to hexewkb string */
					hexewkb_binary = PQunescapeBytea((unsigned char *)val, &hexewkb_len);
					hexewkb = convert_bytes_to_hex(hexewkb_binary, hexewkb_len);
				}
				else
				{
					LWDEBUG(4, "PostGIS < 1.0, non-binary cursor");

					/* Input is already hexewkb string, so we can just
					copy directly to hexewkb */
					hexewkb_len = PQgetlength(state->fetchres, state->curresrow, geocolnum);
					hexewkb = malloc(hexewkb_len + 1);
					strncpy(hexewkb, val, hexewkb_len + 1);
				}
			}
			else /* binary */
			{
				LWDEBUG(4, "PostGIS (any version) using binary cursor");

				/* Input is binary field - must convert to hexewkb string */
				hexewkb_len = PQgetlength(state->fetchres, state->curresrow, geocolnum);
				hexewkb = convert_bytes_to_hex((unsigned char *)val, hexewkb_len);
			}

			LWDEBUGF(4, "HexEWKB - length: %d  value: %s", strlen(hexewkb), hexewkb);

			/* Deserialize the LWGEOM */
			lwgeom = lwgeom_from_hexwkb(hexewkb, LW_PARSER_CHECK_NONE);
			if (!lwgeom)
			{
				snprintf(state->message, SHPDUMPERMSGLEN, _("Error parsing HEXEWKB for record %d"), state->currow);
				PQclear(state->fetchres);
				return SHPDUMPERERR;
			}
	
			/* Call the relevant method depending upon the geometry type */
			LWDEBUGF(4, "geomtype: %s\n", lwtype_name(lwgeom->type));
	
			switch (lwgeom->type)
			{
			case POINTTYPE:
				obj = create_point(state, lwgeom_as_lwpoint(lwgeom));
				break;
	
			case MULTIPOINTTYPE:
				obj = create_multipoint(state, lwgeom_as_lwmpoint(lwgeom));
				break;
	
			case POLYGONTYPE:
				obj = create_polygon(state, lwgeom_as_lwpoly(lwgeom));
				break;
	
			case MULTIPOLYGONTYPE:
				obj = create_multipolygon(state, lwgeom_as_lwmpoly(lwgeom));
				break;
	
			case LINETYPE:
				obj = create_linestring(state, lwgeom_as_lwline(lwgeom));
				break;
	
			case MULTILINETYPE:
				obj = create_multilinestring(state, lwgeom_as_lwmline(lwgeom));
				break;
	
			default:
				snprintf(state->message, SHPDUMPERMSGLEN, _("Unknown WKB type (%d) for record %d"), lwgeom->type, state->currow);
				PQclear(state->fetchres);
				SHPDestroyObject(obj);
				return SHPDUMPERERR;
			}
	
			/* Free both the original and geometries */
			lwgeom_free(lwgeom);

			/* Write the shape out to the file */
			if (SHPWriteObject(state->shp, -1, obj) == -1)
			{
				snprintf(state->message, SHPDUMPERMSGLEN, _("Error writing shape %d"), state->currow);
				PQclear(state->fetchres);
				SHPDestroyObject(obj);
				return SHPDUMPERERR;
			}

			SHPDestroyObject(obj);

			/* Free the hexewkb (and temporary bytea unescaped string if used) */
			if (hexewkb) free(hexewkb);
			if (hexewkb_binary) PQfreemem(hexewkb_binary);
		}
	}

	/* Increment ready for next time */
	state->curresrow++;
	state->currow++;

	return SHPDUMPEROK;
}


/* Return a count of the number of rows in the table being dumped */
int
ShpDumperGetRecordCount(SHPDUMPERSTATE *state)
{
	return state->rowcount;
}


/* Close the specified table and flush all files to disk */
int
ShpDumperCloseTable(SHPDUMPERSTATE *state)
{
	int ret = SHPDUMPEROK;

	/* Clear the current batch fetch resource */
	PQclear(state->fetchres);

	/* If a geo column is present, generate the projection file */
	if (state->geo_col_name)
		ret = projFileCreate(state);	

	/* Close the DBF and SHP files */
	if (state->dbf)
		DBFClose(state->dbf);
	if (state->shp)
		SHPClose(state->shp);

	return ret;
}


void
ShpDumperDestroy(SHPDUMPERSTATE *state)
{
	/* Destroy a state object created with ShpDumperConnect */
	int i;

	if (state != NULL)
	{
		/* Disconnect from the database */
		if (state->conn)
			PQfinish(state->conn);

		/* Free the query strings */
		if (state->fetch_query)
			free(state->fetch_query);
		if (state->main_scan_query)
			free(state->main_scan_query);

		/* Free the DBF information fields */
		if (state->dbffieldnames)
		{
			for (i = 0; i < state->fieldcount; i++)
				free(state->dbffieldnames[i]);
			free(state->dbffieldnames);
		}
		
		if (state->dbffieldtypes)
			free(state->dbffieldtypes);
		
		if (state->pgfieldnames)
			free(state->pgfieldnames);

		/* Free any column map fieldnames if specified */
		colmap_clean(&state->column_map);
		
		/* Free other names */
		if (state->table)
			free(state->table);
		if (state->schema)
			free(state->schema);
		if (state->geo_col_name)
			free(state->geo_col_name);

		/* Free the state itself */
		free(state);
	}
}
