/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of hte GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

/** @file
*
* SVG output routines.
* Originally written by: Klaus Förster <klaus@svg.cc>
* Refactored by: Olivier Courtin (Camptocamp)
*
* BNF SVG Path: <http://www.w3.org/TR/SVG/paths.html#PathDataBNF>
**********************************************************************/

#include "liblwgeom_internal.h"

static char * assvg_point(const LWPOINT *point, int relative, int precision);
static char * assvg_line(const LWLINE *line, int relative, int precision);
static char * assvg_polygon(const LWPOLY *poly, int relative, int precision);
static char * assvg_multipoint(const LWMPOINT *mpoint, int relative, int precision);
static char * assvg_multiline(const LWMLINE *mline, int relative, int precision);
static char * assvg_multipolygon(const LWMPOLY *mpoly, int relative, int precision);
static char * assvg_collection(const LWCOLLECTION *col, int relative, int precision);

static size_t assvg_geom_size(const LWGEOM *geom, int relative, int precision);
static size_t assvg_geom_buf(const LWGEOM *geom, char *output, int relative, int precision);
static size_t pointArray_svg_size(POINTARRAY *pa, int precision);
static size_t pointArray_svg_rel(POINTARRAY *pa, char * output, int close_ring, int precision);
static size_t pointArray_svg_abs(POINTARRAY *pa, char * output, int close_ring, int precision);


/**
 * Takes a GEOMETRY and returns a SVG representation
 */
char *
lwgeom_to_svg(const LWGEOM *geom, int precision, int relative)
{
	char *ret = NULL;
	int type = geom->type;

	/* Empty string for empties */
	if( lwgeom_is_empty(geom) )
	{
		ret = lwalloc(1);
		ret[0] = '\0';
		return ret;
	}
	
	switch (type)
	{
	case POINTTYPE:
		ret = assvg_point((LWPOINT*)geom, relative, precision);
		break;
	case LINETYPE:
		ret = assvg_line((LWLINE*)geom, relative, precision);
		break;
	case POLYGONTYPE:
		ret = assvg_polygon((LWPOLY*)geom, relative, precision);
		break;
	case MULTIPOINTTYPE:
		ret = assvg_multipoint((LWMPOINT*)geom, relative, precision);
		break;
	case MULTILINETYPE:
		ret = assvg_multiline((LWMLINE*)geom, relative, precision);
		break;
	case MULTIPOLYGONTYPE:
		ret = assvg_multipolygon((LWMPOLY*)geom, relative, precision);
		break;
	case COLLECTIONTYPE:
		ret = assvg_collection((LWCOLLECTION*)geom, relative, precision);
		break;

	default:
		lwerror("lwgeom_to_svg: '%s' geometry type not supported",
		        lwtype_name(type));
	}

	return ret;
}


/**
 * Point Geometry
 */

static size_t
assvg_point_size(const LWPOINT *point, int circle, int precision)
{
	size_t size;

	size = (OUT_MAX_DIGS_DOUBLE + precision) * 2;
	if (circle) size += sizeof("cx='' cy=''");
	else size += sizeof("x='' y=''");

	return size;
}

static size_t
assvg_point_buf(const LWPOINT *point, char * output, int circle, int precision)
{
	char *ptr=output;
	char x[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	char y[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	POINT2D pt;

	getPoint2d_p(point->point, 0, &pt);

	if (fabs(pt.x) < OUT_MAX_DOUBLE)
		sprintf(x, "%.*f", precision, pt.x);
	else
		sprintf(x, "%g", pt.x);
	trim_trailing_zeros(x);

	/* SVG Y axis is reversed, an no need to transform 0 into -0 */
	if (fabs(pt.y) < OUT_MAX_DOUBLE)
		sprintf(y, "%.*f", precision, fabs(pt.y) ? pt.y * -1 : pt.y);
	else
		sprintf(y, "%g", fabs(pt.y) ? pt.y * -1 : pt.y);
	trim_trailing_zeros(y);

	if (circle) ptr += sprintf(ptr, "x=\"%s\" y=\"%s\"", x, y);
	else ptr += sprintf(ptr, "cx=\"%s\" cy=\"%s\"", x, y);

	return (ptr-output);
}

static char *
assvg_point(const LWPOINT *point, int circle, int precision)
{
	char *output;
	int size;

	size = assvg_point_size(point, circle, precision);
	output = lwalloc(size);
	assvg_point_buf(point, output, circle, precision);

	return output;
}


/**
 * Line Geometry
 */

static size_t
assvg_line_size(const LWLINE *line, int relative, int precision)
{
	size_t size;

	size = sizeof("M ");
	size += pointArray_svg_size(line->points, precision);

	return size;
}

static size_t
assvg_line_buf(const LWLINE *line, char * output, int relative, int precision)
{
	char *ptr=output;

	/* Start path with SVG MoveTo */
	ptr += sprintf(ptr, "M ");
	if (relative)
		ptr += pointArray_svg_rel(line->points, ptr, 1, precision);
	else
		ptr += pointArray_svg_abs(line->points, ptr, 1, precision);

	return (ptr-output);
}

static char *
assvg_line(const LWLINE *line, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_line_size(line, relative, precision);
	output = lwalloc(size);
	assvg_line_buf(line, output, relative, precision);

	return output;
}


/**
 * Polygon Geometry
 */

static size_t
assvg_polygon_size(const LWPOLY *poly, int relative, int precision)
{
	int i;
	size_t size=0;

	for (i=0; i<poly->nrings; i++)
		size += pointArray_svg_size(poly->rings[i], precision) + sizeof(" ");
	size += sizeof("M  Z") * poly->nrings;

	return size;
}

static size_t
assvg_polygon_buf(const LWPOLY *poly, char * output, int relative, int precision)
{
	int i;
	char *ptr=output;

	for (i=0; i<poly->nrings; i++)
	{
		if (i) ptr += sprintf(ptr, " ");	/* Space beetween each ring */
		ptr += sprintf(ptr, "M ");		/* Start path with SVG MoveTo */

		if (relative)
		{
			ptr += pointArray_svg_rel(poly->rings[i], ptr, 0, precision);
			ptr += sprintf(ptr, " z");	/* SVG closepath */
		}
		else
		{
			ptr += pointArray_svg_abs(poly->rings[i], ptr, 0, precision);
			ptr += sprintf(ptr, " Z");	/* SVG closepath */
		}
	}

	return (ptr-output);
}

static char *
assvg_polygon(const LWPOLY *poly, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_polygon_size(poly, relative, precision);
	output = lwalloc(size);
	assvg_polygon_buf(poly, output, relative, precision);

	return output;
}


/**
 * Multipoint Geometry
 */

static size_t
assvg_multipoint_size(const LWMPOINT *mpoint, int relative, int precision)
{
	const LWPOINT *point;
	size_t size=0;
	int i;

	for (i=0 ; i<mpoint->ngeoms ; i++)
	{
		point = mpoint->geoms[i];
		size += assvg_point_size(point, relative, precision);
	}
	size += sizeof(",") * --i;  /* Arbitrary comma separator */

	return size;
}

static size_t
assvg_multipoint_buf(const LWMPOINT *mpoint, char *output, int relative, int precision)
{
	const LWPOINT *point;
	int i;
	char *ptr=output;

	for (i=0 ; i<mpoint->ngeoms ; i++)
	{
		if (i) ptr += sprintf(ptr, ",");  /* Arbitrary comma separator */
		point = mpoint->geoms[i];
		ptr += assvg_point_buf(point, ptr, relative, precision);
	}

	return (ptr-output);
}

static char *
assvg_multipoint(const LWMPOINT *mpoint, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_multipoint_size(mpoint, relative, precision);
	output = lwalloc(size);
	assvg_multipoint_buf(mpoint, output, relative, precision);

	return output;
}


/**
 * Multiline Geometry
 */

static size_t
assvg_multiline_size(const LWMLINE *mline, int relative, int precision)
{
	const LWLINE *line;
	size_t size=0;
	int i;

	for (i=0 ; i<mline->ngeoms ; i++)
	{
		line = mline->geoms[i];
		size += assvg_line_size(line, relative, precision);
	}
	size += sizeof(" ") * --i;   /* SVG whitespace Separator */

	return size;
}

static size_t
assvg_multiline_buf(const LWMLINE *mline, char *output, int relative, int precision)
{
	const LWLINE *line;
	int i;
	char *ptr=output;

	for (i=0 ; i<mline->ngeoms ; i++)
	{
		if (i) ptr += sprintf(ptr, " ");  /* SVG whitespace Separator */
		line = mline->geoms[i];
		ptr += assvg_line_buf(line, ptr, relative, precision);
	}

	return (ptr-output);
}

static char *
assvg_multiline(const LWMLINE *mline, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_multiline_size(mline, relative, precision);
	output = lwalloc(size);
	assvg_multiline_buf(mline, output, relative, precision);

	return output;
}


/*
 * Multipolygon Geometry
 */

static size_t
assvg_multipolygon_size(const LWMPOLY *mpoly, int relative, int precision)
{
	const LWPOLY *poly;
	size_t size=0;
	int i;

	for (i=0 ; i<mpoly->ngeoms ; i++)
	{
		poly = mpoly->geoms[i];
		size += assvg_polygon_size(poly, relative, precision);
	}
	size += sizeof(" ") * --i;   /* SVG whitespace Separator */

	return size;
}

static size_t
assvg_multipolygon_buf(const LWMPOLY *mpoly, char *output, int relative, int precision)
{
	const LWPOLY *poly;
	int i;
	char *ptr=output;

	for (i=0 ; i<mpoly->ngeoms ; i++)
	{
		if (i) ptr += sprintf(ptr, " ");  /* SVG whitespace Separator */
		poly = mpoly->geoms[i];
		ptr += assvg_polygon_buf(poly, ptr, relative, precision);
	}

	return (ptr-output);
}

static char *
assvg_multipolygon(const LWMPOLY *mpoly, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_multipolygon_size(mpoly, relative, precision);
	output = lwalloc(size);
	assvg_multipolygon_buf(mpoly, output, relative, precision);

	return output;
}


/**
* Collection Geometry
*/

static size_t
assvg_collection_size(const LWCOLLECTION *col, int relative, int precision)
{
	int i = 0;
	size_t size=0;
	const LWGEOM *subgeom;

	for (i=0; i<col->ngeoms; i++)
	{
		subgeom = col->geoms[i];
		size += assvg_geom_size(subgeom, relative, precision);
	}

	if ( i ) /* We have some geometries, so add space for delimiters. */
		size += sizeof(";") * --i;

	if (size == 0) size++; /* GEOMETRYCOLLECTION EMPTY, space for null terminator */

	return size;
}

static size_t
assvg_collection_buf(const LWCOLLECTION *col, char *output, int relative, int precision)
{
	int i;
	char *ptr=output;
	const LWGEOM *subgeom;

	/* EMPTY GEOMETRYCOLLECTION */
	if (col->ngeoms == 0) *ptr = '\0';

	for (i=0; i<col->ngeoms; i++)
	{
		if (i) ptr += sprintf(ptr, ";");
		subgeom = col->geoms[i];
		ptr += assvg_geom_buf(subgeom, ptr, relative, precision);
	}

	return (ptr - output);
}

static char *
assvg_collection(const LWCOLLECTION *col, int relative, int precision)
{
	char *output;
	int size;

	size = assvg_collection_size(col, relative, precision);
	output = lwalloc(size);
	assvg_collection_buf(col, output, relative, precision);

	return output;
}


static size_t
assvg_geom_buf(const LWGEOM *geom, char *output, int relative, int precision)
{
    int type = geom->type;
	char *ptr=output;

	switch (type)
	{
	case POINTTYPE:
		ptr += assvg_point_buf((LWPOINT*)geom, ptr, relative, precision);
		break;

	case LINETYPE:
		ptr += assvg_line_buf((LWLINE*)geom, ptr, relative, precision);
		break;

	case POLYGONTYPE:
		ptr += assvg_polygon_buf((LWPOLY*)geom, ptr, relative, precision);
		break;

	case MULTIPOINTTYPE:
		ptr += assvg_multipoint_buf((LWMPOINT*)geom, ptr, relative, precision);
		break;

	case MULTILINETYPE:
		ptr += assvg_multiline_buf((LWMLINE*)geom, ptr, relative, precision);
		break;

	case MULTIPOLYGONTYPE:
		ptr += assvg_multipolygon_buf((LWMPOLY*)geom, ptr, relative, precision);
		break;

	default:
		lwerror("assvg_geom_buf: '%s' geometry type not supported.",
		        lwtype_name(type));
	}

	return (ptr-output);
}


static size_t
assvg_geom_size(const LWGEOM *geom, int relative, int precision)
{
    int type = geom->type;
	size_t size = 0;

	switch (type)
	{
	case POINTTYPE:
		size = assvg_point_size((LWPOINT*)geom, relative, precision);
		break;

	case LINETYPE:
		size = assvg_line_size((LWLINE*)geom, relative, precision);
		break;

	case POLYGONTYPE:
		size = assvg_polygon_size((LWPOLY*)geom, relative, precision);
		break;

	case MULTIPOINTTYPE:
		size = assvg_multipoint_size((LWMPOINT*)geom, relative, precision);
		break;

	case MULTILINETYPE:
		size = assvg_multiline_size((LWMLINE*)geom, relative, precision);
		break;

	case MULTIPOLYGONTYPE:
		size = assvg_multipolygon_size((LWMPOLY*)geom, relative, precision);
		break;

	default:
		lwerror("assvg_geom_size: '%s' geometry type not supported.",
		        lwtype_name(type));
	}

	return size;
}


static size_t
pointArray_svg_rel(POINTARRAY *pa, char *output, int close_ring, int precision)
{
	int i, end;
	char *ptr;
	char x[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	char y[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	POINT2D pt, lpt;

	ptr = output;

	if (close_ring) end = pa->npoints;
	else end = pa->npoints - 1;

	/* Starting point */
	getPoint2d_p(pa, 0, &pt);

	if (fabs(pt.x) < OUT_MAX_DOUBLE)
		sprintf(x, "%.*f", precision, pt.x);
	else
		sprintf(x, "%g", pt.x);
	trim_trailing_zeros(x);

	if (fabs(pt.y) < OUT_MAX_DOUBLE)
		sprintf(y, "%.*f", precision, fabs(pt.y) ? pt.y * -1 : pt.y);
	else
		sprintf(y, "%g", fabs(pt.y) ? pt.y * -1 : pt.y);
	trim_trailing_zeros(y);

	ptr += sprintf(ptr,"%s %s l", x, y);

	/* All the following ones */
	for (i=1 ; i < end ; i++)
	{
		lpt = pt;

		getPoint2d_p(pa, i, &pt);
		if (fabs(pt.x -lpt.x) < OUT_MAX_DOUBLE)
			sprintf(x, "%.*f", precision, pt.x -lpt.x);
		else
			sprintf(x, "%g", pt.x -lpt.x);
		trim_trailing_zeros(x);

		/* SVG Y axis is reversed, an no need to transform 0 into -0 */
		if (fabs(pt.y -lpt.y) < OUT_MAX_DOUBLE)
			sprintf(y, "%.*f", precision,
			        fabs(pt.y -lpt.y) ? (pt.y - lpt.y) * -1: (pt.y - lpt.y));
		else
			sprintf(y, "%g",
			        fabs(pt.y -lpt.y) ? (pt.y - lpt.y) * -1: (pt.y - lpt.y));
		trim_trailing_zeros(y);

		ptr += sprintf(ptr," %s %s", x, y);
	}

	return (ptr-output);
}


/**
 * Returns maximum size of rendered pointarray in bytes.
 */
static size_t
pointArray_svg_abs(POINTARRAY *pa, char *output, int close_ring, int precision)
{
	int i, end;
	char *ptr;
	char x[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	char y[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	POINT2D pt;

	ptr = output;

	if (close_ring) end = pa->npoints;
	else end = pa->npoints - 1;

	for (i=0 ; i < end ; i++)
	{
		getPoint2d_p(pa, i, &pt);

		if (fabs(pt.x) < OUT_MAX_DOUBLE)
			sprintf(x, "%.*f", precision, pt.x);
		else
			sprintf(x, "%g", pt.x);
		trim_trailing_zeros(x);

		/* SVG Y axis is reversed, an no need to transform 0 into -0 */
		if (fabs(pt.y) < OUT_MAX_DOUBLE)
			sprintf(y, "%.*f", precision, fabs(pt.y) ? pt.y * -1:pt.y);
		else
			sprintf(y, "%g", fabs(pt.y) ? pt.y * -1:pt.y);
		trim_trailing_zeros(y);

		if (i == 1) ptr += sprintf(ptr, " L ");
		else if (i) ptr += sprintf(ptr, " ");
		ptr += sprintf(ptr,"%s %s", x, y);
	}

	return (ptr-output);
}


/**
 * Returns maximum size of rendered pointarray in bytes.
 */
static size_t
pointArray_svg_size(POINTARRAY *pa, int precision)
{
	return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(" "))
	       * 2 * pa->npoints + sizeof(" L ");
}
