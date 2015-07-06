/**********************************************************************
*
* PostGIS - Spatial Types for PostgreSQL
* http://postgis.net
*
* Copyright 2014 Kashif Rasul <kashif.rasul@gmail.com> and
*                Shoaib Burq <saburq@gmail.com>
*
* This is free software; you can redistribute and/or modify it under
* the terms of the GNU General Public Licence. See the COPYING file.
*
**********************************************************************/

#include "stringbuffer.h"
#include "liblwgeom_internal.h"

static char * lwline_to_encoded_polyline(const LWLINE*, int precision);
static char * lwmmpoint_to_encoded_polyline(const LWMPOINT*, int precision);
static char * pointarray_to_encoded_polyline(const POINTARRAY*, int precision);

/* takes a GEOMETRY and returns an Encoded Polyline representation */
extern char *
lwgeom_to_encoded_polyline(const LWGEOM *geom, int precision)
{
	int type = geom->type;
	switch (type)
	{
	case LINETYPE:
		return lwline_to_encoded_polyline((LWLINE*)geom, precision);
	case MULTIPOINTTYPE:
		return lwmmpoint_to_encoded_polyline((LWMPOINT*)geom, precision);
	default:
		lwerror("lwgeom_to_encoded_polyline: '%s' geometry type not supported", lwtype_name(type));
		return NULL;
	}
}

static
char * lwline_to_encoded_polyline(const LWLINE *line, int precision)
{
	return pointarray_to_encoded_polyline(line->points, precision);
}

static
char * lwmmpoint_to_encoded_polyline(const LWMPOINT *mpoint, int precision)
{
	LWLINE *line = lwline_from_lwmpoint(mpoint->srid, mpoint);
	char *encoded_polyline = lwline_to_encoded_polyline(line, precision);

	lwline_free(line);
	return encoded_polyline;
}

static
char * pointarray_to_encoded_polyline(const POINTARRAY *pa, int precision)
{
	int i;
	const POINT2D *prevPoint;
	int *delta = lwalloc(2*sizeof(int)*pa->npoints);
	char *encoded_polyline = NULL;
	stringbuffer_t *sb;
	double scale = pow(10,precision);

	/* Take the double value and multiply it by 1x10^percision, rounding the result */
	prevPoint = getPoint2d_cp(pa, 0);
	delta[0] = round(prevPoint->y*scale);
	delta[1] = round(prevPoint->x*scale);

	/*  points only include the offset from the previous point */
	for (i=1; i<pa->npoints; i++)
	{
		const POINT2D *point = getPoint2d_cp(pa, i);
		delta[2*i] = round(point->y*scale) - round(prevPoint->y*scale);
		delta[(2*i)+1] = round(point->x*scale) - round(prevPoint->x*scale);
		prevPoint = point;
	}

	/* value to binary: a negative value must be calculated using its two's complement */
	for (i=0; i<pa->npoints*2; i++)
	{
		/* Left-shift the binary value one bit */
		delta[i] <<= 1;
		/* if value is negative, invert this encoding */
		if (delta[i] < 0) {
			delta[i] = ~(delta[i]);
		}
	}

	sb = stringbuffer_create();
	for (i=0; i<pa->npoints*2; i++)
	{
		int numberToEncode = delta[i];

		while (numberToEncode >= 0x20) {
			/* Place the 5-bit chunks into reverse order or
			 each value with 0x20 if another bit chunk follows and add 63*/
			int nextValue = (0x20 | (numberToEncode & 0x1f)) + 63;
			stringbuffer_aprintf(sb, "%c", (char)nextValue);
			if(92 == nextValue)
				stringbuffer_aprintf(sb, "%c", (char)nextValue);

			/* Break the binary value out into 5-bit chunks */
			numberToEncode >>= 5;
		}

		numberToEncode += 63;
		stringbuffer_aprintf(sb, "%c", (char)numberToEncode);
		if(92 == numberToEncode)
			stringbuffer_aprintf(sb, "%c", (char)numberToEncode);
	}

	lwfree(delta);
	encoded_polyline = stringbuffer_getstringcopy(sb);
	stringbuffer_destroy(sb);

	return encoded_polyline;
}
