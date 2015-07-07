/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2004 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "lwgeom_log.h"
#include "liblwgeom.h"

#include <stdio.h>
#include <string.h>

/* Place to hold the ZM string used in other summaries */
static char tflags[6];

static char *
lwgeom_flagchars(LWGEOM *lwg)
{
	int flagno = 0;
	if ( FLAGS_GET_Z(lwg->flags) ) tflags[flagno++] = 'Z';
	if ( FLAGS_GET_M(lwg->flags) ) tflags[flagno++] = 'M';
	if ( FLAGS_GET_BBOX(lwg->flags) ) tflags[flagno++] = 'B';
	if ( FLAGS_GET_GEODETIC(lwg->flags) ) tflags[flagno++] = 'G';
	if ( lwg->srid != SRID_UNKNOWN ) tflags[flagno++] = 'S';
	tflags[flagno] = '\0';

	LWDEBUGF(4, "Flags: %s - returning %p", flags, tflags);

	return tflags;
}

/*
 * Returns an alloced string containing summary for the LWGEOM object
 */
static char *
lwpoint_summary(LWPOINT *point, int offset)
{
	char *result;
	char *pad="";
	char *zmflags = lwgeom_flagchars((LWGEOM*)point);

	result = (char *)lwalloc(128+offset);

	sprintf(result, "%*.s%s[%s]",
	        offset, pad, lwtype_name(point->type),
	        zmflags);
	return result;
}

static char *
lwline_summary(LWLINE *line, int offset)
{
	char *result;
	char *pad="";
	char *zmflags = lwgeom_flagchars((LWGEOM*)line);

	result = (char *)lwalloc(128+offset);

	sprintf(result, "%*.s%s[%s] with %d points",
	        offset, pad, lwtype_name(line->type),
	        zmflags,
	        line->points->npoints);
	return result;
}


static char *
lwcollection_summary(LWCOLLECTION *col, int offset)
{
	size_t size = 128;
	char *result;
	char *tmp;
	int i;
	static char *nl = "\n";
	char *pad="";
	char *zmflags = lwgeom_flagchars((LWGEOM*)col);

	LWDEBUG(2, "lwcollection_summary called");

	result = (char *)lwalloc(size);

	sprintf(result, "%*.s%s[%s] with %d elements\n",
	        offset, pad, lwtype_name(col->type),
	        zmflags,
	        col->ngeoms);

	for (i=0; i<col->ngeoms; i++)
	{
		tmp = lwgeom_summary(col->geoms[i], offset+2);
		size += strlen(tmp)+1;
		result = lwrealloc(result, size);

		LWDEBUGF(4, "Reallocated %d bytes for result", size);
		if ( i > 0 ) strcat(result,nl);

		strcat(result, tmp);
		lwfree(tmp);
	}

	LWDEBUG(3, "lwcollection_summary returning");

	return result;
}

static char *
lwpoly_summary(LWPOLY *poly, int offset)
{
	char tmp[256];
	size_t size = 64*(poly->nrings+1)+128;
	char *result;
	int i;
	char *pad="";
	static char *nl = "\n";
	char *zmflags = lwgeom_flagchars((LWGEOM*)poly);

	LWDEBUG(2, "lwpoly_summary called");

	result = (char *)lwalloc(size);

	sprintf(result, "%*.s%s[%s] with %i rings\n",
	        offset, pad, lwtype_name(poly->type),
	        zmflags,
	        poly->nrings);

	for (i=0; i<poly->nrings; i++)
	{
		sprintf(tmp,"%s   ring %i has %i points",
		        pad, i, poly->rings[i]->npoints);
		if ( i > 0 ) strcat(result,nl);
		strcat(result,tmp);
	}

	LWDEBUG(3, "lwpoly_summary returning");

	return result;
}

char *
lwgeom_summary(const LWGEOM *lwgeom, int offset)
{
	char *result;

	switch (lwgeom->type)
	{
	case POINTTYPE:
		return lwpoint_summary((LWPOINT *)lwgeom, offset);

	case LINETYPE:
		return lwline_summary((LWLINE *)lwgeom, offset);

	case POLYGONTYPE:
		return lwpoly_summary((LWPOLY *)lwgeom, offset);

	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTIPOLYGONTYPE:
	case COLLECTIONTYPE:
		return lwcollection_summary((LWCOLLECTION *)lwgeom, offset);
	default:
		result = (char *)lwalloc(256);
		sprintf(result, "Object is of unknown type: %d",
		        lwgeom->type);
		return result;
	}

	return NULL;
}
