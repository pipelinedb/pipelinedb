/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2001-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"


void
lwmpoly_release(LWMPOLY *lwmpoly)
{
	lwgeom_release(lwmpoly_as_lwgeom(lwmpoly));
}

LWMPOLY *
lwmpoly_construct_empty(int srid, char hasz, char hasm)
{
	LWMPOLY *ret = (LWMPOLY*)lwcollection_construct_empty(MULTIPOLYGONTYPE, srid, hasz, hasm);
	return ret;
}


LWMPOLY* lwmpoly_add_lwpoly(LWMPOLY *mobj, const LWPOLY *obj)
{
	return (LWMPOLY*)lwcollection_add_lwgeom((LWCOLLECTION*)mobj, (LWGEOM*)obj);
}


void lwmpoly_free(LWMPOLY *mpoly)
{
	int i;
	if ( ! mpoly ) return;
	if ( mpoly->bbox )
		lwfree(mpoly->bbox);

	for ( i = 0; i < mpoly->ngeoms; i++ )
		if ( mpoly->geoms && mpoly->geoms[i] )
			lwpoly_free(mpoly->geoms[i]);

	if ( mpoly->geoms )
		lwfree(mpoly->geoms);

	lwfree(mpoly);
}

