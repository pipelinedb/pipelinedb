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
lwmpoint_release(LWMPOINT *lwmpoint)
{
	lwgeom_release(lwmpoint_as_lwgeom(lwmpoint));
}

LWMPOINT *
lwmpoint_construct_empty(int srid, char hasz, char hasm)
{
	LWMPOINT *ret = (LWMPOINT*)lwcollection_construct_empty(MULTIPOINTTYPE, srid, hasz, hasm);
	return ret;
}

LWMPOINT* lwmpoint_add_lwpoint(LWMPOINT *mobj, const LWPOINT *obj)
{
	LWDEBUG(4, "Called");
	return (LWMPOINT*)lwcollection_add_lwgeom((LWCOLLECTION*)mobj, (LWGEOM*)obj);
}

LWMPOINT *
lwmpoint_construct(int srid, const POINTARRAY *pa)
{
	int i;
	int hasz = ptarray_has_z(pa);
	int hasm = ptarray_has_m(pa);
	LWMPOINT *ret = (LWMPOINT*)lwcollection_construct_empty(MULTIPOINTTYPE, srid, hasz, hasm);
	
	for ( i = 0; i < pa->npoints; i++ )
	{
		LWPOINT *lwp;
		POINT4D p;
		getPoint4d_p(pa, i, &p);		
		lwp = lwpoint_make(srid, hasz, hasm, &p);
		lwmpoint_add_lwpoint(ret, lwp);
	}
	
	return ret;
}


void lwmpoint_free(LWMPOINT *mpt)
{
	int i;

	if ( ! mpt ) return;
	
	if ( mpt->bbox )
		lwfree(mpt->bbox);

	for ( i = 0; i < mpt->ngeoms; i++ )
		if ( mpt->geoms && mpt->geoms[i] )
			lwpoint_free(mpt->geoms[i]);

	if ( mpt->geoms )
		lwfree(mpt->geoms);

	lwfree(mpt);
}

LWGEOM*
lwmpoint_remove_repeated_points(LWMPOINT *mpoint)
{
	uint32_t nnewgeoms;
	uint32_t i, j;
	LWGEOM **newgeoms;

	newgeoms = lwalloc(sizeof(LWGEOM *)*mpoint->ngeoms);
	nnewgeoms = 0;
	for (i=0; i<mpoint->ngeoms; ++i)
	{
		/* Brute force, may be optimized by building an index */
		int seen=0;
		for (j=0; j<nnewgeoms; ++j)
		{
			if ( lwpoint_same((LWPOINT*)newgeoms[j],
			                  (LWPOINT*)mpoint->geoms[i]) )
			{
				seen=1;
				break;
			}
		}
		if ( seen ) continue;
		newgeoms[nnewgeoms++] = (LWGEOM*)lwpoint_clone(mpoint->geoms[i]);
	}

	return (LWGEOM*)lwcollection_construct(mpoint->type,
	                                       mpoint->srid, mpoint->bbox ? gbox_copy(mpoint->bbox) : NULL,
	                                       nnewgeoms, newgeoms);

}

