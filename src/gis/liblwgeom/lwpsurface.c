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


LWPSURFACE* lwpsurface_add_lwpoly(LWPSURFACE *mobj, const LWPOLY *obj)
{
	return (LWPSURFACE*)lwcollection_add_lwgeom((LWCOLLECTION*)mobj, (LWGEOM*)obj);
}


void lwpsurface_free(LWPSURFACE *psurf)
{
	int i;
	if ( ! psurf ) return;
	if ( psurf->bbox )
		lwfree(psurf->bbox);

	for ( i = 0; i < psurf->ngeoms; i++ )
		if ( psurf->geoms && psurf->geoms[i] )
			lwpoly_free(psurf->geoms[i]);

	if ( psurf->geoms )
		lwfree(psurf->geoms);

	lwfree(psurf);
}


void printLWPSURFACE(LWPSURFACE *psurf)
{
	int i, j;
	LWPOLY *patch;

	if (psurf->type != POLYHEDRALSURFACETYPE)
		lwerror("printLWPSURFACE called with something else than a POLYHEDRALSURFACE");

	lwnotice("LWPSURFACE {");
	lwnotice("    ndims = %i", (int)FLAGS_NDIMS(psurf->flags));
	lwnotice("    SRID = %i", (int)psurf->srid);
	lwnotice("    ngeoms = %i", (int)psurf->ngeoms);

	for (i=0; i<psurf->ngeoms; i++)
	{
		patch = (LWPOLY *) psurf->geoms[i];
		for (j=0; j<patch->nrings; j++)
		{
			lwnotice("    RING # %i :",j);
			printPA(patch->rings[j]);
		}
	}
	lwnotice("}");
}




/*
 * TODO rewrite all this stuff to be based on a truly topological model
 */

struct struct_psurface_arcs
{
	double ax, ay, az;
	double bx, by, bz;
	int cnt, face;
};
typedef struct struct_psurface_arcs *psurface_arcs;

/* We supposed that the geometry is valid
   we could have wrong result if not */
int lwpsurface_is_closed(const LWPSURFACE *psurface)
{
	int i, j, k;
	int narcs, carc;
	int found;
	psurface_arcs arcs;
	POINT4D pa, pb;
	LWPOLY *patch;

	/* If surface is not 3D, it's can't be closed */
	if (!FLAGS_GET_Z(psurface->flags)) return 0;

	/* If surface is less than 4 faces hard to be closed too */
	if (psurface->ngeoms < 4) return 0;

	/* Max theorical arcs number if no one is shared ... */
	for (i=0, narcs=0 ; i < psurface->ngeoms ; i++)
	{
		patch = (LWPOLY *) psurface->geoms[i];
		narcs += patch->rings[0]->npoints - 1;
	}

	arcs = lwalloc(sizeof(struct struct_psurface_arcs) * narcs);
	for (i=0, carc=0; i < psurface->ngeoms ; i++)
	{

		patch = (LWPOLY *) psurface->geoms[i];
		for (j=0; j < patch->rings[0]->npoints - 1; j++)
		{

			getPoint4d_p(patch->rings[0], j,   &pa);
			getPoint4d_p(patch->rings[0], j+1, &pb);

			/* remove redundant points if any */
			if (pa.x == pb.x && pa.y == pb.y && pa.z == pb.z) continue;

			/* Make sure to order the 'lower' point first */
			if ( (pa.x > pb.x) ||
			        (pa.x == pb.x && pa.y > pb.y) ||
			        (pa.x == pb.x && pa.y == pb.y && pa.z > pb.z) )
			{
				pa = pb;
				getPoint4d_p(patch->rings[0], j, &pb);
			}

			for (found=0, k=0; k < carc ; k++)
			{

				if (  ( arcs[k].ax == pa.x && arcs[k].ay == pa.y &&
				        arcs[k].az == pa.z && arcs[k].bx == pb.x &&
				        arcs[k].by == pb.y && arcs[k].bz == pb.z &&
				        arcs[k].face != i) )
				{
					arcs[k].cnt++;
					found = 1;

					/* Look like an invalid PolyhedralSurface
					      anyway not a closed one */
					if (arcs[k].cnt > 2)
					{
						lwfree(arcs);
						return 0;
					}
				}
			}

			if (!found)
			{
				arcs[carc].cnt=1;
				arcs[carc].face=i;
				arcs[carc].ax = pa.x;
				arcs[carc].ay = pa.y;
				arcs[carc].az = pa.z;
				arcs[carc].bx = pb.x;
				arcs[carc].by = pb.y;
				arcs[carc].bz = pb.z;
				carc++;

				/* Look like an invalid PolyhedralSurface
				      anyway not a closed one */
				if (carc > narcs)
				{
					lwfree(arcs);
					return 0;
				}
			}
		}
	}

	/* A polyhedron is closed if each edge
	       is shared by exactly 2 faces */
	for (k=0; k < carc ; k++)
	{
		if (arcs[k].cnt != 2)
		{
			lwfree(arcs);
			return 0;
		}
	}
	lwfree(arcs);

	/* Invalid Polyhedral case */
	if (carc < psurface->ngeoms) return 0;

	return 1;
}
