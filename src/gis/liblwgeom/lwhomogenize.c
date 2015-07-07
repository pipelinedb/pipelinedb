/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 Olivier Courtin <olivier.courtin@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdlib.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"


typedef struct {
	int cnt[NUMTYPES];
	LWCOLLECTION* buf[NUMTYPES];
} HomogenizeBuffer;

static void
init_homogenizebuffer(HomogenizeBuffer *buffer)
{
	int i;
	for ( i = 0; i < NUMTYPES; i++ )
	{
		buffer->cnt[i] = 0;
		buffer->buf[i] = NULL;
	}
}

/*
static void
free_homogenizebuffer(HomogenizeBuffer *buffer)
{
	int i;
	for ( i = 0; i < NUMTYPES; i++ )
	{
		if ( buffer->buf[i] )
		{
			lwcollection_free(buffer->buf[i]);
		}
	}
}
*/

/*
** Given a generic collection, return the "simplest" form.
**
** eg: GEOMETRYCOLLECTION(MULTILINESTRING()) => MULTILINESTRING()
**
**     GEOMETRYCOLLECTION(MULTILINESTRING(), MULTILINESTRING(), POINT())
**      => GEOMETRYCOLLECTION(MULTILINESTRING(), POINT())
**
** In general, if the subcomponents are homogeneous, return a properly
** typed collection.
** Otherwise, return a generic collection, with the subtypes in minimal
** typed collections.
*/
static void
lwcollection_build_buffer(const LWCOLLECTION *col, HomogenizeBuffer *buffer)
{
	int i;
	
	if ( ! col ) return;
	if ( lwgeom_is_empty(lwcollection_as_lwgeom(col)) ) return;
	for ( i = 0; i < col->ngeoms; i++ )
	{
		LWGEOM *geom = col->geoms[i];
		switch(geom->type)
		{
			case POINTTYPE:
			case LINETYPE:
			case CIRCSTRINGTYPE:
			case COMPOUNDTYPE:
			case TRIANGLETYPE:
			case CURVEPOLYTYPE:
			case POLYGONTYPE:
			{
				/* Init if necessary */
				if ( ! buffer->buf[geom->type] )
				{
					LWCOLLECTION *bufcol = lwcollection_construct_empty(COLLECTIONTYPE, col->srid, FLAGS_GET_Z(col->flags), FLAGS_GET_M(col->flags));
					bufcol->type = lwtype_get_collectiontype(geom->type);
					buffer->buf[geom->type] = bufcol;
				}
				/* Add sub-geom to buffer */
				lwcollection_add_lwgeom(buffer->buf[geom->type], lwgeom_clone(geom));
				/* Increment count for this singleton type */
				buffer->cnt[geom->type] = buffer->cnt[geom->type] + 1;
			}
			default:
			{
				lwcollection_build_buffer(lwgeom_as_lwcollection(geom), buffer);
			}
		}
	}
	return;
}

static LWGEOM*
lwcollection_homogenize(const LWCOLLECTION *col)
{
	int i;
	int ntypes = 0;
	int type = 0;
	LWGEOM *outgeom = NULL;
	
	HomogenizeBuffer buffer;

	/* Sort all the parts into a buffer */
	init_homogenizebuffer(&buffer);
	lwcollection_build_buffer(col, &buffer);
	
	/* Check for homogeneity */
	for ( i = 0; i < NUMTYPES; i++ )
	{
		if ( buffer.cnt[i] > 0 )
		{
			ntypes++;
			type = i;
		}
	}
	
	/* No types? Huh. Return empty. */
	if ( ntypes == 0 )
	{
		LWCOLLECTION *outcol;
		outcol = lwcollection_construct_empty(COLLECTIONTYPE, col->srid, FLAGS_GET_Z(col->flags), FLAGS_GET_M(col->flags));
		outgeom = lwcollection_as_lwgeom(outcol);
	}
	/* One type, return homogeneous collection */
	else if ( ntypes == 1 )
	{
		LWCOLLECTION *outcol;
		outcol = buffer.buf[type];
		if ( outcol->ngeoms == 1 )
		{
			outgeom = outcol->geoms[0];
			outcol->ngeoms=0; lwcollection_free(outcol);
		}
		else
		{
			outgeom = lwcollection_as_lwgeom(outcol);
		}
		outgeom->srid = col->srid;
	}
	/* Bah, more than out type, return anonymous collection */
	else if ( ntypes > 1 )
	{
		int j;
		LWCOLLECTION *outcol;
		outcol = lwcollection_construct_empty(COLLECTIONTYPE, col->srid, FLAGS_GET_Z(col->flags), FLAGS_GET_M(col->flags));
		for ( j = 0; j < NUMTYPES; j++ )
		{
			if ( buffer.buf[j] )
			{
				LWCOLLECTION *bcol = buffer.buf[j];
				if ( bcol->ngeoms == 1 )
				{
					lwcollection_add_lwgeom(outcol, bcol->geoms[0]);
					bcol->ngeoms=0; lwcollection_free(bcol);
				}
				else 
				{
					lwcollection_add_lwgeom(outcol, lwcollection_as_lwgeom(bcol));
				}
			}
		}
		outgeom = lwcollection_as_lwgeom(outcol);
	}

	return outgeom;
}





/*
** Given a generic geometry, return the "simplest" form.
**
** eg:
**     LINESTRING() => LINESTRING()
**
**     MULTILINESTRING(with a single line) => LINESTRING()
**
**     GEOMETRYCOLLECTION(MULTILINESTRING()) => MULTILINESTRING()
**
**     GEOMETRYCOLLECTION(MULTILINESTRING(), MULTILINESTRING(), POINT())
**      => GEOMETRYCOLLECTION(MULTILINESTRING(), POINT())
*/
LWGEOM *
lwgeom_homogenize(const LWGEOM *geom)
{
	LWGEOM *hgeom;

	/* EMPTY Geometry */
	if (lwgeom_is_empty(geom)) 
	{
		if( lwgeom_is_collection(geom) )
		{
			return lwcollection_as_lwgeom(lwcollection_construct_empty(geom->type, geom->srid, lwgeom_has_z(geom), lwgeom_has_m(geom)));
		}
		
		return lwgeom_clone(geom);
	}

	switch (geom->type)
	{

		/* Return simple geometries untouched */
		case POINTTYPE:
		case LINETYPE:
		case CIRCSTRINGTYPE:
		case COMPOUNDTYPE:
		case TRIANGLETYPE:
		case CURVEPOLYTYPE:
		case POLYGONTYPE:
			return lwgeom_clone(geom);

		/* Process homogeneous geometries lightly */
		case MULTIPOINTTYPE:
		case MULTILINETYPE:
		case MULTIPOLYGONTYPE:
		case MULTICURVETYPE:
		case MULTISURFACETYPE:
		case POLYHEDRALSURFACETYPE:
		case TINTYPE:
		{
			LWCOLLECTION *col = (LWCOLLECTION*)geom;

			/* Strip single-entry multi-geometries down to singletons */
			if ( col->ngeoms == 1 )
			{
				hgeom = lwgeom_clone((LWGEOM*)(col->geoms[0]));
				hgeom->srid = geom->srid;
				if (geom->bbox)
					hgeom->bbox = gbox_copy(geom->bbox);
				return hgeom;
			}

			/* Return proper multigeometry untouched */
			return lwgeom_clone(geom);
		}
	
		/* Work on anonymous collections separately */
		case COLLECTIONTYPE: 
			return lwcollection_homogenize((LWCOLLECTION *) geom);
	}

	/* Unknown type */
	lwerror("lwgeom_homogenize: Geometry Type not supported (%i)",
	        lwtype_name(geom->type));

	return NULL; /* Never get here! */
}
