/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "liblwgeom_internal.h"
#include "lwgeom_log.h"
#include "stringbuffer.h"

static void lwgeom_to_wkt_sb(const LWGEOM *geom, stringbuffer_t *sb, int precision, uint8_t variant);


/*
* ISO format uses both Z and M qualifiers.
* Extended format only uses an M qualifier for 3DM variants, where it is not
* clear what the third dimension represents.
* SFSQL format never has more than two dimensions, so no qualifiers.
*/
static void dimension_qualifiers_to_wkt_sb(const LWGEOM *geom, stringbuffer_t *sb, uint8_t variant)
{

	/* Extended WKT: POINTM(0 0 0) */
#if 0
	if ( (variant & WKT_EXTENDED) && ! (variant & WKT_IS_CHILD) && FLAGS_GET_M(geom->flags) && (!FLAGS_GET_Z(geom->flags)) )
#else
	if ( (variant & WKT_EXTENDED) && FLAGS_GET_M(geom->flags) && (!FLAGS_GET_Z(geom->flags)) )
#endif
	{
		stringbuffer_append(sb, "M"); /* "M" */
		return;
	}

	/* ISO WKT: POINT ZM (0 0 0 0) */
	if ( (variant & WKT_ISO) && (FLAGS_NDIMS(geom->flags) > 2) )
	{
		stringbuffer_append(sb, " ");
		if ( FLAGS_GET_Z(geom->flags) )
			stringbuffer_append(sb, "Z");
		if ( FLAGS_GET_M(geom->flags) )
			stringbuffer_append(sb, "M");
		stringbuffer_append(sb, " ");
	}
}

/*
* Write an empty token out, padding with a space if
* necessary. 
*/
static void empty_to_wkt_sb(stringbuffer_t *sb)
{
	if ( ! strchr(" ,(", stringbuffer_lastchar(sb)) ) /* "EMPTY" */
	{ 
		stringbuffer_append(sb, " "); 
	}
	stringbuffer_append(sb, "EMPTY"); 
}

/*
* Point array is a list of coordinates. Depending on output mode,
* we may suppress some dimensions. ISO and Extended formats include
* all dimensions. Standard OGC output only includes X/Y coordinates.
*/
static void ptarray_to_wkt_sb(const POINTARRAY *ptarray, stringbuffer_t *sb, int precision, uint8_t variant)
{
	/* OGC only includes X/Y */
	int dimensions = 2;
	int i, j;

	/* ISO and extended formats include all dimensions */
	if ( variant & ( WKT_ISO | WKT_EXTENDED ) )
		dimensions = FLAGS_NDIMS(ptarray->flags);

	/* Opening paren? */
	if ( ! (variant & WKT_NO_PARENS) )
		stringbuffer_append(sb, "(");

	/* Digits and commas */
	for (i = 0; i < ptarray->npoints; i++)
	{
		double *dbl_ptr = (double*)getPoint_internal(ptarray, i);

		/* Commas before ever coord but the first */
		if ( i > 0 )
			stringbuffer_append(sb, ",");

		for (j = 0; j < dimensions; j++)
		{
			/* Spaces before every ordinate but the first */
			if ( j > 0 )
				stringbuffer_append(sb, " ");
			stringbuffer_aprintf(sb, "%.*g", precision, dbl_ptr[j]);
		}
	}

	/* Closing paren? */
	if ( ! (variant & WKT_NO_PARENS) )
		stringbuffer_append(sb, ")");
}

/*
* A four-dimensional point will have different outputs depending on variant.
*   ISO: POINT ZM (0 0 0 0)
*   Extended: POINT(0 0 0 0)
*   OGC: POINT(0 0)
* A three-dimensional m-point will have different outputs too.
*   ISO: POINT M (0 0 0)
*   Extended: POINTM(0 0 0)
*   OGC: POINT(0 0)
*/
static void lwpoint_to_wkt_sb(const LWPOINT *pt, stringbuffer_t *sb, int precision, uint8_t variant)
{
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "POINT"); /* "POINT" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)pt, sb, variant);
	}

	if ( lwpoint_is_empty(pt) )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	ptarray_to_wkt_sb(pt->point, sb, precision, variant);
}

/*
* LINESTRING(0 0 0, 1 1 1)
*/
static void lwline_to_wkt_sb(const LWLINE *line, stringbuffer_t *sb, int precision, uint8_t variant)
{
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "LINESTRING"); /* "LINESTRING" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)line, sb, variant);
	}
	if ( lwline_is_empty(line) )
	{  
		empty_to_wkt_sb(sb);
		return;
	}

	ptarray_to_wkt_sb(line->points, sb, precision, variant);
}

/*
* POLYGON(0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)
*/
static void lwpoly_to_wkt_sb(const LWPOLY *poly, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "POLYGON"); /* "POLYGON" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)poly, sb, variant);
	}
	if ( lwpoly_is_empty(poly) )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "(");
	for ( i = 0; i < poly->nrings; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		ptarray_to_wkt_sb(poly->rings[i], sb, precision, variant);
	}
	stringbuffer_append(sb, ")");
}

/*
* CIRCULARSTRING
*/
static void lwcircstring_to_wkt_sb(const LWCIRCSTRING *circ, stringbuffer_t *sb, int precision, uint8_t variant)
{
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "CIRCULARSTRING"); /* "CIRCULARSTRING" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)circ, sb, variant);
	}
	if ( lwcircstring_is_empty(circ) )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	ptarray_to_wkt_sb(circ->points, sb, precision, variant);
}


/*
* Multi-points do not wrap their sub-members in parens, unlike other multi-geometries.
*   MULTPOINT(0 0, 1 1) instead of MULTIPOINT((0 0),(1 1))
*/
static void lwmpoint_to_wkt_sb(const LWMPOINT *mpoint, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "MULTIPOINT"); /* "MULTIPOINT" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)mpoint, sb, variant);
	}
	if ( mpoint->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < mpoint->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* We don't want type strings or parens on our subgeoms */
		lwpoint_to_wkt_sb(mpoint->geoms[i], sb, precision, variant | WKT_NO_PARENS | WKT_NO_TYPE );
	}
	stringbuffer_append(sb, ")");
}

/*
* MULTILINESTRING
*/
static void lwmline_to_wkt_sb(const LWMLINE *mline, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "MULTILINESTRING"); /* "MULTILINESTRING" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)mline, sb, variant);
	}
	if ( mline->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < mline->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* We don't want type strings on our subgeoms */
		lwline_to_wkt_sb(mline->geoms[i], sb, precision, variant | WKT_NO_TYPE );
	}
	stringbuffer_append(sb, ")");
}

/*
* MULTIPOLYGON
*/
static void lwmpoly_to_wkt_sb(const LWMPOLY *mpoly, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "MULTIPOLYGON"); /* "MULTIPOLYGON" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)mpoly, sb, variant);
	}
	if ( mpoly->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < mpoly->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* We don't want type strings on our subgeoms */
		lwpoly_to_wkt_sb(mpoly->geoms[i], sb, precision, variant | WKT_NO_TYPE );
	}
	stringbuffer_append(sb, ")");
}

/*
* Compound curves provide type information for their curved sub-geometries
* but not their linestring sub-geometries.
*   COMPOUNDCURVE((0 0, 1 1), CURVESTRING(1 1, 2 2, 3 3))
*/
static void lwcompound_to_wkt_sb(const LWCOMPOUND *comp, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "COMPOUNDCURVE"); /* "COMPOUNDCURVE" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)comp, sb, variant);
	}
	if ( comp->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < comp->ngeoms; i++ )
	{
		int type = comp->geoms[i]->type;
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* Linestring subgeoms don't get type identifiers */
		if ( type == LINETYPE )
		{
			lwline_to_wkt_sb((LWLINE*)comp->geoms[i], sb, precision, variant | WKT_NO_TYPE );
		}
		/* But circstring subgeoms *do* get type identifiers */
		else if ( type == CIRCSTRINGTYPE )
		{
			lwcircstring_to_wkt_sb((LWCIRCSTRING*)comp->geoms[i], sb, precision, variant );
		}
		else
		{
			lwerror("lwcompound_to_wkt_sb: Unknown type recieved %d - %s", type, lwtype_name(type));
		}
	}
	stringbuffer_append(sb, ")");
}

/*
* Curve polygons provide type information for their curved rings
* but not their linestring rings.
*   CURVEPOLYGON((0 0, 1 1, 0 1, 0 0), CURVESTRING(0 0, 1 1, 0 1, 0.5 1, 0 0))
*/
static void lwcurvepoly_to_wkt_sb(const LWCURVEPOLY *cpoly, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "CURVEPOLYGON"); /* "CURVEPOLYGON" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)cpoly, sb, variant);
	}
	if ( cpoly->nrings < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < cpoly->nrings; i++ )
	{
		int type = cpoly->rings[i]->type;
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		switch (type)
		{
		case LINETYPE:
			/* Linestring subgeoms don't get type identifiers */
			lwline_to_wkt_sb((LWLINE*)cpoly->rings[i], sb, precision, variant | WKT_NO_TYPE );
			break;
		case CIRCSTRINGTYPE:
			/* But circstring subgeoms *do* get type identifiers */
			lwcircstring_to_wkt_sb((LWCIRCSTRING*)cpoly->rings[i], sb, precision, variant );
			break;
		case COMPOUNDTYPE:
			/* And compoundcurve subgeoms *do* get type identifiers */
			lwcompound_to_wkt_sb((LWCOMPOUND*)cpoly->rings[i], sb, precision, variant );
			break;
		default:
			lwerror("lwcurvepoly_to_wkt_sb: Unknown type recieved %d - %s", type, lwtype_name(type));
		}
	}
	stringbuffer_append(sb, ")");
}


/*
* Multi-curves provide type information for their curved sub-geometries
* but not their linear sub-geometries.
*   MULTICURVE((0 0, 1 1), CURVESTRING(0 0, 1 1, 2 2))
*/
static void lwmcurve_to_wkt_sb(const LWMCURVE *mcurv, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "MULTICURVE"); /* "MULTICURVE" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)mcurv, sb, variant);
	}
	if ( mcurv->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < mcurv->ngeoms; i++ )
	{
		int type = mcurv->geoms[i]->type;
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		switch (type)
		{
		case LINETYPE:
			/* Linestring subgeoms don't get type identifiers */
			lwline_to_wkt_sb((LWLINE*)mcurv->geoms[i], sb, precision, variant | WKT_NO_TYPE );
			break;
		case CIRCSTRINGTYPE:
			/* But circstring subgeoms *do* get type identifiers */
			lwcircstring_to_wkt_sb((LWCIRCSTRING*)mcurv->geoms[i], sb, precision, variant );
			break;
		case COMPOUNDTYPE:
			/* And compoundcurve subgeoms *do* get type identifiers */
			lwcompound_to_wkt_sb((LWCOMPOUND*)mcurv->geoms[i], sb, precision, variant );
			break;
		default:
			lwerror("lwmcurve_to_wkt_sb: Unknown type recieved %d - %s", type, lwtype_name(type));
		}
	}
	stringbuffer_append(sb, ")");
}


/*
* Multi-surfaces provide type information for their curved sub-geometries
* but not their linear sub-geometries.
*   MULTISURFACE(((0 0, 1 1, 1 0, 0 0)), CURVEPOLYGON(CURVESTRING(0 0, 1 1, 2 2, 0 1, 0 0)))
*/
static void lwmsurface_to_wkt_sb(const LWMSURFACE *msurf, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "MULTISURFACE"); /* "MULTISURFACE" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)msurf, sb, variant);
	}
	if ( msurf->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */
	for ( i = 0; i < msurf->ngeoms; i++ )
	{
		int type = msurf->geoms[i]->type;
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		switch (type)
		{
		case POLYGONTYPE:
			/* Linestring subgeoms don't get type identifiers */
			lwpoly_to_wkt_sb((LWPOLY*)msurf->geoms[i], sb, precision, variant | WKT_NO_TYPE );
			break;
		case CURVEPOLYTYPE:
			/* But circstring subgeoms *do* get type identifiers */
			lwcurvepoly_to_wkt_sb((LWCURVEPOLY*)msurf->geoms[i], sb, precision, variant);
			break;
		default:
			lwerror("lwmsurface_to_wkt_sb: Unknown type recieved %d - %s", type, lwtype_name(type));
		}
	}
	stringbuffer_append(sb, ")");
}

/*
* Geometry collections provide type information for all their curved sub-geometries
* but not their linear sub-geometries.
*   GEOMETRYCOLLECTION(POLYGON((0 0, 1 1, 1 0, 0 0)), CURVEPOLYGON(CURVESTRING(0 0, 1 1, 2 2, 0 1, 0 0)))
*/
static void lwcollection_to_wkt_sb(const LWCOLLECTION *collection, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "GEOMETRYCOLLECTION"); /* "GEOMETRYCOLLECTION" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)collection, sb, variant);
	}
	if ( collection->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}
	stringbuffer_append(sb, "(");
	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are children */
	for ( i = 0; i < collection->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		lwgeom_to_wkt_sb((LWGEOM*)collection->geoms[i], sb, precision, variant );
	}
	stringbuffer_append(sb, ")");
}

/*
* TRIANGLE 
*/
static void lwtriangle_to_wkt_sb(const LWTRIANGLE *tri, stringbuffer_t *sb, int precision, uint8_t variant)
{
	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "TRIANGLE"); /* "TRIANGLE" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)tri, sb, variant);
	}
	if ( lwtriangle_is_empty(tri) )
	{  
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "("); /* Triangles have extraneous brackets */
	ptarray_to_wkt_sb(tri->points, sb, precision, variant);
	stringbuffer_append(sb, ")"); 
}

/*
* TIN
*/
static void lwtin_to_wkt_sb(const LWTIN *tin, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "TIN"); /* "TIN" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)tin, sb, variant);
	}
	if ( tin->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	stringbuffer_append(sb, "(");
	for ( i = 0; i < tin->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* We don't want type strings on our subgeoms */
		lwtriangle_to_wkt_sb(tin->geoms[i], sb, precision, variant | WKT_NO_TYPE );
	}
	stringbuffer_append(sb, ")");
}

/*
* POLYHEDRALSURFACE
*/
static void lwpsurface_to_wkt_sb(const LWPSURFACE *psurf, stringbuffer_t *sb, int precision, uint8_t variant)
{
	int i = 0;

	if ( ! (variant & WKT_NO_TYPE) )
	{
		stringbuffer_append(sb, "POLYHEDRALSURFACE"); /* "POLYHEDRALSURFACE" */
		dimension_qualifiers_to_wkt_sb((LWGEOM*)psurf, sb, variant);
	}
	if ( psurf->ngeoms < 1 )
	{
		empty_to_wkt_sb(sb);
		return;
	}

	variant = variant | WKT_IS_CHILD; /* Inform the sub-geometries they are childre */

	stringbuffer_append(sb, "(");
	for ( i = 0; i < psurf->ngeoms; i++ )
	{
		if ( i > 0 )
			stringbuffer_append(sb, ",");
		/* We don't want type strings on our subgeoms */
		lwpoly_to_wkt_sb(psurf->geoms[i], sb, precision, variant | WKT_NO_TYPE );
	}
	stringbuffer_append(sb, ")");	
}


/*
* Generic GEOMETRY
*/
static void lwgeom_to_wkt_sb(const LWGEOM *geom, stringbuffer_t *sb, int precision, uint8_t variant)
{
	LWDEBUGF(4, "lwgeom_to_wkt_sb: type %s, hasz %d, hasm %d",
		lwtype_name(geom->type), (geom->type),
		FLAGS_GET_Z(geom->flags)?1:0, FLAGS_GET_M(geom->flags)?1:0);

	switch (geom->type)
	{
	case POINTTYPE:
		lwpoint_to_wkt_sb((LWPOINT*)geom, sb, precision, variant);
		break;
	case LINETYPE:
		lwline_to_wkt_sb((LWLINE*)geom, sb, precision, variant);
		break;
	case POLYGONTYPE:
		lwpoly_to_wkt_sb((LWPOLY*)geom, sb, precision, variant);
		break;
	case MULTIPOINTTYPE:
		lwmpoint_to_wkt_sb((LWMPOINT*)geom, sb, precision, variant);
		break;
	case MULTILINETYPE:
		lwmline_to_wkt_sb((LWMLINE*)geom, sb, precision, variant);
		break;
	case MULTIPOLYGONTYPE:
		lwmpoly_to_wkt_sb((LWMPOLY*)geom, sb, precision, variant);
		break;
	case COLLECTIONTYPE:
		lwcollection_to_wkt_sb((LWCOLLECTION*)geom, sb, precision, variant);
		break;
	case CIRCSTRINGTYPE:
		lwcircstring_to_wkt_sb((LWCIRCSTRING*)geom, sb, precision, variant);
		break;
	case COMPOUNDTYPE:
		lwcompound_to_wkt_sb((LWCOMPOUND*)geom, sb, precision, variant);
		break;
	case CURVEPOLYTYPE:
		lwcurvepoly_to_wkt_sb((LWCURVEPOLY*)geom, sb, precision, variant);
		break;
	case MULTICURVETYPE:
		lwmcurve_to_wkt_sb((LWMCURVE*)geom, sb, precision, variant);
		break;
	case MULTISURFACETYPE:
		lwmsurface_to_wkt_sb((LWMSURFACE*)geom, sb, precision, variant);
		break;
	case TRIANGLETYPE:
		lwtriangle_to_wkt_sb((LWTRIANGLE*)geom, sb, precision, variant);
		break;
	case TINTYPE:
		lwtin_to_wkt_sb((LWTIN*)geom, sb, precision, variant);
		break;
	case POLYHEDRALSURFACETYPE:
		lwpsurface_to_wkt_sb((LWPSURFACE*)geom, sb, precision, variant);
		break;
	default:
		lwerror("lwgeom_to_wkt_sb: Type %d - %s unsupported.",
		        geom->type, lwtype_name(geom->type));
	}
}

/**
* WKT emitter function. Allocates a new *char and fills it with the WKT
* representation. If size_out is not NULL, it will be set to the size of the
* allocated *char.
*
* @param variant Bitmasked value, accepts one of WKT_ISO, WKT_SFSQL, WKT_EXTENDED.
* @param precision Number of significant digits in the output doubles.
* @param size_out If supplied, will return the size of the returned string,
* including the null terminator.
*/
char* lwgeom_to_wkt(const LWGEOM *geom, uint8_t variant, int precision, size_t *size_out)
{
	stringbuffer_t *sb;
	char *str = NULL;
	if ( geom == NULL )
		return NULL;
	sb = stringbuffer_create();
	/* Extended mode starts with an "SRID=" section for geoms that have one */
	if ( (variant & WKT_EXTENDED) && lwgeom_has_srid(geom) )
	{
		stringbuffer_aprintf(sb, "SRID=%d;", geom->srid);
	}
	lwgeom_to_wkt_sb(geom, sb, precision, variant);
	if ( stringbuffer_getstring(sb) == NULL )
	{
		lwerror("Uh oh");
		return NULL;
	}
	str = stringbuffer_getstringcopy(sb);
	if ( size_out )
		*size_out = stringbuffer_getlength(sb) + 1;
	stringbuffer_destroy(sb);
	return str;
}

