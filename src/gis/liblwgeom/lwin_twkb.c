/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2014 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <math.h>
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"
#include "varint.h"

#define TWKB_IN_MAXCOORDS 4

/**
* Used for passing the parse state between the parsing functions.
*/
typedef struct
{
	/* Pointers to the bytes */
	uint8_t *twkb; /* Points to start of TWKB */
	uint8_t *twkb_end; /* Points to end of TWKB */
	uint8_t *pos; /* Current read position */

	uint32_t check; /* Simple validity checks on geometries */
	uint32_t lwtype; /* Current type we are handling */

	uint8_t has_bbox;
	uint8_t has_size;
	uint8_t has_idlist;
	uint8_t has_z;
	uint8_t has_m;
	uint8_t is_empty;

	/* Precision factors to convert ints to double */
	double factor;
	double factor_z;
	double factor_m;

	uint64_t size;

	/* Info about current geometry */
	uint8_t magic_byte; /* the magic byte contain info about if twkb contain id, size info, bboxes and precision */

	int ndims; /* Number of dimensions */

	int64_t *coords; /* An array to keep delta values from 4 dimensions */

} twkb_parse_state;


/**
* Internal function declarations.
*/
LWGEOM* lwgeom_from_twkb_state(twkb_parse_state *s);


/**********************************************************************/

/**
* Check that we are not about to read off the end of the WKB
* array.
*/
static inline void twkb_parse_state_advance(twkb_parse_state *s, size_t next)
{
	if( (s->pos + next) > s->twkb_end)
	{
		lwerror("%s: TWKB structure does not match expected size!", __func__);
		// lwnotice("TWKB structure does not match expected size!");
	}

	s->pos += next;
}

static inline int64_t twkb_parse_state_varint(twkb_parse_state *s)
{
	size_t size;
	int64_t val = varint_s64_decode(s->pos, s->twkb_end, &size);
	twkb_parse_state_advance(s, size);
	return val;
}

static inline uint64_t twkb_parse_state_uvarint(twkb_parse_state *s)
{
	size_t size;
	uint64_t val = varint_u64_decode(s->pos, s->twkb_end, &size);
	twkb_parse_state_advance(s, size);
	return val;
}

static inline double twkb_parse_state_double(twkb_parse_state *s, double factor)
{
	size_t size;
	int64_t val = varint_s64_decode(s->pos, s->twkb_end, &size);
	twkb_parse_state_advance(s, size);
	return val / factor;
}

static inline void twkb_parse_state_varint_skip(twkb_parse_state *s)
{
	size_t size = varint_size(s->pos, s->twkb_end);

	if ( ! size )
		lwerror("%s: no varint to skip", __func__);

	twkb_parse_state_advance(s, size);
	return;
}



static uint32_t lwtype_from_twkb_type(uint8_t twkb_type)
{
	switch (twkb_type)
	{
		case 1:
			return POINTTYPE;
		case 2:
			return LINETYPE;
		case 3:
			return POLYGONTYPE;
		case 4:
			return MULTIPOINTTYPE;
		case 5:
			return MULTILINETYPE;
		case 6:
			return MULTIPOLYGONTYPE;
		case 7:
			return COLLECTIONTYPE;

		default: /* Error! */
			lwerror("Unknown WKB type");
			return 0;
	}
	return 0;
}

/**
* Byte
* Read a byte and advance the parse state forward.
*/
static uint8_t byte_from_twkb_state(twkb_parse_state *s)
{
	uint8_t val = *(s->pos);
	twkb_parse_state_advance(s, WKB_BYTE_SIZE);
	return val;
}


/**
* POINTARRAY
* Read a dynamically sized point array and advance the parse state forward.
*/
static POINTARRAY* ptarray_from_twkb_state(twkb_parse_state *s, uint32_t npoints)
{
	POINTARRAY *pa = NULL;
	uint32_t ndims = s->ndims;
	int i;
	double *dlist;

	LWDEBUG(2,"Entering ptarray_from_twkb_state");
	LWDEBUGF(4,"Pointarray has %d points", npoints);

	/* Empty! */
	if( npoints == 0 )
		return ptarray_construct_empty(s->has_z, s->has_m, 0);

	pa = ptarray_construct(s->has_z, s->has_m, npoints);
	dlist = (double*)(pa->serialized_pointlist);
	for( i = 0; i < npoints; i++ )
	{
		int j = 0;
		/* X */
		s->coords[j] += twkb_parse_state_varint(s);
		dlist[ndims*i + j] = s->coords[j] / s->factor;
		j++;
		/* Y */
		s->coords[j] += twkb_parse_state_varint(s);
		dlist[ndims*i + j] = s->coords[j] / s->factor;
		j++;
		/* Z */
		if ( s->has_z )
		{
			s->coords[j] += twkb_parse_state_varint(s);
			dlist[ndims*i + j] = s->coords[j] / s->factor_z;
			j++;
		}
		/* M */
		if ( s->has_m )
		{
			s->coords[j] += twkb_parse_state_varint(s);
			dlist[ndims*i + j] = s->coords[j] / s->factor_m;
			j++;
		}
	}

	return pa;
}

/**
* POINT
*/
static LWPOINT* lwpoint_from_twkb_state(twkb_parse_state *s)
{
	static uint32_t npoints = 1;
	POINTARRAY *pa;

	LWDEBUG(2,"Entering lwpoint_from_twkb_state");

	if ( s->is_empty )
		return lwpoint_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	pa = ptarray_from_twkb_state(s, npoints);
	return lwpoint_construct(SRID_UNKNOWN, NULL, pa);
}

/**
* LINESTRING
*/
static LWLINE* lwline_from_twkb_state(twkb_parse_state *s)
{
	uint32_t npoints;
	POINTARRAY *pa;

	LWDEBUG(2,"Entering lwline_from_twkb_state");

	if ( s->is_empty )
		return lwline_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	/* Read number of points */
	npoints = twkb_parse_state_uvarint(s);

	if ( npoints == 0 )
		return lwline_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	/* Read coordinates */
	pa = ptarray_from_twkb_state(s, npoints);

	if( pa == NULL )
		return lwline_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	if( s->check & LW_PARSER_CHECK_MINPOINTS && pa->npoints < 2 )
	{
		lwerror("%s must have at least two points", lwtype_name(s->lwtype));
		return NULL;
	}

	return lwline_construct(SRID_UNKNOWN, NULL, pa);
}

/**
* POLYGON
*/
static LWPOLY* lwpoly_from_twkb_state(twkb_parse_state *s)
{
	uint32_t nrings;
	int i;
	LWPOLY *poly;

	LWDEBUG(2,"Entering lwpoly_from_twkb_state");

	if ( s->is_empty )
		return lwpoly_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	/* Read number of rings */
	nrings = twkb_parse_state_uvarint(s);

	/* Start w/ empty polygon */
	poly = lwpoly_construct_empty(SRID_UNKNOWN, s->has_z, s->has_m);

	LWDEBUGF(4,"Polygon has %d rings", nrings);

	/* Empty polygon? */
	if( nrings == 0 )
		return poly;

	for( i = 0; i < nrings; i++ )
	{
		/* Ret number of points */
		uint32_t npoints = twkb_parse_state_uvarint(s);
		POINTARRAY *pa = ptarray_from_twkb_state(s, npoints);

		/* Skip empty rings */
		if( pa == NULL )
			continue;

		/* Force first and last points to be the same. */
		if( ! ptarray_is_closed_2d(pa) )
		{
			POINT4D pt;
			getPoint4d_p(pa, 0, &pt);
			ptarray_append_point(pa, &pt, LW_FALSE);
		}

		/* Check for at least four points. */
		if( s->check & LW_PARSER_CHECK_MINPOINTS && pa->npoints < 4 )
		{
			LWDEBUGF(2, "%s must have at least four points in each ring", lwtype_name(s->lwtype));
			lwerror("%s must have at least four points in each ring", lwtype_name(s->lwtype));
			return NULL;
		}

		/* Add ring to polygon */
		if ( lwpoly_add_ring(poly, pa) == LW_FAILURE )
		{
			LWDEBUG(2, "Unable to add ring to polygon");
			lwerror("Unable to add ring to polygon");
		}

	}
	return poly;
}


/**
* MULTIPOINT
*/
static LWCOLLECTION* lwmultipoint_from_twkb_state(twkb_parse_state *s)
{
	int ngeoms, i;
	LWGEOM *geom = NULL;
	LWCOLLECTION *col = lwcollection_construct_empty(s->lwtype, SRID_UNKNOWN, s->has_z, s->has_m);

	LWDEBUG(2,"Entering lwmultipoint_from_twkb_state");

	if ( s->is_empty )
		return col;

	/* Read number of geometries */
	ngeoms = twkb_parse_state_uvarint(s);
	LWDEBUGF(4,"Number of geometries %d", ngeoms);

	/* It has an idlist, we need to skip that */
	if ( s->has_idlist )
	{
		for ( i = 0; i < ngeoms; i++ )
			twkb_parse_state_varint_skip(s);
	}

	for ( i = 0; i < ngeoms; i++ )
	{
		geom = lwpoint_as_lwgeom(lwpoint_from_twkb_state(s));
		if ( lwcollection_add_lwgeom(col, geom) == NULL )
		{
			lwerror("Unable to add geometry (%p) to collection (%p)", geom, col);
			return NULL;
		}
	}

	return col;
}

/**
* MULTILINESTRING
*/
static LWCOLLECTION* lwmultiline_from_twkb_state(twkb_parse_state *s)
{
	int ngeoms, i;
	LWGEOM *geom = NULL;
	LWCOLLECTION *col = lwcollection_construct_empty(s->lwtype, SRID_UNKNOWN, s->has_z, s->has_m);

	LWDEBUG(2,"Entering lwmultilinestring_from_twkb_state");

	if ( s->is_empty )
		return col;

	/* Read number of geometries */
	ngeoms = twkb_parse_state_uvarint(s);

	LWDEBUGF(4,"Number of geometries %d",ngeoms);

	/* It has an idlist, we need to skip that */
	if ( s->has_idlist )
	{
		for ( i = 0; i < ngeoms; i++ )
			twkb_parse_state_varint_skip(s);
	}

	for ( i = 0; i < ngeoms; i++ )
	{
		geom = lwline_as_lwgeom(lwline_from_twkb_state(s));
		if ( lwcollection_add_lwgeom(col, geom) == NULL )
		{
			lwerror("Unable to add geometry (%p) to collection (%p)", geom, col);
			return NULL;
		}
	}

	return col;
}

/**
* MULTIPOLYGON
*/
static LWCOLLECTION* lwmultipoly_from_twkb_state(twkb_parse_state *s)
{
	int ngeoms, i;
	LWGEOM *geom = NULL;
	LWCOLLECTION *col = lwcollection_construct_empty(s->lwtype, SRID_UNKNOWN, s->has_z, s->has_m);

	LWDEBUG(2,"Entering lwmultipolygon_from_twkb_state");

	if ( s->is_empty )
		return col;

	/* Read number of geometries */
	ngeoms = twkb_parse_state_uvarint(s);
	LWDEBUGF(4,"Number of geometries %d",ngeoms);

	/* It has an idlist, we need to skip that */
	if ( s->has_idlist )
	{
		for ( i = 0; i < ngeoms; i++ )
			twkb_parse_state_varint_skip(s);
	}

	for ( i = 0; i < ngeoms; i++ )
	{
		geom = lwpoly_as_lwgeom(lwpoly_from_twkb_state(s));
		if ( lwcollection_add_lwgeom(col, geom) == NULL )
		{
			lwerror("Unable to add geometry (%p) to collection (%p)", geom, col);
			return NULL;
		}
	}

	return col;
}


/**
* COLLECTION, MULTIPOINTTYPE, MULTILINETYPE, MULTIPOLYGONTYPE
**/
static LWCOLLECTION* lwcollection_from_twkb_state(twkb_parse_state *s)
{
	int ngeoms, i;
	LWGEOM *geom = NULL;
	LWCOLLECTION *col = lwcollection_construct_empty(s->lwtype, SRID_UNKNOWN, s->has_z, s->has_m);

	LWDEBUG(2,"Entering lwcollection_from_twkb_state");

	if ( s->is_empty )
		return col;

	/* Read number of geometries */
	ngeoms = twkb_parse_state_uvarint(s);

	LWDEBUGF(4,"Number of geometries %d",ngeoms);

	/* It has an idlist, we need to skip that */
	if ( s->has_idlist )
	{
		for ( i = 0; i < ngeoms; i++ )
			twkb_parse_state_varint_skip(s);
	}

	for ( i = 0; i < ngeoms; i++ )
	{
		geom = lwgeom_from_twkb_state(s);
		if ( lwcollection_add_lwgeom(col, geom) == NULL )
		{
			lwerror("Unable to add geometry (%p) to collection (%p)", geom, col);
			return NULL;
		}
	}


	return col;
}


static void header_from_twkb_state(twkb_parse_state *s)
{
	LWDEBUG(2,"Entering magicbyte_from_twkb_state");

	uint8_t extended_dims;

	/* Read the first two bytes */
	uint8_t type_precision = byte_from_twkb_state(s);
	uint8_t metadata = byte_from_twkb_state(s);

	/* Strip type and precision out of first byte */
	uint8_t type = type_precision & 0x0F;
	int8_t precision = unzigzag8((type_precision & 0xF0) >> 4);

	/* Convert TWKB type to internal type */
	s->lwtype = lwtype_from_twkb_type(type);

	/* Convert the precision into factor */
	s->factor = pow(10, (double)precision);

	/* Strip metadata flags out of second byte */
	s->has_bbox   =  metadata & 0x01;
	s->has_size   = (metadata & 0x02) >> 1;
	s->has_idlist = (metadata & 0x04) >> 2;
	extended_dims = (metadata & 0x08) >> 3;
	s->is_empty   = (metadata & 0x10) >> 4;

	/* Flag for higher dims means read a third byte */
	if ( extended_dims )
	{
		int8_t precision_z, precision_m;

		extended_dims = byte_from_twkb_state(s);

		/* Strip Z/M presence and precision from ext byte */
		s->has_z    = (extended_dims & 0x01);
		s->has_m    = (extended_dims & 0x02) >> 1;
		precision_z = (extended_dims & 0x1C) >> 2;
		precision_m = (extended_dims & 0xE0) >> 5;

		/* Convert the precision into factor */
		s->factor_z = pow(10, (double)precision_z);
		s->factor_m = pow(10, (double)precision_m);
	}
	else
	{
		s->has_z = 0;
		s->has_m = 0;
		s->factor_z = 0;
		s->factor_m = 0;
	}

	/* Read the size, if there is one */
	if ( s->has_size )
	{
		s->size = twkb_parse_state_uvarint(s);
	}

	/* Calculate the number of dimensions */
	s->ndims = 2 + s->has_z + s->has_m;

	return;
}



/**
* Generic handling for TWKB geometries. The front of every TWKB geometry
* (including those embedded in collections) is a type byte and metadata byte,
* then optional size, bbox, etc. Read those, then switch to particular type
* handling code.
*/
LWGEOM* lwgeom_from_twkb_state(twkb_parse_state *s)
{
	GBOX bbox;
	LWGEOM *geom = NULL;
	uint32_t has_bbox = LW_FALSE;
	int i;

	/* Read the first two bytes, and optional */
	/* extended precision info and optional size info */
	header_from_twkb_state(s);

	/* Just experienced a geometry header, so now we */
	/* need to reset our coordinate deltas */
	for ( i = 0; i < TWKB_IN_MAXCOORDS; i++ )
	{
		s->coords[i] = 0.0;
	}

	/* Read the bounding box, is there is one */
	if ( s->has_bbox )
	{
		/* Initialize */
		has_bbox = s->has_bbox;
		memset(&bbox, 0, sizeof(GBOX));
		bbox.flags = gflags(s->has_z, s->has_m, 0);

		/* X */
		bbox.xmin = twkb_parse_state_double(s, s->factor);
		bbox.xmax = bbox.xmin + twkb_parse_state_double(s, s->factor);
		/* Y */
		bbox.ymin = twkb_parse_state_double(s, s->factor);
		bbox.ymax = bbox.ymin + twkb_parse_state_double(s, s->factor);
		/* Z */
		if ( s->has_z )
		{
			bbox.zmin = twkb_parse_state_double(s, s->factor_z);
			bbox.zmax = bbox.zmin + twkb_parse_state_double(s, s->factor_z);
		}
		/* M */
		if ( s->has_z )
		{
			bbox.mmin = twkb_parse_state_double(s, s->factor_m);
			bbox.mmax = bbox.mmin + twkb_parse_state_double(s, s->factor_m);
		}
	}

	/* Switch to code for the particular type we're dealing with */
	switch( s->lwtype )
	{
		case POINTTYPE:
			geom = lwpoint_as_lwgeom(lwpoint_from_twkb_state(s));
			break;
		case LINETYPE:
			geom = lwline_as_lwgeom(lwline_from_twkb_state(s));
			break;
		case POLYGONTYPE:
			geom = lwpoly_as_lwgeom(lwpoly_from_twkb_state(s));
			break;
		case MULTIPOINTTYPE:
			geom = lwcollection_as_lwgeom(lwmultipoint_from_twkb_state(s));
			break;
		case MULTILINETYPE:
			geom = lwcollection_as_lwgeom(lwmultiline_from_twkb_state(s));
			break;
		case MULTIPOLYGONTYPE:
			geom = lwcollection_as_lwgeom(lwmultipoly_from_twkb_state(s));
			break;
		case COLLECTIONTYPE:
			geom = lwcollection_as_lwgeom(lwcollection_from_twkb_state(s));
			break;
		/* Unknown type! */
		default:
			lwerror("Unsupported geometry type: %s [%d]", lwtype_name(s->lwtype), s->lwtype);
			break;
	}

	if ( has_bbox )
	{
		geom->bbox = gbox_clone(&bbox);
	}

	return geom;
}


/**
* WKB inputs *must* have a declared size, to prevent malformed WKB from reading
* off the end of the memory segment (this stops a malevolent user from declaring
* a one-ring polygon to have 10 rings, causing the WKB reader to walk off the
* end of the memory).
*
* Check is a bitmask of: LW_PARSER_CHECK_MINPOINTS, LW_PARSER_CHECK_ODD,
* LW_PARSER_CHECK_CLOSURE, LW_PARSER_CHECK_NONE, LW_PARSER_CHECK_ALL
*/
LWGEOM* lwgeom_from_twkb(uint8_t *twkb, size_t twkb_size, char check)
{
	int64_t coords[TWKB_IN_MAXCOORDS] = {0, 0, 0, 0};
	twkb_parse_state s;

	LWDEBUG(2,"Entering lwgeom_from_twkb");
	LWDEBUGF(4,"twkb_size: %d",(int) twkb_size);

	/* Zero out the state */
	memset(&s, 0, sizeof(twkb_parse_state));

	/* Initialize the state appropriately */
	s.twkb = s.pos = twkb;
	s.twkb_end = twkb + twkb_size;
	s.check = check;
	s.coords = coords;

	/* Handle the check catch-all values */
	if ( check & LW_PARSER_CHECK_NONE )
		s.check = 0;
	else
		s.check = check;


	/* Read the rest of the geometry */
	return lwgeom_from_twkb_state(&s);
}
