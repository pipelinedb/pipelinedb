/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "liblwgeom_internal.h"
#include "lwgeom_log.h"

/***********************************************************************
* GSERIALIZED metadata utility functions.
*/

int gserialized_has_bbox(const GSERIALIZED *gser)
{
	return FLAGS_GET_BBOX(gser->flags);
}

int gserialized_has_z(const GSERIALIZED *gser)
{
	return FLAGS_GET_Z(gser->flags);
}

int gserialized_has_m(const GSERIALIZED *gser)
{
	return FLAGS_GET_M(gser->flags);
}

int gserialized_get_zm(const GSERIALIZED *gser)
{
	return 2 * FLAGS_GET_Z(gser->flags) + FLAGS_GET_M(gser->flags);
}

int gserialized_ndims(const GSERIALIZED *gser)
{
	return FLAGS_NDIMS(gser->flags);
}

int gserialized_is_geodetic(const GSERIALIZED *gser)
{
	  return FLAGS_GET_GEODETIC(gser->flags);
}
 
uint32_t gserialized_max_header_size(void) 
{
	/* read GSERIALIZED size + max bbox according gbox_serialized_size (2 + Z + M) + 1 int for type */
	return sizeof(GSERIALIZED) + 8 * sizeof(float) + sizeof(int);
}

uint32_t gserialized_get_type(const GSERIALIZED *s)
{
	uint32_t *ptr;
	assert(s);
	ptr = (uint32_t*)(s->data);
	LWDEBUG(4,"entered");
	if ( FLAGS_GET_BBOX(s->flags) )
	{
		LWDEBUGF(4,"skipping forward past bbox (%d bytes)",gbox_serialized_size(s->flags));
		ptr += (gbox_serialized_size(s->flags) / sizeof(uint32_t));
	}
	return *ptr;
}

int32_t gserialized_get_srid(const GSERIALIZED *s)
{
	int32_t srid = 0;
	srid = srid | (s->srid[0] << 16);
	srid = srid | (s->srid[1] << 8);
	srid = srid | s->srid[2];
	/* Only the first 21 bits are set. Slide up and back to pull
	   the negative bits down, if we need them. */
	srid = (srid<<11)>>11;
	
	/* 0 is our internal unknown value. We'll map back and forth here for now */
	if ( srid == 0 ) 
		return SRID_UNKNOWN;
	else
		return clamp_srid(srid);
}

void gserialized_set_srid(GSERIALIZED *s, int32_t srid)
{
	LWDEBUGF(3, "Called with srid = %d", srid);

	srid = clamp_srid(srid);

	/* 0 is our internal unknown value.
	 * We'll map back and forth here for now */
	if ( srid == SRID_UNKNOWN )
		srid = 0;
		
	s->srid[0] = (srid & 0x001F0000) >> 16;
	s->srid[1] = (srid & 0x0000FF00) >> 8;
	s->srid[2] = (srid & 0x000000FF);
}

GSERIALIZED* gserialized_copy(const GSERIALIZED *g)
{
	GSERIALIZED *g_out = NULL;
	assert(g);
	g_out = (GSERIALIZED*)lwalloc(SIZE_GET(g->size));
	memcpy((uint8_t*)g_out,(uint8_t*)g,SIZE_GET(g->size));
	return g_out;
}

int gserialized_is_empty(const GSERIALIZED *g)
{
	uint8_t *p = (uint8_t*)g;
	int i;
	assert(g);

	p += 8; /* Skip varhdr and srid/flags */
	if( FLAGS_GET_BBOX(g->flags) )
		p += gbox_serialized_size(g->flags); /* Skip the box */
	p += 4; /* Skip type number */
	
	/* For point/line/circstring this is npoints */
	/* For polygons this is nrings */
	/* For collections this is ngeoms */
	memcpy(&i, p, sizeof(int));
	
	/* If it is non-zero, it's not empty */
	if ( i > 0 )
		return LW_FALSE;
	else
		return LW_TRUE;	
}

char* gserialized_to_string(const GSERIALIZED *g)
{
	return lwgeom_to_wkt(lwgeom_from_gserialized(g), WKT_ISO, 12, 0);
}

int gserialized_read_gbox_p(const GSERIALIZED *g, GBOX *gbox)
{

	/* Null input! */
	if ( ! ( g && gbox ) ) return LW_FAILURE;

	/* Initialize the flags on the box */
	gbox->flags = g->flags;

	/* Has pre-calculated box */
	if ( FLAGS_GET_BBOX(g->flags) )
	{
		int i = 0;
		float *fbox = (float*)(g->data);
		gbox->xmin = fbox[i++];
		gbox->xmax = fbox[i++];
		gbox->ymin = fbox[i++];
		gbox->ymax = fbox[i++];

		/* Geodetic? Read next dimension (geocentric Z) and return */
		if ( FLAGS_GET_GEODETIC(g->flags) )
		{
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
			return LW_SUCCESS;
		}
		/* Cartesian? Read extra dimensions (if there) and return */
		if ( FLAGS_GET_Z(g->flags) )
		{
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
		}
		if ( FLAGS_GET_M(g->flags) )
		{
			gbox->mmin = fbox[i++];
			gbox->mmax = fbox[i++];
		}
		return LW_SUCCESS;
	}

	return LW_FAILURE;
}

/*
* Populate a bounding box *without* allocating an LWGEOM. Useful
* for some performance purposes.
*/
static int gserialized_peek_gbox_p(const GSERIALIZED *g, GBOX *gbox)
{
	uint32_t type = gserialized_get_type(g);

	/* Peeking doesn't help if you already have a box or are geodetic */
	if ( FLAGS_GET_GEODETIC(g->flags) || FLAGS_GET_BBOX(g->flags) )
	{
		return LW_FAILURE;
	}
	
	/* Boxes of points are easy peasy */
	if ( type == POINTTYPE )
	{
		int i = 1; /* Start past <pointtype><padding> */
		double *dptr = (double*)(g->data);

		/* Read the empty flag */
		int *iptr = (int*)(g->data);
		int isempty = (iptr[1] == 0);

		/* EMPTY point has no box */
		if ( isempty ) return LW_FAILURE;

		gbox->xmin = gbox->xmax = dptr[i++];
		gbox->ymin = gbox->ymax = dptr[i++];
		if ( FLAGS_GET_Z(g->flags) )
		{
			gbox->zmin = gbox->zmax = dptr[i++];
		}
		if ( FLAGS_GET_M(g->flags) )
		{
			gbox->mmin = gbox->mmax = dptr[i++];
		}
		gbox_float_round(gbox);
		return LW_SUCCESS;
	}
	/* We can calculate the box of a two-point cartesian line trivially */
	else if ( type == LINETYPE )
	{
		int ndims = FLAGS_NDIMS(g->flags);
		int i = 0; /* Start at <linetype><npoints> */
		double *dptr = (double*)(g->data);
		int *iptr = (int*)(g->data);
		int npoints = iptr[1]; /* Read the npoints */
	
		/* This only works with 2-point lines */
		if ( npoints != 2 )
			return LW_FAILURE;
		
		/* Advance to X */
		/* Past <linetype><npoints> */
		i++;
		gbox->xmin = FP_MIN(dptr[i], dptr[i+ndims]);
		gbox->xmax = FP_MAX(dptr[i], dptr[i+ndims]);
	
		/* Advance to Y */
		i++;
		gbox->ymin = FP_MIN(dptr[i], dptr[i+ndims]);
		gbox->ymax = FP_MAX(dptr[i], dptr[i+ndims]);
	
		if ( FLAGS_GET_Z(g->flags) )
		{
			/* Advance to Z */
			i++;
			gbox->zmin = FP_MIN(dptr[i], dptr[i+ndims]);
			gbox->zmax = FP_MAX(dptr[i], dptr[i+ndims]);
		}
		if ( FLAGS_GET_M(g->flags) )
		{
			/* Advance to M */
			i++;
			gbox->mmin = FP_MIN(dptr[i], dptr[i+ndims]);
			gbox->mmax = FP_MAX(dptr[i], dptr[i+ndims]);
		}
		gbox_float_round(gbox);
		return LW_SUCCESS;
	}
	/* We can also do single-entry multi-points */
	else if ( type == MULTIPOINTTYPE )
	{
		int i = 0; /* Start at <multipointtype><ngeoms> */
		double *dptr = (double*)(g->data);
		int *iptr = (int*)(g->data);
		int ngeoms = iptr[1]; /* Read the ngeoms */
	
		/* This only works with single-entry multipoints */
		if ( ngeoms != 1 )
			return LW_FAILURE;

		/* Move forward two doubles (four ints) */
		/* Past <multipointtype><ngeoms> */
		/* Past <pointtype><emtpyflat> */
		i += 2;

		/* Read the doubles from the one point */
		gbox->xmin = gbox->xmax = dptr[i++];
		gbox->ymin = gbox->ymax = dptr[i++];
		if ( FLAGS_GET_Z(g->flags) )
		{
			gbox->zmin = gbox->zmax = dptr[i++];
		}
		if ( FLAGS_GET_M(g->flags) )
		{
			gbox->mmin = gbox->mmax = dptr[i++];
		}
		gbox_float_round(gbox);
		return LW_SUCCESS;
	}
	/* And we can do single-entry multi-lines with two vertices (!!!) */
	else if ( type == MULTILINETYPE )
	{
		int ndims = FLAGS_NDIMS(g->flags);
		int i = 0; /* Start at <multilinetype><ngeoms> */
		double *dptr = (double*)(g->data);
		int *iptr = (int*)(g->data);
		int ngeoms = iptr[1]; /* Read the ngeoms */
		int npoints;
	
		/* This only works with 1-line multilines */
		if ( ngeoms != 1 )
			return LW_FAILURE;

		/* Npoints is at <multilinetype><ngeoms><linetype><npoints> */
		npoints = iptr[3];

		if ( npoints != 2 )
			return LW_FAILURE;

		/* Advance to X */
		/* Move forward two doubles (four ints) */
		/* Past <multilinetype><ngeoms> */
		/* Past <linetype><npoints> */
		i += 2;
		gbox->xmin = FP_MIN(dptr[i], dptr[i+ndims]);
		gbox->xmax = FP_MAX(dptr[i], dptr[i+ndims]);
	
		/* Advance to Y */
		i++;
		gbox->ymin = FP_MIN(dptr[i], dptr[i+ndims]);
		gbox->ymax = FP_MAX(dptr[i], dptr[i+ndims]);
	
		if ( FLAGS_GET_Z(g->flags) )
		{
			/* Advance to Z */
			i++;
			gbox->zmin = FP_MIN(dptr[i], dptr[i+ndims]);
			gbox->zmax = FP_MAX(dptr[i], dptr[i+ndims]);
		}
		if ( FLAGS_GET_M(g->flags) )
		{
			/* Advance to M */
			i++;
			gbox->mmin = FP_MIN(dptr[i], dptr[i+ndims]);
			gbox->mmax = FP_MAX(dptr[i], dptr[i+ndims]);
		}
		gbox_float_round(gbox);
		return LW_SUCCESS;
	}
	
	return LW_FAILURE;
}

/**
* Read the bounding box off a serialization and calculate one if
* it is not already there.
*/
int gserialized_get_gbox_p(const GSERIALIZED *g, GBOX *box)
{
	/* Try to just read the serialized box. */
	if ( gserialized_read_gbox_p(g, box) == LW_SUCCESS )
	{
		return LW_SUCCESS;
	}
	/* No box? Try to peek into simpler geometries and */
	/* derive a box without creating an lwgeom */
	else if ( gserialized_peek_gbox_p(g, box) == LW_SUCCESS )
	{
		return LW_SUCCESS;
	}
	/* Damn! Nothing for it but to create an lwgeom... */
	/* See http://trac.osgeo.org/postgis/ticket/1023 */
	else
	{
		LWGEOM *lwgeom = lwgeom_from_gserialized(g);
		int ret = lwgeom_calculate_gbox(lwgeom, box);
		gbox_float_round(box);
		lwgeom_free(lwgeom);
		return ret;
	}
}


/***********************************************************************
* Calculate the GSERIALIZED size for an LWGEOM.
*/

/* Private functions */

static size_t gserialized_from_any_size(const LWGEOM *geom); /* Local prototype */

static size_t gserialized_from_lwpoint_size(const LWPOINT *point)
{
	size_t size = 4; /* Type number. */

	assert(point);

	size += 4; /* Number of points (one or zero (empty)). */
	size += point->point->npoints * FLAGS_NDIMS(point->flags) * sizeof(double);

	LWDEBUGF(3, "point size = %d", size);

	return size;
}

static size_t gserialized_from_lwline_size(const LWLINE *line)
{
	size_t size = 4; /* Type number. */

	assert(line);

	size += 4; /* Number of points (zero => empty). */
	size += line->points->npoints * FLAGS_NDIMS(line->flags) * sizeof(double);

	LWDEBUGF(3, "linestring size = %d", size);

	return size;
}

static size_t gserialized_from_lwtriangle_size(const LWTRIANGLE *triangle)
{
	size_t size = 4; /* Type number. */

	assert(triangle);

	size += 4; /* Number of points (zero => empty). */
	size += triangle->points->npoints * FLAGS_NDIMS(triangle->flags) * sizeof(double);

	LWDEBUGF(3, "triangle size = %d", size);

	return size;
}

static size_t gserialized_from_lwpoly_size(const LWPOLY *poly)
{
	size_t size = 4; /* Type number. */
	int i = 0;

	assert(poly);

	size += 4; /* Number of rings (zero => empty). */
	if ( poly->nrings % 2 )
		size += 4; /* Padding to double alignment. */

	for ( i = 0; i < poly->nrings; i++ )
	{
		size += 4; /* Number of points in ring. */
		size += poly->rings[i]->npoints * FLAGS_NDIMS(poly->flags) * sizeof(double);
	}

	LWDEBUGF(3, "polygon size = %d", size);

	return size;
}

static size_t gserialized_from_lwcircstring_size(const LWCIRCSTRING *curve)
{
	size_t size = 4; /* Type number. */

	assert(curve);

	size += 4; /* Number of points (zero => empty). */
	size += curve->points->npoints * FLAGS_NDIMS(curve->flags) * sizeof(double);

	LWDEBUGF(3, "circstring size = %d", size);

	return size;
}

static size_t gserialized_from_lwcollection_size(const LWCOLLECTION *col)
{
	size_t size = 4; /* Type number. */
	int i = 0;

	assert(col);

	size += 4; /* Number of sub-geometries (zero => empty). */

	for ( i = 0; i < col->ngeoms; i++ )
	{
		size_t subsize = gserialized_from_any_size(col->geoms[i]);
		size += subsize;
		LWDEBUGF(3, "lwcollection subgeom(%d) size = %d", i, subsize);
	}

	LWDEBUGF(3, "lwcollection size = %d", size);

	return size;
}

static size_t gserialized_from_any_size(const LWGEOM *geom)
{
	LWDEBUGF(2, "Input type: %s", lwtype_name(geom->type));

	switch (geom->type)
	{
	case POINTTYPE:
		return gserialized_from_lwpoint_size((LWPOINT *)geom);
	case LINETYPE:
		return gserialized_from_lwline_size((LWLINE *)geom);
	case POLYGONTYPE:
		return gserialized_from_lwpoly_size((LWPOLY *)geom);
	case TRIANGLETYPE:
		return gserialized_from_lwtriangle_size((LWTRIANGLE *)geom);
	case CIRCSTRINGTYPE:
		return gserialized_from_lwcircstring_size((LWCIRCSTRING *)geom);
	case CURVEPOLYTYPE:
	case COMPOUNDTYPE:
	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTICURVETYPE:
	case MULTIPOLYGONTYPE:
	case MULTISURFACETYPE:
	case POLYHEDRALSURFACETYPE:
	case TINTYPE:
	case COLLECTIONTYPE:
		return gserialized_from_lwcollection_size((LWCOLLECTION *)geom);
	default:
		lwerror("Unknown geometry type: %d - %s", geom->type, lwtype_name(geom->type));
		return 0;
	}
}

/* Public function */

size_t gserialized_from_lwgeom_size(const LWGEOM *geom)
{
	size_t size = 8; /* Header overhead. */
	assert(geom);
	
	if ( geom->bbox )
		size += gbox_serialized_size(geom->flags);	
		
	size += gserialized_from_any_size(geom);
	LWDEBUGF(3, "g_serialize size = %d", size);
	
	return size;
}

/***********************************************************************
* Serialize an LWGEOM into GSERIALIZED.
*/

/* Private functions */

static size_t gserialized_from_lwgeom_any(const LWGEOM *geom, uint8_t *buf);

static size_t gserialized_from_lwpoint(const LWPOINT *point, uint8_t *buf)
{
	uint8_t *loc;
	int ptsize = ptarray_point_size(point->point);
	int type = POINTTYPE;

	assert(point);
	assert(buf);

	if ( FLAGS_GET_ZM(point->flags) != FLAGS_GET_ZM(point->point->flags) )
		lwerror("Dimensions mismatch in lwpoint");

	LWDEBUGF(2, "lwpoint_to_gserialized(%p, %p) called", point, buf);

	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);
	/* Write in the number of points (0 => empty). */
	memcpy(loc, &(point->point->npoints), sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Copy in the ordinates. */
	if ( point->point->npoints > 0 )
	{
		memcpy(loc, getPoint_internal(point->point, 0), ptsize);
		loc += ptsize;
	}

	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwline(const LWLINE *line, uint8_t *buf)
{
	uint8_t *loc;
	int ptsize;
	size_t size;
	int type = LINETYPE;

	assert(line);
	assert(buf);

	LWDEBUGF(2, "lwline_to_gserialized(%p, %p) called", line, buf);

	if ( FLAGS_GET_Z(line->flags) != FLAGS_GET_Z(line->points->flags) )
		lwerror("Dimensions mismatch in lwline");

	ptsize = ptarray_point_size(line->points);

	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the npoints. */
	memcpy(loc, &(line->points->npoints), sizeof(uint32_t));
	loc += sizeof(uint32_t);

	LWDEBUGF(3, "lwline_to_gserialized added npoints (%d)", line->points->npoints);

	/* Copy in the ordinates. */
	if ( line->points->npoints > 0 )
	{
		size = line->points->npoints * ptsize;
		memcpy(loc, getPoint_internal(line->points, 0), size);
		loc += size;
	}
	LWDEBUGF(3, "lwline_to_gserialized copied serialized_pointlist (%d bytes)", ptsize * line->points->npoints);

	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwpoly(const LWPOLY *poly, uint8_t *buf)
{
	int i;
	uint8_t *loc;
	int ptsize;
	int type = POLYGONTYPE;

	assert(poly);
	assert(buf);

	LWDEBUG(2, "lwpoly_to_gserialized called");

	ptsize = sizeof(double) * FLAGS_NDIMS(poly->flags);
	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the nrings. */
	memcpy(loc, &(poly->nrings), sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the npoints per ring. */
	for ( i = 0; i < poly->nrings; i++ )
	{
		memcpy(loc, &(poly->rings[i]->npoints), sizeof(uint32_t));
		loc += sizeof(uint32_t);
	}

	/* Add in padding if necessary to remain double aligned. */
	if ( poly->nrings % 2 )
	{
		memset(loc, 0, sizeof(uint32_t));
		loc += sizeof(uint32_t);
	}

	/* Copy in the ordinates. */
	for ( i = 0; i < poly->nrings; i++ )
	{
		POINTARRAY *pa = poly->rings[i];
		size_t pasize;

		if ( FLAGS_GET_ZM(poly->flags) != FLAGS_GET_ZM(pa->flags) )
			lwerror("Dimensions mismatch in lwpoly");

		pasize = pa->npoints * ptsize;
		memcpy(loc, getPoint_internal(pa, 0), pasize);
		loc += pasize;
	}
	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwtriangle(const LWTRIANGLE *triangle, uint8_t *buf)
{
	uint8_t *loc;
	int ptsize;
	size_t size;
	int type = TRIANGLETYPE;

	assert(triangle);
	assert(buf);

	LWDEBUGF(2, "lwtriangle_to_gserialized(%p, %p) called", triangle, buf);

	if ( FLAGS_GET_ZM(triangle->flags) != FLAGS_GET_ZM(triangle->points->flags) )
		lwerror("Dimensions mismatch in lwtriangle");

	ptsize = ptarray_point_size(triangle->points);

	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the npoints. */
	memcpy(loc, &(triangle->points->npoints), sizeof(uint32_t));
	loc += sizeof(uint32_t);

	LWDEBUGF(3, "lwtriangle_to_gserialized added npoints (%d)", triangle->points->npoints);

	/* Copy in the ordinates. */
	if ( triangle->points->npoints > 0 )
	{
		size = triangle->points->npoints * ptsize;
		memcpy(loc, getPoint_internal(triangle->points, 0), size);
		loc += size;
	}
	LWDEBUGF(3, "lwtriangle_to_gserialized copied serialized_pointlist (%d bytes)", ptsize * triangle->points->npoints);

	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwcircstring(const LWCIRCSTRING *curve, uint8_t *buf)
{
	uint8_t *loc;
	int ptsize;
	size_t size;
	int type = CIRCSTRINGTYPE;

	assert(curve);
	assert(buf);

	if (FLAGS_GET_ZM(curve->flags) != FLAGS_GET_ZM(curve->points->flags))
		lwerror("Dimensions mismatch in lwcircstring");


	ptsize = ptarray_point_size(curve->points);
	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the npoints. */
	memcpy(loc, &curve->points->npoints, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Copy in the ordinates. */
	if ( curve->points->npoints > 0 )
	{
		size = curve->points->npoints * ptsize;
		memcpy(loc, getPoint_internal(curve->points, 0), size);
		loc += size;
	}

	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwcollection(const LWCOLLECTION *coll, uint8_t *buf)
{
	size_t subsize = 0;
	uint8_t *loc;
	int i;
	int type;

	assert(coll);
	assert(buf);

	type = coll->type;
	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Write in the number of subgeoms. */
	memcpy(loc, &coll->ngeoms, sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Serialize subgeoms. */
	for ( i=0; i<coll->ngeoms; i++ )
	{
		if (FLAGS_GET_ZM(coll->flags) != FLAGS_GET_ZM(coll->geoms[i]->flags))
			lwerror("Dimensions mismatch in lwcollection");
		subsize = gserialized_from_lwgeom_any(coll->geoms[i], loc);
		loc += subsize;
	}

	return (size_t)(loc - buf);
}

static size_t gserialized_from_lwgeom_any(const LWGEOM *geom, uint8_t *buf)
{
	assert(geom);
	assert(buf);

	LWDEBUGF(2, "Input type (%d) %s, hasz: %d hasm: %d",
		geom->type, lwtype_name(geom->type),
		FLAGS_GET_Z(geom->flags), FLAGS_GET_M(geom->flags));
	LWDEBUGF(2, "LWGEOM(%p) uint8_t(%p)", geom, buf);

	switch (geom->type)
	{
	case POINTTYPE:
		return gserialized_from_lwpoint((LWPOINT *)geom, buf);
	case LINETYPE:
		return gserialized_from_lwline((LWLINE *)geom, buf);
	case POLYGONTYPE:
		return gserialized_from_lwpoly((LWPOLY *)geom, buf);
	case TRIANGLETYPE:
		return gserialized_from_lwtriangle((LWTRIANGLE *)geom, buf);
	case CIRCSTRINGTYPE:
		return gserialized_from_lwcircstring((LWCIRCSTRING *)geom, buf);
	case CURVEPOLYTYPE:
	case COMPOUNDTYPE:
	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTICURVETYPE:
	case MULTIPOLYGONTYPE:
	case MULTISURFACETYPE:
	case POLYHEDRALSURFACETYPE:
	case TINTYPE:
	case COLLECTIONTYPE:
		return gserialized_from_lwcollection((LWCOLLECTION *)geom, buf);
	default:
		lwerror("Unknown geometry type: %d - %s", geom->type, lwtype_name(geom->type));
		return 0;
	}
	return 0;
}

static size_t gserialized_from_gbox(const GBOX *gbox, uint8_t *buf)
{
	uint8_t *loc = buf;
	float f;
	size_t return_size;

	assert(buf);

	f = next_float_down(gbox->xmin);
	memcpy(loc, &f, sizeof(float));
	loc += sizeof(float);

	f = next_float_up(gbox->xmax);
	memcpy(loc, &f, sizeof(float));
	loc += sizeof(float);

	f = next_float_down(gbox->ymin);
	memcpy(loc, &f, sizeof(float));
	loc += sizeof(float);

	f = next_float_up(gbox->ymax);
	memcpy(loc, &f, sizeof(float));
	loc += sizeof(float);

	if ( FLAGS_GET_GEODETIC(gbox->flags) )
	{
		f = next_float_down(gbox->zmin);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);

		f = next_float_up(gbox->zmax);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);

		return_size = (size_t)(loc - buf);
		LWDEBUGF(4, "returning size %d", return_size);
		return return_size;
	}

	if ( FLAGS_GET_Z(gbox->flags) )
	{
		f = next_float_down(gbox->zmin);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);

		f = next_float_up(gbox->zmax);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);

	}

	if ( FLAGS_GET_M(gbox->flags) )
	{
		f = next_float_down(gbox->mmin);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);

		f = next_float_up(gbox->mmax);
		memcpy(loc, &f, sizeof(float));
		loc += sizeof(float);
	}
	return_size = (size_t)(loc - buf);
	LWDEBUGF(4, "returning size %d", return_size);
	return return_size;
}

/* Public function */

GSERIALIZED* gserialized_from_lwgeom(LWGEOM *geom, int is_geodetic, size_t *size)
{
	size_t expected_size = 0;
	size_t return_size = 0;
	uint8_t *serialized = NULL;
	uint8_t *ptr = NULL;
	GSERIALIZED *g = NULL;
	assert(geom);

	/*
	** See if we need a bounding box, add one if we don't have one.
	*/
	if ( (! geom->bbox) && lwgeom_needs_bbox(geom) && (!lwgeom_is_empty(geom)) )
	{
		lwgeom_add_bbox(geom);
	}
	
	/*
	** Harmonize the flags to the state of the lwgeom 
	*/
	if ( geom->bbox )
		FLAGS_SET_BBOX(geom->flags, 1);

	/* Set up the uint8_t buffer into which we are going to write the serialized geometry. */
	expected_size = gserialized_from_lwgeom_size(geom);
	serialized = lwalloc(expected_size);
	ptr = serialized;

	/* Move past size, srid and flags. */
	ptr += 8;

	/* Write in the serialized form of the gbox, if necessary. */
	if ( geom->bbox )
		ptr += gserialized_from_gbox(geom->bbox, ptr);

	/* Write in the serialized form of the geometry. */
	ptr += gserialized_from_lwgeom_any(geom, ptr);

	/* Calculate size as returned by data processing functions. */
	return_size = ptr - serialized;

	if ( expected_size != return_size ) /* Uh oh! */
	{
		lwerror("Return size (%d) not equal to expected size (%d)!", return_size, expected_size);
		return NULL;
	}

	if ( size ) /* Return the output size to the caller if necessary. */
		*size = return_size;

	g = (GSERIALIZED*)serialized;

	/*
	** We are aping PgSQL code here, PostGIS code should use
	** VARSIZE to set this for real.
	*/
	g->size = return_size << 2;

	/* Set the SRID! */
	gserialized_set_srid(g, geom->srid);

	g->flags = geom->flags;

	return g;
}

/***********************************************************************
* De-serialize GSERIALIZED into an LWGEOM.
*/

static LWGEOM* lwgeom_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size);

static LWPOINT* lwpoint_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint8_t *start_ptr = data_ptr;
	LWPOINT *point;
	uint32_t npoints = 0;

	assert(data_ptr);

	point = (LWPOINT*)lwalloc(sizeof(LWPOINT));
	point->srid = SRID_UNKNOWN; /* Default */
	point->bbox = NULL;
	point->type = POINTTYPE;
	point->flags = g_flags;

	data_ptr += 4; /* Skip past the type. */
	npoints = lw_get_uint32_t(data_ptr); /* Zero => empty geometry */
	data_ptr += 4; /* Skip past the npoints. */

	if ( npoints > 0 )
		point->point = ptarray_construct_reference_data(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), 1, data_ptr);
	else
		point->point = ptarray_construct(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), 0); /* Empty point */

	data_ptr += npoints * FLAGS_NDIMS(g_flags) * sizeof(double);

	if ( g_size )
		*g_size = data_ptr - start_ptr;

	return point;
}

static LWLINE* lwline_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint8_t *start_ptr = data_ptr;
	LWLINE *line;
	uint32_t npoints = 0;

	assert(data_ptr);

	line = (LWLINE*)lwalloc(sizeof(LWLINE));
	line->srid = SRID_UNKNOWN; /* Default */
	line->bbox = NULL;
	line->type = LINETYPE;
	line->flags = g_flags;

	data_ptr += 4; /* Skip past the type. */
	npoints = lw_get_uint32_t(data_ptr); /* Zero => empty geometry */
	data_ptr += 4; /* Skip past the npoints. */

	if ( npoints > 0 )
		line->points = ptarray_construct_reference_data(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), npoints, data_ptr);
		
	else
		line->points = ptarray_construct(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), 0); /* Empty linestring */

	data_ptr += FLAGS_NDIMS(g_flags) * npoints * sizeof(double);

	if ( g_size )
		*g_size = data_ptr - start_ptr;

	return line;
}

static LWPOLY* lwpoly_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint8_t *start_ptr = data_ptr;
	LWPOLY *poly;
	uint8_t *ordinate_ptr;
	uint32_t nrings = 0;
	int i = 0;

	assert(data_ptr);

	poly = (LWPOLY*)lwalloc(sizeof(LWPOLY));
	poly->srid = SRID_UNKNOWN; /* Default */
	poly->bbox = NULL;
	poly->type = POLYGONTYPE;
	poly->flags = g_flags;

	data_ptr += 4; /* Skip past the polygontype. */
	nrings = lw_get_uint32_t(data_ptr); /* Zero => empty geometry */
	poly->nrings = nrings;
	LWDEBUGF(4, "nrings = %d", nrings);
	data_ptr += 4; /* Skip past the nrings. */

	ordinate_ptr = data_ptr; /* Start the ordinate pointer. */
	if ( nrings > 0)
	{
		poly->rings = (POINTARRAY**)lwalloc( sizeof(POINTARRAY*) * nrings );
		ordinate_ptr += nrings * 4; /* Move past all the npoints values. */
		if ( nrings % 2 ) /* If there is padding, move past that too. */
			ordinate_ptr += 4;
	}
	else /* Empty polygon */
	{
		poly->rings = NULL;
	}

	for ( i = 0; i < nrings; i++ )
	{
		uint32_t npoints = 0;

		/* Read in the number of points. */
		npoints = lw_get_uint32_t(data_ptr);
		data_ptr += 4;

		/* Make a point array for the ring, and move the ordinate pointer past the ring ordinates. */
		poly->rings[i] = ptarray_construct_reference_data(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), npoints, ordinate_ptr);
		
		ordinate_ptr += sizeof(double) * FLAGS_NDIMS(g_flags) * npoints;
	}

	if ( g_size )
		*g_size = ordinate_ptr - start_ptr;

	return poly;
}

static LWTRIANGLE* lwtriangle_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint8_t *start_ptr = data_ptr;
	LWTRIANGLE *triangle;
	uint32_t npoints = 0;

	assert(data_ptr);

	triangle = (LWTRIANGLE*)lwalloc(sizeof(LWTRIANGLE));
	triangle->srid = SRID_UNKNOWN; /* Default */
	triangle->bbox = NULL;
	triangle->type = TRIANGLETYPE;
	triangle->flags = g_flags;

	data_ptr += 4; /* Skip past the type. */
	npoints = lw_get_uint32_t(data_ptr); /* Zero => empty geometry */
	data_ptr += 4; /* Skip past the npoints. */

	if ( npoints > 0 )
		triangle->points = ptarray_construct_reference_data(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), npoints, data_ptr);		
	else
		triangle->points = ptarray_construct(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), 0); /* Empty triangle */

	data_ptr += FLAGS_NDIMS(g_flags) * npoints * sizeof(double);

	if ( g_size )
		*g_size = data_ptr - start_ptr;

	return triangle;
}

static LWCIRCSTRING* lwcircstring_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint8_t *start_ptr = data_ptr;
	LWCIRCSTRING *circstring;
	uint32_t npoints = 0;

	assert(data_ptr);

	circstring = (LWCIRCSTRING*)lwalloc(sizeof(LWCIRCSTRING));
	circstring->srid = SRID_UNKNOWN; /* Default */
	circstring->bbox = NULL;
	circstring->type = CIRCSTRINGTYPE;
	circstring->flags = g_flags;

	data_ptr += 4; /* Skip past the circstringtype. */
	npoints = lw_get_uint32_t(data_ptr); /* Zero => empty geometry */
	data_ptr += 4; /* Skip past the npoints. */

	if ( npoints > 0 )
		circstring->points = ptarray_construct_reference_data(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), npoints, data_ptr);		
	else
		circstring->points = ptarray_construct(FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), 0); /* Empty circularstring */

	data_ptr += FLAGS_NDIMS(g_flags) * npoints * sizeof(double);

	if ( g_size )
		*g_size = data_ptr - start_ptr;

	return circstring;
}

static LWCOLLECTION* lwcollection_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint32_t type;
	uint8_t *start_ptr = data_ptr;
	LWCOLLECTION *collection;
	uint32_t ngeoms = 0;
	int i = 0;

	assert(data_ptr);

	type = lw_get_uint32_t(data_ptr);
	data_ptr += 4; /* Skip past the type. */

	collection = (LWCOLLECTION*)lwalloc(sizeof(LWCOLLECTION));
	collection->srid = SRID_UNKNOWN; /* Default */
	collection->bbox = NULL;
	collection->type = type;
	collection->flags = g_flags;

	ngeoms = lw_get_uint32_t(data_ptr);
	collection->ngeoms = ngeoms; /* Zero => empty geometry */
	data_ptr += 4; /* Skip past the ngeoms. */

	if ( ngeoms > 0 )
		collection->geoms = lwalloc(sizeof(LWGEOM*) * ngeoms);
	else
		collection->geoms = NULL;

	/* Sub-geometries are never de-serialized with boxes (#1254) */
	FLAGS_SET_BBOX(g_flags, 0);

	for ( i = 0; i < ngeoms; i++ )
	{
		uint32_t subtype = lw_get_uint32_t(data_ptr);
		size_t subsize = 0;

		if ( ! lwcollection_allows_subtype(type, subtype) )
		{
			lwerror("Invalid subtype (%s) for collection type (%s)", lwtype_name(subtype), lwtype_name(type));
			lwfree(collection);
			return NULL;
		}
		collection->geoms[i] = lwgeom_from_gserialized_buffer(data_ptr, g_flags, &subsize);
		data_ptr += subsize;
	}

	if ( g_size )
		*g_size = data_ptr - start_ptr;

	return collection;
}

LWGEOM* lwgeom_from_gserialized_buffer(uint8_t *data_ptr, uint8_t g_flags, size_t *g_size)
{
	uint32_t type;

	assert(data_ptr);

	type = lw_get_uint32_t(data_ptr);

	LWDEBUGF(2, "Got type %d (%s), hasz=%d hasm=%d geodetic=%d hasbox=%d", type, lwtype_name(type),
		FLAGS_GET_Z(g_flags), FLAGS_GET_M(g_flags), FLAGS_GET_GEODETIC(g_flags), FLAGS_GET_BBOX(g_flags));

	switch (type)
	{
	case POINTTYPE:
		return (LWGEOM *)lwpoint_from_gserialized_buffer(data_ptr, g_flags, g_size);
	case LINETYPE:
		return (LWGEOM *)lwline_from_gserialized_buffer(data_ptr, g_flags, g_size);
	case CIRCSTRINGTYPE:
		return (LWGEOM *)lwcircstring_from_gserialized_buffer(data_ptr, g_flags, g_size);
	case POLYGONTYPE:
		return (LWGEOM *)lwpoly_from_gserialized_buffer(data_ptr, g_flags, g_size);
	case TRIANGLETYPE:
		return (LWGEOM *)lwtriangle_from_gserialized_buffer(data_ptr, g_flags, g_size);
	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTIPOLYGONTYPE:
	case COMPOUNDTYPE:
	case CURVEPOLYTYPE:
	case MULTICURVETYPE:
	case MULTISURFACETYPE:
	case POLYHEDRALSURFACETYPE:
	case TINTYPE:
	case COLLECTIONTYPE:
		return (LWGEOM *)lwcollection_from_gserialized_buffer(data_ptr, g_flags, g_size);
	default:
		lwerror("Unknown geometry type: %d - %s", type, lwtype_name(type));
		return NULL;
	}
}

LWGEOM* lwgeom_from_gserialized(const GSERIALIZED *g)
{
	uint8_t g_flags = 0;
	int32_t g_srid = 0;
	uint32_t g_type = 0;
	uint8_t *data_ptr = NULL;
	LWGEOM *lwgeom = NULL;
	GBOX bbox;
	size_t g_size = 0;

	assert(g);

	g_srid = gserialized_get_srid(g);
	g_flags = g->flags;
	g_type = gserialized_get_type(g);
	LWDEBUGF(4, "Got type %d (%s), srid=%d", g_type, lwtype_name(g_type), g_srid);

	data_ptr = (uint8_t*)g->data;
	if ( FLAGS_GET_BBOX(g_flags) )
		data_ptr += gbox_serialized_size(g_flags);

	lwgeom = lwgeom_from_gserialized_buffer(data_ptr, g_flags, &g_size);

	if ( ! lwgeom ) 
		lwerror("lwgeom_from_gserialized: unable create geometry"); /* Ooops! */

	lwgeom->type = g_type;
	lwgeom->flags = g_flags;

	if ( gserialized_read_gbox_p(g, &bbox) == LW_SUCCESS )
	{
		lwgeom->bbox = gbox_copy(&bbox);
	}
	else if ( lwgeom_needs_bbox(lwgeom) && (lwgeom_calculate_gbox(lwgeom, &bbox) == LW_SUCCESS) )
	{
		lwgeom->bbox = gbox_copy(&bbox);
	}
	else
	{
		lwgeom->bbox = NULL;
	}

	lwgeom_set_srid(lwgeom, g_srid);

	return lwgeom;
}

