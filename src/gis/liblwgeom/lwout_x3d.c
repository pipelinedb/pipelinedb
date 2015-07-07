/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * adapted from lwout_asgml.c
 * Copyright 2011-2015 Arrival 3D
 * 				Regina Obe with input from Dave Arendash
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/
/**
* @file X3D output routines.
*
**********************************************************************/


#include <string.h>
#include "liblwgeom_internal.h"

/** defid is the id of the coordinate can be used to hold other elements DEF='abc' transform='' etc. **/
static size_t asx3d3_point_size(const LWPOINT *point, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_point(const LWPOINT *point, char *srs, int precision, int opts, const char *defid);
static size_t asx3d3_line_size(const LWLINE *line, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_line(const LWLINE *line, char *srs, int precision, int opts, const char *defid);
static size_t asx3d3_poly_size(const LWPOLY *poly, char *srs, int precision, int opts, const char *defid);
static size_t asx3d3_triangle_size(const LWTRIANGLE *triangle, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_triangle(const LWTRIANGLE *triangle, char *srs, int precision, int opts, const char *defid);
static size_t asx3d3_multi_size(const LWCOLLECTION *col, char *srs, int precisioSn, int opts, const char *defid);
static char *asx3d3_multi(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_psurface(const LWPSURFACE *psur, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_tin(const LWTIN *tin, char *srs, int precision, int opts, const char *defid);
static size_t asx3d3_collection_size(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid);
static char *asx3d3_collection(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid);
static size_t pointArray_toX3D3(POINTARRAY *pa, char *buf, int precision, int opts, int is_closed);

static size_t pointArray_X3Dsize(POINTARRAY *pa, int precision);


/*
 * VERSION X3D 3.0.2 http://www.web3d.org/specifications/x3d-3.0.dtd
 */


/* takes a GEOMETRY and returns an X3D representation */
extern char *
lwgeom_to_x3d3(const LWGEOM *geom, char *srs, int precision, int opts, const char *defid)
{
	int type = geom->type;

	switch (type)
	{
	case POINTTYPE:
		return asx3d3_point((LWPOINT*)geom, srs, precision, opts, defid);

	case LINETYPE:
		return asx3d3_line((LWLINE*)geom, srs, precision, opts, defid);

	case POLYGONTYPE:
	{
		/** We might change this later, but putting a polygon in an indexed face set
		* seems like the simplest way to go so treat just like a mulitpolygon
		*/
		LWCOLLECTION *tmp = (LWCOLLECTION*)lwgeom_as_multi(geom);
		char *ret = asx3d3_multi(tmp, srs, precision, opts, defid);
		lwcollection_free(tmp);
		return ret;
	}

	case TRIANGLETYPE:
		return asx3d3_triangle((LWTRIANGLE*)geom, srs, precision, opts, defid);

	case MULTIPOINTTYPE:
	case MULTILINETYPE:
	case MULTIPOLYGONTYPE:
		return asx3d3_multi((LWCOLLECTION*)geom, srs, precision, opts, defid);

	case POLYHEDRALSURFACETYPE:
		return asx3d3_psurface((LWPSURFACE*)geom, srs, precision, opts, defid);

	case TINTYPE:
		return asx3d3_tin((LWTIN*)geom, srs, precision, opts, defid);

	case COLLECTIONTYPE:
		return asx3d3_collection((LWCOLLECTION*)geom, srs, precision, opts, defid);

	default:
		lwerror("lwgeom_to_x3d3: '%s' geometry type not supported", lwtype_name(type));
		return NULL;
	}
}

static size_t
asx3d3_point_size(const LWPOINT *point, char *srs, int precision, int opts, const char *defid)
{
	int size;
	/* size_t defidlen = strlen(defid); */

	size = pointArray_X3Dsize(point->point, precision);
	/* size += ( sizeof("<point><pos>/") + (defidlen*2) ) * 2; */
	/* if (srs)     size += strlen(srs) + sizeof(" srsName=.."); */
	return size;
}

static size_t
asx3d3_point_buf(const LWPOINT *point, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr = output;
	/* int dimension=2; */

	/* if (FLAGS_GET_Z(point->flags)) dimension = 3; */
	/*	if ( srs )
		{
			ptr += sprintf(ptr, "<%sPoint srsName=\"%s\">", defid, srs);
		}
		else*/
	/* ptr += sprintf(ptr, "%s", defid); */

	/* ptr += sprintf(ptr, "<%spos>", defid); */
	ptr += pointArray_toX3D3(point->point, ptr, precision, opts, 0);
	/* ptr += sprintf(ptr, "</%spos></%sPoint>", defid, defid); */

	return (ptr-output);
}

static char *
asx3d3_point(const LWPOINT *point, char *srs, int precision, int opts, const char *defid)
{
	char *output;
	int size;

	size = asx3d3_point_size(point, srs, precision, opts, defid);
	output = lwalloc(size);
	asx3d3_point_buf(point, srs, output, precision, opts, defid);
	return output;
}


static size_t
asx3d3_line_size(const LWLINE *line, char *srs, int precision, int opts, const char *defid)
{
	int size;
	size_t defidlen = strlen(defid);

	size = pointArray_X3Dsize(line->points, precision)*2;
	
	if ( X3D_USE_GEOCOORDS(opts) ) {
			size += (
	            sizeof("<LineSet vertexCount=''><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' /></LineSet>")  + defidlen
	        ) * 2;
	}
	else {
		size += (
		            sizeof("<LineSet vertexCount=''><Coordinate point='' /></LineSet>")  + defidlen
		        ) * 2;
	}

	/* if (srs)     size += strlen(srs) + sizeof(" srsName=.."); */
	return size;
}

static size_t
asx3d3_line_buf(const LWLINE *line, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr=output;
	/* int dimension=2; */
	POINTARRAY *pa;


	/* if (FLAGS_GET_Z(line->flags)) dimension = 3; */

	pa = line->points;
	ptr += sprintf(ptr, "<LineSet %s vertexCount='%d'>", defid, pa->npoints);

	if ( X3D_USE_GEOCOORDS(opts) ) ptr += sprintf(ptr, "<GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & LW_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
	else
		ptr += sprintf(ptr, "<Coordinate point='");
	ptr += pointArray_toX3D3(line->points, ptr, precision, opts, lwline_is_closed((LWLINE *) line));

	ptr += sprintf(ptr, "' />");

	ptr += sprintf(ptr, "</LineSet>");
	return (ptr-output);
}

static size_t
asx3d3_line_coords(const LWLINE *line, char *output, int precision, int opts)
{
	char *ptr=output;
	/* ptr += sprintf(ptr, ""); */
	ptr += pointArray_toX3D3(line->points, ptr, precision, opts, lwline_is_closed(line));
	return (ptr-output);
}

/* Calculate the coordIndex property of the IndexedLineSet for the multilinestring */
static size_t
asx3d3_mline_coordindex(const LWMLINE *mgeom, char *output)
{
	char *ptr=output;
	LWLINE *geom;
	int i, j, k, si;
	POINTARRAY *pa;
	int np;

	j = 0;
	for (i=0; i < mgeom->ngeoms; i++)
	{
		geom = (LWLINE *) mgeom->geoms[i];
		pa = geom->points;
		np = pa->npoints;
		si = j;  /* start index of first point of linestring */
		for (k=0; k < np ; k++)
		{
			if (k)
			{
				ptr += sprintf(ptr, " ");
			}
			/** if the linestring is closed, we put the start point index
			*   for the last vertex to denote use first point
			*    and don't increment the index **/
			if (!lwline_is_closed(geom) || k < (np -1) )
			{
				ptr += sprintf(ptr, "%d", j);
				j += 1;
			}
			else
			{
				ptr += sprintf(ptr,"%d", si);
			}
		}
		if (i < (mgeom->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " -1 "); /* separator for each linestring */
		}
	}
	return (ptr-output);
}

/* Calculate the coordIndex property of the IndexedLineSet for a multipolygon
    This is not ideal -- would be really nice to just share this function with psurf,
    but I'm not smart enough to do that yet*/
static size_t
asx3d3_mpoly_coordindex(const LWMPOLY *psur, char *output)
{
	char *ptr=output;
	LWPOLY *patch;
	int i, j, k, l;
	int np;
	j = 0;
	for (i=0; i<psur->ngeoms; i++)
	{
		patch = (LWPOLY *) psur->geoms[i];
		for (l=0; l < patch->nrings; l++)
		{
			np = patch->rings[l]->npoints - 1;
			for (k=0; k < np ; k++)
			{
				if (k)
				{
					ptr += sprintf(ptr, " ");
				}
				ptr += sprintf(ptr, "%d", (j + k));
			}
			j += k;
			if (l < (patch->nrings - 1) )
			{
				/** @todo TODO: Decide the best way to render holes
				*  Evidentally according to my X3D expert the X3D consortium doesn't really
				*  support holes and it's an issue of argument among many that feel it should. He thinks CAD x3d extensions to spec might.
				*  What he has done and others developing X3D exports to simulate a hole is to cut around it.
				*  So if you have a donut, you would cut it into half and have 2 solid polygons.  Not really sure the best way to handle this.
				*  For now will leave it as polygons stacked on top of each other -- which is what we are doing here and perhaps an option
				*  to color differently.  It's not ideal but the alternative sounds complicated.
				**/
				ptr += sprintf(ptr, " -1 "); /* separator for each inner ring. Ideally we should probably triangulate and cut around as others do */
			}
		}
		if (i < (psur->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " -1 "); /* separator for each subgeom */
		}
	}
	return (ptr-output);
}

/** Return the linestring as an X3D LineSet */
static char *
asx3d3_line(const LWLINE *line, char *srs, int precision, int opts, const char *defid)
{
	char *output;
	int size;

	size = sizeof("<LineSet><CoordIndex ='' /></LineSet>") + asx3d3_line_size(line, srs, precision, opts, defid);
	output = lwalloc(size);
	asx3d3_line_buf(line, srs, output, precision, opts, defid);
	return output;
}

/** Compute the string space needed for the IndexedFaceSet representation of the polygon **/
static size_t
asx3d3_poly_size(const LWPOLY *poly,  char *srs, int precision, int opts, const char *defid)
{
	size_t size;
	size_t defidlen = strlen(defid);
	int i;

	size = ( sizeof("<IndexedFaceSet></IndexedFaceSet>") + (defidlen*3) ) * 2 + 6 * (poly->nrings - 1);

	for (i=0; i<poly->nrings; i++)
		size += pointArray_X3Dsize(poly->rings[i], precision);

	return size;
}

/** Compute the X3D coordinates of the polygon **/
static size_t
asx3d3_poly_buf(const LWPOLY *poly, char *srs, char *output, int precision, int opts, int is_patch, const char *defid)
{
	int i;
	char *ptr=output;

	ptr += pointArray_toX3D3(poly->rings[0], ptr, precision, opts, 1);
	for (i=1; i<poly->nrings; i++)
	{
		ptr += sprintf(ptr, " "); /* inner ring points start */
		ptr += pointArray_toX3D3(poly->rings[i], ptr, precision, opts,1);
	}
	return (ptr-output);
}

static size_t
asx3d3_triangle_size(const LWTRIANGLE *triangle, char *srs, int precision, int opts, const char *defid)
{
	size_t size;
	size_t defidlen = strlen(defid);

	/** 6 for the 3 sides and space to separate each side **/
	size = sizeof("<IndexedTriangleSet index=''></IndexedTriangleSet>") + defidlen + 6;
	size += pointArray_X3Dsize(triangle->points, precision);

	return size;
}

static size_t
asx3d3_triangle_buf(const LWTRIANGLE *triangle, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr=output;
	ptr += pointArray_toX3D3(triangle->points, ptr, precision, opts, 1);

	return (ptr-output);
}

static char *
asx3d3_triangle(const LWTRIANGLE *triangle, char *srs, int precision, int opts, const char *defid)
{
	char *output;
	int size;

	size = asx3d3_triangle_size(triangle, srs, precision, opts, defid);
	output = lwalloc(size);
	asx3d3_triangle_buf(triangle, srs, output, precision, opts, defid);
	return output;
}


/**
 * Compute max size required for X3D version of this
 * inspected geometry. Will recurse when needed.
 * Don't call this with single-geoms inspected.
 */
static size_t
asx3d3_multi_size(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid)
{
	int i;
	size_t size;
	size_t defidlen = strlen(defid);
	LWGEOM *subgeom;

	/* the longest possible multi version needs to hold DEF=defid and coordinate breakout */
	if ( X3D_USE_GEOCOORDS(opts) )
		size = sizeof("<PointSet><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' /></PointSet>");
	else
		size = sizeof("<PointSet><Coordinate point='' /></PointSet>") + defidlen;
	

	/* if ( srs ) size += strlen(srs) + sizeof(" srsName=.."); */

	for (i=0; i<col->ngeoms; i++)
	{
		subgeom = col->geoms[i];
		if (subgeom->type == POINTTYPE)
		{
			/* size += ( sizeof("point=''") + defidlen ) * 2; */
			size += asx3d3_point_size((LWPOINT*)subgeom, 0, precision, opts, defid);
		}
		else if (subgeom->type == LINETYPE)
		{
			/* size += ( sizeof("<curveMember>/") + defidlen ) * 2; */
			size += asx3d3_line_size((LWLINE*)subgeom, 0, precision, opts, defid);
		}
		else if (subgeom->type == POLYGONTYPE)
		{
			/* size += ( sizeof("<surfaceMember>/") + defidlen ) * 2; */
			size += asx3d3_poly_size((LWPOLY*)subgeom, 0, precision, opts, defid);
		}
	}

	return size;
}

/*
 * Don't call this with single-geoms inspected!
 */
static size_t
asx3d3_multi_buf(const LWCOLLECTION *col, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr, *x3dtype;
	int i;
	int dimension=2;

	if (FLAGS_GET_Z(col->flags)) dimension = 3;
	LWGEOM *subgeom;
	ptr = output;
	x3dtype="";


	switch (col->type)
	{
        case MULTIPOINTTYPE:
            x3dtype = "PointSet";
            if ( dimension == 2 ){ /** Use Polypoint2D instead **/
                x3dtype = "Polypoint2D";   
                ptr += sprintf(ptr, "<%s %s point='", x3dtype, defid);
            }
            else {
                ptr += sprintf(ptr, "<%s %s>", x3dtype, defid);
            }
            break;
        case MULTILINETYPE:
            x3dtype = "IndexedLineSet";
            ptr += sprintf(ptr, "<%s %s coordIndex='", x3dtype, defid);
            ptr += asx3d3_mline_coordindex((const LWMLINE *)col, ptr);
            ptr += sprintf(ptr, "'>");
            break;
        case MULTIPOLYGONTYPE:
            x3dtype = "IndexedFaceSet";
            ptr += sprintf(ptr, "<%s %s coordIndex='", x3dtype, defid);
            ptr += asx3d3_mpoly_coordindex((const LWMPOLY *)col, ptr);
            ptr += sprintf(ptr, "'>");
            break;
        default:
            lwerror("asx3d3_multi_buf: '%s' geometry type not supported", lwtype_name(col->type));
            return 0;
    }
    if (dimension == 3){
		if ( X3D_USE_GEOCOORDS(opts) ) 
			ptr += sprintf(ptr, "<GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ((opts & LW_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
		else
        	ptr += sprintf(ptr, "<Coordinate point='");
    }

	for (i=0; i<col->ngeoms; i++)
	{
		subgeom = col->geoms[i];
		if (subgeom->type == POINTTYPE)
		{
			ptr += asx3d3_point_buf((LWPOINT*)subgeom, 0, ptr, precision, opts, defid);
			ptr += sprintf(ptr, " ");
		}
		else if (subgeom->type == LINETYPE)
		{
			ptr += asx3d3_line_coords((LWLINE*)subgeom, ptr, precision, opts);
			ptr += sprintf(ptr, " ");
		}
		else if (subgeom->type == POLYGONTYPE)
		{
			ptr += asx3d3_poly_buf((LWPOLY*)subgeom, 0, ptr, precision, opts, 0, defid);
			ptr += sprintf(ptr, " ");
		}
	}

	/* Close outmost tag */
	if (dimension == 3){
	    ptr += sprintf(ptr, "' /></%s>", x3dtype);
	}
	else { ptr += sprintf(ptr, "' />"); }    
	return (ptr-output);
}

/*
 * Don't call this with single-geoms inspected!
 */
static char *
asx3d3_multi(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid)
{
	char *x3d;
	size_t size;

	size = asx3d3_multi_size(col, srs, precision, opts, defid);
	x3d = lwalloc(size);
	asx3d3_multi_buf(col, srs, x3d, precision, opts, defid);
	return x3d;
}


static size_t
asx3d3_psurface_size(const LWPSURFACE *psur, char *srs, int precision, int opts, const char *defid)
{
	int i;
	size_t size;
	size_t defidlen = strlen(defid);

	if ( X3D_USE_GEOCOORDS(opts) ) size = sizeof("<IndexedFaceSet coordIndex=''><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' />") + defidlen;
	else size = sizeof("<IndexedFaceSet coordIndex=''><Coordinate point='' />") + defidlen;
	

	for (i=0; i<psur->ngeoms; i++)
	{
		size += asx3d3_poly_size(psur->geoms[i], 0, precision, opts, defid)*5; /** need to make space for coordIndex values too including -1 separating each poly**/
	}

	return size;
}


/*
 * Don't call this with single-geoms inspected!
 */
static size_t
asx3d3_psurface_buf(const LWPSURFACE *psur, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr;
	int i;
	int j;
	int k;
	int np;
	LWPOLY *patch;

	ptr = output;

	/* Open outmost tag */
	ptr += sprintf(ptr, "<IndexedFaceSet %s coordIndex='",defid);

	j = 0;
	for (i=0; i<psur->ngeoms; i++)
	{
		patch = (LWPOLY *) psur->geoms[i];
		np = patch->rings[0]->npoints - 1;
		for (k=0; k < np ; k++)
		{
			if (k)
			{
				ptr += sprintf(ptr, " ");
			}
			ptr += sprintf(ptr, "%d", (j + k));
		}
		if (i < (psur->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " -1 "); /* separator for each subgeom */
		}
		j += k;
	}

	if ( X3D_USE_GEOCOORDS(opts) ) 
		ptr += sprintf(ptr, "'><GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & LW_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
	else ptr += sprintf(ptr, "'><Coordinate point='");

	for (i=0; i<psur->ngeoms; i++)
	{
		ptr += asx3d3_poly_buf(psur->geoms[i], 0, ptr, precision, opts, 1, defid);
		if (i < (psur->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " "); /* only add a trailing space if its not the last polygon in the set */
		}
	}

	/* Close outmost tag */
	ptr += sprintf(ptr, "' /></IndexedFaceSet>");

	return (ptr-output);
}

/*
 * Don't call this with single-geoms inspected!
 */
static char *
asx3d3_psurface(const LWPSURFACE *psur, char *srs, int precision, int opts, const char *defid)
{
	char *x3d;
	size_t size;

	size = asx3d3_psurface_size(psur, srs, precision, opts, defid);
	x3d = lwalloc(size);
	asx3d3_psurface_buf(psur, srs, x3d, precision, opts, defid);
	return x3d;
}


static size_t
asx3d3_tin_size(const LWTIN *tin, char *srs, int precision, int opts, const char *defid)
{
	int i;
	size_t size;
	size_t defidlen = strlen(defid);
	/* int dimension=2; */

	/** Need to make space for size of additional attributes,
	** the coordIndex has a value for each edge for each triangle plus a space to separate so we need at least that much extra room ***/
	size = sizeof("<IndexedTriangleSet coordIndex=''></IndexedTriangleSet>") + defidlen + tin->ngeoms*12;

	for (i=0; i<tin->ngeoms; i++)
	{
		size += (asx3d3_triangle_size(tin->geoms[i], 0, precision, opts, defid) * 20); /** 3 is to make space for coordIndex **/
	}

	return size;
}


/*
 * Don't call this with single-geoms inspected!
 */
static size_t
asx3d3_tin_buf(const LWTIN *tin, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr;
	int i;
	int k;
	/* int dimension=2; */

	ptr = output;

	ptr += sprintf(ptr, "<IndexedTriangleSet %s index='",defid);
	k = 0;
	/** Fill in triangle index **/
	for (i=0; i<tin->ngeoms; i++)
	{
		ptr += sprintf(ptr, "%d %d %d", k, (k+1), (k+2));
		if (i < (tin->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " ");
		}
		k += 3;
	}

	if ( X3D_USE_GEOCOORDS(opts) ) ptr += sprintf(ptr, "'><GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & LW_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
	else ptr += sprintf(ptr, "'><Coordinate point='");
	
	for (i=0; i<tin->ngeoms; i++)
	{
		ptr += asx3d3_triangle_buf(tin->geoms[i], 0, ptr, precision,
		                           opts, defid);
		if (i < (tin->ngeoms - 1) )
		{
			ptr += sprintf(ptr, " ");
		}
	}

	/* Close outmost tag */

	ptr += sprintf(ptr, "'/></IndexedTriangleSet>");

	return (ptr-output);
}

/*
 * Don't call this with single-geoms inspected!
 */
static char *
asx3d3_tin(const LWTIN *tin, char *srs, int precision, int opts, const char *defid)
{
	char *x3d;
	size_t size;

	size = asx3d3_tin_size(tin, srs, precision, opts, defid);
	x3d = lwalloc(size);
	asx3d3_tin_buf(tin, srs, x3d, precision, opts, defid);
	return x3d;
}

static size_t
asx3d3_collection_size(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid)
{
	int i;
	size_t size;
	size_t defidlen = strlen(defid);
	LWGEOM *subgeom;

	/* size = sizeof("<MultiGeometry></MultiGeometry>") + defidlen*2; */
	size = defidlen*2;

	/** if ( srs )
		size += strlen(srs) + sizeof(" srsName=.."); **/

	for (i=0; i<col->ngeoms; i++)
	{
		subgeom = col->geoms[i];
		size += ( sizeof("<Shape />") + defidlen ) * 2; /** for collections we need to wrap each in a shape tag to make valid **/
		if ( subgeom->type == POINTTYPE )
		{
			size += asx3d3_point_size((LWPOINT*)subgeom, 0, precision, opts, defid);
		}
		else if ( subgeom->type == LINETYPE )
		{
			size += asx3d3_line_size((LWLINE*)subgeom, 0, precision, opts, defid);
		}
		else if ( subgeom->type == POLYGONTYPE )
		{
			size += asx3d3_poly_size((LWPOLY*)subgeom, 0, precision, opts, defid);
		}
		else if ( subgeom->type == TINTYPE )
		{
			size += asx3d3_tin_size((LWTIN*)subgeom, 0, precision, opts, defid);
		}
		else if ( subgeom->type == POLYHEDRALSURFACETYPE )
		{
			size += asx3d3_psurface_size((LWPSURFACE*)subgeom, 0, precision, opts, defid);
		}
		else if ( lwgeom_is_collection(subgeom) )
		{
			size += asx3d3_multi_size((LWCOLLECTION*)subgeom, 0, precision, opts, defid);
		}
		else
			lwerror("asx3d3_collection_size: unknown geometry type");
	}

	return size;
}

static size_t
asx3d3_collection_buf(const LWCOLLECTION *col, char *srs, char *output, int precision, int opts, const char *defid)
{
	char *ptr;
	int i;
	LWGEOM *subgeom;

	ptr = output;

	/* Open outmost tag */
	/** @TODO: decide if we need outtermost tags, this one was just a copy from gml so is wrong **/
#ifdef PGIS_X3D_OUTERMOST_TAGS
	if ( srs )
	{
		ptr += sprintf(ptr, "<%sMultiGeometry srsName=\"%s\">", defid, srs);
	}
	else
	{
		ptr += sprintf(ptr, "<%sMultiGeometry>", defid);
	}
#endif

	for (i=0; i<col->ngeoms; i++)
	{
		subgeom = col->geoms[i];
		ptr += sprintf(ptr, "<Shape%s>", defid);
		if ( subgeom->type == POINTTYPE )
		{
			ptr += asx3d3_point_buf((LWPOINT*)subgeom, 0, ptr, precision, opts, defid);
		}
		else if ( subgeom->type == LINETYPE )
		{
			ptr += asx3d3_line_buf((LWLINE*)subgeom, 0, ptr, precision, opts, defid);
		}
		else if ( subgeom->type == POLYGONTYPE )
		{
			ptr += asx3d3_poly_buf((LWPOLY*)subgeom, 0, ptr, precision, opts, 0, defid);
		}
		else if ( subgeom->type == TINTYPE )
		{
			ptr += asx3d3_tin_buf((LWTIN*)subgeom, srs, ptr, precision, opts,  defid);
			
		}
		else if ( subgeom->type == POLYHEDRALSURFACETYPE )
		{
			ptr += asx3d3_psurface_buf((LWPSURFACE*)subgeom, srs, ptr, precision, opts,  defid);
			
		}
		else if ( lwgeom_is_collection(subgeom) )
		{
			if ( subgeom->type == COLLECTIONTYPE )
				ptr += asx3d3_collection_buf((LWCOLLECTION*)subgeom, 0, ptr, precision, opts, defid);
			else
				ptr += asx3d3_multi_buf((LWCOLLECTION*)subgeom, 0, ptr, precision, opts, defid);
		}
		else
			lwerror("asx3d3_collection_buf: unknown geometry type");

		ptr += printf(ptr, "</Shape>");
	}

	/* Close outmost tag */
#ifdef PGIS_X3D_OUTERMOST_TAGS
	ptr += sprintf(ptr, "</%sMultiGeometry>", defid);
#endif

	return (ptr-output);
}

/*
 * Don't call this with single-geoms inspected!
 */
static char *
asx3d3_collection(const LWCOLLECTION *col, char *srs, int precision, int opts, const char *defid)
{
	char *x3d;
	size_t size;

	size = asx3d3_collection_size(col, srs, precision, opts, defid);
	x3d = lwalloc(size);
	asx3d3_collection_buf(col, srs, x3d, precision, opts, defid);
	return x3d;
}


/** In X3D3, coordinates are separated by a space separator
 */
static size_t
pointArray_toX3D3(POINTARRAY *pa, char *output, int precision, int opts, int is_closed)
{
	int i;
	char *ptr;
	char x[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	char y[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
	char z[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];

	ptr = output;

	if ( ! FLAGS_GET_Z(pa->flags) )
	{
		for (i=0; i<pa->npoints; i++)
		{
			/** Only output the point if it is not the last point of a closed object or it is a non-closed type **/
			if ( !is_closed || i < (pa->npoints - 1) )
			{
				POINT2D pt;
				getPoint2d_p(pa, i, &pt);

				if (fabs(pt.x) < OUT_MAX_DOUBLE)
					sprintf(x, "%.*f", precision, pt.x);
				else
					sprintf(x, "%g", pt.x);
				trim_trailing_zeros(x);

				if (fabs(pt.y) < OUT_MAX_DOUBLE)
					sprintf(y, "%.*f", precision, pt.y);
				else
					sprintf(y, "%g", pt.y);
				trim_trailing_zeros(y);

				if ( i )
					ptr += sprintf(ptr, " ");
					
				if ( ( opts & LW_X3D_FLIP_XY) )
					ptr += sprintf(ptr, "%s %s", y, x);
				else
					ptr += sprintf(ptr, "%s %s", x, y);
			}
		}
	}
	else
	{
		for (i=0; i<pa->npoints; i++)
		{
			/** Only output the point if it is not the last point of a closed object or it is a non-closed type **/
			if ( !is_closed || i < (pa->npoints - 1) )
			{
				POINT4D pt;
				getPoint4d_p(pa, i, &pt);

				if (fabs(pt.x) < OUT_MAX_DOUBLE)
					sprintf(x, "%.*f", precision, pt.x);
				else
					sprintf(x, "%g", pt.x);
				trim_trailing_zeros(x);

				if (fabs(pt.y) < OUT_MAX_DOUBLE)
					sprintf(y, "%.*f", precision, pt.y);
				else
					sprintf(y, "%g", pt.y);
				trim_trailing_zeros(y);

				if (fabs(pt.z) < OUT_MAX_DOUBLE)
					sprintf(z, "%.*f", precision, pt.z);
				else
					sprintf(z, "%g", pt.z);
				trim_trailing_zeros(z);

				if ( i )
					ptr += sprintf(ptr, " ");

				if ( ( opts & LW_X3D_FLIP_XY) )
					ptr += sprintf(ptr, "%s %s %s", y, x, z);
				else
					ptr += sprintf(ptr, "%s %s %s", x, y, z);
			}
		}
	}

	return ptr-output;
}



/**
 * Returns maximum size of rendered pointarray in bytes.
 */
static size_t
pointArray_X3Dsize(POINTARRAY *pa, int precision)
{
	if (FLAGS_NDIMS(pa->flags) == 2)
		return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(" "))
		       * 2 * pa->npoints;

	return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(" ")) * 3 * pa->npoints;
}
