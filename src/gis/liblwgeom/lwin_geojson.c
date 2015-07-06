/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright 2013 Sandro Santilli <strk@keybit.net>
 * Copyright 2011 Kashif Rasul <kashif.rasul@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <assert.h>
#include "liblwgeom.h"
#include "lwgeom_log.h"
#include "../postgis_config.h"

#if defined(HAVE_LIBJSON) || defined(HAVE_LIBJSON_C) /* --{ */

#ifdef HAVE_LIBJSON_C
#include <json-c/json.h>
#include <json-c/json_object_private.h>
#else
#include <json/json.h>
#include <json/json_object_private.h>
#endif

#ifndef JSON_C_VERSION
/* Adds support for libjson < 0.10 */
# define json_tokener_error_desc(x) json_tokener_errors[(x)]
#endif

#include <string.h>

static void geojson_lwerror(char *msg, int error_code)
{
	LWDEBUGF(3, "lwgeom_from_geojson ERROR %i", error_code);
	lwerror("%s", msg);
}

/* Prototype */
static LWGEOM* parse_geojson(json_object *geojson, int *hasz,  int root_srid);

static json_object*
findMemberByName(json_object* poObj, const char* pszName )
{
	json_object* poTmp;
	json_object_iter it;

	poTmp = poObj;

	if( NULL == pszName || NULL == poObj)
		return NULL;

	it.key = NULL;
	it.val = NULL;
	it.entry = NULL;

	if( NULL != json_object_get_object(poTmp) )
	{
		if( NULL == json_object_get_object(poTmp)->head )
		{
			geojson_lwerror("invalid GeoJSON representation", 2);
			return NULL;
		}

		for( it.entry = json_object_get_object(poTmp)->head;
		        ( it.entry ?
		          ( it.key = (char*)it.entry->k,
		            it.val = (json_object*)it.entry->v, it.entry) : 0);
		        it.entry = it.entry->next)
		{
			if( strcasecmp((char *)it.key, pszName )==0 )
				return it.val;
		}
	}

	return NULL;
}


static int
parse_geojson_coord(json_object *poObj, int *hasz, POINTARRAY *pa)
{
	POINT4D pt;

	LWDEBUGF(3, "parse_geojson_coord called for object %s.", json_object_to_json_string( poObj ) );

	if( json_type_array == json_object_get_type( poObj ) )
	{

		json_object* poObjCoord = NULL;
		const int nSize = json_object_array_length( poObj );
		LWDEBUGF(3, "parse_geojson_coord called for array size %d.", nSize );

		if ( nSize < 2 )
		{
			geojson_lwerror("Too few ordinates in GeoJSON", 4);
			return LW_FAILURE;
		}
		
		/* Read X coordinate */
		poObjCoord = json_object_array_get_idx( poObj, 0 );
		pt.x = json_object_get_double( poObjCoord );
		LWDEBUGF(3, "parse_geojson_coord pt.x = %f.", pt.x );

		/* Read Y coordinate */
		poObjCoord = json_object_array_get_idx( poObj, 1 );
		pt.y = json_object_get_double( poObjCoord );
		LWDEBUGF(3, "parse_geojson_coord pt.y = %f.", pt.y );

		if( nSize > 2 ) /* should this be >= 3 ? */
		{
			/* Read Z coordinate */
			poObjCoord = json_object_array_get_idx( poObj, 2 );
			pt.z = json_object_get_double( poObjCoord );
			LWDEBUGF(3, "parse_geojson_coord pt.z = %f.", pt.z );
			*hasz = LW_TRUE;
		}
		else if ( nSize == 2 )
		{
			*hasz = LW_FALSE;
			/* Initialize Z coordinate, if required */
			if ( FLAGS_GET_Z(pa->flags) ) pt.z = 0.0;
		}
		else 
		{
			/* TODO: should we account for nSize > 3 ? */
			/* more than 3 coordinates, we're just dropping dimensions here... */
		}

		/* Initialize M coordinate, if required */
		if ( FLAGS_GET_M(pa->flags) ) pt.m = 0.0;

	}
	else
	{
		/* If it's not an array, just don't handle it */
		return LW_FAILURE;
	}

	return ptarray_append_point(pa, &pt, LW_TRUE);
}

static LWGEOM*
parse_geojson_point(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom;
	POINTARRAY *pa;
	json_object* coords = NULL;

	LWDEBUGF(3, "parse_geojson_point called with root_srid = %d.", root_srid );

	coords = findMemberByName( geojson, "coordinates" );
	if ( ! coords ) {
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
    return NULL;
  }
	
	pa = ptarray_construct_empty(1, 0, 1);
	parse_geojson_coord(coords, hasz, pa);

	geom = (LWGEOM *) lwpoint_construct(root_srid, NULL, pa);
	LWDEBUG(2, "parse_geojson_point finished.");
	return geom;
}

static LWGEOM*
parse_geojson_linestring(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom;
	POINTARRAY *pa;
	json_object* points = NULL;
	int i = 0;

	LWDEBUG(2, "parse_geojson_linestring called.");

	points = findMemberByName( geojson, "coordinates" );
	if ( ! points ) {
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
    return NULL;
  }

	pa = ptarray_construct_empty(1, 0, 1);

	if( json_type_array == json_object_get_type( points ) )
	{
		const int nPoints = json_object_array_length( points );
		for(i = 0; i < nPoints; ++i)
		{
			json_object* coords = NULL;
			coords = json_object_array_get_idx( points, i );
			parse_geojson_coord(coords, hasz, pa);
		}
	}

	geom = (LWGEOM *) lwline_construct(root_srid, NULL, pa);

	LWDEBUG(2, "parse_geojson_linestring finished.");
	return geom;
}

static LWGEOM*
parse_geojson_polygon(json_object *geojson, int *hasz,  int root_srid)
{
	POINTARRAY **ppa = NULL;
	json_object* rings = NULL;
	json_object* points = NULL;
	int i = 0, j = 0;
	int nRings = 0, nPoints = 0;

	rings = findMemberByName( geojson, "coordinates" );
	if ( ! rings ) 
	{
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
		return NULL;
	}

	if ( json_type_array != json_object_get_type(rings) )
	{
		geojson_lwerror("The 'coordinates' in GeoJSON are not an array", 4);
		return NULL;
	}

	nRings = json_object_array_length( rings );

	/* No rings => POLYGON EMPTY */
	if ( ! nRings )
	{
		return (LWGEOM *)lwpoly_construct_empty(root_srid, 0, 0);
	}
	
	for ( i = 0; i < nRings; i++ )
	{
		points = json_object_array_get_idx(rings, i);
		if ( ! points || json_object_get_type(points) != json_type_array )
		{
			geojson_lwerror("The 'coordinates' in GeoJSON ring are not an array", 4);
			return NULL;
		}
		nPoints = json_object_array_length(points);
		
		/* Skip empty rings */
		if ( nPoints == 0 ) continue;
		
		if ( ! ppa )
			ppa = (POINTARRAY**)lwalloc(sizeof(POINTARRAY*) * nRings);
		
		ppa[i] = ptarray_construct_empty(1, 0, 1);
		for ( j = 0; j < nPoints; j++ )
		{
			json_object* coords = NULL;
			coords = json_object_array_get_idx( points, j );
			parse_geojson_coord(coords, hasz, ppa[i]);
		}
	}	
	
	/* All the rings were empty! */
	if ( ! ppa )
		return (LWGEOM *)lwpoly_construct_empty(root_srid, 0, 0);
	
	return (LWGEOM *) lwpoly_construct(root_srid, NULL, nRings, ppa);
}

static LWGEOM*
parse_geojson_multipoint(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom;
	int i = 0;
	json_object* poObjPoints = NULL;

	if (!root_srid)
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTIPOINTTYPE, root_srid, 1, 0);
	}
	else
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTIPOINTTYPE, -1, 1, 0);
	}

	poObjPoints = findMemberByName( geojson, "coordinates" );
	if ( ! poObjPoints ) 
	{
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
		return NULL;
	}

	if( json_type_array == json_object_get_type( poObjPoints ) )
	{
		const int nPoints = json_object_array_length( poObjPoints );
		for( i = 0; i < nPoints; ++i)
		{
			POINTARRAY *pa;
			json_object* poObjCoords = NULL;
			poObjCoords = json_object_array_get_idx( poObjPoints, i );

			pa = ptarray_construct_empty(1, 0, 1);
			parse_geojson_coord(poObjCoords, hasz, pa);

			geom = (LWGEOM*)lwmpoint_add_lwpoint((LWMPOINT*)geom,
			                                     (LWPOINT*)lwpoint_construct(root_srid, NULL, pa));
		}
	}

	return geom;
}

static LWGEOM*
parse_geojson_multilinestring(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom = NULL;
	int i, j;
	json_object* poObjLines = NULL;

	if (!root_srid)
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTILINETYPE, root_srid, 1, 0);
	}
	else
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTILINETYPE, -1, 1, 0);
	}

	poObjLines = findMemberByName( geojson, "coordinates" );
	if ( ! poObjLines ) {
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
    return NULL;
  }

	if( json_type_array == json_object_get_type( poObjLines ) )
	{
		const int nLines = json_object_array_length( poObjLines );
		for( i = 0; i < nLines; ++i)
		{
			POINTARRAY *pa = NULL;
			json_object* poObjLine = NULL;
			poObjLine = json_object_array_get_idx( poObjLines, i );
			pa = ptarray_construct_empty(1, 0, 1);

			if( json_type_array == json_object_get_type( poObjLine ) )
			{
				const int nPoints = json_object_array_length( poObjLine );
				for(j = 0; j < nPoints; ++j)
				{
					json_object* coords = NULL;
					coords = json_object_array_get_idx( poObjLine, j );
					parse_geojson_coord(coords, hasz, pa);
				}

				geom = (LWGEOM*)lwmline_add_lwline((LWMLINE*)geom,
				                                   (LWLINE*)lwline_construct(root_srid, NULL, pa));
			}
		}
	}

	return geom;
}

static LWGEOM*
parse_geojson_multipolygon(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom = NULL;
	int i, j, k;
	json_object* poObjPolys = NULL;

	if (!root_srid)
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTIPOLYGONTYPE, root_srid, 1, 0);
	}
	else
	{
		geom = (LWGEOM *)lwcollection_construct_empty(MULTIPOLYGONTYPE, -1, 1, 0);
	}

	poObjPolys = findMemberByName( geojson, "coordinates" );
	if ( ! poObjPolys ) 
	{
		geojson_lwerror("Unable to find 'coordinates' in GeoJSON string", 4);
		return NULL;
	}

	if( json_type_array == json_object_get_type( poObjPolys ) )
	{
		const int nPolys = json_object_array_length( poObjPolys );

		for(i = 0; i < nPolys; ++i)
		{			
			json_object* poObjPoly = json_object_array_get_idx( poObjPolys, i );

			if( json_type_array == json_object_get_type( poObjPoly ) )
			{
				LWPOLY *lwpoly = lwpoly_construct_empty(geom->srid, lwgeom_has_z(geom), lwgeom_has_m(geom));
				int nRings = json_object_array_length( poObjPoly );
				
				for(j = 0; j < nRings; ++j)
				{
					json_object* points = json_object_array_get_idx( poObjPoly, j );
					
					if( json_type_array == json_object_get_type( poObjPoly ) )
					{

						POINTARRAY *pa = ptarray_construct_empty(1, 0, 1);

						int nPoints = json_object_array_length( points );
						for ( k=0; k < nPoints; k++ )
						{
							json_object* coords = json_object_array_get_idx( points, k );
							parse_geojson_coord(coords, hasz, pa);
						}
						
						lwpoly_add_ring(lwpoly, pa);
					}
				}
				geom = (LWGEOM*)lwmpoly_add_lwpoly((LWMPOLY*)geom, lwpoly);
			}
		}
	}

	return geom;
}

static LWGEOM*
parse_geojson_geometrycollection(json_object *geojson, int *hasz,  int root_srid)
{
	LWGEOM *geom = NULL;
	int i;
	json_object* poObjGeoms = NULL;

	if (!root_srid)
	{
		geom = (LWGEOM *)lwcollection_construct_empty(COLLECTIONTYPE, root_srid, 1, 0);
	}
	else
	{
		geom = (LWGEOM *)lwcollection_construct_empty(COLLECTIONTYPE, -1, 1, 0);
	}

	poObjGeoms = findMemberByName( geojson, "geometries" );
	if ( ! poObjGeoms ) {
		geojson_lwerror("Unable to find 'geometries' in GeoJSON string", 4);
    return NULL;
  }

	if( json_type_array == json_object_get_type( poObjGeoms ) )
	{
		const int nGeoms = json_object_array_length( poObjGeoms );
		json_object* poObjGeom = NULL;
		for(i = 0; i < nGeoms; ++i )
		{
			poObjGeom = json_object_array_get_idx( poObjGeoms, i );
			geom = (LWGEOM*)lwcollection_add_lwgeom((LWCOLLECTION *)geom,
			                                        parse_geojson(poObjGeom, hasz, root_srid));
		}
	}

	return geom;
}

static LWGEOM*
parse_geojson(json_object *geojson, int *hasz,  int root_srid)
{
	json_object* type = NULL;
	const char* name;

	if( NULL == geojson ) {
		geojson_lwerror("invalid GeoJSON representation", 2);
    return NULL;
  }

	type = findMemberByName( geojson, "type" );
	if( NULL == type ) {
		geojson_lwerror("unknown GeoJSON type", 3);
    return NULL;
  }

	name = json_object_get_string( type );

	if( strcasecmp( name, "Point" )==0 )
		return parse_geojson_point(geojson, hasz, root_srid);

	if( strcasecmp( name, "LineString" )==0 )
		return parse_geojson_linestring(geojson, hasz, root_srid);

	if( strcasecmp( name, "Polygon" )==0 )
		return parse_geojson_polygon(geojson, hasz, root_srid);

	if( strcasecmp( name, "MultiPoint" )==0 )
		return parse_geojson_multipoint(geojson, hasz, root_srid);

	if( strcasecmp( name, "MultiLineString" )==0 )
		return parse_geojson_multilinestring(geojson, hasz, root_srid);

	if( strcasecmp( name, "MultiPolygon" )==0 )
		return parse_geojson_multipolygon(geojson, hasz, root_srid);

	if( strcasecmp( name, "GeometryCollection" )==0 )
		return parse_geojson_geometrycollection(geojson, hasz, root_srid);

	lwerror("invalid GeoJson representation");
	return NULL; /* Never reach */
}

#endif /* HAVE_LIBJSON or HAVE_LIBJSON_C --} */

LWGEOM*
lwgeom_from_geojson(const char *geojson, char **srs)
{
#ifndef HAVE_LIBJSON
	*srs = NULL;
	lwerror("You need JSON-C for lwgeom_from_geojson");
	return NULL;
#else /* HAVE_LIBJSON  */

	/* size_t geojson_size = strlen(geojson); */

	LWGEOM *lwgeom;
	int hasz=LW_TRUE;
	json_tokener* jstok = NULL;
	json_object* poObj = NULL;
	json_object* poObjSrs = NULL;
	*srs = NULL;

	/* Begin to Parse json */
	jstok = json_tokener_new();
	poObj = json_tokener_parse_ex(jstok, geojson, -1);
	if( jstok->err != json_tokener_success)
	{
		char err[256];
		snprintf(err, 256, "%s (at offset %d)", json_tokener_error_desc(jstok->err), jstok->char_offset);
		json_tokener_free(jstok);
		json_object_put(poObj);
		geojson_lwerror(err, 1);
		return NULL;
	}
	json_tokener_free(jstok);

	poObjSrs = findMemberByName( poObj, "crs" );
	if (poObjSrs != NULL)
	{
		json_object* poObjSrsType = findMemberByName( poObjSrs, "type" );
		if (poObjSrsType != NULL)
		{
			json_object* poObjSrsProps = findMemberByName( poObjSrs, "properties" );
			json_object* poNameURL = findMemberByName( poObjSrsProps, "name" );
			const char* pszName = json_object_get_string( poNameURL );
      *srs = lwalloc(strlen(pszName) + 1);
      strcpy(*srs, pszName);
		}
	}

	lwgeom = parse_geojson(poObj, &hasz, 0);
  json_object_put(poObj);

	lwgeom_add_bbox(lwgeom);

	if (!hasz)
	{
		LWGEOM *tmp = lwgeom_force_2d(lwgeom);
		lwgeom_free(lwgeom);
		lwgeom = tmp;

		LWDEBUG(2, "geom_from_geojson called.");
	}

  return lwgeom;
#endif /* HAVE_LIBJSON } */
}


