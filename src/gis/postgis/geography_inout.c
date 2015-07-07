/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2009-2011 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"

#include "../postgis_config.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "utils/elog.h"
#include "utils/array.h"
#include "utils/builtins.h"  /* for pg_atoi */
#include "lib/stringinfo.h"  /* For binary input */
#include "catalog/pg_type.h" /* for CSTRINGOID */

#include "liblwgeom.h"         /* For standard geometry types. */
#include "lwgeom_pg.h"       /* For debugging macros. */
#include "geography.h"	     /* For utility functions. */
#include "lwgeom_export.h"   /* For export functions. */
#include "lwgeom_transform.h"

Datum geography_in(PG_FUNCTION_ARGS);
Datum geography_out(PG_FUNCTION_ARGS);

Datum geography_as_text(PG_FUNCTION_ARGS);
Datum geography_from_text(PG_FUNCTION_ARGS);
Datum geography_as_geojson(PG_FUNCTION_ARGS);
Datum geography_as_gml(PG_FUNCTION_ARGS);
Datum geography_as_kml(PG_FUNCTION_ARGS);
Datum geography_as_svg(PG_FUNCTION_ARGS);
Datum geography_from_binary(PG_FUNCTION_ARGS);
Datum geography_from_geometry(PG_FUNCTION_ARGS);
Datum geometry_from_geography(PG_FUNCTION_ARGS);
Datum geography_send(PG_FUNCTION_ARGS);
Datum geography_recv(PG_FUNCTION_ARGS);

GSERIALIZED* gserialized_geography_from_lwgeom(LWGEOM *lwgeom, int32 geog_typmod);

/**
* The geography type only support POINT, LINESTRING, POLYGON, MULTI* variants
* of same, and GEOMETRYCOLLECTION. If the input type is not one of those, shut
* down the query.
*/
void geography_valid_type(uint8_t type)
{
	if ( ! (
	            type == POINTTYPE ||
	            type == LINETYPE ||
	            type == POLYGONTYPE ||
	            type == MULTIPOINTTYPE ||
	            type == MULTILINETYPE ||
	            type == MULTIPOLYGONTYPE ||
	            type == COLLECTIONTYPE
	        ) )
	{
		ereport(ERROR, (
		            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		            errmsg("Geography type does not support %s", lwtype_name(type) )));

	}
}

GSERIALIZED* gserialized_geography_from_lwgeom(LWGEOM *lwgeom, int32 geog_typmod)
{
	GSERIALIZED *g_ser = NULL;

	/* Set geodetic flag */
	lwgeom_set_geodetic(lwgeom, true);

	/* Check that this is a type we can handle */
	geography_valid_type(lwgeom->type);

	/* Force the geometry to have valid geodetic coordinate range. */
	lwgeom_nudge_geodetic(lwgeom);
	if ( lwgeom_force_geodetic(lwgeom) == LW_TRUE )
	{
		ereport(NOTICE, (
		        errmsg_internal("Coordinate values were coerced into range [-180 -90, 180 90] for GEOGRAPHY" ))
		);
	}

	/* Force default SRID to the default */
	if ( (int)lwgeom->srid <= 0 )
		lwgeom->srid = SRID_DEFAULT;

	/*
	** Serialize our lwgeom and set the geodetic flag so subsequent
	** functions do the right thing.
	*/
	g_ser = geography_serialize(lwgeom);

	/* Check for typmod agreement */
	if ( geog_typmod >= 0 )
	{
		g_ser = postgis_valid_typmod(g_ser, geog_typmod);
		POSTGIS_DEBUG(3, "typmod and geometry were consistent");
	}
	else
	{
		POSTGIS_DEBUG(3, "typmod was -1");
	}

	return g_ser;
}


/*
** geography_in(cstring) returns *GSERIALIZED
*/
PG_FUNCTION_INFO_V1(geography_in);
Datum geography_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	/* Datum geog_oid = PG_GETARG_OID(1); Not needed. */
	int32 geog_typmod = -1;
	LWGEOM_PARSER_RESULT lwg_parser_result;
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g_ser = NULL;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) ) {
		geog_typmod = PG_GETARG_INT32(2);
	}

	lwgeom_parser_result_init(&lwg_parser_result);

	/* Empty string. */
	if ( str[0] == '\0' )
		ereport(ERROR,(errmsg("parse error - invalid geometry")));

	/* WKB? Let's find out. */
	if ( str[0] == '0' )
	{
		/* TODO: 20101206: No parser checks! This is inline with current 1.5 behavior, but needs discussion */
		lwgeom = lwgeom_from_hexwkb(str, LW_PARSER_CHECK_NONE);
		/* Error out if something went sideways */
		if ( ! lwgeom ) 
			ereport(ERROR,(errmsg("parse error - invalid geometry")));
	}
	/* WKT then. */
	else
	{
		if ( lwgeom_parse_wkt(&lwg_parser_result, str, LW_PARSER_CHECK_ALL) == LW_FAILURE )
			PG_PARSER_ERROR(lwg_parser_result);

		lwgeom = lwg_parser_result.geom;
	}

	/* Error on any SRID != default */
	srid_is_latlong(fcinfo, lwgeom->srid);
	
	/* Convert to gserialized */
	g_ser = gserialized_geography_from_lwgeom(lwgeom, geog_typmod);

	/* Clean up temporary object */
	lwgeom_free(lwgeom);


	PG_RETURN_POINTER(g_ser);
}

/*
** geography_out(*GSERIALIZED) returns cstring
*/
PG_FUNCTION_INFO_V1(geography_out);
Datum geography_out(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	char *hexwkb;

	g = PG_GETARG_GSERIALIZED_P(0);
	lwgeom = lwgeom_from_gserialized(g);
	hexwkb = lwgeom_to_hexwkb(lwgeom, WKB_EXTENDED, 0);
	lwgeom_free(lwgeom);

	PG_RETURN_CSTRING(hexwkb);
}


/*
** geography_as_gml(*GSERIALIZED) returns text
*/
PG_FUNCTION_INFO_V1(geography_as_gml);
Datum geography_as_gml(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	char *gml;
	text *result;
	int version;
	char *srs;
	int srid = SRID_DEFAULT;
	int precision = DBL_DIG;
	int option=0;
	int lwopts = LW_GML_IS_DIMS;
	static const char *default_prefix = "gml:";
	const char *prefix = default_prefix;
	char *prefix_buf = "";
	text *prefix_text, *id_text = NULL;
	const char *id=NULL;
	char *id_buf;


	/* Get the version */
	version = PG_GETARG_INT32(0);
	if ( version != 2 && version != 3 )
	{
		elog(ERROR, "Only GML 2 and GML 3 are supported");
		PG_RETURN_NULL();
	}

	/* Get the geography */
	if ( PG_ARGISNULL(1) ) PG_RETURN_NULL();
	g = PG_GETARG_GSERIALIZED_P(1);

	/* Convert to lwgeom so we can run the old functions */
	lwgeom = lwgeom_from_gserialized(g);

	/* Retrieve precision if any (default is max) */
	if (PG_NARGS() >2 && !PG_ARGISNULL(2))
	{
		precision = PG_GETARG_INT32(2);
		/* TODO: leave this to liblwgeom */
		if ( precision > DBL_DIG )
			precision = DBL_DIG;
		else if ( precision < 0 ) precision = 0;
	}

	/* retrieve option */
	if (PG_NARGS() >3 && !PG_ARGISNULL(3))
		option = PG_GETARG_INT32(3);


	/* retrieve prefix */
	if (PG_NARGS() >4 && !PG_ARGISNULL(4))
	{
		prefix_text = PG_GETARG_TEXT_P(4);
		if ( VARSIZE(prefix_text)-VARHDRSZ == 0 )
		{
			prefix = "";
		}
		else
		{
			/* +2 is one for the ':' and one for term null */
			prefix_buf = palloc(VARSIZE(prefix_text)-VARHDRSZ+2);
			memcpy(prefix_buf, VARDATA(prefix_text),
			       VARSIZE(prefix_text)-VARHDRSZ);
			/* add colon and null terminate */
			prefix_buf[VARSIZE(prefix_text)-VARHDRSZ] = ':';
			prefix_buf[VARSIZE(prefix_text)-VARHDRSZ+1] = '\0';
			prefix = prefix_buf;
		}
	}

	/* retrieve id */
	if (PG_NARGS() >5 && !PG_ARGISNULL(5))
	{
		id_text = PG_GETARG_TEXT_P(5);
		if ( VARSIZE(id_text)-VARHDRSZ == 0 )
		{
			id = "";
		}
		else
		{
			id_buf = palloc(VARSIZE(id_text)-VARHDRSZ+1);
			memcpy(id_buf, VARDATA(id_text), VARSIZE(id_text)-VARHDRSZ);
			prefix_buf[VARSIZE(id_text)-VARHDRSZ+1] = '\0';
			id = id_buf;
		}
	}

	if (option & 1) srs = getSRSbySRID(srid, false);
	else srs = getSRSbySRID(srid, true);
	if (!srs)
	{
		elog(ERROR, "SRID %d unknown in spatial_ref_sys table", SRID_DEFAULT);
		PG_RETURN_NULL();
	}

	/* Revert lat/lon only with long SRS */
	if (option & 1) lwopts |= LW_GML_IS_DEGREE;
	if (option & 2) lwopts &= ~LW_GML_IS_DIMS; 

	if (version == 2)
		gml = lwgeom_to_gml2(lwgeom, srs, precision, prefix);
	else
		gml = lwgeom_to_gml3(lwgeom, srs, precision, lwopts, prefix, id);

    lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(g, 1);

	/* Return null on null */
	if ( ! gml ) 
		PG_RETURN_NULL();

	/* Turn string result into text for return */
	result = cstring2text(gml);
	lwfree(gml);

	PG_RETURN_TEXT_P(result);
}


/*
** geography_as_kml(*GSERIALIZED) returns text
*/
PG_FUNCTION_INFO_V1(geography_as_kml);
Datum geography_as_kml(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g = NULL;
	LWGEOM *lwgeom = NULL;
	char *kml;
	text *result;
	int version;
	int precision = DBL_DIG;
	static const char *default_prefix = "";
	char *prefixbuf;
	const char* prefix = default_prefix;
	text *prefix_text;


	/* Get the version */
	version = PG_GETARG_INT32(0);
	if ( version != 2)
	{
		elog(ERROR, "Only KML 2 is supported");
		PG_RETURN_NULL();
	}

	/* Get the geometry */
	if ( PG_ARGISNULL(1) ) PG_RETURN_NULL();
	g = PG_GETARG_GSERIALIZED_P(1);

	/* Convert to lwgeom so we can run the old functions */
	lwgeom = lwgeom_from_gserialized(g);

	/* Retrieve precision if any (default is max) */
	if (PG_NARGS() >2 && !PG_ARGISNULL(2))
	{
		precision = PG_GETARG_INT32(2);
		/* TODO: leave this to liblwgeom */
		if ( precision > DBL_DIG )
			precision = DBL_DIG;
		else if ( precision < 0 ) precision = 0;
	}

	/* retrieve prefix */
	if (PG_NARGS() >3 && !PG_ARGISNULL(3))
	{
		prefix_text = PG_GETARG_TEXT_P(3);
		if ( VARSIZE(prefix_text)-VARHDRSZ == 0 )
		{
			prefix = "";
		}
		else
		{
			/* +2 is one for the ':' and one for term null */
			prefixbuf = palloc(VARSIZE(prefix_text)-VARHDRSZ+2);
			memcpy(prefixbuf, VARDATA(prefix_text),
			       VARSIZE(prefix_text)-VARHDRSZ);
			/* add colon and null terminate */
			prefixbuf[VARSIZE(prefix_text)-VARHDRSZ] = ':';
			prefixbuf[VARSIZE(prefix_text)-VARHDRSZ+1] = '\0';
			prefix = prefixbuf;
		}
	}

	kml = lwgeom_to_kml2(lwgeom, precision, prefix);

    lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(g, 1);

	if ( ! kml )
		PG_RETURN_NULL();

	result = cstring2text(kml);
	lwfree(kml);

	PG_RETURN_TEXT_P(result);
}


/*
** geography_as_svg(*GSERIALIZED) returns text
*/
PG_FUNCTION_INFO_V1(geography_as_svg);
Datum geography_as_svg(PG_FUNCTION_ARGS)
{
	GSERIALIZED *g = NULL;
	LWGEOM *lwgeom = NULL;
	char *svg;
	text *result;
	int relative = 0;
	int precision=DBL_DIG;

	if ( PG_ARGISNULL(0) ) PG_RETURN_NULL();

	g = PG_GETARG_GSERIALIZED_P(0);

	/* Convert to lwgeom so we can run the old functions */
	lwgeom = lwgeom_from_gserialized(g);

	/* check for relative path notation */
	if ( PG_NARGS() > 1 && ! PG_ARGISNULL(1) )
		relative = PG_GETARG_INT32(1) ? 1:0;

	if ( PG_NARGS() > 2 && ! PG_ARGISNULL(2) )
	{
		precision = PG_GETARG_INT32(2);
		/* TODO: leave this to liblwgeom */
		if ( precision > DBL_DIG )
			precision = DBL_DIG;
		else if ( precision < 0 ) precision = 0;
	}

	svg = lwgeom_to_svg(lwgeom, precision, relative);
	
    lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(g, 0);

	result = cstring2text(svg);
	lwfree(svg);

	PG_RETURN_TEXT_P(result);
}


/*
** geography_as_geojson(*GSERIALIZED) returns text
*/
PG_FUNCTION_INFO_V1(geography_as_geojson);
Datum geography_as_geojson(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	char *geojson;
	text *result;
	int version;
	int option = 0;
	int has_bbox = 0;
	int precision = DBL_DIG;
	char * srs = NULL;

	/* Get the version */
	version = PG_GETARG_INT32(0);
	if ( version != 1)
	{
		elog(ERROR, "Only GeoJSON 1 is supported");
		PG_RETURN_NULL();
	}

	/* Get the geography */
	if (PG_ARGISNULL(1) ) PG_RETURN_NULL();
	g = PG_GETARG_GSERIALIZED_P(1);

	/* Convert to lwgeom so we can run the old functions */
	lwgeom = lwgeom_from_gserialized(g);

	/* Retrieve precision if any (default is max) */
	if (PG_NARGS() >2 && !PG_ARGISNULL(2))
	{
		precision = PG_GETARG_INT32(2);
		/* TODO: leave this to liblwgeom */
		if ( precision > DBL_DIG )
			precision = DBL_DIG;
		else if ( precision < 0 ) precision = 0;
	}

	/* Retrieve output option
	 * 0 = without option (default)
	 * 1 = bbox
	 * 2 = short crs
	 * 4 = long crs
	 */
	if (PG_NARGS() >3 && !PG_ARGISNULL(3))
		option = PG_GETARG_INT32(3);

	if (option & 2 || option & 4)
	{
		/* Geography only handle srid SRID_DEFAULT */
		if (option & 2) srs = getSRSbySRID(SRID_DEFAULT, true);
		if (option & 4) srs = getSRSbySRID(SRID_DEFAULT, false);

		if (!srs)
		{
			elog(ERROR, "SRID SRID_DEFAULT unknown in spatial_ref_sys table");
			PG_RETURN_NULL();
		}
	}

	if (option & 1) has_bbox = 1;

	geojson = lwgeom_to_geojson(lwgeom, srs, precision, has_bbox);
    lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(g, 1);
	if (srs) pfree(srs);

	result = cstring2text(geojson);
	lwfree(geojson);

	PG_RETURN_TEXT_P(result);
}


/*
** geography_from_text(*char) returns *GSERIALIZED
**
** Convert text (varlena) to cstring and then call geography_in().
*/
PG_FUNCTION_INFO_V1(geography_from_text);
Datum geography_from_text(PG_FUNCTION_ARGS)
{
	LWGEOM_PARSER_RESULT lwg_parser_result;
	GSERIALIZED *g_ser = NULL;
	text *wkt_text = PG_GETARG_TEXT_P(0);
	
	/* Extract the cstring from the varlena */
	char *wkt = text2cstring(wkt_text);

	/* Pass the cstring to the input parser, and magic occurs! */	
	if ( lwgeom_parse_wkt(&lwg_parser_result, wkt, LW_PARSER_CHECK_ALL) == LW_FAILURE )
		PG_PARSER_ERROR(lwg_parser_result);

	/* Clean up string */
	pfree(wkt);
	g_ser = gserialized_geography_from_lwgeom(lwg_parser_result.geom, -1);

	/* Clean up temporary object */
	lwgeom_free(lwg_parser_result.geom);

	PG_RETURN_POINTER(g_ser);
}

/*
** geography_from_binary(*char) returns *GSERIALIZED
*/
PG_FUNCTION_INFO_V1(geography_from_binary);
Datum geography_from_binary(PG_FUNCTION_ARGS)
{
	char *wkb_bytea = (char*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	GSERIALIZED *gser = NULL;
	size_t wkb_size = VARSIZE(wkb_bytea);
	uint8_t *wkb = (uint8_t*)VARDATA(wkb_bytea);
	LWGEOM *lwgeom = lwgeom_from_wkb(wkb, wkb_size, LW_PARSER_CHECK_NONE);
	
	if ( ! lwgeom )
		lwerror("Unable to parse WKB");
 		
	gser = gserialized_geography_from_lwgeom(lwgeom, -1);
	lwgeom_free(lwgeom);
	PG_RETURN_POINTER(gser);
}


PG_FUNCTION_INFO_V1(geography_from_geometry);
Datum geography_from_geometry(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P_COPY(0);
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g_ser = NULL;

	geography_valid_type(gserialized_get_type(geom));

	lwgeom = lwgeom_from_gserialized(geom);

	/* Force default SRID */
	if ( (int)lwgeom->srid <= 0 )
	{
		lwgeom->srid = SRID_DEFAULT;
	}

	/* Error on any SRID != default */
	srid_is_latlong(fcinfo, lwgeom->srid);

	/* Force the geometry to have valid geodetic coordinate range. */
	lwgeom_nudge_geodetic(lwgeom);
	if ( lwgeom_force_geodetic(lwgeom) == LW_TRUE )
	{
		ereport(NOTICE, (
		        errmsg_internal("Coordinate values were coerced into range [-180 -90, 180 90] for GEOGRAPHY" ))
		);
	}

	/*
	** Serialize our lwgeom and set the geodetic flag so subsequent
	** functions do the right thing.
	*/
	lwgeom_set_geodetic(lwgeom, true);
	/* Recalculate the boxes after re-setting the geodetic bit */
	lwgeom_drop_bbox(lwgeom);
	lwgeom_add_bbox(lwgeom);
	g_ser = geography_serialize(lwgeom);

	/*
	** Replace the unaligned lwgeom with a new aligned one based on GSERIALIZED.
	*/
	lwgeom_free(lwgeom);

	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_POINTER(g_ser);

}

PG_FUNCTION_INFO_V1(geometry_from_geography);
Datum geometry_from_geography(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *ret = NULL;
	GSERIALIZED *g_ser = PG_GETARG_GSERIALIZED_P(0);

	lwgeom = lwgeom_from_gserialized(g_ser);

	/* Recalculate the boxes after re-setting the geodetic bit */
	lwgeom_set_geodetic(lwgeom, false);	
	lwgeom_drop_bbox(lwgeom);
	lwgeom_add_bbox(lwgeom);

	/* We want "geometry" to think all our "geography" has an SRID, and the
	   implied SRID is the default, so we fill that in if our SRID is actually unknown. */
	if ( (int)lwgeom->srid <= 0 )
		lwgeom->srid = SRID_DEFAULT;

	ret = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	PG_RETURN_POINTER(ret);
}

PG_FUNCTION_INFO_V1(geography_recv);
Datum geography_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	int32 geog_typmod = -1;
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g_ser = NULL;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) ) {
		geog_typmod = PG_GETARG_INT32(2);
	}

	lwgeom = lwgeom_from_wkb((uint8_t*)buf->data, buf->len, LW_PARSER_CHECK_ALL);

	/* Error on any SRID != default */
	srid_is_latlong(fcinfo, lwgeom->srid);

	g_ser = gserialized_geography_from_lwgeom(lwgeom, geog_typmod);

	/* Clean up temporary object */
	lwgeom_free(lwgeom);

	/* Set cursor to the end of buffer (so the backend is happy) */
	buf->cursor = buf->len;

	PG_RETURN_POINTER(g_ser);
}


PG_FUNCTION_INFO_V1(geography_send);
Datum geography_send(PG_FUNCTION_ARGS)
{
	LWGEOM *lwgeom = NULL;
	GSERIALIZED *g = NULL;
	size_t size_result;
	uint8_t *wkb;
	bytea *result;

	g = PG_GETARG_GSERIALIZED_P(0);
	lwgeom = lwgeom_from_gserialized(g);
	wkb = lwgeom_to_wkb(lwgeom, WKB_EXTENDED, &size_result);
	lwgeom_free(lwgeom);

	result = palloc(size_result + VARHDRSZ);
	SET_VARSIZE(result, size_result + VARHDRSZ);
	memcpy(VARDATA(result), wkb, size_result);
	pfree(wkb);

	PG_RETURN_POINTER(result);
}
