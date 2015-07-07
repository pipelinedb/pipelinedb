#include "postgres.h"

#include "../postgis_config.h"

#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "access/gist.h"
#include "access/itup.h"

#include "fmgr.h"
#include "utils/elog.h"
#include "mb/pg_wchar.h"
# include "lib/stringinfo.h" /* for binary input */
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "funcapi.h"

#include "liblwgeom.h"
#include "lwgeom_pg.h"
#include "geography.h" /* for lwgeom_valid_typmod */
#include "lwgeom_transform.h"

#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h"
#endif

void elog_ERROR(const char* string);

Datum LWGEOM_in(PG_FUNCTION_ARGS);
Datum LWGEOM_out(PG_FUNCTION_ARGS);
Datum LWGEOM_to_text(PG_FUNCTION_ARGS);
Datum LWGEOM_to_bytea(PG_FUNCTION_ARGS);
Datum LWGEOM_from_bytea(PG_FUNCTION_ARGS);
Datum LWGEOM_asHEXEWKB(PG_FUNCTION_ARGS);
Datum parse_WKT_lwgeom(PG_FUNCTION_ARGS);
Datum LWGEOM_recv(PG_FUNCTION_ARGS);
Datum LWGEOM_send(PG_FUNCTION_ARGS);
Datum LWGEOM_to_latlon(PG_FUNCTION_ARGS);
Datum WKBFromLWGEOM(PG_FUNCTION_ARGS);
Datum TWKBFromLWGEOM(PG_FUNCTION_ARGS);
Datum TWKBFromLWGEOMArray(PG_FUNCTION_ARGS);


/*
 * LWGEOM_in(cstring)
 * format is '[SRID=#;]wkt|wkb'
 *  LWGEOM_in( 'SRID=99;POINT(0 0)')
 *  LWGEOM_in( 'POINT(0 0)')            --> assumes SRID=SRID_UNKNOWN
 *  LWGEOM_in( 'SRID=99;0101000000000000000000F03F000000000000004')
 *  LWGEOM_in( '0101000000000000000000F03F000000000000004')
 *  returns a GSERIALIZED object
 */
PG_FUNCTION_INFO_V1(LWGEOM_in);
Datum LWGEOM_in(PG_FUNCTION_ARGS)
{
	char *input = PG_GETARG_CSTRING(0);
	int32 geom_typmod = -1;
	char *str = input;
	LWGEOM_PARSER_RESULT lwg_parser_result;
	LWGEOM *lwgeom;
	GSERIALIZED *ret;
	int srid = 0;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) ) {
		geom_typmod = PG_GETARG_INT32(2);
	}

	lwgeom_parser_result_init(&lwg_parser_result);

	/* Empty string. */
	if ( str[0] == '\0' ) {
		ereport(ERROR,(errmsg("parse error - invalid geometry")));
		PG_RETURN_NULL();
	}

	/* Starts with "SRID=" */
	if( strncasecmp(str,"SRID=",5) == 0 )
	{
		/* Roll forward to semi-colon */
		char *tmp = str;
		while ( tmp && *tmp != ';' )
			tmp++;
		
		/* Check next character to see if we have WKB  */
		if ( tmp && *(tmp+1) == '0' )
		{
			/* Null terminate the SRID= string */
			*tmp = '\0';
			/* Set str to the start of the real WKB */
			str = tmp + 1;
			/* Move tmp to the start of the numeric part */
			tmp = input + 5;
			/* Parse out the SRID number */
			srid = atoi(tmp);
		}
	}
	
	/* WKB? Let's find out. */
	if ( str[0] == '0' )
	{
		size_t hexsize = strlen(str);
		unsigned char *wkb = bytes_from_hexbytes(str, hexsize);
		/* TODO: 20101206: No parser checks! This is inline with current 1.5 behavior, but needs discussion */
		lwgeom = lwgeom_from_wkb(wkb, hexsize/2, LW_PARSER_CHECK_NONE);
		/* If we picked up an SRID at the head of the WKB set it manually */
		if ( srid ) lwgeom_set_srid(lwgeom, srid);
		/* Add a bbox if necessary */
		if ( lwgeom_needs_bbox(lwgeom) ) lwgeom_add_bbox(lwgeom);
		pfree(wkb);
		ret = geometry_serialize(lwgeom);
		lwgeom_free(lwgeom);
	}
	/* WKT then. */
	else
	{
		if ( lwgeom_parse_wkt(&lwg_parser_result, str, LW_PARSER_CHECK_ALL) == LW_FAILURE )
		{
			PG_PARSER_ERROR(lwg_parser_result);
			PG_RETURN_NULL();
		}
		lwgeom = lwg_parser_result.geom;
		if ( lwgeom_needs_bbox(lwgeom) )
			lwgeom_add_bbox(lwgeom);		
		ret = geometry_serialize(lwgeom);
		lwgeom_parser_result_free(&lwg_parser_result);
	}

	if ( geom_typmod >= 0 )
	{
		ret = postgis_valid_typmod(ret, geom_typmod);
		POSTGIS_DEBUG(3, "typmod and geometry were consistent");
	}
	else
	{
		POSTGIS_DEBUG(3, "typmod was -1");
	}

	/* Don't free the parser result (and hence lwgeom) until we have done */
	/* the typemod check with lwgeom */
	
	PG_RETURN_POINTER(ret);

}

/*
 * LWGEOM_to_latlon(GEOMETRY, text)
 *  NOTE: Geometry must be a point.  It is assumed that the coordinates
 *        of the point are in a lat/lon projection, and they will be
 *        normalized in the output to -90-90 and -180-180.
 *
 *  The text parameter is a format string containing the format for the
 *  resulting text, similar to a date format string.  Valid tokens
 *  are "D" for degrees, "M" for minutes, "S" for seconds, and "C" for
 *  cardinal direction (NSEW).  DMS tokens may be repeated to indicate
 *  desired width and precision ("SSS.SSSS" means "  1.0023").
 *  "M", "S", and "C" are optional.  If "C" is omitted, degrees are
 *  shown with a "-" sign if south or west.  If "S" is omitted,
 *  minutes will be shown as decimal with as many digits of precision
 *  as you specify.  If "M" is omitted, degrees are shown as decimal
 *  with as many digits precision as you specify.
 *
 *  If the format string is omitted (null or 0-length) a default
 *  format will be used.
 *
 *  returns text
 */
PG_FUNCTION_INFO_V1(LWGEOM_to_latlon);
Datum LWGEOM_to_latlon(PG_FUNCTION_ARGS)
{
	/* Get the parameters */
	GSERIALIZED *pg_lwgeom = PG_GETARG_GSERIALIZED_P(0);
	text *format_text = PG_GETARG_TEXT_P(1);

	LWGEOM *lwgeom;
	char *format_str = NULL;

	char * formatted_str;
	text * formatted_text;
	char * tmp;

	/* Only supports points. */
	uint8_t geom_type = gserialized_get_type(pg_lwgeom);
	if (POINTTYPE != geom_type)
	{
		lwerror("Only points are supported, you tried type %s.", lwtype_name(geom_type));
	}
	/* Convert to LWGEOM type */
	lwgeom = lwgeom_from_gserialized(pg_lwgeom);

  if (format_text == NULL) {
    lwerror("ST_AsLatLonText: invalid format string (null");
    PG_RETURN_NULL();
  }

	format_str = text2cstring(format_text);
  assert(format_str != NULL);

  /* The input string supposedly will be in the database encoding,
     so convert to UTF-8. */
  tmp = (char *)pg_do_encoding_conversion(
    (uint8_t *)format_str, strlen(format_str), GetDatabaseEncoding(), PG_UTF8);
  assert(tmp != NULL);
  if ( tmp != format_str ) {
    pfree(format_str);
    format_str = tmp;
  }

	/* Produce the formatted string. */
	formatted_str = lwpoint_to_latlon((LWPOINT *)lwgeom, format_str);
  assert(formatted_str != NULL);
  pfree(format_str);

  /* Convert the formatted string from UTF-8 back to database encoding. */
  tmp = (char *)pg_do_encoding_conversion(
    (uint8_t *)formatted_str, strlen(formatted_str),
    PG_UTF8, GetDatabaseEncoding());
  assert(tmp != NULL);
  if ( tmp != formatted_str) {
    pfree(formatted_str);
    formatted_str = tmp;
  }

	/* Convert to the postgres output string type. */
	formatted_text = cstring2text(formatted_str);
  pfree(formatted_str);

	PG_RETURN_POINTER(formatted_text);
}

/*
 * LWGEOM_out(lwgeom) --> cstring
 * output is 'SRID=#;<wkb in hex form>'
 * ie. 'SRID=-99;0101000000000000000000F03F0000000000000040'
 * WKB is machine endian
 * if SRID=-1, the 'SRID=-1;' will probably not be present.
 */
PG_FUNCTION_INFO_V1(LWGEOM_out);
Datum LWGEOM_out(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom;
	char *hexwkb;
	size_t hexwkb_size;

	lwgeom = lwgeom_from_gserialized(geom);
	hexwkb = lwgeom_to_hexwkb(lwgeom, WKB_EXTENDED, &hexwkb_size);
	lwgeom_free(lwgeom);
	
	PG_RETURN_CSTRING(hexwkb);
}

/*
 * AsHEXEWKB(geom, string)
 */
PG_FUNCTION_INFO_V1(LWGEOM_asHEXEWKB);
Datum LWGEOM_asHEXEWKB(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom;
	char *hexwkb;
	size_t hexwkb_size;
	uint8_t variant = 0;
	text *result;
	text *type;
	size_t text_size;

	/* If user specified endianness, respect it */
	if ( (PG_NARGS()>1) && (!PG_ARGISNULL(1)) )
	{
		type = PG_GETARG_TEXT_P(1);

		if  ( ! strncmp(VARDATA(type), "xdr", 3) ||
		      ! strncmp(VARDATA(type), "XDR", 3) )
		{
			variant = variant | WKB_XDR;
		}
		else
		{
			variant = variant | WKB_NDR;
		}
	}

	/* Create WKB hex string */
	lwgeom = lwgeom_from_gserialized(geom);
	hexwkb = lwgeom_to_hexwkb(lwgeom, variant | WKB_EXTENDED, &hexwkb_size);
	lwgeom_free(lwgeom);
	
	/* Prepare the PgSQL text return type */
	text_size = hexwkb_size - 1 + VARHDRSZ;
	result = palloc(text_size);
	memcpy(VARDATA(result), hexwkb, hexwkb_size - 1);
	SET_VARSIZE(result, text_size);
	
	/* Clean up and return */
	pfree(hexwkb);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_TEXT_P(result);
}


/*
 * LWGEOM_to_text(lwgeom) --> text
 * output is 'SRID=#;<wkb in hex form>'
 * ie. 'SRID=-99;0101000000000000000000F03F0000000000000040'
 * WKB is machine endian
 * if SRID=-1, the 'SRID=-1;' will probably not be present.
 */
PG_FUNCTION_INFO_V1(LWGEOM_to_text);
Datum LWGEOM_to_text(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom;
	char *hexwkb;
	size_t hexwkb_size;
	text *result;

	/* Generate WKB hex text */
	lwgeom = lwgeom_from_gserialized(geom);
	hexwkb = lwgeom_to_hexwkb(lwgeom, WKB_EXTENDED, &hexwkb_size);
	lwgeom_free(lwgeom);
	
	/* Copy into text obect */
	result = cstring2text(hexwkb);
	pfree(hexwkb);
	
	/* Clean up and return */
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_TEXT_P(result);
}

/*
 * LWGEOMFromEWKB(wkb,  [SRID] )
 * NOTE: wkb is in *binary* not hex form.
 *
 * NOTE: this function parses EWKB (extended form)
 *       which also contains SRID info. 
 */
PG_FUNCTION_INFO_V1(LWGEOMFromEWKB);
Datum LWGEOMFromEWKB(PG_FUNCTION_ARGS)
{
	bytea *bytea_wkb = (bytea*)PG_GETARG_BYTEA_P(0);
	int32 srid = 0;
	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);
	
	lwgeom = lwgeom_from_wkb(wkb, VARSIZE(bytea_wkb)-VARHDRSZ, LW_PARSER_CHECK_ALL);
	
	if (  ( PG_NARGS()>1) && ( ! PG_ARGISNULL(1) ))
	{
		srid = PG_GETARG_INT32(1);
		lwgeom_set_srid(lwgeom, srid);
	}

	if ( lwgeom_needs_bbox(lwgeom) )
		lwgeom_add_bbox(lwgeom);

	geom = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(bytea_wkb, 0);
	PG_RETURN_POINTER(geom);
}
/*
 * LWGEOMFromTWKB(wkb)
 * NOTE: twkb is in *binary* not hex form.
 *
 */
PG_FUNCTION_INFO_V1(LWGEOMFromTWKB);
Datum LWGEOMFromTWKB(PG_FUNCTION_ARGS)
{
	bytea *bytea_twkb = (bytea*)PG_GETARG_BYTEA_P(0);
	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	uint8_t *twkb = (uint8_t*)VARDATA(bytea_twkb);
	
	lwgeom = lwgeom_from_twkb(twkb, VARSIZE(bytea_twkb)-VARHDRSZ, LW_PARSER_CHECK_ALL);

	if ( lwgeom_needs_bbox(lwgeom) )
		lwgeom_add_bbox(lwgeom);

	geom = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);
	PG_FREE_IF_COPY(bytea_twkb, 0);
	PG_RETURN_POINTER(geom);
}

/*
 * WKBFromLWGEOM(lwgeom) --> wkb
 * this will have no 'SRID=#;'
 */
PG_FUNCTION_INFO_V1(WKBFromLWGEOM);
Datum WKBFromLWGEOM(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	LWGEOM *lwgeom;
	uint8_t *wkb;
	size_t wkb_size;
	uint8_t variant = 0;
 	bytea *result;
	text *type;
	/* If user specified endianness, respect it */
	if ( (PG_NARGS()>1) && (!PG_ARGISNULL(1)) )
	{
		type = PG_GETARG_TEXT_P(1);

		if  ( ! strncmp(VARDATA(type), "xdr", 3) ||
		      ! strncmp(VARDATA(type), "XDR", 3) )
		{
			variant = variant | WKB_XDR;
		}
		else
		{
			variant = variant | WKB_NDR;
		}
	}
	wkb_size= VARSIZE(geom) - VARHDRSZ;
	/* Create WKB hex string */
	lwgeom = lwgeom_from_gserialized(geom);

	wkb = lwgeom_to_wkb(lwgeom, variant | WKB_EXTENDED , &wkb_size);
	lwgeom_free(lwgeom);
	
	/* Prepare the PgSQL text return type */
	result = palloc(wkb_size + VARHDRSZ);
	memcpy(VARDATA(result), wkb, wkb_size);
	SET_VARSIZE(result, wkb_size+VARHDRSZ);
	
	/* Clean up and return */
	pfree(wkb);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(TWKBFromLWGEOM);
Datum TWKBFromLWGEOM(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom;
	LWGEOM *lwgeom;
	uint8_t *twkb;
	size_t twkb_size;
	uint8_t variant = 0;
 	bytea *result;
	srs_precision sp;
	
	/*check for null input since we cannot have the sql-function as strict. 
	That is because we use null as default for optional ID*/	
	if ( PG_ARGISNULL(0) ) PG_RETURN_NULL();
	
	geom = PG_GETARG_GSERIALIZED_P(0);

	/* Read sensible precision defaults (about one meter) given the srs */
	sp = srid_axis_precision(fcinfo, gserialized_get_srid(geom), TWKB_DEFAULT_PRECISION);
	
	/* If user specified XY precision, use it */
	if ( PG_NARGS() > 1 && ! PG_ARGISNULL(1) )
		sp.precision_xy = PG_GETARG_INT32(1);

	/* If user specified Z precision, use it */
	if ( PG_NARGS() > 2 && ! PG_ARGISNULL(2) )
		sp.precision_z = PG_GETARG_INT32(2);

	/* If user specified M precision, use it */
	if ( PG_NARGS() > 3 && ! PG_ARGISNULL(3) )
		sp.precision_m = PG_GETARG_INT32(3);

	/* We don't permit ids for single geoemtries */
	variant = variant & ~TWKB_ID;

	/* If user wants registered twkb sizes */
	if ( PG_NARGS() > 4 && ! PG_ARGISNULL(4) && PG_GETARG_BOOL(4) )
		variant |= TWKB_SIZE;
	
	/* If user wants bounding boxes */
	if ( PG_NARGS() > 5 && ! PG_ARGISNULL(5) && PG_GETARG_BOOL(5) )
		variant |= TWKB_BBOX;

	/* Create TWKB binary string */
	lwgeom = lwgeom_from_gserialized(geom);
	twkb = lwgeom_to_twkb(lwgeom, variant, sp.precision_xy, sp.precision_z, sp.precision_m, &twkb_size);
	lwgeom_free(lwgeom);
	
	/* Prepare the PgSQL text return type */
	result = palloc(twkb_size + VARHDRSZ);
	memcpy(VARDATA(result), twkb, twkb_size);
	SET_VARSIZE(result, twkb_size + VARHDRSZ);
	
	pfree(twkb);
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_BYTEA_P(result);
}


PG_FUNCTION_INFO_V1(TWKBFromLWGEOMArray);
Datum TWKBFromLWGEOMArray(PG_FUNCTION_ARGS)
{
	ArrayType *arr_geoms = NULL;
	ArrayType *arr_ids = NULL;
	int num_geoms, num_ids, i = 0;

	ArrayIterator iter_geoms, iter_ids;
	bool null_geom, null_id;
	Datum val_geom, val_id;

	int is_homogeneous = true;
	int subtype = 0;
	LWCOLLECTION *col = NULL;
	int64_t *idlist = NULL;
	uint8_t variant = 0;

	srs_precision sp;
	uint8_t *twkb;
	size_t twkb_size;
 	bytea *result;

	/* The first two arguments are required */
	if ( PG_NARGS() < 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1) ) 
		PG_RETURN_NULL();

	arr_geoms = PG_GETARG_ARRAYTYPE_P(0);
	arr_ids = PG_GETARG_ARRAYTYPE_P(1);

	num_geoms = ArrayGetNItems(ARR_NDIM(arr_geoms), ARR_DIMS(arr_geoms));
	num_ids = ArrayGetNItems(ARR_NDIM(arr_ids), ARR_DIMS(arr_ids));
	
	if ( num_geoms != num_ids )
	{
		elog(ERROR, "size of geometry[] and integer[] arrays must match");
		PG_RETURN_NULL();
	}

	/* Loop through array and build a collection of geometry and */
	/* a simple array of ids. If either side is NULL, skip it */

#if POSTGIS_PGSQL_VERSION >= 95	
	iter_geoms = array_create_iterator(arr_geoms, 0, NULL);
	iter_ids = array_create_iterator(arr_ids, 0, NULL);
#else
	iter_geoms = array_create_iterator(arr_geoms, 0);
	iter_ids = array_create_iterator(arr_ids, 0);
#endif

	while( array_iterate(iter_geoms, &val_geom, &null_geom) && 
	       array_iterate(iter_ids, &val_id, &null_id) )
	{
		LWGEOM *geom;
		int32_t uid;

		if ( null_geom || null_id )
		{
			elog(NOTICE, "ST_AsTWKB skipping NULL entry at position %d", i);
			continue;
		}

		geom = lwgeom_from_gserialized((GSERIALIZED*)DatumGetPointer(val_geom));
		uid = DatumGetInt64(val_id);

		/* Construct collection/idlist first time through */
		if ( ! col )
			col = lwcollection_construct_empty(COLLECTIONTYPE, lwgeom_get_srid(geom), lwgeom_has_z(geom), lwgeom_has_m(geom));
		if ( ! idlist ) 
			idlist = palloc0(num_geoms * sizeof(int64_t));

		/* Store the values */
		lwcollection_add_lwgeom(col, geom);
		idlist[i++] = uid;
		
		/* Grab the geometry type and note if all geometries share it */
		/* If so, we can make this a homogeneous collection and save some space */
		if ( lwgeom_get_type(geom) != subtype && subtype )
		{
			is_homogeneous = false;
		}
		else
		{
			subtype = lwgeom_get_type(geom);
		}

	}
	array_free_iterator(iter_geoms);
	array_free_iterator(iter_ids);
	
	if ( is_homogeneous )
	{
		col->type = lwtype_get_collectiontype(subtype);
	}

	/* Read sensible precision defaults (about one meter) given the srs */
	sp = srid_axis_precision(fcinfo, lwgeom_get_srid(lwcollection_as_lwgeom(col)), TWKB_DEFAULT_PRECISION);
	
	/* If user specified XY precision, use it */
	if ( PG_NARGS() > 2 && ! PG_ARGISNULL(2) )
		sp.precision_xy = PG_GETARG_INT32(2);

	/* If user specified Z precision, use it */
	if ( PG_NARGS() > 3 && ! PG_ARGISNULL(3) )
		sp.precision_z = PG_GETARG_INT32(3);

	/* If user specified M precision, use it */
	if ( PG_NARGS() > 4 && ! PG_ARGISNULL(4) )
		sp.precision_m = PG_GETARG_INT32(4);

	/* We are building an ID'ed output */
	variant = TWKB_ID;
	
	/* If user wants registered twkb sizes */
	if ( PG_NARGS() > 5 && ! PG_ARGISNULL(5) && PG_GETARG_BOOL(5) )
		variant |= TWKB_SIZE;
	
	/* If user wants bounding boxes */
	if ( PG_NARGS() > 6 && ! PG_ARGISNULL(6) && PG_GETARG_BOOL(6) )
		variant |= TWKB_BBOX;

	/* Write out the TWKB */
	twkb = lwgeom_to_twkb_with_idlist(lwcollection_as_lwgeom(col), 
	                                  idlist, variant, 
	                                  sp.precision_xy, sp.precision_z, sp.precision_m, 
	                                  &twkb_size);
					  
	/* Convert to a bytea return type */
	result = palloc(twkb_size + VARHDRSZ);
	memcpy(VARDATA(result), twkb, twkb_size);
	SET_VARSIZE(result, twkb_size + VARHDRSZ);
	
	/* Clean up */
	pfree(twkb);
	pfree(idlist);
	lwcollection_free(col);
	PG_FREE_IF_COPY(arr_geoms, 0);
	PG_FREE_IF_COPY(arr_ids, 1);
	
	PG_RETURN_BYTEA_P(result);
}


/* puts a bbox inside the geometry */
PG_FUNCTION_INFO_V1(LWGEOM_addBBOX);
Datum LWGEOM_addBBOX(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);
	GSERIALIZED *result;
	LWGEOM *lwgeom;

	lwgeom = lwgeom_from_gserialized(geom);
	lwgeom_add_bbox(lwgeom);
	result = geometry_serialize(lwgeom);
	
	PG_FREE_IF_COPY(geom, 0);
	PG_RETURN_POINTER(result);
}

/* removes a bbox from a geometry */
PG_FUNCTION_INFO_V1(LWGEOM_dropBBOX);
Datum LWGEOM_dropBBOX(PG_FUNCTION_ARGS)
{
	GSERIALIZED *geom = PG_GETARG_GSERIALIZED_P(0);

	/* No box? we're done already! */
	if ( ! gserialized_has_bbox(geom) )
		PG_RETURN_POINTER(geom);
	
	PG_RETURN_POINTER(gserialized_drop_gidx(geom));
}


/* for the wkt parser */
void elog_ERROR(const char* string)
{
	elog(ERROR, "%s", string);
}

/*
* This just does the same thing as the _in function,
* except it has to handle a 'text' input. First
* unwrap the text into a cstring, then call
* geometry_in
*/
PG_FUNCTION_INFO_V1(parse_WKT_lwgeom);
Datum parse_WKT_lwgeom(PG_FUNCTION_ARGS)
{
	text *wkt_text = PG_GETARG_TEXT_P(0);
	char *wkt;
	Datum result;

	/* Unwrap the PgSQL text type into a cstring */
	wkt = text2cstring(wkt_text); 
	
	/* Now we call over to the geometry_in function */
	result = DirectFunctionCall1(LWGEOM_in, CStringGetDatum(wkt));

	/* Return null on null */
	if ( ! result ) 
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}


/*
 * This function must advance the StringInfo.cursor pointer
 * and leave it at the end of StringInfo.buf. If it fails
 * to do so the backend will raise an exception with message:
 * ERROR:  incorrect binary data format in bind parameter #
 *
 */
PG_FUNCTION_INFO_V1(LWGEOM_recv);
Datum LWGEOM_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	int32 geom_typmod = -1;
	GSERIALIZED *geom;
	LWGEOM *lwgeom;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) ) {
		geom_typmod = PG_GETARG_INT32(2);
	}
	
	lwgeom = lwgeom_from_wkb((uint8_t*)buf->data, buf->len, LW_PARSER_CHECK_ALL);

	if ( lwgeom_needs_bbox(lwgeom) )
		lwgeom_add_bbox(lwgeom);

	/* Set cursor to the end of buffer (so the backend is happy) */
	buf->cursor = buf->len;

	geom = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	if ( geom_typmod >= 0 )
	{
		geom = postgis_valid_typmod(geom, geom_typmod);
		POSTGIS_DEBUG(3, "typmod and geometry were consistent");
	}
	else
	{
		POSTGIS_DEBUG(3, "typmod was -1");
	}

	
	PG_RETURN_POINTER(geom);
}



PG_FUNCTION_INFO_V1(LWGEOM_send);
Datum LWGEOM_send(PG_FUNCTION_ARGS)
{
	POSTGIS_DEBUG(2, "LWGEOM_send called");

	PG_RETURN_POINTER(
	  DatumGetPointer(
	    DirectFunctionCall1(
	      WKBFromLWGEOM, 
	      PG_GETARG_DATUM(0)
	    )));
}

PG_FUNCTION_INFO_V1(LWGEOM_to_bytea);
Datum LWGEOM_to_bytea(PG_FUNCTION_ARGS)
{
	POSTGIS_DEBUG(2, "LWGEOM_to_bytea called");

	PG_RETURN_POINTER(
	  DatumGetPointer(
	    DirectFunctionCall1(
	      WKBFromLWGEOM, 
	      PG_GETARG_DATUM(0)
	    )));
}

PG_FUNCTION_INFO_V1(LWGEOM_from_bytea);
Datum LWGEOM_from_bytea(PG_FUNCTION_ARGS)
{
	GSERIALIZED *result;

	POSTGIS_DEBUG(2, "LWGEOM_from_bytea start");

	result = (GSERIALIZED *)DatumGetPointer(DirectFunctionCall1(
	                                          LWGEOMFromEWKB, PG_GETARG_DATUM(0)));

	PG_RETURN_POINTER(result);
}

