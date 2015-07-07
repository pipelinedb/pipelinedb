/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <ctype.h>

#include "liblwgeom_internal.h"

/* Structure for the type array */
struct geomtype_struct
{
	char *typename;
	int type;
	int z;
	int m;
};

/* Type array. Note that the order of this array is important in
   that any typename in the list must *NOT* occur within an entry
   before it. Otherwise if we search for "POINT" at the top of the
   list we would also match MULTIPOINT, for example. */

struct geomtype_struct geomtype_struct_array[] =
{
	{ "GEOMETRYCOLLECTIONZM", COLLECTIONTYPE, 1, 1 },
	{ "GEOMETRYCOLLECTIONZ", COLLECTIONTYPE, 1, 0 },
	{ "GEOMETRYCOLLECTIONM", COLLECTIONTYPE, 0, 1 },
	{ "GEOMETRYCOLLECTION", COLLECTIONTYPE, 0, 0 },

	{ "GEOMETRYZM", 0, 1, 1 },
	{ "GEOMETRYZ", 0, 1, 0 },
	{ "GEOMETRYM", 0, 0, 1 },
	{ "GEOMETRY", 0, 0, 0 },

	{ "POLYHEDRALSURFACEZM", POLYHEDRALSURFACETYPE, 1, 1 },
	{ "POLYHEDRALSURFACEZ", POLYHEDRALSURFACETYPE, 1, 0 },
	{ "POLYHEDRALSURFACEM", POLYHEDRALSURFACETYPE, 0, 1 },
	{ "POLYHEDRALSURFACE", POLYHEDRALSURFACETYPE, 0, 0 },

	{ "TINZM", TINTYPE, 1, 1 },
	{ "TINZ", TINTYPE, 1, 0 },
	{ "TINM", TINTYPE, 0, 1 },
	{ "TIN", TINTYPE, 0, 0 },

	{ "CIRCULARSTRINGZM", CIRCSTRINGTYPE, 1, 1 },
	{ "CIRCULARSTRINGZ", CIRCSTRINGTYPE, 1, 0 },
	{ "CIRCULARSTRINGM", CIRCSTRINGTYPE, 0, 1 },
	{ "CIRCULARSTRING", CIRCSTRINGTYPE, 0, 0 },

	{ "COMPOUNDCURVEZM", COMPOUNDTYPE, 1, 1 },
	{ "COMPOUNDCURVEZ", COMPOUNDTYPE, 1, 0 },
	{ "COMPOUNDCURVEM", COMPOUNDTYPE, 0, 1 },
	{ "COMPOUNDCURVE", COMPOUNDTYPE, 0, 0 },

	{ "CURVEPOLYGONZM", CURVEPOLYTYPE, 1, 1 },
	{ "CURVEPOLYGONZ", CURVEPOLYTYPE, 1, 0 },
	{ "CURVEPOLYGONM", CURVEPOLYTYPE, 0, 1 },
	{ "CURVEPOLYGON", CURVEPOLYTYPE, 0, 0 },

	{ "MULTICURVEZM", MULTICURVETYPE, 1, 1 },
	{ "MULTICURVEZ", MULTICURVETYPE, 1, 0 },
	{ "MULTICURVEM", MULTICURVETYPE, 0, 1 },
	{ "MULTICURVE", MULTICURVETYPE, 0, 0 },

	{ "MULTISURFACEZM", MULTISURFACETYPE, 1, 1 },
	{ "MULTISURFACEZ", MULTISURFACETYPE, 1, 0 },
	{ "MULTISURFACEM", MULTISURFACETYPE, 0, 1 },
	{ "MULTISURFACE", MULTISURFACETYPE, 0, 0 },

	{ "MULTILINESTRINGZM", MULTILINETYPE, 1, 1 },
	{ "MULTILINESTRINGZ", MULTILINETYPE, 1, 0 },
	{ "MULTILINESTRINGM", MULTILINETYPE, 0, 1 },
	{ "MULTILINESTRING", MULTILINETYPE, 0, 0 },

	{ "MULTIPOLYGONZM", MULTIPOLYGONTYPE, 1, 1 },
	{ "MULTIPOLYGONZ", MULTIPOLYGONTYPE, 1, 0 },
	{ "MULTIPOLYGONM", MULTIPOLYGONTYPE, 0, 1 },
	{ "MULTIPOLYGON", MULTIPOLYGONTYPE, 0, 0 },

	{ "MULTIPOINTZM", MULTIPOINTTYPE, 1, 1 },
	{ "MULTIPOINTZ", MULTIPOINTTYPE, 1, 0 },
	{ "MULTIPOINTM", MULTIPOINTTYPE, 0, 1 },
	{ "MULTIPOINT", MULTIPOINTTYPE, 0, 0 },

	{ "LINESTRINGZM", LINETYPE, 1, 1 },
	{ "LINESTRINGZ", LINETYPE, 1, 0 },
	{ "LINESTRINGM", LINETYPE, 0, 1 },
	{ "LINESTRING", LINETYPE, 0, 0 },

	{ "TRIANGLEZM", TRIANGLETYPE, 1, 1 },
	{ "TRIANGLEZ", TRIANGLETYPE, 1, 0 },
	{ "TRIANGLEM", TRIANGLETYPE, 0, 1 },
	{ "TRIANGLE", TRIANGLETYPE, 0, 0 },

	{ "POLYGONZM", POLYGONTYPE, 1, 1 },
	{ "POLYGONZ", POLYGONTYPE, 1, 0 },
	{ "POLYGONM", POLYGONTYPE, 0, 1 },
	{ "POLYGON", POLYGONTYPE, 0, 0 },

	{ "POINTZM", POINTTYPE, 1, 1 },
	{ "POINTZ", POINTTYPE, 1, 0 },
	{ "POINTM", POINTTYPE, 0, 1 },
	{ "POINT", POINTTYPE, 0, 0 }

};
#define GEOMTYPE_STRUCT_ARRAY_LEN (sizeof geomtype_struct_array/sizeof(struct geomtype_struct))

/*
* We use a very simple upper case mapper here, because the system toupper() function
* is locale dependent and may have trouble mapping lower case strings to the upper
* case ones we expect (see, the "Turkisk I", http://www.i18nguy.com/unicode/turkish-i18n.html)
* We could also count on PgSQL sending us *lower* case inputs, as it seems to do that
* regardless of the case the user provides for the type arguments.
*/
const char dumb_upper_map[128] = "................................................0123456789.......ABCDEFGHIJKLMNOPQRSTUVWXYZ......ABCDEFGHIJKLMNOPQRSTUVWXYZ.....";

static char dump_toupper(int in)
{
	if ( in < 0 || in > 127 ) 
		return '.';
	return dumb_upper_map[in];
}

uint8_t gflags(int hasz, int hasm, int geodetic)
{
	uint8_t flags = 0;
	if ( hasz )
		FLAGS_SET_Z(flags, 1);
	if ( hasm )
		FLAGS_SET_M(flags, 1);
	if ( geodetic )
		FLAGS_SET_GEODETIC(flags, 1);
	return flags;
}

/**
* Calculate type integer and dimensional flags from string input.
* Case insensitive, and insensitive to spaces at front and back.
* Type == 0 in the case of the string "GEOMETRY" or "GEOGRAPHY".
* Return LW_SUCCESS for success.
*/
int geometry_type_from_string(const char *str, uint8_t *type, int *z, int *m)
{
	char *tmpstr;
	int tmpstartpos, tmpendpos;
	int i;

	assert(str);
	assert(type);
	assert(z);
	assert(m);

	/* Initialize. */
	*type = 0;
	*z = 0;
	*m = 0;

	/* Locate any leading/trailing spaces */
	tmpstartpos = 0;
	for (i = 0; i < strlen(str); i++)
	{
		if (str[i] != ' ')
		{
			tmpstartpos = i;
			break;
		}
	}

	tmpendpos = strlen(str) - 1;
	for (i = strlen(str) - 1; i >= 0; i--)
	{
		if (str[i] != ' ')
		{
			tmpendpos = i;
			break;
		}
	}

	/* Copy and convert to upper case for comparison */
	tmpstr = lwalloc(tmpendpos - tmpstartpos + 2);
	for (i = tmpstartpos; i <= tmpendpos; i++)
		tmpstr[i - tmpstartpos] = dump_toupper(str[i]);

	/* Add NULL to terminate */
	tmpstr[i - tmpstartpos] = '\0';

	/* Now check for the type */
	for (i = 0; i < GEOMTYPE_STRUCT_ARRAY_LEN; i++)
	{
		if (!strcmp(tmpstr, geomtype_struct_array[i].typename))
		{
			*type = geomtype_struct_array[i].type;
			*z = geomtype_struct_array[i].z;
			*m = geomtype_struct_array[i].m;

			lwfree(tmpstr);

			return LW_SUCCESS;
		}

	}

	lwfree(tmpstr);

	return LW_FAILURE;
}




