/*
 *
 * WKTRaster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2011-2013 Regents of the University of California
 *   <bkpark@ucdavis.edu>
 * Copyright (C) 2010-2011 Jorge Arevalo <jorge.arevalo@deimos-space.com>
 * Copyright (C) 2010-2011 David Zwarg <dzwarg@azavea.com>
 * Copyright (C) 2009-2011 Pierre Racine <pierre.racine@sbf.ulaval.ca>
 * Copyright (C) 2009-2011 Mateusz Loskot <mateusz@loskot.net>
 * Copyright (C) 2008-2009 Sandro Santilli <strk@keybit.net>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */

#include <postgres.h> /* for palloc */
#include <fmgr.h>
#include <utils/builtins.h>

#include "../../postgis_config.h"
#include "lwgeom_pg.h"

#include "rtpostgis.h"

Datum RASTER_lib_version(PG_FUNCTION_ARGS);
Datum RASTER_lib_build_date(PG_FUNCTION_ARGS);
Datum RASTER_gdal_version(PG_FUNCTION_ARGS);
Datum RASTER_minPossibleValue(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(RASTER_lib_version);
Datum RASTER_lib_version(PG_FUNCTION_ARGS)
{
    char ver[64];
    text *result;

    snprintf(ver, 64, "%s r%d", POSTGIS_LIB_VERSION, POSTGIS_SVN_REVISION);
    ver[63] = '\0';

    result = cstring2text(ver);
    PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(RASTER_lib_build_date);
Datum RASTER_lib_build_date(PG_FUNCTION_ARGS)
{
    char *ver = POSTGIS_BUILD_DATE;
    text *result;
    result = palloc(VARHDRSZ  + strlen(ver));
    SET_VARSIZE(result, VARHDRSZ + strlen(ver));
    memcpy(VARDATA(result), ver, strlen(ver));
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(RASTER_gdal_version);
Datum RASTER_gdal_version(PG_FUNCTION_ARGS)
{
	const char *ver = rt_util_gdal_version("--version");
	text *result;

	/* add indicator if GDAL isn't configured right */
	if (!rt_util_gdal_configured()) {
		char *rtn = NULL;
		rtn = palloc(strlen(ver) + strlen(" GDAL_DATA not found") + 1);
		if (!rtn)
			result = cstring2text(ver);
		else {
			sprintf(rtn, "%s GDAL_DATA not found", ver);
			result = cstring2text(rtn);
			pfree(rtn);
		}
	}
	else
		result = cstring2text(ver);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(RASTER_minPossibleValue);
Datum RASTER_minPossibleValue(PG_FUNCTION_ARGS)
{
	text *pixeltypetext = NULL;
	char *pixeltypechar = NULL;
	rt_pixtype pixtype = PT_END;
	double pixsize = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	pixeltypetext = PG_GETARG_TEXT_P(0);
	pixeltypechar = text_to_cstring(pixeltypetext);

	pixtype = rt_pixtype_index_from_name(pixeltypechar);
	if (pixtype == PT_END) {
		elog(ERROR, "RASTER_minPossibleValue: Invalid pixel type: %s", pixeltypechar);
		PG_RETURN_NULL();
	}

	pixsize = rt_pixtype_get_min_value(pixtype);

	/*
		correct pixsize of unsigned pixel types
		example: for PT_8BUI, the value is CHAR_MIN but if char is signed, 
			the value returned is -127 instead of 0.
	*/
	switch (pixtype) {
		case PT_1BB:
		case PT_2BUI:
		case PT_4BUI:
		case PT_8BUI:
		case PT_16BUI:
		case PT_32BUI:
			pixsize = 0;
			break;
		default:
			break;
	}

	PG_RETURN_FLOAT8(pixsize);
}

/** find the detoasted size of a value */
Datum RASTER_memsize(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(RASTER_memsize);
Datum RASTER_memsize(PG_FUNCTION_ARGS)
{
  void *detoasted = PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
  size_t size = VARSIZE(detoasted);
  PG_FREE_IF_COPY(detoasted,0);
  PG_RETURN_INT32(size);
}


