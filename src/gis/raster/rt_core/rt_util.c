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

#include "librtcore.h"
#include "librtcore_internal.h"

uint8_t
rt_util_clamp_to_1BB(double value) {
    return (uint8_t)fmin(fmax((value), 0), POSTGIS_RT_1BBMAX);
}

uint8_t
rt_util_clamp_to_2BUI(double value) {
    return (uint8_t)fmin(fmax((value), 0), POSTGIS_RT_2BUIMAX);
}

uint8_t
rt_util_clamp_to_4BUI(double value) {
    return (uint8_t)fmin(fmax((value), 0), POSTGIS_RT_4BUIMAX);
}

int8_t
rt_util_clamp_to_8BSI(double value) {
    return (int8_t)fmin(fmax((value), SCHAR_MIN), SCHAR_MAX);
}

uint8_t
rt_util_clamp_to_8BUI(double value) {
    return (uint8_t)fmin(fmax((value), 0), UCHAR_MAX);
}

int16_t
rt_util_clamp_to_16BSI(double value) {
    return (int16_t)fmin(fmax((value), SHRT_MIN), SHRT_MAX);
}

uint16_t
rt_util_clamp_to_16BUI(double value) {
    return (uint16_t)fmin(fmax((value), 0), USHRT_MAX);
}

int32_t
rt_util_clamp_to_32BSI(double value) {
    return (int32_t)fmin(fmax((value), INT_MIN), INT_MAX);
}

uint32_t
rt_util_clamp_to_32BUI(double value) {
    return (uint32_t)fmin(fmax((value), 0), UINT_MAX);
}

float
rt_util_clamp_to_32F(double value) {
    return (float)fmin(fmax((value), -FLT_MAX), FLT_MAX);
}

/**
 * Convert cstring name to GDAL Resample Algorithm
 *
 * @param algname : cstring name to convert
 *
 * @return valid GDAL resampling algorithm
 */
GDALResampleAlg
rt_util_gdal_resample_alg(const char *algname) {
	assert(algname != NULL && strlen(algname) > 0);

	if (strcmp(algname, "NEARESTNEIGHBOUR") == 0)
		return GRA_NearestNeighbour;
	else if (strcmp(algname, "NEARESTNEIGHBOR") == 0)
		return GRA_NearestNeighbour;
	else if (strcmp(algname, "BILINEAR") == 0)
		return GRA_Bilinear;
	else if (strcmp(algname, "CUBICSPLINE") == 0)
		return GRA_CubicSpline;
	else if (strcmp(algname, "CUBIC") == 0)
		return GRA_Cubic;
	else if (strcmp(algname, "LANCZOS") == 0)
		return GRA_Lanczos;

	return GRA_NearestNeighbour;
}

/**
 * Convert rt_pixtype to GDALDataType
 *
 * @param pt : pixeltype to convert
 *
 * @return valid GDALDataType
 */
GDALDataType
rt_util_pixtype_to_gdal_datatype(rt_pixtype pt) {
	switch (pt) {
		case PT_1BB:
		case PT_2BUI:
		case PT_4BUI:
		case PT_8BUI:
			return GDT_Byte;
		case PT_8BSI:
		case PT_16BSI:
			return GDT_Int16;
		case PT_16BUI:
			return GDT_UInt16;
		case PT_32BSI:
			return GDT_Int32;
		case PT_32BUI:
			return GDT_UInt32;
		case PT_32BF:
			return GDT_Float32;
		case PT_64BF:
			return GDT_Float64;
		default:
			return GDT_Unknown;
	}

	return GDT_Unknown;
}

/**
 * Convert GDALDataType to rt_pixtype
 *
 * @param gdt : GDAL datatype to convert
 *
 * @return valid rt_pixtype
 */
rt_pixtype
rt_util_gdal_datatype_to_pixtype(GDALDataType gdt) {
	switch (gdt) {
		case GDT_Byte:
			return PT_8BUI;
		case GDT_UInt16:
			return PT_16BUI;
		case GDT_Int16:
			return PT_16BSI;
		case GDT_UInt32:
			return PT_32BUI;
		case GDT_Int32:
			return PT_32BSI;
		case GDT_Float32:
			return PT_32BF;
		case GDT_Float64:
			return PT_64BF;
		default:
			return PT_END;
	}

	return PT_END;
}

/*
	get GDAL runtime version information
*/
const char*
rt_util_gdal_version(const char *request) {
	if (NULL == request || !strlen(request))
		return GDALVersionInfo("RELEASE_NAME");
	else
		return GDALVersionInfo(request);
}

/*
	computed extent type
*/
rt_extenttype
rt_util_extent_type(const char *name) {
	assert(name != NULL && strlen(name) > 0);

	if (strcmp(name, "UNION") == 0)
		return ET_UNION;
	else if (strcmp(name, "FIRST") == 0)
		return ET_FIRST;
	else if (strcmp(name, "SECOND") == 0)
		return ET_SECOND;
	else if (strcmp(name, "LAST") == 0)
		return ET_LAST;
	else if (strcmp(name, "CUSTOM") == 0)
		return ET_CUSTOM;
	else
		return ET_INTERSECTION;
}

/*
	convert the spatial reference string from a GDAL recognized format to either WKT or Proj4
*/
char*
rt_util_gdal_convert_sr(const char *srs, int proj4) {
	OGRSpatialReferenceH hsrs;
	char *rtn = NULL;

	assert(srs != NULL);

	hsrs = OSRNewSpatialReference(NULL);
	if (OSRSetFromUserInput(hsrs, srs) == OGRERR_NONE) {
		if (proj4)
			OSRExportToProj4(hsrs, &rtn);
		else
			OSRExportToWkt(hsrs, &rtn);
	}
	else {
		rterror("rt_util_gdal_convert_sr: Could not process the provided srs: %s", srs);
		return NULL;
	}

	OSRDestroySpatialReference(hsrs);
	if (rtn == NULL) {
		rterror("rt_util_gdal_convert_sr: Could not process the provided srs: %s", srs);
		return NULL;
	}

	return rtn;
}

/*
	is the spatial reference string supported by GDAL
*/
int
rt_util_gdal_supported_sr(const char *srs) {
	OGRSpatialReferenceH hsrs;
	OGRErr rtn = OGRERR_NONE;

	assert(srs != NULL);

	hsrs = OSRNewSpatialReference(NULL);
	rtn = OSRSetFromUserInput(hsrs, srs);
	OSRDestroySpatialReference(hsrs);

	if (rtn == OGRERR_NONE)
		return 1;
	else
		return 0;
}

/**
 * Get auth name and code
 *
 * @param authname: authority organization of code. calling function
 * is expected to free the memory allocated for value
 * @param authcode: code assigned by authority organization. calling function
 * is expected to free the memory allocated for value
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate
rt_util_gdal_sr_auth_info(GDALDatasetH hds, char **authname, char **authcode) {
	const char *srs = NULL;

	assert(authname != NULL);
	assert(authcode != NULL);

	*authname = NULL;
	*authcode = NULL;

	srs = GDALGetProjectionRef(hds);
	if (srs != NULL && srs[0] != '\0') {
		OGRSpatialReferenceH hSRS = OSRNewSpatialReference(NULL);

		if (OSRSetFromUserInput(hSRS, srs) == OGRERR_NONE) {
			const char* pszAuthorityName = OSRGetAuthorityName(hSRS, NULL);
			const char* pszAuthorityCode = OSRGetAuthorityCode(hSRS, NULL);

			if (pszAuthorityName != NULL && pszAuthorityCode != NULL) {
				*authname = rtalloc(sizeof(char) * (strlen(pszAuthorityName) + 1));
				*authcode = rtalloc(sizeof(char) * (strlen(pszAuthorityCode) + 1));

				if (*authname == NULL || *authcode == NULL) {
					rterror("rt_util_gdal_sr_auth_info: Could not allocate memory for auth name and code");
					if (*authname != NULL) rtdealloc(*authname);
					if (*authcode != NULL) rtdealloc(*authcode);
					OSRDestroySpatialReference(hSRS);
					return ES_ERROR;
				}

				strncpy(*authname, pszAuthorityName, strlen(pszAuthorityName) + 1);
				strncpy(*authcode, pszAuthorityCode, strlen(pszAuthorityCode) + 1);
			}
		}

		OSRDestroySpatialReference(hSRS);
	}

	return ES_NONE;
}

/*
	is GDAL configured correctly?
*/
int rt_util_gdal_configured(void) {

	/* set of EPSG codes */
	if (!rt_util_gdal_supported_sr("EPSG:4326"))
		return 0;
	if (!rt_util_gdal_supported_sr("EPSG:4269"))
		return 0;
	if (!rt_util_gdal_supported_sr("EPSG:4267"))
		return 0;
	if (!rt_util_gdal_supported_sr("EPSG:3310"))
		return 0;
	if (!rt_util_gdal_supported_sr("EPSG:2163"))
		return 0;

	return 1;
}

/*
	register all GDAL drivers
*/
int
rt_util_gdal_register_all(int force_register_all) {
	static int registered = 0;

	if (registered && !force_register_all) {
		RASTER_DEBUG(3, "Already called once... not calling GDALAllRegister");
		return 0;
	}

	RASTER_DEBUG(3, "Calling GDALAllRegister");
	GDALAllRegister();
	registered = 1;

	return 1;
}

/*
	is the driver registered?
*/
int
rt_util_gdal_driver_registered(const char *drv) {
	int count = GDALGetDriverCount();
	int i = 0;
	GDALDriverH hdrv = NULL;

	if (drv == NULL || !strlen(drv) || count < 1)
		return 0;

	for (i = 0; i < count; i++) {
		hdrv = GDALGetDriver(i);
		if (hdrv == NULL) continue;

		if (strcmp(drv, GDALGetDriverShortName(hdrv)) == 0)
			return 1;
	}

	return 0;
}

/* variable for PostgreSQL GUC: postgis.gdal_enabled_drivers */
char *gdal_enabled_drivers = NULL;

/*
	wrapper for GDALOpen and GDALOpenShared
*/
GDALDatasetH
rt_util_gdal_open(const char *fn, GDALAccess fn_access, int shared) {
	assert(NULL != fn);

	if (gdal_enabled_drivers != NULL) {
		if (strstr(gdal_enabled_drivers, GDAL_DISABLE_ALL) != NULL) {
			rterror("rt_util_gdal_open: Cannot open file. All GDAL drivers disabled");
			return NULL;
		}
		else if (strstr(gdal_enabled_drivers, GDAL_ENABLE_ALL) != NULL) {
			/* do nothing */
		}
		else if (
			(strstr(fn, "/vsicurl") != NULL) &&
			(strstr(gdal_enabled_drivers, GDAL_VSICURL) == NULL)
		) {
			rterror("rt_util_gdal_open: Cannot open VSICURL file. VSICURL disabled");
			return NULL;
		}
	}

	if (shared)
		return GDALOpenShared(fn, fn_access);
	else
		return GDALOpen(fn, fn_access);
}

void
rt_util_from_ogr_envelope(
	OGREnvelope	env,
	rt_envelope *ext
) {
	assert(ext != NULL);

	ext->MinX = env.MinX;
	ext->MaxX = env.MaxX;
	ext->MinY = env.MinY;
	ext->MaxY = env.MaxY;

	ext->UpperLeftX = env.MinX;
	ext->UpperLeftY = env.MaxY;
}

void
rt_util_to_ogr_envelope(
	rt_envelope ext,
	OGREnvelope	*env
) {
	assert(env != NULL);

	env->MinX = ext.MinX;
	env->MaxX = ext.MaxX;
	env->MinY = ext.MinY;
	env->MaxY = ext.MaxY;
}

LWPOLY *
rt_util_envelope_to_lwpoly(
	rt_envelope env
) {
	LWPOLY *npoly = NULL;
	POINTARRAY **rings = NULL;
	POINTARRAY *pts = NULL;
	POINT4D p4d;

	rings = (POINTARRAY **) rtalloc(sizeof (POINTARRAY*));
	if (!rings) {
		rterror("rt_util_envelope_to_lwpoly: Out of memory building envelope's geometry");
		return NULL;
	}
	rings[0] = ptarray_construct(0, 0, 5);
	if (!rings[0]) {
		rterror("rt_util_envelope_to_lwpoly: Out of memory building envelope's geometry ring");
		return NULL;
	}

	pts = rings[0];
	
	/* Upper-left corner (first and last points) */
	p4d.x = env.MinX;
	p4d.y = env.MaxY;
	ptarray_set_point4d(pts, 0, &p4d);
	ptarray_set_point4d(pts, 4, &p4d);

	/* Upper-right corner (we go clockwise) */
	p4d.x = env.MaxX;
	p4d.y = env.MaxY;
	ptarray_set_point4d(pts, 1, &p4d);

	/* Lower-right corner */
	p4d.x = env.MaxX;
	p4d.y = env.MinY;
	ptarray_set_point4d(pts, 2, &p4d);

	/* Lower-left corner */
	p4d.x = env.MinX;
	p4d.y = env.MinY;
	ptarray_set_point4d(pts, 3, &p4d);

	npoly = lwpoly_construct(SRID_UNKNOWN, 0, 1, rings);
	if (npoly == NULL) {
		rterror("rt_util_envelope_to_lwpoly: Could not build envelope's geometry");
		return NULL;
	}

	return npoly;
}

int
rt_util_same_geotransform_matrix(double *gt1, double *gt2) {
	int k = 0;

	if (gt1 == NULL || gt2 == NULL)
		return FALSE;

	for (k = 0; k < 6; k++) {
		if (FLT_NEQ(gt1[k], gt2[k]))
			return FALSE;
	}

	return TRUE;
}

/* coordinates in RGB and HSV are floating point values between 0 and 1 */
rt_errorstate
rt_util_rgb_to_hsv(double rgb[3], double hsv[3]) {
	int i;

	double minc;
	double maxc;

	double h = 0.;
	double s = 0.;
	double v = 0.;

	minc = rgb[0];
	maxc = rgb[0];

	/* get min and max values from RGB */
	for (i = 1; i < 3; i++) {
		if (rgb[i] > maxc)
			maxc = rgb[i];
		if (rgb[i] < minc)
			minc = rgb[i];
	}
	v = maxc;

	if (maxc != minc) {
		double diff = 0.;
		double rc = 0.;
		double gc = 0.;
		double bc = 0.;
		double junk = 0.;

		diff = maxc - minc;
		s = diff / maxc;
		rc = (maxc - rgb[0]) / diff;
		gc = (maxc - rgb[1]) / diff;
		bc = (maxc - rgb[2]) / diff;

		if (DBL_EQ(rgb[0], maxc))
			h = bc - gc;
		else if (DBL_EQ(rgb[1], maxc))
			h = 2.0 + rc - bc;
		else
			h = 4.0 + gc - rc;

		h = modf((h / 6.0), &junk);
	}

	hsv[0] = h;
	hsv[1] = s;
	hsv[2] = v;

	return ES_NONE;
}

/* coordinates in RGB and HSV are floating point values between 0 and 1 */
rt_errorstate
rt_util_hsv_to_rgb(double hsv[3], double rgb[3]) {
	double r = 0;
	double g = 0;
	double b = 0;
	double v = hsv[2];

	if (DBL_EQ(hsv[1], 0.))
		r = g = b = v;
	else {
		double i;
		double f;
		double p;
		double q;
		double t;

		int a;

		i = floor(hsv[0] * 6.);
		f = (hsv[0] * 6.0) - i;
		p = v * (1. - hsv[1]);
		q = v * (1. - hsv[1] * f);
		t = v * (1. - hsv[1] * (1. - f));

		a = (int) i;
		switch (a) {
			case 1:
				r = q;
				g = v;
				b = p;
				break;
			case 2:
				r = p;
				g = v;
				b = t;
				break;
			case 3:
				r = p;
				g = q;
				b = v;
				break;
			case 4:
				r = t;
				g = p;
				b = v;
				break;
			case 5:
				r = v;
				g = p;
				b = q;
				break;
			case 0:
			case 6:
			default:
				r = v;
				g = t;
				b = p;
				break;
		}
	}

	rgb[0] = r;
	rgb[1] = g;
	rgb[2] = b;

	return ES_NONE;
}

int
rt_util_dbl_trunc_warning(
	double initialvalue,
	int32_t checkvalint, uint32_t checkvaluint,
	float checkvalfloat, double checkvaldouble,
	rt_pixtype pixtype
) {
	int result = 0;

	switch (pixtype) {
		case PT_1BB:
		case PT_2BUI:
		case PT_4BUI:
		case PT_8BSI:
		case PT_8BUI:
		case PT_16BSI:
		case PT_16BUI:
		case PT_32BSI: {
			if (fabs(checkvalint - initialvalue) >= 1) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got clamped from %f to %d",
					rt_pixtype_name(pixtype),
					initialvalue, checkvalint
				);
#endif
				result = 1;
			}
			else if (FLT_NEQ(checkvalint, initialvalue)) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got truncated from %f to %d",
					rt_pixtype_name(pixtype),
					initialvalue, checkvalint
				);
#endif
				result = 1;
			}
			break;
		}
		case PT_32BUI: {
			if (fabs(checkvaluint - initialvalue) >= 1) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got clamped from %f to %u",
					rt_pixtype_name(pixtype),
					initialvalue, checkvaluint
				);
#endif
				result = 1;
			}
			else if (FLT_NEQ(checkvaluint, initialvalue)) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got truncated from %f to %u",
					rt_pixtype_name(pixtype),
					initialvalue, checkvaluint
				);
#endif
				result = 1;
			}
			break;
		}
		case PT_32BF: {
			/*
				For float, because the initial value is a double,
				there is very often a difference between the desired value and the obtained one
			*/
			if (FLT_NEQ(checkvalfloat, initialvalue)) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got converted from %f to %f",
					rt_pixtype_name(pixtype),
					initialvalue, checkvalfloat
				);
#endif
				result = 1;
			}
			break;
		}
		case PT_64BF: {
			if (FLT_NEQ(checkvaldouble, initialvalue)) {
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
				rtwarn("Value set for %s band got converted from %f to %f",
					rt_pixtype_name(pixtype),
					initialvalue, checkvaldouble
				);
#endif
				result = 1;
			}
			break;
		}
		case PT_END:
			break;
	}

	return result;
}

