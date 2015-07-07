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
#include "rt_serialize.h"

/******************************************************************************
* rt_raster_perimeter()
******************************************************************************/

static rt_errorstate
_rti_raster_get_band_perimeter(rt_band band, uint16_t *trim) {
	uint16_t width = 0;
	uint16_t height = 0;
	int x = 0;
	int y = 0;
	int offset = 0;
	int done[4] = {0};
	double value = 0;
	int nodata = 0;

	assert(band != NULL);
	assert(band->raster != NULL);
	assert(trim != NULL);

	memset(trim, 0, sizeof(uint16_t) * 4);

	width = rt_band_get_width(band);
	height = rt_band_get_height(band);
		
	/* top */
	for (y = 0; y < height; y++) {
		for (offset = 0; offset < 3; offset++) {
			/* every third pixel */
			for (x = offset; x < width; x += 3) {
				if (rt_band_get_pixel(band, x, y, &value, &nodata) != ES_NONE) {
					rterror("_rti_raster_get_band_perimeter: Could not get band pixel");
					return ES_ERROR;
				}

				RASTER_DEBUGF(4, "top (x, y, value, nodata) = (%d, %d, %f, %d)", x, y, value, nodata);
				if (!nodata) {
					trim[0] = y;
					done[0] = 1;
					break;
				}
			}

			if (done[0])
				break;
		}

		if (done[0])
			break;
	}

	/* right */
	for (x = width - 1; x >= 0; x--) {
		for (offset = 0; offset < 3; offset++) {
			/* every third pixel */
			for (y = offset; y < height; y += 3) {
				if (rt_band_get_pixel(band, x, y, &value, &nodata) != ES_NONE) {
					rterror("_rti_raster_get_band_perimeter: Could not get band pixel");
					return ES_ERROR;
				}

				RASTER_DEBUGF(4, "right (x, y, value, nodata) = (%d, %d, %f, %d)", x, y, value, nodata);
				if (!nodata) {
					trim[1] = width - (x + 1);
					done[1] = 1;
					break;
				}
			}

			if (done[1])
				break;
		}

		if (done[1])
			break;
	}

	/* bottom */
	for (y = height - 1; y >= 0; y--) {
		for (offset = 0; offset < 3; offset++) {
			/* every third pixel */
			for (x = offset; x < width; x += 3) {
				if (rt_band_get_pixel(band, x, y, &value, &nodata) != ES_NONE) {
					rterror("_rti_raster_get_band_perimeter: Could not get band pixel");
					return ES_ERROR;
				}

				RASTER_DEBUGF(4, "bottom (x, y, value, nodata) = (%d, %d, %f, %d)", x, y, value, nodata);
				if (!nodata) {
					trim[2] = height - (y + 1);
					done[2] = 1;
					break;
				}
			}

			if (done[2])
				break;
		}

		if (done[2])
			break;
	}

	/* left */
	for (x = 0; x < width; x++) {
		for (offset = 0; offset < 3; offset++) {
			/* every third pixel */
			for (y = offset; y < height; y += 3) {
				if (rt_band_get_pixel(band, x, y, &value, &nodata) != ES_NONE) {
					rterror("_rti_raster_get_band_perimeter: Could not get band pixel");
					return ES_ERROR;
				}

				RASTER_DEBUGF(4, "left (x, , value, nodata) = (%d, %d, %f, %d)", x, y, value, nodata);
				if (!nodata) {
					trim[3] = x;
					done[3] = 1;
					break;
				}
			}

			if (done[3])
				break;
		}

		if (done[3])
			break;
	}

	RASTER_DEBUGF(4, "trim = (%d, %d, %d, %d)",
		trim[0], trim[1], trim[2], trim[3]);

	return ES_NONE;
}

/**
 * Get raster perimeter
 *
 * The perimeter is a 4 vertices (5 to be closed)
 * single ring polygon bearing the raster's rotation and using
 * projection coordinates.
 *
 * @param raster : the raster to get info from
 * @param nband : the band for the perimeter. 0-based
 * value less than zero means all bands
 * @param **perimeter : pointer to perimeter
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate rt_raster_get_perimeter(
	rt_raster raster, int nband,
	LWGEOM **perimeter
) {
	rt_band band = NULL;
	int numband = 0;
	uint16_t *_nband = NULL;
	int i = 0;
	int j = 0;
	uint16_t _trim[4] = {0};
	uint16_t trim[4] = {0}; /* top, right, bottom, left */
	int isset[4] = {0};
	double gt[6] = {0.0};
	int srid = SRID_UNKNOWN;

	POINTARRAY *pts = NULL;
	POINT4D p4d;
	POINTARRAY **rings = NULL;
	LWPOLY* poly = NULL;

	assert(perimeter != NULL);

	*perimeter = NULL;

	/* empty raster, no perimeter */
	if (rt_raster_is_empty(raster))
		return ES_NONE;

	/* raster metadata */
	srid = rt_raster_get_srid(raster);
	rt_raster_get_geotransform_matrix(raster, gt);
	numband = rt_raster_get_num_bands(raster);

	RASTER_DEBUGF(3, "rt_raster_get_perimeter: raster is %dx%d", raster->width, raster->height); 

	/* nband < 0 means all bands */
	if (nband >= 0) {
		if (nband >= numband) {
			rterror("rt_raster_get_boundary: Band %d not found for raster", nband);
			return ES_ERROR;
		}

		numband = 1;
	}
	else
		nband = -1;
	
	RASTER_DEBUGF(3, "rt_raster_get_perimeter: nband, numband = %d, %d", nband, numband); 

	_nband = rtalloc(sizeof(uint16_t) * numband);
	if (_nband == NULL) {
		rterror("rt_raster_get_boundary: Could not allocate memory for band indices");
		return ES_ERROR;
	}

	if (nband < 0) {
		for (i = 0; i < numband; i++)
			_nband[i] = i;
	}
	else
		_nband[0] = nband;

	for (i = 0; i < numband; i++) {
		band = rt_raster_get_band(raster, _nband[i]);
		if (band == NULL) {
			rterror("rt_raster_get_boundary: Could not get band at index %d", _nband[i]);
			rtdealloc(_nband);
			return ES_ERROR;
		}

		/* band is nodata */
		if (rt_band_get_isnodata_flag(band) != 0)
			continue;

		if (_rti_raster_get_band_perimeter(band, trim) != ES_NONE) {
			rterror("rt_raster_get_boundary: Could not get band perimeter");
			rtdealloc(_nband);
			return ES_ERROR;
		}

		for (j = 0; j < 4; j++) {
			if (!isset[j] || trim[j] < _trim[j]) {
				_trim[j] = trim[j];
				isset[j] = 1;
			}
		}
	}

	/* no longer needed */
	rtdealloc(_nband);

	/* check isset, just need to check one element */
	if (!isset[0]) {
		/* return NULL as bands are empty */
		return ES_NONE;
	}

	RASTER_DEBUGF(4, "trim = (%d, %d, %d, %d)",
		trim[0], trim[1], trim[2], trim[3]);

	/* only one ring */
	rings = (POINTARRAY **) rtalloc(sizeof (POINTARRAY*));
	if (!rings) {
		rterror("rt_raster_get_perimeter: Could not allocate memory for polygon ring");
		return ES_ERROR;
	}
	rings[0] = ptarray_construct(0, 0, 5);
	if (!rings[0]) {
		rterror("rt_raster_get_perimeter: Could not construct point array");
		return ES_ERROR;
	}
	pts = rings[0];

	/* Upper-left corner (first and last points) */
	rt_raster_cell_to_geopoint(
		raster,
		_trim[3], _trim[0],
		&p4d.x, &p4d.y,
		gt
	);
	ptarray_set_point4d(pts, 0, &p4d);
	ptarray_set_point4d(pts, 4, &p4d);

	/* Upper-right corner (we go clockwise) */
	rt_raster_cell_to_geopoint(
		raster,
		raster->width - _trim[1], _trim[0],
		&p4d.x, &p4d.y,
		gt
	);
	ptarray_set_point4d(pts, 1, &p4d);

	/* Lower-right corner */
	rt_raster_cell_to_geopoint(
		raster,
		raster->width - _trim[1], raster->height - _trim[2],
		&p4d.x, &p4d.y,
		gt
	);
	ptarray_set_point4d(pts, 2, &p4d);

	/* Lower-left corner */
	rt_raster_cell_to_geopoint(
		raster,
		_trim[3], raster->height - _trim[2],
		&p4d.x, &p4d.y,
		gt
	);
	ptarray_set_point4d(pts, 3, &p4d);

	poly = lwpoly_construct(srid, 0, 1, rings);
	*perimeter = lwpoly_as_lwgeom(poly);

	return ES_NONE;
}

/******************************************************************************
* rt_raster_surface()
******************************************************************************/

/**
 * Get a raster as a surface (multipolygon).  If a band is specified,
 * those pixels with value (not NODATA) contribute to the area
 * of the output multipolygon.
 *
 * @param raster : the raster to convert to a multipolygon
 * @param nband : the 0-based band of raster rast to use
 *   if value is less than zero, bands are ignored.
 * @param *surface : raster as a surface (multipolygon).
 *   if all pixels are NODATA, NULL is set
 *
 * @return ES_NONE on success, ES_ERROR on error
 */
rt_errorstate rt_raster_surface(rt_raster raster, int nband, LWMPOLY **surface) {
	rt_band band = NULL;
	LWGEOM *mpoly = NULL;
	LWGEOM *tmp = NULL;
	LWGEOM *clone = NULL;
	rt_geomval gv = NULL;
	int gvcount = 0;
	GEOSGeometry *gc = NULL;
	GEOSGeometry *gunion = NULL;
	GEOSGeometry **geoms = NULL;
	int geomscount = 0;
	int i = 0;

	assert(surface != NULL);

	/* init *surface to NULL */
	*surface = NULL;

	/* raster is empty, surface = NULL */
	if (rt_raster_is_empty(raster))
		return ES_NONE;

	/* if nband < 0, return the convex hull as a multipolygon */
	if (nband < 0) {
		/*
			lwgeom_as_multi() only does a shallow clone internally
			so input and output geometries may share memory
			hence the deep clone of the output geometry for returning
			is the only way to guarentee the memory isn't shared
		*/
		if (rt_raster_get_convex_hull(raster, &tmp) != ES_NONE) {
			rterror("rt_raster_surface: Could not get convex hull of raster");
			return ES_ERROR;
		}
		mpoly = lwgeom_as_multi(tmp);
		clone = lwgeom_clone_deep(mpoly);
		lwgeom_free(tmp);
		lwgeom_free(mpoly);

		*surface = lwgeom_as_lwmpoly(clone);
		return ES_NONE;
	}
	/* check that nband is valid */
	else if (nband >= rt_raster_get_num_bands(raster)) {
		rterror("rt_raster_surface: The band index %d is invalid", nband);
		return ES_ERROR;
	}

	/* get band */
	band = rt_raster_get_band(raster, nband);
	if (band == NULL) {
		rterror("rt_raster_surface: Error getting band %d from raster", nband);
		return ES_ERROR;
	}

	/* band does not have a NODATA flag, return convex hull */
	if (!rt_band_get_hasnodata_flag(band)) {
		/*
			lwgeom_as_multi() only does a shallow clone internally
			so input and output geometries may share memory
			hence the deep clone of the output geometry for returning
			is the only way to guarentee the memory isn't shared
		*/
		if (rt_raster_get_convex_hull(raster, &tmp) != ES_NONE) {
			rterror("rt_raster_surface: Could not get convex hull of raster");
			return ES_ERROR;
		}
		mpoly = lwgeom_as_multi(tmp);
		clone = lwgeom_clone_deep(mpoly);
		lwgeom_free(tmp);
		lwgeom_free(mpoly);

		*surface = lwgeom_as_lwmpoly(clone);
		return ES_NONE;
	}
	/* band is NODATA, return NULL */
	else if (rt_band_get_isnodata_flag(band)) {
		RASTER_DEBUG(3, "Band is NODATA.  Returning NULL");
		return ES_NONE;
	}

	/* initialize GEOS */
	initGEOS(lwnotice, lwgeom_geos_error);

	/* use gdal polygonize */
	gv = rt_raster_gdal_polygonize(raster, nband, 1, &gvcount);
	/* no polygons returned */
	if (gvcount < 1) {
		RASTER_DEBUG(3, "All pixels of band are NODATA.  Returning NULL");
		if (gv != NULL) rtdealloc(gv);
		return ES_NONE;
	}
	/* more than 1 polygon */
	else if (gvcount > 1) {
		/* convert LWPOLY to GEOSGeometry */
		geomscount = gvcount;
		geoms = rtalloc(sizeof(GEOSGeometry *) * geomscount);
		if (geoms == NULL) {
			rterror("rt_raster_surface: Could not allocate memory for pixel polygons as GEOSGeometry");
			for (i = 0; i < gvcount; i++) lwpoly_free(gv[i].geom);
			rtdealloc(gv);
			return ES_ERROR;
		}
		for (i = 0; i < gvcount; i++) {
#if POSTGIS_DEBUG_LEVEL > 3
			{
				char *wkt = lwgeom_to_wkt(lwpoly_as_lwgeom(gv[i].geom), WKT_ISO, DBL_DIG, NULL);
				RASTER_DEBUGF(4, "geom %d = %s", i, wkt);
				rtdealloc(wkt);
			}
#endif

			geoms[i] = LWGEOM2GEOS(lwpoly_as_lwgeom(gv[i].geom), 0);
			lwpoly_free(gv[i].geom);
		}
		rtdealloc(gv);

		/* create geometry collection */
#if POSTGIS_GEOS_VERSION >= 33
		gc = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, geoms, geomscount);
#else
		gc = GEOSGeom_createCollection(GEOS_MULTIPOLYGON, geoms, geomscount);
#endif

		if (gc == NULL) {
#if POSTGIS_GEOS_VERSION >= 33
			rterror("rt_raster_surface: Could not create GEOS GEOMETRYCOLLECTION from set of pixel polygons");
#else
			rterror("rt_raster_surface: Could not create GEOS MULTIPOLYGON from set of pixel polygons");
#endif

			for (i = 0; i < geomscount; i++)
				GEOSGeom_destroy(geoms[i]);
			rtdealloc(geoms);
			return ES_ERROR;
		}

		/* run the union */
#if POSTGIS_GEOS_VERSION >= 33
		gunion = GEOSUnaryUnion(gc);
#else
		gunion = GEOSUnionCascaded(gc);
#endif
		GEOSGeom_destroy(gc);
		rtdealloc(geoms);

		if (gunion == NULL) {
#if POSTGIS_GEOS_VERSION >= 33
			rterror("rt_raster_surface: Could not union the pixel polygons using GEOSUnaryUnion()");
#else
			rterror("rt_raster_surface: Could not union the pixel polygons using GEOSUnionCascaded()");
#endif
			return ES_ERROR;
		}

		/* convert union result from GEOSGeometry to LWGEOM */
		mpoly = GEOS2LWGEOM(gunion, 0);

		/*
			is geometry valid?
			if not, try to make valid
		*/
		do {
			LWGEOM *mpolyValid = NULL;

#if POSTGIS_GEOS_VERSION < 33
			break;
#endif

			if (GEOSisValid(gunion))
				break;

			/* make geometry valid */
			mpolyValid = lwgeom_make_valid(mpoly);
			if (mpolyValid == NULL) {
				rtwarn("Cannot fix invalid geometry");
				break;
			}

			lwgeom_free(mpoly);
			mpoly = mpolyValid;
		}
		while (0);

		GEOSGeom_destroy(gunion);
	}
	else {
		mpoly = lwpoly_as_lwgeom(gv[0].geom);
		rtdealloc(gv);

#if POSTGIS_DEBUG_LEVEL > 3
			{
				char *wkt = lwgeom_to_wkt(mpoly, WKT_ISO, DBL_DIG, NULL);
				RASTER_DEBUGF(4, "geom 0 = %s", wkt);
				rtdealloc(wkt);
			}
#endif
	}

	/* specify SRID */
	lwgeom_set_srid(mpoly, rt_raster_get_srid(raster));

	if (mpoly != NULL) {
		/* convert to multi */
		if (!lwgeom_is_collection(mpoly)) {
			tmp = mpoly;

#if POSTGIS_DEBUG_LEVEL > 3
			{
				char *wkt = lwgeom_to_wkt(mpoly, WKT_ISO, DBL_DIG, NULL);
				RASTER_DEBUGF(4, "before multi = %s", wkt);
				rtdealloc(wkt);
			}
#endif

			RASTER_DEBUGF(4, "mpoly @ %p", mpoly);

			/*
				lwgeom_as_multi() only does a shallow clone internally
				so input and output geometries may share memory
				hence the deep clone of the output geometry for returning
				is the only way to guarentee the memory isn't shared
			*/
			mpoly = lwgeom_as_multi(tmp);
			clone = lwgeom_clone_deep(mpoly);
			lwgeom_free(tmp);
			lwgeom_free(mpoly);
			mpoly = clone;

			RASTER_DEBUGF(4, "mpoly @ %p", mpoly);

#if POSTGIS_DEBUG_LEVEL > 3
			{
				char *wkt = lwgeom_to_wkt(mpoly, WKT_ISO, DBL_DIG, NULL);
				RASTER_DEBUGF(4, "after multi = %s", wkt);
				rtdealloc(wkt);
			}
#endif
		}

#if POSTGIS_DEBUG_LEVEL > 3
		{
			char *wkt = lwgeom_to_wkt(mpoly, WKT_ISO, DBL_DIG, NULL);
			RASTER_DEBUGF(4, "returning geometry = %s", wkt);
			rtdealloc(wkt);
		}
#endif

		*surface = lwgeom_as_lwmpoly(mpoly);
		return ES_NONE;
	}

	return ES_NONE;
}

/******************************************************************************
* rt_raster_pixel_as_polygon()
******************************************************************************/

/**
 * Get a raster pixel as a polygon.
 *
 * The pixel shape is a 4 vertices (5 to be closed) single
 * ring polygon bearing the raster's rotation
 * and using projection coordinates
 *
 * @param raster : the raster to get pixel from
 * @param x : the column number
 * @param y : the row number
 *
 * @return the pixel polygon, or NULL on error.
 *
 */
LWPOLY*
rt_raster_pixel_as_polygon(rt_raster rast, int x, int y)
{
    double scale_x, scale_y;
    double skew_x, skew_y;
    double ul_x, ul_y;
    int srid;
    POINTARRAY **points;
    POINT4D p, p0;
    LWPOLY *poly;

		assert(rast != NULL);

    scale_x = rt_raster_get_x_scale(rast);
    scale_y = rt_raster_get_y_scale(rast);
    skew_x = rt_raster_get_x_skew(rast);
    skew_y = rt_raster_get_y_skew(rast);
    ul_x = rt_raster_get_x_offset(rast);
    ul_y = rt_raster_get_y_offset(rast);
    srid = rt_raster_get_srid(rast);

    points = rtalloc(sizeof(POINTARRAY *)*1);
    points[0] = ptarray_construct(0, 0, 5);

    p0.x = scale_x * x + skew_x * y + ul_x;
    p0.y = scale_y * y + skew_y * x + ul_y;
    ptarray_set_point4d(points[0], 0, &p0);

    p.x = p0.x + scale_x;
    p.y = p0.y + skew_y;
    ptarray_set_point4d(points[0], 1, &p);

    p.x = p0.x + scale_x + skew_x;
    p.y = p0.y + scale_y + skew_y;
    ptarray_set_point4d(points[0], 2, &p);

    p.x = p0.x + skew_x;
    p.y = p0.y + scale_y;
    ptarray_set_point4d(points[0], 3, &p);

    /* close it */
    ptarray_set_point4d(points[0], 4, &p0);

    poly = lwpoly_construct(srid, NULL, 1, points);

    return poly;
}

/******************************************************************************
* rt_raster_get_envelope_geom()
******************************************************************************/

/**
 * Get raster's envelope as a geometry
 *
 * @param raster : the raster to get info from
 * @param **env : pointer to envelope
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_get_envelope_geom(rt_raster raster, LWGEOM **env) {
	double gt[6] = {0.0};
	int srid = SRID_UNKNOWN;

	POINTARRAY *pts = NULL;
	POINT4D p4d;

	assert(env != NULL);
	*env = NULL;

	/* raster is NULL, envelope is NULL */
	if (raster == NULL)
		return ES_NONE;

	/* raster metadata */
	srid = rt_raster_get_srid(raster);
	rt_raster_get_geotransform_matrix(raster, gt);

	RASTER_DEBUGF(
		3,
		"rt_raster_get_envelope: raster is %dx%d",
		raster->width,
		raster->height
	); 

	/* return point or line since at least one of the two dimensions is 0 */
	if ((!raster->width) || (!raster->height)) {
		p4d.x = gt[0];
		p4d.y = gt[3];

		/* return point */
		if (!raster->width && !raster->height) {
			LWPOINT *point = lwpoint_make2d(srid, p4d.x, p4d.y);
			*env = lwpoint_as_lwgeom(point);
		}
		/* return linestring */
		else {
			LWLINE *line = NULL;
			pts = ptarray_construct_empty(0, 0, 2);

			/* first point of line */
			ptarray_append_point(pts, &p4d, LW_TRUE);

			/* second point of line */
			if (rt_raster_cell_to_geopoint(
				raster,
				rt_raster_get_width(raster), rt_raster_get_height(raster),
				&p4d.x, &p4d.y,
				gt
			) != ES_NONE) {
				rterror("rt_raster_get_envelope: Could not get second point for linestring");
				return ES_ERROR;
			}
			ptarray_append_point(pts, &p4d, LW_TRUE);
			line = lwline_construct(srid, NULL, pts);

			*env = lwline_as_lwgeom(line);
		}

		return ES_NONE;
	}
	else {
		rt_envelope rtenv;
		int err = ES_NONE;
		POINTARRAY **rings = NULL;
		LWPOLY* poly = NULL;

		/* only one ring */
		rings = (POINTARRAY **) rtalloc(sizeof (POINTARRAY*));
		if (!rings) {
			rterror("rt_raster_get_envelope_geom: Could not allocate memory for polygon ring");
			return ES_ERROR;
		}
		rings[0] = ptarray_construct(0, 0, 5);
		if (!rings[0]) {
			rterror("rt_raster_get_envelope_geom: Could not construct point array");
			return ES_ERROR;
		}
		pts = rings[0];

		err = rt_raster_get_envelope(raster, &rtenv);
		if (err != ES_NONE) {
			rterror("rt_raster_get_envelope_geom: Could not get raster envelope");
			return err;
		}

		/* build ring */

		/* minx, maxy */
		p4d.x = rtenv.MinX;
		p4d.y = rtenv.MaxY;
		ptarray_set_point4d(pts, 0, &p4d);
		ptarray_set_point4d(pts, 4, &p4d);

		/* maxx, maxy */
		p4d.x = rtenv.MaxX;
		p4d.y = rtenv.MaxY;
		ptarray_set_point4d(pts, 1, &p4d);

		/* maxx, miny */
		p4d.x = rtenv.MaxX;
		p4d.y = rtenv.MinY;
		ptarray_set_point4d(pts, 2, &p4d);

		/* minx, miny */
		p4d.x = rtenv.MinX;
		p4d.y = rtenv.MinY;
		ptarray_set_point4d(pts, 3, &p4d);

		poly = lwpoly_construct(srid, 0, 1, rings);
		*env = lwpoly_as_lwgeom(poly);
	}

	return ES_NONE;
}

/******************************************************************************
* rt_raster_get_convex_hull()
******************************************************************************/

/**
 * Get raster's convex hull.
 *
 * The convex hull is typically a 4 vertices (5 to be closed)
 * single ring polygon bearing the raster's rotation and using
 * projection coordinates.
 *
 * @param raster : the raster to get info from
 * @param **hull : pointer to convex hull
 *
 * @return ES_NONE if success, ES_ERROR if error
 */
rt_errorstate
rt_raster_get_convex_hull(rt_raster raster, LWGEOM **hull) {
	double gt[6] = {0.0};
	int srid = SRID_UNKNOWN;

	POINTARRAY *pts = NULL;
	POINT4D p4d;

	assert(hull != NULL);
	*hull = NULL;

	/* raster is NULL, convex hull is NULL */
	if (raster == NULL)
		return ES_NONE;

	/* raster metadata */
	srid = rt_raster_get_srid(raster);
	rt_raster_get_geotransform_matrix(raster, gt);

	RASTER_DEBUGF(3, "rt_raster_get_convex_hull: raster is %dx%d", raster->width, raster->height); 

	/* return point or line since at least one of the two dimensions is 0 */
	if ((!raster->width) || (!raster->height)) {
		p4d.x = gt[0];
		p4d.y = gt[3];

		/* return point */
		if (!raster->width && !raster->height) {
			LWPOINT *point = lwpoint_make2d(srid, p4d.x, p4d.y);
			*hull = lwpoint_as_lwgeom(point);
		}
		/* return linestring */
		else {
			LWLINE *line = NULL;
			pts = ptarray_construct_empty(0, 0, 2);

			/* first point of line */
			ptarray_append_point(pts, &p4d, LW_TRUE);

			/* second point of line */
			if (rt_raster_cell_to_geopoint(
				raster,
				rt_raster_get_width(raster), rt_raster_get_height(raster),
				&p4d.x, &p4d.y,
				gt
			) != ES_NONE) {
				rterror("rt_raster_get_convex_hull: Could not get second point for linestring");
				return ES_ERROR;
			}
			ptarray_append_point(pts, &p4d, LW_TRUE);
			line = lwline_construct(srid, NULL, pts);

			*hull = lwline_as_lwgeom(line);
		}

		return ES_NONE;
	}
	else {
		POINTARRAY **rings = NULL;
		LWPOLY* poly = NULL;

		/* only one ring */
		rings = (POINTARRAY **) rtalloc(sizeof (POINTARRAY*));
		if (!rings) {
			rterror("rt_raster_get_convex_hull: Could not allocate memory for polygon ring");
			return ES_ERROR;
		}
		rings[0] = ptarray_construct(0, 0, 5);
		/* TODO: handle error on ptarray construction */
		/* XXX jorgearevalo: the error conditions aren't managed in ptarray_construct */
		if (!rings[0]) {
			rterror("rt_raster_get_convex_hull: Could not construct point array");
			return ES_ERROR;
		}
		pts = rings[0];

		/* Upper-left corner (first and last points) */
		p4d.x = gt[0];
		p4d.y = gt[3];
		ptarray_set_point4d(pts, 0, &p4d);
		ptarray_set_point4d(pts, 4, &p4d);

		/* Upper-right corner (we go clockwise) */
		rt_raster_cell_to_geopoint(
			raster,
			raster->width, 0,
			&p4d.x, &p4d.y,
			gt
		);
		ptarray_set_point4d(pts, 1, &p4d);

		/* Lower-right corner */
		rt_raster_cell_to_geopoint(
			raster,
			raster->width, raster->height,
			&p4d.x, &p4d.y,
			gt
		);
		ptarray_set_point4d(pts, 2, &p4d);

		/* Lower-left corner */
		rt_raster_cell_to_geopoint(
			raster,
			0, raster->height,
			&p4d.x, &p4d.y,
			gt
		);
		ptarray_set_point4d(pts, 3, &p4d);

		poly = lwpoly_construct(srid, 0, 1, rings);
		*hull = lwpoly_as_lwgeom(poly);
	}

	return ES_NONE;
}

/******************************************************************************
* rt_raster_gdal_polygonize()
******************************************************************************/

/**
 * Returns a set of "geomval" value, one for each group of pixel
 * sharing the same value for the provided band.
 *
 * A "geomval" value is a complex type composed of a geometry 
 * in LWPOLY representation (one for each group of pixel sharing
 * the same value) and the value associated with this geometry.
 *
 * @param raster : the raster to get info from.
 * @param nband : the band to polygonize. 0-based
 * @param exclude_nodata_value : if non-zero, ignore nodata values
 * to check for pixels with value
 *
 * @return A set of "geomval" values, one for each group of pixels
 * sharing the same value for the provided band. The returned values are
 * LWPOLY geometries.
 */
rt_geomval
rt_raster_gdal_polygonize(
	rt_raster raster, int nband,
	int exclude_nodata_value,
	int *pnElements
) {
	CPLErr cplerr = CE_None;
	char *pszQuery;
	long j;
	OGRSFDriverH ogr_drv = NULL;
	GDALDriverH gdal_drv = NULL;
	int destroy_gdal_drv = 0;
	GDALDatasetH memdataset = NULL;
	GDALRasterBandH gdal_band = NULL;
	OGRDataSourceH memdatasource = NULL;
	rt_geomval pols = NULL;
	OGRLayerH hLayer = NULL;
	OGRFeatureH hFeature = NULL;
	OGRGeometryH hGeom = NULL;
	OGRFieldDefnH hFldDfn = NULL;
	unsigned char *wkb = NULL;
	int wkbsize = 0;
	LWGEOM *lwgeom = NULL;
	int nFeatureCount = 0;
	rt_band band = NULL;
	int iPixVal = -1;
	double dValue = 0.0;
	int iBandHasNodataValue = FALSE;
	double dBandNoData = 0.0;

	/* for checking that a geometry is valid */
	GEOSGeometry *ggeom = NULL;
	int isValid;
	LWGEOM *lwgeomValid = NULL;

	uint32_t bandNums[1] = {nband};
	int excludeNodataValues[1] = {exclude_nodata_value};

	/* checks */
	assert(NULL != raster);
	assert(NULL != pnElements);

	RASTER_DEBUG(2, "In rt_raster_gdal_polygonize");

	*pnElements = 0;

	/*******************************
	 * Get band
	 *******************************/
	band = rt_raster_get_band(raster, nband);
	if (NULL == band) {
		rterror("rt_raster_gdal_polygonize: Error getting band %d from raster", nband);
		return NULL;
	}

	if (exclude_nodata_value) {

		/* band is NODATA */
		if (rt_band_get_isnodata_flag(band)) {
			RASTER_DEBUG(3, "Band is NODATA.  Returning null");
			*pnElements = 0;
			return NULL;
		}

		iBandHasNodataValue = rt_band_get_hasnodata_flag(band);
		if (iBandHasNodataValue)
			rt_band_get_nodata(band, &dBandNoData);
		else
			exclude_nodata_value = FALSE;
	}

	/*****************************************************
	 * Convert raster to GDAL MEM dataset
	 *****************************************************/
	memdataset = rt_raster_to_gdal_mem(raster, NULL, bandNums, excludeNodataValues, 1, &gdal_drv, &destroy_gdal_drv);
	if (NULL == memdataset) {
		rterror("rt_raster_gdal_polygonize: Couldn't convert raster to GDAL MEM dataset");
		return NULL;
	}

	/*****************************
	 * Register ogr mem driver
	 *****************************/
#ifdef GDAL_DCAP_RASTER
	/* in GDAL 2.0, OGRRegisterAll() is an alias to GDALAllRegister() */
	rt_util_gdal_register_all(0);
#else
	OGRRegisterAll();
#endif

	RASTER_DEBUG(3, "creating OGR MEM vector");

	/*****************************************************
	 * Create an OGR in-memory vector for layers
	 *****************************************************/
	ogr_drv = OGRGetDriverByName("Memory");
	memdatasource = OGR_Dr_CreateDataSource(ogr_drv, "", NULL);
	if (NULL == memdatasource) {
		rterror("rt_raster_gdal_polygonize: Couldn't create a OGR Datasource to store pols");
		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		return NULL;
	}

	/* Can MEM driver create new layers? */
	if (!OGR_DS_TestCapability(memdatasource, ODsCCreateLayer)) {
		rterror("rt_raster_gdal_polygonize: MEM driver can't create new layers, aborting");

		/* xxx jorgearevalo: what should we do now? */
		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		OGRReleaseDataSource(memdatasource);

		return NULL;
	}

	RASTER_DEBUG(3, "polygonizying GDAL MEM raster band");

	/*****************************
	 * Polygonize the raster band
	 *****************************/

	/**
	 * From GDALPolygonize function header: "Polygon features will be
	 * created on the output layer, with polygon geometries representing
	 * the polygons". So,the WKB geometry type should be "wkbPolygon"
	 **/
	hLayer = OGR_DS_CreateLayer(memdatasource, "PolygonizedLayer", NULL, wkbPolygon, NULL);

	if (NULL == hLayer) {
		rterror("rt_raster_gdal_polygonize: Couldn't create layer to store polygons");

		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		OGRReleaseDataSource(memdatasource);

		return NULL;
	}

	/**
	 * Create a new field in the layer, to store the px value
	 */

	/* First, create a field definition to create the field */
	hFldDfn = OGR_Fld_Create("PixelValue", OFTReal);

	/* Second, create the field */
	if (OGR_L_CreateField(hLayer, hFldDfn, TRUE) != OGRERR_NONE) {
		rtwarn("Couldn't create a field in OGR Layer. The polygons generated won't be able to store the pixel value");
		iPixVal = -1;
	}
	else {
		/* Index to the new field created in the layer */
		iPixVal = 0;
	}

	/* Get GDAL raster band */
	gdal_band = GDALGetRasterBand(memdataset, 1);
	if (NULL == gdal_band) {
		rterror("rt_raster_gdal_polygonize: Couldn't get GDAL band to polygonize");

		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		OGR_Fld_Destroy(hFldDfn);
		OGR_DS_DeleteLayer(memdatasource, 0);
		OGRReleaseDataSource(memdatasource);
		
		return NULL;
	}

	/**
	 * We don't need a raster mask band. Each band has a nodata value.
	 **/
#ifdef GDALFPOLYGONIZE
	cplerr = GDALFPolygonize(gdal_band, NULL, hLayer, iPixVal, NULL, NULL, NULL);
#else
	cplerr = GDALPolygonize(gdal_band, NULL, hLayer, iPixVal, NULL, NULL, NULL);
#endif

	if (cplerr != CE_None) {
		rterror("rt_raster_gdal_polygonize: Could not polygonize GDAL band");

		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		OGR_Fld_Destroy(hFldDfn);
		OGR_DS_DeleteLayer(memdatasource, 0);
		OGRReleaseDataSource(memdatasource);

		return NULL;
	}

	/**
	 * Optimization: Apply a OGR SQL filter to the layer to select the
	 * features different from NODATA value.
	 *
	 * Thanks to David Zwarg.
	 **/
	if (iBandHasNodataValue) {
		pszQuery = (char *) rtalloc(50 * sizeof (char));
		sprintf(pszQuery, "PixelValue != %f", dBandNoData );
		OGRErr e = OGR_L_SetAttributeFilter(hLayer, pszQuery);
		if (e != OGRERR_NONE) {
			rtwarn("Error filtering NODATA values for band. All values will be treated as data values");
		}
	}
	else {
		pszQuery = NULL;
	}

	/*********************************************************************
	 * Transform OGR layers to WKB polygons
	 * XXX jorgearevalo: GDALPolygonize does not set the coordinate system
	 * on the output layer. Application code should do this when the layer
	 * is created, presumably matching the raster coordinate system.
	 *********************************************************************/
	nFeatureCount = OGR_L_GetFeatureCount(hLayer, TRUE);

	/* Allocate memory for pols */
	pols = (rt_geomval) rtalloc(nFeatureCount * sizeof(struct rt_geomval_t));

	if (NULL == pols) {
		rterror("rt_raster_gdal_polygonize: Could not allocate memory for geomval set");

		GDALClose(memdataset);
		if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
		OGR_Fld_Destroy(hFldDfn);
		OGR_DS_DeleteLayer(memdatasource, 0);
		if (NULL != pszQuery)
			rtdealloc(pszQuery);
		OGRReleaseDataSource(memdatasource);

		return NULL;
	}

	/* initialize GEOS */
	initGEOS(lwnotice, lwgeom_geos_error);

	RASTER_DEBUGF(3, "storing polygons (%d)", nFeatureCount);

	/* Reset feature reading to start in the first feature */
	OGR_L_ResetReading(hLayer);

	for (j = 0; j < nFeatureCount; j++) {
		hFeature = OGR_L_GetNextFeature(hLayer);
		dValue = OGR_F_GetFieldAsDouble(hFeature, iPixVal);

		hGeom = OGR_F_GetGeometryRef(hFeature);
		wkbsize = OGR_G_WkbSize(hGeom);

		/* allocate wkb buffer */
		wkb = rtalloc(sizeof(unsigned char) * wkbsize);
		if (wkb == NULL) {
			rterror("rt_raster_gdal_polygonize: Could not allocate memory for WKB buffer");

			OGR_F_Destroy(hFeature);
			GDALClose(memdataset);
			if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);
			OGR_Fld_Destroy(hFldDfn);
			OGR_DS_DeleteLayer(memdatasource, 0);
			if (NULL != pszQuery)
				rtdealloc(pszQuery);
			OGRReleaseDataSource(memdatasource);

			return NULL;
		}

		/* export WKB with LSB byte order */
		OGR_G_ExportToWkb(hGeom, wkbNDR, wkb);

		/* convert WKB to LWGEOM */
		lwgeom = lwgeom_from_wkb(wkb, wkbsize, LW_PARSER_CHECK_NONE);

#if POSTGIS_DEBUG_LEVEL > 3
		{
			char *wkt = NULL;
			OGR_G_ExportToWkt(hGeom, &wkt);
			RASTER_DEBUGF(4, "GDAL wkt = %s", wkt);
			CPLFree(wkt);

			d_print_binary_hex("GDAL wkb", wkb, wkbsize);
		}
#endif

		/* cleanup unnecessary stuff */
		rtdealloc(wkb);
		wkb = NULL;
		wkbsize = 0;

		OGR_F_Destroy(hFeature);

		/* specify SRID */
		lwgeom_set_srid(lwgeom, rt_raster_get_srid(raster));

		/*
			is geometry valid?
			if not, try to make valid
		*/
		do {
#if POSTGIS_GEOS_VERSION < 33
			/* nothing can be done if the geometry was invalid if GEOS < 3.3 */
			break;
#endif

			ggeom = (GEOSGeometry *) LWGEOM2GEOS(lwgeom, 0);
			if (ggeom == NULL) {
				rtwarn("Cannot test geometry for validity");
				break;
			}

			isValid = GEOSisValid(ggeom);

			GEOSGeom_destroy(ggeom);
			ggeom = NULL;

			/* geometry is valid */
			if (isValid)
				break;

			RASTER_DEBUG(3, "fixing invalid geometry");

			/* make geometry valid */
			lwgeomValid = lwgeom_make_valid(lwgeom);
			if (lwgeomValid == NULL) {
				rtwarn("Cannot fix invalid geometry");
				break;
			}

			lwgeom_free(lwgeom);
			lwgeom = lwgeomValid;
		}
		while (0);

		/* save lwgeom */
		pols[j].geom = lwgeom_as_lwpoly(lwgeom);

#if POSTGIS_DEBUG_LEVEL > 3
		{
			char *wkt = lwgeom_to_wkt(lwgeom, WKT_ISO, DBL_DIG, NULL);
			RASTER_DEBUGF(4, "LWGEOM wkt = %s", wkt);
			rtdealloc(wkt);

			size_t lwwkbsize = 0;
			uint8_t *lwwkb = lwgeom_to_wkb(lwgeom, WKB_ISO | WKB_NDR, &lwwkbsize);
			if (lwwkbsize) {
				d_print_binary_hex("LWGEOM wkb", lwwkb, lwwkbsize);
				rtdealloc(lwwkb);
			}
		}
#endif

		/* set pixel value */
		pols[j].val = dValue;
	}

	*pnElements = nFeatureCount;

	RASTER_DEBUG(3, "destroying GDAL MEM raster");
	GDALClose(memdataset);
	if (destroy_gdal_drv) GDALDestroyDriver(gdal_drv);

	RASTER_DEBUG(3, "destroying OGR MEM vector");
	OGR_Fld_Destroy(hFldDfn);
	OGR_DS_DeleteLayer(memdatasource, 0);
	if (NULL != pszQuery) rtdealloc(pszQuery);
	OGRReleaseDataSource(memdatasource);

	return pols;
}

