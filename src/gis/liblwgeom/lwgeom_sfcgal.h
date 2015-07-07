/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Wrapper around SFCGAL for 3D functions
 *
 * Copyright 2012-2013 Oslandia <infos@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/


#include "liblwgeom_internal.h"
#include <SFCGAL/capi/sfcgal_c.h>


/* return SFCGAL version string */
const char*
lwgeom_sfcgal_version(void);

/* Convert SFCGAL structure to lwgeom PostGIS */
LWGEOM*
SFCGAL2LWGEOM(const sfcgal_geometry_t* geom, int force3D, int SRID);

/* Convert lwgeom PostGIS to SFCGAL structure */
sfcgal_geometry_t*
LWGEOM2SFCGAL(const LWGEOM* geom);

/* No Operation SFCGAL function, used (only) for cunit tests
 * Take a PostGIS geometry, send it to SFCGAL and return it unchanged
 */ 
LWGEOM*
lwgeom_sfcgal_noop(const LWGEOM* geom_in);
