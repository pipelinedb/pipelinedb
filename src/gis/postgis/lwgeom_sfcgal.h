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

#include "../liblwgeom/lwgeom_sfcgal.h"

/* Conversion from GSERIALIZED* to SFCGAL::Geometry */
sfcgal_geometry_t* POSTGIS2SFCGALGeometry(GSERIALIZED *pglwgeom);

/* Conversion from GSERIALIZED* to SFCGAL::PreparedGeometry */
sfcgal_prepared_geometry_t* POSTGIS2SFCGALPreparedGeometry(GSERIALIZED *pglwgeom);

/* Conversion from SFCGAL::Geometry to GSERIALIZED */
GSERIALIZED* SFCGALGeometry2POSTGIS( const sfcgal_geometry_t* geom, int force3D, int SRID );

/* Conversion from SFCGAL::PreparedGeometry to GSERIALIZED */
GSERIALIZED* SFCGALPreparedGeometry2POSTGIS( const sfcgal_prepared_geometry_t* geom, int force3D );

Datum sfcgal_intersects(PG_FUNCTION_ARGS);
Datum sfcgal_intersects3D(PG_FUNCTION_ARGS);
Datum sfcgal_intersection(PG_FUNCTION_ARGS);
Datum sfcgal_triangulate(PG_FUNCTION_ARGS);
Datum sfcgal_area(PG_FUNCTION_ARGS);
Datum sfcgal_distance(PG_FUNCTION_ARGS);
Datum sfcgal_distance3D(PG_FUNCTION_ARGS);


/* Initialize sfcgal with PostGIS error handlers */
void sfcgal_postgis_init(void);
