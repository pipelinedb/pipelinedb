/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"

typedef struct srs_precision 
{
	int precision_xy;
	int precision_z;
	int precision_m;
} srs_precision;

char* GetProj4StringSPI(int srid);
void SetPROJ4LibPath(void) ;


/**
 * Opaque type to use in the projection cache API.
 */
typedef void *Proj4Cache ;

void SetPROJ4LibPath(void);
Proj4Cache GetPROJ4Cache(FunctionCallInfo fcinfo) ;
bool IsInPROJ4Cache(Proj4Cache cache, int srid) ;
void AddToPROJ4Cache(Proj4Cache cache, int srid, int other_srid);
void DeleteFromPROJ4Cache(Proj4Cache cache, int srid) ;
projPJ GetProjectionFromPROJ4Cache(Proj4Cache cache, int srid);
int GetProjectionsUsingFCInfo(FunctionCallInfo fcinfo, int srid1, int srid2, projPJ *pj1, projPJ *pj2);
int spheroid_init_from_srid(FunctionCallInfo fcinfo, int srid, SPHEROID *s);
void srid_is_latlong(FunctionCallInfo fcinfo, int srid);
srs_precision srid_axis_precision(FunctionCallInfo fcinfo, int srid, int precision);

/**
 * Builtin SRID values
 * @{
 */

/**  Start of the reserved offset */
#define SRID_RESERVE_OFFSET  999000

/**  World Mercator, equivalent to EPSG:3395 */
#define SRID_WORLD_MERCATOR  999000

/**  Start of UTM North zone, equivalent to EPSG:32601 */
#define SRID_NORTH_UTM_START 999001

/**  End of UTM North zone, equivalent to EPSG:32660 */
#define SRID_NORTH_UTM_END   999060

/** Lambert Azimuthal Equal Area (LAEA) North Pole, equivalent to EPSG:3574 */
#define SRID_NORTH_LAMBERT   999061

/** PolarSteregraphic North, equivalent to EPSG:3995 */
#define SRID_NORTH_STEREO    999062

/**  Start of UTM South zone, equivalent to EPSG:32701 */
#define SRID_SOUTH_UTM_START 999101

/**  Start of UTM South zone, equivalent to EPSG:32760 */
#define SRID_SOUTH_UTM_END   999160

/** Lambert Azimuthal Equal Area (LAEA) South Pole, equivalent to EPSG:3409 */
#define SRID_SOUTH_LAMBERT   999161

/** PolarSteregraphic South, equivalent to EPSG:3031 */
#define SRID_SOUTH_STEREO    999162

/** LAEA zones start (6 latitude bands x up to 20 longitude bands) */
#define SRID_LAEA_START 999163

/** LAEA zones end (6 latitude bands x up to 20 longitude bands) */
#define SRID_LAEA_END 999283


/** @} */

