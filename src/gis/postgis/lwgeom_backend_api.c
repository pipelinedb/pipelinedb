/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Wrapper around external librairies functions (GEOS/CGAL...)
 *
 * Copyright 2012-2013 Oslandia <infos@oslandia.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h" /* for custom variables */

#include "../postgis_config.h"
#include "lwgeom_pg.h"
#include "liblwgeom.h"

#include "lwgeom_backend_api.h"
#include "lwgeom_geos.h"
#if HAVE_SFCGAL
#include "lwgeom_sfcgal.h"
#endif

Datum intersects(PG_FUNCTION_ARGS);
Datum intersects3d(PG_FUNCTION_ARGS);
Datum intersection(PG_FUNCTION_ARGS);
Datum area(PG_FUNCTION_ARGS);
Datum distance(PG_FUNCTION_ARGS);
Datum distance3d(PG_FUNCTION_ARGS);

Datum intersects3d_dwithin(PG_FUNCTION_ARGS);


struct lwgeom_backend_definition
{
    const char* name;
    Datum (*intersects_fn)    (PG_FUNCTION_ARGS);
    Datum (*intersects3d_fn)  (PG_FUNCTION_ARGS);
    Datum (*intersection_fn)  (PG_FUNCTION_ARGS);
    Datum (*area_fn)          (PG_FUNCTION_ARGS);
    Datum (*distance_fn)      (PG_FUNCTION_ARGS);
    Datum (*distance3d_fn)    (PG_FUNCTION_ARGS);
};

#if HAVE_SFCGAL
#define LWGEOM_NUM_BACKENDS   2
#else
#define LWGEOM_NUM_BACKENDS   1
#endif

struct lwgeom_backend_definition lwgeom_backends[LWGEOM_NUM_BACKENDS] = {
    { .name = "geos",
      .intersects_fn    = geos_intersects,
      .intersects3d_fn  = intersects3d_dwithin,
      .intersection_fn  = geos_intersection,
      .area_fn          = LWGEOM_area_polygon,
      .distance_fn      = LWGEOM_mindistance2d,
      .distance3d_fn    = LWGEOM_mindistance3d
    },
#if HAVE_SFCGAL
    { .name = "sfcgal",
      .intersects_fn    = sfcgal_intersects,
      .intersects3d_fn  = sfcgal_intersects3D,
      .intersection_fn  = sfcgal_intersection,
      .area_fn          = sfcgal_area,
      .distance_fn      = sfcgal_distance,
      .distance3d_fn    = sfcgal_distance3D
    }
#endif
};


/* Geometry Backend */
char* lwgeom_backend_name;
struct lwgeom_backend_definition* lwgeom_backend = &lwgeom_backends[0];

static void lwgeom_backend_switch( const char* newvalue, void* extra )
{
    int i;

    if (!newvalue) { return; }

    for ( i = 0; i < LWGEOM_NUM_BACKENDS; ++i ) {
	if ( !strcmp(lwgeom_backends[i].name, newvalue) ) {
	    lwgeom_backend = &lwgeom_backends[i];
	    return;
	}
    }
    lwerror("Can't find %s geometry backend", newvalue );
}

void lwgeom_init_backend()
{
    DefineCustomStringVariable( "postgis.backend", /* name */
				"Sets the PostGIS Geometry Backend.", /* short_desc */
				"Sets the PostGIS Geometry Backend (allowed values are 'geos' or 'sfcgal')", /* long_desc */
				&lwgeom_backend_name, /* valueAddr */
				(char *)lwgeom_backends[0].name, /* bootValue */
				PGC_USERSET, /* GucContext context */
				0, /* int flags */
#if POSTGIS_PGSQL_VERSION >= 91
				NULL, /* GucStringCheckHook check_hook */
#endif
				lwgeom_backend_switch, /* GucStringAssignHook assign_hook */
				NULL  /* GucShowHook show_hook */
				);
}

PG_FUNCTION_INFO_V1(intersects);
Datum intersects(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->intersects_fn)( fcinfo );
}

PG_FUNCTION_INFO_V1(intersection);
Datum intersection(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->intersection_fn)( fcinfo );
}

PG_FUNCTION_INFO_V1(area);
Datum area(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->area_fn)( fcinfo );
}

PG_FUNCTION_INFO_V1(distance);
Datum distance(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->distance_fn)( fcinfo );
}

PG_FUNCTION_INFO_V1(distance3d);
Datum distance3d(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->distance3d_fn)( fcinfo );
}

PG_FUNCTION_INFO_V1(intersects3d);
Datum intersects3d(PG_FUNCTION_ARGS)
{
    return (*lwgeom_backend->intersects3d_fn)( fcinfo );
}



/* intersects3d through dwithin
 * used by the 'geos' backend
 */
PG_FUNCTION_INFO_V1(intersects3d_dwithin);
Datum intersects3d_dwithin(PG_FUNCTION_ARGS)
{
    double mindist;
    GSERIALIZED *geom1 = PG_GETARG_GSERIALIZED_P(0);
    GSERIALIZED *geom2 = PG_GETARG_GSERIALIZED_P(1);
    LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
    LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);

    if (lwgeom1->srid != lwgeom2->srid)
    {
	elog(ERROR,"Operation on two GEOMETRIES with different SRIDs\n");
	PG_RETURN_NULL();
    }
    
    mindist = lwgeom_mindistance3d_tolerance(lwgeom1,lwgeom2,0.0);
    
    PG_FREE_IF_COPY(geom1, 0);
    PG_FREE_IF_COPY(geom2, 1);
    /*empty geometries cases should be right handled since return from underlying
      functions should be FLT_MAX which causes false as answer*/
    PG_RETURN_BOOL(0.0 == mindist);
}
