/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2012 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#ifndef LWGEOM_CACHE_H_
#define LWGEOM_CACHE_H_ 1

#include "postgres.h"
#include "fmgr.h"

#include "liblwgeom_internal.h"
#include "lwgeodetic_tree.h"
#include "lwgeom_pg.h"


#define PROJ_CACHE_ENTRY 0
#define PREP_CACHE_ENTRY 1
#define RTREE_CACHE_ENTRY 2
#define CIRC_CACHE_ENTRY 3
#define RECT_CACHE_ENTRY 4

#define NUM_CACHE_ENTRIES 16


/* 
* A generic GeomCache just needs space for the cache type,
* the cache keys (GSERIALIZED geometries), the key sizes, 
* and the argument number the cached index/tree is going
* to refer to.
*/
typedef struct {
	int                         type;
	GSERIALIZED*                geom1;
	GSERIALIZED*                geom2;
	size_t                      geom1_size;
	size_t                      geom2_size;
	int32                       argnum; 
} GeomCache;

/*
* Other specific geometry cache types are the 
* RTreeGeomCache - lwgeom_rtree.h
* PrepGeomCache - lwgeom_geos_prepared.h
*/

/* 
* Proj4 caching has it's own mechanism, but is 
* integrated into the generic caching system because
* some geography functions make cached SRID lookup
* calls and also CircTree accelerated calls, so 
* there needs to be a management object on 
* fcinfo->flinfo->fn_extra to avoid collisions.
*/

/* An entry in the PROJ4 SRS cache */
typedef struct struct_PROJ4SRSCacheItem
{
	int srid;
	projPJ projection;
	MemoryContext projection_mcxt;
}
PROJ4SRSCacheItem;

/* PROJ 4 lookup transaction cache methods */
#define PROJ4_CACHE_ITEMS	8

/*
* The proj4 cache holds a fixed number of reprojection
* entries. In normal usage we don't expect it to have
* many entries, so we always linearly scan the list.
*/
typedef struct struct_PROJ4PortalCache
{
	int type;
	PROJ4SRSCacheItem PROJ4SRSCache[PROJ4_CACHE_ITEMS];
	int PROJ4SRSCacheCount;
	MemoryContext PROJ4SRSCacheContext;
}
PROJ4PortalCache;

/**
* Generic signature for functions to manage a geometry
* cache structure.  
*/
typedef struct 
{
	int entry_number; /* What kind of structure is this? */
	int (*GeomIndexBuilder)(const LWGEOM* lwgeom, GeomCache* cache); /* Build an index/tree and add it to your cache */
	int (*GeomIndexFreer)(GeomCache* cache); /* Free the index/tree in your cache */
	GeomCache* (*GeomCacheAllocator)(void); /* Allocate the kind of cache object you use (GeomCache+some extra space) */
} GeomCacheMethods;

/* 
* Cache retrieval functions
*/
PROJ4PortalCache*  GetPROJ4SRSCache(FunctionCallInfoData *fcinfo);
GeomCache*         GetGeomCache(FunctionCallInfoData *fcinfo, const GeomCacheMethods* cache_methods, const GSERIALIZED* g1, const GSERIALIZED* g2);

#endif /* LWGEOM_CACHE_H_ */
