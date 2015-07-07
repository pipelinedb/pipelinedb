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

#include "postgres.h"
#include "fmgr.h"

#include "../postgis_config.h"
#include "lwgeom_cache.h"

/* 
* Generic statement caching infrastructure. We cache 
* the following kinds of objects:
* 
*   geometries-with-trees
*      PreparedGeometry, RTree, CIRC_TREE, RECT_TREE
*   srids-with-projections
*      projPJ
* 
* Each GenericCache* has a type, and after that
* some data. Similar to generic LWGEOM*. Test that
* the type number is what you expect before casting
* and de-referencing struct members.
*/
typedef struct {
	int type;
	char data[1];
} GenericCache;

/* 
* Although there are only two basic kinds of 
* cache entries, the actual trees stored in the
* geometries-with-trees pattern are quite diverse,
* and they might be used in combination, so we have 
* one slot for each tree type as well as a slot for
* projections.
*/
typedef struct {
	GenericCache* entry[NUM_CACHE_ENTRIES];
} GenericCacheCollection;

/**
* Utility function to read the upper memory context off a function call 
* info data.
*/
static MemoryContext 
FIContext(FunctionCallInfoData* fcinfo)
{
	return fcinfo->flinfo->fn_mcxt;
}

/**
* Get the generic collection off the statement, allocate a 
* new one if we don't have one already.
*/ 
static GenericCacheCollection* 
GetGenericCacheCollection(FunctionCallInfoData* fcinfo)
{
	GenericCacheCollection* cache = fcinfo->flinfo->fn_extra;

	if ( ! cache ) 
	{
		cache = MemoryContextAlloc(FIContext(fcinfo), sizeof(GenericCacheCollection));
		memset(cache, 0, sizeof(GenericCacheCollection));
		fcinfo->flinfo->fn_extra = cache;
	}
	return cache;		
}
	

/**
* Get the Proj4 entry from the generic cache if one exists.
* If it doesn't exist, make a new empty one and return it.
*/
PROJ4PortalCache*
GetPROJ4SRSCache(FunctionCallInfoData* fcinfo)
{
	GenericCacheCollection* generic_cache = GetGenericCacheCollection(fcinfo);
	PROJ4PortalCache* cache = (PROJ4PortalCache*)(generic_cache->entry[PROJ_CACHE_ENTRY]);
	
	if ( ! cache )
	{
		/* Allocate in the upper context */
		cache = MemoryContextAlloc(FIContext(fcinfo), sizeof(PROJ4PortalCache));

		if (cache)
		{
			int i;

			POSTGIS_DEBUGF(3, "Allocating PROJ4Cache for portal with transform() MemoryContext %p", FIContext(fcinfo));
			/* Put in any required defaults */
			for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
			{
				cache->PROJ4SRSCache[i].srid = SRID_UNKNOWN;
				cache->PROJ4SRSCache[i].projection = NULL;
				cache->PROJ4SRSCache[i].projection_mcxt = NULL;
			}
			cache->type = PROJ_CACHE_ENTRY;
			cache->PROJ4SRSCacheCount = 0;
			cache->PROJ4SRSCacheContext = FIContext(fcinfo);

			/* Store the pointer in GenericCache */
			generic_cache->entry[PROJ_CACHE_ENTRY] = (GenericCache*)cache;
		}
	}
	return cache;
}

/**
* Get an appropriate (based on the entry type number) 
* GeomCache entry from the generic cache if one exists.
* Returns a cache pointer if there is a cache hit and we have an
* index built and ready to use. Returns NULL otherwise.
*/
GeomCache*            
GetGeomCache(FunctionCallInfoData* fcinfo, const GeomCacheMethods* cache_methods, const GSERIALIZED* g1, const GSERIALIZED* g2)
{
	GeomCache* cache;
	int cache_hit = 0;
	MemoryContext old_context;
	const GSERIALIZED *geom;
	GenericCacheCollection* generic_cache = GetGenericCacheCollection(fcinfo);
	int entry_number = cache_methods->entry_number;
	
	Assert(entry_number >= 0);
	Assert(entry_number < NUM_CACHE_ENTRIES);
	
	cache = (GeomCache*)(generic_cache->entry[entry_number]);
	
	if ( ! cache )
	{
		old_context = MemoryContextSwitchTo(FIContext(fcinfo));
		/* Allocate in the upper context */
		cache = cache_methods->GeomCacheAllocator();
		MemoryContextSwitchTo(old_context);
		/* Store the pointer in GenericCache */
		cache->type = entry_number;
		generic_cache->entry[entry_number] = (GenericCache*)cache;
	}	

	/* Cache hit on the first argument */
	if ( g1 &&
	     cache->argnum != 2 &&
	     cache->geom1_size == VARSIZE(g1) &&
	     memcmp(cache->geom1, g1, cache->geom1_size) == 0 )
	{
		cache_hit = 1;
		geom = cache->geom1;

	}
	/* Cache hit on second argument */
	else if ( g2 &&
	          cache->argnum != 1 &&
	          cache->geom2_size == VARSIZE(g2) &&
	          memcmp(cache->geom2, g2, cache->geom2_size) == 0 )
	{
		cache_hit = 2;
		geom = cache->geom2;
	}
	/* No cache hit. If we have a tree, free it. */
	else
	{
		cache_hit = 0;
		if ( cache->argnum )
		{
			cache_methods->GeomIndexFreer(cache);
			cache->argnum = 0;
		}
	}

	/* Cache hit, but no tree built yet, build it! */
	if ( cache_hit && ! cache->argnum )
	{
		int rv;
		LWGEOM *lwgeom = lwgeom_from_gserialized(geom);

		/* Can't build a tree on a NULL or empty */
		if ( (!lwgeom) || lwgeom_is_empty(lwgeom) )
			return NULL;

		old_context = MemoryContextSwitchTo(FIContext(fcinfo));
		rv = cache_methods->GeomIndexBuilder(lwgeom, cache);
		MemoryContextSwitchTo(old_context);
		cache->argnum = cache_hit;

		/* Something went awry in the tree build phase */
		if ( ! rv )
		{
			cache->argnum = 0;
			return NULL;
		}

	}

	/* We have a hit and a calculated tree, we're done */
	if ( cache_hit && cache->argnum )
		return cache;

	/* Argument one didn't match, so copy the new value in. */
	if ( g1 && cache_hit != 1 )
	{
		if ( cache->geom1 ) pfree(cache->geom1);
		cache->geom1_size = VARSIZE(g1);
		cache->geom1 = MemoryContextAlloc(FIContext(fcinfo), cache->geom1_size);
		memcpy(cache->geom1, g1, cache->geom1_size);
	}
	/* Argument two didn't match, so copy the new value in. */
	if ( g2 && cache_hit != 2 )
	{
		if ( cache->geom2 ) pfree(cache->geom2);
		cache->geom2_size = VARSIZE(g2);
		cache->geom2 = MemoryContextAlloc(FIContext(fcinfo), cache->geom2_size);
		memcpy(cache->geom2, g2, cache->geom2_size);
	}

	return NULL;
}


