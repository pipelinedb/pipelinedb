/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2001-2003 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/


/* PostgreSQL headers */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "access/hash.h"
#include "utils/hsearch.h"

/* PostGIS headers */
#include "../postgis_config.h"
#include "liblwgeom.h"
#include "lwgeom_pg.h"
#include "lwgeom_cache.h"
#include "lwgeom_transform.h"

/* C headers */
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>


/* Expose an internal Proj function */
int pj_transform_nodatum(projPJ srcdefn, projPJ dstdefn, long point_count, int point_offset, double *x, double *y, double *z );


/*
 * PROJ 4 backend hash table initial hash size
 * (since 16 is the default portal hash table size, and we would
 * typically have 2 entries per portal
 * then we shall use a default size of 32)
 */
#define PROJ4_BACKEND_HASH_SIZE	32


/**
 * Backend projPJ hash table
 *
 * This hash table stores a key/value pair of MemoryContext/projPJ objects.
 * Whenever we create a projPJ object using pj_init(), we create a separate
 * MemoryContext as a child context of the current executor context.
 * The MemoryContext/projPJ object is stored in this hash table so
 * that when PROJ4SRSCacheDelete() is called during query cleanup, we can
 * lookup the projPJ object based upon the MemoryContext parameter and hence
 * pj_free() it.
 */
static HTAB *PJHash = NULL;

typedef struct struct_PJHashEntry
{
	MemoryContext ProjectionContext;
	projPJ projection;
}
PJHashEntry;


/* PJ Hash API */
uint32 mcxt_ptr_hash(const void *key, Size keysize);

static HTAB *CreatePJHash(void);
static void AddPJHashEntry(MemoryContext mcxt, projPJ projection);
static projPJ GetPJHashEntry(MemoryContext mcxt);
static void DeletePJHashEntry(MemoryContext mcxt);

/* Internal Cache API */
/* static PROJ4PortalCache *GetPROJ4SRSCache(FunctionCallInfo fcinfo) ; */
static bool IsInPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid);
static projPJ GetProjectionFromPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid);
static void AddToPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid, int other_srid);
static void DeleteFromPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid);

/* Search path for PROJ.4 library */
static bool IsPROJ4LibPathSet = false;
void SetPROJ4LibPath(void);

/* Memory context cache functions */
static void PROJ4SRSCacheInit(MemoryContext context);
static void PROJ4SRSCacheDelete(MemoryContext context);
static void PROJ4SRSCacheReset(MemoryContext context);
static bool PROJ4SRSCacheIsEmpty(MemoryContext context);
static void PROJ4SRSCacheStats(MemoryContext context, int level);
#ifdef MEMORY_CONTEXT_CHECKING
static void PROJ4SRSCacheCheck(MemoryContext context);
#endif


/* Memory context definition must match the current version of PostgreSQL */
static MemoryContextMethods PROJ4SRSCacheContextMethods =
{
	NULL,
	NULL,
	NULL,
	PROJ4SRSCacheInit,
	PROJ4SRSCacheReset,
	PROJ4SRSCacheDelete,
	NULL,
	PROJ4SRSCacheIsEmpty,
	PROJ4SRSCacheStats
#ifdef MEMORY_CONTEXT_CHECKING
	,PROJ4SRSCacheCheck
#endif
};


static void
PROJ4SRSCacheInit(MemoryContext context)
{
	/*
	 * Do nothing as the cache is initialised when the transform()
	 * function is first called
	 */
}

static void
PROJ4SRSCacheDelete(MemoryContext context)
{
	projPJ projection;

	/* Lookup the projPJ pointer in the global hash table so we can free it */
	projection = GetPJHashEntry(context);

	if (!projection)
		elog(ERROR, "PROJ4SRSCacheDelete: Trying to delete non-existant projection object with MemoryContext key (%p)", (void *)context);

	POSTGIS_DEBUGF(3, "deleting projection object (%p) with MemoryContext key (%p)", projection, context);

	/* Free it */
	pj_free(projection);

	/* Remove the hash entry as it is no longer needed */
	DeletePJHashEntry(context);
}

static void
PROJ4SRSCacheReset(MemoryContext context)
{
	/*
	 * Do nothing, but we must supply a function since this call is mandatory according to tgl
	 * (see postgis-devel archives July 2007)
	 */
}

static bool
PROJ4SRSCacheIsEmpty(MemoryContext context)
{
	/*
	 * Always return false since this call is mandatory according to tgl
	 * (see postgis-devel archives July 2007)
	 */
	return FALSE;
}

static void
PROJ4SRSCacheStats(MemoryContext context, int level)
{
	/*
	 * Simple stats display function - we must supply a function since this call is mandatory according to tgl
	 * (see postgis-devel archives July 2007)
	 */

	fprintf(stderr, "%s: PROJ4 context\n", context->name);
}

#ifdef MEMORY_CONTEXT_CHECKING
static void
PROJ4SRSCacheCheck(MemoryContext context)
{
	/*
	 * Do nothing - stub required for when PostgreSQL is compiled
	 * with MEMORY_CONTEXT_CHECKING defined
	 */
}
#endif


/*
 * PROJ4 projPJ Hash Table functions
 */


/**
 * A version of tag_hash - we specify this here as the implementation
 * has changed over the years....
 */

uint32 mcxt_ptr_hash(const void *key, Size keysize)
{
	uint32 hashval;

	hashval = DatumGetUInt32(hash_any(key, keysize));

	return hashval;
}


static HTAB *CreatePJHash(void)
{
	HASHCTL ctl;

	ctl.keysize = sizeof(MemoryContext);
	ctl.entrysize = sizeof(PJHashEntry);
	ctl.hash = mcxt_ptr_hash;

	return hash_create("PostGIS PROJ4 Backend projPJ MemoryContext Hash", PROJ4_BACKEND_HASH_SIZE, &ctl, (HASH_ELEM | HASH_FUNCTION));
}

static void AddPJHashEntry(MemoryContext mcxt, projPJ projection)
{
	bool found;
	void **key;
	PJHashEntry *he;

	/* The hash key is the MemoryContext pointer */
	key = (void *)&mcxt;

	he = (PJHashEntry *) hash_search(PJHash, key, HASH_ENTER, &found);
	if (!found)
	{
		/* Insert the entry into the new hash element */
		he->ProjectionContext = mcxt;
		he->projection = projection;
	}
	else
	{
		elog(ERROR, "AddPJHashEntry: PROJ4 projection object already exists for this MemoryContext (%p)",
		     (void *)mcxt);
	}
}

static projPJ GetPJHashEntry(MemoryContext mcxt)
{
	void **key;
	PJHashEntry *he;

	/* The hash key is the MemoryContext pointer */
	key = (void *)&mcxt;

	/* Return the projection object from the hash */
	he = (PJHashEntry *) hash_search(PJHash, key, HASH_FIND, NULL);

	return he->projection;
}


static void DeletePJHashEntry(MemoryContext mcxt)
{
	void **key;
	PJHashEntry *he;

	/* The hash key is the MemoryContext pointer */
	key = (void *)&mcxt;

	/* Delete the projection object from the hash */
	he = (PJHashEntry *) hash_search(PJHash, key, HASH_REMOVE, NULL);

	he->projection = NULL;

	if (!he)
		elog(ERROR, "DeletePJHashEntry: There was an error removing the PROJ4 projection object from this MemoryContext (%p)", (void *)mcxt);
}

bool
IsInPROJ4Cache(Proj4Cache PROJ4Cache, int srid) {
	return IsInPROJ4SRSCache((PROJ4PortalCache *)PROJ4Cache, srid) ;
}

/*
 * Per-cache management functions
 */

static bool
IsInPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid)
{
	/*
	 * Return true/false depending upon whether the item
	 * is in the SRS cache.
	 */

	int i;

	for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
	{
		if (PROJ4Cache->PROJ4SRSCache[i].srid == srid)
			return 1;
	}

	/* Otherwise not found */
	return 0;
}

projPJ GetProjectionFromPROJ4Cache(Proj4Cache cache, int srid)
{
	return GetProjectionFromPROJ4SRSCache((PROJ4PortalCache *)cache, srid) ;
}

/**
 * Return the projection object from the cache (we should
 * already have checked it exists using IsInPROJ4SRSCache first)
 */
static projPJ
GetProjectionFromPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid)
{
	int i;

	for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
	{
		if (PROJ4Cache->PROJ4SRSCache[i].srid == srid)
			return PROJ4Cache->PROJ4SRSCache[i].projection;
	}

	return NULL;
}

char* GetProj4StringSPI(int srid)
{
	static int maxproj4len = 512;
	int spi_result;
	char *proj_str = palloc(maxproj4len);
	char proj4_spi_buffer[256];

	/* Connect */
	spi_result = SPI_connect();
	if (spi_result != SPI_OK_CONNECT)
	{
		elog(ERROR, "GetProj4StringSPI: Could not connect to database using SPI");
	}

	/* Execute the lookup query */
	snprintf(proj4_spi_buffer, 255, "SELECT proj4text FROM spatial_ref_sys WHERE srid = %d LIMIT 1", srid);
	spi_result = SPI_exec(proj4_spi_buffer, 1);

	/* Read back the PROJ4 text */
	if (spi_result == SPI_OK_SELECT && SPI_processed > 0)
	{
		/* Select the first (and only tuple) */
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		HeapTuple tuple = tuptable->vals[0];
		char *proj4text = SPI_getvalue(tuple, tupdesc, 1);

		if ( proj4text )
		{
			/* Make a projection object out of it */
			strncpy(proj_str, proj4text, maxproj4len - 1);
		}
		else
		{
			proj_str[0] = 0;
		}
	}
	else
	{
		elog(ERROR, "GetProj4StringSPI: Cannot find SRID (%d) in spatial_ref_sys", srid);
	}

	spi_result = SPI_finish();
	if (spi_result != SPI_OK_FINISH)
	{
		elog(ERROR, "GetProj4StringSPI: Could not disconnect from database using SPI");
	}

	return proj_str;
}


/**
 *  Given an SRID, return the proj4 text.
 *  If the integer is one of the "well known" projections we support
 *  (WGS84 UTM N/S, Polar Stereographic N/S - see SRID_* macros),
 *  return the proj4text for those.
 */
static char* GetProj4String(int srid)
{
	static int maxproj4len = 512;

	/* SRIDs in SPATIAL_REF_SYS */
	if ( srid < SRID_RESERVE_OFFSET )
	{
		return GetProj4StringSPI(srid);
	}
	/* Automagic SRIDs */
	else
	{
		char *proj_str = palloc(maxproj4len);
		int id = srid;
		/* UTM North */
		if ( id >= SRID_NORTH_UTM_START && id <= SRID_NORTH_UTM_END )
		{
			snprintf(proj_str, maxproj4len, "+proj=utm +zone=%d +ellps=WGS84 +datum=WGS84 +units=m +no_defs", id - SRID_NORTH_UTM_START + 1);
		}
		/* UTM South */
		else if ( id >= SRID_SOUTH_UTM_START && id <= SRID_SOUTH_UTM_END )
		{
			snprintf(proj_str, maxproj4len, "+proj=utm +zone=%d +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs", id - SRID_SOUTH_UTM_START + 1);
		}
		/* Lambert zones (about 30x30, larger in higher latitudes) */
		/* There are three latitude zones, divided at -90,-60,-30,0,30,60,90. */
		/* In yzones 2,3 (equator) zones, the longitudinal zones are divided every 30 degrees (12 of them) */
		/* In yzones 1,4 (temperate) zones, the longitudinal zones are every 45 degrees (8 of them) */
		/* In yzones 0,5 (polar) zones, the longitudinal zones are ever 90 degrees (4 of them) */
		else if ( id >= SRID_LAEA_START && id <= SRID_LAEA_END )
		{
			int zone = id - SRID_LAEA_START;
			int xzone = zone % 20;
			int yzone = zone / 20;
			double lat_0 = 30.0 * (yzone - 3) + 15.0;
			double lon_0 = 0.0;
			
			/* The number of xzones is variable depending on yzone */
			if  ( yzone == 2 || yzone == 3 )
				lon_0 = 30.0 * (xzone - 6) + 15.0;
			else if ( yzone == 1 || yzone == 4 )
				lon_0 = 45.0 * (xzone - 4) + 22.5;
			else if ( yzone == 0 || yzone == 5 )
				lon_0 = 90.0 * (xzone - 2) + 45.0;
			else
				lwerror("Unknown yzone encountered!");
			
			snprintf(proj_str, maxproj4len, "+proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs", lat_0, lon_0);
		}
		/* Lambert Azimuthal Equal Area South Pole */
		else if ( id == SRID_SOUTH_LAMBERT )
		{
			strncpy(proj_str, "+proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs", maxproj4len );
		}
		/* Polar Sterographic South */
		else if ( id == SRID_SOUTH_STEREO )
		{
			strncpy(proj_str, "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs", maxproj4len);
		}
		/* Lambert Azimuthal Equal Area North Pole */
		else if ( id == SRID_NORTH_LAMBERT )
		{
			strncpy(proj_str, "+proj=laea +lat_0=90 +lon_0=-40 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs", maxproj4len );
		}
		/* Polar Stereographic North */
		else if ( id == SRID_NORTH_STEREO )
		{
			strncpy(proj_str, "+proj=stere +lat_0=90 +lat_ts=71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs", maxproj4len );
		}
		/* World Mercator */
		else if ( id == SRID_WORLD_MERCATOR )
		{
			strncpy(proj_str, "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs", maxproj4len );
		}
		else
		{
			elog(ERROR, "Invalid reserved SRID (%d)", srid);
			return NULL;
		}

		POSTGIS_DEBUGF(3, "returning on SRID=%d: %s", srid, proj_str);
		return proj_str;
	}
}

void AddToPROJ4Cache(Proj4Cache cache, int srid, int other_srid) {
	AddToPROJ4SRSCache((PROJ4PortalCache *)cache, srid, other_srid) ;
}


/**
 * Add an entry to the local PROJ4 SRS cache. If we need to wrap around then
 * we must make sure the entry we choose to delete does not contain other_srid
 * which is the definition for the other half of the transformation.
 */
static void
AddToPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid, int other_srid)
{
	MemoryContext PJMemoryContext;
	projPJ projection = NULL;
	char *proj_str = NULL;

	/*
	** Turn the SRID number into a proj4 string, by reading from spatial_ref_sys
	** or instantiating a magical value from a negative srid.
	*/
	proj_str = GetProj4String(srid);
	if ( ! proj_str )
	{
		elog(ERROR, "GetProj4String returned NULL for SRID (%d)", srid);
	}

	projection = lwproj_from_string(proj_str);
	if ( projection == NULL )
	{
		char *pj_errstr = pj_strerrno(*pj_get_errno_ref());
		if ( ! pj_errstr )
			pj_errstr = "";
		
		elog(ERROR,
		    "AddToPROJ4SRSCache: could not parse proj4 string '%s' %s",
		    proj_str, pj_errstr);
	}

	/*
	 * If the cache is already full then find the first entry
	 * that doesn't contain other_srid and use this as the
	 * subsequent value of PROJ4SRSCacheCount
	 */
	if (PROJ4Cache->PROJ4SRSCacheCount == PROJ4_CACHE_ITEMS)
	{
		bool found = false;
		int i;

		for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
		{
			if (PROJ4Cache->PROJ4SRSCache[i].srid != other_srid && found == false)
			{
				POSTGIS_DEBUGF(3, "choosing to remove item from query cache with SRID %d and index %d", PROJ4Cache->PROJ4SRSCache[i].srid, i);

				DeleteFromPROJ4SRSCache(PROJ4Cache, PROJ4Cache->PROJ4SRSCache[i].srid);
				PROJ4Cache->PROJ4SRSCacheCount = i;

				found = true;
			}
		}
	}

	/*
	 * Now create a memory context for this projection and
	 * store it in the backend hash
	 */
	POSTGIS_DEBUGF(3, "adding SRID %d with proj4text \"%s\" to query cache at index %d", srid, proj_str, PROJ4Cache->PROJ4SRSCacheCount);

	PJMemoryContext = MemoryContextCreate(T_AllocSetContext, 8192,
	                                      &PROJ4SRSCacheContextMethods,
	                                      PROJ4Cache->PROJ4SRSCacheContext,
	                                      "PostGIS PROJ4 PJ Memory Context");

	/* Create the backend hash if it doesn't already exist */
	if (!PJHash)
		PJHash = CreatePJHash();

	/*
	 * Add the MemoryContext to the backend hash so we can
	 * clean up upon portal shutdown
	 */
	POSTGIS_DEBUGF(3, "adding projection object (%p) to hash table with MemoryContext key (%p)", projection, PJMemoryContext);

	AddPJHashEntry(PJMemoryContext, projection);

	PROJ4Cache->PROJ4SRSCache[PROJ4Cache->PROJ4SRSCacheCount].srid = srid;
	PROJ4Cache->PROJ4SRSCache[PROJ4Cache->PROJ4SRSCacheCount].projection = projection;
	PROJ4Cache->PROJ4SRSCache[PROJ4Cache->PROJ4SRSCacheCount].projection_mcxt = PJMemoryContext;
	PROJ4Cache->PROJ4SRSCacheCount++;

	/* Free the projection string */
	pfree(proj_str);

}

void DeleteFromPROJ4Cache(Proj4Cache cache, int srid) {
	DeleteFromPROJ4SRSCache((PROJ4PortalCache *)cache, srid) ;
}


static void DeleteFromPROJ4SRSCache(PROJ4PortalCache *PROJ4Cache, int srid)
{
	/*
	 * Delete the SRID entry from the cache
	 */

	int i;

	for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
	{
		if (PROJ4Cache->PROJ4SRSCache[i].srid == srid)
		{
			POSTGIS_DEBUGF(3, "removing query cache entry with SRID %d at index %d", srid, i);

			/*
			 * Zero out the entries and free the PROJ4 handle
			 * by deleting the memory context
			 */
			MemoryContextDelete(PROJ4Cache->PROJ4SRSCache[i].projection_mcxt);
			PROJ4Cache->PROJ4SRSCache[i].projection = NULL;
			PROJ4Cache->PROJ4SRSCache[i].projection_mcxt = NULL;
			PROJ4Cache->PROJ4SRSCache[i].srid = SRID_UNKNOWN;
		}
	}
}


/**
 * Specify an alternate directory for the PROJ.4 grid files
 * (this should augment the PROJ.4 compile-time path)
 *
 * It's main purpose is to allow Win32 PROJ.4 installations
 * to find a set grid shift files, although other platforms
 * may find this useful too.
 *
 * Note that we currently ignore this on PostgreSQL < 8.0
 * since the method of determining the current installation
 * path are different on older PostgreSQL versions.
 */
void SetPROJ4LibPath(void)
{
	char *path;
	char *share_path;
	const char **proj_lib_path;

	if (!IsPROJ4LibPathSet) {

		/*
		 * Get the sharepath and append /contrib/postgis/proj to form a suitable
		 * directory in which to store the grid shift files
		 */
		proj_lib_path = palloc(sizeof(char *));

		share_path = palloc(MAXPGPATH);
		get_share_path(my_exec_path, share_path);

		path = palloc(MAXPGPATH);
		*proj_lib_path = path;

		snprintf(path, MAXPGPATH - 1, "%s/contrib/postgis-%s.%s/proj", share_path, POSTGIS_MAJOR_VERSION, POSTGIS_MINOR_VERSION);

		/* Set the search path for PROJ.4 */
		pj_set_searchpath(1, proj_lib_path);

		/* Ensure we only do this once... */
		IsPROJ4LibPathSet = true;
	}
}

Proj4Cache GetPROJ4Cache(FunctionCallInfo fcinfo) {
	return (Proj4Cache)GetPROJ4SRSCache(fcinfo);
}

#if 0
static PROJ4PortalCache *GetPROJ4SRSCache(FunctionCallInfo fcinfo)
{
	PROJ4PortalCache *PROJ4Cache = (GetGeomCache(fcinfo))->proj;

	/*
	 * If we have not already created PROJ4 cache for this portal
	 * then create it
	 */
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		MemoryContext old_context;

		old_context = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		PROJ4Cache = palloc(sizeof(PROJ4PortalCache));
		MemoryContextSwitchTo(old_context);

		if (PROJ4Cache)
		{
			int i;

			POSTGIS_DEBUGF(3, "Allocating PROJ4Cache for portal with transform() MemoryContext %p", fcinfo->flinfo->fn_mcxt);
			/* Put in any required defaults */
			for (i = 0; i < PROJ4_CACHE_ITEMS; i++)
			{
				PROJ4Cache->PROJ4SRSCache[i].srid = SRID_UNKNOWN;
				PROJ4Cache->PROJ4SRSCache[i].projection = NULL;
				PROJ4Cache->PROJ4SRSCache[i].projection_mcxt = NULL;
			}
			PROJ4Cache->PROJ4SRSCacheCount = 0;
			PROJ4Cache->PROJ4SRSCacheContext = fcinfo->flinfo->fn_mcxt;

			/* Store the pointer in fcinfo->flinfo->fn_extra */
			fcinfo->flinfo->fn_extra = PROJ4Cache;
		}
	}
	else
	{
		/* Use the existing cache */
		PROJ4Cache = fcinfo->flinfo->fn_extra;
	}

	return PROJ4Cache ;
}
#endif

int
GetProjectionsUsingFCInfo(FunctionCallInfo fcinfo, int srid1, int srid2, projPJ *pj1, projPJ *pj2)
{
	Proj4Cache *proj_cache = NULL;

	/* Set the search path if we haven't already */
	SetPROJ4LibPath();

	/* get or initialize the cache for this round */
	proj_cache = GetPROJ4Cache(fcinfo);
	if ( !proj_cache )
		return LW_FAILURE;

	/* Add the output srid to the cache if it's not already there */
	if (!IsInPROJ4Cache(proj_cache, srid1))
		AddToPROJ4Cache(proj_cache, srid1, srid2);

	/* Add the input srid to the cache if it's not already there */
	if (!IsInPROJ4Cache(proj_cache, srid2))
		AddToPROJ4Cache(proj_cache, srid2, srid1);

	/* Get the projections */
	*pj1 = GetProjectionFromPROJ4Cache(proj_cache, srid1);
	*pj2 = GetProjectionFromPROJ4Cache(proj_cache, srid2);

	return LW_SUCCESS;
}

int
spheroid_init_from_srid(FunctionCallInfo fcinfo, int srid, SPHEROID *s)
{
	projPJ pj1, pj2;
#if POSTGIS_PROJ_VERSION >= 48
	double major_axis, minor_axis, eccentricity_squared;
#endif

	if ( GetProjectionsUsingFCInfo(fcinfo, srid, srid, &pj1, &pj2) == LW_FAILURE)
		return LW_FAILURE;

	if ( ! pj_is_latlong(pj1) )
		return LW_FAILURE;

#if POSTGIS_PROJ_VERSION >= 48
	/* For newer versions of Proj we can pull the spheroid paramaeters and initialize */
	/* using them */
	pj_get_spheroid_defn(pj1, &major_axis, &eccentricity_squared);
	minor_axis = major_axis * sqrt(1-eccentricity_squared);
	spheroid_init(s, major_axis, minor_axis);
#else
	/* For old versions of Proj we cannot lookup the spheroid parameters from the API */
	/* So we use the WGS84 parameters (boo!) */
	spheroid_init(s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);
#endif

	return LW_SUCCESS;
}

void srid_is_latlong(FunctionCallInfo fcinfo, int srid)
{
	projPJ pj1;
	projPJ pj2;

	if ( srid == SRID_DEFAULT || srid == SRID_UNKNOWN )
		return;

	if ( GetProjectionsUsingFCInfo(fcinfo, srid, srid, &pj1, &pj2) == LW_FAILURE)
		return;

	if ( pj_is_latlong(pj1) )
		return;

	ereport(ERROR, (
	            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	            errmsg("Only lon/lat coordinate systems are supported in geography.")));
}


srs_precision srid_axis_precision(FunctionCallInfo fcinfo, int srid, int precision)
{
	projPJ pj1;
	projPJ pj2;

	srs_precision sp;
	
	sp.precision_xy = precision;
	sp.precision_z = precision;
	sp.precision_m = precision;
	
	if ( srid == SRID_UNKNOWN )
		return sp;

	if ( GetProjectionsUsingFCInfo(fcinfo, srid, srid, &pj1, &pj2) == LW_FAILURE)
		return sp;

	if ( pj_is_latlong(pj1) )
	{
		sp.precision_xy += 5;
		return sp;
	}
	
	return sp;
}
