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

/***************************************************************
 * Some rules for returning NOTICE or ERROR...
 *
 * Send an ERROR like:
 *
 * elog(ERROR, "RASTER_out: Could not deserialize raster");
 *
 * only when:
 *
 * -something wrong happen with memory,
 * -a function got an invalid argument ('3BUI' as pixel type) so that no row can
 * be processed
 *
 * *** IMPORTANT: elog(ERROR, ...) does NOT return to calling function ***
 *
 * Send a NOTICE like:
 *
 * elog(NOTICE, "Invalid band index (must use 1-based). Returning NULL");
 *
 * when arguments (e.g. x, y, band) are NULL or out of range so that some or
 * most rows can be processed anyway
 *
 * in this case,
 * for SET functions or function normally returning a modified raster, return
 * the original raster
 * for GET functions, return NULL
 * try to deduce a valid parameter value if it makes sence (e.g. out of range
 * index for addBand)
 *
 * Do not put the name of the faulty function for NOTICEs, only with ERRORs.
 *
 ****************************************************************/

/******************************************************************************
 * Some notes on memory management...
 *
 * Every time a SQL function is called, PostgreSQL creates a  new memory context.
 * So, all the memory allocated with palloc/repalloc in that context is
 * automatically free'd at the end of the function. If you want some data to
 * live between function calls, you have 2 options:
 *
 * - Use fcinfo->flinfo->fn_mcxt contex to store the data (by pointing the
 *   data you want to keep with fcinfo->flinfo->fn_extra)
 * - Use SRF funcapi, and storing the data at multi_call_memory_ctx (by pointing
 *   the data you want to keep with funcctx->user_fctx. funcctx is created by
 *   funcctx = SPI_FIRSTCALL_INIT()). Recommended way in functions returning rows,
 *   like RASTER_dumpAsPolygons (see section 34.9.9 at
 *   http://www.postgresql.org/docs/8.4/static/xfunc-c.html).
 *
 * But raster code follows the same philosophy than the rest of PostGIS: keep
 * memory as clean as possible. So, we free all allocated memory.
 *
 * TODO: In case of functions returning NULL, we should free the memory too.
 *****************************************************************************/

/******************************************************************************
 * Notes for use of PG_DETOAST_DATUM(), PG_DETOAST_DATUM_SLICE()
 *   and PG_DETOAST_DATUM_COPY()
 *
 * When ONLY getting raster (not band) metadata, use PG_DETOAST_DATUM_SLICE()
 *   as it is generally quicker to get only the chunk of memory that contains
 *   the raster metadata.
 *
 *   Example: PG_DETOAST_DATUM_SLICE(PG_GETARG_DATUM(0), 0,
 *     sizeof(struct rt_raster_serialized_t))
 *
 * When ONLY setting raster or band(s) metadata OR reading band data, use
 *   PG_DETOAST_DATUM() as rt_raster_deserialize() allocates local memory
 *   for the raster and band(s) metadata.
 *
 *   Example: PG_DETOAST_DATUM(PG_GETARG_DATUM(0))
 *
 * When SETTING band pixel values, use PG_DETOAST_DATUM_COPY().  This is
 *   because band data (not metadata) is just a pointer to the correct
 *   memory location in the detoasted datum.  What is returned from
 *   PG_DETOAST_DATUM() may or may not be a copy of the input datum.
 *
 *   From the comments in postgresql/src/include/fmgr.h...
 *
 *     pg_detoast_datum() gives you either the input datum (if not toasted)
 *     or a detoasted copy allocated with palloc().
 *
 *   From the mouth of Tom Lane...
 *   http://archives.postgresql.org/pgsql-hackers/2002-01/msg01289.php
 *
 *     PG_DETOAST_DATUM_COPY guarantees to give you a copy, even if the
 *     original wasn't toasted.  This allows you to scribble on the input,
 *     in case that happens to be a useful way of forming your result.
 *     Without a forced copy, a routine for a pass-by-ref datatype must
 *     NEVER, EVER scribble on its input ... because very possibly it'd
 *     be scribbling on a valid tuple in a disk buffer, or a valid entry
 *     in the syscache.
 *
 *   The key detail above is that the raster datatype is a varlena, a
 *     passed by reference datatype.
 *
 *   Example: PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0))
 *
 * If in doubt, use PG_DETOAST_DATUM_COPY() as that guarantees that the input
 *   datum is copied for use.
 *****************************************************************************/

#include <postgres.h> /* for palloc */
#include <fmgr.h> /* for PG_MODULE_MAGIC */
#include "utils/guc.h"

#include "../../postgis_config.h"
#include "lwgeom_pg.h"

#include "rtpostgis.h"
#include "rtpg_internal.h"

/*
 * This is required for builds against pgsql
 */
PG_MODULE_MAGIC;

/* Module load callback */
void _PG_init(void);

#define RT_MSG_MAXLEN 256

/* ---------------------------------------------------------------- */
/*  PostGIS raster GUCs                                             */
/* ---------------------------------------------------------------- */

static char *gdal_datapath = NULL;
extern char *gdal_enabled_drivers;
extern char enable_outdb_rasters;

/* postgis.gdal_datapath */
static void
rtpg_assignHookGDALDataPath(const char *newpath, void *extra) {
	POSTGIS_RT_DEBUGF(4, "newpath = %s", newpath);
	POSTGIS_RT_DEBUGF(4, "gdaldatapath = %s", gdal_datapath);

	/* clear finder cache */
	CPLFinderClean();

	/* clear cached OSR */
	OSRCleanup();

	/* set GDAL_DATA */
	CPLSetConfigOption("GDAL_DATA", newpath);
	POSTGIS_RT_DEBUGF(4, "GDAL_DATA = %s", CPLGetConfigOption("GDAL_DATA", ""));
}

/* postgis.gdal_enabled_drivers */
static void
rtpg_assignHookGDALEnabledDrivers(const char *enabled_drivers, void *extra) {
	int enable_all = 0;
	int disable_all = 0;

	char **enabled_drivers_array = NULL;
	int enabled_drivers_count = 0;
	bool *enabled_drivers_found = NULL;
	char *gdal_skip = NULL;

	uint32_t i;
	uint32_t j;

	POSTGIS_RT_DEBUGF(4, "GDAL_SKIP = %s", CPLGetConfigOption("GDAL_SKIP", ""));
	POSTGIS_RT_DEBUGF(4, "enabled_drivers = %s", enabled_drivers);

	/* if NULL, nothing to do */
	if (enabled_drivers == NULL)
		return;

	/* destroy the driver manager */
	/* this is the only way to ensure GDAL_SKIP is recognized */
	GDALDestroyDriverManager();
	CPLSetConfigOption("GDAL_SKIP", NULL);

	/* force wrapper function to call GDALAllRegister() */
	rt_util_gdal_register_all(1);

	enabled_drivers_array = rtpg_strsplit(enabled_drivers, " ", &enabled_drivers_count);
	enabled_drivers_found = palloc(sizeof(bool) * enabled_drivers_count);
	memset(enabled_drivers_found, FALSE, sizeof(bool) * enabled_drivers_count);

	/* scan for keywords DISABLE_ALL and ENABLE_ALL */
	disable_all = 0;
	enable_all = 0;
	if (strstr(enabled_drivers, GDAL_DISABLE_ALL) != NULL) {
		for (i = 0; i < enabled_drivers_count; i++) {
			if (strstr(enabled_drivers_array[i], GDAL_DISABLE_ALL) != NULL) {
				enabled_drivers_found[i] = TRUE;
				disable_all = 1;
			}
		}
	}
	else if (strstr(enabled_drivers, GDAL_ENABLE_ALL) != NULL) {
		for (i = 0; i < enabled_drivers_count; i++) {
			if (strstr(enabled_drivers_array[i], GDAL_ENABLE_ALL) != NULL) {
				enabled_drivers_found[i] = TRUE;
				enable_all = 1;
			}
		}
	}

	if (!enable_all) {
		int found = 0;
		uint32_t drv_count = 0;
		rt_gdaldriver drv_set = rt_raster_gdal_drivers(&drv_count, 0);

		POSTGIS_RT_DEBUGF(4, "driver count = %d", drv_count);

		/* all other drivers than those in new drivers are added to GDAL_SKIP */
		for (i = 0; i < drv_count; i++) {
			found = 0;

			if (!disable_all) {
				/* gdal driver found in enabled_drivers, continue to thorough search */
				if (strstr(enabled_drivers, drv_set[i].short_name) != NULL) {
					/* thorough search of enabled_drivers */
					for (j = 0; j < enabled_drivers_count; j++) {
						/* driver found */
						if (strcmp(enabled_drivers_array[j], drv_set[i].short_name) == 0) {
							enabled_drivers_found[j] = TRUE;
							found = 1;
						}
					}
				}
			}

			/* driver found, continue */
			if (found)
				continue;

			/* driver not found, add to gdal_skip */
			if (gdal_skip == NULL) {
				gdal_skip = palloc(sizeof(char) * (strlen(drv_set[i].short_name) + 1));
				gdal_skip[0] = '\0';
			}
			else {
				gdal_skip = repalloc(
					gdal_skip,
					sizeof(char) * (
						strlen(gdal_skip) + 1 + strlen(drv_set[i].short_name) + 1
					)
				);
				strcat(gdal_skip, " "); 
			}
			strcat(gdal_skip, drv_set[i].short_name);
		}

		for (i = 0; i < drv_count; i++) {
			pfree(drv_set[i].short_name);
			pfree(drv_set[i].long_name);
			pfree(drv_set[i].create_options);
		}
		if (drv_count) pfree(drv_set);

	}

	for (i = 0; i < enabled_drivers_count; i++) {
		if (enabled_drivers_found[i])
			continue;

		if (disable_all)
			elog(WARNING, "%s set. Ignoring GDAL driver: %s", GDAL_DISABLE_ALL, enabled_drivers_array[i]);
		else if (enable_all)
			elog(WARNING, "%s set. Ignoring GDAL driver: %s", GDAL_ENABLE_ALL, enabled_drivers_array[i]);
		else
			elog(WARNING, "Unknown GDAL driver: %s", enabled_drivers_array[i]);
	}

	/* destroy the driver manager */
	/* this is the only way to ensure GDAL_SKIP is recognized */
	GDALDestroyDriverManager();

	/* set GDAL_SKIP */
	POSTGIS_RT_DEBUGF(4, "gdal_skip = %s", gdal_skip);
	CPLSetConfigOption("GDAL_SKIP", gdal_skip);
	if (gdal_skip != NULL) pfree(gdal_skip);

	/* force wrapper function to call GDALAllRegister() */
	rt_util_gdal_register_all(1);

	pfree(enabled_drivers_array);
	pfree(enabled_drivers_found);
	POSTGIS_RT_DEBUGF(4, "GDAL_SKIP = %s", CPLGetConfigOption("GDAL_SKIP", ""));
}

/* postgis.enable_outdb_rasters */
static void
rtpg_assignHookEnableOutDBRasters(bool enable, void *extra) {
	/* do nothing for now */
}

/* Module load callback */
void
_PG_init(void) {

	char *env_postgis_gdal_enabled_drivers = NULL;
	char *boot_postgis_gdal_enabled_drivers = NULL;

	char *env_postgis_enable_outdb_rasters = NULL;
	bool boot_postgis_enable_outdb_rasters = false;

	/*
	 use POSTGIS_GDAL_ENABLED_DRIVERS to set the bootValue
	 of GUC postgis.gdal_enabled_drivers
	*/
	env_postgis_gdal_enabled_drivers = getenv("POSTGIS_GDAL_ENABLED_DRIVERS");
	if (env_postgis_gdal_enabled_drivers == NULL) {
		boot_postgis_gdal_enabled_drivers = palloc(
			sizeof(char) * (strlen(GDAL_DISABLE_ALL) + 1)
		);
		sprintf(boot_postgis_gdal_enabled_drivers, "%s", GDAL_DISABLE_ALL);
	}
	else {
		boot_postgis_gdal_enabled_drivers = rtpg_trim(
			env_postgis_gdal_enabled_drivers
		);
	}
	POSTGIS_RT_DEBUGF(
		4,
		"boot_postgis_gdal_enabled_drivers = %s",
		boot_postgis_gdal_enabled_drivers
	);

	/*
	 use POSTGIS_ENABLE_OUTDB_RASTERS to set the bootValue
	 of GUC postgis.enable_outdb_rasters
	*/
	env_postgis_enable_outdb_rasters = getenv("POSTGIS_ENABLE_OUTDB_RASTERS");
	if (env_postgis_enable_outdb_rasters != NULL) {
		char *env = rtpg_trim(env_postgis_enable_outdb_rasters);

		/* out of memory */
		if (env == NULL) {
			elog(ERROR, "_PG_init: Cannot process environmental variable: POSTGIS_ENABLE_OUTDB_RASTERS");
			return;
		}

		if (strcmp(env, "1") == 0)
			boot_postgis_enable_outdb_rasters = true;

		pfree(env);
	}
	POSTGIS_RT_DEBUGF(
		4,
		"boot_postgis_enable_outdb_rasters = %s",
		boot_postgis_enable_outdb_rasters ? "TRUE" : "FALSE"
	);

	/* Install liblwgeom handlers */
	pg_install_lwgeom_handlers();

	/* TODO: Install raster callbacks (see rt_init_allocators)??? */

	/* Define custom GUC variables. */

	DefineCustomStringVariable(
		"postgis.gdal_datapath", /* name */
		"Path to GDAL data files.", /* short_desc */
		"Physical path to directory containing GDAL data files (sets the GDAL_DATA config option).", /* long_desc */
		&gdal_datapath, /* valueAddr */
		NULL, /* bootValue */
		PGC_SUSET, /* GucContext context */
		0, /* int flags */
#if POSTGIS_PGSQL_VERSION >= 91
		NULL, /* GucStringCheckHook check_hook */
#endif
		rtpg_assignHookGDALDataPath, /* GucStringAssignHook assign_hook */
		NULL  /* GucShowHook show_hook */
	);

	DefineCustomStringVariable(
		"postgis.gdal_enabled_drivers", /* name */
		"Enabled GDAL drivers.", /* short_desc */
		"List of enabled GDAL drivers by short name. To enable/disable all drivers, use 'ENABLE_ALL' or 'DISABLE_ALL' (sets the GDAL_SKIP config option).", /* long_desc */
		&gdal_enabled_drivers, /* valueAddr */
		boot_postgis_gdal_enabled_drivers, /* bootValue */
		PGC_SUSET, /* GucContext context */
		0, /* int flags */
#if POSTGIS_PGSQL_VERSION >= 91
		NULL, /* GucStringCheckHook check_hook */
#endif
		rtpg_assignHookGDALEnabledDrivers, /* GucStringAssignHook assign_hook */
		NULL  /* GucShowHook show_hook */
	);

	DefineCustomBoolVariable(
		"postgis.enable_outdb_rasters", /* name */
		"Enable Out-DB raster bands", /* short_desc */
		"If true, rasters can access data located outside the database", /* long_desc */
		&enable_outdb_rasters, /* valueAddr */
		boot_postgis_enable_outdb_rasters, /* bootValue */
		PGC_SUSET, /* GucContext context */
		0, /* int flags */
#if POSTGIS_PGSQL_VERSION >= 91
		NULL, /* GucStringCheckHook check_hook */
#endif
		rtpg_assignHookEnableOutDBRasters, /* GucBoolAssignHook assign_hook */
		NULL  /* GucShowHook show_hook */
	);

	/* free memory allocations */
	pfree(boot_postgis_gdal_enabled_drivers);
}

/* ---------------------------------------------------------------- */
/*  Memory allocation / error reporting hooks                       */
/*  TODO: reuse the ones in libpgcommon ?                           */
/* ---------------------------------------------------------------- */

static void *
rt_pg_alloc(size_t size)
{
    void * result;

    POSTGIS_RT_DEBUGF(5, "rt_pgalloc(%ld) called", (long int) size);

    result = palloc(size);

    return result;
}

static void *
rt_pg_realloc(void *mem, size_t size)
{
    void * result;

    POSTGIS_RT_DEBUGF(5, "rt_pg_realloc(%ld) called", (long int) size);

    if (mem)
        result = repalloc(mem, size);

    else
        result = palloc(size);

    return result;
}

static void
rt_pg_free(void *ptr)
{
    POSTGIS_RT_DEBUG(5, "rt_pfree called");
    pfree(ptr);
}

static void
rt_pg_error(const char *fmt, va_list ap)
{
    char errmsg[RT_MSG_MAXLEN+1];

    vsnprintf (errmsg, RT_MSG_MAXLEN, fmt, ap);

    errmsg[RT_MSG_MAXLEN]='\0';
    ereport(ERROR, (errmsg_internal("%s", errmsg)));
}

static void
rt_pg_notice(const char *fmt, va_list ap)
{
    char msg[RT_MSG_MAXLEN+1];

    vsnprintf (msg, RT_MSG_MAXLEN, fmt, ap);

    msg[RT_MSG_MAXLEN]='\0';
    ereport(NOTICE, (errmsg_internal("%s", msg)));
}


void
rt_init_allocators(void)
{
    /* raster callback - install raster handlers */
    rt_set_handlers(rt_pg_alloc, rt_pg_realloc, rt_pg_free, rt_pg_error,
            rt_pg_notice, rt_pg_notice);
}
