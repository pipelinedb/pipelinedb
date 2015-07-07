/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2010 LISAsoft Pty Ltd
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include "cu_shp2pgsql.h"
#include "cu_tester.h"
#include "../shp2pgsql-core.h"

/* Test functions */
void test_ShpLoaderCreate(void);
void test_ShpLoaderDestroy(void);

SHPLOADERCONFIG *loader_config;
SHPLOADERSTATE *loader_state;

/*
** Called from test harness to register the tests in this file.
*/
CU_pSuite register_shp2pgsql_suite(void)
{
	CU_pSuite pSuite;
	pSuite = CU_add_suite("Shapefile Loader File shp2pgsql Test", init_shp2pgsql_suite, clean_shp2pgsql_suite);
	if (NULL == pSuite)
	{
		CU_cleanup_registry();
		return NULL;
	}

	if (
	    (NULL == CU_add_test(pSuite, "test_ShpLoaderCreate()", test_ShpLoaderCreate)) ||
	    (NULL == CU_add_test(pSuite, "test_ShpLoaderDestroy()", test_ShpLoaderDestroy))
	)
	{
		CU_cleanup_registry();
		return NULL;
	}
	return pSuite;
}

/*
** The suite initialization function.
** Create any re-used objects.
*/
int init_shp2pgsql_suite(void)
{
	return 0;
}

/*
** The suite cleanup function.
** Frees any global objects.
*/
int clean_shp2pgsql_suite(void)
{
	return 0;
}

void test_ShpLoaderCreate(void)
{	
	loader_config = (SHPLOADERCONFIG*)calloc(1, sizeof(SHPLOADERCONFIG));
	set_loader_config_defaults(loader_config);
	loader_state = ShpLoaderCreate(loader_config);
	CU_ASSERT_PTR_NOT_NULL(loader_state);
	CU_ASSERT_STRING_EQUAL(loader_state->config->encoding, ENCODING_DEFAULT);
}

void test_ShpLoaderDestroy(void)
{
	ShpLoaderDestroy(loader_state);
}
