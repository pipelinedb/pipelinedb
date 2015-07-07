/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2008 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <string.h>
#include "CUnit/Basic.h"
#include "cu_tester.h"

/* Internal funcs */
static void 
cu_error_reporter(const char *fmt, va_list ap);

/* ADD YOUR SUITE SETUP FUNCTION HERE (1 of 2) */
extern void pixtype_suite_setup(void);
extern void raster_basics_suite_setup(void);
extern void band_basics_suite_setup(void);
extern void raster_wkb_suite_setup(void);
extern void gdal_suite_setup(void);
extern void raster_geometry_suite_setup(void);
extern void raster_misc_suite_setup(void);
extern void band_stats_suite_setup(void);
extern void band_misc_suite_setup(void);
extern void mapalgebra_suite_setup(void);
extern void spatial_relationship_suite_setup(void);
extern void misc_suite_setup(void);

/* AND ADD YOUR SUITE SETUP FUNCTION HERE (2 of 2) */
PG_SuiteSetup setupfuncs[] =
{
	pixtype_suite_setup,
	raster_basics_suite_setup,
	band_basics_suite_setup,
	raster_wkb_suite_setup,
	gdal_suite_setup,
	raster_geometry_suite_setup,
	raster_misc_suite_setup,
	band_stats_suite_setup,
	band_misc_suite_setup,
	mapalgebra_suite_setup,
	spatial_relationship_suite_setup,
	misc_suite_setup,
	NULL
};


/*
** The main() function for setting up and running the tests.
** Returns a CUE_SUCCESS on successful running, another
** CUnit error code on failure.
*/
int main(int argc, char *argv[])
{
	int index;
	char *suite_name;
	CU_pSuite suite_to_run;
	char *test_name;
	CU_pTest test_to_run;
	CU_ErrorCode errCode = 0;
	CU_pTestRegistry registry;
	int num_run;
	int num_failed;
	PG_SuiteSetup *setupfunc = setupfuncs;

	/* install the custom error handler */
	lwgeom_set_handlers(0, 0, 0, cu_error_reporter, 0);

	/* initialize the CUnit test registry */
	if (CUE_SUCCESS != CU_initialize_registry())
	{
		errCode = CU_get_error();
		printf("    Error attempting to initialize registry: %d.  See CUError.h for error code list.\n", errCode);
		return errCode;
	}

	/* Register all the test suites. */
	while ( *setupfunc )
	{
		(*setupfunc)();
		setupfunc++;
	}

	/* Run all tests using the CUnit Basic interface */
	CU_basic_set_mode(CU_BRM_VERBOSE);
	if (argc <= 1)
	{
		errCode = CU_basic_run_tests();
	}
	else
	{
		/* NOTE: The cunit functions used here (CU_get_registry, CU_get_suite_by_name, and CU_get_test_by_name) are
		 *       listed with the following warning: "Internal CUnit system functions.  Should not be routinely called by users."
		 *       However, there didn't seem to be any other way to get tests by name, so we're calling them. */
		registry = CU_get_registry();
		for (index = 1; index < argc; index++)
		{
			suite_name = argv[index];
			test_name = NULL;
			suite_to_run = CU_get_suite_by_name(suite_name, registry);
			if (NULL == suite_to_run)
			{
				/* See if it's a test name instead of a suite name. */
				suite_to_run = registry->pSuite;
				while (suite_to_run != NULL)
				{
					test_to_run = CU_get_test_by_name(suite_name, suite_to_run);
					if (test_to_run != NULL)
					{
						/* It was a test name. */
						test_name = suite_name;
						suite_name = suite_to_run->pName;
						break;
					}
					suite_to_run = suite_to_run->pNext;
				}
			}
			if (suite_to_run == NULL)
			{
				printf("\n'%s' does not appear to be either a suite name or a test name.\n\n", suite_name);
			}
			else
			{
				if (test_name != NULL)
				{
					/* Run only this test. */
					printf("\nRunning test '%s' in suite '%s'.\n", test_name, suite_name);
					/* This should be CU_basic_run_test, but that method is broken, see:
					 *     https://sourceforge.net/tracker/?func=detail&aid=2851925&group_id=32992&atid=407088
					 * This one doesn't output anything for success, so we have to do it manually. */
					errCode = CU_run_test(suite_to_run, test_to_run);
					if (errCode != CUE_SUCCESS)
					{
						printf("    Error attempting to run tests: %d.  See CUError.h for error code list.\n", errCode);
					}
					else
					{
						num_run = CU_get_number_of_asserts();
						num_failed = CU_get_number_of_failures();
						printf("\n    %s - asserts - %3d passed, %3d failed, %3d total.\n\n",
						       (0 == num_failed ? "PASSED" : "FAILED"), (num_run - num_failed), num_failed, num_run);
					}
				}
				else
				{
					/* Run all the tests in the suite. */
					printf("\nRunning all tests in suite '%s'.\n", suite_name);
					/* This should be CU_basic_run_suite, but that method is broken, see:
					 *     https://sourceforge.net/tracker/?func=detail&aid=2851925&group_id=32992&atid=407088
					 * This one doesn't output anything for success, so we have to do it manually. */
					errCode = CU_run_suite(suite_to_run);
					if (errCode != CUE_SUCCESS)
					{
						printf("    Error attempting to run tests: %d.  See CUError.h for error code list.\n", errCode);
					}
					else
					{
						num_run = CU_get_number_of_tests_run();
						num_failed = CU_get_number_of_tests_failed();
						printf("\n    %s -   tests - %3d passed, %3d failed, %3d total.\n",
						       (0 == num_failed ? "PASSED" : "FAILED"), (num_run - num_failed), num_failed, num_run);
						num_run = CU_get_number_of_asserts();
						num_failed = CU_get_number_of_failures();
						printf("           - asserts - %3d passed, %3d failed, %3d total.\n\n",
						       (num_run - num_failed), num_failed, num_run);
					}
				}
			}
		}
		/* Presumably if the CU_basic_run_[test|suite] functions worked, we wouldn't have to do this. */
		CU_basic_show_failures(CU_get_failure_list());
		printf("\n\n"); /* basic_show_failures leaves off line breaks. */
	}
	num_failed = CU_get_number_of_failures();
	CU_cleanup_registry();
	return num_failed;
}

/**
 * CUnit error handler
 * Log message in a global var instead of printing in stderr
 *
 * CAUTION: Not stop execution on rterror case !!!
 */
static void cu_error_reporter(const char *fmt, va_list ap) {
  vsnprintf (cu_error_msg, MAX_CUNIT_MSG_LENGTH, fmt, ap);
  cu_error_msg[MAX_CUNIT_MSG_LENGTH]='\0';
}

void cu_error_msg_reset() {
	memset(cu_error_msg, '\0', MAX_CUNIT_MSG_LENGTH);
}

void cu_free_raster(rt_raster raster) {
	uint16_t i;
	uint16_t nbands = rt_raster_get_num_bands(raster);

	for (i = 0; i < nbands; ++i) {
		rt_band band = rt_raster_get_band(raster, i);
		rt_band_destroy(band);
	}

	rt_raster_destroy(raster);
	raster = NULL;
}

rt_band cu_add_band(rt_raster raster, rt_pixtype pixtype, int hasnodata, double nodataval) {
	void* mem = NULL;
	int32_t bandNum = 0;
	size_t datasize = 0;
	rt_band band = NULL;
	uint16_t width = 0;
	uint16_t height = 0;

	width = rt_raster_get_width(raster);
	height = rt_raster_get_height(raster);

	datasize = rt_pixtype_size(pixtype) * width * height;
	mem = rtalloc(datasize);
	CU_ASSERT(mem != NULL);

	if (hasnodata)
		memset(mem, nodataval, datasize);
	else
		memset(mem, 0, datasize);

	band = rt_band_new_inline(width, height, pixtype, hasnodata, nodataval, mem);
	CU_ASSERT(band != NULL);
	rt_band_set_ownsdata_flag(band, 1);

	bandNum = rt_raster_add_band(raster, band, rt_raster_get_num_bands(raster));
	CU_ASSERT(bandNum >= 0);

	return band;
}

void rt_init_allocators(void) {
	rt_set_handlers(
		default_rt_allocator,
		default_rt_reallocator,
		default_rt_deallocator,
		cu_error_reporter,
		default_rt_info_handler,
		default_rt_warning_handler
	);
}
