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
#include "liblwgeom_internal.h"
#include "cu_tester.h"
#include "../postgis_config.h"

/* Internal funcs */
static void
cu_errorreporter(const char *fmt, va_list ap);


/* ADD YOUR SUITE SETUP FUNCTION HERE (1 of 2) */
extern void print_suite_setup();
extern void algorithms_suite_setup();
extern void buildarea_suite_setup();
extern void clean_suite_setup();
extern void clip_by_rect_suite_setup();
extern void force_sfs_suite_setup(void);
extern void geodetic_suite_setup(void);
extern void geos_suite_setup(void);
extern void homogenize_suite_setup(void);
extern void in_encoded_polyline_suite_setup(void);
extern void in_geojson_suite_setup(void);
extern void twkb_in_suite_setup(void);
extern void libgeom_suite_setup(void);
extern void measures_suite_setup(void);
extern void effectivearea_suite_setup(void);
extern void misc_suite_setup(void);
extern void node_suite_setup(void);
extern void out_encoded_polyline_suite_setup(void);
extern void out_geojson_suite_setup(void);
extern void out_gml_suite_setup(void);
extern void out_kml_suite_setup(void);
extern void out_svg_suite_setup(void);
extern void twkb_out_suite_setup(void);
extern void out_x3d_suite_setup(void);
extern void ptarray_suite_setup(void);
extern void sfcgal_suite_setup(void);
extern void split_suite_setup(void);
extern void stringbuffer_suite_setup(void);
extern void tree_suite_setup(void);
extern void triangulate_suite_setup(void);
extern void varint_suite_setup(void);
extern void wkt_out_suite_setup(void);
extern void wkb_out_suite_setup(void);
extern void surface_suite_setup(void);
extern void wkb_in_suite_setup(void);
extern void wkt_in_suite_setup(void);


/* AND ADD YOUR SUITE SETUP FUNCTION HERE (2 of 2) */
PG_SuiteSetup setupfuncs[] =
{
	algorithms_suite_setup,
	buildarea_suite_setup,
	clean_suite_setup,
	clip_by_rect_suite_setup,
	force_sfs_suite_setup,
	geodetic_suite_setup,
	geos_suite_setup,
	homogenize_suite_setup,
	in_encoded_polyline_suite_setup,
#if HAVE_LIBJSON
	in_geojson_suite_setup,
#endif
	twkb_in_suite_setup,
	libgeom_suite_setup,
	measures_suite_setup,
	effectivearea_suite_setup,
	misc_suite_setup,
	node_suite_setup,
	out_encoded_polyline_suite_setup,
	out_geojson_suite_setup,
	out_gml_suite_setup,
	out_kml_suite_setup,
	out_svg_suite_setup,
	out_x3d_suite_setup,
	ptarray_suite_setup,
	print_suite_setup,
#if HAVE_SFCGAL
	sfcgal_suite_setup,
#endif
	split_suite_setup,
	stringbuffer_suite_setup,
	surface_suite_setup,
	tree_suite_setup,
	triangulate_suite_setup,
	twkb_out_suite_setup,
	varint_suite_setup,
	wkb_in_suite_setup,
	wkb_out_suite_setup,
	wkt_in_suite_setup,
	wkt_out_suite_setup,
	NULL
};


#define MAX_CUNIT_MSG_LENGTH 256

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

	/* Install the custom error handler */
	lwgeom_set_handlers(0, 0, 0, cu_errorreporter, 0);

	/* Initialize the CUnit test registry */
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
 * CAUTION: Not stop execution on lwerror case !!!
 */
static void
cu_errorreporter(const char *fmt, va_list ap)
{
  vsnprintf (cu_error_msg, MAX_CUNIT_MSG_LENGTH, fmt, ap);
  cu_error_msg[MAX_CUNIT_MSG_LENGTH]='\0';
}

void
cu_error_msg_reset()
{
	memset(cu_error_msg, '\0', MAX_CUNIT_ERROR_LENGTH);
}
