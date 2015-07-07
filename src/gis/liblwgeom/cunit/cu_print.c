/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 * Copyright 2008 Paul Ramsey
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"

#include "liblwgeom_internal.h"
#include "cu_tester.h"

static void test_lwprint_assert_format(char * point_wkt, const char * format, const char * expected)
{
	LWPOINT * test_point = (LWPOINT*)lwgeom_from_wkt(point_wkt, LW_PARSER_CHECK_NONE);
	int num_old_failures, num_new_failures;
	char * actual;
	cu_error_msg_reset();
	actual = lwpoint_to_latlon(test_point, format);
	if (0 != strlen(cu_error_msg))
	{
		printf("\nAssert failed:\n\tFormat [%s] generated an error: %s\n", format, cu_error_msg);
		CU_FAIL();
	}
	num_old_failures = CU_get_number_of_failures();
	CU_ASSERT_STRING_EQUAL(actual, expected);
	num_new_failures = CU_get_number_of_failures();
	if (num_new_failures > num_old_failures)
	{
		printf("\nAssert failed:\n\t%s\t(actual)\n\t%s\t(expected)\n", actual, expected);
	}
	lwfree(actual);
	lwpoint_free(test_point);
}
static void test_lwprint_assert_error(char * point_wkt, const char * format)
{
	LWPOINT * test_point = (LWPOINT*)lwgeom_from_wkt(point_wkt, LW_PARSER_CHECK_NONE);
	cu_error_msg_reset();
	char* tmp = lwpoint_to_latlon(test_point, format);
	lwfree(tmp);
	if (0 == strlen(cu_error_msg))
	{
		printf("\nAssert failed:\n\tFormat [%s] did not generate an error.\n", format);
		CU_FAIL();
	}
	else
	{
		cu_error_msg_reset();
	}
	lwpoint_free(test_point);
}

/*
** Test points around the globe using the default format.  Null and empty string both mean use the default.
*/
static void test_lwprint_default_format(void)
{
	test_lwprint_assert_format("POINT(0 0)",                NULL, "0\xC2\xB0""0'0.000\"N 0\xC2\xB0""0'0.000\"E");
	test_lwprint_assert_format("POINT(45.4545 12.34567)",   ""  , "12\xC2\xB0""20'44.412\"N 45\xC2\xB0""27'16.200\"E");
	test_lwprint_assert_format("POINT(180 90)",             NULL, "90\xC2\xB0""0'0.000\"N 180\xC2\xB0""0'0.000\"E");
	test_lwprint_assert_format("POINT(181 91)",             ""  , "89\xC2\xB0""0'0.000\"N 1\xC2\xB0""0'0.000\"E");
	test_lwprint_assert_format("POINT(180.0001 90.0001)",   NULL, "89\xC2\xB0""59'59.640\"N 0\xC2\xB0""0'0.360\"E");
	test_lwprint_assert_format("POINT(45.4545 -12.34567)",  ""  , "12\xC2\xB0""20'44.412\"S 45\xC2\xB0""27'16.200\"E");
	test_lwprint_assert_format("POINT(180 -90)",            NULL, "90\xC2\xB0""0'0.000\"S 180\xC2\xB0""0'0.000\"E");
	test_lwprint_assert_format("POINT(181 -91)",            ""  , "89\xC2\xB0""0'0.000\"S 1\xC2\xB0""0'0.000\"E");
	test_lwprint_assert_format("POINT(180.0001 -90.0001)",  NULL, "89\xC2\xB0""59'59.640\"S 0\xC2\xB0""0'0.360\"E");
	test_lwprint_assert_format("POINT(-45.4545 12.34567)",  ""  , "12\xC2\xB0""20'44.412\"N 45\xC2\xB0""27'16.200\"W");
	test_lwprint_assert_format("POINT(-180 90)",            NULL, "90\xC2\xB0""0'0.000\"N 180\xC2\xB0""0'0.000\"W");
	test_lwprint_assert_format("POINT(-181 91)",            ""  , "89\xC2\xB0""0'0.000\"N 1\xC2\xB0""0'0.000\"W");
	test_lwprint_assert_format("POINT(-180.0001 90.0001)",  NULL, "89\xC2\xB0""59'59.640\"N 0\xC2\xB0""0'0.360\"W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", ""  , "12\xC2\xB0""20'44.412\"S 45\xC2\xB0""27'16.200\"W");
	test_lwprint_assert_format("POINT(-180 -90)",           NULL, "90\xC2\xB0""0'0.000\"S 180\xC2\xB0""0'0.000\"W");
	test_lwprint_assert_format("POINT(-181 -91)",           ""  , "89\xC2\xB0""0'0.000\"S 1\xC2\xB0""0'0.000\"W");
	test_lwprint_assert_format("POINT(-180.0001 -90.0001)", NULL, "89\xC2\xB0""59'59.640\"S 0\xC2\xB0""0'0.360\"W");
	test_lwprint_assert_format("POINT(-2348982391.123456 -238749827.34879)", ""  , "12\xC2\xB0""39'4.356\"N 31\xC2\xB0""7'24.442\"W");
}

/*
 * Test all possible combinations of the orders of the parameters.
 */
static void test_lwprint_format_orders(void)
{
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C DD MM SS", "S 12 20 44 W 45 27 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C DD SS MM", "S 12 44 20 W 45 16 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C MM DD SS", "S 20 12 44 W 27 45 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C MM SS DD", "S 20 44 12 W 27 16 45");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C SS DD MM", "S 44 12 20 W 16 45 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "C SS MM DD", "S 44 20 12 W 16 27 45");

	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD C MM SS", "12 S 20 44 45 W 27 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD C SS MM", "12 S 44 20 45 W 16 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM C DD SS", "20 S 12 44 27 W 45 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM C SS DD", "20 S 44 12 27 W 16 45");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS C DD MM", "44 S 12 20 16 W 45 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS C MM DD", "44 S 20 12 16 W 27 45");

	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD MM C SS", "12 20 S 44 45 27 W 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD SS C MM", "12 44 S 20 45 16 W 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM DD C SS", "20 12 S 44 27 45 W 16");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM SS C DD", "20 44 S 12 27 16 W 45");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS DD C MM", "44 12 S 20 16 45 W 27");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS MM C DD", "44 20 S 12 16 27 W 45");

	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD MM SS C", "12 20 44 S 45 27 16 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD SS MM C", "12 44 20 S 45 16 27 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM DD SS C", "20 12 44 S 27 45 16 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "MM SS DD C", "20 44 12 S 27 16 45 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS DD MM C", "44 12 20 S 16 45 27 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "SS MM DD C", "44 20 12 S 16 27 45 W");
}

/*
 * Test with and without the optional parameters.
 */
static void test_lwprint_optional_format(void)
{
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD", "-12.346 -45.455");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD C", "12.346 S 45.455 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM", "-12.000 20.740 -45.000 27.270");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM C", "12.000 20.740 S 45.000 27.270 W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM SS.SSS", "-12.000 20.000 44.412 -45.000 27.000 16.200");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDD MM.MMM SS.SSS C", "12.000 20.000 44.412 S 45.000 27.000 16.200 W");
}

static void test_lwprint_oddball_formats(void)
{
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.DDDMM.MMMSS.SSSC", "12.00020.00044.412S 45.00027.00016.200W");
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DDMM.MMM", "-1220.740 -4527.270");
	/* "##." will be printed as "##" */
	test_lwprint_assert_format("POINT(-45.4545 -12.34567)", "DD.MM.MMM", "-1220.740 -4527.270");
}

/*
 * Test using formats that should produce errors.
 */
static void test_lwprint_bad_formats(void)
{
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD.DDD SS.SSS");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "MM.MMM SS.SSS");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD.DDD SS.SSS DD");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD MM SS MM");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD MM SS SS");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C DD.DDD C");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C \xC2""DD.DDD");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C DD.DDD \xC2");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C DD\x80""MM ");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C DD \xFF""MM");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "C DD \xB0""MM");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD.DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
	test_lwprint_assert_error("POINT(1.23456 7.89012)", "DD.DDD jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj");
}

/*
** Callback used by the test harness to register the tests in this file.
*/
void print_suite_setup(void);
void print_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("printing", NULL, NULL);
	PG_ADD_TEST(suite, test_lwprint_default_format);
	PG_ADD_TEST(suite, test_lwprint_format_orders);
	PG_ADD_TEST(suite, test_lwprint_optional_format);
	PG_ADD_TEST(suite, test_lwprint_oddball_formats);
	PG_ADD_TEST(suite, test_lwprint_bad_formats);
}

