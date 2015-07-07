/**********************************************************************
 * $Id$
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright 2012 Sandro Santilli <strk@keybit.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"

#include "bytebuffer.h"
#include "cu_tester.h"

#if 0

static void test_bytebuffer_append(void)
{
	bytebuffer_t *bb1;
	int64_t res;
	bb1 = bytebuffer_create_with_size(2);
	
	bytebuffer_append_varint(bb1,(int64_t) -12345);

	
	bytebuffer_reset_reading(bb1);
	
	res= bytebuffer_read_varint(bb1);

	CU_ASSERT_EQUAL(res, -12345);

	bytebuffer_destroy(bb1);
}


/* TODO: add more... */



/*
** Used by the test harness to register the tests in this file.
*/
void bytebuffer_suite_setup(void);
void bytebuffer_suite_setup(void)
{
	PG_TEST(test_bytebuffer_append),
	CU_TEST_INFO_NULL
};
CU_SuiteInfo bytebuffer_suite = {"bytebuffer", NULL, NULL, bytebuffer_tests };

#endif
