/*
 * PostGIS Raster - Raster Types for PostGIS
 * http://trac.osgeo.org/postgis/wiki/WKTRaster
 *
 * Copyright (C) 2012 Regents of the University of California
 *   <bkpark@ucdavis.edu>
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

#include "CUnit/Basic.h"
#include "cu_tester.h"

static void test_pixtype_size() {
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_1BB), 1);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_2BUI), 1);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_4BUI), 1);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_8BUI), 1);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_8BSI), 1);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_16BUI), 2);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_16BSI), 2);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_32BUI), 4);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_32BSI), 4);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_32BF), 4);
	CU_ASSERT_EQUAL(rt_pixtype_size(PT_64BF), 8);

	CU_ASSERT_EQUAL(rt_pixtype_size(PT_END), -1);
}

static void test_pixtype_alignment() {
	/* rt_pixtype_alignment() just forwards to rt_pixtype_size() */
}

static void test_pixtype_name() {
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_1BB), "1BB");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_2BUI), "2BUI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_4BUI), "4BUI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_8BUI), "8BUI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_8BSI), "8BSI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_16BUI), "16BUI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_16BSI), "16BSI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_32BUI), "32BUI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_32BSI), "32BSI");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_32BF), "32BF");
	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_64BF), "64BF");

	CU_ASSERT_STRING_EQUAL(rt_pixtype_name(PT_END), "Unknown");
}

static void test_pixtype_index_from_name() {
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("1BB"), PT_1BB);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("2BUI"), PT_2BUI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("4BUI"), PT_4BUI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("8BUI"), PT_8BUI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("8BSI"), PT_8BSI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("16BUI"), PT_16BUI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("16BSI"), PT_16BSI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("32BUI"), PT_32BUI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("32BSI"), PT_32BSI);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("32BF"), PT_32BF);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("64BF"), PT_64BF);

	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("END"), PT_END);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("1bb"), PT_END);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("1bB"), PT_END);
	CU_ASSERT_EQUAL(rt_pixtype_index_from_name("3BUI"), PT_END);
}

static void test_pixtype_get_min_value() {
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_1BB), rt_util_clamp_to_1BB((double) CHAR_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_2BUI), rt_util_clamp_to_2BUI((double) CHAR_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_4BUI), rt_util_clamp_to_4BUI((double) CHAR_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_8BUI), rt_util_clamp_to_8BUI((double) CHAR_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_8BSI), rt_util_clamp_to_8BSI((double) SCHAR_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_16BUI), rt_util_clamp_to_16BUI((double) SHRT_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_16BSI), rt_util_clamp_to_16BSI((double) SHRT_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_32BUI), rt_util_clamp_to_32BUI((double) INT_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_32BSI), rt_util_clamp_to_32BSI((double) INT_MIN), DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_32BF), -FLT_MAX, DBL_EPSILON);
	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_64BF), -DBL_MAX, DBL_EPSILON);

	CU_ASSERT_DOUBLE_EQUAL(rt_pixtype_get_min_value(PT_END), rt_util_clamp_to_8BUI((double) CHAR_MIN), DBL_EPSILON);
}

static void test_pixtype_compare_clamped_values() {
	int isequal = 0;

	/* 1BB */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 0, 0, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 0, 1, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 1, 0, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 1, 1, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 0, 2, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_1BB, 0, -9999, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 2BUI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 0, 0, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 0, 1, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 0, 3, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 1, 1, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 3, 2, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, 4, 0, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_2BUI, -1, 0, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 4BUI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 10, 10, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 10, 1, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 0, 15, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 15, 15, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 0, 16, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_4BUI, 16, 15, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 8BUI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, 155, 155, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, 155, 255, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, 0, 155, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, -1, -1, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, 0, -1, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BUI, 256, 255, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 8BSI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, 120, 120, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, -120, 120, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, -10, -10, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, -128, -128, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, -128, 128, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, -129, -128, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_8BSI, 129, 128, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 16BUI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, 65535, 65535, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, 0, 0, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, 12345, 12344, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, 0, 65535, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, 65536, -1, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BUI, -9999, 0, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 16BSI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, -32000, -32000, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, -32767, -32767, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, 32767, 32768, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, 32766, 32768, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, 0, -32768, &isequal), ES_NONE);
	CU_ASSERT(!isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_16BSI, 32767, -32767, &isequal), ES_NONE);
	CU_ASSERT(!isequal);

	/* 32BUI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BUI, 4294967295UL, 4294967295UL, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BUI, 4294967296ULL, 4294967295UL, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 32BSI */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BSI, 2147483647, 2147483647, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BSI, 2147483648UL, 2147483647, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 32BF */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BF, 65535.5, 65535.5, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_32BF, 0.0060000000521540, 0.0060000000521540, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	/* 64BF */
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_64BF, 65535.5, 65535.5, &isequal), ES_NONE);
	CU_ASSERT(isequal);
	CU_ASSERT_EQUAL(rt_pixtype_compare_clamped_values(PT_64BF, 0.0060000000521540, 0.0060000000521540, &isequal), ES_NONE);
	CU_ASSERT(isequal);

	CU_ASSERT_NOT_EQUAL(rt_pixtype_compare_clamped_values(PT_END, 1, 1, &isequal), ES_NONE);
}

/* register tests */
void pixtype_suite_setup(void);
void pixtype_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("pixtype", NULL, NULL);
	PG_ADD_TEST(suite, test_pixtype_size);
	PG_ADD_TEST(suite, test_pixtype_alignment);
	PG_ADD_TEST(suite, test_pixtype_name);
	PG_ADD_TEST(suite, test_pixtype_index_from_name);
	PG_ADD_TEST(suite, test_pixtype_get_min_value);
	PG_ADD_TEST(suite, test_pixtype_compare_clamped_values);
}

