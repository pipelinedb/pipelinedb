/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2013 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "CUnit/Basic.h"
#include "CUnit/CUnit.h"
#include "liblwgeom_internal.h" 
#include "varint.h"
#include "cu_tester.h"



// size_t varint_u32_encode_buf(uint32_t val, uint8_t *buf);
// size_t varint_s32_encode_buf(int32_t val, uint8_t *buf);
// size_t varint_u64_encode_buf(uint64_t val, uint8_t *buf);
// size_t varint_s64_encode_buf(int64_t val, uint8_t *buf);
// int64_t varint_s64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size);
// uint64_t varint_u64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size);
//
// size_t varint_size(const uint8_t *the_start, const uint8_t *the_end);
//

static void do_test_u32_varint(uint32_t nr, int expected_size, char* expected_res)
{
	int size;
	char *hex;
	uint8_t buf[16];
	
	size = varint_u32_encode_buf(nr, buf);
	if ( size != expected_size ) 
		printf("Expected: %d\nObtained: %d\n", expected_size, size);

	CU_ASSERT_EQUAL(size, expected_size);
	
	hex = hexbytes_from_bytes(buf, size);
	if ( strcmp(hex, expected_res) ) 
		printf("Expected: %s\nObtained: %s\n", expected_res, hex);

	CU_ASSERT_STRING_EQUAL(hex, expected_res);	
	lwfree(hex);
}

static void do_test_s32_varint(int32_t nr,int expected_size, char* expected_res)
{
	uint8_t buf[16];
	int size;
	char *hex;
	
	size = varint_s32_encode_buf(nr, buf);
	if ( size != expected_size ) 
	{
		printf("Expected: %d\nObtained: %d\n", expected_size, size);
	}
	CU_ASSERT_EQUAL(size,expected_size);

	hex = hexbytes_from_bytes(buf, size);
	if ( strcmp(hex,expected_res) ) 
	{
		printf("Expected: %s\nObtained: %s\n", expected_res, hex);
	}
	CU_ASSERT_STRING_EQUAL(hex, expected_res);	
	lwfree(hex);
}

static void do_test_u64_varint(uint64_t nr,int expected_size, char* expected_res)
{
	uint8_t buf[16];
	int size;
	char *hex;
	
	size = varint_u64_encode_buf(nr, buf);
	if ( size != expected_size ) 
	{
		printf("Expected: %d\nObtained: %d\n", expected_size, size);
	}
	CU_ASSERT_EQUAL(size,expected_size);

	hex = hexbytes_from_bytes(buf,size);
	if ( strcmp(hex,expected_res) ) 
	{
		printf("Expected: %s\nObtained: %s\n", expected_res, hex);
	}
	CU_ASSERT_STRING_EQUAL(hex, expected_res);
	lwfree(hex);
}

static void do_test_s64_varint(int64_t nr,int expected_size, char* expected_res)
{
	uint8_t buf[16];
	int size;
	char *hex;
	
	size = varint_s64_encode_buf(nr, buf);
	if ( size != expected_size ) 
	{
		printf("Expected: %d\nObtained: %d\n", expected_size, size);
	}
	CU_ASSERT_EQUAL(size,expected_size);
	
	hex = hexbytes_from_bytes(buf,size);
	if ( strcmp(hex,expected_res) ) 
	{
		printf("Expected: %s\nObtained: %s\n", expected_res, hex);
	}
	CU_ASSERT_STRING_EQUAL(hex, expected_res);	
	lwfree(hex);
}

static void test_varint(void)
{

	do_test_u64_varint(1, 1, "01");
	do_test_u64_varint(300, 2, "AC02");
	do_test_u64_varint(150, 2, "9601");
	do_test_u64_varint(240, 2, "F001"); 
	do_test_u64_varint(0x4000, 3, "808001");
  /*
                0100:0000 0000:0000 - input (0x4000)
      1000:0000 1000:0000 0000:0001 - output (0x808001)
       000:0000  000:0000  000:0001 - chop
       000:0001  000:0000  000:0000 - swap
         0:0000 0100:0000 0000:0000 - concat = input
   */
	do_test_u64_varint(2147483647, 5, "FFFFFFFF07");
  /*
              0111:1111 1111:1111 1111:1111 1111:1111 - input (0x7FFFFFFF)
    1111:1111 1111:1111 1111:1111 1111:1111 0000:0111 - output(0xFFFFFFFF07)
     111:1111  111:1111  111:1111  111:1111  000:0111 - chop
     000:0111  111:1111  111:1111  111:1111  111:1111 - swap
              0111:1111 1111:1111 1111:1111 1111:1111 - concat = input
                      |         |         |         |
                   2^32      2^16       2^8       2^0
   */
	do_test_s64_varint(1, 1, "02");
	do_test_s64_varint(-1, 1, "01");
	do_test_s64_varint(-2, 1, "03");

	do_test_u32_varint(2147483647, 5, "FFFFFFFF07");
  /*
              0111:1111 1111:1111 1111:1111 1111:1111 - input (7fffffff)
    1111:1111 1111:1111 1111:1111 1111:1111 0000:0111 - output (ffffff07)
     111:1111  111:1111  111:1111  111:1111  000:0111 - chop
     000:0111  111:1111  111:1111  111:1111  111:1111 - swap
              0111:1111 1111:1111 1111:1111 1111:1111 - concat = input
                      |         |         |         |
                   2^32      2^16       2^8       2^0
   */
	do_test_s32_varint(2147483647, 5, "FEFFFFFF0F");
  /*
              0111:1111 1111:1111 1111:1111 1111:1111 - input (7fffffff)
    1111:1110 1111:1111 1111:1111 1111:1111 0000:1111 - output(feffffff0f)
    1111:1111 1111:1111 1111:1111 1111:1111 0000:0111 - zigzag (ffffff07)
     111:1111  111:1111  111:1111  111:1111  000:0111 - chop
     000:0111  111:1111  111:1111  111:1111  111:1111 - swap
              0111:1111 1111:1111 1111:1111 1111:1111 - concat = input
                      |         |         |         |
                   2^32      2^16       2^8       2^0
   */
	do_test_s32_varint(-2147483648, 5, "FFFFFFFF0F");

	do_test_s32_varint(1, 1, "02");
  /*
    0000:0001 - input (01)
    0000:0010 - A: input << 1
    0000:0000 - B: input >> 31
    0000:0010 - zigzag (A xor B) == output
   */

	do_test_s32_varint(-1, 1, "01");
  /*
    1111:1111 ... 1111:1111 - input (FFFFFFFF)
    1111:1111 ... 1111:1110 - A: input << 1
    1111:1111 ... 1111:1111 - B: input >> 31
    0000:0000 ... 0000:0001 - zigzag (A xor B) == output
   */


}


static void do_test_u64_roundtrip(uint64_t i64_in)
{
	uint8_t buffer[16];
	uint64_t i64_out;
	size_t size_in, size_out;
	size_in = varint_u64_encode_buf(i64_in, buffer);
	i64_out = varint_u64_decode(buffer, buffer + size_in, &size_out);
	CU_ASSERT_EQUAL(i64_in, i64_out);
	CU_ASSERT_EQUAL(size_in, size_out);
}

static void do_test_s64_roundtrip(int64_t i64_in)
{
	uint8_t buffer[16];
	int64_t i64_out;
	size_t size_in, size_out;
	size_in = varint_s64_encode_buf(i64_in, buffer);
	i64_out = varint_s64_decode(buffer, buffer + size_in, &size_out);
	CU_ASSERT_EQUAL(i64_in, i64_out);
	CU_ASSERT_EQUAL(size_in, size_out);
}

static void test_varint_roundtrip(void)
{
	int i;
	for ( i = 0; i < 1024; i += 63 )
	{
		do_test_u64_roundtrip(i);
		do_test_s64_roundtrip(i);
		do_test_s64_roundtrip(-1*i);
	}
}

static void test_zigzag(void)
{
	int64_t a;
	int32_t b;
	int i;

	for ( i = 1; i < 1024; i += 31 )
	{
		a = b = i;
		CU_ASSERT_EQUAL(a, unzigzag64(zigzag64(a)));
		CU_ASSERT_EQUAL(b, unzigzag32(zigzag64(b)));
		
		a = b = -1 * i;
		CU_ASSERT_EQUAL(a, unzigzag64(zigzag64(a)));
		CU_ASSERT_EQUAL(b, unzigzag32(zigzag64(b)));
	}

}


/*
** Used by the test harness to register the tests in this file.
*/
void varint_suite_setup(void);
void varint_suite_setup(void)
{
	CU_pSuite suite = CU_add_suite("varint", NULL, NULL);
	PG_ADD_TEST(suite, test_zigzag);
	PG_ADD_TEST(suite, test_varint);
	PG_ADD_TEST(suite, test_varint_roundtrip);
}
