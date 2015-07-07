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

#ifndef RT_SERIALIZE_H_INCLUDED
#define RT_SERIALIZE_H_INCLUDED

#include "librtcore.h"

#define BANDTYPE_FLAGS_MASK 0xF0
#define BANDTYPE_PIXTYPE_MASK 0x0F
#define BANDTYPE_FLAG_OFFDB     (1<<7)
#define BANDTYPE_FLAG_HASNODATA (1<<6)
#define BANDTYPE_FLAG_ISNODATA  (1<<5)
#define BANDTYPE_FLAG_RESERVED3 (1<<4)

#define BANDTYPE_PIXTYPE(x) ((x)&BANDTYPE_PIXTYPE_MASK)
#define BANDTYPE_IS_OFFDB(x) ((x)&BANDTYPE_FLAG_OFFDB)
#define BANDTYPE_HAS_NODATA(x) ((x)&BANDTYPE_FLAG_HASNODATA)
#define BANDTYPE_IS_NODATA(x) ((x)&BANDTYPE_FLAG_ISNODATA)

#if POSTGIS_DEBUG_LEVEL > 2
char*
d_binary_to_hex(const uint8_t * const raw, uint32_t size, uint32_t *hexsize);

void
d_print_binary_hex(const char* msg, const uint8_t * const raw, uint32_t size);

size_t
d_binptr_to_pos(const uint8_t * const ptr, const uint8_t * const end, size_t size);

#define CHECK_BINPTR_POSITION(ptr, end, size, pos) \
    { if (pos != d_binptr_to_pos(ptr, end, size)) { \
    fprintf(stderr, "Check of binary stream pointer position failed on line %d\n", __LINE__); \
    fprintf(stderr, "\tactual = %lu, expected = %lu\n", (long unsigned)d_binptr_to_pos(ptr, end, size), (long unsigned)pos); \
    } }

#else

#define CHECK_BINPTR_POSITION(ptr, end, size, pos) ((void)0);

#endif /* if POSTGIS_DEBUG_LEVEL > 2 */

#ifdef OPTIMIZE_SPACE

/*
 * Set given number of bits of the given byte,
 * starting from given bitOffset (from the first)
 * to the given value.
 *
 * Examples:
 *   char ch;
 *   ch=0; setBits(&ch, 1, 1, 0) -> ch==8
 *   ch=0; setBits(&ch, 3, 2, 1) -> ch==96 (0x60)
 *
 * Note that number of bits set must be <= 8-bitOffset
 *
 */
void
setBits(char* ch, double val, int bits, int bitOffset);

#endif /* ifdef OPTIMIZE_SPACE */

void
swap_char(uint8_t *a, uint8_t *b);

void
flip_endian_16(uint8_t *d);

void
flip_endian_32(uint8_t *d);

void
flip_endian_64(uint8_t *d);

uint8_t
isMachineLittleEndian(void);

uint8_t
read_uint8(const uint8_t** from);

/* unused up to now
void
write_uint8(uint8_t** from, uint8_t v);
*/

int8_t
read_int8(const uint8_t** from);

/* unused up to now
void
write_int8(uint8_t** from, int8_t v);
*/

uint16_t
read_uint16(const uint8_t** from, uint8_t littleEndian);

void
write_uint16(uint8_t** to, uint8_t littleEndian, uint16_t v);

int16_t
read_int16(const uint8_t** from, uint8_t littleEndian);

/* unused up to now
void
write_int16(uint8_t** to, uint8_t littleEndian, int16_t v);
*/

uint32_t
read_uint32(const uint8_t** from, uint8_t littleEndian);

/* unused up to now
void
write_uint32(uint8_t** to, uint8_t littleEndian, uint32_t v);
*/

int32_t
read_int32(const uint8_t** from, uint8_t littleEndian);

/* unused up to now
void
write_int32(uint8_t** to, uint8_t littleEndian, int32_t v);
*/

float
read_float32(const uint8_t** from, uint8_t littleEndian);

/* unused up to now
void
write_float32(uint8_t** from, uint8_t littleEndian, float f);
*/

double
read_float64(const uint8_t** from, uint8_t littleEndian);

/* unused up to now
void
write_float64(uint8_t** to, uint8_t littleEndian, double v);
*/

#endif /* RT_SERIALIZE_H_INCLUDED */
