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

#include "librtcore.h"
#include "librtcore_internal.h"
#include "rt_serialize.h"

/******************************************************************************
* Debug and Testing Utilities
******************************************************************************/

#if POSTGIS_DEBUG_LEVEL > 2

char*
d_binary_to_hex(const uint8_t * const raw, uint32_t size, uint32_t *hexsize) {
    char* hex = NULL;
    uint32_t i = 0;


    assert(NULL != raw);
    assert(NULL != hexsize);


    *hexsize = size * 2; /* hex is 2 times bytes */
    hex = (char*) rtalloc((*hexsize) + 1);
    if (!hex) {
        rterror("d_binary_to_hex: Out of memory hexifying raw binary");
        return NULL;
    }
    hex[*hexsize] = '\0'; /* Null-terminate */

    for (i = 0; i < size; ++i) {
        deparse_hex(raw[i], &(hex[2 * i]));
    }

    assert(NULL != hex);
    assert(0 == strlen(hex) % 2);
    return hex;
}

void
d_print_binary_hex(const char* msg, const uint8_t * const raw, uint32_t size) {
    char* hex = NULL;
    uint32_t hexsize = 0;


    assert(NULL != msg);
    assert(NULL != raw);


    hex = d_binary_to_hex(raw, size, &hexsize);
    if (NULL != hex) {
        rtinfo("%s\t%s", msg, hex);
        rtdealloc(hex);
    }
}

size_t
d_binptr_to_pos(const uint8_t * const ptr, const uint8_t * const end, size_t size) {
    assert(NULL != ptr && NULL != end);

    return (size - (end - ptr));
}

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
setBits(char* ch, double val, int bits, int bitOffset) {
    char mask = 0xFF >> (8 - bits);
    char ival = val;


		assert(ch != NULL);
    assert(8 - bitOffset >= bits);

    RASTER_DEBUGF(4, "ival:%d bits:%d mask:%hhx bitoffset:%d\n",
            ival, bits, mask, bitOffset);

    /* clear all but significant bits from ival */
    ival &= mask;
#if POSTGIS_RASTER_WARN_ON_TRUNCATION > 0
    if (ival != val) {
        rtwarn("Pixel value for %d-bits band got truncated"
                " from %g to %hhu", bits, val, ival);
    }
#endif /* POSTGIS_RASTER_WARN_ON_TRUNCATION */

    RASTER_DEBUGF(4, " cleared ival:%hhx\n", ival);


    /* Shift ival so the significant bits start at
     * the first bit */
    ival <<= (8 - bitOffset - bits);

    RASTER_DEBUGF(4, " ival shifted:%hhx\n", ival);
    RASTER_DEBUGF(4, "  ch:%hhx\n", *ch);

    /* clear first bits of target */
    *ch &= ~(mask << (8 - bits - bitOffset));

    RASTER_DEBUGF(4, "  ch cleared:%hhx\n", *ch);

    /* Set the first bit of target */
    *ch |= ival;

    RASTER_DEBUGF(4, "  ch ored:%hhx\n", *ch);

}
#endif /* OPTIMIZE_SPACE */

void
swap_char(uint8_t *a, uint8_t *b) {
    uint8_t c = 0;

    assert(NULL != a && NULL != b);

    c = *a;
    *a = *b;
    *b = c;
}

void
flip_endian_16(uint8_t *d) {
    assert(NULL != d);

    swap_char(d, d + 1);
}

void
flip_endian_32(uint8_t *d) {
    assert(NULL != d);

    swap_char(d, d + 3);
    swap_char(d + 1, d + 2);
}

void
flip_endian_64(uint8_t *d) {
    assert(NULL != d);

    swap_char(d + 7, d);
    swap_char(d + 6, d + 1);
    swap_char(d + 5, d + 2);
    swap_char(d + 4, d + 3);
}

uint8_t
isMachineLittleEndian(void) {
    static int endian_check_int = 1; /* dont modify this!!! */
    /* 0=big endian|xdr --  1=little endian|ndr */
    return *((uint8_t *) & endian_check_int);
}

uint8_t
read_uint8(const uint8_t** from) {
    assert(NULL != from);

    return *(*from)++;
}

/* unused up to now
void
write_uint8(uint8_t** from, uint8_t v) {
    assert(NULL != from);

 *(*from)++ = v;
}
*/

int8_t
read_int8(const uint8_t** from) {
    assert(NULL != from);

    return (int8_t) read_uint8(from);
}

/* unused up to now
void
write_int8(uint8_t** from, int8_t v) {
    assert(NULL != from);

 *(*from)++ = v;
}
*/

uint16_t
read_uint16(const uint8_t** from, uint8_t littleEndian) {
    uint16_t ret = 0;

    assert(NULL != from);

    if (littleEndian) {
        ret = (*from)[0] |
                (*from)[1] << 8;
    } else {
        /* big endian */
        ret = (*from)[0] << 8 |
                (*from)[1];
    }
    *from += 2;
    return ret;
}

void
write_uint16(uint8_t** to, uint8_t littleEndian, uint16_t v) {
    assert(NULL != to);

    if (littleEndian) {
        (*to)[0] = v & 0x00FF;
        (*to)[1] = v >> 8;
    } else {
        (*to)[1] = v & 0x00FF;
        (*to)[0] = v >> 8;
    }
    *to += 2;
}

int16_t
read_int16(const uint8_t** from, uint8_t littleEndian) {
    assert(NULL != from);

    return read_uint16(from, littleEndian);
}

/* unused up to now
void
write_int16(uint8_t** to, uint8_t littleEndian, int16_t v) {
    assert(NULL != to);

    if ( littleEndian )
    {
        (*to)[0] = v & 0x00FF;
        (*to)[1] = v >> 8;
    }
    else
    {
        (*to)[1] = v & 0x00FF;
        (*to)[0] = v >> 8;
    }
 *to += 2;
}
*/

uint32_t
read_uint32(const uint8_t** from, uint8_t littleEndian) {
    uint32_t ret = 0;

    assert(NULL != from);

    if (littleEndian) {
        ret = (uint32_t) ((*from)[0] & 0xff) |
                (uint32_t) ((*from)[1] & 0xff) << 8 |
                (uint32_t) ((*from)[2] & 0xff) << 16 |
                (uint32_t) ((*from)[3] & 0xff) << 24;
    } else {
        /* big endian */
        ret = (uint32_t) ((*from)[3] & 0xff) |
                (uint32_t) ((*from)[2] & 0xff) << 8 |
                (uint32_t) ((*from)[1] & 0xff) << 16 |
                (uint32_t) ((*from)[0] & 0xff) << 24;
    }

    *from += 4;
    return ret;
}

/* unused up to now
void
write_uint32(uint8_t** to, uint8_t littleEndian, uint32_t v) {
    assert(NULL != to);

    if ( littleEndian )
    {
        (*to)[0] = v & 0x000000FF;
        (*to)[1] = ( v & 0x0000FF00 ) >> 8;
        (*to)[2] = ( v & 0x00FF0000 ) >> 16;
        (*to)[3] = ( v & 0xFF000000 ) >> 24;
    }
    else
    {
        (*to)[3] = v & 0x000000FF;
        (*to)[2] = ( v & 0x0000FF00 ) >> 8;
        (*to)[1] = ( v & 0x00FF0000 ) >> 16;
        (*to)[0] = ( v & 0xFF000000 ) >> 24;
    }
 *to += 4;
}
*/

int32_t
read_int32(const uint8_t** from, uint8_t littleEndian) {
    assert(NULL != from);

    return read_uint32(from, littleEndian);
}

/* unused up to now
void
write_int32(uint8_t** to, uint8_t littleEndian, int32_t v) {
    assert(NULL != to);

    if ( littleEndian )
    {
        (*to)[0] = v & 0x000000FF;
        (*to)[1] = ( v & 0x0000FF00 ) >> 8;
        (*to)[2] = ( v & 0x00FF0000 ) >> 16;
        (*to)[3] = ( v & 0xFF000000 ) >> 24;
    }
    else
    {
        (*to)[3] = v & 0x000000FF;
        (*to)[2] = ( v & 0x0000FF00 ) >> 8;
        (*to)[1] = ( v & 0x00FF0000 ) >> 16;
        (*to)[0] = ( v & 0xFF000000 ) >> 24;
    }
 *to += 4;
}
*/

float
read_float32(const uint8_t** from, uint8_t littleEndian) {

    union {
        float f;
        uint32_t i;
    } ret;

    ret.i = read_uint32(from, littleEndian);

    return ret.f;
}

/* unused up to now
void
write_float32(uint8_t** from, uint8_t littleEndian, float f) {
    union {
        float f;
        uint32_t i;
    } u;

    u.f = f;
    write_uint32(from, littleEndian, u.i);
}
*/

double
read_float64(const uint8_t** from, uint8_t littleEndian) {

    union {
        double d;
        uint64_t i;
    } ret;

    assert(NULL != from);

    if (littleEndian) {
        ret.i = (uint64_t) ((*from)[0] & 0xff) |
                (uint64_t) ((*from)[1] & 0xff) << 8 |
                (uint64_t) ((*from)[2] & 0xff) << 16 |
                (uint64_t) ((*from)[3] & 0xff) << 24 |
                (uint64_t) ((*from)[4] & 0xff) << 32 |
                (uint64_t) ((*from)[5] & 0xff) << 40 |
                (uint64_t) ((*from)[6] & 0xff) << 48 |
                (uint64_t) ((*from)[7] & 0xff) << 56;
    } else {
        /* big endian */
        ret.i = (uint64_t) ((*from)[7] & 0xff) |
                (uint64_t) ((*from)[6] & 0xff) << 8 |
                (uint64_t) ((*from)[5] & 0xff) << 16 |
                (uint64_t) ((*from)[4] & 0xff) << 24 |
                (uint64_t) ((*from)[3] & 0xff) << 32 |
                (uint64_t) ((*from)[2] & 0xff) << 40 |
                (uint64_t) ((*from)[1] & 0xff) << 48 |
                (uint64_t) ((*from)[0] & 0xff) << 56;
    }

    *from += 8;
    return ret.d;
}

/* unused up to now
void
write_float64(uint8_t** to, uint8_t littleEndian, double v) {
    union {
        double d;
        uint64_t i;
    } u;

    assert(NULL != to);

    u.d = v;

    if ( littleEndian )
    {
        (*to)[0] =   u.i & 0x00000000000000FFULL;
        (*to)[1] = ( u.i & 0x000000000000FF00ULL ) >> 8;
        (*to)[2] = ( u.i & 0x0000000000FF0000ULL ) >> 16;
        (*to)[3] = ( u.i & 0x00000000FF000000ULL ) >> 24;
        (*to)[4] = ( u.i & 0x000000FF00000000ULL ) >> 32;
        (*to)[5] = ( u.i & 0x0000FF0000000000ULL ) >> 40;
        (*to)[6] = ( u.i & 0x00FF000000000000ULL ) >> 48;
        (*to)[7] = ( u.i & 0xFF00000000000000ULL ) >> 56;
    }
    else
    {
        (*to)[7] =   u.i & 0x00000000000000FFULL;
        (*to)[6] = ( u.i & 0x000000000000FF00ULL ) >> 8;
        (*to)[5] = ( u.i & 0x0000000000FF0000ULL ) >> 16;
        (*to)[4] = ( u.i & 0x00000000FF000000ULL ) >> 24;
        (*to)[3] = ( u.i & 0x000000FF00000000ULL ) >> 32;
        (*to)[2] = ( u.i & 0x0000FF0000000000ULL ) >> 40;
        (*to)[1] = ( u.i & 0x00FF000000000000ULL ) >> 48;
        (*to)[0] = ( u.i & 0xFF00000000000000ULL ) >> 56;
    }
 *to += 8;
}
*/

static uint32_t
rt_raster_serialized_size(rt_raster raster) {
	uint32_t size = sizeof (struct rt_raster_serialized_t);
	uint16_t i = 0;

	assert(NULL != raster);

	RASTER_DEBUGF(3, "Serialized size with just header:%d - now adding size of %d bands",
		size, raster->numBands);

	for (i = 0; i < raster->numBands; ++i) {
		rt_band band = raster->bands[i];
		rt_pixtype pixtype = band->pixtype;
		int pixbytes = rt_pixtype_size(pixtype);

		if (pixbytes < 1) {
			rterror("rt_raster_serialized_size: Corrupted band: unknown pixtype");
			return 0;
		}

		/* Add space for band type, hasnodata flag and data padding */
		size += pixbytes;

		/* Add space for nodata value */
		size += pixbytes;

		if (band->offline) {
			/* Add space for band number */
			size += 1;

			/* Add space for null-terminated path */
			size += strlen(band->data.offline.path) + 1;
		}
		else {
			/* Add space for raster band data */
			size += pixbytes * raster->width * raster->height;
		}

		RASTER_DEBUGF(3, "Size before alignment is %d", size);

		/* Align size to 8-bytes boundary (trailing padding) */
		/* XXX jorgearevalo: bug here. If the size is actually 8-bytes aligned,
		   this line will add 8 bytes trailing padding, and it's not necessary */
		/*size += 8 - (size % 8);*/
		if (size % 8)
			size += 8 - (size % 8);

		RASTER_DEBUGF(3, "Size after alignment is %d", size);
	}

	return size;
}

/**
 * Return this raster in serialized form.
 * Memory (band data included) is copied from rt_raster.
 *
 * Serialized form is documented in doc/RFC1-SerializedFormat.
 */
void*
rt_raster_serialize(rt_raster raster) {
	uint32_t size = 0;
	uint8_t* ret = NULL;
	uint8_t* ptr = NULL;
	uint16_t i = 0;

	assert(NULL != raster);

	size = rt_raster_serialized_size(raster);
	ret = (uint8_t*) rtalloc(size);
	if (!ret) {
		rterror("rt_raster_serialize: Out of memory allocating %d bytes for serializing a raster", size);
		return NULL;
	}
	memset(ret, '-', size);
	ptr = ret;

	RASTER_DEBUGF(3, "sizeof(struct rt_raster_serialized_t):%u",
		sizeof (struct rt_raster_serialized_t));
	RASTER_DEBUGF(3, "sizeof(struct rt_raster_t):%u",
		sizeof (struct rt_raster_t));
	RASTER_DEBUGF(3, "serialized size:%lu", (long unsigned) size);

	/* Set size */
	/* NOTE: Value of rt_raster.size may be updated in
	 * returned object, for instance, by rt_pg layer to
	 * store value calculated by SET_VARSIZE.
	 */
	raster->size = size;

	/* Set version */
	raster->version = 0;

	/* Copy header */
	memcpy(ptr, raster, sizeof (struct rt_raster_serialized_t));

	RASTER_DEBUG(3, "Start hex dump of raster being serialized using 0x2D to mark non-written bytes");

#if POSTGIS_DEBUG_LEVEL > 2
	uint8_t* dbg_ptr = ptr;
	d_print_binary_hex("HEADER", dbg_ptr, size);
#endif

	ptr += sizeof (struct rt_raster_serialized_t);

	/* Serialize bands now */
	for (i = 0; i < raster->numBands; ++i) {
		rt_band band = raster->bands[i];
		assert(NULL != band);

		rt_pixtype pixtype = band->pixtype;
		int pixbytes = rt_pixtype_size(pixtype);
		if (pixbytes < 1) {
			rterror("rt_raster_serialize: Corrupted band: unknown pixtype");
			rtdealloc(ret);
			return NULL;
		}

		/* Add band type */
		*ptr = band->pixtype;
		if (band->offline) {
#ifdef POSTGIS_RASTER_DISABLE_OFFLINE
      rterror("rt_raster_serialize: offdb raster support disabled at compile-time");
      return NULL;
#endif
			*ptr |= BANDTYPE_FLAG_OFFDB;
		}
		if (band->hasnodata) {
			*ptr |= BANDTYPE_FLAG_HASNODATA;
		}

		if (band->isnodata) {
			*ptr |= BANDTYPE_FLAG_ISNODATA;
		}

#if POSTGIS_DEBUG_LEVEL > 2
		d_print_binary_hex("PIXTYPE", dbg_ptr, size);
#endif

		ptr += 1;

		/* Add padding (if needed) */
		if (pixbytes > 1) {
			memset(ptr, '\0', pixbytes - 1);
			ptr += pixbytes - 1;
		}

#if POSTGIS_DEBUG_LEVEL > 2
		d_print_binary_hex("PADDING", dbg_ptr, size);
#endif

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((ptr - ret) % pixbytes));

		/* Add nodata value */
		switch (pixtype) {
			case PT_1BB:
			case PT_2BUI:
			case PT_4BUI:
			case PT_8BUI: {
				uint8_t v = band->nodataval;
				*ptr = v;
				ptr += 1;
				break;
			}
			case PT_8BSI: {
				int8_t v = band->nodataval;
				*ptr = v;
				ptr += 1;
				break;
			}
			case PT_16BSI:
			case PT_16BUI: {
				uint16_t v = band->nodataval;
				memcpy(ptr, &v, 2);
				ptr += 2;
				break;
			}
			case PT_32BSI:
			case PT_32BUI: {
				uint32_t v = band->nodataval;
				memcpy(ptr, &v, 4);
				ptr += 4;
				break;
			}
			case PT_32BF: {
				float v = band->nodataval;
				memcpy(ptr, &v, 4);
				ptr += 4;
				break;
			}
			case PT_64BF: {
				memcpy(ptr, &band->nodataval, 8);
				ptr += 8;
				break;
			}
			default:
				rterror("rt_raster_serialize: Fatal error caused by unknown pixel type. Aborting.");
				rtdealloc(ret);
				return NULL;
		}

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((ptr - ret) % pixbytes));

#if POSTGIS_DEBUG_LEVEL > 2
		d_print_binary_hex("nodata", dbg_ptr, size);
#endif

		if (band->offline) {
			/* Write band number */
			*ptr = band->data.offline.bandNum;
			ptr += 1;

			/* Write path */
			strcpy((char*) ptr, band->data.offline.path);
			ptr += strlen(band->data.offline.path) + 1;
		}
		else {
			/* Write data */
			uint32_t datasize = raster->width * raster->height * pixbytes;
			memcpy(ptr, band->data.mem, datasize);
			ptr += datasize;
		}

#if POSTGIS_DEBUG_LEVEL > 2
		d_print_binary_hex("BAND", dbg_ptr, size);
#endif

		/* Pad up to 8-bytes boundary */
		while ((uintptr_t) ptr % 8) {
			*ptr = 0;
			++ptr;

			RASTER_DEBUGF(3, "PAD at %d", (uintptr_t) ptr % 8);
		}

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((ptr - ret) % pixbytes));
	} /* for-loop over bands */

#if POSTGIS_DEBUG_LEVEL > 2
		d_print_binary_hex("SERIALIZED RASTER", dbg_ptr, size);
#endif
	return ret;
}

/**
 * Return a raster from a serialized form.
 *
 * Serialized form is documented in doc/RFC1-SerializedFormat.
 *
 * NOTE: the raster will contain pointer to the serialized
 * form (including band data), which must be kept alive.
 */
rt_raster
rt_raster_deserialize(void* serialized, int header_only) {
	rt_raster rast = NULL;
	const uint8_t *ptr = NULL;
	const uint8_t *beg = NULL;
	uint16_t i = 0;
	uint16_t j = 0;
	uint8_t littleEndian = isMachineLittleEndian();

	assert(NULL != serialized);

	RASTER_DEBUG(2, "rt_raster_deserialize: Entering...");

	/* NOTE: Value of rt_raster.size may be different
	 * than actual size of raster data being read.
	 * See note on SET_VARSIZE in rt_raster_serialize function above.
	 */

	/* Allocate memory for deserialized raster header */ 
	RASTER_DEBUG(3, "rt_raster_deserialize: Allocating memory for deserialized raster header");
	rast = (rt_raster) rtalloc(sizeof (struct rt_raster_t));
	if (!rast) {
		rterror("rt_raster_deserialize: Out of memory allocating raster for deserialization");
		return NULL;
	}

	/* Deserialize raster header */
	RASTER_DEBUG(3, "rt_raster_deserialize: Deserialize raster header");
	memcpy(rast, serialized, sizeof (struct rt_raster_serialized_t));

	if (0 == rast->numBands || header_only) {
		rast->bands = 0;
		return rast;
	}

	beg = (const uint8_t*) serialized;

	/* Allocate registry of raster bands */
	RASTER_DEBUG(3, "rt_raster_deserialize: Allocating memory for bands");
	rast->bands = rtalloc(rast->numBands * sizeof (rt_band));
	if (rast->bands == NULL) {
		rterror("rt_raster_deserialize: Out of memory allocating bands");
		rtdealloc(rast);
		return NULL;
	}

	RASTER_DEBUGF(3, "rt_raster_deserialize: %d bands", rast->numBands);

	/* Move to the beginning of first band */
	ptr = beg;
	ptr += sizeof (struct rt_raster_serialized_t);

	/* Deserialize bands now */
	for (i = 0; i < rast->numBands; ++i) {
		rt_band band = NULL;
		uint8_t type = 0;
		int pixbytes = 0;

		band = rtalloc(sizeof(struct rt_band_t));
		if (!band) {
			rterror("rt_raster_deserialize: Out of memory allocating rt_band during deserialization");
			for (j = 0; j < i; j++) rt_band_destroy(rast->bands[j]);
			rt_raster_destroy(rast);
			return NULL;
		}

		rast->bands[i] = band;

		type = *ptr;
		ptr++;
		band->pixtype = type & BANDTYPE_PIXTYPE_MASK;

		RASTER_DEBUGF(3, "rt_raster_deserialize: band %d with pixel type %s", i, rt_pixtype_name(band->pixtype));

		band->offline = BANDTYPE_IS_OFFDB(type) ? 1 : 0;
		band->hasnodata = BANDTYPE_HAS_NODATA(type) ? 1 : 0;
		band->isnodata = band->hasnodata ? (BANDTYPE_IS_NODATA(type) ? 1 : 0) : 0;
		band->width = rast->width;
		band->height = rast->height;
		band->ownsdata = 0; /* we do NOT own this data!!! */
		band->raster = rast;

		/* Advance by data padding */
		pixbytes = rt_pixtype_size(band->pixtype);
		ptr += pixbytes - 1;

		/* Read nodata value */
		switch (band->pixtype) {
			case PT_1BB: {
				band->nodataval = ((int) read_uint8(&ptr)) & 0x01;
				break;
			}
			case PT_2BUI: {
				band->nodataval = ((int) read_uint8(&ptr)) & 0x03;
				break;
			}
			case PT_4BUI: {
				band->nodataval = ((int) read_uint8(&ptr)) & 0x0F;
				break;
			}
			case PT_8BSI: {
				band->nodataval = read_int8(&ptr);
				break;
			}
			case PT_8BUI: {
				band->nodataval = read_uint8(&ptr);
				break;
			}
			case PT_16BSI: {
				band->nodataval = read_int16(&ptr, littleEndian);
				break;
			}
			case PT_16BUI: {
				band->nodataval = read_uint16(&ptr, littleEndian);
				break;
			}
			case PT_32BSI: {
				band->nodataval = read_int32(&ptr, littleEndian);
				break;
			}
			case PT_32BUI: {
				band->nodataval = read_uint32(&ptr, littleEndian);
				break;
			}
			case PT_32BF: {
				band->nodataval = read_float32(&ptr, littleEndian);
				break;
			}
			case PT_64BF: {
				band->nodataval = read_float64(&ptr, littleEndian);
				break;
			}
			default: {
				rterror("rt_raster_deserialize: Unknown pixeltype %d", band->pixtype);
				for (j = 0; j <= i; j++) rt_band_destroy(rast->bands[j]);
				rt_raster_destroy(rast);
				return NULL;
			}
		}

		RASTER_DEBUGF(3, "rt_raster_deserialize: has nodata flag %d", band->hasnodata);
		RASTER_DEBUGF(3, "rt_raster_deserialize: nodata value %g", band->nodataval);

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((ptr - beg) % pixbytes));

		if (band->offline) {
			int pathlen = 0;

			/* Read band number */
			band->data.offline.bandNum = *ptr;
			ptr += 1;

			/* Register path */
			pathlen = strlen((char*) ptr);
			band->data.offline.path = rtalloc(sizeof(char) * (pathlen + 1));
			if (band->data.offline.path == NULL) {
				rterror("rt_raster_deserialize: Could not allocate memory for offline band path");
				for (j = 0; j <= i; j++) rt_band_destroy(rast->bands[j]);
				rt_raster_destroy(rast);
				return NULL;
			}

			memcpy(band->data.offline.path, ptr, pathlen);
			band->data.offline.path[pathlen] = '\0';
			ptr += pathlen + 1;

			band->data.offline.mem = NULL;
		}
		else {
			/* Register data */
			const uint32_t datasize = rast->width * rast->height * pixbytes;
			band->data.mem = (uint8_t*) ptr;
			ptr += datasize;
		}

		/* Skip bytes of padding up to 8-bytes boundary */
#if POSTGIS_DEBUG_LEVEL > 0
		const uint8_t *padbeg = ptr;
#endif
		while (0 != ((ptr - beg) % 8)) {
			++ptr;
		}

		RASTER_DEBUGF(3, "rt_raster_deserialize: skip %d bytes of 8-bytes boundary padding", ptr - padbeg);

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((ptr - beg) % pixbytes));
	}

	return rast;
}
