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

/* Read band from WKB as at start of band */
static rt_band
rt_band_from_wkb(
	uint16_t width, uint16_t height,
	const uint8_t** ptr, const uint8_t* end,
	uint8_t littleEndian
) {
	rt_band band = NULL;
	int pixbytes = 0;
	uint8_t type = 0;
	unsigned long sz = 0;
	uint32_t v = 0;

	assert(NULL != ptr);
	assert(NULL != end);

	band = rtalloc(sizeof (struct rt_band_t));
	if (!band) {
		rterror("rt_band_from_wkb: Out of memory allocating rt_band during WKB parsing");
		return NULL;
	}
	band->ownsdata = 0; /* assume we don't own data */

	if (end - *ptr < 1) {
		rterror("rt_band_from_wkb: Premature end of WKB on band reading (%s:%d)",
			__FILE__, __LINE__);
		rt_band_destroy(band);
		return NULL;
	}
	type = read_uint8(ptr);

	if ((type & BANDTYPE_PIXTYPE_MASK) >= PT_END) {
		rterror("rt_band_from_wkb: Invalid pixtype %d", type & BANDTYPE_PIXTYPE_MASK);
		rt_band_destroy(band);
		return NULL;
	}

	band->pixtype = type & BANDTYPE_PIXTYPE_MASK;
	band->offline = BANDTYPE_IS_OFFDB(type) ? 1 : 0;
	band->hasnodata = BANDTYPE_HAS_NODATA(type) ? 1 : 0;
	band->isnodata = band->hasnodata ? (BANDTYPE_IS_NODATA(type) ? 1 : 0) : 0;
	band->width = width;
	band->height = height;

	RASTER_DEBUGF(3, " Band pixtype:%s, offline:%d, hasnodata:%d",
		rt_pixtype_name(band->pixtype),
		band->offline,
		band->hasnodata
	);

	/* Check there's enough bytes to read nodata value */
	pixbytes = rt_pixtype_size(band->pixtype);
	if (((*ptr) + pixbytes) >= end) {
		rterror("rt_band_from_wkb: Premature end of WKB on band novalue reading");
		rt_band_destroy(band);
		return NULL;
	}

	/* Read nodata value */
	switch (band->pixtype) {
		case PT_1BB: {
			band->nodataval = ((int) read_uint8(ptr)) & 0x01;
			break;
		}
		case PT_2BUI: {
			band->nodataval = ((int) read_uint8(ptr)) & 0x03;
			break;
		}
		case PT_4BUI: {
			band->nodataval = ((int) read_uint8(ptr)) & 0x0F;
			break;
		}
		case PT_8BSI: {
			band->nodataval = read_int8(ptr);
			break;
		}
		case PT_8BUI: {
			band->nodataval = read_uint8(ptr);
			break;
		}
		case PT_16BSI: {
			band->nodataval = read_int16(ptr, littleEndian);
			break;
		}
		case PT_16BUI: {
			band->nodataval = read_uint16(ptr, littleEndian);
			break;
		}
		case PT_32BSI: {
			band->nodataval = read_int32(ptr, littleEndian);
			break;
		}
		case PT_32BUI: {
			band->nodataval = read_uint32(ptr, littleEndian);
			break;
		}
		case PT_32BF: {
			band->nodataval = read_float32(ptr, littleEndian);
			break;
		}
		case PT_64BF: {
			band->nodataval = read_float64(ptr, littleEndian);
			break;
		}
		default: {
			rterror("rt_band_from_wkb: Unknown pixeltype %d", band->pixtype);
			rt_band_destroy(band);
			return NULL;
		}
	}

	RASTER_DEBUGF(3, " Nodata value: %g, pixbytes: %d, ptr @ %p, end @ %p",
		band->nodataval, pixbytes, *ptr, end);

	if (band->offline) {
		if (((*ptr) + 1) >= end) {
			rterror("rt_band_from_wkb: Premature end of WKB on offline "
				"band data bandNum reading (%s:%d)",
				__FILE__, __LINE__
			);
			rt_band_destroy(band);
			return NULL;
		}

		band->data.offline.bandNum = read_int8(ptr);
		band->data.offline.mem = NULL;

		{
			/* check we have a NULL-termination */
			sz = 0;
			while ((*ptr)[sz] && &((*ptr)[sz]) < end) ++sz;
			if (&((*ptr)[sz]) >= end) {
				rterror("rt_band_from_wkb: Premature end of WKB on band offline path reading");
				rt_band_destroy(band);
				return NULL;
			}

			/* we never own offline band data */
			band->ownsdata = 0;

			band->data.offline.path = rtalloc(sz + 1);
			if (band->data.offline.path == NULL) {
				rterror("rt_band_from_wkb: Out of memory allocating for offline path of band");
				rt_band_destroy(band);
				return NULL;
			}

			memcpy(band->data.offline.path, *ptr, sz);
			band->data.offline.path[sz] = '\0';

			RASTER_DEBUGF(3, "OFFDB band path is %s (size is %d)",
				band->data.offline.path, sz);

			*ptr += sz + 1;

			/* TODO: How could we know if the offline band is a nodata band? */
			/* trust in the force */
			/*band->isnodata = FALSE;*/
		}

		return band;
	}

	/* This is an on-disk band */
	sz = width * height * pixbytes;
	if (((*ptr) + sz) > end) {
		rterror("rt_band_from_wkb: Premature end of WKB on band data reading (%s:%d)",
			__FILE__, __LINE__);
		rt_band_destroy(band);
		return NULL;
	}

	band->data.mem = rtalloc(sz);
	if (!band->data.mem) {
		rterror("rt_band_from_wkb: Out of memory during band creation in WKB parser");
		rt_band_destroy(band);
		return NULL;
	}

	band->ownsdata = 1; /* we DO own this data!!! */
	memcpy(band->data.mem, *ptr, sz);
	*ptr += sz;

	/* Should now flip values if > 8bit and
	 * littleEndian != isMachineLittleEndian */
	if (pixbytes > 1) {
		if (isMachineLittleEndian() != littleEndian) {
			void (*flipper)(uint8_t*) = 0;
			uint8_t *flipme = NULL;

			if (pixbytes == 2)
				flipper = flip_endian_16;
			else if (pixbytes == 4)
				flipper = flip_endian_32;
			else if (pixbytes == 8)
				flipper = flip_endian_64;
			else {
				rterror("rt_band_from_wkb: Unexpected pix bytes %d", pixbytes);
				rt_band_destroy(band);
				return NULL;
			}

			flipme = band->data.mem;
			sz = width * height;
			for (v = 0; v < sz; ++v) {
				flipper(flipme);
				flipme += pixbytes;
			}
		}
	}
	/* And should check for invalid values for < 8bit types */
	else if (
		band->pixtype == PT_1BB ||
		band->pixtype == PT_2BUI ||
		band->pixtype == PT_4BUI
	) {
		uint8_t maxVal = band->pixtype == PT_1BB ? 1 : (band->pixtype == PT_2BUI ? 3 : 15);
		uint8_t val;

		sz = width*height;
		for (v = 0; v < sz; ++v) {
			val = ((uint8_t*) band->data.mem)[v];
			if (val > maxVal) {
				rterror("rt_band_from_wkb: Invalid value %d for pixel of type %s",
					val, rt_pixtype_name(band->pixtype));
				rt_band_destroy(band);
				return NULL;
			}
		}
	}

	/* And we should check if the band is a nodata band */
	/* TODO: No!! This is too slow */
	/*rt_band_check_is_nodata(band);*/

	return band;
}

/* -4 for size, +1 for endian */
#define RT_WKB_HDR_SZ (sizeof(struct rt_raster_serialized_t)-4+1)

rt_raster
rt_raster_from_wkb(const uint8_t* wkb, uint32_t wkbsize) {
	const uint8_t *ptr = wkb;
	const uint8_t *wkbend = NULL;
	rt_raster rast = NULL;
	uint8_t endian = 0;
	uint16_t version = 0;
	uint16_t i = 0;
	uint16_t j = 0;

	assert(NULL != ptr);

	/* Check that wkbsize is >= sizeof(rt_raster_serialized) */
	if (wkbsize < RT_WKB_HDR_SZ) {
		rterror("rt_raster_from_wkb: wkb size (%d)  < min size (%d)",
			wkbsize, RT_WKB_HDR_SZ);
		return NULL;
	}
	wkbend = wkb + wkbsize;

	RASTER_DEBUGF(3, "Parsing header from wkb position %d (expected 0)",
		d_binptr_to_pos(ptr, wkbend, wkbsize));

	CHECK_BINPTR_POSITION(ptr, wkbend, wkbsize, 0);

	/* Read endianness */
	endian = *ptr;
	ptr += 1;

	/* Read version of protocol */
	version = read_uint16(&ptr, endian);
	if (version != 0) {
		rterror("rt_raster_from_wkb: WKB version %d unsupported", version);
		return NULL;
	}

	/* Read other components of raster header */
	rast = (rt_raster) rtalloc(sizeof (struct rt_raster_t));
	if (!rast) {
		rterror("rt_raster_from_wkb: Out of memory allocating raster for wkb input");
		return NULL;
	}

	rast->numBands = read_uint16(&ptr, endian);
	rast->scaleX = read_float64(&ptr, endian);
	rast->scaleY = read_float64(&ptr, endian);
	rast->ipX = read_float64(&ptr, endian);
	rast->ipY = read_float64(&ptr, endian);
	rast->skewX = read_float64(&ptr, endian);
	rast->skewY = read_float64(&ptr, endian);
	rast->srid = clamp_srid(read_int32(&ptr, endian));
	rast->width = read_uint16(&ptr, endian);
	rast->height = read_uint16(&ptr, endian);

	/* Consistency checking, should have been checked before */
	assert(ptr <= wkbend);

	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster numBands: %d",
		rast->numBands);
	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster scale: %gx%g",
		rast->scaleX, rast->scaleY);
	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster ip: %gx%g",
		rast->ipX, rast->ipY);
	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster skew: %gx%g",
		rast->skewX, rast->skewY);
	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster srid: %d",
		rast->srid);
	RASTER_DEBUGF(3, "rt_raster_from_wkb: Raster dims: %dx%d",
		rast->width, rast->height);
	RASTER_DEBUGF(3, "Parsing raster header finished at wkb position %d (expected 61)",
		d_binptr_to_pos(ptr, wkbend, wkbsize));

	CHECK_BINPTR_POSITION(ptr, wkbend, wkbsize, 61);

	/* Read all bands of raster */
	if (!rast->numBands) {
		/* Here ptr should have been left to right after last used byte */
		if (ptr < wkbend) {
			rtwarn("%d bytes of WKB remained unparsed", wkbend - ptr);
		}
		else if (ptr > wkbend) {
			/* Easier to get a segfault before I guess */
			rtwarn("We parsed %d bytes more then available!", ptr - wkbend);
		}

		rast->bands = NULL;
		return rast;
	}

	/* Now read the bands */
	rast->bands = (rt_band*) rtalloc(sizeof(rt_band) * rast->numBands);
	if (!rast->bands) {
		rterror("rt_raster_from_wkb: Out of memory allocating bands for WKB raster decoding");
		rt_raster_destroy(rast);
		return NULL;
	}

	/* ptr should now point to start of first band */
	/* we should have checked this before */
	assert(ptr <= wkbend);

	for (i = 0; i < rast->numBands; ++i) {
		RASTER_DEBUGF(3, "Parsing band %d from wkb position %d", i,
			d_binptr_to_pos(ptr, wkbend, wkbsize));

		rt_band band = rt_band_from_wkb(rast->width, rast->height,
			&ptr, wkbend, endian);
		if (!band) {
			rterror("rt_raster_from_wkb: Error reading WKB form of band %d", i);
			for (j = 0; j < i; j++) rt_band_destroy(rast->bands[j]);
			rt_raster_destroy(rast);
			return NULL;
		}

		band->raster = rast;
		rast->bands[i] = band;
	}

	/* Here ptr should have been left to right after last used byte */
	if (ptr < wkbend) {
		rtwarn("%d bytes of WKB remained unparsed", wkbend - ptr);
	}
	else if (ptr > wkbend) {
		/* Easier to get a segfault before I guess */
		rtwarn("We parsed %d bytes more then available!", ptr - wkbend);
	}

	return rast;
}

rt_raster
rt_raster_from_hexwkb(const char* hexwkb, uint32_t hexwkbsize) {
	rt_raster ret = NULL;
	uint8_t* wkb = NULL;
	uint32_t wkbsize = 0;
	uint32_t i = 0;

	assert(NULL != hexwkb);

	RASTER_DEBUGF(3, "input wkb: %s", hexwkb);
	RASTER_DEBUGF(3, "input wkbsize: %d", hexwkbsize);

	if (hexwkbsize % 2) {
		rterror("rt_raster_from_hexwkb: Raster HEXWKB input must have an even number of characters");
		return NULL;
	}
	wkbsize = hexwkbsize / 2;

	wkb = rtalloc(wkbsize);
	if (!wkb) {
		rterror("rt_raster_from_hexwkb: Out of memory allocating memory for decoding HEXWKB");
		return NULL;
	}

	/* parse full hex */
	for (i = 0; i < wkbsize; ++i) {
		wkb[i] = parse_hex((char*) & (hexwkb[i * 2]));
	}

	ret = rt_raster_from_wkb(wkb, wkbsize);
	rtdealloc(wkb); /* as long as rt_raster_from_wkb copies memory */

	return ret;
}

static uint32_t
rt_raster_wkb_size(rt_raster raster, int outasin) {
	uint32_t size = RT_WKB_HDR_SZ;
	uint16_t i = 0;

	assert(NULL != raster);

	RASTER_DEBUGF(3, "rt_raster_wkb_size: computing size for %d bands",
		raster->numBands);

	for (i = 0; i < raster->numBands; ++i) {
		rt_band band = raster->bands[i];
		rt_pixtype pixtype = band->pixtype;
		int pixbytes = rt_pixtype_size(pixtype);

		RASTER_DEBUGF(3, "rt_raster_wkb_size: adding size of band %d", i);

		if (pixbytes < 1) {
			rterror("rt_raster_wkb_size: Corrupted band: unknown pixtype");
			return 0;
		}

		/* Add space for band type */
		size += 1;

		/* Add space for nodata value */
		size += pixbytes;

		if (!outasin && band->offline) {
			/* Add space for band number */
			size += 1;

			/* Add space for null-terminated path */
			size += strlen(band->data.offline.path) + 1;
		}
		else {
			/* Add space for actual data */
			size += pixbytes * raster->width * raster->height;
		}
	}

	return size;
}

/**
 * Return this raster in WKB form
 *
 * @param raster : the raster
 * @param outasin : if TRUE, out-db bands are treated as in-db
 * @param wkbsize : will be set to the size of returned wkb form
 *
 * @return WKB of raster or NULL on error
 */
uint8_t *
rt_raster_to_wkb(rt_raster raster, int outasin, uint32_t *wkbsize) {

#if POSTGIS_DEBUG_LEVEL > 0
	const uint8_t *wkbend = NULL;
#endif

	uint8_t *wkb = NULL;
	uint8_t *ptr = NULL;
	uint16_t i = 0;
	uint8_t littleEndian = isMachineLittleEndian();

	assert(NULL != raster);
	assert(NULL != wkbsize);

	RASTER_DEBUG(2, "rt_raster_to_wkb: about to call rt_raster_wkb_size");

	*wkbsize = rt_raster_wkb_size(raster, outasin);
	RASTER_DEBUGF(3, "rt_raster_to_wkb: found size: %d", *wkbsize);

	wkb = (uint8_t*) rtalloc(*wkbsize);
	if (!wkb) {
		rterror("rt_raster_to_wkb: Out of memory allocating WKB for raster");
		return NULL;
	}

	ptr = wkb;

#if POSTGIS_DEBUG_LEVEL > 2
	wkbend = ptr + (*wkbsize);
#endif
	RASTER_DEBUGF(3, "Writing raster header to wkb on position %d (expected 0)",
		d_binptr_to_pos(ptr, wkbend, *wkbsize));

	/* Write endianness */
	*ptr = littleEndian;
	ptr += 1;

	/* Write version(size - (end - ptr)) */
	write_uint16(&ptr, littleEndian, 0);

	/* Copy header (from numBands up) */
	memcpy(ptr, &(raster->numBands), sizeof (struct rt_raster_serialized_t) - 6);
	ptr += sizeof (struct rt_raster_serialized_t) - 6;

	RASTER_DEBUGF(3, "Writing bands header to wkb position %d (expected 61)",
		d_binptr_to_pos(ptr, wkbend, *wkbsize));

	/* Serialize bands now */
	for (i = 0; i < raster->numBands; ++i) {
		rt_band band = raster->bands[i];
		rt_pixtype pixtype = band->pixtype;
		int pixbytes = rt_pixtype_size(pixtype);

		RASTER_DEBUGF(3, "Writing WKB for band %d", i);
		RASTER_DEBUGF(3, "Writing band pixel type to wkb position %d",
			d_binptr_to_pos(ptr, wkbend, *wkbsize));

		if (pixbytes < 1) {
			rterror("rt_raster_to_wkb: Corrupted band: unknown pixtype");
			rtdealloc(wkb);
			return NULL;
		}

		/* Add band type */
		*ptr = band->pixtype;
		if (!outasin && band->offline) *ptr |= BANDTYPE_FLAG_OFFDB;
		if (band->hasnodata) *ptr |= BANDTYPE_FLAG_HASNODATA;
		if (band->isnodata) *ptr |= BANDTYPE_FLAG_ISNODATA;
		ptr += 1;

#if 0
		/* no padding required for WKB */
		/* Add padding (if needed) */
		if (pixbytes > 1) {
			memset(ptr, '\0', pixbytes - 1);
			ptr += pixbytes - 1;
		}
		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!(((uint64_t) ptr) % pixbytes));
#endif

		RASTER_DEBUGF(3, "Writing band nodata to wkb position %d",
			d_binptr_to_pos(ptr, wkbend, *wkbsize));

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
				rterror("rt_raster_to_wkb: Fatal error caused by unknown pixel type. Aborting.");
				rtdealloc(wkb);
				abort(); /* shoudn't happen */
				return 0;
		}

#if 0
		/* no padding for WKB */
		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((uint64_t) ptr % pixbytes));
#endif

		if (!outasin && band->offline) {
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
			RASTER_DEBUGF(4, "rt_raster_to_wkb: Copying %d bytes", datasize);

			memcpy(ptr, rt_band_get_data(band), datasize);

			ptr += datasize;
		}

#if 0
		/* no padding for WKB */
		/* Pad up to 8-bytes boundary */
		while ((uint64_t) ptr % 8) {
			*ptr = 0;
			++ptr;
		}

		/* Consistency checking (ptr is pixbytes-aligned) */
		assert(!((uint64_t) ptr % pixbytes));
#endif
	}

	return wkb;
}

char *
rt_raster_to_hexwkb(rt_raster raster, int outasin, uint32_t *hexwkbsize) {
	uint8_t *wkb = NULL;
	char* hexwkb = NULL;
	uint32_t wkbsize = 0;

	assert(NULL != raster);
	assert(NULL != hexwkbsize);

	RASTER_DEBUG(2, "rt_raster_to_hexwkb: calling rt_raster_to_wkb");

	wkb = rt_raster_to_wkb(raster, outasin, &wkbsize);

	RASTER_DEBUG(3, "rt_raster_to_hexwkb: rt_raster_to_wkb returned");

	*hexwkbsize = wkbsize * 2; /* hex is 2 times bytes */
	hexwkb = (char*) rtalloc((*hexwkbsize) + 1);
	if (!hexwkb) {
		rterror("rt_raster_to_hexwkb: Out of memory hexifying raster WKB");
		rtdealloc(wkb);
		return NULL;
	}

	char *optr = hexwkb;
	uint8_t *iptr = wkb;
	const char hexchar[]="0123456789ABCDEF";
	while (wkbsize--) {
		uint8_t v = *iptr++;
		*optr++ = hexchar[v>>4];
		*optr++ = hexchar[v & 0x0F];
	}
	*optr = '\0'; /* Null-terminate */

	rtdealloc(wkb); /* we don't need this anymore */

	RASTER_DEBUGF(3, "rt_raster_to_hexwkb: output wkb: %s", hexwkb);
	return hexwkb;
}
