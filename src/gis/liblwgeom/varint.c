/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * Copyright (C) 2014 Sandro Santilli <strk@keybit.net>
 * Copyright (C) 2013 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 **********************************************************************
 * 
 * Handle varInt values, as described here:
 * http://developers.google.com/protocol-buffers/docs/encoding#varints
 *
 **********************************************************************/

#include "varint.h"
#include "lwgeom_log.h"
#include "liblwgeom.h"

/* -------------------------------------------------------------------------------- */

static size_t 
_varint_u64_encode_buf(uint64_t val, uint8_t *buf)
{
	uint8_t grp;	
	uint64_t q = val;
	uint8_t *ptr = buf;
	while (1) 
	{
		/* We put the 7 least significant bits in grp */
		grp = 0x7f & q; 
		/* We rightshift our input value 7 bits */
		/* which means that the 7 next least significant bits */
		/* becomes the 7 least significant */
		q = q >> 7;	
		/* Check if, after our rightshifting, we still have */
		/* anything to read in our input value. */
		if ( q > 0 )
		{
			/* In the next line quite a lot is happening. */
			/* Since there is more to read in our input value */
			/* we signal that by setting the most siginicant bit */
			/* in our byte to 1. */
			/* Then we put that byte in our buffer and move the pointer */
			/* forward one step */
			*ptr = 0x80 | grp;
			ptr++;
		}
		else
		{
			/* The same as above, but since there is nothing more */
			/* to read in our input value we leave the most significant bit unset */
			*ptr = grp;
			ptr++;
			return ptr - buf;
		}
	}
	/* This cannot happen */
	lwerror("%s: Got out of infinite loop. Consciousness achieved.", __func__);
	return (size_t)0;
}


size_t
varint_u64_encode_buf(uint64_t val, uint8_t *buf)
{
	return _varint_u64_encode_buf(val, buf);
}


size_t
varint_u32_encode_buf(uint32_t val, uint8_t *buf)
{
	return _varint_u64_encode_buf((uint64_t)val, buf);
}

size_t
varint_s64_encode_buf(int64_t val, uint8_t *buf)
{
	return _varint_u64_encode_buf(zigzag64(val), buf);
}

size_t
varint_s32_encode_buf(int32_t val, uint8_t *buf)
{
	return _varint_u64_encode_buf((uint64_t)zigzag32(val), buf);
}

/* Read from signed 64bit varint */
int64_t 
varint_s64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size)
{	
	return unzigzag64(varint_u64_decode(the_start, the_end, size));
}

/* Read from unsigned 64bit varint */
uint64_t 
varint_u64_decode(const uint8_t *the_start, const uint8_t *the_end, size_t *size)
{
	uint64_t nVal = 0;
	int nShift = 0;
	uint8_t nByte;
	const uint8_t *ptr = the_start;

	/* Check so we don't read beyond the twkb */
	while( ptr < the_end )
	{
		nByte = *ptr;
		/* Hibit is set, so this isn't the last byte */
		if (nByte & 0x80)
		{
			/* We get here when there is more to read in the input varInt */
			/* Here we take the least significant 7 bits of the read */
			/* byte and put it in the most significant place in the result variable. */
			nVal |= ((uint64_t)(nByte & 0x7f)) << nShift; 
			/* move the "cursor" of the input buffer step (8 bits) */
			ptr++; 
			/* move the cursor in the resulting variable (7 bits) */
			nShift += 7;
		}
		else
		{
			/* move the "cursor" one step */
			ptr++; 
			/* Move the last read byte to the most significant */
			/* place in the result and return the whole result */
			*size = ptr - the_start;
			return nVal | ((uint64_t)nByte << nShift);
		}
	}
	lwerror("%s: varint extends past end of buffer", __func__);
	return 0;
}

size_t 
varint_size(const uint8_t *the_start, const uint8_t *the_end)
{
	const uint8_t *ptr = the_start;

	/* Check so we don't read beyond the twkb */
	while( ptr < the_end )
	{
		/* Hibit is set, this isn't the last byte */
		if (*ptr & 0x80)
		{
			ptr++;
		}
		else
		{
			ptr++;
			return ptr - the_start;
		}
	}
	return 0;
}

uint64_t zigzag64(int64_t val)
{
	return (val << 1) ^ (val >> 63);
}

uint32_t zigzag32(int32_t val)
{
	return (val << 1) ^ (val >> 31);
}
	
uint8_t zigzag8(int8_t val)
{
	return (val << 1) ^ (val >> 7);
}
	
int64_t unzigzag64(uint64_t val)
{
        if ( val & 0x01 ) 
            return -1 * (int64_t)((val+1) >> 1);
        else
            return (int64_t)(val >> 1);
}
	
int32_t unzigzag32(uint32_t val)
{
        if ( val & 0x01 ) 
            return -1 * (int32_t)((val+1) >> 1);
        else
            return (int32_t)(val >> 1);
}
	
int8_t unzigzag8(uint8_t val)
{
        if ( val & 0x01 ) 
            return -1 * (int8_t)((val+1) >> 1);
        else
            return (int8_t)(val >> 1);
}
	

