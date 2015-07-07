/**********************************************************************
 * $Id: bytebuffer.c 11218 2013-03-28 13:32:44Z robe $
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2015 Nicklas Avén <nicklas.aven@jordogskog.no>
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided
 * with the distribution.
 *
 * The name of the author may not be used to endorse or promote
 * products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 **********************************************************************/


#include "liblwgeom_internal.h"
#include "bytebuffer.h"

/**
* Allocate a new bytebuffer_t. Use bytebuffer_destroy to free.
*/
bytebuffer_t* 
bytebuffer_create(void)
{
	LWDEBUG(2,"Entered bytebuffer_create");
	return bytebuffer_create_with_size(BYTEBUFFER_STARTSIZE);
}

/**
* Allocate a new bytebuffer_t. Use bytebuffer_destroy to free.
*/
bytebuffer_t* 
bytebuffer_create_with_size(size_t size)
{
	LWDEBUGF(2,"Entered bytebuffer_create_with_size %d", size);
	bytebuffer_t *s;

	s = lwalloc(sizeof(bytebuffer_t));
	s->buf_start = lwalloc(size);
	s->readcursor = s->writecursor = s->buf_start;
	s->capacity = size;
	memset(s->buf_start,0,size);
	LWDEBUGF(4,"We create a buffer on %p of %d bytes", s->buf_start, size);
	return s;
}

/**
* Free the bytebuffer_t and all memory managed within it.
*/
void 
bytebuffer_destroy(bytebuffer_t *s)
{
	LWDEBUG(2,"Entered bytebuffer_destroy");
	LWDEBUGF(4,"The buffer has used %d bytes",bytebuffer_getlength(s));
	
	if ( s->buf_start ) 
	{
		LWDEBUGF(4,"let's free buf_start %p",s->buf_start);
		lwfree(s->buf_start);
		LWDEBUG(4,"buf_start is freed");
	}
	if ( s ) 
	{
		lwfree(s);		
		LWDEBUG(4,"bytebuffer_t is freed");
	}
	return;
}

/**
* Set the read cursor to the beginning
*/
void 
bytebuffer_reset_reading(bytebuffer_t *s)
{
	s->readcursor = s->buf_start;
}

/**
* Reset the bytebuffer_t. Useful for starting a fresh string
* without the expense of freeing and re-allocating a new
* bytebuffer_t.
*/
void 
bytebuffer_clear(bytebuffer_t *s)
{
	s->readcursor = s->writecursor = s->buf_start;
}

/**
* If necessary, expand the bytebuffer_t internal buffer to accomodate the
* specified additional size.
*/
static inline void 
bytebuffer_makeroom(bytebuffer_t *s, size_t size_to_add)
{
	LWDEBUGF(2,"Entered bytebuffer_makeroom with space need of %d", size_to_add);
	size_t current_write_size = (s->writecursor - s->buf_start);
	size_t capacity = s->capacity;
	size_t required_size = current_write_size + size_to_add;

	LWDEBUGF(2,"capacity = %d and required size = %d",capacity ,required_size);
	while (capacity < required_size)
		capacity *= 2;

	if ( capacity > s->capacity )
	{
		LWDEBUGF(4,"We need to realloc more memory. New capacity is %d", capacity);
		s->buf_start = lwrealloc(s->buf_start, capacity);
		s->capacity = capacity;
		s->writecursor = s->buf_start + current_write_size;
		s->readcursor = s->buf_start + (s->readcursor - s->buf_start);
	}
	return;
}

/**
* Writes a uint8_t value to the buffer
*/
void 
bytebuffer_append_byte(bytebuffer_t *s, const uint8_t val)
{	
	LWDEBUGF(2,"Entered bytebuffer_append_byte with value %d", val);	
	bytebuffer_makeroom(s, 1);
	*(s->writecursor)=val;
	s->writecursor += 1;
	return;
}


/**
* Writes a uint8_t value to the buffer
*/
void 
bytebuffer_append_bulk(bytebuffer_t *s, void * start, size_t size)
{	
	LWDEBUGF(2,"bytebuffer_append_bulk with size %d",size);	
	bytebuffer_makeroom(s, size);
	memcpy(s->writecursor, start, size);
	s->writecursor += size;
	return;
}

/**
* Writes a uint8_t value to the buffer
*/
void 
bytebuffer_append_bytebuffer(bytebuffer_t *write_to,bytebuffer_t *write_from )
{	
	LWDEBUG(2,"bytebuffer_append_bytebuffer");	
	size_t size = bytebuffer_getlength(write_from);
	bytebuffer_makeroom(write_to, size);
	memcpy(write_to->writecursor, write_from->buf_start, size);
	write_to->writecursor += size;
	return;
}


/**
* Writes a signed varInt to the buffer
*/
void 
bytebuffer_append_varint(bytebuffer_t *b, const int64_t val)
{	
	size_t size;
	bytebuffer_makeroom(b, 8);
	size = varint_s64_encode_buf(val, b->writecursor);
	b->writecursor += size;
	return;
}

/**
* Writes a unsigned varInt to the buffer
*/
void 
bytebuffer_append_uvarint(bytebuffer_t *b, const uint64_t val)
{	
	size_t size;
	bytebuffer_makeroom(b, 8);
	size = varint_u64_encode_buf(val, b->writecursor);
	b->writecursor += size;
	return;
}


/*
* Writes Integer to the buffer
*/
void
bytebuffer_append_int(bytebuffer_t *buf, const int val, int swap)
{
	LWDEBUGF(2,"Entered bytebuffer_append_int with value %d, swap = %d", val, swap);	
	
	LWDEBUGF(4,"buf_start = %p and write_cursor=%p", buf->buf_start,buf->writecursor);
	char *iptr = (char*)(&val);
	int i = 0;

	if ( sizeof(int) != WKB_INT_SIZE )
	{
		lwerror("Machine int size is not %d bytes!", WKB_INT_SIZE);
	}
	
	bytebuffer_makeroom(buf, WKB_INT_SIZE);
	/* Machine/request arch mismatch, so flip byte order */
	if ( swap)
	{
		LWDEBUG(4,"Ok, let's do the swaping thing");	
		for ( i = 0; i < WKB_INT_SIZE; i++ )
		{
			*(buf->writecursor) = iptr[WKB_INT_SIZE - 1 - i];
			buf->writecursor += 1;
		}
	}
	/* If machine arch and requested arch match, don't flip byte order */
	else
	{
		LWDEBUG(4,"Ok, let's do the memcopying thing");		
		memcpy(buf->writecursor, iptr, WKB_INT_SIZE);
		buf->writecursor += WKB_INT_SIZE;
	}
	
	LWDEBUGF(4,"buf_start = %p and write_cursor=%p", buf->buf_start,buf->writecursor);
	return;

}





/**
* Writes a float64 to the buffer
*/
void
bytebuffer_append_double(bytebuffer_t *buf, const double val, int swap)
{
	LWDEBUGF(2,"Entered bytebuffer_append_double with value %lf swap = %d", val, swap);	
	
	LWDEBUGF(4,"buf_start = %p and write_cursor=%p", buf->buf_start,buf->writecursor);
	char *dptr = (char*)(&val);
	int i = 0;

	if ( sizeof(double) != WKB_DOUBLE_SIZE )
	{
		lwerror("Machine double size is not %d bytes!", WKB_DOUBLE_SIZE);
	}

	bytebuffer_makeroom(buf, WKB_DOUBLE_SIZE);
	
	/* Machine/request arch mismatch, so flip byte order */
	if ( swap )
	{
		LWDEBUG(4,"Ok, let's do the swapping thing");		
		for ( i = 0; i < WKB_DOUBLE_SIZE; i++ )
		{
			*(buf->writecursor) = dptr[WKB_DOUBLE_SIZE - 1 - i];
			buf->writecursor += 1;
		}
	}
	/* If machine arch and requested arch match, don't flip byte order */
	else
	{
		LWDEBUG(4,"Ok, let's do the memcopying thing");			
		memcpy(buf->writecursor, dptr, WKB_DOUBLE_SIZE);
		buf->writecursor += WKB_DOUBLE_SIZE;
	}
	
	LWDEBUG(4,"Return from bytebuffer_append_double");		
	return;

}

/**
* Reads a signed varInt from the buffer
*/
int64_t 
bytebuffer_read_varint(bytebuffer_t *b)
{
	size_t size;
	int64_t val = varint_s64_decode(b->readcursor, b->buf_start + b->capacity, &size);
	b->readcursor += size;
	return val;
}

/**
* Reads a unsigned varInt from the buffer
*/
uint64_t 
bytebuffer_read_uvarint(bytebuffer_t *b)
{	
	size_t size;
	uint64_t val = varint_u64_decode(b->readcursor, b->buf_start + b->capacity, &size);
	b->readcursor += size;
	return val;
}

/**
* Returns the length of the current buffer
*/
size_t 
bytebuffer_getlength(bytebuffer_t *s)
{
	return (size_t) (s->writecursor - s->buf_start);
}


/**
* Returns a new bytebuffer were both ingoing bytebuffers is merged.
* Caller is responsible for freeing both incoming bytefyffers and resulting bytebuffer
*/
bytebuffer_t*
bytebuffer_merge(bytebuffer_t **buff_array, int nbuffers)
{
	size_t total_size = 0, current_size, acc_size = 0;
	int i;
	for ( i = 0; i < nbuffers; i++ )
	{
		total_size += bytebuffer_getlength(buff_array[i]);
	}
		
	bytebuffer_t *res = bytebuffer_create_with_size(total_size);
	for ( i = 0; i < nbuffers; i++)
	{
		current_size = bytebuffer_getlength(buff_array[i]);
		memcpy(res->buf_start+acc_size, buff_array[i]->buf_start, current_size);
		acc_size += current_size;
	}
	res->writecursor = res->buf_start + total_size;
	res->readcursor = res->buf_start;
	return res;
}


