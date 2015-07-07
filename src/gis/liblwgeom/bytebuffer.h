/**********************************************************************
 * $Id: bytebuffer.h 12198 2014-01-29 17:49:35Z pramsey $
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

#ifndef _BYTEBUFFER_H
#define _BYTEBUFFER_H 1

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include "varint.h"

#include "lwgeom_log.h"
#define BYTEBUFFER_STARTSIZE 128

typedef struct
{
	size_t capacity;
	uint8_t *buf_start;
	uint8_t *writecursor;	
	uint8_t *readcursor;	
}
bytebuffer_t;

bytebuffer_t *bytebuffer_create_with_size(size_t size);
bytebuffer_t *bytebuffer_create(void);
void bytebuffer_destroy(bytebuffer_t *s);
void bytebuffer_clear(bytebuffer_t *s);
void bytebuffer_append_byte(bytebuffer_t *s, const uint8_t val);
void bytebuffer_append_varint(bytebuffer_t *s, const int64_t val);
void bytebuffer_append_uvarint(bytebuffer_t *s, const uint64_t val);
uint64_t bytebuffer_read_uvarint(bytebuffer_t *s);
int64_t bytebuffer_read_varint(bytebuffer_t *s);
size_t bytebuffer_getlength(bytebuffer_t *s);
bytebuffer_t* bytebuffer_merge(bytebuffer_t **buff_array, int nbuffers);
void bytebuffer_reset_reading(bytebuffer_t *s);

void bytebuffer_append_bytebuffer(bytebuffer_t *write_to,bytebuffer_t *write_from);
void bytebuffer_append_bulk(bytebuffer_t *s, void * start, size_t size);
void bytebuffer_append_int(bytebuffer_t *buf, const int val, int swap);
void bytebuffer_append_double(bytebuffer_t *buf, const double val, int swap);
#endif /* _BYTEBUFFER_H */
