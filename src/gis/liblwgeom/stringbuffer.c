/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2002 Thamer Alharbash
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
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
#include "stringbuffer.h"

/**
* Allocate a new stringbuffer_t. Use stringbuffer_destroy to free.
*/
stringbuffer_t* 
stringbuffer_create(void)
{
	return stringbuffer_create_with_size(STRINGBUFFER_STARTSIZE);
}

/**
* Allocate a new stringbuffer_t. Use stringbuffer_destroy to free.
*/
stringbuffer_t* 
stringbuffer_create_with_size(size_t size)
{
	stringbuffer_t *s;

	s = lwalloc(sizeof(stringbuffer_t));
	s->str_start = lwalloc(size);
	s->str_end = s->str_start;
	s->capacity = size;
	memset(s->str_start,0,size);
	return s;
}

/**
* Free the stringbuffer_t and all memory managed within it.
*/
void 
stringbuffer_destroy(stringbuffer_t *s)
{
	if ( s->str_start ) lwfree(s->str_start);
	if ( s ) lwfree(s);
}

/**
* Reset the stringbuffer_t. Useful for starting a fresh string
* without the expense of freeing and re-allocating a new
* stringbuffer_t.
*/
void 
stringbuffer_clear(stringbuffer_t *s)
{
	s->str_start[0] = '\0';
	s->str_end = s->str_start;
}

/**
* If necessary, expand the stringbuffer_t internal buffer to accomodate the
* specified additional size.
*/
static inline void 
stringbuffer_makeroom(stringbuffer_t *s, size_t size_to_add)
{
	size_t current_size = (s->str_end - s->str_start);
	size_t capacity = s->capacity;
	size_t required_size = current_size + size_to_add;

	while (capacity < required_size)
		capacity *= 2;

	if ( capacity > s->capacity )
	{
		s->str_start = lwrealloc(s->str_start, capacity);
		s->capacity = capacity;
		s->str_end = s->str_start + current_size;
	}
}

/**
* Return the last character in the buffer.
*/
char 
stringbuffer_lastchar(stringbuffer_t *s)
{
	if( s->str_end == s->str_start ) 
		return 0;
	
	return *(s->str_end - 1);
}

/**
* Append the specified string to the stringbuffer_t.
*/
void 
stringbuffer_append(stringbuffer_t *s, const char *a)
{
	int alen = strlen(a); /* Length of string to append */
	int alen0 = alen + 1; /* Length including null terminator */
	stringbuffer_makeroom(s, alen0);
	memcpy(s->str_end, a, alen0);
	s->str_end += alen;
}

/**
* Returns a reference to the internal string being managed by
* the stringbuffer. The current string will be null-terminated
* within the internal string.
*/
const char* 
stringbuffer_getstring(stringbuffer_t *s)
{
	return s->str_start;
}

/**
* Returns a newly allocated string large enough to contain the
* current state of the string. Caller is responsible for
* freeing the return value.
*/
char* 
stringbuffer_getstringcopy(stringbuffer_t *s)
{
	size_t size = (s->str_end - s->str_start) + 1;
	char *str = lwalloc(size);
	memcpy(str, s->str_start, size);
	str[size - 1] = '\0';
	return str;
}

/**
* Returns the length of the current string, not including the
* null terminator (same behavior as strlen()).
*/
int 
stringbuffer_getlength(stringbuffer_t *s)
{
	return (s->str_end - s->str_start);
}

/**
* Clear the stringbuffer_t and re-start it with the specified string.
*/
void 
stringbuffer_set(stringbuffer_t *s, const char *str)
{
	stringbuffer_clear(s);
	stringbuffer_append(s, str);
}

/**
* Copy the contents of src into dst.
*/
void 
stringbuffer_copy(stringbuffer_t *dst, stringbuffer_t *src)
{
	stringbuffer_set(dst, stringbuffer_getstring(src));
}

/**
* Appends a formatted string to the current string buffer,
* using the format and argument list provided. Returns -1 on error,
* check errno for reasons, documented in the printf man page.
*/
static int 
stringbuffer_avprintf(stringbuffer_t *s, const char *fmt, va_list ap)
{
	int maxlen = (s->capacity - (s->str_end - s->str_start));
	int len = 0; /* Length of the output */
	va_list ap2;

	/* Make a copy of the variadic arguments, in case we need to print twice */
	/* Print to our buffer */
	va_copy(ap2, ap);
	len = vsnprintf(s->str_end, maxlen, fmt, ap2);
	va_end(ap2);

	/* Propogate errors up */
	if ( len < 0 ) 
		#if defined(__MINGW64_VERSION_MAJOR)
		len = _vscprintf(fmt, ap2);/**Assume windows flaky vsnprintf that returns -1 if initial buffer to small and add more space **/
		#else
		return len;
		#endif

	/* We didn't have enough space! */
	/* Either Unix vsnprint returned write length larger than our buffer */
	/*     or Windows vsnprintf returned an error code. */
	if ( len >= maxlen )
	{
		stringbuffer_makeroom(s, len + 1);
		maxlen = (s->capacity - (s->str_end - s->str_start));

		/* Try to print a second time */
		len = vsnprintf(s->str_end, maxlen, fmt, ap);

		/* Printing error? Error! */
		if ( len < 0 ) return len;
		/* Too long still? Error! */
		if ( len >= maxlen ) return -1;
	}

	/* Move end pointer forward and return. */
	s->str_end += len;
	return len;
}

/**
* Appends a formatted string to the current string buffer,
* using the format and argument list provided.
* Returns -1 on error, check errno for reasons,
* as documented in the printf man page.
*/
int 
stringbuffer_aprintf(stringbuffer_t *s, const char *fmt, ...)
{
	int r;
	va_list ap;
	va_start(ap, fmt);
	r = stringbuffer_avprintf(s, fmt, ap);
	va_end(ap);
	return r;
}

/**
* Trims whitespace off the end of the stringbuffer. Returns
* the number of characters trimmed.
*/
int 
stringbuffer_trim_trailing_white(stringbuffer_t *s)
{
	char *ptr = s->str_end;
	int dist = 0;
	
	/* Roll backwards until we hit a non-space. */
	while( ptr > s->str_start )
	{	
		ptr--;
		if( (*ptr == ' ') || (*ptr == '\t') )
		{
			continue;
		}
		else
		{
			ptr++;
			dist = s->str_end - ptr;
			*ptr = '\0';
			s->str_end = ptr;
			return dist;
		}
	}
	return dist;	
}

/**
* Trims zeroes off the end of the last number in the stringbuffer.
* The number has to be the very last thing in the buffer. Only the
* last number will be trimmed. Returns the number of characters
* trimmed.
* 
* eg: 1.22000 -> 1.22
*     1.0 -> 1
*     0.0 -> 0
*/
int 
stringbuffer_trim_trailing_zeroes(stringbuffer_t *s)
{
	char *ptr = s->str_end;
	char *decimal_ptr = NULL;
	int dist;
	
	if ( s->str_end - s->str_start < 2) 
		return 0;

	/* Roll backwards to find the decimal for this number */
	while( ptr > s->str_start )
	{	
		ptr--;
		if ( *ptr == '.' )
		{
			decimal_ptr = ptr;
			break;
		}
		if ( (*ptr >= '0') && (*ptr <= '9' ) )
			continue;
		else
			break;
	}

	/* No decimal? Nothing to trim! */
	if ( ! decimal_ptr )
		return 0;
	
	ptr = s->str_end;
	
	/* Roll backwards again, with the decimal as stop point, trimming contiguous zeroes */
	while( ptr >= decimal_ptr )
	{
		ptr--;
		if ( *ptr == '0' )
			continue;
		else
			break;
	}
	
	/* Huh, we get anywhere. Must not have trimmed anything. */
	if ( ptr == s->str_end )
		return 0;

	/* If we stopped at the decimal, we want to null that out. 
	   It we stopped on a numeral, we want to preserve that, so push the 
	   pointer forward one space. */
	if ( *ptr != '.' )
		ptr++;

	/* Add null terminator re-set the end of the stringbuffer. */
	*ptr = '\0';
	dist = s->str_end - ptr;
	s->str_end = ptr;
	return dist;
}

