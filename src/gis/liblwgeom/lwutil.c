#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h> /* for tolower */

/* Global variables */
#include "../postgis_config.h"
#include "liblwgeom_internal.h"
#include "lwgeom_log.h"

/* Default allocators */
static void * default_allocator(size_t size);
static void default_freeor(void *mem);
static void * default_reallocator(void *mem, size_t size);
lwallocator lwalloc_var = default_allocator;
lwreallocator lwrealloc_var = default_reallocator;
lwfreeor lwfree_var = default_freeor;

/* Default reporters */
static void default_noticereporter(const char *fmt, va_list ap);
static void default_errorreporter(const char *fmt, va_list ap);
lwreporter lwnotice_var = default_noticereporter;
lwreporter lwerror_var = default_errorreporter;

#define LW_MSG_MAXLEN 256

static char *lwgeomTypeName[] =
{
	"Unknown",
	"Point",
	"LineString",
	"Polygon",
	"MultiPoint",
	"MultiLineString",
	"MultiPolygon",
	"GeometryCollection",
	"CircularString",
	"CompoundCurve",
	"CurvePolygon",
	"MultiCurve",
	"MultiSurface",
	"PolyhedralSurface",
	"Triangle",
	"Tin"
};

/*
 * Default lwnotice/lwerror handlers
 *
 * Since variadic functions cannot pass their parameters directly, we need
 * wrappers for these functions to convert the arguments into a va_list
 * structure.
 */

void
lwnotice(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	/* Call the supplied function */
	(*lwnotice_var)(fmt, ap);

	va_end(ap);
}

void
lwerror(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	/* Call the supplied function */
	(*lwerror_var)(fmt, ap);

	va_end(ap);
}

/*
 * Default allocators
 *
 * We include some default allocators that use malloc/free/realloc
 * along with stdout/stderr since this is the most common use case
 *
 */

static void *
default_allocator(size_t size)
{
	void *mem = malloc(size);
	return mem;
}

static void
default_freeor(void *mem)
{
	free(mem);
}

static void *
default_reallocator(void *mem, size_t size)
{
	void *ret = realloc(mem, size);
	return ret;
}

static void
default_noticereporter(const char *fmt, va_list ap)
{
	char msg[LW_MSG_MAXLEN+1];
	vsnprintf (msg, LW_MSG_MAXLEN, fmt, ap);
	msg[LW_MSG_MAXLEN]='\0';
	printf("%s\n", msg);
}

static void
default_errorreporter(const char *fmt, va_list ap)
{
	char msg[LW_MSG_MAXLEN+1];
	vsnprintf (msg, LW_MSG_MAXLEN, fmt, ap);
	msg[LW_MSG_MAXLEN]='\0';
	fprintf(stderr, "%s\n", msg);
	exit(1);
}

/**
 * This function is called by programs which want to set up custom handling 
 * for memory management and error reporting
 *
 * Only non-NULL values change their respective handler
 */
void
lwgeom_set_handlers(lwallocator allocator, lwreallocator reallocator,
	        lwfreeor freeor, lwreporter errorreporter,
	        lwreporter noticereporter) {

	if ( allocator ) lwalloc_var = allocator;
	if ( reallocator ) lwrealloc_var = reallocator;
	if ( freeor ) lwfree_var = freeor;

	if ( errorreporter ) lwerror_var = errorreporter;
	if ( noticereporter ) lwnotice_var = noticereporter;
}

const char* 
lwtype_name(uint8_t type)
{
	if ( type > 15 )
	{
		/* assert(0); */
		return "Invalid type";
	}
	return lwgeomTypeName[(int ) type];
}

void *
lwalloc(size_t size)
{
	void *mem = lwalloc_var(size);
	LWDEBUGF(5, "lwalloc: %d@%p", size, mem);
	return mem;
}

void *
lwrealloc(void *mem, size_t size)
{
	LWDEBUGF(5, "lwrealloc: %d@%p", size, mem);
	return lwrealloc_var(mem, size);
}

void
lwfree(void *mem)
{
	lwfree_var(mem);
}

/*
 * Removes trailing zeros and dot for a %f formatted number.
 * Modifies input.
 */
void
trim_trailing_zeros(char *str)
{
	char *ptr, *totrim=NULL;
	int len;
	int i;

	LWDEBUGF(3, "input: %s", str);

	ptr = strchr(str, '.');
	if ( ! ptr ) return; /* no dot, no decimal digits */

	LWDEBUGF(3, "ptr: %s", ptr);

	len = strlen(ptr);
	for (i=len-1; i; i--)
	{
		if ( ptr[i] != '0' ) break;
		totrim=&ptr[i];
	}
	if ( totrim )
	{
		if ( ptr == totrim-1 ) *ptr = '\0';
		else *totrim = '\0';
	}

	LWDEBUGF(3, "output: %s", str);
}

/*
 * Returns a new string which contains a maximum of maxlength characters starting
 * from startpos and finishing at endpos (0-based indexing). If the string is
 * truncated then the first or last characters are replaced by "..." as
 * appropriate.
 *
 * The caller should specify start or end truncation by setting the truncdirection
 * parameter as follows:
 *    0 - start truncation (i.e. characters are removed from the beginning)
 *    1 - end trunctation (i.e. characters are removed from the end)
 */

char *lwmessage_truncate(char *str, int startpos, int endpos, int maxlength, int truncdirection)
{
	char *output;
	char *outstart;

	/* Allocate space for new string */
	output = lwalloc(maxlength + 4);
	output[0] = '\0';

	/* Start truncation */
	if (truncdirection == 0)
	{
		/* Calculate the start position */
		if (endpos - startpos < maxlength)
		{
			outstart = str + startpos;
			strncat(output, outstart, endpos - startpos + 1);
		}
		else
		{
			if (maxlength >= 3)
			{
				/* Add "..." prefix */
				outstart = str + endpos + 1 - maxlength + 3;
				strncat(output, "...", 3);
				strncat(output, outstart, maxlength - 3);
			}
			else
			{
				/* maxlength is too small; just output "..." */
				strncat(output, "...", 3);
			}
		}
	}

	/* End truncation */
	if (truncdirection == 1)
	{
		/* Calculate the end position */
		if (endpos - startpos < maxlength)
		{
			outstart = str + startpos;
			strncat(output, outstart, endpos - startpos + 1);
		}
		else
		{
			if (maxlength >= 3)
			{
				/* Add "..." suffix */
				outstart = str + startpos;
				strncat(output, outstart, maxlength - 3);
				strncat(output, "...", 3);
			}
			else
			{
				/* maxlength is too small; just output "..." */
				strncat(output, "...", 3);
			}
		}
	}

	return output;
}


char
getMachineEndian(void)
{
	static int endian_check_int = 1; /* dont modify this!!! */

	return *((char *) &endian_check_int); /* 0 = big endian | xdr,
	                                       * 1 = little endian | ndr
	                                       */
}


void
error_if_srid_mismatch(int srid1, int srid2)
{
	if ( srid1 != srid2 )
	{
		lwerror("Operation on mixed SRID geometries");
	}
}

int
clamp_srid(int srid)
{
	int newsrid = srid;

	if ( newsrid <= 0 ) {
		if ( newsrid != SRID_UNKNOWN ) {
			newsrid = SRID_UNKNOWN;
			lwnotice("SRID value %d converted to the officially unknown SRID value %d", srid, newsrid);
		}
	} else if ( srid > SRID_MAXIMUM ) {
    newsrid = SRID_USER_MAXIMUM + 1 +
      /* -1 is to reduce likelyhood of clashes */
      /* NOTE: must match implementation in postgis_restore.pl */
      ( srid % ( SRID_MAXIMUM - SRID_USER_MAXIMUM - 1 ) );
		lwnotice("SRID value %d > SRID_MAXIMUM converted to %d", srid, newsrid);
	}
	
	return newsrid;
}

