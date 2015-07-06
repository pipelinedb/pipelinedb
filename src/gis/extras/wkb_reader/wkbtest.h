#include <math.h>
#include <float.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <endian.h>


#include <libpq-fe.h>

typedef signed int int32;               /* == 32 bits */
typedef unsigned int uint32;    /* == 32 bits */
typedef char bool;

#define TRUE 1
#define FALSE 0



// This is modified from the postgres documentation for client programs (example programs)

void decode_wkb(char *wkb, int *size);

/*
//we need to know the endian of the client machine.  This is
// taken from postgres's os.h file

#if defined(__i386) && !defined(__i386__)
#define __i386__
#endif

#if defined(__sparc) && !defined(__sparc__)
#define __sparc__
#endif

#ifndef                 BIG_ENDIAN
#define                 BIG_ENDIAN              4321
#endif
#ifndef                 LITTLE_ENDIAN
#define                 LITTLE_ENDIAN   1234
#endif


#ifndef                 BYTE_ORDER
#ifdef __sparc__
#define           BYTE_ORDER      BIG_ENDIAN
#endif
#ifdef __i386__
#define          BYTE_ORDER              LITTLE_ENDIAN
#endif
#endif
*/
