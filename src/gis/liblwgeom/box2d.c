#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "liblwgeom_internal.h"

#ifndef EPSILON
#define EPSILON        1.0E-06
#endif
#ifndef FPeq
#define FPeq(A,B)     (fabs((A) - (B)) <= EPSILON)
#endif



GBOX *
box2d_clone(const GBOX *in)
{
	GBOX *ret = lwalloc(sizeof(GBOX));
	memcpy(ret, in, sizeof(GBOX));
	return ret;
}
