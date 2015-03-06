/*-------------------------------------------------------------------------
 *
 * gcs.c
 *	  Golomb-coded Set implementation.
 *
 * src/backend/pipeline/gcs.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "pipeline/gcs.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define DEFAULT_P 0.02
#define DEFAULT_N (2 << 17)

GolombCodedSet *
GolombCodedSetCreateWithPAndN(float8 p, uint32_t n)
{

}

GolombCodedSet *
GolombCodedSetCreate(void)
{

}

void
GolombCodedSetDestroy(GolombCodedSet *gcs)
{

}

GolombCodedSet *
GolombCodedSetCopy(GolombCodedSet *gcs)
{

}

void
GolombCodedSetAdd(GolombCodedSet *gcs, void *key, Size size)
{

}

bool
GolombCodedSetContains(GolombCodedSet *gcs, void *key, Size size)
{

}

GolombCodedSet *
GolombCodedSetUnion(GolombCodedSet *result, GolombCodedSet *incoming)
{

}

GolombCodedSet *
GolombCodedSetIntersection(GolombCodedSet *result, GolombCodedSet *incoming)
{

}

float8
GolombCodedSetFillRatio(GolombCodedSet *gcs)
{

}

Size
GolombCodedSetSize(GolombCodedSet *gcs)
{

}
