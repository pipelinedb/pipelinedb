/*-------------------------------------------------------------------------
 *
 * bloom.h
 *	  Interface for Golomb-coded Set support
 *
 * src/include/pipeline/gcs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_GCS_H
#define PIPELINE_GCS_H

#include "c.h"
#include "nodes/pg_list.h"

typedef struct GolombCodedSet
{
	uint32	vl_len_;
	float8 p;
	uint32_t n;
	uint32_t nvals;
	List *vals;
	uint32_t blen;
	uint8_t b[1];
} GolombCodedSet;

extern GolombCodedSet *GolombCodedSetCreateWithPAndN(float8 p, uint32_t n);
extern GolombCodedSet *GolombCodedSetCreate(void);
extern void GolombCodedSetDestroy(GolombCodedSet *gcs);

extern GolombCodedSet *GolombCodedSetCopy(GolombCodedSet *gcs);
extern void GolombCodedSetAdd(GolombCodedSet *gcs, void *key, Size size);
extern bool GolombCodedSetContains(GolombCodedSet *gcs, void *key, Size size);
extern GolombCodedSet *GolombCodedSetUnion(GolombCodedSet *result, GolombCodedSet *incoming);
extern GolombCodedSet *GolombCodedSetIntersection(GolombCodedSet *result, GolombCodedSet *incoming);
extern float8 GolombCodedSetFillRatio(GolombCodedSet *gcs);
extern Size GolombCodedSetSize(GolombCodedSet *gcs);
extern GolombCodedSet *GolombCodedSetCompress(GolombCodedSet *gcs);

#endif
