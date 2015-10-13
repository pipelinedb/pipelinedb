/*-------------------------------------------------------------------------
 *
 * fss.h
 *	  Interface for Filtered Space Saving support
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/fss.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_FSS_H
#define PIPELINE_FSS_H

#include "c.h"
#include "postgres.h"
#include "utils/array.h"
#include "utils/typcache.h"

#define FSS_STORES_DATUMS(fss) ((fss)->top_k != NULL)

typedef struct FSSTypeInfo
{
	Oid typoid;
	int16 typlen;
	bool typbyval;
	char typalign;
} FSSTypeInfo;

typedef struct Counter
{
	uint64_t alpha;
	uint16_t count;
} Counter;

typedef struct MonitoredElement
{
	bool set;
	uint64_t frequency;
	uint32_t error;
	uint16_t counter;
	Datum value;
} MonitoredElement;

typedef struct FSS
{
	uint32	vl_len_;
	bool packed;
	uint64_t count;
	uint16_t h;
	uint16_t m;
	uint16_t k;
	FSSTypeInfo typ;
	Counter *bitmap_counter; /* length h */
	MonitoredElement *monitored_elements; /* length m */
	Datum *top_k; /* length k */
} FSS;

extern FSS *FSSCreateWithMAndH(uint64_t k, TypeCacheEntry *typ, uint64_t m, uint64_t h);
extern FSS *FSSCreate(uint64_t k, TypeCacheEntry *typ);
extern void FSSDestroy(FSS *fss);

extern FSS *FSSCopy(FSS *fss);
extern void FSSIncrement(FSS *fss, Datum datum);
extern void FSSIncrementWeighted(FSS *fss, Datum datum, uint64_t weight);
extern FSS *FSSMerge(FSS *fss, FSS *incoming);
extern Datum *FSSTopK(FSS *fss, uint16_t k, uint16_t *found);
extern uint64_t *FSSTopKCounts(FSS *fss, uint16_t k, uint16_t *found);
extern uint64_t FSSTotal(FSS *fss);
extern Size FSSSize(FSS *fss);
extern FSS *FSSCompress(FSS *fss);
extern void FSSPrint(FSS *fss);

#endif
