/*-------------------------------------------------------------------------
 *
 * fss.h
 *	  Interface for Filtered Space Saving support
 *
 * Copyright (c) 2018, PipelineDB, Inc.
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

#define ELEMENT_SET 	0x01
#define ELEMENT_NEW 	0x02
#define ELEMENT_NULL 0x04

#define IS_SET(el) ((el)->flags & ELEMENT_SET)
#define IS_NEW(el) ((el)->flags & ELEMENT_NEW)
#define IS_NULL(el) ((el)->flags & ELEMENT_NULL)
#define SET_NEW(el) ((el)->flags |= (ELEMENT_SET | ELEMENT_NEW))
#define UNSET_NEW(el) ((el)->flags = ELEMENT_SET)
#define SET(el) ((el)->flags |= ELEMENT_SET)
#define SET_NULL(el) ((el)-> flags |= ELEMENT_NULL)

typedef struct FSSTypeInfo
{
	Oid typoid;
	int16 typlen;
	bool typbyval;
	char typalign;
	char typtype;
} FSSTypeInfo;

typedef struct Counter
{
	uint64_t alpha;
	uint16_t count;
} Counter;

typedef struct MonitoredElement
{
	char flags;
	uint64_t frequency;
	uint32_t error;
	uint16_t counter;
	uint16_t varlen_index;
	Datum value;
} MonitoredElement;

typedef struct FSS
{
	uint32	vl_len_;
	uint64_t count;
	uint16_t h;
	uint16_t m;
	uint16_t k;
	FSSTypeInfo typ;
	Counter *bitmap_counter; /* length h */
	MonitoredElement *monitored_elements; /* length m */
	ArrayType *top_k; /* length k */
} FSS;

extern FSS *FSSFromBytes(struct varlena *bytes);
extern FSS *FSSCreateWithMAndH(uint16_t k, TypeCacheEntry *typ, uint16_t m, uint16_t h);
extern FSS *FSSCreate(uint64_t k, TypeCacheEntry *typ);
extern void FSSDestroy(FSS *fss);

extern uint64_t HashDatum(FSS* fss, Datum d);
extern int MonitoredElementComparator(const void *a, const void *b);

extern FSS *FSSCopy(FSS *fss);
extern FSS *FSSIncrement(FSS *fss, Datum datum, bool isnull);
extern FSS *FSSIncrementWeighted(FSS *fss, Datum datum, bool isnull, uint64_t weight);
extern FSS *FSSMerge(FSS *fss, FSS *incoming);
extern int FSSMonitoredLength(FSS *fss);
extern Datum *FSSTopK(FSS *fss, uint16_t k, bool **nulls, uint16_t *found);
extern uint64_t *FSSTopKCounts(FSS *fss, uint16_t k, uint16_t *found);
extern uint64_t FSSTotal(FSS *fss);
extern Size FSSSize(FSS *fss);
extern FSS *FSSCompress(FSS *fss);
extern void FSSPrint(FSS *fss);

#endif
