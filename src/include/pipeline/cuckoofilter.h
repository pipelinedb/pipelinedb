/*-------------------------------------------------------------------------
 *
 * cuckoofilter.h
 *	  Interface for Cuckoo Filter support
 *
 * src/include/pipeline/cuckoofilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_CUCKOOFILTER_H
#define PIPELINE_CUCKOOFILTER_H

#include "c.h"

typedef struct
{
	int32_t num_buckets;
	int16_t bits_per_fingerprint;
	char *bytea;
} FingerprintTable;

extern FingerprintTable *FingerprintTableCreate(int32_t num_buckets, int16_t bits_per_tag);
extern bool FingerprintTableInsert(FingerprintTable *t, int32_t index, int32_t fingerprint, int32_t *old_fingerprint);
extern bool FingerprintTableRemove(FingerprintTable *t, int32_t index, int32_t fingerprint);
extern bool FingerprintTableContains(FingerprintTable *t, int32_t index, int32_t fingerprint);
extern int64_t FingerprintTableMaxFingerprints(FingerprintTable *t);
extern Size FingerprintTableSize(FingerprintTable *t);

typedef struct
{
	int64_t num_items;
	FingerprintTable *table;
} CuckooFilter;

extern CuckooFilter *CuckooFilterCreateWithPAndN(float8 p, int64_t n);
extern CuckooFilter *CuckooFilterCreate(void);

extern bool CuckooFilterAdd(CuckooFilter *cf, int64_t key);
extern bool CuckooFilterRemove(CuckooFilter *cf, int64_t key);
extern bool CuckooFilterContains(CuckooFilter *cf, int64_t key);
extern float8 CuckooFilterLoadFactor(CuckooFilter *cf);
extern float8 CuckooFilterBitsPerItem(CuckooFilter *cf);
extern int64_t CuckooFilterCount(CuckooFilter *cf);
extern Size CuckooFilterSize(CuckooFilter *cf);

#endif
