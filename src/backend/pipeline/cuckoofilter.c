/*-------------------------------------------------------------------------
 *
 * cuckoofilter.c
 *	  Cuckoo Filter implementation based on:
 *      https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
 *
 * src/backend/pipeline/cuckoofilter.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "pipeline/cuckoofilter.h"
#include "pipeline/miscutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#define MAX_CUCKOO_COUNT 500
#define HIGH_BITS(x) ((int32_t) (x >> 32))
#define LOW_BITS(x) ((int32_t) (x & 0xFFFFFFFF))
#define FINGERPRINTS_PER_BUCKET 4
#define DEFAULT_P 0.01
#define DEFAULT_N (1 << 24)
#define MAX_BITS_PER_FINGERPRINT 64

#define BITS(c, i) ((c) & ((1 << (i)) - 1))
#define BITS_REVERSE(c, i) (((c) & ((1 << (8 - (i))) - 1)) >> (8 - (i)))
#define BIT_IN_ARRAY(t, i, s) (((i) * (t)->bits_per_fingerprint * FINGERPRINTS_PER_BUCKET) + ((s) * (t)->bits_per_fingerprint))

/*
 * TODO(usmanm): Use semi-sorting buckets to save 1 bit per fingerprint? Might not be that
 * useful because we're not too worried about memory usage, rather we care about
 * having a super compact on-disk representation. For now we use a sparse representation
 * which consumes 2 extra bits per bucket, but never has a write an empty fingerprint
 * entry.
 */

FingerprintTable *
FingerprintTableCreate(int32_t num_buckets, int16_t bits_per_fingerprint)
{
	FingerprintTable *t = palloc(sizeof(FingerprintTable));
	t->num_buckets = num_buckets;
	t->bits_per_fingerprint = Min(bits_per_fingerprint, MAX_BITS_PER_FINGERPRINT);
	t->bytea = palloc0(num_buckets * bits_per_fingerprint * FINGERPRINTS_PER_BUCKET);
	return t;
}

static void
get_fingerprint(FingerprintTable *t, int32_t index, int slot)
{
	int64_t start = BIT_IN_ARRAY(t, index, slot);
	int64_t end = start + t->bits_per_fingerprint;
	int64_t fingerprint = 0;

	while (start < end)
	{
		int i = start / 8;
		char c = t->bytea[i];
		int8_t partial = start % 8;

		if (partial)
		{
			fingerprint |= BITS_REVERSE(c, partial);
			start += partial;
		}

		if (start + 8 >= end)
		{
			partial = start + 8 - end;
			fingerprint <<= partial;
			fingerprint |= BITS(c, partial);
			start += partial;
			break;
		}
		else
		{
			fingerprint <<= 8;
			fingerprint |= c;
			start += 8;
		}
	}
}

static void
set_fingerprint(FingerprintTable *t, int32_t index, int slot, int32_t fingerprint)
{

}


bool
FingerprintTableInsert(FingerprintTable *t, int32_t index, int32_t fingerprint, int32_t *old_fingerprint)
{
	return false;
}

bool
FingerprintTableRemove(FingerprintTable *t, int32_t index, int32_t fingerprint)
{
	return false;
}

bool
FingerprintTableContains(FingerprintTable *t, int32_t index, int32_t fingerprint)
{
	return false;
}

int64_t
FingerprintTableMaxFingerprints(FingerprintTable *t)
{
	return t->num_buckets * FINGERPRINTS_PER_BUCKET;
}

Size
FingerprintTableSize(FingerprintTable *t)
{
	return sizeof(FingerprintTable);
}

CuckooFilter *
CuckooFilterCreateWithPAndN(float8 p, int64_t n)
{
	CuckooFilter *cf = palloc(sizeof(CuckooFilter));
	cf->num_items = 0;
	cf->table = FingerprintTableCreate(0, 0);
	return cf;
}

CuckooFilter *
CuckooFilterCreate(void)
{
	return CuckooFilterCreateWithPAndN(DEFAULT_P, DEFAULT_N);
}

static int32_t
index_hash(CuckooFilter *cf, uint32_t hash)
{
	return hash % cf->table->num_buckets;
}

static int32_t
fingerprint_hash(CuckooFilter *cf, uint32_t hash)
{
	int32_t fingerprint;
	fingerprint = hash & ((1 << cf->table->bits_per_fingerprint) - 1);
	fingerprint += (fingerprint == 0);
	return fingerprint;
}

static int32_t
alt_index(CuckooFilter *cf, int32_t index, int32_t fingerprint)
{
	return index_hash(cf,  (index ^ LOW_BITS(MurmurHash64A(&fingerprint, sizeof(int32_t)))));
}

bool
CuckooFilterAdd(CuckooFilter *cf, int64_t key)
{
	int32_t fingerprint = fingerprint_hash(cf, LOW_BITS(key));
	int32_t index = index_hash(cf, HIGH_BITS(key));
	int i;
	int32_t old_fingerprint;

	for (i = 0; i < MAX_CUCKOO_COUNT; i++)
	{
		old_fingerprint = 0;
		FingerprintTableInsert(cf->table, index, fingerprint, &old_fingerprint);
		if (!old_fingerprint)
			break;
		fingerprint = old_fingerprint;
		index = alt_index(cf, index, fingerprint);
	}

	return !old_fingerprint;
}

bool
CuckooFilterRemove(CuckooFilter *cf, int64_t key)
{
	int32_t fingerprint = fingerprint_hash(cf, LOW_BITS(key));
	int32_t index = index_hash(cf, HIGH_BITS(key));
	bool removed = FingerprintTableRemove(cf->table, index, fingerprint);

	if (!removed)
	{
		index = alt_index(cf, index, fingerprint);
		removed = FingerprintTableRemove(cf->table, index, fingerprint);
	}

	if (removed)
		cf->num_items++;

	return removed;
}

bool
CuckooFilterContains(CuckooFilter *cf, int64_t key)
{
	int32_t fingerprint = fingerprint_hash(cf, LOW_BITS(key));
	int32_t index = index_hash(cf, HIGH_BITS(key));

	if (FingerprintTableContains(cf->table, index, fingerprint))
		return true;

	index = alt_index(cf, index, fingerprint);

	return FingerprintTableContains(cf->table, index, fingerprint);
}

Size
CuckooFilterSize(CuckooFilter *cf)
{
	return sizeof(CuckooFilter) + FingerprintTableSize(cf->table);
}

int64_t
CuckooFilterCount(CuckooFilter *cf)
{
	return cf->num_items;
}

float8
CuckooFilterLoadFactor(CuckooFilter *cf)
{
	return 1.0 * CuckooFilterCount(cf) / FingerprintTableMaxFingerprints(cf->table);
}

float8
CuckooFilterBitsPerItem(CuckooFilter *cf)
{
	return 8.0 * FingerprintTableSize(cf->table) / CuckooFilterCount(cf);
}
