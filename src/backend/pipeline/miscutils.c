/*
 * Miscellaneous utilities
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/miscutils.c
 */
#include "postgres.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <math.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pipeline/miscutils.h"
#include "port.h"
#include "utils/datum.h"
#include "utils/typcache.h"

extern double continuous_query_proc_priority;

void
append_suffix(char *str, char *suffix, int max_len)
{
	strcpy(&str[Min(strlen(str), max_len - strlen(suffix))], suffix);
}

int
skip_token(const char *str, char* token, int start)
{
	while(start < strlen(str) - strlen(token))
	{
		if (pg_strncasecmp(token, &str[start], strlen(token)) == 0)
		{
			/* If the match isn't the tail of the string, make sure it's followed by white space. */
			if (start < strlen(str) - strlen(token) - 1)
			{
				char next = str[start + strlen(token)];
				if (next != ' ' && next != '\n' && next != '\t')
				{
					start++;
					continue;
				}
			}

			/* If the match isn't at the head of the string, make sure it's preceeded by white space. */
			if (start != 0)
			{
				char prev = str[start - 1];
				if (prev != ' ' && prev != '\n' && prev != '\t')
				{
					start++;
					continue;
				}
			}

			break; /* match found */
		}

		start++;
	}

	if (start == strlen(str) - strlen(token))
		return -1;

	return start + strlen(token);
}

char *
random_hex(int len)
{
	int i = 0;
	char *buf = palloc(len + 1);
	while (i < len) {
		sprintf(&buf[i++], "%x", rand() % 16);
	}
	return buf;
}

static inline uint64_t
rotl64 (uint64_t x, int8_t r)
{
	return (x << r) | (x >> (64 - r));
}

static inline uint64_t
getblock64(const uint64_t *p, int i)
{
	return p[i];
}

static inline
uint64_t fmix64(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccd;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53;
	k ^= k >> 33;

	return k;
}

/*
 * Murmur3 Hash Function
 * http://web.mit.edu/mmadinot/Desktop/mmadinot/MacData/afs/athena.mit.edu/software/julia_v0.2.0/julia/src/support/MurmurHash3.c
 */
void
MurmurHash3_128(const void *key, const Size len, const uint64_t seed, void *out)
{
	const uint8_t * data = (const uint8_t*)key;
	const int nblocks = len / 16;

	uint64_t h1 = seed;
	uint64_t h2 = seed;

	uint64_t c1 = 0x87c37b91114253d5;
	uint64_t c2 = 0x4cf5ad432745937f;

	//----------
	// body
	int i;
	const uint64_t * blocks = (const uint64_t *)(data);

	const uint8_t * tail;
	uint64_t k1 = 0;
	uint64_t k2 = 0;

	for(i = 0; i < nblocks; i++)
	{
		uint64_t k1 = getblock64(blocks,i*2+0);
		uint64_t k2 = getblock64(blocks,i*2+1);

		k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;

		h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

		k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

		h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
	}

	//----------
	// tail

	tail = (const uint8_t*)(data + nblocks*16);

	switch(len & 15)
	{
	case 15: k2 ^= ((uint64_t)(tail[14])) << 48;
	case 14: k2 ^= ((uint64_t)(tail[13])) << 40;
	case 13: k2 ^= ((uint64_t)(tail[12])) << 32;
	case 12: k2 ^= ((uint64_t)(tail[11])) << 24;
	case 11: k2 ^= ((uint64_t)(tail[10])) << 16;
	case 10: k2 ^= ((uint64_t)(tail[ 9])) << 8;
	case  9: k2 ^= ((uint64_t)(tail[ 8])) << 0;
	k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

	case  8: k1 ^= ((uint64_t)(tail[ 7])) << 56;
	case  7: k1 ^= ((uint64_t)(tail[ 6])) << 48;
	case  6: k1 ^= ((uint64_t)(tail[ 5])) << 40;
	case  5: k1 ^= ((uint64_t)(tail[ 4])) << 32;
	case  4: k1 ^= ((uint64_t)(tail[ 3])) << 24;
	case  3: k1 ^= ((uint64_t)(tail[ 2])) << 16;
	case  2: k1 ^= ((uint64_t)(tail[ 1])) << 8;
	case  1: k1 ^= ((uint64_t)(tail[ 0])) << 0;
	k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
	};

	//----------
	// finalization

	h1 ^= len; h2 ^= len;

	h1 += h2;
	h2 += h1;

	h1 = fmix64(h1);
	h2 = fmix64(h2);

	h1 += h2;
	h2 += h1;

	((uint64_t*)out)[0] = h1;
	((uint64_t*)out)[1] = h2;
}

uint64_t
MurmurHash3_64(const void *key, const Size len, const uint64_t seed)
{
	uint64_t hash[2];
	MurmurHash3_128(key, len, seed, &hash);
	return hash[0];
}

/*
 * DatumToBytes
 */
void
DatumToBytes(Datum d, TypeCacheEntry *typ, StringInfo buf)
{
	if (!typ->typbyval && !d)
		return;

	if (typ->type_id != RECORDOID && typ->typtype != TYPTYPE_COMPOSITE)
	{
		Size size;

		if (typ->typlen == -1) /* varlena */
			size = VARSIZE_ANY_EXHDR(DatumGetPointer(d));
		else
			size = datumGetSize(d, typ->typbyval, typ->typlen);

		if (typ->typbyval)
			appendBinaryStringInfo(buf, (char *) &d, size);
		else
			appendBinaryStringInfo(buf, VARDATA_ANY(d), size);
	}
	else
	{
		/* For composite/RECORD types, we need to serialize all attrs */
		HeapTupleHeader rec = DatumGetHeapTupleHeader(d);
		TupleDesc desc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(rec), HeapTupleHeaderGetTypMod(rec));
		HeapTupleData tmptup;
		int i;

		tmptup.t_len = HeapTupleHeaderGetDatumLength(rec);
		tmptup.t_data = rec;

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = desc->attrs[i];
			bool isnull;
			Datum tmp = heap_getattr(&tmptup, i + 1, desc, &isnull);

			if (isnull)
			{
				appendStringInfoChar(buf, '0');
				continue;
			}

			appendStringInfoChar(buf, '1');
			DatumToBytes(tmp, lookup_type_cache(att->atttypid, 0), buf);
		}
	}
}

/*
 * SlotAttrsToBytes
 */
void
SlotAttrsToBytes(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, StringInfo buf)
{
	TupleDesc desc = slot->tts_tupleDescriptor;
	int i;

	num_attrs = num_attrs == -1 ? desc->natts : num_attrs;

	for (i = 0; i < num_attrs; i++)
	{
		bool isnull;
		AttrNumber attno = attrs == NULL ? i + 1 : attrs[i];
		Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[attno - 1];
		Datum d = slot_getattr(slot, attno, &isnull);
		TypeCacheEntry *typ = lookup_type_cache(att->atttypid, 0);

		if (isnull)
		{
			appendStringInfoChar(buf, '0');
			continue;
		}

		appendStringInfoChar(buf, '1');
		DatumToBytes(d, typ, buf);
	}
}

static int default_priority = 0;
#define MAX_PRIORITY 20 /* XXX(usmanm): can we get this from some sys header? */

int
SetNicePriority()
{
	int priority = 0;
	default_priority = getpriority(PRIO_PROCESS, MyProcPid);
	priority = Max(default_priority, MAX_PRIORITY - 
			ceil(continuous_query_proc_priority * (MAX_PRIORITY - default_priority)));

	return nice(priority);
}

int
SetDefaultPriority()
{
	return nice(default_priority);
}

dsm_segment *
dsm_find_or_attach(dsm_handle handle)
{
	dsm_segment *segment = dsm_find_mapping(handle);
	if (segment == NULL)
		segment = dsm_attach(handle);
	return segment;
}
